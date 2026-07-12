# Architecture — Freemarket Relationship-Network Data Layer

A durable reference for **how the warehouse is structured and how data flows through it**.
Requirements live in [`SPEC.md`](SPEC.md); the work breakdown and status live in
[`tickets.md`](tickets.md) and the GitHub issues. This document is the design/topology — it changes
only when the design changes.

---

## 1. Overview

- The entire warehouse is a **single local DuckDB file** — `submission/warehouse.duckdb`. No server,
  no Docker.
- **Medallion architecture**, Bronze → Silver → Gold, with each layer split into two schemas so every
  stage is explicit and separately queryable (per `docs/build_protocol.md`, which is authoritative).
- Goal: a **GBP-normalised relationship-network dataset** — directed money-flow edges between a focal
  group and its counterparts, sliceable by month/year and drillable up/down the group↔company
  hierarchy.

## 2. Warehouse topology — catalog vs schema

DuckDB has the same three-level namespace as Databricks/Unity Catalog: **`catalog.schema.table`**.
But a DuckDB **catalog is a physical database** (one file / one `ATTACH`), not a logical container.

- The Databricks-style "catalog = bronze/silver/gold" pattern would need three attached `.duckdb`
  files, which conflicts with the hard constraint that the whole warehouse is **one file**.
- So the medallion layers map to the **schema** level inside the single `warehouse` catalog. Tables
  are addressable as `warehouse.<schema>.<table>` (or just `<schema>.<table>`).
- If the single-file rule were ever lifted, the 3-catalog layout
  (`ATTACH 'bronze.duckdb' AS bronze;` → `bronze.raw.deposit`) is how you'd do it in DuckDB.

## 3. Layers & schemas

| Layer | Schemas | Purpose |
|---|---|---|
| **Bronze** | `raw`, `live` | Land the raw sources with minimal transformation |
| **Silver** | `core`, `shape` | Clean, conform, resolve relationships (`core`); apply FX → GBP + final cleanup (`shape`) |
| **Gold** | `data_mart`, `curated` | Model/aggregate the network (`data_mart`); final nodes + edges product (`curated`) |

All six schemas are created up front.

## 4. Tables per schema

Names shown in the clean **singular / `snake_case`** form. **Facts stay facts** — the
transaction/fee facts live once in Bronze `live` (not copied into `core`); FX is a *dimension*
and the as-of match is a *separate* `core` table keyed to the `live` fact, never inline `fx_*`
columns on the fact.

| Layer.schema | Tables | Role |
|---|---|---|
| **bronze.raw** | `deposit_2025_07 … _12`, `withdrawal_2025_07 … _12` | raw per-month landings, values as-is |
| **bronze.live** | `deposit`, `withdrawal`, `counterparty`, `fee` | consolidated / landed; the facts + the counterparty dimension live here |
| **silver.core** | dims: `company`, `corporate_group`, `exchange_rate` | unpicked / cleaned reference data |
| | FX match: `deposit_fx`, `withdrawal_fx`, `fee_fx` | as-of result per `live` fact: `key, fx_instant_ms, fx_rate_id, fx_rate, fx_quarantine_reason` |
| **silver.shape** | `company`, `corporate_group` (attributes resolved); `deposit`, `withdrawal`, `fee` (GBP-normalised) | entity `attributes` flattened to columns; fact ⨝ its `*_fx` → `gbp_amount`, unresolved quarantined |
| **gold.data_mart** | `entity` (+`source`), `money_flow` (+`source`) | `entity` = groups + companies (Silver) + counterparties (`live`) with counterpart→group resolution + provenance; `money_flow` = `focal_group × focal_company × counterpart × direction × month` measures (finest grain; group view is the roll-up) |
| **gold.curated** | `node`, `edge` | final network product: `node` = circle/diamond nodes participating in edges; `edge` = directed `source→target` edges with GBP volume/count/fee, sliceable by month/year and drillable group↔company (carries `focal_company_name` beside `focal_company_id` so the drill renders without a dimension join); reads only from `data_mart` |

> `group` is a SQL reserved word, so the group dimension is named `corporate_group` (avoids pervasive quoting).

## 5. Data flow

```
companies.json ──┐ unpick ─────────────────► core.company
groups.json ─────┤ unpick ─────────────────► core.corporate_group
exchange_rates ──┘ points ─────────────────► core.exchange_rate
                                                     │  as-of match (FX unit, reads live facts)
Excel Deposit/Withdrawal ─► raw.deposit_MM ─► live.deposit ───────────┐  (fact — lives here only)
                            (per month)      (consolidated)           │
Excel Counterparty ───────────────────────► live.counterparty        │
Excel Fees ───────────────────────────────► live.fee ────────────────┤  (fact — lives here only)
                                                     │                │
                          core.deposit_fx (key, rate_id, rate, instant, reason) ◄─┘
                                                     │
                                                     ▼
                    shape.deposit  =  live.deposit ⨝ core.deposit_fx
                                      gbp_amount = tx_value_ccy × fx_rate   (+ quarantine unpriceable)
                    shape.withdrawal / shape.fee   (same pattern)
                                                     │
                    gold.data_mart.entity   +   gold.data_mart.money_flow
                                                     │   (curated reads only from data_mart)
                    gold.curated.node       +   gold.curated.edge   ──►  network view
```

## 6. FX design (the central modelling challenge)

- **As-of join.** Each amount is converted at the rate effective **at the transaction's own instant**.
  The settlement instant is `Tx Date + Tx Time` for transactions and the fee `Date` (midnight, UTC)
  for fees.
- **Isolated, tested unit** (`src/fx.py`, `FxRates`). Pure `(currency, instant) → rate | quarantine`:
  sorts the (unsorted) rate points once, range-matches `validFrom ≤ t < validTill` by bisect, treats
  `GBP` as the identity (`1.0`), strips the JSON-quoted `rateStr`, and reads the ~18 MB file with a
  raised `maximum_object_size`.
- **Dimension, not inline columns.** The rate points are persisted as `core.exchange_rate`
  (`rate_id, valid_from/till (+ *_ms), rate`), and the match per fact lives in `core.<stream>_fx`
  (keyed to the `live` fact). Facts are never touched; GBP is computed in `shape`.
- **Why a separate match table** (rather than resolving FX only in `shape`): it mirrors the
  build-protocol's two steps — `core` *attaches* the rate (selection: the as-of match →
  `core.<stream>_fx`), `shape` *applies* it (`× rate` → GBP). This separates the two failure modes
  (wrong rate *selected* vs wrong *arithmetic*) into two inspectable stages, and keeps the `live`
  fact untouched. The alternative — fold match + apply into `shape` — is fewer tables but collapses
  those two steps and deviates from the protocol's `core`-attaches-FX step.
- **Lineage.** `core.<stream>_fx.fx_rate_id` → `core.exchange_rate.rate_id` traces each transaction to
  the exact rate point (and its validity window) that priced it.
- **Quarantine reasons** (never silently mis-price): `fx_out_of_coverage`, `fx_unknown_currency`,
  `fx_no_rate_point` (gap within coverage), `fx_missing_currency`, `fx_missing_instant`.

## 7. Data quality & conservation

- Every exclusion is **dropped / quarantined / kept with a reason** — nothing vanishes silently.
- **Conservation invariants** are asserted as each layer is built (via `src/reporting.StageReport`,
  which logs rows in/out and DQ counts): e.g. Σ(raw month tables) = `live`; attach/cleanup change no
  row counts; promoted + quarantined = input at the `shape` boundary; no measure fan-out into Gold.
- A consolidated test suite runs every layer's invariant plus the cross-boundary checks as one pass.
- **Known DQ findings:** 42 fee rows have a NULL `Fee currency` → `fx_missing_currency` (kept, not
  priced); 306 `Transaction ID`s appear in both Deposit and Withdrawals (distinct streams).

## 8. Naming conventions (target — issue #20)

- **Tables:** singular, `snake_case` (`deposit`, not `Deposits`).
- **Columns:** `lower_snake_case`, no spaces/slashes/parentheses (`tx_value_ccy`, not `"Tx Value (CCY)"`).
- One canonical rename map (`src/naming.py`), applied when consolidating Bronze `raw` → `live`
  (conforming names is a cleaning step, so `raw` stays source-faithful and `live` onward is
  `snake_case`); raw **values/types** preserved, only names conformed. (This deviates from
  `build_protocol.md`'s literal plural table names — an intentional, documented cleanup.)

## 9. Engineering standards (summary)

Full text in [`SPEC.md`](SPEC.md) § Engineering Standards. In brief: transformation logic in tested
`src/` modules (the notebook orchestrates and narrates); idempotent (`CREATE OR REPLACE`),
deterministic, re-runnable from an empty warehouse; a single config surface (no scattered literals);
inputs validated at ingestion (fail loud); pinned dependencies; one command runs the pipeline + tests.

## 10. Where things live

```
data/                       four raw sources (read-only)
docs/build_protocol.md      authoritative layer/schema layout
plan/SPEC.md                requirements/spec
plan/tickets.md             work breakdown + status
plan/ARCHITECTURE.md        this document (design/topology)
src/                        pipeline modules (config, naming, warehouse, bronze, silver_core, silver_shape, fx, gold, reporting, pipeline)
tests/                      per-layer + FX unit tests
notebooks/pipeline.ipynb    thin orchestrator that calls src/
submission/warehouse.duckdb built output, committed per SETUP.md §8 (rebuild: `make pipeline`)
```
