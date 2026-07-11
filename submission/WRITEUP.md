# Freemarket Relationship-Network Data Layer — Write-up

The written record that lets a reviewer trust the pipeline without reading all the code:
the FX approach, the layer boundaries and why, every transformation's reasoning, the
data-quality decisions, the live-stream ingestion strategy, the warehouse-topology
decision, and how the Gold output satisfies the network deliverable.

> **Companion docs:** [`plan/SPEC.md`](../plan/SPEC.md) (requirements), [`plan/ARCHITECTURE.md`](../plan/ARCHITECTURE.md)
> (topology/data-flow), [`docs/build_protocol.md`](../docs/build_protocol.md) (the data team's
> authoritative build order). This file is the narrative that ties them to the delivered warehouse.

---

## 0. TL;DR — what was built and how to run it

A **Bronze → Silver → Gold (Medallion) pipeline in a single local DuckDB file**
(`submission/warehouse.duckdb`) that ingests four heterogeneous raw sources, normalises
every monetary amount to **GBP** via a point-in-time (as-of) FX join, and lands a **directed
money-flow network**: nodes (groups as circles, unaffiliated counterparties as diamonds) and
directed edges (focal group ↔ counterpart) carrying **GBP volume, transaction count and GBP fee
revenue**, sliceable by month/year and drillable up/down the group↔company hierarchy.

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt      # or: make install
make pipeline                        # build the warehouse end-to-end (python -m src.pipeline)
make test                            # run the conservation + FX + layer test suite
make render                          # optional: submission/star_map.html illustrative proof
```

The notebook [`notebook/pipeline.ipynb`](../notebook/pipeline.ipynb) is a thin orchestrator that
runs the same tested `src/` helpers top-to-bottom and narrates each stage ("Restart & Run All"
from an empty warehouse). All transformation logic lives in importable, unit-tested `src/`
modules; the notebook hides no logic.

**Headline figures (this data drop):** 12,982 transactions (6,383 deposits + 6,599 withdrawals),
21,921 fees, 1,585 counterparties, 13 groups, 44 direct companies. The network resolves to
**1,376 nodes** (13 group circles + 1,363 standalone diamonds) and **4,110 directed edges**
(449 of them group-to-group), carrying **£26.15 bn** of GBP volume and **£1.46 m** of fee revenue.

---

## 1. Warehouse topology — one catalog, six schemas (catalog vs schema)

DuckDB has the same three-level namespace as Databricks / Unity Catalog —
**`catalog.schema.table`** — so the instinct is to map the medallion layers onto *catalogs*
(the Databricks "catalog = bronze/silver/gold" pattern). We deliberately did **not** do that.

**Why not a catalog per layer.** In DuckDB a **catalog is a physical database** — one file, one
`ATTACH` — not a logical container. A catalog-per-layer layout (`bronze`, `silver`, `gold`) would
therefore require **three separate `.duckdb` files** attached together. That directly conflicts
with the brief's hard constraint that *the whole warehouse is a single local DuckDB file*, and
`docs/build_protocol.md` names the six schemas directly.

**What we did instead.** The medallion layers map to the **schema** level inside the single
`warehouse` catalog. Every table is addressable as `warehouse.<schema>.<table>` (or just
`<schema>.<table>`):

| Layer | Schemas |
|---|---|
| **Bronze** | `raw`, `live` |
| **Silver** | `core`, `shape` |
| **Gold** | `data_mart`, `curated` |

All six schemas are created up front (SPEC user story 21) so every stage is explicit and
separately queryable.

**If the single-file rule were ever lifted**, the 3-catalog layout is exactly how you'd express it
in DuckDB — `ATTACH 'bronze.duckdb' AS bronze; ATTACH 'silver.duckdb' AS silver; …` then reference
`bronze.raw.deposit`. The schema names we chose (`raw`, `live`, …) would become the middle level
unchanged, so the migration is mechanical. This is the intended talking point for the discussion
round.

---

## 2. Layer boundaries — what went in Bronze vs Silver vs Gold, and why

The guiding rule: **each schema has one job, and facts live once.** The transaction/fee facts are
landed in Bronze `live` and never copied forward — Silver and Gold *reference* them (FX match keyed
to them, GBP computed by joining to them) rather than duplicating them. This keeps a single source
of truth and makes conservation checkable at every boundary.

### Bronze — land the raw sources with minimal transformation

- **`raw`** — the two transactional sheets (`Deposit`, `Withdrawals`) split into **one table per
  month** (`deposit_2025_07 … _12`, `withdrawal_2025_07 … _12`). *Why per-month:* the build
  protocol asks for it, and it means a problem in one month can be fixed in one small table and
  flow through to the consolidated view. Month is assigned from the transaction's **own** `tx_date`
  (not the convenience `tx_month` column), and the build **fails loud** if any row falls outside
  the six-month window.
- **`live`** — consolidate the per-month tables into single `deposit` and `withdrawal` tables, and
  land the `counterparty` and `fee` sheets. Deposit and Withdrawal sheets differ in column *order*,
  so consolidation uses `UNION ALL BY NAME` — aligned on **column name, never position** (an
  explicit brief warning). Values/types are preserved; only column **names** are conformed to
  singular `snake_case` at landing (one canonical map in `src/naming.py`).

*Boundary rationale:* Bronze is faithful landing only — no currency conversion, no relationship
resolution. If a downstream layer is wrong you can always diff it back to an untouched Bronze row.

### Silver — clean, conform, resolve relationships, normalise to GBP

- **`core`** does the heavy lifting: first-pass unpicking of the nested `companies.json` →
  `core.company` and `groups.json` → `core.corporate_group`; the FX rate points landed as a
  dimension `core.exchange_rate`; and the FX **as-of match** resolved into **separate** per-stream
  tables `core.deposit_fx` / `core.withdrawal_fx` / `core.fee_fx`, each keyed 1:1 back to its `live`
  fact. The facts are **not** copied here.
- **`shape`** does the remaining cleanup: the heterogeneous entity `attributes` array flattened to
  queryable `attr_*` columns, and FX **applied** — each `live` fact joined to its `core.<stream>_fx`
  match, `gbp_amount = native × fx_rate`, with any unpriceable row routed to a `*_quarantine` table.

*Boundary rationale — why FX is split across `core` and `shape`:* `core` **selects** the rate (the
as-of match), `shape` **applies** it (the arithmetic). Splitting these separates the two failure
modes — *wrong rate chosen* vs *wrong multiplication* — into two independently inspectable stages,
mirrors the build protocol's two steps, and keeps the `live` fact pure (no `fx_*` columns bolted
onto it). See §3.

### Gold — model the network, then present it

- **`data_mart`** holds the modelled/aggregated network with a **`source` provenance column** on
  every row (SPEC user story 17): `entity` (groups + companies + counterparties with
  counterpart→group resolution) and `money_flow` (the directed aggregate at its finest grain,
  `focal_group × focal_company × counterpart × direction × month`).
- **`curated`** is the final product reporting reads from — `node` (circle/diamond nodes) and `edge`
  (directed `source → target` edges). **`curated` reads only from `data_mart`, never the reverse**
  (SPEC user story 22), so the final product has a single clean upstream.

*Boundary rationale:* `data_mart` is where modelling decisions and provenance live; `curated` is a
thin, graph-ready projection. Keeping them separate means the presentation shape can change without
touching the model, and a graph tool can consume `curated` with **no further transformation** — see
§6 and the illustrative render (`make render`).

### Each transformation and why (beyond the boundary rationale above)

The layer boundaries above explain *where* work happens; these are the individual transformations
and the reasoning behind each (FX has its own section, §3):

- **Per-month split → consolidate (`raw` → `live`).** Split by the transaction's own `tx_date` so a
  bad month is fixable in one small table; re-consolidate with `UNION ALL BY NAME` because the two
  sheets differ in column order and aligning by position would silently mis-map columns.
- **JSON unpick (`core.company`, `core.corporate_group`).** `companies.json` (under `records`) and
  `groups.json` (under `result.groups`) are unnested and their nested structs projected to flat
  columns, with the top-level shape validated first (fail loud) so a source change errors here rather
  than corrupting Gold. The `parentGroup.value` is surfaced as the group bridge key.
- **Heterogeneous `attributes` → `attr_*` columns (`shape`).** The `attributes` array's `value` is
  *sometimes a scalar and sometimes an object* (e.g. `country_of_incorporation → {label, iso2}`), so
  the cleanup **branches on the value's type** — scalar → `attr_<name>`, object → one
  `attr_<name>_<key>` per key, array → JSON text. Reasoning: a shape difference must never break
  ingestion, and if the same attribute is scalar in some rows and an object in others, each row
  simply populates whichever columns apply (the rest NULL). Entity row counts are unchanged.
- **Counterpart → group resolution (`data_mart.entity`).** Each counterparty resolves to a group via
  its `group_id`, else via `dc_id → company → parent group`, else stands alone. Reasoning: a
  **deterministic key exists** (profiling found zero orphans among populated counterparties), so no
  fragile name-matching is used; unresolved counterparties are standalone **diamonds** by design, not
  errors. Resolved counterparties carry no node shape — they are represented by their group's circle.
- **Directed money-flow aggregation (`data_mart.money_flow`).** Deposits/withdrawals are unioned with
  a `direction` tag, the focal group/company derived from `dc_id → company → group`, the counterpart
  resolved as above, and fees attached per transaction by **`(link_id, fee_type)`** (which
  disambiguates the 306 transaction ids shared across the two streams), then aggregated to
  `focal_group × focal_company × counterpart × direction × month` with additive measures. Reasoning:
  storing the **finest (company) grain** keeps the drill-down recoverable downstream, where `curated`
  reads only from `data_mart`; the group view is simply the roll-up.
- **Node/edge projection (`curated`).** `node` selects the distinct edge endpoints that carry a node
  shape; `edge` is a 1:1 projection of `money_flow` into directed `source → target` rows. Reasoning:
  keep `curated` a pure projection so the graph-ready guarantee is conservation-checkable (equal rows
  and measures to `money_flow`).

---

## 3. FX approach — the central modelling challenge

Every monetary amount is converted to GBP **at the rate effective at the transaction's own
instant** (a point-in-time / as-of join), because cross-currency totals are only comparable when
each amount is priced at its own moment. The FX logic is isolated in a pure, independently tested
unit (`src/fx.py`, `FxRates`) — it is the highest-risk surface, so it earns its own seam and is
testable without DuckDB or the file.

### The as-of match

- `exchange_rates.json` (~18 MB, base **GBP**, direction `source_to_base`) is read with a raised
  `maximum_object_size` (64 MB) — the file exceeds DuckDB's default 16 MB JSON object limit.
- Each currency's rate points are **positional tuples**
  `[rateId, validFromEpochMs, validTillEpochMs, rateStr, rateMantissaE10]` per `meta.tuple.fieldOrder`
  and are **not pre-sorted**. `FxRates` sorts each currency's points once on load, then range-matches
  `validFrom ≤ t < validTill` by **bisect** (`validFrom` inclusive, `validTill` exclusive).
- The settlement instant used for the match: **`Tx Date + Tx Time`** for transactions; the fee
  **`Date`** (midnight, UTC) for fees — fees carry only a date.
- `rateStr` is JSON-quoted (e.g. `"0.8639"`) — the quotes are stripped before `float()`; then
  `GBP = amount × rate`.

### The GBP special case

**GBP has no series in the file** (it is the base). It is handled as an explicit identity:
`rate = 1.0` at any instant, with no rate point and no lineage id. This is a branch in
`FxRates.rate_at`, not a data hack.

### Quarantine policy — never silently mis-price (SPEC user story 16)

Any amount we cannot price at its own instant is **quarantined with a stated reason**, never
converted with a wrong or nearest rate and never null-filled. Reasons (stable strings, so downstream
can group on them):

| Reason | Meaning |
|---|---|
| `fx_out_of_coverage` | instant outside the file's `meta.coverage` window (≈ 2025-06-16 → 2026-01-12) |
| `fx_unknown_currency` | currency has no series in the file |
| `fx_no_rate_point` | within coverage, but no point spans this instant (a gap in that currency's series) |
| `fx_missing_currency` | the row itself has a NULL currency (can't even consult FX) |
| `fx_missing_instant` | the row itself has a NULL settlement instant |

Quarantined rows are **kept** in `shape.<stream>_quarantine` tables (not dropped) so they are
auditable and the ledger reconciles (§4). Deciding coverage/gap handling was an engineering call we
made and documented, not an external question.

### Lineage

The rate points are persisted as `core.exchange_rate` (`rate_id`, `valid_from/till`, `rate`), and
each priced fact carries the `fx_rate_id` of the exact point that priced it. So
`shape.<stream>.fx_rate_id → core.exchange_rate.rate_id` traces any GBP figure back to the precise
rate and its validity window. (This drop: 161,342 rate points across 36 currencies; the transactions
use 10 distinct currencies.)

---

## 4. Data quality — dropped / quarantined / kept, with reasons (SPEC user story 18)

The policy is **nothing vanishes silently**: every exclusion is classified with a reason, and each
layer boundary asserts a conservation invariant (`src/reporting.StageReport` logs rows in/out and
DQ counts as the pipeline runs). The consolidated suite `tests/test_conservation.py` gates the build
on these holding.

| Issue found | Count | Classification | Reason / handling |
|---|---:|---|---|
| Fee rows with NULL `Fee currency` | 42 | **Quarantined** | `fx_missing_currency` — kept in `shape.fee_quarantine`, not priced, not dropped. The **entire** quarantine ledger for this drop. |
| `Transaction ID` appearing in both Deposit *and* Withdrawals | 306 | **Kept** | Not duplicates — deposits and withdrawals are distinct streams that legitimately reuse ids. Fees disambiguate onto a transaction by `(link_id, fee_type)`, so the shared id never fans out. |
| Counterparties with neither `Group ID` nor `DC Id` | 1,363 | **Kept** | Standalone by design → **diamond** nodes. Not an error: the brief expects unaffiliated counterparties to stand alone. |
| Counterparties resolving to a group | 222 | **Kept** | Resolve via `group_id`, else `dc_id → company → parent group`. Profiling confirmed **zero orphans** among populated counterparties, so no heuristic name-matching was needed. |
| `Counterparty.CP website` defeats type inference | — | **Kept** | The Counterparty sheet is landed `all_varchar` to avoid a spurious inference failure; other sheets keep inference so dates/amounts land typed (not Excel serials). |
| Transactions out of the Jul–Dec window | 0 | **Would fail loud** | Bronze raises if any row lands outside the six month tables — a schema/period contract, not a silent drop. |

**Dropped: none.** No row is discarded in this drop — everything is promoted to GBP or quarantined
with a reason. **Quarantined:** the 42 null-currency fees. **Kept:** everything else, including the
"messy but valid" cases above.

**Conservation invariants asserted (the trust story):**

- Σ(per-month `raw` tables) = consolidated `live`, per stream.
- Each `core.<stream>_fx` match is exactly **one row per `live` fact** (no fan-out/loss); every
  priced `fx_rate_id` is a real `exchange_rate` point.
- At the `shape` boundary, **promoted + quarantined = input**, per stream.
- End-to-end, the **12,982 transactions** are never lost or fanned out — the count is invariant
  Bronze → Silver → Gold and lands as `curated.edge` `txn_count`.
- **GBP volume and fee revenue are conserved** Silver → `data_mart` → `curated` (no aggregation
  invents or drops value).
- The quarantine ledger reconciles: `Σ(input) − Σ(promoted) = Σ(quarantined)`, and the whole ledger
  is `{fx_missing_currency: 42}`.

---

## 5. Live-stream ingestion strategy (strategy only — not implemented)

The batch pipeline lands the transactional workbook per-month in Bronze `raw`. If deposits and
withdrawals arrived as a **live stream** instead, the medallion shape stays the same — only the
Bronze landing changes from "read a sheet" to "append events." The design intent:

1. **Ingest append-only into Bronze `raw`.** A stream consumer (e.g. Kafka/Kinesis → an object-store
   sink, or DuckDB via `read_json`/Arrow micro-batches) appends raw events partitioned **by event
   month** — the same per-month grain the batch build already uses. Land raw and immutable: no
   conversion, no resolution, exactly as a sheet is landed today. Idempotency via the natural key
   (`transaction_id` + stream) so replays/at-least-once delivery don't double-count.
2. **Schema-on-read contract at the edge.** Reuse the existing `_validate_columns` fail-loud check so
   a producer that drops/renames a column errors at ingestion rather than corrupting a downstream
   layer. Unknown-but-additive fields are tolerated; missing required fields are rejected/dead-lettered.
3. **Consolidate incrementally into `live`.** Rather than rebuilding `live.deposit` wholesale, append
   the new micro-batch and process only the new rows downstream (a high-watermark on
   ingest time / offset).
4. **FX as-of match on arrival.** The rate series is slow-moving relative to transactions, so cache
   `FxRates` in memory and resolve each event's rate at its own instant as it lands — the **same pure
   unit** used in batch. Late-arriving rate points or events whose instant has no rate yet are
   **quarantined with the same reasons** (`fx_out_of_coverage`, `fx_no_rate_point`, …) and
   re-evaluated when coverage catches up, rather than blocking the stream.
5. **Incremental Gold.** `money_flow` is additive on `(focal_group, focal_company, counterpart,
   direction, month)`, so a streaming build is an **upsert into the month's aggregate** (or a
   materialised view refreshed per micro-batch). `curated` stays a thin projection and is refreshed
   from `data_mart`.
6. **Late & out-of-order data.** Keep a small reprocessing window per month (allow a month's aggregate
   to be revised for N days after month end) and treat corrections as new events, never in-place edits
   to Bronze.

The key property preserved: **the transformation logic is identical** — batch and stream differ only
in *how rows enter Bronze `raw`* and in *incremental vs full rebuild*, because the facts already live
once and every downstream step is idempotent (`CREATE OR REPLACE` / additive aggregates).

---

## 6. How the Gold output satisfies the relationship-network deliverable

The brief asks for a GBP-normalised dataset that drives a **network / graph view of money flow**
between a **focal group** and its counterparts, sliceable by month/year and drillable up/down the
hierarchy. `gold.curated` is that dataset, shaped as **nodes + directed edges** so a graph tool
renders it with no further transformation (SPEC user story 23):

- **Focal group + counterparts, as a star.** `curated.node` is one row per node that participates in
  an edge. Groups are **circle** nodes; unaffiliated counterparties are **diamond** nodes — matching
  the reference view's shapes. Companies (the drill level) and group-resolved counterparties carry no
  node shape: they are represented by their group's circle.
- **Group-to-group flows are visible.** When a counterpart is itself one of our groups, both ends are
  circles. There are **449** such group-to-group edges in this drop — so "two of our own clients
  transacting" is explicit in the data (SPEC user story 5).
- **Directed edges with the required measures.** `curated.edge` carries `source_node_id →
  target_node_id`, `direction` (**deposit = inflow** to the focal group, **withdrawal = outflow**),
  and the three measures the reference labels show: **GBP volume, transaction count, and GBP fee
  revenue** — so Finance can see the revenue earned on each relationship, and an analyst can tell a
  few large flows from many small ones.
- **Sliceable by month and year.** Every edge carries `month` (the additive base) and `year` (rolls
  up from month by summation), so the whole network can be sliced to any period.
- **Drillable up and down the hierarchy.** Edges carry both `focal_group_id` (roll up) and
  `focal_company_id` (drill to the entity that actually transacted) — the group view is the sum over
  its companies, so the hierarchy expands and collapses without losing detail.

Any focal group and period is a `WHERE` clause away; nothing else needs reshaping. This is
demonstrated by [`make render`](../src/render.py), which reads **only** `curated.node` +
`curated.edge` and hands them straight to pyvis — proof (optional, per SPEC § Out of Scope) that the
Gold output is genuinely graph-ready. See [`submission/star_map.html`](star_map.html).

---

## 7. Assumptions & open questions (carried as documented decisions)

- **Direction convention.** Inferred from the data and stated as an assumption: **deposit = inflow**
  to the focal group, **withdrawal = outflow**. Documented rather than asked.
- **FX settlement instant.** Where a transaction could carry multiple dates, the economic settlement
  instant (`Tx Date + Tx Time`) is used; fees use their `Date` at midnight UTC. Self-serve choice,
  documented.
- **Reference snapshot is illustrative, not golden.** `docs/star_map_snapshot.png` is a picture of
  the *target view* from a **different data drop**, so its exact figures are **not** reproducible
  against this warehouse and are **not** used as a golden test (the reconciliation ticket #15 was
  closed invalid on that basis). We treat the snapshot as a shape/spec reference — node types, edge
  direction, and the label format `£volume (count) Fee: £fee` — not as expected numbers. The single
  genuinely external unknown (whether any snapshot figure is a validated reference for a named focal
  group + period) is the one item that would warrant a stakeholder email; everything else was
  resolvable from the data.

---

## 8. Why you can trust it (reproducibility & tests)

- **Idempotent & deterministic.** Every step is `CREATE OR REPLACE` with explicit ordering; a full
  re-run from an empty warehouse produces the same output and never duplicates rows.
- **Config over hardcoding.** DB path, FX file + object-size limit, coverage window, and the period
  months live in one surface (`src/config.py`) — no scattered literals or absolute paths.
- **Tested, shifted-left.** The FX unit and each layer-conservation invariant are asserted as that
  layer is built and run as one pass (`make test`); a failing invariant fails the build. The single
  highest seam — the consolidated conservation suite — reconciles the whole Bronze → Silver → Gold
  spine plus the quarantine ledger in one file.
- **Pinned environment.** Dependencies are pinned in `requirements.txt`; `warehouse.duckdb` is a
  build output, not hand-edited.
