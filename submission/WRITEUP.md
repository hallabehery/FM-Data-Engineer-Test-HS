# Freemarket Relationship-Network Write-up

This document includes how the pipeline is laid out, where each transformation lives and why, how FX and data-quality edge cases were handled, and how the Gold output drives the money-flow network. Companion docs:
[`plan/ARCHITECTURE.md`](../plan/ARCHITECTURE.md) (topology/data-flow) and
[`docs/build_protocol.md`](../docs/build_protocol.md) (the data team's build order).

## What it is and how to run it

It's a Bronze → Silver → Gold pipeline in a single DuckDB file. It ingests four raw sources, a
4-sheet Excel workbook and three nested JSON files, and converts every amount to GBP at the rate
effective at its own instant. The output is a **directed money-flow network**: groups and their
counterparts as nodes, and directed edges carrying GBP volume, transaction count and fee revenue.
The whole network slices by month/year and drills down the group → company hierarchy.

```bash
make install     # pinned deps into a venv
make pipeline    # build submission/warehouse.duckdb end-to-end
make test        # FX + per-layer + conservation suite
make render      # optional: submission/star_map.html, an illustrative graph
```

All transformation logic lives in tested `src/` modules; the notebook
[`pipeline.ipynb`](../notebook/pipeline.ipynb) just orchestrates and narrates them top-to-bottom.

For scale, this data drop: **12,982 transactions** (6,383 deposits + 6,599 withdrawals), 21,921
fees, 1,585 counterparties, 13 groups, 44 companies — resolving to **1,376 nodes** (13 group
circles + 1,363 standalone diamonds) and **4,110 directed edges** carrying **£26.15 bn** of volume.

## How it's laid out

### one file, six schemas

The whole warehouse is a single DuckDB file, so the medallion layers map to **schemas**, not
catalogs. This is a deliberate choice worth explaining, because DuckDB shares the
`catalog.schema.table` namespace with Databricks/Unity Catalog, where the idiom is "catalog =
bronze/silver/gold". In DuckDB a catalog is a *physical database* — one file per `ATTACH` — so
that idiom would need three separate `.duckdb` files, which breaks the single-file constraint.
Instead the three layers each split into two schemas inside the one `warehouse` catalog:

| Layer | Schemas | Job |
|---|---|---|
| **Bronze** | `raw`, `live` | land the sources faithfully |
| **Silver** | `core`, `shape` | clean, resolve relationships, normalise to GBP |
| **Gold** | `data_mart`, `curated` | model the network, then present it |

If the single-file rule was lifted, the layers would lift cleanly to catalogs
(`ATTACH 'bronze.duckdb' AS bronze` → `bronze.raw.deposit`) with the schema names unchanged — a
mechanical migration. That trade-off is the intended talking point for the discussion round.

One principle underpins everything: **keep the raw fact pure.** The transaction and fee facts land
once in Bronze `live`. Silver `core` doesn't copy them — it attaches FX as a separate table keyed
back to the `live` fact, rather than bolting `fx_*` columns onto the fact itself. Silver `shape`
*does* then materialise cleaned, GBP-normalised fact tables and Gold aggregates them — that
per-layer materialisation is ordinary medallion practice; the discipline is only that the raw fact
is never mutated in place. Keeping lineage traceable to an untouched source row is what makes
row/measure counts checkable at every boundary (see [Data quality](#data-quality)).

## The pipeline, layer by layer

**Bronze - land it, don't touch it.** The Deposit and Withdrawal sheets are split into one `raw`
table per month, keyed off each transaction's own date (not the workbook's convenience
`Tx Month`), so a problem in one month is a small, isolated fix that flows through on rebuild; the
build fails loud if any row falls outside the Jul–Dec window. `raw` is landed **as-is** — source
column names and types preserved, nothing projected away — so it stays a faithful, auditable
mirror of the workbook. `live` then reunites the months into single `deposit`/`withdrawal` tables
(and lands `counterparty` and `fee`), and it's *here*, not in `raw`, that names are conformed:
the two sheets differ in column order, so consolidation projects each to `snake_case` and combines
them with `UNION ALL BY NAME` — aligned on name, never position. Conforming names is a cleaning
step, so it belongs at the `live` boundary; `raw` stays true to source. Values and types are
untouched throughout.

**Silver - clean and convert.** `core` does the heavy lifting: it unpicks the nested
`companies.json` and `groups.json` into flat columns (validating the top-level shape first, so a
source change fails here rather than corrupting Gold), lands the FX rate points as a dimension, and
resolves the FX as-of match into per-stream `*_fx` tables keyed 1:1 back to each `live` fact.
`shape` finishes the job: it flattens the awkward `attributes` array into `attr_*` columns
(branching on whether each value is a scalar, an object, or an array, so a shape difference can't
break ingestion), and — after asserting each fact's business key is unique — it *applies* FX
(`gbp_amount = native × rate`). Priced rows land in the cleaned, GBP-normalised facts
`shape.deposit`/`withdrawal`/`fee` (everything downstream reads these); anything unpriceable (bad
or missing currency, out-of-coverage instant, or a null amount) is split off into a parallel
per-stream quarantine table — `shape.deposit_quarantine` / `withdrawal_quarantine` /
`fee_quarantine` — carrying its `quarantine_reason`, so the excluded rows are kept and inspectable
rather than dropped. Those quarantine tables are **terminal**: Gold reads only the promoted facts,
so an un-priceable row is never silently carried into the network product — it stops at Silver as
an auditable record.

Splitting FX across the two schemas is intentional: `core` *selects* the rate, `shape` *applies*
it. That separates the two ways FX can go wrong — the wrong rate chosen versus the wrong
arithmetic — into two stages you can inspect independently, and it keeps the `live` fact pure (no
`fx_*` columns bolted onto it).

**Gold - model, then present.** `data_mart` holds the modelled network, each row tagged with a
`source` provenance list. `entity` unifies groups, companies and counterparties, resolving each
counterparty to a group where a deterministic key exists (its `group_id`, or `dc_id → company →
group`); the ~1,363 with neither key stand alone by design. `money_flow` is the directed aggregate,
stored at its finest `focal_group × focal_company × counterpart × direction × month` grain — keeping
the company grain is what makes the hierarchy drill-down recoverable later. Fees attach to their
transaction by `(link_id, fee_type)`, which disambiguates the 306 transaction IDs that legitimately
appear in both streams.

`curated` is the thin, graph-ready layer reporting reads from. It reads **only** from `data_mart`
(never the reverse), and is a near-passthrough: `node` picks the circle/diamond nodes that
participate in an edge, and `edge` is a 1:1 projection of `money_flow` into directed `source →
target` rows. The one enrichment is the drill label: `edge` carries `focal_company_name` beside the
`focal_company_id` key (joined from the `entity` dimension, still within `data_mart`), so a consumer
can render the group→company drill straight from `curated` without a further join. Keeping it a pure
projection is what lets a graph tool consume it with no reshaping — and lets a test assert equal
rows and measures against `money_flow`.

## FX transformation

Every amount is converted at the rate effective **at its own instant** (a point-in-time / as-of
join), because cross-currency totals only compare if each amount is priced at its own moment. The
logic is isolated in one pure, independently tested unit (`src/fx.py`) — it's the highest-risk
surface, so it earns its own seam and needs neither DuckDB nor the file to test.

Each currency's rate points are positional tuples and arrive **unsorted**, so `FxRates` sorts each
currency once on load and range-matches `validFrom ≤ t < validTill` by bisect (`validFrom`
inclusive, `validTill` exclusive). The settlement instant is `Tx Date + Tx Time` for transactions,
and the fee `Date` at midnight UTC for fees. **GBP is the base currency and has no series in the
file**, so it's an explicit identity — `rate = 1.0`, no rate point — not a fudge. The file is ~18 MB
(over DuckDB's default 16 MB JSON limit), so it's read with a raised object-size limit.

Crucially, nothing is ever silently mis-priced. Anything we can't price at its instant is
**quarantined with a stated reason** — kept aside in its stream's `shape.*_quarantine` table, not
dropped and not null-filled — so the ledger reconciles and the row stays auditable:

| Reason | When |
|---|---|
| `fx_out_of_coverage` | instant outside the file's coverage window (≈ 2025-06-16 → 2026-01-12) |
| `fx_unknown_currency` | currency has no series in the file |
| `fx_no_rate_point` | within coverage, but a gap in that currency's series spans the instant |
| `fx_missing_currency` | the row itself has no currency |
| `fx_missing_instant` | the row itself has no settlement instant |

Every priced fact also carries the `fx_rate_id` of the exact point that priced it, so any GBP figure
traces back to its rate and validity window.

## Data quality

The policy is simple: **nothing vanishes silently.** Every exclusion is classified — **dropped /
quarantined / kept** — with a reason, and each layer boundary asserts a conservation count as it
runs. The facts themselves actually arrived clean, so Silver's work on them is GBP normalisation
plus the one quarantine below. Importantly, the integrity checks are **enforced, not just
observed**: a duplicate fact key fails the build, and an unpriceable row is quarantined — so the
*next* data drop can't slip a bad row through, even though this one has none. Here's everything the
data threw up:

| What we found | Count | Verdict | Why |
|---|---:|---|---|
| Fees with no currency | 42 | **quarantined** | can't be priced (`fx_missing_currency`); kept aside, not dropped — the *entire* quarantine ledger |
| Transaction IDs in both Deposit and Withdrawals | 306 | **kept** | not duplicates — distinct streams reusing IDs; fees key on `(link_id, fee_type)` so nothing fans out |
| Counterparties with no group/company key | 1,363 | **kept** | standalone by design → diamond nodes, exactly as the brief expects |
| Counterparties with a key | 222 | **kept** | resolve to a group with **zero orphans**, so no fragile name-matching was needed |
| `CP website` breaks type inference | — | **kept** | that one sheet lands as all-VARCHAR; others keep inference so dates/amounts stay typed |
| Duplicate `transaction_id`/`fee_id` within a stream | 0 | **would fail loud** | `validate_facts` asserts a unique key before pricing — a dup would inflate volume/count and fan out fee revenue |
| Transaction/fee with a null amount | 0 | **would quarantine** | can't be priced (`amount_missing`); routed aside like a null currency, never promoted as a silent null GBP |
| Transactions outside Jul–Dec | 0 | **would fail loud** | Bronze raises rather than dropping — a period contract |

**Nothing is dropped.** Every row is either promoted to GBP or quarantined with a reason. The
conservation suite gates the build on all of it: per-month rows sum to `live`; each fact has a
unique key and matches exactly one FX row; promoted + quarantined = input; the 12,982 transactions
survive Bronze → Silver → Gold uncounted-out and land as `curated.edge` `txn_count`; and GBP
volume/fee totals are unchanged from Silver through to `curated`. If any invariant breaks, `make
test` goes red.

## Running it live — what would actually change

Today the pipeline is pure batch: `python -m src.pipeline` reads the whole workbook and rebuilds
every table (`CREATE OR REPLACE`) in one pass. If deposits and withdrawals arrived as a
**live-stream** instead, most of that is untouched — the changes are concentrated at the two ends:
how rows get *in*, and how much gets *rebuilt*.

**What stays exactly the same:** the FX as-of unit (`src/fx.py`), the JSON dimension unpicking, the
network model (`entity` → `money_flow` → `curated`), the quarantine rules, and the conservation
checks. The current design is built so the transformation logic doesn't care whether a row came
from a sheet or a stream — facts live once and every step is idempotent.

**What we'd change — four concrete things:**

1. **Swap the ingestion source.** `bronze.build_raw_monthly` reads an Excel sheet today; replace
   that one function with a stream consumer (Kafka / Kinesis / webhook) that appends incoming events
   to the Bronze `raw` month partition. Nothing downstream of `live` needs to know.
2. **Make Bronze writes append-only and idempotent.** Bronze currently does `CREATE OR REPLACE`
   (full rebuild); live, it becomes `INSERT … ON CONFLICT DO NOTHING` keyed on the natural key
   (`transaction_id` + stream, `fee_id`), so at-least-once delivery and replays can't double-count.
3. **Process incrementally instead of rebuilding.** Track a watermark (last offset / timestamp) and
   run FX-match → GBP → aggregate for only the new rows. Gold `money_flow` is additive on its grain,
   so a micro-batch is an **upsert into the affected month's aggregate** rather than a full
   `GROUP BY`; `curated` stays a thin refresh off `data_mart`.
4. **Refresh FX as a live dimension.** `FxRates.load()` reads the file once; live, keep the lookup in
   memory and refresh the rate series on a schedule. A late-arriving rate point then re-prices rows
   previously quarantined as `fx_no_rate_point`, so nothing stays stuck.

Two supporting pieces: an **orchestrator** to replace the single script — a timer-driven micro-batch
or a streaming runtime (Airflow / Dagster, or a stream engine) — and a short **late-data window** so
a correction arriving after month-end revises that month's aggregate (corrections are new events,
never in-place edits to Bronze).

## Does it deliver the network?

Yes — `curated` is the deliverable, shaped so a graph tool renders it directly. Groups are **circle**
nodes and unaffiliated counterparties are **diamond** nodes; edges are **directed** (deposit =
**inflow** to the focal group, withdrawal = **outflow**) and carry the three measures the brief's
reference labels show — GBP volume, transaction count, and fee revenue, so Finance sees the revenue
earned on each relationship. Group-to-group flows are first-class: **449** edges have a group on both
ends, so "two of our own clients transacting" is visible, not hidden. Every edge carries `month` and
`year` for slicing, and both `focal_group_id` and `focal_company_id` so the hierarchy can **drill**
down to the entity that actually transacted and roll back up. Any focal group and period is a `WHERE`
clause away — nothing needs reshaping.

`make render` proves it: it reads only `curated.node` + `curated.edge`, hands them straight to pyvis,
and produces [`star_map.html`](star_map.html) with no transformation in between (it picks a focal
group that visibly connects to other groups, so the circle↔circle structure shows). Hovering the
focal group even reveals its drill level — the direct companies it rolls up **by name** (from
`curated.edge.focal_company_name`) — so the hierarchy is visible, not just present in the data. (It's
a static illustrative snapshot, not a click-to-expand app — that production UI is out of scope.)

The drill itself is proven in SQL in a separate notebook, [`notebook/drill.ipynb`](../notebook/drill.ipynb):
using `curated` alone it shows the group-grain star view, resolves the same group down to its named
direct companies, drills into one company's own counterparts, and confirms the group total is exactly
the sum of its companies (drill is lossless) — all sliceable by month.

## Key decisions

The load-bearing choices and why we made them (fuller detail in the sections above); where a
decision had a real alternative, the rejected one is noted.

- **One catalog, six schemas — not a catalog per layer.** A DuckDB catalog is a physical file, so
  the Databricks "catalog = bronze/silver/gold" idiom would need three attached files and break the
  single-file rule. The layers map to schemas instead.
- **`raw` faithful to source; conform names in `live`.** Renaming columns is a cleaning step, so
  `raw` stays a byte-faithful mirror (`SELECT *`, source names) and the `snake_case` map is applied
  at the `raw → live` consolidation. *Rejected:* renaming at `raw` landing (breaks the mirror and
  silently drops any unmapped column) and renaming in Silver (fights "facts live once in `live`").
- **Keep the raw fact pure — attach FX, don't bolt it on.** Silver `core` holds FX as a separate
  table keyed to the `live` fact rather than adding `fx_*` columns to the fact. This splits "which
  rate" (`core`) from "apply the rate" (`shape`) into two inspectable failure modes and keeps
  lineage back to an untouched source row.
- **Facts live once; later layers reference them.** The transaction/fee facts aren't copied into
  `core`; `shape` materialises cleaned GBP facts and Gold aggregates them — ordinary per-layer
  materialisation, but the raw fact is never mutated in place.
- **FX as an isolated, pure unit.** The as-of conversion `(currency, instant) → rate | reason` lives
  in one tested module that needs neither DuckDB nor the file — it's the highest-risk logic, so it
  earns its own seam.
- **Quarantine, never silently mis-price — and quarantine is terminal.** Unpriceable rows are kept
  aside in Silver `shape.*_quarantine` with a reason; Gold reads only the promoted facts, so no
  wrong or null number reaches the network product.
- **DQ handling belongs in Silver, not Gold.** Detecting and quarantining is Silver's job; Gold
  holds only trustworthy, cleaned data. *Considered and deferred:* a Gold DQ *reporting* view to
  serve the steward's "auditable" story as a product.
- **Enforce fact integrity, don't just assume it.** `validate_facts` fails the build on a duplicate
  business key, and a null amount is quarantined — both no-ops on this drop, but they mean the facts
  are clean *because the build won't let them not be*, protecting the next data drop.
- **Money-flow stored at the finest (company) grain.** The group view is the roll-up; keeping
  `focal_company_id` is what makes the hierarchy drill-down recoverable, since `curated` reads only
  from `data_mart`.
- **`curated` reads only from `data_mart`.** The final nodes/edges product has a single clean
  upstream and is a pure projection, so a graph tool renders it with no further shaping.
- **Counterpart → group resolution uses a deterministic key** (`group_id`, else `dc_id → company →
  group`). Profiling found zero orphans, so no fragile name-matching — counterparties with no key
  are standalone diamonds by design, not errors.

## Assumptions

- **Direction** (deposit = inflow, withdrawal = outflow) and the **FX settlement instant** (`Tx Date + Tx Time`; fees at their date) are inferred from the data and documented. 

- **The reference snapshot is illustrative, not a golden target.** `docs/star_map_snapshot.png` is
  from a different data drop, so its figures aren't reproducible here and aren't used as a test; it's a
  guide to the *shape* (node types, edge direction, label format), not the numbers. Whether any snapshot
  figure is a validated reference for a named group and period is the one genuinely external unknown —
  the single thing worth a stakeholder email. Everything else was resolvable from the data.

Beyond that, trust rests on the build being reproducible: every step is `CREATE OR REPLACE` and
deterministic (a clean rebuild reproduces the output, never duplicates rows); paths, the FX window and
the period live in one config surface; dependencies are pinned; and `warehouse.duckdb` is a build
output, not hand-edited.
