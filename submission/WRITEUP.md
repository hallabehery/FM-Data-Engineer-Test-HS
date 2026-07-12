# Freemarket Relationship-Network Write-up

This document includes how the pipeline is laid out, where each transformation lives and why, how FX and data-quality edge cases were handled, and how the Gold output drives the money-flow network. Companion docs:
[`plan/ARCHITECTURE.md`](../plan/ARCHITECTURE.md) (topology/data-flow) and
[`docs/build_protocol.md`](../docs/build_protocol.md) (the data team's build order).

## What it is, and how to run it

It's a Bronze → Silver → Gold pipeline in a single DuckDB file. It ingests four raw sources — a
4-sheet Excel workbook and three nested JSON files — and converts every amount to GBP at the rate
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

## How it's laid out — one file, six schemas

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

If the single-file rule were ever lifted, the layers would lift cleanly to catalogs
(`ATTACH 'bronze.duckdb' AS bronze` → `bronze.raw.deposit`) with the schema names unchanged — a
mechanical migration. That trade-off is the intended talking point for the discussion round.

One rule underpins everything: **facts live exactly once.** The transaction and fee facts land in
Bronze `live` and are never copied forward — every later layer references them rather than
duplicating them. That keeps a single source of truth and makes row/measure counts checkable at
every boundary (see [Data quality](#data-quality)).

## The pipeline, layer by layer

**Bronze — land it, don't touch it.** The Deposit and Withdrawal sheets are split into one `raw`
table per month, keyed off each transaction's own date (not the workbook's convenience
`tx_month`), so a problem in one month is a small, isolated fix that flows through on rebuild; the
build fails loud if any row falls outside the Jul–Dec window. `live` then reunites them into single
`deposit`/`withdrawal` tables and lands the `counterparty` and `fee` sheets. The two sheets differ
in column order, so consolidation is `UNION ALL BY NAME` — aligned on name, never position. Values
and types are untouched; only column names are conformed to `snake_case`.

**Silver — clean and convert.** `core` does the heavy lifting: it unpicks the nested
`companies.json` and `groups.json` into flat columns (validating the top-level shape first, so a
source change fails here rather than corrupting Gold), lands the FX rate points as a dimension, and
resolves the FX as-of match into per-stream `*_fx` tables keyed 1:1 back to each `live` fact.
`shape` finishes the job: it flattens the awkward `attributes` array into `attr_*` columns
(branching on whether each value is a scalar, an object, or an array, so a shape difference can't
break ingestion), and it *applies* FX — `gbp_amount = native × rate` — sending anything unpriceable
to a quarantine table.

Splitting FX across the two schemas is intentional: `core` *selects* the rate, `shape` *applies*
it. That separates the two ways FX can go wrong — the wrong rate chosen versus the wrong
arithmetic — into two stages you can inspect independently, and it keeps the `live` fact pure (no
`fx_*` columns bolted onto it).

**Gold — model, then present.** `data_mart` holds the modelled network, each row tagged with a
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
target` rows. Keeping it a pure projection is what lets a graph tool consume it with no further
shaping — and lets a test assert equal rows and measures against `money_flow`.

## FX — the hard part

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
**quarantined with a stated reason** and kept (not dropped, not null-filled), so the ledger
reconciles and the row is auditable:

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
runs. Here's everything the data threw up in this drop:

| What we found | Count | Verdict | Why |
|---|---:|---|---|
| Fees with no currency | 42 | **quarantined** | can't be priced (`fx_missing_currency`); kept aside, not dropped — the *entire* quarantine ledger |
| Transaction IDs in both Deposit and Withdrawals | 306 | **kept** | not duplicates — distinct streams reusing IDs; fees key on `(link_id, fee_type)` so nothing fans out |
| Counterparties with no group/company key | 1,363 | **kept** | standalone by design → diamond nodes, exactly as the brief expects |
| Counterparties with a key | 222 | **kept** | resolve to a group with **zero orphans**, so no fragile name-matching was needed |
| `CP website` breaks type inference | — | **kept** | that one sheet lands as all-VARCHAR; others keep inference so dates/amounts stay typed |
| Transactions outside Jul–Dec | 0 | **would fail loud** | Bronze raises rather than dropping — a period contract |

**Nothing is dropped.** Every row is either promoted to GBP or quarantined with a reason. The
conservation suite gates the build on this: per-month rows sum to `live`; each fact matches exactly
one FX row; promoted + quarantined = input; the 12,982 transactions survive Bronze → Silver → Gold
uncounted-out and land as `curated.edge` `txn_count`; and GBP volume/fee totals are unchanged from
Silver through to `curated`. If any invariant breaks, `make test` goes red.

## If the source were a live-stream

Today the workbook is loaded per-month in batch. If deposits and withdrawals instead arrived as a
**live-stream**, the medallion shape wouldn't change — only *how rows enter Bronze* and *batch vs
incremental* would. The plan:

- **Land append-only** into Bronze `raw`, partitioned by event month (the grain we already use),
  raw and immutable. Dedupe on the natural key (`transaction_id` + stream) so at-least-once delivery
  and replays don't double-count.
- **Validate at the edge** with the same fail-loud column check, so a producer dropping a field
  errors on arrival instead of corrupting a downstream layer.
- **Match FX on arrival** using the same pure unit, with `FxRates` cached in memory. Events whose
  instant has no rate yet are quarantined with the *same* reasons and re-evaluated when coverage
  catches up — never blocking the stream.
- **Upsert Gold incrementally.** `money_flow` is additive on its grain, so a micro-batch is an upsert
  into the affected month's aggregate; `curated` stays a thin refresh off `data_mart`.
- **Tolerate late data** with a short per-month revision window, treating corrections as new events
  rather than in-place edits.

This works precisely because the facts already live once and every step is idempotent — batch and
stream share the same transformation code.

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
and produces [`star_map.html`](star_map.html) with no transformation in between.

## Assumptions and what would need a question

- **Direction** (deposit = inflow, withdrawal = outflow) and the **FX settlement instant** (`Tx Date
  + Tx Time`; fees at their date) are inferred from the data and documented rather than asked — they're
  ours to decide.
- **The reference snapshot is illustrative, not a golden target.** `docs/star_map_snapshot.png` is
  from a different data drop, so its figures aren't reproducible here and aren't used as a test; it's a
  guide to the *shape* (node types, edge direction, label format), not the numbers. Whether any snapshot
  figure is a validated reference for a named group and period is the one genuinely external unknown —
  the single thing worth a stakeholder email. Everything else was resolvable from the data.

Beyond that, trust rests on the build being reproducible: every step is `CREATE OR REPLACE` and
deterministic (a clean rebuild reproduces the output, never duplicates rows); paths, the FX window and
the period live in one config surface; dependencies are pinned; and `warehouse.duckdb` is a build
output, not hand-edited.
