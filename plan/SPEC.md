# Spec — Freemarket Relationship-Network Data Layer

> Source docs: `README.md`, `BRIEF.md`, `docs/build_protocol.md`, `docs/star_map_snapshot.png`, `SETUP.md`.
> This is a take-home for a Senior Data Engineer role. The "product" is a trustworthy,
> GBP-normalised Medallion pipeline in DuckDB whose Gold output drives a directed
> money-flow network graph.

## Problem Statement

Freemarket's Commercial and Finance leaders cannot answer a basic question about their
own payments network — *"who is moving how much money with whom, and when?"* — because
the raw data is scattered across four heterogeneous systems (a 4-sheet Excel workbook,
three differently-nested JSON files), denominated in many currencies, and carries no
single trustworthy, GBP-normalised view. There is no clean layer they can slice by
period or drill through the group → company hierarchy, and no way to see the flow of
money (and the fee revenue earned on it) between a focal group and its counterparties.

## Solution

Build a **Bronze → Silver → Gold (Medallion) pipeline in a single local DuckDB file**
that ingests the four raw sources, normalises every monetary amount to **GBP** via a
point-in-time (as-of) FX join, and lands a **Gold relationship-network dataset**: a set
of **directed edges** (focal group ↔ counterpart) carrying **GBP volume, transaction
count, and fee revenue**, plus the **nodes** (groups as circles, unaffiliated
counterparties as diamonds). The dataset is **sliceable by month/year** and
**drillable up and down the group ↔ company hierarchy**, and can drive a star-map view
matching `docs/star_map_snapshot.png` for any focal group and period. Every modelling
decision is documented in `submission/WRITEUP.md` so the pipeline can be trusted.

## User Stories

1. As a Finance analyst, I want every transaction amount converted to GBP at the rate
   effective at the transaction's own timestamp, so that cross-currency totals are
   comparable and correct.
2. As a Finance analyst, I want fee amounts also normalised to GBP, so that I can see
   the revenue Freemarket earned on each relationship.
3. As a Commercial lead, I want to pick a focal group and see every counterpart it
   transacted with, so that I understand that group's relationship network.
4. As a Commercial lead, I want counterparts that are themselves one of our groups shown
   as group nodes and unaffiliated counterparties shown as standalone nodes, so that the
   picture reflects the real corporate hierarchy.
5. As a Commercial lead, I want group-to-group flows to be visible, so that I can see
   when two of our own clients transact with each other.
6. As an analyst, I want each edge to be directed (inflow vs outflow), so that I know
   which way money moved between the focal group and each counterpart.
7. As an analyst, I want each edge to carry both GBP volume and a transaction count, so
   that I can distinguish a few large flows from many small ones.
8. As an analyst, I want to slice the whole network by month and by year, so that I can
   see how relationships change over the Jul–Dec 2025 window.
9. As an analyst, I want to drill from a group down to its constituent direct companies
   (and back up), so that I can attribute flows to the entity that actually transacted.
10. As a data consumer, I want the focal group to aggregate all of its legal entities,
    so that the group-level view is complete.
11. As a data engineer, I want the transactional data landed per-month in Bronze `raw`,
    so that I can fix a problem in one small month-table and have it flow through to the
    consolidated tables.
12. As a data engineer, I want all months consolidated into single `deposits` and
    `withdrawals` tables in Bronze `live`, so that downstream layers read one place.
13. As a data engineer, I want the groups and companies JSON unpicked in Silver, so that
    nested registration, relationship, and attribute fields become queryable columns.
14. As a data engineer, I want heterogeneous `attributes` (scalar-or-object values)
    handled robustly, so that ingestion doesn't break on shape differences.
15. As a data engineer, I want FX rates attached to each transaction/fee in Silver `core`
    and the GBP amounts computed in Silver `shape`, so that normalisation is a clear,
    inspectable two-step.
16. As a data engineer, I want out-of-coverage or unmatched FX instants handled
    explicitly (quarantined, not silently mis-priced), so that no wrong number reaches
    Gold unnoticed.
17. As a data engineer, I want each Gold `data_mart` row to carry a `source` column
    listing every source that composed it, so that lineage is obvious on the row.
18. As a data steward, I want data-quality issues (duplicates, nulls, bad currency codes,
    out-of-window timestamps, column mismatches) classified as dropped / quarantined /
    kept with a reason, so that the pipeline's trustworthiness is auditable.
19. As a reviewer, I want the reference snapshot's figures reproducible for its focal
    group and period, so that I can trust the whole pipeline against a known-good target.
20. As a reviewer, I want a written FX approach, layer-boundary rationale, and ingestion
    strategy (including how a live stream would be ingested), so that I can evaluate the
    design without reading all the code.
21. As a data engineer, I want the six schemas (`raw`, `live`, `core`, `shape`,
    `data_mart`, `curated`) created up front, so that every stage is explicit and
    separately queryable.
22. As a reporting consumer, I want `curated` to read only from `data_mart`, so that the
    final product has a single, clean upstream.
23. As an analyst, I want the `curated` output to be shaped as nodes + directed edges,
    so that a graph tool can render it with no further transformation.

## Implementation Decisions

**Architecture (from `docs/build_protocol.md` — authoritative, do not "clean up"):**
- Six schemas created up front: Bronze = `raw` + `live`; Silver = `core` + `shape`;
  Gold = `data_mart` + `curated`.
- Bronze `raw`: transactional sheets split into one table **per month** (Jul–Dec).
- Bronze `live`: consolidate to single `deposits` and `withdrawals` tables. Deposit and
  Withdrawals sheets differ in column order — align on **column name**, never position.
- Silver `core`: first-pass JSON unpicking of groups + companies; FX rate *attached* to
  each transaction/fee row (as-of match), but GBP not yet computed.
- Silver `shape`: remaining JSON cleanup; FX **applied** to produce GBP-normalised
  amounts.
- Gold `data_mart`: modelled/aggregated network data with a `source` provenance column;
  entity (client/group) tables may be combined here but transactional aggregation is the
  core. `curated` reads from `data_mart` only (never the reverse).
- Gold `curated`: final nodes + directed-edges product driving the network view.

**FX as-of join (the central modelling challenge):**
- `exchange_rates.json` read with raised `maximum_object_size` (~64 MB). Points are
  positional tuples `[rateId, validFrom, validTill, rateStr, rateMantissaE10]` per
  `meta.tuple.fieldOrder`, **not pre-sorted** — range-match `validFrom <= t < validTill`.
- Convert: `GBP = amount * float(rateStr)` (`rateStr` is JSON-quoted — strip quotes).
- GBP has no series → rate = 1.0, handled as a special case.
- Instants outside `meta.coverage` (~2025-06-16 → 2026-01-12) or falling in a gap are
  **quarantined with a reason**, never silently converted. (Engineering decision, ours
  to make and document — not a question for the data-team lead.)
- The FX conversion is implemented as an isolated, independently-testable unit (the
  primary risk surface).

**Network model:**
- Grain of the edge fact: `focal_group × counterpart × direction × period(month)`, with
  additive measures `gbp_volume`, `txn_count`, `gbp_fee_revenue`. Year and all-period
  roll up from month by summation.
- Node types: group nodes (circles) for counterparts that resolve to one of our groups;
  standalone nodes (diamonds) for counterparties that do not.
- Drill: edges also resolvable at direct-company grain so the hierarchy can expand/collapse.

**Open questions carried as documented assumptions (candidate for a single stakeholder email
to the data-team lead):**
- **[Ask — highest value] Reconciliation:** are the figures in `star_map_snapshot.png`
  a *validated* reference for their focal group over the full window, usable as a golden
  test? Phrase so the answer yields checkable numbers (focal group id, exact period,
  volume + count for at least one named edge). This is the only genuinely external unknown.
- **[Self-serve] FX timestamp choice:** if a transaction carries multiple dates
  (created/settled/value), pick the economic settlement instant; document the choice.
- **[Document, do not ask] Direction convention:** infer deposit = inflow to the client
  / withdrawal = outflow to counterpart from the data and state the assumption.

**Resolved from the data (no longer open):**
- **Counterparty → group linkage — a deterministic key exists.** The `Counterparty` sheet
  carries `Group ID` and `DC Id` columns that join directly to `groups.json` (`groupId`) and
  `companies.json` (`dcId`). Profiling confirmed all 222 populated counterparties resolve with
  **zero orphans**; the remaining 1,363 carry neither key and are the standalone (diamond)
  nodes by design. No heuristic name-matching is needed and no escalation is required.

## Testing Decisions

Good tests here assert **external, observable pipeline behaviour** (row-level outputs and
conservation properties), not the internal SQL. Proposed seams, highest first:

1. **Primary seam — Gold `curated` reconciliation.** Assert the final nodes/edges for a
   known focal group + period reproduce the reference snapshot figures (pending
   confirmation from the data-team lead that they are golden). If `curated` is correct, everything upstream is
   correct. This is the single highest seam.
2. **FX as-of conversion (pure function).** Unit-test `(currency, timestamp) → rate`:
   exact-boundary match (`validFrom` inclusive, `validTill` exclusive), a mid-interval
   instant, GBP → 1.0, an out-of-coverage instant → quarantined, and an unsorted-points
   case. Highest-risk logic, testable in isolation, so it earns its own seam.
3. **Layer-conservation invariants.** Assert measure/row conservation across boundaries:
   sum(Bronze raw per-month) = Bronze `live`; no unexplained row loss or fan-out
   Bronze → Silver → Gold; quarantined rows accounted for = input − promoted. Catches
   join fan-out and silent drops — the classic pipeline failure.

Prior art: none in-repo (greenfield); tests live alongside the notebook/`src` helpers as
assertion cells / lightweight `assert` checks reconciled at each layer.

## Engineering Standards (non-functional)

These apply to *how* the build is written; they do **not** override the build protocol.
The six-schema layout is mandated and graded on fidelity — these standards raise code
quality **within** that structure, never restructure it.

- **Thin notebook, tested modules.** Transformation logic (FX conversion, JSON unpicking,
  aggregation, DQ classification) lives in importable, unit-testable `src/` functions with
  type hints and docstrings. The notebook *orchestrates and narrates* — it calls the
  helpers and shows results; it does not hide untested logic in cells.
- **Runs clean top-to-bottom.** A fresh "Restart & Run All" of the notebook (and a single
  documented command to run the pipeline + tests) succeeds from an empty warehouse with no
  manual steps and no out-of-order cell dependencies.
- **Idempotent & deterministic.** Every step is re-runnable (`CREATE OR REPLACE`), produces
  the same output on repeat runs, and uses explicit ordering — no reliance on unordered
  result sets. Re-running the whole pipeline never duplicates rows.
- **Configuration over hardcoding.** DB path, FX file + `maximum_object_size`, coverage
  window, and the transaction period live in one config surface, not scattered literals.
  No magic numbers or hardcoded absolute paths.
- **Validate inputs; fail loud.** Each ingestion asserts its expected columns exist before
  proceeding (schema-on-read contract), so upstream shape drift errors immediately rather
  than corrupting a downstream layer. Nothing is silently dropped or null-filled — every
  exclusion is a quarantine with a reason.
- **Test shifted left.** The FX unit and each layer-conservation invariant are asserted *as
  that layer is built*, not only at the end, and are runnable as a single test pass. A
  failing invariant fails the build.
- **Observability.** Each stage reports row-count in/out and DQ counts
  (dropped/quarantined/kept) so a reader can see conservation hold as the pipeline runs.
- **Reproducible environment.** Pinned dependencies and a documented `README`/run recipe so
  the reviewer reproduces the exact result. `warehouse.duckdb` is a build output, not
  hand-edited.
- **Consistent naming.** Table, column, and function names follow one convention and the
  domain glossary (group / direct company / counterpart / edge / node), so lineage reads
  cleanly across layers.

## Out of Scope

- Rendering a production graph UI. We produce the Gold data that *drives* a graph; a
  single illustrative render (e.g. pyvis/networkx) is optional proof, not the deliverable.
- Any server, Docker, or non-local warehouse — the whole warehouse is one DuckDB file.
- Editing the raw sources in `data/` (read-only).
- Real-time/streaming ingestion implementation — only the *strategy* is written up.
- Non-network analytics (e.g. modelling `annualRevenue`) beyond what the network needs.
- Slides content (separate deliverable prepared for the discussion round).

## Further Notes

- AI-first is expected and assessed on *how* it's used.
- Deliverables land in `submission/`: populated `warehouse.duckdb`, notebook(s)/SQL,
  `WRITEUP.md`, slides.
- The build protocol is deliberately non-elegant by the author's own admission; fidelity
  to it is a graded requirement — do not refactor it into something "cleaner."
- Assessment axes: Planning, Design, Code & Build, Documentation, Presentation.
