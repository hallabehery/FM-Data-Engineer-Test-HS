# Tickets: Freemarket Relationship-Network Data Layer

Builds the Bronze → Silver → Gold (Medallion) DuckDB pipeline specified in `plan/SPEC.md`:
ingest four raw sources, normalise every amount to GBP via a point-in-time FX join, and land a
Gold nodes + directed-edges dataset that drives the money-flow network view.

Work the **frontier**: any ticket whose blockers are all done. Foundation goes first; once it
lands, the FX unit and both JSON unpicks open up and can be picked up in parallel.

**Engineering standards (apply to every ticket).** Per `plan/SPEC.md` § Engineering Standards,
and *without* restructuring the mandated six-schema layout: transformation logic lives in tested
`src/` modules (the notebook orchestrates and narrates); every step is idempotent
(`CREATE OR REPLACE`), deterministic, and re-runnable from an empty warehouse; each ingestion
validates its expected columns before proceeding; nothing is silently dropped (quarantine with a
reason); and tests/invariants for a layer are written *with* that layer, runnable as one pass.

## Target warehouse layout (tables per schema)

Naming shown in the clean singular / `snake_case` form (see the naming-cleanup issue). **Facts stay
facts:** a transaction/fee table holds only its own columns — the FX rate is a *dimension*
(`exchange_rate`) and the as-of match is a *separate* per-stream table, never inline columns on the
fact. (This supersedes the earlier approach that put `fx_rate`/`fx_rate_id` on `core.deposit`.)

| Layer.schema | Tables | Role |
|---|---|---|
| **bronze.raw** | `deposit_2025_07 … _12`, `withdrawal_2025_07 … _12` | raw per-month landings, values as-is |
| **bronze.live** | `deposit`, `withdrawal`, `counterparty`, `fee` | consolidated / landed; **the facts live here (one place)** |
| **silver.core** | dims: `company`, `corporate_group`, `counterparty`, `exchange_rate` | unpicked / cleaned reference data |
| | FX match (bridge): `deposit_fx`, `withdrawal_fx`, `fee_fx` | as-of result per `live` fact: `key, fx_instant_ms, fx_rate_id, fx_rate, fx_quarantine_reason` |
| **silver.shape** | `deposit`, `withdrawal`, `fee` (GBP-normalised) | fact ⨝ its `*_fx` → `gbp_amount`; unresolved quarantined; entity attributes resolved |
| **gold.data_mart** | `entity` (+`source`), `edge_fact` (+`source`) | counterpart→group resolution; `focal_group × counterpart × direction × month` measures |
| **gold.curated** | `node`, `edge` | final network product (circles/diamonds + directed edges); reads only from `data_mart` |

Flow: `raw` → `live` (facts) → `core` (dims + FX match) → `shape` (apply FX → GBP) → `data_mart`
(model/aggregate) → `curated` (nodes + edges).

---

## ~~Pipeline foundation — warehouse, six schemas & engineering scaffolding~~ ✅ DONE (`feature/pipeline-foundation`)

**What to build:** A working DuckDB warehouse and the engineering scaffolding every later ticket
builds on: anyone can clone, run one documented command, and get an empty warehouse with all six
schemas plus a passing (empty) test suite. This establishes the tested-module + thin-notebook pattern
so no later ticket has to invent it.

**Blocked by:** None — can start immediately.

- [x] `warehouse.duckdb` is created on connect and reopens cleanly; re-running never errors
- [x] The six schemas `raw`, `live`, `core`, `shape`, `data_mart`, `curated` all exist
- [x] A single config surface holds DB path, FX file + `maximum_object_size`, coverage window, and period (no scattered literals or absolute paths)
- [x] A `src/` package is importable from the notebook; the notebook is a thin orchestrator
- [x] A single documented command runs the pipeline and the test suite; dependencies are pinned
- [x] A lightweight logging/reporting helper exists for per-stage row-count and DQ counts

## ~~FX as-of conversion unit (pure, tested)~~ ✅ DONE (`feature/fx-as-of-conversion`)

**What to build:** An isolated, independently-testable conversion that, given a currency and an
instant, returns the GBP rate effective at that instant — or an explicit quarantine reason when no
rate applies. This is the pipeline's primary risk surface, built and tested before anything depends
on it.

**Blocked by:** Pipeline foundation — warehouse, six schemas & engineering scaffolding.

- [x] Loads `exchange_rates.json` with a raised `maximum_object_size` (default 16 MB is too small)
- [x] Range-matches `validFrom <= t < validTill` on points that are NOT pre-sorted
- [x] `rateStr` is stripped of its JSON quotes before conversion; `GBP = amount × float(rateStr)`
- [x] `GBP` returns rate `1.0` (no series in the file)
- [x] An instant outside `meta.coverage` or in a gap returns a quarantine reason, never a wrong number
- [x] Unit tests cover: exact boundary (from-inclusive, till-exclusive), mid-interval, GBP=1.0,
      out-of-coverage, and an unsorted-points case

## ~~Bronze `raw` — transactional sheets split per month~~ ✅ DONE (`feature/bronze-raw-monthly-split`)

**What to build:** The Deposit and Withdrawals sheets landed as one small table per calendar month
(July–December 2025), so an issue in a single month can be fixed in a small table and flow downstream.

**Blocked by:** Pipeline foundation — warehouse, six schemas & engineering scaffolding.

- [x] Expected sheet columns are validated before landing; a missing/renamed column fails loudly
- [x] Deposit rows land in six month tables (Jul–Dec) under `raw`
- [x] Withdrawals rows land in six month tables (Jul–Dec) under `raw`
- [x] Every source row lands in exactly one month table (no loss, no duplication)
- [x] Month assignment is driven by the transaction's own period, documented

## ~~Bronze `live` — consolidate deposits/withdrawals + land counterparty & fees~~ ✅ DONE (`feature/bronze-live-consolidation`, #6)

**What to build:** A single `deposits` table and a single `withdrawals` table that bring every month
back together in one place, plus the counterparty dimension and fees landed for downstream use. Column
alignment is by name, never position (the two sheets differ in column order).

**Blocked by:** Bronze `raw` — transactional sheets split per month.

- [x] `live.deposits` and `live.withdrawals` each consolidate all six months
- [x] Consolidation aligns columns by name, not position
- [x] Counterparty and fees are landed and queryable
- [x] Conservation: Σ(rows across the month tables) = rows in the consolidated table, per stream

## ~~Silver `core` — companies JSON unpick~~ ✅ DONE (`feature/silver-core-companies-unpick`)

**What to build:** The nested `companies.json` unpicked into a flat, queryable direct-company table,
including the bridge key to its parent group, so the hierarchy can be resolved downstream.

**Blocked by:** Pipeline foundation — warehouse, six schemas & engineering scaffolding.

- [x] One row per company keyed on `dcId`, no duplicates
- [x] Nested registration / classification / financials / footprint fields become columns
- [x] `relationships.parentGroup.value` is exposed as the group bridge key
- [x] Row count matches the source record count

## ~~Silver `core` — groups JSON unpick (first pass)~~ ✅ DONE (`feature/silver-core-groups-unpick`)

**What to build:** The nested `groups.json` (records under `result.groups`) unpicked into a flat,
queryable group table, with the raw heterogeneous `attributes` carried forward in a shape that a later
cleanup step can normalise.

**Blocked by:** Pipeline foundation — warehouse, six schemas & engineering scaffolding.

- [x] One row per group keyed on `groupId`, no duplicates
- [x] Profile / segmentation / lifecycle fields become columns
- [x] Ingestion does not break on the scalar-or-object `attributes` values
- [x] Row count matches the source record count

## ~~Silver `core` — clean facts, `exchange_rate` dim, and FX as-of match (facts stay pure)~~ ✅ DONE (`feature/silver-core-fx-match`, #8)

> **Superseded in part by #24:** the `core.deposit`/`withdrawal`/`fee` fact copies below were
> later removed as redundant with Bronze `live`. `core` now holds only dims + the `*_fx` match,
> which keys back to the `live` facts. The rest of this ticket stands.

**What to build:** Clean, typed transaction/fee **facts** (`core.deposit`, `core.withdrawal`,
`core.fee`) that carry only their own columns — no FX, no GBP. The FX rate points land as their own
dimension (`core.exchange_rate`), and the as-of match is resolved into **separate per-stream match
tables** (`core.deposit_fx`, `core.withdrawal_fx`, `core.fee_fx`) keyed to the fact — so the fact
table is just the fact, and the chosen rate is still inspectable before any arithmetic.

**Blocked by:** FX as-of conversion unit (pure, tested); Bronze `live` — consolidate deposits/withdrawals + land counterparty & fees.

- [x] `core.deposit` / `core.withdrawal` / `core.fee` are clean typed facts with **no** `fx_*` columns
- [x] `core.exchange_rate` holds the rate points (one row per interval: `rate_id, valid_from/till (+ *_ms), rate`)
- [x] `core.deposit_fx` / `core.withdrawal_fx` / `core.fee_fx` hold one row per fact: `key, fx_instant_ms, fx_rate_id, fx_rate, fx_quarantine_reason`
- [x] The settlement instant used for the as-of match is chosen and documented
- [x] Rows with no rate are flagged in the match table with a quarantine reason (not dropped here)
- [x] Lineage: `*_fx.fx_rate_id` joins `core.exchange_rate.rate_id` back to the exact point used
- [x] Conservation: each match table has exactly one row per fact (no fan-out, no loss)

## ~~Silver `shape` — heterogeneous attribute cleanup~~ ✅ DONE (`feature/silver-shape-attributes`, #7)

**What to build:** The remaining JSON cleanup, resolving the group/company `attributes` whose value is
sometimes a scalar and sometimes an object into consistent, queryable columns.

**Blocked by:** Silver `core` — companies JSON unpick; Silver `core` — groups JSON unpick (first pass).

- [x] Scalar-valued attributes and object-valued attributes both resolve without error
- [x] Attribute names that appear in more than one shape are handled by branching on type
- [x] Cleaned entity attributes are queryable as columns
- [x] Conservation: entity row counts are unchanged by the cleanup

## Silver `shape` — FX applied → GBP normalisation

**What to build:** GBP-normalised facts (`shape.deposit` / `withdrawal` / `fee`) produced by joining
each `core` fact to its `core.*_fx` match table and computing `gbp_amount = native_amount × fx_rate`.
Any row whose FX could not be resolved (a quarantine reason in the match table) is routed to
quarantine, so no wrong number reaches Gold.

**Blocked by:** Silver `core` — clean facts, `exchange_rate` dim, and FX as-of match (facts stay pure).

- [ ] Each fact is joined to its `*_fx` match table; `gbp_amount = native_amount × fx_rate`
- [ ] Rows with a match-table quarantine reason are routed to quarantine, not priced
- [ ] Out-of-coverage / gap / unpriced rows are quarantined with a stated reason
- [ ] Conservation: promoted rows + quarantined rows = input rows

## Gold `data_mart` — combined entity tables with provenance

**What to build:** The group and company reference data combined into the entity tables the network
needs, each row carrying a `source` column that lists every source it was composed from, so lineage
is obvious on the row.

**Blocked by:** Silver `shape` — heterogeneous attribute cleanup.

- [ ] Group and company entities are combined into the modelled entity table(s)
- [ ] Counterparties resolve to a group where a deterministic key exists (`Group ID`/`DC Id`), else stand alone
- [ ] Every row carries a `source` column listing all contributing sources
- [ ] Conservation: entity counts reconcile to distinct source groups/companies (the join adds no fan-out)

## Gold `data_mart` — directed edge fact with measures

**What to build:** The core aggregated network fact: one row per focal group × counterpart ×
direction × month, carrying additive GBP volume, transaction count, and GBP fee revenue, with source
provenance — the additive base that year and all-period views roll up from.

**Blocked by:** Silver `shape` — FX applied → GBP normalisation; Gold `data_mart` — combined entity tables with provenance.

- [ ] Grain is focal_group × counterpart × direction × month, one row each
- [ ] Measures `gbp_volume`, `txn_count`, `gbp_fee_revenue` are additive and correct
- [ ] Direction (deposit = inflow / withdrawal = outflow) is applied and documented
- [ ] Each row carries a `source` provenance column
- [ ] Conservation: Σ(`gbp_volume`) reconciles to Σ(promoted GBP amounts) and Σ(`txn_count`) = promoted transaction rows — no join fan-out

## Gold `curated` — network nodes

**What to build:** The node side of the final graph product: group nodes (circles) for counterparts
that resolve to one of our groups, and standalone nodes (diamonds) for those that do not. Reads only
from `data_mart`.

**Blocked by:** Gold `data_mart` — combined entity tables with provenance; Gold `data_mart` — directed edge fact with measures.

- [ ] Every node participating in an edge is present exactly once
- [ ] Group-resolved counterparts are typed as group (circle) nodes
- [ ] Unaffiliated counterparts are typed as standalone (diamond) nodes
- [ ] `curated` reads only from `data_mart`

## Gold `curated` — directed edges, slicing & drill

**What to build:** The edge side of the final product a graph tool can render with no further
transformation: directed edges carrying GBP volume, count and fee revenue, sliceable by month and
year and drillable up and down the group ↔ company hierarchy.

**Blocked by:** Gold `data_mart` — directed edge fact with measures; Gold `curated` — network nodes.

- [ ] Directed edges carry GBP volume, transaction count, and GBP fee revenue
- [ ] Output is sliceable by month and by year (year rolls up from month by summation)
- [ ] Group-to-group flows between two of our own groups are visible
- [ ] Edges are resolvable at direct-company grain so the hierarchy can expand/collapse

## Consolidated conservation test suite

**What to build:** The single test pass that runs every layer's own conservation invariant (each
asserted in its own ticket) *plus* the cross-boundary invariants that no single layer owns — end-to-end
row/measure conservation and quarantine accounting across the whole Bronze → Silver → Gold spine. This
is the one command a reviewer runs to see the pipeline is trustworthy; a failing invariant fails the build.

**Blocked by:** Gold `curated` — directed edges, slicing & drill.

- [ ] All per-layer invariants (from their tickets) run and pass as one suite
- [ ] End-to-end: no unexplained row loss or measure fan-out across Bronze → Silver → Gold
- [ ] Quarantine ledger reconciles end-to-end: total input − total promoted = total quarantined, by reason
- [ ] The suite runs via the single documented command and gates the build (any failure = red)

## Reconciliation against the reference snapshot

**What to build:** A check that the final `curated` nodes/edges for a known focal group and period
reproduce the figures in `star_map_snapshot.png` — the single highest-value seam, pending confirmation
from the data-team lead that those figures are a validated golden reference.

**Blocked by:** Gold `curated` — directed edges, slicing & drill.

- [ ] A focal group + period is reconciled against the snapshot's named-edge figures
- [ ] Volume and transaction count match for at least one named edge (within a documented tolerance)
- [ ] Any dependency on unconfirmed "golden" status is stated explicitly

## Illustrative network render (optional proof)

**What to build:** A single illustrative render of the curated network for a focal group and period,
proving the Gold output drives a graph with no further transformation. Optional proof, not the
deliverable.

**Blocked by:** Gold `curated` — directed edges, slicing & drill.

- [ ] One render (e.g. pyvis/networkx) shows the focal group, circle/diamond nodes, and directed edges
- [ ] Edge labels show GBP volume, count and fee revenue
- [ ] Driven directly from `curated` with no extra shaping

## `WRITEUP.md` — documentation deliverable

**What to build:** The written record that lets a reviewer trust the pipeline without reading all the
code: the FX approach, layer-boundary rationale, each transformation's reasoning, the data-quality
decisions, the live-stream ingestion strategy, and how Gold satisfies the network deliverable.

**Blocked by:** Gold `curated` — directed edges, slicing & drill; Consolidated conservation test suite; Reconciliation against the reference snapshot.

- [ ] FX approach documented (as-of match, quarantine policy, GBP special case)
- [ ] Layer boundaries explained — what went in Bronze vs Silver vs Gold, and why
- [ ] Data-quality issues classified dropped / quarantined / kept, with reasons
- [ ] Live-stream ingestion strategy described (strategy only, not implemented)
- [ ] Explains how the Gold output satisfies the relationship-network deliverable
