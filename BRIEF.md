# The Brief — Freemarket Relationship Network

## Context

You've joined the data team at **Freemarket**, a cross-border payments network. 
Money moves as **deposits** and **withdrawals** through corporate entities ("direct companies") 
that belong to **parent groups**, transacting with **counterparties** in many currencies.

Finance, BI and Commercial all want the same thing and don't have it: a clean,
**GBP-normalised**, trustworthy data layer they can trust to answer "who is moving
how much money with whom, and when". Your job is to build that layer from the raw
sources using **DuckDB**, shaping data with a mix of **SQL and Python** across a
**Bronze → Silver → Gold** (Medallion) architecture, and to land a Gold dataset that
can drive a **relationship-network view** of money flow.

This is a senior role. We care about how you handle messy data, how cleanly you execute
the build, and the quality and trustworthiness of the finished artefact. Build it the way
the data team's working notes (`docs/`) set out.

---

## The data sources

All sources live in `data/`. They are raw extracts from different systems and were
**not** produced for your convenience.

| File | What it is | Key(s) |
|---|---|---|
| `transactional_data_jul25_dec25.xlsx` | The core fact data, **4 sheets**: `Deposit`, `Withdrawals`, `Counterparty`, `Fees`. Covers **July–December 2025**. | `Transaction ID`, `CP ID`, `FeeId` |
| `companies.json` | **Direct companies** — the entities that actually transact. Deeply nested records. | `dcId` |
| `groups.json` | **Parent groups** — the corporate hierarchy the companies roll up into. | `groupId` |
| `exchange_rates.json` | **Time-versioned FX rates** to GBP (~19 MB). | currency code |

### FX usage guidance (read this carefully — reproduced from the file's `meta`)

`exchange_rates.json` is structured as `{ "meta": {...}, "rates": { "series": { "<CCY>": {...} } } }`.

- The base currency is **GBP**, direction **`source_to_base`** — i.e. each rate
  converts *from* the listed currency *to* GBP.
- Each currency has a **`points`** array. Each point is a **positional tuple**, not an
  object, with the field order declared in `meta.tuple.fieldOrder`:
  `[rateId, validFromEpochMs, validTillEpochMs, rateStr, rateMantissaE10]`.
- To convert `X` units of a currency to GBP at instant `t` (epoch milliseconds): find
  the point where `validFromEpochMs <= t < validTillEpochMs`, then
  `GBP = X * float(rateStr)`.
- **The points are NOT pre-sorted.** You must sort or range-match them yourself.
- Rates only cover the window in `meta.coverage` (roughly **2025-06-16 → 2026-01-12**).
  The transaction window sits inside this, but your approach should handle any
  out-of-window instants gracefully rather than crashing or silently mis-pricing.
- **GBP itself has no series** in the file (its rate to GBP is `1.0`) — handle that.

This point-in-time **as-of join** between each transaction/fee timestamp and the
correct FX point for its currency is the central modelling challenge.

## The Gold deliverable — a relationship network

The Gold layer must produce a **GBP-normalised** dataset that can drive a
**network / graph view of money flow** between a **focal group** and its counterparts,
**sliceable by month/year** and **drillable up and down the entity hierarchy**.

A reference picture of the target view is provided: **`docs/star_map_snapshot.png`**.
In that view:

- The **focal group** represents the Group company, which may contain multiple legal entities within. 
  ultimatly we want to be able to drill down a level when needed
- **Other groups** that transact with our focal groups appear as **circle nodes** around the centre.
  Some clients transact with each other, group to group, hence it should be clearly visible in the 
  data set
- **Counterparties not linked to any group** appear as their **own individual nodes**
  (the diamonds in the picture).
- **Edges are directed** and carry the **GBP-normalised volume** and the **transaction
  count** between the focal group and each counterpart (e.g. *"£141.3M (415x) Fee: £15.5k"*).
  Deposits and withdrawals give the direction of flow. 
  Finance will most deifiently want to know how much revenue we earned on those.


## Documentation is a first-class deliverable (required)

Every Medallion decision must be written down so that someone else
could understand and trust your pipeline:

- Layer boundaries — what you placed in Bronze vs Silver vs Gold, and why.
- Each transformation and the reasoning behind it.
- How you handled the data quality issues you found — **dropped / quarantined / kept**,
  and why.
- Your FX approach.
- How your Gold output satisfies the network deliverable.

This lives in `submission/WRITEUP.md`.

## Working conventions

Before you start building, read `docs/` for outputs and build framework and follow them.

## What we assess

| Dimension | What we're looking for |
|---|---|
| **Planning** | How you scope, sequence and prioritise the work. |
| **Design** | The Gold model — does the output cleanly satisfy the network deliverable for any group / period? |
| **Code & Build** | Correct, readable SQL + Python; sound FX as-of join; the joins reconciled. |
| **Documentation** | The write-up: clear, honest, enough to trust the pipeline. |
| **Presentation** | Your live walkthrough of methodology, model and the resulting view. |

## Ground rules

- **AI tools are allowed and encouraged** — our team works AI-first. We care about *how*
  you work with AI

Start with `SETUP.md` to get your environment running, then build your pipeline in a notebook under `notebooks/`.

