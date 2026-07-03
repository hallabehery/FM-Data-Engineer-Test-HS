# Senior Data Engineer — Take-Home Exercise

Welcome. This is a self-contained, local data engineering exercise. You'll build a
**Bronze → Silver → Gold** (Medallion) pipeline in **DuckDB** from four raw sources
(three JSON, one multi-sheet Excel) and produce a **GBP-normalised relationship-network
dataset** that can drive a money-flow graph between corporate groups and their
counterparties.

No server, no Docker — the whole warehouse is a single local DuckDB file.

## Start here

1. **[`SETUP.md`](SETUP.md)** — set up your environment and learn how to read each
   source into DuckDB.
2. **[`BRIEF.md`](BRIEF.md)** — the task, the deliverable, scope, and what we assess.
3. **[`notebooks/`](notebooks/)** — create your working notebook here.

## What's in the repo

```
data/                       four raw sources (do not edit)
docs/star_map_snapshot.png  reference picture of the target network view
notebooks/                  your working notebook(s)
src/                        your Python modules (optional)
submission/                 put your deliverables here (write-up + slides + warehouse)
```

Put your deliverables in `submission/`. Good luck — we're looking forward to seeing how
you think.
