# Setup & Usage Guide

Everything you need to get the environment running and to read each data source into
DuckDB. This guide gets you to the **starting line** — it intentionally does **not**
solve the modelling for you.

---

## 1. Prerequisites

- **Python 3.10+** (the repo was verified on 3.13).
- No database server, no Docker. The entire warehouse is a single local DuckDB file.

---

## 2. Set up the environment

Create and activate a virtual environment, then install dependencies.

**macOS / Linux**

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

**Windows (PowerShell)**

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

**Windows (cmd)**

```bat
python -m venv .venv
.venv\Scripts\activate.bat
pip install -r requirements.txt
```

Then launch JupyterLab and create your working notebook under `notebooks/`:

```bash
jupyter lab
```

---

## 3. How the engine works (DuckDB in 60 seconds)

- DuckDB is an in-process analytical database. The **whole warehouse is one file**
  (`warehouse.duckdb`); there is no server to run.
- Open a persistent connection and run SQL:

  ```python
  import duckdb
  con = duckdb.connect("warehouse.duckdb")   # created if it doesn't exist
  con.sql("SELECT 42 AS answer").show()
  ```

- `con.sql(...)` returns a relation you can `.show()`, `.df()` (to a pandas
  DataFrame), or `.fetchall()`.
- **pandas interop is seamless** — a DataFrame in scope can be queried by name:

  ```python
  import pandas as pd
  my_df = pd.DataFrame({"x": [1, 2, 3]})
  con.sql("SELECT SUM(x) FROM my_df").show()
  ```

- Anything you write into the database persists to the file. Close with `con.close()`.

---

## 4. Ingesting the JSON sources

DuckDB reads JSON natively with `read_json_auto`. The three JSON files each nest their
records differently, so you need to navigate to the right path.

### 4a. `companies.json` — records under `records`

```python
con.sql("""
    SELECT
        r.dcId,
        r.registration.legalName            AS legal_name,
        r.relationships.parentGroup.value   AS parent_group_id,
        r.financials.annualRevenue          AS annual_revenue   -- string e.g. "USD 5.95M"
    FROM (
        SELECT unnest(records) AS r
        FROM read_json_auto('data/companies.json')
    )
""").show()
```

`unnest(records)` turns the array into one row per company; then reach into the nested
structs with dot paths (`r.registration.legalName`, `r.relationships.parentGroup.value`,
`r.classification.vertical`, `r.footprint.countriesOfOperation`, …).

### 4b. `groups.json` — records under `result.groups`

The groups are **not** at the top level — they live under `result.groups`.

```python
con.sql("""
    SELECT
        g.groupId,
        g.profile.displayName          AS display_name,
        g.segmentation.commercialTier  AS commercial_tier,
        g.segmentation.vertical        AS vertical
    FROM (
        SELECT unnest(result.groups) AS g
        FROM read_json_auto('data/groups.json')
    )
""").show()
```

**Heterogeneous `attributes`.** Both `companies.json` and `groups.json` carry an
`attributes` array of `{name, value, ...}` pairs. In `groups.json` the `value` is
*sometimes a scalar and sometimes an object* (e.g. `country_of_incorporation` →
`{label, iso2}`). Unnest the array, then handle the two shapes — a robust route is to
read the values as JSON and branch on type:

```python
con.sql("""
    SELECT
        g.groupId,
        a.name                          AS attr_name,
        to_json(a.value)                AS attr_value_json   -- keep it as JSON, decide per row
    FROM (
        SELECT unnest(result.groups) AS g
        FROM read_json_auto('data/groups.json')
    ), unnest(g.attributes) AS t(a)
""").show()
```

### 4c. `exchange_rates.json` — and the size gotcha

This file is ~19 MB, **larger than DuckDB's default JSON `maximum_object_size` (16 MB)**.
A plain read fails with *"maximum_object_size … exceeded"*. Raise the limit:

```python
FX = "read_json_auto('data/exchange_rates.json', maximum_object_size=67108864)"

# meta: format guidance lives here — read it
con.sql(f"SELECT meta.base, meta.direction, meta.tuple.fieldOrder, meta.coverage FROM {FX}").show()

# one currency's points (each point is a positional tuple, see meta.tuple.fieldOrder)
con.sql(f"""
    SELECT p[1] AS rate_id, p[2] AS valid_from, p[3] AS valid_till, p[4] AS rate_str
    FROM (SELECT unnest(rates.series.EUR.points) AS p FROM {FX})
    LIMIT 5
""").show()
```

> Note: inside a point tuple the `rateStr` element is JSON-encoded, so it comes back as
> a quoted string like `"0.8639262551"`. Strip the quotes before `CAST`/`float`.

---

## 5. Ingesting the Excel workbook

The workbook has four sheets: **`Deposit`**, **`Withdrawals`**, **`Counterparty`**,
**`Fees`**.

### 5a. Preferred — DuckDB's `excel` extension (verified working here)

```python
con.execute("INSTALL excel; LOAD excel;")
for sheet in ("Deposit", "Withdrawals", "Counterparty", "Fees"):
    con.sql(f"""
        SELECT * FROM read_xlsx(
            'data/transactional_data_jul25_dec25.xlsx', sheet = '{sheet}'
        ) LIMIT 3
    """).show()
```

### 5b. Fallback — pandas, then register into DuckDB

If `read_xlsx` is unavailable in your environment, use pandas (`openpyxl` is already a
dependency) and register the DataFrames:

```python
import pandas as pd

deposit = pd.read_excel("data/transactional_data_jul25_dec25.xlsx", sheet_name="Deposit")
withdrawals = pd.read_excel("data/transactional_data_jul25_dec25.xlsx", sheet_name="Withdrawals")
counterparty = pd.read_excel("data/transactional_data_jul25_dec25.xlsx", sheet_name="Counterparty")
fees = pd.read_excel("data/transactional_data_jul25_dec25.xlsx", sheet_name="Fees")

con.register("deposit_raw", deposit)            # now queryable as a table name
con.sql("SELECT COUNT(*) FROM deposit_raw").show()
```

> The `Deposit` and `Withdrawals` sheets do **not** share an identical column order.
> Don't rely on positional column order when you combine them — align on column names.

---

## 6. Creating the Medallion schemas and writing tables

The exact schema layout is specified in the data team's working notes
(`docs/build_protocol.md`) — follow it. The snippet below just shows the DuckDB
mechanics; use the schema and table names your layout calls for.

```python
# create whatever schemas your layout defines (see docs/build_protocol.md)
for schema in ("bronze", "silver", "gold"):
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

# Land a Bronze table straight from a source (CREATE TABLE AS SELECT)
con.execute("""
    CREATE OR REPLACE TABLE bronze.deposit AS
    SELECT * FROM read_xlsx('data/transactional_data_jul25_dec25.xlsx', sheet = 'Deposit')
""")

# Or persist a pandas DataFrame you've shaped in Python
con.execute("CREATE OR REPLACE TABLE bronze.fees AS SELECT * FROM fees")  # 'fees' is a DataFrame
con.sql("SELECT COUNT(*) FROM bronze.deposit").show()
```

Keep any reusable Python helpers you write under `src/` and import them into your
notebook(s).

---

## 7. The FX as-of join (pointer, not a solution)

GBP normalisation is the core challenge and we are not going to solve it for you. The
`exchange_rates.json` file's own `meta` block describes the format you'll be reading;
the FX usage guidance in `BRIEF.md` sets out what's expected. How you perform the
point-in-time conversion — and how you handle anything outside the rate coverage — is
yours to design.

---

## 8. Where things live & how to submit

```
data/                      the four raw sources (do not edit)
docs/star_map_snapshot.png reference picture of the target network view
notebooks/                 your working notebook(s)
src/                       your Python modules (optional)
warehouse.duckdb           created when you connect — your Bronze/Silver/Gold layers
submission/                put your deliverables here
```

Hand back, in `submission/`: the populated **`warehouse.duckdb`**, your
**notebook(s)/SQL**, **`WRITEUP.md`**, and your **slides**. Commit them (the
`.gitignore` ignores `*.duckdb` by default — see the comment in `.gitignore` if you want
to commit your warehouse) or zip the lot.

Read **`BRIEF.md`** for the task itself.
