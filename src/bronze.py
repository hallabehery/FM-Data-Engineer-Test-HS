"""Bronze — landing the raw sources with minimal transformation.

`raw` holds the transactional sheets **split one table per month** (July–December 2025),
landed **as-is** — source column names and values/types preserved — so `raw` is a faithful,
auditable mirror of the workbook. `live` then **consolidates** the months into single tables
and lands the counterparty and fee sheets; the column **names** are conformed to `snake_case`
at this consolidation step (via `naming`), so every layer downstream of `live` is `snake_case`
while `raw` stays true to source.

Month assignment is driven by the transaction's own date (source `"Tx Date"`), not the
provided `Tx Month` convenience column. The build fails loud if any row falls outside the
six-month period.
"""
from __future__ import annotations

import duckdb

from . import config, naming
from .reporting import StageReport, report_stage

# Sheet name in the workbook -> singular stream name used for the raw/live tables.
SHEETS: dict[str, str] = {"Deposit": "deposit", "Withdrawals": "withdrawal"}

# Expected raw column sets (validated before landing). Derived from the canonical
# naming maps; module-level so the shape-drift behaviour is easy to exercise.
EXPECTED_COLUMNS: frozenset[str] = frozenset(naming.TRANSACTION_COLUMNS)
EXPECTED_COUNTERPARTY_COLUMNS: frozenset[str] = frozenset(naming.COUNTERPARTY_COLUMNS)
EXPECTED_FEES_COLUMNS: frozenset[str] = frozenset(naming.FEE_COLUMNS)


def _read_relation(xlsx, sheet: str, all_varchar: bool = False) -> str:
    options = ", all_varchar=true" if all_varchar else ""
    return f"read_xlsx('{xlsx}', sheet='{sheet}'{options})"


def _validate_columns(
    con: duckdb.DuckDBPyConnection, relation: str, expected: frozenset[str], sheet: str
) -> None:
    """Fail loud if the sheet is missing any expected column (schema drift)."""
    cols = {row[0] for row in con.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()}
    missing = expected - cols
    if missing:
        raise ValueError(f"{sheet!r} sheet missing expected columns: {sorted(missing)}")


def build_raw_monthly(con: duckdb.DuckDBPyConnection) -> list[StageReport]:
    """Land Deposit/Withdrawals as one `raw` table per month, source-faithful. Idempotent.

    Columns are landed **as-is** (`SELECT *` — source names and inferred types preserved), so
    `raw` mirrors the workbook and nothing is silently projected away. Names are conformed later,
    in `build_live`. Raises if any source row fails to land in exactly one month table.
    """
    con.execute("INSTALL excel; LOAD excel;")
    xlsx = config.TRANSACTIONS_XLSX
    reports: list[StageReport] = []

    for sheet, stream in SHEETS.items():
        relation = _read_relation(xlsx, sheet)
        _validate_columns(con, relation, EXPECTED_COLUMNS, sheet)

        # Land the sheet as-is (raw = faithful mirror), then split from the temp table on the
        # transaction's own date (source column "Tx Date").
        con.execute(f"CREATE OR REPLACE TEMP TABLE _src AS SELECT * FROM {relation}")
        source_rows = con.execute("SELECT COUNT(*) FROM _src").fetchone()[0]

        landed = 0
        for year_month in config.PERIOD_MONTHS:  # e.g. "2025-07"
            table = f"raw.{stream}_{year_month.replace('-', '_')}"
            con.execute(
                f"""
                CREATE OR REPLACE TABLE {table} AS
                SELECT *
                FROM _src
                WHERE CAST(date_trunc('month', "Tx Date") AS DATE) = DATE '{year_month}-01'
                """
            )
            landed += con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

        con.execute("DROP TABLE _src")
        if landed != source_rows:
            raise ValueError(
                f"{sheet!r}: {source_rows} source rows but {landed} landed across "
                f"months — some rows fall outside the period"
            )
        reports.append(
            report_stage(f"bronze.raw.{stream}", rows_in=source_rows, rows_out=landed, kept=landed)
        )

    return reports


def _land_sheet(
    con: duckdb.DuckDBPyConnection,
    sheet: str,
    table: str,
    rename: dict[str, str],
    expected: frozenset[str],
    all_varchar: bool = False,
) -> StageReport:
    """Land a whole sheet into `table`, aliasing columns to `snake_case`.

    `all_varchar=True` only where type inference breaks (Counterparty `CP website`);
    other sheets keep inference so dates/amounts land typed, not as Excel serials.
    """
    relation = _read_relation(config.TRANSACTIONS_XLSX, sheet, all_varchar=all_varchar)
    _validate_columns(con, relation, expected, sheet)
    con.execute(
        f"CREATE OR REPLACE TABLE {table} AS SELECT {naming.aliased_select(rename)} FROM {relation}"
    )
    rows = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    return report_stage(f"bronze.{table}", rows_in=rows, rows_out=rows, kept=rows)


def build_live(con: duckdb.DuckDBPyConnection) -> list[StageReport]:
    """Consolidate the per-month raw tables into single `live` tables, and land
    the counterparty and fee sheets. Idempotent.

    This is where names are conformed: each source-named `raw` month table is projected to
    `snake_case` (via `naming`) and the months are combined with `UNION ALL BY NAME`, so columns
    align on name, never position. Raises if a stream's consolidated count does not match the
    sum of its months.
    """
    con.execute("INSTALL excel; LOAD excel;")
    alias = naming.aliased_select(naming.TRANSACTION_COLUMNS)
    reports: list[StageReport] = []

    for stream in SHEETS.values():  # "deposit", "withdrawal"
        month_tables = [
            f"raw.{stream}_{ym.replace('-', '_')}" for ym in config.PERIOD_MONTHS
        ]
        union_sql = " UNION ALL BY NAME ".join(
            f"SELECT {alias} FROM {t}" for t in month_tables
        )
        con.execute(f"CREATE OR REPLACE TABLE live.{stream} AS {union_sql}")

        raw_total = sum(
            con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in month_tables
        )
        live_total = con.execute(f"SELECT COUNT(*) FROM live.{stream}").fetchone()[0]
        if live_total != raw_total:
            raise ValueError(
                f"live.{stream}: consolidated {live_total} rows but month tables hold {raw_total}"
            )
        reports.append(
            report_stage(
                f"bronze.live.{stream}", rows_in=raw_total, rows_out=live_total, kept=live_total
            )
        )

    # Counterparty needs all-VARCHAR (CP website defeats inference); Fees keeps
    # inference so its date/amount land typed, not as Excel serials.
    reports.append(
        _land_sheet(
            con, "Counterparty", "live.counterparty",
            naming.COUNTERPARTY_COLUMNS, EXPECTED_COUNTERPARTY_COLUMNS, all_varchar=True,
        )
    )
    reports.append(
        _land_sheet(con, "Fees", "live.fee", naming.FEE_COLUMNS, EXPECTED_FEES_COLUMNS)
    )
    return reports
