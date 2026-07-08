"""Bronze — landing the raw sources with minimal transformation.

`raw` holds the transactional sheets **split one table per month** (July–December
2025), so a problem in a single month can be fixed in a small table and flow
through to the consolidated `live` tables. Rows are landed as-is (no renaming, no
retyping) — the only decision is which month table a row belongs to.

Month assignment is driven by the transaction's **own** date (`Tx Date`), not the
provided `Tx Month` convenience column, so the split reflects when the
transaction actually happened. The build fails loud if any row falls outside the
six-month period (which would mean a row silently lands in no table).
"""
from __future__ import annotations

import duckdb

from . import config
from .reporting import StageReport, report_stage

# Sheet name in the workbook -> stream name used for the raw tables.
SHEETS: dict[str, str] = {"Deposit": "deposits", "Withdrawals": "withdrawals"}

# Both sheets carry this column set (in different orders — align on name).
EXPECTED_COLUMNS: frozenset[str] = frozenset(
    {
        "Freemarket Entity",
        "Transaction Type",
        "Deal ID/DC ID",
        "Account ID",
        "Transaction ID",
        "Tx Date",
        "Tx Time",
        "Tx Currency",
        "Tx Value (CCY)",
        "Counterparty",
        "Tx Month",
        "Scheme",
    }
)

# Dimension / fee sheets landed directly into `live` (not monthly-split).
EXPECTED_COUNTERPARTY_COLUMNS: frozenset[str] = frozenset(
    {"CP ID", "CP name", "CP vertical", "CP website", "CP business desc", "Group ID", "DC Id"}
)
EXPECTED_FEES_COLUMNS: frozenset[str] = frozenset(
    {"FeeId", "Type", "Date", "FeeDetail", "Fee amount (CCY)", "Fee currency", "Link Id"}
)


def _read_relation(xlsx, sheet: str) -> str:
    return f"read_xlsx('{xlsx}', sheet='{sheet}')"


def _validate_columns(con: duckdb.DuckDBPyConnection, xlsx, sheet: str) -> None:
    """Fail loud if the sheet is missing any expected column (schema drift)."""
    cols = {
        row[0]
        for row in con.execute(
            f"DESCRIBE SELECT * FROM {_read_relation(xlsx, sheet)}"
        ).fetchall()
    }
    missing = EXPECTED_COLUMNS - cols
    if missing:
        raise ValueError(f"{sheet!r} sheet missing expected columns: {sorted(missing)}")


def build_raw_monthly(con: duckdb.DuckDBPyConnection) -> list[StageReport]:
    """Land Deposit/Withdrawals as one `raw` table per month. Idempotent.

    Returns one `StageReport` per stream; raises if any source row fails to land
    in exactly one month table.
    """
    con.execute("INSTALL excel; LOAD excel;")
    xlsx = config.TRANSACTIONS_XLSX
    reports: list[StageReport] = []

    for sheet, stream in SHEETS.items():
        _validate_columns(con, xlsx, sheet)

        # Read the sheet once, then split from the temp table (avoids re-reading
        # the workbook per month).
        con.execute(
            f"CREATE OR REPLACE TEMP TABLE _src AS SELECT * FROM {_read_relation(xlsx, sheet)}"
        )
        source_rows = con.execute("SELECT COUNT(*) FROM _src").fetchone()[0]

        landed = 0
        for year_month in config.PERIOD_MONTHS:  # e.g. "2025-07"
            suffix = year_month.replace("-", "_")
            table = f"raw.{stream}_{suffix}"
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

        # Conservation: every source row must land in exactly one month table.
        if landed != source_rows:
            raise ValueError(
                f"{sheet!r}: {source_rows} source rows but {landed} landed across "
                f"months — some rows fall outside {config.PERIOD_MONTHS[0]}.."
                f"{config.PERIOD_MONTHS[-1]}"
            )
        reports.append(
            report_stage(
                f"bronze.raw.{stream}", rows_in=source_rows, rows_out=landed, kept=landed
            )
        )

    return reports


def _land_sheet(
    con: duckdb.DuckDBPyConnection,
    sheet: str,
    table: str,
    expected: frozenset[str],
) -> StageReport:
    """Land a whole sheet as-is into `table` (all-VARCHAR raw fidelity)."""
    xlsx = config.TRANSACTIONS_XLSX
    # Read as all-VARCHAR: e.g. the Counterparty `CP website` column is mostly
    # empty and defeats type inference (a URL can't cast to the inferred DOUBLE).
    # Bronze lands raw; downstream layers cast as needed.
    relation = f"read_xlsx('{xlsx}', sheet='{sheet}', all_varchar=true)"
    cols = {row[0] for row in con.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()}
    missing = expected - cols
    if missing:
        raise ValueError(f"{sheet!r} sheet missing expected columns: {sorted(missing)}")

    con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM {relation}")
    rows = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    return report_stage(f"bronze.{table}", rows_in=rows, rows_out=rows, kept=rows)


def build_live(con: duckdb.DuckDBPyConnection) -> list[StageReport]:
    """Consolidate the per-month raw tables into single `live` tables, and land
    the counterparty and fees sheets. Idempotent.

    Consolidation uses `UNION ALL BY NAME` so columns align on name, never
    position (the source sheets differ in column order). Raises if a stream's
    consolidated row count does not match the sum of its month tables.
    """
    con.execute("INSTALL excel; LOAD excel;")
    reports: list[StageReport] = []

    for stream in SHEETS.values():  # "deposits", "withdrawals"
        month_tables = [
            f"raw.{stream}_{ym.replace('-', '_')}" for ym in config.PERIOD_MONTHS
        ]
        union_sql = " UNION ALL BY NAME ".join(
            f"SELECT * FROM {t}" for t in month_tables
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

    # Dimension + fee sheets land directly into `live` for downstream use.
    reports.append(
        _land_sheet(con, "Counterparty", "live.counterparty", EXPECTED_COUNTERPARTY_COLUMNS)
    )
    reports.append(_land_sheet(con, "Fees", "live.fees", EXPECTED_FEES_COLUMNS))
    return reports
