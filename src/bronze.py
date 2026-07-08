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
