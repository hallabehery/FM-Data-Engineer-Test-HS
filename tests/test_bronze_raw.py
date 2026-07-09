"""Bronze raw: transactional sheets split one table per month, with row conservation."""
from __future__ import annotations

from pathlib import Path

import pytest

from src import bronze, config, warehouse

SKIP = pytest.mark.skipif(
    not Path(config.TRANSACTIONS_XLSX).exists(), reason="workbook not present"
)


@pytest.fixture()
def con(tmp_path):
    c = warehouse.connect(tmp_path / "wh.duckdb")
    warehouse.create_schemas(c)
    yield c
    c.close()


def _source_count(con, sheet: str) -> int:
    con.execute("INSTALL excel; LOAD excel;")
    return con.execute(
        f"SELECT COUNT(*) FROM read_xlsx('{config.TRANSACTIONS_XLSX}', sheet='{sheet}')"
    ).fetchone()[0]


@SKIP
def test_twelve_month_tables_created(con):
    bronze.build_raw_monthly(con)
    tables = {
        (r[0], r[1])
        for r in con.execute(
            "SELECT table_schema, table_name FROM information_schema.tables "
            "WHERE table_schema = 'raw'"
        ).fetchall()
    }
    for stream in ("deposit", "withdrawal"):
        for ym in config.PERIOD_MONTHS:
            assert ("raw", f"{stream}_{ym.replace('-', '_')}") in tables


@SKIP
def test_rows_conserved_per_stream(con):
    reports = bronze.build_raw_monthly(con)
    by_stream = {r.stage.split(".")[-1]: r for r in reports}
    for sheet, stream in bronze.SHEETS.items():
        landed = sum(
            con.execute(
                f"SELECT COUNT(*) FROM raw.{stream}_{ym.replace('-', '_')}"
            ).fetchone()[0]
            for ym in config.PERIOD_MONTHS
        )
        assert landed == _source_count(con, sheet)
        assert by_stream[stream].conserved
        assert by_stream[stream].rows_in == landed


@SKIP
def test_each_row_lands_in_exactly_one_month(con):
    # Routing correctness: a month table holds exactly the rows whose Tx Date is
    # in that month, so the six tables partition the source with no overlap.
    bronze.build_raw_monthly(con)
    for ym in config.PERIOD_MONTHS:
        table = f"raw.deposit_{ym.replace('-', '_')}"
        off_month = con.execute(
            f"SELECT COUNT(*) FROM {table} "
            f"WHERE CAST(date_trunc('month', tx_date) AS DATE) <> DATE '{ym}-01'"
        ).fetchone()[0]
        assert off_month == 0, f"{table} contains rows from another month"


@SKIP
def test_missing_column_fails_loud(con, monkeypatch):
    monkeypatch.setattr(
        bronze, "EXPECTED_COLUMNS", bronze.EXPECTED_COLUMNS | {"Nonexistent Column"}
    )
    with pytest.raises(ValueError, match="missing expected columns"):
        bronze.build_raw_monthly(con)
