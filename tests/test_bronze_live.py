"""Bronze live: monthly tables consolidated by name, counterparty & fees landed, rows conserved."""
from __future__ import annotations

from pathlib import Path

import pytest

from src import bronze, config, naming, warehouse

SKIP = pytest.mark.skipif(
    not Path(config.TRANSACTIONS_XLSX).exists(), reason="workbook not present"
)


@pytest.fixture()
def con(tmp_path):
    c = warehouse.connect(tmp_path / "wh.duckdb")
    warehouse.create_schemas(c)
    bronze.build_raw_monthly(c)  # live consolidates the raw month tables
    yield c
    c.close()


@SKIP
def test_live_consolidates_all_months_conserved(con):
    reports = {r.stage: r for r in bronze.build_live(con)}
    for stream in ("deposit", "withdrawal"):
        raw_total = sum(
            con.execute(
                f"SELECT COUNT(*) FROM raw.{stream}_{ym.replace('-', '_')}"
            ).fetchone()[0]
            for ym in config.PERIOD_MONTHS
        )
        live_total = con.execute(f"SELECT COUNT(*) FROM live.{stream}").fetchone()[0]
        assert live_total == raw_total
        assert reports[f"bronze.live.{stream}"].conserved


@SKIP
def test_columns_aligned_by_name(con):
    bronze.build_live(con)
    # Consolidated table carries exactly the month table's columns (BY NAME union).
    live_cols = {r[0] for r in con.execute("DESCRIBE live.deposit").fetchall()}
    month_cols = {r[0] for r in con.execute("DESCRIBE raw.deposit_2025_07").fetchall()}
    assert live_cols == month_cols


@SKIP
def test_counterparty_and_fees_landed(con):
    bronze.build_live(con)
    cp = con.execute("SELECT COUNT(*) FROM live.counterparty").fetchone()[0]
    fees = con.execute("SELECT COUNT(*) FROM live.fee").fetchone()[0]
    assert cp == 1585
    assert fees == 21921
    # Columns are landed in snake_case (per the naming cleanup).
    cp_cols = {r[0] for r in con.execute("DESCRIBE live.counterparty").fetchall()}
    assert set(naming.COUNTERPARTY_COLUMNS.values()) <= cp_cols


@SKIP
def test_missing_counterparty_column_fails_loud(con, monkeypatch):
    monkeypatch.setattr(
        bronze,
        "EXPECTED_COUNTERPARTY_COLUMNS",
        bronze.EXPECTED_COUNTERPARTY_COLUMNS | {"Nonexistent"},
    )
    with pytest.raises(ValueError, match="missing expected columns"):
        bronze.build_live(con)
