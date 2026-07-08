"""Foundation tests: the warehouse opens, reopens cleanly, and carries all six schemas."""
from __future__ import annotations

from src import config, warehouse
from src.pipeline import build_foundation
from src.reporting import report_stage


def test_all_six_schemas_named():
    assert config.SCHEMAS == ("raw", "live", "core", "shape", "data_mart", "curated")


def test_schemas_are_created(tmp_path):
    con = warehouse.connect(tmp_path / "wh.duckdb")
    try:
        warehouse.create_schemas(con)
        assert set(config.SCHEMAS) <= warehouse.existing_schemas(con)
    finally:
        con.close()


def test_reopen_is_clean_and_idempotent(tmp_path):
    db = tmp_path / "wh.duckdb"
    con = warehouse.connect(db)
    warehouse.create_schemas(con)
    con.close()

    # Reopen and re-create: must not error and schemas must still be present.
    con = warehouse.connect(db)
    warehouse.create_schemas(con)  # idempotent
    try:
        assert set(config.SCHEMAS) <= warehouse.existing_schemas(con)
    finally:
        con.close()


def test_build_foundation_returns_all_schemas(tmp_path):
    con = warehouse.connect(tmp_path / "wh.duckdb")
    try:
        assert set(build_foundation(con)) == set(config.SCHEMAS)
    finally:
        con.close()


def test_stage_report_conservation():
    good = report_stage("demo", rows_in=100, rows_out=95, kept=95, quarantined=5)
    assert good.conserved
    bad = report_stage("demo", rows_in=100, rows_out=90)  # 10 unaccounted for
    assert not bad.conserved
