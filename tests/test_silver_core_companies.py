"""Silver `core` companies unpick: one row per dcId, nested fields flattened, bridge key exposed."""
from __future__ import annotations

from pathlib import Path

import pytest

from src import config, warehouse
from src.silver_core import build_companies


@pytest.fixture()
def con(tmp_path):
    c = warehouse.connect(tmp_path / "wh.duckdb")
    warehouse.create_schemas(c)
    yield c
    c.close()


@pytest.mark.skipif(
    not Path(config.COMPANIES_JSON).exists(), reason="companies.json not present"
)
def test_row_count_matches_source(con):
    report = build_companies(con)
    source_total = con.execute(
        f"SELECT metadata.total FROM read_json_auto('{config.COMPANIES_JSON}')"
    ).fetchone()[0]
    table_rows = con.execute("SELECT COUNT(*) FROM core.company").fetchone()[0]
    assert table_rows == source_total
    # Conservation: unpicking neither adds nor drops rows.
    assert report.rows_in == report.rows_out == table_rows
    assert report.conserved


@pytest.mark.skipif(
    not Path(config.COMPANIES_JSON).exists(), reason="companies.json not present"
)
def test_dc_id_is_a_clean_natural_key(con):
    build_companies(con)
    stats = con.execute(
        "SELECT COUNT(*), COUNT(dc_id), COUNT(DISTINCT dc_id) FROM core.company"
    ).fetchone()
    total, non_null, distinct = stats
    assert non_null == total, "dc_id must be non-null for every row"
    assert distinct == total, "dc_id must be unique (no duplicates)"


@pytest.mark.skipif(
    not Path(config.COMPANIES_JSON).exists(), reason="companies.json not present"
)
def test_nested_fields_and_bridge_key_present(con):
    build_companies(con)
    cols = {
        row[0] for row in con.execute("DESCRIBE core.company").fetchall()
    }
    expected = {
        "legal_name",
        "status",
        "incorporation_country",
        "vertical",
        "industry",
        "financials_currency",
        "annual_revenue",
        "operation_count",
        "parent_group_id",  # the group bridge key
        "parent_group_role",
    }
    assert expected <= cols, f"missing columns: {expected - cols}"
    # The bridge key resolves for most companies (some may be unaffiliated).
    linked = con.execute(
        "SELECT COUNT(parent_group_id) FROM core.company"
    ).fetchone()[0]
    assert linked > 0
