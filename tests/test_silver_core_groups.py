"""Silver `core` groups unpick: one row per groupId, fields flattened, heterogeneous attributes survive."""
from __future__ import annotations

from pathlib import Path

import pytest

from src import config, warehouse
from src.silver_core import build_groups


@pytest.fixture()
def con(tmp_path):
    c = warehouse.connect(tmp_path / "wh.duckdb")
    warehouse.create_schemas(c)
    yield c
    c.close()


@pytest.mark.skipif(
    not Path(config.GROUPS_JSON).exists(), reason="groups.json not present"
)
def test_row_count_matches_source(con):
    report = build_groups(con)
    source_total = con.execute(
        f"SELECT export.recordCount FROM read_json_auto('{config.GROUPS_JSON}')"
    ).fetchone()[0]
    table_rows = con.execute("SELECT COUNT(*) FROM core.corporate_group").fetchone()[0]
    assert table_rows == source_total
    assert report.rows_in == report.rows_out == table_rows
    assert report.conserved


@pytest.mark.skipif(
    not Path(config.GROUPS_JSON).exists(), reason="groups.json not present"
)
def test_group_id_is_a_clean_natural_key(con):
    build_groups(con)
    total, non_null, distinct = con.execute(
        "SELECT COUNT(*), COUNT(group_id), COUNT(DISTINCT group_id) FROM core.corporate_group"
    ).fetchone()
    assert non_null == total, "group_id must be non-null for every row"
    assert distinct == total, "group_id must be unique (no duplicates)"


@pytest.mark.skipif(
    not Path(config.GROUPS_JSON).exists(), reason="groups.json not present"
)
def test_profile_segmentation_lifecycle_columns_present(con):
    build_groups(con)
    cols = {row[0] for row in con.execute("DESCRIBE core.corporate_group").fetchall()}
    expected = {
        "display_name",
        "description",
        "status_code",
        "active",
        "created_on",
        "pod",
        "vertical",
        "industry",
        "commercial_tier",
        "attributes_json",
    }
    assert expected <= cols, f"missing columns: {expected - cols}"


@pytest.mark.skipif(
    not Path(config.GROUPS_JSON).exists(), reason="groups.json not present"
)
def test_heterogeneous_attributes_survive_as_json(con):
    build_groups(con)
    # The scalar-or-object attributes must be queryable back out per row without error:
    # country_of_incorporation is an object, risk_level a scalar string.
    shapes = con.execute(
        """
        SELECT a.name, json_type(a.value) AS jtype, COUNT(*) AS n
        FROM core.corporate_group, unnest(from_json(attributes_json,
             '[{"name":"VARCHAR","value":"JSON","valueType":"VARCHAR"}]')) AS t(a)
        GROUP BY 1, 2
        ORDER BY 1
        """
    ).fetchall()
    shape_map = {name: jtype for name, jtype, _ in shapes}
    assert shape_map.get("country_of_incorporation") == "OBJECT"
    assert shape_map.get("risk_level") == "VARCHAR"
