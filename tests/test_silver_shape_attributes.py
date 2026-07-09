"""Silver shape: heterogeneous entity attributes resolved into queryable columns."""
from __future__ import annotations

from pathlib import Path

import pytest

from src import config, silver_core, silver_shape, warehouse

SKIP = pytest.mark.skipif(
    not (Path(config.COMPANIES_JSON).exists() and Path(config.GROUPS_JSON).exists()),
    reason="source JSON not present",
)


@pytest.fixture()
def con(tmp_path):
    c = warehouse.connect(tmp_path / "wh.duckdb")
    warehouse.create_schemas(c)
    silver_core.build_companies(c)
    silver_core.build_groups(c)
    yield c
    c.close()


def _cols(con, table):
    return {r[0] for r in con.execute(f"DESCRIBE {table}").fetchall()}


@SKIP
def test_company_attributes_resolved_and_conserved(con):
    reports = {r.stage: r for r in silver_shape.build_entity_shape(con)}
    cols = _cols(con, "shape.company")
    assert {"attr_risk_level", "attr_entity_role"} <= cols
    assert "attributes_json" not in cols
    n_core = con.execute("SELECT COUNT(*) FROM core.company").fetchone()[0]
    n_shape = con.execute("SELECT COUNT(*) FROM shape.company").fetchone()[0]
    assert n_shape == n_core
    assert reports["silver.shape.company"].conserved


@SKIP
def test_group_scalar_and_object_attributes_resolved(con):
    silver_shape.build_entity_shape(con)
    cols = _cols(con, "shape.corporate_group")
    # scalar attribute → one column; object attribute → one column per key.
    assert "attr_risk_level" in cols
    assert {"attr_country_of_incorporation_label", "attr_country_of_incorporation_iso2"} <= cols
    row = con.execute(
        "SELECT attr_risk_level, attr_country_of_incorporation_iso2 FROM shape.corporate_group "
        "WHERE attr_country_of_incorporation_iso2 IS NOT NULL LIMIT 1"
    ).fetchone()
    assert isinstance(row[0], str)              # scalar resolved to a string
    assert row[1] is not None and len(row[1]) == 2  # iso2 extracted from the object


@SKIP
def test_row_counts_unchanged(con):
    silver_shape.build_entity_shape(con)
    for name in ("company", "corporate_group"):
        assert (
            con.execute(f"SELECT COUNT(*) FROM shape.{name}").fetchone()[0]
            == con.execute(f"SELECT COUNT(*) FROM core.{name}").fetchone()[0]
        )


def test_flatten_branches_on_type():
    # Unit test of the branch-on-type resolver — no warehouse needed.
    scalar = silver_shape._flatten_attributes('[{"name":"x","value":"s1"}]')
    obj = silver_shape._flatten_attributes('[{"name":"x","value":{"label":"L","iso2":"XX"}}]')
    assert scalar == {"attr_x": "s1"}
    assert obj == {"attr_x_label": "L", "attr_x_iso2": "XX"}
    # Same attribute name appearing as scalar vs object → each populates its own columns.
    assert set(scalar) & set(obj) == set()


@SKIP
def test_same_name_scalar_or_object_both_resolve(con):
    # A synthetic core table where attribute `x` is scalar in one row, object in another.
    con.execute(
        """
        CREATE OR REPLACE TABLE core.synth AS
        SELECT * FROM (VALUES
            ('E1', '[{"name":"x","value":"scalar1"}]'),
            ('E2', '[{"name":"x","value":{"label":"L","iso2":"XX"}}]')
        ) AS t(id, attributes_json)
        """
    )
    silver_shape._resolve_attributes(con, "core.synth", "shape.synth", "test.synth")
    got = {
        r[0]: (r[1], r[2], r[3])
        for r in con.execute(
            "SELECT id, attr_x, attr_x_label, attr_x_iso2 FROM shape.synth ORDER BY id"
        ).fetchall()
    }
    assert got["E1"] == ("scalar1", None, None)
    assert got["E2"] == (None, "L", "XX")
