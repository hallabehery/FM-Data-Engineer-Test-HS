"""Gold data_mart: combined entity/node dimension with counterparty resolution + provenance."""
from __future__ import annotations

from pathlib import Path

import pytest

from src import bronze, config, gold, silver_core, silver_shape, warehouse

SKIP = pytest.mark.skipif(
    not (Path(config.TRANSACTIONS_XLSX).exists()
         and Path(config.COMPANIES_JSON).exists()
         and Path(config.GROUPS_JSON).exists()),
    reason="source data not present",
)


@pytest.fixture()
def con(tmp_path):
    c = warehouse.connect(tmp_path / "wh.duckdb")
    warehouse.create_schemas(c)
    bronze.build_raw_monthly(c)
    bronze.build_live(c)          # live.counterparty
    silver_core.build_companies(c)
    silver_core.build_groups(c)
    silver_shape.build_entity_shape(c)  # shape.company / shape.corporate_group
    yield c
    c.close()


@SKIP
def test_kinds_conserved_no_fan_out(con):
    report = gold.build_entity(con)
    counts = dict(
        con.execute("SELECT entity_kind, COUNT(*) FROM data_mart.entity GROUP BY 1").fetchall()
    )
    assert counts == {"group": 13, "company": 44, "counterparty": 1585}
    assert report.rows_out == 13 + 44 + 1585 == 1642
    assert report.conserved


@SKIP
def test_node_shapes(con):
    gold.build_entity(con)
    # groups are circles; standalone counterparties are diamonds; nothing else is a node.
    assert con.execute(
        "SELECT COUNT(*) FROM data_mart.entity WHERE entity_kind='group' AND node_shape='circle'"
    ).fetchone()[0] == 13
    shapes = dict(
        con.execute(
            "SELECT node_shape, COUNT(*) FROM data_mart.entity "
            "WHERE entity_kind='counterparty' GROUP BY 1"
        ).fetchall()
    )
    assert shapes.get("diamond") == 1363          # standalone counterparties
    assert shapes.get(None) == 222                # resolved counterparties (no own node)
    # companies are the drill level, not nodes
    assert con.execute(
        "SELECT COUNT(*) FROM data_mart.entity WHERE entity_kind='company' AND node_shape IS NOT NULL"
    ).fetchone()[0] == 0


@SKIP
def test_counterparty_resolution(con):
    gold.build_entity(con)
    resolved = con.execute(
        "SELECT COUNT(*) FROM data_mart.entity "
        "WHERE entity_kind='counterparty' AND NOT is_standalone AND group_id IS NOT NULL"
    ).fetchone()[0]
    standalone = con.execute(
        "SELECT COUNT(*) FROM data_mart.entity "
        "WHERE entity_kind='counterparty' AND is_standalone"
    ).fetchone()[0]
    assert (resolved, standalone) == (222, 1363)
    # Every resolved group_id is a real group (referential integrity, no orphans).
    orphans = con.execute(
        "SELECT COUNT(*) FROM data_mart.entity e "
        "WHERE e.entity_kind='counterparty' AND NOT e.is_standalone "
        "AND e.group_id NOT IN (SELECT group_id FROM shape.corporate_group)"
    ).fetchone()[0]
    assert orphans == 0


@SKIP
def test_source_provenance(con):
    gold.build_entity(con)
    src = {
        r[0]: r[1]
        for r in con.execute(
            """
            SELECT entity_kind || ':' ||
                   CASE WHEN entity_kind='counterparty' AND is_standalone THEN 'standalone'
                        WHEN entity_kind='counterparty' THEN 'resolved' ELSE 'x' END AS k,
                   ANY_VALUE(source) AS source
            FROM data_mart.entity GROUP BY 1
            """
        ).fetchall()
    }
    assert src["group:x"] == ["groups.json"]
    assert src["company:x"] == ["companies.json"]
    assert src["counterparty:standalone"] == ["counterparty"]
    assert src["counterparty:resolved"] == ["counterparty", "groups.json"]
