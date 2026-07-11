"""Gold curated: network nodes — participation, circle/diamond typing, data_mart-only."""
from __future__ import annotations

from pathlib import Path

import pytest

from src import bronze, config, gold, gold_curated, silver_core, silver_shape, warehouse

SKIP = pytest.mark.skipif(
    not (Path(config.TRANSACTIONS_XLSX).exists() and Path(config.EXCHANGE_RATES_JSON).exists()
         and Path(config.COMPANIES_JSON).exists() and Path(config.GROUPS_JSON).exists()),
    reason="source data not present",
)


@pytest.fixture()
def con(tmp_path):
    c = warehouse.connect(tmp_path / "wh.duckdb")
    warehouse.create_schemas(c)
    bronze.build_raw_monthly(c)
    bronze.build_live(c)
    silver_core.build_companies(c)
    silver_core.build_groups(c)
    silver_core.build_exchange_rates(c)
    silver_core.build_fx_match(c)
    silver_shape.build_entity_shape(c)
    silver_shape.build_gbp_facts(c)
    gold.build_entity(c)
    gold.build_money_flow(c)
    yield c
    c.close()


@SKIP
def test_every_edge_node_present_exactly_once(con):
    report = gold_curated.build_node(con)
    # Distinct endpoints of money_flow (either end) == node rows, each present once.
    endpoints = con.execute(
        "SELECT COUNT(*) FROM (SELECT focal_group_id AS n FROM data_mart.money_flow "
        "UNION SELECT counterpart_id FROM data_mart.money_flow)"
    ).fetchone()[0]
    n_nodes = con.execute("SELECT COUNT(*) FROM curated.node").fetchone()[0]
    assert n_nodes == endpoints == report.rows_out
    dup = con.execute(
        "SELECT COUNT(*) FROM (SELECT node_id FROM curated.node GROUP BY 1 HAVING COUNT(*) > 1)"
    ).fetchone()[0]
    assert dup == 0
    # Every endpoint resolves to a node (no loss).
    unresolved = con.execute(
        "SELECT COUNT(*) FROM (SELECT focal_group_id AS n FROM data_mart.money_flow "
        "UNION SELECT counterpart_id FROM data_mart.money_flow) en "
        "WHERE en.n NOT IN (SELECT node_id FROM curated.node)"
    ).fetchone()[0]
    assert unresolved == 0


@SKIP
def test_group_counterparts_are_circles(con):
    gold_curated.build_node(con)
    # Group-resolved endpoints (focal groups + group counterparts) are circle nodes.
    shapes = dict(
        con.execute("SELECT node_shape, COUNT(*) FROM curated.node GROUP BY 1").fetchall()
    )
    assert shapes.get("circle") == 13     # our 13 corporate groups
    # Every circle is a real group with referential integrity to the group dimension.
    non_group_circle = con.execute(
        "SELECT COUNT(*) FROM curated.node "
        "WHERE node_shape='circle' AND NOT (node_kind='group' AND NOT is_standalone)"
    ).fetchone()[0]
    assert non_group_circle == 0


@SKIP
def test_unaffiliated_counterparts_are_diamonds(con):
    gold_curated.build_node(con)
    shapes = dict(
        con.execute("SELECT node_shape, COUNT(*) FROM curated.node GROUP BY 1").fetchall()
    )
    assert shapes.get("diamond") == 1363     # standalone counterparties that transacted
    # Every diamond is a standalone counterparty; no other shapes exist.
    bad = con.execute(
        "SELECT COUNT(*) FROM curated.node "
        "WHERE node_shape='diamond' AND NOT (node_kind='counterparty' AND is_standalone)"
    ).fetchone()[0]
    assert bad == 0
    only_two_shapes = con.execute(
        "SELECT COUNT(*) FROM curated.node WHERE node_shape NOT IN ('circle', 'diamond')"
    ).fetchone()[0]
    assert only_two_shapes == 0


@SKIP
def test_reads_only_from_data_mart(con):
    # curated.node must be derivable with data_mart alone: dropping every other schema's
    # tables leaves the build working and its output unchanged.
    gold_curated.build_node(con)
    before = con.execute("SELECT COUNT(*) FROM curated.node").fetchone()[0]
    for schema in ("raw", "live", "core", "shape"):
        for (tbl,) in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = ?", [schema]
        ).fetchall():
            con.execute(f"DROP TABLE {schema}.{tbl}")
    gold_curated.build_node(con)     # rebuild against data_mart only — must still succeed
    after = con.execute("SELECT COUNT(*) FROM curated.node").fetchone()[0]
    assert after == before
