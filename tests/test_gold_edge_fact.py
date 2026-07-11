"""Gold data_mart: directed edge fact — grain, additive measures, direction, provenance."""
from __future__ import annotations

from pathlib import Path

import pytest

from src import bronze, config, gold, silver_core, silver_shape, warehouse

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
    yield c
    c.close()


@SKIP
def test_grain_is_unique(con):
    gold.build_edge_fact(con)
    total, distinct = con.execute(
        """
        SELECT COUNT(*),
               COUNT(*) FILTER (WHERE TRUE)
        FROM data_mart.edge_fact
        """
    ).fetchone()
    dup = con.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT focal_group_id, counterpart_id, direction, month, COUNT(*) c
            FROM data_mart.edge_fact
            GROUP BY 1,2,3,4 HAVING COUNT(*) > 1
        )
        """
    ).fetchone()[0]
    assert dup == 0  # one row per focal_group × counterpart × direction × month


@SKIP
def test_measures_reconcile_to_silver(con):
    report = gold.build_edge_fact(con)
    vol, cnt, fee = con.execute(
        "SELECT SUM(gbp_volume), SUM(txn_count), SUM(gbp_fee_revenue) FROM data_mart.edge_fact"
    ).fetchone()
    src_vol = con.execute(
        "SELECT (SELECT SUM(gbp_amount) FROM shape.deposit)+(SELECT SUM(gbp_amount) FROM shape.withdrawal)"
    ).fetchone()[0]
    assert cnt == 6383 + 6599                       # promoted transactions
    assert abs(vol - src_vol) < 1e-3
    assert abs(fee - con.execute("SELECT SUM(gbp_amount) FROM shape.fee").fetchone()[0]) < 1e-3
    # edge_fact is an aggregation; the report row count is the edge count.
    assert report.rows_out == con.execute("SELECT COUNT(*) FROM data_mart.edge_fact").fetchone()[0]


@SKIP
def test_direction_convention(con):
    gold.build_edge_fact(con)
    dirs = dict(
        con.execute("SELECT direction, SUM(txn_count) FROM data_mart.edge_fact GROUP BY 1").fetchall()
    )
    assert dirs["inflow"] == 6383      # deposits
    assert dirs["outflow"] == 6599     # withdrawals


@SKIP
def test_counterpart_resolution_and_group_to_group(con):
    gold.build_edge_fact(con)
    # Resolved counterparts carry a real group id; standalone carry their cp id.
    orphan = con.execute(
        "SELECT COUNT(*) FROM data_mart.edge_fact "
        "WHERE counterpart_is_group AND counterpart_id NOT IN (SELECT group_id FROM shape.corporate_group)"
    ).fetchone()[0]
    assert orphan == 0
    # Group-to-group edges (counterpart resolves to one of our groups) exist and are visible.
    assert con.execute(
        "SELECT COUNT(*) FROM data_mart.edge_fact WHERE counterpart_is_group"
    ).fetchone()[0] > 0


@SKIP
def test_source_provenance(con):
    gold.build_edge_fact(con)
    # deposits/withdrawals always present; 'fees' only when there is fee revenue.
    bad_dir = con.execute(
        "SELECT COUNT(*) FROM data_mart.edge_fact "
        "WHERE NOT list_contains(source, IF(direction='inflow','deposits','withdrawals'))"
    ).fetchone()[0]
    assert bad_dir == 0
    bad_fee = con.execute(
        "SELECT COUNT(*) FROM data_mart.edge_fact "
        "WHERE (gbp_fee_revenue > 0) <> list_contains(source, 'fees')"
    ).fetchone()[0]
    assert bad_fee == 0
    # standalone counterpart edges cite the counterparty source.
    bad_cp = con.execute(
        "SELECT COUNT(*) FROM data_mart.edge_fact "
        "WHERE NOT counterpart_is_group AND NOT list_contains(source, 'counterparty')"
    ).fetchone()[0]
    assert bad_cp == 0
