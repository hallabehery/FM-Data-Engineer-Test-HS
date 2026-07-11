"""Gold curated: directed edges — measures, month/year slicing, group↔company drill."""
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
    gold.build_edge_fact(c)
    gold_curated.build_node(c)
    yield c
    c.close()


@SKIP
def test_edges_carry_measures_and_reconcile(con):
    report = gold_curated.build_edge(con)
    # Every edge carries the three additive measures; totals reconcile to the fact.
    e_vol, e_cnt, e_fee = con.execute(
        "SELECT SUM(gbp_volume), SUM(txn_count), SUM(gbp_fee_revenue) FROM curated.edge"
    ).fetchone()
    f_vol, f_cnt, f_fee = con.execute(
        "SELECT SUM(gbp_volume), SUM(txn_count), SUM(gbp_fee_revenue) FROM data_mart.edge_fact"
    ).fetchone()
    assert e_cnt == f_cnt
    assert abs(e_vol - f_vol) < 1e-3
    assert abs(e_fee - f_fee) < 1e-3
    # 1:1 projection of the fact — no loss or fan-out.
    assert report.rows_out == con.execute("SELECT COUNT(*) FROM data_mart.edge_fact").fetchone()[0]
    # No null measures.
    assert con.execute(
        "SELECT COUNT(*) FROM curated.edge "
        "WHERE gbp_volume IS NULL OR txn_count IS NULL OR gbp_fee_revenue IS NULL"
    ).fetchone()[0] == 0


@SKIP
def test_directed_endpoints_reference_nodes(con):
    gold_curated.build_edge(con)
    # inflow = money into the focal group (counterpart -> focal); outflow = focal -> counterpart.
    bad = con.execute(
        """
        SELECT COUNT(*) FROM curated.edge WHERE NOT (
            (direction = 'inflow'  AND source_node_id = counterpart_id AND target_node_id = focal_group_id) OR
            (direction = 'outflow' AND source_node_id = focal_group_id AND target_node_id = counterpart_id)
        )
        """
    ).fetchone()[0]
    assert bad == 0
    # Every endpoint resolves to a node in curated.node (referential integrity).
    dangling = con.execute(
        "SELECT COUNT(*) FROM curated.edge e WHERE "
        "e.source_node_id NOT IN (SELECT node_id FROM curated.node) OR "
        "e.target_node_id NOT IN (SELECT node_id FROM curated.node)"
    ).fetchone()[0]
    assert dangling == 0


@SKIP
def test_sliceable_by_month_and_year(con):
    gold_curated.build_edge(con)
    # year is the calendar year of month; year totals roll up from month by summation.
    bad_year = con.execute(
        "SELECT COUNT(*) FROM curated.edge WHERE year <> year(month)"
    ).fetchone()[0]
    assert bad_year == 0
    # Six monthly slices (Jul–Dec 2025), all within one year → summing months == summing year.
    n_months = con.execute("SELECT COUNT(DISTINCT month) FROM curated.edge").fetchone()[0]
    assert n_months == 6
    by_month = con.execute("SELECT SUM(gbp_volume) FROM curated.edge").fetchone()[0]
    by_year = con.execute(
        "SELECT SUM(v) FROM (SELECT year, SUM(gbp_volume) v FROM curated.edge GROUP BY year)"
    ).fetchone()[0]
    assert abs(by_month - by_year) < 1e-3


@SKIP
def test_group_to_group_flows_visible(con):
    gold_curated.build_edge(con)
    # Flows where the counterpart is itself one of our groups are present and both ends are circles.
    g2g = con.execute(
        "SELECT COUNT(*) FROM curated.edge WHERE counterpart_is_group"
    ).fetchone()[0]
    assert g2g > 0
    non_circle = con.execute(
        "SELECT COUNT(*) FROM curated.edge e WHERE e.counterpart_is_group AND e.counterpart_id NOT IN "
        "(SELECT node_id FROM curated.node WHERE node_shape = 'circle')"
    ).fetchone()[0]
    assert non_circle == 0


@SKIP
def test_drillable_company_rolls_up_to_group(con):
    gold_curated.build_edge(con)
    # Each focal_company_id belongs to exactly one focal_group_id (clean roll-up key).
    inconsistent = con.execute(
        "SELECT COUNT(*) FROM (SELECT focal_company_id FROM curated.edge "
        "GROUP BY focal_company_id HAVING COUNT(DISTINCT focal_group_id) > 1)"
    ).fetchone()[0]
    assert inconsistent == 0
    # The company-grain rows exist and are finer than the group grain (drill adds detail).
    n_company_grain = con.execute(
        "SELECT COUNT(*) FROM (SELECT focal_company_id, counterpart_id, direction, month "
        "FROM curated.edge GROUP BY 1,2,3,4)"
    ).fetchone()[0]
    n_group_grain = con.execute(
        "SELECT COUNT(*) FROM (SELECT focal_group_id, counterpart_id, direction, month "
        "FROM curated.edge GROUP BY 1,2,3,4)"
    ).fetchone()[0]
    assert n_company_grain >= n_group_grain > 0
    # Rolling company grain up to group grain conserves the measure total.
    total = con.execute("SELECT SUM(gbp_volume) FROM curated.edge").fetchone()[0]
    rolled = con.execute(
        "SELECT SUM(v) FROM (SELECT focal_group_id, counterpart_id, direction, month, "
        "SUM(gbp_volume) v FROM curated.edge GROUP BY 1,2,3,4)"
    ).fetchone()[0]
    assert abs(total - rolled) < 1e-3


@SKIP
def test_reads_only_from_data_mart(con):
    gold_curated.build_edge(con)
    before = con.execute("SELECT COUNT(*) FROM curated.edge").fetchone()[0]
    # Drop every non-data_mart schema's tables; the rebuild must still succeed unchanged.
    for schema in ("raw", "live", "core", "shape"):
        for (tbl,) in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = ?", [schema]
        ).fetchall():
            con.execute(f"DROP TABLE {schema}.{tbl}")
    gold_curated.build_edge(con)
    after = con.execute("SELECT COUNT(*) FROM curated.edge").fetchone()[0]
    assert after == before
