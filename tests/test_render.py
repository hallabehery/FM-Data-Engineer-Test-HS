"""Illustrative render — the (nodes, edges) frame is driven solely by Gold `curated`.

The render itself is optional proof, so the *tested* seam is `star_map_frame`: the pure query
that turns `curated` into graph-ready rows. We assert it takes node shapes and directed edges
verbatim from `curated`, slices by period, caps counterparts for legibility, and — crucially —
depends on nothing but the `curated` schema (proving "no extra shaping").
"""
from __future__ import annotations

from pathlib import Path

import pytest

from src import (
    bronze, config, gold, gold_curated, render, silver_core, silver_shape, warehouse,
)

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
    gold_curated.build_node(c)
    gold_curated.build_edge(c)
    yield c
    c.close()


@SKIP
def test_default_focal_group_is_the_top_by_volume(con):
    fg = render.default_focal_group(con)
    top = con.execute(
        "SELECT focal_group_id FROM curated.edge GROUP BY 1 ORDER BY SUM(gbp_volume) DESC LIMIT 1"
    ).fetchone()[0]
    assert fg == top


@SKIP
def test_nodes_carry_shape_verbatim_from_curated(con):
    fg = render.default_focal_group(con)
    nodes, _ = render.star_map_frame(con, fg, top_n=None)
    assert nodes, "expected participating nodes"
    # Every node shape is one curated assigns, and matches curated.node exactly.
    assert all(n["node_shape"] in ("circle", "diamond") for n in nodes)
    for n in nodes:
        curated_shape = con.execute(
            "SELECT node_shape FROM curated.node WHERE node_id = ?", [n["node_id"]]
        ).fetchone()[0]
        assert n["node_shape"] == curated_shape
    # The focal group itself is present, is a circle, and is flagged focal.
    focal = [n for n in nodes if n["is_focal"]]
    assert len(focal) == 1
    assert focal[0]["node_id"] == fg and focal[0]["node_shape"] == "circle"


@SKIP
def test_edges_are_directed_and_labelled_with_the_three_measures(con):
    fg = render.default_focal_group(con)
    nodes, edges = render.star_map_frame(con, fg, top_n=None)
    assert edges, "expected directed edges"
    node_ids = {n["node_id"] for n in nodes}
    for e in edges:
        assert e["direction"] in ("inflow", "outflow")
        # Directed endpoints both resolve to nodes in the frame (referential integrity).
        assert e["source_node_id"] in node_ids and e["target_node_id"] in node_ids
        # Exactly one endpoint is the focal group (a star map centred on it).
        assert fg in (e["source_node_id"], e["target_node_id"])
        # All three measures present and non-negative; the label shows all three.
        assert e["gbp_volume"] >= 0 and e["txn_count"] >= 0 and e["gbp_fee_revenue"] >= 0
        label = render._edge_label(e)
        assert "£" in label and "txns" in label and "fee" in label


@SKIP
def test_frame_reconciles_to_curated_edge_for_the_focal_group(con):
    fg = render.default_focal_group(con)
    _, edges = render.star_map_frame(con, fg, top_n=None)
    frame_vol = sum(e["gbp_volume"] for e in edges)
    frame_cnt = sum(e["txn_count"] for e in edges)
    cur_vol, cur_cnt = con.execute(
        "SELECT SUM(gbp_volume), SUM(txn_count) FROM curated.edge WHERE focal_group_id = ?", [fg]
    ).fetchone()
    # Uncapped, the star-map roll-up conserves the focal group's totals exactly.
    assert abs(frame_vol - cur_vol) < 1e-3
    assert frame_cnt == cur_cnt


@SKIP
def test_top_n_caps_counterparts_for_legibility(con):
    fg = render.default_focal_group(con)
    _, edges = render.star_map_frame(con, fg, top_n=3)
    assert len({e["counterpart_id"] for e in edges}) <= 3


@SKIP
def test_year_and_month_slices_are_subsets_that_sum_to_the_whole(con):
    fg = render.default_focal_group(con)
    _, all_edges = render.star_map_frame(con, fg, top_n=None)
    all_vol = sum(e["gbp_volume"] for e in all_edges)
    # Summing every year slice present in the data reconciles to the whole (derive years,
    # never hardcode — robust even if the window ever spans a calendar-year boundary).
    years = [r[0] for r in con.execute(
        "SELECT DISTINCT year FROM curated.edge WHERE focal_group_id = ? ORDER BY year", [fg]
    ).fetchall()]
    year_total = sum(
        sum(e["gbp_volume"] for e in render.star_map_frame(con, fg, year=y, top_n=None)[1])
        for y in years
    )
    assert abs(year_total - all_vol) < 1e-3
    # Summing the six month slices reconciles to the whole (period slicing is additive).
    month_total = 0.0
    for m in config.PERIOD_MONTHS:
        _, month_edges = render.star_map_frame(con, fg, month=m, top_n=None)
        month_total += sum(e["gbp_volume"] for e in month_edges)
    assert abs(month_total - all_vol) < 1e-3


@SKIP
def test_month_and_year_are_mutually_exclusive(con):
    fg = render.default_focal_group(con)
    with pytest.raises(ValueError, match="not both"):
        render.star_map_frame(con, fg, month="2025-07", year=2025)


@SKIP
def test_driven_only_from_curated(con):
    """Dropping every upstream schema leaves the frame unchanged — curated is self-sufficient."""
    fg = render.default_focal_group(con)
    before_nodes, before_edges = render.star_map_frame(con, fg, top_n=None)
    for schema in ("raw", "live", "core", "shape", "data_mart"):
        for (tbl,) in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = ?", [schema]
        ).fetchall():
            con.execute(f"DROP TABLE {schema}.{tbl}")
    after_nodes, after_edges = render.star_map_frame(con, fg, top_n=None)
    assert len(after_nodes) == len(before_nodes)
    assert len(after_edges) == len(before_edges)


@SKIP
def test_focal_node_carries_company_drill_breakdown(con):
    fg = render.default_focal_group(con)
    nodes, _ = render.star_map_frame(con, fg, top_n=None)
    focal = next(n for n in nodes if n["is_focal"])
    companies = focal["companies"]
    # The focal group carries its drill level (direct companies), summed from curated.edge only.
    assert companies and all(c["gbp_volume"] >= 0 for c in companies)
    total = sum(c["gbp_volume"] for c in companies)
    cur = con.execute(
        "SELECT SUM(gbp_volume) FROM curated.edge WHERE focal_group_id = ?", [fg]
    ).fetchone()[0]
    assert abs(total - cur) < 1e-3  # the breakdown reconciles to the group's total
    # Counterparts (circles or diamonds) have no drill level here — they carry no sub-entities.
    assert all(n["companies"] is None for n in nodes if not n["is_focal"])


@SKIP
def test_node_tooltip_reveals_drill_level(con):
    fg = render.default_focal_group(con)
    nodes, edges = render.star_map_frame(con, fg, top_n=None)
    focal = next(n for n in nodes if n["is_focal"])
    title = render._node_title(focal, edges)
    assert "Drill level" in title and "direct" in title
    # A standalone diamond is flagged as a leaf with nothing beneath it.
    diamond = next((n for n in nodes if n["node_shape"] == "diamond"), None)
    if diamond is not None:
        assert "leaf" in render._node_title(diamond, edges)


@SKIP
def test_render_star_map_writes_self_contained_html(con, tmp_path):
    pytest.importorskip("pyvis")
    out = render.render_star_map(con, top_n=5, out_path=tmp_path / "star_map.html")
    assert out.exists() and out.stat().st_size > 0
    html = out.read_text()
    # Self-contained (inlined vis.js, no external fetch) and carries at least one edge label.
    assert "vis-network" in html or "vis.min.js" in html
    assert "txns" in html
    # The drill level is surfaced in the focal group's tooltip.
    assert "Drill level" in html
