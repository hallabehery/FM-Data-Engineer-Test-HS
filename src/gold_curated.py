"""Gold `curated` — the final network product a graph tool can render directly.

`curated` is the presentation layer: it reads **only** from `data_mart` (never the
reverse) and reshapes the modelled network into the two tables a graph library
expects — `node` (`build_node`) and `edge` (`build_edge`).

`build_node` lands `curated.node`: exactly one row per node that participates in an
edge, typed by shape — **group nodes (circles)** for counterparts that resolve to one
of our groups, **standalone nodes (diamonds)** for those that do not. A node
participates if it appears on either end of a `data_mart.money_flow` row (as a
`focal_group_id` or a `counterpart_id`); its display attributes come from the
`data_mart.entity` dimension. Companies and group-resolved counterparties are *not*
nodes — the drill level and resolved counterparts are represented by their group's
circle — so they are excluded here (they carry no `node_shape` in `entity`).

`build_edge` lands `curated.edge`: the directed money-flow edges a graph tool renders
with no further transformation. Each edge carries directed endpoints, GBP volume,
transaction count and GBP fee revenue, is sliceable by `month` and `year` (year rolls
up from month by summation), and is drillable up/down the hierarchy via `focal_group_id`
(roll up) and `focal_company_id` (drill down) — both grains summing from the same rows.
"""
from __future__ import annotations

import duckdb

from .reporting import StageReport, report_stage


def build_node(con: duckdb.DuckDBPyConnection) -> StageReport:
    """Build `curated.node` — the network nodes (circles + diamonds). Idempotent.

    Reads only from `data_mart` (`money_flow` for participation, `entity` for the node
    attributes). Raises if any edge endpoint fails to resolve to a node, or if the
    join fans a single endpoint out to more than one node (an id-domain collision).
    """
    con.execute(
        """
        CREATE OR REPLACE TABLE curated.node AS
        WITH edge_node AS (  -- every id on either end of an edge, de-duplicated
            SELECT focal_group_id AS node_id FROM data_mart.money_flow
            UNION
            SELECT counterpart_id AS node_id FROM data_mart.money_flow
        )
        SELECT e.entity_id     AS node_id,
               e.name          AS name,
               e.entity_kind   AS node_kind,
               e.node_shape    AS node_shape,   -- 'circle' (group) | 'diamond' (standalone)
               e.is_standalone AS is_standalone,
               e.group_id      AS group_id,     -- a group node rolls up to itself
               e.source        AS source
        FROM edge_node en
        -- node_shape IS NOT NULL keeps only true nodes: excludes companies (drill level)
        -- and group-resolved counterparts (represented by their group's circle).
        JOIN data_mart.entity e
             ON e.entity_id = en.node_id AND e.node_shape IS NOT NULL
        ORDER BY node_shape, node_id
        """
    )

    # Conservation: exactly one node per distinct edge endpoint — no loss (an endpoint
    # that resolved to no node) and no fan-out (an id matching more than one node row).
    n_endpoints = con.execute(
        "SELECT COUNT(*) FROM ("
        "SELECT focal_group_id AS node_id FROM data_mart.money_flow "
        "UNION SELECT counterpart_id FROM data_mart.money_flow)"
    ).fetchone()[0]
    n_nodes = con.execute("SELECT COUNT(*) FROM curated.node").fetchone()[0]
    if n_nodes != n_endpoints:
        raise ValueError(
            f"curated.node: {n_nodes} nodes != {n_endpoints} distinct edge endpoints "
            "(unresolved endpoint or join fan-out)"
        )
    # And each node id is present exactly once.
    dup = con.execute(
        "SELECT COUNT(*) FROM (SELECT node_id FROM curated.node GROUP BY node_id HAVING COUNT(*) > 1)"
    ).fetchone()[0]
    if dup:
        raise ValueError(f"curated.node: {dup} node ids appear more than once")

    return report_stage(
        "gold.curated.node", rows_in=n_endpoints, rows_out=n_nodes, kept=n_nodes
    )


def build_edge(con: duckdb.DuckDBPyConnection) -> StageReport:
    """Build `curated.edge` — the directed money-flow edges. Idempotent.

    Reads only from `data_mart` (`money_flow` for the fact, `entity` for the drill label). A 1:1
    projection of the fact (finest grain: `focal_group × focal_company × counterpart × direction ×
    month`) into the shape a graph tool renders directly:

    - **Directed endpoints** `source_node_id → target_node_id` at group grain (the ids in
      `curated.node`): inflow = money into the focal group (`counterpart → focal_group`);
      outflow = money out (`focal_group → counterpart`).
    - **Measures** `gbp_volume`, `txn_count`, `gbp_fee_revenue` carried per edge.
    - **Slicing** by `month` and `year` (year = calendar year of `month`; rolls up by
      summation).
    - **Drill** via `focal_group_id` (roll up) and `focal_company_id` (drill to the entity
      that actually transacted) — the group view is the sum over its companies. The company's
      display name is carried alongside the key as `focal_company_name`, so a consumer can render
      the drill straight from `curated` without joining back to the `entity` dimension.

    Raises if the projection loses/duplicates rows, if any company key fails to resolve to a name,
    or if the measures do not reconcile to `data_mart.money_flow`.
    """
    con.execute(
        """
        CREATE OR REPLACE TABLE curated.edge AS
        SELECT
            -- Directed endpoints at group grain — both are ids present in curated.node.
            IF(mf.direction = 'inflow', mf.counterpart_id, mf.focal_group_id)  AS source_node_id,
            IF(mf.direction = 'inflow', mf.focal_group_id, mf.counterpart_id)  AS target_node_id,
            mf.direction,
            -- Hierarchy: roll up on focal_group_id, drill down on focal_company_id (+ its name).
            mf.focal_group_id,
            mf.focal_company_id,
            fc.name                          AS focal_company_name,
            mf.counterpart_id,
            mf.counterpart_is_group,
            -- Slicing: month is the additive base; year rolls up from it by summation.
            mf.month,
            CAST(year(mf.month) AS INTEGER)  AS year,
            mf.gbp_volume,
            mf.txn_count,
            mf.gbp_fee_revenue,
            mf.source
        FROM data_mart.money_flow mf
        -- Drill label: each focal_company_id resolves to exactly one company (1:1, no fan-out).
        LEFT JOIN data_mart.entity fc
             ON fc.entity_kind = 'company' AND fc.entity_id = mf.focal_company_id
        ORDER BY mf.focal_group_id, mf.focal_company_id, mf.counterpart_id, mf.direction, mf.month
        """
    )

    # Conservation: 1:1 projection — no row loss/fan-out, and measures reconcile to the fact.
    n_fact = con.execute("SELECT COUNT(*) FROM data_mart.money_flow").fetchone()[0]
    n_edge = con.execute("SELECT COUNT(*) FROM curated.edge").fetchone()[0]
    if n_edge != n_fact:
        raise ValueError(
            f"curated.edge: {n_edge} edges != {n_fact} data_mart.money_flow rows "
            "(projection lost or duplicated rows)"
        )
    # Integrity: the drill label must always resolve — no company key left without a name.
    unnamed = con.execute(
        "SELECT COUNT(*) FROM curated.edge WHERE focal_company_name IS NULL"
    ).fetchone()[0]
    if unnamed:
        raise ValueError(
            f"curated.edge: {unnamed} rows have no focal_company_name "
            "(a focal_company_id did not resolve to a company in data_mart.entity)"
        )
    e_vol, e_cnt, e_fee = con.execute(
        "SELECT SUM(gbp_volume), SUM(txn_count), SUM(gbp_fee_revenue) FROM curated.edge"
    ).fetchone()
    f_vol, f_cnt, f_fee = con.execute(
        "SELECT SUM(gbp_volume), SUM(txn_count), SUM(gbp_fee_revenue) FROM data_mart.money_flow"
    ).fetchone()
    if e_cnt != f_cnt or abs(e_vol - f_vol) > 1e-3 or abs(e_fee - f_fee) > 1e-3:
        raise ValueError(
            "curated.edge measures do not reconcile to data_mart.money_flow "
            f"(vol {e_vol} vs {f_vol}, cnt {e_cnt} vs {f_cnt}, fee {e_fee} vs {f_fee})"
        )

    return report_stage("gold.curated.edge", rows_in=n_fact, rows_out=n_edge, kept=n_edge)
