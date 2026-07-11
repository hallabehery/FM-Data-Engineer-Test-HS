"""Gold `curated` — the final network product a graph tool can render directly.

`curated` is the presentation layer: it reads **only** from `data_mart` (never the
reverse) and reshapes the modelled network into the two tables a graph library
expects — `node` (this module) and `edge` (a later ticket).

`build_node` lands `curated.node`: exactly one row per node that participates in an
edge, typed by shape — **group nodes (circles)** for counterparts that resolve to one
of our groups, **standalone nodes (diamonds)** for those that do not. A node
participates if it appears on either end of a `data_mart.edge_fact` row (as a
`focal_group_id` or a `counterpart_id`); its display attributes come from the
`data_mart.entity` dimension. Companies and group-resolved counterparties are *not*
nodes — the drill level and resolved counterparts are represented by their group's
circle — so they are excluded here (they carry no `node_shape` in `entity`).
"""
from __future__ import annotations

import duckdb

from .reporting import StageReport, report_stage


def build_node(con: duckdb.DuckDBPyConnection) -> StageReport:
    """Build `curated.node` — the network nodes (circles + diamonds). Idempotent.

    Reads only from `data_mart` (`edge_fact` for participation, `entity` for the node
    attributes). Raises if any edge endpoint fails to resolve to a node, or if the
    join fans a single endpoint out to more than one node (an id-domain collision).
    """
    con.execute(
        """
        CREATE OR REPLACE TABLE curated.node AS
        WITH edge_node AS (  -- every id on either end of an edge, de-duplicated
            SELECT focal_group_id AS node_id FROM data_mart.edge_fact
            UNION
            SELECT counterpart_id AS node_id FROM data_mart.edge_fact
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
        "SELECT focal_group_id AS node_id FROM data_mart.edge_fact "
        "UNION SELECT counterpart_id FROM data_mart.edge_fact)"
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
