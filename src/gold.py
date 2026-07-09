"""Gold `data_mart` — modelled network data with provenance.

`build_entity` combines the group + company hierarchy (Silver `shape`) with the
counterparties (Bronze `live` — a dimension that needs no Silver transformation) into a
single entity/node dimension:

- **groups** → circle nodes;
- **companies** → the drill level, rolling up to their parent group (no node shape);
- **counterparties** → resolved to a group where a deterministic key exists
  (`group_id`, or `dc_id` → company → parent group), otherwise **standalone** diamond nodes.

Every row carries a `source` list naming the raw sources that composed it (provenance).
`build_protocol.md` places this "combine client and group tables" step in `data_mart`.
"""
from __future__ import annotations

import duckdb

from .reporting import StageReport, report_stage


def build_entity(con: duckdb.DuckDBPyConnection) -> StageReport:
    """Build `data_mart.entity` — the unified entity/node dimension. Idempotent.

    Raises if any entity kind does not reconcile 1:1 with its source (join fan-out).
    """
    con.execute(
        """
        CREATE OR REPLACE TABLE data_mart.entity AS
        -- Groups: circle nodes, roll up to themselves.
        SELECT group_id       AS entity_id,
               'group'        AS entity_kind,
               display_name   AS name,
               group_id       AS group_id,
               FALSE          AS is_standalone,
               'circle'       AS node_shape,
               ['groups.json'] AS source
        FROM shape.corporate_group
        UNION ALL BY NAME
        -- Companies: drill level, roll up to their parent group (not a node themselves).
        SELECT dc_id            AS entity_id,
               'company'        AS entity_kind,
               legal_name       AS name,
               parent_group_id  AS group_id,
               FALSE            AS is_standalone,
               NULL             AS node_shape,
               ['companies.json'] AS source
        FROM shape.company
        UNION ALL BY NAME
        -- Counterparties: resolve to a group via group_id, else via dc_id -> company,
        -- else standalone (diamond). Resolved counterparties carry no node shape — they
        -- are represented by their group's circle node.
        SELECT cp.cp_id                                      AS entity_id,
               'counterparty'                                AS entity_kind,
               cp.cp_name                                    AS name,
               COALESCE(cp.group_id, co.parent_group_id)     AS group_id,
               (COALESCE(cp.group_id, co.parent_group_id) IS NULL) AS is_standalone,
               CASE WHEN COALESCE(cp.group_id, co.parent_group_id) IS NULL
                    THEN 'diamond' END                       AS node_shape,
               CASE WHEN cp.group_id IS NOT NULL THEN ['counterparty', 'groups.json']
                    WHEN cp.dc_id IS NOT NULL    THEN ['counterparty', 'companies.json']
                    ELSE ['counterparty'] END                AS source
        FROM live.counterparty cp
        LEFT JOIN shape.company co ON cp.dc_id = co.dc_id
        """
    )

    # Conservation: one entity row per distinct source row, no join fan-out.
    counts = dict(
        con.execute(
            "SELECT entity_kind, COUNT(*) FROM data_mart.entity GROUP BY 1"
        ).fetchall()
    )
    expected = {
        "group": con.execute("SELECT COUNT(*) FROM shape.corporate_group").fetchone()[0],
        "company": con.execute("SELECT COUNT(*) FROM shape.company").fetchone()[0],
        "counterparty": con.execute("SELECT COUNT(*) FROM live.counterparty").fetchone()[0],
    }
    for kind, exp in expected.items():
        if counts.get(kind, 0) != exp:
            raise ValueError(
                f"data_mart.entity {kind}: {counts.get(kind, 0)} rows != {exp} source rows "
                "(join fan-out or loss)"
            )
    total = sum(expected.values())
    return report_stage("gold.data_mart.entity", rows_in=total, rows_out=total, kept=total)
