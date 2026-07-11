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

from .reporting import StageReport, logger, report_stage


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


def build_edge_fact(con: duckdb.DuckDBPyConnection) -> StageReport:
    """Build `data_mart.edge_fact` — the directed money-flow edges. Idempotent.

    Grain: `focal_group × counterpart × direction × month`, with additive measures
    `gbp_volume`, `txn_count`, `gbp_fee_revenue`. Direction convention: **deposit = inflow**
    (money into the focal group), **withdrawal = outflow**. The focal group is the
    transacting company's parent group (`dc_id → company → group`); the counterpart is the
    counterparty resolved to its group where possible (`data_mart.entity`), else the
    standalone counterparty itself. Fees attach to their transaction by
    `(link_id, fee_type)` — disambiguating the transaction IDs shared across the two streams.
    Each row carries a `source` provenance list. Raises on join fan-out/loss.
    """
    con.execute(
        """
        CREATE OR REPLACE TABLE data_mart.edge_fact AS
        WITH txn AS (
            SELECT 'inflow' AS direction, 'Deposit' AS txn_type, transaction_id, dc_id,
                   counterparty_id, gbp_amount,
                   CAST(date_trunc('month', tx_date) AS DATE) AS month
            FROM shape.deposit
            UNION ALL
            SELECT 'outflow', 'Withdrawal', transaction_id, dc_id, counterparty_id, gbp_amount,
                   CAST(date_trunc('month', tx_date) AS DATE)
            FROM shape.withdrawal
        ),
        fee_rev AS (  -- one fee total per (transaction, stream) — no txn fan-out on join
            SELECT link_id, fee_type, SUM(gbp_amount) AS fee_gbp
            FROM shape.fee GROUP BY link_id, fee_type
        ),
        resolved AS (
            SELECT t.direction, t.month, t.gbp_amount,
                   fc.group_id                                   AS focal_group_id,
                   COALESCE(cpe.group_id, cpe.entity_id)         AS counterpart_id,
                   NOT cpe.is_standalone                         AS counterpart_is_group,
                   COALESCE(fr.fee_gbp, 0.0)                     AS fee_gbp
            FROM txn t
            JOIN data_mart.entity fc
                 ON fc.entity_kind = 'company' AND fc.entity_id = t.dc_id
            JOIN data_mart.entity cpe
                 ON cpe.entity_kind = 'counterparty' AND cpe.entity_id = t.counterparty_id
            LEFT JOIN fee_rev fr
                 ON fr.link_id = t.transaction_id AND fr.fee_type = t.txn_type
        ),
        agg AS (
            SELECT focal_group_id, counterpart_id, counterpart_is_group, direction, month,
                   SUM(gbp_amount)  AS gbp_volume,
                   COUNT(*)         AS txn_count,
                   SUM(fee_gbp)     AS gbp_fee_revenue
            FROM resolved
            GROUP BY focal_group_id, counterpart_id, counterpart_is_group, direction, month
        )
        SELECT *,
               list_distinct(
                   [IF(direction = 'inflow', 'deposits', 'withdrawals'),
                    'companies.json', 'groups.json']
                   || IF(gbp_fee_revenue > 0, ['fees'], []::VARCHAR[])
                   || IF(counterpart_is_group, []::VARCHAR[], ['counterparty'])
               ) AS source
        FROM agg
        """
    )

    # Conservation: measures reconcile to the promoted Silver facts, no fan-out.
    vol, cnt, fee = con.execute(
        "SELECT SUM(gbp_volume), SUM(txn_count), SUM(gbp_fee_revenue) FROM data_mart.edge_fact"
    ).fetchone()
    src_vol = con.execute(
        "SELECT (SELECT SUM(gbp_amount) FROM shape.deposit) "
        "+ (SELECT SUM(gbp_amount) FROM shape.withdrawal)"
    ).fetchone()[0]
    src_cnt = con.execute(
        "SELECT (SELECT COUNT(*) FROM shape.deposit) + (SELECT COUNT(*) FROM shape.withdrawal)"
    ).fetchone()[0]
    src_fee = con.execute("SELECT SUM(gbp_amount) FROM shape.fee").fetchone()[0]
    if cnt != src_cnt:
        raise ValueError(f"edge_fact txn_count {cnt} != promoted transactions {src_cnt} (fan-out?)")
    if abs(vol - src_vol) > 1e-3:
        raise ValueError(f"edge_fact gbp_volume {vol} != promoted GBP {src_vol}")
    if abs(fee - src_fee) > 1e-3:
        raise ValueError(f"edge_fact gbp_fee_revenue {fee} != promoted fee GBP {src_fee}")

    # This is an aggregation (many transactions → one edge), so row-count conservation
    # doesn't apply; the measure reconciliation above is the invariant. Report the edge
    # count and log the transaction → edge collapse explicitly.
    n_edges = con.execute("SELECT COUNT(*) FROM data_mart.edge_fact").fetchone()[0]
    logger.info(
        f"[gold.data_mart.edge_fact] {src_cnt} transactions -> {n_edges} edges; "
        "gbp_volume / txn_count / gbp_fee_revenue reconciled to Silver"
    )
    return report_stage("gold.data_mart.edge_fact", rows_in=n_edges, rows_out=n_edges, kept=n_edges)
