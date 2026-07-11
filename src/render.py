"""Illustrative network render — optional proof that Gold `curated` drives a graph directly.

This is **not** a pipeline stage and **not** the deliverable: SPEC § Out of Scope is explicit
that we produce the data that *drives* a graph and a single render is optional proof. The
point of this module is to demonstrate that Gold `curated` needs no further transformation —
it reads **only** from `curated.node` + `curated.edge` and hands those rows straight to a
graph library:

- **circle** nodes for our groups, **diamond** nodes for standalone counterparties
  (`curated.node.node_shape`, taken verbatim);
- **directed** edges `source_node_id -> target_node_id` (`curated.edge`, taken verbatim);
- edge labels carry **GBP volume, transaction count and GBP fee revenue**.

The only presentation-layer choices are (a) filtering to one focal group and period — the
slicing/roll-up `curated` is built to support (SPEC user stories 3, 8) — and (b) an optional
top-N cap on counterparts so a busy hub stays legible. Neither reshapes the model: the arrows
are `curated.edge` rows and the measures are summed straight from them (the documented
month→period, company→group roll-up), never re-derived.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import duckdb

from . import config

# Default legibility cap: a hub group can transact with 200+ counterparties; showing the
# top few by volume keeps the illustrative picture readable without reshaping the data.
DEFAULT_TOP_N: int = 12
DEFAULT_OUTPUT: Path = config.SUBMISSION_DIR / "star_map.html"

# pyvis node shapes mapped from curated's node_shape vocabulary.
_PYVIS_SHAPE: dict[str, str] = {"circle": "dot", "diamond": "diamond"}


def default_focal_group(con: duckdb.DuckDBPyConnection) -> str:
    """Return the focal group with the largest total GBP volume — a sensible render default."""
    row = con.execute(
        "SELECT focal_group_id FROM curated.edge GROUP BY 1 ORDER BY SUM(gbp_volume) DESC LIMIT 1"
    ).fetchone()
    if row is None:
        raise ValueError("curated.edge is empty — build the pipeline before rendering")
    return row[0]


def _normalise_month(month: str | date | None) -> str | None:
    """Normalise a month slice to a first-of-month `YYYY-MM-01` string (or None)."""
    if month is None:
        return None
    if isinstance(month, date):
        return month.replace(day=1).isoformat()
    month = str(month)
    return f"{month}-01" if len(month) == 7 else month  # accept 'YYYY-MM' or 'YYYY-MM-01'


def star_map_frame(
    con: duckdb.DuckDBPyConnection,
    focal_group_id: str,
    *,
    month: str | date | None = None,
    year: int | None = None,
    top_n: int | None = DEFAULT_TOP_N,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Build the (nodes, edges) a graph tool renders for one focal group + period.

    Reads **only** from `curated.node` and `curated.edge`. The finest company×month edge grain
    is rolled up to the group-level star-map grain — one directed arrow per
    `(source, target, direction)` — by the summation `curated` is built to support; `month`
    /`year` slice the period (mutually exclusive), and `top_n` caps counterparts by total
    volume for legibility. Nothing is re-shaped: shapes come from `curated.node` and measures
    are summed straight from `curated.edge`.
    """
    if month is not None and year is not None:
        raise ValueError("pass either month or year to slice the period, not both")

    where = ["focal_group_id = ?"]
    params: list[Any] = [focal_group_id]
    if (m := _normalise_month(month)) is not None:
        where.append("month = CAST(? AS DATE)")
        params.append(m)
    if year is not None:
        where.append("year = ?")
        params.append(year)
    predicate = " AND ".join(where)

    # Roll the finest grain up to the group-level star-map grain (sum over company + month).
    edge_rows = con.execute(
        f"""
        SELECT source_node_id, target_node_id, direction, counterpart_id, counterpart_is_group,
               SUM(gbp_volume)      AS gbp_volume,
               SUM(txn_count)       AS txn_count,
               SUM(gbp_fee_revenue) AS gbp_fee_revenue
        FROM curated.edge
        WHERE {predicate}
        GROUP BY source_node_id, target_node_id, direction, counterpart_id, counterpart_is_group
        ORDER BY gbp_volume DESC
        """,
        params,
    ).fetchall()
    edge_cols = [
        "source_node_id", "target_node_id", "direction", "counterpart_id",
        "counterpart_is_group", "gbp_volume", "txn_count", "gbp_fee_revenue",
    ]
    edges = [dict(zip(edge_cols, r)) for r in edge_rows]

    # Legibility cap: keep the top-N counterparts by total (in + out) volume, focal group aside.
    if top_n is not None:
        vol_by_cp: dict[str, float] = {}
        for e in edges:
            vol_by_cp[e["counterpart_id"]] = vol_by_cp.get(e["counterpart_id"], 0.0) + e["gbp_volume"]
        keep = {cp for cp, _ in sorted(vol_by_cp.items(), key=lambda kv: kv[1], reverse=True)[:top_n]}
        edges = [e for e in edges if e["counterpart_id"] in keep]

    # Nodes = every id on either end of a kept edge, attributes taken verbatim from curated.node.
    node_ids = {focal_group_id}
    for e in edges:
        node_ids.add(e["source_node_id"])
        node_ids.add(e["target_node_id"])
    node_rows = con.execute(
        "SELECT node_id, name, node_kind, node_shape, is_standalone "
        "FROM curated.node WHERE node_id IN "
        f"({', '.join('?' * len(node_ids))}) ORDER BY node_shape, node_id",
        list(node_ids),
    ).fetchall()
    nodes = [
        {
            "node_id": nid, "name": name, "node_kind": kind, "node_shape": shape,
            "is_standalone": standalone, "is_focal": nid == focal_group_id,
        }
        for nid, name, kind, shape, standalone in node_rows
    ]
    return nodes, edges


def _edge_label(edge: dict[str, Any]) -> str:
    """Format the edge label: GBP volume · transaction count · GBP fee revenue."""
    return (
        f"£{edge['gbp_volume']:,.0f} · {int(edge['txn_count'])} txns · "
        f"£{edge['gbp_fee_revenue']:,.0f} fee"
    )


def render_star_map(
    con: duckdb.DuckDBPyConnection,
    focal_group_id: str | None = None,
    *,
    month: str | date | None = None,
    year: int | None = None,
    top_n: int | None = DEFAULT_TOP_N,
    out_path: Path | str | None = None,
    notebook: bool = False,
) -> Path:
    """Render one focal group's money-flow network to a self-contained HTML file.

    Circles are groups, diamonds are standalone counterparts (the focal group is highlighted);
    edges are directed and labelled with GBP volume, count and fee revenue. Returns the path of
    the written HTML. `pyvis` is imported lazily so the rest of the module (and its tests) work
    without the optional render dependency installed.
    """
    from pyvis.network import Network  # lazy: optional render-only dependency

    if focal_group_id is None:
        focal_group_id = default_focal_group(con)
    nodes, edges = star_map_frame(con, focal_group_id, month=month, year=year, top_n=top_n)
    out = Path(out_path) if out_path is not None else DEFAULT_OUTPUT
    out.parent.mkdir(parents=True, exist_ok=True)

    net = Network(height="750px", width="100%", directed=True, notebook=notebook, cdn_resources="in_line")
    for n in nodes:
        focal = n["is_focal"]
        net.add_node(
            n["node_id"],
            label=n["name"] or n["node_id"],
            shape=_PYVIS_SHAPE.get(n["node_shape"], "dot"),
            color="#e4572e" if focal else ("#4c9f70" if n["node_shape"] == "circle" else "#8896ab"),
            size=32 if focal else 18,
            title=f"{n['name']} ({n['node_kind']}, {n['node_shape']})",
        )
    for e in edges:
        net.add_edge(
            e["source_node_id"], e["target_node_id"],
            label=_edge_label(e),
            title=f"{e['direction']}: {_edge_label(e)}",
            color="#4c9f70" if e["direction"] == "inflow" else "#e4572e",
        )
    net.write_html(str(out), notebook=notebook, open_browser=False)
    return out


def main() -> None:
    """Render the default focal group's star map from the built warehouse (optional proof)."""
    from . import warehouse
    from .reporting import logger

    con = warehouse.connect()
    try:
        fg = default_focal_group(con)
        out = render_star_map(con, fg, top_n=DEFAULT_TOP_N)
    finally:
        con.close()
    logger.info(f"[render] star map for focal group {fg} -> {out}")


if __name__ == "__main__":
    main()
