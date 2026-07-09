"""Silver `shape` — remaining cleanup.

Resolves the heterogeneous entity `attributes` (carried forward as JSON in `core`)
into consistent, queryable columns. The `value` of an attribute is sometimes a scalar
and sometimes an object; we **branch on the value's type**:

- scalar (string/number/bool)  → one column `attr_<name>`
- object `{k: v, ...}`          → one column per key, `attr_<name>_<k>`
- array                         → the JSON text in `attr_<name>`

So a shape difference never breaks the build, and if the same attribute name appears as
a scalar in some rows and an object in others, each row simply populates whichever
columns apply (the rest are NULL). Entity row counts are unchanged by the cleanup.
"""
from __future__ import annotations

import json

import duckdb
import pandas as pd

from .reporting import StageReport, report_stage


def _flatten_attributes(attributes_json: str | None) -> dict:
    """Flatten one entity's `attributes` array into `attr_*` columns (branch on type)."""
    flat: dict[str, object] = {}
    for attr in json.loads(attributes_json) if attributes_json else []:
        name = attr["name"]
        value = attr.get("value")
        if isinstance(value, dict):  # object → one column per key
            for key, sub in value.items():
                flat[f"attr_{name}_{key}"] = sub
        elif isinstance(value, list):  # array → keep as JSON text
            flat[f"attr_{name}"] = json.dumps(value)
        else:  # scalar
            flat[f"attr_{name}"] = value
    return flat


def _resolve_attributes(
    con: duckdb.DuckDBPyConnection, source: str, target: str, stage: str
) -> StageReport:
    """Land `target` = `source` (minus `attributes_json`) + resolved `attr_*` columns."""
    df = con.sql(f"SELECT * FROM {source}").df()
    attr_df = pd.DataFrame([_flatten_attributes(v) for v in df["attributes_json"]])
    base = df.drop(columns=["attributes_json"]).reset_index(drop=True)
    out = pd.concat([base, attr_df], axis=1)

    con.register("_shape_out", out)
    try:
        con.execute(f"CREATE OR REPLACE TABLE {target} AS SELECT * FROM _shape_out")
    finally:
        con.unregister("_shape_out")

    n_in = con.execute(f"SELECT COUNT(*) FROM {source}").fetchone()[0]
    n_out = con.execute(f"SELECT COUNT(*) FROM {target}").fetchone()[0]
    if n_in != n_out:
        raise ValueError(f"{target}: cleanup changed row count ({n_in} -> {n_out})")
    return report_stage(stage, rows_in=n_in, rows_out=n_out, kept=n_out)


def build_entity_shape(con: duckdb.DuckDBPyConnection) -> list[StageReport]:
    """Resolve company/group attributes into `shape.company` / `shape.corporate_group`."""
    return [
        _resolve_attributes(con, "core.company", "shape.company", "silver.shape.company"),
        _resolve_attributes(
            con, "core.corporate_group", "shape.corporate_group", "silver.shape.corporate_group"
        ),
    ]


# --- FX applied → GBP normalisation ------------------------------------------
# (stream, key column, native amount column). Facts live in `live`; their FX match
# lives in `core.<stream>_fx`. GBP = native amount × the matched rate.
_GBP_FACTS = [
    ("deposit", "transaction_id", "tx_value_ccy"),
    ("withdrawal", "transaction_id", "tx_value_ccy"),
    ("fee", "fee_id", "fee_amount_ccy"),
]


def _apply_fx(
    con: duckdb.DuckDBPyConnection, stream: str, key_col: str, amount_col: str
) -> StageReport:
    """Join a `live` fact to its `core.*_fx` match, compute GBP, split promoted/quarantined.

    Priced rows land in `shape.<stream>` with `gbp_amount = <amount> × fx_rate` (plus
    `fx_rate`/`fx_rate_id` for lineage). Rows whose match carries a
    `fx_quarantine_reason` are routed to `shape.<stream>_quarantine` (not priced), so no
    wrong number reaches Gold.
    """
    fact, match = f"live.{stream}", f"core.{stream}_fx"
    promoted, quarantine = f"shape.{stream}", f"shape.{stream}_quarantine"

    # Join the fact to its match once, then split priced vs quarantined from the shared
    # result. The join is inner: the 1:1 match invariant is asserted upstream in
    # `silver_core._match_fx`, and the conservation guard below would raise if any fact
    # row failed to match (it would appear in neither output).
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE _shape_join AS
        SELECT f.*, m.fx_rate_id, m.fx_rate, m.fx_quarantine_reason
        FROM {fact} f
        JOIN {match} m ON f.{key_col} = m.{key_col}
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {promoted} AS
        SELECT * EXCLUDE (fx_quarantine_reason), ({amount_col} * fx_rate) AS gbp_amount
        FROM _shape_join
        WHERE fx_quarantine_reason IS NULL
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {quarantine} AS
        SELECT * EXCLUDE (fx_rate_id, fx_rate)
        FROM _shape_join
        WHERE fx_quarantine_reason IS NOT NULL
        """
    )
    con.execute("DROP TABLE _shape_join")

    n_in = con.execute(f"SELECT COUNT(*) FROM {fact}").fetchone()[0]
    n_promoted = con.execute(f"SELECT COUNT(*) FROM {promoted}").fetchone()[0]
    n_quarantined = con.execute(f"SELECT COUNT(*) FROM {quarantine}").fetchone()[0]
    if n_promoted + n_quarantined != n_in:
        raise ValueError(
            f"shape.{stream}: promoted {n_promoted} + quarantined {n_quarantined} != input {n_in}"
        )
    return report_stage(
        f"silver.shape.{stream}",
        rows_in=n_in,
        rows_out=n_promoted,
        kept=n_promoted,
        quarantined=n_quarantined,
    )


def build_gbp_facts(con: duckdb.DuckDBPyConnection) -> list[StageReport]:
    """Apply FX to each fact → GBP-normalised `shape.<stream>` (+ quarantine). Idempotent."""
    return [_apply_fx(con, stream, key, amount) for stream, key, amount in _GBP_FACTS]
