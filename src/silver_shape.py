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
