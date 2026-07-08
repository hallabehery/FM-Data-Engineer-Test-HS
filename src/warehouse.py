"""Warehouse connection and schema management.

The whole warehouse is one DuckDB file. `connect()` opens it (creating the parent
folder and file if needed); `create_schemas()` creates the six Medallion schemas
up front. Both are idempotent — safe to re-run against an empty or existing
warehouse.
"""
from __future__ import annotations

from pathlib import Path

import duckdb

from . import config


def connect(db_path: Path | str | None = None) -> duckdb.DuckDBPyConnection:
    """Open (or create) the warehouse DuckDB file and return a connection.

    Falls back to the configured `WAREHOUSE_PATH` when no path is given.
    """
    path = Path(db_path) if db_path is not None else config.WAREHOUSE_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(path))


def create_schemas(
    con: duckdb.DuckDBPyConnection,
    schemas: tuple[str, ...] = config.SCHEMAS,
) -> tuple[str, ...]:
    """Create all Medallion schemas up front. Idempotent (`IF NOT EXISTS`)."""
    for schema in schemas:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    return tuple(schemas)


def existing_schemas(con: duckdb.DuckDBPyConnection) -> set[str]:
    """Return the set of schema names currently present in the warehouse."""
    rows = con.execute(
        "SELECT schema_name FROM information_schema.schemata"
    ).fetchall()
    return {row[0] for row in rows}
