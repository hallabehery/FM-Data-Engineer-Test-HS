"""Pipeline orchestrator — the single entrypoint that builds the warehouse.

Run with `python -m src.pipeline` (or `make pipeline`). Each later ticket adds
its stage here; today it builds the foundation: connect and create the six
Medallion schemas. Idempotent and re-runnable from an empty warehouse.
"""
from __future__ import annotations

import duckdb

from . import config, warehouse
from .reporting import logger


def build_foundation(con: duckdb.DuckDBPyConnection) -> tuple[str, ...]:
    """Create the six Medallion schemas up front. Returns the schema names."""
    created = warehouse.create_schemas(con)
    logger.info(f"[foundation] schemas ready: {', '.join(created)}")
    return created


def main() -> None:
    """Build the pipeline end-to-end (foundation only, for now)."""
    logger.info(f"[foundation] opening warehouse at {config.WAREHOUSE_PATH}")
    con = warehouse.connect()
    try:
        build_foundation(con)
    finally:
        con.close()
    logger.info("[foundation] done")


if __name__ == "__main__":
    main()
