"""Pipeline orchestrator — the single entrypoint that builds the warehouse.

Run with `python -m src.pipeline` (or `make pipeline`). Each later ticket adds
its stage here; today it builds the foundation: connect and create the six
Medallion schemas. Idempotent and re-runnable from an empty warehouse.
"""
from __future__ import annotations

import duckdb

from . import bronze, config, fx, silver_core, warehouse
from .reporting import logger


def build_foundation(con: duckdb.DuckDBPyConnection) -> tuple[str, ...]:
    """Create the six Medallion schemas up front. Returns the schema names."""
    created = warehouse.create_schemas(con)
    logger.info(f"[foundation] schemas ready: {', '.join(created)}")
    return created


def main() -> None:
    """Build the pipeline end-to-end (foundation → Silver core, so far)."""
    logger.info(f"opening warehouse at {config.WAREHOUSE_PATH}")
    con = warehouse.connect()
    try:
        build_foundation(con)
        bronze.build_raw_monthly(con)
        bronze.build_live(con)
        silver_core.build_companies(con)
        silver_core.build_groups(con)
        fx_rates = fx.FxRates.load()  # load the ~18 MB rate file once, share it
        silver_core.build_exchange_rates(con, fx_rates)
        silver_core.build_fx_match(con, fx_rates)
    finally:
        con.close()
    logger.info("pipeline done")


if __name__ == "__main__":
    main()
