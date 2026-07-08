"""Central configuration for the Freemarket Medallion pipeline.

Single source of truth for paths, FX settings, the FX coverage window, and the
transaction period. Import values from here — never hardcode paths or magic
numbers in transformation modules or the notebook.
"""
from __future__ import annotations

from pathlib import Path

# --- Repository layout -------------------------------------------------------
# config.py lives in <repo>/src/, so the repository root is one level up.
REPO_ROOT: Path = Path(__file__).resolve().parents[1]
DATA_DIR: Path = REPO_ROOT / "data"
SUBMISSION_DIR: Path = REPO_ROOT / "submission"

# --- Warehouse ---------------------------------------------------------------
# The whole warehouse is a single local DuckDB file, landed in the deliverable
# folder so `submission/` is self-contained.
WAREHOUSE_PATH: Path = SUBMISSION_DIR / "warehouse.duckdb"

# --- Raw sources (read-only) -------------------------------------------------
TRANSACTIONS_XLSX: Path = DATA_DIR / "transactional_data_jul25_dec25.xlsx"
COMPANIES_JSON: Path = DATA_DIR / "companies.json"
GROUPS_JSON: Path = DATA_DIR / "groups.json"
EXCHANGE_RATES_JSON: Path = DATA_DIR / "exchange_rates.json"

# exchange_rates.json (~18 MB) exceeds DuckDB's default JSON max object size
# (16 MB); read it with this raised limit (64 MB).
FX_MAX_OBJECT_SIZE: int = 67_108_864

# --- FX coverage window (from exchange_rates.json meta.coverage) -------------
# Instants outside this window have no rate and must be quarantined downstream,
# never silently converted.
FX_COVERAGE_FROM_EPOCH_MS: int = 1_750_061_645_260  # ~2025-06-16T08:14:05Z
FX_COVERAGE_TILL_EPOCH_MS: int = 1_768_204_893_003  # ~2026-01-12T08:01:33Z

# --- Transaction period ------------------------------------------------------
# The transactional workbook covers July–December 2025. Bronze `raw` lands one
# table per month; these are the six months, in order.
PERIOD_MONTHS: tuple[str, ...] = (
    "2025-07",
    "2025-08",
    "2025-09",
    "2025-10",
    "2025-11",
    "2025-12",
)

# --- Medallion schemas (created up front, per docs/build_protocol.md) --------
# Bronze = raw + live; Silver = core + shape; Gold = data_mart + curated.
SCHEMAS: tuple[str, ...] = ("raw", "live", "core", "shape", "data_mart", "curated")
