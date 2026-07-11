"""Consolidated conservation suite — the whole Bronze → Silver → Gold spine, one build.

This is the single pass a reviewer reads to see the pipeline is trustworthy. It builds the
entire warehouse **once** (module-scoped fixture) and then asserts, in data-flow order:

1. every layer's own conservation invariant (re-stated at the spine level so the whole
   conservation story reads in one file), and
2. the **cross-boundary** invariants that no single layer owns — end-to-end row and GBP
   measure conservation across Bronze → Silver → Gold, and the quarantine ledger
   (total input − total promoted = total quarantined, by reason).

Any failure here fails `pytest` (non-zero exit), so `make test` / `make all` go red — the
build is gated on conservation holding. The per-layer test files still own the fine-grained
assertions; this file owns the end-to-end story.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from src import (
    bronze,
    config,
    gold,
    gold_curated,
    silver_core,
    silver_shape,
    warehouse,
)

SKIP = pytest.mark.skipif(
    not (Path(config.TRANSACTIONS_XLSX).exists() and Path(config.EXCHANGE_RATES_JSON).exists()
         and Path(config.COMPANIES_JSON).exists() and Path(config.GROUPS_JSON).exists()),
    reason="source data not present",
)

# Known-good reference counts (the same figures the per-layer tests pin).
DEPOSITS, WITHDRAWALS = 6383, 6599
TRANSACTIONS = DEPOSITS + WITHDRAWALS            # 12982
COUNTERPARTIES, FEES = 1585, 21921
FEE_QUARANTINED = 42                             # null-currency fees → fx_missing_currency
FEE_PROMOTED = FEES - FEE_QUARANTINED            # 21879
STREAMS = ("deposit", "withdrawal", "fee")


@pytest.fixture(scope="module")
def con(tmp_path_factory):
    """Build the entire Bronze → Silver → Gold warehouse once for the whole module."""
    c = warehouse.connect(tmp_path_factory.mktemp("wh") / "warehouse.duckdb")
    warehouse.create_schemas(c)
    # Bronze
    bronze.build_raw_monthly(c)
    bronze.build_live(c)
    # Silver core
    silver_core.build_companies(c)
    silver_core.build_groups(c)
    silver_core.build_exchange_rates(c)
    silver_core.build_fx_match(c)
    # Silver shape
    silver_shape.build_entity_shape(c)
    silver_shape.build_gbp_facts(c)
    # Gold
    gold.build_entity(c)
    gold.build_edge_fact(c)
    gold_curated.build_node(c)
    gold_curated.build_edge(c)
    yield c
    c.close()


def _count(con, table: str) -> int:
    return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


# --- Per-layer invariants, re-stated at the spine level ----------------------

@SKIP
def test_bronze_raw_partitions_into_live(con):
    """Bronze: Σ(per-month raw tables) == consolidated live, per stream (no loss, no dup)."""
    for stream, expected in (("deposit", DEPOSITS), ("withdrawal", WITHDRAWALS)):
        raw_total = sum(
            _count(con, f"raw.{stream}_{ym.replace('-', '_')}") for ym in config.PERIOD_MONTHS
        )
        assert raw_total == expected
        assert _count(con, f"live.{stream}") == raw_total


@SKIP
def test_bronze_live_dimensions_landed(con):
    """Bronze: the counterparty dimension and fee fact land at their known counts."""
    assert _count(con, "live.counterparty") == COUNTERPARTIES
    assert _count(con, "live.fee") == FEES


@SKIP
def test_core_fx_match_is_one_row_per_fact(con):
    """Silver core: each *_fx match table has exactly one row per live fact — no fan-out/loss."""
    for stream in STREAMS:
        assert _count(con, f"core.{stream}_fx") == _count(con, f"live.{stream}")
    # Every matched rate_id is a real exchange_rate point (lineage integrity, priced rows).
    dangling = con.execute(
        "SELECT COUNT(*) FROM core.deposit_fx "
        "WHERE fx_rate_id IS NOT NULL "
        "AND fx_rate_id NOT IN (SELECT rate_id FROM core.exchange_rate)"
    ).fetchone()[0]
    assert dangling == 0


@SKIP
def test_silver_shape_promoted_plus_quarantined_equals_input(con):
    """Silver shape: promoted + quarantined == input, per fact stream (nothing vanishes)."""
    for stream in STREAMS:
        n_in = _count(con, f"live.{stream}")
        n_prom = _count(con, f"shape.{stream}")
        n_quar = _count(con, f"shape.{stream}_quarantine")
        assert n_prom + n_quar == n_in
    # Transactions are fully priced; only the 42 null-currency fees quarantine.
    assert _count(con, "shape.deposit_quarantine") == 0
    assert _count(con, "shape.withdrawal_quarantine") == 0
    assert _count(con, "shape.fee_quarantine") == FEE_QUARANTINED


@SKIP
def test_gold_entity_and_nodes_reconcile(con):
    """Gold: entity reconciles 1:1 to its sources; every edge endpoint is a node exactly once."""
    kinds = dict(
        con.execute("SELECT entity_kind, COUNT(*) FROM data_mart.entity GROUP BY 1").fetchall()
    )
    assert kinds == {"group": 13, "company": 44, "counterparty": COUNTERPARTIES}
    endpoints = con.execute(
        "SELECT COUNT(*) FROM (SELECT focal_group_id AS n FROM data_mart.edge_fact "
        "UNION SELECT counterpart_id FROM data_mart.edge_fact)"
    ).fetchone()[0]
    assert _count(con, "curated.node") == endpoints


# --- Cross-boundary invariants no single layer owns --------------------------

@SKIP
def test_end_to_end_transaction_rows_never_lost(con):
    """Row spine: every transaction flows Bronze → Silver → Gold with none lost or fanned out.

    Transactions are never quarantined, so the count is invariant the whole way down and
    lands as edge_fact / curated.edge `txn_count`.
    """
    raw = sum(
        _count(con, f"raw.{s}_{ym.replace('-', '_')}")
        for s in ("deposit", "withdrawal") for ym in config.PERIOD_MONTHS
    )
    live = _count(con, "live.deposit") + _count(con, "live.withdrawal")
    promoted = _count(con, "shape.deposit") + _count(con, "shape.withdrawal")
    edge_fact_cnt = con.execute("SELECT SUM(txn_count) FROM data_mart.edge_fact").fetchone()[0]
    curated_cnt = con.execute("SELECT SUM(txn_count) FROM curated.edge").fetchone()[0]
    assert raw == live == promoted == edge_fact_cnt == curated_cnt == TRANSACTIONS


@SKIP
def test_end_to_end_gbp_measures_conserved(con):
    """Measure spine: Σ GBP volume and Σ GBP fee revenue are invariant Silver → Gold.

    No aggregation step invents or drops value: the promoted Silver GBP totals equal the
    edge_fact measures, which equal the curated.edge measures.
    """
    shape_vol = con.execute(
        "SELECT (SELECT SUM(gbp_amount) FROM shape.deposit) "
        "+ (SELECT SUM(gbp_amount) FROM shape.withdrawal)"
    ).fetchone()[0]
    shape_fee = con.execute("SELECT SUM(gbp_amount) FROM shape.fee").fetchone()[0]

    fact_vol, fact_fee = con.execute(
        "SELECT SUM(gbp_volume), SUM(gbp_fee_revenue) FROM data_mart.edge_fact"
    ).fetchone()
    edge_vol, edge_fee = con.execute(
        "SELECT SUM(gbp_volume), SUM(gbp_fee_revenue) FROM curated.edge"
    ).fetchone()

    assert abs(fact_vol - shape_vol) < 1e-3
    assert abs(edge_vol - shape_vol) < 1e-3
    assert abs(fact_fee - shape_fee) < 1e-3
    assert abs(edge_fee - shape_fee) < 1e-3


@SKIP
def test_curated_edge_is_lossless_projection_of_edge_fact(con):
    """Gold: curated.edge is a 1:1 projection of edge_fact — equal rows and equal measures."""
    assert _count(con, "curated.edge") == _count(con, "data_mart.edge_fact")
    e = con.execute(
        "SELECT SUM(gbp_volume), SUM(txn_count), SUM(gbp_fee_revenue) FROM curated.edge"
    ).fetchone()
    f = con.execute(
        "SELECT SUM(gbp_volume), SUM(txn_count), SUM(gbp_fee_revenue) FROM data_mart.edge_fact"
    ).fetchone()
    assert e[1] == f[1]
    assert abs(e[0] - f[0]) < 1e-3 and abs(e[2] - f[2]) < 1e-3


@SKIP
def test_quarantine_ledger_reconciles_by_reason(con):
    """Quarantine ledger: Σ(input) − Σ(promoted) == Σ(quarantined), reconciled by reason.

    Across the fact spine, every row is either promoted to GBP or quarantined with a stated
    reason — none silently dropped. The whole ledger is the 42 null-currency fees.
    """
    total_in = sum(_count(con, f"live.{s}") for s in STREAMS)
    total_promoted = sum(_count(con, f"shape.{s}") for s in STREAMS)

    ledger: dict[str, int] = {}
    for stream in STREAMS:
        for reason, n in con.execute(
            f"SELECT fx_quarantine_reason, COUNT(*) FROM shape.{stream}_quarantine GROUP BY 1"
        ).fetchall():
            ledger[reason] = ledger.get(reason, 0) + n

    total_quarantined = sum(ledger.values())
    assert total_in - total_promoted == total_quarantined
    assert ledger == {"fx_missing_currency": FEE_QUARANTINED}
    # Cross-check the promoted totals against the known reference.
    assert total_promoted == DEPOSITS + WITHDRAWALS + FEE_PROMOTED
