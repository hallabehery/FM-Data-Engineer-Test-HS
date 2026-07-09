"""Silver core FX: pure facts, exchange_rate dimension, and per-stream as-of match tables."""
from __future__ import annotations

from pathlib import Path

import pytest

from src import bronze, config, fx, silver_core, warehouse

SKIP = pytest.mark.skipif(
    not (Path(config.TRANSACTIONS_XLSX).exists() and Path(config.EXCHANGE_RATES_JSON).exists()),
    reason="source data not present",
)

FACT_KEYS = {"deposit": "Transaction ID", "withdrawal": "Transaction ID", "fee": "FeeId"}
FX_COLS = {"fx_instant_ms", "fx_rate_id", "fx_rate", "fx_quarantine_reason"}


@pytest.fixture()
def con(tmp_path):
    c = warehouse.connect(tmp_path / "wh.duckdb")
    warehouse.create_schemas(c)
    bronze.build_raw_monthly(c)
    bronze.build_live(c)
    silver_core.build_facts(c)
    yield c
    c.close()


@SKIP
def test_facts_are_pure_and_conserved(con):
    for name, source in silver_core._FACTS.items():
        n_fact = con.execute(f"SELECT COUNT(*) FROM core.{name}").fetchone()[0]
        n_src = con.execute(f"SELECT COUNT(*) FROM {source}").fetchone()[0]
        assert n_fact == n_src
        cols = {r[0] for r in con.execute(f"DESCRIBE core.{name}").fetchall()}
        assert not (cols & FX_COLS), f"core.{name} must not carry FX columns"


@SKIP
def test_fee_date_is_typed_not_excel_serial(con):
    # Bronze fees-typing fix: fee Date lands as a TIMESTAMP, not a serial string.
    dtype = con.execute("DESCRIBE core.fee").df().set_index("column_name").loc["Date", "column_type"]
    assert "TIMESTAMP" in dtype.upper()


@SKIP
def test_exchange_rate_dim(con):
    silver_core.build_exchange_rates(con)
    n = con.execute("SELECT COUNT(*) FROM core.exchange_rate").fetchone()[0]
    assert n == 161342  # meta.rowCount
    distinct = con.execute("SELECT COUNT(DISTINCT rate_id) FROM core.exchange_rate").fetchone()[0]
    assert distinct == n


@SKIP
def test_match_tables_one_to_one_and_lineage(con):
    silver_core.build_exchange_rates(con)
    reports = {r.stage: r for r in silver_core.build_fx_match(con)}

    for name in ("deposit", "withdrawal", "fee"):
        match = f"core.{name}_fx"
        key = FACT_KEYS[name]
        # Exactly one match row per fact.
        n_fact = con.execute(f"SELECT COUNT(*) FROM core.{name}").fetchone()[0]
        n_match = con.execute(f"SELECT COUNT(*) FROM {match}").fetchone()[0]
        assert n_match == n_fact
        # Match carries the fact key + FX columns; every row has rate xor reason.
        cols = {r[0] for r in con.execute(f"DESCRIBE {match}").fetchall()}
        assert key in cols and FX_COLS <= cols
        bad = con.execute(
            f"SELECT COUNT(*) FROM {match} "
            "WHERE (fx_rate IS NULL) = (fx_quarantine_reason IS NULL)"
        ).fetchone()[0]
        assert bad == 0

    # Lineage: every priced deposit match resolves to its rate point, and its
    # instant falls inside that point's validity window.
    bad = con.execute(
        """
        SELECT COUNT(*)
        FROM core.deposit_fx m
        JOIN core.exchange_rate r ON m.fx_rate_id = r.rate_id
        WHERE m.fx_rate <> r.rate
           OR m.fx_instant_ms < r.valid_from_ms
           OR m.fx_instant_ms >= r.valid_till_ms
        """
    ).fetchone()[0]
    assert bad == 0
    assert reports["silver.core.deposit_fx"].conserved


@SKIP
def test_gbp_match_has_identity_rate_and_no_rate_id(con):
    silver_core.build_exchange_rates(con)
    silver_core.build_fx_match(con)
    bad = con.execute(
        """
        SELECT COUNT(*) FROM core.deposit d
        JOIN core.deposit_fx m ON d."Transaction ID" = m."Transaction ID"
        WHERE d."Tx Currency" = 'GBP' AND (m.fx_rate <> 1.0 OR m.fx_rate_id IS NOT NULL)
        """
    ).fetchone()[0]
    assert bad == 0


@SKIP
def test_flagging_mechanism(con):
    con.execute(
        """
        CREATE OR REPLACE TABLE core.synth AS
        SELECT * FROM (VALUES
            ('T1', 'EUR', TIMESTAMP '2025-09-15 12:00:00'),
            ('T2', 'XYZ', TIMESTAMP '2025-09-15 12:00:00'),
            ('T3', 'EUR', TIMESTAMP '2019-01-01 00:00:00'),
            ('T4', NULL,  TIMESTAMP '2025-09-15 12:00:00')
        ) AS t(txid, ccy, ts)
        """
    )
    silver_core._match_fx(
        con, fx.FxRates.load(), "synth", "core.synth_fx",
        "txid", "ccy", "epoch_ms(ts)", "test.synth",
    )
    got = {r[0]: (r[1], r[2]) for r in con.execute(
        "SELECT txid, fx_rate, fx_quarantine_reason FROM core.synth_fx"
    ).fetchall()}
    assert got["T1"][0] is not None and got["T1"][1] is None
    assert got["T2"][1] == fx.REASON_UNKNOWN_CURRENCY
    assert got["T3"][1] == fx.REASON_OUT_OF_COVERAGE
    assert got["T4"][1] == silver_core.REASON_MISSING_CURRENCY
