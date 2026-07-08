"""Silver core FX attach: every transaction/fee row carries an as-of rate or a quarantine reason."""
from __future__ import annotations

from pathlib import Path

import pytest

from src import bronze, config, fx, silver_core, warehouse

SKIP = pytest.mark.skipif(
    not (Path(config.TRANSACTIONS_XLSX).exists() and Path(config.EXCHANGE_RATES_JSON).exists()),
    reason="source data not present",
)


@pytest.fixture()
def con(tmp_path):
    c = warehouse.connect(tmp_path / "wh.duckdb")
    warehouse.create_schemas(c)
    bronze.build_raw_monthly(c)
    bronze.build_live(c)
    yield c
    c.close()


@SKIP
def test_rows_conserved_and_each_row_priced_or_flagged(con):
    reports = {r.stage: r for r in silver_core.build_fx_attached(con)}
    for stream, live in [("deposits", "live.deposits"),
                         ("withdrawals", "live.withdrawals"),
                         ("fees", "live.fees")]:
        core = f"core.{stream}"
        n_live = con.execute(f"SELECT COUNT(*) FROM {live}").fetchone()[0]
        n_core = con.execute(f"SELECT COUNT(*) FROM {core}").fetchone()[0]
        assert n_core == n_live  # no rows added or dropped
        # Every row has exactly one of (rate, quarantine reason).
        both_null = con.execute(
            f"SELECT COUNT(*) FROM {core} WHERE fx_rate IS NULL AND fx_quarantine_reason IS NULL"
        ).fetchone()[0]
        both_set = con.execute(
            f"SELECT COUNT(*) FROM {core} WHERE fx_rate IS NOT NULL AND fx_quarantine_reason IS NOT NULL"
        ).fetchone()[0]
        assert both_null == 0 and both_set == 0
    assert reports["silver.core.deposits_fx"].conserved


@SKIP
def test_exchange_rates_table_and_lineage(con):
    silver_core.build_exchange_rates(con)
    silver_core.build_fx_attached(con)

    # The rates table holds every point from the file (meta.rowCount = 161342).
    n_points = con.execute("SELECT COUNT(*) FROM core.exchange_rates").fetchone()[0]
    assert n_points == 161342
    # rate_id uniquely identifies a point.
    distinct_ids = con.execute(
        "SELECT COUNT(DISTINCT rate_id) FROM core.exchange_rates"
    ).fetchone()[0]
    assert distinct_ids == n_points

    # Lineage: every priced deposit's fx_rate_id resolves to a rate point, and the
    # attached rate + instant fall inside that point's validity window.
    bad = con.execute(
        """
        SELECT COUNT(*)
        FROM core.deposits d
        JOIN core.exchange_rates r ON d.fx_rate_id = r.rate_id
        WHERE d.fx_rate <> r.rate
           OR d.fx_instant_ms < r.valid_from_ms
           OR d.fx_instant_ms >= r.valid_till_ms
        """
    ).fetchone()[0]
    assert bad == 0
    # Every non-GBP priced row has a rate_id; GBP rows have rate 1.0 and no rate_id.
    orphan = con.execute(
        """
        SELECT COUNT(*) FROM core.deposits
        WHERE fx_rate IS NOT NULL AND "Tx Currency" <> 'GBP' AND fx_rate_id IS NULL
        """
    ).fetchone()[0]
    assert orphan == 0


@SKIP
def test_gbp_rows_get_identity_rate(con):
    silver_core.build_fx_attached(con)
    bad = con.execute(
        'SELECT COUNT(*) FROM core.deposits WHERE "Tx Currency" = \'GBP\' AND fx_rate <> 1.0'
    ).fetchone()[0]
    assert bad == 0


@SKIP
def test_attached_rate_matches_the_fx_unit(con):
    silver_core.build_fx_attached(con)
    rates = fx.FxRates.load()
    # Re-derive the rate for a sampled row straight from the unit and compare.
    row = con.execute(
        'SELECT "Tx Currency", fx_instant_ms, fx_rate FROM core.deposits '
        "WHERE fx_rate IS NOT NULL LIMIT 1"
    ).fetchone()
    ccy, instant, attached = row
    assert rates.rate_at(ccy, int(instant)).rate == attached


@SKIP
def test_flagging_mechanism_for_unpriceable_rows(con):
    # Craft a tiny source table exercising priced / unknown-currency / out-of-coverage.
    con.execute(
        """
        CREATE OR REPLACE TABLE live.synth AS
        SELECT * FROM (VALUES
            ('T1', 'EUR', TIMESTAMP '2025-09-15 12:00:00'),
            ('T2', 'XYZ', TIMESTAMP '2025-09-15 12:00:00'),
            ('T3', 'EUR', TIMESTAMP '2019-01-01 00:00:00'),
            ('T4', NULL,  TIMESTAMP '2025-09-15 12:00:00')
        ) AS t(txid, ccy, ts)
        """
    )
    silver_core._attach_fx(
        con, fx.FxRates.load(), "live.synth", "core.synth",
        "txid", "ccy", "epoch_ms(ts)", "test.synth",
    )
    got = {
        r[0]: (r[1], r[2])
        for r in con.execute(
            "SELECT txid, fx_rate, fx_quarantine_reason FROM core.synth"
        ).fetchall()
    }
    assert got["T1"][0] is not None and got["T1"][1] is None       # priced
    assert got["T2"][1] == fx.REASON_UNKNOWN_CURRENCY              # unknown currency
    assert got["T3"][1] == fx.REASON_OUT_OF_COVERAGE               # out of coverage
    assert got["T4"][1] == silver_core.REASON_MISSING_CURRENCY     # null currency
