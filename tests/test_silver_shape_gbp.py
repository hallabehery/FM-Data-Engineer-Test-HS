"""Silver shape: FX applied → GBP-normalised facts, with quarantine of unpriceable rows."""
from __future__ import annotations

from pathlib import Path

import pytest

from src import bronze, config, silver_core, silver_shape, warehouse

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
    silver_core.build_exchange_rates(c)
    silver_core.build_fx_match(c)
    yield c
    c.close()


@SKIP
def test_conservation_promoted_plus_quarantined_equals_input(con):
    reports = {r.stage: r for r in silver_shape.build_gbp_facts(con)}
    for stream in ("deposit", "withdrawal", "fee"):
        n_in = con.execute(f"SELECT COUNT(*) FROM live.{stream}").fetchone()[0]
        n_prom = con.execute(f"SELECT COUNT(*) FROM shape.{stream}").fetchone()[0]
        n_quar = con.execute(f"SELECT COUNT(*) FROM shape.{stream}_quarantine").fetchone()[0]
        assert n_prom + n_quar == n_in
        assert reports[f"silver.shape.{stream}"].conserved


@SKIP
def test_fee_quarantine_is_the_42_null_currency_rows(con):
    silver_shape.build_gbp_facts(con)
    # deposits/withdrawals fully priced; the 42 null-currency fees are quarantined.
    assert con.execute("SELECT COUNT(*) FROM shape.deposit_quarantine").fetchone()[0] == 0
    assert con.execute("SELECT COUNT(*) FROM shape.withdrawal_quarantine").fetchone()[0] == 0
    fee_q = con.execute("SELECT COUNT(*) FROM shape.fee_quarantine").fetchone()[0]
    assert fee_q == 42
    reasons = {
        r[0] for r in con.execute(
            "SELECT DISTINCT quarantine_reason FROM shape.fee_quarantine"
        ).fetchall()
    }
    assert reasons == {"fx_missing_currency"}


@SKIP
def test_gbp_amount_is_native_times_rate(con):
    silver_shape.build_gbp_facts(con)
    # Every promoted row has a gbp_amount = native × rate (within float tolerance).
    bad = con.execute(
        "SELECT COUNT(*) FROM shape.deposit "
        "WHERE abs(gbp_amount - tx_value_ccy * fx_rate) > 1e-6"
    ).fetchone()[0]
    assert bad == 0
    # No promoted row is unpriced.
    assert con.execute("SELECT COUNT(*) FROM shape.deposit WHERE gbp_amount IS NULL").fetchone()[0] == 0


@SKIP
def test_gbp_currency_rows_are_identity(con):
    silver_shape.build_gbp_facts(con)
    bad = con.execute(
        "SELECT COUNT(*) FROM shape.deposit "
        "WHERE tx_currency = 'GBP' AND abs(gbp_amount - tx_value_ccy) > 1e-6"
    ).fetchone()[0]
    assert bad == 0


@SKIP
def test_promoted_carries_lineage_and_quarantine_does_not_price(con):
    silver_shape.build_gbp_facts(con)
    prom_cols = {r[0] for r in con.execute("DESCRIBE shape.fee").fetchall()}
    quar_cols = {r[0] for r in con.execute("DESCRIBE shape.fee_quarantine").fetchall()}
    assert {"fx_rate", "fx_rate_id", "gbp_amount"} <= prom_cols
    assert "gbp_amount" not in quar_cols
    assert "quarantine_reason" in quar_cols


@SKIP
def test_validate_facts_rejects_a_duplicate_business_key(con):
    # Inject a duplicate transaction_id into a stream; validation must fail loud.
    con.execute("INSERT INTO live.deposit SELECT * FROM live.deposit LIMIT 1")
    with pytest.raises(ValueError, match="duplicate"):
        silver_shape.validate_facts(con)
    # And it's wired into the build, so building the GBP facts fails loud too.
    with pytest.raises(ValueError, match="duplicate"):
        silver_shape.build_gbp_facts(con)


@SKIP
def test_null_native_amount_is_quarantined_not_priced(con):
    # A NULL native amount can't be priced (amount × rate would be a silent NULL) → quarantine.
    con.execute(
        "UPDATE live.deposit SET tx_value_ccy = NULL "
        "WHERE transaction_id = (SELECT transaction_id FROM live.deposit LIMIT 1)"
    )
    silver_shape.build_gbp_facts(con)
    q = con.execute(
        "SELECT COUNT(*) FROM shape.deposit_quarantine WHERE quarantine_reason = 'amount_missing'"
    ).fetchone()[0]
    assert q == 1
    # It did not slip into the priced table with a NULL gbp_amount.
    assert con.execute(
        "SELECT COUNT(*) FROM shape.deposit WHERE gbp_amount IS NULL"
    ).fetchone()[0] == 0
