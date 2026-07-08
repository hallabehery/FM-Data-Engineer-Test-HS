"""FX as-of conversion unit tests.

Covers the cases named in the ticket: exact boundary (from-inclusive,
till-exclusive), mid-interval, GBP=1.0, out-of-coverage, and unsorted points —
plus gaps, unknown currency, conversion arithmetic, and one integration test
against the real `exchange_rates.json`.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from src import config
from src.fx import (
    REASON_GAP,
    REASON_OUT_OF_COVERAGE,
    REASON_UNKNOWN_CURRENCY,
    FxRates,
)

FIELD_ORDER = ["rateId", "validFromEpochMs", "validTillEpochMs", "rateStr", "rateMantissaE10"]
COV_FROM, COV_TILL = 0, 1_000


def _point(valid_from: int, valid_till: int, rate: str):
    # rateStr is JSON-quoted in the source file; keep quotes to exercise stripping.
    return ["r", valid_from, valid_till, f'"{rate}"', 0]


def _rates(points_by_ccy: dict[str, list], cov_from=COV_FROM, cov_till=COV_TILL) -> FxRates:
    series = {ccy: {"points": pts} for ccy, pts in points_by_ccy.items()}
    return FxRates.from_series(series, FIELD_ORDER, cov_from, cov_till)


# --- the named cases ---------------------------------------------------------

def test_exact_boundary_from_inclusive_till_exclusive():
    fx = _rates({"EUR": [_point(100, 200, "1.0"), _point(200, 300, "2.0")]})
    # validFrom is inclusive
    assert fx.rate_at("EUR", 100).rate == 1.0
    # validTill is exclusive → 200 belongs to the next interval, not the first
    assert fx.rate_at("EUR", 199).rate == 1.0
    assert fx.rate_at("EUR", 200).rate == 2.0


def test_mid_interval():
    fx = _rates({"EUR": [_point(100, 200, "1.5")]})
    assert fx.rate_at("EUR", 150).rate == 1.5


def test_gbp_is_always_one():
    fx = _rates({"EUR": [_point(100, 200, "1.5")]})
    r = fx.rate_at("GBP", 10_000)  # even outside coverage, GBP is 1.0
    assert r.ok and r.rate == 1.0


def test_out_of_coverage_is_quarantined():
    fx = _rates({"EUR": [_point(100, 900, "1.5")]}, cov_from=100, cov_till=900)
    below = fx.rate_at("EUR", 50)
    at_till = fx.rate_at("EUR", 900)  # coverage till is exclusive
    assert not below.ok and below.quarantine_reason == REASON_OUT_OF_COVERAGE
    assert not at_till.ok and at_till.quarantine_reason == REASON_OUT_OF_COVERAGE


def test_unsorted_points_are_handled():
    # Points supplied out of order; the lookup must sort/range-match itself.
    fx = _rates(
        {"EUR": [_point(300, 400, "3.0"), _point(100, 200, "1.0"), _point(200, 300, "2.0")]}
    )
    assert fx.rate_at("EUR", 150).rate == 1.0
    assert fx.rate_at("EUR", 250).rate == 2.0
    assert fx.rate_at("EUR", 350).rate == 3.0


# --- additional robustness ---------------------------------------------------

def test_gap_within_coverage_is_quarantined():
    fx = _rates({"EUR": [_point(100, 200, "1.0"), _point(300, 400, "3.0")]})
    in_gap = fx.rate_at("EUR", 250)          # between two points
    before_first = fx.rate_at("EUR", 50)     # within coverage, before first point
    assert in_gap.quarantine_reason == REASON_GAP
    assert before_first.quarantine_reason == REASON_GAP


def test_unknown_currency_is_quarantined():
    fx = _rates({"EUR": [_point(100, 200, "1.0")]})
    r = fx.rate_at("XYZ", 150)
    assert not r.ok and r.quarantine_reason == REASON_UNKNOWN_CURRENCY


def test_to_gbp_applies_rate_and_quarantines():
    fx = _rates({"EUR": [_point(100, 200, "2.0")]})
    ok = fx.to_gbp(10.0, "EUR", 150)
    assert ok.ok and ok.gbp == 20.0 and ok.rate == 2.0

    bad = fx.to_gbp(10.0, "EUR", 5_000)
    assert not bad.ok and bad.gbp is None and bad.quarantine_reason == REASON_OUT_OF_COVERAGE


# --- integration against the real file ---------------------------------------

@pytest.mark.skipif(
    not Path(config.EXCHANGE_RATES_JSON).exists(),
    reason="exchange_rates.json not present",
)
def test_load_real_file_and_convert():
    fx = FxRates.load()
    # A mid-window 2025 instant (2025-09-15T00:00:00Z) should price EUR sanely.
    t = 1_757_894_400_000
    eur = fx.rate_at("EUR", t)
    assert eur.ok and 0.5 < eur.rate < 1.5, eur
    # Conversion arithmetic holds and GBP is the identity.
    assert fx.to_gbp(100.0, "EUR", t).gbp == pytest.approx(100.0 * eur.rate)
    assert fx.rate_at("GBP", t).rate == 1.0
    # An instant well before coverage is quarantined, not mis-priced.
    assert fx.rate_at("EUR", 0).quarantine_reason == REASON_OUT_OF_COVERAGE
