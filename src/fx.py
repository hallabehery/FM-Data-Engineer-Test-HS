"""FX as-of conversion — the pipeline's central modelling challenge, isolated.

Given a currency and an instant (epoch ms), resolve the GBP rate effective at
that instant, or return an explicit *quarantine reason* when no rate applies —
never a silently wrong number. This module is pure and independently testable:
the lookup is built from an in-memory series dict (`from_series`) so tests need
neither the file nor DuckDB; `load()` reads the real `exchange_rates.json` via
DuckDB with a raised `maximum_object_size`.

FX facts (from the file's `meta` and `BRIEF.md`):
- Base is GBP, direction `source_to_base`: rate converts FROM the currency TO GBP.
- Each point is a positional tuple ordered by `meta.tuple.fieldOrder`
  (`[rateId, validFromEpochMs, validTillEpochMs, rateStr, rateMantissaE10]`);
  match `validFrom <= t < validTill`. Points are NOT pre-sorted.
- `rateStr` is JSON-quoted; strip quotes before `float()`. `GBP = amount * rate`.
- `GBP` has no series — its rate is always `1.0`.
- Instants outside `meta.coverage`, an unknown currency, or an instant falling in
  a gap between a currency's points are quarantined with a reason.
"""
from __future__ import annotations

import bisect
import json
from dataclasses import dataclass

from . import config

# --- Quarantine reasons (stable strings so downstream can group on them) -----
GBP = "GBP"
REASON_OUT_OF_COVERAGE = "fx_out_of_coverage"
REASON_UNKNOWN_CURRENCY = "fx_unknown_currency"
REASON_GAP = "fx_no_rate_point"


@dataclass(frozen=True)
class RateResult:
    """Outcome of an as-of rate lookup: either a `rate` or a `quarantine_reason`."""

    currency: str
    instant_ms: int
    rate: float | None = None
    quarantine_reason: str | None = None

    @property
    def ok(self) -> bool:
        return self.rate is not None


@dataclass(frozen=True)
class ConversionResult:
    """Outcome of converting an amount to GBP: either `gbp` or a `quarantine_reason`."""

    amount: float
    currency: str
    instant_ms: int
    gbp: float | None = None
    rate: float | None = None
    quarantine_reason: str | None = None

    @property
    def ok(self) -> bool:
        return self.gbp is not None


def _parse_rate(raw) -> float:
    """Parse a `rateStr` value; tolerate JSON-quoting (e.g. `"0.8639"`)."""
    return float(str(raw).strip().strip('"'))


class FxRates:
    """As-of FX lookup for many currencies against a fixed coverage window."""

    def __init__(
        self,
        intervals: dict[str, list[tuple[int, int, float]]],
        coverage_from_ms: int,
        coverage_till_ms: int,
    ) -> None:
        # Per currency: intervals sorted by valid_from, plus the parallel list of
        # starts for bisect. Sorting here means callers never rely on input order.
        self._intervals: dict[str, list[tuple[int, int, float]]] = {}
        self._starts: dict[str, list[int]] = {}
        for ccy, ivs in intervals.items():
            ordered = sorted(ivs, key=lambda iv: iv[0])
            self._intervals[ccy] = ordered
            self._starts[ccy] = [iv[0] for iv in ordered]
        self.coverage_from_ms = coverage_from_ms
        self.coverage_till_ms = coverage_till_ms

    # -- construction ---------------------------------------------------------
    @classmethod
    def from_series(
        cls,
        series: dict,
        field_order: list[str],
        coverage_from_ms: int,
        coverage_till_ms: int,
    ) -> "FxRates":
        """Build from a `{ccy: {"points": [...] }}` dict and its tuple field order."""
        i_from = field_order.index("validFromEpochMs")
        i_till = field_order.index("validTillEpochMs")
        i_rate = field_order.index("rateStr")
        intervals: dict[str, list[tuple[int, int, float]]] = {}
        for ccy, obj in series.items():
            rows = []
            for point in obj["points"]:
                rows.append(
                    (int(point[i_from]), int(point[i_till]), _parse_rate(point[i_rate]))
                )
            intervals[ccy] = rows
        return cls(intervals, coverage_from_ms, coverage_till_ms)

    @classmethod
    def load(
        cls,
        path=config.EXCHANGE_RATES_JSON,
        max_object_size: int = config.FX_MAX_OBJECT_SIZE,
    ) -> "FxRates":
        """Load the real `exchange_rates.json` via DuckDB (raised `maximum_object_size`).

        `exchange_rates.json` (~18 MB) exceeds DuckDB's default 16 MB JSON object
        size, so the read must raise the limit. DuckDB reads the file; the nested
        series is handed back as a JSON string and parsed into the pure structure.
        """
        import duckdb

        rel = f"read_json_auto('{path}', maximum_object_size={max_object_size})"
        con = duckdb.connect()
        try:
            row = con.sql(
                f"""
                SELECT
                    to_json(meta.tuple.fieldOrder) AS field_order,
                    meta.coverage.fromEpochMs      AS cov_from,
                    meta.coverage.tillEpochMs      AS cov_till,
                    to_json(rates.series)          AS series_json
                FROM {rel}
                """
            ).fetchone()
        finally:
            con.close()
        field_order = json.loads(row[0])
        return cls.from_series(
            json.loads(row[3]), field_order, int(row[1]), int(row[2])
        )

    # -- lookup ---------------------------------------------------------------
    def rate_at(self, currency: str, instant_ms: int) -> RateResult:
        """Resolve the rate for `currency` at `instant_ms`, or a quarantine reason."""
        # GBP is the base — always 1.0, at any instant.
        if currency == GBP:
            return RateResult(currency, instant_ms, rate=1.0)

        if currency not in self._intervals:
            return RateResult(
                currency, instant_ms, quarantine_reason=REASON_UNKNOWN_CURRENCY
            )

        # Outside the file's coverage window → no trustworthy rate.
        if not (self.coverage_from_ms <= instant_ms < self.coverage_till_ms):
            return RateResult(
                currency, instant_ms, quarantine_reason=REASON_OUT_OF_COVERAGE
            )

        # Range-match validFrom <= t < validTill (points are pre-sorted here).
        starts = self._starts[currency]
        idx = bisect.bisect_right(starts, instant_ms) - 1
        if idx >= 0:
            valid_from, valid_till, rate = self._intervals[currency][idx]
            if valid_from <= instant_ms < valid_till:
                return RateResult(currency, instant_ms, rate=rate)

        # Within coverage but no point spans this instant → a gap in the series.
        return RateResult(currency, instant_ms, quarantine_reason=REASON_GAP)

    def to_gbp(self, amount: float, currency: str, instant_ms: int) -> ConversionResult:
        """Convert `amount` of `currency` at `instant_ms` to GBP, or quarantine."""
        result = self.rate_at(currency, instant_ms)
        if not result.ok:
            return ConversionResult(
                amount,
                currency,
                instant_ms,
                quarantine_reason=result.quarantine_reason,
            )
        return ConversionResult(
            amount,
            currency,
            instant_ms,
            gbp=amount * result.rate,
            rate=result.rate,
        )
