"""Silver `core` — first-pass JSON unpicking.

The `core` schema does the heavy lifting: it flattens the nested JSON sources into
queryable columns. This module builds `core.companies` from `companies.json`.
Later `core` steps (groups unpick, FX attach) add their own builders here.

Heterogeneous / awkward fields (e.g. the scalar-or-object `attributes` array) are
carried forward as JSON for the Silver `shape` cleanup step, not resolved here.
"""
from __future__ import annotations

import duckdb
import pandas as pd

from . import config, fx
from .reporting import StageReport, logger, report_stage

# Quarantine reasons for rows the FX unit can't even be consulted for (the currency
# or the instant is missing on the row). Coverage/gap/unknown reasons come from `fx`.
REASON_MISSING_CURRENCY = "fx_missing_currency"
REASON_MISSING_INSTANT = "fx_missing_instant"


def build_companies(con: duckdb.DuckDBPyConnection) -> StageReport:
    """Unpick `companies.json` into `core.companies` (one row per `dcId`).

    Exposes the nested registration / classification / financials / footprint
    fields as columns and surfaces `relationships.parentGroup.value` as the group
    bridge key. Idempotent (`CREATE OR REPLACE`).
    """
    src = config.COMPANIES_JSON

    # Validate the expected top-level shape before proceeding (fail loud, not silent).
    columns = {
        row[0]
        for row in con.execute(
            f"DESCRIBE SELECT * FROM read_json_auto('{src}')"
        ).fetchall()
    }
    missing = {"records", "metadata"} - columns
    if missing:
        raise ValueError(f"companies.json missing expected top-level keys: {missing}")

    con.execute(
        f"""
        CREATE OR REPLACE TABLE core.companies AS
        SELECT
            r.dcId                                AS dc_id,
            r.registration.legalName              AS legal_name,
            r.registration.status                 AS status,
            r.registration.incorporation.country  AS incorporation_country,
            r.registration.incorporation.iso2     AS incorporation_iso2,
            r.registration.incorporatedOn         AS incorporated_on,
            r.registration.createdEpochMs         AS created_epoch_ms,
            r.classification.vertical             AS vertical,
            r.classification.industry             AS industry,
            r.regulatory.licensed                 AS licensed,
            r.workforce.headcount                 AS headcount,
            r.workforce.headcountBasis            AS headcount_basis,
            r.financials.currency                 AS financials_currency,
            r.financials.annualRevenue            AS annual_revenue,
            r.financials.totalAssets              AS total_assets,
            r.footprint.operationCount            AS operation_count,
            r.footprint.countriesOfOperation      AS countries_of_operation,
            r.relationships.parentGroup.value     AS parent_group_id,
            r.relationships.parentGroup.role      AS parent_group_role,
            to_json(r.attributes)                 AS attributes_json
        FROM (SELECT unnest(records) AS r FROM read_json_auto('{src}'))
        """
    )

    rows_in = con.execute(
        f"SELECT len(records) FROM read_json_auto('{src}')"
    ).fetchone()[0]
    rows_out = con.execute("SELECT COUNT(*) FROM core.companies").fetchone()[0]
    return report_stage(
        "silver.core.companies", rows_in=rows_in, rows_out=rows_out, kept=rows_out
    )


def build_groups(con: duckdb.DuckDBPyConnection) -> StageReport:
    """Unpick `groups.json` into `core.groups` (one row per `groupId`).

    Groups live under `result.groups`. Exposes the profile / segmentation /
    lifecycle fields as columns. The heterogeneous `attributes` array (whose
    `value` is sometimes a scalar and sometimes an object) is carried forward as
    JSON — DuckDB types it as `JSON`, so ingestion does not break on the shape
    difference; the Silver `shape` step resolves it. Idempotent.
    """
    src = config.GROUPS_JSON

    # Validate the expected top-level shape before proceeding (fail loud, not silent).
    columns = {
        row[0]
        for row in con.execute(
            f"DESCRIBE SELECT * FROM read_json_auto('{src}')"
        ).fetchall()
    }
    missing = {"result", "export"} - columns
    if missing:
        raise ValueError(f"groups.json missing expected top-level keys: {missing}")

    con.execute(
        f"""
        CREATE OR REPLACE TABLE core.groups AS
        SELECT
            g.groupId                            AS group_id,
            g.profile.displayName                AS display_name,
            g.profile.description                AS description,
            g.profile.lifecycle.status.code      AS status_code,
            g.profile.lifecycle.status.active    AS active,
            g.profile.lifecycle.createdOn        AS created_on,
            g.segmentation.pod                   AS pod,
            g.segmentation.vertical              AS vertical,
            g.segmentation.industry              AS industry,
            g.segmentation.commercialTier        AS commercial_tier,
            to_json(g.attributes)                AS attributes_json
        FROM (SELECT unnest(result.groups) AS g FROM read_json_auto('{src}'))
        """
    )

    rows_in = con.execute(
        f"SELECT len(result.groups) FROM read_json_auto('{src}')"
    ).fetchone()[0]
    rows_out = con.execute("SELECT COUNT(*) FROM core.groups").fetchone()[0]
    return report_stage(
        "silver.core.groups", rows_in=rows_in, rows_out=rows_out, kept=rows_out
    )


# The settlement instant used for the as-of FX match, per source. The transactional
# sheets carry a single timestamp (Tx Date + Tx Time), which we take as the settlement
# instant. Fees carry only a Date, so their instant is that date at midnight (UTC).
_TXN_INSTANT_SQL = 'epoch_ms(CAST("Tx Date" AS DATE) + "Tx Time")'
_FEE_INSTANT_SQL = 'epoch_ms("Date")'


def build_exchange_rates(con: duckdb.DuckDBPyConnection) -> StageReport:
    """Land the FX rate points as `core.exchange_rates` (one row per rate interval).

    Persists the same rate data the FX unit uses, so a transaction's `fx_rate_id`
    can be joined back to the exact interval (currency, valid window, rate) that
    priced it — full FX lineage. Idempotent.
    """
    rates = fx.FxRates.load()
    points = pd.DataFrame(rates.points())
    points["valid_from"] = pd.to_datetime(points["valid_from_ms"], unit="ms", utc=True)
    points["valid_till"] = pd.to_datetime(points["valid_till_ms"], unit="ms", utc=True)

    con.register("_fx_points", points)
    try:
        con.execute(
            """
            CREATE OR REPLACE TABLE core.exchange_rates AS
            SELECT currency, rate_id, valid_from_ms, valid_till_ms,
                   valid_from, valid_till, rate
            FROM _fx_points
            """
        )
    finally:
        con.unregister("_fx_points")

    n = con.execute("SELECT COUNT(*) FROM core.exchange_rates").fetchone()[0]
    return report_stage("silver.core.exchange_rates", rows_in=n, rows_out=n, kept=n)


def _attach_fx(
    con: duckdb.DuckDBPyConnection,
    rates: fx.FxRates,
    source: str,
    target: str,
    key_col: str,
    ccy_col: str,
    instant_sql: str,
    stage: str,
) -> StageReport:
    """Attach the as-of FX rate to every row of `source`, landing `target`.

    Uses the tested FX unit (`fx.FxRates`) as the single source of truth for the
    lookup. Only a small (key, currency, instant) map round-trips through pandas;
    the source columns keep their types via a keyed join. Rates are *attached*,
    not applied — GBP is computed in Silver `shape`. Rows with no rate carry a
    `fx_quarantine_reason` and are kept (not dropped here).
    """
    lookup = con.sql(
        f'SELECT {key_col} AS _k, {ccy_col} AS _ccy, {instant_sql} AS _instant FROM {source}'
    ).df()

    fx_rate: list[float | None] = []
    rate_id: list[object | None] = []
    reason: list[str | None] = []
    for currency, instant in zip(lookup["_ccy"], lookup["_instant"]):
        if pd.isna(currency):
            fx_rate.append(None)
            rate_id.append(None)
            reason.append(REASON_MISSING_CURRENCY)
            continue
        if pd.isna(instant):
            fx_rate.append(None)
            rate_id.append(None)
            reason.append(REASON_MISSING_INSTANT)
            continue
        result = rates.rate_at(str(currency), int(instant))
        fx_rate.append(result.rate)
        rate_id.append(result.rate_id)
        reason.append(result.quarantine_reason)

    lookup["fx_rate"] = fx_rate
    # Nullable integer so it joins cleanly to core.exchange_rates.rate_id (GBP and
    # quarantined rows have no rate point → NULL).
    lookup["fx_rate_id"] = pd.array(rate_id, dtype="Int64")
    lookup["fx_quarantine_reason"] = reason
    fx_map = lookup.rename(columns={"_instant": "fx_instant_ms"})[
        ["_k", "fx_instant_ms", "fx_rate_id", "fx_rate", "fx_quarantine_reason"]
    ]

    con.register("_fx_map", fx_map)
    try:
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {target} AS
            SELECT s.*, f.fx_instant_ms, f.fx_rate_id, f.fx_rate, f.fx_quarantine_reason
            FROM {source} s
            JOIN _fx_map f ON s.{key_col} = f._k
            """
        )
    finally:
        con.unregister("_fx_map")

    n_in = con.execute(f"SELECT COUNT(*) FROM {source}").fetchone()[0]
    n_out = con.execute(f"SELECT COUNT(*) FROM {target}").fetchone()[0]
    flagged = con.execute(
        f"SELECT COUNT(*) FROM {target} WHERE fx_quarantine_reason IS NOT NULL"
    ).fetchone()[0]
    if n_in != n_out:
        raise ValueError(
            f"{target}: attach changed row count ({n_in} -> {n_out}); key {key_col!r} "
            "may be non-unique or null (join fan-out/loss)"
        )
    report = report_stage(stage, rows_in=n_in, rows_out=n_out, kept=n_out)
    if flagged:
        # Flagged rows are KEPT here (not dropped); Silver `shape` handles them.
        logger.info(f"[{stage}] flagged for FX quarantine (kept, not dropped): {flagged}")
    return report


def build_fx_attached(con: duckdb.DuckDBPyConnection) -> list[StageReport]:
    """Attach as-of FX rates to deposits, withdrawals, and fees. Idempotent."""
    rates = fx.FxRates.load()
    return [
        _attach_fx(con, rates, "live.deposits", "core.deposits",
                   '"Transaction ID"', '"Tx Currency"', _TXN_INSTANT_SQL,
                   "silver.core.deposits_fx"),
        _attach_fx(con, rates, "live.withdrawals", "core.withdrawals",
                   '"Transaction ID"', '"Tx Currency"', _TXN_INSTANT_SQL,
                   "silver.core.withdrawals_fx"),
        _attach_fx(con, rates, "live.fees", "core.fees",
                   '"FeeId"', '"Fee currency"', _FEE_INSTANT_SQL,
                   "silver.core.fees_fx"),
    ]
