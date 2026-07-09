"""Silver `core` — first-pass JSON unpicking, clean facts, and the FX as-of match.

The `core` schema does the heavy lifting: it flattens the nested JSON sources into
queryable columns (`core.company`, `core.corporate_group`), lands the clean transaction/fee
**facts** (`core.deposit`/`withdrawal`/`fee`, kept pure — no FX, no GBP), persists the
FX rate points as a dimension (`core.exchange_rate`), and resolves the FX as-of match
into separate per-stream tables (`core.deposit_fx`/`withdrawal_fx`/`fee_fx`).

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
    """Unpick `companies.json` into `core.company` (one row per `dcId`).

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
        CREATE OR REPLACE TABLE core.company AS
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
    rows_out = con.execute("SELECT COUNT(*) FROM core.company").fetchone()[0]
    return report_stage(
        "silver.core.company", rows_in=rows_in, rows_out=rows_out, kept=rows_out
    )


def build_groups(con: duckdb.DuckDBPyConnection) -> StageReport:
    """Unpick `groups.json` into `core.corporate_group` (one row per `groupId`).

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
        CREATE OR REPLACE TABLE core.corporate_group AS
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
    rows_out = con.execute("SELECT COUNT(*) FROM core.corporate_group").fetchone()[0]
    return report_stage(
        "silver.core.corporate_group", rows_in=rows_in, rows_out=rows_out, kept=rows_out
    )


# --- Clean facts (pure) ------------------------------------------------------
# core facts are the conformed transaction/fee tables the rest of Silver/Gold read.
# They carry only their own columns — no FX, no GBP. `live.fee` is typed (Bronze
# lands Fees with inference), so these are typed pass-throughs of the `live` tables.
_FACTS = {"deposit": "live.deposit", "withdrawal": "live.withdrawal", "fee": "live.fee"}


def build_facts(con: duckdb.DuckDBPyConnection) -> list[StageReport]:
    """Land the clean, pure transaction/fee facts in `core`. Idempotent."""
    reports = []
    for name, source in _FACTS.items():
        con.execute(f"CREATE OR REPLACE TABLE core.{name} AS SELECT * FROM {source}")
        n_in = con.execute(f"SELECT COUNT(*) FROM {source}").fetchone()[0]
        n_out = con.execute(f"SELECT COUNT(*) FROM core.{name}").fetchone()[0]
        if n_in != n_out:
            raise ValueError(f"core.{name}: {n_in} source rows but {n_out} landed")
        reports.append(
            report_stage(f"silver.core.{name}", rows_in=n_in, rows_out=n_out, kept=n_out)
        )
    return reports


# --- exchange_rate dimension -------------------------------------------------
def build_exchange_rates(
    con: duckdb.DuckDBPyConnection, rates: fx.FxRates | None = None
) -> StageReport:
    """Land the FX rate points as `core.exchange_rate` (one row per rate interval).

    Persists the same rate data the FX unit uses, so a fact's `fx_rate_id` can be
    joined back to the exact interval (currency, valid window, rate) that priced
    it — full FX lineage. Idempotent. Accepts a pre-loaded `FxRates` to avoid
    re-parsing the ~18 MB file (the pipeline loads it once).
    """
    if rates is None:
        rates = fx.FxRates.load()
    points = pd.DataFrame(rates.points())
    points["valid_from"] = pd.to_datetime(points["valid_from_ms"], unit="ms", utc=True)
    points["valid_till"] = pd.to_datetime(points["valid_till_ms"], unit="ms", utc=True)

    con.register("_fx_points", points)
    try:
        con.execute(
            """
            CREATE OR REPLACE TABLE core.exchange_rate AS
            SELECT currency, rate_id, valid_from_ms, valid_till_ms,
                   valid_from, valid_till, rate
            FROM _fx_points
            """
        )
    finally:
        con.unregister("_fx_points")

    n = con.execute("SELECT COUNT(*) FROM core.exchange_rate").fetchone()[0]
    return report_stage("silver.core.exchange_rate", rows_in=n, rows_out=n, kept=n)


# --- FX as-of match (bridge; facts stay pure) --------------------------------
# The settlement instant used for the as-of match: transactions carry a single
# timestamp (Tx Date + Tx Time); fees carry only a Date, taken at midnight (UTC).
_TXN_INSTANT_SQL = "epoch_ms(CAST(tx_date AS DATE) + tx_time)"
_FEE_INSTANT_SQL = "epoch_ms(fee_date)"


def _match_fx(
    con: duckdb.DuckDBPyConnection,
    rates: fx.FxRates,
    fact: str,
    match_table: str,
    key_col: str,
    ccy_col: str,
    instant_sql: str,
    stage: str,
) -> StageReport:
    """Resolve the as-of FX match for `fact` into `match_table` (one row per fact).

    Keeps the fact pure: the match lives in its own table, keyed to the fact by
    `key_col`. Uses the tested FX unit as the single source of truth. Rows with no
    rate carry a `fx_quarantine_reason` (kept, not dropped — Silver `shape` handles).
    """
    lookup = con.sql(
        f'SELECT {key_col} AS _k, {ccy_col} AS _ccy, {instant_sql} AS _instant FROM core.{fact}'
    ).df()

    fx_rate: list[float | None] = []
    rate_id: list[int | None] = []
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

    out = lookup.rename(columns={"_k": key_col, "_instant": "fx_instant_ms"})
    out = out.drop(columns=["_ccy"])
    out["fx_rate_id"] = pd.array(rate_id, dtype="Int64")
    out["fx_rate"] = fx_rate
    out["fx_quarantine_reason"] = reason

    con.register("_fx_out", out)
    try:
        con.execute(f"CREATE OR REPLACE TABLE {match_table} AS SELECT * FROM _fx_out")
    finally:
        con.unregister("_fx_out")

    n_fact = con.execute(f"SELECT COUNT(*) FROM core.{fact}").fetchone()[0]
    n_match = con.execute(f"SELECT COUNT(*) FROM {match_table}").fetchone()[0]
    if n_match != n_fact:
        raise ValueError(f"{match_table}: {n_match} match rows for {n_fact} facts (expected 1:1)")
    flagged = con.execute(
        f"SELECT COUNT(*) FROM {match_table} WHERE fx_quarantine_reason IS NOT NULL"
    ).fetchone()[0]
    report = report_stage(stage, rows_in=n_fact, rows_out=n_match, kept=n_match)
    if flagged:
        logger.info(f"[{stage}] flagged for FX quarantine (kept, not dropped): {flagged}")
    return report


def build_fx_match(
    con: duckdb.DuckDBPyConnection, rates: fx.FxRates | None = None
) -> list[StageReport]:
    """Resolve the FX as-of match into per-stream `core.*_fx` tables. Idempotent.

    Accepts a pre-loaded `FxRates` to avoid re-parsing the file (pipeline shares one).
    """
    if rates is None:
        rates = fx.FxRates.load()
    return [
        _match_fx(con, rates, "deposit", "core.deposit_fx",
                  "transaction_id", "tx_currency", _TXN_INSTANT_SQL, "silver.core.deposit_fx"),
        _match_fx(con, rates, "withdrawal", "core.withdrawal_fx",
                  "transaction_id", "tx_currency", _TXN_INSTANT_SQL, "silver.core.withdrawal_fx"),
        _match_fx(con, rates, "fee", "core.fee_fx",
                  "fee_id", "fee_currency", _FEE_INSTANT_SQL, "silver.core.fee_fx"),
    ]
