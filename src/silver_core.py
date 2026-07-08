"""Silver `core` — first-pass JSON unpicking.

The `core` schema does the heavy lifting: it flattens the nested JSON sources into
queryable columns. This module builds `core.companies` from `companies.json`.
Later `core` steps (groups unpick, FX attach) add their own builders here.

Heterogeneous / awkward fields (e.g. the scalar-or-object `attributes` array) are
carried forward as JSON for the Silver `shape` cleanup step, not resolved here.
"""
from __future__ import annotations

import duckdb

from . import config
from .reporting import StageReport, report_stage


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
