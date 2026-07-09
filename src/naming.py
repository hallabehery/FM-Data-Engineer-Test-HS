"""Canonical column-name mapping — raw source names → `lower_snake_case`.

Defined once here and reused by the Bronze landing so every table downstream carries
`snake_case` columns. Raw **values/types are preserved**; only names are conformed
(names standardised at landing, per the naming-cleanup decision).
"""
from __future__ import annotations

# Deposit / Withdrawals sheets (same column set, different order — aligned by name).
TRANSACTION_COLUMNS: dict[str, str] = {
    "Freemarket Entity": "freemarket_entity",
    "Transaction Type": "transaction_type",
    "Deal ID/DC ID": "dc_id",
    "Account ID": "account_id",
    "Transaction ID": "transaction_id",
    "Tx Date": "tx_date",
    "Tx Time": "tx_time",
    "Tx Currency": "tx_currency",
    "Tx Value (CCY)": "tx_value_ccy",
    "Counterparty": "counterparty_id",
    "Tx Month": "tx_month",
    "Scheme": "scheme",
}

FEE_COLUMNS: dict[str, str] = {
    "FeeId": "fee_id",
    "Type": "fee_type",
    "Date": "fee_date",
    "FeeDetail": "fee_detail",
    "Fee amount (CCY)": "fee_amount_ccy",
    "Fee currency": "fee_currency",
    "Link Id": "link_id",
}

COUNTERPARTY_COLUMNS: dict[str, str] = {
    "CP ID": "cp_id",
    "CP name": "cp_name",
    "CP vertical": "cp_vertical",
    "CP website": "cp_website",
    "CP business desc": "cp_business_desc",
    "Group ID": "group_id",
    "DC Id": "dc_id",
}


def aliased_select(rename: dict[str, str]) -> str:
    """Build a SELECT list that renames source columns to `snake_case`.

    e.g. `"Transaction ID" AS transaction_id, "Tx Date" AS tx_date, ...`.
    """
    return ", ".join(f'"{src}" AS {dst}' for src, dst in rename.items())
