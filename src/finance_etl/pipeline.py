from __future__ import annotations

from pathlib import Path

import pandas as pd

from .config import Settings
from .io_utils import read_csv, write_parquet, write_csv
from .quality import (
    validate_or_collect,
    sales_schema,
    expenses_schema,
    payroll_schema,
    inventory_schema,
    fx_schema,
    add_severity,
    dq_overall_status,
    dq_summary_table,
)
from .transform import build_dim_accounts, fx_to_base, to_fact_transactions, kpi_monthly


def _month_window(month: str) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Return [start, end) timestamps for a YYYY-MM month."""
    start = pd.to_datetime(f"{month}-01")
    end = start + pd.offsets.MonthBegin(1)
    return start, end

def _dq_account_in_coa(df: pd.DataFrame, dataset: str, coa_codes: set[str], issues: list[pd.DataFrame]) -> None:
    """
    Adds DQ exceptions for rows whose account_code is not in Chart of Accounts.
    """
    if df is None or df.empty or "account_code" not in df.columns:
        return

    bad_mask = ~df["account_code"].astype(str).isin(coa_codes)
    if bad_mask.any():
        bad = df.loc[bad_mask, ["account_code"]].copy()
        bad["dataset"] = dataset
        bad["index"] = bad.index
        bad["column"] = "account_code"
        bad["check"] = "account_in_coa"
        bad["failure_case"] = bad["account_code"]
        bad["schema_context"] = "Column"
        bad["check_number"] = None
        issues.append(
            bad[["dataset", "index", "column", "check", "failure_case", "schema_context", "check_number"]]
        )


def run_month(
    settings: Settings,
    month: str,
    raw_dir: Path,
    curated_dir: Path,
    reference_dir: Path,
    fail_on: str = "ERROR",  # ERROR | WARN | NEVER
) -> dict[str, Path]:
    # Normalize and validate fail_on early
    fail_on = (fail_on or "ERROR").upper().strip()
    if fail_on not in {"ERROR", "WARN", "NEVER"}:
        raise ValueError("fail_on must be one of: ERROR, WARN, NEVER")

    raw_dir = Path(raw_dir)
    curated_dir = Path(curated_dir)
    reference_dir = Path(reference_dir)
    curated_dir.mkdir(parents=True, exist_ok=True)

    # Reference (force codes to strings)
    coa = read_csv(
        reference_dir / "chart_of_accounts.csv",
        dtype={"account_code": str, "account_name": str, "account_type": str},
    )
    dim_accounts = build_dim_accounts(coa)
    
    coa_codes = set(dim_accounts["account_code"].astype(str).unique())

    

    # Raw (force IDs/codes to strings)
    sales = read_csv(
        raw_dir / "sales.csv",
        dtype={"entity": str, "invoice_id": str, "account_code": str, "currency": str},
        parse_dates=["date"],
    )
    expenses = read_csv(
        raw_dir / "expenses.csv",
        dtype={"entity": str, "bill_id": str, "account_code": str, "currency": str},
        parse_dates=["date"],
    )
    payroll = read_csv(
        raw_dir / "payroll.csv",
        dtype={"month": str, "entity": str, "employee_id": str, "currency": str},
    )
    inventory = read_csv(
        raw_dir / "inventory_movements.csv",
        dtype={"entity": str, "sku": str, "movement_type": str, "currency": str},
        parse_dates=["date"],
    )
    fx_rates = read_csv(
        raw_dir / "fx_rates.csv",
        dtype={"from_currency": str, "to_currency": str},
        parse_dates=["date"],
    )

    # Validate raw + collect DQ issues
    issues: list[pd.DataFrame] = []
    v_sales = validate_or_collect(sales, sales_schema(settings.allowed_currencies), "sales", issues)
    v_exp = validate_or_collect(expenses, expenses_schema(settings.allowed_currencies), "expenses", issues)
    v_pay = validate_or_collect(payroll, payroll_schema(settings.allowed_currencies), "payroll", issues)
    v_inv = validate_or_collect(inventory, inventory_schema(settings.allowed_currencies), "inventory_movements", issues)
    v_fx = validate_or_collect(
        fx_rates, fx_schema(settings.allowed_currencies, settings.base_currency), "fx_rates", issues
    )

    # IMPORTANT: Some validate_or_collect implementations return None when issues exist.
    # We still want to proceed when fail_on allows it, so fallback to original dataframes.
    if v_sales is None:
        v_sales = sales
    if v_exp is None:
        v_exp = expenses
    if v_pay is None:
        v_pay = payroll
    if v_inv is None:
        v_inv = inventory
    if v_fx is None:
        v_fx = fx_rates

    _dq_account_in_coa(v_sales, "sales", coa_codes, issues)
    _dq_account_in_coa(v_exp, "expenses", coa_codes, issues)


    dq_exceptions_path = curated_dir / "dq_exceptions.csv"
    dq_summary_path = curated_dir / "dq_summary.csv"

    if issues:
        dq_exceptions = pd.concat(issues, ignore_index=True)

        # Add severity + compute audit-ready summary
        dq_exceptions = add_severity(dq_exceptions)
        summary = dq_summary_table(dq_exceptions, fail_on=fail_on)
        overall = dq_overall_status(dq_exceptions, fail_on=fail_on)

        # Write audit trail
        write_csv(dq_exceptions, dq_exceptions_path)
        write_csv(summary, dq_summary_path)

        # Fail behavior controlled here
        if overall == "FAIL" and fail_on != "NEVER":
            raise ValueError(
                f"Data quality checks failed. See {dq_exceptions_path} and {dq_summary_path}"
            )
    else:
        # Write empty audit trail files with expected columns
        empty_ex = pd.DataFrame(
            columns=[
                "dataset",
                "index",
                "column",
                "check",
                "failure_case",
                "schema_context",
                "check_number",
                "severity",
            ]
        )
        write_csv(empty_ex, dq_exceptions_path)
        write_csv(dq_summary_table(empty_ex, fail_on=fail_on), dq_summary_path)

    # Filter to month window
    start, end = _month_window(month)

    v_sales = v_sales[(v_sales["date"] >= start) & (v_sales["date"] < end)].copy()
    v_exp = v_exp[(v_exp["date"] >= start) & (v_exp["date"] < end)].copy()
    v_inv = v_inv[(v_inv["date"] >= start) & (v_inv["date"] < end)].copy()
    v_pay = v_pay[v_pay["month"] == month].copy()

    fx = fx_to_base(v_fx, settings.base_currency)

    fact = to_fact_transactions(v_sales, v_exp, v_pay, v_inv, fx, settings.base_currency)
    kpi = kpi_monthly(fact, dim_accounts)

    out_fact = curated_dir / "fact_transactions.parquet"
    out_dim = curated_dir / "dim_accounts.parquet"
    out_kpi = curated_dir / "kpi_monthly.parquet"

    write_parquet(fact, out_fact)
    write_parquet(dim_accounts, out_dim)
    write_parquet(kpi, out_kpi)

    return {
        "dq_exceptions": dq_exceptions_path,
        "dq_summary": dq_summary_path,
        "fact": out_fact,
        "dim_accounts": out_dim,
        "kpi": out_kpi,
    }

