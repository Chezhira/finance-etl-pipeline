from __future__ import annotations

import pandas as pd
import pandera.pandas as pa
from pandera import Column, Check

def _dup_check(keys: list[str], label: str) -> Check:
    return Check(
        lambda df: df.groupby(keys).size().max() == 1,
        element_wise=False,
        error=f"Duplicates found for keys {keys} in {label}",
    )

def sales_schema(allowed_currencies: tuple[str, ...]) -> pa.DataFrameSchema:
    return pa.DataFrameSchema(
        {
            "date": Column(pa.DateTime, coerce=True, nullable=False),
            "entity": Column(str, nullable=False),
            "invoice_id": Column(str, nullable=False),
            "account_code": Column(str, nullable=False),
            "currency": Column(str, checks=Check.isin(list(allowed_currencies)), nullable=False),
            "amount": Column(float, checks=Check.gt(0), coerce=True, nullable=False),
            "description": Column(str, nullable=True),
        },
        checks=[_dup_check(["entity", "invoice_id"], "sales")],
        strict=True,
    )

def expenses_schema(allowed_currencies: tuple[str, ...]) -> pa.DataFrameSchema:
    return pa.DataFrameSchema(
        {
            "date": Column(pa.DateTime, coerce=True, nullable=False),
            "entity": Column(str, nullable=False),
            "bill_id": Column(str, nullable=False),
            "account_code": Column(str, nullable=False),
            "currency": Column(str, checks=Check.isin(list(allowed_currencies)), nullable=False),
            "amount": Column(float, checks=Check.gt(0), coerce=True, nullable=False),
            "description": Column(str, nullable=True),
        },
        checks=[_dup_check(["entity", "bill_id"], "expenses")],
        strict=True,
    )

def payroll_schema(allowed_currencies: tuple[str, ...]) -> pa.DataFrameSchema:
    return pa.DataFrameSchema(
        {
            "month": Column(str, nullable=False),
            "entity": Column(str, nullable=False),
            "employee_id": Column(str, nullable=False),
            "currency": Column(str, checks=Check.isin(list(allowed_currencies)), nullable=False),
            "gross": Column(float, checks=Check.ge(0), coerce=True, nullable=False),
            "deductions": Column(float, checks=Check.ge(0), coerce=True, nullable=False),
            "net": Column(float, checks=Check.ge(0), coerce=True, nullable=False),
        },
        checks=[
            Check(
                lambda df: (df["gross"] - df["deductions"] - df["net"]).abs().max() < 0.01,
                element_wise=False,
                error="Payroll identity gross - deductions = net violated",
            ),
        ],
        strict=True,
    )

def inventory_schema(allowed_currencies: tuple[str, ...]) -> pa.DataFrameSchema:
    return pa.DataFrameSchema(
        {
            "date": Column(pa.DateTime, coerce=True, nullable=False),
            "entity": Column(str, nullable=False),
            "sku": Column(str, nullable=False),
            "movement_type": Column(str, checks=Check.isin(["receipt", "issue", "adjustment"]), nullable=False),
            "qty": Column(float, checks=Check.ne(0), coerce=True, nullable=False),
            "unit_cost": Column(float, checks=Check.ge(0), coerce=True, nullable=False),
            "currency": Column(str, checks=Check.isin(list(allowed_currencies)), nullable=False),
        },
        strict=True,
    )

def fx_schema(allowed_currencies: tuple[str, ...], base_currency: str) -> pa.DataFrameSchema:
    return pa.DataFrameSchema(
        {
            "date": Column(pa.DateTime, coerce=True, nullable=False),
            "from_currency": Column(str, checks=Check.isin(list(allowed_currencies)), nullable=False),
            "to_currency": Column(str, checks=Check.isin([base_currency]), nullable=False),
            "rate": Column(float, checks=Check.gt(0), coerce=True, nullable=False),
        },
        checks=[_dup_check(["date", "from_currency", "to_currency"], "fx_rates")],
        strict=True,
    )

def validate_or_collect(
    df: pd.DataFrame,
    schema: pa.DataFrameSchema,
    dataset_name: str,
    issues: list[pd.DataFrame],
) -> pd.DataFrame | None:
    try:
        return schema.validate(df, lazy=True)
    except pa.errors.SchemaErrors as e:
        fc = e.failure_cases.copy()
        if "index" not in fc.columns and "row" in fc.columns:
            fc["index"] = fc["row"]
        fc["dataset"] = dataset_name
        keep = [c for c in ["dataset", "index", "column", "check", "failure_case", "schema_context"] if c in fc.columns]
        rest = [c for c in fc.columns if c not in keep]
        fc = fc[keep + rest]
        issues.append(fc)
        return None
# --- DQ severity + summary helpers (audit-ready) ---
import pandas as pd

DATASETS = ["sales", "expenses", "payroll", "inventory_movements", "fx_rates"]

def add_severity(dq_exceptions: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a severity column to DQ exceptions:
    - ERROR: key fields, type/required checks, account mapping, FX dataset issues
    - WARN: non-critical issues (extend later)
    """
    if dq_exceptions is None:
        return pd.DataFrame(
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

    dq = dq_exceptions.copy()

    if dq.empty:
        if "severity" not in dq.columns:
            dq["severity"] = pd.Series(dtype="string")
        return dq

    dq["severity"] = "WARN"

    # Column-based criticals
    error_cols = {
        "account_code",
        "date",
        "invoice_id",
        "bill_id",
        "employee_id",
        "sku",
        "currency",
        "from_currency",
        "to_currency",
        "rate",
    }
    if "column" in dq.columns:
        dq.loc[dq["column"].isin(error_cols), "severity"] = "ERROR"

    # Any FX dataset issues are ERROR
    if "dataset" in dq.columns:
        dq.loc[dq["dataset"].eq("fx_rates"), "severity"] = "ERROR"

    # Any schema "required"/"dtype" checks are ERROR
    if "check" in dq.columns:
        check_str = dq["check"].astype(str)
        dq.loc[check_str.str.contains("required", case=False, na=False), "severity"] = "ERROR"
        dq.loc[check_str.str.contains("dtype", case=False, na=False), "severity"] = "ERROR"

    # COA membership violations -> ERROR
    # (we will add these checks in pipeline using check name "account_in_coa")
    if "check" in dq.columns:
        dq.loc[dq["check"].astype(str).str.contains("account_in_coa", case=False, na=False), "severity"] = "ERROR"

    return dq


def dq_overall_status(dq_exceptions: pd.DataFrame, fail_on: str = "ERROR") -> str:
    fail_on = (fail_on or "ERROR").upper()

    if dq_exceptions is None or dq_exceptions.empty:
        return "PASS"

    if fail_on == "NEVER":
        return "PASS"

    if fail_on == "WARN":
        return "FAIL"

    # ERROR mode: fail only if any ERROR exists
    if "severity" not in dq_exceptions.columns:
        return "FAIL"  # safest fallback

    return "FAIL" if (dq_exceptions["severity"] == "ERROR").any() else "PASS"


def dq_summary_table(dq_exceptions: pd.DataFrame, fail_on: str = "ERROR") -> pd.DataFrame:
    fail_on = (fail_on or "ERROR").upper()

    # start with all datasets, zero counts
    base = pd.DataFrame({"dataset": DATASETS, "error_count": 0, "warn_count": 0})

    if dq_exceptions is None or dq_exceptions.empty:
        base["issue_count"] = 0
        base["status"] = "PASS"
        return base[["dataset", "error_count", "warn_count", "issue_count", "status"]]

    dq = dq_exceptions.copy()
    if "severity" not in dq.columns:
        dq["severity"] = "ERROR"

    counts = (
        dq.groupby(["dataset", "severity"])
          .size()
          .unstack(fill_value=0)
          .reset_index()
    )

    # Normalize column names
    if "ERROR" not in counts.columns:
        counts["ERROR"] = 0
    if "WARN" not in counts.columns:
        counts["WARN"] = 0

    counts = counts.rename(columns={"ERROR": "error_count", "WARN": "warn_count"})

    out = base.merge(counts, on="dataset", how="left", suffixes=("", "_y"))
    # if merge created *_y cols, use them
    for c in ["error_count", "warn_count"]:
        y = f"{c}_y"
        if y in out.columns:
            out[c] = out[y].fillna(out[c]).astype(int)
            out = out.drop(columns=[y])
        else:
            out[c] = out[c].fillna(0).astype(int)

    out["issue_count"] = out["error_count"] + out["warn_count"]

    if fail_on == "NEVER":
        out["status"] = "PASS"
    elif fail_on == "WARN":
        out["status"] = out["issue_count"].apply(lambda x: "FAIL" if x > 0 else "PASS")
    else:  # ERROR
        out["status"] = out["error_count"].apply(lambda x: "FAIL" if x > 0 else "PASS")

    return out[["dataset", "error_count", "warn_count", "issue_count", "status"]]

