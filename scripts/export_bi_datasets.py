from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd


KPI_COLS = ["entity", "month", "Asset", "COGS", "Expense", "Revenue", "gross_profit", "operating_profit"]


def _read_parquet(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path) if path.exists() else pd.DataFrame()


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path) if path.exists() else pd.DataFrame()


def _month_from_any(x) -> str:
    if pd.isna(x):
        return ""
    if isinstance(x, str):
        return x[:7]
    try:
        return pd.to_datetime(x).strftime("%Y-%m")
    except Exception:
        return str(x)[:7]


def _infer_month(kpi: pd.DataFrame) -> str | None:
    if kpi.empty or "month" not in kpi.columns:
        return None
    months = sorted({_month_from_any(m) for m in kpi["month"].dropna().unique()})
    return months[-1] if months else None


def _filter_fact_to_month(fact: pd.DataFrame, month: str) -> pd.DataFrame:
    for col in ["tx_date", "date", "transaction_date", "posting_date", "invoice_date"]:
        if col in fact.columns:
            m = pd.to_datetime(fact[col], errors="coerce").dt.strftime("%Y-%m")
            return fact.loc[m == month].copy()
    return fact.copy()


def _add_margin_cols(kpi: pd.DataFrame) -> pd.DataFrame:
    out = kpi.copy()
    if "Revenue" in out.columns:
        rev = pd.to_numeric(out["Revenue"], errors="coerce")
        if "gross_profit" in out.columns:
            gp = pd.to_numeric(out["gross_profit"], errors="coerce")
            out["gross_margin_pct"] = (gp / rev) * 100
        if "operating_profit" in out.columns:
            op = pd.to_numeric(out["operating_profit"], errors="coerce")
            out["operating_margin_pct"] = (op / rev) * 100
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--curated-dir", default="data/curated")
    ap.add_argument("--month", default=None, help="YYYY-MM e.g. 2025-12 (optional; can infer from KPI file)")
    ap.add_argument("--out-dir", default=None, help="Default: data/bi/<month>")
    args = ap.parse_args()

    curated = Path(args.curated_dir)

    fact = _read_parquet(curated / "fact_transactions.parquet")
    dim_accounts = _read_parquet(curated / "dim_accounts.parquet")
    kpi = _read_parquet(curated / "kpi_monthly.parquet")
    dq_ex = _read_csv(curated / "dq_exceptions.csv")
    dq_sum = _read_csv(curated / "dq_summary.csv")

    # normalize month to YYYY-MM
    if not kpi.empty and "month" in kpi.columns:
        kpi = kpi.copy()
        kpi["month"] = kpi["month"].map(_month_from_any)

    month = args.month or _infer_month(kpi)
    if not month:
        raise SystemExit("Could not infer month. Provide --month YYYY-MM.")

    out_dir = Path(args.out_dir) if args.out_dir else Path("data") / "bi" / month
    out_dir.mkdir(parents=True, exist_ok=True)

    # fact filtered to month (if a date column exists)
    fact_m = _filter_fact_to_month(fact, month)
    if not fact_m.empty:
        fact_m["month"] = month

    # KPI enriched + filtered
    kpi2 = _add_margin_cols(kpi)
    if not kpi2.empty and "month" in kpi2.columns:
        kpi_m = kpi2.loc[kpi2["month"] == month].copy()
    else:
        kpi_m = kpi2.copy()

    # Keep KPI columns in a stable order (if present)
    keep_kpi = [c for c in KPI_COLS if c in kpi_m.columns] + [c for c in ["gross_margin_pct", "operating_margin_pct"] if c in kpi_m.columns]
    if not kpi_m.empty and keep_kpi:
        kpi_m = kpi_m[keep_kpi]

    # Write CSVs (Power BI/Tableau friendly)
    fact_m.to_csv(out_dir / "fact_transactions.csv", index=False)
    dim_accounts.to_csv(out_dir / "dim_accounts.csv", index=False)
    kpi_m.to_csv(out_dir / "kpi_monthly.csv", index=False)
    dq_sum.to_csv(out_dir / "dq_summary.csv", index=False)
    dq_ex.to_csv(out_dir / "dq_exceptions.csv", index=False)

    # Simple schema dictionary
    dd_lines = []
    dd_lines.append(f"month={month}")
    dd_lines.append(f"fact_transactions.csv columns={list(fact_m.columns)}")
    dd_lines.append(f"dim_accounts.csv columns={list(dim_accounts.columns)}")
    dd_lines.append(f"kpi_monthly.csv columns={list(kpi_m.columns)}")
    dd_lines.append(f"dq_summary.csv columns={list(dq_sum.columns)}")
    dd_lines.append(f"dq_exceptions.csv columns={list(dq_ex.columns)}")
    (out_dir / "data_dictionary.txt").write_text("\n".join(dd_lines), encoding="utf-8")

    print(str(out_dir.resolve()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
