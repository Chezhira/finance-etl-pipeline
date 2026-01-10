from __future__ import annotations

import argparse
from pathlib import Path
from datetime import datetime
import pandas as pd

try:
    import plotly.express as px
except ImportError as e:
    raise SystemExit("Missing dependency: plotly. Install with: pip install plotly") from e


KPI_COLS = ["Asset", "COGS", "Expense", "Revenue", "gross_profit", "operating_profit"]


def _read_parquet(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path) if path.exists() else pd.DataFrame()


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path) if path.exists() else pd.DataFrame()


def _pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _to_month_str(x) -> str:
    if pd.isna(x):
        return ""
    if isinstance(x, str):
        return x[:7]
    try:
        return pd.to_datetime(x).strftime("%Y-%m")
    except Exception:
        return str(x)[:7]


def _infer_month(kpi: pd.DataFrame) -> str | None:
    if kpi.empty:
        return None
    if "month" not in kpi.columns:
        return None
    months = sorted({_to_month_str(m) for m in kpi["month"].dropna().unique()})
    return months[-1] if months else None


def _filter_fact_to_month(fact: pd.DataFrame, month: str) -> pd.DataFrame:
    date_col = _pick_col(fact, ["tx_date", "date", "transaction_date", "posting_date", "invoice_date"])
    if not date_col or fact.empty:
        return fact.copy()
    m = pd.to_datetime(fact[date_col], errors="coerce").dt.strftime("%Y-%m")
    return fact.loc[m == month].copy()


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


def build_dashboard(curated_dir: Path, month: str | None, out_html: Path) -> Path:
    curated_dir = curated_dir.resolve()

    fact = _read_parquet(curated_dir / "fact_transactions.parquet")
    dim = _read_parquet(curated_dir / "dim_accounts.parquet")
    kpi = _read_parquet(curated_dir / "kpi_monthly.parquet")
    dq_ex = _read_csv(curated_dir / "dq_exceptions.csv")
    dq_sum = _read_csv(curated_dir / "dq_summary.csv")

    month = month or _infer_month(kpi)
    if not month:
        raise SystemExit("Could not infer month. Provide --month YYYY-MM (e.g., 2025-12).")

    # --- KPI: normalize + enrich ---
    kpi2 = kpi.copy()
    if "month" in kpi2.columns:
        kpi2["month"] = kpi2["month"].map(_to_month_str)

    kpi2 = _add_margin_cols(kpi2)

    # KPI charts (Revenue + operating_profit)
    kpi_chart_html = "<p class='muted'>No KPI chart available.</p>"
    kpi_chart2_html = "<p class='muted'>No Operating Profit chart available.</p>"

    if not kpi2.empty and all(c in kpi2.columns for c in ["entity", "month", "Revenue"]):
        # keep top entities by Revenue across all months for readability
        top_entities = (
            kpi2.groupby("entity")["Revenue"].sum().sort_values(ascending=False).head(8).index.tolist()
        )
        kpi_top = kpi2.loc[kpi2["entity"].isin(top_entities)].sort_values(["month", "entity"])

        fig = px.line(
            kpi_top,
            x="month",
            y="Revenue",
            color="entity",
            markers=True,
            title="Revenue Trend (Top Entities)",
        )
        kpi_chart_html = fig.to_html(full_html=False, include_plotlyjs="cdn")

        if "operating_profit" in kpi_top.columns:
            fig2 = px.line(
                kpi_top,
                x="month",
                y="operating_profit",
                color="entity",
                markers=True,
                title="Operating Profit Trend (Top Entities)",
            )
            kpi_chart2_html = fig2.to_html(full_html=False, include_plotlyjs=False)

    # KPI table for selected month
    kpi_table_html = "<p class='muted'>No KPI rows for this month.</p>"
    if not kpi2.empty and all(c in kpi2.columns for c in ["entity", "month"]):
        kpi_m = kpi2.loc[kpi2["month"] == month].copy()
        keep = ["entity", "month"] + [c for c in KPI_COLS if c in kpi_m.columns] + [
            c for c in ["gross_margin_pct", "operating_margin_pct"] if c in kpi_m.columns
        ]
        if not kpi_m.empty:
            # formatting
            for c in keep:
                if c not in ["entity", "month"]:
                    kpi_m[c] = pd.to_numeric(kpi_m[c], errors="coerce")
            kpi_table_html = (
                kpi_m[keep]
                .sort_values("entity")
                .to_html(index=False, float_format=lambda x: f"{x:,.2f}")
            )

    # Expense chart (Top expense accounts, absolute value)
    exp_chart_html = "<p class='muted'>No expense chart available.</p>"
    if not fact.empty:
        fact_m = _filter_fact_to_month(fact, month)
        amt_col = _pick_col(fact_m, ["amount_base", "amount", "amount_tzs", "amount_usd"])
        acc_col = _pick_col(fact_m, ["account_code", "gl_account", "account"])

        if amt_col and acc_col and not fact_m.empty:
            df = fact_m.copy()

            if not dim.empty and "account_code" in dim.columns and acc_col in df.columns:
                df = df.merge(dim, left_on=acc_col, right_on="account_code", how="left", suffixes=("", "_dim"))

            type_col = _pick_col(df, ["account_type", "type", "account_type_dim"])
            if type_col:
                df = df.loc[df[type_col].astype(str).str.lower().eq("expense")].copy()

            name_col = _pick_col(df, ["account_name", "account_name_dim", "name"])
            df["_label"] = df[acc_col].astype(str)
            if name_col:
                df["_label"] = df["_label"] + " - " + df[name_col].astype(str)

            df["_abs"] = pd.to_numeric(df[amt_col], errors="coerce").abs()
            top = (
                df.groupby("_label")["_abs"].sum()
                .sort_values(ascending=False)
                .head(15)
                .reset_index()
            )
            if not top.empty:
                fig = px.bar(top, x="_abs", y="_label", orientation="h", title="Top Expense Accounts (Abs Value)")
                exp_chart_html = fig.to_html(full_html=False, include_plotlyjs=False)

    # DQ tables
    dq_sum_html = dq_sum.head(200).to_html(index=False) if not dq_sum.empty else "<p class='muted'>No dq_summary.csv</p>"
    dq_ex_html = dq_ex.head(200).to_html(index=False) if not dq_ex.empty else "<p class='muted'>No dq_exceptions.csv</p>"

    # Build HTML
    out_html.parent.mkdir(parents=True, exist_ok=True)
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    html = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>Finance Dashboard - {month}</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; }}
    .muted {{ color: #666; font-size: 12px; }}
    .card {{ border: 1px solid #ddd; border-radius: 10px; padding: 16px; margin-bottom: 16px; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border: 1px solid #eee; padding: 8px; font-size: 12px; }}
    th {{ background: #fafafa; }}
  </style>
</head>
<body>
  <h1>Finance Monthly Close Dashboard</h1>
  <p class="muted">Month: <b>{month}</b> | Built: {now} | Curated: {curated_dir}</p>

  <div class="card">
    <h2>KPI Trend</h2>
    {kpi_chart_html}
    <br/>
    {kpi_chart2_html}
  </div>

  <div class="card">
    <h2>KPIs (Selected Month)</h2>
    {kpi_table_html}
  </div>

  <div class="card">
    <h2>Expense Breakdown</h2>
    {exp_chart_html}
  </div>

  <div class="card">
    <h2>Data Quality Summary</h2>
    {dq_sum_html}
    <h3>DQ Exceptions (sample)</h3>
    {dq_ex_html}
  </div>
</body>
</html>"""

    out_html.write_text(html, encoding="utf-8")
    return out_html


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--curated-dir", default="data/curated")
    ap.add_argument("--month", default=None, help="YYYY-MM (e.g., 2025-12). If omitted, will infer from KPI file.")
    ap.add_argument("--out", default=None, help="Default: reports/<month>/dashboard.html")
    args = ap.parse_args()

    curated = Path(args.curated_dir)
    month = args.month or _infer_month(_read_parquet(curated / "kpi_monthly.parquet"))
    if not month:
        raise SystemExit("Could not infer month. Provide --month YYYY-MM.")

    out = Path(args.out) if args.out else Path("reports") / month / "dashboard.html"
    final = build_dashboard(curated, month, out)
    print(str(final.resolve()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
