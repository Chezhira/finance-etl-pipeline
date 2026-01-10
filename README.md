Finance ETL Pipeline (Monthly Close Dataset)

A portfolio-grade data engineering mini-project that turns messy monthly finance extracts into validated, curated Parquet datasets for dashboards and FP&A.

📐 Architecture Overview
+-----------+      +-----------+      +-----------+      +-----------+
|   Raw     | ---> |  Staging  | ---> | Transform | ---> |  Curated  |
|  CSVs     |      |  Cleaned  |      |  DQ + FX  |      | Parquet + |
| (Extract) |      |  Types OK |      |  Rules    |      | DQ Reports|
+-----------+      +-----------+      +-----------+      +-----------+


🚀 Quickstart (Hello World)
Shell# 1) Install package locallypip install -e .# 2) Generate one month of synthetic datapython scripts/generate_synthetic_data.py --month 2025-12 --out-dir data/raw# 3) Run the pipeline with strict DQ checksfinance-etl run --month 2025-12 --fail-on ERROR# 4) Run testspytest -qShow more lines

💼 Business Problem
Monthly close data often comes from multiple sources (sales, expenses, payroll, FX, inventory) and usually arrives with:

inconsistent formats (dates, currencies)
silent type problems (IDs treated as numbers)
duplicates / missing keys
FX conversion inconsistencies

This creates rework, wrong KPIs, and stress during audit/tax season.
This pipeline solves it by enforcing data quality rules and producing a single trusted dataset each month.

✅ Outputs
After a run, the pipeline produces:

data/curated/fact_transactions.parquet — unified transaction-level dataset (base currency)
data/curated/dim_accounts.parquet — chart of accounts dimension
data/curated/kpi_monthly.parquet — monthly KPIs for dashboards
data/curated/dq_exceptions.csv — row-level DQ failures (audit trail)
data/curated/dq_summary.csv — PASS/FAIL summary (controls)


🔍 Data Contracts
transactions.csv


ColumnTypeAllowed Values / RulesentitystrNon-null, length ≤ 10account_codestrMust exist in dim_accountsamountfloat-1e9 ≤ amount ≤ 1e9currencystrOne of ["TZS", "USD", "EUR"]tx_datedate≤ today
accounts.csv






ColumnTypeAllowed Values / Rulesaccount_codestrUnique, non-nullaccount_namestrNon-nullaccount_typestrOne of ["Asset", "Liability", "Revenue", "Expense"]
fx_rates.csv







ColumnTypeAllowed Values / RulescurrencystrOne of ["TZS", "USD", "EUR"]ratefloat> 0effective_datedate≤ today

🛡️ Data Quality Controls
--fail-on controls when the pipeline stops:

ERROR (default): fail only if critical issues exist
WARN: fail if any issues exist
NEVER: always produce outputs, but write DQ reports

Example:
Shellfinance-etl run --month 2025-12 --fail-on WARNShow more lines

📊 Example KPI Output
Sample rows from data/curated/kpi_monthly.parquet:













entitymonthAssetCOGSExpenseRevenuegross_profitoperating_profitTLM2025-124771.96-15648.55-38682.5748129.3632480.81-6201.76UPE2025-1212717.67-17281.12