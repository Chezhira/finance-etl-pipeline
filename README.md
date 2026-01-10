# **Finance ETL Pipeline**  
*(Monthly Close Dataset)*  

Portfolio-grade data engineering mini-project producing validated, curated **Parquet datasets** for dashboards and FP&A.

---

## **Repository & CI**
- **Repository:** GitHub (see README for CI badge)  
- **CI:** GitHub Actions workflow status shown in README  

---

## **Architecture Overview**
```
+-----------+      +-----------+      +-----------+      +-----------+
|   Raw     | ---> |  Staging  | ---> | Transform | ---> |  Curated  |
|  CSVs     |      |  Cleaned  |      |  DQ + FX  |      | Parquet + |
| (Extract) |      |  Types OK |      |  Rules    |      | DQ Reports|
+-----------+      +-----------+      +-----------+      +-----------+
```

---

## **Quickstart**
### 1. Install package locally
```bash
pip install -e .
```

### 2. Generate one month of synthetic data
```bash
python scripts/generate_synthetic_data.py --month 2025-12 --out-dir data/raw
```

### 3. Run the pipeline with strict DQ checks
```bash
finance-etl run --month 2025-12 --fail-on ERROR
```

### 4. Run tests
```bash
pytest -q
```

---

## **Business Problem**
Monthly close data often comes from multiple sources (sales, expenses, payroll, FX, inventory) and usually arrives with:
- inconsistent formats (dates, currencies)
- silent type problems (IDs treated as numbers)
- duplicates / missing keys
- FX conversion inconsistencies  

This creates **rework, wrong KPIs, and stress during audit/tax season**.  
**This pipeline solves it by enforcing data quality rules and producing a single trusted dataset each month.**

---

## **Outputs**
After a run, the pipeline produces:
- `data/curated/fact_transactions.parquet` — unified transaction-level dataset (base currency)
- `data/curated/dim_accounts.parquet` — chart of accounts dimension
- `data/curated/kpi_monthly.parquet` — monthly KPIs for dashboards
- `data/curated/dq_exceptions.csv` — row-level DQ failures (audit trail)
- `data/curated/dq_summary.csv` — PASS/FAIL summary (controls)

---

## **Data Contracts**
Define expected columns, types, and rules for raw inputs.

### **transactions.csv**
| Column       | Type  | Rules                                  |
|-------------|-------|----------------------------------------|
| entity      | str   | Non-null, length ≤ 10                |
| account_code| str   | Must exist in dim_accounts           |
| amount      | float | -1e9 ≤ amount ≤ 1e9                 |
| currency    | str   | One of ["TZS", "USD", "EUR"]        |
| tx_date     | date  | ≤ today                              |

### **accounts.csv**
| Column       | Type  | Rules                                  |
|-------------|-------|----------------------------------------|
| account_code| str   | Unique, non-null                      |
| account_name| str   | Non-null                              |
| account_type| str   | One of ["Asset", "Liability", "Revenue", "Expense"] |

### **fx_rates.csv**
| Column       | Type  | Rules                                  |
|-------------|-------|----------------------------------------|
| currency    | str   | One of ["TZS", "USD", "EUR"]          |
| rate        | float | > 0                                   |
| effective_date| date| ≤ today                                |

---

## **Data Quality Controls**
`--fail-on` controls when the pipeline stops:
- **ERROR (default):** fail only if critical issues exist
- **WARN:** fail if any issues exist
- **NEVER:** always produce outputs, but write DQ reports  

Example:
```bash
finance-etl run --month 2025-12 --fail-on WARN
```

---

## **Example KPI Output**
Sample rows from `data/curated/kpi_monthly.parquet`:
| entity | month    | Asset    | COGS      | Expense   | Revenue   | gross_profit | operating_profit |
|--------|----------|----------|-----------|-----------|-----------|--------------|------------------|
| TLM    | 2025-12  | 4771.96  | -15648.55 | -38682.57 | 48129.36  | 32480.81     | -6201.76         |
| UPE    | 2025-12  | 12717.67 | -17281.12 | -31250.48 | 30050.52  | 12769.40     | -18481.08        |
