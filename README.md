markdown# 🛒 E-Commerce SQL Cleaning Pipeline & KPI Analysis

> Cleaning legacy orders data through a structured SQL pipeline 
> and delivering 10 trusted business KPIs for reporting.

---

## 📌 Project Overview

A retail team inherited a legacy e-commerce orders extract from an
acquisition. The data was usable but not trustworthy:

- Mixed date formats (YYYY-MM-DD and DD-MM-YYYY)
- Inconsistent customer segment labels with typos
- Duplicate records across the dataset
- Missing and invalid values in key fields

**The goal:** Build one clean, validated "source of truth" table
and compute 10 business KPIs ready for dashboard reporting.

---

## 📁 Repository Structure
ecommerce-sql-cleaning-pipeline/
│
├── sql/
│   ├── data_cleaning_pipeline.sql   # Raw → parsed → normalised → filtered → clean
│   └── kpi_calculations.sql         # KPI 1–10 → kpi_results table
│
├── data/
│   └── ecommerce_raw_data.csv       # Original raw dataset
│
├── assets/
│   └── screenshots/                 # Before/after row counts, KPI output
│
├── data_dictionary.md               # Column definitions and business rules
└── README.md                        # You are here

---

## 🏗️ Pipeline Architecture
RAW DATA
│
▼
┌─────────────────────────────┐
│  BRONZE LAYER               │
│  C01_l01_ecommerce_retail   │
│  Raw table as-is            │
└─────────────────────────────┘
│
▼
┌─────────────────────────────┐
│  SILVER LAYER — Parsed      │
│  silver_parsed              │
│  • Multi-format date parse  │
│  • Safe type casting        │
│  • Lowercase + trim         │
└─────────────────────────────┘
│
▼
┌─────────────────────────────┐
│  SILVER LAYER — Normalised  │
│  silver_normalised          │
│  • Typo correction          │
│  • Segment standardisation  │
│  • Return flag enforcement  │
└─────────────────────────────┘
│
▼
┌─────────────────────────────┐
│  SILVER LAYER — Filtered    │
│  silver_filtered            │
│  • Business rule enforcement│
│  • Invalid row removal      │
└─────────────────────────────┘
│
▼
┌─────────────────────────────┐
│  GOLD LAYER                 │
│  clean_table                │
│  • Deduplicated             │
│  • Validated                │
│  • KPI-ready                │
└─────────────────────────────┘
│
▼
┌─────────────────────────────┐
│  KPI LAYER                  │
│  kpi_1 → kpi_10             │
│  kpi_results (final table)  │
└─────────────────────────────┘

---

## 📊 10 KPIs Delivered

| # | KPI Name | Business Meaning |
|---|----------|-----------------|
| 1 | Average Order Value (AOV) | Average revenue per valid order |
| 2 | Overall Gross Margin | % of revenue kept as profit after cost |
| 3 | Return Rate | Share of total orders that were returned |
| 4 | Median Order Amount | Middle order value, less skewed than average |
| 5 | Return Rate by Payment Method | Return rate broken down by payment type |
| 6 | High-Value Segment GMV Share | Revenue share from premium + platinum tiers |
| 7 | Below-Target Margin Rate | Share of orders below their segment margin floor |
| 8 | Top GMV Month | Best calendar month by total revenue |
| 9 | Latest MoM GMV Growth % | Most recent month vs prior month revenue change |
| 10 | Max Payment Mix Shift | Biggest MoM change in payment method share |

---

## 🔧 Data Issues & How They Were Solved

| # | Problem Found | Solution Applied |
|---|--------------|-----------------|
| 1 | Mixed date formats (YYYY-MM-DD and DD-MM-YYYY) | Multi-format parsing with `try_strptime` + `COALESCE` |
| 2 | Customer segment typos (standrad, premuim, platnum) | `REGEXP_REPLACE` + `CASE` mapping to canonical values |
| 3 | Null and invalid numeric fields | Rule-based `WHERE` filter with pre-filter diagnostics |
| 4 | Invalid return flags (not 0 or 1) | `CASE WHEN is_return IN (0,1)` enforcement |
| 5 | Impossible hour values (outside 0–23) | `BETWEEN 0 AND 23` filter |
| 6 | Duplicate records | `SELECT DISTINCT` over full business key |
| 7 | Order amounts below minimum threshold | `order_amount_old >= 5.0` business rule |
| 8 | Zero or negative cost values | `cost > 0` business rule enforcement |

---

## ✅ Results & Validation

| Stage | Row Count |
|-------|-----------|
| Raw data (bronze) | 10,286 |
| After parsing (silver_parsed) | 10,286 |
| After normalisation (silver_normalised) | 10,286 |
| After business rules (silver_filtered) | ~9,720 |
| After deduplication (clean_table) | ~9,720 |

### Validation Checks Included at Every Stage:
- ✔ NULL rate audit across all key fields
- ✔ Row count checkpoints before and after each phase
- ✔ Segment distribution check before and after normalisation
- ✔ Date parse failure count (target = 0)
- ✔ Duplicate collision count on business key
- ✔ Post-filter retained row count

---

## 🔑 Key SQL Patterns Used

```sql
-- PATTERN 1: Multi-format date parsing
COALESCE(
    try_strptime(replace(date, '.', '-'), '%Y-%m-%d'),
    try_strptime(replace(date, '.', '-'), '%d-%m-%Y')
) AS parsed_dt

-- PATTERN 2: Typo correction with regex + CASE
WHEN regexp_replace(customer_segment_raw, '[^a-z]', '') 
     IN ('standrad') THEN 'standard'

-- PATTERN 3: Business rule enforcement
WHERE order_amount_old >= 5.0
  AND cost > 0
  AND is_return IS NOT NULL
  AND hour_of_day BETWEEN 0 AND 23

-- PATTERN 4: Deduplication on business key
SELECT DISTINCT
    row_id, date, customer_segment,
    order_amount_old, cost, is_return,
    payment_method, hour_of_day
FROM silver_filtered

-- PATTERN 5: Window function for trend analysis
LAG(gmv) OVER (ORDER BY month_key) AS prev_gmv

-- PATTERN 6: Uniform KPI schema for easy consolidation
SELECT
    'kpi_1'         AS kpi_name,
    value           AS kpi_value,
    NULL            AS kpi_key
```

---

## 🛠️ Tech Stack

| Tool | Usage |
|------|-------|
| SQL | Core language for all pipeline and KPI logic |
| DuckDB / Verulam Blue Mint | Execution environment |
| CTEs | Multi-step pipeline logic |
| Window Functions | LAG() for MoM trend KPIs |
| REGEXP_REPLACE | Typo normalisation |
| TRY_STRPTIME / TRY_CAST | Safe type conversion |
| Git / GitHub | Version control and portfolio hosting |

---

## 💡 Key Learnings

- **Profile before cleaning** — measuring the mess first makes
  every cleaning decision transparent and defensible in interviews
- **Business rules are not just technical** — deciding what counts
  as a valid order requires business context, not just SQL
- **Layered architecture pays off** — Bronze → Silver → Gold keeps
  each transformation isolated and easy to debug
- **Uniform KPI schema** — building all KPIs with the same
  3-column structure makes consolidation and dashboard connection trivial
- **Window functions unlock time-series insight** — `LAG()` turns
  a simple aggregation into a powerful trend KPI with one extra line

---

## ▶️ How to Run

```sql
-- Step 1: Load raw data
-- Import ecommerce_raw_data.csv into your SQL environment
-- as table: C01_l01_ecommerce_retail_data_table

-- Step 2: Run the cleaning pipeline
-- Open and run: sql/data_cleaning_pipeline.sql
-- This produces: clean_table

-- Step 3: Run KPI calculations
-- Open and run: sql/kpi_calculations.sql
-- This produces: kpi_results

-- Step 4: View all KPIs
SELECT * FROM kpi_results;
```

---

## 👤 Author

**Suleman**  
Data Analyst | SQL • Data Cleaning • Analytics Engineering  

[![LinkedIn](https://www.linkedin.com/in/muhammad-suleman-z/)

---

## 📄 License

This project is open source and available under the
[MIT License](LICENSE).

---

> 💬 *"Good data cleaning is not about deleting rows —  
> it is about making every decision measurable, documented,  
> and defensible."*
