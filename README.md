# 🛒 E-Commerce SQL Cleaning Pipeline & KPI Analysis

> Building a structured SQL cleaning pipeline for legacy e-commerce data
> and delivering trusted KPI reporting for business analytics.

---

## 📌 Project Overview

A retail company inherited a legacy e-commerce orders dataset after an acquisition.
Although the data was available, it was unreliable for reporting and business
decision-making due to several data quality issues:

- Mixed date formats (`YYYY-MM-DD` and `DD-MM-YYYY`)
- Inconsistent customer segment labels with spelling variations
- Duplicate records
- Missing and invalid values in important fields

### 🎯 Objective
Design a complete SQL-based data cleaning pipeline that transforms raw
transactional data into a validated **single source of truth** and generates
10 business-ready KPIs for reporting and dashboard analysis.

> ⚠️ Note: This repository focuses on the **conceptual workflow, SQL logic,
> data cleaning methodology, and KPI design approach**. It is intended for
> educational, portfolio, and analytical demonstration purposes.

---

## 📁 Repository Structure

ecommerce-sql-cleaning-pipeline/
│
├── sql/
│   ├── data_cleaning_pipeline.sql
│   └── kpi_calculations.sql
│
├── data/
│   └── ecommerce_raw_data.csv
│
├── assets/
│   └── screenshots/
│
├── data_dictionary.md
└── README.md

---

## 🏗️ Pipeline Architecture

RAW DATA
│
▼
┌─────────────────────────────┐
│ BRONZE LAYER                │
│ Raw source table            │
└─────────────────────────────┘
│
▼
┌─────────────────────────────┐
│ SILVER — Parsed Layer       │
│ • Multi-format date parsing │
│ • Safe type conversions     │
│ • String standardisation    │
└─────────────────────────────┘
│
▼
┌─────────────────────────────┐
│ SILVER — Normalised Layer   │
│ • Typo correction           │
│ • Segment standardisation   │
│ • Return flag validation    │
└─────────────────────────────┘
│
▼
┌─────────────────────────────┐
│ SILVER — Filtered Layer     │
│ • Business rule checks      │
│ • Invalid row removal       │
└─────────────────────────────┘
│
▼
┌─────────────────────────────┐
│ GOLD LAYER                  │
│ • Cleaned dataset           │
│ • Deduplicated records      │
│ • KPI-ready data            │
└─────────────────────────────┘
│
▼
┌─────────────────────────────┐
│ KPI LAYER                   │
│ Final KPI reporting table   │
└─────────────────────────────┘

---

## 📊 Business KPIs Delivered

| # | KPI Name | Description |
|---|-----------|-------------|
| 1 | Average Order Value (AOV) | Average revenue generated per valid order |
| 2 | Overall Gross Margin | Percentage of revenue retained after cost |
| 3 | Return Rate | Percentage of returned orders |
| 4 | Median Order Amount | Median order value to reduce skew impact |
| 5 | Return Rate by Payment Method | Return percentage grouped by payment type |
| 6 | High-Value Segment GMV Share | Revenue contribution from premium customer segments |
| 7 | Below-Target Margin Rate | Orders falling below expected segment margin |
| 8 | Top GMV Month | Highest revenue-generating month |
| 9 | Month-over-Month GMV Growth | Revenue growth compared to the previous month |
| 10 | Maximum Payment Mix Shift | Largest month-over-month payment distribution change |

---

## 🔧 Data Quality Issues & Solutions

| Issue | Solution |
|-------|----------|
| Mixed date formats | Multi-format date parsing and standardisation |
| Customer segment typos | Regex-based cleaning and mapping |
| Missing or invalid numeric values | Rule-based validation filtering |
| Invalid return flags | Binary validation enforcement |
| Invalid hour values | Range-based filtering |
| Duplicate records | Deduplication using business keys |
| Unrealistic order amounts | Minimum threshold enforcement |
| Zero or negative costs | Business rule validation |

---

## ✅ Results & Validation

| Processing Stage | Row Count |
|------------------|-----------|
| Raw Dataset | 10,286 |
| Parsed Layer | 10,286 |
| Normalised Layer | 10,286 |
| Filtered Layer | ~9,720 |
| Final Clean Table | ~9,720 |

### Validation Checks Performed

- NULL value audit across key columns
- Row count verification after each pipeline stage
- Segment distribution analysis before and after normalisation
- Date parsing validation
- Duplicate detection checks
- Business rule retention analysis

---

## 🔑 Key SQL Concepts Used

- Multi-format date parsing
- Safe type conversion
- Regex-based typo correction
- Rule-based filtering
- Deduplication logic
- Window functions for trend analysis
- Layered SQL architecture using CTEs
- Standardised KPI reporting structure

---

## 🛠️ Tech Stack

| Technology | Purpose |
|------------|---------|
| SQL | Data cleaning and KPI calculations |
| DuckDB / Verulam Blue Mint | SQL execution environment |
| CTEs | Layered transformation pipeline |
| Window Functions | Trend and time-series analysis |
| REGEXP_REPLACE | Data standardisation |
| TRY_STRPTIME / TRY_CAST | Safe parsing and type conversion |
| Git & GitHub | Version control and portfolio hosting |

---

## 💡 Key Learnings

- Data profiling is essential before starting the cleaning process
- Business rules require both technical and domain understanding
- Layered architectures improve maintainability and debugging
- Standardised KPI schemas simplify dashboard integration
- Window functions provide powerful analytical capabilities for trend analysis

---

## ▶️ Workflow Summary

1. Import raw e-commerce transactional data
2. Apply SQL-based cleaning and standardisation pipeline
3. Validate and filter invalid records
4. Generate a clean analytical dataset
5. Compute business KPIs for reporting and dashboards

---

## 👤 Author

**Muhammad Suleman**  
Data Analyst | SQL | Data Cleaning | Analytics Engineering

---

## 📄 License

This project is licensed under the MIT License.

---

> *“Effective data cleaning is not about removing rows —
> it is about making every transformation measurable,
> traceable, and reliable.”*
