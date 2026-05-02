-- =========================================
-- Project: E-Commerce SQL Cleaning Pipeline
-- Author: Muhammad Suleman
-- Description: 10 Business KPIs calculated
--              from the validated clean_table.
--              All KPIs share a uniform schema
--              (kpi_name, kpi_value, kpi_key)
--              for easy consolidation.
-- Environment: DuckDB / Verulam Blue Mint
-- Date: April 2026
-- =========================================


-- =========================================
-- KPI 1: Average Order Value (AOV)
-- =========================================

-- Business meaning: Average revenue per valid order.
-- High AOV = customers spending more per transaction.
-- Formula: SUM(order_amount) / COUNT(orders)

CREATE OR REPLACE TEMP VIEW kpi_1 AS
SELECT
    'kpi_1'                                          AS kpi_name,
    CAST(ROUND(AVG(order_amount_old), 2) AS VARCHAR) AS kpi_value,
    CAST(NULL AS VARCHAR)                            AS kpi_key
FROM clean_table;

SELECT * FROM kpi_1;


-- =========================================
-- KPI 2: Overall Gross Margin
-- =========================================

-- Business meaning: What % of revenue we keep as profit after cost.
-- Formula: (Total Revenue - Total Cost) / Total Revenue
-- Higher = more profitable business

CREATE OR REPLACE TEMP VIEW kpi_2 AS
SELECT
    'kpi_2'                                                                   AS kpi_name,
    CAST(ROUND(SUM(order_amount_old - cost) / SUM(order_amount_old), 6)
         AS VARCHAR)                                                          AS kpi_value,
    CAST(NULL AS VARCHAR)                                                     AS kpi_key
FROM clean_table;

SELECT * FROM kpi_2;


-- =========================================
-- KPI 3: Return Rate
-- =========================================

-- Business meaning: Share of orders that were returned.
-- Formula: Total Returns / Total Orders
-- Lower = better customer satisfaction

CREATE OR REPLACE TEMP VIEW kpi_3 AS
SELECT
    'kpi_3'                                               AS kpi_name,
    CAST(ROUND(SUM(is_return) / COUNT(*), 6) AS VARCHAR)  AS kpi_value,
    CAST(NULL AS VARCHAR)                                 AS kpi_key
FROM clean_table;

SELECT * FROM kpi_3;


-- =========================================
-- KPI 4: Median Order Amount
-- =========================================

-- Business meaning: The middle order value — less skewed than average.
-- Useful when a few very large orders inflate the AOV.
-- Formula: MEDIAN(order_amount_old)

CREATE OR REPLACE TEMP VIEW kpi_4 AS
SELECT
    'kpi_4'                                             AS kpi_name,
    CAST(ROUND(median(order_amount_old), 2) AS VARCHAR) AS kpi_value,
    CAST(NULL AS VARCHAR)                               AS kpi_key
FROM clean_table;

SELECT * FROM kpi_4;


-- =========================================
-- KPI 5: Return Rate by Payment Method
-- =========================================

-- Business meaning: Which payment methods are linked to higher returns?
-- Helps identify friction points or fraud patterns.
-- KEY PATTERN: GROUP BY used to produce one row per payment method

CREATE OR REPLACE TEMP VIEW kpi_5 AS
SELECT
    'kpi_5'                                                      AS kpi_name,
    CAST(ROUND(SUM(is_return) / COUNT(*), 6) AS VARCHAR)         AS kpi_value,
    CAST(payment_method AS VARCHAR)                              AS kpi_key
FROM clean_table
GROUP BY payment_method;

SELECT * FROM kpi_5;


-- =========================================
-- KPI 6: High-Value Segment GMV Share
-- =========================================

-- Business meaning: What share of total revenue comes from
--                   premium + platinum customers combined?
-- Formula: HV Segment Revenue / Total Revenue
-- KEY PATTERN: CTE used to pre-aggregate before ratio calculation

CREATE OR REPLACE TEMP VIEW kpi_6 AS
WITH gmv AS (
    SELECT
        SUM(order_amount_old)                                                      AS total_gmv,
        SUM(CASE WHEN customer_segment IN ('premium','platinum')
                 THEN order_amount_old ELSE 0 END)                                 AS hv_gmv
    FROM clean_table
)
SELECT
    'kpi_6'                                       AS kpi_name,
    CAST(ROUND(hv_gmv / total_gmv, 6) AS VARCHAR) AS kpi_value,
    CAST(NULL AS VARCHAR)                         AS kpi_key
FROM gmv;

SELECT * FROM kpi_6;


-- =========================================
-- KPI 7: Below-Target Margin Rate
-- =========================================

-- Business meaning: Share of orders falling below their segment margin floor.
-- Floor margins: standard=40%, premium=30%, platinum=25%
-- Higher rate = more orders being sold at insufficient profit margin
-- KEY PATTERN: Multi-CTE pipeline — base → eligible → final KPI

CREATE OR REPLACE TEMP VIEW kpi_7 AS
WITH base AS (
    -- Step 1: Calculate per-order gross margin
    SELECT
        customer_segment,
        (order_amount_old - cost) / order_amount_old AS gross_margin
    FROM clean_table
),
eligible AS (
    -- Step 2: Assign segment floor margin and filter to known segments
    SELECT
        customer_segment,
        gross_margin,
        CASE
            WHEN customer_segment = 'standard' THEN 0.40
            WHEN customer_segment = 'premium'  THEN 0.30
            WHEN customer_segment = 'platinum' THEN 0.25
        END AS floor_margin
    FROM base
    WHERE customer_segment IN ('standard','premium','platinum')
)
-- Step 3: Count orders below floor as a share of total
SELECT
    'kpi_7' AS kpi_name,
    CAST(ROUND(
        1.0 * SUM(
            CASE
                WHEN customer_segment = 'platinum'
                     AND gross_margin <= floor_margin THEN 1
                WHEN customer_segment IN ('standard','premium')
                     AND gross_margin <  floor_margin THEN 1
                ELSE 0
            END
        ) / COUNT(*), 6
    ) AS VARCHAR)         AS kpi_value,
    CAST(NULL AS VARCHAR) AS kpi_key
FROM eligible;

SELECT * FROM kpi_7;


-- =========================================
-- KPI 8: Top GMV Month
-- =========================================

-- Business meaning: Which calendar month generated the most revenue?
-- Helps identify seasonality and peak trading periods.
-- KEY PATTERN: Date formatting with STRFTIME + STRPTIME to extract month

CREATE OR REPLACE TEMP VIEW kpi_8 AS
WITH month_gmv AS (
    -- Aggregate GMV by calendar month
    SELECT
        strftime(strptime(date, '%d-%m-%Y'), '%Y-%m') AS month_key,
        SUM(order_amount_old)                         AS gmv
    FROM clean_table
    GROUP BY month_key
)
SELECT
    'kpi_8'                    AS kpi_name,
    CAST(month_key AS VARCHAR) AS kpi_value,
    CAST(NULL AS VARCHAR)      AS kpi_key
FROM month_gmv
ORDER BY gmv DESC, month_key DESC
LIMIT 1;

SELECT * FROM kpi_8;


-- =========================================
-- KPI 9: Latest Month-over-Month GMV Growth %
-- =========================================

-- Business meaning: Is revenue growing or shrinking vs last month?
-- Formula: (Current Month GMV - Prior Month GMV) / Prior Month GMV
-- KEY PATTERN: Window function LAG() used for time-series comparison

CREATE OR REPLACE TEMP VIEW kpi_9 AS
WITH month_gmv AS (
    -- Step 1: Aggregate GMV per calendar month
    SELECT
        strftime(strptime(date, '%d-%m-%Y'), '%Y-%m') AS month_key,
        SUM(order_amount_old)                         AS gmv
    FROM clean_table
    GROUP BY month_key
),
with_lag AS (
    -- Step 2: Pull prior month GMV using LAG window function
    SELECT
        month_key,
        gmv,
        LAG(gmv) OVER (ORDER BY month_key) AS prev_gmv
    FROM month_gmv
),
latest AS (
    -- Step 3: Isolate the most recent month only
    SELECT * FROM with_lag
    ORDER BY month_key DESC
    LIMIT 1
)
SELECT
    'kpi_9'                                                 AS kpi_name,
    CAST(ROUND((gmv - prev_gmv) / prev_gmv, 6) AS VARCHAR) AS kpi_value,
    CAST(NULL AS VARCHAR)                                   AS kpi_key
FROM latest;

SELECT * FROM kpi_9;


-- =========================================
-- KPI 10: Max Payment Mix Shift (MoM)
-- =========================================

-- Business meaning: Which payment method saw the biggest
--                   month-over-month share change?
-- Detects shifts in customer payment behaviour over time.
-- KEY PATTERN: Window function LAG() partitioned by payment_method
--              to track each method's share independently per month

CREATE OR REPLACE TEMP VIEW kpi_10 AS
WITH with_month AS (
    -- Step 1: Extract month key and payment method per order
    SELECT
        strftime(strptime(date, '%d-%m-%Y'), '%Y-%m') AS month_key,
        payment_method
    FROM clean_table
),
counts AS (
    -- Step 2: Count orders per month per payment method
    SELECT
        month_key,
        payment_method,
        COUNT(*) AS n
    FROM with_month
    GROUP BY month_key, payment_method
),
totals AS (
    -- Step 3: Total orders per month (denominator)
    SELECT
        month_key,
        SUM(n) AS total
    FROM counts
    GROUP BY month_key
),
shares AS (
    -- Step 4: Calculate each method's share of monthly orders
    SELECT
        c.month_key,
        c.payment_method,
        1.0 * c.n / t.total AS share
    FROM counts c
    JOIN totals t USING (month_key)
),
diffs AS (
    -- Step 5: Calculate absolute MoM shift per payment method
    SELECT
        payment_method,
        month_key,
        ABS(share - LAG(share) OVER (
            PARTITION BY payment_method
            ORDER BY month_key
        )) AS diff
    FROM shares
)
-- Step 6: Return the single largest shift observed
SELECT
    'kpi_10'                             AS kpi_name,
    CAST(ROUND(MAX(diff), 6) AS VARCHAR) AS kpi_value,
    CAST(NULL AS VARCHAR)                AS kpi_key
FROM diffs
WHERE diff IS NOT NULL;

SELECT * FROM kpi_10;


-- =========================================
-- FINAL: Consolidate All KPIs into One Table
-- =========================================

-- All KPIs share the same 3-column schema:
-- kpi_name (label), kpi_value (result), kpi_key (breakdown dimension)
-- UNION ALL stacks them into one dashboard-ready results table

CREATE OR REPLACE TABLE kpi_results AS
SELECT * FROM kpi_1
UNION ALL SELECT * FROM kpi_2
UNION ALL SELECT * FROM kpi_3
UNION ALL SELECT * FROM kpi_4
UNION ALL SELECT * FROM kpi_5
UNION ALL SELECT * FROM kpi_6
UNION ALL SELECT * FROM kpi_7
UNION ALL SELECT * FROM kpi_8
UNION ALL SELECT * FROM kpi_9
UNION ALL SELECT * FROM kpi_10;

SELECT * FROM kpi_results;