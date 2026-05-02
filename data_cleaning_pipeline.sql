-- =========================================
-- Project: E-Commerce SQL Cleaning Pipeline
-- Author:  Muhammad Suleman
-- Description: Cleaned raw e-commerce data
--              from a legacy acquisition dataset.
--              Handles mixed formats, typos,
--              invalid values and duplicates.
-- Environment: DuckDB / Verulam Blue Mint
-- Date: April 2026
-- =========================================


-- =========================================
-- PHASE 1: Data Profiling
-- =========================================

-- KEY PATTERN: Always profile before cleaning
-- Inspect raw data to understand structure and volume
SELECT *
FROM C01_l01_ecommerce_retail_data_table
LIMIT 20;


-- =========================================
-- PHASE 2: NULL Analysis
-- =========================================

-- KEY PATTERN: SUM(CASE WHEN ... IS NULL) pattern
-- Count NULLs across every key field before touching data
-- This gives us a baseline to measure cleaning progress
SELECT
    COUNT(*)                                                            AS total_rows,
    SUM(CASE WHEN date             IS NULL THEN 1 ELSE 0 END)          AS null_date,
    SUM(CASE WHEN customer_segment IS NULL THEN 1 ELSE 0 END)          AS null_customer_segment,
    SUM(CASE WHEN order_amount_old IS NULL THEN 1 ELSE 0 END)          AS null_order_amount_old,
    SUM(CASE WHEN cost             IS NULL THEN 1 ELSE 0 END)          AS null_cost,
    SUM(CASE WHEN is_return        IS NULL THEN 1 ELSE 0 END)          AS null_is_return,
    SUM(CASE WHEN payment_method   IS NULL THEN 1 ELSE 0 END)          AS null_payment_method,
    SUM(CASE WHEN hour_of_day      IS NULL THEN 1 ELSE 0 END)          AS null_hour_of_day
FROM C01_l01_ecommerce_retail_data_table;


-- =========================================
-- PHASE 3: Categorical Profiling
-- =========================================

-- Understand how customer_segment values are distributed
-- This reveals typos before we attempt normalisation
SELECT
    customer_segment,
    COUNT(*) AS nbr_segments
FROM C01_l01_ecommerce_retail_data_table
GROUP BY customer_segment
ORDER BY nbr_segments DESC;


-- Understand how payment_method values are distributed
-- Confirms valid categories and spots unexpected values
SELECT
    payment_method,
    COUNT(*) AS nbr_methods
FROM C01_l01_ecommerce_retail_data_table
GROUP BY payment_method
ORDER BY nbr_methods DESC;


-- =========================================
-- PHASE 4: Silver Layer — Type Casting & Parsing
-- =========================================

-- KEY PATTERN: Multi-format date parsing using COALESCE + TRY_STRPTIME
-- Problem: dates arrive in both YYYY-MM-DD and DD-MM-YYYY formats
-- Solution: try both formats, take whichever succeeds first
-- Also: cast all numeric and flag fields to correct types safely

CREATE OR REPLACE TEMP VIEW silver_parsed AS
SELECT
    row_id,
    COALESCE(
        try_strptime(replace(date, '.', '-'), '%Y-%m-%d'),
        try_strptime(replace(date, '.', '-'), '%d-%m-%Y')
    )                                        AS parsed_dt,
    lower(trim(customer_segment))            AS customer_segment_raw,
    try_cast(order_amount_old AS DOUBLE)     AS order_amount_old,
    try_cast(cost             AS DOUBLE)     AS cost,
    try_cast(is_return        AS INTEGER)    AS is_return,
    payment_method,
    try_cast(hour_of_day      AS INTEGER)    AS hour_of_day
FROM C01_l01_ecommerce_retail_data_table;


-- =========================================
-- PHASE 5: Parsing Health Check
-- =========================================

-- Validate: how many dates failed to parse?
-- Target = 0 failures before moving forward
SELECT
    COUNT(*)                                              AS total_rows,
    SUM(CASE WHEN parsed_dt IS NULL THEN 1 ELSE 0 END)   AS date_parse_failures
FROM silver_parsed;

-- Sample the parsed results for visual inspection
SELECT *
FROM silver_parsed
LIMIT 10;


-- =========================================
-- PHASE 6: Silver Layer — Normalisation
-- =========================================

-- KEY PATTERN: Typo correction using REGEXP_REPLACE + CASE mapping
-- Problem: customer_segment has typos like 'standrad', 'premuim', 'platnum'
-- Solution: strip non-alpha characters then map to canonical tier names
-- Also: re-format date to standard DD-MM-YYYY string
-- Also: enforce is_return to only 0 or 1, NULL everything else

CREATE OR REPLACE TEMP VIEW silver_normalised AS
SELECT
    row_id,
    parsed_dt,
    /* Standard DD-MM-YYYY string for reporting */
    strftime(parsed_dt::DATE, '%d-%m-%Y')   AS date,
    /* Map common typos → canonical tiers */
    CASE
        WHEN customer_segment_raw IS NULL                                               THEN NULL
        WHEN regexp_replace(customer_segment_raw, '[^a-z]', '') IN ('standrad')        THEN 'standard'
        WHEN regexp_replace(customer_segment_raw, '[^a-z]', '') IN ('premuim')         THEN 'premium'
        WHEN regexp_replace(customer_segment_raw, '[^a-z]', '') IN ('platnum')         THEN 'platinum'
        ELSE customer_segment_raw
    END                                     AS customer_segment,
    order_amount_old,
    cost,
    /* Only accept valid return flags: 0 or 1 */
    CASE WHEN is_return IN (0,1) THEN is_return ELSE NULL END AS is_return,
    payment_method,
    hour_of_day
FROM silver_parsed;


-- =========================================
-- PHASE 7: Normalisation Health Check
-- =========================================

-- Confirm segment distribution looks correct after typo fix
-- Expected: only standard / premium / platinum remain
SELECT
    customer_segment,
    COUNT(*) AS nbr_segments
FROM silver_normalised
GROUP BY customer_segment
ORDER BY nbr_segments DESC;


-- =========================================
-- PHASE 8: Business Rule Impact Assessment
-- =========================================

-- KEY PATTERN: Pre-filter diagnostics
-- Measure how many rows each business rule would remove
-- This makes the cleaning decision transparent and defensible
SELECT
    SUM(CASE WHEN parsed_dt        IS NULL                              THEN 1 ELSE 0 END) AS bad_date,
    SUM(CASE WHEN order_amount_old IS NULL OR order_amount_old < 5.0   THEN 1 ELSE 0 END) AS bad_amount,
    SUM(CASE WHEN cost             IS NULL OR cost <= 0                 THEN 1 ELSE 0 END) AS bad_cost,
    SUM(CASE WHEN is_return NOT IN (0,1)   OR is_return IS NULL        THEN 1 ELSE 0 END) AS bad_return_flag,
    SUM(CASE WHEN hour_of_day IS NULL
                 OR hour_of_day NOT BETWEEN 0 AND 23                   THEN 1 ELSE 0 END) AS bad_hour
FROM silver_normalised;


-- =========================================
-- PHASE 9: Business Rule Enforcement
-- =========================================

-- Keep only valid transactions:
-- amount >= 5, cost > 0, valid parsed date,
-- valid return flag (0 or 1), hour between 0 and 23
CREATE OR REPLACE TEMP VIEW silver_filtered AS
SELECT *
FROM silver_normalised
WHERE
    parsed_dt        IS NOT NULL
    AND order_amount_old IS NOT NULL AND order_amount_old >= 5.0
    AND cost         IS NOT NULL     AND cost > 0
    AND is_return    IS NOT NULL
    AND hour_of_day  BETWEEN 0 AND 23;

-- Post-filter row count checkpoint
-- Compare against raw count to measure data loss
SELECT COUNT(*) AS kept_after_filter
FROM silver_filtered;


-- =========================================
-- PHASE 10: Duplicate Detection
-- =========================================

-- KEY PATTERN: Deduplication using SELECT DISTINCT over business key
-- Check distinct row count before building final table
-- Business key = all meaningful columns (not internal row_id)
SELECT COUNT(*) AS distinct_rows
FROM (
    SELECT DISTINCT
        row_id,
        date,
        customer_segment,
        order_amount_old,
        cost,
        is_return,
        payment_method,
        hour_of_day
    FROM silver_filtered
);


-- =========================================
-- PHASE 11: Final Clean Table (Gold Layer)
-- =========================================

-- Remove all duplicates and produce the single source of truth
-- This is the table all KPIs will be built from
CREATE OR REPLACE TEMP VIEW clean_table AS
SELECT DISTINCT
    row_id,
    date,
    customer_segment,
    order_amount_old,
    cost,
    is_return,
    payment_method,
    hour_of_day
FROM silver_filtered;

-- Final visual check on clean data
SELECT *
FROM clean_table
LIMIT 10;