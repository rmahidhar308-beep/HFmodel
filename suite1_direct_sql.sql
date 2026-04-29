-- ============================================================
-- TEST SUITE 1 — MARKET VALUE VALIDATION  (Direct SQL)
-- Target    : p_ictech_discovery.p_ic_an_foundation_fact_wealth_assets
-- Reference : p_sales_reporting.climl_fact_assets
-- Filter    : source_system = 'CLIML_SAS'
-- Window    : 2025-09-01 → 2026-03-31
-- Run each block independently in Databricks SQL / SQL editor
-- PASS = query returns 0 rows  |  FAIL = query returns rows
-- ============================================================


-- ============================================================
-- MV-01 | Row Count & Coverage
-- Target has CLIML_SAS records in date window
-- PASS WHEN : returns 1 row (data exists)
-- ============================================================
SELECT 1
FROM   p_ictech_discovery.p_ic_an_foundation_fact_wealth_assets
WHERE  source_system = 'CLIML_SAS'
  AND  extract_date BETWEEN '2025-09-01' AND '2026-03-31'
LIMIT  1;


-- ============================================================
-- MV-02 | Row Count & Coverage
-- Reference table has records in date window
-- PASS WHEN : returns 1 row (data exists)
-- ============================================================
SELECT 1
FROM   p_sales_reporting.climl_fact_assets
WHERE  extract_date BETWEEN '2025-09-01' AND '2026-03-31'
LIMIT  1;


-- ============================================================
-- MV-03 | Row Count & Coverage
-- Row count variance by extract_date must be < 1%
-- PASS WHEN : 0 rows  |  FAIL = dates where gap > 1%
-- ============================================================
WITH tgt AS (
    SELECT extract_date,
           COUNT(*) AS tgt_rows
    FROM   p_ictech_discovery.p_ic_an_foundation_fact_wealth_assets
    WHERE  source_system = 'CLIML_SAS'
      AND  extract_date BETWEEN '2025-09-01' AND '2026-03-31'
    GROUP BY extract_date
),
ref AS (
    SELECT extract_date,
           COUNT(*) AS ref_rows
    FROM   p_sales_reporting.climl_fact_assets
    WHERE  extract_date BETWEEN '2025-09-01' AND '2026-03-31'
    GROUP BY extract_date
)
SELECT  t.extract_date,
        t.tgt_rows,
        r.ref_rows,
        ABS(t.tgt_rows - r.ref_rows)                           AS row_diff,
        ROUND(ABS(t.tgt_rows - r.ref_rows)
              / NULLIF(r.ref_rows, 0) * 100, 4)                AS pct_diff
FROM    tgt  t
JOIN    ref  r ON t.extract_date = r.extract_date
WHERE   ABS(t.tgt_rows - r.ref_rows) / NULLIF(r.ref_rows, 0) > 0.01
ORDER BY pct_diff DESC;


-- ============================================================
-- MV-04 | Market Value Formula
-- Reference self-check: market_value = canadian_unit_price x units
-- PASS WHEN : 0 rows  |  FAIL = reference itself has bad values
-- ============================================================
SELECT  contract_policy_number,
        fund_code,
        fund_management_company_code,
        extract_date,
        number_of_units,
        canadian_unit_price,
        market_value                                             AS stored_mv,
        ROUND(canadian_unit_price * number_of_units, 2)          AS calculated_mv,
        ABS(market_value - canadian_unit_price * number_of_units) AS variance
FROM    p_sales_reporting.climl_fact_assets
WHERE   extract_date BETWEEN '2025-09-01' AND '2026-03-31'
  AND   ABS(market_value - canadian_unit_price * number_of_units) > 0.01
ORDER BY variance DESC
LIMIT  25;


-- ============================================================
-- MV-05 | Market Value Formula  *** PRIMARY TEST ***
-- market_value in target matches canadian_unit_price x units
-- from reference  (record-level, $0.01 tolerance)
-- PASS WHEN : 0 rows  |  FAIL = records with variance > $0.01
-- ============================================================
WITH tgt AS (
    SELECT policy_number,
           manufacturing_company_code,
           extract_date,
           number_of_units  AS tgt_units,
           unit_value        AS tgt_unit_price,
           market_value      AS tgt_mv
    FROM   p_ictech_discovery.p_ic_an_foundation_fact_wealth_assets
    WHERE  source_system = 'CLIML_SAS'
      AND  extract_date BETWEEN '2025-09-01' AND '2026-03-31'
),
ref AS (
    SELECT contract_policy_number,
           fund_management_company_code,
           extract_date,
           number_of_units                                  AS ref_units,
           canadian_unit_price                              AS ref_cad_price,
           ROUND(canadian_unit_price * number_of_units, 2)  AS expected_mv
    FROM   p_sales_reporting.climl_fact_assets
    WHERE  extract_date BETWEEN '2025-09-01' AND '2026-03-31'
)
SELECT  t.policy_number,
        t.manufacturing_company_code,
        t.extract_date,
        r.ref_units,
        r.ref_cad_price,
        r.expected_mv,
        t.tgt_mv,
        ABS(t.tgt_mv - r.expected_mv) AS variance
FROM    tgt t
JOIN    ref r
     ON t.policy_number              = r.contract_policy_number
    AND t.manufacturing_company_code = r.fund_management_company_code
    AND t.extract_date               = r.extract_date
WHERE   ABS(t.tgt_mv - r.expected_mv) > 0.01
ORDER BY variance DESC;


-- ============================================================
-- MV-06 | Market Value Formula
-- unit_value in target = canadian_unit_price in reference
-- PASS WHEN : 0 rows  |  FAIL = CAD price mismatch > $0.01
-- ============================================================
WITH tgt AS (
    SELECT policy_number,
           manufacturing_company_code,
           extract_date,
           unit_value AS tgt_unit_value
    FROM   p_ictech_discovery.p_ic_an_foundation_fact_wealth_assets
    WHERE  source_system = 'CLIML_SAS'
      AND  extract_date BETWEEN '2025-09-01' AND '2026-03-31'
),
ref AS (
    SELECT contract_policy_number,
           fund_management_company_code,
           extract_date,
           canadian_unit_price AS ref_price
    FROM   p_sales_reporting.climl_fact_assets
    WHERE  extract_date BETWEEN '2025-09-01' AND '2026-03-31'
)
SELECT  t.policy_number,
        t.manufacturing_company_code,
        t.extract_date,
        t.tgt_unit_value,
        r.ref_price,
        ABS(t.tgt_unit_value - r.ref_price) AS variance
FROM    tgt t
JOIN    ref r
     ON t.policy_number              = r.contract_policy_number
    AND t.manufacturing_company_code = r.fund_management_company_code
    AND t.extract_date               = r.extract_date
WHERE   ABS(t.tgt_unit_value - r.ref_price) > 0.01
ORDER BY variance DESC;


-- ============================================================
-- MV-07 | Market Value Formula
-- number_of_units in target = number_of_units in reference
-- PASS WHEN : 0 rows  |  FAIL = unit count mismatch > 0.01
-- ============================================================
WITH tgt AS (
    SELECT policy_number,
           manufacturing_company_code,
           extract_date,
           number_of_units AS tgt_units
    FROM   p_ictech_discovery.p_ic_an_foundation_fact_wealth_assets
    WHERE  source_system = 'CLIML_SAS'
      AND  extract_date BETWEEN '2025-09-01' AND '2026-03-31'
),
ref AS (
    SELECT contract_policy_number,
           fund_management_company_code,
           extract_date,
           number_of_units AS ref_units
    FROM   p_sales_reporting.climl_fact_assets
    WHERE  extract_date BETWEEN '2025-09-01' AND '2026-03-31'
)
SELECT  t.policy_number,
        t.manufacturing_company_code,
        t.extract_date,
        t.tgt_units,
        r.ref_units,
        ABS(t.tgt_units - r.ref_units) AS diff
FROM    tgt t
JOIN    ref r
     ON t.policy_number              = r.contract_policy_number
    AND t.manufacturing_company_code = r.fund_management_company_code
    AND t.extract_date               = r.extract_date
WHERE   ABS(t.tgt_units - r.ref_units) > 0.01
ORDER BY diff DESC;


-- ============================================================
-- MV-08 | Dimensional Breakdown
-- Aggregate market value variance by extract_date < 0.01%
-- PASS WHEN : 0 rows  |  FAIL = dates with aggregate gap > 0.01%
-- ============================================================
WITH tgt AS (
    SELECT extract_date,
           SUM(market_value) AS tgt_total
    FROM   p_ictech_discovery.p_ic_an_foundation_fact_wealth_assets
    WHERE  source_system = 'CLIML_SAS'
      AND  extract_date BETWEEN '2025-09-01' AND '2026-03-31'
    GROUP BY extract_date
),
ref AS (
    SELECT extract_date,
           SUM(ROUND(canadian_unit_price * number_of_units, 2)) AS ref_total
    FROM   p_sales_reporting.climl_fact_assets
    WHERE  extract_date BETWEEN '2025-09-01' AND '2026-03-31'
    GROUP BY extract_date
)
SELECT  t.extract_date,
        ROUND(t.tgt_total, 2)  AS tgt_total,
        ROUND(r.ref_total, 2)  AS ref_total,
        ROUND(ABS(t.tgt_total - r.ref_total), 2)               AS variance,
        ROUND(ABS(t.tgt_total - r.ref_total)
              / NULLIF(r.ref_total, 0) * 100, 6)                AS pct_variance
FROM    tgt t
JOIN    ref r ON t.extract_date = r.extract_date
WHERE   ABS(t.tgt_total - r.ref_total) / NULLIF(r.ref_total, 0) > 0.0001
ORDER BY pct_variance DESC;


-- ============================================================
-- MV-09 | Dimensional Breakdown
-- Aggregate market value by management company (MAX vs CGF)
-- PASS WHEN : 0 rows  |  FAIL = company+date with gap > 0.01%
-- ============================================================
WITH tgt AS (
    SELECT extract_date,
           manufacturing_company_code,
           SUM(market_value) AS tgt_total
    FROM   p_ictech_discovery.p_ic_an_foundation_fact_wealth_assets
    WHERE  source_system = 'CLIML_SAS'
      AND  extract_date BETWEEN '2025-09-01' AND '2026-03-31'
    GROUP BY extract_date, manufacturing_company_code
),
ref AS (
    SELECT extract_date,
           fund_management_company_code,
           SUM(ROUND(canadian_unit_price * number_of_units, 2)) AS ref_total
    FROM   p_sales_reporting.climl_fact_assets
    WHERE  extract_date BETWEEN '2025-09-01' AND '2026-03-31'
    GROUP BY extract_date, fund_management_company_code
)
SELECT  t.extract_date,
        t.manufacturing_company_code,
        ROUND(t.tgt_total, 2)  AS tgt_total,
        ROUND(r.ref_total, 2)  AS ref_total,
        ROUND(ABS(t.tgt_total - r.ref_total), 2)               AS variance,
        ROUND(ABS(t.tgt_total - r.ref_total)
              / NULLIF(r.ref_total, 0) * 100, 6)                AS pct_variance
FROM    tgt t
JOIN    ref r
     ON t.extract_date               = r.extract_date
    AND t.manufacturing_company_code = r.fund_management_company_code
WHERE   ABS(t.tgt_total - r.ref_total) / NULLIF(r.ref_total, 0) > 0.0001
ORDER BY pct_variance DESC;


-- ============================================================
-- MV-10 | Dimensional Breakdown
-- Aggregate market value by dealer_code — variance < 0.01%
-- PASS WHEN : 0 rows  |  FAIL = dealer+date with gap > 0.01%
-- ============================================================
WITH tgt AS (
    SELECT extract_date,
           dealer_code,
           SUM(market_value) AS tgt_total
    FROM   p_ictech_discovery.p_ic_an_foundation_fact_wealth_assets
    WHERE  source_system = 'CLIML_SAS'
      AND  extract_date BETWEEN '2025-09-01' AND '2026-03-31'
    GROUP BY extract_date, dealer_code
),
ref AS (
    SELECT extract_date,
           dealer_code,
           SUM(ROUND(canadian_unit_price * number_of_units, 2)) AS ref_total
    FROM   p_sales_reporting.climl_fact_assets
    WHERE  extract_date BETWEEN '2025-09-01' AND '2026-03-31'
    GROUP BY extract_date, dealer_code
)
SELECT  t.extract_date,
        t.dealer_code,
        ROUND(t.tgt_total, 2)  AS tgt_total,
        ROUND(r.ref_total, 2)  AS ref_total,
        ROUND(ABS(t.tgt_total - r.ref_total), 2)               AS variance,
        ROUND(ABS(t.tgt_total - r.ref_total)
              / NULLIF(r.ref_total, 0) * 100, 6)                AS pct_variance
FROM    tgt t
JOIN    ref r
     ON t.extract_date = r.extract_date
    AND t.dealer_code  = r.dealer_code
WHERE   ABS(t.tgt_total - r.ref_total) / NULLIF(r.ref_total, 0) > 0.0001
ORDER BY pct_variance DESC;


-- ============================================================
-- MV-11 | Dimensional Breakdown
-- No NULL plan_identifier_sk in the date window
-- PASS WHEN : 0 rows  |  FAIL = records with no plan assigned
-- ============================================================
SELECT  policy_number,
        manufacturing_company_code,
        extract_date,
        plan_identifier_sk
FROM    p_ictech_discovery.p_ic_an_foundation_fact_wealth_assets
WHERE   source_system      = 'CLIML_SAS'
  AND   extract_date BETWEEN '2025-09-01' AND '2026-03-31'
  AND   plan_identifier_sk IS NULL
ORDER BY extract_date DESC;


-- ============================================================
-- MV-12 | Indicator Flags
-- monthly_extract_indicator matches reference
-- PASS WHEN : 0 rows  |  FAIL = flag mismatch between tables
-- ============================================================
WITH tgt AS (
    SELECT policy_number,
           manufacturing_company_code,
           extract_date,
           monthly_extract_indicator AS tgt_monthly
    FROM   p_ictech_discovery.p_ic_an_foundation_fact_wealth_assets
    WHERE  source_system = 'CLIML_SAS'
      AND  extract_date BETWEEN '2025-09-01' AND '2026-03-31'
),
ref AS (
    SELECT contract_policy_number,
           fund_management_company_code,
           extract_date,
           monthly_extract_indicator AS ref_monthly
    FROM   p_sales_reporting.climl_fact_assets
    WHERE  extract_date BETWEEN '2025-09-01' AND '2026-03-31'
)
SELECT  t.policy_number,
        t.manufacturing_company_code,
        t.extract_date,
        t.tgt_monthly,
        r.ref_monthly
FROM    tgt t
JOIN    ref r
     ON t.policy_number              = r.contract_policy_number
    AND t.manufacturing_company_code = r.fund_management_company_code
    AND t.extract_date               = r.extract_date
WHERE   t.tgt_monthly <> r.ref_monthly;


-- ============================================================
-- MV-13 | Indicator Flags
-- weekly_extract_indicator matches reference
-- PASS WHEN : 0 rows  |  FAIL = flag mismatch between tables
-- ============================================================
WITH tgt AS (
    SELECT policy_number,
           manufacturing_company_code,
           extract_date,
           weekly_extract_indicator AS tgt_weekly
    FROM   p_ictech_discovery.p_ic_an_foundation_fact_wealth_assets
    WHERE  source_system = 'CLIML_SAS'
      AND  extract_date BETWEEN '2025-09-01' AND '2026-03-31'
),
ref AS (
    SELECT contract_policy_number,
           fund_management_company_code,
           extract_date,
           weekly_extract_indicator AS ref_weekly
    FROM   p_sales_reporting.climl_fact_assets
    WHERE  extract_date BETWEEN '2025-09-01' AND '2026-03-31'
)
SELECT  t.policy_number,
        t.manufacturing_company_code,
        t.extract_date,
        t.tgt_weekly,
        r.ref_weekly
FROM    tgt t
JOIN    ref r
     ON t.policy_number              = r.contract_policy_number
    AND t.manufacturing_company_code = r.fund_management_company_code
    AND t.extract_date               = r.extract_date
WHERE   t.tgt_weekly <> r.ref_weekly;


-- ============================================================
-- MV-14 | Indicator Flags
-- All guarantee boolean indicators = FALSE for CLIML_SAS
-- PASS WHEN : 0 rows  |  FAIL = any TRUE found (hardcoded wrong)
-- ============================================================
SELECT  policy_number,
        manufacturing_company_code,
        extract_date,
        death_guarantee_reset_indicator,
        econo_guarantee_indicator,
        enhanced_guarantee_benefit_indicator,
        extended_death_benefit_option_indicator,
        flexible_income_option_indicator,
        guarantee_indicator,
        income_transition_period_option_indicator,
        life_income_benefit_indicator,
        life_income_benefit_joint_indicator,
        life_income_benefit_single_indicator,
        maturity_guarantee_reset_indicator,
        nominee_account_indicator,
        short_term_rate_protection_six_month_indicator,
        short_term_rate_protection_twelve_month_indicator
FROM    p_ictech_discovery.p_ic_an_foundation_fact_wealth_assets
WHERE   source_system = 'CLIML_SAS'
  AND   extract_date BETWEEN '2025-09-01' AND '2026-03-31'
  AND   (
            death_guarantee_reset_indicator               = TRUE
         OR econo_guarantee_indicator                     = TRUE
         OR enhanced_guarantee_benefit_indicator          = TRUE
         OR extended_death_benefit_option_indicator       = TRUE
         OR flexible_income_option_indicator              = TRUE
         OR guarantee_indicator                           = TRUE
         OR income_transition_period_option_indicator     = TRUE
         OR life_income_benefit_indicator                 = TRUE
         OR life_income_benefit_joint_indicator           = TRUE
         OR life_income_benefit_single_indicator          = TRUE
         OR maturity_guarantee_reset_indicator            = TRUE
         OR nominee_account_indicator                     = TRUE
         OR short_term_rate_protection_six_month_indicator   = TRUE
         OR short_term_rate_protection_twelve_month_indicator = TRUE
        );


-- ============================================================
-- MV-15 | Plan & Product Matching
-- plan_identifier_sk in target matches reference
-- PASS WHEN : 0 rows  |  FAIL = plan SK mismatch or NULL
-- ============================================================
WITH tgt AS (
    SELECT policy_number,
           manufacturing_company_code,
           extract_date,
           plan_identifier_sk AS tgt_plan_sk
    FROM   p_ictech_discovery.p_ic_an_foundation_fact_wealth_assets
    WHERE  source_system = 'CLIML_SAS'
      AND  extract_date BETWEEN '2025-09-01' AND '2026-03-31'
),
ref AS (
    SELECT contract_policy_number,
           fund_management_company_code,
           extract_date,
           plan_identifier_sk AS ref_plan_sk
    FROM   p_sales_reporting.climl_fact_assets
    WHERE  extract_date BETWEEN '2025-09-01' AND '2026-03-31'
)
SELECT  t.policy_number,
        t.manufacturing_company_code,
        t.extract_date,
        t.tgt_plan_sk,
        r.ref_plan_sk
FROM    tgt t
JOIN    ref r
     ON t.policy_number              = r.contract_policy_number
    AND t.manufacturing_company_code = r.fund_management_company_code
    AND t.extract_date               = r.extract_date
WHERE   t.tgt_plan_sk <> r.ref_plan_sk
   OR   t.tgt_plan_sk IS NULL;


-- ============================================================
-- MV-16 | Plan & Product Matching
-- fund_sk > 0 for all CLIML_SAS records in window
-- PASS WHEN : 0 rows  |  FAIL = fund not resolved from dimension
-- ============================================================
SELECT  policy_number,
        manufacturing_company_code,
        extract_date,
        fund_sk
FROM    p_ictech_discovery.p_ic_an_foundation_fact_wealth_assets
WHERE   source_system = 'CLIML_SAS'
  AND   extract_date BETWEEN '2025-09-01' AND '2026-03-31'
  AND   (fund_sk = 0 OR fund_sk IS NULL)
ORDER BY extract_date DESC;


-- ============================================================
-- MV-17 | Plan & Product Matching
-- extract_date_sk > 0 for all records in window
-- PASS WHEN : 0 rows  |  FAIL = date not joined to date dimension
-- ============================================================
SELECT  policy_number,
        manufacturing_company_code,
        extract_date,
        extract_date_sk
FROM    p_ictech_discovery.p_ic_an_foundation_fact_wealth_assets
WHERE   source_system = 'CLIML_SAS'
  AND   extract_date BETWEEN '2025-09-01' AND '2026-03-31'
  AND   (extract_date_sk = 0 OR extract_date_sk IS NULL)
ORDER BY extract_date DESC;


-- ============================================================
-- MV-18 | Edge Cases
-- No negative market values in window
-- PASS WHEN : 0 rows  |  FAIL = negative market value found
-- ============================================================
SELECT  policy_number,
        manufacturing_company_code,
        extract_date,
        number_of_units,
        unit_value,
        market_value
FROM    p_ictech_discovery.p_ic_an_foundation_fact_wealth_assets
WHERE   source_system = 'CLIML_SAS'
  AND   extract_date BETWEEN '2025-09-01' AND '2026-03-31'
  AND   market_value < 0
ORDER BY market_value ASC;


-- ============================================================
-- MV-19 | Edge Cases
-- Zero units → zero market value
-- PASS WHEN : 0 rows  |  FAIL = non-zero MV with zero units
-- ============================================================
SELECT  policy_number,
        manufacturing_company_code,
        extract_date,
        number_of_units,
        unit_value,
        market_value
FROM    p_ictech_discovery.p_ic_an_foundation_fact_wealth_assets
WHERE   source_system   = 'CLIML_SAS'
  AND   extract_date BETWEEN '2025-09-01' AND '2026-03-31'
  AND   number_of_units = 0
  AND   market_value   <> 0;


-- ============================================================
-- MV-20 | Edge Cases
-- High-value accounts (>$1M) in reference are present in target
-- PASS WHEN : 0 rows  |  FAIL = high-value account dropped
-- ============================================================
SELECT  r.contract_policy_number,
        r.fund_management_company_code,
        r.extract_date,
        ROUND(r.canadian_unit_price * r.number_of_units, 2) AS expected_mv
FROM    p_sales_reporting.climl_fact_assets r
WHERE   r.extract_date BETWEEN '2025-09-01' AND '2026-03-31'
  AND   ROUND(r.canadian_unit_price * r.number_of_units, 2) > 1000000
  AND   NOT EXISTS (
            SELECT 1
            FROM   p_ictech_discovery.p_ic_an_foundation_fact_wealth_assets t
            WHERE  t.policy_number              = r.contract_policy_number
              AND  t.manufacturing_company_code = r.fund_management_company_code
              AND  t.extract_date               = r.extract_date
              AND  t.source_system              = 'CLIML_SAS'
        )
ORDER BY expected_mv DESC;
