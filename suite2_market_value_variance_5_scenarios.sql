-- ============================================================
-- SUITE 2 — SOURCE TO DESTINATION MARKET VALUE VARIANCE
-- Reference (source) : p_sales_reporting.climl_fact_assets
-- Target (dest)      : p_ictech_discovery.p_ic_an_foundation_fact_wealth_assets
-- Filter             : source_system = 'CLIML_SAS'
-- Window             : 2025-09-01 → 2026-03-31
-- Source MV formula  : canadian_unit_price × number_of_units
-- Target MV column   : market_value
--
-- HOW TO READ RESULTS:
--   source_mv     = what the source says MV should be
--   target_mv     = what landed in the final table
--   variance_amt  = target_mv - source_mv  (negative = target is LESS)
--   variance_pct  = variance as % of source
--   status        = MATCH / INVESTIGATE / MISMATCH
--
-- PASS when variance_pct is within ±0.01% for all rows
-- ============================================================


-- ============================================================
-- SCENARIO 1 of 5
-- Report Month — Overall Market Value
-- Highest level check. Is the total MV correct per month?
-- One row per report month.
-- If this fails, something is structurally wrong for that month.
-- ============================================================

SELECT
    DATE_FORMAT(ref.extract_date, 'yyyy-MM')          AS report_month,

    -- Source numbers
    COUNT(DISTINCT ref.contract_policy_number)         AS source_account_count,
    COUNT(*)                                           AS source_row_count,
    ROUND(SUM(ref.canadian_unit_price
              * ref.number_of_units), 2)               AS source_mv,

    -- Destination numbers
    COUNT(DISTINCT tgt.policy_number)                  AS target_account_count,
    COUNT(tgt.policy_number)                           AS target_row_count,
    ROUND(SUM(tgt.market_value), 2)                    AS target_mv,

    -- Variance
    ROUND(SUM(tgt.market_value)
          - SUM(ref.canadian_unit_price
                * ref.number_of_units), 2)             AS variance_amt,

    ROUND((SUM(tgt.market_value)
           - SUM(ref.canadian_unit_price
                 * ref.number_of_units))
          / NULLIF(SUM(ref.canadian_unit_price
                       * ref.number_of_units), 0)
          * 100, 6)                                    AS variance_pct,

    -- Status flag
    CASE
        WHEN ABS(
            (SUM(tgt.market_value)
             - SUM(ref.canadian_unit_price * ref.number_of_units))
            / NULLIF(SUM(ref.canadian_unit_price * ref.number_of_units), 0)
        ) <= 0.0001 THEN 'MATCH'
        WHEN ABS(
            (SUM(tgt.market_value)
             - SUM(ref.canadian_unit_price * ref.number_of_units))
            / NULLIF(SUM(ref.canadian_unit_price * ref.number_of_units), 0)
        ) <= 0.001  THEN 'INVESTIGATE'
        ELSE 'MISMATCH'
    END                                                AS status

FROM p_sales_reporting.climl_fact_assets ref
LEFT JOIN p_ictech_discovery.p_ic_an_foundation_fact_wealth_assets tgt
       ON tgt.policy_number              = ref.contract_policy_number
      AND tgt.manufacturing_company_code = ref.fund_management_company_code
      AND tgt.extract_date               = ref.extract_date
      AND tgt.source_system              = 'CLIML_SAS'

WHERE ref.extract_date BETWEEN '2025-09-01' AND '2026-03-31'

GROUP BY DATE_FORMAT(ref.extract_date, 'yyyy-MM')
ORDER BY report_month;


-- ============================================================
-- SCENARIO 2 of 5
-- Report Month + Management Company (MAX vs CGF)
-- Did the right amounts flow through per company per month?
-- One row per month per company.
-- Catches cross-company blending or one company being dropped.
-- ============================================================

SELECT
    DATE_FORMAT(ref.extract_date, 'yyyy-MM')          AS report_month,
    ref.fund_management_company_code                   AS management_company,

    -- Source numbers
    COUNT(DISTINCT ref.contract_policy_number)         AS source_account_count,
    COUNT(*)                                           AS source_row_count,
    ROUND(SUM(ref.canadian_unit_price
              * ref.number_of_units), 2)               AS source_mv,

    -- Destination numbers
    COUNT(DISTINCT tgt.policy_number)                  AS target_account_count,
    COUNT(tgt.policy_number)                           AS target_row_count,
    ROUND(SUM(tgt.market_value), 2)                    AS target_mv,

    -- Variance
    ROUND(SUM(tgt.market_value)
          - SUM(ref.canadian_unit_price
                * ref.number_of_units), 2)             AS variance_amt,

    ROUND((SUM(tgt.market_value)
           - SUM(ref.canadian_unit_price
                 * ref.number_of_units))
          / NULLIF(SUM(ref.canadian_unit_price
                       * ref.number_of_units), 0)
          * 100, 6)                                    AS variance_pct,

    -- Status flag
    CASE
        WHEN ABS(
            (SUM(tgt.market_value)
             - SUM(ref.canadian_unit_price * ref.number_of_units))
            / NULLIF(SUM(ref.canadian_unit_price * ref.number_of_units), 0)
        ) <= 0.0001 THEN 'MATCH'
        WHEN ABS(
            (SUM(tgt.market_value)
             - SUM(ref.canadian_unit_price * ref.number_of_units))
            / NULLIF(SUM(ref.canadian_unit_price * ref.number_of_units), 0)
        ) <= 0.001  THEN 'INVESTIGATE'
        ELSE 'MISMATCH'
    END                                                AS status

FROM p_sales_reporting.climl_fact_assets ref
LEFT JOIN p_ictech_discovery.p_ic_an_foundation_fact_wealth_assets tgt
       ON tgt.policy_number              = ref.contract_policy_number
      AND tgt.manufacturing_company_code = ref.fund_management_company_code
      AND tgt.extract_date               = ref.extract_date
      AND tgt.source_system              = 'CLIML_SAS'

WHERE ref.extract_date BETWEEN '2025-09-01' AND '2026-03-31'

GROUP BY
    DATE_FORMAT(ref.extract_date, 'yyyy-MM'),
    ref.fund_management_company_code

ORDER BY
    report_month,
    management_company;


-- ============================================================
-- SCENARIO 3 of 5
-- Report Month + Dealer Code
-- Did money move correctly through each dealer channel per month?
-- One row per month per dealer.
-- Dealers see their own numbers — this is where complaints come from.
-- Shows top dealers by source MV descending within each month.
-- ============================================================

WITH monthly_dealer AS (
    SELECT
        DATE_FORMAT(ref.extract_date, 'yyyy-MM')      AS report_month,
        ref.dealer_code,

        -- Source
        COUNT(DISTINCT ref.contract_policy_number)     AS source_account_count,
        COUNT(*)                                       AS source_row_count,
        ROUND(SUM(ref.canadian_unit_price
                  * ref.number_of_units), 2)           AS source_mv,

        -- Destination
        COUNT(DISTINCT tgt.policy_number)              AS target_account_count,
        COUNT(tgt.policy_number)                       AS target_row_count,
        ROUND(SUM(tgt.market_value), 2)                AS target_mv,

        -- Variance
        ROUND(SUM(tgt.market_value)
              - SUM(ref.canadian_unit_price
                    * ref.number_of_units), 2)         AS variance_amt,

        ROUND((SUM(tgt.market_value)
               - SUM(ref.canadian_unit_price
                     * ref.number_of_units))
              / NULLIF(SUM(ref.canadian_unit_price
                           * ref.number_of_units), 0)
              * 100, 6)                                AS variance_pct

    FROM p_sales_reporting.climl_fact_assets ref
    LEFT JOIN p_ictech_discovery.p_ic_an_foundation_fact_wealth_assets tgt
           ON tgt.policy_number              = ref.contract_policy_number
          AND tgt.manufacturing_company_code = ref.fund_management_company_code
          AND tgt.extract_date               = ref.extract_date
          AND tgt.source_system              = 'CLIML_SAS'

    WHERE ref.extract_date BETWEEN '2025-09-01' AND '2026-03-31'

    GROUP BY
        DATE_FORMAT(ref.extract_date, 'yyyy-MM'),
        ref.dealer_code
)
SELECT
    report_month,
    dealer_code,
    source_account_count,
    source_row_count,
    source_mv,
    target_account_count,
    target_row_count,
    target_mv,
    variance_amt,
    variance_pct,

    CASE
        WHEN ABS(variance_pct) <= 0.0001 THEN 'MATCH'
        WHEN ABS(variance_pct) <= 0.001  THEN 'INVESTIGATE'
        ELSE 'MISMATCH'
    END                                                AS status

FROM monthly_dealer

ORDER BY
    report_month,
    source_mv DESC;


-- ============================================================
-- SCENARIO 4 of 5
-- Report Month + Fund Code + Management Company
-- Did each fund's market value land correctly per month?
-- One row per month per fund per company.
-- Fund-level is the most granular comparison before record level.
-- Catches specific funds with pricing or unit load issues.
-- ============================================================

WITH monthly_fund AS (
    SELECT
        DATE_FORMAT(ref.extract_date, 'yyyy-MM')       AS report_month,
        ref.fund_management_company_code               AS management_company,
        ref.fund_code,

        -- Source
        COUNT(DISTINCT ref.contract_policy_number)      AS source_account_count,
        COUNT(*)                                        AS source_row_count,
        MAX(ref.canadian_unit_price)                    AS source_canadian_unit_price,
        ROUND(SUM(ref.number_of_units), 4)              AS source_total_units,
        ROUND(SUM(ref.canadian_unit_price
                  * ref.number_of_units), 2)            AS source_mv,

        -- Destination
        COUNT(DISTINCT tgt.policy_number)               AS target_account_count,
        COUNT(tgt.policy_number)                        AS target_row_count,
        MAX(tgt.unit_value)                             AS target_unit_value,
        ROUND(SUM(tgt.number_of_units), 4)              AS target_total_units,
        ROUND(SUM(tgt.market_value), 2)                 AS target_mv,

        -- Variance
        ROUND(SUM(tgt.market_value)
              - SUM(ref.canadian_unit_price
                    * ref.number_of_units), 2)          AS variance_amt,

        ROUND((SUM(tgt.market_value)
               - SUM(ref.canadian_unit_price
                     * ref.number_of_units))
              / NULLIF(SUM(ref.canadian_unit_price
                           * ref.number_of_units), 0)
              * 100, 6)                                 AS variance_pct,

        -- Unit price difference (did CAD conversion happen correctly?)
        ROUND(MAX(tgt.unit_value)
              - MAX(ref.canadian_unit_price), 4)        AS unit_price_diff

    FROM p_sales_reporting.climl_fact_assets ref
    LEFT JOIN p_ictech_discovery.p_ic_an_foundation_fact_wealth_assets tgt
           ON tgt.policy_number              = ref.contract_policy_number
          AND tgt.manufacturing_company_code = ref.fund_management_company_code
          AND tgt.extract_date               = ref.extract_date
          AND tgt.source_system              = 'CLIML_SAS'

    WHERE ref.extract_date BETWEEN '2025-09-01' AND '2026-03-31'

    GROUP BY
        DATE_FORMAT(ref.extract_date, 'yyyy-MM'),
        ref.fund_management_company_code,
        ref.fund_code
)
SELECT
    report_month,
    management_company,
    fund_code,
    source_account_count,
    source_total_units,
    source_canadian_unit_price,
    source_mv,
    target_account_count,
    target_total_units,
    target_unit_value,
    target_mv,
    variance_amt,
    variance_pct,
    unit_price_diff,

    -- Pinpoint what is wrong: price issue or units issue
    CASE
        WHEN ABS(unit_price_diff)  > 0.01 THEN 'PRICE MISMATCH'
        WHEN ABS(source_total_units
                 - target_total_units) > 0.01 THEN 'UNITS MISMATCH'
        WHEN ABS(variance_pct) <= 0.0001  THEN 'MATCH'
        WHEN ABS(variance_pct) <= 0.001   THEN 'INVESTIGATE'
        ELSE 'MISMATCH'
    END                                                 AS status

FROM monthly_fund

ORDER BY
    report_month,
    ABS(variance_amt) DESC;


-- ============================================================
-- SCENARIO 5 of 5
-- Report Month + Monthly vs Daily Extract Split
-- Did the monthly file and daily file each contribute
-- the right market value per month?
-- One row per month per extract type.
-- Catches cases where monthly file data is dropped or
-- daily data is overwriting monthly for the same positions.
-- ============================================================

SELECT
    DATE_FORMAT(ref.extract_date, 'yyyy-MM')           AS report_month,

    CASE
        WHEN ref.monthly_extract_indicator = TRUE
        THEN 'Monthly File'
        ELSE 'Daily File'
    END                                                 AS extract_type,

    -- Source
    COUNT(DISTINCT ref.contract_policy_number)          AS source_account_count,
    COUNT(*)                                            AS source_row_count,
    ROUND(SUM(ref.canadian_unit_price
              * ref.number_of_units), 2)                AS source_mv,

    -- Destination
    COUNT(DISTINCT tgt.policy_number)                   AS target_account_count,
    COUNT(tgt.policy_number)                            AS target_row_count,
    ROUND(SUM(tgt.market_value), 2)                     AS target_mv,

    -- Variance
    ROUND(SUM(tgt.market_value)
          - SUM(ref.canadian_unit_price
                * ref.number_of_units), 2)              AS variance_amt,

    ROUND((SUM(tgt.market_value)
           - SUM(ref.canadian_unit_price
                 * ref.number_of_units))
          / NULLIF(SUM(ref.canadian_unit_price
                       * ref.number_of_units), 0)
          * 100, 6)                                     AS variance_pct,

    -- Extra: total units comparison
    ROUND(SUM(ref.number_of_units), 2)                  AS source_total_units,
    ROUND(SUM(tgt.number_of_units), 2)                  AS target_total_units,
    ROUND(SUM(tgt.number_of_units)
          - SUM(ref.number_of_units), 4)                AS units_variance,

    -- Status
    CASE
        WHEN ABS(
            (SUM(tgt.market_value)
             - SUM(ref.canadian_unit_price * ref.number_of_units))
            / NULLIF(SUM(ref.canadian_unit_price * ref.number_of_units), 0)
        ) <= 0.0001 THEN 'MATCH'
        WHEN ABS(
            (SUM(tgt.market_value)
             - SUM(ref.canadian_unit_price * ref.number_of_units))
            / NULLIF(SUM(ref.canadian_unit_price * ref.number_of_units), 0)
        ) <= 0.001  THEN 'INVESTIGATE'
        ELSE 'MISMATCH'
    END                                                 AS status

FROM p_sales_reporting.climl_fact_assets ref
LEFT JOIN p_ictech_discovery.p_ic_an_foundation_fact_wealth_assets tgt
       ON tgt.policy_number              = ref.contract_policy_number
      AND tgt.manufacturing_company_code = ref.fund_management_company_code
      AND tgt.extract_date               = ref.extract_date
      AND tgt.source_system              = 'CLIML_SAS'

WHERE ref.extract_date BETWEEN '2025-09-01' AND '2026-03-31'

GROUP BY
    DATE_FORMAT(ref.extract_date, 'yyyy-MM'),
    ref.monthly_extract_indicator

ORDER BY
    report_month,
    extract_type;
