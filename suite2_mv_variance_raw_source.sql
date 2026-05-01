-- ============================================================
-- SUITE 2 — SOURCE TO DESTINATION MARKET VALUE VARIANCE
-- Source tables (from ETL script):
--   DAILY  : p_ictech_discovery.e_purchased_product_sas_account_fund_position
--   MONTHLY: p_ictech_discovery.e_purchased_product_sas_account_fund_position_monthly
--   PRICE  : p_ictech_discovery.e_product_shelf_sas_fund_price
--   FX     : p_ictech_discovery.e_reference_usd_daily_exchange_rate
--   ACCT   : p_ictech_discovery.e_purchased_product_sas_fund_accounts
--
-- Destination:
--   p_ictech_discovery.p_ic_an_foundation_fact_wealth_assets
--   WHERE source_system = 'CLIML_SAS'
--
-- Source MV formula (exact from ETL script):
--   shares_owned_amount
--   * CASE WHEN fund_price_currency = 'U'
--          THEN fund_price * currency_exchange_rate
--          ELSE fund_price
--     END
--
-- extract_date in final table:
--   DAILY   → fund_price_date (where price_date BETWEEN effective_date AND end_date)
--   MONTHLY → to_date(file_date, 'yyMMdd') matched to fund_price_date
--
-- Window  : 2025-09-01 → 2026-03-31
-- PASS    : variance_pct within ±0.01% for all rows
-- STATUS  : MATCH / INVESTIGATE / MISMATCH
-- ============================================================


-- ============================================================
-- SHARED SOURCE CTE (reused in all 5 scenarios)
-- Reconstructs the sas_fund_position view logic from the script.
-- UNION ALL of monthly + daily paths exactly as the ETL does it.
-- ============================================================
--
-- PATH A — MONTHLY
--   afp   = sas_account_fund_position_monthly
--   dt    = to_date(file_date, 'yyMMdd')  →  becomes extract_date
--   fp    = sas_fund_price joined on fund_code + company + fund_price_date = dt
--   exr   = usd_daily_exchange_rate joined on fund_price_date
--   fa    = sas_fund_accounts joined on account_id + company + dt in (effective, end)
--
-- PATH B — DAILY
--   afp   = sas_account_fund_position
--   dt    = fund_price_date (where price_date BETWEEN effective_date AND end_date)
--   fp    = sas_fund_price joined on fund_code + company + fund_price_date
--   exr   = usd_daily_exchange_rate joined on fund_price_date
--   fa    = sas_fund_accounts joined on account_id + company + price_date in (effective, end)
-- ============================================================


-- ============================================================
-- SCENARIO 1 of 5
-- Report Month — Total Market Value
-- Highest level. One row per month.
-- Source (raw tables) total MV vs destination total MV.
-- If this fails for a month, something dropped or miscalculated
-- at scale for that entire reporting period.
-- ============================================================

WITH source_mv AS (

    -- PATH A: MONTHLY positions
    SELECT
        afp.account_id                                              AS account_id,
        afp.management_company_code                                 AS management_company_code,
        afp.fund_code                                               AS fund_code,
        TO_DATE(afp.file_date, 'yyMMdd')                           AS extract_date,
        afp.shares_owned_amount                                     AS shares,
        fp.fund_price                                               AS raw_price,
        fp.fund_price_currency,
        exr.currency_exchange_rate,
        CASE WHEN fp.fund_price_currency = 'U'
             THEN COALESCE(fp.fund_price * exr.currency_exchange_rate, 0)
             ELSE COALESCE(fp.fund_price, 0)
        END                                                         AS cad_price,
        afp.shares_owned_amount
            * CASE WHEN fp.fund_price_currency = 'U'
                   THEN COALESCE(fp.fund_price * exr.currency_exchange_rate, 0)
                   ELSE COALESCE(fp.fund_price, 0)
              END                                                   AS source_market_value,
        'MONTHLY'                                                   AS source_path

    FROM p_ictech_discovery.e_purchased_product_sas_account_fund_position_monthly afp

    INNER JOIN p_ictech_discovery.e_product_shelf_sas_fund_price fp
            ON afp.fund_code              = fp.fund_code
           AND afp.management_company_code = fp.management_company_code
           AND TO_DATE(afp.file_date, 'yyMMdd') = fp.fund_price_date

    LEFT JOIN p_ictech_discovery.e_reference_usd_daily_exchange_rate exr
           ON fp.fund_price_date = exr.currency_exchange_date

    WHERE TO_DATE(afp.file_date, 'yyMMdd')
          BETWEEN '2025-09-01' AND '2026-03-31'

    UNION ALL

    -- PATH B: DAILY positions
    SELECT
        afp.account_id,
        afp.management_company_code,
        afp.fund_code,
        fp.fund_price_date                                          AS extract_date,
        afp.shares_owned_amount                                     AS shares,
        fp.fund_price                                               AS raw_price,
        fp.fund_price_currency,
        exr.currency_exchange_rate,
        CASE WHEN fp.fund_price_currency = 'U'
             THEN COALESCE(fp.fund_price * exr.currency_exchange_rate, 0)
             ELSE COALESCE(fp.fund_price, 0)
        END                                                         AS cad_price,
        afp.shares_owned_amount
            * CASE WHEN fp.fund_price_currency = 'U'
                   THEN COALESCE(fp.fund_price * exr.currency_exchange_rate, 0)
                   ELSE COALESCE(fp.fund_price, 0)
              END                                                   AS source_market_value,
        'DAILY'                                                     AS source_path

    FROM p_ictech_discovery.e_purchased_product_sas_account_fund_position afp

    INNER JOIN p_ictech_discovery.e_product_shelf_sas_fund_price fp
            ON afp.fund_code               = fp.fund_code
           AND afp.management_company_code  = fp.management_company_code
           AND fp.fund_price_date BETWEEN afp.effective_date AND afp.end_date

    LEFT JOIN p_ictech_discovery.e_reference_usd_daily_exchange_rate exr
           ON fp.fund_price_date = exr.currency_exchange_date

    WHERE fp.fund_price_date BETWEEN '2025-09-01' AND '2026-03-31'
      AND afp.management_company_code IN ('MAX', 'CGF')
),

dest_mv AS (
    SELECT
        policy_number,
        manufacturing_company_code,
        extract_date,
        market_value,
        number_of_units,
        unit_value
    FROM p_ictech_discovery.p_ic_an_foundation_fact_wealth_assets
    WHERE source_system = 'CLIML_SAS'
      AND extract_date BETWEEN '2025-09-01' AND '2026-03-31'
)

SELECT
    DATE_FORMAT(s.extract_date, 'yyyy-MM')          AS report_month,

    -- Source (raw tables)
    COUNT(DISTINCT s.account_id)                    AS source_account_count,
    COUNT(*)                                        AS source_row_count,
    ROUND(SUM(s.shares), 2)                         AS source_total_units,
    ROUND(SUM(s.source_market_value), 2)            AS source_mv,

    -- Destination (final table)
    COUNT(DISTINCT d.policy_number)                 AS dest_account_count,
    COUNT(d.policy_number)                          AS dest_row_count,
    ROUND(SUM(d.number_of_units), 2)                AS dest_total_units,
    ROUND(SUM(d.market_value), 2)                   AS dest_mv,

    -- Variance
    ROUND(SUM(d.market_value)
          - SUM(s.source_market_value), 2)          AS variance_amt,

    ROUND((SUM(d.market_value)
           - SUM(s.source_market_value))
          / NULLIF(SUM(s.source_market_value), 0)
          * 100, 6)                                 AS variance_pct,

    CASE
        WHEN ABS((SUM(d.market_value) - SUM(s.source_market_value))
             / NULLIF(SUM(s.source_market_value), 0)) <= 0.0001
             THEN 'MATCH'
        WHEN ABS((SUM(d.market_value) - SUM(s.source_market_value))
             / NULLIF(SUM(s.source_market_value), 0)) <= 0.001
             THEN 'INVESTIGATE'
        ELSE 'MISMATCH'
    END                                             AS status

FROM source_mv s
LEFT JOIN dest_mv d
       ON d.policy_number              = s.account_id
      AND d.manufacturing_company_code = s.management_company_code
      AND d.extract_date               = s.extract_date

GROUP BY DATE_FORMAT(s.extract_date, 'yyyy-MM')
ORDER BY report_month;


-- ============================================================
-- SCENARIO 2 of 5
-- Report Month + Management Company (MAX vs CGF)
-- Breaks Scenario 1 down by company.
-- MAX uses plan_sk 18322. CGF uses 21817/21818/21219.
-- Catches one company being dropped or its MV absorbed into
-- the other's total. One row per month per company.
-- ============================================================

WITH source_mv AS (

    SELECT
        afp.account_id,
        afp.management_company_code,
        afp.fund_code,
        TO_DATE(afp.file_date, 'yyMMdd')                AS extract_date,
        afp.shares_owned_amount                         AS shares,
        afp.shares_owned_amount
            * CASE WHEN fp.fund_price_currency = 'U'
                   THEN COALESCE(fp.fund_price * exr.currency_exchange_rate, 0)
                   ELSE COALESCE(fp.fund_price, 0)
              END                                       AS source_market_value
    FROM p_ictech_discovery.e_purchased_product_sas_account_fund_position_monthly afp
    INNER JOIN p_ictech_discovery.e_product_shelf_sas_fund_price fp
            ON afp.fund_code              = fp.fund_code
           AND afp.management_company_code = fp.management_company_code
           AND TO_DATE(afp.file_date, 'yyMMdd') = fp.fund_price_date
    LEFT JOIN p_ictech_discovery.e_reference_usd_daily_exchange_rate exr
           ON fp.fund_price_date = exr.currency_exchange_date
    WHERE TO_DATE(afp.file_date, 'yyMMdd') BETWEEN '2025-09-01' AND '2026-03-31'

    UNION ALL

    SELECT
        afp.account_id,
        afp.management_company_code,
        afp.fund_code,
        fp.fund_price_date                              AS extract_date,
        afp.shares_owned_amount,
        afp.shares_owned_amount
            * CASE WHEN fp.fund_price_currency = 'U'
                   THEN COALESCE(fp.fund_price * exr.currency_exchange_rate, 0)
                   ELSE COALESCE(fp.fund_price, 0)
              END
    FROM p_ictech_discovery.e_purchased_product_sas_account_fund_position afp
    INNER JOIN p_ictech_discovery.e_product_shelf_sas_fund_price fp
            ON afp.fund_code               = fp.fund_code
           AND afp.management_company_code  = fp.management_company_code
           AND fp.fund_price_date BETWEEN afp.effective_date AND afp.end_date
    LEFT JOIN p_ictech_discovery.e_reference_usd_daily_exchange_rate exr
           ON fp.fund_price_date = exr.currency_exchange_date
    WHERE fp.fund_price_date BETWEEN '2025-09-01' AND '2026-03-31'
      AND afp.management_company_code IN ('MAX', 'CGF')
),

dest_mv AS (
    SELECT policy_number, manufacturing_company_code,
           extract_date, market_value, number_of_units
    FROM p_ictech_discovery.p_ic_an_foundation_fact_wealth_assets
    WHERE source_system = 'CLIML_SAS'
      AND extract_date BETWEEN '2025-09-01' AND '2026-03-31'
)

SELECT
    DATE_FORMAT(s.extract_date, 'yyyy-MM')           AS report_month,
    s.management_company_code,

    COUNT(DISTINCT s.account_id)                     AS source_account_count,
    COUNT(*)                                         AS source_row_count,
    ROUND(SUM(s.source_market_value), 2)             AS source_mv,

    COUNT(DISTINCT d.policy_number)                  AS dest_account_count,
    COUNT(d.policy_number)                           AS dest_row_count,
    ROUND(SUM(d.market_value), 2)                    AS dest_mv,

    ROUND(SUM(d.market_value)
          - SUM(s.source_market_value), 2)           AS variance_amt,

    ROUND((SUM(d.market_value)
           - SUM(s.source_market_value))
          / NULLIF(SUM(s.source_market_value), 0)
          * 100, 6)                                  AS variance_pct,

    CASE
        WHEN ABS((SUM(d.market_value) - SUM(s.source_market_value))
             / NULLIF(SUM(s.source_market_value), 0)) <= 0.0001 THEN 'MATCH'
        WHEN ABS((SUM(d.market_value) - SUM(s.source_market_value))
             / NULLIF(SUM(s.source_market_value), 0)) <= 0.001  THEN 'INVESTIGATE'
        ELSE 'MISMATCH'
    END                                              AS status

FROM source_mv s
LEFT JOIN dest_mv d
       ON d.policy_number              = s.account_id
      AND d.manufacturing_company_code = s.management_company_code
      AND d.extract_date               = s.extract_date

GROUP BY
    DATE_FORMAT(s.extract_date, 'yyyy-MM'),
    s.management_company_code

ORDER BY report_month, management_company_code;


-- ============================================================
-- SCENARIO 3 of 5
-- Report Month + Fund Code + Management Company
-- Most granular financial breakdown before going record-level.
-- Shows source_price vs dest_price and source_units vs dest_units
-- so you can see exactly WHERE the variance is coming from:
--   PRICE MISMATCH  = CAD conversion went wrong for this fund
--   UNITS MISMATCH  = wrong shares_owned_amount joined
--   MV MISMATCH     = both are off
-- One row per month per fund per company.
-- ============================================================

WITH source_mv AS (

    SELECT
        afp.account_id,
        afp.management_company_code,
        afp.fund_code,
        TO_DATE(afp.file_date, 'yyMMdd')               AS extract_date,
        afp.shares_owned_amount                        AS shares,
        fp.fund_price_currency,
        CASE WHEN fp.fund_price_currency = 'U'
             THEN COALESCE(fp.fund_price * exr.currency_exchange_rate, 0)
             ELSE COALESCE(fp.fund_price, 0)
        END                                            AS cad_price,
        afp.shares_owned_amount
            * CASE WHEN fp.fund_price_currency = 'U'
                   THEN COALESCE(fp.fund_price * exr.currency_exchange_rate, 0)
                   ELSE COALESCE(fp.fund_price, 0)
              END                                      AS source_market_value
    FROM p_ictech_discovery.e_purchased_product_sas_account_fund_position_monthly afp
    INNER JOIN p_ictech_discovery.e_product_shelf_sas_fund_price fp
            ON afp.fund_code              = fp.fund_code
           AND afp.management_company_code = fp.management_company_code
           AND TO_DATE(afp.file_date, 'yyMMdd') = fp.fund_price_date
    LEFT JOIN p_ictech_discovery.e_reference_usd_daily_exchange_rate exr
           ON fp.fund_price_date = exr.currency_exchange_date
    WHERE TO_DATE(afp.file_date, 'yyMMdd') BETWEEN '2025-09-01' AND '2026-03-31'

    UNION ALL

    SELECT
        afp.account_id,
        afp.management_company_code,
        afp.fund_code,
        fp.fund_price_date,
        afp.shares_owned_amount,
        fp.fund_price_currency,
        CASE WHEN fp.fund_price_currency = 'U'
             THEN COALESCE(fp.fund_price * exr.currency_exchange_rate, 0)
             ELSE COALESCE(fp.fund_price, 0)
        END,
        afp.shares_owned_amount
            * CASE WHEN fp.fund_price_currency = 'U'
                   THEN COALESCE(fp.fund_price * exr.currency_exchange_rate, 0)
                   ELSE COALESCE(fp.fund_price, 0)
              END
    FROM p_ictech_discovery.e_purchased_product_sas_account_fund_position afp
    INNER JOIN p_ictech_discovery.e_product_shelf_sas_fund_price fp
            ON afp.fund_code               = fp.fund_code
           AND afp.management_company_code  = fp.management_company_code
           AND fp.fund_price_date BETWEEN afp.effective_date AND afp.end_date
    LEFT JOIN p_ictech_discovery.e_reference_usd_daily_exchange_rate exr
           ON fp.fund_price_date = exr.currency_exchange_date
    WHERE fp.fund_price_date BETWEEN '2025-09-01' AND '2026-03-31'
      AND afp.management_company_code IN ('MAX', 'CGF')
),

dest_mv AS (
    SELECT policy_number, manufacturing_company_code,
           extract_date, market_value, number_of_units, unit_value
    FROM p_ictech_discovery.p_ic_an_foundation_fact_wealth_assets
    WHERE source_system = 'CLIML_SAS'
      AND extract_date BETWEEN '2025-09-01' AND '2026-03-31'
)

SELECT
    DATE_FORMAT(s.extract_date, 'yyyy-MM')            AS report_month,
    s.management_company_code,
    s.fund_code,
    MAX(s.fund_price_currency)                        AS currency_flag,

    -- Source (raw tables)
    COUNT(DISTINCT s.account_id)                      AS source_account_count,
    ROUND(SUM(s.shares), 4)                           AS source_total_units,
    ROUND(MAX(s.cad_price), 6)                        AS source_cad_price,
    ROUND(SUM(s.source_market_value), 2)              AS source_mv,

    -- Destination (final table)
    COUNT(DISTINCT d.policy_number)                   AS dest_account_count,
    ROUND(SUM(d.number_of_units), 4)                  AS dest_total_units,
    ROUND(MAX(d.unit_value), 6)                       AS dest_unit_value,
    ROUND(SUM(d.market_value), 2)                     AS dest_mv,

    -- Variance breakdown — tells you what is wrong
    ROUND(MAX(d.unit_value) - MAX(s.cad_price), 6)    AS price_diff,
    ROUND(SUM(d.number_of_units)
          - SUM(s.shares), 4)                         AS units_diff,
    ROUND(SUM(d.market_value)
          - SUM(s.source_market_value), 2)            AS mv_variance_amt,
    ROUND((SUM(d.market_value) - SUM(s.source_market_value))
          / NULLIF(SUM(s.source_market_value), 0)
          * 100, 6)                                   AS mv_variance_pct,

    -- Root cause flag
    CASE
        WHEN ABS(MAX(d.unit_value) - MAX(s.cad_price)) > 0.01
             THEN 'PRICE MISMATCH'
        WHEN ABS(SUM(d.number_of_units) - SUM(s.shares)) > 0.01
             THEN 'UNITS MISMATCH'
        WHEN ABS((SUM(d.market_value) - SUM(s.source_market_value))
             / NULLIF(SUM(s.source_market_value), 0)) <= 0.0001
             THEN 'MATCH'
        WHEN ABS((SUM(d.market_value) - SUM(s.source_market_value))
             / NULLIF(SUM(s.source_market_value), 0)) <= 0.001
             THEN 'INVESTIGATE'
        ELSE 'MV MISMATCH'
    END                                               AS status

FROM source_mv s
LEFT JOIN dest_mv d
       ON d.policy_number              = s.account_id
      AND d.manufacturing_company_code = s.management_company_code
      AND d.extract_date               = s.extract_date

GROUP BY
    DATE_FORMAT(s.extract_date, 'yyyy-MM'),
    s.management_company_code,
    s.fund_code

ORDER BY
    report_month,
    ABS(ROUND(SUM(d.market_value) - SUM(s.source_market_value), 2)) DESC;


-- ============================================================
-- SCENARIO 4 of 5
-- Report Month + Dealer Code
-- Dealer-level MV from raw source vs final table per month.
-- dealer_code in source comes from sas_fund_accounts (fa),
-- joined on account_id + management_company_code + date range.
-- dealer_code in destination comes from fact_wealth_assets.
-- One row per month per dealer.
-- ============================================================

WITH source_mv AS (

    -- MONTHLY path with dealer
    SELECT
        afp.account_id,
        afp.management_company_code,
        afp.fund_code,
        TO_DATE(afp.file_date, 'yyMMdd')               AS extract_date,
        afp.shares_owned_amount                        AS shares,
        afp.shares_owned_amount
            * CASE WHEN fp.fund_price_currency = 'U'
                   THEN COALESCE(fp.fund_price * exr.currency_exchange_rate, 0)
                   ELSE COALESCE(fp.fund_price, 0)
              END                                      AS source_market_value,
        fa.dealer_code
    FROM p_ictech_discovery.e_purchased_product_sas_account_fund_position_monthly afp
    INNER JOIN p_ictech_discovery.e_product_shelf_sas_fund_price fp
            ON afp.fund_code              = fp.fund_code
           AND afp.management_company_code = fp.management_company_code
           AND TO_DATE(afp.file_date, 'yyMMdd') = fp.fund_price_date
    LEFT JOIN p_ictech_discovery.e_reference_usd_daily_exchange_rate exr
           ON fp.fund_price_date = exr.currency_exchange_date
    LEFT JOIN p_ictech_discovery.e_purchased_product_sas_fund_accounts fa
           ON afp.account_id              = fa.account_id
          AND afp.management_company_code = fa.management_company_code
          AND TO_DATE(afp.file_date, 'yyMMdd')
              BETWEEN fa.effective_date AND fa.end_date
    WHERE TO_DATE(afp.file_date, 'yyMMdd') BETWEEN '2025-09-01' AND '2026-03-31'

    UNION ALL

    -- DAILY path with dealer
    SELECT
        afp.account_id,
        afp.management_company_code,
        afp.fund_code,
        fp.fund_price_date,
        afp.shares_owned_amount,
        afp.shares_owned_amount
            * CASE WHEN fp.fund_price_currency = 'U'
                   THEN COALESCE(fp.fund_price * exr.currency_exchange_rate, 0)
                   ELSE COALESCE(fp.fund_price, 0)
              END,
        fa.dealer_code
    FROM p_ictech_discovery.e_purchased_product_sas_account_fund_position afp
    INNER JOIN p_ictech_discovery.e_product_shelf_sas_fund_price fp
            ON afp.fund_code               = fp.fund_code
           AND afp.management_company_code  = fp.management_company_code
           AND fp.fund_price_date BETWEEN afp.effective_date AND afp.end_date
    LEFT JOIN p_ictech_discovery.e_reference_usd_daily_exchange_rate exr
           ON fp.fund_price_date = exr.currency_exchange_date
    LEFT JOIN p_ictech_discovery.e_purchased_product_sas_fund_accounts fa
           ON afp.account_id              = fa.account_id
          AND afp.management_company_code = fa.management_company_code
          AND fp.fund_price_date BETWEEN fa.effective_date AND fa.end_date
    WHERE fp.fund_price_date BETWEEN '2025-09-01' AND '2026-03-31'
      AND afp.management_company_code IN ('MAX', 'CGF')
),

dest_mv AS (
    SELECT policy_number, manufacturing_company_code,
           extract_date, market_value, dealer_code
    FROM p_ictech_discovery.p_ic_an_foundation_fact_wealth_assets
    WHERE source_system = 'CLIML_SAS'
      AND extract_date BETWEEN '2025-09-01' AND '2026-03-31'
)

SELECT
    DATE_FORMAT(s.extract_date, 'yyyy-MM')            AS report_month,
    s.dealer_code                                     AS source_dealer_code,
    MAX(d.dealer_code)                                AS dest_dealer_code,

    COUNT(DISTINCT s.account_id)                      AS source_account_count,
    COUNT(*)                                          AS source_row_count,
    ROUND(SUM(s.source_market_value), 2)              AS source_mv,

    COUNT(DISTINCT d.policy_number)                   AS dest_account_count,
    COUNT(d.policy_number)                            AS dest_row_count,
    ROUND(SUM(d.market_value), 2)                     AS dest_mv,

    ROUND(SUM(d.market_value)
          - SUM(s.source_market_value), 2)            AS variance_amt,
    ROUND((SUM(d.market_value) - SUM(s.source_market_value))
          / NULLIF(SUM(s.source_market_value), 0)
          * 100, 6)                                   AS variance_pct,

    CASE
        WHEN ABS((SUM(d.market_value) - SUM(s.source_market_value))
             / NULLIF(SUM(s.source_market_value), 0)) <= 0.0001 THEN 'MATCH'
        WHEN ABS((SUM(d.market_value) - SUM(s.source_market_value))
             / NULLIF(SUM(s.source_market_value), 0)) <= 0.001  THEN 'INVESTIGATE'
        ELSE 'MISMATCH'
    END                                               AS status

FROM source_mv s
LEFT JOIN dest_mv d
       ON d.policy_number              = s.account_id
      AND d.manufacturing_company_code = s.management_company_code
      AND d.extract_date               = s.extract_date

GROUP BY
    DATE_FORMAT(s.extract_date, 'yyyy-MM'),
    s.dealer_code

ORDER BY
    report_month,
    source_mv DESC;


-- ============================================================
-- SCENARIO 5 of 5
-- Report Month + Extract Path (Monthly File vs Daily File)
-- Did the monthly file records and daily file records each
-- contribute the right MV to the final table?
-- In the ETL: monthly positions → monthly_extract_indicator = TRUE
--             daily positions  → monthly_extract_indicator = FALSE
-- We compare each path separately using that indicator.
-- If MONTHLY path matches but DAILY path does not → daily join
-- to fund price has date range issue.
-- If MONTHLY path fails → file_date conversion is wrong.
-- ============================================================

WITH source_monthly AS (
    SELECT
        afp.account_id,
        afp.management_company_code,
        TO_DATE(afp.file_date, 'yyMMdd')               AS extract_date,
        afp.shares_owned_amount                        AS shares,
        afp.shares_owned_amount
            * CASE WHEN fp.fund_price_currency = 'U'
                   THEN COALESCE(fp.fund_price * exr.currency_exchange_rate, 0)
                   ELSE COALESCE(fp.fund_price, 0)
              END                                      AS source_market_value,
        TRUE                                           AS is_monthly
    FROM p_ictech_discovery.e_purchased_product_sas_account_fund_position_monthly afp
    INNER JOIN p_ictech_discovery.e_product_shelf_sas_fund_price fp
            ON afp.fund_code              = fp.fund_code
           AND afp.management_company_code = fp.management_company_code
           AND TO_DATE(afp.file_date, 'yyMMdd') = fp.fund_price_date
    LEFT JOIN p_ictech_discovery.e_reference_usd_daily_exchange_rate exr
           ON fp.fund_price_date = exr.currency_exchange_date
    WHERE TO_DATE(afp.file_date, 'yyMMdd') BETWEEN '2025-09-01' AND '2026-03-31'
),

source_daily AS (
    SELECT
        afp.account_id,
        afp.management_company_code,
        fp.fund_price_date                             AS extract_date,
        afp.shares_owned_amount                        AS shares,
        afp.shares_owned_amount
            * CASE WHEN fp.fund_price_currency = 'U'
                   THEN COALESCE(fp.fund_price * exr.currency_exchange_rate, 0)
                   ELSE COALESCE(fp.fund_price, 0)
              END                                      AS source_market_value,
        FALSE                                          AS is_monthly
    FROM p_ictech_discovery.e_purchased_product_sas_account_fund_position afp
    INNER JOIN p_ictech_discovery.e_product_shelf_sas_fund_price fp
            ON afp.fund_code               = fp.fund_code
           AND afp.management_company_code  = fp.management_company_code
           AND fp.fund_price_date BETWEEN afp.effective_date AND afp.end_date
    LEFT JOIN p_ictech_discovery.e_reference_usd_daily_exchange_rate exr
           ON fp.fund_price_date = exr.currency_exchange_date
    WHERE fp.fund_price_date BETWEEN '2025-09-01' AND '2026-03-31'
      AND afp.management_company_code IN ('MAX', 'CGF')
),

source_all AS (
    SELECT * FROM source_monthly
    UNION ALL
    SELECT * FROM source_daily
),

dest_mv AS (
    SELECT
        policy_number, manufacturing_company_code,
        extract_date, market_value, number_of_units,
        monthly_extract_indicator
    FROM p_ictech_discovery.p_ic_an_foundation_fact_wealth_assets
    WHERE source_system = 'CLIML_SAS'
      AND extract_date BETWEEN '2025-09-01' AND '2026-03-31'
)

SELECT
    DATE_FORMAT(s.extract_date, 'yyyy-MM')            AS report_month,

    CASE WHEN s.is_monthly = TRUE
         THEN 'Monthly File (sas_account_fund_position_monthly)'
         ELSE 'Daily File  (sas_account_fund_position)'
    END                                               AS source_path,

    -- Source
    COUNT(DISTINCT s.account_id)                      AS source_account_count,
    COUNT(*)                                          AS source_row_count,
    ROUND(SUM(s.shares), 2)                           AS source_total_units,
    ROUND(SUM(s.source_market_value), 2)              AS source_mv,

    -- Destination (matched by indicator)
    COUNT(DISTINCT d.policy_number)                   AS dest_account_count,
    COUNT(d.policy_number)                            AS dest_row_count,
    ROUND(SUM(d.number_of_units), 2)                  AS dest_total_units,
    ROUND(SUM(d.market_value), 2)                     AS dest_mv,

    -- Variance
    ROUND(SUM(d.market_value)
          - SUM(s.source_market_value), 2)            AS variance_amt,
    ROUND((SUM(d.market_value) - SUM(s.source_market_value))
          / NULLIF(SUM(s.source_market_value), 0)
          * 100, 6)                                   AS variance_pct,
    ROUND(SUM(d.number_of_units)
          - SUM(s.shares), 4)                         AS units_variance,

    CASE
        WHEN ABS((SUM(d.market_value) - SUM(s.source_market_value))
             / NULLIF(SUM(s.source_market_value), 0)) <= 0.0001 THEN 'MATCH'
        WHEN ABS((SUM(d.market_value) - SUM(s.source_market_value))
             / NULLIF(SUM(s.source_market_value), 0)) <= 0.001  THEN 'INVESTIGATE'
        ELSE 'MISMATCH'
    END                                               AS status

FROM source_all s
LEFT JOIN dest_mv d
       ON d.policy_number                = s.account_id
      AND d.manufacturing_company_code   = s.management_company_code
      AND d.extract_date                 = s.extract_date
      AND d.monthly_extract_indicator    = s.is_monthly

GROUP BY
    DATE_FORMAT(s.extract_date, 'yyyy-MM'),
    s.is_monthly

ORDER BY
    report_month,
    s.is_monthly DESC;
