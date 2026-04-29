# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Test Suite 1: Market Value Validation — CLIML SAS
# MAGIC ## IPC Phase 2.1 Testing | fact_wealth_assets ↔ climl_fact_assets
# MAGIC
# MAGIC **Purpose:** Validate that market values in the target wealth assets table match the
# MAGIC reference CLIML fact assets table, using the canonical formula:
# MAGIC `market_value = canadian_unit_price × number_of_units`
# MAGIC
# MAGIC **Scope:** Only records where `source_system = 'CLIML_SAS'` (uppercase)
# MAGIC **Date Window:** Configurable in Cell 2 — default last 6 months (Sep 2025 – Mar 2026)
# MAGIC
# MAGIC **Test Categories:**
# MAGIC - MV-01 to MV-03 → Row Count & Coverage
# MAGIC - MV-04 to MV-07 → Market Value Formula Validation
# MAGIC - MV-08 to MV-11 → Dimensional Breakdowns (Date, Company, Fund, Dealer)
# MAGIC - MV-12 to MV-14 → Indicator & Flag Validation
# MAGIC - MV-15 to MV-17 → Plan & Product Matching
# MAGIC - MV-18 to MV-20 → Edge Cases
# MAGIC
# MAGIC **Total Tests:** 20
# MAGIC **Tolerance:** Individual record variance < $0.01 | Aggregate variance < 0.01%
# MAGIC
# MAGIC ---
# MAGIC **Author:** Mahi | IPC Phase 2.1 | April 2026

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2: Configuration — Flip Table Names / Date Window Here

# COMMAND ----------

# ============================================================
# CONFIGURATION — UPDATE THIS CELL TO SWITCH ENVIRONMENTS
# All table references AND date filters flow from here.
# No changes needed in any other cell.
# ============================================================

CONFIG = {

    # ---- TARGET TABLE (what we are validating) ----
    "target_table":    "p_ictech_discovery.p_ic_an_foundation_fact_wealth_assets",

    # ---- REFERENCE TABLE (source of truth) ----
    "reference_table": "p_sales_reporting.climl_fact_assets",

    # ---- FUND DIMENSION (for product kind lookups) ----
    "dim_fund": "p_ictech_discovery.p_ic_an_foundation_dimension_fund",

    # ---- PLAN DIMENSION (for plan classification) ----
    "dim_plan": "p_ictech_discovery.p_ic_an_foundation_dimension_plan",

    # ---- SOURCE SYSTEM FILTER — do not change (business rule) ----
    "source_system_filter": "CLIML_SAS",

    # ---- DATE WINDOW — change these to shift the test window ----
    # Default: last 6 months  (Sep 2025 → Mar 2026)
    "date_from": "2025-09-01",
    "date_to":   "2026-03-31",

    # ---- TOLERANCE ----
    "mv_tolerance_dollars": 0.01,    # per-record variance threshold
    "mv_tolerance_pct":     0.0001,  # aggregate variance threshold (0.01%)

    # ---- DISPLAY ----
    "max_failed_rows": 25,
}

# ── Shorthand references — used in every query below ──────────────────────
TGT      = CONFIG["target_table"]
REF      = CONFIG["reference_table"]
DIM_FUND = CONFIG["dim_fund"]
DIM_PLAN = CONFIG["dim_plan"]
FILTER   = CONFIG["source_system_filter"]
D_FROM   = CONFIG["date_from"]
D_TO     = CONFIG["date_to"]
TOL      = CONFIG["mv_tolerance_dollars"]
TOL_PCT  = CONFIG["mv_tolerance_pct"]
MAX_ROWS = CONFIG["max_failed_rows"]

# ── Reusable date clause snippets ─────────────────────────────────────────
# Paste TGT_DATE or REF_DATE into any WHERE block — keeps SQL consistent.
TGT_DATE = f"extract_date BETWEEN '{D_FROM}' AND '{D_TO}'"
REF_DATE = f"extract_date BETWEEN '{D_FROM}' AND '{D_TO}'"

print("=" * 65)
print("  TEST SUITE 1 — MARKET VALUE VALIDATION")
print("=" * 65)
print(f"  Target    : {TGT}")
print(f"  Reference : {REF}")
print(f"  Filter    : source_system = '{FILTER}'")
print(f"  Date From : {D_FROM}")
print(f"  Date To   : {D_TO}")
print(f"  Tolerance : ${TOL} per record | {TOL_PCT*100:.2f}% aggregate")
print("=" * 65)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3: Test Framework — Helper Functions & Runner

# COMMAND ----------

from datetime import datetime
from pyspark.sql import functions as F
import traceback

ALL_TEST_RESULTS = []
SUITE_START_TIME = datetime.now()

GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
RESET  = "\033[0m"
BOLD   = "\033[1m"

def run_test(test_id, test_name, category, description, validates, sql_query,
             pass_when="zero_rows", threshold=0):
    """
    Execute one test case.
    pass_when:
        'zero_rows'  → PASS if query returns 0 rows  (no violations)
        'has_rows'   → PASS if query returns > 0 rows (data exists)
        'within_pct' → PASS if single numeric result <= threshold
    """
    start = datetime.now()
    result = {
        "test_id"      : test_id,
        "test_name"    : test_name,
        "category"     : category,
        "description"  : description,
        "validates"    : validates,
        "sql_query"    : sql_query,
        "pass_when"    : pass_when,
        "status"       : None,
        "row_count"    : 0,
        "metric_value" : None,
        "error"        : None,
        "sample_df"    : None,
        "elapsed_ms"   : 0,
    }
    try:
        df    = spark.sql(sql_query)
        count = df.count()
        result["row_count"] = count

        if pass_when == "zero_rows":
            result["status"] = "PASS" if count == 0 else "FAIL"
            if count > 0:
                result["sample_df"] = df.limit(MAX_ROWS)

        elif pass_when == "has_rows":
            result["status"] = "PASS" if count > 0 else "FAIL"

        elif pass_when == "within_pct":
            val = df.collect()[0][0]
            result["metric_value"] = val
            result["status"] = "PASS" if (val is not None and float(val) <= threshold) else "FAIL"

    except Exception as e:
        result["status"] = "ERROR"
        result["error"]  = f"{type(e).__name__}: {str(e)}"

    elapsed = (datetime.now() - start).total_seconds() * 1000
    result["elapsed_ms"] = round(elapsed, 1)

    icon  = "✅" if result["status"] == "PASS" else ("❌" if result["status"] == "FAIL" else "⚠️ ")
    color = GREEN if result["status"] == "PASS" else (RED if result["status"] == "FAIL" else YELLOW)
    metric = f" | rows={result['row_count']}" if pass_when != "within_pct" else f" | value={result['metric_value']}"
    print(f"{icon} {color}[{test_id}]{RESET} {test_name}{metric} ({result['elapsed_ms']}ms) → {color}{result['status']}{RESET}")

    if result["status"] == "FAIL" and result["sample_df"] is not None:
        print(f"   {YELLOW}▶ Sample failing records (up to {MAX_ROWS}):{RESET}")
        result["sample_df"].show(MAX_ROWS, truncate=False)

    if result["status"] == "ERROR":
        print(f"   {RED}▶ Error: {result['error']}{RESET}")

    ALL_TEST_RESULTS.append(result)
    return result

print("✔  Test framework loaded. Ready to execute tests.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4: Test Execution — All 20 Tests | Window: Sep 2025 – Mar 2026

# COMMAND ----------

print(f"\n{'='*65}")
print(f"  EXECUTING TESTS  |  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"  Date Window      : {D_FROM} → {D_TO}")
print(f"{'='*65}\n")

# ─────────────────────────────────────────────────────────────────────────────
# CATEGORY A — ROW COUNT & COVERAGE  (MV-01 to MV-03)
# ─────────────────────────────────────────────────────────────────────────────

run_test(
    test_id     = "MV-01",
    test_name   = "Target has CLIML_SAS records in date window",
    category    = "Row Count & Coverage",
    description = "Confirm rows exist in fact_wealth_assets for CLIML_SAS within the 6-month window. If zero, all other tests are invalid.",
    validates   = "Basic data presence in date window",
    sql_query   = f"""
        SELECT 1
        FROM   {TGT}
        WHERE  source_system = '{FILTER}'
          AND  {TGT_DATE}
        LIMIT  1
    """,
    pass_when   = "has_rows",
)

run_test(
    test_id     = "MV-02",
    test_name   = "Reference table has records in date window",
    category    = "Row Count & Coverage",
    description = "Confirm climl_fact_assets has data within the same 6-month window to compare against.",
    validates   = "Reference table availability for the date window",
    sql_query   = f"""
        SELECT 1
        FROM   {REF}
        WHERE  {REF_DATE}
        LIMIT  1
    """,
    pass_when   = "has_rows",
)

run_test(
    test_id     = "MV-03",
    test_name   = "Row count variance by extract_date < 1% (within window)",
    category    = "Row Count & Coverage",
    description = "For each extract_date in the window, compare row counts between target (CLIML_SAS) and reference. Flags any date where the gap exceeds 1%. Note: exact match may not hold due to IPC plan splitting — tolerance applied.",
    validates   = "Same policies/funds present on same dates in both tables",
    sql_query   = f"""
        WITH tgt AS (
            SELECT extract_date, COUNT(*) AS tgt_rows
            FROM   {TGT}
            WHERE  source_system = '{FILTER}'
              AND  {TGT_DATE}
            GROUP BY extract_date
        ),
        ref AS (
            SELECT extract_date, COUNT(*) AS ref_rows
            FROM   {REF}
            WHERE  {REF_DATE}
            GROUP BY extract_date
        )
        SELECT  t.extract_date,
                t.tgt_rows,
                r.ref_rows,
                ABS(t.tgt_rows - r.ref_rows)                              AS row_diff,
                ROUND(ABS(t.tgt_rows - r.ref_rows)
                      / NULLIF(r.ref_rows,0) * 100, 4)                    AS pct_diff
        FROM    tgt t
        JOIN    ref r ON t.extract_date = r.extract_date
        WHERE   ABS(t.tgt_rows - r.ref_rows) / NULLIF(r.ref_rows,0) > 0.01
        ORDER BY pct_diff DESC
    """,
    pass_when   = "zero_rows",
)

# ─────────────────────────────────────────────────────────────────────────────
# CATEGORY B — MARKET VALUE FORMULA  (MV-04 to MV-07)
# ─────────────────────────────────────────────────────────────────────────────

run_test(
    test_id     = "MV-04",
    test_name   = "Reference self-check: market_value = canadian_unit_price × units",
    category    = "Market Value Formula",
    description = "Before using the reference as source of truth, validate its own market_value matches the formula canadian_unit_price × number_of_units within $0.01 tolerance. Only checks the 6-month window.",
    validates   = "Reference formula integrity: market_value = canadian_unit_price × number_of_units",
    sql_query   = f"""
        SELECT  contract_policy_number,
                fund_code,
                fund_management_company_code,
                extract_date,
                number_of_units,
                canadian_unit_price,
                market_value                                            AS stored_mv,
                ROUND(canadian_unit_price * number_of_units, 2)         AS calculated_mv,
                ABS(market_value - canadian_unit_price * number_of_units) AS variance
        FROM    {REF}
        WHERE   {REF_DATE}
          AND   ABS(market_value - canadian_unit_price * number_of_units) > {TOL}
        ORDER BY variance DESC
        LIMIT   {MAX_ROWS}
    """,
    pass_when   = "zero_rows",
)

run_test(
    test_id     = "MV-05",
    test_name   = "market_value in target matches reference (record-level, $0.01 tolerance)",
    category    = "Market Value Formula",
    description = "Join target and reference on policy_number + management_company + extract_date within the 6-month window. Expected value = canadian_unit_price × number_of_units from reference (CAD). This is the PRIMARY market value test.",
    validates   = "Core market value accuracy — PRIMARY TEST",
    sql_query   = f"""
        WITH tgt AS (
            SELECT policy_number,
                   manufacturing_company_code,
                   extract_date,
                   number_of_units  AS tgt_units,
                   unit_value       AS tgt_unit_price,
                   market_value     AS tgt_mv
            FROM   {TGT}
            WHERE  source_system = '{FILTER}'
              AND  {TGT_DATE}
        ),
        ref AS (
            SELECT contract_policy_number,
                   fund_management_company_code,
                   extract_date,
                   number_of_units                                 AS ref_units,
                   canadian_unit_price                             AS ref_cad_price,
                   ROUND(canadian_unit_price * number_of_units, 2) AS expected_mv
            FROM   {REF}
            WHERE  {REF_DATE}
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
        WHERE   ABS(t.tgt_mv - r.expected_mv) > {TOL}
        ORDER BY variance DESC
    """,
    pass_when   = "zero_rows",
)

run_test(
    test_id     = "MV-06",
    test_name   = "unit_value in target = canadian_unit_price in reference",
    category    = "Market Value Formula",
    description = "unit_value stored in fact_wealth_assets must equal canadian_unit_price from climl_fact_assets. Validates CAD conversion was applied before loading. Checked within 6-month window.",
    validates   = "unit_value = canadian_unit_price (CAD price correctly stored)",
    sql_query   = f"""
        WITH tgt AS (
            SELECT policy_number, manufacturing_company_code, extract_date,
                   unit_value AS tgt_unit_value
            FROM   {TGT}
            WHERE  source_system = '{FILTER}'
              AND  {TGT_DATE}
        ),
        ref AS (
            SELECT contract_policy_number, fund_management_company_code, extract_date,
                   canadian_unit_price AS ref_price
            FROM   {REF}
            WHERE  {REF_DATE}
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
        WHERE   ABS(t.tgt_unit_value - r.ref_price) > {TOL}
        ORDER BY variance DESC
    """,
    pass_when   = "zero_rows",
)

run_test(
    test_id     = "MV-07",
    test_name   = "number_of_units in target = number_of_units in reference",
    category    = "Market Value Formula",
    description = "number_of_units is a direct pass-through from shares_owned_amount — exact match expected. Checked within 6-month window.",
    validates   = "number_of_units passed through without transformation",
    sql_query   = f"""
        WITH tgt AS (
            SELECT policy_number, manufacturing_company_code, extract_date,
                   number_of_units AS tgt_units
            FROM   {TGT}
            WHERE  source_system = '{FILTER}'
              AND  {TGT_DATE}
        ),
        ref AS (
            SELECT contract_policy_number, fund_management_company_code, extract_date,
                   number_of_units AS ref_units
            FROM   {REF}
            WHERE  {REF_DATE}
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
        WHERE   ABS(t.tgt_units - r.ref_units) > {TOL}
        ORDER BY diff DESC
    """,
    pass_when   = "zero_rows",
)

# ─────────────────────────────────────────────────────────────────────────────
# CATEGORY C — DIMENSIONAL BREAKDOWNS  (MV-08 to MV-11)
# ─────────────────────────────────────────────────────────────────────────────

run_test(
    test_id     = "MV-08",
    test_name   = "Aggregate market value variance by extract_date < 0.01%",
    category    = "Dimensional Breakdown",
    description = "Group by extract_date within the 6-month window, sum market values, compare. Flags any date where aggregate variance > 0.01%.",
    validates   = "Market value totals correct per reporting period",
    sql_query   = f"""
        WITH tgt AS (
            SELECT extract_date, SUM(market_value) AS tgt_total
            FROM   {TGT}
            WHERE  source_system = '{FILTER}'
              AND  {TGT_DATE}
            GROUP BY extract_date
        ),
        ref AS (
            SELECT extract_date,
                   SUM(ROUND(canadian_unit_price * number_of_units, 2)) AS ref_total
            FROM   {REF}
            WHERE  {REF_DATE}
            GROUP BY extract_date
        )
        SELECT  t.extract_date,
                ROUND(t.tgt_total, 2) AS tgt_total,
                ROUND(r.ref_total, 2) AS ref_total,
                ROUND(ABS(t.tgt_total - r.ref_total), 2)                  AS variance,
                ROUND(ABS(t.tgt_total - r.ref_total)
                      / NULLIF(r.ref_total,0) * 100, 6)                    AS pct_variance
        FROM    tgt t
        JOIN    ref r ON t.extract_date = r.extract_date
        WHERE   ABS(t.tgt_total - r.ref_total) / NULLIF(r.ref_total,0) > {TOL_PCT}
        ORDER BY pct_variance DESC
    """,
    pass_when   = "zero_rows",
)

run_test(
    test_id     = "MV-09",
    test_name   = "Aggregate market value by management company (MAX vs CGF) < 0.01%",
    category    = "Dimensional Breakdown",
    description = "Group by extract_date + manufacturing_company_code within the window. MAX and CGF validated separately — ensures no cross-company blending.",
    validates   = "Market value correct per management company",
    sql_query   = f"""
        WITH tgt AS (
            SELECT extract_date, manufacturing_company_code,
                   SUM(market_value) AS tgt_total
            FROM   {TGT}
            WHERE  source_system = '{FILTER}'
              AND  {TGT_DATE}
            GROUP BY extract_date, manufacturing_company_code
        ),
        ref AS (
            SELECT extract_date, fund_management_company_code,
                   SUM(ROUND(canadian_unit_price * number_of_units, 2)) AS ref_total
            FROM   {REF}
            WHERE  {REF_DATE}
            GROUP BY extract_date, fund_management_company_code
        )
        SELECT  t.extract_date,
                t.manufacturing_company_code,
                ROUND(t.tgt_total, 2) AS tgt_total,
                ROUND(r.ref_total, 2) AS ref_total,
                ROUND(ABS(t.tgt_total - r.ref_total), 2)             AS variance,
                ROUND(ABS(t.tgt_total - r.ref_total)
                      / NULLIF(r.ref_total,0) * 100, 6)               AS pct_variance
        FROM    tgt t
        JOIN    ref r
             ON t.extract_date               = r.extract_date
            AND t.manufacturing_company_code = r.fund_management_company_code
        WHERE   ABS(t.tgt_total - r.ref_total) / NULLIF(r.ref_total,0) > {TOL_PCT}
        ORDER BY pct_variance DESC
    """,
    pass_when   = "zero_rows",
)

run_test(
    test_id     = "MV-10",
    test_name   = "Aggregate market value by dealer_code — variance < 0.01%",
    category    = "Dimensional Breakdown",
    description = "Group by extract_date + dealer_code within the 6-month window. Validates dealer-level totals are correct.",
    validates   = "Dealer-level market value alignment",
    sql_query   = f"""
        WITH tgt AS (
            SELECT extract_date, dealer_code, SUM(market_value) AS tgt_total
            FROM   {TGT}
            WHERE  source_system = '{FILTER}'
              AND  {TGT_DATE}
            GROUP BY extract_date, dealer_code
        ),
        ref AS (
            SELECT extract_date, dealer_code,
                   SUM(ROUND(canadian_unit_price * number_of_units, 2)) AS ref_total
            FROM   {REF}
            WHERE  {REF_DATE}
            GROUP BY extract_date, dealer_code
        )
        SELECT  t.extract_date,
                t.dealer_code,
                ROUND(t.tgt_total, 2) AS tgt_total,
                ROUND(r.ref_total, 2) AS ref_total,
                ROUND(ABS(t.tgt_total - r.ref_total), 2)             AS variance,
                ROUND(ABS(t.tgt_total - r.ref_total)
                      / NULLIF(r.ref_total,0) * 100, 6)               AS pct_variance
        FROM    tgt t
        JOIN    ref r ON t.extract_date = r.extract_date
                     AND t.dealer_code  = r.dealer_code
        WHERE   ABS(t.tgt_total - r.ref_total) / NULLIF(r.ref_total,0) > {TOL_PCT}
        ORDER BY pct_variance DESC
    """,
    pass_when   = "zero_rows",
)

run_test(
    test_id     = "MV-11",
    test_name   = "No NULL plan_identifier_sk within date window",
    category    = "Dimensional Breakdown",
    description = "Every CLIML_SAS record in the window must have a non-null plan_identifier_sk. NULL = fund was not classified by temp_plan_fund_dimension.",
    validates   = "Plan-level classification applied to all records",
    sql_query   = f"""
        SELECT  policy_number,
                manufacturing_company_code,
                extract_date,
                plan_identifier_sk
        FROM    {TGT}
        WHERE   source_system      = '{FILTER}'
          AND   {TGT_DATE}
          AND   plan_identifier_sk IS NULL
        ORDER BY extract_date DESC
    """,
    pass_when   = "zero_rows",
)

# ─────────────────────────────────────────────────────────────────────────────
# CATEGORY D — INDICATOR & FLAG VALIDATION  (MV-12 to MV-14)
# ─────────────────────────────────────────────────────────────────────────────

run_test(
    test_id     = "MV-12",
    test_name   = "monthly_extract_indicator matches reference (within window)",
    category    = "Indicator Flags",
    description = "monthly_extract_indicator in target must match reference within the 6-month window. Monthly records (from sas_account_fund_position_monthly) = TRUE; daily = FALSE.",
    validates   = "monthly_extract_indicator correctly set from source file type",
    sql_query   = f"""
        WITH tgt AS (
            SELECT policy_number, manufacturing_company_code, extract_date,
                   monthly_extract_indicator AS tgt_monthly
            FROM   {TGT}
            WHERE  source_system = '{FILTER}'
              AND  {TGT_DATE}
        ),
        ref AS (
            SELECT contract_policy_number, fund_management_company_code, extract_date,
                   monthly_extract_indicator AS ref_monthly
            FROM   {REF}
            WHERE  {REF_DATE}
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
        WHERE   t.tgt_monthly <> r.ref_monthly
    """,
    pass_when   = "zero_rows",
)

run_test(
    test_id     = "MV-13",
    test_name   = "weekly_extract_indicator matches reference (within window)",
    category    = "Indicator Flags",
    description = "weekly_extract_indicator must align with the weekly date logic (typically the Friday of each week) within the 6-month window.",
    validates   = "weekly_extract_indicator correctly derived from temp_view_asset_date_weekly",
    sql_query   = f"""
        WITH tgt AS (
            SELECT policy_number, manufacturing_company_code, extract_date,
                   weekly_extract_indicator AS tgt_weekly
            FROM   {TGT}
            WHERE  source_system = '{FILTER}'
              AND  {TGT_DATE}
        ),
        ref AS (
            SELECT contract_policy_number, fund_management_company_code, extract_date,
                   weekly_extract_indicator AS ref_weekly
            FROM   {REF}
            WHERE  {REF_DATE}
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
        WHERE   t.tgt_weekly <> r.ref_weekly
    """,
    pass_when   = "zero_rows",
)

run_test(
    test_id     = "MV-14",
    test_name   = "All guarantee boolean indicators = FALSE for CLIML_SAS (within window)",
    category    = "Indicator Flags",
    description = "For CLIML_SAS records in the window, all guarantee indicators are hardcoded FALSE in the ETL. Any TRUE value is a data error.",
    validates   = "Hardcoded boolean guarantee fields = FALSE",
    sql_query   = f"""
        SELECT  policy_number, extract_date, manufacturing_company_code,
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
        FROM    {TGT}
        WHERE   source_system = '{FILTER}'
          AND   {TGT_DATE}
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
                 OR short_term_rate_protection_six_month_indicator  = TRUE
                 OR short_term_rate_protection_twelve_month_indicator = TRUE
                )
    """,
    pass_when   = "zero_rows",
)

# ─────────────────────────────────────────────────────────────────────────────
# CATEGORY E — PLAN & PRODUCT MATCHING  (MV-15 to MV-17)
# ─────────────────────────────────────────────────────────────────────────────

run_test(
    test_id     = "MV-15",
    test_name   = "plan_identifier_sk in target matches reference (within window)",
    category    = "Plan & Product Matching",
    description = "Per transcript: test plan matching at plan_identifier_sk level within the window. Note: CLIML_SAS splits mutual funds into 3 IPC categories (21817, 21818, 21219) which may not match reference 1-to-1. Mismatches surface here.",
    validates   = "plan_identifier_sk correctly assigned from IPC classification",
    sql_query   = f"""
        WITH tgt AS (
            SELECT policy_number, manufacturing_company_code, extract_date,
                   plan_identifier_sk AS tgt_plan_sk
            FROM   {TGT}
            WHERE  source_system = '{FILTER}'
              AND  {TGT_DATE}
        ),
        ref AS (
            SELECT contract_policy_number, fund_management_company_code, extract_date,
                   plan_identifier_sk AS ref_plan_sk
            FROM   {REF}
            WHERE  {REF_DATE}
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
           OR   t.tgt_plan_sk IS NULL
    """,
    pass_when   = "zero_rows",
)

run_test(
    test_id     = "MV-16",
    test_name   = "fund_sk > 0 for all CLIML_SAS records in window",
    category    = "Plan & Product Matching",
    description = "fund_sk = 0 or NULL means the fund was not resolved from temp_plan_fund_dimension. Every record in the window must have a valid fund_sk.",
    validates   = "fund_sk resolved correctly — no unmatched funds",
    sql_query   = f"""
        SELECT  policy_number,
                manufacturing_company_code,
                extract_date,
                fund_sk
        FROM    {TGT}
        WHERE   source_system = '{FILTER}'
          AND   {TGT_DATE}
          AND   (fund_sk = 0 OR fund_sk IS NULL)
        ORDER BY extract_date DESC
    """,
    pass_when   = "zero_rows",
)

run_test(
    test_id     = "MV-17",
    test_name   = "extract_date_sk > 0 for all records in window",
    category    = "Plan & Product Matching",
    description = "extract_date_sk = 0 or NULL means the asset date failed to join to the date dimension. All records in the window must have a valid date SK.",
    validates   = "extract_date_sk resolved from dimension_date join",
    sql_query   = f"""
        SELECT  policy_number,
                manufacturing_company_code,
                extract_date,
                extract_date_sk
        FROM    {TGT}
        WHERE   source_system = '{FILTER}'
          AND   {TGT_DATE}
          AND   (extract_date_sk = 0 OR extract_date_sk IS NULL)
        ORDER BY extract_date DESC
    """,
    pass_when   = "zero_rows",
)

# ─────────────────────────────────────────────────────────────────────────────
# CATEGORY F — EDGE CASES  (MV-18 to MV-20)
# ─────────────────────────────────────────────────────────────────────────────

run_test(
    test_id     = "MV-18",
    test_name   = "No negative market values in window",
    category    = "Edge Cases",
    description = "market_value should never be negative within the 6-month window. Negative shares × positive price = negative; flag if found.",
    validates   = "market_value >= 0 for all CLIML_SAS records in window",
    sql_query   = f"""
        SELECT  policy_number,
                manufacturing_company_code,
                extract_date,
                number_of_units,
                unit_value,
                market_value
        FROM    {TGT}
        WHERE   source_system = '{FILTER}'
          AND   {TGT_DATE}
          AND   market_value < 0
        ORDER BY market_value ASC
    """,
    pass_when   = "zero_rows",
)

run_test(
    test_id     = "MV-19",
    test_name   = "Zero units → zero market value (within window)",
    category    = "Edge Cases",
    description = "If number_of_units = 0, market_value must = 0. Validates formula consistency at zero-unit boundary within the window.",
    validates   = "Zero units → zero market value",
    sql_query   = f"""
        SELECT  policy_number,
                manufacturing_company_code,
                extract_date,
                number_of_units,
                unit_value,
                market_value
        FROM    {TGT}
        WHERE   source_system   = '{FILTER}'
          AND   {TGT_DATE}
          AND   number_of_units = 0
          AND   market_value   <> 0
    """,
    pass_when   = "zero_rows",
)

run_test(
    test_id     = "MV-20",
    test_name   = "High-value accounts (>$1M) in reference also in target (within window)",
    category    = "Edge Cases",
    description = "High-value accounts in the 6-month window that exist in reference but are missing from target are flagged. These are the most visible records and must not be dropped.",
    validates   = "No high-value accounts dropped from target",
    sql_query   = f"""
        SELECT  r.contract_policy_number,
                r.fund_management_company_code,
                r.extract_date,
                ROUND(r.canadian_unit_price * r.number_of_units, 2) AS expected_mv
        FROM    {REF} r
        WHERE   {REF_DATE}
          AND   ROUND(r.canadian_unit_price * r.number_of_units, 2) > 1000000
          AND   NOT EXISTS (
                    SELECT 1
                    FROM   {TGT} t
                    WHERE  t.policy_number              = r.contract_policy_number
                      AND  t.manufacturing_company_code = r.fund_management_company_code
                      AND  t.extract_date               = r.extract_date
                      AND  t.source_system              = '{FILTER}'
                )
        ORDER BY expected_mv DESC
    """,
    pass_when   = "zero_rows",
)

print(f"\n{'='*65}")
print(f"  ALL TESTS EXECUTED  |  Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"{'='*65}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5: Summary Report

# COMMAND ----------

passed  = [r for r in ALL_TEST_RESULTS if r["status"] == "PASS"]
failed  = [r for r in ALL_TEST_RESULTS if r["status"] == "FAIL"]
errored = [r for r in ALL_TEST_RESULTS if r["status"] == "ERROR"]
total   = len(ALL_TEST_RESULTS)
elapsed = round((datetime.now() - SUITE_START_TIME).total_seconds(), 1)
pass_rate = round(len(passed) / total * 100, 1) if total > 0 else 0

print(f"\n{'='*65}")
print(f"  TEST EXECUTION SUMMARY")
print(f"{'='*65}")
print(f"  Suite      : Test Suite 1 — Market Value Validation (CLIML_SAS)")
print(f"  Target     : {TGT}")
print(f"  Reference  : {REF}")
print(f"  Filter     : source_system = '{FILTER}'")
print(f"  Date Window: {D_FROM}  →  {D_TO}")
print(f"  Run Time   : {elapsed}s  |  Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"{'='*65}")
print(f"  Total Tests : {total}")
print(f"  {GREEN}Passed      : {len(passed)}{RESET}")
print(f"  {RED}Failed      : {len(failed)}{RESET}")
print(f"  {YELLOW}Errors      : {len(errored)}{RESET}")
print(f"  Pass Rate   : {pass_rate}%")
print(f"{'='*65}\n")

print(f"  {'TEST ID':<10} {'CATEGORY':<28} {'TEST NAME':<45} {'STATUS':<8} {'ROWS'}")
print(f"  {'-'*10} {'-'*28} {'-'*45} {'-'*8} {'-'*8}")
for r in ALL_TEST_RESULTS:
    color  = GREEN if r["status"] == "PASS" else (RED if r["status"] == "FAIL" else YELLOW)
    metric = str(r["row_count"]) if r["metric_value"] is None else str(r["metric_value"])
    print(f"  {r['test_id']:<10} {r['category']:<28} {r['test_name'][:44]:<45} {color}{r['status']:<8}{RESET} {metric}")

if failed:
    print(f"\n{'='*65}")
    print(f"  {RED}FAILED TESTS — DETAIL{RESET}")
    print(f"{'='*65}")
    for r in failed:
        print(f"\n  ❌ [{r['test_id']}] {r['test_name']}")
        print(f"     Category  : {r['category']}")
        print(f"     Validates : {r['validates']}")
        print(f"     Rows      : {r['row_count']} violation(s)")
        print(f"     Description: {r['description'].strip()}")
        print(f"\n     SQL:")
        for line in r['sql_query'].strip().splitlines():
            print(f"       {line}")
        if r["sample_df"] is not None:
            print(f"\n     Sample failing records:")
            r["sample_df"].show(MAX_ROWS, truncate=False)

if errored:
    print(f"\n{'='*65}")
    print(f"  {YELLOW}ERRORED TESTS — DETAIL{RESET}")
    print(f"{'='*65}")
    for r in errored:
        print(f"\n  ⚠️  [{r['test_id']}] {r['test_name']}")
        print(f"     Category : {r['category']}")
        print(f"     Error    : {r['error']}")
        print(f"\n     SQL:")
        for line in r['sql_query'].strip().splitlines():
            print(f"       {line}")

print(f"\n{'='*65}")
if not failed and not errored:
    print(f"  {GREEN}{BOLD}  ALL TESTS PASSED ✅  Market value validated for CLIML_SAS.{RESET}")
if failed:
    print(f"  {RED}{BOLD}  {len(failed)} TEST(S) FAILED ❌  Review failed tests above.{RESET}")
if errored:
    print(f"  {YELLOW}{BOLD}  {len(errored)} TEST(S) ERRORED ⚠️  Check table access / SQL syntax.{RESET}")
print(f"{'='*65}\n")
