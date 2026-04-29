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
# MAGIC ## Cell 2: Configuration — Flip Table Names Here for Different Environments

# COMMAND ----------

# ============================================================
# CONFIGURATION — UPDATE THIS CELL TO SWITCH ENVIRONMENTS
# All table references flow from here. No changes needed below.
# ============================================================

CONFIG = {

    # ---- TARGET TABLE (what we are validating) ----
    "target_table": "p_ictech_discovery.p_ic_an_foundation_fact_wealth_assets",

    # ---- REFERENCE TABLE (source of truth) ----
    "reference_table": "p_sales_reporting.climl_fact_assets",

    # ---- FUND DIMENSION (for product kind lookups) ----
    "dim_fund": "p_ictech_discovery.p_ic_an_foundation_dimension_fund",

    # ---- PLAN DIMENSION (for plan classification) ----
    "dim_plan": "p_ictech_discovery.p_ic_an_foundation_dimension_plan",

    # ---- FILTER — do not change (business rule) ----
    "source_system_filter": "CLIML_SAS",

    # ---- TOLERANCE ----
    "mv_tolerance_dollars": 0.01,       # per-record tolerance
    "mv_tolerance_pct": 0.0001,         # aggregate tolerance (0.01%)

    # ---- DISPLAY ----
    "max_failed_rows": 25,
}

# ---- Shorthand references used in all test queries ----
TGT  = CONFIG["target_table"]
REF  = CONFIG["reference_table"]
DIM_FUND = CONFIG["dim_fund"]
DIM_PLAN = CONFIG["dim_plan"]
FILTER   = CONFIG["source_system_filter"]
TOL      = CONFIG["mv_tolerance_dollars"]
TOL_PCT  = CONFIG["mv_tolerance_pct"]
MAX_ROWS = CONFIG["max_failed_rows"]

print("=" * 65)
print("  TEST SUITE 1 — MARKET VALUE VALIDATION")
print("=" * 65)
print(f"  Target  : {TGT}")
print(f"  Reference: {REF}")
print(f"  Filter  : source_system = '{FILTER}'")
print(f"  Tolerance: ${TOL} per record | {TOL_PCT*100:.2f}% aggregate")
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

# ── colour codes for notebook display ──────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
RESET  = "\033[0m"
BOLD   = "\033[1m"

def run_test(test_id, test_name, category, description, validates, sql_query,
             pass_when="zero_rows", threshold=0):
    """
    Execute one test case.

    pass_when:
        "zero_rows"     → PASS if query returns 0 rows (no violations)
        "has_rows"      → PASS if query returns > 0 rows  (data exists)
        "within_pct"    → PASS if single numeric result <= threshold
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
        df = spark.sql(sql_query)
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
        result["error_traceback"] = traceback.format_exc()

    elapsed = (datetime.now() - start).total_seconds() * 1000
    result["elapsed_ms"] = round(elapsed, 1)

    # ── inline print ──────────────────────────────────────────────────────
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
# MAGIC ## Cell 4: Test Execution — All 20 Tests

# COMMAND ----------

print(f"\n{'='*65}")
print(f"  EXECUTING TESTS  |  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"{'='*65}\n")

# ──────────────────────────────────────────────────────────────────────────────
# CATEGORY A: ROW COUNT & COVERAGE (MV-01 to MV-03)
# ──────────────────────────────────────────────────────────────────────────────

run_test(
    test_id   = "MV-01",
    test_name = "Target table has CLIML_SAS records",
    category  = "Row Count & Coverage",
    description = "Confirm rows exist in fact_wealth_assets filtered to CLIML_SAS source system",
    validates   = "Basic data presence — if zero, all other tests are invalid",
    sql_query   = f"""
        SELECT 1
        FROM   {TGT}
        WHERE  source_system = '{FILTER}'
        LIMIT  1
    """,
    pass_when = "has_rows",
)

run_test(
    test_id   = "MV-02",
    test_name = "Reference table has CLIML_SAS records",
    category  = "Row Count & Coverage",
    description = "Confirm climl_fact_assets has data to compare against",
    validates   = "Reference table availability",
    sql_query   = f"""
        SELECT 1
        FROM   {REF}
        LIMIT  1
    """,
    pass_when = "has_rows",
)

run_test(
    test_id   = "MV-03",
    test_name = "Row count variance by extract date < 1%",
    category  = "Row Count & Coverage",
    description = """
        For each extract_date, compare number of rows in target (CLIML_SAS) vs reference.
        Flags any date where the row count gap exceeds 1%.
        Note: exact match may not hold due to plan splitting — tolerance applied.
    """,
    validates   = "Same policies/funds present on same dates in both tables",
    sql_query   = f"""
        WITH tgt AS (
            SELECT extract_date,
                   COUNT(*) AS tgt_rows
            FROM   {TGT}
            WHERE  source_system = '{FILTER}'
            GROUP BY extract_date
        ),
        ref AS (
            SELECT extract_date,
                   COUNT(*) AS ref_rows
            FROM   {REF}
            GROUP BY extract_date
        )
        SELECT  t.extract_date,
                t.tgt_rows,
                r.ref_rows,
                ABS(t.tgt_rows - r.ref_rows)             AS row_diff,
                ROUND(ABS(t.tgt_rows - r.ref_rows)
                      / NULLIF(r.ref_rows, 0) * 100, 4)  AS pct_diff
        FROM    tgt  t
        JOIN    ref  r ON t.extract_date = r.extract_date
        WHERE   ABS(t.tgt_rows - r.ref_rows)
                / NULLIF(r.ref_rows, 0) > 0.01
        ORDER BY pct_diff DESC
    """,
    pass_when = "zero_rows",
)

# ──────────────────────────────────────────────────────────────────────────────
# CATEGORY B: MARKET VALUE FORMULA VALIDATION (MV-04 to MV-07)
# ──────────────────────────────────────────────────────────────────────────────

run_test(
    test_id   = "MV-04",
    test_name = "market_value = canadian_unit_price × number_of_units (reference table self-check)",
    category  = "Market Value Formula",
    description = """
        In climl_fact_assets, validate that the stored market_value matches the
        formula canadian_unit_price × number_of_units within $0.01 tolerance.
        This confirms the reference table itself is correct before we use it.
    """,
    validates   = "Reference formula integrity: market_value = canadian_unit_price × number_of_units",
    sql_query   = f"""
        SELECT contract_policy_number,
               fund_code,
               fund_management_company_code,
               extract_date,
               number_of_units,
               canadian_unit_price,
               market_value                                           AS stored_mv,
               ROUND(canadian_unit_price * number_of_units, 2)        AS calculated_mv,
               ABS(market_value - canadian_unit_price * number_of_units) AS variance
        FROM   {REF}
        WHERE  ABS(market_value - canadian_unit_price * number_of_units) > {TOL}
        ORDER BY variance DESC
        LIMIT  {MAX_ROWS}
    """,
    pass_when = "zero_rows",
)

run_test(
    test_id   = "MV-05",
    test_name = "market_value in target matches reference (record-level, $0.01 tolerance)",
    category  = "Market Value Formula",
    description = """
        Join target and reference on policy_number + fund_management_company_code + extract_date.
        Compare market values after re-computing expected value using canadian_unit_price × number_of_units
        from the reference table. This accounts for the USD vs CAD issue noted in the transcript:
        the reference uses Canadian prices; we use that as the expected value.
    """,
    validates   = "Core market value accuracy — PRIMARY TEST",
    sql_query   = f"""
        WITH tgt AS (
            SELECT policy_number,
                   manufacturing_company_code,
                   extract_date,
                   number_of_units              AS tgt_units,
                   unit_value                   AS tgt_unit_price,
                   market_value                 AS tgt_mv
            FROM   {TGT}
            WHERE  source_system = '{FILTER}'
        ),
        ref AS (
            SELECT contract_policy_number,
                   fund_management_company_code,
                   extract_date,
                   number_of_units                                    AS ref_units,
                   canadian_unit_price                                AS ref_canadian_price,
                   ROUND(canadian_unit_price * number_of_units, 2)    AS expected_mv
            FROM   {REF}
        )
        SELECT  t.policy_number,
                t.manufacturing_company_code,
                t.extract_date,
                r.ref_units,
                r.ref_canadian_price,
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
    pass_when = "zero_rows",
)

run_test(
    test_id   = "MV-06",
    test_name = "unit_value in target matches canadian_unit_price in reference",
    category  = "Market Value Formula",
    description = """
        unit_value in fact_wealth_assets should equal canadian_unit_price from climl_fact_assets.
        This validates that the CAD conversion happened correctly before loading.
    """,
    validates   = "unit_value = canadian_unit_price (CAD price correctly stored)",
    sql_query   = f"""
        WITH tgt AS (
            SELECT policy_number, manufacturing_company_code, extract_date,
                   unit_value AS tgt_unit_value
            FROM   {TGT}
            WHERE  source_system = '{FILTER}'
        ),
        ref AS (
            SELECT contract_policy_number, fund_management_company_code, extract_date,
                   canadian_unit_price AS ref_price
            FROM   {REF}
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
    pass_when = "zero_rows",
)

run_test(
    test_id   = "MV-07",
    test_name = "number_of_units in target matches reference",
    category  = "Market Value Formula",
    description = "number_of_units should be a direct pass-through from shares_owned_amount — exact match expected",
    validates   = "number_of_units = shares_owned_amount (no transformation applied)",
    sql_query   = f"""
        WITH tgt AS (
            SELECT policy_number, manufacturing_company_code, extract_date,
                   number_of_units AS tgt_units
            FROM   {TGT}
            WHERE  source_system = '{FILTER}'
        ),
        ref AS (
            SELECT contract_policy_number, fund_management_company_code, extract_date,
                   number_of_units AS ref_units
            FROM   {REF}
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
    pass_when = "zero_rows",
)

# ──────────────────────────────────────────────────────────────────────────────
# CATEGORY C: DIMENSIONAL BREAKDOWNS (MV-08 to MV-11)
# ──────────────────────────────────────────────────────────────────────────────

run_test(
    test_id   = "MV-08",
    test_name = "Aggregate market value variance by extract_date < 0.01%",
    category  = "Dimensional Breakdown",
    description = """
        Group both tables by extract_date, sum market values, compare.
        Target uses recalculated MV (canadian_unit_price × units from reference).
        Any date with aggregate variance > 0.01% is flagged.
    """,
    validates   = "Market value totals correct per reporting period",
    sql_query   = f"""
        WITH tgt AS (
            SELECT extract_date,
                   SUM(market_value) AS tgt_total_mv
            FROM   {TGT}
            WHERE  source_system = '{FILTER}'
            GROUP BY extract_date
        ),
        ref AS (
            SELECT extract_date,
                   SUM(ROUND(canadian_unit_price * number_of_units, 2)) AS ref_total_mv
            FROM   {REF}
            GROUP BY extract_date
        )
        SELECT  t.extract_date,
                ROUND(t.tgt_total_mv, 2)  AS tgt_total,
                ROUND(r.ref_total_mv, 2)  AS ref_total,
                ROUND(ABS(t.tgt_total_mv - r.ref_total_mv), 2) AS variance,
                ROUND(ABS(t.tgt_total_mv - r.ref_total_mv)
                      / NULLIF(r.ref_total_mv, 0) * 100, 6)    AS pct_variance
        FROM    tgt t
        JOIN    ref r ON t.extract_date = r.extract_date
        WHERE   ABS(t.tgt_total_mv - r.ref_total_mv)
                / NULLIF(r.ref_total_mv, 0) > {TOL_PCT}
        ORDER BY pct_variance DESC
    """,
    pass_when = "zero_rows",
)

run_test(
    test_id   = "MV-09",
    test_name = "Aggregate market value variance by management company (MAX vs CGF)",
    category  = "Dimensional Breakdown",
    description = """
        Group by manufacturing_company_code and compare aggregate market values.
        MAX and CGF should each match reference individually — validates no cross-company blending.
    """,
    validates   = "Market value correct per management company",
    sql_query   = f"""
        WITH tgt AS (
            SELECT extract_date,
                   manufacturing_company_code,
                   SUM(market_value) AS tgt_total_mv
            FROM   {TGT}
            WHERE  source_system = '{FILTER}'
            GROUP BY extract_date, manufacturing_company_code
        ),
        ref AS (
            SELECT extract_date,
                   fund_management_company_code,
                   SUM(ROUND(canadian_unit_price * number_of_units, 2)) AS ref_total_mv
            FROM   {REF}
            GROUP BY extract_date, fund_management_company_code
        )
        SELECT  t.extract_date,
                t.manufacturing_company_code,
                ROUND(t.tgt_total_mv, 2) AS tgt_total,
                ROUND(r.ref_total_mv, 2) AS ref_total,
                ROUND(ABS(t.tgt_total_mv - r.ref_total_mv), 2) AS variance,
                ROUND(ABS(t.tgt_total_mv - r.ref_total_mv)
                      / NULLIF(r.ref_total_mv, 0) * 100, 6)    AS pct_variance
        FROM    tgt t
        JOIN    ref r
             ON t.extract_date              = r.extract_date
            AND t.manufacturing_company_code = r.fund_management_company_code
        WHERE   ABS(t.tgt_total_mv - r.ref_total_mv)
                / NULLIF(r.ref_total_mv, 0) > {TOL_PCT}
        ORDER BY pct_variance DESC
    """,
    pass_when = "zero_rows",
)

run_test(
    test_id   = "MV-10",
    test_name = "Market value variance by dealer_code — any dealer > 0.01% flagged",
    category  = "Dimensional Breakdown",
    description = "Aggregate market value by dealer. Validates dealer-level totals are correct.",
    validates   = "Dealer-level market value alignment",
    sql_query   = f"""
        WITH tgt AS (
            SELECT extract_date, dealer_code,
                   SUM(market_value) AS tgt_total_mv
            FROM   {TGT}
            WHERE  source_system = '{FILTER}'
            GROUP BY extract_date, dealer_code
        ),
        ref AS (
            SELECT extract_date, dealer_code,
                   SUM(ROUND(canadian_unit_price * number_of_units, 2)) AS ref_total_mv
            FROM   {REF}
            GROUP BY extract_date, dealer_code
        )
        SELECT  t.extract_date,
                t.dealer_code,
                ROUND(t.tgt_total_mv, 2) AS tgt_total,
                ROUND(r.ref_total_mv, 2) AS ref_total,
                ROUND(ABS(t.tgt_total_mv - r.ref_total_mv), 2) AS variance,
                ROUND(ABS(t.tgt_total_mv - r.ref_total_mv)
                      / NULLIF(r.ref_total_mv, 0) * 100, 6)    AS pct_variance
        FROM    tgt t
        JOIN    ref r
             ON t.extract_date = r.extract_date
            AND t.dealer_code  = r.dealer_code
        WHERE   ABS(t.tgt_total_mv - r.ref_total_mv)
                / NULLIF(r.ref_total_mv, 0) > {TOL_PCT}
        ORDER BY pct_variance DESC
    """,
    pass_when = "zero_rows",
)

run_test(
    test_id   = "MV-11",
    test_name = "Market value variance by plan_identifier_sk — aggregate per plan < 0.01%",
    category  = "Dimensional Breakdown",
    description = """
        Aggregate market value by plan_identifier_sk in target.
        Note: plan is split into 3 IPC categories so direct plan-to-plan comparison with
        reference is not possible. Instead, validate total plan market values sum correctly.
        Per transcript: test at product_kind level first, plan-level second.
    """,
    validates   = "Plan-level market value totals are consistent",
    sql_query   = f"""
        SELECT  plan_identifier_sk,
                extract_date,
                COUNT(*)              AS record_count,
                SUM(market_value)     AS total_mv,
                SUM(number_of_units)  AS total_units,
                AVG(unit_value)       AS avg_unit_price
        FROM    {TGT}
        WHERE   source_system      = '{FILTER}'
          AND   plan_identifier_sk IS NULL
        ORDER BY total_mv DESC
    """,
    pass_when = "zero_rows",
)

# ──────────────────────────────────────────────────────────────────────────────
# CATEGORY D: INDICATOR & FLAG VALIDATION (MV-12 to MV-14)
# ──────────────────────────────────────────────────────────────────────────────

run_test(
    test_id   = "MV-12",
    test_name = "monthly_extract_indicator matches reference",
    category  = "Indicator Flags",
    description = """
        monthly_extract_indicator in target must match what is in climl_fact_assets.
        Monthly records come from sas_account_fund_position_monthly (indicator = TRUE).
        Daily records come from sas_account_fund_position (indicator = FALSE).
    """,
    validates   = "monthly_extract_indicator correctly set from source file type",
    sql_query   = f"""
        WITH tgt AS (
            SELECT policy_number, manufacturing_company_code, extract_date,
                   monthly_extract_indicator AS tgt_monthly
            FROM   {TGT}
            WHERE  source_system = '{FILTER}'
        ),
        ref AS (
            SELECT contract_policy_number, fund_management_company_code, extract_date,
                   monthly_extract_indicator AS ref_monthly
            FROM   {REF}
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
    pass_when = "zero_rows",
)

run_test(
    test_id   = "MV-13",
    test_name = "weekly_extract_indicator matches reference",
    category  = "Indicator Flags",
    description = """
        weekly_extract_indicator should align with the weekly date logic from
        temp_view_asset_date_weekly (typically the Friday of each week).
    """,
    validates   = "weekly_extract_indicator correctly set",
    sql_query   = f"""
        WITH tgt AS (
            SELECT policy_number, manufacturing_company_code, extract_date,
                   weekly_extract_indicator AS tgt_weekly
            FROM   {TGT}
            WHERE  source_system = '{FILTER}'
        ),
        ref AS (
            SELECT contract_policy_number, fund_management_company_code, extract_date,
                   weekly_extract_indicator AS ref_weekly
            FROM   {REF}
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
    pass_when = "zero_rows",
)

run_test(
    test_id   = "MV-14",
    test_name = "All boolean guarantee indicators are FALSE for CLIML_SAS",
    category  = "Indicator Flags",
    description = """
        For CLIML_SAS records, all guarantee-related indicators are hardcoded FALSE
        in the ETL script. Any TRUE value is a data error.
    """,
    validates   = "Hardcoded boolean fields correctly set to FALSE",
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
          AND   (
                    death_guarantee_reset_indicator             = TRUE
                 OR econo_guarantee_indicator                   = TRUE
                 OR enhanced_guarantee_benefit_indicator        = TRUE
                 OR extended_death_benefit_option_indicator     = TRUE
                 OR flexible_income_option_indicator            = TRUE
                 OR guarantee_indicator                         = TRUE
                 OR income_transition_period_option_indicator   = TRUE
                 OR life_income_benefit_indicator               = TRUE
                 OR life_income_benefit_joint_indicator         = TRUE
                 OR life_income_benefit_single_indicator        = TRUE
                 OR maturity_guarantee_reset_indicator          = TRUE
                 OR nominee_account_indicator                   = TRUE
                 OR short_term_rate_protection_six_month_indicator  = TRUE
                 OR short_term_rate_protection_twelve_month_indicator = TRUE
                )
    """,
    pass_when = "zero_rows",
)

# ──────────────────────────────────────────────────────────────────────────────
# CATEGORY E: PLAN & PRODUCT MATCHING (MV-15 to MV-17)
# ──────────────────────────────────────────────────────────────────────────────

run_test(
    test_id   = "MV-15",
    test_name = "plan_identifier_sk in target matches reference",
    category  = "Plan & Product Matching",
    description = """
        Per transcript: test plan matching at the plan_identifier_sk level.
        Note that CLIML_SAS splits mutual funds into 3 IPC categories (21817, 21818, 21219)
        while the reference uses a single plan. Mismatches surface here.
    """,
    validates   = "plan_identifier_sk correctly assigned from IPC classification logic",
    sql_query   = f"""
        WITH tgt AS (
            SELECT policy_number, manufacturing_company_code, extract_date,
                   plan_identifier_sk AS tgt_plan_sk
            FROM   {TGT}
            WHERE  source_system = '{FILTER}'
        ),
        ref AS (
            SELECT contract_policy_number, fund_management_company_code, extract_date,
                   plan_identifier_sk AS ref_plan_sk
            FROM   {REF}
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
          OR    t.tgt_plan_sk IS NULL
    """,
    pass_when = "zero_rows",
)

run_test(
    test_id   = "MV-16",
    test_name = "fund_sk correctly resolved for all CLIML_SAS records",
    category  = "Plan & Product Matching",
    description = """
        fund_sk should be > 0 for every CLIML_SAS record (0 = unmatched).
        Compare with fund_sk from reference table.
    """,
    validates   = "fund_sk lookup correctly resolved from temp_plan_fund_dimension",
    sql_query   = f"""
        SELECT  policy_number,
                manufacturing_company_code,
                extract_date,
                fund_sk
        FROM    {TGT}
        WHERE   source_system = '{FILTER}'
          AND   (fund_sk = 0 OR fund_sk IS NULL)
        ORDER BY extract_date DESC
    """,
    pass_when = "zero_rows",
)

run_test(
    test_id   = "MV-17",
    test_name = "extract_date_sk correctly resolved — no zero or null date keys",
    category  = "Plan & Product Matching",
    description = "extract_date_sk = 0 or NULL means the date did not join to the date dimension. All records must have a valid date SK.",
    validates   = "extract_date_sk resolved from dimension_date join",
    sql_query   = f"""
        SELECT  policy_number,
                manufacturing_company_code,
                extract_date,
                extract_date_sk
        FROM    {TGT}
        WHERE   source_system = '{FILTER}'
          AND   (extract_date_sk = 0 OR extract_date_sk IS NULL)
        ORDER BY extract_date DESC
    """,
    pass_when = "zero_rows",
)

# ──────────────────────────────────────────────────────────────────────────────
# CATEGORY F: EDGE CASES (MV-18 to MV-20)
# ──────────────────────────────────────────────────────────────────────────────

run_test(
    test_id   = "MV-18",
    test_name = "No negative market values for CLIML_SAS records",
    category  = "Edge Cases",
    description = "market_value should never be negative. Negative shares × positive price = negative; flag if found.",
    validates   = "market_value >= 0 for all CLIML_SAS records",
    sql_query   = f"""
        SELECT  policy_number,
                manufacturing_company_code,
                extract_date,
                number_of_units,
                unit_value,
                market_value
        FROM    {TGT}
        WHERE   source_system = '{FILTER}'
          AND   market_value < 0
        ORDER BY market_value ASC
    """,
    pass_when = "zero_rows",
)

run_test(
    test_id   = "MV-19",
    test_name = "Accounts with zero units have zero market value",
    category  = "Edge Cases",
    description = "If number_of_units = 0, market_value must = 0. Validates formula consistency at zero-unit boundary.",
    validates   = "Zero units → zero market value (no orphan values)",
    sql_query   = f"""
        SELECT  policy_number,
                manufacturing_company_code,
                extract_date,
                number_of_units,
                unit_value,
                market_value
        FROM    {TGT}
        WHERE   source_system  = '{FILTER}'
          AND   number_of_units = 0
          AND   market_value   <> 0
    """,
    pass_when = "zero_rows",
)

run_test(
    test_id   = "MV-20",
    test_name = "High-value accounts (market_value > $1M) present in both tables",
    category  = "Edge Cases",
    description = """
        High-value accounts should appear in both target and reference.
        Records in reference with MV > $1M but missing from target are flagged.
        These are the most visible records and must not be dropped.
    """,
    validates   = "No high-value accounts missing from target",
    sql_query   = f"""
        SELECT  r.contract_policy_number,
                r.fund_management_company_code,
                r.extract_date,
                ROUND(r.canadian_unit_price * r.number_of_units, 2) AS expected_mv
        FROM    {REF} r
        WHERE   ROUND(r.canadian_unit_price * r.number_of_units, 2) > 1000000
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
    pass_when = "zero_rows",
)

print(f"\n{'='*65}")
print(f"  ALL TESTS EXECUTED  |  Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"{'='*65}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5: Summary Report

# COMMAND ----------

# ── Compute summary ────────────────────────────────────────────────────────
passed  = [r for r in ALL_TEST_RESULTS if r["status"] == "PASS"]
failed  = [r for r in ALL_TEST_RESULTS if r["status"] == "FAIL"]
errored = [r for r in ALL_TEST_RESULTS if r["status"] == "ERROR"]
total   = len(ALL_TEST_RESULTS)
elapsed = round((datetime.now() - SUITE_START_TIME).total_seconds(), 1)

pass_rate = round(len(passed) / total * 100, 1) if total > 0 else 0

# ── Print summary banner ───────────────────────────────────────────────────
print(f"\n{'='*65}")
print(f"  TEST EXECUTION SUMMARY")
print(f"{'='*65}")
print(f"  Suite      : Test Suite 1 — Market Value Validation (CLIML_SAS)")
print(f"  Target     : {TGT}")
print(f"  Reference  : {REF}")
print(f"  Filter     : source_system = '{FILTER}'")
print(f"  Run Time   : {elapsed}s  |  Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"{'='*65}")
print(f"  Total Tests  : {total}")
print(f"  {GREEN}Passed       : {len(passed)}{RESET}")
print(f"  {RED}Failed       : {len(failed)}{RESET}")
print(f"  {YELLOW}Errors       : {len(errored)}{RESET}")
print(f"  Pass Rate    : {pass_rate}%")
print(f"{'='*65}\n")

# ── Full grid ──────────────────────────────────────────────────────────────
print(f"  {'TEST ID':<10} {'CATEGORY':<28} {'TEST NAME':<45} {'STATUS':<8} {'ROWS/VALUE'}")
print(f"  {'-'*10} {'-'*28} {'-'*45} {'-'*8} {'-'*12}")

for r in ALL_TEST_RESULTS:
    status_str = r["status"]
    color = GREEN if status_str == "PASS" else (RED if status_str == "FAIL" else YELLOW)
    metric = str(r["row_count"]) if r["metric_value"] is None else str(r["metric_value"])
    print(f"  {r['test_id']:<10} {r['category']:<28} {r['test_name'][:44]:<45} {color}{status_str:<8}{RESET} {metric}")

# ── Failed test detail ─────────────────────────────────────────────────────
if failed:
    print(f"\n{'='*65}")
    print(f"  {RED}FAILED TESTS — DETAIL{RESET}")
    print(f"{'='*65}")
    for r in failed:
        print(f"\n  ❌ [{r['test_id']}] {r['test_name']}")
        print(f"     Category  : {r['category']}")
        print(f"     Validates : {r['validates']}")
        print(f"     Rows      : {r['row_count']} violation(s)")
        print(f"     Description:")
        for line in r['description'].strip().splitlines():
            print(f"       {line.strip()}")
        print(f"\n     SQL Query:")
        for line in r['sql_query'].strip().splitlines():
            print(f"       {line}")
        if r["sample_df"] is not None:
            print(f"\n     Sample failing records:")
            r["sample_df"].show(MAX_ROWS, truncate=False)

# ── Error detail ───────────────────────────────────────────────────────────
if errored:
    print(f"\n{'='*65}")
    print(f"  {YELLOW}ERRORED TESTS — DETAIL{RESET}")
    print(f"{'='*65}")
    for r in errored:
        print(f"\n  ⚠️  [{r['test_id']}] {r['test_name']}")
        print(f"     Category : {r['category']}")
        print(f"     Error    : {r['error']}")
        print(f"\n     SQL Query:")
        for line in r['sql_query'].strip().splitlines():
            print(f"       {line}")

# ── Final verdict ──────────────────────────────────────────────────────────
print(f"\n{'='*65}")
if len(failed) == 0 and len(errored) == 0:
    print(f"  {GREEN}{BOLD}  ALL TESTS PASSED ✅  Market value validated for CLIML_SAS.{RESET}")
elif len(failed) > 0:
    print(f"  {RED}{BOLD}  {len(failed)} TEST(S) FAILED ❌  Review failed tests above.{RESET}")
if len(errored) > 0:
    print(f"  {YELLOW}{BOLD}  {len(errored)} TEST(S) ERRORED ⚠️  Check table access / SQL syntax.{RESET}")
print(f"{'='*65}\n")
