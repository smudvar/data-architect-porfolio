-- ============================================================
-- ADVANCED WINDOW FUNCTIONS: CLINICAL ANALYTICS
-- Dialect: ANSI SQL (PostgreSQL / Snowflake / BigQuery compatible)
-- Purpose: Population health metrics, readmission risk, provider
--          performance — all using window functions
-- ============================================================


-- ────────────────────────────────────────────────────────────────
-- 1. ROLLING 30-DAY READMISSION DETECTION
--    For each encounter, determine if the patient was readmitted
--    within 30 days of discharge (CMS quality metric).
-- ────────────────────────────────────────────────────────────────
WITH ranked_encounters AS (
    SELECT
        fe.encounter_id,
        fe.patient_key,
        dd_admit.full_date                          AS admit_date,
        dd_disch.full_date                          AS discharge_date,
        fe.encounter_type,
        fe.primary_diagnosis_key,
        -- Look at the NEXT admission for this patient
        LEAD(dd_admit.full_date)
            OVER (PARTITION BY fe.patient_key
                  ORDER BY     dd_admit.full_date)  AS next_admit_date,
        LEAD(fe.encounter_type)
            OVER (PARTITION BY fe.patient_key
                  ORDER BY     dd_admit.full_date)  AS next_encounter_type
    FROM   fact_encounter fe
    JOIN   dim_date        dd_admit ON dd_admit.date_key = fe.admit_date_key
    JOIN   dim_date        dd_disch ON dd_disch.date_key = fe.discharge_date_key
    WHERE  fe.encounter_type = 'IP'   -- inpatient only (CMS definition)
)
SELECT
    re.encounter_id,
    re.patient_key,
    re.admit_date,
    re.discharge_date,
    re.next_admit_date,
    CASE
        WHEN re.next_admit_date IS NOT NULL
         AND re.next_encounter_type = 'IP'
         AND re.next_admit_date - re.discharge_date <= 30
        THEN TRUE
        ELSE FALSE
    END                                             AS is_30d_readmission,
    re.next_admit_date - re.discharge_date          AS days_to_readmit
FROM   ranked_encounters re
ORDER BY re.patient_key, re.admit_date;


-- ────────────────────────────────────────────────────────────────
-- 2. PROVIDER PERFORMANCE SCORECARD
--    Rank providers within their specialty on cost and quality
--    using NTILE and PERCENT_RANK window functions.
-- ────────────────────────────────────────────────────────────────
WITH provider_metrics AS (
    SELECT
        dp.provider_key,
        dp.npi,
        dp.provider_name,
        dp.specialty,
        COUNT(DISTINCT fe.encounter_key)            AS total_encounters,
        AVG(fe.length_of_stay_days)                 AS avg_los,
        AVG(fe.total_charges)                       AS avg_charges,
        AVG(fe.total_payments)                      AS avg_payments,
        SUM(fe.readmission_30d_flag::INT)           AS readmissions,
        ROUND(
            SUM(fe.readmission_30d_flag::INT) * 100.0
            / NULLIF(COUNT(*), 0), 2
        )                                           AS readmission_rate_pct,
        SUM(fe.mortality_flag::INT)                 AS mortalities
    FROM   fact_encounter fe
    JOIN   dim_provider   dp ON dp.provider_key = fe.attending_provider_key
    JOIN   dim_date       dd ON dd.date_key      = fe.admit_date_key
    WHERE  dd.year_number = EXTRACT(YEAR FROM CURRENT_DATE)
    GROUP BY dp.provider_key, dp.npi, dp.provider_name, dp.specialty
),
ranked_providers AS (
    SELECT
        pm.*,
        -- Rank within specialty (1 = best)
        RANK()          OVER spec_cost    AS cost_rank,
        RANK()          OVER spec_quality AS quality_rank,
        -- Percentile position (lower readmit = better = higher percentile)
        PERCENT_RANK()  OVER spec_quality AS quality_percentile,
        -- Group into performance tiers (1=top, 4=bottom)
        NTILE(4)        OVER spec_cost    AS cost_quartile,
        NTILE(4)        OVER spec_quality AS quality_quartile,
        -- Running total of encounters within specialty
        SUM(total_encounters)
            OVER (PARTITION BY specialty
                  ORDER BY total_encounters DESC
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                                          AS cumulative_encounters_in_specialty
    FROM provider_metrics pm
    WINDOW
        spec_cost    AS (PARTITION BY specialty ORDER BY avg_charges       ASC),
        spec_quality AS (PARTITION BY specialty ORDER BY readmission_rate_pct ASC)
)
SELECT
    rp.*,
    -- Composite score: equally weight cost and quality quartiles
    ROUND((cost_quartile + quality_quartile) / 2.0, 1) AS composite_tier
FROM   ranked_providers rp
ORDER BY specialty, composite_tier, total_encounters DESC;


-- ────────────────────────────────────────────────────────────────
-- 3. PATIENT CHRONIC CONDITION PROGRESSION
--    Detect when a patient first develops each chronic condition
--    and calculate time-between episodes using GAP analysis.
-- ────────────────────────────────────────────────────────────────
WITH chronic_diagnoses AS (
    SELECT
        fe.patient_key,
        dd.icd10_code,
        dd.icd10_description,
        dd.cms_hcc_category,
        dt.full_date                                AS encounter_date,
        -- First time this patient received this diagnosis
        MIN(dt.full_date)
            OVER (PARTITION BY fe.patient_key, dd.icd10_code)
                                                    AS first_diagnosis_date,
        -- Previous occurrence of same diagnosis
        LAG(dt.full_date)
            OVER (PARTITION BY fe.patient_key, dd.icd10_code
                  ORDER BY     dt.full_date)        AS prev_diagnosis_date,
        -- Cumulative count of this diagnosis per patient
        ROW_NUMBER()
            OVER (PARTITION BY fe.patient_key, dd.icd10_code
                  ORDER BY     dt.full_date)        AS diagnosis_occurrence_num
    FROM   fact_encounter        fe
    JOIN   bridge_encounter_diagnosis bed ON bed.encounter_key = fe.encounter_key
    JOIN   dim_diagnosis          dd  ON dd.diagnosis_key      = bed.diagnosis_key
    JOIN   dim_date               dt  ON dt.date_key           = fe.admit_date_key
    WHERE  dd.is_chronic = TRUE
),
condition_timeline AS (
    SELECT
        cd.*,
        encounter_date - first_diagnosis_date       AS days_since_first_dx,
        encounter_date - prev_diagnosis_date        AS days_since_last_episode,
        -- Flag "gap" if > 365 days since last episode (disease remission proxy)
        CASE
            WHEN encounter_date - prev_diagnosis_date > 365
            THEN TRUE ELSE FALSE
        END                                         AS is_new_episode_after_gap
    FROM chronic_diagnoses cd
)
SELECT
    ct.patient_key,
    ct.icd10_code,
    ct.icd10_description,
    ct.cms_hcc_category,
    ct.first_diagnosis_date,
    ct.encounter_date,
    ct.diagnosis_occurrence_num,
    ct.days_since_first_dx,
    ct.days_since_last_episode,
    ct.is_new_episode_after_gap,
    -- Patient's total chronic condition burden (distinct conditions)
    COUNT(DISTINCT ct.icd10_code)
        OVER (PARTITION BY ct.patient_key)          AS total_chronic_conditions,
    -- Condition burden category
    CASE
        WHEN COUNT(DISTINCT ct.icd10_code)
             OVER (PARTITION BY ct.patient_key) >= 5 THEN 'Complex'
        WHEN COUNT(DISTINCT ct.icd10_code)
             OVER (PARTITION BY ct.patient_key) >= 2 THEN 'Moderate'
        ELSE 'Low'
    END                                             AS condition_burden_category
FROM condition_timeline ct
ORDER BY ct.patient_key, ct.icd10_code, ct.encounter_date;


-- ────────────────────────────────────────────────────────────────
-- 4. MONTHLY REVENUE CYCLE TREND WITH MOVING AVERAGES
--    3-month and 12-month moving averages on payments + denial rate
-- ────────────────────────────────────────────────────────────────
WITH monthly_summary AS (
    SELECT
        dd.year_number,
        dd.month_number,
        DATE_TRUNC('month', dd.full_date)           AS month_start,
        COUNT(*)                                    AS total_claims,
        SUM(fc.billed_amount)                       AS total_billed,
        SUM(fc.paid_amount)                         AS total_paid,
        SUM(CASE WHEN fc.claim_status = 'Denied' THEN 1 ELSE 0 END)
                                                    AS denied_claims,
        ROUND(
            SUM(CASE WHEN fc.claim_status = 'Denied' THEN 1 ELSE 0 END)
            * 100.0 / NULLIF(COUNT(*), 0), 2
        )                                           AS denial_rate_pct
    FROM   fact_claim fc
    JOIN   dim_date   dd ON dd.date_key = fc.service_date_key
    GROUP BY dd.year_number, dd.month_number, DATE_TRUNC('month', dd.full_date)
)
SELECT
    ms.month_start,
    ms.total_claims,
    ms.total_billed,
    ms.total_paid,
    ms.denial_rate_pct,
    -- 3-month moving average (smooths seasonal noise)
    ROUND(
        AVG(ms.total_paid)
            OVER (ORDER BY ms.month_start
                  ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2
    )                                               AS paid_3mo_moving_avg,
    -- 12-month moving average (trend line)
    ROUND(
        AVG(ms.total_paid)
            OVER (ORDER BY ms.month_start
                  ROWS BETWEEN 11 PRECEDING AND CURRENT ROW), 2
    )                                               AS paid_12mo_moving_avg,
    -- Month-over-month growth rate
    ROUND(
        (ms.total_paid - LAG(ms.total_paid) OVER (ORDER BY ms.month_start))
        * 100.0
        / NULLIF(LAG(ms.total_paid) OVER (ORDER BY ms.month_start), 0),
        2
    )                                               AS mom_growth_pct,
    -- Year-over-year comparison (same month last year)
    LAG(ms.total_paid, 12)
        OVER (ORDER BY ms.month_start)              AS prior_year_paid
FROM monthly_summary ms
ORDER BY ms.month_start;
