-- ============================================================
-- DATA QUALITY CHECKS & MONITORING
-- Dialect: Snowflake (ANSI-compatible patterns noted)
-- Purpose: A reusable library of SQL-based data quality checks
--          that run after every ETL load. Results are written
--          to dq_results for trending, alerting, and reporting.
-- ============================================================


-- ── 1. Results table — every check writes one row here ─────────
CREATE TABLE IF NOT EXISTS governance.dq_results (
    check_id            VARCHAR(36)     NOT NULL DEFAULT GEN_RANDOM_UUID(),
    run_id              VARCHAR(36)     NOT NULL,       -- groups all checks in one ETL run
    check_name          VARCHAR(200)    NOT NULL,
    check_category      VARCHAR(50)     NOT NULL,       -- completeness, validity, consistency, timeliness, uniqueness
    target_table        VARCHAR(200)    NOT NULL,
    target_column       VARCHAR(100),
    records_checked     BIGINT          NOT NULL DEFAULT 0,
    records_failed      BIGINT          NOT NULL DEFAULT 0,
    failure_rate_pct    DECIMAL(8,4)    NOT NULL DEFAULT 0,
    threshold_pct       DECIMAL(8,4)    NOT NULL,       -- max acceptable failure rate
    status              VARCHAR(10)     NOT NULL,       -- PASS / WARN / FAIL
    sample_failures     VARIANT,                        -- JSON array of up to 5 failing rows
    checked_at          TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_dq_results PRIMARY KEY (check_id)
)
CLUSTER BY (checked_at::DATE);


-- ── 2. Helper: single macro to evaluate and log any check ───────
-- Called by each check below. Writes one row to dq_results.
CREATE OR REPLACE PROCEDURE governance.sp_log_dq_check(
    p_run_id            VARCHAR,
    p_check_name        VARCHAR,
    p_check_category    VARCHAR,
    p_target_table      VARCHAR,
    p_target_column     VARCHAR,
    p_records_checked   BIGINT,
    p_records_failed    BIGINT,
    p_threshold_pct     DECIMAL,
    p_sample_failures   VARIANT
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_failure_rate  DECIMAL(8,4);
    v_status        VARCHAR(10);
BEGIN
    v_failure_rate := IFF(p_records_checked = 0, 0,
                         ROUND(p_records_failed * 100.0 / p_records_checked, 4));

    v_status := CASE
        WHEN v_failure_rate = 0                        THEN 'PASS'
        WHEN v_failure_rate <= p_threshold_pct         THEN 'WARN'
        ELSE                                                'FAIL'
    END;

    INSERT INTO governance.dq_results (
        run_id, check_name, check_category, target_table, target_column,
        records_checked, records_failed, failure_rate_pct,
        threshold_pct, status, sample_failures, checked_at
    ) VALUES (
        p_run_id, p_check_name, p_check_category, p_target_table, p_target_column,
        p_records_checked, p_records_failed, v_failure_rate,
        p_threshold_pct, v_status, p_sample_failures, CURRENT_TIMESTAMP()
    );

    RETURN v_status;
END;
$$;


-- ════════════════════════════════════════════════════════════════
-- SECTION A: COMPLETENESS CHECKS
-- Are required fields populated?
-- ════════════════════════════════════════════════════════════════

-- A1. Null check on critical patient fields
CREATE OR REPLACE PROCEDURE governance.dq_check_patient_completeness(p_run_id VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_total     BIGINT;
    v_failed    BIGINT;
    v_samples   VARIANT;
BEGIN
    -- Total current patients
    SELECT COUNT(*) INTO v_total
    FROM   edw.dim_patient
    WHERE  is_current_record = TRUE;

    -- Patients missing date_of_birth OR gender
    SELECT COUNT(*) INTO v_failed
    FROM   edw.dim_patient
    WHERE  is_current_record = TRUE
      AND  (date_of_birth IS NULL OR gender IS NULL OR gender NOT IN ('M','F','U'));

    -- Capture up to 5 sample failures for investigation
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
               'patient_id', patient_id,
               'missing_field',
               CASE WHEN date_of_birth IS NULL THEN 'date_of_birth'
                    WHEN gender IS NULL        THEN 'gender'
                    ELSE                            'invalid_gender' END
           )) WITHIN GROUP (ORDER BY patient_id)
    INTO   v_samples
    FROM (
        SELECT patient_id, date_of_birth, gender
        FROM   edw.dim_patient
        WHERE  is_current_record = TRUE
          AND  (date_of_birth IS NULL OR gender IS NULL OR gender NOT IN ('M','F','U'))
        LIMIT 5
    );

    CALL governance.sp_log_dq_check(
        p_run_id, 'patient_completeness_dob_gender', 'completeness',
        'edw.dim_patient', 'date_of_birth, gender',
        v_total, v_failed, 1.0,   -- threshold: max 1% nulls
        v_samples
    );

    RETURN 'done';
END;
$$;


-- ════════════════════════════════════════════════════════════════
-- SECTION B: VALIDITY CHECKS
-- Are values in the expected domain / format?
-- ════════════════════════════════════════════════════════════════

-- B1. ICD-10 format validation (must match A00–Z99 pattern)
CREATE OR REPLACE PROCEDURE governance.dq_check_icd10_format(p_run_id VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_total   BIGINT;
    v_failed  BIGINT;
    v_samples VARIANT;
BEGIN
    SELECT COUNT(*) INTO v_total  FROM edw.dim_diagnosis;

    SELECT COUNT(*) INTO v_failed
    FROM   edw.dim_diagnosis
    WHERE  NOT REGEXP_LIKE(icd10_code, '^[A-Z][0-9]{2}(\\..*)?$');

    SELECT ARRAY_AGG(OBJECT_CONSTRUCT('icd10_code', icd10_code))
           WITHIN GROUP (ORDER BY icd10_code)
    INTO   v_samples
    FROM (
        SELECT icd10_code FROM edw.dim_diagnosis
        WHERE  NOT REGEXP_LIKE(icd10_code, '^[A-Z][0-9]{2}(\\..*)?$')
        LIMIT 5
    );

    CALL governance.sp_log_dq_check(
        p_run_id, 'icd10_code_format_validity', 'validity',
        'edw.dim_diagnosis', 'icd10_code',
        v_total, v_failed, 0.0,   -- threshold: zero tolerance for invalid codes
        v_samples
    );

    RETURN 'done';
END;
$$;


-- B2. NPI format check (must be exactly 10 digits)
CREATE OR REPLACE PROCEDURE governance.dq_check_npi_format(p_run_id VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_total   BIGINT;
    v_failed  BIGINT;
    v_samples VARIANT;
BEGIN
    SELECT COUNT(*) INTO v_total  FROM edw.dim_provider WHERE is_current_record = TRUE;

    SELECT COUNT(*) INTO v_failed
    FROM   edw.dim_provider
    WHERE  is_current_record = TRUE
      AND  NOT REGEXP_LIKE(npi, '^[0-9]{10}$');

    SELECT ARRAY_AGG(OBJECT_CONSTRUCT('npi', npi, 'provider_name', provider_name))
           WITHIN GROUP (ORDER BY npi)
    INTO   v_samples
    FROM (
        SELECT npi, provider_name FROM edw.dim_provider
        WHERE  is_current_record = TRUE
          AND  NOT REGEXP_LIKE(npi, '^[0-9]{10}$')
        LIMIT 5
    );

    CALL governance.sp_log_dq_check(
        p_run_id, 'npi_format_validity', 'validity',
        'edw.dim_provider', 'npi',
        v_total, v_failed, 0.0,
        v_samples
    );

    RETURN 'done';
END;
$$;


-- ════════════════════════════════════════════════════════════════
-- SECTION C: CONSISTENCY CHECKS
-- Are values logically consistent across columns / tables?
-- ════════════════════════════════════════════════════════════════

-- C1. Discharge date must be >= admit date
CREATE OR REPLACE PROCEDURE governance.dq_check_encounter_date_logic(p_run_id VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_total   BIGINT;
    v_failed  BIGINT;
    v_samples VARIANT;
BEGIN
    SELECT COUNT(*) INTO v_total  FROM edw.fact_encounter;

    SELECT COUNT(*) INTO v_failed
    FROM   edw.fact_encounter fe
    JOIN   edw.dim_date       da ON da.date_key = fe.admit_date_key
    JOIN   edw.dim_date       dd ON dd.date_key = fe.discharge_date_key
    WHERE  dd.full_date < da.full_date;

    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
               'encounter_id',   fe.encounter_id,
               'admit_date',     da.full_date::VARCHAR,
               'discharge_date', dd.full_date::VARCHAR
           )) WITHIN GROUP (ORDER BY fe.encounter_id)
    INTO   v_samples
    FROM (
        SELECT fe.encounter_id, fe.admit_date_key, fe.discharge_date_key
        FROM   edw.fact_encounter fe
        JOIN   edw.dim_date da ON da.date_key = fe.admit_date_key
        JOIN   edw.dim_date dd ON dd.date_key = fe.discharge_date_key
        WHERE  dd.full_date < da.full_date
        LIMIT 5
    ) fe
    JOIN edw.dim_date da ON da.date_key = fe.admit_date_key
    JOIN edw.dim_date dd ON dd.date_key = fe.discharge_date_key;

    CALL governance.sp_log_dq_check(
        p_run_id, 'encounter_discharge_before_admit', 'consistency',
        'edw.fact_encounter', 'admit_date_key, discharge_date_key',
        v_total, v_failed, 0.0,
        v_samples
    );

    RETURN 'done';
END;
$$;


-- C2. Claim paid amount must not exceed billed amount
CREATE OR REPLACE PROCEDURE governance.dq_check_claim_amounts(p_run_id VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_total   BIGINT;
    v_failed  BIGINT;
    v_samples VARIANT;
BEGIN
    SELECT COUNT(*) INTO v_total  FROM edw.fact_claim;

    SELECT COUNT(*) INTO v_failed
    FROM   edw.fact_claim
    WHERE  paid_amount > billed_amount
      AND  paid_amount IS NOT NULL
      AND  billed_amount IS NOT NULL;

    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
               'claim_id',      claim_id,
               'billed_amount', billed_amount::VARCHAR,
               'paid_amount',   paid_amount::VARCHAR
           )) WITHIN GROUP (ORDER BY claim_id)
    INTO   v_samples
    FROM (
        SELECT claim_id, billed_amount, paid_amount
        FROM   edw.fact_claim
        WHERE  paid_amount > billed_amount
        LIMIT 5
    );

    CALL governance.sp_log_dq_check(
        p_run_id, 'claim_paid_exceeds_billed', 'consistency',
        'edw.fact_claim', 'paid_amount, billed_amount',
        v_total, v_failed, 0.1,   -- threshold: 0.1% — small tolerance for adjustments
        v_samples
    );

    RETURN 'done';
END;
$$;


-- ════════════════════════════════════════════════════════════════
-- SECTION D: TIMELINESS CHECKS
-- Did data arrive on time? Are there unexpected gaps?
-- ════════════════════════════════════════════════════════════════

-- D1. Check that yesterday's encounters loaded (no missing day)
CREATE OR REPLACE PROCEDURE governance.dq_check_encounter_timeliness(p_run_id VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_yesterday_count   BIGINT;
    v_7day_avg          DECIMAL(10,2);
    v_failed            BIGINT;
    v_samples           VARIANT;
BEGIN
    -- Count yesterday's encounters
    SELECT COUNT(*) INTO v_yesterday_count
    FROM   edw.fact_encounter fe
    JOIN   edw.dim_date       dd ON dd.date_key = fe.admit_date_key
    WHERE  dd.full_date = CURRENT_DATE() - 1;

    -- Average daily encounters over last 7 days (baseline)
    SELECT AVG(daily_count) INTO v_7day_avg
    FROM (
        SELECT dd.full_date, COUNT(*) AS daily_count
        FROM   edw.fact_encounter fe
        JOIN   edw.dim_date       dd ON dd.date_key = fe.admit_date_key
        WHERE  dd.full_date BETWEEN CURRENT_DATE() - 8 AND CURRENT_DATE() - 2
        GROUP BY dd.full_date
    );

    -- Fail if yesterday's count is < 50% of the 7-day average (data gap signal)
    v_failed := IFF(v_yesterday_count < v_7day_avg * 0.5, 1, 0);

    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
               'check',          'encounter_volume_drop',
               'yesterday_count', v_yesterday_count::VARCHAR,
               '7day_avg',        v_7day_avg::VARCHAR
           ))
    INTO   v_samples
    FROM (SELECT 1);

    CALL governance.sp_log_dq_check(
        p_run_id, 'encounter_daily_volume_timeliness', 'timeliness',
        'edw.fact_encounter', 'admit_date_key',
        1, v_failed, 0.0,
        v_samples
    );

    RETURN 'done';
END;
$$;


-- ════════════════════════════════════════════════════════════════
-- SECTION E: UNIQUENESS CHECKS
-- Are primary / natural keys truly unique?
-- ════════════════════════════════════════════════════════════════

-- E1. Duplicate encounter_id check
CREATE OR REPLACE PROCEDURE governance.dq_check_encounter_uniqueness(p_run_id VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_total   BIGINT;
    v_failed  BIGINT;
    v_samples VARIANT;
BEGIN
    SELECT COUNT(*) INTO v_total FROM edw.fact_encounter;

    SELECT COUNT(*) INTO v_failed
    FROM (
        SELECT encounter_id
        FROM   edw.fact_encounter
        GROUP BY encounter_id
        HAVING COUNT(*) > 1
    );

    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
               'encounter_id', encounter_id,
               'count',        cnt::VARCHAR
           )) WITHIN GROUP (ORDER BY cnt DESC)
    INTO   v_samples
    FROM (
        SELECT encounter_id, COUNT(*) AS cnt
        FROM   edw.fact_encounter
        GROUP BY encounter_id
        HAVING COUNT(*) > 1
        LIMIT 5
    );

    CALL governance.sp_log_dq_check(
        p_run_id, 'encounter_id_uniqueness', 'uniqueness',
        'edw.fact_encounter', 'encounter_id',
        v_total, v_failed, 0.0,
        v_samples
    );

    RETURN 'done';
END;
$$;


-- ════════════════════════════════════════════════════════════════
-- SECTION F: MASTER RUNNER + MONITORING VIEWS
-- ════════════════════════════════════════════════════════════════

-- F1. Master runner — calls all checks in sequence
CREATE OR REPLACE PROCEDURE governance.sp_run_all_dq_checks()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_run_id    VARCHAR := GEN_RANDOM_UUID();
    v_fails     INT     := 0;
BEGIN
    -- Completeness
    CALL governance.dq_check_patient_completeness(v_run_id);
    -- Validity
    CALL governance.dq_check_icd10_format(v_run_id);
    CALL governance.dq_check_npi_format(v_run_id);
    -- Consistency
    CALL governance.dq_check_encounter_date_logic(v_run_id);
    CALL governance.dq_check_claim_amounts(v_run_id);
    -- Timeliness
    CALL governance.dq_check_encounter_timeliness(v_run_id);
    -- Uniqueness
    CALL governance.dq_check_encounter_uniqueness(v_run_id);

    -- Count failures in this run
    SELECT COUNT(*) INTO v_fails
    FROM   governance.dq_results
    WHERE  run_id = v_run_id
      AND  status = 'FAIL';

    RETURN 'Run ' || v_run_id || ' complete. FAILs: ' || v_fails::VARCHAR;
END;
$$;


-- F2. Monitoring view — latest status of every check
CREATE OR REPLACE VIEW governance.v_dq_latest_status AS
WITH latest_runs AS (
    SELECT
        check_name,
        MAX(checked_at) AS latest_checked_at
    FROM governance.dq_results
    GROUP BY check_name
)
SELECT
    r.check_name,
    r.check_category,
    r.target_table,
    r.target_column,
    r.records_checked,
    r.records_failed,
    r.failure_rate_pct,
    r.threshold_pct,
    r.status,
    r.checked_at,
    -- Trend: how has this check performed over the last 7 runs?
    ARRAY_AGG(r2.status)
        WITHIN GROUP (ORDER BY r2.checked_at DESC)
        OVER (PARTITION BY r.check_name
              ORDER BY r.checked_at DESC
              ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last_7_statuses
FROM       governance.dq_results r
JOIN       latest_runs           lr ON lr.check_name = r.check_name
                                    AND lr.latest_checked_at = r.checked_at
LEFT JOIN  governance.dq_results r2 ON r2.check_name = r.check_name;


-- F3. Trend view — failure rate over time per check (for dashboards)
CREATE OR REPLACE VIEW governance.v_dq_failure_trend AS
SELECT
    check_name,
    check_category,
    target_table,
    checked_at::DATE            AS check_date,
    AVG(failure_rate_pct)       AS avg_failure_rate_pct,
    MAX(failure_rate_pct)       AS max_failure_rate_pct,
    COUNT_IF(status = 'FAIL')   AS fail_count,
    COUNT_IF(status = 'WARN')   AS warn_count,
    COUNT_IF(status = 'PASS')   AS pass_count
FROM governance.dq_results
GROUP BY check_name, check_category, target_table, checked_at::DATE
ORDER BY check_date DESC, check_name;
