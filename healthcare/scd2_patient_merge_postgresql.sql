-- ============================================================
-- SCD TYPE 2 MERGE: dim_patient
-- Dialect: PostgreSQL  (see snowflake_variant.sql for Snowflake)
-- Purpose: Incrementally load patient dimension while preserving
--          full change history (Slowly Changing Dimension Type 2)
-- ============================================================

-- ── Helper: compute a deterministic row hash from tracked cols ──
CREATE OR REPLACE FUNCTION fn_patient_row_hash(
    p_first_name    VARCHAR,
    p_last_name     VARCHAR,
    p_dob           DATE,
    p_gender        CHAR,
    p_zip           CHAR,
    p_state         CHAR,
    p_payer_type    VARCHAR
)
RETURNS CHAR(64)
LANGUAGE SQL IMMUTABLE PARALLEL SAFE
AS $$
    SELECT encode(
        sha256(
            concat_ws('|',
                UPPER(TRIM(p_first_name)),
                UPPER(TRIM(p_last_name)),
                p_dob::TEXT,
                UPPER(p_gender),
                p_zip,
                UPPER(p_state),
                UPPER(TRIM(p_payer_type))
            )::BYTEA
        ),
        'hex'
    );
$$;


-- ── Stage table (loaded by upstream ETL each run) ──────────────
CREATE TABLE IF NOT EXISTS stg_patient (
    patient_id      VARCHAR(20)     NOT NULL,
    first_name      VARCHAR(100)    NOT NULL,
    last_name       VARCHAR(100)    NOT NULL,
    date_of_birth   DATE            NOT NULL,
    gender          CHAR(1)         NOT NULL,
    race            VARCHAR(50),
    ethnicity       VARCHAR(50),
    zip_code        CHAR(5),
    state_code      CHAR(2),
    payer_type      VARCHAR(50),
    source_system   VARCHAR(50),
    extracted_at    TIMESTAMP       NOT NULL
);


-- ── SCD2 Merge Procedure ───────────────────────────────────────
CREATE OR REPLACE PROCEDURE sp_load_dim_patient(
    p_effective_date DATE DEFAULT CURRENT_DATE
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows_inserted INT := 0;
    v_rows_expired  INT := 0;
    v_rows_new      INT := 0;
BEGIN

    -- ── STEP 1: Expire rows where tracked attributes changed ────
    UPDATE dim_patient dp
    SET
        expiration_date   = p_effective_date - INTERVAL '1 day',
        is_current_record = FALSE,
        updated_at        = CURRENT_TIMESTAMP
    FROM stg_patient stg
    WHERE dp.patient_id        = stg.patient_id
      AND dp.is_current_record = TRUE
      AND dp.row_hash         <>
            fn_patient_row_hash(
                stg.first_name, stg.last_name, stg.date_of_birth,
                stg.gender,     stg.zip_code,   stg.state_code,
                stg.payer_type
            );

    GET DIAGNOSTICS v_rows_expired = ROW_COUNT;

    -- ── STEP 2: Insert new rows for changed + brand-new patients ─
    INSERT INTO dim_patient (
        patient_key,
        patient_id,
        first_name, last_name, date_of_birth, gender,
        race, ethnicity, zip_code, state_code, payer_type,
        effective_date, expiration_date, is_current_record,
        row_hash, created_at, updated_at
    )
    SELECT
        nextval('seq_patient_key'),     -- surrogate key sequence
        stg.patient_id,
        stg.first_name, stg.last_name, stg.date_of_birth, stg.gender,
        stg.race, stg.ethnicity, stg.zip_code, stg.state_code, stg.payer_type,
        p_effective_date,
        '9999-12-31'::DATE,
        TRUE,
        fn_patient_row_hash(
            stg.first_name, stg.last_name, stg.date_of_birth,
            stg.gender,     stg.zip_code,   stg.state_code,
            stg.payer_type
        ),
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
    FROM stg_patient stg
    -- Changed records (just expired above) OR brand-new patients
    WHERE NOT EXISTS (
        SELECT 1
        FROM   dim_patient dp
        WHERE  dp.patient_id        = stg.patient_id
          AND  dp.is_current_record = TRUE
    );

    GET DIAGNOSTICS v_rows_inserted = ROW_COUNT;

    -- ── STEP 3: Log the run ─────────────────────────────────────
    INSERT INTO etl_run_log (procedure_name, rows_inserted, rows_updated, run_at)
    VALUES ('sp_load_dim_patient', v_rows_inserted, v_rows_expired, CURRENT_TIMESTAMP);

    RAISE NOTICE 'sp_load_dim_patient complete — inserted: %, expired: %',
                  v_rows_inserted, v_rows_expired;

END;
$$;
