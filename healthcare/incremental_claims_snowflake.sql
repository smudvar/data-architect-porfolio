-- ============================================================
-- INCREMENTAL CLAIMS ETL — SNOWFLAKE
-- Pattern : MERGE + STREAM + TASK  (Snowflake-native CDC)
-- Purpose : Continuously sync raw claims from the landing zone
--           into the fact_claim table without full reloads
-- ============================================================

USE DATABASE  healthcare_dw;
USE SCHEMA    raw;

-- ── 1. Raw landing table (append-only; receives files from S3) ─
CREATE TABLE IF NOT EXISTS raw.claims_landing (
    claim_json      VARIANT         NOT NULL,   -- semi-structured payload
    file_name       VARCHAR(500),
    loaded_at       TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP()
);


-- ── 2. Snowflake Stream (captures inserts / updates / deletes) ──
CREATE OR REPLACE STREAM stm_claims_landing
    ON TABLE raw.claims_landing
    APPEND_ONLY = TRUE;          -- claims are insert-only from source


-- ── 3. Stored procedure: parse + load into the fact table ───────
CREATE OR REPLACE PROCEDURE raw.sp_load_fact_claim()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN

    -- Parse JSON → staging CTE, then MERGE into fact layer
    MERGE INTO healthcare_dw.edw.fact_claim AS tgt
    USING (
        SELECT
            -- Extract & cast semi-structured fields
            cl.claim_json:claim_id::VARCHAR(30)                     AS claim_id,
            cl.claim_json:claim_line_number::SMALLINT               AS claim_line_number,
            cl.claim_json:encounter_id::VARCHAR(30)                 AS encounter_id,
            cl.claim_json:patient_id::VARCHAR(20)                   AS patient_id,
            cl.claim_json:rendering_npi::CHAR(10)                   AS rendering_npi,
            TO_DATE(cl.claim_json:service_date::VARCHAR, 'YYYY-MM-DD')  AS service_date,
            TO_DATE(cl.claim_json:paid_date::VARCHAR, 'YYYY-MM-DD')     AS paid_date,
            cl.claim_json:procedure_code::VARCHAR(10)               AS procedure_code,
            cl.claim_json:primary_icd10::VARCHAR(10)                AS primary_icd10,
            cl.claim_json:billed_amount::DECIMAL(14,2)              AS billed_amount,
            cl.claim_json:allowed_amount::DECIMAL(14,2)             AS allowed_amount,
            cl.claim_json:paid_amount::DECIMAL(14,2)                AS paid_amount,
            cl.claim_json:patient_responsibility::DECIMAL(14,2)     AS patient_responsibility,
            cl.claim_json:units_of_service::SMALLINT                AS units_of_service,
            cl.claim_json:claim_status::VARCHAR(20)                 AS claim_status,
            cl.claim_json:denial_reason_code::VARCHAR(10)           AS denial_reason_code,
            -- Resolve surrogate keys from dimensions
            dp.patient_key,
            dpr.provider_key                                        AS rendering_provider_key,
            TO_NUMBER(TO_CHAR(service_date, 'YYYYMMDD'))            AS service_date_key,
            TO_NUMBER(TO_CHAR(paid_date,    'YYYYMMDD'))            AS paid_date_key,
            dpc.procedure_key,
            ddx.diagnosis_key                                       AS primary_diagnosis_key,
            fe.encounter_key
        FROM   stm_claims_landing                cl                 -- consume the stream
        -- Dimension lookups (current records only)
        JOIN   healthcare_dw.edw.dim_patient     dp
               ON  dp.patient_id        = cl.claim_json:patient_id::VARCHAR
               AND dp.is_current_record = TRUE
        JOIN   healthcare_dw.edw.dim_provider    dpr
               ON  dpr.npi              = cl.claim_json:rendering_npi::CHAR(10)
               AND dpr.is_current_record = TRUE
        JOIN   healthcare_dw.edw.dim_procedure   dpc
               ON  dpc.procedure_code   = cl.claim_json:procedure_code::VARCHAR
        JOIN   healthcare_dw.edw.dim_diagnosis   ddx
               ON  ddx.icd10_code       = cl.claim_json:primary_icd10::VARCHAR
        LEFT JOIN healthcare_dw.edw.fact_encounter fe
               ON  fe.encounter_id      = cl.claim_json:encounter_id::VARCHAR
        WHERE  cl.METADATA$ACTION = 'INSERT'
    ) AS src
    ON  tgt.claim_id          = src.claim_id
    AND tgt.claim_line_number = src.claim_line_number

    -- Update if claim status or financials changed (late adjustments)
    WHEN MATCHED AND (
        tgt.claim_status   <> src.claim_status   OR
        tgt.paid_amount    <> src.paid_amount     OR
        tgt.denial_reason_code <> src.denial_reason_code
    ) THEN UPDATE SET
        tgt.claim_status           = src.claim_status,
        tgt.allowed_amount         = src.allowed_amount,
        tgt.paid_amount            = src.paid_amount,
        tgt.patient_responsibility = src.patient_responsibility,
        tgt.denial_reason_code     = src.denial_reason_code

    -- Insert net-new claim lines
    WHEN NOT MATCHED THEN INSERT (
        claim_line_key, claim_id, claim_line_number, encounter_key,
        patient_key, rendering_provider_key,
        service_date_key, paid_date_key,
        procedure_key, primary_diagnosis_key,
        billed_amount, allowed_amount, paid_amount,
        patient_responsibility, units_of_service,
        claim_status, denial_reason_code
    ) VALUES (
        healthcare_dw.edw.seq_claim_key.NEXTVAL,
        src.claim_id, src.claim_line_number, src.encounter_key,
        src.patient_key, src.rendering_provider_key,
        src.service_date_key, src.paid_date_key,
        src.procedure_key, src.primary_diagnosis_key,
        src.billed_amount, src.allowed_amount, src.paid_amount,
        src.patient_responsibility, src.units_of_service,
        src.claim_status, src.denial_reason_code
    );

    RETURN 'sp_load_fact_claim completed successfully.';
END;
$$;


-- ── 4. Snowflake Task: schedule every 15 minutes ────────────────
CREATE OR REPLACE TASK tsk_load_fact_claim
    WAREHOUSE     = LOAD_WH
    SCHEDULE      = '15 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('stm_claims_landing')   -- skip if no new data
AS
    CALL raw.sp_load_fact_claim();

ALTER TASK tsk_load_fact_claim RESUME;
