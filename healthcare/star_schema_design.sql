-- ============================================================
-- HEALTHCARE DATA WAREHOUSE: STAR SCHEMA DESIGN
-- Dialect: ANSI SQL (compatible with PostgreSQL, Snowflake,
--          BigQuery, SQL Server)
-- Author:  Healthcare Data Architecture Portfolio
-- Purpose: Dimensional model for patient encounters, claims,
--          and clinical outcomes analytics
-- ============================================================

-- ============================================================
-- DIMENSION TABLES
-- ============================================================

-- Patient dimension (Type 2 SCD — tracks history of changes)
CREATE TABLE dim_patient (
    patient_key         BIGINT          NOT NULL,   -- surrogate key
    patient_id          VARCHAR(20)     NOT NULL,   -- natural/source key
    first_name          VARCHAR(100)    NOT NULL,
    last_name           VARCHAR(100)    NOT NULL,
    date_of_birth       DATE            NOT NULL,
    gender              CHAR(1)         NOT NULL,   -- M / F / U
    race                VARCHAR(50),
    ethnicity           VARCHAR(50),
    zip_code            CHAR(5),
    state_code          CHAR(2),
    payer_type          VARCHAR(50),                -- Commercial, Medicare, Medicaid, Self-Pay
    -- SCD Type 2 columns
    effective_date      DATE            NOT NULL,
    expiration_date     DATE            NOT NULL DEFAULT '9999-12-31',
    is_current_record   BOOLEAN         NOT NULL DEFAULT TRUE,
    row_hash            CHAR(64),                   -- SHA-256 of tracked attributes
    created_at          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_dim_patient PRIMARY KEY (patient_key)
);

CREATE INDEX idx_dim_patient_id         ON dim_patient (patient_id);
CREATE INDEX idx_dim_patient_current    ON dim_patient (patient_id, is_current_record);
CREATE INDEX idx_dim_patient_zip        ON dim_patient (zip_code);


-- Provider dimension
CREATE TABLE dim_provider (
    provider_key        BIGINT          NOT NULL,
    npi                 CHAR(10)        NOT NULL,   -- National Provider Identifier
    provider_name       VARCHAR(200)    NOT NULL,
    specialty           VARCHAR(100),
    sub_specialty       VARCHAR(100),
    facility_name       VARCHAR(200),
    facility_type       VARCHAR(50),                -- Hospital, Clinic, ASC, SNF
    address_city        VARCHAR(100),
    address_state       CHAR(2),
    is_in_network       BOOLEAN         NOT NULL DEFAULT TRUE,
    effective_date      DATE            NOT NULL,
    expiration_date     DATE            NOT NULL DEFAULT '9999-12-31',
    is_current_record   BOOLEAN         NOT NULL DEFAULT TRUE,
    CONSTRAINT pk_dim_provider PRIMARY KEY (provider_key)
);

CREATE INDEX idx_dim_provider_npi       ON dim_provider (npi);
CREATE INDEX idx_dim_provider_specialty ON dim_provider (specialty);


-- Diagnosis dimension (ICD-10 code set)
CREATE TABLE dim_diagnosis (
    diagnosis_key       INT             NOT NULL,
    icd10_code          VARCHAR(10)     NOT NULL,
    icd10_description   VARCHAR(300)    NOT NULL,
    category_code       VARCHAR(10),
    category_desc       VARCHAR(200),
    chapter_code        VARCHAR(10),
    chapter_desc        VARCHAR(200),
    is_chronic          BOOLEAN         NOT NULL DEFAULT FALSE,
    cms_hcc_category    VARCHAR(50),                -- CMS Hierarchical Condition Category
    CONSTRAINT pk_dim_diagnosis PRIMARY KEY (diagnosis_key)
);

CREATE INDEX idx_dim_diagnosis_icd10    ON dim_diagnosis (icd10_code);
CREATE INDEX idx_dim_diagnosis_chronic  ON dim_diagnosis (is_chronic);


-- Procedure dimension (CPT / HCPCS code set)
CREATE TABLE dim_procedure (
    procedure_key       INT             NOT NULL,
    procedure_code      VARCHAR(10)     NOT NULL,
    code_type           VARCHAR(10)     NOT NULL,   -- CPT, HCPCS, ICD-10-PCS
    procedure_desc      VARCHAR(300)    NOT NULL,
    category            VARCHAR(100),
    subcategory         VARCHAR(100),
    is_surgical         BOOLEAN         NOT NULL DEFAULT FALSE,
    rvu_total           DECIMAL(8,2),               -- Relative Value Units
    CONSTRAINT pk_dim_procedure PRIMARY KEY (procedure_key)
);

CREATE INDEX idx_dim_procedure_code     ON dim_procedure (procedure_code);


-- Date dimension (pre-populated; critical for BI tool performance)
CREATE TABLE dim_date (
    date_key            INT             NOT NULL,   -- YYYYMMDD
    full_date           DATE            NOT NULL,
    day_of_week         TINYINT         NOT NULL,   -- 1=Sun … 7=Sat
    day_name            VARCHAR(10)     NOT NULL,
    day_of_month        TINYINT         NOT NULL,
    day_of_year         SMALLINT        NOT NULL,
    week_of_year        TINYINT         NOT NULL,
    iso_week            TINYINT         NOT NULL,
    month_number        TINYINT         NOT NULL,
    month_name          VARCHAR(10)     NOT NULL,
    quarter_number      TINYINT         NOT NULL,
    year_number         SMALLINT        NOT NULL,
    fiscal_period       VARCHAR(10),
    is_weekday          BOOLEAN         NOT NULL,
    is_federal_holiday  BOOLEAN         NOT NULL DEFAULT FALSE,
    CONSTRAINT pk_dim_date PRIMARY KEY (date_key)
);


-- ============================================================
-- FACT TABLES
-- ============================================================

-- Fact: Patient Encounters (grain = one row per encounter)
CREATE TABLE fact_encounter (
    encounter_key           BIGINT          NOT NULL,
    -- Foreign keys to dimensions
    patient_key             BIGINT          NOT NULL,
    attending_provider_key  BIGINT          NOT NULL,
    admit_date_key          INT             NOT NULL,
    discharge_date_key      INT             NOT NULL,
    primary_diagnosis_key   INT             NOT NULL,
    -- Degenerate dimensions
    encounter_id            VARCHAR(30)     NOT NULL,
    encounter_type          VARCHAR(50)     NOT NULL,   -- IP, ED, OP, Telehealth
    admit_source            VARCHAR(50),
    discharge_disposition   VARCHAR(50),
    -- Measures
    length_of_stay_days     SMALLINT        NOT NULL DEFAULT 0,
    icu_days                SMALLINT        NOT NULL DEFAULT 0,
    total_charges           DECIMAL(14,2),
    total_payments          DECIMAL(14,2),
    readmission_30d_flag    BOOLEAN         NOT NULL DEFAULT FALSE,
    mortality_flag          BOOLEAN         NOT NULL DEFAULT FALSE,
    -- Audit
    created_at              TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_fact_encounter PRIMARY KEY (encounter_key),
    CONSTRAINT fk_enc_patient   FOREIGN KEY (patient_key)            REFERENCES dim_patient   (patient_key),
    CONSTRAINT fk_enc_provider  FOREIGN KEY (attending_provider_key) REFERENCES dim_provider  (provider_key),
    CONSTRAINT fk_enc_admit_dt  FOREIGN KEY (admit_date_key)         REFERENCES dim_date      (date_key),
    CONSTRAINT fk_enc_disch_dt  FOREIGN KEY (discharge_date_key)     REFERENCES dim_date      (date_key),
    CONSTRAINT fk_enc_dx        FOREIGN KEY (primary_diagnosis_key)  REFERENCES dim_diagnosis (diagnosis_key)
);

CREATE INDEX idx_fact_enc_patient   ON fact_encounter (patient_key);
CREATE INDEX idx_fact_enc_provider  ON fact_encounter (attending_provider_key);
CREATE INDEX idx_fact_enc_admit_dt  ON fact_encounter (admit_date_key);
CREATE INDEX idx_fact_enc_type      ON fact_encounter (encounter_type);


-- Bridge table: Encounter ↔ Diagnosis (one encounter has many diagnoses)
CREATE TABLE bridge_encounter_diagnosis (
    encounter_key       BIGINT      NOT NULL,
    diagnosis_key       INT         NOT NULL,
    diagnosis_seq       TINYINT     NOT NULL,   -- 1 = primary, 2+ = secondary
    poa_indicator       CHAR(1),                -- Present on Admission: Y/N/U/W
    CONSTRAINT pk_bridge_enc_dx PRIMARY KEY (encounter_key, diagnosis_key),
    CONSTRAINT fk_bed_encounter FOREIGN KEY (encounter_key) REFERENCES fact_encounter (encounter_key),
    CONSTRAINT fk_bed_diagnosis FOREIGN KEY (diagnosis_key) REFERENCES dim_diagnosis  (diagnosis_key)
);


-- Fact: Medical Claims (grain = one row per claim line)
CREATE TABLE fact_claim (
    claim_line_key      BIGINT          NOT NULL,
    claim_id            VARCHAR(30)     NOT NULL,
    claim_line_number   SMALLINT        NOT NULL,
    encounter_key       BIGINT,
    patient_key         BIGINT          NOT NULL,
    rendering_provider_key BIGINT       NOT NULL,
    service_date_key    INT             NOT NULL,
    paid_date_key       INT,
    procedure_key       INT             NOT NULL,
    primary_diagnosis_key INT           NOT NULL,
    -- Measures
    billed_amount       DECIMAL(14,2)   NOT NULL,
    allowed_amount      DECIMAL(14,2),
    paid_amount         DECIMAL(14,2),
    patient_responsibility DECIMAL(14,2),
    units_of_service    SMALLINT        NOT NULL DEFAULT 1,
    claim_status        VARCHAR(20)     NOT NULL,   -- Paid, Denied, Pending, Adjusted
    denial_reason_code  VARCHAR(10),
    CONSTRAINT pk_fact_claim PRIMARY KEY (claim_line_key),
    CONSTRAINT fk_cl_patient   FOREIGN KEY (patient_key)             REFERENCES dim_patient   (patient_key),
    CONSTRAINT fk_cl_provider  FOREIGN KEY (rendering_provider_key)  REFERENCES dim_provider  (provider_key),
    CONSTRAINT fk_cl_svc_dt    FOREIGN KEY (service_date_key)        REFERENCES dim_date      (date_key),
    CONSTRAINT fk_cl_proc      FOREIGN KEY (procedure_key)           REFERENCES dim_procedure (procedure_key),
    CONSTRAINT fk_cl_dx        FOREIGN KEY (primary_diagnosis_key)   REFERENCES dim_diagnosis (diagnosis_key)
);

CREATE INDEX idx_fact_claim_patient     ON fact_claim (patient_key);
CREATE INDEX idx_fact_claim_id          ON fact_claim (claim_id);
CREATE INDEX idx_fact_claim_svc_date    ON fact_claim (service_date_key);
CREATE INDEX idx_fact_claim_status      ON fact_claim (claim_status);
