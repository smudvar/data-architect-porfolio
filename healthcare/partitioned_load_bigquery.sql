-- ============================================================
-- BIGQUERY: PARTITIONED & CLUSTERED FACT TABLE + SCHEDULED LOAD
-- Purpose : Optimise storage and query cost for the fact_encounter
--           table at scale (billions of rows, petabyte range)
-- ============================================================


-- ── 1. Create fact_encounter with partition + cluster strategy ──
CREATE OR REPLACE TABLE `healthcare_project.edw.fact_encounter`
(
    encounter_key               INT64       NOT NULL,
    patient_key                 INT64       NOT NULL,
    attending_provider_key      INT64       NOT NULL,
    admit_date                  DATE        NOT NULL,   -- used as partition col
    discharge_date              DATE,
    admit_date_key              INT64       NOT NULL,
    discharge_date_key          INT64,
    primary_diagnosis_key       INT64       NOT NULL,
    encounter_id                STRING      NOT NULL,
    encounter_type              STRING      NOT NULL,
    admit_source                STRING,
    discharge_disposition       STRING,
    length_of_stay_days         INT64,
    icu_days                    INT64,
    total_charges               NUMERIC,
    total_payments              NUMERIC,
    readmission_30d_flag        BOOL        NOT NULL DEFAULT FALSE,
    mortality_flag              BOOL        NOT NULL DEFAULT FALSE,
    created_at                  TIMESTAMP   NOT NULL
)
PARTITION BY   admit_date                          -- daily partition on admit date
CLUSTER BY     encounter_type,
               attending_provider_key,
               primary_diagnosis_key;              -- cluster by high-cardinality query filters
-- NOTE: BigQuery automatically prunes partitions when WHERE clause
--       filters on admit_date, cutting scan cost by 95%+ on typical queries.


-- ── 2. Scheduled Query: incremental append from staging ────────
-- (Configure as BigQuery Scheduled Query, daily at 02:00 UTC)
INSERT INTO `healthcare_project.edw.fact_encounter`
SELECT
    FARM_FINGERPRINT(src.encounter_id)                  AS encounter_key,
    dp.patient_key,
    dpr.provider_key                                    AS attending_provider_key,
    src.admit_date,
    src.discharge_date,
    CAST(FORMAT_DATE('%Y%m%d', src.admit_date)  AS INT64) AS admit_date_key,
    CAST(FORMAT_DATE('%Y%m%d', src.discharge_date) AS INT64) AS discharge_date_key,
    ddx.diagnosis_key                                   AS primary_diagnosis_key,
    src.encounter_id,
    src.encounter_type,
    src.admit_source,
    src.discharge_disposition,
    DATE_DIFF(src.discharge_date, src.admit_date, DAY)  AS length_of_stay_days,
    src.icu_days,
    src.total_charges,
    src.total_payments,
    src.readmission_30d_flag,
    src.mortality_flag,
    CURRENT_TIMESTAMP()
FROM `healthcare_project.staging.encounters_raw` src
JOIN `healthcare_project.edw.dim_patient`   dp
  ON  dp.patient_id        = src.patient_id
  AND dp.is_current_record = TRUE
JOIN `healthcare_project.edw.dim_provider`  dpr
  ON  dpr.npi              = src.attending_npi
  AND dpr.is_current_record = TRUE
JOIN `healthcare_project.edw.dim_diagnosis` ddx
  ON  ddx.icd10_code       = src.primary_icd10_code
-- Only load records extracted since the last successful run
WHERE src.extracted_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
  -- Dedup: skip rows already loaded
  AND NOT EXISTS (
    SELECT 1
    FROM `healthcare_project.edw.fact_encounter` fe
    WHERE fe.encounter_id = src.encounter_id
  );


-- ── 3. Cost-control view: always filter by partition ───────────
-- Analysts should query this view, not the raw fact table,
-- to enforce partition pruning and avoid accidental full scans.
CREATE OR REPLACE VIEW `healthcare_project.edw.v_encounter_last_365d` AS
SELECT *
FROM   `healthcare_project.edw.fact_encounter`
WHERE  admit_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY);
