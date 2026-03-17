-- ============================================================
-- PERFORMANCE TUNING & INDEXING STRATEGIES
-- Dialect: PostgreSQL (with SQL Server T-SQL variants noted)
-- Purpose: Demonstrate index design, query rewrites, and
--          execution plan optimisation for healthcare workloads
-- ============================================================


-- ════════════════════════════════════════════════════════════════
-- SECTION 1: INDEX DESIGN PATTERNS
-- ════════════════════════════════════════════════════════════════

-- ── 1a. Composite index — column ORDER matters ──────────────────
-- Query pattern: WHERE encounter_type = ? AND admit_date_key BETWEEN ? AND ?
-- Rule: Equality predicates first, range predicates last.

CREATE INDEX CONCURRENTLY idx_fact_enc_type_admit
    ON fact_encounter (encounter_type, admit_date_key);
-- CONCURRENTLY = no table lock; safe on live production (PostgreSQL)


-- ── 1b. Partial index — index only the interesting subset ───────
-- 90% of queries filter on is_current_record = TRUE.
-- Indexing FALSE rows wastes space and hurts write performance.

CREATE INDEX CONCURRENTLY idx_dim_patient_current_id
    ON dim_patient (patient_id)
    WHERE is_current_record = TRUE;


-- ── 1c. Covering index — include all SELECT columns ─────────────
-- Eliminates heap fetch ("Index Only Scan") for this hot query:
-- SELECT patient_key, payer_type FROM dim_patient WHERE patient_id = ?

CREATE INDEX CONCURRENTLY idx_dim_patient_cover
    ON dim_patient (patient_id)
    INCLUDE (patient_key, payer_type)    -- PostgreSQL 11+ / SQL Server syntax
    WHERE is_current_record = TRUE;


-- ── 1d. Functional index — index the expression, not the column ──
-- Query: WHERE UPPER(icd10_code) = 'E11.65'  (mixed-case source data)

CREATE INDEX CONCURRENTLY idx_dim_diagnosis_icd10_upper
    ON dim_diagnosis (UPPER(icd10_code));


-- SQL Server equivalent (for portfolio completeness):
-- CREATE NONCLUSTERED INDEX idx_dim_patient_cover
--     ON dbo.dim_patient (patient_id)
--     INCLUDE (patient_key, payer_type)
--     WHERE is_current_record = 1;


-- ════════════════════════════════════════════════════════════════
-- SECTION 2: QUERY REWRITE PATTERNS
-- ════════════════════════════════════════════════════════════════

-- ── 2a. Replace correlated subquery with window function ─────────
-- SLOW: runs the subquery once per row (O(n²) complexity)
-- ❌ BEFORE
SELECT
    fe.encounter_id,
    fe.patient_key,
    fe.total_charges,
    (
        SELECT AVG(fe2.total_charges)
        FROM   fact_encounter fe2
        WHERE  fe2.attending_provider_key = fe.attending_provider_key
    ) AS provider_avg_charges
FROM fact_encounter fe;

-- FAST: single pass over the data
-- ✅ AFTER
SELECT
    fe.encounter_id,
    fe.patient_key,
    fe.total_charges,
    AVG(fe.total_charges)
        OVER (PARTITION BY fe.attending_provider_key) AS provider_avg_charges
FROM fact_encounter fe;


-- ── 2b. Avoid functions on indexed columns in WHERE ──────────────
-- SLOW: index on claim_status cannot be used; full scan
-- ❌ BEFORE
SELECT * FROM fact_claim
WHERE UPPER(claim_status) = 'DENIED';

-- FAST: use functional index (created in 1d) OR fix the data
-- ✅ AFTER (store values consistently as UPPER at ingest time)
SELECT * FROM fact_claim
WHERE claim_status = 'Denied';


-- ── 2c. EXISTS vs IN for semi-join ───────────────────────────────
-- IN materialises the full result set before comparing.
-- EXISTS short-circuits on first match.
-- ❌ BEFORE  (can be slow when subquery returns many rows)
SELECT patient_key FROM dim_patient
WHERE patient_key IN (
    SELECT patient_key FROM fact_encounter WHERE readmission_30d_flag = TRUE
);

-- ✅ AFTER
SELECT dp.patient_key FROM dim_patient dp
WHERE EXISTS (
    SELECT 1 FROM fact_encounter fe
    WHERE  fe.patient_key         = dp.patient_key
      AND  fe.readmission_30d_flag = TRUE
);


-- ── 2d. CTE vs subquery — materialization control ────────────────
-- PostgreSQL materializes CTEs by default (fence); use subquery or
-- hint to allow the planner to push predicates through.

-- ❌ BEFORE (CTE materialised; planner can't push WHERE into it)
WITH all_encounters AS (
    SELECT * FROM fact_encounter
)
SELECT * FROM all_encounters WHERE encounter_type = 'IP';

-- ✅ AFTER (subquery lets planner use the index on encounter_type)
SELECT *
FROM (SELECT * FROM fact_encounter) sub
WHERE sub.encounter_type = 'IP';

-- ✅ ALTERNATIVE (PostgreSQL 12+: NOT MATERIALIZED hint)
WITH all_encounters AS NOT MATERIALIZED (
    SELECT * FROM fact_encounter
)
SELECT * FROM all_encounters WHERE encounter_type = 'IP';


-- ════════════════════════════════════════════════════════════════
-- SECTION 3: STATISTICS & VACUUM MAINTENANCE (PostgreSQL)
-- ════════════════════════════════════════════════════════════════

-- After large bulk loads, refresh statistics so planner has
-- accurate row counts and avoids bad join strategies.
ANALYZE fact_encounter;
ANALYZE fact_claim;
ANALYZE dim_patient;

-- Reclaim bloat from heavy UPDATE/DELETE workloads
VACUUM (VERBOSE, ANALYZE) fact_claim;


-- ════════════════════════════════════════════════════════════════
-- SECTION 4: EXECUTION PLAN ANALYSIS
-- ════════════════════════════════════════════════════════════════

-- EXPLAIN ANALYZE shows actual vs estimated rows.
-- Look for: Seq Scan on large tables, high row estimate errors,
--           nested loop on large sets, missing index usage.

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT
    dp.provider_name,
    dp.specialty,
    COUNT(*)                        AS encounter_count,
    AVG(fe.length_of_stay_days)     AS avg_los,
    AVG(fe.total_charges)           AS avg_charges
FROM   fact_encounter fe
JOIN   dim_provider   dp ON dp.provider_key = fe.attending_provider_key
JOIN   dim_date       dd ON dd.date_key      = fe.admit_date_key
WHERE  dd.year_number  = 2024
  AND  fe.encounter_type = 'IP'
  AND  dp.is_current_record = TRUE
GROUP BY dp.provider_name, dp.specialty
HAVING COUNT(*) >= 30           -- min volume threshold for significance
ORDER BY avg_charges DESC;


-- ════════════════════════════════════════════════════════════════
-- SECTION 5: TABLE PARTITIONING (PostgreSQL declarative)
-- ════════════════════════════════════════════════════════════════
-- Partition fact_claim by service year to enable partition pruning.

CREATE TABLE fact_claim_partitioned (
    LIKE fact_claim INCLUDING ALL
) PARTITION BY RANGE (service_date_key);

-- Annual partitions — planner skips irrelevant years entirely
CREATE TABLE fact_claim_2022 PARTITION OF fact_claim_partitioned
    FOR VALUES FROM (20220101) TO (20230101);

CREATE TABLE fact_claim_2023 PARTITION OF fact_claim_partitioned
    FOR VALUES FROM (20230101) TO (20240101);

CREATE TABLE fact_claim_2024 PARTITION OF fact_claim_partitioned
    FOR VALUES FROM (20240101) TO (20250101);

CREATE TABLE fact_claim_2025 PARTITION OF fact_claim_partitioned
    FOR VALUES FROM (20250101) TO (20260101);

-- Indexes on each partition are independent — can be built in parallel
CREATE INDEX ON fact_claim_2024 (patient_key);
CREATE INDEX ON fact_claim_2024 (claim_status);
