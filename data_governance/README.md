# Data Governance

> SQL-based data quality framework built on Snowflake. Every check logs results to a central `dq_results` table for trending, alerting, and dashboard reporting.

---

## Structure

```
data_governance/
└── data_quality/
    └── 01_dq_checks_and_monitoring.sql   # Full DQ framework
```

---

## Framework Design

Every data quality check follows the same pattern:
1. Count total records in scope
2. Count records that fail the rule
3. Compute failure rate %
4. Compare against threshold → `PASS` / `WARN` / `FAIL`
5. Capture up to 5 sample failures as JSON for investigation
6. Write one row to `governance.dq_results`

This means every check is **observable, trendable, and alertable** — not just a one-time script.

---

## Check Categories

| Category | What It Catches | Example Check |
|---|---|---|
| **Completeness** | Missing required fields | Patient missing DOB or gender |
| **Validity** | Values outside expected domain/format | ICD-10 code not matching `A00–Z99` pattern |
| **Consistency** | Logical contradictions between columns | Discharge date before admit date |
| **Timeliness** | Missing or delayed data loads | Yesterday's encounter count < 50% of 7-day avg |
| **Uniqueness** | Duplicate primary/natural keys | Duplicate `encounter_id` in fact table |

---

## Key Objects

| Object | Type | Purpose |
|---|---|---|
| `governance.dq_results` | Table | Central log — one row per check per run |
| `governance.sp_log_dq_check` | Procedure | Shared logging helper used by all checks |
| `governance.sp_run_all_dq_checks` | Procedure | Master runner — calls every check in sequence |
| `governance.v_dq_latest_status` | View | Current status of every check + 7-run trend array |
| `governance.v_dq_failure_trend` | View | Daily failure rate time series for dashboards |

---

## Running the Framework

```sql
-- Run all checks (called by Snowflake Task after each ETL load)
CALL governance.sp_run_all_dq_checks();

-- See latest status of every check
SELECT * FROM governance.v_dq_latest_status;

-- Find all current FAILs with sample bad rows
SELECT check_name, records_failed, failure_rate_pct, sample_failures
FROM   governance.v_dq_latest_status
WHERE  status = 'FAIL';

-- Trend a specific check over time
SELECT check_date, avg_failure_rate_pct, fail_count
FROM   governance.v_dq_failure_trend
WHERE  check_name = 'encounter_discharge_before_admit'
ORDER BY check_date DESC;
```

---

## Scheduling

Hook the master runner into your Snowflake Task chain so it fires automatically after each ETL load:

```sql
CREATE OR REPLACE TASK tsk_run_dq_checks
    WAREHOUSE = LOAD_WH
    AFTER     tsk_load_fact_claim       -- runs after ETL completes
AS
    CALL governance.sp_run_all_dq_checks();
```
