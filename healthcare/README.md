# 🏥 Healthcare Data Architecture Portfolio

> **Production-grade SQL showcasing data modeling, ETL pipelines, advanced analytics, and performance engineering across PostgreSQL, Snowflake, BigQuery, and SQL Server.**

---

## 📋 Table of Contents
- [Overview](#overview)
- [Repository Structure](#repository-structure)
- [Module Breakdown](#module-breakdown)
- [Key Concepts Demonstrated](#key-concepts-demonstrated)
- [Schema Reference](#schema-reference)
- [Getting Started](#getting-started)
- [Dialect Compatibility](#dialect-compatibility)

---

## Overview

This repository is a portfolio of advanced SQL for a **multi-site hospital network data warehouse**. The domain covers patient encounters, claims, provider performance, readmission risk, and facility utilization — all common problem spaces in healthcare data architecture roles.

Each file is written to production standards: inline documentation, meaningful naming conventions, proper constraint and index definitions, and architecture commentary explaining *why* design decisions were made, not just *what* was done.

---

## Repository Structure

```
healthcare-data-architecture/
│
├── 01_data_modeling/
│   ├── 01_star_schema_postgresql.sql        # Kimball star schema — fact + dim tables
│   └── 02_data_vault_snowflake.sql          # Data Vault 2.0 — Hubs, Links, Satellites, PIT
│
├── 02_etl_pipelines/
│   ├── 01_incremental_load_postgresql.sql   # Watermark CDC + SCD Type 2 + audit logging
│   └── 02_streams_and_tasks_snowflake.sql   # Snowflake Streams, Tasks, Snowpipe, Dynamic Tables
│
├── 03_window_functions/
│   └── 01_clinical_analytics_postgresql_bq.sql  # 5 production window function use cases
│
├── 04_performance_tuning/
│   ├── 01_indexing_and_optimization_postgresql.sql  # Indexes, partitioning, query rewrites
│   └── 02_bigquery_optimization.sql                 # Partitioning, clustering, cost control
│
└── README.md
```

---

## Module Breakdown

### 01 · Data Modeling

| File | Pattern | Dialect |
|------|---------|---------|
| `01_star_schema_postgresql.sql` | Kimball star schema with dim_date generation, SCD Type 2 markers, computed columns, and a bridge table for multi-valued diagnoses | PostgreSQL 14+ |
| `02_data_vault_snowflake.sql` | Full Data Vault 2.0 implementation: Hub/Link/Satellite tables, HASH_DIFF change detection, Point-in-Time tables, VARIANT columns for FHIR payloads | Snowflake |

**Highlights:**
- `dim_date` auto-populated via `GENERATE_SERIES` with fiscal calendar, weekends, and holiday flags
- `is_30day_readmit` as a `GENERATED ALWAYS AS ... STORED` computed column
- Data Vault PIT table enabling efficient time-sliced joins across multiple satellites
- Snowflake `CLUSTER BY` keys aligned to common query patterns

---

### 02 · ETL Pipelines

| File | Pattern | Dialect |
|------|---------|---------|
| `01_incremental_load_postgresql.sql` | Watermark CDC → staging → DQ checks → UPSERT → SCD2, wrapped in stored procedures with error handling and audit logging | PostgreSQL |
| `02_streams_and_tasks_snowflake.sql` | Snowpipe + Streams + Tasks for near-real-time CDC; FHIR JSON parsing with VARIANT; Dynamic Tables for self-maintaining aggregates | Snowflake |

**Highlights:**
- `etl_watermark` + `etl_audit_log` + `etl_rejected_rows` control tables
- Full `INSERT ... ON CONFLICT DO UPDATE` UPSERT pattern
- `sp_scd2_dim_patient` handles SCD Type 2 close-and-reopen for insurance/PCP changes
- Snowflake `QUALIFY` for top-N deduplication (vs PostgreSQL subquery equivalent)
- Dynamic Table with `TARGET_LAG = '1 hour'` for dashboard pre-aggregation

---

### 03 · Window Functions

Five production use cases in a single annotated file:

| Query | Technique |
|-------|-----------|
| Patient encounter journey | `LAG`, `LEAD`, `SUM() OVER ROWS`, running count, rolling 90-day average with `RANGE INTERVAL` |
| Provider performance ranking | `RANK`, `DENSE_RANK`, `PERCENT_RANK`, `NTILE`, comparison to partition average |
| Cohort retention analysis | First-touch cohort via `ROW_NUMBER`, month-over-month retention rate |
| Readmission risk scoring | LACE score using windowed prior-visit counts, `PERCENT_RANK` risk percentile |
| Facility throughput | Daily census cross-join, rolling 7d/30d `AVG`, `MAX`, day-over-day delta |

---

### 04 · Performance Tuning

| File | Topics |
|------|--------|
| `01_indexing_and_optimization_postgresql.sql` | B-tree, partial, covering (INCLUDE), BRIN, composite indexes; range partitioning; `MATERIALIZED` vs `NOT MATERIALIZED` CTEs; parallel query settings; materialized views with concurrent refresh |
| `02_bigquery_optimization.sql` | Partition pruning (`require_partition_filter`), clustering keys, `APPROX_COUNT_DISTINCT`, `APPROX_QUANTILES`, scripting with `LOOP/DECLARE`, nested `ARRAY<STRUCT>`, SQL Server `MERGE` and Columnstore index reference |

---

## Key Concepts Demonstrated

### Data Modeling
- Kimball dimensional modeling (star schema)
- Data Vault 2.0 (Hubs, Links, Satellites, PIT tables)
- SCD Type 2 (slowly changing dimensions)
- Bridge tables for many-to-many relationships
- Surrogate keys + business key separation
- Computed / generated columns

### ETL / ELT
- Incremental load with high-watermark CDC
- Staging → DQ → Load pipeline pattern
- Data quality rule framework with rejection logging
- UPSERT (`INSERT ... ON CONFLICT`, `MERGE`)
- Snowflake Streams + Tasks (CDC orchestration)
- Snowpipe for continuous file ingest
- Dynamic Tables (declarative ELT)
- FHIR/HL7 JSON parsing with VARIANT

### Analytics
- Window functions: `LAG`, `LEAD`, `RANK`, `DENSE_RANK`, `NTILE`, `PERCENT_RANK`
- Frame clauses: `ROWS BETWEEN`, `RANGE BETWEEN INTERVAL`
- Cohort retention analysis
- LACE readmission risk scoring
- Rolling averages and daily census tracking
- Conditional aggregation (PIVOT pattern)

### Performance Engineering
- Partial indexes and covering indexes (`INCLUDE`)
- BRIN indexes for time-ordered fact tables
- Range partitioning with partition pruning
- Extended statistics for correlated columns
- CTE materialization control
- Sargable query rewrites
- BigQuery partition + cluster cost optimization
- Materialized views with concurrent refresh

---

## Schema Reference

```
                    ┌──────────────┐
                    │  dim_date    │
                    └──────┬───────┘
                           │
┌──────────────┐    ┌──────▼───────────────────────────────────┐    ┌──────────────────┐
│ dim_patient  ├────►                                           ◄────┤  dim_provider    │
└──────────────┘    │             fact_encounter                │    └──────────────────┘
                    │  (grain: one encounter)                   │
┌──────────────┐    │                                           ◄────┬──────────────────┐
│ dim_facility ├────►                                           │    │  dim_diagnosis   │
└──────────────┘    └───────────────┬───────────────────────────┘    └──────────────────┘
                                    │
                    ┌───────────────▼──────────────────┐
                    │  bridge_encounter_diagnosis       │
                    │  (multi-valued secondary dx)      │
                    └──────────────────────────────────┘

                    ┌──────────────────────────────────┐
                    │       fact_claim_line             │
                    │  (grain: one claim line item)     │
                    └──────────────────────────────────┘
```

---

## Getting Started

### PostgreSQL
```bash
createdb healthcare_dw
psql -d healthcare_dw -f 01_data_modeling/01_star_schema_postgresql.sql
psql -d healthcare_dw -f 02_etl_pipelines/01_incremental_load_postgresql.sql
psql -d healthcare_dw -f 03_window_functions/01_clinical_analytics_postgresql_bq.sql
psql -d healthcare_dw -f 04_performance_tuning/01_indexing_and_optimization_postgresql.sql
```

### Snowflake
```sql
USE ROLE SYSADMIN;
CREATE DATABASE IF NOT EXISTS HEALTHCARE_DV;
CREATE SCHEMA IF NOT EXISTS HEALTHCARE_DV.RAW_VAULT;
CREATE SCHEMA IF NOT EXISTS HEALTHCARE_DV.INFORMATION_MART;
CREATE SCHEMA IF NOT EXISTS HEALTHCARE_DV.LANDING;
-- Then run files in 01_data_modeling/ and 02_etl_pipelines/ in order
```

### BigQuery
```bash
bq mk --dataset --location=US healthcare_dw
bq query --use_legacy_sql=false < 04_performance_tuning/02_bigquery_optimization.sql
```

---

## Dialect Compatibility

| Feature | PostgreSQL | Snowflake | BigQuery | SQL Server |
|---------|-----------|-----------|----------|------------|
| Window functions | Full | Full | Full | Full |
| CTE materialization hints | 12+ | Yes | Yes | 2019+ |
| MERGE / UPSERT | ON CONFLICT | MERGE | MERGE | MERGE |
| Table partitioning | Native | Clustering | Native | Native |
| Semi-structured / JSON | JSONB | VARIANT | JSON/ARRAY | Limited |
| QUALIFY clause | No (use subquery) | Yes | Yes | No |
| Streams / CDC | Logical replication | Native Streams | N/A | CDC feature |

---

*Built as a professional portfolio for data architect and senior data engineer roles.*  
*All patient data is entirely synthetic — no real PHI is used or referenced.*
