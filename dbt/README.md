# dbt — Snowflake Models

> Production-grade dbt project targeting Snowflake. Covers staging, mart, and incremental models for a SaaS product analytics warehouse.

---

## Project Structure

```
dbt/
├── dbt_project.yml                    # Project config, materializations, Snowflake settings
├── macros/
│   └── helpers.sql                    # safe_divide, date_spine, surrogate_key, get_column_values
├── models/
│   ├── staging/
│   │   ├── sources.yml                # Source declarations + freshness checks
│   │   ├── stg_events.sql             # Cleaned event stream
│   │   ├── stg_users.sql              # Deduplicated user dimension
│   │   └── stg_sessions.sql           # Sessions with capped duration
│   └── marts/
│       └── product/
│           ├── schema.yml             # Column-level tests & documentation
│           ├── mart_user_activity.sql # Daily per-user activity (INCREMENTAL)
│           ├── mart_dau_wau_mau.sql   # DAU/WAU/MAU aggregates + stickiness
│           └── mart_funnel_analysis.sql # Per-user funnel stage + time-to-convert
└── tests/
    ├── assert_no_future_events.sql    # Singular: no events timestamped in the future
    └── assert_funnel_stage_ordering.sql # Singular: activation can't precede login
```

---

## Materialization Strategy

| Layer | Materialization | Rationale |
|---|---|---|
| Staging | `view` | Always fresh; zero storage cost; never queried directly by BI |
| Intermediate | `ephemeral` | Compiled inline; no Snowflake objects created |
| Core marts | `table` | Rebuilt daily; predictable query performance |
| Product marts | `incremental` (merge) | Append-only facts; only processes new dates each run |

---

## Key Models

### `mart_user_activity` (incremental)
- One row per user per active day
- Incremental merge on `(user_id, activity_date)` with 3-day lookback for late-arriving events
- Computes engagement tier: `power` / `engaged` / `casual` / `passive`

### `mart_dau_wau_mau` (table)
- Pre-aggregated DAU, WAU, MAU by plan and country
- Includes DAU/MAU stickiness ratio (benchmark: >20% = healthy product)
- Feature adoption rate and rolling 7-day DAU average

### `mart_funnel_analysis` (table)
- One row per user — furthest funnel stage reached in first 30 days
- Time-to-convert at P25, P50, P75 for each stage
- `activated_within_7d` flag for product-led growth benchmarking

---

## Tests

### Generic tests (schema.yml)
- `unique` + `not_null` on all primary keys
- `accepted_values` on enums (plan_name, event_type, engagement_tier)
- `dbt_utils.accepted_range` on numeric metrics (stickiness must be 0–1)

### Singular tests
| Test | What It Catches |
|---|---|
| `assert_no_future_events` | ETL timezone bugs causing future-dated events |
| `assert_funnel_stage_ordering` | Source system anomalies where activation precedes login |

---

## Running the Project

```bash
# Install dependencies
pip install dbt-snowflake

# Test connection
dbt debug

# Run all models
dbt run

# Run tests
dbt test

# Run staging only
dbt run --select staging

# Run a single model + its dependencies
dbt run --select +mart_funnel_analysis

# Check source freshness
dbt source freshness

# Generate and serve docs
dbt docs generate
dbt docs serve
```
