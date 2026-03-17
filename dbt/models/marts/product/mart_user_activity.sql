-- ============================================================
-- MART: mart_user_activity  (product.dau_wau_mau)
-- Materialization: INCREMENTAL (merge strategy — Snowflake)
-- Purpose: Daily pre-aggregated activity metrics per user.
--          Powers DAU, WAU, MAU and stickiness dashboards.
--          Incremental load appends only new event dates.
-- ============================================================

{{
    config(
        materialized        = 'incremental',
        incremental_strategy= 'merge',
        unique_key          = ['user_id', 'activity_date'],
        cluster_by          = ['activity_date'],
        on_schema_change    = 'sync_all_columns',
        tags                = ['product', 'daily']
    )
}}

WITH daily_events AS (
    SELECT
        user_id,
        event_date                          AS activity_date,
        COUNT(*)                            AS total_events,
        COUNT(DISTINCT session_id)          AS sessions,
        COUNT(DISTINCT event_type)          AS distinct_event_types,
        COUNT_IF(event_type = 'FEATURE_USED')   AS feature_events,
        COUNT_IF(event_type = 'PAGE_VIEW')       AS page_views,
        MIN(occurred_at)                    AS first_event_at,
        MAX(occurred_at)                    AS last_event_at,
        DATEDIFF('minute',
            MIN(occurred_at),
            MAX(occurred_at))               AS active_minutes
    FROM {{ ref('stg_events') }}

    -- ── Incremental filter ────────────────────────────────────
    -- On first run: process all history
    -- On subsequent runs: process only the last 3 days
    -- (3-day lookback handles late-arriving events)
    {% if is_incremental() %}
    WHERE event_date >= (
        SELECT DATEADD('day', -3, MAX(activity_date))
        FROM   {{ this }}
    )
    {% endif %}

    GROUP BY user_id, event_date
),

-- Enrich with user dimension attributes
enriched AS (
    SELECT
        da.user_id,
        da.activity_date,
        u.plan_name,
        u.signup_date,
        u.country_code,
        u.signup_source,
        DATEDIFF('day', u.signup_date, da.activity_date)  AS days_since_signup,

        -- Cohort week (for cohort retention analysis)
        DATE_TRUNC('week', u.signup_date)::DATE            AS signup_cohort_week,

        -- Activity metrics
        da.total_events,
        da.sessions,
        da.distinct_event_types,
        da.feature_events,
        da.page_views,
        da.active_minutes,
        da.first_event_at,
        da.last_event_at,

        -- Engagement tier for the day
        CASE
            WHEN da.active_minutes >= 30 AND da.feature_events >= 5 THEN 'power'
            WHEN da.active_minutes >= 10 OR  da.feature_events >= 2 THEN 'engaged'
            WHEN da.total_events   >= 1                              THEN 'casual'
            ELSE 'passive'
        END                                                AS engagement_tier,

        CURRENT_TIMESTAMP()                                AS _updated_at

    FROM       daily_events          da
    LEFT JOIN  {{ ref('stg_users') }} u
        ON u.user_id = da.user_id
)

SELECT * FROM enriched
