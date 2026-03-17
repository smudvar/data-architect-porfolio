-- ============================================================
-- MART: mart_dau_wau_mau
-- Materialization: TABLE (rebuilt daily)
-- Purpose: Pre-aggregated DAU / WAU / MAU counts and stickiness
--          ratios. Direct source for executive dashboards.
-- ============================================================

{{
    config(
        materialized = 'table',
        cluster_by   = ['activity_date'],
        tags         = ['product', 'daily', 'kpi']
    )
}}

WITH daily_active AS (
    SELECT
        activity_date,
        plan_name,
        country_code,
        COUNT(DISTINCT user_id)                         AS dau,
        SUM(total_events)                               AS daily_events,
        SUM(feature_events)                             AS daily_feature_events,
        AVG(active_minutes)                             AS avg_active_minutes,
        COUNT_IF(engagement_tier = 'power')             AS power_users,
        COUNT_IF(engagement_tier = 'engaged')           AS engaged_users
    FROM {{ ref('mart_user_activity') }}
    GROUP BY activity_date, plan_name, country_code
),

-- Rolling 7-day and 28-day unique user counts
rolling_windows AS (
    SELECT
        da.activity_date,
        da.plan_name,
        da.country_code,
        da.dau,
        da.daily_events,
        da.daily_feature_events,
        da.avg_active_minutes,
        da.power_users,
        da.engaged_users,

        -- WAU: distinct users in trailing 7 days
        COUNT(DISTINCT ua.user_id)
            FILTER (WHERE ua.activity_date
                         BETWEEN DATEADD('day', -6, da.activity_date)
                             AND da.activity_date)
            OVER (PARTITION BY da.plan_name, da.country_code
                  ORDER BY     da.activity_date
                  ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
                                                        AS wau,

        -- MAU: distinct users in trailing 28 days
        COUNT(DISTINCT ua.user_id)
            FILTER (WHERE ua.activity_date
                         BETWEEN DATEADD('day', -27, da.activity_date)
                             AND da.activity_date)
            OVER (PARTITION BY da.plan_name, da.country_code
                  ORDER BY     da.activity_date
                  ROWS BETWEEN 27 PRECEDING AND CURRENT ROW)
                                                        AS mau

    FROM       daily_active                  da
    LEFT JOIN  {{ ref('mart_user_activity') }} ua
        ON  ua.activity_date BETWEEN DATEADD('day', -27, da.activity_date)
                                 AND da.activity_date
        AND ua.plan_name    = da.plan_name
        AND ua.country_code = da.country_code
)

SELECT
    activity_date,
    plan_name,
    country_code,
    dau,
    wau,
    mau,
    daily_events,
    daily_feature_events,
    avg_active_minutes,
    power_users,
    engaged_users,

    -- Stickiness ratios (key product health metrics)
    ROUND(dau / NULLIF(wau, 0), 4)          AS dau_wau_stickiness,
    ROUND(dau / NULLIF(mau, 0), 4)          AS dau_mau_stickiness,
    ROUND(wau / NULLIF(mau, 0), 4)          AS wau_mau_stickiness,

    -- Feature adoption rate
    ROUND(daily_feature_events
          / NULLIF(daily_events, 0), 4)     AS feature_adoption_rate,

    CURRENT_TIMESTAMP()                     AS _updated_at

FROM rolling_windows
