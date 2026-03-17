-- ============================================================
-- MART: mart_funnel_analysis
-- Materialization: TABLE
-- Purpose: Ordered conversion funnel from signup → activation
--          → feature adoption → upgrade.
--          Each row = one user's furthest funnel stage reached
--          within their first 30 days.
-- ============================================================

{{
    config(
        materialized = 'table',
        tags         = ['product', 'funnel']
    )
}}

-- Define the ordered funnel stages and the events that qualify
WITH funnel_events AS (
    SELECT
        e.user_id,
        e.occurred_at,
        e.event_date,
        CASE e.event_type
            WHEN 'SIGNUP'        THEN 1
            WHEN 'LOGIN'         THEN 2
            WHEN 'FEATURE_USED'  THEN 3
            WHEN 'UPGRADE'       THEN 4
        END                                     AS funnel_stage,
        CASE e.event_type
            WHEN 'SIGNUP'        THEN 'Signup'
            WHEN 'LOGIN'         THEN 'First Login'
            WHEN 'FEATURE_USED'  THEN 'Feature Activated'
            WHEN 'UPGRADE'       THEN 'Converted to Paid'
        END                                     AS stage_name
    FROM {{ ref('stg_events') }} e
    WHERE e.event_type IN ('SIGNUP', 'LOGIN', 'FEATURE_USED', 'UPGRADE')
),

-- First time each user hit each stage
first_stage_hits AS (
    SELECT
        user_id,
        funnel_stage,
        stage_name,
        MIN(occurred_at)                        AS first_hit_at,
        MIN(event_date)                         AS first_hit_date
    FROM funnel_events
    GROUP BY user_id, funnel_stage, stage_name
),

-- Pivot so each user has one row with all stage timestamps
user_funnel AS (
    SELECT
        u.user_id,
        u.signup_date,
        u.plan_name,
        u.country_code,
        u.signup_source,
        DATE_TRUNC('week', u.signup_date)::DATE AS signup_cohort_week,

        -- Stage timestamps
        MAX(CASE WHEN fsh.funnel_stage = 1 THEN fsh.first_hit_at END) AS signed_up_at,
        MAX(CASE WHEN fsh.funnel_stage = 2 THEN fsh.first_hit_at END) AS first_login_at,
        MAX(CASE WHEN fsh.funnel_stage = 3 THEN fsh.first_hit_at END) AS activated_at,
        MAX(CASE WHEN fsh.funnel_stage = 4 THEN fsh.first_hit_at END) AS converted_at,

        -- Furthest stage reached
        MAX(fsh.funnel_stage)                   AS max_stage_reached,
        MAX(fsh.stage_name)
            KEEP (DENSE_RANK LAST ORDER BY fsh.funnel_stage)
                                                AS furthest_stage_name

    FROM       {{ ref('stg_users') }}  u
    LEFT JOIN  first_stage_hits        fsh ON fsh.user_id = u.user_id
    GROUP BY
        u.user_id, u.signup_date, u.plan_name,
        u.country_code, u.signup_source,
        DATE_TRUNC('week', u.signup_date)::DATE
)

SELECT
    uf.*,

    -- Time-to-convert metrics (in hours)
    DATEDIFF('hour', signed_up_at,   first_login_at) AS hours_to_first_login,
    DATEDIFF('hour', signed_up_at,   activated_at)   AS hours_to_activation,
    DATEDIFF('hour', signed_up_at,   converted_at)   AS hours_to_conversion,

    -- Boolean flags for each stage (convenient for cohort slicing)
    signed_up_at   IS NOT NULL                       AS did_signup,
    first_login_at IS NOT NULL                       AS did_first_login,
    activated_at   IS NOT NULL                       AS did_activate,
    converted_at   IS NOT NULL                       AS did_convert,

    -- Activation within 7 days (product-led growth benchmark)
    CASE
        WHEN activated_at IS NOT NULL
         AND DATEDIFF('day', signed_up_at, activated_at) <= 7
        THEN TRUE ELSE FALSE
    END                                              AS activated_within_7d,

    CURRENT_TIMESTAMP()                              AS _updated_at

FROM user_funnel uf
