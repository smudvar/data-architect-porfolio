-- ============================================================
-- SINGULAR TEST: assert_funnel_stage_ordering
-- Purpose: A user cannot activate before they log in, and cannot
--          convert before they activate. Detects event ordering
--          anomalies from the source system.
-- ============================================================

SELECT
    user_id,
    signed_up_at,
    first_login_at,
    activated_at,
    converted_at,
    'login_before_signup'   AS violation_type
FROM {{ ref('mart_funnel_analysis') }}
WHERE first_login_at < signed_up_at

UNION ALL

SELECT
    user_id,
    signed_up_at,
    first_login_at,
    activated_at,
    converted_at,
    'activation_before_login' AS violation_type
FROM {{ ref('mart_funnel_analysis') }}
WHERE activated_at IS NOT NULL
  AND first_login_at IS NOT NULL
  AND activated_at < first_login_at

UNION ALL

SELECT
    user_id,
    signed_up_at,
    first_login_at,
    activated_at,
    converted_at,
    'conversion_before_activation' AS violation_type
FROM {{ ref('mart_funnel_analysis') }}
WHERE converted_at  IS NOT NULL
  AND activated_at  IS NOT NULL
  AND converted_at < activated_at
