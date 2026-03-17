-- ============================================================
-- SINGULAR TEST: assert_no_future_events
-- Purpose: Events should never have an occurred_at timestamp
--          in the future — flags ETL timezone bugs early.
-- dbt runs this as: dbt test --select assert_no_future_events
-- ============================================================

SELECT
    event_id,
    user_id,
    occurred_at,
    CURRENT_TIMESTAMP() AS tested_at
FROM {{ ref('stg_events') }}
WHERE occurred_at > DATEADD('hour', 1, CURRENT_TIMESTAMP())
-- Allow 1-hour buffer for clock skew between source and warehouse
