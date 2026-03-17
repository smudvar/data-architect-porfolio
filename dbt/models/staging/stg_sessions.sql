-- ============================================================
-- STAGING: stg_sessions
-- Source : RAW.PRODUCT.sessions
-- Purpose: Clean session data and compute session duration.
-- ============================================================

WITH source AS (
    SELECT * FROM {{ source('product_raw', 'sessions') }}
),

renamed AS (
    SELECT
        session_id::VARCHAR(36)             AS session_id,
        user_id::VARCHAR(36)                AS user_id,

        CONVERT_TIMEZONE('UTC', started_at)::TIMESTAMP_NTZ   AS started_at,
        CONVERT_TIMEZONE('UTC', ended_at)::TIMESTAMP_NTZ     AS ended_at,
        DATE_TRUNC('day', started_at)::DATE                   AS session_date,

        -- Duration in seconds; cap at 4 hours to filter zombie sessions
        LEAST(
            DATEDIFF('second', started_at, ended_at),
            14400
        )                                   AS session_duration_seconds,

        UPPER(TRIM(platform))               AS platform,
        UPPER(TRIM(device_type))            AS device_type,
        country_code::CHAR(2)               AS country_code,

        -- Engagement signal
        page_views::INT                     AS page_views,
        events_count::INT                   AS events_count,

        _loaded_at::TIMESTAMP_NTZ           AS _loaded_at

    FROM source
    WHERE session_id IS NOT NULL
      AND started_at IS NOT NULL
      AND started_at <= COALESCE(ended_at, CURRENT_TIMESTAMP())
)

SELECT * FROM renamed
