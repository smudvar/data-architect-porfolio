-- ============================================================
-- STAGING: stg_events
-- Source : RAW.PRODUCT.events
-- Purpose: Clean, type-cast, and rename raw event columns.
--          This is the ONLY place raw source logic lives.
--          All downstream models reference this staging model.
-- ============================================================

WITH source AS (
    SELECT * FROM {{ source('product_raw', 'events') }}
),

renamed AS (
    SELECT
        -- Keys
        event_id::VARCHAR(36)               AS event_id,
        user_id::VARCHAR(36)                AS user_id,
        session_id::VARCHAR(36)             AS session_id,

        -- Event metadata
        UPPER(TRIM(event_type))             AS event_type,
        page_url::VARCHAR(500)              AS page_url,
        feature_name::VARCHAR(100)          AS feature_name,
        UPPER(TRIM(platform))               AS platform,        -- WEB / IOS / ANDROID

        -- Device & geography
        UPPER(TRIM(device_type))            AS device_type,     -- DESKTOP / MOBILE / TABLET
        country_code::CHAR(2)               AS country_code,
        UPPER(TRIM(utm_source))             AS utm_source,
        UPPER(TRIM(utm_medium))             AS utm_medium,
        UPPER(TRIM(utm_campaign))           AS utm_campaign,

        -- Timestamps — always store in UTC
        CONVERT_TIMEZONE('UTC', occurred_at)::TIMESTAMP_NTZ  AS occurred_at,
        DATE_TRUNC('day',  occurred_at)::DATE                 AS event_date,
        DATE_TRUNC('hour', occurred_at)::TIMESTAMP_NTZ        AS event_hour,

        -- Audit columns
        _loaded_at::TIMESTAMP_NTZ           AS _loaded_at,
        _source_file::VARCHAR(500)          AS _source_file

    FROM source
    WHERE event_id IS NOT NULL      -- drop records with no PK
      AND occurred_at IS NOT NULL   -- drop records with no timestamp
)

SELECT * FROM renamed
