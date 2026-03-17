-- ============================================================
-- STAGING: stg_users
-- Source : RAW.PRODUCT.users
-- Purpose: Clean and standardise user dimension data.
-- ============================================================

WITH source AS (
    SELECT * FROM {{ source('product_raw', 'users') }}
),

renamed AS (
    SELECT
        -- Keys
        user_id::VARCHAR(36)                AS user_id,

        -- Identity (never expose PII in marts — masked downstream)
        LOWER(TRIM(email))                  AS email,
        TRIM(first_name)                    AS first_name,
        TRIM(last_name)                     AS last_name,

        -- Plan & lifecycle
        LOWER(TRIM(plan_name))              AS plan_name,
        LOWER(TRIM(account_status))         AS account_status,  -- active / churned / suspended

        -- Acquisition
        LOWER(TRIM(signup_source))          AS signup_source,   -- organic / paid / referral
        UPPER(TRIM(country_code))           AS country_code,

        -- Timestamps
        CONVERT_TIMEZONE('UTC', created_at)::TIMESTAMP_NTZ   AS created_at,
        DATE_TRUNC('day', created_at)::DATE                   AS signup_date,
        CONVERT_TIMEZONE('UTC', updated_at)::TIMESTAMP_NTZ   AS updated_at,

        -- Derived
        DATEDIFF('day', created_at, CURRENT_DATE())           AS days_since_signup,

        -- Audit
        _loaded_at::TIMESTAMP_NTZ           AS _loaded_at

    FROM source
    WHERE user_id IS NOT NULL
),

-- Deduplicate: keep most recently updated record per user
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY     updated_at DESC
        ) AS _row_num
    FROM renamed
)

SELECT * EXCLUDE (_row_num)
FROM   deduped
WHERE  _row_num = 1
