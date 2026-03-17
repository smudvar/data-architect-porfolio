-- ============================================================
-- MACROS: Reusable Jinja helpers for Snowflake + dbt
-- ============================================================


-- ── 1. safe_divide ────────────────────────────────────────────
-- Usage : {{ safe_divide('numerator_col', 'denominator_col') }}
-- Returns NULL instead of dividing by zero.
-- Avoids repeating NULLIF(denominator, 0) across every model.

{% macro safe_divide(numerator, denominator) %}
    DIV0({{ numerator }}, {{ denominator }})
{% endmacro %}


-- ── 2. date_spine ────────────────────────────────────────────
-- Usage : {{ date_spine(start_date='2023-01-01', end_date='today') }}
-- Generates a continuous series of dates — essential for filling
-- gaps in DAU charts where users had zero activity on some days.

{% macro date_spine(start_date, end_date='current_date()') %}
    WITH date_series AS (
        SELECT
            DATEADD(
                'day',
                ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1,
                '{{ start_date }}'::DATE
            ) AS spine_date
        FROM TABLE(GENERATOR(ROWCOUNT => 3650))   -- up to 10 years
    )
    SELECT spine_date
    FROM   date_series
    WHERE  spine_date <= {{ end_date }}
{% endmacro %}


-- ── 3. get_column_values ─────────────────────────────────────
-- Usage : {% set plans = get_column_values(ref('stg_users'), 'plan_name') %}
-- Fetches distinct column values at compile time for dynamic pivots.

{% macro get_column_values(table, column, max_records=50) %}
    {% set query %}
        SELECT DISTINCT {{ column }}
        FROM   {{ table }}
        WHERE  {{ column }} IS NOT NULL
        ORDER BY {{ column }}
        LIMIT {{ max_records }}
    {% endset %}

    {% set results = run_query(query) %}

    {% if execute %}
        {{ return(results.columns[0].values()) }}
    {% else %}
        {{ return([]) }}
    {% endif %}
{% endmacro %}


-- ── 4. surrogate_key ─────────────────────────────────────────
-- Usage : {{ surrogate_key(['user_id', 'activity_date']) }}
-- Generates a deterministic surrogate key from composite columns.
-- Consistent with dbt_utils.generate_surrogate_key but Snowflake-native.

{% macro surrogate_key(field_list) %}
    MD5(
        CONCAT_WS('|',
            {% for field in field_list %}
                COALESCE(CAST({{ field }} AS VARCHAR), '_null_')
                {%- if not loop.last %},{% endif %}
            {% endfor %}
        )
    )
{% endmacro %}


-- ── 5. cents_to_dollars ──────────────────────────────────────
-- Usage : {{ cents_to_dollars('amount_cents') }}
-- Prevents the classic "is this dollars or cents?" bug in finance models.

{% macro cents_to_dollars(column_name, precision=2) %}
    ROUND({{ column_name }} / 100.0, {{ precision }})
{% endmacro %}
