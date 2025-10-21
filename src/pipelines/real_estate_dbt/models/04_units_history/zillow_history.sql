-- models/04_units_history/zillow_history.sql

{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['id', 'date'], 'type': 'btree'},
      {'columns': ['msa', 'date'], 'type': 'btree'},
      {'columns': ['zip_code', 'date'], 'type': 'btree'},
      {'columns': ['state', 'date'], 'type': 'btree'}
    ]
  )
}}

WITH valid_zillow_data AS (
    SELECT
        z.id,
        z.msa,
        z.zip_code,
        z.city,
        z.state,
        z.price_history
    FROM {{ ref('stg_zillow') }} z
    WHERE z.price_history IS NOT NULL
      AND LENGTH(z.price_history::text) > 4
      AND z.price_history::text != 'null'
      AND z.price_history::text != ''
      AND z.price_history::text != '""'
),
json_cleaned AS (
    SELECT
        z.id,
        z.msa,
        z.zip_code,
        z.city,
        z.state,
        -- Remove the outer quotes and unescape the JSON
        REPLACE(
            REPLACE(
                TRIM(BOTH '"' FROM z.price_history::text),
                '\"',
                '"'
            ),
            '\\',
            ''
        )::jsonb as clean_json
    FROM valid_zillow_data z
),
price_history_parsed AS (
    SELECT
        z.id,
        z.msa,
        z.zip_code,
        z.city,
        z.state,
        (price_point->>'date')::DATE as date,
        (price_point->>'price')::DECIMAL as price,
        (price_point->>'priceChangeRate')::DECIMAL as price_change_rate
    FROM json_cleaned z,
         jsonb_array_elements(z.clean_json) as price_point
),
price_stats AS (
    SELECT
        id,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY price) as q1,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY price) as q3,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY price_change_rate) as q1_change,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY price_change_rate) as q3_change
    FROM price_history_parsed
    WHERE price > 0
      AND date IS NOT NULL
    GROUP BY id
),
outlier_bounds AS (
    SELECT
        id,
        q1,
        q3,
        q1 - 1.5 * (q3 - q1) as price_lower_bound,
        q3 + 1.5 * (q3 - q1) as price_upper_bound,
        q1_change,
        q3_change,
        q1_change - 1.5 * (q3_change - q1_change) as change_lower_bound,
        q3_change + 1.5 * (q3_change - q1_change) as change_upper_bound
    FROM price_stats
),
filtered_data AS (
    SELECT
        p.id,
        p.date,
        p.price,
        p.price_change_rate,
        p.msa,
        p.zip_code,
        p.city,
        p.state
    FROM price_history_parsed p
    INNER JOIN outlier_bounds b ON p.id = b.id
    WHERE p.price > 0
      AND p.date IS NOT NULL
      AND p.price >= b.price_lower_bound
      AND p.price <= b.price_upper_bound
      AND (
          p.price_change_rate IS NULL
          OR (
              p.price_change_rate >= b.change_lower_bound
              AND p.price_change_rate <= b.change_upper_bound
          )
      )
)
SELECT
    id,
    date,
    price,
    price_change_rate,
    msa,
    zip_code,
    city,
    state
FROM filtered_data
ORDER BY id, date