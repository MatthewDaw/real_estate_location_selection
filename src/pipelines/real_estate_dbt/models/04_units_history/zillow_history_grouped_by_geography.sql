-- models/04_units_history/zillow_history_grouped_by_geography.sql

{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['geography_type', 'geography_value', 'date'], 'type': 'btree'},
      {'columns': ['date'], 'type': 'btree'}
    ]
  )
}}

-- Configuration: Change rolling average window size here
{% set rolling_window_months = 3 %}

WITH zip_aggregated AS (
    SELECT
        'zip_code' AS geography_type,
        zip_code AS geography_value,
        date,
        AVG(price) AS price,
        AVG(price_change_rate) AS price_change_rate,
        COUNT(*) AS property_count
    FROM {{ ref('zillow_history') }}
    WHERE zip_code IS NOT NULL
    GROUP BY zip_code, date
),

city_aggregated AS (
    SELECT
        'city' AS geography_type,
        city AS geography_value,
        date,
        AVG(price) AS price,
        AVG(price_change_rate) AS price_change_rate,
        COUNT(*) AS property_count
    FROM {{ ref('zillow_history') }}
    WHERE city IS NOT NULL
    GROUP BY city, date
),

state_aggregated AS (
    SELECT
        'state' AS geography_type,
        state AS geography_value,
        date,
        AVG(price) AS price,
        AVG(price_change_rate) AS price_change_rate,
        COUNT(*) AS property_count
    FROM {{ ref('zillow_history') }}
    WHERE state IS NOT NULL
    GROUP BY state, date
),

msa_aggregated AS (
    SELECT
        'msa' AS geography_type,
        msa AS geography_value,
        date,
        AVG(price) AS price,
        AVG(price_change_rate) AS price_change_rate,
        COUNT(*) AS property_count
    FROM {{ ref('zillow_history') }}
    WHERE msa IS NOT NULL
    GROUP BY msa, date
),

combined_geography AS (
    SELECT * FROM zip_aggregated
    UNION ALL
    SELECT * FROM city_aggregated
    UNION ALL
    SELECT * FROM state_aggregated
    UNION ALL
    SELECT * FROM msa_aggregated
),

rolling_averages AS (
    SELECT
        geography_type,
        geography_value,
        date,
        price,
        price_change_rate,
        property_count,
        -- Calculate rolling average price over specified date window
        AVG(price) OVER (
            PARTITION BY geography_type, geography_value
            ORDER BY date
            RANGE BETWEEN INTERVAL '{{ rolling_window_months - 1 }} months' PRECEDING AND CURRENT ROW
        ) as price_rolling_avg,
        -- Calculate rolling average price change rate over specified date window
        AVG(price_change_rate) OVER (
            PARTITION BY geography_type, geography_value
            ORDER BY date
            RANGE BETWEEN INTERVAL '{{ rolling_window_months - 1 }} months' PRECEDING AND CURRENT ROW
        ) as price_change_rate_rolling_avg,
        -- Calculate rolling average property count over specified date window
        AVG(property_count::DECIMAL) OVER (
            PARTITION BY geography_type, geography_value
            ORDER BY date
            RANGE BETWEEN INTERVAL '{{ rolling_window_months - 1 }} months' PRECEDING AND CURRENT ROW
        ) as property_count_rolling_avg,
        -- Count of data points in the current date window
        COUNT(*) OVER (
            PARTITION BY geography_type, geography_value
            ORDER BY date
            RANGE BETWEEN INTERVAL '{{ rolling_window_months - 1 }} months' PRECEDING AND CURRENT ROW
        ) as window_count,
        -- Calculate the actual date span of the window for validation
        date - MIN(date) OVER (
            PARTITION BY geography_type, geography_value
            ORDER BY date
            RANGE BETWEEN INTERVAL '{{ rolling_window_months - 1 }} months' PRECEDING AND CURRENT ROW
        ) as actual_window_span
    FROM combined_geography
)

SELECT
    geography_type,
    geography_value,
    actual_window_span,
    date,
    ROUND(price_rolling_avg, 2) as price,
    ROUND(price_change_rate_rolling_avg, 4) as price_change_rate,
    ROUND(property_count_rolling_avg, 0) as property_count
FROM rolling_averages
WHERE window_count >= 2  -- Ensure at least 2 data points in the rolling window
  AND actual_window_span >= ({{ rolling_window_months }} - 1) * 0.5 * 30  -- Ensure reasonable time coverage
ORDER BY geography_type, geography_value, date