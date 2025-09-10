-- 05_prediction_analysis/building_analysis.sql

{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['building_id'], 'type': 'btree'},
      {'columns': ['data_span_days'], 'type': 'btree'},
      {'columns': ['building_id'], 'type': 'btree'},
      {'columns': ['latest_date'], 'type': 'btree'}
    ]
  )
}}

-- Create an empty table with the correct schema
-- This will be populated by the Python ML pipeline
SELECT
    -- Building identifier
    CAST(NULL AS UUID) AS building_id,

    -- Historical price data
    CAST(NULL AS DECIMAL(10,2)) AS latest_price,
    CAST(NULL AS DECIMAL(10,2)) AS latest_effective_price,

    -- Current price predictions (for today)
    CAST(NULL AS DECIMAL(10,2)) AS predicted_current_price,
    CAST(NULL AS DECIMAL(10,2)) AS predicted_current_price_lower,
    CAST(NULL AS DECIMAL(10,2)) AS predicted_current_price_upper,
    CAST(NULL AS DECIMAL(10,2)) AS predicted_current_effective_price,
    CAST(NULL AS DECIMAL(10,2)) AS predicted_current_effective_price_lower,
    CAST(NULL AS DECIMAL(10,2)) AS predicted_current_effective_price_upper,

    -- Future price predictions (forecast_days ahead)
    CAST(NULL AS DECIMAL(10,2)) AS predicted_future_price,
    CAST(NULL AS DECIMAL(10,2)) AS predicted_future_price_lower,
    CAST(NULL AS DECIMAL(10,2)) AS predicted_future_price_upper,
    CAST(NULL AS DECIMAL(10,2)) AS predicted_future_effective_price,
    CAST(NULL AS DECIMAL(10,2)) AS predicted_future_effective_price_lower,
    CAST(NULL AS DECIMAL(10,2)) AS predicted_future_effective_price_upper,

    -- Growth metrics (CAGR)
    CAST(NULL AS DECIMAL(8,4)) AS average_percent_gain_per_year,
    CAST(NULL AS DECIMAL(8,4)) AS average_percent_gain_per_year_effective,

    -- Trend analysis metrics
    CAST(NULL AS DECIMAL(8,4)) AS trend_strength_pct,
    CAST(NULL AS DECIMAL(8,4)) AS trend_strength_effective_pct,
    CAST(NULL AS DECIMAL(8,4)) AS trend_variance_pct,
    CAST(NULL AS DECIMAL(8,4)) AS trend_variance_effective_pct,

    -- Date information
    CAST(NULL AS TIMESTAMP) AS latest_date,
    CAST(NULL AS TIMESTAMP) AS prediction_date,
    CAST(NULL AS TIMESTAMP) AS min_date,
    CAST(NULL AS TIMESTAMP) AS max_date,

    -- Data quality metrics
    CAST(NULL AS INTEGER) AS data_span_days,

    -- System timestamp
    CAST(NULL AS TIMESTAMP) AS created_at

-- This WHERE clause ensures no rows are actually inserted
-- We just want the table structure
WHERE 1 = 0