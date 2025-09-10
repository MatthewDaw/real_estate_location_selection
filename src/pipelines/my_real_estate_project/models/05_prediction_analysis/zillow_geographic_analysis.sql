-- 05_prediction_analysis/zillow_geographic_analysis.sql

{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['geography_type', 'geography_value'], 'type': 'btree'},
      {'columns': ['geography_type'], 'type': 'btree'},
      {'columns': ['geography_value'], 'type': 'btree'},
      {'columns': ['data_span_days'], 'type': 'btree'},
      {'columns': ['geography_type', 'geography_value'], 'type': 'btree'},
      {'columns': ['latest_date'], 'type': 'btree'}
    ]
  )
}}

-- Create an empty table with the correct schema for Zillow geographic time series analysis
-- This will be populated by the Python ML pipeline
SELECT
    -- Geographic identifiers
    CAST(NULL AS VARCHAR) AS geography_type,
    CAST(NULL AS VARCHAR) AS geography_value,

    -- Historical price data (Zillow only has regular price, no effective price)
    CAST(NULL AS DECIMAL(10,2)) AS latest_price,

    -- Current price predictions (for today) with confidence intervals
    CAST(NULL AS DECIMAL(10,2)) AS predicted_current_price,
    CAST(NULL AS DECIMAL(10,2)) AS predicted_current_price_lower,
    CAST(NULL AS DECIMAL(10,2)) AS predicted_current_price_upper,

    -- Future price predictions (forecast_days ahead) with confidence intervals
    CAST(NULL AS DECIMAL(10,2)) AS predicted_future_price,
    CAST(NULL AS DECIMAL(10,2)) AS predicted_future_price_lower,
    CAST(NULL AS DECIMAL(10,2)) AS predicted_future_price_upper,

    -- Growth metrics (CAGR)
    CAST(NULL AS DECIMAL(8,4)) AS average_percent_gain_per_year,

    -- Trend analysis metrics
    CAST(NULL AS DECIMAL(8,4)) AS trend_strength_pct,
    CAST(NULL AS DECIMAL(8,4)) AS trend_variance_pct,

    -- Geographic-specific metrics for Zillow properties
    CAST(NULL AS INTEGER) AS total_properties_in_geography,
    CAST(NULL AS DECIMAL(10,2)) AS median_property_value,
    CAST(NULL AS DECIMAL(10,2)) AS avg_property_value,
    CAST(NULL AS DECIMAL(10,2)) AS property_value_std_dev,

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