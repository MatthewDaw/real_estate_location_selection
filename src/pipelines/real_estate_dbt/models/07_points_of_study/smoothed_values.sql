-- 06_property_summaries/smoothed_values.sql - Create empty table structure for Python population

{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['source'], 'type': 'btree'},
      {'columns': ['source_id'], 'type': 'btree'},
      {'columns': ['source', 'source_id'], 'type': 'btree', 'unique': True}
    ]
  )
}}

-- Create an empty table with the correct schema
-- This will be populated by the Python spatial smoothing script
SELECT
    CAST(NULL AS VARCHAR) AS source,
    CAST(NULL AS VARCHAR) AS source_id,

    -- Building-based smoothed metrics - Current Price per sqft
    CAST(NULL AS DECIMAL(10,2)) AS avg_predicted_current_price_per_sqft,
    CAST(NULL AS DECIMAL(10,2)) AS avg_predicted_current_price_lower_per_sqft,
    CAST(NULL AS DECIMAL(10,2)) AS avg_predicted_current_price_upper_per_sqft,
    CAST(NULL AS DECIMAL(10,2)) AS avg_rent_price_per_sqft,

    -- Building-based smoothed metrics - Current Effective Price per sqft
    CAST(NULL AS DECIMAL(10,2)) AS avg_predicted_current_effective_price_per_sqft,
    CAST(NULL AS DECIMAL(10,2)) AS avg_predicted_current_effective_price_lower_per_sqft,
    CAST(NULL AS DECIMAL(10,2)) AS avg_predicted_current_effective_price_upper_per_sqft,
    CAST(NULL AS DECIMAL(10,2)) AS avg_effective_rent_price_per_sqft,

    -- Building-based smoothed metrics - Future Price per sqft
    CAST(NULL AS DECIMAL(10,2)) AS avg_predicted_future_price_per_sqft,
    CAST(NULL AS DECIMAL(10,2)) AS avg_predicted_future_price_lower_per_sqft,
    CAST(NULL AS DECIMAL(10,2)) AS avg_predicted_future_price_upper_per_sqft,

    -- Building-based smoothed metrics - Future Effective Price per sqft
    CAST(NULL AS DECIMAL(10,2)) AS avg_predicted_future_effective_price_per_sqft,
    CAST(NULL AS DECIMAL(10,2)) AS avg_predicted_future_effective_price_lower_per_sqft,
    CAST(NULL AS DECIMAL(10,2)) AS avg_predicted_future_effective_price_upper_per_sqft,

    -- Building-based smoothed metrics - Current Price per bed
    CAST(NULL AS DECIMAL(10,2)) AS avg_predicted_current_price_per_bed,
    CAST(NULL AS DECIMAL(10,2)) AS avg_predicted_current_price_lower_per_bed,
    CAST(NULL AS DECIMAL(10,2)) AS avg_predicted_current_price_upper_per_bed,
    CAST(NULL AS DECIMAL(10,2)) AS avg_rent_price_per_bed,

    -- Building-based smoothed metrics - Current Effective Price per bed
    CAST(NULL AS DECIMAL(10,2)) AS avg_predicted_current_effective_price_per_bed,
    CAST(NULL AS DECIMAL(10,2)) AS avg_predicted_current_effective_price_lower_per_bed,
    CAST(NULL AS DECIMAL(10,2)) AS avg_predicted_current_effective_price_upper_per_bed,
    CAST(NULL AS DECIMAL(10,2)) AS avg_effective_rent_price_per_bed,

    -- Building-based smoothed metrics - Future Price per bed
    CAST(NULL AS DECIMAL(10,2)) AS avg_predicted_future_price_per_bed,
    CAST(NULL AS DECIMAL(10,2)) AS avg_predicted_future_price_lower_per_bed,
    CAST(NULL AS DECIMAL(10,2)) AS avg_predicted_future_price_upper_per_bed,

    -- Building-based smoothed metrics - Future Effective Price per bed
    CAST(NULL AS DECIMAL(10,2)) AS avg_predicted_future_effective_price_per_bed,
    CAST(NULL AS DECIMAL(10,2)) AS avg_predicted_future_effective_price_lower_per_bed,
    CAST(NULL AS DECIMAL(10,2)) AS avg_predicted_future_effective_price_upper_per_bed,

    -- Building-based smoothed metrics - Unit averages
    CAST(NULL AS DECIMAL(8,2)) AS avg_beds,
    CAST(NULL AS DECIMAL(8,2)) AS avg_baths,
    CAST(NULL AS DECIMAL(10,2)) AS avg_sqft,
    CAST(NULL AS DECIMAL(15,2)) AS avg_price,
    CAST(NULL AS DECIMAL(15,2)) AS avg_effective_price,
    CAST(NULL AS INTEGER) AS num_units,

    -- Context counts
    CAST(NULL AS INTEGER) AS hd_buildings_within_radius,

    -- Building analysis smoothed metrics
    CAST(NULL AS DECIMAL(8,4)) AS avg_building_average_percent_gain_per_year,
    CAST(NULL AS DECIMAL(8,4)) AS avg_building_average_percent_gain_per_year_effective,

    -- Building occupancy and market metrics
    CAST(NULL AS DECIMAL(8,4)) AS avg_leased_percentage,
    CAST(NULL AS DECIMAL(8,4)) AS avg_exposure_percentage,
    CAST(NULL AS DECIMAL(8,4)) AS avg_concession_percentage,

    -- Zillow-based smoothed metrics - Current Price Predictions per bedroom
    CAST(NULL AS DECIMAL(15,2)) AS avg_zillow_predicted_current_price_per_bedroom,
    CAST(NULL AS DECIMAL(15,2)) AS avg_zillow_predicted_current_price_lower_per_bedroom,
    CAST(NULL AS DECIMAL(15,2)) AS avg_zillow_predicted_current_price_upper_per_bedroom,

    -- Zillow-based smoothed metrics - Current Price Predictions per sqft
    CAST(NULL AS DECIMAL(10,2)) AS avg_zillow_predicted_current_price_per_sqft,
    CAST(NULL AS DECIMAL(10,2)) AS avg_zillow_predicted_current_price_lower_per_sqft,
    CAST(NULL AS DECIMAL(10,2)) AS avg_zillow_predicted_current_price_upper_per_sqft,

    -- Zillow-based smoothed metrics - Future Price Predictions per bedroom
    CAST(NULL AS DECIMAL(15,2)) AS avg_zillow_predicted_future_price_per_bedroom,
    CAST(NULL AS DECIMAL(15,2)) AS avg_zillow_predicted_future_price_lower_per_bedroom,
    CAST(NULL AS DECIMAL(15,2)) AS avg_zillow_predicted_future_price_upper_per_bedroom,

    -- Zillow-based smoothed metrics - Future Price Predictions per sqft
    CAST(NULL AS DECIMAL(10,2)) AS avg_zillow_predicted_future_price_per_sqft,
    CAST(NULL AS DECIMAL(10,2)) AS avg_zillow_predicted_future_price_lower_per_sqft,
    CAST(NULL AS DECIMAL(10,2)) AS avg_zillow_predicted_future_price_upper_per_sqft,

    -- Zillow-based smoothed metrics - Actual price metrics
    CAST(NULL AS DECIMAL(15,2)) AS avg_zillow_price_per_bedroom,
    CAST(NULL AS DECIMAL(10,2)) AS avg_zillow_price_per_sqft,

    -- Zillow-based smoothed metrics - Property characteristics
    CAST(NULL AS DECIMAL(15,2)) AS avg_zillow_current_price,
    CAST(NULL AS DECIMAL(8,2)) AS avg_zillow_bedrooms,
    CAST(NULL AS DECIMAL(10,2)) AS avg_zillow_living_area_sqft,

    -- Context counts
    CAST(NULL AS INTEGER) AS zillow_buildings_within_radius,

    -- Zillow-based smoothed metrics - Growth metrics
    CAST(NULL AS DECIMAL(8,4)) AS avg_zillow_average_percent_gain_per_year,

    -- Zillow-based smoothed metrics - Quality and trend metrics
    CAST(NULL AS DECIMAL(8,4)) AS avg_zillow_trend_strength_pct,
    CAST(NULL AS DECIMAL(8,4)) AS avg_zillow_trend_variance_pct,

    -- Zillow-based smoothed metrics - Data quality
    CAST(NULL AS INTEGER) AS avg_zillow_data_span_days,

    -- Empty lot smoothed metrics
    CAST(NULL AS DECIMAL(10,2)) AS avg_empty_lot_cost_per_sqft,

    -- Context counts
    CAST(NULL AS INTEGER) AS empty_lots_within_radius,

    -- Developed properties smoothed metrics
    CAST(NULL AS DECIMAL(10,2)) AS avg_interior_sqft_cost,
    CAST(NULL AS DECIMAL(10,2)) AS avg_developed_lot_cost_per_sqft,

    -- Context counts
    CAST(NULL AS INTEGER) AS developed_properties_within_radius,

    -- Closest buildings and listings (JSON arrays)
    CAST(NULL AS TEXT) AS closest_hd_buildings,
    CAST(NULL AS TEXT) AS closest_zillow_listings,

    -- System timestamp
    CAST(NULL AS TIMESTAMP) AS created_at

-- This WHERE clause ensures no rows are actually inserted
-- We just want the table structure
WHERE 1 = 0