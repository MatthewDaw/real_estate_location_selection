-- 06_property_summaries/zillow_building_summary.sql

with zillow_with_predictions as (
    select
        z.id,
        z.lat,
        z.lon,
        z.geog_point,
        z.price,
        z.living_area_sqft,
        z.url,
        z.bedrooms,
        z.msa,
        z.zip_code,
        z.city,
        z.state,
        za.predicted_current_price,
        za.predicted_current_price_lower,
        za.predicted_current_price_upper,
        za.predicted_future_price,
        za.predicted_future_price_lower,
        za.predicted_future_price_upper,
        za.average_percent_gain_per_year,
        za.trend_strength_pct,
        za.trend_variance_pct,
        za.data_span_days
    from {{ ref('stg_zillow') }} z
    left join {{ ref('zillow_analysis') }} za on z.id = za.id
)

select
    id,
    lat,
    lon,
    msa,
    geog_point,
    zip_code,
    city,
    state,
    url,

    -- Current Price Predictions per bedroom
    case
        when bedrooms > 0 then predicted_current_price / bedrooms
        else null
    end as predicted_current_price_per_bedroom,
    case
        when bedrooms > 0 then predicted_current_price_lower / bedrooms
        else null
    end as predicted_current_price_lower_per_bedroom,
    case
        when bedrooms > 0 then predicted_current_price_upper / bedrooms
        else null
    end as predicted_current_price_upper_per_bedroom,

    -- Current Price Predictions per sqft
    case
        when living_area_sqft > 0 then predicted_current_price / living_area_sqft
        else null
    end as predicted_current_price_per_sqft,
    case
        when living_area_sqft > 0 then predicted_current_price_lower / living_area_sqft
        else null
    end as predicted_current_price_lower_per_sqft,
    case
        when living_area_sqft > 0 then predicted_current_price_upper / living_area_sqft
        else null
    end as predicted_current_price_upper_per_sqft,

    -- Future Price Predictions per bedroom
    case
        when bedrooms > 0 then predicted_future_price / bedrooms
        else null
    end as predicted_future_price_per_bedroom,
    case
        when bedrooms > 0 then predicted_future_price_lower / bedrooms
        else null
    end as predicted_future_price_lower_per_bedroom,
    case
        when bedrooms > 0 then predicted_future_price_upper / bedrooms
        else null
    end as predicted_future_price_upper_per_bedroom,

    -- Future Price Predictions per sqft
    case
        when living_area_sqft > 0 then predicted_future_price / living_area_sqft
        else null
    end as predicted_future_price_per_sqft,
    case
        when living_area_sqft > 0 then predicted_future_price_lower / living_area_sqft
        else null
    end as predicted_future_price_lower_per_sqft,
    case
        when living_area_sqft > 0 then predicted_future_price_upper / living_area_sqft
        else null
    end as predicted_future_price_upper_per_sqft,

    -- Actual price metrics per bedroom
    case
        when bedrooms > 0 then price / bedrooms
        else null
    end as price_per_bedroom,

    -- Actual price metrics per sqft
    case
        when living_area_sqft > 0 then price / living_area_sqft
        else null
    end as price_per_sqft,

    -- Raw prediction values (for reference)
    predicted_current_price,
    predicted_current_price_lower,
    predicted_current_price_upper,
    predicted_future_price,
    predicted_future_price_lower,
    predicted_future_price_upper,

    -- Growth metrics
    average_percent_gain_per_year,

    -- Quality and trend metrics
    trend_strength_pct,
    trend_variance_pct,

    -- Data quality
    data_span_days,

    -- Property characteristics for context
    price as current_price,
    bedrooms,
    living_area_sqft

from zillow_with_predictions
