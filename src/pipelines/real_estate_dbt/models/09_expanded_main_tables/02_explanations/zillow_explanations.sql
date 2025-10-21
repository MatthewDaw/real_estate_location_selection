-- models/09_expanded_main_tables/02_explanations/zillow_explanations.sql

with base_data as (
    select
        source_id,
        -- Limit to first 10 listings using array slice
        case
            when array_length(closest_zillow_listings_detailed, 1) > 4
            then closest_zillow_listings_detailed[1:10]
            else closest_zillow_listings_detailed
        end as closest_zillow_listings_detailed
    from {{ ref('aggregate_explanations') }}
    where closest_zillow_listings_detailed is not null
        and array_length(closest_zillow_listings_detailed, 1) > 0
)

-- Explode the limited closest_zillow_listings_detailed array and extract all columns
select
    source_id,
    -- Extract all listing detail fields into separate columns
    (listing_detail->>'distance_miles')::numeric as distance_miles,
    listing_detail->>'url' as url,
    (listing_detail->>'current_price')::numeric as current_price,
    (listing_detail->>'bedrooms')::numeric as bedrooms,
    (listing_detail->>'living_area_sqft')::numeric as living_area_sqft,
    (listing_detail->>'average_percent_gain_per_year')::numeric as average_percent_gain_per_year,
    (listing_detail->>'price_per_bedroom')::numeric as price_per_bedroom,
    (listing_detail->>'price_per_sqft')::numeric as price_per_sqft,
    (listing_detail->>'predicted_current_price_per_bedroom')::numeric as predicted_current_price_per_bedroom,
    (listing_detail->>'predicted_future_price_per_bedroom')::numeric as predicted_future_price_per_bedroom,
    (listing_detail->>'predicted_current_price_per_sqft')::numeric as predicted_current_price_per_sqft,
    (listing_detail->>'predicted_future_price_per_sqft')::numeric as predicted_future_price_per_sqft
from base_data
cross join lateral unnest(closest_zillow_listings_detailed) as listing_detail
order by source_id, (listing_detail->>'distance_miles')::numeric nulls last