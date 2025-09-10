-- models/09_expanded_main_tables/02_explanations/exploded_hd_explanations.sql

with base_data as (
    select
        source_id,
        -- Limit to first 4 buildings using array slice
        case
            when array_length(closest_hd_buildings_detailed, 1) > 4
            then closest_hd_buildings_detailed[1:10]
            else closest_hd_buildings_detailed
        end as closest_hd_buildings_detailed
    from {{ ref('aggregate_explanations') }}
    where closest_hd_buildings_detailed is not null
        and array_length(closest_hd_buildings_detailed, 1) > 0
)

-- Explode the limited closest_hd_buildings_detailed array and extract all columns
select
    source_id,
    -- Extract all building detail fields into separate columns
    (building_detail->>'distance_miles')::numeric as distance_miles,
    building_detail->>'listing_urls' as listing_urls,
    building_detail->>'leased_percent' as leased_percent,
    (building_detail->>'average_percent_gain_per_year_effective')::numeric as average_percent_gain_per_year_effective,
    (building_detail->>'avg_effective_rent_price_per_bed')::numeric as avg_effective_rent_price_per_bed,
    (building_detail->>'avg_effective_rent_price_per_sqft')::numeric as avg_effective_rent_price_per_sqft,
    (building_detail->>'predicted_current_effective_price_per_bed')::numeric as predicted_current_effective_price_per_bed,
    (building_detail->>'predicted_future_effective_price_per_bed')::numeric as predicted_future_effective_price_per_bed,
    (building_detail->>'predicted_current_effective_price_per_sqft')::numeric as predicted_current_effective_price_per_sqft,
    (building_detail->>'predicted_future_effective_price_per_sqft')::numeric as predicted_future_effective_price_per_sqft
from base_data
cross join lateral unnest(closest_hd_buildings_detailed) as building_detail
order by source_id, (building_detail->>'distance_miles')::numeric nulls last

