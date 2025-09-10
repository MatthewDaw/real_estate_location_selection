-- 09_expanded_main_tables/02_explanations/aggregate_explanations.sql
with base_properties as (
    select
        source,
        source_id,
        geog_point,
        source_url,
        closest_zillow_listings,
        closest_hd_buildings
    from developed_properties
    WHERE
        coalesce(avg_rent_price_per_sqft, avg_predicted_current_effective_price_per_sqft) is not null
    UNION ALL
    select
        source,
        source_id,
        geog_point,
        source_url,
        closest_zillow_listings,
        closest_hd_buildings
    from empty_lots
    WHERE
        coalesce(avg_rent_price_per_sqft, avg_predicted_current_effective_price_per_sqft) is not null
),

-- Unnest HD buildings, calculate distances, and aggregate in one step
hd_buildings_aggregated as (
    select
        bp.source_id,
        array_agg(
            json_build_object(
                'building_id', building_id_unnested,
                'distance_miles', round((ST_Distance(bp.geog_point, hbs.geog_point) / 1609.34)::numeric, 2),
                'listing_urls', hbs.listing_urls,
                'leased_percent', hbs.leased_percentage,
                'average_percent_gain_per_year_effective', hbs.average_percent_gain_per_year_effective,
                'avg_effective_rent_price_per_bed', hbs.avg_effective_rent_price_per_bed,
                'avg_effective_rent_price_per_sqft', hbs.avg_effective_rent_price_per_sqft,
                'predicted_current_effective_price_per_bed', hbs.predicted_current_effective_price_per_bed,
                'predicted_future_effective_price_per_bed', hbs.predicted_future_effective_price_per_bed,
                'predicted_current_effective_price_per_sqft', hbs.predicted_current_effective_price_per_sqft,
                'predicted_future_effective_price_per_sqft', hbs.predicted_future_effective_price_per_sqft
            ) order by ST_Distance(bp.geog_point, hbs.geog_point)
        ) as closest_hd_buildings_detailed
    from base_properties bp
    cross join lateral unnest(
        -- Handle different array formats: remove brackets and split on comma
        string_to_array(
            trim(both '{}' from replace(replace(bp.closest_hd_buildings::text, '[', ''), ']', '')),
            ','
        )
    ) as building_id_unnested
    left join hd_building_summary hbs
        on trim(both '"' from trim(building_id_unnested)) = hbs.building_id
    where bp.closest_hd_buildings is not null
        and bp.closest_hd_buildings::text != ''
        and bp.closest_hd_buildings::text != '{}'
        and bp.closest_hd_buildings::text != '[]'
        and trim(both '"' from trim(building_id_unnested)) != ''
        and hbs.building_id is not null  -- Only include successful joins
    group by bp.source_id
),

-- Unnest Zillow listings, calculate distances, and aggregate in one step
zillow_listings_aggregated as (
    select
        bp.source_id,
        array_agg(
            json_build_object(
                'zillow_id', zillow_id_unnested,
                'distance_miles', round((ST_Distance(bp.geog_point, zbs.geog_point) / 1609.34)::numeric, 2),
                'url', zbs.url,
                'current_price', zbs.current_price,
                'bedrooms', zbs.bedrooms,
                'living_area_sqft', zbs.living_area_sqft,
                'average_percent_gain_per_year', zbs.average_percent_gain_per_year,
                'price_per_bedroom', zbs.price_per_bedroom,
                'price_per_sqft', zbs.price_per_sqft,
                'predicted_current_price_per_bedroom', zbs.predicted_current_price_per_bedroom,
                'predicted_future_price_per_bedroom', zbs.predicted_future_price_per_bedroom,
                'predicted_current_price_per_sqft', zbs.predicted_current_price_per_sqft,
                'predicted_future_price_per_sqft', zbs.predicted_future_price_per_sqft
            ) order by ST_Distance(bp.geog_point, zbs.geog_point)
        ) as closest_zillow_listings_detailed
    from base_properties bp
    cross join lateral unnest(string_to_array(bp.closest_zillow_listings::text, ',')) as zillow_id_unnested
    left join zillow_building_summary zbs
        on trim(zillow_id_unnested) = zbs.id
    where bp.closest_zillow_listings is not null
        and bp.closest_zillow_listings::text != ''
        and bp.closest_zillow_listings::text != '{}'
        and zbs.id is not null  -- Only include successful joins
    group by bp.source_id
)
-- Final result
select
    bp.source_id,
    coalesce(hba.closest_hd_buildings_detailed, array[]::json[]) as closest_hd_buildings_detailed,
    coalesce(zla.closest_zillow_listings_detailed, array[]::json[]) as closest_zillow_listings_detailed
from base_properties bp
left join hd_buildings_aggregated hba
    on bp.source_id = hba.source_id
left join zillow_listings_aggregated zla
    on bp.source_id = zla.source_id
