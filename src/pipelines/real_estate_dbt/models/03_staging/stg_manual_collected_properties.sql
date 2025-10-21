-- models/03_staging/stg_manual_collected_properties.sql

with raw_manual as (
    select * from {{ ref('raw_manual_collected_properties') }}
),

address_cleaned as (
    select
        *,
        initcap(trim(address)) as address_clean,
        -- Extract city, state, zip from address if needed
        -- For now, keeping original address structure
        address as full_address
    from raw_manual
),

numeric_cleaned as (
    select
        *,
        coalesce(bed, 0) as bedrooms,
        coalesce(bath, 0) as bathrooms,
        coalesce(sqft, 0) as living_area_sqft,
        coalesce(cost, 0) as price
    from address_cleaned
),

-- Join with pre-computed closest buildings
closest_building_added as (
    select
        m.*,
        cb.closest_building_id,
        cb.msa,
        cb.zip_code as closest_building_zip_code,
        cb.distance_to_closest_building
    from numeric_cleaned m
    left join {{ ref('closest_buildings_lookup') }} cb
        on m.id::text = cb.property_id
        and cb.source = 'manual'
),

-- Filter properties within reasonable distance (similar to zillow model)
within_reasonable_distance as (
    select m.*
    from closest_building_added m
    where distance_to_closest_building <= 6437.38 -- 4 miles in meters (more lenient than zillow)
)

select
    id::text,
    source_url::text as url,
    created_at::date,
    address_clean as street_address,
    address as full_address,
    bedrooms::integer,
    bathrooms::float,
    living_area_sqft::integer,
    price::numeric,
    lat::float,
    lon::float,
    geog_point,
    closest_building_id::text,
    msa::text,
    closest_building_zip_code::text as zip_code,
    distance_to_closest_building::numeric,
    'manual'::text as source
from within_reasonable_distance
