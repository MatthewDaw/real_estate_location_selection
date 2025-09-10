-- models/03_staging/stg_landwatch.sql

with raw_landwatch as (
    select * from {{ ref('raw_landwatch') }}
),

property_type_normalized as (
    select
        *,
        lower(trim(property_type)) as property_type_lower,
        case
            when lower(trim(property_type)) in ('undeveloped land', 'vacant land', 'vacant') then 'undeveloped-land'
            when lower(trim(property_type)) in ('recreational', 'recreational property') then 'recreational-property'
            when lower(trim(property_type)) in ('commercial', 'commercial property') then 'commercial-property'
            when lower(trim(property_type)) in ('home', 'homes', 'residential') then 'homes'
            when lower(trim(property_type)) in ('horse property', 'horse-property') then 'horse-property'
            when lower(trim(property_type)) in ('hunting property', 'hunting-property') then 'hunting-property'
            else lower(trim(property_type))
        end as property_type_clean
    from raw_landwatch
),

date_standardized as (
    select
        *,
        case
          when listing_date ~ '^\d{4}-\d{2}-\d{2}' then cast(listing_date as date)
          else null
        end as date_posted_parsed
    from property_type_normalized
),

address_cleaned as (
    select
        *,
        initcap(trim(address1)) as address1_clean,
        initcap(trim(address2)) as address2_clean,
        initcap(trim(city)) as city_clean,
        upper(trim(state)) as state_clean,
        trim(zip) as zip_clean,
        concat_ws(' ', initcap(trim(address1)), initcap(trim(address2))) as full_address
    from date_standardized
),

coords_and_lot_size as (
    select
        *,
        case
            when lot_size_units ilike '%acre%' then lot_size * 43560
            when lot_size_units ilike '%sq%' then lot_size
            else null
        end as lot_area_value_sqft
    from address_cleaned
),

missing_numeric_fixed as (
    select
        *,
        case
            when (coalesce(beds, 0) = 0) and (coalesce(homesqft, 0) > 0) then 1
            else coalesce(beds, 0)
        end as beds_fixed,
        coalesce(baths, 0) as baths_fixed,
        coalesce(homesqft, 0) as homesqft_fixed,
        -- Add price per acre and price per sqft calculations
        case
            when lot_area_value_sqft > 0 then price / lot_area_value_sqft * 43560.0
            else 0
        end as cost_per_acre_calc,
        case
            when coalesce(homesqft, 0) > 0 then price / coalesce(homesqft, 0)
            else 0
        end as cost_per_homesqft_calc
    from coords_and_lot_size
),

filtered_impossible_setup as (
      select * from missing_numeric_fixed where
    -- living area with no beds or baths
    not
        (
        homesqft_fixed > 0
        and beds_fixed = 0
        and baths_fixed = 0
        )
    -- no living area with beds or baths
    and
    not
        (
        homesqft_fixed = 0
        and beds_fixed != 0
        and baths_fixed != 0
        )
),

excluded_types as (
    select *
    from filtered_impossible_setup
    where property_type_clean in ('undeveloped-land', 'homes', 'recreational-property')
),

-- Join with pre-computed closest buildings
closest_building_added as (
    select
        e.*,
        cb.closest_building_id,
        cb.distance_to_closest_building,
        cb.msa
    from excluded_types e
    left join {{ ref('closest_buildings_lookup') }} cb
        on e.id::text = cb.property_id
        and cb.source = 'landwatch'
),

within_4_miles as (
    select e.*
    from closest_building_added e
    where distance_to_closest_building <= 2414.01 -- 1.5 miles in meters
),

filtered_smaller_properties as (
      select * from within_4_miles where lot_area_value_sqft <  43560 * 1.5
    )

select
    id::text,
    url::text,
    created_at::timestamp,
    coalesce(name, title)::text as name,
    title::text,
    address1_clean::text as address1,
    address2_clean::text as address2,
    city_clean::text as city,
    state_clean::text as state,
    zip_clean::text as zip,
    price::numeric,
    beds_fixed::integer as beds,
    baths_fixed::integer as baths,
    homesqft_fixed::integer as homesqft,
    property_type_clean::text as property_type,
    lot_area_value_sqft::numeric,
    lot_size::numeric,
    lot_size_units::text,
    coalesce(date_posted_parsed,
             case when date_posted ~ '^\d{4}-\d{2}-\d{2}' then cast(date_posted as date) else null end
    )::date as date_posted,
    coalesce(nullif(cost_per_acre_calc, 0), cost_per_acre)::numeric as cost_per_acre,
    coalesce(nullif(cost_per_homesqft_calc, 0), cost_per_homesqft)::numeric as cost_per_homesqft,
    latitude::float as lat,
    longitude::float as lon,
    city_latitude::float,
    city_longitude::float,
    geog_point,
    msa,
    distance_to_city_miles::float,
    closest_building_id::text
from filtered_smaller_properties
