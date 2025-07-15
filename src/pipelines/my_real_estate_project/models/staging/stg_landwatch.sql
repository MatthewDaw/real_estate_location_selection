-- models/staging/stg_landwatch.sql

with raw_landwatch as (
    select * from {{ ref('raw_landwatch') }}
),

filtered_and_deduplicated as (
    select
        *,
        row_number() over (partition by url order by created_at desc) as row_num
    from raw_landwatch
    where
        price is not null
        and price between 10000 and 800000
        and latitude is not null
        and longitude is not null
        and zip is not null
        and lot_size is not null
        and city is not null
        and address1 is not null
        and city_latitude is not null
        and city_longitude is not null
),

filtered_latest as (
    select * from filtered_and_deduplicated where row_num = 1
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
    from filtered_latest
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
        end as lot_area_value_sqft,
        st_point(longitude, latitude)::geography as geog_point
    from address_cleaned
),

missing_numeric_fixed as (
    select
        *,
        coalesce(beds, 0) as beds_fixed,
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

excluded_types as (
    select *
    from missing_numeric_fixed
    where property_type_clean not in ('farms-and-ranches', 'timberland-property', 'hunting-property')
),

buildings as (
    select geog_point from {{ ref('stg_buildings') }}
),

within_5_miles as (
    select e.*
    from excluded_types e
    join buildings b
      on st_dwithin(e.geog_point, b.geog_point, 8046.72)  -- 5 miles in meters
)

select
    id::text,
    url,
    created_at,
    coalesce(name, title) as name,
    title,
    address1_clean as address1,
    address2_clean as address2,
    city_clean as city,
    state_clean as state,
    zip_clean as zip,
    price,
    beds_fixed as beds,
    baths_fixed as baths,
    homesqft_fixed as homesqft,
    property_type_clean as property_type,
    lot_area_value_sqft,
    lot_size,
    lot_size_units,
    coalesce(date_posted_parsed,
             case when date_posted ~ '^\d{4}-\d{2}-\d{2}' then cast(date_posted as date) else null end
    ) as date_posted,
    coalesce(nullif(cost_per_acre_calc, 0), cost_per_acre) as cost_per_acre,
    coalesce(nullif(cost_per_homesqft_calc, 0), cost_per_homesqft) as cost_per_homesqft,
    latitude,
    longitude,
    city_latitude,
    city_longitude,
    geog_point,
    distance_to_city_miles
from within_5_miles