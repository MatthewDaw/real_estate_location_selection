-- models/combined/property_locations.sql

with for_sale_properties as (
    select
        property_name,
        'for_sale_' || source_platform as source,
        street_address,
        city,
        state,
        zip,
        lat,
        lon,
        geog_point
    from {{ ref('for_sale') }}
),

buildings as (
    select
        coalesce(building_name, street_address) as property_name,
        'buildings' as source,
        street_address,
        city,
        state,
        zip_code as zip,
        lat,
        lon,
        geog_point
    from {{ ref('stg_buildings') }}
),

unified_locations as (
    select * from for_sale_properties
    union all
    select * from buildings
)

select * from unified_locations
