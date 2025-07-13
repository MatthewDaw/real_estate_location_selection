-- models/staging/stg_buildings.sql

with raw_buildings as (
    select * from {{ ref('raw_buildings') }}
),

filtered_buildings as (
    select
        id::text,
        street_address::text,
        street_address_alias,
        city::text,
        state::text,
        zip_code::text,
        lat::float,
        lon::float,
        msa::text,
        building_name::text,
        building_website::text,
        is_single_family::boolean,
        is_condo::boolean,
        is_apartment::boolean,
        number_units::integer,
        year_built::integer,
        number_stories::integer,
        management_company::text,
        created_on::date,
        verified_listing::boolean,
        avg_quality::float,
        estimated_occupancy::float,
        is_student::boolean,
        is_senior::boolean,
        latest_unit_source::text,
        is_affordable::boolean,
        building_amenities,
        unit_amenities,
        leased_percentage::float,
        exposure_percentage::float,

        -- Add row number for deduplication
        row_number() over (
            partition by id
            order by created_on desc
        ) as row_num

    from raw_buildings
    where
        street_address is not null
        and lat is not null
        and lon is not null
)

select
    id,
    street_address,
    street_address_alias,
    city,
    state,
    zip_code,
    lat,
    lon,
    st_point(lon, lat)::geography as geog_point,
    msa,
    building_name,
    building_website,
    is_single_family,
    is_condo,
    is_apartment,
    number_units,
    year_built,
    number_stories,
    management_company,
    created_on,
    verified_listing,
    avg_quality,
    estimated_occupancy,
    is_student,
    is_senior,
    latest_unit_source,
    is_affordable,
    building_amenities,
    unit_amenities,
    leased_percentage,
    exposure_percentage
from filtered_buildings
where row_num = 1