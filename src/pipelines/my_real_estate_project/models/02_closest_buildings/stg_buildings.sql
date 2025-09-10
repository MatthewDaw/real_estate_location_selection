-- models/02_staging/stg_buildings.sql

with raw_buildings as (
    select * from {{ ref('raw_buildings') }}
)

select
    id::text,
    street_address::text,
    street_address_alias,
    city::text,
    state::text,
    zip_code::text,
    lat::float,
    lon::float,
    geog_point,
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
    listing_urls
from raw_buildings