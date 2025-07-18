-- models/raw/raw_landwatch.sql

select
    id,
    url,
    created_at,
    name,
    property_type,
    date_posted,
    county,
    lot_size,
    lot_size_units,
    lot_type,
    amenities,
    mortgage_options,
    activities,
    lot_description,
    geography,
    road_frontage_desc,
    state,
    city,
    zip,
    address1,
    address2,
    latitude,
    longitude,
    city_latitude,
    city_longitude,
    acres,
    beds,
    baths,
    homesqft,
    property_types,
    is_irrigated,
    is_residence,
    price,
    listing_date,
    title,
    description,
    executive_summary,
    is_diamond,
    is_gold,
    is_platinum,
    is_showcase,
    cost_per_acre,
    distance_to_city_miles,
    cost_per_homesqft
from {{ source('raw_data', 'landwatch_properties_raw') }}  limit 1000