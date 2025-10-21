-- models/01_raw/raw_landwatch.sql

with filtered_and_deduplicated as (
    select
        l.*,
        row_number() over (partition by l.url order by l.created_at desc) as row_num
    from {{ source('raw_data', 'landwatch_properties_raw') }} l
    inner join {{ source('raw_data', 'landwatch_urls') }} u
        on l.url = u.url
    where
        l.state = 'UT'
        and l.price is not null
        and l.price between 5000 and 700000
        and l.latitude is not null
        and l.longitude is not null
        and l.zip is not null
        and l.lot_size is not null
        and l.city is not null
        and l.address1 is not null
        and l.city_latitude is not null
        and l.city_longitude is not null
)

select
    'landwatch_' || row_number() over (order by url) as id,
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
    cost_per_homesqft,
    geog_point
from filtered_and_deduplicated
where row_num = 1