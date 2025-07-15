-- models/combined/empty_lots.sql

with landwatch_empty_lots as (
    select
        source_id,
        source_url,
        created_at,
        property_name,
        street_address,
        city,
        state,
        zip,
        lat,
        lon,
        geog_point,
        price,
        lot_area_value_sqft,
        price_per_acre,
        status,
        date_posted,
        -- Optional metadata
        description,
        risks,
        avg_school_distance
    from {{ ref('for_sale') }}
    where
        source_platform = 'landwatch'
        and property_type = 'undeveloped-land'
        -- You can also include other normalized property types here if needed:
        -- or property_type in ('land', 'homesites')
)

select * from landwatch_empty_lots
