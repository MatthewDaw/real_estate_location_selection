-- 06_property_summaries/empty_lot_summaries

with zillow_empty_lots as (
    select
        -- Identifiers
        'zillow' as source,
        id as source_id,
        url as source_url,
        created_at,

        -- Property basics
        coalesce(marketing_name, street_address) as property_name,
        street_address,
        city,
        state,
        zip_code as zip_code,
        lat,
        lon,
        geog_point,
        price,

        -- Lot information
        lot_area_value_sqft,
        lot_size,

        -- Listing details
        status,
        date_posted,
        days_on_zillow as days_on_market,

        -- Optional metadata
        description,
        risks,
        avg_school_distance,

        -- Building relationship
        closest_building_id,
        msa

    from {{ ref('stg_zillow') }}
    where living_area_sqft = 0
),
landwatch_empty_lots as (
    select
        -- Identifiers
        'landwatch' as source,
        id as source_id,
        url as source_url,
        created_at,

        -- Property basics
        coalesce(name, title) as property_name,
        address1 as street_address,
        city,
        state,
        zip,
        lat,
        lon,
        geog_point,
        price,

        -- Lot information
        lot_area_value_sqft,
        lot_size,

        -- Listing details
        'FOR_SALE' as status,
        date_posted,
        null::int as days_on_market,

        -- Optional metadata
        null as description,
        null::jsonb as risks,
        distance_to_city_miles as avg_school_distance,

        -- Building relationship
        closest_building_id,
        msa

    from {{ ref('stg_landwatch') }}
    where homesqft = 0
),

landwatch_duplicates as (
    select distinct l.source_id as landwatch_id
    from landwatch_empty_lots l
    inner join zillow_empty_lots z on (
        st_dwithin(l.geog_point, z.geog_point, 45.72)
        or (
            l.street_address ilike z.street_address
            and l.city ilike z.city
            and l.state ilike z.state
        )
    )
),
landwatch_deduplicated as (
    select l.*
    from landwatch_empty_lots l
    left join landwatch_duplicates ld on l.source_id = ld.landwatch_id
    where ld.landwatch_id is null
),

unified_empty_lots as (
    select * from zillow_empty_lots
    union all
    select * from landwatch_deduplicated
)

select * from unified_empty_lots
