-- 06_property_summaries/developed_properties_summary.sql

with zillow_developed as (
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
        zip_code as zip,
        lat,
        lon,
        geog_point,
        price,
        bedrooms as beds,
        bathrooms as baths,
        living_area_sqft as interior_sqft,
        year_built,
        home_type as property_type,

        -- Lot information
        lot_area_value_sqft,

        -- Listing details
        status,
        date_posted,
        days_on_zillow as days_on_market,
        page_view_count,
        favorite_count,

        -- Boolean flags
        is_income_restricted,
        is_preforeclosure_auction as is_auction,
        false as has_zestimate,

        -- JSON fields for rich data - FIXED TYPE CONVERSIONS
        address::jsonb as address,
        selling_soon::jsonb as selling_soon,
        price_history,
        school_distances,
        risks,
        null::jsonb as description,
        foreclosure,

        -- School metrics
        num_schools_close_to,
        avg_school_distance,

        -- Building relationship
        closest_building_id,
        msa

    from {{ ref('stg_zillow') }}
    where
        living_area_sqft > 0
),

landwatch_developed as (
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
        beds,
        baths,
        homesqft as interior_sqft,
        null::int as year_built,
        property_type,

        -- Lot information
        lot_area_value_sqft,

        -- Listing details
        'FOR_SALE' as status,
        date_posted,
        null::int as days_on_market,
        null::int as page_view_count,
        null::int as favorite_count,

        -- Boolean flags
        false as is_income_restricted,
        false as is_auction,
        false as has_zestimate,

        -- JSON fields for rich data - CONSISTENT JSONB TYPES
        json_build_object(
            'address1', address1,
            'address2', address2,
            'city_coords', json_build_object('lat', city_latitude, 'lon', city_longitude)
        )::jsonb as address,
        null::jsonb as selling_soon,
        null::jsonb as price_history,
        null::jsonb as school_distances,
        null::jsonb as risks,
        null::jsonb as description,
        null::jsonb as foreclosure,

        -- School metrics
        null::int as num_schools_close_to,
        distance_to_city_miles as avg_school_distance,

        -- Building relationship
        closest_building_id,
        msa

    from {{ ref('stg_landwatch') }}
    where homesqft > 0
),

landwatch_duplicates as (
    select distinct l.source_id as landwatch_id
    from landwatch_developed l
    inner join zillow_developed z on (
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
    from landwatch_developed l
    left join landwatch_duplicates ld on l.source_id = ld.landwatch_id
    where ld.landwatch_id is null
),

manual_developed as (
    select
        -- Identifiers
        'manual' as source,
        id as source_id,
        url as source_url,
        created_at,

        -- Property basics
        street_address as property_name,
        street_address,
        null::text as city,
        null::text as state,
        zip_code as zip,
        lat,
        lon,
        geog_point,
        price,
        bedrooms as beds,
        bathrooms as baths,
        living_area_sqft as interior_sqft,
        null::int as year_built,
        'manual_property'::text as property_type,

        -- Lot information
        null::numeric as lot_area_value_sqft,

        -- Listing details
        'FOR_SALE'::text as status,
        null::date as date_posted,
        null::int as days_on_market,
        null::int as page_view_count,
        null::int as favorite_count,

        -- Boolean flags
        false as is_income_restricted,
        false as is_auction,
        false as has_zestimate,

        -- JSON fields for rich data - CONSISTENT JSONB TYPES
        json_build_object(
            'address', street_address,
            'zip_code', zip_code
        )::jsonb as address,
        null::jsonb as selling_soon,
        null::jsonb as price_history,
        null::jsonb as school_distances,
        null::jsonb as risks,
        null::jsonb as description,
        null::jsonb as foreclosure,

        -- School metrics
        null::int as num_schools_close_to,
        null::float as avg_school_distance,

        -- Building relationship
        closest_building_id,
        msa

    from {{ ref('stg_manual_collected_properties') }}
    where living_area_sqft > 0
),

unified_developed_properties as (
    select * from zillow_developed
    union all
    select * from landwatch_deduplicated
    union all
    select * from manual_developed
)

select *, 
price / interior_sqft as price_per_sqft,
price / lot_area_value_sqft as price_per_lot_sqft
from unified_developed_properties where interior_sqft > 0
