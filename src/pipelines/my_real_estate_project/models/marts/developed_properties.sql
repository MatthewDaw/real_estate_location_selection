-- models/marts/developed_properties.sql

with zillow_developed as (
    select
        -- Identifiers
        'zillow' as source_platform,
        id as source_id,
        url as source_url,
        created_at,

        -- Property basics
        coalesce(marketing_name, street_address) as property_name,
        street_address,
        city,
        state,
        zipcode as zip,
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

        -- Derived metrics
        case
            when lot_area_value_sqft > 0 then price / lot_area_value_sqft * 43560.0
            else 0
        end as price_per_acre,
        case
            when living_area_sqft > 0 then price / living_area_sqft
            else 0
        end as price_per_sqft,

        -- Boolean flags
        is_income_restricted,
        is_preforeclosure_auction as is_auction,
        false as has_zestimate,

        -- JSON fields for rich data
        address,
        selling_soon,
        price_history,
        school_distances,
        risks,
        description,
        foreclosure,

        -- School metrics
        num_schools_close_to,
        avg_school_distance

    from {{ ref('stg_zillow') }}
    where
        home_type in (
            'condo',
            'townhouse',
            'apartment',
            'manufactured',
            'multi_family',
            'single_family',
            'home_type_unknown'
        )
        or bedrooms > 0
),

landwatch_developed as (
    select
        -- Identifiers
        'landwatch' as source_platform,
        id as source_id,
        url as source_url,
        created_at,

        -- Property basics
        coalesce(name, title) as property_name,
        address1 as street_address,
        city,
        state,
        zip,
        latitude as lat,
        longitude as lon,
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

        -- Derived metrics
        cost_per_acre as price_per_acre,
        cost_per_homesqft as price_per_sqft,

        -- Boolean flags
        false as is_income_restricted,
        false as is_auction,
        false as has_zestimate,

        -- JSON fields for rich data
        json_build_object(
            'address1', address1,
            'address2', address2,
            'city_coords', json_build_object('lat', city_latitude, 'lon', city_longitude)
        )::jsonb as address,
        null::jsonb as selling_soon,
        null::jsonb as price_history,
        null::jsonb as school_distances,
        null::jsonb as risks,
        null as description,
        null::jsonb as foreclosure,

        -- School metrics
        null::int as num_schools_close_to,
        distance_to_city_miles as avg_school_distance

    from {{ ref('stg_landwatch') }}
    where property_type in (
        'recreational-property',
        'commercial-property',
        'homes',
        'horse-property'
    )
    and (beds > 0 or baths > 0 or homesqft > 0)
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

unified_developed_properties as (
    select * from zillow_developed
    union all
    select * from landwatch_deduplicated
)

select
    row_number() over (order by source_platform, source_id) as unified_id,
    *
from unified_developed_properties