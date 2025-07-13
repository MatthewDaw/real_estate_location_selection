-- models/marts/for_sale.sql

with zillow_standardized as (
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

        -- Property details
        price,
        last_sold_price,
        zestimate as estimated_value,
        bedrooms as beds,
        bathrooms as baths,
        living_area as interior_sqft,
        year_built,
        home_type as property_type,

        -- Lot information
        lot_area_value_sqft,
        lot_area_value_sqft / 43560.0 as acres,
        lot_size,

        -- Listing details
        status,
        date_posted_string as date_posted,
        days_on_zillow as days_on_market,
        page_view_count,
        favorite_count,

        -- Derived metrics
        case
            when lot_area_value_sqft > 0 then price / lot_area_value_sqft * 43560.0
            else null
        end as price_per_acre,
        case
            when living_area > 0 then price / living_area
            else null
        end as price_per_sqft,

        -- Boolean flags
        is_income_restricted,
        is_preforeclosure_auction as is_auction,
        case when zestimate is not null then true else false end as has_zestimate,

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
),

landwatch_standardized as (
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
        lat,
        lon,
        geog_point,

        -- Property details
        price,
        null as last_sold_price,
        null as estimated_value,
        beds,
        baths,
        homesqft as interior_sqft,
        null as year_built,
        property_type,

        -- Lot information
        case
            when lot_size_units ilike '%acre%' then lot_size * 43560
            when lot_size_units ilike '%sq%' then lot_size
            else lot_size
        end as lot_area_value_sqft,
        acres,
        lot_size,

        -- Listing details
        'FOR_SALE' as status,  -- Landwatch is all for sale
        date_posted,
        null as days_on_market,
        null as page_view_count,
        null as favorite_count,

        -- Derived metrics
        cost_per_acre as price_per_acre,
        cost_per_homesqft as price_per_sqft,

        -- Boolean flags
        null as is_income_restricted,
        false as is_auction,  -- Landwatch doesn't have auctions
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
        json_build_object(
            'lot_type', lot_type,
            'amenities', amenities,
            'activities', activities,
            'geography', geography,
            'mortgage_options', mortgage_options
        )::jsonb as description,
        null::jsonb as foreclosure,

        -- School metrics
        null as num_schools_close_to,
        distance_to_city_miles as avg_school_distance  -- Repurposing as distance metric

    from {{ ref('stg_landwatch') }}
),

-- Find Landwatch properties that match Zillow properties
landwatch_duplicates as (
    select distinct l.source_id as landwatch_id
    from landwatch_standardized l
    inner join zillow_standardized z on (
        -- Distance match: within 150 feet (45.72 meters)
        st_dwithin(l.geog_point, z.geog_point, 45.72)
        or
        -- Address match: case insensitive comparison
        (
            l.street_address ilike z.street_address
            and l.city ilike z.city
            and l.state ilike z.state
        )
    )
),

-- Remove duplicate Landwatch properties
landwatch_deduplicated as (
    select l.*
    from landwatch_standardized l
    left join landwatch_duplicates ld on l.source_id = ld.landwatch_id
    where ld.landwatch_id is null
),

unified_properties as (
    select * from zillow_standardized
    union all
    select * from landwatch_deduplicated
)

select
    -- Add unified ID and ranking
    row_number() over (order by source_platform, source_id) as unified_id,
    *,

    -- Add market segments for analysis
    case
        when price < 100000 then 'budget'
        when price < 300000 then 'mid_market'
        when price < 700000 then 'upper_mid'
        else 'luxury'
    end as price_segment,

    case
        when acres >= 5 then 'large_lot'
        when acres >= 1 then 'acreage'
        when acres >= 0.25 then 'large_residential'
        else 'standard_residential'
    end as lot_size_category,

    -- Add data quality scores
    (
        case when street_address is not null then 1 else 0 end +
        case when beds is not null then 1 else 0 end +
        case when baths is not null then 1 else 0 end +
        case when interior_sqft is not null then 1 else 0 end +
        case when year_built is not null then 1 else 0 end +
        case when lot_area_value_sqft is not null then 1 else 0 end
    ) as data_completeness_score

from unified_properties