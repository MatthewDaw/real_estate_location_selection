-- models/staging/stg_zillow.sql

with raw_zillow as (
    select * from {{ ref('raw_zillow') }}
),

filtered as (
    select
        *,
        row_number() over (partition by source_url order by created_at desc) as row_num
    from raw_zillow
    where
        price between 10000 and 800000
        and status not in ('SOLD', 'FOR_RENT')
        and (country is null or country != 'CAN')
        and latitude is not null and longitude is not null
        and latitude between -90 and 90
        and longitude between -180 and 180
        and price is not null
),

deduplicated as (
    select * from filtered where row_num = 1
),

home_type_normalized as (
    select
        *,
        lower(trim(home_type)) as home_type_lower,
        case
            when lower(trim(home_type)) in ('single family', 'single_family') then 'single_family'
            when lower(trim(home_type)) in ('condo', 'condominium') then 'condo'
            when lower(trim(home_type)) in ('townhouse', 'town_home') then 'townhouse'
            when lower(trim(home_type)) in ('apartment', 'apts') then 'apartment'
            when lower(trim(home_type)) in ('manufactured', 'mobile home') then 'manufactured'
            when lower(trim(home_type)) in ('multi_family', 'multi-family') then 'multi_family'
            when lower(trim(home_type)) in ('home_type_unknown', 'unknown') then 'home_type_unknown'
            else lower(trim(home_type))
        end as home_type_clean
    from deduplicated
),

dates_parsed as (
    select
        *,
        case
          when date_posted_string ~ '^\d{4}-\d{2}-\d{2}' then cast(date_posted_string as date)
          else null
        end as date_posted,
        case
          when most_recent_price_date ~ '^\d{4}-\d{2}-\d{2}' then cast(most_recent_price_date as date)
          else null
        end as most_recent_price_date_parsed
    from home_type_normalized
),

address_cleaned as (
    select
        *,
        initcap(trim(street_address)) as street_address_clean,
        initcap(trim(city)) as city_clean,
        upper(trim(state)) as state_clean,
        trim(zipcode) as zip_clean,
        concat_ws(', ', initcap(trim(street_address)), initcap(trim(city)), upper(trim(state)), trim(zipcode)) as full_address
    from dates_parsed
),

lot_size_standardized as (
    select
        *,
        case
            when lot_area_units ilike '%acre%' then lot_area_value * 43560
            when lot_area_units ilike '%sq%' or lot_area_units ilike '%square%' then lot_area_value
            else lot_area_value
        end as lot_area_value_sqft
    from address_cleaned
),

units_fixed as (
    select
        *,
        case
            when lower(living_area_units_short) in ('sqft', 'square feet', 'ft2') then living_area
            when lower(living_area_units_short) in ('sqm', 'square meters') then living_area * 10.7639
            else living_area -- assume sqft if unknown
        end as living_area_sqft
    from lot_size_standardized
),

geography_added as (
    select
        *,
        st_point(longitude, latitude)::geography as geog_point
    from units_fixed
),

json_cleaned as (
    select
        *,
        case when jsonb_typeof(price_history::jsonb) is not null then price_history::jsonb else null end as price_history_json,
        case when jsonb_typeof(school_distances::jsonb) is not null then school_distances::jsonb else null end as school_distances_json,
        case when jsonb_typeof(risks::jsonb) is not null then risks::jsonb else null end as risks_json,
        case when jsonb_typeof(foreclosure::jsonb) is not null then foreclosure::jsonb else null end as foreclosure_json
    from geography_added
),

buildings as (
    select geog_point from {{ ref('stg_buildings') }}
),

within_5_miles as (
    select z.*
    from json_cleaned z
    join buildings b on st_dwithin(z.geog_point, b.geog_point, 8046.72) -- 5 miles in meters
)

select
    id::text,
    source_url as url,
    created_at,
    status,
    is_eligible_property,
    selling_soon,
    last_sold_price,
    date_posted,
    marketing_name,
    posting_product_type,
    lot_area_units,
    lot_area_value_sqft,
    lot_size,
    living_area_sqft,
    street_address_clean as street_address,
    city_clean as city,
    state_clean as state,
    zip_clean as zipcode,
    price,
    home_type_clean as home_type,
    is_preforeclosure_auction,
    address,
    bedrooms,
    bathrooms,
    year_built,
    living_area_units_short,
    country,
    zestimate,
    new_construction_type,
    time_on_zillow,
    page_view_count,
    favorite_count,
    days_on_zillow,
    latitude as lat,
    longitude as lon,
    geog_point,
    is_income_restricted,
    price_history_json as price_history,
    most_recent_price,
    most_recent_price_date_parsed as most_recent_price_date,
    most_recent_price_change_rate,
    school_distances_json as school_distances,
    num_schools_close_to,
    avg_school_distance,
    risks_json as risks,
    description,
    foreclosure_json as foreclosure
from within_5_miles
