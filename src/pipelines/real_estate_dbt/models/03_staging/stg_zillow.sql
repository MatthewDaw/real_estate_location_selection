-- models/03_staging/stg_zillow.sql

with raw_zillow as (
    select * from {{ ref('raw_zillow') }}
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
            when lower(trim(home_type)) in ('lot', 'unknown') then 'lot'
            else lower(trim(home_type))
        end as home_type_clean
    from raw_zillow
),

missing_numeric_fixed as (
    select
        *,
        case
            when (coalesce(bedrooms, 0) = 0) and (coalesce(living_area, 0) > 0) then 1
            else coalesce(bedrooms, 0)
        end as beds_fixed,
        coalesce(bedrooms, 0) as baths_fixed,
        coalesce(living_area, 0) as homesqft_fixed
    from home_type_normalized
),

filtered_impossible_setup as (
      select * from missing_numeric_fixed where
    -- living area with no beds or baths
    not
        (
        homesqft_fixed > 0
        and beds_fixed = 0
        and baths_fixed = 0
        )
    -- no living area with beds or baths
    and
    not
        (
        homesqft_fixed = 0
        and beds_fixed != 0
        and baths_fixed != 0
        )
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
    from filtered_impossible_setup
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
            when lower(living_area_units_short) in ('sqft', 'square feet', 'ft2') then homesqft_fixed
            when lower(living_area_units_short) in ('sqm', 'square meters') then homesqft_fixed * 10.7639
            else homesqft_fixed -- assume sqft if unknown
        end as living_area_sqft
    from lot_size_standardized
),

json_cleaned as (
    select
        *,
        case when jsonb_typeof(price_history::jsonb) is not null then price_history::jsonb else null end as price_history_json,
        case when jsonb_typeof(school_distances::jsonb) is not null then school_distances::jsonb else null end as school_distances_json,
        case when jsonb_typeof(risks::jsonb) is not null then risks::jsonb else null end as risks_json,
        case when jsonb_typeof(foreclosure::jsonb) is not null then foreclosure::jsonb else null end as foreclosure_json
    from units_fixed
),

-- Join with pre-computed closest buildings
closest_building_added as (
    select
        z.*,
        cb.closest_building_id,
        cb.msa,
        cb.zip_code as closest_building_zip_code,
        cb.distance_to_closest_building
    from json_cleaned z
    left join {{ ref('closest_buildings_lookup') }} cb
        on z.id::text = cb.property_id
        and cb.source = 'zillow'
),

within_4_miles as (
    select z.*
    from closest_building_added z
    where distance_to_closest_building <= 2414.01 -- 1.5 miles in meters
),

filtered_smaller_properties as (
      select * from within_4_miles where lot_area_value_sqft <  43560 * 1.5
)

select
    id::text,
    source_url::text as url,
    created_at::timestamp,
    status::text,
    is_eligible_property::boolean,
    selling_soon::jsonb,
    last_sold_price::numeric,
    date_posted::date,
    marketing_name::text,
    posting_product_type::text,
    lot_area_units::text,
    lot_area_value_sqft::numeric,
    lot_size::numeric,
    living_area_sqft::numeric,
    street_address_clean::text as street_address,
    -- Use closest building zip_code, keep original city and state
    city_clean as city,
    state_clean as state,
    coalesce(closest_building_zip_code, zip_clean) as zip_code,
    price::numeric,
    home_type_clean::text as home_type,
    is_preforeclosure_auction::boolean,
    address::text,
    beds_fixed::integer as bedrooms,
    bathrooms::float,
    year_built::integer,
    living_area_units_short::text,
    country::text,
    zestimate::numeric,
    new_construction_type::text,
    REGEXP_REPLACE(time_on_zillow, '[^0-9]', '', 'g')::integer,
    page_view_count::integer,
    favorite_count::integer,
    days_on_zillow::integer,
    latitude::float as lat,
    longitude::float as lon,
    geog_point,
    msa::text,
    is_income_restricted::boolean,
    price_history_json as price_history,
    most_recent_price::numeric,
    most_recent_price_date_parsed::date as most_recent_price_date,
    most_recent_price_change_rate::float,
    school_distances_json as school_distances,
    num_schools_close_to::integer,
    avg_school_distance::float,
    risks_json as risks,
    description::text,
    foreclosure_json as foreclosure,
    closest_building_id::text
from filtered_smaller_properties