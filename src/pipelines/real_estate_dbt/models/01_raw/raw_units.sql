-- models/01_raw/raw_units.sql

with filtered_units as (
    select
        id,
        building_id,
        floorplan_name,
        unit_name,
        is_floorplan,
        availability,
        min_price,
        price,
        max_price,
        bed,
        bath,
        partial_bath,
        min_sqft,
        COALESCE(sqft, min_sqft, max_sqft) as sqft,
        max_sqft,
        created_on,
        image_gcs_file_names,
        enter_market,
        exit_market,
        days_on_market,
        min_effective_price,
        effective_price,
        max_effective_price,
        floor,
        renovated,
        furnished,
        den,
        convertible,
        price_plans,
        amenities,
        tags,
        source,
        deposit,
        lease_term,
        _data_pipeline_only_state as state,
        source_id,

        -- Add row number for deduplication
        row_number() over (
            partition by id
            order by created_on desc
        ) as row_num

    from {{ source('raw_data', 'hello_data_units_raw') }}
    where
        building_id in (select id from {{ ref('raw_buildings') }})
        AND price is not null
        AND COALESCE(sqft, min_sqft, max_sqft) is not null
        AND is_floorplan = FALSE
        AND (
            exit_market is null
            or exit_market >= current_date - interval '2 years'
        )
)

select
    id,
    building_id,
    floorplan_name,
    unit_name,
    is_floorplan,
    availability,
    min_price,
    price,
    max_price,
    bed,
    bath,
    partial_bath,
    min_sqft,
    sqft,
    max_sqft,
    created_on,
    image_gcs_file_names,
    enter_market,
    exit_market,
    days_on_market,
    min_effective_price,
    effective_price,
    max_effective_price,
    floor,
    renovated,
    furnished,
    den,
    convertible,
    price_plans,
    amenities,
    tags,
    source,
    deposit,
    lease_term,
    state,
    source_id
from filtered_units
where row_num = 1