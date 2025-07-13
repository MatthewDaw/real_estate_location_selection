-- models/staging/stg_units.sql

with raw_units as (
    select * from {{ ref('raw_units') }}
),

filtered_and_deduplicated as (
    select
        id::text,
        building_id::text,
        floorplan_name::text,
        unit_name::text,
        availability::date,
        price::integer,
        bed::integer,
        bath::integer,
        partial_bath::integer,
        sqft::integer,
        created_on::date,
        enter_market::date,
        exit_market::date,
        days_on_market::integer,
        effective_price::integer,
        floor::integer,
        renovated::boolean,
        furnished::boolean,
        den::boolean,
        convertible::boolean,
        amenities
    from (
        select
            id,
            building_id,
            floorplan_name,
            unit_name,
            availability,
            price,
            bed,
            bath,
            partial_bath,
            sqft,
            created_on,
            enter_market,
            exit_market,
            days_on_market,
            effective_price,
            floor,
            renovated,
            furnished,
            den,
            convertible,
            amenities,
            row_number() over (partition by id order by created_on desc) as row_num
        from raw_units
        where
            is_floorplan = false
            and price is not null
            and (
                exit_market is null
                or exit_market >= current_date - interval '2 years'
            )
    ) ranked
    where row_num = 1
)

select * from filtered_and_deduplicated