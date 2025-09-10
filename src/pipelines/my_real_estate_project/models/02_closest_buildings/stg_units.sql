-- models/02_staging/stg_units.sql

with raw_units as (
    select * from {{ ref('raw_units') }}
)

select
    id::text,
    building_id::text,
    floorplan_name::text,
    unit_name::text,
    availability::date,
    price::integer,
    case
        when bed = 0 then 1
        else bed
    end::integer as bed,
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
from raw_units