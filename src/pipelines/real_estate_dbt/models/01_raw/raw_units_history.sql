-- models/01_raw/raw_units_history.sql

select
    building_id,
    unit_id,
    min_price,
    COALESCE(price, min_price, max_price) price,
    max_price,
    availability,
    from_date,
    to_date,
    min_effective_price,
    effective_price,
    max_effective_price,
    period_id,
    price_plans,
    lease_term,
    _data_pipeline_only_state
from {{ source('raw_data', 'hello_data_units_history_raw') }}
where building_id in (select id from {{ ref('raw_buildings') }})
AND COALESCE(price, min_price, max_price) is not null
