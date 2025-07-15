-- models/raw/raw_units_history.sql

select
    building_id,
    unit_id,
    min_price,
    price,
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
where building_id in ('83663652-bae9-56af-950e-cb856458e150',
'bc47b916-ad92-5e95-a722-7d053726930b',
'74879860-4a95-55cf-aeea-7a56021b5f4c',
'42933348-9647-5dc9-888c-8464c9eb73b9',
'93dd6da5-ce63-57b7-8125-4d4934eb3944',
'a5dc78b2-ef45-5c68-ba55-f1b32f7106f7',
'8f3d3ff1-104b-5600-a804-74c96143f047',
'eff11745-5c84-558e-8f1b-eea57f25619b',
'd9c42f03-a5e7-5a72-81aa-e0ffedc53109')

