-- models/marts/avg_building_history.sql

with units_history as (
    select * from {{ ref('stg_units_history') }}
)

select
    building_id,
    week_date,
    avg(price) as avg_price,
    avg(effective_price) as avg_effective_price,
    count(*) as unit_count,
    count(case when has_pricing_data then 1 end) as units_with_pricing_data,
    count(case when has_period_data then 1 end) as units_with_period_data,
    -- Calculate percentage of units with data
    round(
        count(case when has_pricing_data then 1 end)::numeric / count(*) * 100,
        2
    ) as pct_units_with_pricing_data,
    round(
        count(case when has_period_data then 1 end)::numeric / count(*) * 100,
        2
    ) as pct_units_with_period_data
from units_history
group by building_id, week_date
order by building_id, week_date desc