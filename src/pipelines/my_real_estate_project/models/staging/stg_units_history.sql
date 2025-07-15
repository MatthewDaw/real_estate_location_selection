-- models/staging/stg_units_history.sql

with raw_units_history as (
    select * from {{ ref('raw_units_history') }}
),

-- Find the earliest from_date and latest to_date for each individual unit
unit_date_ranges as (
    select
        building_id,
        unit_id,
        min(from_date) as earliest_date,
        coalesce(max(to_date), current_date) as latest_date
    from raw_units_history
    where from_date is not null
    group by building_id, unit_id
),

-- Generate weekly date series for each unit from its earliest date to latest date
unit_date_spines as (
    select
        udr.building_id,
        udr.unit_id,
        udr.earliest_date + (interval '14 days' * generate_series(0,
            ceil((udr.latest_date - udr.earliest_date) / 14)::int
        )) as week_date
    from unit_date_ranges udr
),

-- Create a record for every unit for every week (specific to each unit's date range)
unit_week_spine as (
    select
        uds.building_id,
        uds.unit_id,
        uds.week_date
    from unit_date_spines uds
),

-- For each unit and week, find the active record (if any)
units_with_active_periods as (
    select
        s.building_id,
        s.unit_id,
        s.week_date,
        h.price,
        h.effective_price,
        h.period_id,
        h.from_date,
        -- Add row number to handle multiple overlapping periods
        row_number() over (
            partition by s.building_id, s.unit_id, s.week_date
            order by h.from_date desc, h.to_date desc nulls first
        ) as rn
    from unit_week_spine s
    left join raw_units_history h on (
        s.building_id = h.building_id
        and s.unit_id = h.unit_id
        and s.week_date >= h.from_date
        and (h.to_date is null or s.week_date <= h.to_date)
    )
),

-- Take only the most recent period for each unit/week combination
final_periods as (
    select
        building_id,
        unit_id,
        week_date,
        price,
        effective_price,
        period_id,
        from_date
    from units_with_active_periods
    where rn = 1
)

select
    building_id,
    unit_id,
    week_date,
    avg(price) as price,
    avg(effective_price) as effective_price,
    avg(period_id) as period_id,
    -- Add flags for data quality
    bool_or(price is not null) as has_pricing_data,
    bool_or(from_date is not null) as has_period_data
from final_periods
group by building_id, unit_id, week_date
order by building_id, unit_id, week_date desc