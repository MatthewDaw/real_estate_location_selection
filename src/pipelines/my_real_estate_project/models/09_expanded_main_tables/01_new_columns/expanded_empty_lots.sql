-- 09_expanded_main_tables/01_new_columns/expanded_empty_lots.sql

with mortgage_constants as (
    select 
        0.0625::numeric / 12 as monthly_rate,
        30 * 12 as total_payments
),

development_metrics as (
    select
        avg(interior_sqft / lot_area_value_sqft) as interior_sqft_per_lot_sqft,
        avg((price - (lot_area_value_sqft * avg_empty_lot_cost_per_sqft)) / interior_sqft) as estimated_building_cost_per_sqft
    from {{ ref('developed_properties') }}
    where interior_sqft > 0 
      and lot_area_value_sqft > 0
      and avg_empty_lot_cost_per_sqft is not null
),

empty_lots_with_calculations as (
    select
        el.*,
        dm.interior_sqft_per_lot_sqft,
        dm.estimated_building_cost_per_sqft,

        -- Lot mortgage payment
        el.price * (mc.monthly_rate * power(1 + mc.monthly_rate, mc.total_payments)) /
        (power(1 + mc.monthly_rate, mc.total_payments) - 1) as lot_monthly_payment,

        -- Potential interior square footage if developed
        el.lot_area_value_sqft * dm.interior_sqft_per_lot_sqft as potential_interior_sqft,

        -- Estimated building cost
        (el.lot_area_value_sqft * dm.interior_sqft_per_lot_sqft) * dm.estimated_building_cost_per_sqft as estimated_building_cost

    from {{ ref('empty_lots') }} el
    cross join mortgage_constants mc
    cross join development_metrics dm
),
monthly_payments as (
select
    *,
    -- Total development cost (lot + building)
    price + estimated_building_cost as total_development_cost,

    -- Monthly revenue (price per sqft * feet)
    avg_predicted_current_effective_price_per_sqft * potential_interior_sqft as estimate_monthly_income_sqft,

    -- Full monthly payment for lot + building
    (price + estimated_building_cost) * (
        select (mc.monthly_rate * power(1 + mc.monthly_rate, mc.total_payments)) /
               (power(1 + mc.monthly_rate, mc.total_payments) - 1)
        from mortgage_constants mc
    ) as estimated_full_monthly_payment

from empty_lots_with_calculations
)
select
    *,
    estimate_monthly_income_sqft - estimated_full_monthly_payment as estimate_monthly_profit_sqft
from
    monthly_payments