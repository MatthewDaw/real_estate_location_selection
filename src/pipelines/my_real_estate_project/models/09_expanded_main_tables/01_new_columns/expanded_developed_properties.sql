-- 09_expanded_main_tables/01_new_columns/expanded_developed_properties.sql

with new_cols as (
select
    *,
    price * (monthly_rate * POWER(1 + monthly_rate, total_payments)) /
    (POWER(1 + monthly_rate, total_payments) - 1) AS monthly_payment,
    coalesce(avg_rent_price_per_sqft, avg_predicted_current_effective_price_per_sqft) * interior_sqft as estimate_monthly_income_sqft,
    coalesce(avg_rent_price_per_bed, avg_predicted_current_effective_price_per_bed) * beds as estimate_monthly_income_beds,
    coalesce(avg_rent_price_per_bed, avg_predicted_current_effective_price_per_bed) as price_per_bed

from {{ ref('developed_properties') }} dp
CROSS JOIN (
    select 0.082::numeric / 12 AS monthly_rate,
           30 * 12 AS total_payments
) mortgage_stats
WHERE
    coalesce(avg_rent_price_per_sqft, avg_predicted_current_effective_price_per_sqft) is not null
)

select *,
    GREATEST(estimate_monthly_income_sqft - monthly_payment, estimate_monthly_income_beds - monthly_payment) as best_case_profit,
    LEAST(estimate_monthly_income_sqft - monthly_payment, estimate_monthly_income_beds - monthly_payment) as worst_case_profit,
    ((estimate_monthly_income_sqft - monthly_payment) + (estimate_monthly_income_beds - monthly_payment)) / 2 as average_case_profit
from new_cols
