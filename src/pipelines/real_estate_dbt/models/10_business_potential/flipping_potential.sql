
with base_data as (
select 
*,
coalesce(estimated_monthly_income_sqft_lease_percentage, estimated_monthly_income_sqft*0.9) as hoped_for_monthly_income
from {{ ref('developed_properties') }}
)



select 

    price / NULLIF(hoped_for_monthly_income, 0) as invest_to_monthly_return,

    -- cost
    price, 
    price_per_sqft,

    -- comparison cost
    avg_zillow_price_per_sqft,
    avg_leased_percentage,
    avg_exposure_percentage,
    avg_rent_price_per_sqft,

    -- estimate income
    estimated_monthly_income_sqft,
    estimated_monthly_income_sqft_lease_percentage,
    hoped_for_monthly_income,


    -- house characteristics
    beds, 
    baths, 
    interior_sqft,

    -- house identifiers
    source_id,
    source_url, 
    property_name, 
    lat,
    lon,

    -- source reliability
    hd_buildings_within_radius,
    zillow_buildings_within_radius,
    1 - (avg_zillow_price_per_sqft - price_per_sqft) / NULLIF(avg_zillow_price_per_sqft, 0) as percent_of_average_price

from base_data
where 
-- source = 'manual'
-- and
hd_buildings_within_radius > 2
and 
zillow_buildings_within_radius > 2
and 
beds > 0
and 
baths > 0
and 
price > 20000
and 
property_type not in ('undeveloped-land','manufactured')
and 
year_built < 2015
and 
source_url not in (select url from {{ ref('raw_property_rejections') }})
order by invest_to_monthly_return asc


