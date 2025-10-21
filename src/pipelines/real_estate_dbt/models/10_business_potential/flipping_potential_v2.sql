with reduced_developed_properties as (
  select
    -- url
    source_id,
    source,
    source_url,
    -- location
    city,
    state,
    street_address,
    property_type,
    year_built,
    -- basic facts
    price,
    beds,
    baths,
    interior_sqft,
    -- rent statistics
    avg_effective_rent_price_per_sqft as rent_price_per_sqft,
    avg_predicted_current_effective_price_per_sqft as predicted_current_rent_price_per_sqft,
    avg_building_average_percent_gain_per_year_effective as rent_growth_percent_per_year,
    avg_predicted_future_effective_price_per_sqft as predicted_future_rent_price_per_sqft,
    -- rent growth
    predicted_effective_rent_price_growth_percentage,     -- predicted rent price growth percent
    -- property price statistics
    avg_zillow_price_per_sqft as home_price_per_sqft,
    avg_zillow_predicted_current_price_per_sqft as predicted_current_home_price_per_sqft,
    avg_zillow_predicted_future_price_per_bedroom as predicted_future_home_price_per_bedroom, -- one year into future
    avg_zillow_average_percent_gain_per_year / 100 as historic_home_price_growth, -- expected house price growth
    -- property price growth
    predicted_price_growth_percentage,                    -- predicted price growth
    -- leased percentage
    avg_leased_percentage as leased_percentage,
    avg_exposure_percentage as exposure_percentage,
    -- comparable buildings within 4 miles
    hd_buildings_within_radius,
    zillow_buildings_within_radius
  from {{ ref('developed_properties') }}
  where 
  (
  price <= 210000 and state in ('UT', 'ID', 'AZ') and price > 45000
  and 
  source_url in (select url from {{ ref('raw_property_acceptances') }})
  )
  or 
  source = 'manual'

),
performances as (
select 
*,
home_price_per_sqft * interior_sqft as estimate_potential_home_value,
home_price_per_sqft * interior_sqft - price as estimated_home_value_increase,
interior_sqft * predicted_current_rent_price_per_sqft * leased_percentage as esimtated_monthly_income_sqft
from reduced_developed_properties
)

select 
*
from performances
order by estimated_home_value_increase desc
