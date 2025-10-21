-- models/08_main_tables/empty_lots.sql

with unified_empty_lots as (
    select * from {{ ref('empty_lot_summaries') }}
),

final_with_smoothed as (
    select
        row_number() over (order by uel.source, uel.source_id) as unified_id,
        uel.*
    from unified_empty_lots uel
),

final_with_all_smoothed as (
    select
        fws.*,
        sv.avg_predicted_current_price_per_sqft,
        sv.avg_predicted_current_price_lower_per_sqft,
        sv.avg_predicted_current_price_upper_per_sqft,
        sv.avg_rent_price_per_sqft,
        sv.avg_predicted_current_effective_price_per_sqft,
        sv.avg_predicted_current_effective_price_lower_per_sqft,
        sv.avg_predicted_current_effective_price_upper_per_sqft,
        sv.avg_effective_rent_price_per_sqft,
        sv.avg_predicted_future_price_per_sqft,
        sv.avg_predicted_future_price_lower_per_sqft,
        sv.avg_predicted_future_price_upper_per_sqft,
        sv.avg_predicted_future_effective_price_per_sqft,
        sv.avg_predicted_future_effective_price_lower_per_sqft,
        sv.avg_predicted_future_effective_price_upper_per_sqft,
        sv.avg_predicted_current_price_per_bed,
        sv.avg_predicted_current_price_lower_per_bed,
        sv.avg_predicted_current_price_upper_per_bed,
        sv.avg_rent_price_per_bed,
        sv.avg_predicted_current_effective_price_per_bed,
        sv.avg_predicted_current_effective_price_lower_per_bed,
        sv.avg_predicted_current_effective_price_upper_per_bed,
        sv.avg_effective_rent_price_per_bed,
        sv.avg_predicted_future_price_per_bed,
        sv.avg_predicted_future_price_lower_per_bed,
        sv.avg_predicted_future_price_upper_per_bed,
        sv.avg_predicted_future_effective_price_per_bed,
        sv.avg_predicted_future_effective_price_lower_per_bed,
        sv.avg_predicted_future_effective_price_upper_per_bed,
        sv.avg_beds,
        sv.avg_baths,
        sv.avg_sqft,
        sv.avg_price,
        sv.avg_effective_price,
        sv.num_units,
        sv.avg_building_average_percent_gain_per_year,
        sv.avg_building_average_percent_gain_per_year_effective,
        sv.avg_leased_percentage,
        sv.avg_exposure_percentage,
        sv.avg_concession_percentage,
        sv.avg_zillow_predicted_current_price_per_bedroom,
        sv.avg_zillow_predicted_current_price_lower_per_bedroom,
        sv.avg_zillow_predicted_current_price_upper_per_bedroom,
        sv.avg_zillow_predicted_current_price_per_sqft,
        sv.avg_zillow_predicted_current_price_lower_per_sqft,
        sv.avg_zillow_predicted_current_price_upper_per_sqft,
        sv.avg_zillow_predicted_future_price_per_bedroom,
        sv.avg_zillow_predicted_future_price_lower_per_bedroom,
        sv.avg_zillow_predicted_future_price_upper_per_bedroom,
        sv.avg_zillow_predicted_future_price_per_sqft,
        sv.avg_zillow_predicted_future_price_lower_per_sqft,
        sv.avg_zillow_predicted_future_price_upper_per_sqft,
        sv.avg_zillow_price_per_bedroom,
        sv.avg_zillow_price_per_sqft,
        sv.avg_zillow_average_percent_gain_per_year,
        sv.avg_zillow_trend_strength_pct,
        sv.avg_zillow_trend_variance_pct,
        sv.avg_zillow_data_span_days,
        sv.avg_zillow_current_price,
        sv.avg_zillow_bedrooms,
        sv.avg_zillow_living_area_sqft,
        sv.avg_empty_lot_cost_per_sqft,
        sv.avg_interior_sqft_cost,
        sv.avg_developed_lot_cost_per_sqft,
        sv.hd_buildings_within_radius,
        sv.zillow_buildings_within_radius,
        sv.empty_lots_within_radius,
        sv.developed_properties_within_radius,
        sv.closest_hd_buildings,
        sv.closest_zillow_listings
    from final_with_smoothed fws
    left join {{ ref('smoothed_values') }} sv
        on fws.source = sv.source
        and fws.source_id = sv.source_id
)

select * from final_with_all_smoothed