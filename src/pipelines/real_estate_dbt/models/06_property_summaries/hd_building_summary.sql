-- 06_property_summaries/hd_building_summary.sql

with grouped_unit_data as (
    select
        building_id,
        avg(bed::decimal) as avg_beds,
        avg(bath::decimal) as avg_baths,
        avg(sqft::decimal) as avg_sqft,
        avg(price::decimal) as avg_price,
        avg(effective_price::decimal) as avg_effective_price,
        count(*) as num_units
    from {{ ref('stg_units') }}
    group by building_id
),

final as (
    select
        b.id as building_id,
        ba.building_id as building_analysis_building_id,
        gud.building_id as grouped_unit_building_id,
        ba.average_percent_gain_per_year,
        ba.average_percent_gain_per_year_effective,

        b.lat,
        b.lon,
        b.msa,
        b.zip_code,
        b.geog_point,
        b.leased_percentage,
        b.exposure_percentage,
        b.listing_urls,
        gud.avg_price::numeric - gud.avg_effective_price / gud.avg_price as concession_percentage,


        -- Current Price metrics divided by sqft
        ba.predicted_current_price / nullif(gud.avg_sqft, 0) as predicted_current_price_per_sqft,
        ba.predicted_current_price_lower / nullif(gud.avg_sqft, 0) as predicted_current_price_lower_per_sqft,
        ba.predicted_current_price_upper / nullif(gud.avg_sqft, 0) as predicted_current_price_upper_per_sqft,
        gud.avg_price / nullif(gud.avg_sqft, 0) as avg_rent_price_per_sqft,

        -- Current Effective Price metrics divided by sqft
        ba.predicted_current_effective_price / nullif(gud.avg_sqft, 0) as predicted_current_effective_price_per_sqft,
        ba.predicted_current_effective_price_lower / nullif(gud.avg_sqft, 0) as predicted_current_effective_price_lower_per_sqft,
        ba.predicted_current_effective_price_upper / nullif(gud.avg_sqft, 0) as predicted_current_effective_price_upper_per_sqft,
        gud.avg_effective_price / nullif(gud.avg_sqft, 0) as avg_effective_rent_price_per_sqft,

        -- Future Price metrics divided by sqft
        ba.predicted_future_price / nullif(gud.avg_sqft, 0) as predicted_future_price_per_sqft,
        ba.predicted_future_price_lower / nullif(gud.avg_sqft, 0) as predicted_future_price_lower_per_sqft,
        ba.predicted_future_price_upper / nullif(gud.avg_sqft, 0) as predicted_future_price_upper_per_sqft,

        -- Future Effective Price metrics divided by sqft
        ba.predicted_future_effective_price / nullif(gud.avg_sqft, 0) as predicted_future_effective_price_per_sqft,
        ba.predicted_future_effective_price_lower / nullif(gud.avg_sqft, 0) as predicted_future_effective_price_lower_per_sqft,
        ba.predicted_future_effective_price_upper / nullif(gud.avg_sqft, 0) as predicted_future_effective_price_upper_per_sqft,

        -- Current Price metrics divided by beds
        ba.predicted_current_price / nullif(gud.avg_beds, 0) as predicted_current_price_per_bed,
        ba.predicted_current_price_lower / nullif(gud.avg_beds, 0) as predicted_current_price_lower_per_bed,
        ba.predicted_current_price_upper / nullif(gud.avg_beds, 0) as predicted_current_price_upper_per_bed,
        gud.avg_price / nullif(gud.avg_beds, 0) as avg_rent_price_per_bed,

        -- Current Effective Price metrics divided by beds
        ba.predicted_current_effective_price / nullif(gud.avg_beds, 0) as predicted_current_effective_price_per_bed,
        ba.predicted_current_effective_price_lower / nullif(gud.avg_beds, 0) as predicted_current_effective_price_lower_per_bed,
        ba.predicted_current_effective_price_upper / nullif(gud.avg_beds, 0) as predicted_current_effective_price_upper_per_bed,
        gud.avg_effective_price / nullif(gud.avg_beds, 0) as avg_effective_rent_price_per_bed,

        -- Future Price metrics divided by beds
        ba.predicted_future_price / nullif(gud.avg_beds, 0) as predicted_future_price_per_bed,
        ba.predicted_future_price_lower / nullif(gud.avg_beds, 0) as predicted_future_price_lower_per_bed,
        ba.predicted_future_price_upper / nullif(gud.avg_beds, 0) as predicted_future_price_upper_per_bed,

        -- Future Effective Price metrics divided by beds
        ba.predicted_future_effective_price / nullif(gud.avg_beds, 0) as predicted_future_effective_price_per_bed,
        ba.predicted_future_effective_price_lower / nullif(gud.avg_beds, 0) as predicted_future_effective_price_lower_per_bed,
        ba.predicted_future_effective_price_upper / nullif(gud.avg_beds, 0) as predicted_future_effective_price_upper_per_bed,

        -- Raw prediction values (for reference)
        ba.predicted_current_price,
        ba.predicted_current_price_lower,
        ba.predicted_current_price_upper,
        ba.predicted_current_effective_price,
        ba.predicted_current_effective_price_lower,
        ba.predicted_current_effective_price_upper,
        ba.predicted_future_price,
        ba.predicted_future_price_lower,
        ba.predicted_future_price_upper,
        ba.predicted_future_effective_price,
        ba.predicted_future_effective_price_lower,
        ba.predicted_future_effective_price_upper,

        -- Unit averages for context
        gud.avg_beds,
        gud.avg_baths,
        gud.avg_sqft,
        gud.avg_price,
        gud.avg_effective_price,
        gud.num_units

    from {{ ref('stg_buildings') }} b
    left join {{ ref('building_analysis') }} ba
        on b.id = ba.building_id
    left join grouped_unit_data gud
        on b.id = gud.building_id
)

select * from final