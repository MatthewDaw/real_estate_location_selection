-- models/01_raw/raw_zillow.sql


with filtered_and_deduplicated as (
    select
        z.*,
        row_number() over (partition by z.source_url order by z.created_at desc) as row_num
    from {{ source('raw_data', 'zillow_property_raw') }} z
    inner join {{ source('raw_data', 'zillow_urls') }} u
        on z.source_url = u.url
    where
        z.state = 'UT'
        and z.price between 5000 and 700000
        and z.status not in ('SOLD', 'FOR_RENT')
        and (z.country is null or z.country != 'CAN')
        and z.latitude is not null and z.longitude is not null
        and z.latitude between -90 and 90
        and z.longitude between -180 and 180
        and z.price is not null
)

select
    'zillow_' || row_number() over (order by source_url) as id,
    source_url,
    created_at,
    status,
    is_eligible_property,
    selling_soon,
    last_sold_price,
    posting_url,
    date_posted_string,
    marketing_name,
    posting_product_type,
    lot_area_units,
    lot_area_value,
    lot_size,
    living_area_units,
    living_area,
    street_address,
    city,
    state,
    zipcode,
    price,
    currency,
    home_type,
    is_preforeclosure_auction,
    address,
    bedrooms,
    bathrooms,
    year_built,
    living_area_units_short,
    country,
    monthly_hoa_fee,
    zestimate,
    new_construction_type,
    zestimate_low_percent,
    zestimate_high_percent,
    time_on_zillow,
    page_view_count,
    favorite_count,
    days_on_zillow,
    latitude,
    longitude,
    is_income_restricted,
    price_history,
    most_recent_price,
    most_recent_price_date,
    most_recent_price_change_rate,
    rental_application_accepted_type,
    home_insights,
    school_distances,
    num_schools_close_to,
    avg_school_distance,
    risks,
    description,
    foreclosure,
    geog_point
from filtered_and_deduplicated
where row_num = 1