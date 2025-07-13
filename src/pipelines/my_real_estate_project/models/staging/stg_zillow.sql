-- models/staging/stg_zillow.sql

with raw_zillow as (
    select * from {{ ref('raw_zillow') }}
),

filtered_and_deduplicated as (
    select
        id::text,
        source_url::text as url,
        created_at::timestamp,
        status::text,
        is_eligible_property::boolean,
        selling_soon,
        last_sold_price::float,
        date_posted_string::text,
        marketing_name::text,
        posting_product_type::text,
        lot_area_units::text,
        -- Convert lot_area_value to square feet
        case
            when lot_area_units ilike '%acre%' then (lot_area_value * 43560)::float
            when lot_area_units ilike '%sq%' or lot_area_units ilike '%square%' then lot_area_value::float
            else lot_area_value::float
        end as lot_area_value_sqft,
        lot_size::float,
        living_area::float,
        street_address::text,
        city::text,
        state::text,
        zipcode::text,
        price::float,
        home_type::text,
        is_preforeclosure_auction::boolean,
        address,
        bedrooms::integer,
        bathrooms::float,
        year_built::integer,
        living_area_units_short::text,
        country::text,
        zestimate::float,
        new_construction_type::text,
        time_on_zillow::text,
        page_view_count::integer,
        favorite_count::integer,
        days_on_zillow::integer,
        lat::float,
        lon::float,
        st_point(lon, lat)::geography as geog_point,
        is_income_restricted::boolean,
        price_history,
        most_recent_price::float,
        most_recent_price_date::text,
        most_recent_price_change_rate::float,
        school_distances,
        num_schools_close_to::integer,
        avg_school_distance::float,
        risks,
        description::text,
        foreclosure
    from (
        select
            id,
            source_url,
            created_at,
            status,
            is_eligible_property,
            selling_soon,
            last_sold_price,
            date_posted_string,
            marketing_name,
            posting_product_type,
            lot_area_units,
            lot_area_value,
            lot_size,
            living_area,
            street_address,
            city,
            state,
            zipcode,
            price,
            home_type,
            is_preforeclosure_auction,
            address,
            bedrooms,
            bathrooms,
            year_built,
            living_area_units_short,
            country,
            zestimate,
            new_construction_type,
            time_on_zillow,
            page_view_count,
            favorite_count,
            days_on_zillow,
            latitude as lat,
            longitude as lon,
            is_income_restricted,
            price_history,
            most_recent_price,
            most_recent_price_date,
            most_recent_price_change_rate,
            school_distances,
            num_schools_close_to,
            avg_school_distance,
            risks,
            description,
            foreclosure,
            row_number() over (partition by source_url order by created_at desc) as row_num
        from raw_zillow
        where
            price is not null
            and (country is null or country != 'CAN')
            and status not in ('SOLD', 'FOR_RENT')
            and price < 700000
    ) ranked
    where row_num = 1
)

select * from filtered_and_deduplicated