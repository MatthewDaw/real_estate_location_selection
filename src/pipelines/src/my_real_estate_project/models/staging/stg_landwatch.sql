-- models/staging/stg_landwatch.sql

with raw_landwatch as (
    select * from {{ ref('raw_landwatch') }}
),

filtered_and_deduplicated as (
    select
        id::text,
        url::text,
        created_at::timestamp,
        name::text,
        property_type::text,
        date_posted::text,
        lot_size::float,
        lot_size_units::text,
        lot_type,
        amenities,
        mortgage_options,
        activities,
        geography,
        state::text,
        city::text,
        zip::text,
        address1::text,
        address2::text,
        lat::float,
        lon::float,
        st_point(lon, lat)::geography as geog_point,
        city_latitude::float,
        city_longitude::float,
        acres::float,
        beds::integer,
        baths::integer,
        homesqft::float,
        property_types,
        is_residence::boolean,
        price::float,
        listing_date::text,
        title::text,
        cost_per_acre::float,
        distance_to_city_miles::float,
        cost_per_homesqft::float
    from (
        select
            id,
            url,
            created_at,
            name,
            property_type,
            date_posted,
            lot_size,
            lot_size_units,
            lot_type,
            amenities,
            mortgage_options,
            activities,
            geography,
            state,
            city,
            zip,
            address1,
            address2,
            latitude as lat,
            longitude as lon,
            city_latitude,
            city_longitude,
            acres,
            beds,
            baths,
            homesqft,
            property_types,
            is_residence,
            price,
            listing_date,
            title,
            cost_per_acre,
            distance_to_city_miles,
            cost_per_homesqft,
            row_number() over (partition by url order by created_at desc) as row_num
        from raw_landwatch
        where
            price is not null
            and latitude is not null
            and longitude is not null
            and price < 700000
    ) ranked
    where row_num = 1
)

select * from filtered_and_deduplicated