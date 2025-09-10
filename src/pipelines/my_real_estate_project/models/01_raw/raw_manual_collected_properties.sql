-- models/01_raw/raw_manual_collected_properties.sql

select
    address,
    lat,
    lon,
    ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography as geog_point,
    bed::integer as bed,
    bath::integer as bath,
    sqft::integer as sqft,
    cost::integer as cost,
    url as source_url,
    '2025-09-03'::date as created_at,
    -- Add row number for deduplication if needed
    'manual_' || row_number() over (
        order by address, lat, lon
    ) as id
FROM {{ ref('manual_collected_properties') }}
WHERE
    address IS NOT NULL
    AND lat IS NOT NULL
    AND lon IS NOT NULL
    AND bed IS NOT NULL
    AND bath IS NOT NULL
    AND sqft IS NOT NULL
    AND cost IS NOT NULL
