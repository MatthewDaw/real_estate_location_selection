-- models/04_units_history/units_history.sql

WITH raw_units AS (
    SELECT * FROM {{ ref('raw_units_history') }}
),

buildings AS (
    SELECT
        id::text,
        zip_code,
        city,
        state,
        msa
    FROM {{ ref('stg_buildings') }}
),

averaged_values AS (
    SELECT
        building_id::text,
        unit_id::text,
        MAX(to_date::date) AS to_date,
        from_date::date,
        AVG(price::float) AS price,
        AVG(effective_price::float) AS effective_price
    FROM raw_units
    GROUP BY building_id::text, unit_id::text, from_date::date
),

-- Filter to only units with data within the last 7 months
units_with_recent_data AS (
    SELECT unit_id::text
    FROM averaged_values
    GROUP BY unit_id::text
    HAVING MAX(to_date) >= CURRENT_DATE - INTERVAL '7 months'
),

recent_units AS (
    SELECT
        av.building_id::text,
        av.unit_id::text,
        av.to_date::date,
        av.from_date::date,
        av.price::float,
        av.effective_price::float
    FROM averaged_values av
    INNER JOIN units_with_recent_data ur ON av.unit_id = ur.unit_id
),

date_bounds AS (
    SELECT
        MIN(from_date) AS min_from_date,
        MAX(to_date) AS max_to_date
    FROM recent_units
),

date_series AS (
    SELECT
        gs::date AS focus_date
    FROM date_bounds,
         generate_series(
             min_from_date,
             max_to_date,
             '7 days'::interval
         ) AS gs
),

unit_history_with_dates AS (
    SELECT
        h.building_id::text,
        h.unit_id::text,
        h.from_date::date,
        h.to_date::date,
        h.price::float,
        h.effective_price::float,
        d.focus_date::date AS date
    FROM recent_units h
    CROSS JOIN date_series d
    WHERE h.from_date <= d.focus_date AND d.focus_date <= h.to_date
)

SELECT
    uh.building_id::text,
    uh.unit_id::text,
    uh.price::float,
    uh.effective_price::float,
    uh.date::date,
    b.zip_code,
    b.city,
    b.state,
    b.msa
FROM unit_history_with_dates uh
LEFT JOIN buildings b ON uh.building_id = b.id
ORDER BY date, building_id, unit_id