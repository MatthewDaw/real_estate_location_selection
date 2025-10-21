-- models/04_units_history/units_history_grouped_by_geography.sql

WITH zip_aggregated AS (
    SELECT
        'zip_code' AS geography_type,
        zip_code AS geography_value,
        date,
        AVG(price) AS price,
        AVG(effective_price) AS effective_price,
        COUNT(*) AS unit_count
    FROM {{ ref('units_history') }}
    WHERE zip_code IS NOT NULL
    GROUP BY zip_code, date
),

city_aggregated AS (
    SELECT
        'city' AS geography_type,
        city AS geography_value,
        date,
        AVG(price) AS price,
        AVG(effective_price) AS effective_price,
        COUNT(*) AS unit_count
    FROM {{ ref('units_history') }}
    WHERE city IS NOT NULL
    GROUP BY city, date
),

state_aggregated AS (
    SELECT
        'state' AS geography_type,
        state AS geography_value,
        date,
        AVG(price) AS price,
        AVG(effective_price) AS effective_price,
        COUNT(*) AS unit_count
    FROM {{ ref('units_history') }}
    WHERE state IS NOT NULL
    GROUP BY state, date
),

msa_aggregated AS (
    SELECT
        'msa' AS geography_type,
        msa AS geography_value,
        date,
        AVG(price) AS price,
        AVG(effective_price) AS effective_price,
        COUNT(*) AS unit_count
    FROM {{ ref('units_history') }}
    WHERE msa IS NOT NULL
    GROUP BY msa, date
),

combined_geography AS (
    SELECT * FROM zip_aggregated
    UNION ALL
    SELECT * FROM city_aggregated
    UNION ALL
    SELECT * FROM state_aggregated
    UNION ALL
    SELECT * FROM msa_aggregated
)

SELECT
    geography_type,
    geography_value,
    date,
    price,
    effective_price,
    unit_count
FROM combined_geography
ORDER BY geography_type, geography_value, date