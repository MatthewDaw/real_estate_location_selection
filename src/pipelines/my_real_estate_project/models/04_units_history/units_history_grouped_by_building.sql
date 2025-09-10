-- models/04_units_history/units_history_grouped_by_building.sql

WITH unit_date_aggregated AS (
    SELECT
        building_id,
        date,
        AVG(price) AS price,
        AVG(effective_price) AS effective_price
    FROM {{ ref('units_history') }}
    GROUP BY building_id, date
)

SELECT
    building_id,
    date,
    price,
    effective_price
FROM unit_date_aggregated
ORDER BY building_id, date
