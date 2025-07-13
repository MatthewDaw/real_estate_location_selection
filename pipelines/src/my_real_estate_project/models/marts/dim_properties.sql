select
    buildings.id,
    buildings.street_address,
    buildings.city,
    buildings.state,
    buildings.building_name,
    buildings.is_single_family,
    buildings.is_condo,
    buildings.is_apartment,
    count(units.id) as total_units,
    avg(units.price) as avg_unit_price,
    min(units.price) as min_unit_price,
    max(units.price) as max_unit_price
from {{ ref('raw_buildings') }} as buildings
left join {{ ref('raw_units') }} as units
    on buildings.id = units.building_id
group by 1, 2, 3, 4, 5, 6, 7, 8