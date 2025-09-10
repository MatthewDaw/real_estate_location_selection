-- models/01_raw/raw_property_rejections.sql

select
    row_number() over (order by rejected_properties) as id,
    rejected_properties as url,
    current_timestamp as created_at
from {{ ref('property_rejections') }}
