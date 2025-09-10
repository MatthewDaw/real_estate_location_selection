-- 06_property_summaries/points_of_interest.sql

{{
  config(
    materialized='view'
  )
}}

with zillow_points as (
    select
        'zillow' as source,
        id as source_id,
        lat,
        lon
    from {{ ref('stg_zillow') }}
),

landwatch_points as (
    select
        'landwatch' as source,
        id as source_id,
        lat,
        lon
    from {{ ref('stg_landwatch') }}
),

manual_points as (
    select
        'manual' as source,
        id as source_id,
        lat,
        lon
    from {{ ref('stg_manual_collected_properties') }}
),

unified_points as (
    select * from zillow_points
    union all
    select * from landwatch_points
    union all
    select * from manual_points
)

select
    source,
    source_id,
    lat,
    lon
from unified_points
