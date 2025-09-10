-- models/02_staging/closest_buildings_lookup.sql
-- This table is populated by the Python script via dbt operation
-- The actual data comes from the operation, this just defines the schema

{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['closest_building_id'], 'type': 'btree'},
      {'columns': ['source', 'property_id'], 'type': 'btree'}
    ]
  )
}}

select
    'dummy'::text as property_id,
    'dummy'::text as source,
    'dummy'::text as closest_building_id,
    'dummy'::text as msa,
    'dummy'::text as zip_code,
    0.0 as distance_to_closest_building
WHERE 1 = 0  -- No actual data, just schema definition