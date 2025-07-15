-- models/marts/units_history.sql

select * from {{ ref('stg_units_history') }}
