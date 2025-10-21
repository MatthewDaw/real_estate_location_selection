-- macros/02_closest_buildings.sql
{% macro run_closest_buildings_script() %}
  {% if execute %}
    {% set schema_name = target.schema %}
    {% set project_root = project_root %}

    -- Log what we're about to do
    {{ log("Running closest buildings calculation for schema: " ~ schema_name, info=True) }}
    {{ log("Project root: " ~ project_root, info=True) }}

    -- We can't execute shell commands directly from dbt macros
    -- This macro just logs instructions for manual execution
    {{ log("Please run this command manually:", info=True) }}
    {{ log("python " ~ project_root ~ "/scripts/02_closest_buildings.py --output-table " ~ schema_name ~ ".closest_buildings_lookup", info=True) }}

  {% endif %}
{% endmacro %}