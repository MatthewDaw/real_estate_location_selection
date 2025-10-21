"""
Dagster definitions for the real estate data pipeline.
This file wires together all assets, resources, and jobs.
"""
from pathlib import Path
from dagster import (
    Definitions,
    define_asset_job,
    AssetSelection,
    ScheduleDefinition,
)
from dagster_dbt import DbtCliResource

from .assets import dbt_assets, python_transformations
from .resources import DatabaseResource

# Path to dbt project
DBT_PROJECT_DIR = Path(__file__).parent.parent

# Define resources
resources = {
    "dbt": DbtCliResource(project_dir=str(DBT_PROJECT_DIR)),
    "database": DatabaseResource(),
}

# Define asset jobs that mirror the shell script stages
# Each job represents one or more stages from run_all.sh

stage_01_job = define_asset_job(
    name="stage_01_raw",
    selection=AssetSelection.groups("01_raw"),
    description="Stage 01: Raw data models",
)

stage_02_job = define_asset_job(
    name="stage_02_closest_buildings",
    selection=AssetSelection.groups("02_closest_buildings"),
    description="Stage 02: Closest buildings calculation (dbt + Python)",
)

stage_03_job = define_asset_job(
    name="stage_03_staging",
    selection=AssetSelection.groups("03_staging"),
    description="Stage 03: Staging transformations",
)

stage_04_job = define_asset_job(
    name="stage_04_units_history",
    selection=AssetSelection.groups("04_units_history"),
    description="Stage 04: Units history processing",
)

stage_05_job = define_asset_job(
    name="stage_05_prediction_analysis",
    selection=AssetSelection.groups("05_prediction_analysis"),
    description="Stage 05: Price predictions & analysis (dbt + Python)",
)

stage_06_job = define_asset_job(
    name="stage_06_property_summaries",
    selection=AssetSelection.groups("06_property_summaries"),
    description="Stage 06: Building summaries",
)

stage_07_job = define_asset_job(
    name="stage_07_points_of_study",
    selection=AssetSelection.groups("07_points_of_study"),
    description="Stage 07: Points of study & spatial smoothing (dbt + Python)",
)

stage_08_job = define_asset_job(
    name="stage_08_main_tables",
    selection=AssetSelection.groups("08_main_tables"),
    description="Stage 08: Final main tables",
)

stage_09_job = define_asset_job(
    name="stage_09_expanded_main_tables",
    selection=AssetSelection.groups("09_expanded_main_tables"),
    description="Stage 09: General statistics",
)

stage_10_job = define_asset_job(
    name="stage_10_business_potential",
    selection=AssetSelection.groups("10_business_potential"),
    description="Stage 10: Business potential analysis",
)

# Complete pipeline job - equivalent to running run_all.sh
complete_pipeline_job = define_asset_job(
    name="complete_real_estate_pipeline",
    selection=AssetSelection.all(),
    description="Complete end-to-end real estate data pipeline (equivalent to run_all.sh)",
)

# Optional: Define a schedule to run the complete pipeline
# Uncomment and adjust cron expression as needed
# daily_pipeline_schedule = ScheduleDefinition(
#     job=complete_pipeline_job,
#     cron_schedule="0 2 * * *",  # Run at 2 AM daily
#     name="daily_real_estate_pipeline",
# )

# Combine all definitions
defs = Definitions(
    assets=[
        # dbt assets
        dbt_assets.stage_01_raw_models,
        dbt_assets.stage_02_closest_buildings_dbt_models,
        dbt_assets.stage_03_staging_models,
        dbt_assets.stage_04_units_history_models,
        dbt_assets.stage_05_prediction_analysis_dbt_models,
        dbt_assets.stage_06_property_summaries_models,
        dbt_assets.stage_07_points_of_study_dbt_models,
        dbt_assets.stage_08_main_tables_models,
        dbt_assets.stage_09_expanded_main_tables_models,
        dbt_assets.stage_10_business_potential_models,
        # Python transformation assets
        python_transformations.stage_02_closest_buildings_python,
        python_transformations.stage_05_prediction_analysis_python,
        python_transformations.stage_07_spatial_smoothing_python,
    ],
    resources=resources,
    jobs=[
        stage_01_job,
        stage_02_job,
        stage_03_job,
        stage_04_job,
        stage_05_job,
        stage_06_job,
        stage_07_job,
        stage_08_job,
        stage_09_job,
        stage_10_job,
        complete_pipeline_job,
    ],
    # schedules=[daily_pipeline_schedule],  # Uncomment to enable scheduling
)
