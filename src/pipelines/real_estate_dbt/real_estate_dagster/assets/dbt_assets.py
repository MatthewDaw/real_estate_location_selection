"""
dbt assets for the real estate data pipeline.
These assets represent all the dbt models in the project, organized by layer.
"""
from pathlib import Path
from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets, DbtProject

# Path to the dbt project (parent directory of real_estate_dagster)
DBT_PROJECT_DIR = Path(__file__).parent.parent.parent

# Create dbt project instance
dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
    packaged_project_dir=DBT_PROJECT_DIR,
)

# Prepare dbt project - this builds manifest.json
dbt_project.prepare_if_dev()


@dbt_assets(
    manifest=dbt_project.manifest_path,
    project=dbt_project,
    name="stage_01_raw",
    group_name="01_raw",
)
def stage_01_raw_models(context: AssetExecutionContext, dbt: DbtCliResource):
    """Stage 01: Raw data models - source data with minimal transformation"""
    yield from dbt.cli(["build", "--select", "01_raw"], context=context).stream()


@dbt_assets(
    manifest=dbt_project.manifest_path,
    project=dbt_project,
    name="stage_02_closest_buildings_dbt",
    group_name="02_closest_buildings",
)
def stage_02_closest_buildings_dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    """Stage 02: Closest buildings dbt models - staging for proximity analysis"""
    yield from dbt.cli(["build", "--select", "02_closest_buildings"], context=context).stream()


@dbt_assets(
    manifest=dbt_project.manifest_path,
    project=dbt_project,
    name="stage_03_staging",
    group_name="03_staging",
)
def stage_03_staging_models(context: AssetExecutionContext, dbt: DbtCliResource):
    """Stage 03: Staging layer - cleaned and standardized data"""
    yield from dbt.cli(["build", "--select", "03_staging"], context=context).stream()


@dbt_assets(
    manifest=dbt_project.manifest_path,
    project=dbt_project,
    name="stage_04_units_history",
    group_name="04_units_history",
)
def stage_04_units_history_models(context: AssetExecutionContext, dbt: DbtCliResource):
    """Stage 04: Units historical analysis - time-series analysis of unit pricing"""
    yield from dbt.cli(["build", "--select", "04_units_history"], context=context).stream()


@dbt_assets(
    manifest=dbt_project.manifest_path,
    project=dbt_project,
    name="stage_05_prediction_analysis_dbt",
    group_name="05_prediction_analysis",
)
def stage_05_prediction_analysis_dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    """Stage 05: Prediction analysis dbt models - preparatory models for ML predictions"""
    yield from dbt.cli(["build", "--select", "05_prediction_analysis"], context=context).stream()


@dbt_assets(
    manifest=dbt_project.manifest_path,
    project=dbt_project,
    name="stage_06_property_summaries",
    group_name="06_property_summaries",
)
def stage_06_property_summaries_models(context: AssetExecutionContext, dbt: DbtCliResource):
    """Stage 06: Building summary metrics - aggregated building-level KPIs"""
    yield from dbt.cli(["build", "--select", "06_property_summaries"], context=context).stream()


@dbt_assets(
    manifest=dbt_project.manifest_path,
    project=dbt_project,
    name="stage_07_points_of_study_dbt",
    group_name="07_points_of_study",
)
def stage_07_points_of_study_dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    """Stage 07: Points of study dbt models - curated datasets for research"""
    yield from dbt.cli(["build", "--select", "07_points_of_study"], context=context).stream()


@dbt_assets(
    manifest=dbt_project.manifest_path,
    project=dbt_project,
    name="stage_08_main_tables",
    group_name="08_main_tables",
)
def stage_08_main_tables_models(context: AssetExecutionContext, dbt: DbtCliResource):
    """Stage 08: Final main tables - optimized for reporting and dashboards"""
    yield from dbt.cli(["build", "--select", "08_main_tables"], context=context).stream()


@dbt_assets(
    manifest=dbt_project.manifest_path,
    project=dbt_project,
    name="stage_09_expanded_main_tables",
    group_name="09_expanded_main_tables",
)
def stage_09_expanded_main_tables_models(context: AssetExecutionContext, dbt: DbtCliResource):
    """Stage 09: Expanded main tables - general statistics for profitability estimation"""
    yield from dbt.cli(["build", "--select", "09_expanded_main_tables"], context=context).stream()


@dbt_assets(
    manifest=dbt_project.manifest_path,
    project=dbt_project,
    name="stage_10_business_potential",
    group_name="10_business_potential",
)
def stage_10_business_potential_models(context: AssetExecutionContext, dbt: DbtCliResource):
    """Stage 10: Business potential analysis - final profitability tables"""
    yield from dbt.cli(["build", "--select", "10_business_potential"], context=context).stream()
