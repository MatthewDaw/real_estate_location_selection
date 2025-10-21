"""
Example usage of the real estate Dagster pipeline.

This shows how to programmatically execute the pipeline or individual stages.
"""

from real_estate_dagster import defs

def run_complete_pipeline():
    """Run the complete end-to-end pipeline (equivalent to run_all.sh)"""
    print("Running complete real estate pipeline...")

    job = defs.get_job_def("complete_real_estate_pipeline")
    result = job.execute_in_process()

    if result.success:
        print("✓ Pipeline completed successfully!")
    else:
        print("✗ Pipeline failed")

    return result


def run_specific_stage(stage_name: str):
    """
    Run a specific pipeline stage.

    Args:
        stage_name: One of:
            - stage_01_raw
            - stage_02_closest_buildings
            - stage_03_staging
            - stage_04_units_history
            - stage_05_prediction_analysis
            - stage_06_property_summaries
            - stage_07_points_of_study
            - stage_08_main_tables
            - stage_09_expanded_main_tables
            - stage_10_business_potential
    """
    print(f"Running {stage_name}...")

    job = defs.get_job_def(stage_name)
    result = job.execute_in_process()

    if result.success:
        print(f"✓ {stage_name} completed successfully!")
    else:
        print(f"✗ {stage_name} failed")

    return result


def get_asset_metadata(result):
    """Extract metadata from execution result"""
    for event in result.all_events:
        if event.event_type_value == "ASSET_MATERIALIZATION":
            print(f"\nAsset: {event.asset_key}")
            if event.step_materialization_data.materialization.metadata:
                for key, value in event.step_materialization_data.materialization.metadata.items():
                    print(f"  {key}: {value.value}")


if __name__ == "__main__":
    # Example 1: Run the complete pipeline
    # result = run_complete_pipeline()

    # Example 2: Run just the prediction analysis stage
    result = run_specific_stage("stage_05_prediction_analysis")

    # Example 3: Get metadata from the execution
    get_asset_metadata(result)

    print("\nFor interactive use, run: dagster dev -f real_estate_dagster/definitions.py")
