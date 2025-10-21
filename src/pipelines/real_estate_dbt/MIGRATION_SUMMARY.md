# Dagster Migration Summary

## Overview

The `real_estate_dbt` project has been refactored into a full Dagster project. Running **`dagster build`** of all assets is now equivalent to running **`run_all.sh`**.

## What Was Created

### Core Dagster Files

1. **`real_estate_dagster/__init__.py`**
   - Package entry point
   - Exports `defs` (Dagster Definitions)

2. **`real_estate_dagster/definitions.py`**
   - Main definitions file
   - Defines all jobs (one per stage + complete pipeline job)
   - Wires together assets and resources
   - Contains commented-out schedule for automation

3. **`real_estate_dagster/resources.py`**
   - `DatabaseResource` class
   - Reads database config from dbt profiles.yml
   - Provides connection strings to assets

4. **`real_estate_dagster/assets/dbt_assets.py`**
   - 10 dbt asset definitions (one per stage)
   - Each corresponds to a folder in `models/`
   - Organized by asset groups

5. **`real_estate_dagster/assets/python_transformations.py`**
   - 3 Python transformation assets:
     - `stage_02_closest_buildings_python` - KDTree spatial indexing
     - `stage_05_prediction_analysis_python` - ML price forecasting
     - `stage_07_spatial_smoothing_python` - Distance-weighted spatial averaging
   - Loads and executes existing Python scripts from `scripts/` directory

6. **`real_estate_dagster/assets/__init__.py`**
   - Asset package init
   - Exports all assets

### Configuration Files

7. **`pyproject.toml`**
   - Python package configuration
   - Dagster dependencies (dagster, dagster-dbt, dagster-postgres)
   - All existing dependencies (pandas, numpy, scipy, sklearn, prophet, etc.)
   - Dev dependencies (dagster-webserver, pytest)

8. **`setup.py`**
   - Minimal setup.py for editable install

9. **`.gitignore`** (updated)
   - Added Python build artifacts
   - Added Dagster-specific ignores

### Documentation

10. **`DAGSTER_README.md`**
    - Complete setup and usage guide
    - Pipeline stage descriptions
    - Installation instructions
    - Running options (UI, CLI, Python API)
    - Comparison table: `run_all.sh` vs Dagster
    - Troubleshooting guide

11. **`MIGRATION_SUMMARY.md`** (this file)
    - Migration overview and details

12. **`dagster_quickstart.sh`**
    - Automated setup script
    - Installs package, dbt deps, generates manifest
    - Checks dbt connection

## Pipeline Structure

### Stage-to-Job Mapping

Each stage from `run_all.sh` now has a corresponding Dagster job:

| Shell Script | Dagster Job | Assets Included |
|--------------|-------------|-----------------|
| `01_run_raw.sh` | `stage_01_raw` | dbt models in `01_raw/` |
| `02_run_closest_buildings.sh` | `stage_02_closest_buildings` | dbt models in `02_closest_buildings/` + Python KDTree calculation |
| `03_run_staging.sh` | `stage_03_staging` | dbt models in `03_staging/` |
| `04_run_units_history.sh` | `stage_04_units_history` | dbt models in `04_units_history/` |
| `05_run_prediction_analysis.sh` | `stage_05_prediction_analysis` | dbt models in `05_prediction_analysis/` + Python ML pipeline |
| `06_run_building_summaries.sh` | `stage_06_property_summaries` | dbt models in `06_property_summaries/` |
| `07_run_points_of_study.sh` | `stage_07_points_of_study` | dbt models in `07_points_of_study/` + Python spatial smoothing |
| `08_run_main_tables.sh` | `stage_08_main_tables` | dbt models in `08_main_tables/` |
| `09_run_general_stats.sh` | `stage_09_expanded_main_tables` | dbt models in `09_expanded_main_tables/` |
| `10_run_business_potential.sh` | `stage_10_business_potential` | dbt models in `10_business_potential/` |
| **`run_all.sh` (complete)** | **`complete_real_estate_pipeline`** | **All assets above** |

### Asset Groups

Assets are organized into 10 groups matching the pipeline stages:
- `01_raw`
- `02_closest_buildings`
- `03_staging`
- `04_units_history`
- `05_prediction_analysis`
- `06_property_summaries`
- `07_points_of_study`
- `08_main_tables`
- `09_expanded_main_tables`
- `10_business_potential`

## Dependencies

The Dagster pipeline automatically manages dependencies:

```
01_raw
  ↓
02_closest_buildings (dbt) → 02_closest_buildings (Python)
  ↓
03_staging
  ↓
04_units_history
  ↓
05_prediction_analysis (dbt) → 05_prediction_analysis (Python)
  ↓
06_property_summaries
  ↓
07_points_of_study (dbt) → 07_spatial_smoothing (Python)
  ↓
08_main_tables
  ↓
09_expanded_main_tables
  ↓
10_business_potential
```

Python assets depend on their corresponding dbt assets to run first.

## What Didn't Change

1. **dbt models** - All `.sql` files in `models/` remain unchanged
2. **Python scripts** - All files in `scripts/` remain unchanged
3. **dbt configuration** - `dbt_project.yml`, `properties.yml`, etc. remain unchanged
4. **Seeds, macros, tests** - All dbt project components remain unchanged
5. **Shell scripts** - Original `.sh` scripts remain for backward compatibility

## Getting Started

### Quick Start

```bash
cd /Users/matthewdaw/Documents/official_repos/real_estate_location_selection/src/pipelines/real_estate_dbt
./dagster_quickstart.sh
```

This will:
1. Install the Python package
2. Install dbt packages
3. Generate the dbt manifest
4. Verify dbt connection

### Run the Pipeline

**Option 1: Dagster UI (Recommended)**
```bash
dagster dev -f real_estate_dagster/definitions.py
```
Then open http://localhost:3000 and click "Materialize all" for the complete pipeline.

**Option 2: CLI**
```bash
# Complete pipeline
dagster job execute -f real_estate_dagster/definitions.py -j complete_real_estate_pipeline

# Individual stage
dagster job execute -f real_estate_dagster/definitions.py -j stage_05_prediction_analysis
```

**Option 3: Keep using shell scripts**
```bash
./run_all.sh  # Still works!
```

## Key Benefits

### 1. Dependency Management
Dagster automatically handles the execution order. You can't accidentally run stage 05 before stage 01.

### 2. Incremental Execution
Only re-run what's needed when data changes. If stage 03 data hasn't changed, stages 04-10 won't re-run unnecessarily.

### 3. Observability
- Real-time execution monitoring in the UI
- Detailed logs for each asset
- Execution history and timing
- Data lineage visualization

### 4. Metadata Tracking
Each asset tracks:
- Number of records processed
- Execution time
- Success/failure status
- Custom metadata (e.g., "zillow_properties: 1234")

### 5. Parallelization
Independent stages can run in parallel (e.g., if you have multiple independent analysis steps).

### 6. Scheduling
Uncomment the schedule in `definitions.py` to run the pipeline automatically (e.g., daily at 2 AM).

### 7. Error Handling
- Better error messages
- Automatic retry capabilities
- Ability to re-run failed assets only

### 8. Testing
Easy to test individual assets in isolation.

## Migration Strategy

You can migrate gradually:

1. **Phase 1**: Keep using `run_all.sh` while getting familiar with Dagster
2. **Phase 2**: Start using Dagster UI to run individual stages for development
3. **Phase 3**: Use Dagster for complete pipeline runs
4. **Phase 4**: Set up scheduling and automation
5. **Phase 5**: Decommission shell scripts (optional)

## Backward Compatibility

The shell scripts (`run_all.sh`, `01_run_raw.sh`, etc.) continue to work exactly as before. Nothing breaks.

## Next Steps

1. Run the quickstart script
2. Explore the Dagster UI
3. Try materializing individual stages
4. Run the complete pipeline
5. Review logs and metadata
6. Consider enabling scheduling

## Questions or Issues?

See `DAGSTER_README.md` for detailed documentation and troubleshooting.
