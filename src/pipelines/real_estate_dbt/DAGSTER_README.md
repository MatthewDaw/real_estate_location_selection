# Real Estate Dagster Pipeline

This directory contains a full Dagster implementation of the real estate data pipeline, combining dbt models with Python transformations.

## Project Structure

```
real_estate_dbt/
├── real_estate_dagster/          # Dagster code location
│   ├── __init__.py               # Package init, exports definitions
│   ├── definitions.py            # Main definitions file - wires everything together
│   ├── resources.py              # Database and other resources
│   └── assets/
│       ├── __init__.py
│       ├── dbt_assets.py         # dbt model assets (10 stages)
│       └── python_transformations.py  # Python transformation assets (stages 2, 5, 7)
├── models/                       # dbt models (unchanged)
├── scripts/                      # Python transformation scripts (unchanged)
├── dbt_project.yml              # dbt project config (unchanged)
├── pyproject.toml               # Python package and Dagster config
└── DAGSTER_README.md            # This file

```

## Pipeline Stages

The Dagster pipeline mirrors the execution flow from `run_all.sh`:

1. **Stage 01: Raw Models** - dbt only
2. **Stage 02: Closest Buildings** - dbt + Python (KDTree spatial indexing)
3. **Stage 03: Staging** - dbt only
4. **Stage 04: Units History** - dbt only
5. **Stage 05: Prediction Analysis** - dbt + Python (ML price forecasting)
6. **Stage 06: Property Summaries** - dbt only
7. **Stage 07: Points of Study** - dbt + Python (spatial smoothing)
8. **Stage 08: Main Tables** - dbt only
9. **Stage 09: Expanded Main Tables** - dbt only
10. **Stage 10: Business Potential** - dbt only

## Installation

1. Install the package in development mode:

```bash
cd /Users/matthewdaw/Documents/official_repos/real_estate_location_selection/src/pipelines/real_estate_dbt
pip install -e ".[dev]"
```

2. Ensure your dbt profile is configured:
   - The pipeline reads database connection from `~/.dbt/profiles.yml`
   - Profile name: `my_real_estate_project`
   - Target: `dev`

3. Prepare the dbt manifest:

```bash
dbt deps  # Install dbt packages
dbt parse  # Generate manifest.json
```

## Running the Pipeline

### Option 1: Dagster UI (Recommended)

Start the Dagster development server:

```bash
dagster dev -f real_estate_dagster/definitions.py
```

Then open http://localhost:3000 in your browser.

From the UI, you can:
- **Materialize all assets** - Equivalent to running `run_all.sh`
- **Materialize specific stages** - Run individual jobs like `stage_01_raw`, `stage_05_prediction_analysis`, etc.
- View asset lineage graph
- Monitor execution progress
- View logs and metadata

### Option 2: CLI

Run the complete pipeline:

```bash
dagster job execute -f real_estate_dagster/definitions.py -j complete_real_estate_pipeline
```

Run a specific stage:

```bash
dagster job execute -f real_estate_dagster/definitions.py -j stage_05_prediction_analysis
```

### Option 3: Python API

```python
from dagster import build_asset_context
from real_estate_dagster import defs

# Materialize all assets
result = defs.get_job_def("complete_real_estate_pipeline").execute_in_process()

# Or materialize a specific stage
result = defs.get_job_def("stage_02_closest_buildings").execute_in_process()
```

## Available Jobs

- `stage_01_raw` - Raw data models
- `stage_02_closest_buildings` - Closest buildings (dbt + Python)
- `stage_03_staging` - Staging transformations
- `stage_04_units_history` - Units history
- `stage_05_prediction_analysis` - Price predictions (dbt + Python)
- `stage_06_property_summaries` - Property summaries
- `stage_07_points_of_study` - Spatial smoothing (dbt + Python)
- `stage_08_main_tables` - Main tables
- `stage_09_expanded_main_tables` - Expanded tables
- `stage_10_business_potential` - Business potential
- **`complete_real_estate_pipeline`** - Full pipeline (equivalent to `run_all.sh`)

## Asset Groups

Assets are organized by stage in groups:
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

## Dagster Build vs run_all.sh

Running a **Dagster build** of all assets is equivalent to running `run_all.sh`:

| run_all.sh | Dagster Equivalent |
|------------|-------------------|
| `./01_run_raw.sh` | `stage_01_raw` job |
| `./02_run_closest_buildings.sh` | `stage_02_closest_buildings` job |
| `./03_run_staging.sh` | `stage_03_staging` job |
| `./04_run_units_history.sh` | `stage_04_units_history` job |
| `./05_run_prediction_analysis.sh` | `stage_05_prediction_analysis` job |
| `./06_run_building_summaries.sh` | `stage_06_property_summaries` job |
| `./07_run_points_of_study.sh` | `stage_07_points_of_study` job |
| `./08_run_main_tables.sh` | `stage_08_main_tables` job |
| `./09_run_general_stats.sh` | `stage_09_expanded_main_tables` job |
| `./10_run_business_potential.sh` | `stage_10_business_potential` job |
| `dbt test` | Included in dbt build within each stage |
| **Full pipeline** | `complete_real_estate_pipeline` job or "Materialize all" in UI |

## Benefits of Dagster

1. **Dependency Management**: Dagster automatically handles dependencies between stages
2. **Incremental Execution**: Only re-run what's needed when data changes
3. **Observability**: View pipeline execution in real-time with detailed logging
4. **Scheduling**: Schedule pipeline runs (uncomment schedule in definitions.py)
5. **Metadata & Lineage**: Track data lineage and asset metadata
6. **Error Handling**: Better error messages and retry capabilities
7. **Parallelization**: Dagster can run independent stages in parallel
8. **Testing**: Easy to test individual assets and stages

## Development

### Adding New Assets

1. **dbt models**: Add to appropriate `models/XX_*` directory, they'll be automatically included
2. **Python transformations**: Add to `assets/python_transformations.py`

### Modifying Existing Assets

Edit the relevant files:
- dbt models: Edit `.sql` files in `models/`
- Python: Edit `assets/python_transformations.py`
- Resources: Edit `resources.py`

After changes, reload the Dagster UI or restart `dagster dev`.

## Troubleshooting

### "Module not found" errors
Ensure you've installed the package:
```bash
pip install -e ".[dev]"
```

### dbt manifest errors
Regenerate the manifest:
```bash
dbt deps && dbt parse
```

### Database connection errors
Verify your dbt profile is correct:
```bash
dbt debug
```

### Python script import errors
The Python transformation assets load scripts using `importlib.util` from the `scripts/` directory. Ensure all required dependencies are installed:
```bash
pip install -e ".[dev]"
```

## Production Deployment

For production deployment, consider:

1. **Dagster Cloud**: Deploy to Dagster Cloud for managed infrastructure
2. **Docker**: Containerize the application
3. **Kubernetes**: Deploy with dagster-k8s
4. **Scheduling**: Enable the daily schedule or use Dagster sensors

See [Dagster deployment docs](https://docs.dagster.io/deployment) for more information.
