"""
Python transformation assets for the real estate data pipeline.
These assets run Python scripts between dbt model layers for complex spatial and ML operations.
"""
import sys
from pathlib import Path
from dagster import asset, AssetExecutionContext, Output
import pandas as pd
import numpy as np
from scipy.spatial import cKDTree
from sqlalchemy import create_engine, text
import yaml
import logging

# Add scripts directory to path
SCRIPTS_DIR = Path(__file__).parent.parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

# Import helper functions from scripts
# Note: We import the actual script modules directly
import importlib.util

# Load the price prediction module
spec_05 = importlib.util.spec_from_file_location("price_prediction_module", SCRIPTS_DIR / "05_price_prediction_growth.py")
price_prediction_module = importlib.util.module_from_spec(spec_05)
spec_05.loader.exec_module(price_prediction_module)
UnifiedPriceForecastingPipeline = price_prediction_module.UnifiedPriceForecastingPipeline

# Load the spatial smoothing module
spec_07 = importlib.util.spec_from_file_location("spatial_smoothing_module", SCRIPTS_DIR / "07_precompute_spatial_smoothing.py")
spatial_smoothing_module = importlib.util.module_from_spec(spec_07)
spec_07.loader.exec_module(spatial_smoothing_module)
precompute_weights = spatial_smoothing_module.precompute_weights
compute_all_weighted_metrics = spatial_smoothing_module.compute_all_weighted_metrics

logger = logging.getLogger(__name__)


def get_database_url_from_resource(context: AssetExecutionContext) -> str:
    """Get database URL from Dagster resource or dbt profiles"""
    # Try to get from Dagster resource first
    if hasattr(context.resources, 'database'):
        return context.resources.database.get_connection_string()

    # Fall back to dbt profiles
    profiles_paths = [
        Path.home() / '.dbt' / 'profiles.yml',
        Path('profiles.yml'),
        Path('~/.dbt/profiles.yml').expanduser()
    ]

    for path in profiles_paths:
        if path.exists():
            try:
                with open(path, 'r') as f:
                    profiles = yaml.safe_load(f)

                profile_name = 'my_real_estate_project'
                target = 'dev'

                if profile_name in profiles:
                    profile = profiles[profile_name]
                    if 'outputs' in profile and target in profile['outputs']:
                        output = profile['outputs'][target]

                        if output.get('type') == 'postgres':
                            host = output.get('host', 'localhost')
                            port = output.get('port', 5432)
                            user = output.get('user')
                            password = output.get('password', '')
                            dbname = output.get('dbname')

                            return f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
            except Exception as e:
                logger.warning(f"Could not read dbt profiles from {path}: {e}")
                continue

    raise ValueError("No database connection configuration found")


@asset(
    name="stage_02_closest_buildings_python",
    group_name="02_closest_buildings",
    deps=["stage_02_closest_buildings_dbt"],
    description="Calculate closest buildings for all properties using KDTree spatial indexing",
)
def stage_02_closest_buildings_python(context: AssetExecutionContext) -> Output[dict]:
    """
    Stage 02 Python Transformation: Closest Buildings Calculation

    Uses KDTree for efficient spatial proximity analysis to find the nearest building
    for each property (Zillow, Landwatch, and manual collected properties).
    """
    context.log.info("Starting closest buildings calculation...")

    db_url = get_database_url_from_resource(context)
    engine = create_engine(db_url)

    try:
        # Load buildings data
        context.log.info("Loading buildings data...")
        buildings_df = pd.read_sql("""
            SELECT
                id::text as id,
                msa,
                zip_code,
                lon,
                lat
            FROM stg_buildings
            WHERE geog_point IS NOT NULL
        """, engine)

        context.log.info(f"Loaded {len(buildings_df)} buildings")

        # Build KDTree
        buildings_coords = buildings_df[['lon', 'lat']].values
        tree = cKDTree(buildings_coords)

        all_results = []

        # Process Zillow properties
        context.log.info("Processing Zillow properties...")
        zillow_df = pd.read_sql("""
            SELECT id::text, 'zillow' as source, longitude as lon, latitude as lat
            FROM raw_zillow
            WHERE geog_point IS NOT NULL
        """, engine)

        if len(zillow_df) > 0:
            zillow_df = zillow_df.drop_duplicates(subset=['id'], keep='first')
            zillow_coords = zillow_df[['lon', 'lat']].values
            distances, indices = tree.query(zillow_coords, k=1)

            zillow_results = pd.DataFrame({
                'property_id': zillow_df['id'],
                'source': 'zillow',
                'closest_building_id': buildings_df.iloc[indices]['id'].values,
                'msa': buildings_df.iloc[indices]['msa'].values,
                'zip_code': buildings_df.iloc[indices]['zip_code'].values,
                'distance_to_closest_building': distances * 111000
            })
            all_results.append(zillow_results)
            context.log.info(f"Processed {len(zillow_results)} Zillow properties")

        # Process Landwatch properties
        context.log.info("Processing Landwatch properties...")
        landwatch_df = pd.read_sql("""
            SELECT id::text, 'landwatch' as source, longitude as lon, latitude as lat
            FROM raw_landwatch
            WHERE geog_point IS NOT NULL
        """, engine)

        if len(landwatch_df) > 0:
            landwatch_df = landwatch_df.drop_duplicates(subset=['id'], keep='first')
            landwatch_coords = landwatch_df[['lon', 'lat']].values
            distances, indices = tree.query(landwatch_coords, k=1)

            landwatch_results = pd.DataFrame({
                'property_id': landwatch_df['id'],
                'source': 'landwatch',
                'closest_building_id': buildings_df.iloc[indices]['id'].values,
                'msa': buildings_df.iloc[indices]['msa'].values,
                'zip_code': buildings_df.iloc[indices]['zip_code'].values,
                'distance_to_closest_building': distances * 111000
            })
            all_results.append(landwatch_results)
            context.log.info(f"Processed {len(landwatch_results)} Landwatch properties")

        # Process Manual Collected Properties
        context.log.info("Processing Manual Collected Properties...")
        manual_df = pd.read_sql("""
            SELECT id::text, 'manual' as source, lon, lat
            FROM raw_manual_collected_properties
            WHERE geog_point IS NOT NULL
        """, engine)

        if len(manual_df) > 0:
            manual_df = manual_df.drop_duplicates(subset=['id'], keep='first')
            manual_coords = manual_df[['lon', 'lat']].values
            distances, indices = tree.query(manual_coords, k=1)

            manual_results = pd.DataFrame({
                'property_id': manual_df['id'],
                'source': 'manual',
                'closest_building_id': buildings_df.iloc[indices]['id'].values,
                'msa': buildings_df.iloc[indices]['msa'].values,
                'zip_code': buildings_df.iloc[indices]['zip_code'].values,
                'distance_to_closest_building': distances * 111000
            })
            all_results.append(manual_results)
            context.log.info(f"Processed {len(manual_results)} Manual Collected Properties")

        # Combine and write results
        if all_results:
            final_results = pd.concat(all_results, ignore_index=True)
            final_results = final_results.drop_duplicates(subset=['source', 'property_id'], keep='first')

            context.log.info(f"Writing {len(final_results)} records to closest_buildings_lookup...")

            with engine.connect() as conn:
                conn.execute(text("TRUNCATE TABLE closest_buildings_lookup"))
                conn.commit()

            final_results.to_sql('closest_buildings_lookup', engine, if_exists='append', index=False, method='multi')

            context.log.info(f"Successfully processed {len(final_results)} total properties")

            return Output(
                value={"total_properties": len(final_results), "zillow": len(zillow_results) if len(zillow_df) > 0 else 0,
                       "landwatch": len(landwatch_results) if len(landwatch_df) > 0 else 0,
                       "manual": len(manual_results) if len(manual_df) > 0 else 0},
                metadata={
                    "total_properties": len(final_results),
                    "zillow_properties": len(zillow_results) if len(zillow_df) > 0 else 0,
                    "landwatch_properties": len(landwatch_results) if len(landwatch_df) > 0 else 0,
                    "manual_properties": len(manual_results) if len(manual_df) > 0 else 0,
                }
            )
        else:
            context.log.warning("No properties found to process")
            return Output(value={"total_properties": 0})

    except Exception as e:
        context.log.error(f"Error in closest buildings calculation: {e}")
        raise


@asset(
    name="stage_05_prediction_analysis_python",
    group_name="05_prediction_analysis",
    deps=["stage_05_prediction_analysis_dbt"],
    description="Run ML price prediction models for buildings and Zillow properties with geographic analysis",
)
def stage_05_prediction_analysis_python(context: AssetExecutionContext) -> Output[dict]:
    """
    Stage 05 Python Transformation: Price Prediction & Analysis

    Runs comprehensive ML forecasting pipeline for:
    - Building rental prices (individual and geographic)
    - Zillow property prices (individual and geographic)

    Uses linear regression with confidence intervals for predictions.
    """
    context.log.info("Starting price prediction and forecasting pipeline...")

    db_url = get_database_url_from_resource(context)

    try:
        # Initialize the forecasting pipeline
        pipeline = UnifiedPriceForecastingPipeline(db_url=db_url)

        # Run building pipeline
        context.log.info("Running building price forecasting...")
        building_success = pipeline.run_building_pipeline(forecast_days=28)

        # Run Zillow pipeline
        context.log.info("Running Zillow price forecasting...")
        zillow_success = pipeline.run_zillow_pipeline(prediction_days=365)

        # Run geographic building pipeline
        context.log.info("Running geographic building forecasting...")
        geographic_success = pipeline.run_geographic_pipeline(forecast_days=365)

        # Run Zillow geographic pipeline
        context.log.info("Running Zillow geographic forecasting...")
        zillow_geographic_success = pipeline.run_zillow_geographic_pipeline(forecast_days=365)

        overall_success = building_success and zillow_success and geographic_success and zillow_geographic_success

        context.log.info(f"Pipeline completed. Overall success: {overall_success}")
        context.log.info(f"  - Building pipeline: {'✓' if building_success else '✗'}")
        context.log.info(f"  - Zillow pipeline: {'✓' if zillow_success else '✗'}")
        context.log.info(f"  - Building geographic pipeline: {'✓' if geographic_success else '✗'}")
        context.log.info(f"  - Zillow geographic pipeline: {'✓' if zillow_geographic_success else '✗'}")

        return Output(
            value={
                "building_success": building_success,
                "zillow_success": zillow_success,
                "geographic_success": geographic_success,
                "zillow_geographic_success": zillow_geographic_success,
                "overall_success": overall_success,
            },
            metadata={
                "building_pipeline": "success" if building_success else "failed",
                "zillow_pipeline": "success" if zillow_success else "failed",
                "building_geographic_pipeline": "success" if geographic_success else "failed",
                "zillow_geographic_pipeline": "success" if zillow_geographic_success else "failed",
            }
        )

    except Exception as e:
        context.log.error(f"Error in price prediction pipeline: {e}")
        raise


@asset(
    name="stage_07_spatial_smoothing_python",
    group_name="07_points_of_study",
    deps=["stage_07_points_of_study_dbt"],
    description="Precompute spatial smoothing values using KDTree with distance-weighted averaging",
)
def stage_07_spatial_smoothing_python(context: AssetExecutionContext) -> Output[dict]:
    """
    Stage 07 Python Transformation: Spatial Smoothing

    Pre-computes distance-weighted spatial averages for all points of interest using:
    - KDTree for efficient spatial indexing
    - Haversine distance for accurate geographic calculations
    - Exponential decay weights based on distance

    Computes smoothed metrics from nearby:
    - HD buildings (rental data)
    - Zillow properties (sale data)
    - Empty lots
    - Developed properties
    """
    context.log.info("Starting spatial smoothing computation...")

    db_url = get_database_url_from_resource(context)
    engine = create_engine(db_url)

    # Configuration
    radius_miles = 5.0
    radius_meters = radius_miles * 1609.34
    decay_factor = 4.4
    output_table = 'smoothed_values'

    try:
        from haversine import haversine_vector, Unit

        # Load points of interest
        context.log.info("Loading points of interest...")
        poi_df = pd.read_sql("SELECT source, source_id, lon, lat FROM points_of_interest", engine)

        if len(poi_df) == 0:
            context.log.warning("No points of interest found")
            return Output(value={"points_processed": 0})

        # Load all data sources for smoothing
        context.log.info("Loading building summary data...")
        buildings_df = pd.read_sql("""
            SELECT building_id, lon, lat,
                   predicted_current_price_per_sqft, predicted_current_price_lower_per_sqft,
                   predicted_current_price_upper_per_sqft, avg_rent_price_per_sqft,
                   predicted_current_effective_price_per_sqft, predicted_current_effective_price_lower_per_sqft,
                   predicted_current_effective_price_upper_per_sqft, avg_effective_rent_price_per_sqft,
                   predicted_future_price_per_sqft, predicted_future_price_lower_per_sqft,
                   predicted_future_price_upper_per_sqft,
                   predicted_future_effective_price_per_sqft, predicted_future_effective_price_lower_per_sqft,
                   predicted_future_effective_price_upper_per_sqft,
                   predicted_current_price_per_bed, predicted_current_price_lower_per_bed,
                   predicted_current_price_upper_per_bed, avg_rent_price_per_bed,
                   predicted_current_effective_price_per_bed, predicted_current_effective_price_lower_per_bed,
                   predicted_current_effective_price_upper_per_bed, avg_effective_rent_price_per_bed,
                   predicted_future_price_per_bed, predicted_future_price_lower_per_bed,
                   predicted_future_price_upper_per_bed,
                   predicted_future_effective_price_per_bed, predicted_future_effective_price_lower_per_bed,
                   predicted_future_effective_price_upper_per_bed,
                   avg_beds, avg_baths, avg_sqft, avg_price, avg_effective_price, num_units,
                   leased_percentage, exposure_percentage, concession_percentage
            FROM hd_building_summary
        """, engine)

        context.log.info("Loading building analysis data...")
        building_analysis_df = pd.read_sql("""
            SELECT ba.building_id, b.lon, b.lat,
                   ba.average_percent_gain_per_year, ba.average_percent_gain_per_year_effective
            FROM building_analysis ba
            JOIN stg_buildings b ON ba.building_id = b.id
        """, engine)

        context.log.info("Loading Zillow data...")
        zillow_df = pd.read_sql("""
            SELECT id, lon, lat,
                   predicted_current_price_per_bedroom, predicted_current_price_lower_per_bedroom,
                   predicted_current_price_upper_per_bedroom,
                   predicted_current_price_per_sqft, predicted_current_price_lower_per_sqft,
                   predicted_current_price_upper_per_sqft,
                   predicted_future_price_per_bedroom, predicted_future_price_lower_per_bedroom,
                   predicted_future_price_upper_per_bedroom,
                   predicted_future_price_per_sqft, predicted_future_price_lower_per_sqft,
                   predicted_future_price_upper_per_sqft,
                   price_per_bedroom, price_per_sqft,
                   current_price, bedrooms, living_area_sqft
            FROM zillow_building_summary
            WHERE lon IS NOT NULL AND lat IS NOT NULL
        """, engine)

        context.log.info("Loading Zillow analysis data...")
        zillow_analysis_df = pd.read_sql("""
            SELECT za.id, z.lon, z.lat,
                   za.average_percent_gain_per_year, za.trend_strength_pct,
                   za.trend_variance_pct, za.data_span_days
            FROM zillow_analysis za
            JOIN stg_zillow z ON za.id = z.id
            WHERE z.lon IS NOT NULL AND z.lat IS NOT NULL
        """, engine)

        context.log.info("Loading empty lot data...")
        empty_lots_df = pd.read_sql("""
            SELECT source, source_id, lon, lat, lot_area_value_sqft, price,
                   CASE WHEN lot_area_value_sqft > 0 THEN price / lot_area_value_sqft ELSE NULL END as cost_per_sqft
            FROM empty_lot_summaries
            WHERE lon IS NOT NULL AND lat IS NOT NULL AND lot_area_value_sqft > 0
        """, engine)

        context.log.info("Loading developed properties data...")
        developed_properties_df = pd.read_sql("""
            SELECT source, source_id, lon, lat, lot_area_value_sqft, interior_sqft, price,
                   CASE WHEN interior_sqft > 0 THEN price / interior_sqft ELSE NULL END as cost_per_interior_sqft,
                   CASE WHEN lot_area_value_sqft > 0 THEN price / lot_area_value_sqft ELSE NULL END as cost_per_lot_sqft
            FROM developed_properties_summaries
            WHERE lon IS NOT NULL AND lat IS NOT NULL AND (interior_sqft > 0 OR lot_area_value_sqft > 0)
        """, engine)

        context.log.info(f"Loaded {len(poi_df)} POIs, {len(buildings_df)} buildings, "
                        f"{len(building_analysis_df)} building analyses, {len(zillow_df)} Zillow buildings, "
                        f"{len(zillow_analysis_df)} Zillow analyses, {len(empty_lots_df)} empty lots, "
                        f"{len(developed_properties_df)} developed properties")

        # Build KDTrees
        trees = {}
        if len(buildings_df) > 0:
            trees['buildings'] = cKDTree(buildings_df[['lon', 'lat']].values)
        if len(building_analysis_df) > 0:
            trees['building_analysis'] = cKDTree(building_analysis_df[['lon', 'lat']].values)
        if len(zillow_df) > 0:
            trees['zillow'] = cKDTree(zillow_df[['lon', 'lat']].values)
        if len(zillow_analysis_df) > 0:
            trees['zillow_analysis'] = cKDTree(zillow_analysis_df[['lon', 'lat']].values)
        if len(empty_lots_df) > 0:
            trees['empty_lots'] = cKDTree(empty_lots_df[['lon', 'lat']].values)
        if len(developed_properties_df) > 0:
            trees['developed_properties'] = cKDTree(developed_properties_df[['lon', 'lat']].values)

        radius_degrees = (radius_miles / 69.0) * 1.2
        results = []

        context.log.info("Computing distance-weighted spatial smoothing...")
        for idx, row in poi_df.iterrows():
            if idx % 1000 == 0:
                context.log.info(f"Processed {idx}/{len(poi_df)} points")

            poi_coord = [row['lon'], row['lat']]
            poi_lat_lon = (row['lat'], row['lon'])

            result = {'source': row['source'], 'source_id': row['source_id']}

            # Find nearby data for each source
            nearby_buildings = pd.DataFrame()
            if 'buildings' in trees:
                building_indices = trees['buildings'].query_ball_point(poi_coord, radius_degrees)
                if building_indices:
                    candidates = buildings_df.iloc[building_indices]
                    candidate_points = list(zip(candidates['lat'], candidates['lon']))
                    distances = haversine_vector([poi_lat_lon] * len(candidate_points), candidate_points, Unit.METERS)
                    within_radius = distances <= radius_meters
                    if within_radius.any():
                        nearby_buildings = candidates[within_radius].copy()

            nearby_building_analysis = pd.DataFrame()
            if 'building_analysis' in trees:
                ba_indices = trees['building_analysis'].query_ball_point(poi_coord, radius_degrees)
                if ba_indices:
                    candidates = building_analysis_df.iloc[ba_indices]
                    candidate_points = list(zip(candidates['lat'], candidates['lon']))
                    distances = haversine_vector([poi_lat_lon] * len(candidate_points), candidate_points, Unit.METERS)
                    within_radius = distances <= radius_meters
                    if within_radius.any():
                        nearby_building_analysis = candidates[within_radius].copy()

            nearby_zillow = pd.DataFrame()
            if 'zillow' in trees:
                zillow_indices = trees['zillow'].query_ball_point(poi_coord, radius_degrees)
                if zillow_indices:
                    candidates = zillow_df.iloc[zillow_indices]
                    candidate_points = list(zip(candidates['lat'], candidates['lon']))
                    distances = haversine_vector([poi_lat_lon] * len(candidate_points), candidate_points, Unit.METERS)
                    within_radius = distances <= radius_meters
                    if within_radius.any():
                        nearby_zillow = candidates[within_radius].copy()

            nearby_zillow_analysis = pd.DataFrame()
            if 'zillow_analysis' in trees:
                za_indices = trees['zillow_analysis'].query_ball_point(poi_coord, radius_degrees)
                if za_indices:
                    candidates = zillow_analysis_df.iloc[za_indices]
                    candidate_points = list(zip(candidates['lat'], candidates['lon']))
                    distances = haversine_vector([poi_lat_lon] * len(candidate_points), candidate_points, Unit.METERS)
                    within_radius = distances <= radius_meters
                    if within_radius.any():
                        nearby_zillow_analysis = candidates[within_radius].copy()

            nearby_empty_lots = pd.DataFrame()
            if 'empty_lots' in trees:
                el_indices = trees['empty_lots'].query_ball_point(poi_coord, radius_degrees)
                if el_indices:
                    candidates = empty_lots_df.iloc[el_indices]
                    candidate_points = list(zip(candidates['lat'], candidates['lon']))
                    distances = haversine_vector([poi_lat_lon] * len(candidate_points), candidate_points, Unit.METERS)
                    within_radius = distances <= radius_meters
                    if within_radius.any():
                        nearby_empty_lots = candidates[within_radius].copy()

            nearby_developed_properties = pd.DataFrame()
            if 'developed_properties' in trees:
                dp_indices = trees['developed_properties'].query_ball_point(poi_coord, radius_degrees)
                if dp_indices:
                    candidates = developed_properties_df.iloc[dp_indices]
                    candidate_points = list(zip(candidates['lat'], candidates['lon']))
                    distances = haversine_vector([poi_lat_lon] * len(candidate_points), candidate_points, Unit.METERS)
                    within_radius = distances <= radius_meters
                    if within_radius.any():
                        nearby_developed_properties = candidates[within_radius].copy()

            # Compute weighted metrics
            metrics = compute_all_weighted_metrics(
                nearby_buildings, nearby_zillow, nearby_building_analysis, nearby_zillow_analysis,
                nearby_empty_lots, nearby_developed_properties,
                poi_coord, decay_factor, radius_meters
            )
            result.update(metrics)
            results.append(result)

        # Create results DataFrame
        results_df = pd.DataFrame(results)
        results_df['created_at'] = pd.Timestamp.now()

        # Write to database
        context.log.info(f"Writing {len(results_df)} records to {output_table}...")

        with engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {output_table} CASCADE"))
            conn.commit()

        results_df.to_sql(output_table, engine, if_exists='replace', index=False, method='multi')

        context.log.info(f"Successfully created {output_table} with {len(results_df)} records")

        return Output(
            value={"points_processed": len(results_df)},
            metadata={
                "points_processed": len(results_df),
                "radius_miles": radius_miles,
                "decay_factor": decay_factor,
            }
        )

    except Exception as e:
        context.log.error(f"Error in spatial smoothing: {e}")
        raise
