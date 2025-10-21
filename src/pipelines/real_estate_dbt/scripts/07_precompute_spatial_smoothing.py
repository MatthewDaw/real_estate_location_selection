# scripts/07_precompute_spatial_smoothing.py
"""
Pre-compute spatial smoothing values using KDTree for maximum performance.
This replaces the slow SQL spatial joins with fast Python spatial calculations.
Now includes distance-weighted averaging using exponential decay with precomputed weights.
Updated to include comprehensive building and Zillow analysis metrics.
"""

import os
import sys
import click
import pandas as pd
import numpy as np
from scipy.spatial import cKDTree
from scipy.stats import hmean
from sqlalchemy import create_engine, text
import logging
import yaml
from pathlib import Path
from haversine import haversine_vector, Unit

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_dbt_database_url():
    """Extract database URL from dbt profiles."""
    profiles_paths = [
        Path.home() / '.dbt' / 'profiles.yml',
        Path('profiles.yml'),
        Path('~/.dbt/profiles.yml').expanduser()
    ]

    profiles_path = None
    for path in profiles_paths:
        if path.exists():
            profiles_path = path
            break

    if not profiles_path:
        return None

    try:
        with open(profiles_path, 'r') as f:
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
        logger.warning(f"Could not read dbt profiles: {e}")
        return None

    return None


def precompute_weights(poi_coord, nearby_data, decay_factor, radius_meters):
    """
    Precompute weights for all nearby points using exponential decay with Haversine distance.

    Args:
        poi_coord: [lon, lat] of the point of interest
        nearby_data: DataFrame with 'lon', 'lat' columns
        decay_factor: Controls how quickly weights decay with distance
        radius_meters: Radius in meters for normalization

    Returns:
        numpy array of weights corresponding to nearby_data rows
    """
    if len(nearby_data) == 0:
        return np.array([])

    # Calculate Haversine distances in meters
    poi_points = [(poi_coord[1], poi_coord[0])]  # haversine expects (lat, lon)
    nearby_points = list(zip(nearby_data['lat'], nearby_data['lon']))

    # Calculate distances using haversine
    distances = haversine_vector(poi_points * len(nearby_points), nearby_points, Unit.METERS)

    # Use radius_meters for normalization if provided, otherwise use max distance
    max_distance = radius_meters if radius_meters else distances.max()
    if max_distance < 1e-10:
        max_distance = 1.0  # Fallback to avoid division by zero

    # Calculate exponential decay weights
    # Weight = exp(-decay_factor * distance / max_distance)
    weights = np.exp(-decay_factor * distances / max_distance)

    # Handle case where all points are at the same location
    if distances.max() < 1e-10:
        weights = np.ones(len(nearby_data))

    return weights


def calculate_weighted_mean_with_precomputed_weights(values, weights):
    """
    Calculate weighted arithmetic mean using precomputed weights.
    
    Args:
        values: numpy array or pandas series of values
        weights: numpy array or pandas series of corresponding weights
        
    Returns:
        float: weighted mean, or None if all values are null or no valid data
    """
    if len(values) == 0 or len(weights) == 0:
        return None

    # Convert to numpy arrays if they aren't already
    values = np.asarray(values)
    weights = np.asarray(weights)
    
    # Ensure arrays have the same length
    if len(values) != len(weights):
        logger.warning(f"Values and weights arrays have different lengths: {len(values)} vs {len(weights)}")
        return None
    
    # Filter out null values
    valid_mask = ~pd.isna(values)
    
    # If all values are null, return None
    if not valid_mask.any():
        return None
    
    # Get valid values and corresponding weights
    valid_values = values[valid_mask]
    valid_weights = weights[valid_mask]
    
    # If no valid weights, return None
    if len(valid_weights) == 0 or np.sum(valid_weights) == 0:
        return None
    
    # Ensure weights are non-negative
    if np.any(valid_weights < 0):
        logger.warning("Negative weights detected, using absolute values")
        valid_weights = np.abs(valid_weights)
    
    # Calculate weighted mean
    weighted_sum = np.sum(valid_values * valid_weights)
    weight_sum = np.sum(valid_weights)
    
    return weighted_sum / weight_sum


def calculate_weighted_mean_for_integers(values, weights):
    """Calculate weighted mean for integer values and round to nearest integer."""
    result = calculate_weighted_mean_with_precomputed_weights(values, weights)
    return int(round(result)) if result is not None else None


def compute_all_weighted_metrics(nearby_buildings, nearby_zillow, nearby_building_analysis, nearby_zillow_analysis,
                                 nearby_empty_lots, nearby_developed_properties,
                                 poi_coord, decay_factor, radius_meters):
    """
    Compute all weighted metrics for buildings, Zillow data, empty lots, developed properties, and their analysis using precomputed weights.
    Also returns lists of closest buildings and Zillow listings.
    """
    result = {}

    # Precompute weights for buildings
    if len(nearby_buildings) > 0:
        building_weights = precompute_weights(poi_coord, nearby_buildings, decay_factor, radius_meters)

        # Calculate distances for sorting
        poi_lat_lon = (poi_coord[1], poi_coord[0])  # Convert to (lat, lon)
        building_points = list(zip(nearby_buildings['lat'], nearby_buildings['lon']))
        distances = haversine_vector([poi_lat_lon] * len(building_points), building_points, Unit.METERS)

        # Create a DataFrame with buildings and their distances
        buildings_with_distance = nearby_buildings.copy()
        buildings_with_distance['distance_meters'] = distances

        # Sort by distance and get top 10 closest buildings
        closest_buildings = buildings_with_distance.nsmallest(10, 'distance_meters')

        # Create list of closest buildings with only IDs
        closest_buildings_list = [building['building_id'] for _, building in closest_buildings.iterrows()]
        result['closest_hd_buildings'] = closest_buildings_list

        # Building-based smoothed metrics - Current Price per sqft
        result.update({
            'avg_predicted_current_price_per_sqft': calculate_weighted_mean_with_precomputed_weights(
                nearby_buildings['predicted_current_price_per_sqft'].values, building_weights),
            'avg_predicted_current_price_lower_per_sqft': calculate_weighted_mean_with_precomputed_weights(
                nearby_buildings['predicted_current_price_lower_per_sqft'].values, building_weights),
            'avg_predicted_current_price_upper_per_sqft': calculate_weighted_mean_with_precomputed_weights(
                nearby_buildings['predicted_current_price_upper_per_sqft'].values, building_weights),
            'avg_rent_price_per_sqft': calculate_weighted_mean_with_precomputed_weights(
                nearby_buildings['avg_rent_price_per_sqft'].values, building_weights),
        })

        # Building-based smoothed metrics - Current Effective Price per sqft
        result.update({
            'avg_predicted_current_effective_price_per_sqft': calculate_weighted_mean_with_precomputed_weights(
                nearby_buildings['predicted_current_effective_price_per_sqft'].values, building_weights),
            'avg_predicted_current_effective_price_lower_per_sqft': calculate_weighted_mean_with_precomputed_weights(
                nearby_buildings['predicted_current_effective_price_lower_per_sqft'].values, building_weights),
            'avg_predicted_current_effective_price_upper_per_sqft': calculate_weighted_mean_with_precomputed_weights(
                nearby_buildings['predicted_current_effective_price_upper_per_sqft'].values, building_weights),
            'avg_effective_rent_price_per_sqft': calculate_weighted_mean_with_precomputed_weights(
                nearby_buildings['avg_effective_rent_price_per_sqft'].values, building_weights),
        })

        # Building-based smoothed metrics - Future Price per sqft
        result.update({
            'avg_predicted_future_price_per_sqft': calculate_weighted_mean_with_precomputed_weights(
                nearby_buildings['predicted_future_price_per_sqft'].values, building_weights),
            'avg_predicted_future_price_lower_per_sqft': calculate_weighted_mean_with_precomputed_weights(
                nearby_buildings['predicted_future_price_lower_per_sqft'].values, building_weights),
            'avg_predicted_future_price_upper_per_sqft': calculate_weighted_mean_with_precomputed_weights(
                nearby_buildings['predicted_future_price_upper_per_sqft'].values, building_weights),
        })

        # Building-based smoothed metrics - Future Effective Price per sqft
        result.update({
            'avg_predicted_future_effective_price_per_sqft': calculate_weighted_mean_with_precomputed_weights(
                nearby_buildings['predicted_future_effective_price_per_sqft'].values, building_weights),
            'avg_predicted_future_effective_price_lower_per_sqft': calculate_weighted_mean_with_precomputed_weights(
                nearby_buildings['predicted_future_effective_price_lower_per_sqft'].values, building_weights),
            'avg_predicted_future_effective_price_upper_per_sqft': calculate_weighted_mean_with_precomputed_weights(
                nearby_buildings['predicted_future_effective_price_upper_per_sqft'].values, building_weights),
        })

        # Building-based smoothed metrics - Current Price per bed
        result.update({
            'avg_predicted_current_price_per_bed': calculate_weighted_mean_with_precomputed_weights(
                nearby_buildings['predicted_current_price_per_bed'].values, building_weights),
            'avg_predicted_current_price_lower_per_bed': calculate_weighted_mean_with_precomputed_weights(
                nearby_buildings['predicted_current_price_lower_per_bed'].values, building_weights),
            'avg_predicted_current_price_upper_per_bed': calculate_weighted_mean_with_precomputed_weights(
                nearby_buildings['predicted_current_price_upper_per_bed'].values, building_weights),
            'avg_rent_price_per_bed': calculate_weighted_mean_with_precomputed_weights(
                nearby_buildings['avg_rent_price_per_bed'].values, building_weights),
        })

        # Building-based smoothed metrics - Current Effective Price per bed
        result.update({
            'avg_predicted_current_effective_price_per_bed': calculate_weighted_mean_with_precomputed_weights(
                nearby_buildings['predicted_current_effective_price_per_bed'].values, building_weights),
            'avg_predicted_current_effective_price_lower_per_bed': calculate_weighted_mean_with_precomputed_weights(
                nearby_buildings['predicted_current_effective_price_lower_per_bed'].values, building_weights),
            'avg_predicted_current_effective_price_upper_per_bed': calculate_weighted_mean_with_precomputed_weights(
                nearby_buildings['predicted_current_effective_price_upper_per_bed'].values, building_weights),
            'avg_effective_rent_price_per_bed': calculate_weighted_mean_with_precomputed_weights(
                nearby_buildings['avg_effective_rent_price_per_bed'].values, building_weights),
        })

        # Building-based smoothed metrics - Future Price per bed
        result.update({
            'avg_predicted_future_price_per_bed': calculate_weighted_mean_with_precomputed_weights(
                nearby_buildings['predicted_future_price_per_bed'].values, building_weights),
            'avg_predicted_future_price_lower_per_bed': calculate_weighted_mean_with_precomputed_weights(
                nearby_buildings['predicted_future_price_lower_per_bed'].values, building_weights),
            'avg_predicted_future_price_upper_per_bed': calculate_weighted_mean_with_precomputed_weights(
                nearby_buildings['predicted_future_price_upper_per_bed'].values, building_weights),
        })

        # Building-based smoothed metrics - Future Effective Price per bed
        result.update({
            'avg_predicted_future_effective_price_per_bed': calculate_weighted_mean_with_precomputed_weights(
                nearby_buildings['predicted_future_effective_price_per_bed'].values, building_weights),
            'avg_predicted_future_effective_price_lower_per_bed': calculate_weighted_mean_with_precomputed_weights(
                nearby_buildings['predicted_future_effective_price_lower_per_bed'].values, building_weights),
            'avg_predicted_future_effective_price_upper_per_bed': calculate_weighted_mean_with_precomputed_weights(
                nearby_buildings['predicted_future_effective_price_upper_per_bed'].values, building_weights),
        })

        # Building-based smoothed metrics - Unit averages
        result.update({
            'avg_beds': calculate_weighted_mean_with_precomputed_weights(
                nearby_buildings['avg_beds'].values, building_weights),
            'avg_baths': calculate_weighted_mean_with_precomputed_weights(
                nearby_buildings['avg_baths'].values, building_weights),
            'avg_sqft': calculate_weighted_mean_with_precomputed_weights(
                nearby_buildings['avg_sqft'].values, building_weights),
            'avg_price': calculate_weighted_mean_with_precomputed_weights(
                nearby_buildings['avg_price'].values, building_weights),
            'avg_effective_price': calculate_weighted_mean_with_precomputed_weights(
                nearby_buildings['avg_effective_price'].values, building_weights),
            'num_units': calculate_weighted_mean_for_integers(
                nearby_buildings['num_units'].values, building_weights),
        })

        # Building-based smoothed metrics - Occupancy and market metrics
        result.update({
            'avg_leased_percentage': calculate_weighted_mean_with_precomputed_weights(
                nearby_buildings['leased_percentage'].values, building_weights),
            'avg_exposure_percentage': calculate_weighted_mean_with_precomputed_weights(
                nearby_buildings['exposure_percentage'].values, building_weights),
            'avg_concession_percentage': calculate_weighted_mean_with_precomputed_weights(
                nearby_buildings['concession_percentage'].values, building_weights),
        })

        result['hd_buildings_within_radius'] = len(nearby_buildings)
    else:
        # Set all building metrics to None when no buildings are found
        building_metrics = [
            'avg_predicted_current_price_per_sqft', 'avg_predicted_current_price_lower_per_sqft',
            'avg_predicted_current_price_upper_per_sqft', 'avg_rent_price_per_sqft',
            'avg_predicted_current_effective_price_per_sqft', 'avg_predicted_current_effective_price_lower_per_sqft',
            'avg_predicted_current_effective_price_upper_per_sqft', 'avg_effective_rent_price_per_sqft',
            'avg_predicted_future_price_per_sqft', 'avg_predicted_future_price_lower_per_sqft',
            'avg_predicted_future_price_upper_per_sqft',
            'avg_predicted_future_effective_price_per_sqft', 'avg_predicted_future_effective_price_lower_per_sqft',
            'avg_predicted_future_effective_price_upper_per_sqft',
            'avg_predicted_current_price_per_bed', 'avg_predicted_current_price_lower_per_bed',
            'avg_predicted_current_price_upper_per_bed', 'avg_rent_price_per_bed',
            'avg_predicted_current_effective_price_per_bed', 'avg_predicted_current_effective_price_lower_per_bed',
            'avg_predicted_current_effective_price_upper_per_bed', 'avg_effective_rent_price_per_bed',
            'avg_predicted_future_price_per_bed', 'avg_predicted_future_price_lower_per_bed',
            'avg_predicted_future_price_upper_per_bed',
            'avg_predicted_future_effective_price_per_bed', 'avg_predicted_future_effective_price_lower_per_bed',
            'avg_predicted_future_effective_price_upper_per_bed',
            'avg_beds', 'avg_baths', 'avg_sqft', 'avg_price', 'avg_effective_price', 'num_units',
            'avg_leased_percentage', 'avg_exposure_percentage', 'avg_concession_percentage'
        ]
        result.update({metric: None for metric in building_metrics})
        result['hd_buildings_within_radius'] = 0
        result['closest_hd_buildings'] = []

    # Precompute weights for building analysis data
    if len(nearby_building_analysis) > 0:
        building_analysis_weights = precompute_weights(poi_coord, nearby_building_analysis, decay_factor, radius_meters)

        # Building analysis metrics
        result.update({
            'avg_building_average_percent_gain_per_year': calculate_weighted_mean_with_precomputed_weights(
                nearby_building_analysis['average_percent_gain_per_year'].values, building_analysis_weights),
            'avg_building_average_percent_gain_per_year_effective': calculate_weighted_mean_with_precomputed_weights(
                nearby_building_analysis['average_percent_gain_per_year_effective'].values, building_analysis_weights),
        })
    else:
        result.update({
            'avg_building_average_percent_gain_per_year': None,
            'avg_building_average_percent_gain_per_year_effective': None,
        })

    # Precompute weights for Zillow data
    if len(nearby_zillow) > 0:
        zillow_weights = precompute_weights(poi_coord, nearby_zillow, decay_factor, radius_meters)

        # Calculate distances for sorting Zillow listings
        poi_lat_lon = (poi_coord[1], poi_coord[0])  # Convert to (lat, lon)
        zillow_points = list(zip(nearby_zillow['lat'], nearby_zillow['lon']))
        distances = haversine_vector([poi_lat_lon] * len(zillow_points), zillow_points, Unit.METERS)

        # Create a DataFrame with Zillow listings and their distances
        zillow_with_distance = nearby_zillow.copy()
        zillow_with_distance['distance_meters'] = distances

        # Sort by distance and get top 10 closest Zillow listings
        closest_zillow = zillow_with_distance.nsmallest(10, 'distance_meters')

        # Create list of closest Zillow listings with only IDs
        closest_zillow_list = [listing['id'] for _, listing in closest_zillow.iterrows()]
        result['closest_zillow_listings'] = closest_zillow_list

        # Zillow-based smoothed metrics - Current Price Predictions per bedroom
        result.update({
            'avg_zillow_predicted_current_price_per_bedroom': calculate_weighted_mean_with_precomputed_weights(
                nearby_zillow['predicted_current_price_per_bedroom'].values, zillow_weights),
            'avg_zillow_predicted_current_price_lower_per_bedroom': calculate_weighted_mean_with_precomputed_weights(
                nearby_zillow['predicted_current_price_lower_per_bedroom'].values, zillow_weights),
            'avg_zillow_predicted_current_price_upper_per_bedroom': calculate_weighted_mean_with_precomputed_weights(
                nearby_zillow['predicted_current_price_upper_per_bedroom'].values, zillow_weights),
        })

        # Zillow-based smoothed metrics - Current Price Predictions per sqft
        result.update({
            'avg_zillow_predicted_current_price_per_sqft': calculate_weighted_mean_with_precomputed_weights(
                nearby_zillow['predicted_current_price_per_sqft'].values, zillow_weights),
            'avg_zillow_predicted_current_price_lower_per_sqft': calculate_weighted_mean_with_precomputed_weights(
                nearby_zillow['predicted_current_price_lower_per_sqft'].values, zillow_weights),
            'avg_zillow_predicted_current_price_upper_per_sqft': calculate_weighted_mean_with_precomputed_weights(
                nearby_zillow['predicted_current_price_upper_per_sqft'].values, zillow_weights),
        })

        # Zillow-based smoothed metrics - Future Price Predictions per bedroom
        result.update({
            'avg_zillow_predicted_future_price_per_bedroom': calculate_weighted_mean_with_precomputed_weights(
                nearby_zillow['predicted_future_price_per_bedroom'].values, zillow_weights),
            'avg_zillow_predicted_future_price_lower_per_bedroom': calculate_weighted_mean_with_precomputed_weights(
                nearby_zillow['predicted_future_price_lower_per_bedroom'].values, zillow_weights),
            'avg_zillow_predicted_future_price_upper_per_bedroom': calculate_weighted_mean_with_precomputed_weights(
                nearby_zillow['predicted_future_price_upper_per_bedroom'].values, zillow_weights),
        })

        # Zillow-based smoothed metrics - Future Price Predictions per sqft
        result.update({
            'avg_zillow_predicted_future_price_per_sqft': calculate_weighted_mean_with_precomputed_weights(
                nearby_zillow['predicted_future_price_per_sqft'].values, zillow_weights),
            'avg_zillow_predicted_future_price_lower_per_sqft': calculate_weighted_mean_with_precomputed_weights(
                nearby_zillow['predicted_future_price_lower_per_sqft'].values, zillow_weights),
            'avg_zillow_predicted_future_price_upper_per_sqft': calculate_weighted_mean_with_precomputed_weights(
                nearby_zillow['predicted_future_price_upper_per_sqft'].values, zillow_weights),
        })

        # Zillow-based smoothed metrics - Actual price metrics
        result.update({
            'avg_zillow_price_per_bedroom': calculate_weighted_mean_with_precomputed_weights(
                nearby_zillow['price_per_bedroom'].values, zillow_weights),
            'avg_zillow_price_per_sqft': calculate_weighted_mean_with_precomputed_weights(
                nearby_zillow['price_per_sqft'].values, zillow_weights),
        })

        # Zillow-based smoothed metrics - Property characteristics
        result.update({
            'avg_zillow_current_price': calculate_weighted_mean_with_precomputed_weights(
                nearby_zillow['current_price'].values, zillow_weights),
            'avg_zillow_bedrooms': calculate_weighted_mean_with_precomputed_weights(
                nearby_zillow['bedrooms'].values, zillow_weights),
            'avg_zillow_living_area_sqft': calculate_weighted_mean_with_precomputed_weights(
                nearby_zillow['living_area_sqft'].values, zillow_weights),
        })

        result['zillow_buildings_within_radius'] = len(nearby_zillow)
    else:
        # Set all Zillow metrics to None when no Zillow data is found
        zillow_metrics = [
            'avg_zillow_predicted_current_price_per_bedroom', 'avg_zillow_predicted_current_price_lower_per_bedroom',
            'avg_zillow_predicted_current_price_upper_per_bedroom',
            'avg_zillow_predicted_current_price_per_sqft', 'avg_zillow_predicted_current_price_lower_per_sqft',
            'avg_zillow_predicted_current_price_upper_per_sqft',
            'avg_zillow_predicted_future_price_per_bedroom', 'avg_zillow_predicted_future_price_lower_per_bedroom',
            'avg_zillow_predicted_future_price_upper_per_bedroom',
            'avg_zillow_predicted_future_price_per_sqft', 'avg_zillow_predicted_future_price_lower_per_sqft',
            'avg_zillow_predicted_future_price_upper_per_sqft',
            'avg_zillow_price_per_bedroom', 'avg_zillow_price_per_sqft',
            'avg_zillow_current_price', 'avg_zillow_bedrooms', 'avg_zillow_living_area_sqft'
        ]
        result.update({metric: None for metric in zillow_metrics})
        result['zillow_buildings_within_radius'] = 0
        result['closest_zillow_listings'] = []

    # Precompute weights for Zillow analysis data
    if len(nearby_zillow_analysis) > 0:
        zillow_analysis_weights = precompute_weights(poi_coord, nearby_zillow_analysis, decay_factor, radius_meters)

        # Zillow analysis metrics
        result.update({
            'avg_zillow_average_percent_gain_per_year': calculate_weighted_mean_with_precomputed_weights(
                nearby_zillow_analysis['average_percent_gain_per_year'].values, zillow_analysis_weights),
            'avg_zillow_trend_strength_pct': calculate_weighted_mean_with_precomputed_weights(
                nearby_zillow_analysis['trend_strength_pct'].values, zillow_analysis_weights),
            'avg_zillow_trend_variance_pct': calculate_weighted_mean_with_precomputed_weights(
                nearby_zillow_analysis['trend_variance_pct'].values, zillow_analysis_weights),
            'avg_zillow_data_span_days': calculate_weighted_mean_for_integers(
                nearby_zillow_analysis['data_span_days'].values, zillow_analysis_weights),
        })
    else:
        result.update({
            'avg_zillow_average_percent_gain_per_year': None,
            'avg_zillow_trend_strength_pct': None,
            'avg_zillow_trend_variance_pct': None,
            'avg_zillow_data_span_days': None,
        })

    # Precompute weights for empty lots data
    if len(nearby_empty_lots) > 0:
        empty_lots_weights = precompute_weights(poi_coord, nearby_empty_lots, decay_factor, radius_meters)

        # Empty lots metrics
        result.update({
            'avg_empty_lot_cost_per_sqft': calculate_weighted_mean_with_precomputed_weights(
                nearby_empty_lots['cost_per_sqft'].values, empty_lots_weights),
        })

        result['empty_lots_within_radius'] = len(nearby_empty_lots)
    else:
        result.update({
            'avg_empty_lot_cost_per_sqft': None,
        })
        result['empty_lots_within_radius'] = 0

    # Precompute weights for developed properties data
    if len(nearby_developed_properties) > 0:
        developed_properties_weights = precompute_weights(poi_coord, nearby_developed_properties, decay_factor,
                                                          radius_meters)

        # Developed properties metrics
        result.update({
            'avg_interior_sqft_cost': calculate_weighted_mean_with_precomputed_weights(
                nearby_developed_properties['cost_per_interior_sqft'].values, developed_properties_weights),
            'avg_developed_lot_cost_per_sqft': calculate_weighted_mean_with_precomputed_weights(
                nearby_developed_properties['cost_per_lot_sqft'].values, developed_properties_weights),
        })

        result['developed_properties_within_radius'] = len(nearby_developed_properties)
    else:
        result.update({
            'avg_interior_sqft_cost': None,
            'avg_developed_lot_cost_per_sqft': None,
        })
        result['developed_properties_within_radius'] = 0

    return result

@click.command()
@click.option('--db-url', help='Database URL')
@click.option('--radius-miles', default=5.0, help='Radius in miles for smoothing')
@click.option('--output-table', default='smoothed_values', help='Output table name')
@click.option('--decay-factor', default=4.4,
              help='Exponential decay factor for distance weighting (higher = steeper decay)')
def precompute_spatial_smoothing(db_url, radius_miles, output_table, decay_factor):
    """Pre-compute spatial smoothing using KDTree for fast performance with distance weighting."""

    if not db_url:
        db_url = os.environ.get('DATABASE_URL')
        if not db_url:
            db_url = get_dbt_database_url()
            if db_url:
                logger.info("Using database URL from dbt profiles")

        if not db_url:
            logger.error("No database URL provided")
            sys.exit(1)

    engine = create_engine(db_url)
    radius_meters = radius_miles * 1609.34  # Convert miles to meters

    try:
        # Load points of interest
        logger.info("Loading points of interest...")
        poi_df = pd.read_sql("""
                             SELECT source,
                                    source_id,
                                    lon,
                                    lat
                             FROM points_of_interest
                             """, engine)

        if len(poi_df) == 0:
            logger.warning("No points of interest found")
            return

        # Load building summary data
        logger.info("Loading building summary data...")
        buildings_df = pd.read_sql("""
                                   SELECT building_id,
                                          lon,
                                          lat,
                                          -- Current price per sqft metrics
                                          predicted_current_price_per_sqft,
                                          predicted_current_price_lower_per_sqft,
                                          predicted_current_price_upper_per_sqft,
                                          avg_rent_price_per_sqft,

                                          -- Current effective price per sqft metrics  
                                          predicted_current_effective_price_per_sqft,
                                          predicted_current_effective_price_lower_per_sqft,
                                          predicted_current_effective_price_upper_per_sqft,
                                          avg_effective_rent_price_per_sqft,

                                          -- Future price per sqft metrics
                                          predicted_future_price_per_sqft,
                                          predicted_future_price_lower_per_sqft,
                                          predicted_future_price_upper_per_sqft,

                                          -- Future effective price per sqft metrics
                                          predicted_future_effective_price_per_sqft,
                                          predicted_future_effective_price_lower_per_sqft,
                                          predicted_future_effective_price_upper_per_sqft,

                                          -- Current price per bed metrics
                                          predicted_current_price_per_bed,
                                          predicted_current_price_lower_per_bed,
                                          predicted_current_price_upper_per_bed,
                                          avg_rent_price_per_bed,

                                          -- Current effective price per bed metrics
                                          predicted_current_effective_price_per_bed,
                                          predicted_current_effective_price_lower_per_bed,
                                          predicted_current_effective_price_upper_per_bed,
                                          avg_effective_rent_price_per_bed,

                                          -- Future price per bed metrics
                                          predicted_future_price_per_bed,
                                          predicted_future_price_lower_per_bed,
                                          predicted_future_price_upper_per_bed,

                                          -- Future effective price per bed metrics
                                          predicted_future_effective_price_per_bed,
                                          predicted_future_effective_price_lower_per_bed,
                                          predicted_future_effective_price_upper_per_bed,

                                          -- Unit averages
                                          avg_beds,
                                          avg_baths,
                                          avg_sqft,
                                          avg_price,
                                          avg_effective_price,
                                          num_units,
                                          
                                          -- Building occupancy and market metrics
                                          leased_percentage,
                                          exposure_percentage,
                                          concession_percentage
                                   FROM hd_building_summary
                                   """, engine)

        # Load building analysis data (removed deleted columns)
        logger.info("Loading building analysis data...")
        building_analysis_df = pd.read_sql("""
                                           SELECT ba.building_id,
                                                  b.lon,
                                                  b.lat,
                                                  ba.average_percent_gain_per_year,
                                                  ba.average_percent_gain_per_year_effective
                                           FROM building_analysis ba
                                                    JOIN stg_buildings b ON ba.building_id = b.id
                                           """, engine)

        # Load Zillow building summary data
        logger.info("Loading Zillow building summary data...")
        zillow_df = pd.read_sql("""
                                SELECT id,
                                       lon,
                                       lat,
                                       -- Current price predictions per bedroom
                                       predicted_current_price_per_bedroom,
                                       predicted_current_price_lower_per_bedroom,
                                       predicted_current_price_upper_per_bedroom,

                                       -- Current price predictions per sqft
                                       predicted_current_price_per_sqft,
                                       predicted_current_price_lower_per_sqft,
                                       predicted_current_price_upper_per_sqft,

                                       -- Future price predictions per bedroom
                                       predicted_future_price_per_bedroom,
                                       predicted_future_price_lower_per_bedroom,
                                       predicted_future_price_upper_per_bedroom,

                                       -- Future price predictions per sqft
                                       predicted_future_price_per_sqft,
                                       predicted_future_price_lower_per_sqft,
                                       predicted_future_price_upper_per_sqft,

                                       -- Actual price metrics
                                       price_per_bedroom,
                                       price_per_sqft,

                                       -- Property characteristics
                                       current_price,
                                       bedrooms,
                                       living_area_sqft
                                FROM zillow_building_summary
                                WHERE lon IS NOT NULL
                                  AND lat IS NOT NULL
                                """, engine)

        # Load Zillow analysis data (removed deleted columns)
        logger.info("Loading Zillow analysis data...")
        zillow_analysis_df = pd.read_sql("""
                                         SELECT za.id,
                                                z.lon,
                                                z.lat,
                                                za.average_percent_gain_per_year,
                                                za.trend_strength_pct,
                                                za.trend_variance_pct,
                                                za.data_span_days
                                         FROM zillow_analysis za
                                                  JOIN stg_zillow z ON za.id = z.id
                                         WHERE z.lon IS NOT NULL
                                           AND z.lat IS NOT NULL
                                         """, engine)

        # Load empty lot summaries data
        logger.info("Loading empty lot summaries data...")
        empty_lots_df = pd.read_sql("""
                                    SELECT source,
                                           source_id,
                                           lon,
                                           lat,
                                           lot_area_value_sqft,
                                           price,
                                           CASE
                                               WHEN lot_area_value_sqft > 0
                                                   THEN price / lot_area_value_sqft
                                               ELSE NULL
                                               END as cost_per_sqft
                                    FROM empty_lot_summaries
                                    WHERE lon IS NOT NULL
                                      AND lat IS NOT NULL
                                      AND lot_area_value_sqft > 0
                                    """, engine)

        # Load developed properties summaries data
        logger.info("Loading developed properties summaries data...")
        developed_properties_df = pd.read_sql("""
                                              SELECT source,
                                                     source_id,
                                                     lon,
                                                     lat,
                                                     lot_area_value_sqft,
                                                     interior_sqft,
                                                     price,
                                                     CASE
                                                         WHEN interior_sqft > 0
                                                             THEN price / interior_sqft
                                                         ELSE NULL
                                                         END as cost_per_interior_sqft,
                                                     CASE
                                                         WHEN lot_area_value_sqft > 0
                                                             THEN price / lot_area_value_sqft
                                                         ELSE NULL
                                                         END as cost_per_lot_sqft
                                              FROM developed_properties_summaries
                                              WHERE lon IS NOT NULL
                                                AND lat IS NOT NULL
                                                AND (interior_sqft > 0 OR lot_area_value_sqft > 0)
                                              """, engine)

        logger.info(f"Loaded {len(poi_df)} POIs, {len(buildings_df)} buildings, "
                    f"{len(building_analysis_df)} building analyses, {len(zillow_df)} Zillow buildings, "
                    f"{len(zillow_analysis_df)} Zillow analyses, {len(empty_lots_df)} empty lots, "
                    f"{len(developed_properties_df)} developed properties")
        logger.info(f"Using decay factor: {decay_factor} and radius: {radius_miles} miles ({radius_meters:.0f} meters)")

        # Build KDTrees only if we have data
        buildings_tree = None
        building_analysis_tree = None
        zillow_tree = None
        zillow_analysis_tree = None
        empty_lots_tree = None
        developed_properties_tree = None

        if len(buildings_df) > 0:
            logger.info("Building spatial index for buildings...")
            buildings_coords = buildings_df[['lon', 'lat']].values
            buildings_tree = cKDTree(buildings_coords)

        if len(building_analysis_df) > 0:
            logger.info("Building spatial index for building analysis...")
            building_analysis_coords = building_analysis_df[['lon', 'lat']].values
            building_analysis_tree = cKDTree(building_analysis_coords)

        if len(zillow_df) > 0:
            logger.info("Building spatial index for Zillow data...")
            zillow_coords = zillow_df[['lon', 'lat']].values
            zillow_tree = cKDTree(zillow_coords)

        if len(zillow_analysis_df) > 0:
            logger.info("Building spatial index for Zillow analysis...")
            zillow_analysis_coords = zillow_analysis_df[['lon', 'lat']].values
            zillow_analysis_tree = cKDTree(zillow_analysis_coords)

        if len(empty_lots_df) > 0:
            logger.info("Building spatial index for empty lots...")
            empty_lots_coords = empty_lots_df[['lon', 'lat']].values
            empty_lots_tree = cKDTree(empty_lots_coords)

        if len(developed_properties_df) > 0:
            logger.info("Building spatial index for developed properties...")
            developed_properties_coords = developed_properties_df[['lon', 'lat']].values
            developed_properties_tree = cKDTree(developed_properties_coords)

        # Convert radius to degrees for KDTree search (with generous buffer)
        # At worst case (equator), 1 degree ≈ 69 miles, so we use a conservative conversion
        # Add 20% buffer to ensure we don't miss any points due to approximation
        radius_degrees = (radius_miles / 69.0) * 1.2

        results = []

        logger.info("Computing distance-weighted spatial smoothing with KDTree + haversine refinement...")
        for idx, row in poi_df.iterrows():
            if idx % 1000 == 0:
                logger.info(f"Processed {idx}/{len(poi_df)} points")

            poi_coord = [row['lon'], row['lat']]
            poi_lat_lon = (row['lat'], row['lon'])  # For haversine calculations

            # Initialize result
            result = {
                'source': row['source'],
                'source_id': row['source_id'],
            }

            # Find nearby buildings using KDTree + haversine refinement
            nearby_buildings = pd.DataFrame()
            if buildings_tree is not None:
                building_indices = buildings_tree.query_ball_point(poi_coord, radius_degrees)
                if building_indices:
                    candidates = buildings_df.iloc[building_indices]
                    # Refine with precise haversine distance
                    candidate_points = list(zip(candidates['lat'], candidates['lon']))
                    distances = haversine_vector([poi_lat_lon] * len(candidate_points), candidate_points, Unit.METERS)
                    within_radius = distances <= radius_meters
                    if within_radius.any():
                        nearby_buildings = candidates[within_radius].copy()

            # Find nearby building analysis using KDTree + haversine refinement
            nearby_building_analysis = pd.DataFrame()
            if building_analysis_tree is not None:
                building_analysis_indices = building_analysis_tree.query_ball_point(poi_coord, radius_degrees)
                if building_analysis_indices:
                    candidates = building_analysis_df.iloc[building_analysis_indices]
                    # Refine with precise haversine distance
                    candidate_points = list(zip(candidates['lat'], candidates['lon']))
                    distances = haversine_vector([poi_lat_lon] * len(candidate_points), candidate_points, Unit.METERS)
                    within_radius = distances <= radius_meters
                    if within_radius.any():
                        nearby_building_analysis = candidates[within_radius].copy()

            # Find nearby Zillow buildings using KDTree + haversine refinement
            nearby_zillow = pd.DataFrame()
            if zillow_tree is not None:
                zillow_indices = zillow_tree.query_ball_point(poi_coord, radius_degrees)
                if zillow_indices:
                    candidates = zillow_df.iloc[zillow_indices]
                    # Refine with precise haversine distance
                    candidate_points = list(zip(candidates['lat'], candidates['lon']))
                    distances = haversine_vector([poi_lat_lon] * len(candidate_points), candidate_points, Unit.METERS)
                    within_radius = distances <= radius_meters
                    if within_radius.any():
                        nearby_zillow = candidates[within_radius].copy()

            # Find nearby Zillow analysis using KDTree + haversine refinement
            nearby_zillow_analysis = pd.DataFrame()
            if zillow_analysis_tree is not None:
                zillow_analysis_indices = zillow_analysis_tree.query_ball_point(poi_coord, radius_degrees)
                if zillow_analysis_indices:
                    candidates = zillow_analysis_df.iloc[zillow_analysis_indices]
                    # Refine with precise haversine distance
                    candidate_points = list(zip(candidates['lat'], candidates['lon']))
                    distances = haversine_vector([poi_lat_lon] * len(candidate_points), candidate_points, Unit.METERS)
                    within_radius = distances <= radius_meters
                    if within_radius.any():
                        nearby_zillow_analysis = candidates[within_radius].copy()

            # Find nearby empty lots using KDTree + haversine refinement
            nearby_empty_lots = pd.DataFrame()
            if empty_lots_tree is not None:
                empty_lots_indices = empty_lots_tree.query_ball_point(poi_coord, radius_degrees)
                if empty_lots_indices:
                    candidates = empty_lots_df.iloc[empty_lots_indices]
                    # Refine with precise haversine distance
                    candidate_points = list(zip(candidates['lat'], candidates['lon']))
                    distances = haversine_vector([poi_lat_lon] * len(candidate_points), candidate_points, Unit.METERS)
                    within_radius = distances <= radius_meters
                    if within_radius.any():
                        nearby_empty_lots = candidates[within_radius].copy()

            # Find nearby developed properties using KDTree + haversine refinement
            nearby_developed_properties = pd.DataFrame()
            if developed_properties_tree is not None:
                developed_properties_indices = developed_properties_tree.query_ball_point(poi_coord, radius_degrees)
                if developed_properties_indices:
                    candidates = developed_properties_df.iloc[developed_properties_indices]
                    # Refine with precise haversine distance
                    candidate_points = list(zip(candidates['lat'], candidates['lon']))
                    distances = haversine_vector([poi_lat_lon] * len(candidate_points), candidate_points, Unit.METERS)
                    within_radius = distances <= radius_meters
                    if within_radius.any():
                        nearby_developed_properties = candidates[within_radius].copy()

            # Compute all metrics with precomputed weights
            metrics = compute_all_weighted_metrics(
                nearby_buildings, nearby_zillow, nearby_building_analysis, nearby_zillow_analysis,
                nearby_empty_lots, nearby_developed_properties,
                poi_coord, decay_factor, radius_meters
            )
            result.update(metrics)

            results.append(result)

        # Create results DataFrame
        results_df = pd.DataFrame(results)

        # Add created_at timestamp
        results_df['created_at'] = pd.Timestamp.now()

        # Note: No need to convert list columns to JSON since we're storing simple ID lists
        # PostgreSQL can handle Python lists as arrays directly

        # Write to database
        logger.info(f"Writing {len(results_df)} records to {output_table}...")

        with engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {output_table} CASCADE"))
            conn.commit()

        # Upload the data
        results_df.to_sql(output_table, engine, if_exists='replace', index=False, method='multi')

        logger.info(
            f"Successfully created {output_table} with {len(results_df)} records using decay factor {decay_factor}")

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        sys.exit(1)


if __name__ == '__main__':
    precompute_spatial_smoothing()