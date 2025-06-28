from real_estate_location_selection.connection import local_db_connection
import pandas as pd
import os
import numpy as np
from scipy.spatial import cKDTree
from math import radians, cos, sin, asin, sqrt, exp
import pandas as pd

# Show all columns
pd.set_option('display.max_columns', None)

estimate_beds_per_acre = 12

# File-based cache paths
filtered_cache_path = "cached_filtered_properties.pkl"
unit_prices_cache_path = "cached_unit_prices.pkl"

def get_data(force_refresh=False):
    """
    Downloads the dataframes (or loads from local pickle cache if available).

    Args:
        force_refresh (bool): If True, re-downloads from DB even if cache exists.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: (filtered_properties, unit_prices_summary)
    """
    # Check if cached files exist and refresh is not forced
    if (not force_refresh and
        os.path.exists(filtered_cache_path) and
        os.path.exists(unit_prices_cache_path)):
        print("Loading cached dataframes from disk.")
        lot_prices_df = pd.read_pickle(filtered_cache_path)
        unit_rent_df = pd.read_pickle(unit_prices_cache_path)
        return lot_prices_df, unit_rent_df

    # Query 1: Full Utah property details filtered by 10-mile proximity
    filtered_properties_query = f"""
    WITH lot_property_details AS (
        SELECT 
            id,
            ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography AS geom,
            latitude,
            longitude,
            city_name,
            cost_per_acre,
            url
        FROM
            lot_property_details
        WHERE
            state = 'UT'
    ),
    unit_locations AS (
        SELECT
            id,
            ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography AS geom
        FROM
            unit_prices_summary
        WHERE
            state = 'UT'
    )
    SELECT
        p.id,
        p.url,
        p.latitude,
        p.longitude,
        p.city_name,
        p.cost_per_acre,
        (p.cost_per_acre / {estimate_beds_per_acre}) as acre_cost_per_bed
    FROM
        lot_property_details p
    WHERE
        EXISTS (
            SELECT 1
            FROM unit_locations u
            WHERE ST_Distance(p.geom, u.geom) <= 16093.44
        );
    """

    # Query 2: Raw Utah unit prices summary
    unit_prices_query = """
    SELECT
        id,
        lat AS latitude,
        lon AS longitude,
        price_per_bed as rent_per_bed
    FROM
        unit_prices_summary
    WHERE
        state = 'UT';
    """

    # Execute both queries
    with local_db_connection() as conn:
        lot_prices_df = pd.read_sql(filtered_properties_query, conn)
        unit_rent_df = pd.read_sql(unit_prices_query, conn)

    # Save to disk as pickle files
    lot_prices_df.to_pickle(filtered_cache_path)
    unit_rent_df.to_pickle(unit_prices_cache_path)

    print("Returning new dataframes from DB and saving to disk.")
    return lot_prices_df, unit_rent_df


def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance in miles between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 3963  # Radius of earth in miles
    return c * r

def gaussian_weight(distance, sigma=5.0):
    """
    Returns a weight for a given distance, following a Gaussian curve.
    Closer distances get higher weights, distances around 20 miles get ~0.
    """
    return exp(-0.5 * (distance / sigma)**2)


def interpolate_rent_per_bed_kdtree(lot_prices_df, unit_rent_df, search_radius_miles=1):
    """
    For each row in lot_prices_df, find nearby unit_rent_df neighbors (within 20 miles)
    and interpolate an estimated_rent_per_bed using Gaussian weights.
    """
    # Build KDTree for unit_rent_df
    unit_coords = unit_rent_df[['latitude', 'longitude']].to_numpy()
    kdtree = cKDTree(unit_coords)

    estimated_prices = []

    for idx, lot_row in lot_prices_df.iterrows():
        lot_lat = lot_row['latitude']
        lot_lon = lot_row['longitude']

        # Calculate scaling for longitude degrees at this latitude
        lat_rad = radians(lot_lat)
        lat_deg_miles = 69.0
        lon_deg_miles = 69.0 * cos(lat_rad)

        # Compute search window in degrees
        lat_radius_deg = search_radius_miles / lat_deg_miles
        lon_radius_deg = search_radius_miles / lon_deg_miles

        # Use KDTree to find nearby unit_rent_df indices
        indices = kdtree.query_ball_point(
            [lot_lat, lot_lon],
            r=np.sqrt(lat_radius_deg**2 + lon_radius_deg**2)
        )

        if not indices:
            estimated_prices.append(np.nan)
            continue

        # Compute precise haversine distances for these candidates
        nearby_units = unit_rent_df.iloc[indices].copy()
        distances = nearby_units.apply(
            lambda row: haversine(lot_lon, lot_lat, row['longitude'], row['latitude']),
            axis=1
        )

        # Filter to truly within search_radius_miles
        mask_within_radius = distances <= search_radius_miles
        # must have at least 2 properties within radius
        if sum(mask_within_radius) < 1:
            estimated_prices.append(np.nan)
            continue

        # Apply Gaussian weights
        final_distances = distances.loc[mask_within_radius]
        weights = final_distances.apply(gaussian_weight)
        weighted_sum = np.sum(weights * nearby_units.loc[mask_within_radius, 'rent_per_bed'])
        total_weight = np.sum(weights)

        if total_weight == 0:
            estimated_prices.append(np.nan)
        else:
            estimated_prices.append(weighted_sum / total_weight)

    # Add the interpolated column to lot_prices_df
    lot_prices_df['potential_rent_per_bed'] = estimated_prices
    lot_prices_df = lot_prices_df[~lot_prices_df['potential_rent_per_bed'].isna()]
    return lot_prices_df

def estimate_profitability(lot_prices_df):
    lot_prices_df['estimate_profit'] = lot_prices_df['potential_rent_per_bed'] / lot_prices_df['acre_cost_per_bed']
    lot_prices_df = lot_prices_df.sort_values('estimate_profit', ascending=False)
    print("think more here")
    lot_prices_df[['ur', 'potential_rent_per_bed', 'acre_cost_per_bed', 'estimate_profit']]


if __name__ == "__main__":
    lot_prices_df, unit_rent_df = get_data()
    lot_prices_df = interpolate_rent_per_bed_kdtree(lot_prices_df, unit_rent_df)
    estimate_profitability(lot_prices_df)
    print("pause")

