# scripts/02_closest_buildings.py
import os
import sys
import click
import pandas as pd
import numpy as np
from scipy.spatial import cKDTree
from sqlalchemy import create_engine, text
import logging
import yaml
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_dbt_database_url():
    """Extract database URL from dbt profiles."""

    # Try to find profiles.yml
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

        # Get the profile name (usually matches your project name)
        profile_name = 'my_real_estate_project'  # Update this to match your profile name
        target = 'dev'  # or whatever your default target is

        if profile_name in profiles:
            profile = profiles[profile_name]
            if 'outputs' in profile and target in profile['outputs']:
                output = profile['outputs'][target]

                # Build connection string
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


@click.command()
@click.option('--db-url', help='Database URL')
@click.option('--buildings-table', default='stg_buildings', help='Buildings table name')
@click.option('--output-table', default='closest_buildings_lookup', help='Output table name')
@click.option('--batch-size', default=10000, help='Process in batches for large datasets')
def calculate_closest_buildings(db_url, buildings_table, output_table, batch_size):
    """Calculate closest buildings for all properties using KDTree."""

    if not db_url:
        # Try to get from environment
        db_url = os.environ.get('DATABASE_URL')

        # If still not found, try dbt profiles
        if not db_url:
            db_url = get_dbt_database_url()
            if db_url:
                logger.info("Using database URL from dbt profiles")

        if not db_url:
            logger.error(
                "No database URL provided. Please set DATABASE_URL environment variable, use --db-url, or configure dbt profiles.")
            sys.exit(1)

    engine = create_engine(db_url)

    try:
        logger.info("Loading buildings data...")
        buildings_df = pd.read_sql(f"""
            SELECT 
                id::text as id, 
                msa,
                zip_code,
                lon, 
                lat
            FROM {buildings_table}
            WHERE geog_point IS NOT NULL
        """, engine)

        logger.info(f"Loaded {len(buildings_df)} buildings")

        # Build KDTree
        buildings_coords = buildings_df[['lon', 'lat']].values
        tree = cKDTree(buildings_coords)

        # Process both Zillow and Landwatch data
        all_results = []

        # Get Zillow properties (up to json_cleaned step)
        logger.info("Processing Zillow properties...")
        zillow_query = """
                       SELECT id::text, 'zillow' as source, \
                              longitude as lon, \
                              latitude as lat
                       FROM raw_zillow
                       WHERE geog_point IS NOT NULL \
                       """

        zillow_df = pd.read_sql(zillow_query, engine)
        if len(zillow_df) > 0:
            # Check for duplicates within zillow data
            zillow_dupes = zillow_df.duplicated(subset=['id']).sum()
            if zillow_dupes > 0:
                logger.warning(f"Found {zillow_dupes} duplicate Zillow IDs, removing...")
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
            logger.info(f"Processed {len(zillow_results)} Zillow properties")

        # Get Landwatch properties (up to excluded_types step)
        logger.info("Processing Landwatch properties...")
        landwatch_query = """
                          SELECT id::text, 'landwatch' as source, \
                                 longitude as lon, \
                                 latitude  as lat
                          FROM raw_landwatch
                          WHERE geog_point IS NOT NULL \
                          """

        landwatch_df = pd.read_sql(landwatch_query, engine)
        if len(landwatch_df) > 0:
            # Check for duplicates within landwatch data
            landwatch_dupes = landwatch_df.duplicated(subset=['id']).sum()
            if landwatch_dupes > 0:
                logger.warning(f"Found {landwatch_dupes} duplicate Landwatch IDs, removing...")
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
            logger.info(f"Processed {len(landwatch_results)} Landwatch properties")

        # Get Manual Collected Properties
        logger.info("Processing Manual Collected Properties...")
        manual_query = """
                      SELECT id::text, 'manual' as source, \
                             lon, \
                             lat
                      FROM raw_manual_collected_properties
                      WHERE geog_point IS NOT NULL \
                      """

        manual_df = pd.read_sql(manual_query, engine)
        if len(manual_df) > 0:
            # Check for duplicates within manual data
            manual_dupes = manual_df.duplicated(subset=['id']).sum()
            if manual_dupes > 0:
                logger.warning(f"Found {manual_dupes} duplicate Manual Collected Property IDs, removing...")
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
            logger.info(f"Processed {len(manual_results)} Manual Collected Properties")

        # Combine results
        if all_results:
            final_results = pd.concat(all_results, ignore_index=True)
            logger.info(f"Combined results: {len(final_results)} records")

            # Remove any remaining duplicates across platforms
            before_dedup = len(final_results)
            final_results = final_results.drop_duplicates(subset=['source', 'property_id'], keep='first')
            after_dedup = len(final_results)

            if before_dedup != after_dedup:
                logger.info(f"Removed {before_dedup - after_dedup} cross-platform duplicates")

            # Write to database
            logger.info(f"Writing {len(final_results)} records to {output_table}...")

            with engine.connect() as conn:
                # Drop table without CASCADE (safer)
                conn.execute(text(f"TRUNCATE TABLE {output_table}"))
                conn.commit()

            final_results.to_sql(output_table, engine, if_exists='append', index=False, method='multi')

            logger.info(f"Successfully processed {len(final_results)} total properties")
        else:
            logger.warning("No properties found to process")

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        sys.exit(1)


if __name__ == '__main__':
    calculate_closest_buildings()
