#!/usr/bin/env python3
"""
Script to download all data from BigQuery tables to local PostgreSQL instance.
"""


from dotenv import load_dotenv
load_dotenv('../../../../.env')

import json
import logging
import os
import tempfile
from typing import Dict, List, Any

import psycopg2
import psycopg2.extras
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import bigquery_storage
from google.cloud.bigquery_storage import types
from sqlalchemy import create_engine, text

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Table configurations
TABLE_CONFIGS = {

    "zillow_urls":{
        'bigquery_table': 'projects/flowing-flame-464314-j5/datasets/real_estate/tables/zillow_urls',
        'postgres_table': 'zillow_urls',
        'dedup_column': 'url',
        'json_columns': []
    },
    'landwatch_urls':{
        'bigquery_table': 'projects/flowing-flame-464314-j5/datasets/real_estate/tables/landwatch_urls',
        'postgres_table': 'landwatch_urls',
        'dedup_column': 'url',
        'json_columns': []
    },
    # 'landwatch_properties_raw': {
    #     'bigquery_table': 'projects/flowing-flame-464314-j5/datasets/real_estate/tables/landwatch_properties',
    #     'postgres_table': 'landwatch_properties_raw',
    #     'dedup_column': 'url',
    #     'json_columns': ['lot_type', 'amenities', 'mortgage_options', 'activities', 'lot_description',
    #                      'geography', 'road_frontage_desc', 'property_types', 'description', 'foreclosure']
    # },
    # 'zillow_property_raw': {
    #     'bigquery_table': 'projects/flowing-flame-464314-j5/datasets/real_estate/tables/zillow_property_details',
    #     'postgres_table': 'zillow_property_raw',
    #     'dedup_column': 'source_url',
    #     'json_columns': ['selling_soon', 'address', 'price_history', 'home_insights', 'school_distances',
    #                      'risks', 'foreclosure']
    # },

}


def get_postgres_connection() -> psycopg2.extensions.connection:
    """Create a PostgreSQL connection using environment variables."""
    try:
        conn = psycopg2.connect(
            host=os.getenv('LOCAL_DB_HOST'),
            database=os.getenv('LOCAL_DB_NAME'),
            user=os.getenv('LOCAL_DB_USER'),
            password=os.getenv('LOCAL_DB_PASS')
        )
        return conn
    except psycopg2.Error as e:
        logger.error(f"Error connecting to PostgreSQL: {e}")
        raise


def validate_environment_variables() -> bool:
    """Validate that all required environment variables are set."""
    required_vars = ['LOCAL_DB_HOST', 'LOCAL_DB_NAME', 'LOCAL_DB_USER', 'LOCAL_DB_PASS']
    missing_vars = [var for var in required_vars if not os.getenv(var)]

    if missing_vars:
        logger.error(f"Missing environment variables: {', '.join(missing_vars)}")
        return False
    return True


def download_bigquery_to_parquet(table_path: str, parquet_file: str) -> None:
    """Download BigQuery table data to a parquet file."""
    logger.info(f"Downloading {table_path} to {parquet_file}")

    client = bigquery_storage.BigQueryReadClient()

    session = client.create_read_session(
        parent="projects/flowing-flame-464314-j5",
        read_session=types.ReadSession(
            table=table_path,
            data_format=types.DataFormat.ARROW,
        ),
        max_stream_count=1,
    )

    if not session.streams:
        logger.warning(f"No data streams available for {table_path}")
        return

    reader = client.read_rows(session.streams[0].name)
    writer = None

    try:
        for page in reader.rows().pages:
            record_batch = page.to_arrow()
            if writer is None:
                writer = pq.ParquetWriter(parquet_file, record_batch.schema)
            writer.write_table(pa.Table.from_batches([record_batch]))
    finally:
        if writer:
            writer.close()

    logger.info(f"Downloaded {table_path} successfully")


def get_sqlalchemy_engine():
    """Create SQLAlchemy engine for PostgreSQL."""
    host = os.getenv('LOCAL_DB_HOST')
    database = os.getenv('LOCAL_DB_NAME')
    user = os.getenv('LOCAL_DB_USER')
    password = os.getenv('LOCAL_DB_PASS')

    connection_string = f"postgresql://{user}:{password}@{host}/{database}"
    return create_engine(connection_string)


def upload_parquet_to_postgres(parquet_file: str, table_name: str, dedup_column: str, json_columns: List[str]) -> int:
    """Upload parquet data directly to PostgreSQL."""
    logger.info(f"Uploading {parquet_file} directly to {table_name}")

    # Read parquet file
    table = pq.read_table(parquet_file)
    df = table.to_pandas()

    if len(df) == 0:
        logger.warning(f"No data found in {parquet_file}")
        return 0

    # Convert JSON columns to strings for JSONB
    for col in json_columns:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: json.dumps(x) if x is not None else None)

    # Remove duplicates based on the specified dedup column
    initial_count = len(df)
    df = df.drop_duplicates(subset=[dedup_column], keep='first')
    if len(df) < initial_count:
        logger.warning(f"Removed {initial_count - len(df)} duplicate entries based on {dedup_column}")

    # Create SQLAlchemy engine and upload
    engine = get_sqlalchemy_engine()

    try:
        with engine.begin() as conn:
            # Truncate existing data using 01_raw SQL
            logger.info(f"Truncating table {table_name}")
            conn.execute(text(f"TRUNCATE TABLE {table_name} CASCADE"))

            # Use pandas to_sql for direct upload
            logger.info(f"Uploading {len(df)} rows to {table_name}")
            df.to_sql(
                table_name,
                conn,
                if_exists='append',
                index=False,
                method='multi'
            )
            # Transaction is automatically committed when exiting the context manager

        logger.info(f"Successfully uploaded {len(df)} rows to {table_name}")
        return len(df)

    except Exception as err:
        logger.error(f"Error uploading to {table_name}: {err}")
        raise

    finally:
        engine.dispose()


def sync_table(table_key: str, config: Dict[str, Any]) -> bool:
    """Sync a single table from BigQuery to PostgreSQL."""
    logger.info(f"Starting sync for {table_key}")

    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            parquet_file = os.path.join(temp_dir, f"{table_key}.parquet")

            # Download from BigQuery to parquet
            download_bigquery_to_parquet(config['bigquery_table'], parquet_file)

            # Upload directly from parquet to PostgreSQL
            row_count = upload_parquet_to_postgres(
                parquet_file,
                config['postgres_table'],
                config['dedup_column'],
                config['json_columns']
            )

            if row_count == 0:
                logger.warning(f"No data found for {table_key}")
                return True

            logger.info(f"Successfully synced {row_count} rows for {table_key}")
            return True

    except Exception as e:
        logger.error(f"Failed to sync {table_key}: {e}")
        return False


def main():
    """Main function to sync all tables."""
    logger.info("Starting BigQuery to PostgreSQL sync")

    # Validate environment
    if not validate_environment_variables():
        logger.error("Environment validation failed")
        return 1

    # Sync each table
    success_count = 0
    total_tables = len(TABLE_CONFIGS)

    for table_key, config in TABLE_CONFIGS.items():
        logger.info(f"Processing table {table_key} ({success_count + 1}/{total_tables})")

        if sync_table(table_key, config):
            success_count += 1
            logger.info(f"✓ {table_key} synced successfully")
        else:
            logger.error(f"✗ {table_key} sync failed")

    # Summary
    logger.info("=" * 60)
    logger.info("SYNC SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total tables: {total_tables}")
    logger.info(f"Successful: {success_count}")
    logger.info(f"Failed: {total_tables - success_count}")

    if success_count == total_tables:
        logger.info("All tables synced successfully!")
        return 0
    else:
        logger.error("Some tables failed to sync")
        return 1


if __name__ == "__main__":
    main()