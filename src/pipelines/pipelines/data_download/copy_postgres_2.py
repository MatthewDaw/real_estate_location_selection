#!/usr/bin/env python3
"""
Script to copy all data from the 'buildings', 'units', and '04_units_history' tables from one PostgreSQL instance to another.
"""

import logging
import psycopg2.extras
"""
Utility functions for database connections and common operations
"""

import os
import psycopg2
import logging
from urllib.parse import quote_plus

logger = logging.getLogger(__name__)


def get_table_columns(cursor, table_name: str) -> list:
    """Get all column names from the specified table."""
    cursor.execute("""
                   SELECT column_name
                   FROM information_schema.columns
                   WHERE table_name = %s
                     AND table_schema = 'public'
                   ORDER BY ordinal_position
                   """, (table_name,))
    columns = [row[0] for row in cursor.fetchall()]
    return columns

def get_view_columns(cursor, view_name: str) -> list:
    """Get all column names from the specified view."""
    cursor.execute("""
                   SELECT column_name
                   FROM information_schema.columns
                   WHERE table_name = %s
                     AND table_schema = 'public'
                     AND table_type = 'VIEW'
                   ORDER BY ordinal_position
                   """, (view_name,))
    columns = [row[0] for row in cursor.fetchall()]
    return columns

def get_materialized_view_columns(cursor, materialized_view_name: str) -> list:
    """Get all column names from the specified materialized view."""
    cursor.execute("""
                   SELECT a.attname as column_name
                   FROM pg_attribute a
                   JOIN pg_class c ON a.attrelid = c.oid
                   JOIN pg_namespace n ON c.relnamespace = n.oid
                   WHERE c.relname = %s
                     AND n.nspname = 'public'
                     AND c.relkind = 'm'
                     AND a.attnum > 0
                     AND NOT a.attisdropped
                   ORDER BY a.attnum
                   """, (materialized_view_name,))
    columns = [row[0] for row in cursor.fetchall()]
    return columns

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Table configurations
TABLE_CONFIGS = {
    'building_quality':
        {
        'source_table': 'building_quality',
        'dest_table': 'building_quality_table',
        'order_column': 'building_id',
        'special_columns':[],
            'state_column': 'state'
        }
    # 'buildings': {
    #     'source_table': 'buildings',
    #     'dest_table': 'hello_data_buildings_raw',
    #     'state_column': 'state',
    #     'order_column': 'id',
    #     'special_columns': ['unit_mix', 'unit_mix_old']
    # },
    # 'units': {
    #     'source_table': 'units',
    #     'dest_table': 'hello_data_units_raw',
    #     'state_column': '_data_pipeline_only_state',
    #     'order_column': 'id',
    #     'special_columns': []
    # },
    # '04_units_history': {
    #     'source_table': '04_units_history',
    #     'dest_table': 'hello_data_units_history_raw',
    #     'state_column': '_data_pipeline_only_state',
    #     'order_column': 'building_id',
    #     'special_columns': ['price_plans']
    # }
}

import io
import csv
from typing import List


def copy_state_data(state: str, source_conn, dest_conn, table_config: dict, columns: list) -> int:
    """Copy data for a specific state using COPY for maximum speed."""
    source_cursor = source_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    dest_cursor = dest_conn.cursor()

    source_table = table_config['source_table']
    dest_table = table_config['dest_table']
    state_column = table_config['state_column']
    order_column = table_config['order_column']
    special_columns = table_config['special_columns']

    # Get all data for the state at once
    columns_str = ', '.join([f'"{col}"' for col in columns])
    select_query = f"""
        SELECT {columns_str}
        FROM {source_table}
        WHERE building_id IN (
            SELECT id FROM buildings WHERE state = %s
        )
        ORDER BY {order_column}
    """

    source_cursor.execute(select_query, (state,))
    all_data = source_cursor.fetchall()

    if not all_data:
        logger.info(f"No data found for state {state} in {source_table}")
        return 0

    logger.info(f"Copying {len(all_data)} rows for state {state} from {source_table}")

    # Create StringIO buffer
    buffer = io.StringIO()
    writer = csv.writer(buffer, delimiter='\t', quoting=csv.QUOTE_MINIMAL)

    # Process and write data to buffer
    for row in all_data:
        processed_row = []
        for col in columns:
            value = row[col]

            # Handle special conversions
            if col in special_columns and value is not None:
                if col in ['unit_mix', 'unit_mix_old']:
                    if isinstance(value, str):
                        try:
                            import json
                            parsed_list = json.loads(value)
                            value = json.dumps(parsed_list)  # Convert to JSON string for COPY
                        except (json.JSONDecodeError, ValueError):
                            value = None
                elif col == 'price_plans':
                    import json
                    if isinstance(value, str):
                        try:
                            parsed_data = json.loads(value)
                            value = json.dumps(parsed_data)
                        except (json.JSONDecodeError, ValueError):
                            value = None
                    elif isinstance(value, (list, dict)):
                        try:
                            value = json.dumps(value)
                        except (TypeError, ValueError):
                            value = None

            # Handle None values and escape for COPY
            if value is None:
                processed_row.append('\\N')  # PostgreSQL NULL representation in COPY
            else:
                processed_row.append(str(value))

        writer.writerow(processed_row)

    # Reset buffer position
    buffer.seek(0)

    # Use COPY to insert all data at once
    try:
        dest_cursor.copy_from(
            buffer,
            dest_table,
            columns=columns,
            sep='\t'
        )
        dest_conn.commit()
        logger.info(f"Successfully copied {len(all_data)} rows for state {state}")
        return len(all_data)
    except Exception as e:
        dest_conn.rollback()
        logger.error(f"Error during COPY operation: {e}")
        raise

def copy_table_data(table_key: str, source_conn, dest_conn, states: list) -> dict:
    """Copy all data for a specific table across all states."""
    logger.info(f"Starting copy for table: {table_key}")

    table_config = TABLE_CONFIGS[table_key]
    source_table = table_config['source_table']
    dest_table = table_config['dest_table']

    # Get table columns
    logger.info(f"Getting table structure for {source_table}...")
    source_cursor = source_conn.cursor()
    columns = get_materialized_view_columns(source_cursor, source_table)
    if not columns:
        logger.error(f"No columns found for {source_table} table in source database")
        return {'success': False, 'total_copied': 0, 'successful_states': [], 'failed_states': states}

    logger.info(f"Found {len(columns)} columns for {source_table}: {', '.join(columns)}")

    # # Clear destination table at start of run
    # logger.info(f"Truncating destination {dest_table} table...")
    # dest_cursor = dest_conn.cursor()
    # dest_cursor.execute(f"TRUNCATE TABLE {dest_table}")
    # dest_conn.commit()
    # logger.info(f"Destination table {dest_table} cleared successfully")

    # Copy data state by state
    total_copied = 0
    successful_states = []
    failed_states = []

    for i, state in enumerate(states, 1):
        logger.info(f"Processing {source_table} - state {i}/{len(states)}: {state}")
        try:
            copied_rows = copy_state_data(state, source_conn, dest_conn, table_config, columns)
            total_copied += copied_rows
            successful_states.append((state, copied_rows))
            logger.info(f"Completed {source_table} - {state}: {copied_rows} rows copied")
            dest_conn.commit()
        except Exception as e:
            logger.error(f"Failed to copy {source_table} data for state {state}: {e}")
            failed_states.append(state)
            continue

    return {
        'success': len(failed_states) == 0,
        'total_copied': total_copied,
        'successful_states': successful_states,
        'failed_states': failed_states
    }


def copy_all_tables():
    """Copy all data from source tables to destination tables, state by state."""

    # States to download
    states = ['UT', 'ID', 'NV', 'WY', 'MT', 'NH', 'CO', 'AZ', 'NM', 'TX', 'OK', 'KS', 'NE', 'IA', 'IL', 'MO', 'IN',
              'AR', 'LA', 'MS', 'MI']

    states = ['IL']

    source_conn = None
    dest_conn = None

    try:
        # Connect to databases using utility functions
        logger.info("Connecting to source database...")
        source_conn = get_hello_data_connection()

        logger.info("Connecting to destination database...")
        dest_conn = get_local_data_connection()

        # Copy each table
        overall_success = True
        table_results = {}

        for table_key in TABLE_CONFIGS.keys():
            if table_key == 'building_quality':
                logger.info("=" * 60)
                logger.info(f"PROCESSING TABLE: {table_key.upper()}")
                logger.info("=" * 60)

                result = copy_table_data(table_key, source_conn, dest_conn, states)
                table_results[table_key] = result

                if not result['success']:
                    overall_success = False

        # Overall Summary
        logger.info("=" * 60)
        logger.info("OVERALL COPY SUMMARY")
        logger.info("=" * 60)

        total_rows_all_tables = 0
        for table_key, result in table_results.items():
            total_rows_all_tables += result['total_copied']
            logger.info(f"{table_key}: {result['total_copied']} rows copied")

            if result['successful_states']:
                logger.info(
                    f"  Successful states ({len(result['successful_states'])}): {', '.join([s[0] for s in result['successful_states']])}")

            if result['failed_states']:
                logger.warning(
                    f"  Failed states ({len(result['failed_states'])}): {', '.join(result['failed_states'])}")

        logger.info(f"Total rows copied across all tables: {total_rows_all_tables}")

        return overall_success

    except Exception as e:
        logger.error(f"Error in copy process: {e}")
        return False
    finally:
        # Close connections
        if source_conn:
            source_conn.close()
            logger.info("Source connection closed")
        if dest_conn:
            dest_conn.close()
            logger.info("Destination connection closed")


def main():
    """Main function."""
    logger.info("Starting buildings, units, and 04_units_history table copy...")
    success = copy_all_tables()

    if success:
        logger.info("Copy completed successfully!")
    else:
        logger.error("Copy failed!")
        exit(1)


if __name__ == "__main__":
    main()

