#!/usr/bin/env python3
"""
Script to copy all data from the 'buildings' and 'units' tables from one PostgreSQL instance to another.
"""

import os
import psycopg2
import psycopg2.extras
from typing import Optional
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Table configurations
TABLE_CONFIGS = {
    'buildings': {
        'source_table': 'buildings',
        'dest_table': 'hello_data_buildings_raw',
        'state_column': 'state',
        'order_column': 'id',
        'special_columns': ['unit_mix', 'unit_mix_old']
    },
    'units': {
        'source_table': 'units',
        'dest_table': 'hello_data_units_raw',
        'state_column': '_data_pipeline_only_state',
        'order_column': 'id',
        'special_columns': []
    }
}


def get_connection(host: str, database: str, user: str, password: str) -> psycopg2.extensions.connection:
    """Create a PostgreSQL connection."""
    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )
        return conn
    except psycopg2.Error as e:
        logger.error(f"Error connecting to database: {e}")
        raise


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


def copy_state_data(state: str, source_conn, dest_conn, table_config: dict, columns: list) -> int:
    """Copy data for a specific state and table."""
    source_cursor = source_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    dest_cursor = dest_conn.cursor()

    source_table = table_config['source_table']
    dest_table = table_config['dest_table']
    state_column = table_config['state_column']
    order_column = table_config['order_column']
    special_columns = table_config['special_columns']

    # Get total row count for this state
    count_query = f"SELECT COUNT(*) as count FROM {source_table} WHERE {state_column} = %s"
    source_cursor.execute(count_query, (state,))
    result = source_cursor.fetchone()
    state_total_rows = result['count']

    if state_total_rows == 0:
        logger.info(f"No data found for state {state} in {source_table}")
        return 0

    logger.info(f"Copying {state_total_rows} rows for state {state} from {source_table}")

    # Copy data in batches
    batch_size = 15000
    offset = 0
    copied_rows = 0

    columns_str = ', '.join([f'"{col}"' for col in columns])
    placeholders = ', '.join(['%s'] * len(columns))

    insert_query = f"""
        INSERT INTO {dest_table} ({columns_str})
        VALUES ({placeholders})
        ON CONFLICT (id) DO NOTHING
    """

    while offset < state_total_rows:
        # Fetch batch from source for this state
        select_query = f"""
            SELECT {columns_str}
            FROM {source_table}
            WHERE {state_column} = %s
            ORDER BY {order_column}
            LIMIT {batch_size} OFFSET {offset}
        """

        source_cursor.execute(select_query, (state,))
        batch_data = source_cursor.fetchall()

        if not batch_data:
            break

        # Prepare data for insertion
        batch_values = []
        for row in batch_data:
            # Convert RealDictRow to tuple in column order with proper type handling
            row_values = []
            for col in columns:
                value = row[col]

                # Handle special conversions for specific columns
                if col in special_columns and value is not None:
                    # Convert string representation of array to list
                    if isinstance(value, str):
                        try:
                            # Parse the string as a Python list and convert to PostgreSQL array format
                            import json
                            parsed_list = json.loads(value)
                            value = parsed_list
                        except (json.JSONDecodeError, ValueError):
                            # If parsing fails, keep as None
                            value = None

                row_values.append(value)

            batch_values.append(tuple(row_values))

        # Insert batch into destination
        dest_cursor.executemany(insert_query, batch_values)
        dest_conn.commit()

        copied_rows += len(batch_data)
        offset += batch_size

        logger.info(
            f"  {source_table} - {state}: Copied {copied_rows}/{state_total_rows} rows ({copied_rows / state_total_rows * 100:.1f}%)")

    return copied_rows


def copy_table_data(table_key: str, source_conn, dest_conn, states: list) -> dict:
    """Copy all data for a specific table across all states."""
    logger.info(f"Starting copy for table: {table_key}")

    table_config = TABLE_CONFIGS[table_key]
    source_table = table_config['source_table']
    dest_table = table_config['dest_table']

    # Get table columns
    logger.info(f"Getting table structure for {source_table}...")
    source_cursor = source_conn.cursor()
    columns = get_table_columns(source_cursor, source_table)
    if not columns:
        logger.error(f"No columns found for {source_table} table in source database")
        return {'success': False, 'total_copied': 0, 'successful_states': [], 'failed_states': states}

    logger.info(f"Found {len(columns)} columns for {source_table}: {', '.join(columns)}")

    # Clear destination table at start of run
    logger.info(f"Truncating destination {dest_table} table...")
    dest_cursor = dest_conn.cursor()
    dest_cursor.execute(f"TRUNCATE TABLE {dest_table}")
    dest_conn.commit()
    logger.info(f"Destination table {dest_table} cleared successfully")

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

    states = ['UT']

    # Get source database credentials
    source_host = os.getenv('HELLO_DATA_DB_HOST').split(':')[0]
    source_db = os.getenv('HELLO_DATA_DB_NAME')
    source_user = os.getenv('HELLO_DATA_DB_USER')
    source_pass = os.getenv('HELLO_DATA_DB_PASS')

    # Get destination database credentials
    dest_host = os.getenv('LOCAL_DB_HOST')
    dest_db = os.getenv('LOCAL_DB_NAME')
    dest_user = os.getenv('LOCAL_DB_USER')
    dest_pass = os.getenv('LOCAL_DB_PASS')

    # Validate environment variables
    required_vars = [
        ('HELLO_DATA_DB_HOST', source_host),
        ('HELLO_DATA_DB_NAME', source_db),
        ('HELLO_DATA_DB_USER', source_user),
        ('HELLO_DATA_DB_PASS', source_pass),
        ('LOCAL_DB_HOST', dest_host),
        ('LOCAL_DB_NAME', dest_db),
        ('LOCAL_DB_USER', dest_user),
        ('LOCAL_DB_PASS', dest_pass)
    ]

    missing_vars = [var_name for var_name, var_value in required_vars if not var_value]
    if missing_vars:
        logger.error(f"Missing environment variables: {', '.join(missing_vars)}")
        return False

    source_conn = None
    dest_conn = None

    try:
        # Connect to source database
        logger.info("Connecting to source database...")
        source_conn = get_connection(source_host, source_db, source_user, source_pass)

        # Connect to destination database
        logger.info("Connecting to destination database...")
        dest_conn = get_connection(dest_host, dest_db, dest_user, dest_pass)

        # Copy each table
        overall_success = True
        table_results = {}

        for table_key in TABLE_CONFIGS.keys():
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

    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        if dest_conn:
            dest_conn.rollback()
        return False
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
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
    logger.info("Starting buildings and units table copy...")
    success = copy_all_tables()

    if success:
        logger.info("Copy completed successfully!")
    else:
        logger.error("Copy failed!")
        exit(1)


if __name__ == "__main__":
    main()