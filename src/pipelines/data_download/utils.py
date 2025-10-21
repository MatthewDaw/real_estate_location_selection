"""
Utility functions for database connections and common operations
"""

import os
import psycopg2
import logging

logger = logging.getLogger(__name__)


def get_hello_data_connection() -> psycopg2.extensions.connection:
    """Create a connection to the Hello Data PostgreSQL database."""
    # Get source database credentials
    host = os.getenv('HELLO_DATA_DB_HOST').split(':')[0]
    database = os.getenv('HELLO_DATA_DB_NAME')
    user = os.getenv('HELLO_DATA_DB_USER')
    password = os.getenv('HELLO_DATA_DB_PASS')

    # Validate environment variables
    required_vars = [
        ('HELLO_DATA_DB_HOST', host),
        ('HELLO_DATA_DB_NAME', database),
        ('HELLO_DATA_DB_USER', user),
        ('HELLO_DATA_DB_PASS', password)
    ]

    missing_vars = [var_name for var_name, var_value in required_vars if not var_value]
    if missing_vars:
        raise ValueError(f"Missing Hello Data environment variables: {', '.join(missing_vars)}")

    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )
        logger.info("Connected to Hello Data database")
        return conn
    except psycopg2.Error as e:
        logger.error(f"Error connecting to Hello Data database: {e}")
        raise


def get_local_data_connection() -> psycopg2.extensions.connection:
    """Create a connection to the local PostgreSQL database."""
    # Get destination database credentials
    host = os.getenv('LOCAL_DB_HOST')
    database = os.getenv('LOCAL_DB_NAME')
    user = os.getenv('LOCAL_DB_USER')
    password = os.getenv('LOCAL_DB_PASS')

    # Validate environment variables
    required_vars = [
        ('LOCAL_DB_HOST', host),
        ('LOCAL_DB_NAME', database),
        ('LOCAL_DB_USER', user),
        ('LOCAL_DB_PASS', password)
    ]

    missing_vars = [var_name for var_name, var_value in required_vars if not var_value]
    if missing_vars:
        raise ValueError(f"Missing local database environment variables: {', '.join(missing_vars)}")

    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )
        logger.info("Connected to local database")
        return conn
    except psycopg2.Error as e:
        logger.error(f"Error connecting to local database: {e}")
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