import psycopg
import os
import time
from contextlib import contextmanager


def wait_for_database(max_attempts=30):
    """Wait for Cloud SQL Proxy to be ready"""
    print("Waiting for database connection...")

    for attempt in range(max_attempts):
        try:
            conn = psycopg.connect(
                host=os.getenv("PERSONAL_GOOGLE_CLOUD_DB_HOST"),
                port=5432,
                dbname=os.environ['PERSONAL_GOOGLE_CLOUD_DB_NAME'],
                user=os.environ['PERSONAL_GOOGLE_CLOUD_DB_USER'],
                password=os.environ['PERSONAL_GOOGLE_CLOUD_DB_PASS'],
                connect_timeout=5
            )
            conn.close()
            print("✅ Database connection successful!")
            return True
        except psycopg.OperationalError as e:
            print(f"Database connection attempt {attempt + 1}/{max_attempts} failed: {e}")
            if attempt < max_attempts - 1:
                time.sleep(2)

    raise Exception(f"Could not connect to database after {max_attempts} attempts")


@contextmanager
def local_db_connection():
    """
    Lazily initializes a database connection.

    Ensures a fresh connection is created only when needed,
    reducing the risk of connection loss during long-running pipeline operations.
    """
    if not hasattr(local_db_connection, '_ready'):
        wait_for_database()
        local_db_connection._ready = True

    conn = psycopg.connect(
        port="5432",
        host=os.getenv("PERSONAL_GOOGLE_CLOUD_DB_HOST"),
        dbname=os.getenv("PERSONAL_GOOGLE_CLOUD_DB_NAME"),
        user=os.getenv("PERSONAL_GOOGLE_CLOUD_DB_USER"),
        password=os.getenv("PERSONAL_GOOGLE_CLOUD_DB_PASS"),
    )

    try:
        yield conn
    finally:
        conn.close()
