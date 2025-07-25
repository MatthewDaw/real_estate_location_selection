import os

import psycopg


def local_db_connection():
    """
    Lazily initializes a database connection.

    Ensures a fresh connection is created only when needed,
    reducing the risk of connection loss during long-running pipeline operations.
    """
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
