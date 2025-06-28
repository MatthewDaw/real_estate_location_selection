import psycopg
import os

def hd_connection():
    """
    Lazily initializes a database connection.

    Ensures a fresh connection is created only when needed,
    reducing the risk of connection loss during long-running pipeline operations.
    """
    return psycopg.connect(
        port="5432",
        host=os.getenv("INSTANCE_UNIX_SOCKET") or os.getenv("DB_HOST")[:-5],
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        application_name="update-core",
    )

def local_db_connection():
    """
    Lazily initializes a database connection.

    Ensures a fresh connection is created only when needed,
    reducing the risk of connection loss during long-running pipeline operations.
    """
    return psycopg.connect(
        port="5432",
        host=os.getenv("LOCAL_DB_HOST"),
        dbname=os.getenv("LOCAL_DB_NAME"),
        user=os.getenv("LOCAL_DB_USER"),
        password=os.getenv("LOCAL_DB_PASS"),
        application_name="update-core",
    )
