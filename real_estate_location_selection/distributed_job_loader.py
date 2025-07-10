#!/usr/bin/env python3
"""
Distributed job loader that uses BigQuery as a distributed lock mechanism
to prevent multiple processes from loading the same jobs.
"""

import time
import uuid
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from real_estate_location_selection.load_pub_sub import load_jobs_into_pub_sub

class DistributedJobLoader:
    """Manages distributed job loading with BigQuery-based locking"""

    def __init__(self, project_id, scraper, dataset_id="your_dataset", jobs_per_batch=5000):
        self.project_id = project_id
        self.scraper = scraper
        self.dataset_id = dataset_id
        self.client = bigquery.Client(project=project_id)
        self.process_id = str(uuid.uuid4())[:8]  # Unique process identifier
        self._ensure_lock_table_exists()
        self.jobs_per_batch = jobs_per_batch

    def _ensure_lock_table_exists(self):
        """Create the lock table if it doesn't exist"""
        table_id = f"{self.project_id}.{self.dataset_id}.job_loader_locks"

        schema = [
            bigquery.SchemaField("lock_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("process_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("acquired_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("expires_at", "TIMESTAMP", mode="REQUIRED"),
        ]

        try:
            self.client.get_table(table_id)
        except NotFound:
            table = bigquery.Table(table_id, schema=schema)
            table = self.client.create_table(table)
            print(f"Created lock table: {table_id}")

    def acquire_lock(self, lock_name, lock_duration_minutes=10):
        """
        Try to acquire a distributed lock

        Args:
            lock_name (str): Name of the lock (e.g., "landwatch_job_loader")
            lock_duration_minutes (int): How long the lock should last

        Returns:
            bool: True if lock acquired, False otherwise
        """
        table_id = f"{self.project_id}.{self.dataset_id}.job_loader_locks"
        now = datetime.utcnow()
        expires_at = now + timedelta(minutes=lock_duration_minutes)

        # First, clean up expired locks
        cleanup_query = f"""
        DELETE FROM `{table_id}`
        WHERE lock_name = @lock_name AND expires_at < CURRENT_TIMESTAMP()
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("lock_name", "STRING", lock_name)
            ]
        )

        self.client.query(cleanup_query, job_config=job_config).result()

        # Try to acquire lock
        try:
            insert_query = f"""
            INSERT INTO `{table_id}` (lock_name, process_id, acquired_at, expires_at)
            SELECT @lock_name, @process_id, @acquired_at, @expires_at
            FROM (SELECT 1) AS dummy
            WHERE NOT EXISTS (
                SELECT 1 FROM `{table_id}` 
                WHERE lock_name = @lock_name AND expires_at > CURRENT_TIMESTAMP()
            )
            """

            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("lock_name", "STRING", lock_name),
                    bigquery.ScalarQueryParameter("process_id", "STRING", self.process_id),
                    bigquery.ScalarQueryParameter("acquired_at", "TIMESTAMP", now),
                    bigquery.ScalarQueryParameter("expires_at", "TIMESTAMP", expires_at),
                ]
            )

            result = self.client.query(insert_query, job_config=job_config).result()

            # Check if we actually got the lock
            check_query = f"""
            SELECT process_id FROM `{table_id}`
            WHERE lock_name = @lock_name AND expires_at > CURRENT_TIMESTAMP()
            """

            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("lock_name", "STRING", lock_name)
                ]
            )

            rows = list(self.client.query(check_query, job_config=job_config).result())

            if rows and rows[0].process_id == self.process_id:
                print(f"Lock '{lock_name}' acquired by process {self.process_id}")
                return True
            else:
                print(f"Lock '{lock_name}' held by another process")
                return False

        except Exception as e:
            print(f"Error acquiring lock: {e}")
            return False

    def release_lock(self, lock_name):
        """Release a lock held by this process"""
        table_id = f"{self.project_id}.{self.dataset_id}.job_loader_locks"

        delete_query = f"""
        DELETE FROM `{table_id}`
        WHERE lock_name = @lock_name AND process_id = @process_id
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("lock_name", "STRING", lock_name),
                bigquery.ScalarQueryParameter("process_id", "STRING", self.process_id),
            ]
        )

        self.client.query(delete_query, job_config=job_config).result()
        print(f"Lock '{lock_name}' released by process {self.process_id}")

    def load_jobs_if_needed(self):
        """
        Load jobs if queue is low and we can acquire the lock

        Args:
            min_queue_size (int): Minimum number of jobs before loading more

        Returns:
            bool: True if jobs were loaded, False otherwise
        """
        lock_name = f"{self.scraper}_job_loader"

        # Check current queue size first (optional optimization)
        # You could add a function to check Pub/Sub queue size here

        # Try to acquire lock
        if not self.acquire_lock(lock_name, lock_duration_minutes=3):
            print(f"Another process is already loading {self.scraper} jobs")
            return False

        try:
            print(f"Loading {self.scraper} jobs...")

            # Call the main function from load_pub_sub
            if self.scraper == "landwatch":
                load_jobs_into_pub_sub("landwatch", self.jobs_per_batch)
            elif self.scraper == "zillow":
                load_jobs_into_pub_sub("zillow", self.jobs_per_batch)
            else:
                raise ValueError(f"Unknown scraper type: {self.scraper}")

            print(f"Successfully loaded {self.scraper} jobs")
            return True

        except Exception as e:
            print(f"Error loading {self.scraper} jobs: {e}")
            return False

        finally:
            # Always release the lock
            self.release_lock(lock_name)

    def smart_load_jobs(self, max_retries=3):
        """
        Smart wrapper that tries to load jobs with retries and backoff

        Args:
            max_retries (int): Maximum number of retry attempts

        Returns:
            bool: True if jobs were loaded, False if failed
        """
        for attempt in range(max_retries):
            try:
                success = self.load_jobs_if_needed()
                if success:
                    return True

                if attempt < max_retries - 1:
                    # Exponential backoff
                    wait_time = 2 ** attempt
                    print(f"Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)

            except Exception as e:
                print(f"Attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)

        print(f"Failed to load {self.scraper} jobs after {max_retries} attempts")
        return False
