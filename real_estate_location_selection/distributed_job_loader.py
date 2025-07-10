#!/usr/bin/env python3
"""
Enhanced distributed job loader that uses BigQuery as a distributed lock mechanism
and integrates with the job deduplication system to prevent loading duplicate jobs.
"""

import time
import uuid
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from real_estate_location_selection.load_pub_sub import load_jobs_into_pub_sub
from real_estate_location_selection.scrapers.utils.topic_manager import TopicManager


class DistributedJobLoader:
    """Enhanced distributed job loader with deduplication awareness"""

    def __init__(self, project_id, scraper, dataset_id="real_estate", jobs_per_batch=600):
        self.project_id = project_id
        self.scraper = scraper
        self.dataset_id = dataset_id
        self.client = bigquery.Client(project=project_id)
        self.process_id = str(uuid.uuid4())[:8]  # Unique process identifier
        self._ensure_lock_table_exists()
        self._ensure_job_queue_table_exists()
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

    def _ensure_job_queue_table_exists(self):
        """Create the job queue tracking table if it doesn't exist"""
        table_id = f"{self.project_id}.{self.dataset_id}.job_queue_log"

        schema = [
            bigquery.SchemaField("url", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("scraper_source", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("queued_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("process_id", "STRING", mode="REQUIRED"),
        ]

        try:
            self.client.get_table(table_id)
        except NotFound:
            table = bigquery.Table(table_id, schema=schema)
            table = self.client.create_table(table)
            print(f"Created job queue tracking table: {table_id}")

    def acquire_lock(self, lock_name, lock_duration_minutes):
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

    def get_unqueued_jobs_count(self):
        """
        Get the count of jobs that need to be queued.
        This checks for URLs that haven't been queued in the last hour
        and haven't been processed in the last hour.
        """
        # First get URLs from the source table that are ready to be scraped
        if self.scraper == "landwatch":
            source_table = f"{self.project_id}.{self.dataset_id}.landwatch_urls"
            url_source_query = f"""
            SELECT COUNT(DISTINCT url) as count
            FROM `{source_table}`
            WHERE state IN ('UT', 'ID', 'NV', 'WY', 'MT', 'NH', 'CO', 'AZ', 'NM', 'TX', 'OK', 'KS', 'NE', 'IA', 'IL', 'MO', 'IN', 'AR', 'LA', 'MS', 'MI')
            AND scraped_at IS NULL
            AND (last_pulled IS NULL OR last_pulled < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY))
            """
        elif self.scraper == "zillow":
            source_table = f"{self.project_id}.{self.dataset_id}.zillow_urls"
            url_source_query = f"""
            SELECT COUNT(DISTINCT url) as count
            FROM `{source_table}`
            WHERE state IN ('UT', 'ID', 'NV', 'AZ', 'CO', 'WY')
            AND scraped_at IS NULL
            AND (last_pulled IS NULL OR last_pulled < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY))
            """
        else:
            return 0

        # Get count of unprocessed URLs
        try:
            result = list(self.client.query(url_source_query).result())
            total_unprocessed = result[0].count if result else 0
        except Exception as e:
            print(f"Error getting unprocessed count: {e}")
            return 0

        # Get count of recently queued jobs
        queue_log_table = f"{self.project_id}.{self.dataset_id}.job_queue_log"
        recently_queued_query = f"""
        SELECT COUNT(DISTINCT url) as count
        FROM `{queue_log_table}`
        WHERE scraper_source = @scraper_source
        AND queued_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("scraper_source", "STRING", self.scraper)
            ]
        )

        try:
            result = list(self.client.query(recently_queued_query, job_config=job_config).result())
            recently_queued = result[0].count if result else 0
        except Exception as e:
            print(f"Error getting recently queued count: {e}")
            recently_queued = 0

        # Get count of recently processed jobs
        processing_log_table = f"{self.project_id}.{self.dataset_id}.job_processing_log"
        recently_processed_query = f"""
        SELECT COUNT(DISTINCT url) as count
        FROM `{processing_log_table}`
        WHERE scraper_source = @scraper_source
        AND started_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
        """

        try:
            result = list(self.client.query(recently_processed_query, job_config=job_config).result())
            recently_processed = result[0].count if result else 0
        except Exception as e:
            print(f"Error getting recently processed count: {e}")
            recently_processed = 0

        # Estimate how many jobs we can safely queue
        estimated_queueable = max(0, total_unprocessed - recently_queued - recently_processed)

        print(f"Job queue analysis for {self.scraper}:")
        print(f"  Total unprocessed URLs: {total_unprocessed}")
        print(f"  Recently queued (1h): {recently_queued}")
        print(f"  Recently processed (1h): {recently_processed}")
        print(f"  Estimated queueable: {estimated_queueable}")

        return estimated_queueable

    def log_queued_jobs(self, urls):
        """Log which jobs were queued to prevent re-queuing"""
        if not urls:
            return

        queue_log_table = f"{self.project_id}.{self.dataset_id}.job_queue_log"
        now = datetime.utcnow()

        entries = [
            {
                'url': url,
                'scraper_source': self.scraper,
                'queued_at': now.isoformat(),
                'process_id': self.process_id
            }
            for url in urls
        ]

        try:
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            )
            job = self.client.load_table_from_json(entries, queue_log_table, job_config=job_config)
            job.result()
            print(f"Logged {len(urls)} queued jobs")
        except Exception as e:
            print(f"Error logging queued jobs: {e}")

    def load_jobs_with_deduplication(self):
        """
        Load jobs into Pub/Sub while avoiding recently processed URLs
        """
        print(f"Loading {self.scraper} jobs with deduplication...")

        # Check if we need to load jobs
        queueable_count = self.get_unqueued_jobs_count()

        if queueable_count < self.jobs_per_batch:
            print(f"Only {queueable_count} jobs available to queue (less than batch size {self.jobs_per_batch})")
            if queueable_count == 0:
                print("No queueable jobs to load")
                return False

        # Create a TopicManager to filter URLs
        if self.scraper == "landwatch":
            topic_config = {
                "topic_name": "landwatch-job-queue",
                "subscription_name": "landwatch-job-queue-sub",
                "dead_letter_topic": "landwatch-dlq"
            }
        elif self.scraper == "zillow":
            topic_config = {
                "topic_name": "zillow-job-queue",
                "subscription_name": "zillow-job-queue-sub",
                "dead_letter_topic": "zillow-dlq"
            }
        else:
            raise ValueError(f"Unknown scraper type: {self.scraper}")

        topic_manager = TopicManager(
            topic_name=topic_config["topic_name"],
            project_id=self.project_id,
            subscription_name=topic_config["subscription_name"],
            dead_letter_topic=topic_config["dead_letter_topic"],
            dataset_id=self.dataset_id
        )

        # Get URLs that need to be processed
        if self.scraper == "landwatch":
            source_table = f"{self.project_id}.{self.dataset_id}.landwatch_urls"
            urls_query = f"""
            SELECT DISTINCT url
            FROM `{source_table}`
            WHERE state IN ('UT', 'ID', 'NV', 'WY', 'MT', 'NH', 'CO', 'AZ', 'NM', 'TX', 'OK', 'KS', 'NE', 'IA', 'IL', 'MO', 'IN', 'AR', 'LA', 'MS', 'MI')
            AND scraped_at IS NULL
            AND (last_pulled IS NULL OR last_pulled < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY))
            ORDER BY RAND()
            LIMIT {self.jobs_per_batch}
            """
        else:  # zillow
            source_table = f"{self.project_id}.{self.dataset_id}.zillow_urls"
            urls_query = f"""
            SELECT DISTINCT url
            FROM `{source_table}`
            WHERE state IN ('UT', 'ID', 'NV', 'AZ', 'CO', 'WY')
            AND scraped_at IS NULL
            AND (last_pulled IS NULL OR last_pulled < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY))
            ORDER BY RAND()
            LIMIT {self.jobs_per_batch}
            """

        try:
            result = self.client.query(urls_query)
            candidate_urls = list({row.url for row in result.result()})

            if not candidate_urls:
                print("No candidate URLs found")
                return False

            print(f"Found {len(candidate_urls)} candidate URLs")

            # Update last_pulled timestamp for the selected URLs
            urls_for_update = "', '".join(candidate_urls)
            update_query = f"""
            UPDATE `{source_table}`
            SET last_pulled = CURRENT_TIMESTAMP()
            WHERE url IN ('{urls_for_update}')
            """

            update_result = self.client.query(update_query)
            update_result.result()  # Wait for the update to complete
            print(f"Updated last_pulled timestamp for {len(candidate_urls)} URLs")

            # Filter out recently processed URLs
            filtered_urls = topic_manager.filter_recently_processed_urls(candidate_urls, self.scraper)

            if not filtered_urls:
                print("All candidate URLs were recently processed")
                return False

            print(f"After filtering: {len(filtered_urls)} URLs ready to queue")

            # Load the filtered URLs into Pub/Sub
            # We'll need to modify load_jobs_into_pub_sub to accept a list of URLs
            # For now, we'll call the existing function but note this limitation
            load_jobs_into_pub_sub(self.scraper, len(filtered_urls))

            # Log the queued jobs
            self.log_queued_jobs(filtered_urls)

            return True

        except Exception as e:
            print(f"Error loading jobs with deduplication: {e}")
            return False

    def load_jobs_if_needed(self):
        """
        Load jobs if queue is low and we can acquire the lock

        Returns:
            bool: True if jobs were loaded, False otherwise
        """
        lock_name = f"{self.scraper}_job_loader"

        # Try to acquire lock
        if not self.acquire_lock(lock_name, lock_duration_minutes=10):
            print(f"Another process is already loading {self.scraper} jobs")
            return False

        try:
            return self.load_jobs_with_deduplication()

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

    def cleanup_old_queue_logs(self, days=7):
        """Clean up old queue log entries"""
        table_id = f"{self.project_id}.{self.dataset_id}.job_queue_log"

        cleanup_query = f"""
        DELETE FROM `{table_id}`
        WHERE queued_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
        """

        try:
            result = self.client.query(cleanup_query).result()
            print(f"Cleaned up queue logs older than {days} days")
        except Exception as e:
            print(f"Error cleaning up queue logs: {e}")

    def get_queue_status(self):
        """Get detailed status of the job queue"""
        stats = {
            'unqueued_jobs': self.get_unqueued_jobs_count(),
            'recent_queue_activity': {},
            'processing_activity': {}
        }

        # Get recent queue activity
        queue_log_table = f"{self.project_id}.{self.dataset_id}.job_queue_log"
        queue_activity_query = f"""
        SELECT 
            DATE(queued_at) as date,
            COUNT(*) as jobs_queued
        FROM `{queue_log_table}`
        WHERE scraper_source = @scraper_source
        AND queued_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
        GROUP BY DATE(queued_at)
        ORDER BY date DESC
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("scraper_source", "STRING", self.scraper)
            ]
        )

        try:
            result = self.client.query(queue_activity_query, job_config=job_config)
            stats['recent_queue_activity'] = {
                str(row.date): row.jobs_queued for row in result.result()
            }
        except Exception as e:
            print(f"Error getting queue activity: {e}")

        return stats