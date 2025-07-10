from google.cloud import pubsub_v1
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import json
import time
import hashlib
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta


class TopicManager:
    """Enhanced Pub/Sub topic management with job deduplication and 1-hour minimum retry interval"""

    def __init__(self, topic_name: str, project_id: str, subscription_name: str,
                 dead_letter_topic: str = None, dataset_id: str = "real_estate"):
        self.topic_name = topic_name
        self.project_id = project_id
        self.subscription_name = subscription_name
        self.dead_letter_topic = dead_letter_topic
        self.dataset_id = dataset_id

        self.publisher = pubsub_v1.PublisherClient()
        self.subscriber = pubsub_v1.SubscriberClient()
        self.bigquery_client = bigquery.Client(project=project_id)

        self.subscription_path = self.subscriber.subscription_path(project_id, subscription_name)
        if dead_letter_topic:
            self.dead_letter_topic_path = self.publisher.topic_path(project_id, dead_letter_topic)

        self._ensure_job_tracking_table_exists()

    def _ensure_job_tracking_table_exists(self):
        """Create the job tracking table if it doesn't exist"""
        table_id = f"{self.project_id}.{self.dataset_id}.job_processing_log"

        schema = [
            bigquery.SchemaField("job_hash", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("url", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("scraper_source", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("status", "STRING", mode="REQUIRED"),  # 'processing', 'completed', 'failed'
            bigquery.SchemaField("process_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("started_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("completed_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("expires_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("retry_count", "INTEGER", mode="REQUIRED", default_value_expression="0"),
            bigquery.SchemaField("last_error", "STRING", mode="NULLABLE"),
        ]

        try:
            self.bigquery_client.get_table(table_id)
        except NotFound:
            table = bigquery.Table(table_id, schema=schema)
            table = self.bigquery_client.create_table(table)
            print(f"Created job tracking table: {table_id}")

    def _generate_job_hash(self, url: str, scraper_source: str) -> str:
        """Generate a unique hash for a job based on URL and scraper source"""
        combined = f"{scraper_source}:{url}"
        return hashlib.sha256(combined.encode('utf-8')).hexdigest()

    def _is_job_recently_processed(self, url: str, scraper_source: str) -> bool:
        """
        Check if a job has been processed or is currently being processed within the last hour

        Returns True if the job should be skipped (recently processed or currently processing)
        """
        job_hash = self._generate_job_hash(url, scraper_source)
        table_id = f"{self.project_id}.{self.dataset_id}.job_processing_log"

        # Check if job was completed in the last hour OR is currently being processed
        query = f"""
        SELECT COUNT(*) as count
        FROM `{table_id}`
        WHERE job_hash = @job_hash
        AND (
            (status = 'completed' AND completed_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR))
            OR (status = 'processing' AND expires_at > CURRENT_TIMESTAMP())
        )
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("job_hash", "STRING", job_hash)
            ]
        )

        try:
            query_job = self.bigquery_client.query(query, job_config=job_config)
            result = list(query_job.result())
            return result[0].count > 0
        except Exception as e:
            print(f"Error checking job status: {e}")
            # If we can't check, err on the side of caution and allow processing
            return False

    def _mark_jobs_as_processing_batch(self, urls: List[str], scraper_source: str, process_id: str) -> List[str]:
        if not urls:
            return []

        table_id = f"{self.project_id}.{self.dataset_id}.job_processing_log"
        now = datetime.utcnow()
        expires_at = now + timedelta(minutes=90)

        # Clean up expired jobs first
        cleanup_query = f"""
        DELETE FROM `{table_id}`
        WHERE status = 'processing' AND expires_at < CURRENT_TIMESTAMP()
        """
        try:
            self.bigquery_client.query(cleanup_query).result()
        except Exception as e:
            print(f"Warning: Could not clean up expired jobs: {e}")

        # Insert jobs one by one to maintain the original conflict resolution logic
        successful_urls = []

        for url in urls:
            job_hash = self._generate_job_hash(url, scraper_source)

            # Check if job already exists first
            check_existing_query = f"""
            SELECT COUNT(*) as count FROM `{table_id}`
            WHERE job_hash = @job_hash 
            AND (
                (status = 'completed' AND completed_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR))
                OR (status = 'processing' AND expires_at > CURRENT_TIMESTAMP())
            )
            """

            check_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("job_hash", "STRING", job_hash)
                ]
            )

            try:
                check_result = self.bigquery_client.query(check_existing_query, job_config=check_config).result()
                existing_count = list(check_result)[0].count

                if existing_count > 0:
                    continue  # Skip this URL, it's already processing or recently completed

            except Exception as e:
                print(f"Error checking existing job for {url}: {e}")
                continue

            # Insert the job
            insert_query = f"""
            INSERT INTO `{table_id}` (job_hash, url, scraper_source, status, process_id, started_at, expires_at, retry_count)
            VALUES (@job_hash, @url, @scraper_source, 'processing', @process_id, @started_at, @expires_at, 0)
            """

            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("job_hash", "STRING", job_hash),
                    bigquery.ScalarQueryParameter("url", "STRING", url),
                    bigquery.ScalarQueryParameter("scraper_source", "STRING", scraper_source),
                    bigquery.ScalarQueryParameter("process_id", "STRING", process_id),
                    bigquery.ScalarQueryParameter("started_at", "TIMESTAMP", now),
                    bigquery.ScalarQueryParameter("expires_at", "TIMESTAMP", expires_at),
                ]
            )

            try:
                result = self.bigquery_client.query(insert_query, job_config=job_config).result()
                # If we get here without error, the row was inserted
                successful_urls.append(url)
            except Exception as e:
                print(f"Error inserting job for {url}: {e}")
                continue

        print(f"Successfully marked {len(successful_urls)}/{len(urls)} jobs as processing")
        return successful_urls

    def _mark_job_as_completed(self, url: str, scraper_source: str, process_id: str, success: bool = True,
                               error_message: str = None):
        """Mark a job as completed (successfully or failed)"""
        job_hash = self._generate_job_hash(url, scraper_source)
        table_id = f"{self.project_id}.{self.dataset_id}.job_processing_log"

        status = 'completed' if success else 'failed'
        now = datetime.utcnow()

        update_query = f"""
        UPDATE `{table_id}`
        SET status = @status, completed_at = @completed_at, last_error = @last_error
        WHERE job_hash = @job_hash AND process_id = @process_id AND status = 'processing'
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("job_hash", "STRING", job_hash),
                bigquery.ScalarQueryParameter("process_id", "STRING", process_id),
                bigquery.ScalarQueryParameter("status", "STRING", status),
                bigquery.ScalarQueryParameter("completed_at", "TIMESTAMP", now),
                bigquery.ScalarQueryParameter("last_error", "STRING", error_message),
            ]
        )

        try:
            self.bigquery_client.query(update_query, job_config=job_config).result()
        except Exception as e:
            print(f"Error marking job as completed: {e}")

    def filter_recently_processed_urls(self, urls: List[str], scraper_source: str) -> List[str]:
        """
        Filter out URLs that have been processed within the last hour

        Args:
            urls: List of URLs to check
            scraper_source: The scraper source name

        Returns:
            List of URLs that are safe to process
        """
        if not urls:
            return []

        # Generate hashes for all URLs
        url_to_hash = {url: self._generate_job_hash(url, scraper_source) for url in urls}
        hashes = list(url_to_hash.values())

        table_id = f"{self.project_id}.{self.dataset_id}.job_processing_log"

        # Check which jobs have been recently processed
        query = f"""
        SELECT job_hash
        FROM `{table_id}`
        WHERE job_hash IN UNNEST(@job_hashes)
        AND (
            (status = 'completed' AND completed_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR))
            OR (status = 'processing' AND expires_at > CURRENT_TIMESTAMP())
        )
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("job_hashes", "STRING", hashes)
            ]
        )

        try:
            query_job = self.bigquery_client.query(query, job_config=job_config)
            recently_processed_hashes = {row.job_hash for row in query_job.result()}

            # Return URLs whose hashes are NOT in the recently processed set
            filtered_urls = list({
                url for url, job_hash in url_to_hash.items()
                if job_hash not in recently_processed_hashes
            })

            if len(filtered_urls) < len(urls):
                print(f"Filtered out {len(urls) - len(filtered_urls)} recently processed URLs")

            return filtered_urls

        except Exception as e:
            print(f"Error filtering URLs: {e}")
            # If we can't check, return all URLs to be safe
            return urls

    def pull_messages_batch(self, batch_size=10, scraper_source: str = None, process_id: str = None):
        """
        Enhanced generator that yields a batch of message data with deduplication.
        Automatically filters out jobs that have been processed within the last hour.
        """
        pull_request = {
            "subscription": self.subscription_path,
            "max_messages": batch_size * 2  # Pull more to account for filtering
        }

        response = self.subscriber.pull(request=pull_request)

        if not response.received_messages:
            return

        batch_messages = []
        urls_to_check = []

        # First pass: collect all URLs and parse messages
        for received_message in response.received_messages:
            message = received_message.message

            try:
                # Try to parse as JSON, fall back to string
                data = json.loads(message.data.decode('utf-8'))
            except (json.JSONDecodeError, UnicodeDecodeError):
                data = message.data.decode('utf-8', errors='ignore')

            message_data = {
                'data': data,
                'attributes': dict(message.attributes),
                'message_id': message.message_id,
                'publish_time': message.publish_time,
                'ack_id': received_message.ack_id
            }

            # Extract URL from message data
            url = None
            if isinstance(data, dict) and 'input' in data:
                url = data['input']
            elif isinstance(data, str):
                # Try to parse as URL directly
                url = data

            if url:
                urls_to_check.append(url)
                batch_messages.append((message_data, url))
            else:
                # If we can't extract URL, include it anyway
                batch_messages.append((message_data, None))

        # Filter out recently processed URLs
        if scraper_source and urls_to_check:
            valid_urls = set(self.filter_recently_processed_urls(urls_to_check, scraper_source))
        else:
            valid_urls = set(urls_to_check)

        # Second pass: collect URLs that need processing and build batches
        final_batch = []
        messages_to_nack = []
        urls_to_mark = []
        url_to_message = {}

        for message_data, url in batch_messages:
            if url is None:
                # Include messages where we couldn't extract URL
                final_batch.append(message_data)
            elif url in valid_urls:
                if process_id and scraper_source:
                    # Collect for batch processing
                    urls_to_mark.append(url)
                    url_to_message[url] = message_data
                else:
                    # No process tracking, just include it
                    final_batch.append(message_data)
            else:
                # URL was recently processed, nack the message
                messages_to_nack.append(message_data)

        # Mark all valid URLs as processing in one batch call
        if urls_to_mark and process_id and scraper_source:
            successfully_marked_urls = set(
                self._mark_jobs_as_processing_batch(urls_to_mark, scraper_source, process_id))

            # Add successfully marked messages to final batch, nack the rest
            for url in urls_to_mark:
                message_data = url_to_message[url]
                if url in successfully_marked_urls:
                    final_batch.append(message_data)
                else:
                    # Another process grabbed it or other conflict
                    messages_to_nack.append(message_data)

        # Nack messages that shouldn't be processed right now
        for message_data in messages_to_nack:
            try:
                self.subscriber.modify_ack_deadline(
                    subscription=self.subscription_path,
                    ack_ids=[message_data['ack_id']],
                    ack_deadline_seconds=0  # Nack immediately
                )
            except Exception as e:
                print(f"Error nacking message: {e}")

        # Only yield if we have valid messages
        if final_batch and len(final_batch) <= batch_size:
            yield final_batch
        elif final_batch:
            # If we have too many, yield only the requested batch size
            yield final_batch[:batch_size]

    def delete_message(self, ack_id: str, url: str = None, scraper_source: str = None,
                       process_id: str = None, success: bool = True) -> bool:
        """
        Enhanced delete (acknowledge) a message from the subscription and mark job as completed.
        """
        try:
            # Mark job as completed in tracking table
            if url and scraper_source and process_id:
                self._mark_job_as_completed(url, scraper_source, process_id, success=success)

            # Acknowledge the message
            self.subscriber.acknowledge(
                subscription=self.subscription_path,
                ack_ids=[ack_id]
            )
            return True
        except Exception as e:
            print(f"Error deleting message: {e}")
            return False

    def send_to_dead_letter(self, message_data: Dict[str, Any], error_reason: str = None,
                            scraper_source: str = None, process_id: str = None) -> bool:
        """
        Enhanced send message to dead letter queue and mark job as failed.
        """
        # Extract URL if possible
        url = None
        data = message_data.get('data', {})
        if isinstance(data, dict) and 'input' in data:
            url = data['input']
        elif isinstance(data, str):
            url = data

        # Mark job as failed in tracking table
        if url and scraper_source and process_id:
            self._mark_job_as_completed(url, scraper_source, process_id, success=False, error_message=error_reason)

        if not self.dead_letter_topic:
            print("No dead letter topic configured, deleting message instead")
            return self.delete_message(message_data['ack_id'], url, scraper_source, process_id, success=False)

        try:
            # Create the dead letter message payload
            dlq_payload = {
                'original_data': message_data['data'],
                'original_attributes': message_data['attributes'],
                'original_message_id': message_data['message_id'],
                'original_publish_time': str(message_data['publish_time']),
                'error_reason': error_reason,
                'failed_at': time.time(),
                'dlq_source': 'scraper_error'
            }

            # Publish to dead letter topic
            future = self.publisher.publish(
                self.dead_letter_topic_path,
                json.dumps(dlq_payload).encode('utf-8'),
                **message_data['attributes']  # Preserve original attributes
            )

            # Wait for publish to complete
            message_id = future.result()
            print(f"Message {message_data['message_id']} sent to dead letter queue with ID: {message_id}")

            # Acknowledge the original message to remove it from the subscription
            success = self.delete_message(message_data['ack_id'], url, scraper_source, process_id, success=False)

            return success

        except Exception as e:
            print(f"Failed to send message to dead letter queue: {e}")
            # In case of DLQ failure, still acknowledge the original message to prevent infinite retry
            return self.delete_message(message_data['ack_id'], url, scraper_source, process_id, success=False)

    def get_job_statistics(self, scraper_source: str = None, hours: int = 24) -> Dict[str, int]:
        """Get statistics about job processing over the specified time period"""
        table_id = f"{self.project_id}.{self.dataset_id}.job_processing_log"

        where_clause = f"WHERE started_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)"
        if scraper_source:
            where_clause += f" AND scraper_source = '{scraper_source}'"

        query = f"""
        SELECT 
            status,
            COUNT(*) as count
        FROM `{table_id}`
        {where_clause}
        GROUP BY status
        """

        try:
            query_job = self.bigquery_client.query(query)
            results = {row.status: row.count for row in query_job.result()}

            return {
                'completed': results.get('completed', 0),
                'failed': results.get('failed', 0),
                'processing': results.get('processing', 0),
                'total': sum(results.values())
            }
        except Exception as e:
            print(f"Error getting job statistics: {e}")
            return {'completed': 0, 'failed': 0, 'processing': 0, 'total': 0}

    # Keep all other existing methods unchanged
    def pull_message(self):
        """Original single message pull method - kept for compatibility"""
        pull_request = {
            "subscription": self.subscription_path,
            "max_messages": 1
        }

        response = self.subscriber.pull(request=pull_request)

        if not response.received_messages:
            return

        received_message = response.received_messages[0]
        message = received_message.message

        try:
            data = json.loads(message.data.decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError):
            data = message.data.decode('utf-8', errors='ignore')

        message_data = {
            'data': data,
            'attributes': dict(message.attributes),
            'message_id': message.message_id,
            'publish_time': message.publish_time,
            'ack_id': received_message.ack_id
        }

        try:
            yield message_data
        except Exception as e:
            print(f"Error processing message {message_data['message_id']}: {e}")
            if self.dead_letter_topic:
                self.send_to_dead_letter(message_data, str(e))
            else:
                self.delete_message(message_data['ack_id'])
            raise

    def delete_and_send_to_dlq(self, message_data: Dict[str, Any]) -> bool:
        """Legacy method - kept for compatibility"""
        return self.send_to_dead_letter(message_data)

    def empty_topic(self) -> int:
        """Empty all messages from the current subscription - kept unchanged"""
        total = 0
        while True:
            response = self.subscriber.pull(
                request={
                    "subscription": self.subscription_path,
                    "max_messages": 100
                }
            )

            if not response.received_messages:
                break

            ack_ids = [msg.ack_id for msg in response.received_messages]
            self.subscriber.acknowledge(subscription=self.subscription_path, ack_ids=ack_ids)

            total += len(ack_ids)
            print(f"Deleted {len(ack_ids)} messages (total: {total})")

        print(f"✅ Emptied subscription '{self.subscription_name}': {total} messages deleted")
        return total