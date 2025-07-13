
import nest_asyncio
nest_asyncio.apply()

import uuid
from real_estate_location_selection.scrapers.land_watch.land_watch_scraper import Landwatch
from real_estate_location_selection.scrapers.zillow.zillow_scraper import Zillow
from real_estate_location_selection.scrapers.utils.common_functions import get_browser
from google.cloud import bigquery
from datetime import datetime
from typing import List
import time

from real_estate_location_selection.scrapers.utils.big_query_wrapper import create_client

scrapers_config = {
    "dataset": "real_estate",
    "project_id": "flowing-flame-464314-j5",
    "zillow": {
        "topic_name": "zillow-job-queue",
        "subscription_name": "zillow-job-queue-sub",
        "dead_letter_topic": "zillow-dlq",
        "scraper": Zillow,
        "table_name": "zillow_urls",
        "states": ['UT', 'ID', 'NV', 'AZ', 'CO', 'WY']
    },
    "landwatch": {
        "topic_name": "landwatch-job-queue",
        "subscription_name": "landwatch-job-queue-sub",
        "dead_letter_topic": "landwatch-dlq",
        "scraper": Landwatch,
        "table_name": "landwatch_urls",
        "states": ['UT', 'ID', 'NV', 'WY', 'MT', 'NH', 'CO', 'AZ', 'NM', 'TX', 'OK', 'KS', 'NE', 'IA', 'IL', 'MO', 'IN', 'AR', 'LA', 'MS', 'MI']
    },
}


def pull_from_queue(scraper_source: str, batch_size: int, process_id: str) -> List[str]:
    """
    Enhanced queue puller with job deduplication and race-condition safety
    """
    client = create_client(project=scrapers_config['project_id'])

    source_config = scrapers_config[scraper_source]
    source_table = f"{scrapers_config['project_id']}.{scrapers_config['dataset']}.{source_config['table_name']}"
    states = source_config['states']

    while True:
        # Use both process_id and precise timestamp for maximum uniqueness
        unique_timestamp = datetime.utcnow()

        # Step 1: Update with both process_id and timestamp as markers
        states_str = "', '".join(states)
        update_query = f"""
                UPDATE `{source_table}`
                SET 
                    last_pulled = @update_timestamp,
                    processing_id = @process_id
                WHERE url IN (
                  SELECT DISTINCT url
                  FROM (
                    SELECT url,
                    FROM `flowing-flame-464314-j5.real_estate.landwatch_urls`
                    WHERE state IN ('{states_str}')
                    -- Only include URLs where ALL rows have scraped_at IS NULL
                    AND url NOT IN (
                      SELECT DISTINCT url 
                      FROM `{source_table}` 
                      WHERE scraped_at IS NOT NULL
                      OR 
                      last_pulled > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 HOUR)
                    )
                  ) filtered
                  ORDER BY RAND()
                  LIMIT @batch_size
                )
                """

        update_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("update_timestamp", "TIMESTAMP", unique_timestamp),
                bigquery.ScalarQueryParameter("process_id", "STRING", process_id),
                bigquery.ScalarQueryParameter("batch_size", "INT64", batch_size)
            ]
        )

        update_result = client.query(update_query, job_config=update_config).result()
        rows_updated = update_result.num_dml_affected_rows
        print(f"Updated {rows_updated} rows for {scraper_source} with process_id {process_id}")

        if rows_updated == 0:
            print(f"No URLs available for {scraper_source}")
            break

        # sleep for a couple seconds so that if multiple threads updated this at the same time, the new process id
        # will be updated and we avoid pulling that job here
        time.sleep(5)

        # Step 2: Get URLs that THIS process actually claimed
        select_query = f"""
        SELECT distinct url
        FROM `{source_table}`
        WHERE last_pulled = @update_timestamp 
        AND processing_id = @process_id
        AND scraped_at IS NULL
        ORDER BY url
        """

        select_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("update_timestamp", "TIMESTAMP", unique_timestamp),
                bigquery.ScalarQueryParameter("process_id", "STRING", process_id)
            ]
        )

        result = client.query(select_query, job_config=select_config).result()
        urls = [row.url for row in result]
        print(f"Retrieved {len(urls)} URLs for {scraper_source} processing")
        yield urls


def run(scraper_source, batch_size, browser):
    """
    Enhanced scraper runner with job deduplication and proper tracking

    Args:
        scraper_source (str): "zillow" or "landwatch"
        max_empty_queue_attempts (int): Maximum times to try loading jobs before terminating
        batch_size (int): Number of messages to process in each batch
    """
    # Generate unique process ID for this scraper instance
    process_id = f"{scraper_source}_{str(uuid.uuid4())[:8]}"
    print(f"Starting scraper with process ID: {process_id}")

    scraper = scrapers_config[scraper_source]["scraper"](browser, scrapers_config[scraper_source]["states"])

    processed_count = 0
    success_count = 0
    error_count = 0
    retry_count = 0
    while retry_count < 5:
        for urls in pull_from_queue(scraper_source, batch_size, process_id):
            if urls:
                retry_count = 0
                print(f"Processing batch of {len(urls)} URLs")
                processed_count += len(urls)
                # Process URLs and get list of successfully processed ones
                successfully_scraped_urls = scraper.process_urls(urls)
                for url in urls:
                    if url in successfully_scraped_urls:
                        success_count += 1
                    else:
                        error_count += 1
                        print(f"Failed to acknowledge successful processing of {url}")
                print(f"Batch complete. Processed: {processed_count}, Success: {success_count}, Errors: {error_count}")
            else:
                retry_count += 1
                time.sleep(10)


# google cloud will sometimes crash due to connectivity errors
# there isn't really anything that we can do about this, other than
# just restart
def run_scraper(scraper_source, batch_size):
    completed = False
    attempts = 0
    browser = get_browser()
    start_time = time.time()
    while not completed:
        try:
            run(scraper_source, batch_size, browser)
            completed = True
        except Exception as ex:
            if attempts > 10:
                raise ex
            if time.time() - start_time < 240:
                attempts += 1
            start_time = time.time()
            print(f"Exception encountered: {ex}")
            time.sleep(20)
