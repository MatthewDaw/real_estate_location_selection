import time
from real_estate_location_selection.distributed_job_loader import DistributedJobLoader
from real_estate_location_selection.scrapers.land_watch.land_watch_scraper import Landwatch
from real_estate_location_selection.scrapers.zillow.zillow_scraper import Zillow
from real_estate_location_selection.scrapers.utils.common_functions import get_browser
from real_estate_location_selection.scrapers.utils.topic_manager import TopicManager

scrapers_config = {
    "dataset": "real_estate",
    "project_id": "flowing-flame-464314-j5",
    "zillow": {
        "topic_name": "zillow-job-queue",
        "subscription_name": "zillow-job-queue-sub",
        "dead_letter_topic": "zillow-dlq",
        "scraper": Zillow
    },
    "landwatch": {
        "topic_name": "landwatch-job-queue",
        "subscription_name": "landwatch-job-queue-sub",
        "dead_letter_topic": "landwatch-dlq",
        "scraper": Landwatch
    },
}


def pull_from_queue(scraper_source, max_empty_queue_attempts, batch_size):
    topic_manager = TopicManager(
        topic_name=scrapers_config[scraper_source]["topic_name"],
        project_id=scrapers_config['project_id'],
        subscription_name=scrapers_config[scraper_source]['subscription_name'],
        dead_letter_topic=scrapers_config[scraper_source]['dead_letter_topic']
    )
    job_loader = DistributedJobLoader(scrapers_config['project_id'], scraper_source,
                                      dataset_id=scrapers_config['dataset'])
    empty_queue_attempts = 0

    while empty_queue_attempts < max_empty_queue_attempts:
        # Try to pull a batch of messages
        message_batch = None
        for batch in topic_manager.pull_messages_batch(batch_size):
            message_batch = batch
            break  # Only get the first batch from the generator

        if message_batch:
            yield message_batch
            empty_queue_attempts = 0  # Reset counter since we got messages
        else:
            # If no message was found, try to load more jobs
            empty_queue_attempts += 1
            print(f"Queue empty (attempt {empty_queue_attempts}/{max_empty_queue_attempts})")

            # Try to load more jobs
            print(f"Attempting to load more {scraper_source} jobs...")
            job_load_success = job_loader.smart_load_jobs()

            if job_load_success:
                print(f"Successfully loaded more {scraper_source} jobs, continuing...")
                empty_queue_attempts = 0  # Reset counter since jobs were loaded
                time.sleep(2)  # Brief pause to let jobs propagate
            else:
                print(f"Failed to load {scraper_source} jobs")
                if empty_queue_attempts < max_empty_queue_attempts:
                    wait_time = min(30, 5 * empty_queue_attempts)  # Increasing wait, max 30s
                    print(f"Waiting {wait_time} seconds before trying again...")
                    time.sleep(wait_time)


def run_scraper(scraper_source, max_empty_queue_attempts=5, batch_size=30):
    """
    Run scraper with automatic job loading when queue is empty

    Args:
        scraper_source (str): "zillow" or "landwatch"
        max_empty_queue_attempts (int): Maximum times to try loading jobs before terminating
        batch_size (int): Number of messages to process in each batch
    """
    browser = get_browser()
    scraper = scrapers_config[scraper_source]["scraper"](browser)
    topic_manager = TopicManager(
        topic_name=scrapers_config[scraper_source]["topic_name"],
        project_id=scrapers_config['project_id'],
        subscription_name=scrapers_config[scraper_source]['subscription_name'],
        dead_letter_topic=scrapers_config[scraper_source]['dead_letter_topic']
    )
    for message_data_batch in pull_from_queue(scraper_source, max_empty_queue_attempts, batch_size):
        urls = [message_data['data']['input'] for message_data in message_data_batch]
        urls_to_update = scraper.process_urls(urls)
        for message_data in message_data_batch:
            if message_data['data']['input'] in urls_to_update:
                topic_manager.delete_message(message_data['ack_id'])
            else:
                topic_manager.send_to_dead_letter(message_data)
    # Clean up
    browser.close()
