import time
from distributed_job_loader import DistributedJobLoader
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


def run_scraper(scraper_source, max_empty_queue_attempts=5):
    """
    Run scraper with automatic job loading when queue is empty

    Args:
        scraper_source (str): "zillow" or "landwatch"
        max_empty_queue_attempts (int): Maximum times to try loading jobs before terminating
    """
    browser = get_browser()
    scraper = scrapers_config[scraper_source]["scraper"](browser)
    topic_manager = TopicManager(
        topic_name=scrapers_config[scraper_source]["topic_name"],
        project_id=scrapers_config['project_id'],
        subscription_name=scrapers_config[scraper_source]['subscription_name'],
        dead_letter_topic=scrapers_config[scraper_source]['dead_letter_topic']
    )

    empty_queue_attempts = 0
    messages_processed = 0

    # Initialize job loader once
    job_loader = DistributedJobLoader(scrapers_config['project_id'], scraper_source, dataset_id=scrapers_config['dataset'])
    print(f"Starting {scraper_source} scraper...")

    while empty_queue_attempts < max_empty_queue_attempts:
        # Try to pull a message
        message_found = False

        for message_data in topic_manager.pull_message():
            message_found = True
            messages_processed += 1
            # https://www.zillow.com/homedetails/11814-S-Tuzigoot-Ct-Phoenix-AZ-85044/8150226_zpid/
            try:
                print(f"Processing message {messages_processed}: {message_data['data']['input']}")
                scraper.process_urls([message_data['data']['input']])
                topic_manager.delete_message(message_data['ack_id'])
                print(f"Successfully processed message {messages_processed}")

            except Exception as e:
                print(f"Error processing message {messages_processed}: {e}")
                topic_manager.delete_and_send_to_dlq(message_data)
                # Exception automatically sends to DLQ via generator

            # Reset empty queue counter since we found a message
            empty_queue_attempts = 0

        # If no message was found, try to load more jobs
        if not message_found:
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

    print(f"Terminating {scraper_source} scraper after {max_empty_queue_attempts} empty queue attempts")
    print(f"Total messages processed: {messages_processed}")

    # Clean up
    browser.close()


