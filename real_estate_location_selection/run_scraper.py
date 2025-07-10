import time
import uuid
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


def pull_from_queue(scraper_source, max_empty_queue_attempts, batch_size, process_id):
    """
    Enhanced queue puller with job deduplication
    """
    topic_manager = TopicManager(
        topic_name=scrapers_config[scraper_source]["topic_name"],
        project_id=scrapers_config['project_id'],
        subscription_name=scrapers_config[scraper_source]['subscription_name'],
        dead_letter_topic=scrapers_config[scraper_source]['dead_letter_topic'],
        dataset_id=scrapers_config['dataset']
    )
    job_loader = DistributedJobLoader(scrapers_config['project_id'], scraper_source,
                                      dataset_id=scrapers_config['dataset'])
    empty_queue_attempts = 0

    while empty_queue_attempts < max_empty_queue_attempts:
        # Try to pull a batch of messages with deduplication
        message_batch = None
        for batch in topic_manager.pull_messages_batch(
                batch_size=batch_size,
                scraper_source=scraper_source,
                process_id=process_id
        ):
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
    Enhanced scraper runner with job deduplication and proper tracking

    Args:
        scraper_source (str): "zillow" or "landwatch"
        max_empty_queue_attempts (int): Maximum times to try loading jobs before terminating
        batch_size (int): Number of messages to process in each batch
    """
    # Generate unique process ID for this scraper instance
    process_id = f"{scraper_source}_{str(uuid.uuid4())[:8]}"
    print(f"Starting scraper with process ID: {process_id}")

    browser = get_browser()
    scraper = scrapers_config[scraper_source]["scraper"](browser)
    topic_manager = TopicManager(
        topic_name=scrapers_config[scraper_source]["topic_name"],
        project_id=scrapers_config['project_id'],
        subscription_name=scrapers_config[scraper_source]['subscription_name'],
        dead_letter_topic=scrapers_config[scraper_source]['dead_letter_topic'],
        dataset_id=scrapers_config['dataset']
    )

    # Print initial statistics
    stats = topic_manager.get_job_statistics(scraper_source=scraper_source, hours=1)
    print(f"Job statistics for last hour: {stats}")

    processed_count = 0
    success_count = 0
    error_count = 0

    try:
        for message_data_batch in pull_from_queue(scraper_source, max_empty_queue_attempts, batch_size, process_id):
            urls = [message_data['data']['input'] for message_data in message_data_batch]
            print(f"Processing batch of {len(urls)} URLs")

            # Process URLs and get list of successfully processed ones
            try:
                urls_to_update = scraper.process_urls(urls)

                # Handle each message based on processing result
                for message_data in message_data_batch:
                    url = message_data['data']['input']
                    processed_count += 1

                    if url in urls_to_update:
                        # Successfully processed
                        success = topic_manager.delete_message(
                            message_data['ack_id'],
                            url=url,
                            scraper_source=scraper_source,
                            process_id=process_id,
                            success=True
                        )
                        if success:
                            success_count += 1
                        else:
                            error_count += 1
                            print(f"Failed to acknowledge successful processing of {url}")
                    else:
                        # Failed to process
                        error_count += 1
                        topic_manager.send_to_dead_letter(
                            message_data,
                            error_reason="Failed to process URL",
                            scraper_source=scraper_source,
                            process_id=process_id
                        )

                print(f"Batch complete. Processed: {processed_count}, Success: {success_count}, Errors: {error_count}")

            except Exception as e:
                print(f"Error processing batch: {e}")
                # Send all messages in batch to dead letter queue
                for message_data in message_data_batch:
                    url = message_data['data']['input']
                    error_count += 1
                    topic_manager.send_to_dead_letter(
                        message_data,
                        error_reason=f"Batch processing error: {str(e)}",
                        scraper_source=scraper_source,
                        process_id=process_id
                    )

    except KeyboardInterrupt:
        print(
            f"\nScraper interrupted. Final stats - Processed: {processed_count}, Success: {success_count}, Errors: {error_count}")
    except Exception as e:
        print(f"Scraper error: {e}")
    finally:
        # Clean up
        browser.close()

        # Print final statistics
        final_stats = topic_manager.get_job_statistics(scraper_source=scraper_source, hours=1)
        print(f"Final job statistics for last hour: {final_stats}")
        print(f"This session - Processed: {processed_count}, Success: {success_count}, Errors: {error_count}")


def get_scraper_statistics(scraper_source=None, hours=24):
    """
    Get processing statistics for scrapers

    Args:
        scraper_source (str, optional): Specific scraper to get stats for
        hours (int): Number of hours to look back
    """
    # Use any scraper config to get the project and dataset info
    config_key = scraper_source if scraper_source else 'zillow'  # Default fallback
    if config_key in ['dataset', 'project_id']:
        config_key = 'zillow'

    topic_manager = TopicManager(
        topic_name=scrapers_config[config_key]["topic_name"],
        project_id=scrapers_config['project_id'],
        subscription_name=scrapers_config[config_key]['subscription_name'],
        dead_letter_topic=scrapers_config[config_key]['dead_letter_topic'],
        dataset_id=scrapers_config['dataset']
    )

    stats = topic_manager.get_job_statistics(scraper_source=scraper_source, hours=hours)

    print(f"Job Statistics (last {hours} hours):")
    if scraper_source:
        print(f"  Scraper: {scraper_source}")
    print(f"  Total jobs: {stats['total']}")
    print(f"  Completed: {stats['completed']}")
    print(f"  Failed: {stats['failed']}")
    print(f"  Currently processing: {stats['processing']}")

    if stats['total'] > 0:
        success_rate = (stats['completed'] / stats['total']) * 100
        print(f"  Success rate: {success_rate:.1f}%")

    return stats


def clean_expired_jobs(scraper_source=None):
    """
    Clean up expired processing jobs that may be stuck

    Args:
        scraper_source (str, optional): Specific scraper to clean for
    """
    config_key = scraper_source if scraper_source else 'zillow'
    if config_key in ['dataset', 'project_id']:
        config_key = 'zillow'

    topic_manager = TopicManager(
        topic_name=scrapers_config[config_key]["topic_name"],
        project_id=scrapers_config['project_id'],
        subscription_name=scrapers_config[config_key]['subscription_name'],
        dead_letter_topic=scrapers_config[config_key]['dead_letter_topic'],
        dataset_id=scrapers_config['dataset']
    )

    table_id = f"{scrapers_config['project_id']}.{scrapers_config['dataset']}.job_processing_log"

    # Clean up expired processing jobs
    cleanup_query = f"""
    UPDATE `{table_id}`
    SET status = 'failed', completed_at = CURRENT_TIMESTAMP(), last_error = 'Job expired - process likely crashed'
    WHERE status = 'processing' AND expires_at < CURRENT_TIMESTAMP()
    """

    if scraper_source:
        cleanup_query += f" AND scraper_source = '{scraper_source}'"

    try:
        result = topic_manager.bigquery_client.query(cleanup_query).result()
        print(f"Cleaned up expired processing jobs for {scraper_source or 'all scrapers'}")
        return True
    except Exception as e:
        print(f"Error cleaning up expired jobs: {e}")
        return False


def reset_job_tracking(scraper_source=None, hours=1):
    """
    Reset job tracking for jobs processed within the specified hours
    This allows jobs to be reprocessed immediately

    Args:
        scraper_source (str, optional): Specific scraper to reset for
        hours (int): Number of hours to look back for jobs to reset
    """
    config_key = scraper_source if scraper_source else 'zillow'
    if config_key in ['dataset', 'project_id']:
        config_key = 'zillow'

    topic_manager = TopicManager(
        topic_name=scrapers_config[config_key]["topic_name"],
        project_id=scrapers_config['project_id'],
        subscription_name=scrapers_config[config_key]['subscription_name'],
        dead_letter_topic=scrapers_config[config_key]['dead_letter_topic'],
        dataset_id=scrapers_config['dataset']
    )

    table_id = f"{scrapers_config['project_id']}.{scrapers_config['dataset']}.job_processing_log"

    # Delete recent job tracking entries
    delete_query = f"""
    DELETE FROM `{table_id}`
    WHERE started_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
    """

    if scraper_source:
        delete_query += f" AND scraper_source = '{scraper_source}'"

    try:
        result = topic_manager.bigquery_client.query(delete_query).result()
        print(f"Reset job tracking for {scraper_source or 'all scrapers'} (last {hours} hours)")
        return True
    except Exception as e:
        print(f"Error resetting job tracking: {e}")
        return False


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python scraper_main.py run <zillow|landwatch> [max_empty_attempts] [batch_size]")
        print("  python scraper_main.py stats [scraper_source] [hours]")
        print("  python scraper_main.py clean [scraper_source]")
        print("  python scraper_main.py reset [scraper_source] [hours]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "run":
        if len(sys.argv) < 3:
            print("Error: scraper source required (zillow or landwatch)")
            sys.exit(1)

        scraper_source = sys.argv[2]
        max_empty_attempts = int(sys.argv[3]) if len(sys.argv) > 3 else 5
        batch_size = int(sys.argv[4]) if len(sys.argv) > 4 else 30

        if scraper_source not in ['zillow', 'landwatch']:
            print("Error: scraper source must be 'zillow' or 'landwatch'")
            sys.exit(1)

        run_scraper(scraper_source, max_empty_attempts, batch_size)

    elif command == "stats":
        scraper_source = sys.argv[2] if len(sys.argv) > 2 else None
        hours = int(sys.argv[3]) if len(sys.argv) > 3 else 24
        get_scraper_statistics(scraper_source, hours)

    elif command == "clean":
        scraper_source = sys.argv[2] if len(sys.argv) > 2 else None
        clean_expired_jobs(scraper_source)

    elif command == "reset":
        scraper_source = sys.argv[2] if len(sys.argv) > 2 else None
        hours = int(sys.argv[3]) if len(sys.argv) > 3 else 1
        reset_job_tracking(scraper_source, hours)

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)