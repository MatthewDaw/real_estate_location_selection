#!/usr/bin/env python3
"""
Task sender script for sending scraper URLs to Pub/Sub queues.
"""

from datetime import datetime
from real_estate_location_selection.scrapers._scraper import Task
from real_estate_location_selection.scrapers.land_watch.land_watch_scraper import Landwatch
from real_estate_location_selection.scrapers.zillow.zillow_scraper import Zillow

class TaskSender:
    """Sends scraping tasks to Pub/Sub queues from database URLs"""

    def __init__(self, scraper_class, browser=None):
        """
        Initialize with a scraper class (Landwatch or Zillow)

        Args:
            scraper_class: The scraper class (Landwatch or Zillow)
            browser: Browser instance (can be None since we're only using DB operations)
        """
        self.scraper = scraper_class(browser)

    def send_all_tasks(self, limit=1000):
        """
        Fetch URLs from database and send them as tasks to Pub/Sub

        Args:
            limit (int): Maximum number of URLs to fetch and send
        """
        print(f"Fetching up to {limit} URLs for {self.scraper.source}...")

        # Fetch URLs that need to be scraped
        urls = self.scraper._fetch_urls_to_scrape(limit=limit)

        if not urls:
            print("No URLs found to process")
            return 0

        print(f"Found {len(urls)} URLs to send as tasks")

        # Send each URL as a task
        tasks_sent = 0
        for url_row in urls:
            try:
                # Create task
                new_task = Task(
                    source=self.scraper.source,
                    sitemap_updated_at=None,
                    input=url_row.url,
                    now=datetime.utcnow().isoformat(),
                )

                # Send task to Pub/Sub queue
                self.scraper.send_task(new_task)
                tasks_sent += 1

                if tasks_sent % 100 == 0:
                    print(f"Sent {tasks_sent} tasks...")

            except Exception as e:
                print(f"Error sending task for URL {url_row.url}: {e}")
                continue

        print(f"✅ Successfully sent {tasks_sent} tasks to {self.scraper.source} queue")
        return tasks_sent


def load_jobs_into_pub_sub(scraper, limit):
    # Select scraper class
    if scraper == "landwatch":
        scraper_class = Landwatch
    elif scraper == "zillow":
        scraper_class = Zillow
    else:
        raise ValueError(f"Unknown scraper: {scraper}")

    # Create task sender and send tasks
    sender = TaskSender(scraper_class, browser=None)
    tasks_sent = sender.send_all_tasks(limit=limit)

    print(f"Task sending complete. {tasks_sent} tasks sent to queue.")

