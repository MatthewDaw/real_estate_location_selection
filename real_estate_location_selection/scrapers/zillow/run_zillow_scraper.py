
from real_estate_location_selection.scrapers.zillow.zillow_scraper import Zillow
from scrapers.utils.common_functions import get_browser

# # projects/flowing-flame-464314-j5/topics/landwatch-job-queue
# # projects/flowing-flame-464314-j5/subscriptions/landwatch-job-queue-sub
#
# # projects/flowing-flame-464314-j5/subscriptions/landwatch-job-queue-sub
# # projects/flowing-flame-464314-j5/topics/landwatch-job-queue
# # subscription_name: str, dead_letter_topic
# # projects/flowing-flame-464314-j5/topics/landwatch-dlq
# def run_scraper():
#     TopicManager(project_id='flowing-flame-464314-j5', subscription_name='landwatch-job-queue-sub', dead_letter_topic='landwatch-dlq')

def run_scraper():
    print("zillow scraper started")
    browser = get_browser()
    scraper = Zillow(browser)
    print("preparing zillow scraper tasks")
    scraper.prepare_tasks()
    print("processing zillow scraper tasks")
    # scraper.process_tasks()
    print("pause")

if __name__ == '__main__':
    run_scraper()

