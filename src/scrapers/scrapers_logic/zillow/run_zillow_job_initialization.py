from scrapers.scrapers_logic.utils.common_functions import get_browser
from scrapers.scrapers_logic.zillow.zillow_scraper import Zillow


def run_scraper():
    print("zillow scraper started")
    browser = get_browser()
    scraper = Zillow(browser, ['UT', 'ID', 'NV', 'AZ', 'CO', 'WY'])
    print("preparing zillow scraper tasks")
    scraper.prepare_tasks()
    print("processing zillow scraper tasks")

if __name__ == '__main__':
    run_scraper()

