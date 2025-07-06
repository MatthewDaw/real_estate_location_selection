
from camoufox.sync_api import Camoufox
from real_estate_location_selection.scrapers.zillow.zillow_scraper import Zillow
from real_estate_location_selection.scrapers.utils.common_functions import get_browser

def run_scraper():
    print("zillow scraper started")
    browser = get_browser()
    scraper = Zillow(browser)
    print("preparing zillow scraper tasks")
    scraper.prepare_tasks()
    print("processing zillow scraper tasks")

if __name__ == '__main__':
    run_scraper()

