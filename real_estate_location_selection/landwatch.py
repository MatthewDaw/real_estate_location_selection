from camoufox.sync_api import Camoufox
from real_estate_location_selection.scrapers.utils.common_functions import get_browser

def run_scrape():
    scraper = ApartmentListScraper(browser)
    details = scraper.process_tasks("p31370")



