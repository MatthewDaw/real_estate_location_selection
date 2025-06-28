
from camoufox.sync_api import Camoufox
from real_estate_location_selection.scrapers.zillow.zillow_scraper import Zillow


def get_browser():
    browser = Camoufox(
        humanize=True,
        firefox_user_prefs={
            "javascript.enabled": False,
            "permissions.default.image": 2,  # Block images
            "permissions.default.stylesheet": 2,  # Block CSS
            "permissions.default.font": 2,  # Block fonts
            "permissions.default.script": 2,  # Block JavaScript
            "permissions.default.plugin": 2,  # Block plugins
            "permissions.default.autoplay": 2,  # Block autoplay media
            "permissions.default.geo": 2,  # Block geolocation
        },
    ).start()
    return browser

def run_home_details_pass():
    browser = get_browser()
    scraper = Zillow(browser)
    scraper.extract_from_website('https://www.zillow.com/homedetails/25-Scenic-Ln-LOT-32-Hartsville-TN-37074/452763936_zpid/')

def run_scraper():
    browser = get_browser()
    scraper = Zillow(browser)
    scraper.prepare_tasks()
    scraper.process_tasks()
    print("pause")

if __name__ == '__main__':
    run_scraper()

