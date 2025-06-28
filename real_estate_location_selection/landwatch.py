from camoufox.sync_api import Camoufox

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

def run_scrape():
    scraper = ApartmentListScraper(browser)
    details = scraper.process_tasks("p31370")



