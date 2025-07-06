
from camoufox.sync_api import Camoufox
from real_estate_location_selection.scrapers.land_watch.land_watch_scraper import Landwatch
from scrapers.utils.common_functions import get_browser

urls = [
    "/alabama-land-for-sale",
    "/alaska-land-for-sale",
    "/arizona-land-for-sale",
    "/arkansas-land-for-sale",
    "/california-land-for-sale",
    "/colorado-land-for-sale",
    "/connecticut-land-for-sale",
    "/delaware-land-for-sale",
    "/florida-land-for-sale",
    "/georgia-land-for-sale",
    "/hawaii-land-for-sale",
    "/idaho-land-for-sale",
    "/illinois-land-for-sale",
    "/indiana-land-for-sale",
    "/iowa-land-for-sale",
    "/kansas-land-for-sale",
    "/kentucky-land-for-sale",
    "/louisiana-land-for-sale",
    "/maine-land-for-sale",
    "/maryland-land-for-sale",
    "/massachusetts-land-for-sale",
    "/michigan-land-for-sale",
    "/minnesota-land-for-sale",
    "/mississippi-land-for-sale",
    "/missouri-land-for-sale",
    "/montana-land-for-sale",
    "/nebraska-land-for-sale",
    "/nevada-land-for-sale",
    "/new-hampshire-land-for-sale",
    "/new-jersey-land-for-sale",
    "/new-mexico-land-for-sale",
    "/new-york-land-for-sale",
    "/north-carolina-land-for-sale",
    "/north-dakota-land-for-sale",
    "/ohio-land-for-sale",
    "/oklahoma-land-for-sale",
    "/oregon-land-for-sale",
    "/pennsylvania-land-for-sale",
    "/rhode-island-land-for-sale",
    "/south-carolina-land-for-sale",
    "/south-dakota-land-for-sale",
    "/tennessee-land-for-sale",
    "/texas-land-for-sale",
    "/utah-land-for-sale",
    "/vermont-land-for-sale",
    "/virginia-land-for-sale",
    "/washington-land-for-sale",
    "/west-virginia-land-for-sale",
    "/wisconsin-land-for-sale",
    "/wyoming-land-for-sale"
]

state_abbreviations = [
    "AL",  # Alabama
    "AK",  # Alaska
    "AZ",  # Arizona
    "AR",  # Arkansas
    "CA",  # California
    "CO",  # Colorado
    "CT",  # Connecticut
    "DE",  # Delaware
    "FL",  # Florida
    "GA",  # Georgia
    "HI",  # Hawaii
    "ID",  # Idaho
    "IL",  # Illinois
    "IN",  # Indiana
    "IA",  # Iowa
    "KS",  # Kansas
    "KY",  # Kentucky
    "LA",  # Louisiana
    "ME",  # Maine
    "MD",  # Maryland
    "MA",  # Massachusetts
    "MI",  # Michigan
    "MN",  # Minnesota
    "MS",  # Mississippi
    "MO",  # Missouri
    "MT",  # Montana
    "NE",  # Nebraska
    "NV",  # Nevada
    "NH",  # New Hampshire
    "NJ",  # New Jersey
    "NM",  # New Mexico
    "NY",  # New York
    "NC",  # North Carolina
    "ND",  # North Dakota
    "OH",  # Ohio
    "OK",  # Oklahoma
    "OR",  # Oregon
    "PA",  # Pennsylvania
    "RI",  # Rhode Island
    "SC",  # South Carolina
    "SD",  # South Dakota
    "TN",  # Tennessee
    "TX",  # Texas
    "UT",  # Utah
    "VT",  # Vermont
    "VA",  # Virginia
    "WA",  # Washington
    "WV",  # West Virginia
    "WI",  # Wisconsin
    "WY"   # Wyoming
]

def prepare_tasks(scraper):
    for url, state_abbreviation in zip(urls, state_abbreviations):
        if state_abbreviation == 'UT':
            scraper.prepare_tasks(url, state_abbreviation)

def run_scraper():
    print("scraper started")
    browser = get_browser()
    scraper = Landwatch(browser)
    print("preparing tasks")
    prepare_tasks(scraper)

if __name__ == '__main__':
    run_scraper()
