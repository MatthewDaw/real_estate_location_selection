import os
from scrapers.run_scraper import run_scraper

if __name__ == '__main__':
    batch_size = int(os.getenv('ZILLOW_BATCH_SIZE', '150'))
    run_scraper("zillow", batch_size=batch_size)
