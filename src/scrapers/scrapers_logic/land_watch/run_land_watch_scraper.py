import os
from scrapers.run_scraper import run_scraper

if __name__ == '__main__':
    batch_size = int(os.getenv('LANDWATCH_BATCH_SIZE', '50'))
    run_scraper("landwatch", batch_size=batch_size)
