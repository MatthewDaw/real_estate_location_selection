from real_estate_location_selection.run_scraper import run_scraper

if __name__ == '__main__':
    run_scraper("zillow", batch_size=50)
