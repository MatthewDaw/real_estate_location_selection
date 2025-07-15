import re
import time
from urllib.parse import urljoin

import pandas as pd
import requests
import requests_cache
from bs4 import BeautifulSoup
from tqdm import tqdm

# ----------------------------------------------------------------------------
# 1) Configuration
# ----------------------------------------------------------------------------

# LandWatch “Undeveloped Land” for Utah
BASE_URL = "https://www.landwatch.com/utah-land-for-sale/undeveloped-land"
PAGE_PARAM = "pn"  # LandWatch uses ?pn=2, ?pn=3, etc. for pagination

# CSS selectors — inspect your browser and adjust!
CARD_SELECTOR  = ".property-card"     # each listing card
PRICE_SELECTOR = ".price"             # element containing "$123,456"
BEDS_SELECTOR  = ".beds"              # element present only if there's a structure

# Price extractor
PRICE_RE = re.compile(r"\$[\d,]+")

def parse_price(text: str) -> int | None:
    m = PRICE_RE.search(text)
    return int(m.group().replace("$","").replace(",","")) if m else None

# ----------------------------------------------------------------------------
# 2) Setup HTTP caching
# ----------------------------------------------------------------------------

# caches to ./landwatch_cache.sqlite, expires after 1 day
requests_cache.install_cache("landwatch_cache", expire_after=24*3600)
session = requests.Session()

# ----------------------------------------------------------------------------
# 3) Pagination loop
# ----------------------------------------------------------------------------

all_listings = []

for page in tqdm(range(1, 1000), desc="Pages"):
    params = {PAGE_PARAM: page}
    try:
        resp = session.get(BASE_URL, params=params, timeout=60)
    except Exception as e:
        print(e)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    cards = soup.select(CARD_SELECTOR)
    if not cards:
        break  # no more pages

    for card in cards:
        # skip if this listing has beds at all (i.e. isn’t a pure lot)
        if card.select_one(BEDS_SELECTOR):
            continue

        # price
        price_tag = card.select_one(PRICE_SELECTOR)
        price = parse_price(price_tag.get_text()) if price_tag else None

        # title & detail‑page URL
        title_tag = card.select_one("a[href]")
        title = title_tag.get_text(strip=True) if title_tag else ""
        detail_url = urljoin(BASE_URL, title_tag["href"]) if title_tag else ""

        # acreage (grab first “• N acres” chunk)
        text = card.get_text(" ", strip=True)
        acres = None
        if "acre" in text:
            ac_match = re.search(r"([\d\.]+)\s*acre", text)
            if ac_match:
                acres = float(ac_match.group(1))

        all_listings.append({
            "title": title,
            "detail_url": detail_url,
            "price": price,
            "acres": acres,
        })

    # be polite
    time.sleep(1.0)

# ----------------------------------------------------------------------------
# 4) Save results
# ----------------------------------------------------------------------------
if __name__ == '__main__':
    df = pd.DataFrame(all_listings)
    df.to_csv("utah_empty_lots_landwatch.csv", index=False)
    print(f"Found {len(df)} empty‑lot listings; saved to utah_empty_lots_landwatch.csv")
