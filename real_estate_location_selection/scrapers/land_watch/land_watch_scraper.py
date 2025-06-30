import json
from bs4 import BeautifulSoup
from real_estate_location_selection.connection import local_db_connection
from haversine import haversine, Unit
from real_estate_location_selection.scrapers._scraper import _Scraper
import re
from datetime import date
from psycopg.types.json import Json
from real_estate_location_selection.scrapers.utils.common_functions import safe_get, safe_lower, safe_divide

class Landwatch(_Scraper):
    """
    Scraper class for extracting property listings from LandWatch and storing structured data in a database.
    """

    source = "landwatch"
    use_proxies_camoufox = False
    use_resource_intercept = False

    def prepare_tasks(self, state_source, state_abbreviation, pages_per_batch=25):
        """
        Version with configurable batch size for connection resets.

        Args:
            state_source (str): The URL path for the state (e.g., "/texas-land-for-sale").
            state_abbreviation (str): The 2-letter abbreviation of the state (e.g., "TX").
            pages_per_batch (int): Number of pages to process before resetting connection (default: 50).
        """
        page = 1
        state_abbr = state_abbreviation.upper()

        while True:
            batch_start_page = page

            for conn in local_db_connection():
                with conn.cursor() as cur:
                    batch_processed = 0

                    while batch_processed < pages_per_batch:
                        url = f"https://www.landwatch.com{state_source}/page-{page}"
                        self.close_page()
                        self.goto_url(url)

                        soup = BeautifulSoup(self.page.content(), "html.parser")
                        links = [a["href"] for a in soup.find_all("a", href=True) if "/pid/" in a["href"]]

                        if not links:
                            print(f"No more links found at page {page}. Ending crawl.")
                            print(f"Final batch: processed {batch_processed} pages ({batch_start_page}-{page - 1})")
                            return

                        url_data = [
                            (f"https://www.landwatch.com{link}", state_abbr)
                            for link in links
                        ]

                        cur.executemany(
                            """
                            INSERT INTO landwatch_urls (url, state)
                            VALUES (%s, %s) ON CONFLICT (url) DO NOTHING;
                            """,
                            url_data
                        )
                        print(f"Page {page}: Updated {len(url_data)} landwatch_urls")
                        conn.commit()

                        page += 1
                        batch_processed += 1

    def _extract_location_parts_from_url(self, url: str):
        """
        Parse the LandWatch URL to extract the property type, state, and county
        without including the domain.

        Args:
            url (str): Full LandWatch property URL.

        Returns:
            tuple[str | None, str | None, str | None]: (property_type, state, county)
        """
        state_pattern = (
            r"alabama|alaska|arizona|arkansas|california|colorado|connecticut|delaware|florida|"
            r"georgia|hawaii|idaho|illinois|indiana|iowa|kansas|kentucky|louisiana|maine|"
            r"maryland|massachusetts|michigan|minnesota|mississippi|missouri|montana|"
            r"nebraska|nevada|new-hampshire|new-jersey|new-mexico|new-york|north-carolina|"
            r"north-dakota|ohio|oklahoma|oregon|pennsylvania|rhode-island|south-carolina|"
            r"south-dakota|tennessee|texas|utah|vermont|virginia|washington|west-virginia|"
            r"wisconsin|wyoming"
        )

        pattern = rf"https?://[^/]+/(?P<county>[^/]+?)-(?P<state>{state_pattern})-(?P<property_type>.+?)-for-sale/pid/\d+"
        match = re.search(pattern, url.lower())

        if not match:
            return None, None, None

        county = match.group("county")  # e.g. "sanpete-county"
        state = match.group("state")  # e.g. "utah"
        property_type = match.group("property_type")  # e.g. "farms-and-ranches"

        return property_type, state, county

    def _extract_structured_data(self, soup):
        """
        Extract structured JSON-LD data from the page, if available.

        Args:
            soup (BeautifulSoup): The BeautifulSoup-parsed HTML.

        Returns:
            dict: Parsed structured data dictionary or an empty dict.
        """
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string)
                if isinstance(data, dict):
                    return data
            except json.JSONDecodeError:
                continue
        return {}

    def _extract_general_property_details(self, page_source):
        """
        Extract city name and geolocation data from the `window.serverState` JavaScript blob.

        Args:
            page_source (str): Raw HTML content of the property page.

        Returns:
            tuple: (state_data, city_name, latitude, longitude) or (None, None, None, None) on failure.
        """
        m = re.search(r'window\.serverState\s*=\s*"(.+?)";', page_source, re.DOTALL)
        if not m:
            return None, None, None, None

        raw = m.group(1).encode("utf-8").decode("unicode_escape")
        state = json.loads(raw)
        prop_data = state["propertyDetailPage"]["propertyData"]

        # Coordinates
        prop_coords = (prop_data["latitude"], prop_data["longitude"])
        city = prop_data["city"] if prop_data["city"] else {}
        city_coords = (city.get("latitude"), city.get("longitude"))

        # Compute distance
        if prop_data["latitude"] and prop_data["longitude"] and city.get("latitude") and city.get("longitude"):
            distance_to_city = haversine(prop_coords, city_coords, unit=Unit.MILES)
        else:
            distance_to_city = None

        homesqrt = state["propertyDetailPage"]["propertyData"].get("homesqft") if state else None
        return {
            # Location
            'state': safe_get(prop_data, "address", 'stateAbbreviation'),
            'city': safe_lower(safe_get(prop_data, "address", "city")),
            'zip': safe_get(prop_data, "address", 'zip'),
            'address1': safe_lower(safe_get(prop_data, "address", 'address1')),
            'address2': safe_lower(safe_get(prop_data, "address", 'address2')),
            'latitude': safe_get(prop_data, "latitude"),
            'longitude': safe_get(prop_data, "longitude"),
            'city_latitude': safe_get(city, "latitude") if city else None,
            'city_longitude': safe_get(city, "longitude") if city else None,

            # Property characteristics
            'acres': safe_get(prop_data, "acres"),
            'beds': safe_get(prop_data, "beds"),
            'baths': safe_get(prop_data, "baths"),
            'homesqft': homesqrt,
            'property_types': safe_get(state, "propertyDetailPage", "propertyData", 'types') if state else None,
            'is_irrigated': safe_get(prop_data, "isIrrigated"),
            'is_residence': safe_get(prop_data, "isResidence"),

            # Listing details
            'price': safe_get(prop_data, "price"),
            'listing_date': safe_get(prop_data, "listingDate"),

            # Marketing & metadata
            'title': safe_get(prop_data, "title"),
            'description': safe_get(prop_data, "description"),
            'executive_summary': safe_get(prop_data, "executiveSummary"),

            # Badging
            'is_diamond': safe_get(prop_data, "isDiamond"),
            'is_gold': safe_get(prop_data, "isGold"),
            'is_platinum': safe_get(prop_data, "isPlatinum"),
            'is_showcase': safe_get(prop_data, "isShowcase"),

            # Computed Fields
            'cost_per_acre': safe_divide(safe_get(prop_data, "price"), safe_get(prop_data, 'acres')),
            'distance_to_city_miles': distance_to_city,
            'cost_per_homesqft': safe_divide(safe_get(prop_data, "price"), homesqrt),
        }

    def _extract_lot_info(self, soup, page_source):
        """
        Extract lot size, lot type, and inferred property details based on keywords in HTML.

        Args:
            soup (BeautifulSoup): The parsed HTML document.
            page_source (str): The raw HTML string.

        Returns:
            tuple: (lot_size: str | None, lot_types: List[str], additional_details: dict)
        """
        lot_size, lot_types = None, []

        for div in soup.select("div._66d543c"):
            label = div.find("b")
            if not label:
                continue
            label_text = label.get_text(strip=True).rstrip(":").lower()

            if label_text == "size":
                raw = "".join(str(x) for x in div.contents[1:])
                lot_size = raw.strip()
            elif label_text == "type":
                lot_types = [a.get_text(strip=True) for a in div.find_all("a")]

        details = {
            "mortgage_options": ["Owner Finance"] if "Owner Finance" in page_source else [],
            "activities": ["Camping"] if "Camping" in page_source else [],
            "lot_description": ["Acreage"] if "Acreage" in page_source else [],
            "geography": [g for g in ["Desert", "Mountain", "Rural"] if g in page_source],
            "road_frontage_desc": [r for r in ["Dirt", "Gravel/Rock"] if r in page_source],
        }
        return lot_size, lot_types, details

    def extract_from_website(self, url: str, state_abbr: str):
        """
        Visit the property URL and extract all structured data fields needed
        to represent a land property listing.

        Args:
            url (str): The full property URL.
            state_abbr (str): The two-letter abbreviation of the state.

        Returns:
            dict: A dictionary of cleaned and normalized property data.
        """
        self.goto_url(url)
        page_source = self.page.content()
        soup = BeautifulSoup(page_source, "html.parser")

        structured_data = self._extract_structured_data(soup)
        property_type, _, county = self._extract_location_parts_from_url(url)
        property_info = self._extract_general_property_details(page_source)
        lot_size, lot_types, lot_details = self._extract_lot_info(soup, page_source)

        amenities = [
            section.get_text(strip=True)
            for section in soup.select('section[aria-label="Property Description"] div.fa0d4c0 div._01623cf')
        ]

        lot_size_value = float(lot_size.split(" ")[0].replace(',', '')) if lot_size else None
        lot_size_units = lot_size.split(" ")[1] if lot_size and " " in lot_size else None

        return {
        # Core property identification
        "name": structured_data.get("name"),
        "property_type": property_type,
        "url": url,
        "date_posted": structured_data.get("datePosted"),
        # Location info
        "county": county.rstrip('-county'),
        # Lot characteristics
        "lot_size": lot_size_value,
        "lot_size_units": lot_size_units,
        "lot_type": lot_types,
        # Property features
        "amenities": amenities,
        # Extended/structured metadata
        **lot_details,
        **property_info,
    }



    def clean_unicode_surrogates(self, text):
        """
        Remove or replace invalid Unicode surrogate characters that can't be encoded to UTF-8.

        Args:
            text: String that may contain invalid surrogates

        Returns:
            Cleaned string safe for UTF-8 encoding
        """
        if not isinstance(text, str):
            return text

        # Method 1: Remove surrogates entirely
        # return text.encode('utf-8', 'ignore').decode('utf-8')

        # Method 2: Replace surrogates with replacement character
        return text.encode('utf-8', 'replace').decode('utf-8')

        # Method 3: Use regex to remove surrogate pairs (more precise)
        # return re.sub(r'[\ud800-\udfff]', '', text)

    def clean_data_for_unicode(self, data):
        """
        Recursively clean all string values in a data structure to remove invalid Unicode.

        Args:
            data: Dictionary, list, or other data structure that may contain strings

        Returns:
            Cleaned data structure
        """
        if isinstance(data, dict):
            return {key: self.clean_data_for_unicode(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [self.clean_data_for_unicode(item) for item in data]
        elif isinstance(data, str):
            return self.clean_unicode_surrogates(data)
        else:
            return data

    def upload_data(self, data: dict, cursor):
        """
        Insert the extracted property data into the landwatch_properties table.
        Now with Unicode surrogate handling.

        Args:
            data (dict): Dictionary of extracted property fields.
            cursor: A psycopg database cursor for executing the INSERT.
        """
        # Clean the data to remove invalid Unicode characters
        data = self.clean_data_for_unicode(data)

        if date_str := data.get("listing_date"):
            data["listing_date"] = date.fromisoformat(date_str)

        if isinstance(data.get("state"), dict):
            data["state"] = Json(data["state"])  # convert dict to JSON for JSONB column

        insert_sql = """
                     INSERT INTO landwatch_properties (name, property_type, url, county, lot_size, \
                                                       lot_size_units, \
                                                       lot_type, amenities, mortgage_options, activities, \
                                                       lot_description, geography, \
                                                       road_frontage_desc, city, zip, address1, address2, latitude, \
                                                       longitude, \
                                                       city_latitude, city_longitude, acres, beds, baths, homesqft, \
                                                       property_types, is_irrigated, is_residence, price, listing_date, \
                                                       title, \
                                                       description, executive_summary, is_diamond, is_gold, is_platinum, \
                                                       is_showcase, state, cost_per_acre, distance_to_city_miles, \
                                                       cost_per_homesqft) \
                     VALUES (%(name)s, %(property_type)s, %(url)s, %(county)s, %(lot_size)s, \
                             %(lot_size_units)s, \
                             %(lot_type)s, %(amenities)s, %(mortgage_options)s, %(activities)s, %(lot_description)s, \
                             %(geography)s, \
                             %(road_frontage_desc)s, %(city)s, %(zip)s, %(address1)s, %(address2)s, %(latitude)s, \
                             %(longitude)s, \
                             %(city_latitude)s, %(city_longitude)s, %(acres)s, %(beds)s, %(baths)s, %(homesqft)s, \
                             %(property_types)s, %(is_irrigated)s, %(is_residence)s, %(price)s, %(listing_date)s, \
                             %(title)s, \
                             %(description)s, %(executive_summary)s, %(is_diamond)s, %(is_gold)s, %(is_platinum)s, \
                             %(is_showcase)s, %(state)s, %(cost_per_acre)s, %(distance_to_city_miles)s, \
                             %(cost_per_homesqft)s); \
                     """

        try:
            cursor.execute(insert_sql, data)
        except UnicodeEncodeError as e:
            # If we still get Unicode errors, log the problematic data for debugging
            print(f"Unicode error persists. Problematic data keys: {list(data.keys())}")
            for key, value in data.items():
                if isinstance(value, str):
                    try:
                        value.encode('utf-8')
                    except UnicodeEncodeError:
                        print(f"Problematic field '{key}': {repr(value)}")
            raise

    def get_unscraped_urls_paginated(self, cur, page_size=25):
        cur.execute("""
                    SELECT url, state
                    FROM landwatch_urls
                    WHERE scraped_at IS NULL
                    ORDER BY url -- Important: always use ORDER BY with pagination
                        LIMIT %s
                    """, (page_size,))
        return cur.fetchall()

    def process_tasks(self):
        """
        Fetch all URLs from `landwatch_urls` that have not yet been scraped.
        For each, extract data, upload it to the database, and mark the URL as scraped.
        """
        is_complete = False
        while not is_complete:
            today = date.today()
            for conn in local_db_connection():
                with conn.cursor() as cur:
                    results = self.get_unscraped_urls_paginated(cur)
                    if not results:
                        is_complete = True
                        break
                    for url, state in results:
                        self.close_page()
                        print(f"extracting details for {url}")
                        data = self.extract_from_website(url, state)
                        print(f"uploading data for {url}")
                        self.upload_data(data, cur)
                        cur.execute(
                            "UPDATE landwatch_urls SET scraped_at = %s WHERE url = %s;",
                            (today, url),
                        )
                        conn.commit()
                        print(f"Updated scraped_at for: {url}")
