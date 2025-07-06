import json
import re
from datetime import datetime, date
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from bs4 import BeautifulSoup
from haversine import haversine, Unit
from real_estate_location_selection.scrapers._scraper import _Scraper, Task
from real_estate_location_selection.scrapers.utils.common_functions import safe_get, safe_lower, safe_divide
import hashlib
from datetime import datetime, timezone

class Landwatch(_Scraper):
    """
    Scraper class for extracting property listings from LandWatch and storing structured data in BigQuery.
    """

    source = "landwatch"
    use_proxies_camoufox = False
    use_resource_intercept = False
    states_to_scrape = [
        'UT',
        'ID',
        'NV',
        'WY',
        'MT',
        'NH',
        'CO',
        'AZ',
        'NM',
        'TX',
        'OK',
        'KS',
        'NE',
        'IA',
        'IL',
        'MO',
        'IN',
        'AR',
        'LA',
        'MS',
        'MI',
    ]

    def __init__(self, browser):
        super().__init__(browser, "landwatch-job-queue")
        self._ensure_tables_exist()
        self.num_processed = 0
        self.total_batches_processed = 0
        self.num_added = 0

    def _ensure_tables_exist(self):
        """Create BigQuery tables if they don't exist."""

        # Create landwatch_urls table
        urls_table_id = f"{self.project_id}.{self.dataset_id}.landwatch_urls"
        urls_schema = [
            bigquery.SchemaField("url", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("state", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("scraped_at", "DATE", mode="NULLABLE"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
        ]

        try:
            self.client.get_table(urls_table_id)
        except NotFound:
            table = bigquery.Table(urls_table_id, schema=urls_schema)
            table = self.client.create_table(table)
            print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")

        # Create landwatch_properties table
        properties_table_id = f"{self.project_id}.{self.dataset_id}.landwatch_properties"
        properties_schema = [
            bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("url", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("property_type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("date_posted", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("county", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("lot_size", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("lot_size_units", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("lot_type", "JSON", mode="NULLABLE"),
            bigquery.SchemaField("amenities", "JSON", mode="NULLABLE"),
            bigquery.SchemaField("mortgage_options", "JSON", mode="NULLABLE"),
            bigquery.SchemaField("activities", "JSON", mode="NULLABLE"),
            bigquery.SchemaField("lot_description", "JSON", mode="NULLABLE"),
            bigquery.SchemaField("geography", "JSON", mode="NULLABLE"),
            bigquery.SchemaField("road_frontage_desc", "JSON", mode="NULLABLE"),
            bigquery.SchemaField("state", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("city", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("zip", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("address1", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("address2", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("latitude", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("longitude", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("city_latitude", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("city_longitude", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("acres", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("beds", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("baths", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("homesqft", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("property_types", "JSON", mode="NULLABLE"),
            bigquery.SchemaField("is_irrigated", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("is_residence", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("price", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("listing_date", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("description", "JSON", mode="NULLABLE"),
            bigquery.SchemaField("executive_summary", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("is_diamond", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("is_gold", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("is_platinum", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("is_showcase", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("cost_per_acre", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("distance_to_city_miles", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("cost_per_homesqft", "FLOAT", mode="NULLABLE"),
        ]

        try:
            self.client.get_table(properties_table_id)
        except NotFound:
            table = bigquery.Table(properties_table_id, schema=properties_schema)
            table = self.client.create_table(table)
            print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")

    def prepare_tasks(self, state_source, state_abbreviation, pages_per_batch=2):
        """
        Extract URLs from LandWatch and insert them into BigQuery.

        Args:
            state_source (str): The URL path for the state (e.g., "/texas-land-for-sale").
            state_abbreviation (str): The 2-letter abbreviation of the state (e.g., "TX").
            pages_per_batch (int): Number of pages to process before batching (default: 25).
        """
        page = 1
        state_abbr = state_abbreviation.upper()
        self.total_batches_processed = 0

        while True:
            batch_entries = []
            batch_start_page = page

            # Process pages in batches
            for _ in range(pages_per_batch):
                url = f"https://www.landwatch.com{state_source}/page-{page}"
                self.close_page()
                self.goto_url(url)

                soup = BeautifulSoup(self.page.content(), "html.parser")
                links = [a["href"] for a in soup.find_all("a", href=True) if "/pid/" in a["href"]]

                if not links:
                    print(f"No more links found at page {page}. Ending crawl.")
                    if batch_entries:
                        self._insert_url_batch(batch_entries)
                        self.total_batches_processed += 1
                        print(f"Final batch: processed {len(batch_entries)} URLs")
                    print(f"Total batches processed: {self.total_batches_processed}")
                    return

                # Add URLs to batch
                for link in links:
                    batch_entries.append({
                        'url': f"https://www.landwatch.com{link}",
                        'state': state_abbr,
                        'created_at': datetime.now(timezone.utc).isoformat(),
                    })

                print(f"Page {page}: Found {len(links)} URLs")
                page += 1

            # Insert batch when full
            if batch_entries:
                self._insert_url_batch(batch_entries)
                self.total_batches_processed += 1
                print(f"Batch {self.total_batches_processed}: processed pages {batch_start_page}-{page-1} with {len(batch_entries)} URLs")

    def _insert_url_batch(self, entries):
        """Insert batch of URLs into BigQuery with upsert logic."""
        table_id = f"{self.project_id}.{self.dataset_id}.landwatch_urls"

        # Check existing URLs to avoid duplicates
        existing_check_query = f"""
        SELECT url
        FROM `{table_id}`
        WHERE url IN UNNEST(@urls)
        """

        urls = [entry['url'] for entry in entries]

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("urls", "STRING", urls)
            ]
        )

        existing_job = self.client.query(existing_check_query, job_config=job_config)
        existing_urls = {row.url for row in existing_job.result()}

        # Filter out existing entries
        new_entries = [
            entry for entry in entries
            if entry['url'] not in existing_urls or True
        ]
        if new_entries:
            # Insert new entries using load_table_from_json
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            )

            job = self.client.load_table_from_json(new_entries, table_id, job_config=job_config)
            job.result()  # Wait for job to complete
            print(f"Inserted {len(new_entries)} new URLs (skipped {len(entries) - len(new_entries)} duplicates)")
        else:
            print(f"Skipped {len(entries)} URLs (all duplicates)")

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
            dict: Dictionary containing property details
        """
        m = re.search(r'window\.serverState\s*=\s*"(.+?)";', page_source, re.DOTALL)
        if not m:
            return {}

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

    def extract_from_website(self, url: str):
        """
        Visit the property URL and extract all structured data fields needed
        to represent a land property listing.

        Args:
            url (str): The full property URL.

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
            "county": county.rstrip('-county') if county else None,
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

        # Replace surrogates with replacement character
        return text.encode('utf-8', 'replace').decode('utf-8')

    def clean_data_for_unicode(self, data):
        """
        Recursively clean all string values in a data structure to remove invalid Unicode
        and convert datetime/date objects to ISO formatted strings.

        Args:
            data: Dictionary, list, or other data structure that may contain strings or dates

        Returns:
            Cleaned data structure
        """
        if isinstance(data, dict):
            return {key: self.clean_data_for_unicode(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [self.clean_data_for_unicode(item) for item in data]
        elif isinstance(data, str):
            return self.clean_unicode_surrogates(data)
        elif isinstance(data, (datetime, date)):
            return data.isoformat()
        else:
            return data

    def _generate_property_id(self, name, city, zip_code, address1, address2):
        """
        Generate a unique ID hash based on building name, city, zip, address1, and address2.

        Args:
            name (str): Property/building name
            city (str): City name
            zip_code (str): ZIP code
            address1 (str): Primary address
            address2 (str): Secondary address

        Returns:
            str: SHA256 hash of the combined address components
        """
        # Convert all inputs to strings and handle None values
        components = [
            str(name or ''),
            str(city or ''),
            str(zip_code or ''),
            str(address1 or ''),
            str(address2 or '')
        ]

        # Normalize by converting to lowercase and stripping whitespace
        normalized_components = [comp.lower().strip() for comp in components]

        # Join components with a separator
        combined_string = '|'.join(normalized_components)

        # Generate SHA256 hash
        hash_object = hashlib.sha256(combined_string.encode('utf-8'))
        return hash_object.hexdigest()

    def _prepare_data_for_db(self, data):
        """
        Prepares property details for BigQuery insertion.
        """
        # Clean the data to remove invalid Unicode characters
        data = self.clean_data_for_unicode(data)

        # Generate unique ID based on address components
        property_id = self._generate_property_id(
            data.get('name'),
            data.get('city'),
            data.get('zip'),
            data.get('address1'),
            data.get('address2')
        )
        data['id'] = property_id

        # Ensure all fields match the schema
        schema_fields = [
            'id', 'name', 'property_type', 'date_posted', 'county', 'lot_size', 'lot_size_units',
            'lot_type', 'amenities', 'mortgage_options', 'activities', 'lot_description',
            'geography', 'road_frontage_desc', 'state', 'city', 'zip', 'address1',
            'address2', 'latitude', 'longitude', 'city_latitude', 'city_longitude',
            'acres', 'beds', 'baths', 'homesqft', 'property_types', 'is_irrigated',
            'is_residence', 'price', 'listing_date', 'title', 'description',
            'executive_summary', 'is_diamond', 'is_gold', 'is_platinum', 'is_showcase',
            'cost_per_acre', 'distance_to_city_miles', 'cost_per_homesqft'
        ]

        return {field: data.get(field) for field in schema_fields}

    def _fetch_urls_to_scrape(self, limit=1000):
        """
        Fetch URLs to scrape with proper limit and offset support.
        Only returns URLs where ALL associated records have scraped_at IS NULL.
        """
        query = f"""
        SELECT url, ANY_VALUE(state) as state
        FROM `{self.project_id}.{self.dataset_id}.landwatch_urls`
        WHERE state in ('{"','".join(self.states_to_scrape)}')
        GROUP BY url
        HAVING COUNTIF(scraped_at IS NOT NULL) = 0
        ORDER BY url
        LIMIT {limit}
        """
        query_job = self.client.query(query)
        return list(query_job.result())

    def _insert_property_batch(self, entries, urls_to_update):
        """Insert property batch into BigQuery."""
        table_id = f"{self.project_id}.{self.dataset_id}.landwatch_properties"

        # Insert property details
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )

        job = self.client.load_table_from_json(entries, table_id, job_config=job_config)
        job.result()  # Wait for job to complete

        # Update scraped_at for processed URLs
        if urls_to_update:
            update_query = f"""
            UPDATE `{self.project_id}.{self.dataset_id}.landwatch_urls`
            SET scraped_at = CURRENT_DATE()
            WHERE url IN UNNEST(@urls)
            """

            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ArrayQueryParameter("urls", "STRING", urls_to_update)
                ]
            )

            query_job = self.client.query(update_query, job_config=job_config)
            query_job.result()

        print(f"Inserted batch of {len(entries)} property details and updated scraped_at")

    def process_urls(self, urls):
        batch_entries = []
        urls_to_update = []

        for url in urls:
            self.num_processed += 1

            print(f"extracting landwatch url {url} (processed: {self.num_processed}, added: {self.num_added})")
            try:
                data = self.extract_from_website(url)
                if data:
                    safe_data = self._prepare_data_for_db(data)
                    batch_entry = {
                        'url': url,
                        'created_at': datetime.utcnow().isoformat(),
                        **safe_data
                    }
                    batch_entries.append(batch_entry)
                    urls_to_update.append(url)
            except Exception as e:
                print(f"Error processing URL {url}: {e}")
                raise Exception(f"Error processing URL {url}: {e}")

        self._insert_property_batch(batch_entries, urls_to_update)

    def process_tasks(self, max_properties=None, start_offset=0, batch_size=50):
        """
        Process scraping tasks using BigQuery.
        Fetch all URLs from `landwatch_urls` that have not yet been scraped.
        For each, extract data, upload it to BigQuery, and mark the URL as scraped.
        """
        self.num_added = 0
        self.num_processed = start_offset
        self.total_batches_processed = 0

        try:
            while max_properties is None or self.num_added < max_properties:
                # Get URLs to process
                urls = self._fetch_urls_to_scrape(limit=batch_size)
                self.process_urls([el.url for el in urls])
                self.total_batches_processed += 1
                self.num_processed += len(urls)
                print(f"uploaded batch of {len(urls)} (total added: num_processed)")

                # If we got fewer URLs than requested, we're done
                if len(urls) < batch_size:
                    print("Reached end of available URLs")
                    return

        except Exception as e:
            print(f"Error during processing: {e}")
            raise e

        print(f"Complete: {self.num_added} added, {self.num_processed} processed, {self.total_batches_processed} batches")
