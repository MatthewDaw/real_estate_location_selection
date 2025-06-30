import json
import re
from real_estate_location_selection.scrapers._scraper import _Scraper
from real_estate_location_selection.connection import local_db_connection

class Zillow(_Scraper):
    source = "zillow"
    use_proxies_camoufox = True
    use_resource_intercept = False

    def _process_connection_batch(self, connection_batch, batch_offset):
        """
        Helper method to process a batch of batches within a single connection.

        Args:
            connection_batch (list): List of batch_entries to process
            batch_offset (int): Starting batch number for logging
        """
        for conn in local_db_connection():
            with conn.cursor() as cur:
                for i, batch_entries in enumerate(connection_batch):
                    self._insert_url_batch(cur, batch_entries, conn)
                    batch_num = batch_offset + i + 1
                    print(f"Updated {len(batch_entries)} zillow url entries (batch {batch_num})")

            print(f"Connection reset after processing {len(connection_batch)} batches")

    def prepare_tasks(self, batches_per_connection=50):
        """
        Load sitemap URLs and insert property URLs into the `zillow_urls` table.
        Skips the "off-market" sitemap.
        """
        sitemap_urls = [
            "https://www.zillow.com/xml/indexes/us/hdp/for-sale-by-agent.xml.gz",
            "https://www.zillow.com/xml/indexes/us/hdp/for-sale-by-owner.xml.gz",
        ]

        batch_size = 100000
        total_batches_processed = 0

        for sitemap_url in sitemap_urls:
            listing_type = sitemap_url.split('/')[-1].replace('.xml.gz', '')
            print(f"Processing sitemap: {listing_type}")

            listing_urls_generator = self._extract_listing_urls(sitemap_url)
            batch_entries = []
            batches_in_current_connection = 0
            current_connection_batch = []

            for listing_url in listing_urls_generator:
                batch_entries.append((listing_url, listing_type))

                # When batch is full, add to connection batch
                if len(batch_entries) >= batch_size:
                    current_connection_batch.append(batch_entries.copy())
                    batch_entries.clear()
                    batches_in_current_connection += 1

                    # Process batches when connection limit reached
                    if batches_in_current_connection >= batches_per_connection:
                        self._process_connection_batch(current_connection_batch, total_batches_processed)
                        total_batches_processed += len(current_connection_batch)
                        current_connection_batch.clear()
                        batches_in_current_connection = 0

            # Handle remaining batches for this sitemap
            if batch_entries:
                current_connection_batch.append(batch_entries.copy())
                batch_entries.clear()

            if current_connection_batch:
                self._process_connection_batch(current_connection_batch, total_batches_processed)
                total_batches_processed += len(current_connection_batch)
                current_connection_batch.clear()

            print(f"Completed sitemap: {listing_type}")

        print(f"Total batches processed: {total_batches_processed}")

    def _insert_url_batch(self, cur, entries, conn):
        cur.executemany("""
            INSERT INTO zillow_urls (url, type)
            VALUES (%s, %s)
            ON CONFLICT (url, type) DO NOTHING
        """, entries)
        conn.commit()
        print(f"Upserted batch of {len(entries)} URLs")

    def _extract_listing_urls(self, sitemap_url):
        """
        Recursively extract all property URLs from a sitemap index URL.
        """
        content = self.session.get(sitemap_url).text
        sitemap_entries = re.findall(r"<loc>(.*?)</loc>", content)

        for sub_sitemap_url in sitemap_entries:
            sub_content = self.session.get(sub_sitemap_url).text
            listing_urls = re.findall(r"<loc>(.*?)</loc>", sub_content)
            for url in listing_urls:
                yield url

    def extract_from_website(self, url):
        self.close_page()
        self.goto_url(url)
        response_html = self.page.content()
        page_data = self._parse_home_details_from_html(response_html)
        return self._parse_property_details(page_data)

    def _parse_home_details_from_html(self, html):
        script_patterns = [
            r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
            r'<script id=hdpApolloPreloadedData type="application/json">(.*?)</script>'
        ]
        for pattern in script_patterns:
            match = re.findall(pattern, html)
            if match:
                data = json.loads(match[0])
                return self._extract_page_data(data)
        return {}

    def _extract_page_data(self, data):
        if "apiCache" in data:
            cache = json.loads(data["apiCache"])
            for key in cache:
                if "ForRentDoubleScrollFullRenderQuery" in key:
                    return cache[key]

        props = data.get("props", {}).get("pageProps", {})
        props = props.get("componentProps", props)
        if "gdpClientCache" in props:
            return list(json.loads(props["gdpClientCache"]).values())[0]
        return {}

    def _parse_property_details(self, page_content):
        property_data = page_content.get('property', {})

        risks = self._parse_climate_risks(page_content)
        home_insights = self._flatten_home_insights(property_data.get('homeInsights', []))
        school_distances = [el['distance'] for el in property_data.get('schools', [])]
        price_history, most_recent_price = self._parse_price_history(property_data.get('priceHistory', []))
        foreclosure_data = self._extract_foreclosure_data(property_data)

        return {
            'status': property_data.get('homeStatus'),
            "is_eligible_property": property_data.get('buyAbilityData', {}).get('isEligibleProperty'),
            "selling_soon": property_data.get('sellingSoon'),
            "last_sold_price": property_data.get('lastSoldPrice'),
            "posting_url": property_data.get('postingUrl'),
            "date_posted_string": property_data.get('datePostedString'),
            "marketing_name": property_data.get('marketingName'),
            "posting_product_type": property_data.get('postingProductType'),
            "lot_area_units": property_data.get('lotAreaUnits'),
            "lot_area_value": property_data.get('lotAreaValue'),
            "lot_size": property_data.get('lotSize'),
            "living_area_units": property_data.get('livingAreaUnits'),
            "living_area": property_data.get('livingAreaValue'),
            "street_address": property_data.get('streetAddress'),
            "city": property_data.get('city'),
            "state": property_data.get('state'),
            "zipcode": property_data.get('zipCode'),
            "price": property_data.get('price'),
            "currency": property_data.get('currency'),
            "home_type": property_data.get('homeType'),
            "is_preforeclosure_auction": property_data.get('isPreforeclosureAuction'),
            "address": property_data.get('address'),
            "bedrooms": property_data.get('bedrooms'),
            "bathrooms": property_data.get('bathrooms'),
            "year_built": property_data.get('yearBuilt'),
            "living_area_units_short": property_data.get('livingAreaUnitsShort'),
            "country": property_data.get('country'),
            "monthly_hoa_fee": property_data.get('monthlyHoaFee'),
            "zestimate": property_data.get('zestimate'),
            "new_construction_type": property_data.get('newConstructionType'),
            "zestimate_low_percent": property_data.get('zestimateLowPercent'),
            "zestimate_high_percent": property_data.get('zestimateHighPercent'),
            "time_on_zillow": property_data.get('timeOnZillow'),
            "page_view_count": property_data.get('pageViewCount'),
            "favorite_count": property_data.get('favoriteCount'),
            "days_on_zillow": property_data.get('daysOnZillow'),
            "latitude": property_data.get('latitude'),
            "longitude": property_data.get('longitude'),
            "is_income_restricted": property_data.get('isIncomeRestricted'),
            "price_history": price_history,
            "most_recent_price": most_recent_price.get('price'),
            "most_recent_price_date": most_recent_price.get('date'),
            "most_recent_price_change_rate": most_recent_price.get('priceChangeRate'),
            "rental_application_accepted_type": property_data.get('rentalApplicationsAcceptedType'),
            "home_insights": home_insights,
            "school_distances": school_distances,
            "num_schools_close_to": len(school_distances),
            "avg_school_distance": (sum(school_distances) / len(school_distances)) if school_distances else None,
            'risks': risks,
            "description": property_data.get('description'),
            "foreclosure": foreclosure_data,
            "has_bad_geocode": property_data.get('hasBadGeocode'),
            'list_price_low': property_data.get('listPriceLow'),
            'county': property_data.get('county'),
        }

    def _parse_climate_risks(self, page_content):
        risks = {}
        climate_data = page_content.get('zgProperty', {}).get('odpPropertyModels', {}).get('climate')
        if climate_data:
            for risk_name, risk in climate_data.items():
                if 'primary' in risk:
                    primary = risk.get('primary', {})
                    if primary:
                        risk_score = primary.get('riskScore') if isinstance(primary.get('riskScore'), dict) else {}
                        risks[risk_name] = {
                            'probability': primary.get('probability'),
                            'risk_label': risk_score.get('label'),
                            'risk_value': risk_score.get('value'),
                            'risk_value_out_of': risk_score.get('max'),
                        }
        return risks

    def _flatten_home_insights(self, home_insights):
        if not home_insights:
            return []
        phrases = [phrase for insight in home_insights for group in insight['insights'] for phrase in group['phrases']]
        return list(set(phrases))

    def _parse_price_history(self, history):
        """
        Filters and simplifies the price history data.
        Handles None by returning empty structures.
        """
        if not history:
            return [], {}

        filtered = [
            {k: v for k, v in entry.items() if k in ['date', 'price', 'priceChangeRate']}
            for entry in history
        ]
        return filtered, (filtered[0] if filtered else {})

    def _extract_foreclosure_data(self, data):
        keys = [
            "foreclosureDefaultFilingDate", "foreclosureAuctionFilingDate", "foreclosureLoanDate",
            "foreclosureLoanOriginator", "foreclosureLoanAmount", "foreclosurePriorSaleDate",
            "foreclosurePriorSaleAmount", "foreclosureBalanceReportingDate", "foreclosureDefaultDescription",
            "foreclosurePastDueBalance", "foreclosureUnpaidBalance", "foreclosureAuctionTime",
            "foreclosureAuctionDescription", "foreclosureAuctionCity", "foreclosureAuctionLocation",
            "foreclosureDate", "foreclosureAmount", "foreclosingBank", "foreclosureJudicialType"
        ]
        return {k: data.get(k) for k in keys if data.get(k)}

    # Version with progress persistence for resumability
    def process_tasks(self, batches_per_connection=1, max_properties=10000, start_offset=0):
        """
        Cleaner version with proper generator handling.
        """
        num_added = 0
        num_processed = start_offset
        batch_size = 10
        total_batches_processed = 0

        try:
            while num_added < max_properties:
                batches_in_connection = 0

                for conn in local_db_connection():
                    with conn.cursor() as cur:
                        while (batches_in_connection < batches_per_connection and
                               num_added < max_properties):

                            # Get URLs to process
                            urls = self._fetch_urls_to_scrape(cur, limit=batch_size, offset=num_processed)
                            if not urls:
                                print("No more URLs to scrape")
                                return

                            batch_entries, urls_to_update = [], []

                            for (url,) in urls:
                                num_processed += 1

                                print(f"extracting zillow url {url} (processed: {num_processed}, added: {num_added})")
                                try:
                                    data = self.extract_from_website(url)
                                    if data:
                                        safe_data = self._prepare_data_for_db(data)
                                        batch_entries.append((url, *safe_data))
                                        urls_to_update.append(url)
                                except Exception as e:
                                    print(f"Error processing URL {url}: {e}")
                                    continue

                            # Insert the batch
                            if batch_entries:
                                num_added += len(batch_entries)
                                self._insert_property_batch(cur, batch_entries, urls_to_update, conn)
                                print(f"uploaded batch of {len(batch_entries)} (total added: {num_added})")
                                batches_in_connection += 1
                                total_batches_processed += 1
                                conn.commit()

                            # If we got fewer URLs than requested, we're done
                            if len(urls) < batch_size:
                                print("Reached end of available URLs")
                                return

                print(f"Connection reset after {batches_in_connection} batches")

        except Exception as e:
            print(f"Error during processing: {e}")
            print(f"Resume with: process_tasks_clean(start_offset={num_processed})")
            raise e

        print(f"Complete: {num_added} added, {num_processed} processed, {total_batches_processed} batches")

    # Updated helper method to support limit properly
    def _fetch_urls_to_scrape(self, cur, limit=1000, offset=0):
        """
        Fetch URLs to scrape with proper limit and offset support.

        Args:
            cur: Database cursor
            limit (int): Maximum number of URLs to fetch
            offset (int): Number of URLs to skip
        """
        query = """
                SELECT url
                FROM zillow_urls
                WHERE scraped_at IS NULL
                ORDER BY id
                    LIMIT %s \
                OFFSET %s \
                """
        cur.execute(query, (limit, offset))
        return cur.fetchall()

    def _prepare_data_for_db(self, data):
        """
        Prepares property details for DB insertion:
        - Converts dicts/lists to JSON if needed
        - Leaves simple lists of numbers as they are
        """

        def convert(value):
            if isinstance(value, dict):
                return json.dumps(value)
            elif isinstance(value, list):
                if all(isinstance(item, (int, float, str, type(None))) for item in value):
                    # Convert list of numbers to floats if consistent
                    return [float(item) for item in value] if all(
                        isinstance(item, (int, float)) for item in value) else value
                else:
                    # Convert complex/nested lists to JSON
                    return json.dumps(value)
            return value

        fields = [
            'status', 'is_eligible_property', 'selling_soon', 'last_sold_price', 'posting_url',
            'date_posted_string', 'marketing_name', 'posting_product_type', 'lot_area_units',
            'lot_area_value', 'lot_size', 'living_area_units', 'living_area', 'street_address',
            'city', 'state', 'zipcode', 'price', 'currency', 'home_type', 'is_preforeclosure_auction',
            'address', 'bedrooms', 'bathrooms', 'year_built', 'living_area_units_short', 'country',
            'monthly_hoa_fee', 'zestimate', 'new_construction_type', 'zestimate_low_percent',
            'zestimate_high_percent', 'time_on_zillow', 'page_view_count', 'favorite_count',
            'days_on_zillow', 'latitude', 'longitude', 'is_income_restricted', 'price_history',
            'most_recent_price', 'most_recent_price_date', 'most_recent_price_change_rate',
            'rental_application_accepted_type', 'home_insights', 'school_distances',
            'num_schools_close_to', 'avg_school_distance', 'risks', 'description', 'foreclosure'
        ]

        return [convert(data.get(field)) for field in fields]

    def _insert_property_batch(self, cur, entries, urls_to_update, conn):
        print("pause")
        insert_command = cur.executemany("""
            INSERT INTO zillow_property_details (
                source_url, status, is_eligible_property, selling_soon, last_sold_price, posting_url,
                date_posted_string, marketing_name, posting_product_type, lot_area_units, lot_area_value,
                lot_size, living_area_units, living_area, street_address, city, state, zipcode, price,
                currency, home_type, is_preforeclosure_auction, address, bedrooms, bathrooms, year_built,
                living_area_units_short, country, monthly_hoa_fee, zestimate, new_construction_type,
                zestimate_low_percent, zestimate_high_percent, time_on_zillow, page_view_count,
                favorite_count, days_on_zillow, latitude, longitude, is_income_restricted, price_history,
                most_recent_price, most_recent_price_date, most_recent_price_change_rate,
                rental_application_accepted_type, home_insights, school_distances, num_schools_close_to,
                avg_school_distance, risks, description, foreclosure
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, entries)

        cur.execute("""
            UPDATE zillow_urls
            SET scraped_at = CURRENT_DATE
            WHERE url = ANY(%s)
        """, (urls_to_update,))

        conn.commit()
        print(f"Inserted batch of {len(entries)} property details and updated scraped_at")

    def set_property_website(self, gdp):
        """
        Resolves the final property website URL by visiting the PPC redirect link.
        """
        ppc_link = gdp.get("building", {}).get("ppcLink")
        if not ppc_link:
            return

        try:
            with self._context.new_page() as page:
                page.route("**/*", self.block_unwanted_requests)
                page.goto(ppc_link["path"], wait_until="domcontentloaded", referer="https://www.google.com")
                gdp["building"]["ppcLink"]["path"] = page.url
        except Exception as e:
            print(f"Error resolving PPC link: {e}")

    def get_building_details(self, response):
        """
        Extracts building page details from embedded Redux state.
        """
        match = re.findall(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', response)
        if not match:
            return {}

        props = json.loads(match[0]).get("props", {}).get("pageProps", {})
        props = props.get("componentProps", props)
        if "initialReduxState" in props:
            gdp = props["initialReduxState"].get("gdp")
            self.set_property_website(gdp)
            return gdp
        return {}
