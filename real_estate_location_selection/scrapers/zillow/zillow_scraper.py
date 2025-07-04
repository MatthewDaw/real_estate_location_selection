import json
import re
import hashlib
from datetime import datetime
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from real_estate_location_selection.scrapers._scraper import _Scraper


class Zillow(_Scraper):
    source = "zillow"
    use_proxies_camoufox = True
    use_resource_intercept = False

    def __init__(self, browser):
        super().__init__(browser)
        self._ensure_tables_exist()

    def _ensure_tables_exist(self):
        """Create BigQuery tables if they don't exist."""

        # Create zillow_urls table
        urls_table_id = f"{self.project_id}.{self.dataset_id}.zillow_urls"
        urls_schema = [
            bigquery.SchemaField("url", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("type", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("scraped_at", "DATE", mode="NULLABLE"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
        ]

        try:
            self.client.get_table(urls_table_id)
        except NotFound:
            table = bigquery.Table(urls_table_id, schema=urls_schema)
            table = self.client.create_table(table)
            print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")

        # Create zillow_property_details table
        details_table_id = f"{self.project_id}.{self.dataset_id}.zillow_property_details"
        details_schema = [
            bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("source_url", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("is_eligible_property", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("selling_soon", "JSON", mode="NULLABLE"),
            bigquery.SchemaField("last_sold_price", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("posting_url", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("date_posted_string", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("marketing_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("posting_product_type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("lot_area_units", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("lot_area_value", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("lot_size", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("living_area_units", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("living_area", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("street_address", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("city", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("state", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("zipcode", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("price", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("currency", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("home_type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("is_preforeclosure_auction", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("address", "JSON", mode="NULLABLE"),
            bigquery.SchemaField("bedrooms", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("bathrooms", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("year_built", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("living_area_units_short", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("country", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("monthly_hoa_fee", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("zestimate", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("new_construction_type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("zestimate_low_percent", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("zestimate_high_percent", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("time_on_zillow", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("page_view_count", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("favorite_count", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("days_on_zillow", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("latitude", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("longitude", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("is_income_restricted", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("price_history", "JSON", mode="NULLABLE"),
            bigquery.SchemaField("most_recent_price", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("most_recent_price_date", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("most_recent_price_change_rate", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("rental_application_accepted_type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("home_insights", "JSON", mode="NULLABLE"),
            bigquery.SchemaField("school_distances", "JSON", mode="NULLABLE"),
            bigquery.SchemaField("num_schools_close_to", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("avg_school_distance", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("risks", "JSON", mode="NULLABLE"),
            bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("foreclosure", "JSON", mode="NULLABLE"),
        ]

        try:
            self.client.get_table(details_table_id)
        except NotFound:
            table = bigquery.Table(details_table_id, schema=details_schema)
            table = self.client.create_table(table)
            print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")

    def _generate_property_id(self, marketing_name, street_address, city, zipcode):
        """
        Generate a unique ID hash based on marketing name, street address, city, and zipcode.

        Args:
            marketing_name (str): Property/marketing name
            street_address (str): Street address
            city (str): City name
            zipcode (str): ZIP code

        Returns:
            str: SHA256 hash of the combined address components
        """
        # Convert all inputs to strings and handle None values
        components = [
            str(marketing_name or ''),
            str(street_address or ''),
            str(city or ''),
            str(zipcode or '')
        ]

        # Normalize by converting to lowercase and stripping whitespace
        normalized_components = [comp.lower().strip() for comp in components]

        # Join components with a separator
        combined_string = '|'.join(normalized_components)

        # Generate SHA256 hash
        hash_object = hashlib.sha256(combined_string.encode('utf-8'))
        return hash_object.hexdigest()

    def prepare_tasks(self, batch_size=100000):
        """
        Load sitemap URLs and insert property URLs into the `zillow_urls` table.
        Skips the "off-market" sitemap.
        """
        sitemap_urls = [
            "https://www.zillow.com/xml/indexes/us/hdp/for-sale-by-agent.xml.gz",
            "https://www.zillow.com/xml/indexes/us/hdp/for-sale-by-owner.xml.gz",
        ]

        total_batches_processed = 0

        for sitemap_url in sitemap_urls:
            listing_type = sitemap_url.split('/')[-1].replace('.xml.gz', '')
            print(f"Processing sitemap: {listing_type}")

            listing_urls_generator = self._extract_listing_urls(sitemap_url)
            batch_entries = []

            for listing_url in listing_urls_generator:
                batch_entries.append({
                    'url': listing_url,
                    'type': listing_type,
                    'created_at': datetime.utcnow().isoformat()
                })

                # When batch is full, process it
                if len(batch_entries) >= batch_size:
                    self._insert_url_batch(batch_entries)
                    total_batches_processed += 1
                    print(f"Processed batch {total_batches_processed} with {len(batch_entries)} URLs")
                    batch_entries.clear()

            # Handle remaining entries for this sitemap
            if batch_entries:
                self._insert_url_batch(batch_entries)
                total_batches_processed += 1
                print(f"Processed final batch {total_batches_processed} with {len(batch_entries)} URLs")

            print(f"Completed sitemap: {listing_type}")

        print(f"Total batches processed: {total_batches_processed}")

    def _insert_url_batch(self, entries):
        """Insert batch of URLs into BigQuery with upsert logic."""
        table_id = f"{self.project_id}.{self.dataset_id}.zillow_urls"

        # Convert entries to the format expected by BigQuery
        formatted_entries = []
        for entry in entries:
            formatted_entries.append({
                'url': entry['url'],
                'type': entry['type'],
                'created_at': entry['created_at']
            })

        if formatted_entries:
            # Insert new entries using load_table_from_json
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            )

            job = self.client.load_table_from_json(formatted_entries, table_id, job_config=job_config)
            job.result()  # Wait for job to complete
            print(f"Inserted {len(formatted_entries)} new URLs (skipped {len(entries) - len(formatted_entries)} duplicates)")
        else:
            print(f"Skipped {len(entries)} URLs (all duplicates)")

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

    def process_tasks(self, max_properties=10000, start_offset=0, batch_size=1):
        """
        Process scraping tasks using BigQuery.
        """
        num_added = 0
        num_processed = start_offset
        total_batches_processed = 0

        try:
            while num_added < max_properties:
                # Get URLs to process
                urls = self._fetch_urls_to_scrape(limit=batch_size, offset=num_processed)
                if not urls:
                    print("No more URLs to scrape")
                    return

                batch_entries = []
                urls_to_update = []

                for url_row in urls:
                    url = url_row.url
                    num_processed += 1

                    print(f"extracting zillow url {url} (processed: {num_processed}, added: {num_added})")
                    try:
                        data = self.extract_from_website(url)
                        if data:
                            safe_data = self._prepare_data_for_db(data)
                            batch_entry = {
                                'source_url': url,
                                'created_at': datetime.utcnow().isoformat(),
                                **safe_data
                            }
                            batch_entries.append(batch_entry)
                            urls_to_update.append(url)
                    except Exception as e:
                        print(f"Error processing URL {url}: {e}")
                        continue

                # Insert the batch
                if batch_entries:
                    num_added += len(batch_entries)
                    self._insert_property_batch(batch_entries, urls_to_update)
                    print(f"uploaded batch of {len(batch_entries)} (total added: {num_added})")
                    total_batches_processed += 1

                # If we got fewer URLs than requested, we're done
                if len(urls) < batch_size:
                    print("Reached end of available URLs")
                    return

        except Exception as e:
            print(f"Error during processing: {e}")
            print(f"Resume with: process_tasks(start_offset={num_processed})")
            raise e

        print(f"Complete: {num_added} added, {num_processed} processed, {total_batches_processed} batches")

    def _fetch_urls_to_scrape(self, limit=1000, offset=0):
        """
        Fetch URLs to scrape with proper limit and offset support.
        Only returns URLs where ALL associated records have scraped_at IS NULL.
        """
        query = f"""
        SELECT url
        FROM `{self.project_id}.{self.dataset_id}.zillow_urls`
        GROUP BY url
        HAVING COUNTIF(scraped_at IS NOT NULL) = 0
        ORDER BY url
        LIMIT {limit}
        OFFSET {offset}
        """

        query_job = self.client.query(query)
        return list(query_job.result())

    def _prepare_data_for_db(self, data):
        """
        Prepares property details for BigQuery insertion.
        """
        # Generate unique ID based on address components
        property_id = self._generate_property_id(
            data.get('marketing_name'),
            data.get('street_address'),
            data.get('city'),
            data.get('zipcode')
        )

        def convert(value):
            if isinstance(value, dict):
                return value  # BigQuery handles JSON natively
            elif isinstance(value, list):
                if all(isinstance(item, (int, float, str, type(None))) for item in value):
                    return [float(item) if isinstance(item, (int, float)) else item for item in value]
                else:
                    return value  # BigQuery handles JSON arrays
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

        prepared_data = {field: convert(data.get(field)) for field in fields}
        prepared_data['id'] = property_id

        return prepared_data

    def _insert_property_batch(self, entries, urls_to_update):
        """Insert property batch into BigQuery."""
        table_id = f"{self.project_id}.{self.dataset_id}.zillow_property_details"

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
            UPDATE `{self.project_id}.{self.dataset_id}.zillow_urls`
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