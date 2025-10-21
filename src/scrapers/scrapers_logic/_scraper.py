import base64
import gzip
import io
import json
import logging
import time
import uuid
from datetime import datetime, timezone
from multiprocessing.pool import ThreadPool
from typing import TypedDict, Optional

from google.cloud import bigquery, pubsub_v1
from playwright.sync_api import Browser, BrowserContext, Page, Request, Route, Response

import random
from scrapers.scrapers_logic.utils.big_query_wrapper import create_client
from scrapers.scrapers_logic.utils.session import Session

class Task(TypedDict):
    source: str
    lastmod: str
    input: str
    now: str


class _Scraper:
    """Implement _Scraper in all scrapers. Responsible for publishing the
    messages to Google Cloud Pub/Sub as well as establishing the correct
    interface for each scraper.
    """

    source: str | None = None  # Override
    _page: Page | None = None
    _browser: Browser | None = None
    _context: BrowserContext | None = None
    use_proxies_camoufox = False
    use_resource_intercept = True
    rotate_user_agent = False
    proxy_service = "evomi"

    def __init__(self, browser: Browser, default_timeout=10000):
        if self.source is None:
            raise Exception("Assign a source name")
        self.session = Session()
        self.bigquery_client = bigquery.Client()
        self.pubsub_client = pubsub_v1.PublisherClient()
        self.topic_path = self.pubsub_client.topic_path(
            "hello-data-ai", "scraper-residential-topic"
        )
        self.now = datetime.now(timezone.utc).date().isoformat()
        self._browser = browser
        self.default_timeout = default_timeout


        self.now = datetime.now(timezone.utc).date().isoformat()
        self._browser = browser
        self.default_timeout = default_timeout

        project_id = 'flowing-flame-464314-j5'
        dataset_id = 'real_estate'
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.client = create_client(project=project_id)
        self.dataset_ref = self.client.get_dataset(dataset_id)

    @property
    def page(self):
        return self._page

    def _get_previous_sitemap(self):
        """For apartments.com, zillow.com and zumper.com, we are able to see if
        a url has changed before we scrape it so in order to determine which
        urls to scrape, we pull in the values of all previous sitemaps to
        compare against.

        The BigQuery table is clustered by source so we don't need to do a full
        table scan each time.
        """
        previous_sitemap: set[tuple[str, str]] = set()
        if self.source not in ["apartments", "zillow", "zumper"]:
            return previous_sitemap
        query = f"""
            SELECT
                DISTINCT
                loc,
                FORMAT_TIMESTAMP("%Y-%m-%dT%X", lastmod) AS lastmod
            FROM `hello-data-ai.rentsource.sitemap`
            WHERE created_on > "2023-01-01"
                AND source = '{self.source}';
        """
        table = self.bigquery_client.query(query).result().to_arrow()
        for batch in table.to_batches():
            for row in batch.to_pylist():
                previous_sitemap.add((row["loc"], row["lastmod"]))
        return previous_sitemap

    def _get_shuffled_valid_tasks(self):
        """Every day, we want to process the urls in a random order to limit
        any sort of traffic patterns. Since we are loading the tasks into a
        staging BigQuery table, we randomize them on query.
        """
        query = f"""
            SELECT
                DISTINCT *
            FROM rentsource.__scraper_tasks
            WHERE now = "{self.now}"
            ORDER BY RAND()
        """
        valid_tasks: list[Task] = []
        table = self.bigquery_client.query(query).result().to_arrow()
        for batch in table.to_batches():
            for row in batch.to_pylist():
                valid_tasks.append(
                    {
                        "source": row["source"],
                        "lastmod": row["lastmod"],
                        "input": row["input"],
                        "now": row["now"].isoformat(),
                    }
                )
        return valid_tasks

    def fill_tasks(self, target_count: int):
        """Ingest the implemented prepare_tasks function. Push the valid tasks
        into the Queue for processing and the unchanged tasks into BigQuery
        sitemap directly since no scrape needs to take place. We shuffle the
        tasks once the number of sources in today's staging table equals the
        number of sources or the target_count.
        """
        previous_run = self._get_previous_sitemap()
        valid_tasks: list[dict[str, str]] = []
        unchanged_rows: set[tuple[str, str]] = []
        current_queue: set[str] = set()
        for url, lastmod in self.prepare_tasks():
            if url in current_queue:
                # Duplicates may find their way here, make sure to skip those.
                continue
            current_queue.add(url)
            clean_last_mod = lastmod.split(".")[0].replace("Z", "") if lastmod else None
            if lastmod is None or (url, clean_last_mod) not in previous_run:
                valid_tasks.append({"input": url, "lastmod": lastmod})
            elif lastmod:
                unchanged_rows.append((url, lastmod))
        for i in range(0, len(unchanged_rows), 1000):
            self.bigquery_client.insert_rows_json(
                "rentsource.sitemap",
                [
                    {
                        "source": self.source,
                        "loc": loc,
                        "lastmod": lastmod,
                        "created_on": self.now,
                        "folder_id": str(uuid.uuid5(uuid.NAMESPACE_DNS, loc)),
                    }
                    for loc, lastmod in unchanged_rows[i : i + 1000]
                ],
            )
        assert len(valid_tasks) > 0
        # Because we want to process all tasks in a random order, we need to
        # first push all tasks to BigQuery before we can randomly insert them
        # into Pub/Sub
        for i in range(0, len(valid_tasks), 1000):
            self.bigquery_client.insert_rows_json(
                "rentsource.__scraper_tasks",
                [
                    {
                        "source": self.source,
                        "lastmod": task["lastmod"],
                        "input": task["input"],
                        "now": self.now,
                    }
                    for task in valid_tasks[i : i + 1000]
                ],
            )
        # Once we have tasks for all sources, let's initialize the queue.
        bigquery_row = self.bigquery_client.query(
            f"""
            SELECT
                DISTINCT source
            FROM rentsource.__scraper_tasks
            WHERE now = "{self.now}"
        """
        )
        total_rows: int = bigquery_row.result().total_rows
        if total_rows == target_count:
            with ThreadPool(50) as p:
                p.map(self._send_task, self._get_shuffled_valid_tasks())

    def _send_task(self, task: Task):
        """Send the task to Pub/Sub. Wrap in an infinite loop because sometimes
        Google can fail us and we want to infinitely try until it works.
        """
        while True:
            try:
                self.pubsub_client.publish(
                    self.topic_path,
                    data=json.dumps(task).encode("utf-8"),
                    source=task["source"],
                ).result()
                return
            except Exception:
                pass

    def prepare_tasks(self) -> list[tuple[str, str]]:
        """Return a list of urls. these will be published to a Cloud Pub/Sub
        queue.
        """
        raise Exception("Implement prepare_tasks")

    def process_task(self, input: str):
        """The return value from this function will be saved to a blob at
            gs://hello-data-rentsource/bulk_data/<folder_id>/<now>.json
        and streamed to BigQuery.
        """
        raise Exception("Implement process_task")

    def _eval_urls(
        self,
        urls: list[str],
        timeout: str,
        method: str,
        headers: str,
        body: str,
        is_json: str,
        referrer=None,
    ):
        """This function injects JavaScript to fetch the contents of urls from
        the active playwright page. This allows us to make many parallel
        requests that all appear as if they are being sent from the website
        itself.
        """
        referrer = "undefined" if not referrer else f"'{referrer}'"
        code = f"""
            async () => {{
                const urls = {urls};
                return await Promise.all(urls.map(url => {{
                    return new Promise(resolve => {{
                        const makeRequest = () => {{
                            const controller = new AbortController();
                            const timeoutId = setTimeout(
                                () => controller.abort(), {timeout});
                            const p = fetch(url, {{
                                referrer: {referrer},
                                method: '{method}',
                                headers: {headers},
                                body: {body},
                                signal: controller.signal
                            }}).then((data) => {{
                                if (url.endsWith('.gz')) {{
                                    data.blob().then((blob) => {{
                                        const reader = new FileReader();
                                        reader.onloadend = () => {{
                                            const b64Result = atob(reader.result.split("base64,")[1]);
                                            resolve({{ success: true, data: reader.result }});
                                        }}
                                        reader.readAsDataURL(blob);
                                    }});
                                }} else {{
                                    data.text().then((d) => {{
                                        const lowerD = d.toLowerCase();
                                        if (lowerD.indexOf('404 - not found') > -1) {{
                                            resolve({{ success: true, data: null }});
                                        }} else if (lowerD.indexOf('<title>404</title>') > -1) {{
                                            resolve({{ success: false, data: d }});
                                        }} else if (lowerD.indexOf('<title>just a moment...</title>') > -1) {{
                                            resolve({{ success: false, data: d }});
                                        }} else if (lowerD.indexOf('<title>attention required! | cloudflare</title>') > -1) {{
                                            resolve({{ success: false, data: d }});
                                        }} else if (lowerD.indexOf('<title>access to this page has been denied</title>') > -1) {{
                                            resolve({{ success: false, data: d }});
                                        }} else if (lowerD.indexOf('<title>access denied</title>') > -1) {{
                                            resolve({{ success: false, data: d }});
                                        }} else if (lowerD.indexOf('sorry, you have been blocked') > -1) {{
                                            resolve({{ success: false, data: d }});
                                        }} else if (lowerD.indexOf('sorry, this page is locked due to multiple hits from your machine.') > -1) {{
                                            resolve({{ success: false, data: d }});
                                        }} else if (lowerD.indexOf('error code 502') > -1) {{
                                            resolve({{ success: false, data: d }});
                                        }} else if ({is_json}) {{
                                            try {{
                                                JSON.parse(d);
                                                resolve({{ success: true, data: d }});
                                            }} catch (error) {{
                                                resolve({{ success: false, data: null }});
                                            }}
                                        }} else {{
                                            resolve({{ success: true, data: d }});
                                        }}
                                    }}).catch(() => {{
                                        resolve({{ success: false, data: null }});
                                    }});
                                }}
                            }}).catch((e) => {{
                                if (e.message === 'Failed to fetch') {{
                                    resolve({{ success: true, data: null }});
                                }} else {{
                                    resolve({{ success: false, data: null }});
                                }}
                            }});
                        }}
                        makeRequest();
                    }});
                }}));
            }}
        """
        while True:
            try:
                page = self._get_page()
                # When we evalulate the code, we don't want the intercept to block
                # anything.
                if self.use_resource_intercept:
                    self.page.unroute("**/*", self.block_unwanted_requests)
                results: list[dict] = page.evaluate(code)
                if self.use_resource_intercept:
                    self.page.route("**/*", self.block_unwanted_requests)
                success_results: list[str] = []
                for idx, jr in enumerate(results):
                    if jr["success"]:
                        if jr["data"] and jr["data"].startswith(
                            "data:application/octet-stream;base64,"
                        ):
                            _, encoded = jr["data"].split("base64,", 1)
                            decompressed_content = gzip.GzipFile(
                                fileobj=io.BytesIO(base64.b64decode(encoded))
                            ).read()
                            response_text = decompressed_content.decode("utf-8")
                            success_results.append(response_text)
                        else:
                            success_results.append(jr["data"])
                if len(results) == len(success_results):
                    return success_results
                logging.info(f"{self.source} - Robot detected, _eval_urls")
                # Closing and opening the browser generates a new browser
                # fingerprint.
                self.close_page()
                if self.source == "entrata":
                    self.goto_url(json.loads(headers)["Referer"])
                time.sleep(random.randint(3, 7))
            except Exception as e:
                if "Target page, context or browser has been closed" in str(e):
                    raise e

    def _get_page(self) -> Page:
        if not self._page:
            if self.use_proxies_camoufox:
                # Direct all browser traffic through residential proxies
                # currently at $0.50/GB.
                proxy = {}
                proxy = {
                    "server": "premium-residential.evomi.com:1000",
                    "username": self.session.evomi_username,
                    "password": f"{self.session.evomi_pass}_country-US",
                }

                self._context = self._browser.new_context(
                    ignore_https_errors=True,
                    proxy=proxy,
                )
            else:
                self._context = self._browser.new_context()
            # Getting the context/page will raise an exception when browser/context crashes
            page = self._context.new_page()

            if self.use_resource_intercept:
                page.route("**/*", self.block_unwanted_requests)
            page.set_default_navigation_timeout(self.default_timeout)
            goto = self.get_goto()
            if goto:
                # A default goto url is optional but if it exists, make sure to
                # get the page with the provided url. This is important if the
                # scraper is making any requests with _eval_urls.
                try:
                    self._safe_goto(page, goto)
                except Exception:
                    return self._get_page()
            self._page = page
        if self.rotate_user_agent:
            self._page.set_extra_http_headers(
                {"User-Agent": self.session.headers["User-Agent"]}
            )
        return self._page

    def block_unwanted_requests(self, route: Route, request: Request):
        """Intercept all requests made by the browser and only let document
        requests through. This makes sure that we don't pay for any
        unwanted content if we are using residential proxies. This also
        makes the load time much faster.
        """
        if request.resource_type in ["document", "fetch"]:
            route.continue_()
        else:
            route.abort()

    def _safe_goto(
        self, page: Page, url: str, referer: Optional[str] = "https://www.google.com", timeout: Optional[int] = None
    ) -> Response:
        """Go to a url, making sure that after navigating, you are not being
        checked as a robot. If this or any other failure occurs, shut down the
        browser and raise an exception.
        """
        try:
            if timeout:
                response = page.goto(url, wait_until="domcontentloaded", referer=referer, timeout=timeout)
            else:
                response = page.goto(url, wait_until="domcontentloaded", referer=referer)
            if self._browser_robot_detected(page.content()):
                raise Exception(f"{self.source} - Robot detected, goto_url")
            return response
        except Exception as e:
            logging.info(e)
            self.close_page()
            time.sleep(5)
            raise e

    def goto_url(
        self, url: str, referer: Optional[str] = "https://www.google.com",  timeout: Optional[int] = None
    ) -> Response:
        """Safely visit a url, making sure that when the function finishes, it
        for certain is on the requested url.
        """
        page = self._get_page()
        try:
            return self._safe_goto(page, url, referer, timeout)
        except Exception:
            return self.goto_url(url, referer, timeout)

    def _browser_robot_detected(self, content: str):
        """Sometimes we get flagged by a robot detection service like
        CloudFlare. This function can be used to check and see if the current
        page is facing a robot challenge or not.
        """
        lower_content = content.lower()
        return (
            # RentCafe sites use a 404 to indicate a robot
            "<title>404</title>" in lower_content
            # Entrata sites and others using CloudFlare presents this when it
            # isn't sure of the client is a robot or not.
            or "<title>just a moment...</title>" in lower_content
            # RentCafe.com will present this error message when the IP used is
            # blocked.
            or "<title>attention required! | cloudflare</title>" in lower_content
            # Zillow.com and StreetEasy.com uses HUMAN fka PerimeterX and this
            # is the error message sent when it isn't happy.
            or "<title>access to this page has been denied</title>" in lower_content
            # Apartments.com when a blocked residential IP or VPN is used to
            # access their website.
            or "<title>access denied</title>" in lower_content
            # Sometimes our proxy fails to connect, try it again.
            or "failed to initalize proxy" in lower_content
        )

    def close_page(self):
        """Closing the context and page acts like a reset of the browser state."""
        if self._page:
            self._page.close()
            self._page = None
        if self._context:
            self._context.close()
            self._context = None

    def get_goto(self):
        """(Optional) Return an initial page for requests to send from."""
        return None
