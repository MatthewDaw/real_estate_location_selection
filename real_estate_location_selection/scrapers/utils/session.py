import logging
import time

import requests
from browserforge.headers import HeaderGenerator
from google.cloud import secretmanager


def get_secret(secret_name: str) -> str:
    client = secretmanager.SecretManagerServiceClient()
    secret_path = f"projects/flowing-flame-464314-j5/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": secret_path})
    return response.payload.data.decode("UTF-8")

EVOMI_PASSWORD = get_secret("EVOMI_PASSWORD")

class Session:

    def __init__(self):
        self.evomi_pass = EVOMI_PASSWORD
        proxy = f"http://tim6:{self.evomi_pass}_country-US@core-residential.evomi.com:1000"
        self.proxies = {"http": proxy, "https": proxy}
        self.hg = HeaderGenerator()

    @property
    def headers(self):
        return self.hg.generate()

    @property
    def _session(self):
        session = requests.Session()
        session.headers.update(self.headers)
        return session

    def _base_request(self, url, method, **kwargs):
        """All requests for requests based scrapers flow through this function.
        There are random errors that appear when we scrape at scale and the
        point of this functin is to make sure that we can handle them all from
        one place.
        """
        if not kwargs.get("timeout"):
            kwargs["timeout"] = 60
        attempts = 0
        content = ""
        while True:
            try:
                response = getattr(self._session, method)(url, **kwargs)
                try:
                    content = response.text
                    if response.status_code != 429 and response.status_code != 503:
                        return response
                    kwargs["proxies"] = self.proxies
                    kwargs["timeout"] = 60
                except UnicodeDecodeError:
                    return response
            except requests.exceptions.TooManyRedirects as e:
                raise e
            except requests.exceptions.Timeout:
                if "https://leasing.realpage.com" in url:
                    # If site doesn't exist, this just hangs.
                    return None
                if attempts > 0 and "sightmap.com" not in url:
                    # If we timeout more than once, let's try a proxy
                    kwargs["proxies"] = self.proxies
                    kwargs["timeout"] = 60
                logging.info(f"timeout:attempt-{attempts}:{url}")
            except requests.exceptions.ProxyError:
                logging.info(f"proxy_error:attempt-{attempts}:{url}")
                time.sleep(0.5)
            except Exception as e:
                if ".appfolio.com" in url:
                    # If this is an appfolio url, they sometimes stop responding.
                    # This is not a failure
                    time.sleep(1)
                    continue
                logging.info(f"other:attempt-{attempts}:{url}")
                logging.info(f"{str(e)}:{url}")
            if attempts > 5:
                logging.info(f"{content}:{url}")
                return None
            attempts = attempts + 1

    def get(self, url, **kwargs):
        return self._base_request(url, "get", **kwargs)

    def post(self, url, **kwargs):
        return self._base_request(url, "post", **kwargs)

    def put(self, url, **kwargs):
        return self._base_request(url, "put", **kwargs)
