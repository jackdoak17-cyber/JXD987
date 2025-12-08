from __future__ import annotations

import logging
import time
from typing import Dict, Iterable, List, Optional

import requests


log = logging.getLogger(__name__)


class RateLimiter:
    """
    Simple rate limiter to respect hourly caps.
    """

    def __init__(self, requests_per_hour: int) -> None:
        self.min_interval = 3600.0 / float(requests_per_hour)
        self._last_ts = 0.0

    def wait(self) -> None:
        now = time.time()
        delta = now - self._last_ts
        sleep_for = self.min_interval - delta
        if sleep_for > 0:
            time.sleep(sleep_for)
        self._last_ts = time.time()


class SportMonksError(Exception):
    pass


class SportMonksClient:
    def __init__(
        self,
        api_token: str,
        base_url: str = "https://api.sportmonks.com/v3/football",
        requests_per_hour: int = 3500,
        timeout: int = 30,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "Authorization": f"Bearer {api_token}",
            }
        )
        self.rate_limiter = RateLimiter(requests_per_hour=requests_per_hour)

    def _request(self, path: str, params: Optional[Dict[str, object]] = None) -> Dict:
        url = f"{self.base_url}/{path.lstrip('/')}"
        self.rate_limiter.wait()
        response = self.session.get(url, params=params or {}, timeout=self.timeout)
        if not response.ok:
            raise SportMonksError(
                f"SportMonks request failed ({response.status_code}): {response.text}"
            )
        try:
            return response.json()
        except ValueError as exc:
            raise SportMonksError(f"Invalid JSON in response for {url}") from exc

    def fetch_collection(
        self,
        path: str,
        params: Optional[Dict[str, object]] = None,
        includes: Optional[List[str]] = None,
        per_page: int = 200,
    ) -> Iterable[Dict]:
        params = params.copy() if params else {}
        params.setdefault("per_page", per_page)
        if includes:
            params["include"] = ",".join(includes)

        page = 1
        while True:
            params["page"] = page
            payload = self._request(path, params=params)
            items = payload.get("data", []) or []
            for item in items:
                yield item

            pagination = (
                payload.get("pagination")
                or payload.get("meta", {}).get("pagination", {})
                or {}
            )
            has_more = pagination.get("has_more")
            next_page = pagination.get("next_page")
            current = pagination.get("current_page")
            total_pages = pagination.get("total_pages")

            if next_page:
                page = next_page
                continue
            if has_more is True:
                page = (current or page) + 1
                continue
            if current and total_pages and current < total_pages:
                page = current + 1
                continue
            break

    def fetch_single(
        self, path: str, params: Optional[Dict[str, object]] = None
    ) -> Dict:
        params = params.copy() if params else {}
        payload = self._request(path, params=params)
        data = payload.get("data")
        if data is None:
            raise SportMonksError(f"No data returned for {path}")
        return data

