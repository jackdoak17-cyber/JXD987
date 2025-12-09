from __future__ import annotations

import logging
import time
from typing import Dict, Iterable, List, Optional
from urllib.parse import urlparse, parse_qs

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
        use_filters_populate: bool = True,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.use_filters_populate = use_filters_populate
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "Authorization": f"Bearer {api_token}",
                "X-Api-Token": api_token,
            }
        )
        self.rate_limiter = RateLimiter(requests_per_hour=requests_per_hour)

    def _request(self, path: str, params: Optional[Dict[str, object]] = None) -> Dict:
        url = f"{self.base_url}/{path.lstrip('/')}"
        self.rate_limiter.wait()
        merged_params = {"api_token": self.session.headers.get("X-Api-Token")}
        if params:
            merged_params.update(params)
        response = self.session.get(url, params=merged_params, timeout=self.timeout)
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
        includes: Optional[Iterable[str] | str] = None,
        per_page: int = 200,
        filters: Optional[str] = None,
        id_after: Optional[int] = None,
    ) -> Iterable[Dict]:
        params = params.copy() if params else {}
        params.setdefault("per_page", per_page)
        if includes:
            if isinstance(includes, str):
                params["include"] = includes
            else:
                params["include"] = ";".join(includes)
        if filters:
            params["filters"] = filters
        elif self.use_filters_populate and not includes:
            # Best-practice from SportMonks docs: populate allows per_page=1000
            params["filters"] = "populate"
            params["per_page"] = max(per_page, 1000)
        if id_after is not None:
            filters_current = params.get("filters")
            extra = f"idAfter:{id_after}"
            if filters_current:
                params["filters"] = f"{filters_current};{extra}"
            else:
                params["filters"] = extra

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
                if isinstance(next_page, int):
                    page = next_page
                    continue
                if isinstance(next_page, str):
                    # SportMonks sometimes returns full URLs; extract `page` query param
                    parsed = urlparse(next_page)
                    qs_page = parse_qs(parsed.query).get("page")
                    if qs_page and qs_page[0].isdigit():
                        page = int(qs_page[0])
                        continue
                    if next_page.isdigit():
                        page = int(next_page)
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

    def get_raw(self, path: str, params: Optional[Dict[str, object]] = None) -> Dict:
        """
        Low-level GET when the endpoint shape is not standard (odds endpoints, etc.).
        """
        return self._request(path, params=params or {})
