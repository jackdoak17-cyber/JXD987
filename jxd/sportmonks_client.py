import logging
import os
import time
from typing import Dict, Iterator, List, Optional

import requests


class SportMonksError(Exception):
    def __init__(self, message: str, status_code: Optional[int] = None, response_text: Optional[str] = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.response_text = response_text


class SportMonksClient:
    """
    Minimal SportMonks v3 football client with pagination and retry logic.
    """

    def __init__(
        self,
        api_token: Optional[str] = None,
        base_url: str = "https://api.sportmonks.com/v3/football/",
        timeout: int = 20,
        max_retries: int = 5,
    ) -> None:
        self.log = logging.getLogger(__name__)
        self.api_token = api_token or os.environ.get("SPORTMONKS_API_TOKEN")
        if not self.api_token:
            raise SportMonksError("SPORTMONKS_API_TOKEN is required")
        self.base_url = base_url.rstrip("/") + "/"
        self.timeout = timeout
        self.max_retries = max_retries
        self.rate_limit_retries = int(os.environ.get("SM_RATE_LIMIT_RETRIES", str(max_retries)))
        self.rate_limit_sleep_base = float(os.environ.get("SM_RATE_LIMIT_SLEEP_BASE", "1"))
        self.rate_limit_sleep_max = float(os.environ.get("SM_RATE_LIMIT_SLEEP_MAX", "60"))
        self.stats: Dict[str, object] = {
            "total_calls": 0,
            "total_time_seconds": 0.0,
            "by_endpoint": {},
            "rate_limit_hits": 0,
            "rate_limit_retries": 0,
        }

    def _record_stats(self, endpoint: str, elapsed: float) -> None:
        self.stats["total_calls"] = int(self.stats.get("total_calls", 0)) + 1
        self.stats["total_time_seconds"] = float(self.stats.get("total_time_seconds", 0.0)) + float(elapsed)
        by_endpoint = self.stats.get("by_endpoint")
        if not isinstance(by_endpoint, dict):
            by_endpoint = {}
            self.stats["by_endpoint"] = by_endpoint
        by_endpoint[endpoint] = int(by_endpoint.get(endpoint, 0)) + 1

    def _retry_after_seconds(self, resp: requests.Response) -> Optional[float]:
        retry_after = resp.headers.get("Retry-After") or resp.headers.get("retry-after")
        if retry_after:
            try:
                return float(retry_after)
            except ValueError:
                return None
        reset = (
            resp.headers.get("X-RateLimit-Reset")
            or resp.headers.get("x-ratelimit-reset")
            or resp.headers.get("RateLimit-Reset")
        )
        if reset:
            try:
                reset_val = float(reset)
            except ValueError:
                return None
            now = time.time()
            if reset_val > now:
                return max(0.0, reset_val - now)
            if reset_val > 0 and reset_val < 3600:
                return reset_val
        return None

    def request(self, method: str, endpoint: str, params: Optional[Dict[str, object]] = None) -> Dict:
        url = self.base_url + endpoint.lstrip("/")
        params = dict(params or {})
        params.setdefault("api_token", self.api_token)
        attempt = 0
        backoff = 1.0
        while True:
            attempt += 1
            try:
                start = time.time()
                resp = requests.request(method, url, params=params, timeout=self.timeout)
                elapsed = time.time() - start
                self._record_stats(endpoint, elapsed)
            except Exception as exc:
                if attempt >= self.max_retries:
                    raise SportMonksError(f"Request failed after retries: {exc}") from exc
                time.sleep(backoff)
                backoff *= 2
                continue

            if resp.status_code == 200:
                try:
                    return resp.json()
                except Exception as exc:
                    raise SportMonksError(
                        f"Invalid JSON from SportMonks: {exc}",
                        status_code=resp.status_code,
                        response_text=resp.text,
                    ) from exc

            if resp.status_code == 429:
                self.stats["rate_limit_hits"] = int(self.stats.get("rate_limit_hits", 0)) + 1
                if attempt >= self.rate_limit_retries:
                    raise SportMonksError(
                        f"SportMonks request failed 429: {resp.text}",
                        status_code=resp.status_code,
                        response_text=resp.text,
                    )
                retry_after = self._retry_after_seconds(resp)
                sleep_for = retry_after if retry_after is not None else min(backoff, self.rate_limit_sleep_max)
                self.stats["rate_limit_retries"] = int(self.stats.get("rate_limit_retries", 0)) + 1
                self.log.warning(
                    "Rate limited on %s (attempt %s/%s). Sleeping %.1fs.",
                    endpoint,
                    attempt,
                    self.rate_limit_retries,
                    sleep_for,
                )
                time.sleep(max(0.0, sleep_for))
                backoff = min(backoff * 2, self.rate_limit_sleep_max)
                continue

            if 500 <= resp.status_code < 600:
                if attempt >= self.max_retries:
                    raise SportMonksError(
                        f"SportMonks request failed {resp.status_code}: {resp.text}",
                        status_code=resp.status_code,
                        response_text=resp.text,
                    )
                time.sleep(backoff)
                backoff = min(backoff * 2, 16)
                continue

            raise SportMonksError(
                f"SportMonks request failed {resp.status_code}: {resp.text}",
                status_code=resp.status_code,
                response_text=resp.text,
            )

    def fetch_collection(
        self,
        endpoint: str,
        params: Optional[Dict[str, object]] = None,
        includes: Optional[List[str]] = None,
        per_page: int = 50,
    ) -> Iterator[Dict]:
        """
        Yield rows across all pages. Works with standard v3 pagination (pagination or meta.pagination).
        """
        base_params = dict(params or {})
        if includes:
            base_params["include"] = ";".join(includes)
        base_params.setdefault("per_page", per_page)
        page = 1
        while True:
            page_params = dict(base_params)
            page_params["page"] = page
            payload = self.request("GET", endpoint, params=page_params)
            rows = []
            if isinstance(payload, dict):
                if isinstance(payload.get("data"), list):
                    rows = payload["data"]
                elif isinstance(payload.get("data"), dict):
                    rows = [payload["data"]]
            if not rows:
                return

            for row in rows:
                yield row

            pagination = None
            if isinstance(payload, dict):
                pagination = payload.get("pagination") or (payload.get("meta") or {}).get("pagination")

            if pagination:
                current_page = pagination.get("current_page") or pagination.get("page") or page
                total_pages = pagination.get("total_pages")
                has_more = pagination.get("has_more")
                next_page_val = pagination.get("next_page")
                try:
                    next_page_int = int(next_page_val) if next_page_val is not None else None
                except Exception:
                    next_page_int = None

                if next_page_int and next_page_int > current_page:
                    page = next_page_int
                    continue
                if total_pages and current_page < total_pages:
                    page = current_page + 1
                    continue
                if has_more:
                    page = current_page + 1
                    continue
                return

            if len(rows) < per_page:
                return
            page += 1

    def fetch_single(self, endpoint: str, params: Optional[Dict[str, object]] = None, includes: Optional[List[str]] = None) -> Dict:
        payload = self.request("GET", endpoint, params={**(params or {}), **({"include": ",".join(includes)} if includes else {})})
        if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
            return payload["data"]
        return payload
