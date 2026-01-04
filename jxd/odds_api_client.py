from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Optional

import requests


class OddsApiError(RuntimeError):
    pass


class OddsApiRateLimit(OddsApiError):
    pass


@dataclass
class OddsApiStats:
    total_calls: int = 0
    api_time_seconds: float = 0.0
    rate_limit_hits: int = 0
    rate_limit_sleeps: int = 0
    calls_by_endpoint: Dict[str, int] = field(default_factory=dict)
    last_rate_limit: Optional[Dict[str, object]] = None


class OddsApiClient:
    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        timeout: int = 30,
        retries: Optional[int] = None,
        sleep_base: Optional[float] = None,
        sleep_max: Optional[float] = None,
    ) -> None:
        self.api_key = api_key or os.environ.get("ODDS_API_KEY")
        if not self.api_key:
            raise OddsApiError("Missing ODDS_API_KEY")
        self.base_url = (base_url or os.environ.get("ODDS_API_BASE") or "https://api2.odds-api.io/v3").rstrip(
            "/"
        )
        self.timeout = timeout
        self.retries = int(retries or os.environ.get("ODDS_API_RETRIES", "3"))
        self.sleep_base = float(sleep_base or os.environ.get("ODDS_API_SLEEP_BASE", "1"))
        self.sleep_max = float(sleep_max or os.environ.get("ODDS_API_SLEEP_MAX", "10"))
        self.session = requests.Session()
        self.stats = OddsApiStats()

    def request(self, path: str, params: Optional[Dict[str, object]] = None) -> object:
        url = f"{self.base_url}/{path.lstrip('/')}"
        params = dict(params or {})
        params["apiKey"] = self.api_key
        endpoint_key = path.lstrip("/")
        last_error: Optional[Exception] = None
        for attempt in range(self.retries + 1):
            start = time.time()
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout)
            except requests.RequestException as exc:
                last_error = exc
                time.sleep(min(self.sleep_base * (2**attempt), self.sleep_max))
                continue
            elapsed = time.time() - start
            self.stats.total_calls += 1
            self.stats.api_time_seconds += elapsed
            self.stats.calls_by_endpoint[endpoint_key] = self.stats.calls_by_endpoint.get(endpoint_key, 0) + 1

            if resp.status_code == 429:
                self.stats.rate_limit_hits += 1
                retry_after = resp.headers.get("Retry-After")
                reset = resp.headers.get("x-ratelimit-reset")
                remaining = resp.headers.get("x-ratelimit-remaining")
                sleep_seconds = min(self.sleep_base * (2**attempt), self.sleep_max)
                if retry_after:
                    try:
                        sleep_seconds = min(float(retry_after), self.sleep_max)
                    except ValueError:
                        pass
                elif reset:
                    try:
                        reset_dt = datetime.fromisoformat(reset.replace("Z", "+00:00"))
                        seconds = max(0.0, (reset_dt - datetime.utcnow().replace(tzinfo=reset_dt.tzinfo)).total_seconds())
                        sleep_seconds = min(seconds, self.sleep_max)
                    except Exception:
                        pass
                self.stats.last_rate_limit = {
                    "endpoint": endpoint_key,
                    "status": resp.status_code,
                    "remaining": remaining,
                    "retry_after": retry_after,
                    "reset": reset,
                }
                self.stats.rate_limit_sleeps += 1
                time.sleep(sleep_seconds)
                last_error = OddsApiRateLimit(f"429 rate limit for {endpoint_key}")
                continue

            if resp.status_code >= 500:
                last_error = OddsApiError(f"Server error {resp.status_code} for {endpoint_key}")
                time.sleep(min(self.sleep_base * (2**attempt), self.sleep_max))
                continue

            if resp.status_code >= 400:
                raise OddsApiError(f"HTTP {resp.status_code} for {endpoint_key}: {resp.text[:200]}")

            return resp.json()

        raise OddsApiError(f"Odds API request failed for {endpoint_key}: {last_error}")
