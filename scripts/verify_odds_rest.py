#!/usr/bin/env python3
"""
Verify odds export via Supabase REST (no psql).
"""

import os
import time
from typing import Optional

import requests

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")


def require_env() -> None:
    missing = []
    if not SUPABASE_URL:
        missing.append("SUPABASE_URL")
    if not SUPABASE_KEY:
        missing.append("SUPABASE_SERVICE_ROLE_KEY")
    if missing:
        raise SystemExit(f"Missing env vars: {', '.join(missing)}")


def is_html_error(text: str) -> bool:
    if not text:
        return False
    snippet = text.lstrip()[:200].lower()
    return snippet.startswith("<html") or snippet.startswith("<!doctype html") or "cloudflare" in snippet


def count_rows(filter_expr: Optional[str] = None, retries: int = 5) -> int:
    url = SUPABASE_URL.rstrip("/") + "/rest/v1/odds_outcomes?select=id&limit=1"
    if filter_expr:
        url = f"{url}&{filter_expr}"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Prefer": "count=exact",
    }
    attempt = 0
    while True:
        attempt += 1
        try:
            resp = requests.get(url, headers=headers, timeout=20)
        except requests.RequestException as exc:
            if attempt > retries:
                raise SystemExit(f"REST count failed after retries: {exc}")
            time.sleep(min(30, 2**attempt))
            continue
        if resp.status_code in (502, 503, 504, 522, 524, 525) or is_html_error(resp.text or ""):
            if attempt > retries:
                raise SystemExit(f"REST count failed {resp.status_code}: {resp.text}")
            time.sleep(min(30, 2**attempt))
            continue
        if not resp.ok:
            raise SystemExit(f"REST count failed {resp.status_code}: {resp.text}")
        content_range = resp.headers.get("content-range") or resp.headers.get("Content-Range")
        if not content_range:
            return 0
        try:
            total = int(content_range.split("/")[-1])
        except Exception:
            total = 0
        return total


def main() -> None:
    require_env()
    total = count_rows()
    mapped = count_rows("participant_id=not.is.null")
    print(f"odds_outcomes_total={total}")
    print(f"odds_outcomes_mapped={mapped}")


if __name__ == "__main__":
    main()
