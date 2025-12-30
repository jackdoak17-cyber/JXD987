#!/usr/bin/env python3
"""
Supabase REST preflight with retries and HTML/Cloudflare error detection.

Writes rest_ok/rest_http/rest_error to an env output file when provided.
"""

from __future__ import annotations

import argparse
import os
import time
from typing import Optional, Tuple

import requests


def is_html_error(text: str) -> bool:
    if not text:
        return False
    snippet = text.lstrip()[:200].lower()
    if snippet.startswith("<html") or snippet.startswith("<!doctype html"):
        return True
    if "cloudflare" in snippet or "error 525" in snippet:
        return True
    return False


def probe(url: str, headers: dict, timeout: int, retries: int) -> Tuple[bool, int, str]:
    attempt = 0
    last_status = 0
    last_error = "unknown"
    while attempt <= retries:
        attempt += 1
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
        except requests.RequestException as exc:
            last_error = f"request_error:{exc.__class__.__name__}"
            time.sleep(min(30, 2**attempt))
            continue

        last_status = resp.status_code
        body = resp.text or ""
        if resp.ok and not is_html_error(body):
            return True, last_status, "ok"

        if last_status in (522, 524, 525) or is_html_error(body):
            last_error = "transient_html_or_cf"
            time.sleep(min(30, 2**attempt))
            continue

        if last_status in (502, 503, 504):
            last_error = f"transient_{last_status}"
            time.sleep(min(30, 2**attempt))
            continue

        last_error = f"http_{last_status}"
        break

    return False, last_status, last_error


def write_env(path: Optional[str], rest_ok: bool, rest_http: int, rest_error: str) -> None:
    if not path:
        return
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"REST_OK={'true' if rest_ok else 'false'}\n")
        f.write(f"REST_HTTP={rest_http}\n")
        f.write(f"REST_ERROR={rest_error}\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", default="/rest/v1/seasons?select=id&limit=1")
    parser.add_argument("--timeout", type=int, default=20)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--env-out", default=None)
    args = parser.parse_args()

    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    if not supabase_url or not supabase_key:
        rest_ok = False
        rest_http = 0
        rest_error = "missing_env"
        print(f"rest_preflight=not_ok http={rest_http} error={rest_error}")
        write_env(args.env_out, rest_ok, rest_http, rest_error)
        return

    url = supabase_url.rstrip("/") + args.path
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Accept": "application/json",
    }

    rest_ok, rest_http, rest_error = probe(url, headers, args.timeout, args.retries)
    status = "ok" if rest_ok else "not_ok"
    print(f"rest_preflight={status} http={rest_http} error={rest_error}")
    write_env(args.env_out, rest_ok, rest_http, rest_error)


if __name__ == "__main__":
    main()
