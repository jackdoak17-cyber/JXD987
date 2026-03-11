#!/usr/bin/env python3
"""
Sync referee profiles from SportMonks into Supabase.

- Idempotent upsert into public.referees
- Safe retry/backoff on 429/5xx
- Optional dry-run mode
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

import psycopg2
from psycopg2.extras import Json, execute_values
import requests

DEFAULT_BASE_URL = "https://api.sportmonks.com/v3/football"
DEFAULT_PER_PAGE = 100


logger = logging.getLogger("sync_referees")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync SportMonks referees into Supabase")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and normalize only; do not write to DB")
    parser.add_argument("--max-pages", type=int, default=0, help="Limit pages for testing (0 = all pages)")
    parser.add_argument("--per-page", type=int, default=DEFAULT_PER_PAGE)
    parser.add_argument("--sleep-seconds", type=float, default=0.25, help="Delay between API page calls")
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--report-json", type=str, default="", help="Optional path to write run summary JSON")
    return parser.parse_args()


def get_db_url() -> str:
    value = (
        os.getenv("SUPABASE_DB_URL_SESSION")
        or os.getenv("SUPABASE_DB_URL_POOLER")
        or os.getenv("SUPABASE_DB_URL")
    )
    if not value:
        raise RuntimeError("Missing SUPABASE_DB_URL_SESSION / SUPABASE_DB_URL_POOLER / SUPABASE_DB_URL")
    return value


def get_api_token() -> str:
    value = os.getenv("SPORTMONKS_API_TOKEN") or os.getenv("ODDS_API_KEY")
    if not value:
        raise RuntimeError("Missing SPORTMONKS_API_TOKEN (or ODDS_API_KEY)")
    return value


def get_base_url() -> str:
    return (os.getenv("FOOTBALL_API_BASE_URL") or DEFAULT_BASE_URL).rstrip("/")


def to_positive_int(value: Any) -> Optional[int]:
    try:
        number = int(value)
    except Exception:  # noqa: BLE001
        return None
    return number if number > 0 else None


def non_empty(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    value = value.strip()
    return value or None


def fetch_json_with_retry(
    *, session: requests.Session, url: str, timeout: int, max_attempts: int = 5
) -> Dict[str, Any]:
    last_exc: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        try:
            resp = session.get(url, timeout=timeout)
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                sleep_for = int(retry_after) if retry_after and retry_after.isdigit() else attempt
                logger.warning("Rate limited (429) for %s; sleeping %ss", url, sleep_for)
                time.sleep(max(1, sleep_for))
                continue
            if 500 <= resp.status_code < 600:
                logger.warning("Server error %s for %s", resp.status_code, url)
                time.sleep(min(10, attempt))
                continue
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, dict):
                raise RuntimeError(f"Unexpected JSON payload type: {type(data)}")
            return data
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt >= max_attempts:
                break
            time.sleep(min(10, attempt))
    raise RuntimeError(f"Request failed after retries: {url}") from last_exc


def iter_referee_pages(
    *,
    session: requests.Session,
    base_url: str,
    token: str,
    per_page: int,
    max_pages: int,
    timeout: int,
    sleep_seconds: float,
) -> Iterable[Dict[str, Any]]:
    page = 1
    while True:
        if max_pages > 0 and page > max_pages:
            break
        url = (
            f"{base_url}/referees?api_token={token}"
            f"&page={page}&per_page={max(1, per_page)}"
        )
        payload = fetch_json_with_retry(session=session, url=url, timeout=timeout)
        rows = payload.get("data")
        if isinstance(rows, list):
            for row in rows:
                if isinstance(row, dict):
                    yield row
        pagination = payload.get("pagination") or {}
        has_more = bool(pagination.get("has_more"))
        if not has_more:
            break
        page += 1
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)


def normalize_referee(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    rid = to_positive_int(row.get("id"))
    if not rid:
        return None

    country_id = to_positive_int(row.get("country_id"))
    if country_id is None and isinstance(row.get("country"), dict):
        country_id = to_positive_int((row.get("country") or {}).get("id"))

    city_id = to_positive_int(row.get("city_id"))
    if city_id is None and isinstance(row.get("city"), dict):
        city_id = to_positive_int((row.get("city") or {}).get("id"))

    name = non_empty(row.get("name")) or non_empty(row.get("fullname")) or f"Referee {rid}"
    short_name = non_empty(row.get("short_name"))
    common_name = non_empty(row.get("common_name"))
    image_path = non_empty(row.get("image_path"))

    return {
        "id": rid,
        "name": name,
        "short_name": short_name,
        "common_name": common_name,
        "image_path": image_path,
        "country_id": country_id,
        "city_id": city_id,
        "source": "sportmonks",
        "extra": row,
    }


def upsert_referees(conn: psycopg2.extensions.connection, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0

    values: List[Tuple[Any, ...]] = []
    for row in rows:
        values.append(
            (
                row["id"],
                row["name"],
                row.get("short_name"),
                row.get("common_name"),
                row.get("image_path"),
                row.get("country_id"),
                row.get("city_id"),
                row.get("source", "sportmonks"),
                Json(row.get("extra") or {}),
            )
        )

    sql = """
        insert into public.referees
          (id, name, short_name, common_name, image_path, country_id, city_id, source, extra)
        values %s
        on conflict (id) do update
        set
          name = excluded.name,
          short_name = coalesce(excluded.short_name, public.referees.short_name),
          common_name = coalesce(excluded.common_name, public.referees.common_name),
          image_path = coalesce(excluded.image_path, public.referees.image_path),
          country_id = coalesce(excluded.country_id, public.referees.country_id),
          city_id = coalesce(excluded.city_id, public.referees.city_id),
          source = excluded.source,
          extra = coalesce(excluded.extra, public.referees.extra),
          updated_at = now()
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, values, page_size=500)
    conn.commit()
    return len(values)


def write_report(path: str, report: Dict[str, Any]) -> None:
    if not path:
        return
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, ensure_ascii=False)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_args()

    token = get_api_token()
    base_url = get_base_url()

    normalized: List[Dict[str, Any]] = []
    seen_ids: set[int] = set()
    skipped = 0

    session = requests.Session()
    try:
        for raw in iter_referee_pages(
            session=session,
            base_url=base_url,
            token=token,
            per_page=args.per_page,
            max_pages=args.max_pages,
            timeout=args.timeout,
            sleep_seconds=args.sleep_seconds,
        ):
            row = normalize_referee(raw)
            if not row:
                skipped += 1
                continue
            rid = row["id"]
            if rid in seen_ids:
                continue
            seen_ids.add(rid)
            normalized.append(row)
    finally:
        session.close()

    report = {
        "ok": True,
        "dry_run": bool(args.dry_run),
        "fetched": len(normalized),
        "skipped": skipped,
        "sample_referee_ids": [row["id"] for row in normalized[:10]],
    }

    logger.info("Referees fetched=%s skipped=%s dry_run=%s", len(normalized), skipped, args.dry_run)

    if args.dry_run:
        print(json.dumps(report, indent=2, ensure_ascii=False))
        write_report(args.report_json, report)
        return 0

    conn = psycopg2.connect(get_db_url())
    try:
        written = upsert_referees(conn, normalized)
    finally:
        conn.close()

    report["upserted"] = written
    logger.info("Referees upserted=%s", written)
    print(json.dumps(report, indent=2, ensure_ascii=False))
    write_report(args.report_json, report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
