#!/usr/bin/env python3
"""
Hydrate historical main-referee assignments using referee endpoint payloads.

Why this exists:
- Fixture endpoint backfills can hit stricter rate limits.
- Referee endpoint supports include=fixtures and returns fixture-referee links.
- We only persist links for fixtures already present in our DB.

Outcome:
- Expands public.fixture_referees historical depth for primary/main assignments.
- Enables accurate fixture_referee_stats windows (Last 5/10/20).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import psycopg2
from psycopg2.extras import Json, execute_values
import requests

DEFAULT_BASE_URL = "https://api.sportmonks.com/v3/football"

logger = logging.getLogger("hydrate_referee_history")

MAIN_REF_TYPE_IDS = {6}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Hydrate historical referee assignments via /referees/{id}?include=fixtures")
    parser.add_argument("--seed-days-back", type=int, default=30, help="Window to discover active referees from DB fixtures")
    parser.add_argument("--seed-days-forward", type=int, default=14, help="Window to discover active referees from DB fixtures")
    parser.add_argument("--history-days-back", type=int, default=700, help="History fixture window to persist")
    parser.add_argument("--history-days-forward", type=int, default=30, help="Future fixture window to persist")
    parser.add_argument("--max-referees", type=int, default=0, help="Limit referees processed (0 = all)")
    parser.add_argument("--sleep-seconds", type=float, default=0.15, help="Delay between API calls")
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--main-only", action="store_true", default=True, help="Persist only main referee role")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--report-json", type=str, default="")
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


def fetch_json_with_retry(
    *,
    session: requests.Session,
    url: str,
    timeout: int,
    max_attempts: int = 6,
) -> Dict[str, Any]:
    last_exc: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        try:
            resp = session.get(url, timeout=timeout)
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                sleep_for = int(retry_after) if retry_after and retry_after.isdigit() else min(30, attempt * 3)
                logger.warning("Rate limited (429) for %s; sleeping %ss", url, sleep_for)
                time.sleep(max(1, sleep_for))
                continue
            if 500 <= resp.status_code < 600:
                sleep_for = min(20, attempt * 2)
                logger.warning("Server error %s for %s; sleeping %ss", resp.status_code, url, sleep_for)
                time.sleep(sleep_for)
                continue
            resp.raise_for_status()
            payload = resp.json()
            if not isinstance(payload, dict):
                raise RuntimeError(f"Unexpected JSON payload type: {type(payload)}")
            return payload
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt >= max_attempts:
                break
            time.sleep(min(10, attempt))
    raise RuntimeError(f"Request failed after retries: {url}") from last_exc


def fetch_seed_referees(
    conn: psycopg2.extensions.connection,
    *,
    seed_days_back: int,
    seed_days_forward: int,
    max_referees: int,
) -> List[int]:
    sql = """
      select distinct fr.referee_id
      from public.fixture_referees fr
      join public.fixtures f
        on f.id = fr.fixture_id
      where f.starting_at >= (now() - make_interval(days => %s))
        and f.starting_at <= (now() + make_interval(days => %s))
        and (fr.is_primary = true or lower(coalesce(fr.role, '')) in ('main', 'referee'))
      order by fr.referee_id asc
    """
    params: List[Any] = [max(0, seed_days_back), max(0, seed_days_forward)]
    if max_referees > 0:
        sql += " limit %s"
        params.append(max_referees)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    return [int(row[0]) for row in rows if row and row[0] is not None]


def fetch_valid_fixture_ids(
    conn: psycopg2.extensions.connection,
    *,
    history_days_back: int,
    history_days_forward: int,
) -> Set[int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select id
            from public.fixtures
            where starting_at >= (now() - make_interval(days => %s))
              and starting_at <= (now() + make_interval(days => %s))
            """,
            (max(0, history_days_back), max(0, history_days_forward)),
        )
        rows = cur.fetchall()
    return {int(row[0]) for row in rows if row and row[0] is not None}


def fetch_existing_referee_ids(
    conn: psycopg2.extensions.connection,
    referee_ids: Iterable[int],
) -> Set[int]:
    ids = sorted({int(rid) for rid in referee_ids if int(rid) > 0})
    if not ids:
        return set()
    with conn.cursor() as cur:
        cur.execute("select id from public.referees where id = any(%s)", (ids,))
        rows = cur.fetchall()
    return {int(row[0]) for row in rows if row and row[0] is not None}


def normalize_role_from_type_id(type_id: Optional[int]) -> Tuple[str, bool]:
    if type_id in MAIN_REF_TYPE_IDS:
        return ("main", True)
    if type_id == 7:
        return ("assistant_1", False)
    if type_id == 8:
        return ("assistant_2", False)
    if type_id == 9:
        return ("fourth_official", False)
    if type_id == 10:
        return ("var", False)
    if type_id == 11:
        return ("avar", False)
    return ("unknown", False)


def parse_referee_fixture_links(
    payload: Dict[str, Any],
    *,
    valid_fixture_ids: Set[int],
    main_only: bool,
) -> List[Dict[str, Any]]:
    data = payload.get("data")
    if not isinstance(data, dict):
        return []
    referee_id = to_positive_int(data.get("id"))
    if not referee_id:
        return []

    fixtures = data.get("fixtures")
    if not isinstance(fixtures, list):
        return []

    rows: List[Dict[str, Any]] = []
    for entry in fixtures:
        if not isinstance(entry, dict):
            continue
        fixture_id = to_positive_int(entry.get("fixture_id"))
        entry_referee_id = to_positive_int(entry.get("referee_id")) or referee_id
        if not fixture_id or fixture_id not in valid_fixture_ids:
            continue
        type_id = to_positive_int(entry.get("type_id"))
        role, is_primary = normalize_role_from_type_id(type_id)
        if main_only and role != "main":
            continue
        rows.append(
            {
                "fixture_id": fixture_id,
                "referee_id": entry_referee_id,
                "role": role,
                "is_primary": is_primary,
                "source": "sportmonks",
                "extra": entry,
            }
        )
    return rows


def upsert_assignments(
    conn: psycopg2.extensions.connection,
    rows: Iterable[Dict[str, Any]],
) -> int:
    values: List[Tuple[Any, ...]] = []
    for row in rows:
        values.append(
            (
                row["fixture_id"],
                row["referee_id"],
                row["role"],
                bool(row.get("is_primary", False)),
                row.get("source", "sportmonks"),
                Json(row.get("extra") or {}),
            )
        )
    if not values:
        return 0

    sql = """
      insert into public.fixture_referees
        (fixture_id, referee_id, role, is_primary, source, extra)
      values %s
      on conflict (fixture_id, referee_id, role) do update
      set
        is_primary = excluded.is_primary,
        source = excluded.source,
        extra = coalesce(excluded.extra, public.fixture_referees.extra),
        updated_at = now(),
        last_synced_at = now()
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

    db_url = get_db_url()
    token = get_api_token()
    base_url = get_base_url()

    conn = psycopg2.connect(db_url)
    try:
        seed_referees = fetch_seed_referees(
            conn,
            seed_days_back=args.seed_days_back,
            seed_days_forward=args.seed_days_forward,
            max_referees=args.max_referees,
        )
        valid_fixture_ids = fetch_valid_fixture_ids(
            conn,
            history_days_back=args.history_days_back,
            history_days_forward=args.history_days_forward,
        )
        existing_referees = fetch_existing_referee_ids(conn, seed_referees)
    finally:
        conn.close()

    seed_referees = [rid for rid in seed_referees if rid in existing_referees]
    logger.info(
        "Referee history hydrate: referees=%s valid_fixtures=%s",
        len(seed_referees),
        len(valid_fixture_ids),
    )

    total_rows_raw = 0
    referees_failed = 0
    all_rows: List[Dict[str, Any]] = []
    session = requests.Session()
    try:
        for idx, referee_id in enumerate(seed_referees, start=1):
            url = f"{base_url}/referees/{referee_id}?api_token={token}&include=fixtures"
            try:
                payload = fetch_json_with_retry(session=session, url=url, timeout=args.timeout)
            except Exception as exc:  # noqa: BLE001
                referees_failed += 1
                logger.warning("referee_id=%s failed: %s", referee_id, exc)
                continue

            links = parse_referee_fixture_links(
                payload,
                valid_fixture_ids=valid_fixture_ids,
                main_only=args.main_only,
            )
            total_rows_raw += len(links)
            all_rows.extend(links)

            if args.sleep_seconds > 0 and idx < len(seed_referees):
                time.sleep(args.sleep_seconds)
    finally:
        session.close()

    dedup_rows = {
        (row["fixture_id"], row["referee_id"], row["role"]): row for row in all_rows
    }

    report: Dict[str, Any] = {
        "ok": True,
        "dry_run": bool(args.dry_run),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "seed_referees": len(seed_referees),
        "valid_fixture_ids": len(valid_fixture_ids),
        "main_only": bool(args.main_only),
        "referees_failed": referees_failed,
        "rows_raw": total_rows_raw,
        "rows_deduped": len(dedup_rows),
        "sample_referees": seed_referees[:10],
    }

    if args.dry_run:
        print(json.dumps(report, indent=2, ensure_ascii=False))
        write_report(args.report_json, report)
        return 0

    conn_write = psycopg2.connect(db_url)
    try:
        upserted = upsert_assignments(conn_write, dedup_rows.values())
    finally:
        conn_write.close()

    report["upserted_assignments"] = upserted
    print(json.dumps(report, indent=2, ensure_ascii=False))
    write_report(args.report_json, report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
