#!/usr/bin/env python3
"""
Sync fixture-referee assignments from SportMonks into Supabase.

- Reads fixture IDs from public.fixtures within a time window
- Fetches /fixtures/{id}?include=referees
- Upserts public.referees + public.fixture_referees
- Gracefully skips fixtures with no referee payload
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import psycopg2
from psycopg2.extras import Json, execute_values
import requests

DEFAULT_BASE_URL = "https://api.sportmonks.com/v3/football"

logger = logging.getLogger("sync_fixture_referees")

STATE_STATUSES = {"pending", "assigned", "no_assignment", "error"}

REFEREE_TYPE_ROLE_MAP: Dict[int, Tuple[str, bool]] = {
    6: ("main", True),
    7: ("assistant_1", False),
    8: ("assistant_2", False),
    9: ("fourth_official", False),
    10: ("var", False),
    11: ("avar", False),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync SportMonks fixture-referee assignments into Supabase")
    parser.add_argument("--days-back", type=int, default=30)
    parser.add_argument("--days-forward", type=int, default=31)
    parser.add_argument(
        "--resync-hours",
        type=int,
        default=12,
        help="Only re-fetch fixtures whose referee assignments are missing or stale by this many hours",
    )
    parser.add_argument(
        "--fixture-id",
        type=int,
        default=0,
        help="Restrict the run to one fixture (useful for controlled repair/backfill; 0 = window)",
    )
    parser.add_argument("--limit-fixtures", type=int, default=0, help="Limit number of fixtures for testing")
    parser.add_argument("--sleep-seconds", type=float, default=0.3, help="Delay between fixture API calls")
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--enforce-backfill-window",
        action="store_true",
        help="Allow run only during 02:00-05:00 UTC (for heavy backfill runs)",
    )
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


def enforce_backfill_window() -> None:
    now_utc = datetime.now(timezone.utc)
    if not (2 <= now_utc.hour < 5):
        raise RuntimeError(
            f"Backfill window enforcement active. Current UTC hour={now_utc.hour}; allowed window is 02:00-05:00 UTC"
        )


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


def normalize_role(value: Any) -> str:
    raw = non_empty(value)
    if not raw:
        return "unknown"
    text = raw.lower().strip()
    aliases = {
        "referee": "main",
        "main": "main",
        "main referee": "main",
        "assistant referee": "assistant",
        "assistant": "assistant",
        "assistant 1": "assistant_1",
        "assistant 2": "assistant_2",
        "video assistant referee": "var",
        "var": "var",
        "fourth official": "fourth_official",
    }
    if text in aliases:
        return aliases[text]
    text = re.sub(r"[^a-z0-9]+", "_", text).strip("_")
    return text or "unknown"


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


def fetch_fixture_ids(
    conn: psycopg2.extensions.connection,
    *,
    days_back: int,
    days_forward: int,
    resync_hours: int,
    fixture_id: int,
    limit_fixtures: int,
) -> List[int]:
    sql = """
        select id
        from public.fixtures
        left join public.fixture_referee_sync_state state
          on state.fixture_id = public.fixtures.id
        where starting_at >= (now() - make_interval(days => %s))
          and starting_at <= (now() + make_interval(days => %s))
          and (%s = 0 or public.fixtures.id = %s)
          and coalesce(state.next_attempt_at, now()) <= now()
          and (
            state.fixture_id is null
            or state.status in ('pending', 'no_assignment', 'error')
            or state.last_successful_at < (now() - make_interval(hours => %s))
          )
        order by starting_at asc
    """
    if limit_fixtures > 0:
        sql += " limit %s"
    with conn.cursor() as cur:
        if limit_fixtures > 0:
            cur.execute(
                sql,
                (
                    max(0, days_back),
                    max(0, days_forward),
                    max(0, fixture_id),
                    max(0, fixture_id),
                    max(1, resync_hours),
                    limit_fixtures,
                ),
            )
        else:
            cur.execute(
                sql,
                (max(0, days_back), max(0, days_forward), max(0, fixture_id), max(0, fixture_id), max(1, resync_hours)),
            )
        rows = cur.fetchall()
    return [int(row[0]) for row in rows if row and row[0] is not None]


def payload_hash(payload: Dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def mark_fixture_attempt(conn: psycopg2.extensions.connection, fixture_id: int) -> None:
    """Persist an attempt before calling the provider so interrupted runs remain visible."""
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.fixture_referee_sync_state
              (fixture_id, status, last_attempted_at, next_attempt_at, attempt_count, updated_at)
            values (%s, 'pending', now(), now(), 1, now())
            on conflict (fixture_id) do update
            set status = case
                  when public.fixture_referee_sync_state.status = 'assigned' then 'assigned'
                  else 'pending'
                end,
                last_attempted_at = now(),
                next_attempt_at = now(),
                attempt_count = public.fixture_referee_sync_state.attempt_count + 1,
                updated_at = now()
            """,
            (fixture_id,),
        )
    conn.commit()


def record_fixture_state(
    conn: psycopg2.extensions.connection,
    fixture_id: int,
    *,
    status: str,
    assignment_count: int = 0,
    response_hash_value: Optional[str] = None,
    error: Optional[str] = None,
    retry_after: timedelta = timedelta(hours=12),
) -> None:
    if status not in STATE_STATUSES:
        raise ValueError(f"Unsupported referee sync state: {status}")

    if status == "error":
        next_attempt_sql = "%s::interval + now()"
        attempt_value = "1"
        successful_value = "NULL"
    else:
        next_attempt_sql = "%s::interval + now()"
        attempt_value = "0"
        successful_value = "now()"

    sql = f"""
        insert into public.fixture_referee_sync_state
          (fixture_id, status, last_successful_at, next_attempt_at, attempt_count,
           assignment_count, response_hash, last_error, updated_at)
        values (%s, %s, {successful_value}, {next_attempt_sql}, {attempt_value}, %s, %s, %s, now())
        on conflict (fixture_id) do update
        set status = excluded.status,
            last_successful_at = coalesce(excluded.last_successful_at, public.fixture_referee_sync_state.last_successful_at),
            next_attempt_at = excluded.next_attempt_at,
            attempt_count = case
              when excluded.status = 'error' then public.fixture_referee_sync_state.attempt_count
              else excluded.attempt_count
            end,
            assignment_count = excluded.assignment_count,
            response_hash = coalesce(excluded.response_hash, public.fixture_referee_sync_state.response_hash),
            last_error = excluded.last_error,
            updated_at = now()
    """
    params: Tuple[Any, ...]
    if status == "error":
        params = (fixture_id, status, retry_after, assignment_count, response_hash_value, error)
    else:
        params = (fixture_id, status, retry_after, assignment_count, response_hash_value, error)
    with conn.cursor() as cur:
        cur.execute(sql, params)
    conn.commit()


def load_country_ids(conn: psycopg2.extensions.connection) -> Optional[set[int]]:
    try:
        with conn.cursor() as cur:
            cur.execute("select id from public.countries")
            rows = cur.fetchall()
        return {int(row[0]) for row in rows if row and row[0] is not None}
    except Exception:  # noqa: BLE001
        return None


def load_existing_referee_ids(
    conn: psycopg2.extensions.connection, referee_ids: Iterable[int]
) -> set[int]:
    ids = sorted({int(rid) for rid in referee_ids if int(rid) > 0})
    if not ids:
        return set()
    with conn.cursor() as cur:
        cur.execute("select id from public.referees where id = any(%s)", (ids,))
        rows = cur.fetchall()
    return {int(row[0]) for row in rows if row and row[0] is not None}


def extract_referee_items(fixture_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    root = fixture_payload.get("data")
    if not isinstance(root, dict):
        return []

    refs = root.get("referees")
    if isinstance(refs, dict):
        refs = refs.get("data")
    if not isinstance(refs, list):
        return []

    return [row for row in refs if isinstance(row, dict)]


def normalize_referee_profile(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    rid = to_positive_int(row.get("referee_id") or row.get("id"))
    if not rid:
        return None

    # Fixture referee includes usually return relation rows only (id, fixture_id, referee_id, type_id).
    # In that case, avoid writing placeholder profile rows that would overwrite canonical names.
    if not any(non_empty(row.get(key)) for key in ("name", "fullname", "short_name", "common_name", "image_path")):
        return None

    country_id = to_positive_int(row.get("country_id"))
    if country_id is None and isinstance(row.get("country"), dict):
        country_id = to_positive_int((row.get("country") or {}).get("id"))

    city_id = to_positive_int(row.get("city_id"))
    if city_id is None and isinstance(row.get("city"), dict):
        city_id = to_positive_int((row.get("city") or {}).get("id"))

    name = non_empty(row.get("name")) or non_empty(row.get("fullname")) or f"Referee {rid}"

    return {
        "id": rid,
        "name": name,
        "short_name": non_empty(row.get("short_name")),
        "common_name": non_empty(row.get("common_name")),
        "image_path": non_empty(row.get("image_path")),
        "country_id": country_id,
        "city_id": city_id,
        "source": "sportmonks",
        "extra": row,
    }


def normalize_assignment(fixture_id: int, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    rid = to_positive_int(row.get("referee_id") or row.get("id"))
    if not rid:
        return None

    pivot = row.get("pivot") if isinstance(row.get("pivot"), dict) else {}
    type_id = to_positive_int(row.get("type_id"))
    mapped_role, mapped_primary = ("unknown", False)
    if type_id is not None and type_id in REFEREE_TYPE_ROLE_MAP:
        mapped_role, mapped_primary = REFEREE_TYPE_ROLE_MAP[type_id]

    role = normalize_role(
        mapped_role if mapped_role != "unknown" else None
        or row.get("role")
        or row.get("type")
        or row.get("designation")
        or pivot.get("role")
        or pivot.get("type")
    )

    is_primary = bool(
        row.get("is_primary")
        or row.get("main")
        or mapped_primary
        or role == "main"
        or str(row.get("referee_type", "")).lower() == "main"
    )

    return {
        "fixture_id": fixture_id,
        "referee_id": rid,
        "role": role,
        "is_primary": is_primary,
        "source": "sportmonks",
        "extra": row,
    }


def upsert_referees(
    conn: psycopg2.extensions.connection,
    rows: Iterable[Dict[str, Any]],
    valid_country_ids: Optional[set[int]] = None,
) -> int:
    dedup: Dict[int, Dict[str, Any]] = {}
    for row in rows:
        dedup[row["id"]] = row
    if not dedup:
        return 0

    values: List[Tuple[Any, ...]] = []
    for row in dedup.values():
        country_id = row.get("country_id")
        if valid_country_ids is not None and country_id is not None and country_id not in valid_country_ids:
            country_id = None
        values.append(
            (
                row["id"],
                row["name"],
                row.get("short_name"),
                row.get("common_name"),
                row.get("image_path"),
                country_id,
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


def upsert_assignments(conn: psycopg2.extensions.connection, rows: Iterable[Dict[str, Any]]) -> int:
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


def insert_placeholder_referees(
    conn: psycopg2.extensions.connection, referee_ids: Iterable[int]
) -> int:
    """Keep a valid FK for relation-only provider rows until profile sync supplies names."""
    ids = sorted({int(value) for value in referee_ids if int(value) > 0})
    if not ids:
        return 0
    values = [(referee_id, f"Referee {referee_id}", "sportmonks", Json({})) for referee_id in ids]
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            insert into public.referees (id, name, source, extra)
            values %s
            on conflict (id) do nothing
            """,
            values,
            page_size=500,
        )
    conn.commit()
    return len(values)


def reconcile_assignments(
    conn: psycopg2.extensions.connection,
    rows: Iterable[Dict[str, Any]],
) -> Tuple[int, int]:
    """Upsert current assignments and remove stale SportMonks rows per fixture."""
    rows = list(rows)
    grouped: Dict[int, List[Dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(int(row["fixture_id"]), []).append(row)
    if not grouped:
        return 0, 0

    deleted = 0
    inserted = 0
    with conn.cursor() as cur:
        for fixture_id, fixture_rows in grouped.items():
            keys = {(int(row["referee_id"]), str(row["role"])) for row in fixture_rows}
            placeholders = ",".join("(%s, %s)" for _ in keys)
            params: List[Any] = [fixture_id]
            for referee_id, role in sorted(keys):
                params.extend([referee_id, role])
            cur.execute(
                f"""
                delete from public.fixture_referees
                where fixture_id = %s
                  and source = 'sportmonks'
                  and (referee_id, role) not in ({placeholders})
                """,
                params,
            )
            deleted += cur.rowcount

        values = [
            (
                row["fixture_id"],
                row["referee_id"],
                row["role"],
                bool(row.get("is_primary", False)),
                row.get("source", "sportmonks"),
                Json(row.get("extra") or {}),
            )
            for row in rows
        ]
        execute_values(
            cur,
            """
            insert into public.fixture_referees
              (fixture_id, referee_id, role, is_primary, source, extra)
            values %s
            on conflict (fixture_id, referee_id, role) do update
            set is_primary = excluded.is_primary,
                source = excluded.source,
                extra = coalesce(excluded.extra, public.fixture_referees.extra),
                updated_at = now(),
                last_synced_at = now()
            """,
            values,
            page_size=500,
        )
        inserted = len(values)
    conn.commit()
    return inserted, deleted


def cleanup_legacy_assignment_ids(
    conn: psycopg2.extensions.connection, fixture_ids: Iterable[int]
) -> int:
    ids = sorted({int(fid) for fid in fixture_ids if int(fid) > 0})
    if not ids:
        return 0
    with conn.cursor() as cur:
        cur.execute(
            """
            delete from public.fixture_referees fr
            where fr.fixture_id = any(%s)
              and fr.source = 'sportmonks'
              and fr.role = 'unknown'
              and fr.extra ? 'id'
              and fr.extra ? 'referee_id'
              and (fr.extra->>'id') ~ '^[0-9]+$'
              and (fr.extra->>'referee_id') ~ '^[0-9]+$'
              and fr.referee_id = (fr.extra->>'id')::bigint
              and fr.referee_id <> (fr.extra->>'referee_id')::bigint
            """,
            (ids,),
        )
        deleted = cur.rowcount
    conn.commit()
    return deleted


def touch_fixture_sync_timestamp(conn: psycopg2.extensions.connection, fixture_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            update public.fixture_referees
            set last_synced_at = now(), updated_at = now()
            where fixture_id = %s
            """,
            (fixture_id,),
        )
    conn.commit()


def write_report(path: str, report: Dict[str, Any]) -> None:
    if not path:
        return
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, ensure_ascii=False)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_args()

    if args.enforce_backfill_window:
        enforce_backfill_window()

    db_url = get_db_url()
    token = get_api_token()
    base_url = get_base_url()

    conn = psycopg2.connect(db_url)
    try:
        fixture_ids = fetch_fixture_ids(
            conn,
            days_back=args.days_back,
            days_forward=args.days_forward,
            resync_hours=args.resync_hours,
            fixture_id=args.fixture_id,
            limit_fixtures=args.limit_fixtures,
        )
    finally:
        conn.close()

    logger.info(
        "Fixtures queued=%s (days_back=%s days_forward=%s resync_hours=%s)",
        len(fixture_ids),
        args.days_back,
        args.days_forward,
        args.resync_hours,
    )

    total_assignments = 0
    fixtures_with_refs = 0
    fixtures_without_refs = 0
    fixture_errors = 0
    touched_fixtures = 0
    state_updates = 0
    all_profiles: List[Dict[str, Any]] = []
    all_assignments: List[Dict[str, Any]] = []
    successful_fixture_ids: List[int] = []
    state_conn = None if args.dry_run else psycopg2.connect(db_url)

    session = requests.Session()
    try:
        for idx, fixture_id in enumerate(fixture_ids, start=1):
            if state_conn is not None:
                mark_fixture_attempt(state_conn, fixture_id)
            url = f"{base_url}/fixtures/{fixture_id}?api_token={token}&include=referees"
            try:
                payload = fetch_json_with_retry(session=session, url=url, timeout=args.timeout)
            except Exception as exc:  # noqa: BLE001
                fixture_errors += 1
                logger.warning("fixture_id=%s failed: %s", fixture_id, exc)
                if state_conn is not None:
                    record_fixture_state(
                        state_conn,
                        fixture_id,
                        status="error",
                        error=str(exc)[:1000],
                        retry_after=timedelta(minutes=15),
                    )
                    state_updates += 1
                continue

            ref_items = extract_referee_items(payload)
            if not ref_items:
                fixtures_without_refs += 1
                logger.info("fixture_id=%s has no referee payload; will retry", fixture_id)
                if state_conn is not None:
                    record_fixture_state(
                        state_conn,
                        fixture_id,
                        status="no_assignment",
                        response_hash_value=payload_hash(payload),
                        retry_after=timedelta(hours=2),
                    )
                    state_updates += 1
                continue

            fixtures_with_refs += 1
            for item in ref_items:
                profile = normalize_referee_profile(item)
                if profile:
                    all_profiles.append(profile)
                assignment = normalize_assignment(fixture_id, item)
                if assignment:
                    all_assignments.append(assignment)
                    total_assignments += 1

            touched_fixtures += 1
            successful_fixture_ids.append(fixture_id)
            if args.sleep_seconds > 0 and idx < len(fixture_ids):
                time.sleep(args.sleep_seconds)
    finally:
        session.close()

    # De-dup profiles by referee id, assignments by (fixture_id, referee_id, role)
    dedup_profiles = {row["id"]: row for row in all_profiles}
    dedup_assignments = {
        (row["fixture_id"], row["referee_id"], row["role"]): row for row in all_assignments
    }
    assignment_referee_ids = sorted({row["referee_id"] for row in dedup_assignments.values()})

    conn_meta = psycopg2.connect(db_url)
    try:
        existing_referee_ids = load_existing_referee_ids(conn_meta, assignment_referee_ids)
        valid_country_ids = load_country_ids(conn_meta)
    finally:
        conn_meta.close()

    missing_referee_ids = sorted(set(assignment_referee_ids) - existing_referee_ids)

    report: Dict[str, Any] = {
        "ok": fixture_errors == 0,
        "dry_run": bool(args.dry_run),
        "fixtures_scanned": len(fixture_ids),
        "fixtures_with_referees": fixtures_with_refs,
        "fixtures_without_referees": fixtures_without_refs,
        "fixtures_failed": fixture_errors,
        "fixtures_touched": touched_fixtures,
        "assignments_total_raw": total_assignments,
        "assignments_total_deduped": len(dedup_assignments),
        "unique_referees": len(assignment_referee_ids),
        "missing_referees_in_db": len(missing_referee_ids),
        "missing_referee_ids_sample": missing_referee_ids[:10],
        "sample_fixture_ids": fixture_ids[:10],
        "sample_referee_ids": assignment_referee_ids[:10],
    }

    if args.dry_run:
        print(json.dumps(report, indent=2, ensure_ascii=False))
        write_report(args.report_json, report)
        return 0

    conn_write = psycopg2.connect(db_url)
    try:
        upserted_referees = upsert_referees(conn_write, dedup_profiles.values(), valid_country_ids=valid_country_ids)
        placeholder_referees = insert_placeholder_referees(conn_write, missing_referee_ids)
        upserted_assignments, deleted_stale_assignments = reconcile_assignments(
            conn_write, dedup_assignments.values()
        )
        cleaned_legacy_assignments = cleanup_legacy_assignment_ids(
            conn_write, {row["fixture_id"] for row in dedup_assignments.values()}
        )
    finally:
        conn_write.close()

    if state_conn is not None:
        try:
            for fixture_id in successful_fixture_ids:
                fixture_assignments = [
                    row for row in dedup_assignments.values() if row["fixture_id"] == fixture_id
                ]
                response_hash_value = payload_hash(
                    {
                        "fixture_id": fixture_id,
                        "assignments": fixture_assignments,
                    }
                )
                record_fixture_state(
                    state_conn,
                    fixture_id,
                    status="assigned",
                    assignment_count=len(fixture_assignments),
                    response_hash_value=response_hash_value,
                    retry_after=timedelta(hours=max(1, args.resync_hours)),
                )
                state_updates += 1
        finally:
            state_conn.close()

    report["upserted_referees"] = upserted_referees
    report["upserted_assignments"] = upserted_assignments
    report["placeholder_referees_inserted"] = placeholder_referees
    report["deleted_stale_assignments"] = deleted_stale_assignments
    report["cleaned_legacy_assignments"] = cleaned_legacy_assignments
    report["state_updates"] = state_updates

    print(json.dumps(report, indent=2, ensure_ascii=False))
    write_report(args.report_json, report)
    return 1 if fixture_errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
