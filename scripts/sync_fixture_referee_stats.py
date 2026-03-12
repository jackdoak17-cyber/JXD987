#!/usr/bin/env python3
"""
Build fixture-level referee stats snapshot rows from assigned primary referees.

- Reads fixture -> primary referee mapping from DB within a time window
- Fetches /referees/{id}?include=statistics.details.type from SportMonks (one call per unique referee)
- Upserts public.fixture_referee_stats
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import psycopg2
from psycopg2.extras import execute_values
import requests

DEFAULT_BASE_URL = "https://api.sportmonks.com/v3/football"

TYPE_YELLOW_CARDS = 84
TYPE_FOULS = 56
TYPE_RED_CARDS = 83
TYPE_CORNERS = 34

logger = logging.getLogger("sync_fixture_referee_stats")


@dataclass
class RefereeSnapshot:
    name: Optional[str]
    avg_yellow_cards: Optional[float]
    avg_fouls: Optional[float]
    avg_corners: Optional[float]
    avg_red_cards: Optional[float]
    sample: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync fixture_referee_stats from SportMonks referee stats")
    parser.add_argument("--days-back", type=int, default=30)
    parser.add_argument("--days-forward", type=int, default=14)
    parser.add_argument("--limit-fixtures", type=int, default=0)
    parser.add_argument("--sleep-seconds", type=float, default=0.2)
    parser.add_argument("--timeout", type=int, default=30)
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


def to_number(value: Any) -> Optional[float]:
    try:
        n = float(value)
    except Exception:  # noqa: BLE001
        return None
    return n if n == n else None


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
                raise RuntimeError(f"Unexpected payload type: {type(payload)}")
            return payload
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt >= max_attempts:
                break
            time.sleep(min(10, attempt))
    raise RuntimeError(f"Request failed after retries: {url}") from last_exc


def weighted_average_for_type(stats_entries: Iterable[Dict[str, Any]], type_id: int) -> Tuple[Optional[float], int]:
    weighted_sum = 0.0
    total_count = 0
    for entry in stats_entries:
        details = entry.get("details")
        if not isinstance(details, list):
            continue
        for detail in details:
            if not isinstance(detail, dict):
                continue
            if int(detail.get("type_id") or 0) != type_id:
                continue
            all_stats = (((detail.get("value") or {}).get("all") or {}))
            avg = to_number(all_stats.get("average"))
            cnt = to_number(all_stats.get("count"))
            if avg is None or cnt is None or cnt <= 0:
                continue
            weighted_sum += avg * cnt
            total_count += int(cnt)
    if total_count <= 0:
        return None, 0
    return weighted_sum / total_count, total_count


def fetch_referee_snapshot(
    *, session: requests.Session, base_url: str, token: str, referee_id: int, timeout: int
) -> Optional[RefereeSnapshot]:
    url = f"{base_url}/referees/{referee_id}?api_token={token}&include=statistics.details.type"
    payload = fetch_json_with_retry(session=session, url=url, timeout=timeout)
    root = payload.get("data")
    if not isinstance(root, dict):
        return None

    name = root.get("name")
    if isinstance(name, str):
        name = name.strip() or None
    else:
        name = None

    statistics = root.get("statistics")
    if isinstance(statistics, dict):
        stats_entries = statistics.get("data")
    else:
        stats_entries = statistics

    if not isinstance(stats_entries, list):
        stats_entries = []

    avg_yellow, c_yellow = weighted_average_for_type(stats_entries, TYPE_YELLOW_CARDS)
    avg_fouls, c_fouls = weighted_average_for_type(stats_entries, TYPE_FOULS)
    avg_corners, c_corners = weighted_average_for_type(stats_entries, TYPE_CORNERS)
    avg_red, c_red = weighted_average_for_type(stats_entries, TYPE_RED_CARDS)
    sample = max(c_yellow, c_fouls, c_corners, c_red, 0)

    return RefereeSnapshot(
        name=name,
        avg_yellow_cards=avg_yellow,
        avg_fouls=avg_fouls,
        avg_corners=avg_corners,
        avg_red_cards=avg_red,
        sample=sample,
    )


def fetch_fixture_primary_referees(
    conn: psycopg2.extensions.connection,
    *,
    days_back: int,
    days_forward: int,
    limit_fixtures: int,
) -> List[Tuple[int, int, Optional[str]]]:
    sql = """
        select
          f.id as fixture_id,
          pick.referee_id,
          r.name as referee_name
        from public.fixtures f
        join lateral (
          select fr.referee_id
          from public.fixture_referees fr
          where fr.fixture_id = f.id
          order by
            fr.is_primary desc,
            case when fr.role = 'main' then 0 else 1 end,
            fr.updated_at desc
          limit 1
        ) pick on true
        left join public.referees r
          on r.id = pick.referee_id
        where f.starting_at >= (now() - make_interval(days => %s))
          and f.starting_at <= (now() + make_interval(days => %s))
        order by f.starting_at asc
    """
    if limit_fixtures > 0:
        sql += " limit %s"

    with conn.cursor() as cur:
        if limit_fixtures > 0:
            cur.execute(sql, (max(0, days_back), max(0, days_forward), limit_fixtures))
        else:
            cur.execute(sql, (max(0, days_back), max(0, days_forward)))
        rows = cur.fetchall()

    out: List[Tuple[int, int, Optional[str]]] = []
    for row in rows:
        fixture_id = int(row[0])
        referee_id = int(row[1])
        referee_name = row[2] if isinstance(row[2], str) else None
        out.append((fixture_id, referee_id, referee_name))
    return out


def upsert_fixture_referee_stats(
    conn: psycopg2.extensions.connection,
    rows: Iterable[Tuple[int, int, str, Optional[float], Optional[float], Optional[float], Optional[float], int]],
) -> int:
    values: List[Tuple[Any, ...]] = []
    for row in rows:
        fixture_id, referee_id, referee_name, avg_yellow, avg_fouls, avg_corners, red_pct, sample = row
        values.append(
            (
                fixture_id,
                referee_id,
                referee_name,
                avg_yellow,
                avg_fouls,
                None,
                red_pct,
                avg_corners,
                sample,
                "sportmonks",
            )
        )

    if not values:
        return 0

    sql = """
        insert into public.fixture_referee_stats
          (
            fixture_id,
            referee_id,
            referee_name,
            avg_yellow_cards,
            avg_fouls,
            games_with_3plus_cards_pct,
            games_with_red_card_pct,
            avg_corners,
            sample,
            source
          )
        values %s
        on conflict (fixture_id) do update
        set
          referee_id = excluded.referee_id,
          referee_name = excluded.referee_name,
          avg_yellow_cards = excluded.avg_yellow_cards,
          avg_fouls = excluded.avg_fouls,
          games_with_3plus_cards_pct = excluded.games_with_3plus_cards_pct,
          games_with_red_card_pct = excluded.games_with_red_card_pct,
          avg_corners = excluded.avg_corners,
          sample = excluded.sample,
          source = excluded.source,
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

    db_url = get_db_url()
    token = get_api_token()
    base_url = get_base_url()

    conn = psycopg2.connect(db_url)
    try:
        fixture_rows = fetch_fixture_primary_referees(
            conn,
            days_back=args.days_back,
            days_forward=args.days_forward,
            limit_fixtures=args.limit_fixtures,
        )
    finally:
        conn.close()

    unique_referees = sorted({row[1] for row in fixture_rows})
    logger.info("Fixtures with assigned referee=%s unique_referees=%s", len(fixture_rows), len(unique_referees))

    snapshots: Dict[int, RefereeSnapshot] = {}
    fetch_failures = 0

    session = requests.Session()
    try:
        for idx, referee_id in enumerate(unique_referees, start=1):
            try:
                snapshot = fetch_referee_snapshot(
                    session=session,
                    base_url=base_url,
                    token=token,
                    referee_id=referee_id,
                    timeout=args.timeout,
                )
                if snapshot is not None:
                    snapshots[referee_id] = snapshot
            except Exception as exc:  # noqa: BLE001
                fetch_failures += 1
                logger.warning("referee_id=%s fetch failed: %s", referee_id, exc)
            if args.sleep_seconds > 0 and idx < len(unique_referees):
                time.sleep(args.sleep_seconds)
    finally:
        session.close()

    upsert_rows: List[Tuple[int, int, str, Optional[float], Optional[float], Optional[float], Optional[float], int]] = []
    for fixture_id, referee_id, referee_name in fixture_rows:
        snapshot = snapshots.get(referee_id)
        effective_name = (snapshot.name if snapshot and snapshot.name else None) or referee_name or f"Referee {referee_id}"

        red_pct: Optional[float] = None
        if snapshot and snapshot.avg_red_cards is not None:
            red_pct = max(0.0, min(100.0, snapshot.avg_red_cards * 100.0))

        upsert_rows.append(
            (
                fixture_id,
                referee_id,
                effective_name,
                snapshot.avg_yellow_cards if snapshot else None,
                snapshot.avg_fouls if snapshot else None,
                snapshot.avg_corners if snapshot else None,
                red_pct,
                snapshot.sample if snapshot else 0,
            )
        )

    report: Dict[str, Any] = {
        "ok": True,
        "dry_run": bool(args.dry_run),
        "fixtures_with_assigned_referee": len(fixture_rows),
        "unique_referees": len(unique_referees),
        "referee_stats_fetched": len(snapshots),
        "referee_fetch_failures": fetch_failures,
        "rows_to_upsert": len(upsert_rows),
        "sample_fixture_ids": [row[0] for row in fixture_rows[:10]],
        "sample_referee_ids": unique_referees[:10],
    }

    if args.dry_run:
        print(json.dumps(report, indent=2, ensure_ascii=False))
        write_report(args.report_json, report)
        return 0

    conn_write = psycopg2.connect(db_url)
    try:
        upserted = upsert_fixture_referee_stats(conn_write, upsert_rows)
    finally:
        conn_write.close()

    report["upserted"] = upserted
    print(json.dumps(report, indent=2, ensure_ascii=False))
    write_report(args.report_json, report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
