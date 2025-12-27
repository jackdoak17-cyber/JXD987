#!/usr/bin/env python3
"""
Export odds tables to Supabase (upcoming fixtures only).
"""

import argparse
import json
import os
import sqlite3
import time
from datetime import datetime, timedelta
from typing import Dict, List, Sequence

import requests

DB_PATH = os.environ.get("JXD_DB_PATH", "data/jxd.sqlite")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
REST_PATH = "/rest/v1"


def require_env() -> None:
    missing = []
    if not SUPABASE_URL:
        missing.append("SUPABASE_URL")
    if not SUPABASE_KEY:
        missing.append("SUPABASE_SERVICE_ROLE_KEY")
    if missing:
        raise SystemExit(f"Missing env vars: {', '.join(missing)}")


def get_conn() -> sqlite3.Connection:
    if not os.path.exists(DB_PATH):
        raise SystemExit(f"SQLite DB not found at {DB_PATH}")
    return sqlite3.connect(DB_PATH)


def rest_headers() -> Dict[str, str]:
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal,resolution=merge-duplicates",
    }


def upsert_table(
    table: str,
    rows: List[Dict],
    on_conflict: str,
    chunk_size: int,
    timeout: int,
    retries: int,
    sleep_seconds: float,
) -> int:
    if not rows:
        return 0
    url = SUPABASE_URL.rstrip("/") + REST_PATH + f"/{table}"
    total = 0
    chunk = max(1, chunk_size)
    headers = rest_headers()
    for i in range(0, len(rows), chunk):
        batch = rows[i : i + chunk]
        attempt = 0
        while True:
            try:
                resp = requests.post(
                    url,
                    headers=headers,
                    params={"on_conflict": on_conflict},
                    data=json.dumps(batch),
                    timeout=timeout,
                )
                if not resp.ok:
                    raise SystemExit(
                        f"Supabase upsert to {table} failed {resp.status_code}: {resp.text}"
                    )
                break
            except requests.RequestException as exc:
                attempt += 1
                if attempt > retries:
                    raise SystemExit(f"Supabase upsert to {table} failed after retries: {exc}")
                sleep_for = 2**attempt
                time.sleep(sleep_for)
        total += len(batch)
        if sleep_seconds:
            time.sleep(sleep_seconds)
    return total


def fetch_fixture_ids(conn: sqlite3.Connection, league_ids: Sequence[int], days_forward: int) -> List[int]:
    cur = conn.cursor()
    today = datetime.utcnow().date()
    end = today + timedelta(days=days_forward)
    today_iso = today.isoformat()
    end_iso = end.isoformat()
    league_clause = ""
    params: List[object] = [today_iso, end_iso]
    if league_ids:
        placeholders = ",".join("?" for _ in league_ids)
        league_clause = f"and league_id in ({placeholders})"
        params.extend(league_ids)
    cur.execute(
        f"""
        select id
        from fixtures
        where date(starting_at) >= ? and date(starting_at) <= ?
        {league_clause}
        """,
        params,
    )
    return [row[0] for row in cur.fetchall()]


def fetch_odds_snapshots(conn: sqlite3.Connection, fixture_ids: Sequence[int]) -> List[Dict]:
    if not fixture_ids:
        return []
    cur = conn.cursor()
    q = ",".join("?" for _ in fixture_ids)
    cur.execute(
        f"""
        select id, fixture_id, bookmaker_id, pulled_at, raw
        from odds_snapshots
        where fixture_id in ({q})
        """,
        fixture_ids,
    )
    def parse_raw(raw_value):
        if raw_value is None:
            return None
        if isinstance(raw_value, (dict, list)):
            return raw_value
        try:
            return json.loads(raw_value)
        except Exception:
            return raw_value

    return [
        {
            "id": r[0],
            "fixture_id": r[1],
            "bookmaker_id": r[2],
            "pulled_at": r[3],
            "raw": parse_raw(r[4]),
        }
        for r in cur.fetchall()
    ]


def fetch_odds_outcomes(conn: sqlite3.Connection, fixture_ids: Sequence[int]) -> List[Dict]:
    if not fixture_ids:
        return []
    cur = conn.cursor()
    q = ",".join("?" for _ in fixture_ids)
    cur.execute(
        f"""
        select fixture_id, bookmaker_id, market_key, selection_key,
               participant_type, participant_id, line,
               price_decimal, price_american, last_updated_at
        from odds_outcomes
        where fixture_id in ({q})
        """,
        fixture_ids,
    )
    return [
        {
            "fixture_id": r[0],
            "bookmaker_id": r[1],
            "market_key": r[2],
            "selection_key": r[3],
            "participant_type": r[4],
            "participant_id": r[5],
            "line": r[6],
            "price_decimal": r[7],
            "price_american": r[8],
            "last_updated_at": r[9],
        }
        for r in cur.fetchall()
    ]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--leagues", default="8", help="Comma-separated league IDs")
    parser.add_argument("--days-forward", type=int, default=14)
    parser.add_argument("--skip-snapshots", action="store_true")
    parser.add_argument("--chunk-size", type=int, default=500)
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--sleep", type=float, default=0.2)
    args = parser.parse_args()

    require_env()
    league_ids = [int(x) for x in args.leagues.split(",") if x.strip()]
    conn = get_conn()

    fixture_ids = fetch_fixture_ids(conn, league_ids, args.days_forward)
    snapshots = [] if args.skip_snapshots else fetch_odds_snapshots(conn, fixture_ids)
    outcomes = fetch_odds_outcomes(conn, fixture_ids)

    exported = {
        "odds_snapshots": upsert_table(
            "odds_snapshots",
            snapshots,
            "id",
            args.chunk_size,
            args.timeout,
            args.retries,
            args.sleep,
        ),
        "odds_outcomes": upsert_table(
            "odds_outcomes",
            outcomes,
            "fixture_id,bookmaker_id,market_key,selection_key,line",
            args.chunk_size,
            args.timeout,
            args.retries,
            args.sleep,
        ),
    }

    summary = {
        "fixtures": len(fixture_ids),
        "odds_snapshots_exported": exported["odds_snapshots"],
        "odds_outcomes_exported": exported["odds_outcomes"],
    }
    print(json.dumps(summary))


if __name__ == "__main__":
    main()
