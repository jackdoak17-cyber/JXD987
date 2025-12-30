#!/usr/bin/env python3
"""
Patch odds participant mappings in Supabase using REST (no psql).

Reads mapped odds_outcomes rows from SQLite and upserts them into Supabase.
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


def get_conn(db_path: str) -> sqlite3.Connection:
    if not os.path.exists(db_path):
        raise SystemExit(f"SQLite DB not found at {db_path}")
    return sqlite3.connect(db_path)


def rest_headers() -> Dict[str, str]:
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal,resolution=merge-duplicates",
    }


def is_html_error(text: str) -> bool:
    if not text:
        return False
    snippet = text.lstrip()[:200].lower()
    return snippet.startswith("<html") or snippet.startswith("<!doctype html") or "cloudflare" in snippet


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
                if resp.ok and not is_html_error(resp.text or ""):
                    break
                if resp.status_code in (502, 503, 504, 522, 524, 525) or is_html_error(resp.text or ""):
                    attempt += 1
                    if attempt > retries:
                        raise SystemExit(
                            f"Supabase upsert to {table} failed {resp.status_code}: {resp.text}"
                        )
                    time.sleep(min(60, 2**attempt))
                    continue
                raise SystemExit(
                    f"Supabase upsert to {table} failed {resp.status_code}: {resp.text}"
                )
            except requests.RequestException as exc:
                attempt += 1
                if attempt > retries:
                    raise SystemExit(f"Supabase upsert to {table} failed after retries: {exc}")
                time.sleep(min(60, 2**attempt))
        total += len(batch)
        if sleep_seconds:
            time.sleep(sleep_seconds)
    return total


def parse_league_ids(raw: str) -> List[int]:
    return [int(x) for x in raw.split(",") if x.strip()]


def fetch_mapped_outcomes(
    conn: sqlite3.Connection,
    league_ids: Sequence[int],
    days_forward: int,
) -> List[Dict]:
    today = datetime.utcnow().date()
    end = today + timedelta(days=days_forward)
    today_iso = today.isoformat()
    end_iso = end.isoformat()
    params: List[object] = [today_iso, end_iso]
    league_clause = ""
    if league_ids:
        placeholders = ",".join("?" for _ in league_ids)
        league_clause = f"and f.league_id in ({placeholders})"
        params.extend(league_ids)

    cur = conn.cursor()
    cur.execute(
        f"""
        select o.fixture_id,
               o.bookmaker_id,
               o.market_key,
               o.selection_key,
               o.line,
               o.participant_type,
               o.participant_id,
               o.price_decimal,
               o.price_american,
               o.last_updated_at
        from odds_outcomes o
        join fixtures f on f.id = o.fixture_id
        where o.participant_id is not null
          and date(f.starting_at) >= ? and date(f.starting_at) <= ?
          {league_clause}
        """,
        params,
    )
    rows = cur.fetchall()
    return [
        {
            "fixture_id": r[0],
            "bookmaker_id": r[1],
            "market_key": r[2],
            "selection_key": r[3],
            "line": r[4],
            "participant_type": r[5],
            "participant_id": r[6],
            "price_decimal": r[7],
            "price_american": r[8],
            "last_updated_at": r[9],
        }
        for r in rows
    ]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--leagues", default="8,384", help="Comma-separated league IDs")
    parser.add_argument("--days-forward", type=int, default=14)
    parser.add_argument("--db", default=DB_PATH)
    parser.add_argument("--chunk-size", type=int, default=300)
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--retries", type=int, default=5)
    parser.add_argument("--sleep", type=float, default=0.2)
    args = parser.parse_args()

    require_env()
    conn = get_conn(args.db)
    league_ids = parse_league_ids(args.leagues)
    outcomes = fetch_mapped_outcomes(conn, league_ids, args.days_forward)
    conn.close()

    exported = upsert_table(
        "odds_outcomes",
        outcomes,
        "fixture_id,bookmaker_id,market_key,selection_key,line",
        args.chunk_size,
        args.timeout,
        args.retries,
        args.sleep,
    )
    print(json.dumps({"rows": len(outcomes), "patched": exported}))


if __name__ == "__main__":
    main()
