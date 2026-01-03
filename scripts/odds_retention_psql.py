#!/usr/bin/env python3
"""
Run odds retention cleanup in Supabase and report counts.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import urllib.parse
from datetime import datetime
from pathlib import Path
from typing import Optional

DB_URL = os.environ.get("SUPABASE_DB_URL") or os.environ.get("SUPABASE_DB_URL_SESSION")


def redact_db_url(db_url: str) -> str:
    if not db_url:
        return db_url
    parsed = urllib.parse.urlparse(db_url)
    if parsed.password:
        netloc = f"{parsed.username}:***@{parsed.hostname}"
        if parsed.port:
            netloc += f":{parsed.port}"
        return parsed._replace(netloc=netloc).geturl()
    return db_url


def run_psql(sql: str, label: str) -> str:
    if not DB_URL:
        raise SystemExit("Missing SUPABASE_DB_URL")
    cmd = [
        "psql",
        DB_URL,
        "-v",
        "ON_ERROR_STOP=1",
        "--echo-errors",
        "-v",
        "VERBOSITY=verbose",
        "-v",
        "SHOW_CONTEXT=always",
        "-At",
        "-F",
        "\t",
        "-c",
        sql,
    ]
    env = dict(os.environ)
    env.setdefault("PGCONNECT_TIMEOUT", "15")
    try:
        proc = subprocess.run(cmd, check=True, capture_output=True, text=True, env=env)
    except subprocess.CalledProcessError as exc:
        safe_cmd = [redact_db_url(c) if c == DB_URL else c for c in cmd]
        print(f"psql failed ({label}) cmd: {' '.join(safe_cmd)}")
        print("stdout:\n", exc.stdout or "")
        print("stderr:\n", exc.stderr or "")
        raise
    return (proc.stdout or "").strip()


def retention_cleanup_query(days_back: int, days_forward: int) -> str:
    return f"""
with deleted as (
  delete from public.odds_outcomes o
  using public.fixtures f
  where f.id = o.fixture_id
    and (
      f.starting_at < (now() at time zone 'utc') - interval '{days_back} days'
      or f.starting_at >= (now() at time zone 'utc') + interval '{days_forward} days'
    )
  returning 1
)
select count(*)::bigint from deleted;
"""


def retention_snapshots_query(days_back: int, days_forward: int, max_age_days: int) -> str:
    return f"""
with deleted as (
  delete from public.odds_snapshots s
  where s.pulled_at < (now() at time zone 'utc') - interval '{max_age_days} days'
     or exists (
       select 1
       from public.fixtures f
       where f.id = s.fixture_id
         and (
           f.starting_at < (now() at time zone 'utc') - interval '{days_back} days'
           or f.starting_at >= (now() at time zone 'utc') + interval '{days_forward} days'
         )
     )
  returning 1
)
select count(*)::bigint from deleted;
"""


def past_24h_rows_query() -> str:
    return """
select count(*)::bigint
from public.odds_outcomes o
join public.fixtures f on f.id = o.fixture_id
where f.starting_at < (now() at time zone 'utc') - interval '24 hours';
"""


def past_24h_market_breakdown_query() -> str:
    return """
select o.market_key, count(*)::bigint as rows
from public.odds_outcomes o
join public.fixtures f on f.id = o.fixture_id
where f.starting_at < (now() at time zone 'utc') - interval '24 hours'
group by o.market_key
order by rows desc
limit 10;
"""


def past_24h_league_breakdown_query() -> str:
    return """
select f.league_id, count(*)::bigint as rows
from public.odds_outcomes o
join public.fixtures f on f.id = o.fixture_id
where f.starting_at < (now() at time zone 'utc') - interval '24 hours'
group by f.league_id
order by rows desc
limit 10;
"""


def past_24h_range_query() -> str:
    return """
select min(f.starting_at), max(f.starting_at)
from public.odds_outcomes o
join public.fixtures f on f.id = o.fixture_id
where f.starting_at < (now() at time zone 'utc') - interval '24 hours';
"""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days-back", type=int, default=1)
    parser.add_argument("--days-forward", type=int, default=14)
    parser.add_argument("--snapshot-days", type=int, default=30)
    parser.add_argument("--report-out", default="/tmp/odds_retention_report.json")
    args = parser.parse_args()

    start_iso = datetime.utcnow().isoformat() + "Z"
    run_psql("set statement_timeout = 0;", label="set_timeout")
    run_psql("set lock_timeout = 0;", label="set_lock_timeout")
    run_psql("set idle_in_transaction_session_timeout = 0;", label="set_idle_timeout")

    deleted_rows = 0
    snapshots_deleted = 0
    past_24h_rows = 0
    past_24h_market_top: list[dict[str, object]] = []
    past_24h_league_top: list[dict[str, object]] = []
    past_24h_range: dict[str, Optional[str]] = {"min_starting_at": None, "max_starting_at": None}

    try:
        deleted_out = run_psql(retention_cleanup_query(args.days_back, args.days_forward), label="retention")
        deleted_rows = int(deleted_out.strip()) if deleted_out else 0
    except Exception:
        deleted_rows = 0

    try:
        snapshots_out = run_psql(
            retention_snapshots_query(args.days_back, args.days_forward, args.snapshot_days),
            label="retention_snapshots",
        )
        snapshots_deleted = int(snapshots_out.strip()) if snapshots_out else 0
    except Exception:
        snapshots_deleted = 0

    try:
        past_out = run_psql(past_24h_rows_query(), label="past_24h")
        past_24h_rows = int(past_out.strip()) if past_out else 0
    except Exception:
        past_24h_rows = 0

    if past_24h_rows > 0:
        try:
            market_out = run_psql(past_24h_market_breakdown_query(), label="past_24h_markets")
            for line in market_out.splitlines():
                parts = line.split("\t")
                if len(parts) >= 2:
                    past_24h_market_top.append({"market_key": parts[0], "rows": int(parts[1])})
        except Exception:
            pass
        try:
            league_out = run_psql(past_24h_league_breakdown_query(), label="past_24h_leagues")
            for line in league_out.splitlines():
                parts = line.split("\t")
                if len(parts) >= 2:
                    past_24h_league_top.append({"league_id": parts[0], "rows": int(parts[1])})
        except Exception:
            pass
        try:
            range_out = run_psql(past_24h_range_query(), label="past_24h_range")
            parts = range_out.split("\t")
            if len(parts) >= 2:
                past_24h_range["min_starting_at"] = parts[0] or None
                past_24h_range["max_starting_at"] = parts[1] or None
        except Exception:
            pass

    report = {
        "start_time": start_iso,
        "end_time": datetime.utcnow().isoformat() + "Z",
        "days_back": args.days_back,
        "days_forward": args.days_forward,
        "snapshot_days": args.snapshot_days,
        "deleted_rows": deleted_rows,
        "snapshots_deleted": snapshots_deleted,
        "odds_rows_past_24h": past_24h_rows,
        "past_24h_market_top": past_24h_market_top,
        "past_24h_league_top": past_24h_league_top,
        "past_24h_range": past_24h_range,
    }
    Path(args.report_out).write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
