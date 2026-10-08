#!/usr/bin/env python3
"""Refresh card odds inside every currently readable fixture release.

The canonical odds writer commits before this process starts. This process
shares the fixture publisher advisory lock, finds release/fixture pairs whose
eligible card odds differ, and replaces only those small projections in one
transaction.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path
from typing import Any

import psycopg2

try:
    from .refresh_fixture_delivery import ACTIVE_BOOKMAKERS, EXCLUDED_CUPS, MONEYLINE_MARKETS
except ImportError:  # Direct script execution on the VPS.
    from refresh_fixture_delivery import ACTIVE_BOOKMAKERS, EXCLUDED_CUPS, MONEYLINE_MARKETS


LOCK_NAME = "fixture_delivery_refresh"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report-out", default="/tmp/fixture_delivery_odds_sync_report.json")
    parser.add_argument("--delivery-report", default="/tmp/odds_delivery_report.json")
    parser.add_argument("--database-url", default=None, help=argparse.SUPPRESS)
    return parser


def database_url(explicit: str | None = None) -> str:
    url = explicit or (
        os.environ.get("SUPABASE_DB_URL_SESSION")
        or os.environ.get("SUPABASE_DB_URL_POOLER")
        or os.environ.get("SUPABASE_DB_URL")
    )
    if not url:
        raise SystemExit("SUPABASE_DB_URL_SESSION, SUPABASE_DB_URL_POOLER, or SUPABASE_DB_URL is required")
    return url


def write_json(path: str | Path, payload: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    temporary.replace(target)


def add_to_delivery_report(path: str | Path, result: dict[str, Any]) -> None:
    target = Path(path)
    if not target.exists():
        return
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return
    if not isinstance(payload, dict):
        return
    payload["fixture_delivery_odds_sync"] = result
    write_json(target, payload)


def scalar(cur, query: str, params: tuple[Any, ...] | None = None) -> int:
    cur.execute(query, params)
    return int(cur.fetchone()[0])


def reconcile(conn) -> dict[str, Any]:
    started = time.monotonic()
    result: dict[str, Any] = {
        "status": "started",
        "release_count": 0,
        "source_rows": 0,
        "existing_rows": 0,
        "affected_release_fixture_pairs": 0,
        "affected_fixtures": 0,
        "deleted_rows": 0,
        "inserted_rows": 0,
        "release_metadata_updated": 0,
    }
    conn.autocommit = False
    with conn.cursor() as cur:
        cur.execute("set local lock_timeout = '3s'")
        cur.execute("set local statement_timeout = '60s'")
        cur.execute("select pg_try_advisory_xact_lock(hashtextextended(%s, 0))", (LOCK_NAME,))
        if not bool(cur.fetchone()[0]):
            conn.rollback()
            result["status"] = "deferred_lock_busy"
            result["runtime_seconds"] = round(time.monotonic() - started, 3)
            return result

        cur.execute(
            """
            create temp table readable_fixture_releases on commit drop as
            select r.id as release_id
              from public.fixture_delivery_releases r
              cross join public.fixture_delivery_current_publication p
             where p.publication_key = 'fixtures'
               and r.status = 'published'
               and (r.id = p.release_id or r.pin_expires_at > now())
            """
        )
        result["release_count"] = scalar(cur, "select count(*) from readable_fixture_releases")

        cur.execute(
            """
            create temp table desired_fixture_delivery_odds on commit drop as
            select rr.release_id, o.fixture_id, o.bookmaker_id, o.market_key, o.selection_key,
                   coalesce(o.participant_type, 'team') as participant_type,
                   coalesce(o.participant_id, 0) as participant_id,
                   o.line, coalesce(o.line, -9999::numeric) as line_key,
                   o.price_decimal, o.price_american,
                   o.last_updated_at as source_last_updated_at
              from readable_fixture_releases rr
              join public.fixture_delivery_schedule s on s.release_id = rr.release_id
              join public.odds_outcomes o on o.fixture_id = s.fixture_id
             where o.bookmaker_id = any(%s)
               and lower(o.market_key) = any(%s)
               and o.price_decimal > 1 and o.price_decimal <= 500
               and s.league_id <> all(%s)
            """,
            (sorted(ACTIVE_BOOKMAKERS), sorted(MONEYLINE_MARKETS), sorted(EXCLUDED_CUPS)),
        )
        cur.execute(
            """
            create unique index desired_fixture_delivery_odds_key
              on desired_fixture_delivery_odds
                (release_id, fixture_id, bookmaker_id, market_key, selection_key,
                 participant_type, participant_id, line_key)
            """
        )
        result["source_rows"] = scalar(cur, "select count(*) from desired_fixture_delivery_odds")
        result["existing_rows"] = scalar(
            cur,
            """
            select count(*)
              from public.fixture_delivery_odds o
              join readable_fixture_releases rr on rr.release_id = o.release_id
            """,
        )

        cur.execute(
            """
            create temp table mismatched_release_fixtures on commit drop as
            select distinct coalesce(d.release_id, e.release_id) as release_id,
                            coalesce(d.fixture_id, e.fixture_id) as fixture_id
              from desired_fixture_delivery_odds d
              full join (
                select o.*
                  from public.fixture_delivery_odds o
                  join readable_fixture_releases rr on rr.release_id = o.release_id
              ) e
                on e.release_id = d.release_id
               and e.fixture_id = d.fixture_id
               and e.bookmaker_id = d.bookmaker_id
               and e.market_key = d.market_key
               and e.selection_key = d.selection_key
               and e.participant_type = d.participant_type
               and e.participant_id = d.participant_id
               and e.line_key = d.line_key
             where d.release_id is null
                or e.release_id is null
                or e.line is distinct from d.line
                or e.price_decimal is distinct from d.price_decimal
                or e.price_american is distinct from d.price_american
                or e.source_last_updated_at is distinct from d.source_last_updated_at
            """
        )
        result["affected_release_fixture_pairs"] = scalar(
            cur, "select count(*) from mismatched_release_fixtures"
        )
        result["affected_fixtures"] = scalar(
            cur, "select count(distinct fixture_id) from mismatched_release_fixtures"
        )

        if not result["affected_release_fixture_pairs"]:
            conn.commit()
            result["status"] = "noop"
            result["runtime_seconds"] = round(time.monotonic() - started, 3)
            return result

        cur.execute(
            """
            delete from public.fixture_delivery_odds o
             using mismatched_release_fixtures m
             where o.release_id = m.release_id and o.fixture_id = m.fixture_id
            """
        )
        result["deleted_rows"] = max(cur.rowcount, 0)

        cur.execute(
            """
            insert into public.fixture_delivery_odds
              (release_id, fixture_id, bookmaker_id, market_key, selection_key, participant_type,
               participant_id, line, line_key, price_decimal, price_american,
               source_last_updated_at, observed_at)
            select d.release_id, d.fixture_id, d.bookmaker_id, d.market_key, d.selection_key,
                   d.participant_type, d.participant_id, d.line, d.line_key,
                   d.price_decimal, d.price_american, d.source_last_updated_at,
                   transaction_timestamp()
              from desired_fixture_delivery_odds d
              join mismatched_release_fixtures m
                on m.release_id = d.release_id and m.fixture_id = d.fixture_id
            """
        )
        result["inserted_rows"] = max(cur.rowcount, 0)
        cur.execute(
            """
            update public.fixture_delivery_releases r
               set odds_rows = totals.odds_rows
              from (
                select m.release_id, count(o.*)::integer as odds_rows
                  from (select distinct release_id from mismatched_release_fixtures) m
                  left join public.fixture_delivery_odds o on o.release_id = m.release_id
                 group by m.release_id
              ) totals
             where r.id = totals.release_id
               and r.odds_rows is distinct from totals.odds_rows
            """
        )
        result["release_metadata_updated"] = max(cur.rowcount, 0)
        conn.commit()
        result["status"] = "succeeded"
        result["runtime_seconds"] = round(time.monotonic() - started, 3)
        return result


def main() -> int:
    args = build_parser().parse_args()
    result: dict[str, Any]
    conn = None
    try:
        conn = psycopg2.connect(database_url(args.database_url), connect_timeout=20)
        result = reconcile(conn)
    except Exception as error:
        if conn is not None:
            conn.rollback()
        result = {"status": "failed", "error": f"{type(error).__name__}: {error}"}
        write_json(args.report_out, result)
        add_to_delivery_report(args.delivery_report, result)
        raise
    finally:
        if conn is not None:
            conn.close()

    write_json(args.report_out, result)
    add_to_delivery_report(args.delivery_report, result)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
