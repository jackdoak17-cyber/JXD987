#!/usr/bin/env python3
"""Bounded, unscheduled refresh candidate for private Prop Search history.

The installed customer reader is untouched. Dry-run is the default. Apply
requires an exact SQL-function hash and explicit WAL ceiling; no backfill is
promoted by this script. The database schema remains a separate release gate.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import time
from contextlib import closing
from pathlib import Path
from typing import Any

import psycopg2


MAX_FIXTURES = 20
MAX_SECONDS = 60
MAX_WAL_BYTES = 8 * 1024 * 1024
MAX_STATS_PER_FIXTURE = 10_000
MAX_APPEARANCES_PER_FIXTURE = 500
LOCK_KEY = (89117, 1)
SAFETY_SECONDS = 5
COMMIT_WAL_RESERVE_BYTES = 8192


class StopBeforeFixture(Exception):
    """A budget or source-shape guard stopped the run before another commit."""


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--apply", action="store_true", help="allow bounded fixture writes")
    result.add_argument("--max-fixtures", type=int, default=5)
    result.add_argument("--max-seconds", type=int, default=30)
    result.add_argument("--max-wal-bytes", type=int, default=0)
    result.add_argument("--max-stats-per-fixture", type=int, default=5000)
    result.add_argument("--max-appearances-per-fixture", type=int, default=150)
    result.add_argument("--expected-refresh-md5", default="")
    result.add_argument("--report-json", type=Path)
    return result


def validate_args(args: argparse.Namespace) -> None:
    if not 1 <= args.max_fixtures <= MAX_FIXTURES:
        raise ValueError(f"--max-fixtures must be 1..{MAX_FIXTURES}")
    if not SAFETY_SECONDS < args.max_seconds <= MAX_SECONDS:
        raise ValueError(f"--max-seconds must be {SAFETY_SECONDS + 1}..{MAX_SECONDS}")
    if not 1 <= args.max_stats_per_fixture <= MAX_STATS_PER_FIXTURE:
        raise ValueError(f"--max-stats-per-fixture must be 1..{MAX_STATS_PER_FIXTURE}")
    if not 1 <= args.max_appearances_per_fixture <= MAX_APPEARANCES_PER_FIXTURE:
        raise ValueError(
            f"--max-appearances-per-fixture must be 1..{MAX_APPEARANCES_PER_FIXTURE}"
        )
    if args.apply and not 1 <= args.max_wal_bytes <= MAX_WAL_BYTES:
        raise ValueError("--apply requires --max-wal-bytes within the hard 8 MiB ceiling")
    if args.apply and not re.fullmatch(r"[0-9a-f]{32}", args.expected_refresh_md5):
        raise ValueError("--apply requires the exact lowercase refresh-function MD5")


def connect():
    url = os.environ.get("SUPABASE_DB_URL_SESSION") or os.environ.get("SUPABASE_DB_URL")
    if not url:
        raise RuntimeError("a Supabase database URL is required")
    return psycopg2.connect(
        url, connect_timeout=5, application_name="oddssearch-prop-history-refresh-candidate"
    )


def acquire_lock(conn) -> bool:
    with conn.cursor() as cur:
        cur.execute("select pg_try_advisory_lock(%s, %s)", LOCK_KEY)
        acquired = bool(cur.fetchone()[0])
    conn.commit()
    return acquired


def release_lock(conn) -> None:
    conn.rollback()
    with conn.cursor() as cur:
        cur.execute("select pg_advisory_unlock(%s, %s)", LOCK_KEY)
    conn.commit()


def preflight(conn, expected_md5: str) -> str:
    with conn.cursor() as cur:
        cur.execute("select set_config('lock_timeout', '1000ms', true)")
        cur.execute("select set_config('statement_timeout', '5000ms', true)")
        cur.execute(
            """select md5(pg_get_functiondef(
                 to_regprocedure('prop_search_history_v1.refresh_fixture(bigint)')
               )),
               to_regclass('prop_search_history_v1.dirty_fixtures') is not null,
               to_regclass('prop_search_history_v1.refreshed_fixtures') is not null,
               to_regclass('prop_search_history_v1.readiness') is not null"""
        )
        row = cur.fetchone()
    conn.commit()
    if not row or not row[0] or not all(row[1:]):
        raise RuntimeError("private Prop Search history schema is not installed")
    actual_md5 = str(row[0])
    if expected_md5 and actual_md5 != expected_md5:
        raise RuntimeError("refresh function does not match the expected SQL revision")
    return actual_md5


def pending_counts(conn) -> tuple[int, int]:
    with conn.cursor() as cur:
        cur.execute("select set_config('lock_timeout', '1000ms', true)")
        cur.execute("select set_config('statement_timeout', '5000ms', true)")
        cur.execute("select count(*) from prop_search_history_v1.dirty_fixtures")
        dirty = int(cur.fetchone()[0])
        cur.execute(
            """select count(*) from public.fixtures f
               where not exists (
                 select 1 from prop_search_history_v1.refreshed_fixtures r
                 where r.fixture_id = f.id
               )"""
        )
        missing = int(cur.fetchone()[0])
    conn.rollback()
    return dirty, missing


def next_fixture(cur, excluded: list[int]) -> tuple[int, str] | None:
    cur.execute(
        """select fixture_id from prop_search_history_v1.dirty_fixtures
           where fixture_id <> all(%s::bigint[])
           order by last_dirty_at, fixture_id
           for update skip locked limit 1""",
        (excluded,),
    )
    row = cur.fetchone()
    if row:
        return int(row[0]), "dirty"
    cur.execute(
        """select f.id from public.fixtures f
           where f.id <> all(%s::bigint[])
             and not exists (
               select 1 from prop_search_history_v1.refreshed_fixtures r
               where r.fixture_id = f.id
             )
           order by f.id limit 1""",
        (excluded,),
    )
    row = cur.fetchone()
    return (int(row[0]), "backfill") if row else None


def refresh_one(
    conn, excluded: list[int], remaining_seconds: float,
    remaining_wal_bytes: int, max_stats: int, max_appearances: int,
) -> tuple[int, int, int, str] | None:
    """Commit one fixture or roll it back before either hard budget is crossed."""
    with conn:
        with conn.cursor() as cur:
            cur.execute("select set_config('lock_timeout', '1000ms', true)")
            cur.execute(
                "select set_config('statement_timeout', %s, true)",
                (f"{min(5000, max(1, int((remaining_seconds - SAFETY_SECONDS) * 1000)))}ms",),
            )
            selected = next_fixture(cur, excluded)
            if selected is None:
                return None
            fixture_id, source = selected
            cur.execute(
                "select count(*) from public.fixture_player_statistics where fixture_id = %s",
                (fixture_id,),
            )
            stats_count = int(cur.fetchone()[0])
            if stats_count > max_stats:
                raise StopBeforeFixture(f"fixture {fixture_id} exceeds statistic-row ceiling")
            cur.execute(
                "select count(*) from public.fixture_players where fixture_id = %s",
                (fixture_id,),
            )
            appearance_count = int(cur.fetchone()[0])
            if appearance_count > max_appearances:
                raise StopBeforeFixture(f"fixture {fixture_id} exceeds appearance-row ceiling")
            cur.execute("select pg_current_wal_insert_lsn()")
            start_lsn = str(cur.fetchone()[0])
            cur.execute(
                "select prop_search_history_v1.refresh_fixture(%s)", (fixture_id,)
            )
            appearances = int(cur.fetchone()[0])
            cur.execute(
                "select pg_wal_lsn_diff(pg_current_wal_insert_lsn(), %s::pg_lsn)::bigint",
                (start_lsn,),
            )
            wal_bytes = int(cur.fetchone()[0])
            if wal_bytes + COMMIT_WAL_RESERVE_BYTES > remaining_wal_bytes:
                raise StopBeforeFixture("WAL ceiling would be crossed; fixture rolled back")
    return fixture_id, appearances, wal_bytes + COMMIT_WAL_RESERVE_BYTES, source


def run(args: argparse.Namespace, conn) -> dict[str, Any]:
    validate_args(args)
    started = time.monotonic()
    report: dict[str, Any] = {
        "mode": "apply" if args.apply else "dry_run",
        "processed_fixtures": 0,
        "appearance_rows": 0,
        "accounted_wal_bytes": 0,
        "status": "unknown",
    }
    locked = False
    try:
        locked = acquire_lock(conn)
        if not locked:
            report["status"] = "busy"
            return report
        report["refresh_md5"] = preflight(
            conn, args.expected_refresh_md5 if args.apply else ""
        )
        dirty, missing = pending_counts(conn)
        report["dirty_before"] = dirty
        report["unrefreshed_before"] = missing
        if not args.apply:
            report["status"] = "dry_run"
            return report
        excluded: list[int] = []
        while len(excluded) < args.max_fixtures:
            remaining = args.max_seconds - (time.monotonic() - started)
            if remaining <= SAFETY_SECONDS:
                report["status"] = "time_ceiling"
                break
            remaining_wal = args.max_wal_bytes - int(report["accounted_wal_bytes"])
            if remaining_wal <= COMMIT_WAL_RESERVE_BYTES:
                report["status"] = "wal_ceiling"
                break
            try:
                outcome = refresh_one(
                    conn, excluded, remaining, remaining_wal,
                    args.max_stats_per_fixture, args.max_appearances_per_fixture,
                )
            except StopBeforeFixture as exc:
                report["status"] = "guard_stopped"
                report["guard_reason"] = str(exc)
                break
            if outcome is None:
                report["status"] = "queue_empty"
                break
            fixture_id, appearances, wal_bytes, source = outcome
            excluded.append(fixture_id)
            report["processed_fixtures"] += 1
            report["appearance_rows"] += appearances
            report["accounted_wal_bytes"] += wal_bytes
            report.setdefault("source_kinds", {}).setdefault(source, 0)
            report["source_kinds"][source] += 1
        else:
            report["status"] = "fixture_ceiling"
        return report
    finally:
        if locked:
            release_lock(conn)
        report["elapsed_seconds"] = round(time.monotonic() - started, 3)


def main() -> int:
    args = parser().parse_args()
    try:
        validate_args(args)
        with closing(connect()) as conn:
            report = run(args, conn)
        if args.report_json:
            temp_report = args.report_json.with_name(
                f"{args.report_json.name}.tmp.{os.getpid()}"
            )
            try:
                temp_report.write_text(json.dumps(report, sort_keys=True) + "\n")
                os.replace(temp_report, args.report_json)
            finally:
                temp_report.unlink(missing_ok=True)
        print(json.dumps(report, sort_keys=True))
        return 0 if report["status"] not in {"busy", "guard_stopped"} else 2
    except Exception as exc:
        print(json.dumps({"status": "failed", "error_type": type(exc).__name__}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
