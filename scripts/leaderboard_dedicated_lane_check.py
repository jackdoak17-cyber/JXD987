#!/usr/bin/env python3
"""Read-only database admission and post-run checks for the dedicated lane."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path
import re
import stat
import sys

import psycopg2
from psycopg2.extras import RealDictCursor


P95 = "source_ingestion_processing_p95_regressed_gt_10pct"
MIB = 1024 * 1024
MAX_RESOURCE_EVIDENCE_AGE_SECONDS = 4 * 60
MAX_RUN_DURATION_SECONDS = 125


def safe(value):
    if isinstance(value, dict):
        return {key: safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [safe(item) for item in value]
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    return value


def validate_resource_evidence(
    path: Path, now: dt.datetime | None = None,
) -> dict:
    """Reject stale disk evidence before the wrapper creates its quarantine."""
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, encoding="utf-8") as source:
        info = os.fstat(source.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_mode & 0o077:
            raise RuntimeError("disk resource evidence file is untrusted")
        evidence = json.load(source)
    if set(evidence) != {"disk_available_bytes", "observed_at"}:
        raise RuntimeError("disk resource evidence has unexpected or missing fields")
    if type(evidence["disk_available_bytes"]) is not int or evidence["disk_available_bytes"] <= 0:
        raise RuntimeError("disk resource evidence has an invalid capacity")
    observed = dt.datetime.fromisoformat(str(evidence["observed_at"]).replace("Z", "+00:00"))
    if observed.tzinfo is None:
        raise RuntimeError("disk resource evidence timestamp has no timezone")
    age = ((now or dt.datetime.now(dt.timezone.utc)) - observed).total_seconds()
    if not 0 <= age <= MAX_RESOURCE_EVIDENCE_AGE_SECONDS:
        raise RuntimeError("disk resource evidence is not fresh enough to start the lane")
    return evidence


def database_snapshot(connection) -> dict:
    with connection.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute("select public.player_leaderboard_shadow_status_v2() payload")
        status = cursor.fetchone()["payload"]
        cursor.execute("""
            select current_database() database_name,current_user database_user,
                   inet_server_addr() is not null network_connection,
                   current_setting('max_connections')::int max_connections,
                   (select count(*) from pg_stat_activity) total_connections,
                   (select count(*) from pg_stat_activity where state='active') active_connections,
                   (select count(*) from pg_stat_activity where state like 'idle in transaction%') idle_in_transaction,
                   (select count(*) from pg_stat_activity where wait_event_type='Lock') lock_waiters,
                   (select count(*) from leaderboard_shadow_v1.worker_runs where finished_at is null) open_runs,
                   (select count(*) from leaderboard_shadow_v1.worker_runs
                    where resource_start_wal_lsn is not null and resource_window_finished_at is null) open_windows,
                   (select count(*) from leaderboard_trial_v1.reservations where settled_at is null) open_trial_reservations,
                   (select count(*) from leaderboard_rolling_v1.reservations where settled_at is null) open_rolling_reservations
        """)
        database = dict(cursor.fetchone())
        cursor.execute("""
            select latest_status,latest_release_id,release_id,latest_work_units,work_units,
                   sample_count,latest_observed_at,processing_p95_ms
            from operations.pipeline_stats_reconciliation_performance_v1
        """)
        source = dict(cursor.fetchone())
    return {"status": status, "database": database, "source": source}


def validate_before(
    snapshot: dict, release_sha: str, previous_release_sha: str | None = None,
) -> dict:
    status = snapshot["status"]
    database = snapshot["database"]
    source = snapshot["source"]
    now = dt.datetime.now(dt.timezone.utc)
    source_age = (now - source["latest_observed_at"]).total_seconds()
    guard = status.get("resource_guard", {})
    breaches = guard.get("breaches")
    green = (
        guard.get("ok") is True
        and breaches == []
        and status.get("resource_paused") is False
        and status.get("resource_pause_reason") is None
    )
    p95_yellow = (
        guard.get("ok") is False
        and breaches == [P95]
        and status.get("resource_paused") is True
        and status.get("resource_pause_reason") == P95
    )
    if status.get("release_sha") not in {release_sha, previous_release_sha}:
        raise RuntimeError("worker release differs from the dedicated-lane pin")
    if not (
        status.get("worker_enabled") is True
        and status.get("capture_enabled") is True
        and status.get("backfill_enabled") is False
        and (green or p95_yellow)
    ):
        raise RuntimeError("pipeline state is outside the green-or-p95 lane envelope")
    if source.get("latest_status") != "success" or source.get("latest_release_id") != source.get("release_id"):
        raise RuntimeError("latest source cycle is not a successful matching release")
    if source.get("sample_count", 0) < 20 or source.get("latest_work_units", 0) < 1:
        raise RuntimeError("source evidence is undersampled")
    if not 0 <= source_age <= 15 * 60:
        raise RuntimeError("latest source cycle is stale")
    if (database["database_name"], database["database_user"], database["network_connection"]) != (
        "postgres", "postgres", True,
    ):
        raise RuntimeError("database identity changed")
    if database["total_connections"] > int(database["max_connections"] * .75):
        raise RuntimeError("database connection admission exceeded 75 percent")
    if database["active_connections"] > 10:
        raise RuntimeError("active database sessions exceed the lane admission limit")
    for field in ("idle_in_transaction", "lock_waiters", "open_runs", "open_windows",
                  "open_trial_reservations", "open_rolling_reservations"):
        if database[field] != 0:
            raise RuntimeError(f"database admission found nonzero {field}")
    return {
        "ok": True,
        "mode": "before",
        "resource_envelope": "green" if green else "source_p95_yellow",
        "source_age_seconds": round(source_age, 3),
        **snapshot,
    }


def validate_after(snapshot: dict, connection, release_sha: str, run_id: int) -> dict:
    with connection.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute("""
            select run_id,release_sha,started_at,finished_at,claimed_count,succeeded_count,
                   failed_count,wal_bytes,resource_temp_bytes,resource_accounting_mode,
                   resource_window_finished_at,failure_summary
            from leaderboard_shadow_v1.worker_runs where run_id=%s
        """, (run_id,))
        run = cursor.fetchone()
    if not run or run["release_sha"] != release_sha:
        raise RuntimeError("dedicated-lane run identity changed")
    if run["finished_at"] is None or run["resource_window_finished_at"] is None:
        raise RuntimeError("dedicated-lane run or resource window is open")
    duration = (run["finished_at"] - run["started_at"]).total_seconds()
    if duration > MAX_RUN_DURATION_SECONDS:
        raise RuntimeError("dedicated-lane run exceeded its bounded duration")
    if run["failed_count"] != 0 or run["failure_summary"] is not None:
        raise RuntimeError("dedicated-lane run recorded a failure")
    if run["claimed_count"] != run["succeeded_count"]:
        raise RuntimeError("dedicated-lane run did not acknowledge every claimed item")
    if run["resource_accounting_mode"] != "statement_exact":
        raise RuntimeError("dedicated-lane run used fallback resource accounting")
    if int(run["wal_bytes"] or 0) > 32 * MIB or int(run["resource_temp_bytes"] or 0) > 32 * MIB:
        raise RuntimeError("dedicated-lane run exceeded its per-run WAL or temp stop")
    database = snapshot["database"]
    if database["open_runs"] or database["open_windows"]:
        raise RuntimeError("dedicated-lane post-check found an open run")
    return {
        "ok": True,
        "mode": "after",
        "run": dict(run),
        "duration_seconds": duration,
        **snapshot,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("before", "after"))
    parser.add_argument("--run-id", type=int)
    args = parser.parse_args()
    if (args.mode == "after") != (args.run_id is not None):
        parser.error("after mode requires --run-id and before mode rejects it")
    release_sha = os.environ.get("LEADERBOARD_SHADOW_RELEASE_SHA", "")
    previous_release_sha = os.environ.get("LEADERBOARD_DEDICATED_LANE_PREVIOUS_RELEASE_SHA")
    if not re.fullmatch(r"[0-9a-f]{40}", release_sha):
        raise RuntimeError("reviewed leaderboard release SHA is required")
    if previous_release_sha is not None and not re.fullmatch(r"[0-9a-f]{40}", previous_release_sha):
        raise RuntimeError("previous leaderboard release SHA is invalid")
    dsn = os.environ.get("SUPABASE_DB_URL_SESSION") or os.environ.get("SUPABASE_DB_URL")
    if not dsn:
        raise RuntimeError("database URL is required")
    if args.mode == "before":
        resource_evidence = os.environ.get("LEADERBOARD_SHADOW_RESOURCE_EVIDENCE_FILE", "")
        if not resource_evidence:
            raise RuntimeError("disk resource evidence path is required")
        validate_resource_evidence(Path(resource_evidence))
    with psycopg2.connect(
        dsn, connect_timeout=5,
        options="-c statement_timeout=5000 -c lock_timeout=1000 -c default_transaction_read_only=on",
        application_name="oddssearch-leaderboard-dedicated-lane-check",
    ) as connection:
        snapshot = database_snapshot(connection)
        result = (validate_before(snapshot, release_sha, previous_release_sha) if args.mode == "before"
                  else validate_after(snapshot, connection, release_sha, args.run_id))
    print(json.dumps(safe(result), sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as error:
        print(json.dumps({"ok": False, "reason": str(error)[:300]}, sort_keys=True))
        sys.exit(75)
