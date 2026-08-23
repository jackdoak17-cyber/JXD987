#!/usr/bin/env python3
"""Validate production referee assignment and stats coverage.

This is deliberately database-only. Provider responses are recorded by the
assignment worker, while this check answers whether the user-facing database
is fresh, retryable, and complete for the upcoming fixture window.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List

import psycopg2

logger = logging.getLogger("validate_referee_coverage")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate referee assignment and stats coverage")
    parser.add_argument("--days-back", type=int, default=1)
    parser.add_argument("--days-forward", type=int, default=31)
    parser.add_argument("--assignment-sla-hours", type=int, default=24)
    parser.add_argument("--unassigned-grace-hours", type=int, default=12)
    parser.add_argument("--fixture-id", type=int, default=0)
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


def write_report(path: str, report: Dict[str, Any]) -> None:
    if not path:
        return
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2, ensure_ascii=False, default=str)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_args()
    conn = psycopg2.connect(get_db_url())
    try:
        with conn.cursor() as cur:
            params: List[Any] = [max(0, args.days_back), max(0, args.days_forward)]
            fixture_filter = ""
            if args.fixture_id > 0:
                fixture_filter = "and f.id = %s"
                params.append(args.fixture_id)
            cur.execute(
                f"""
                select
                  f.id,
                  f.starting_at,
                  f.status_code,
                  count(fr.referee_id) filter (where fr.source = 'sportmonks')::int as assignment_count,
                  max(fr.last_synced_at) filter (where fr.source = 'sportmonks') as assignment_synced_at,
                  s.status as sync_status,
                  s.next_attempt_at,
                  s.last_error,
                  rs.data_status,
                  rs.sample_5,
                  rs.sample_10,
                  rs.sample_20,
                  rs.updated_at as stats_updated_at
                from public.fixtures f
                left join public.fixture_referees fr on fr.fixture_id = f.id
                left join public.fixture_referee_sync_state s on s.fixture_id = f.id
                left join public.fixture_referee_stats rs on rs.fixture_id = f.id
                where f.starting_at >= (now() - make_interval(days => %s))
                  and f.starting_at <= (now() + make_interval(days => %s))
                  {fixture_filter}
                group by f.id, f.starting_at, f.status_code, s.status, s.next_attempt_at,
                         s.last_error, rs.data_status, rs.sample_5, rs.sample_10,
                         rs.sample_20, rs.updated_at
                order by f.starting_at asc, f.id asc
                """,
                params,
            )
            columns = [description[0] for description in cur.description]
            rows = [dict(zip(columns, row)) for row in cur.fetchall()]
    finally:
        conn.close()

    now = datetime.now(timezone.utc)
    assignment_sla_seconds = max(1, args.assignment_sla_hours) * 3600
    unassigned_grace_seconds = max(1, args.unassigned_grace_hours) * 3600
    unassigned_due: List[int] = []
    stale_assignments: List[int] = []
    overdue_errors: List[int] = []
    stats_without_assignment: List[int] = []

    for row in rows:
        fixture_id = int(row["id"])
        start = row["starting_at"]
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        assignment_count = int(row["assignment_count"] or 0)
        sync_status = (row["sync_status"] or "").strip().lower()
        if assignment_count == 0 and start > now:
            seconds_to_kickoff = (start - now).total_seconds()
            if seconds_to_kickoff <= unassigned_grace_seconds and sync_status in {"", "pending", "error"}:
                unassigned_due.append(fixture_id)
        synced_at = row["assignment_synced_at"]
        if assignment_count > 0 and synced_at is not None:
            if synced_at.tzinfo is None:
                synced_at = synced_at.replace(tzinfo=timezone.utc)
            if (now - synced_at).total_seconds() > assignment_sla_seconds:
                stale_assignments.append(fixture_id)
        next_attempt = row["next_attempt_at"]
        if sync_status == "error" and next_attempt is not None and next_attempt <= now:
            overdue_errors.append(fixture_id)
        if int(row["sample_20"] or 0) == 0 and assignment_count > 0:
            stats_without_assignment.append(fixture_id)

    hard_failures = sorted(set(unassigned_due + stale_assignments + overdue_errors))
    report: Dict[str, Any] = {
        "ok": not hard_failures,
        "generated_at_utc": now.isoformat(),
        "window": {"days_back": args.days_back, "days_forward": args.days_forward},
        "fixtures_scanned": len(rows),
        "fixtures_with_assignments": sum(int(row["assignment_count"] or 0) > 0 for row in rows),
        "fixtures_without_assignments": sum(int(row["assignment_count"] or 0) == 0 for row in rows),
        "unassigned_due_soon": sorted(unassigned_due),
        "stale_assignments": sorted(stale_assignments),
        "overdue_sync_errors": sorted(overdue_errors),
        "assigned_without_20_match_sample": sorted(stats_without_assignment),
        "hard_failures": hard_failures,
    }
    print(json.dumps(report, indent=2, ensure_ascii=False, default=str))
    write_report(args.report_json, report)
    return 1 if hard_failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
