#!/usr/bin/env python3
"""Replayable worker and bounded backfill coordinator for leaderboard shadow v1.

This worker performs database-only processing. It never calls Sportmonks or any
paid provider API. Production activation is separately gated by pipeline_state.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import time
import uuid
from dataclasses import dataclass
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor


DATABASE_URL = os.environ.get("SUPABASE_DB_URL_SESSION") or os.environ.get("SUPABASE_DB_URL")
RELEASE_SHA = os.environ.get("LEADERBOARD_SHADOW_RELEASE_SHA", "development")


@dataclass(frozen=True)
class DirtyClaim:
    fixture_id: int
    dirty_version: int
    claim_token: str
    claim_fence: int


def connect():
    if not DATABASE_URL:
        raise RuntimeError("SUPABASE_DB_URL_SESSION or SUPABASE_DB_URL is required")
    return psycopg2.connect(
        DATABASE_URL,
        connect_timeout=20,
        application_name=f"oddssearch-leaderboard-shadow/{RELEASE_SHA[:12]}",
    )


def require_worker_enabled(conn, allow_disabled: bool) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "select worker_enabled from leaderboard_shadow_v1.pipeline_state where singleton"
        )
        row = cur.fetchone()
    if not row:
        raise RuntimeError("leaderboard shadow pipeline state is missing")
    if not row[0] and not allow_disabled:
        raise RuntimeError("leaderboard shadow worker is not enabled")


def require_backfill_enabled(conn, allow_disabled: bool) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "select backfill_enabled from leaderboard_shadow_v1.pipeline_state where singleton"
        )
        row = cur.fetchone()
    if not row or (not row[0] and not allow_disabled):
        raise RuntimeError("leaderboard shadow backfill is not enabled")


def start_worker_run(conn, worker_id: str) -> int:
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into leaderboard_shadow_v1.worker_runs(worker_id, release_sha)
                values (%s,%s) returning run_id
                """,
                (worker_id, RELEASE_SHA),
            )
            run_id = int(cur.fetchone()[0])
            cur.execute(
                """
                update leaderboard_shadow_v1.pipeline_state
                set release_sha=%s, last_worker_heartbeat_at=clock_timestamp(),
                    updated_at=clock_timestamp()
                where singleton
                """,
                (RELEASE_SHA,),
            )
    return run_id


def finish_worker_run(
    conn, run_id: int, claimed: int, succeeded: int, failed: int,
    failure_summary: str | None = None,
) -> None:
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                update leaderboard_shadow_v1.worker_runs
                set heartbeat_at=clock_timestamp(), finished_at=clock_timestamp(),
                    claimed_count=%s, succeeded_count=%s, failed_count=%s,
                    failure_summary=%s
                where run_id=%s
                """,
                (claimed, succeeded, failed, failure_summary, run_id),
            )
            cur.execute(
                """
                update leaderboard_shadow_v1.pipeline_state
                set last_worker_heartbeat_at=clock_timestamp(), updated_at=clock_timestamp()
                where singleton
                """
            )


def refresh_eligibility_if_due(conn, max_age_seconds: int = 600) -> int:
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select last_eligibility_refresh_at is null
                    or last_eligibility_refresh_at < clock_timestamp()-make_interval(secs=>%s)
                from leaderboard_shadow_v1.pipeline_state where singleton
                """,
                (max_age_seconds,),
            )
            if not cur.fetchone()[0]:
                return 0
            cur.execute("select leaderboard_shadow_v1.refresh_current_player_eligibility()")
            return int(cur.fetchone()[0])


def claim_dirty(conn, worker_id: str, lease_seconds: int = 60) -> DirtyClaim | None:
    token = f"{worker_id}:{uuid.uuid4().hex}"
    with conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                with candidate as (
                  select fixture_id
                  from leaderboard_shadow_v1.fixture_capture_queue
                  where claim_token is null or claim_until <= clock_timestamp()
                  order by last_dirty_at, fixture_id
                  for update skip locked
                  limit 1
                )
                update leaderboard_shadow_v1.fixture_capture_queue queue
                set claim_token=%s,
                    claim_fence=queue.claim_fence+1,
                    claim_until=clock_timestamp()+make_interval(secs=>%s),
                    attempts=queue.attempts+1,
                    last_error=null
                from candidate
                where queue.fixture_id=candidate.fixture_id
                returning queue.fixture_id, queue.dirty_version,
                          queue.claim_token, queue.claim_fence
                """,
                (token, lease_seconds),
            )
            row = cur.fetchone()
    return DirtyClaim(**row) if row else None


def process_dirty_claim(conn, claim: DirtyClaim) -> bool:
    """Capture and acknowledge only the exact dirty version that was claimed."""

    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "select leaderboard_shadow_v1.capture_fixture_source_change(%s)",
                (claim.fixture_id,),
            )
            cur.fetchone()
            cur.execute(
                """
                delete from leaderboard_shadow_v1.fixture_capture_queue
                where fixture_id=%s and dirty_version=%s
                  and claim_token=%s and claim_fence=%s
                """,
                (
                    claim.fixture_id,
                    claim.dirty_version,
                    claim.claim_token,
                    claim.claim_fence,
                ),
            )
            if cur.rowcount == 1:
                return True
            cur.execute(
                """
                update leaderboard_shadow_v1.fixture_capture_queue
                set claim_token=null, claim_until=null
                where fixture_id=%s and claim_token=%s and claim_fence=%s
                """,
                (claim.fixture_id, claim.claim_token, claim.claim_fence),
            )
            return False


def fail_dirty_claim(conn, claim: DirtyClaim, error: Exception) -> None:
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                update leaderboard_shadow_v1.fixture_capture_queue
                set claim_token=null, claim_until=null, last_error=%s
                where fixture_id=%s and claim_token=%s and claim_fence=%s
                """,
                (str(error)[:2000], claim.fixture_id, claim.claim_token, claim.claim_fence),
            )


def process_capture_queue(conn, worker_id: str, limit: int) -> dict[str, int]:
    result = {"claimed": 0, "acknowledged": 0, "redirtied": 0, "failed": 0}
    for _ in range(limit):
        claim = claim_dirty(conn, worker_id)
        if claim is None:
            break
        result["claimed"] += 1
        try:
            if process_dirty_claim(conn, claim):
                result["acknowledged"] += 1
            else:
                result["redirtied"] += 1
        except Exception as error:
            conn.rollback()
            fail_dirty_claim(conn, claim, error)
            result["failed"] += 1
    return result


def pending_cohorts(conn, limit: int) -> list[tuple[int, int]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select cursor.league_id, cursor.season_id
            from leaderboard_shadow_v1.cohort_change_cursors cursor
            left join leaderboard_shadow_v1.cohort_publications publication
              using (league_id, season_id)
            where cursor.committed_version > coalesce(publication.published_cursor, 0)
            order by cursor.updated_at, cursor.league_id, cursor.season_id
            limit %s
            """,
            (limit,),
        )
        return [(int(row[0]), int(row[1])) for row in cur.fetchall()]


def ensure_generation(conn, league_id: int, season_id: int) -> int | None:
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "select leaderboard_shadow_v1.begin_generation(%s,%s)",
                (league_id, season_id),
            )
            generation_id = cur.fetchone()[0]
            if generation_id is not None:
                cur.execute(
                    "select leaderboard_shadow_v1.materialize_generation(%s)",
                    (generation_id,),
                )
                cur.fetchone()
    return generation_id


def process_generation_job(conn, generation_id: int, worker_id: str) -> bool:
    token = f"{worker_id}:{uuid.uuid4().hex}"
    with conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "select * from leaderboard_shadow_v1.claim_generation_job(%s,%s,%s)",
                (generation_id, token, 120),
            )
            job = cur.fetchone()
    if not job:
        return False

    try:
        conn.set_session(isolation_level="REPEATABLE READ")
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "select leaderboard_shadow_v1.fixture_source_snapshot(%s) as snapshot",
                    (job["fixture_id"],),
                )
                snapshot = cur.fetchone()["snapshot"]
                excluded = job["change_kind"] == "exclude"
                if excluded:
                    facts: list[dict[str, Any]] = []
                    observed_hash = job["expected_source_hash"]
                else:
                    if not snapshot:
                        raise RuntimeError("canonical fixture source is missing")
                    facts = snapshot["facts"]
                    observed_hash = snapshot["source_hash"]
                cur.execute(
                    "select leaderboard_shadow_v1.stage_fixture_result(%s,%s,%s,%s,%s,%s,%s::jsonb)",
                    (
                        generation_id,
                        job["fixture_id"],
                        token,
                        job["fencing_number"],
                        observed_hash,
                        excluded,
                        json.dumps(facts),
                    ),
                )
                staged = cur.fetchone()
                return bool(next(iter(staged.values())))
    except Exception as error:
        conn.rollback()
        conn.set_session(isolation_level="READ COMMITTED")
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "select leaderboard_shadow_v1.fail_generation_job(%s,%s,%s,%s,%s,true)",
                    (
                        generation_id,
                        job["fixture_id"],
                        token,
                        job["fencing_number"],
                        str(error)[:2000],
                    ),
                )
        raise
    finally:
        conn.set_session(isolation_level="READ COMMITTED")


def finish_generation(conn, generation_id: int) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            select count(*) filter (where status in ('pending','processing')),
                   count(*) filter (where status='failed')
            from leaderboard_shadow_v1.cohort_generation_jobs
            where generation_id=%s
            """,
            (generation_id,),
        )
        outstanding, failed = cur.fetchone()
    if failed:
        raise RuntimeError(f"generation {generation_id} has {failed} failed jobs")
    if outstanding:
        return False
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "select leaderboard_shadow_v1.publish_generation(%s)",
                (generation_id,),
            )
            return bool(cur.fetchone()[0])


def process_generations(conn, worker_id: str, cohort_limit: int, job_limit: int) -> dict[str, int]:
    result = {"cohorts": 0, "jobs": 0, "published": 0}
    for league_id, season_id in pending_cohorts(conn, cohort_limit):
        generation_id = ensure_generation(conn, league_id, season_id)
        if generation_id is None:
            continue
        result["cohorts"] += 1
        for _ in range(job_limit):
            if not process_generation_job(conn, generation_id, worker_id):
                break
            result["jobs"] += 1
        if finish_generation(conn, generation_id):
            result["published"] += 1
    return result


def enqueue_backfill(conn, league_id: int, season_id: int, batch_size: int) -> int:
    """Restartable bootstrap after live capture is enabled.

    The checkpoint advances only with the queue inserts. Concurrent source
    commits upsert the same fixture row and increment dirty_version, so the
    handover cannot erase newer work.
    """

    failure: str | None = None
    enqueued = 0
    with conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                insert into leaderboard_shadow_v1.backfill_checkpoints(
                  league_id, season_id, source_watermark, expected_fixture_count,
                  source_hash, status, wal_start_lsn
                )
                select %s, %s, coalesce(max(id),0)::text, count(*),
                       md5(coalesce(string_agg(id::text, ',' order by id),'')),
                       'running', pg_current_wal_lsn()
                from public.fixtures where league_id=%s and season_id=%s
                on conflict (league_id, season_id) do nothing
                """,
                (league_id, season_id, league_id, season_id),
            )
            cur.execute(
                """
                select last_fixture_id, source_watermark, expected_fixture_count, source_hash
                from leaderboard_shadow_v1.backfill_checkpoints
                where league_id=%s and season_id=%s
                for update
                """,
                (league_id, season_id),
            )
            checkpoint = cur.fetchone()
            cur.execute(
                """
                select id from public.fixtures
                where league_id=%s and season_id=%s and id > coalesce(%s, 0)
                  and id <= %s::bigint
                order by id limit %s
                """,
                (
                    league_id, season_id, checkpoint["last_fixture_id"],
                    checkpoint["source_watermark"], batch_size,
                ),
            )
            fixture_ids = [int(row["id"]) for row in cur.fetchall()]
            if fixture_ids:
                cur.execute(
                    "select leaderboard_shadow_v1.enqueue_fixture_ids(%s::bigint[], 'historical_backfill')",
                    (fixture_ids,),
                )
                cur.execute(
                    """
                    update leaderboard_shadow_v1.backfill_checkpoints
                    set last_fixture_id=%s,
                        processed_fixture_count=processed_fixture_count+%s,
                        updated_at=clock_timestamp()
                    where league_id=%s and season_id=%s
                    """,
                    (fixture_ids[-1], len(fixture_ids), league_id, season_id),
                )
                enqueued = len(fixture_ids)
            else:
                cur.execute(
                    """
                    select count(*) as fixture_count,
                           md5(coalesce(string_agg(id::text, ',' order by id),'')) as source_hash
                    from public.fixtures
                    where league_id=%s and season_id=%s and id <= %s::bigint
                    """,
                    (league_id, season_id, checkpoint["source_watermark"]),
                )
                final_source = cur.fetchone()
                if (
                    int(final_source["fixture_count"]) != int(checkpoint["expected_fixture_count"])
                    or final_source["source_hash"] != checkpoint["source_hash"]
                ):
                    failure = "bounded source fixture set changed during bootstrap"
                cur.execute(
                    """
                    update leaderboard_shadow_v1.backfill_checkpoints
                    set status=%s, wal_end_lsn=pg_current_wal_lsn(),
                        updated_at=clock_timestamp()
                    where league_id=%s and season_id=%s
                    """,
                    ('failed' if failure else 'complete', league_id, season_id),
                )
    if failure:
        raise RuntimeError(failure)
    return enqueued


def backfill_cohorts(conn, limit: int) -> list[tuple[int, int]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select fixture.league_id::integer, fixture.season_id::integer
            from public.fixtures fixture
            join public.league_product_policy policy
              on policy.league_id=fixture.league_id
             and policy.is_supported and policy.searchable and not policy.is_cup
            left join leaderboard_shadow_v1.backfill_checkpoints checkpoint
              on checkpoint.league_id=fixture.league_id
             and checkpoint.season_id=fixture.season_id
            where checkpoint.status is distinct from 'complete'
            group by fixture.league_id, fixture.season_id
            order by 1, 2
            limit %s
            """,
            (limit,),
        )
        return [(int(row[0]), int(row[1])) for row in cur.fetchall()]


def status(conn) -> dict[str, Any]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("select public.player_leaderboard_shadow_status_v2() as payload")
        return cur.fetchone()["payload"]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("run-once", "backfill", "backfill-all", "status"))
    parser.add_argument("--allow-disabled", action="store_true")
    parser.add_argument("--capture-limit", type=int, default=1000)
    parser.add_argument("--cohort-limit", type=int, default=8)
    parser.add_argument("--job-limit", type=int, default=1000)
    parser.add_argument("--league-id", type=int)
    parser.add_argument("--season-id", type=int)
    parser.add_argument("--batch-size", type=int, default=250)
    args = parser.parse_args()
    worker_id = f"{socket.gethostname()}:{os.getpid()}"

    conn = connect()
    try:
        if args.command == "status":
            print(json.dumps(status(conn), default=str, sort_keys=True))
            return 0
        require_worker_enabled(conn, args.allow_disabled)
        if args.command in {"backfill", "backfill-all"}:
            require_backfill_enabled(conn, args.allow_disabled)
            if args.command == "backfill":
                if args.league_id is None or args.season_id is None:
                    parser.error("backfill requires --league-id and --season-id")
                cohorts = [(args.league_id, args.season_id)]
            else:
                cohorts = backfill_cohorts(conn, args.cohort_limit)
            counts = [
                {
                    "league_id": league_id,
                    "season_id": season_id,
                    "enqueued": enqueue_backfill(
                        conn, league_id, season_id, args.batch_size,
                    ),
                }
                for league_id, season_id in cohorts
            ]
            print(json.dumps({"cohorts": counts, "enqueued": sum(row["enqueued"] for row in counts)}))
            return 0

        started = time.monotonic()
        run_id = start_worker_run(conn, worker_id)
        try:
            eligibility_count = refresh_eligibility_if_due(conn)
            capture = process_capture_queue(conn, worker_id, args.capture_limit)
            generations = process_generations(conn, worker_id, args.cohort_limit, args.job_limit)
            claimed = capture["claimed"] + generations["jobs"]
            succeeded = capture["acknowledged"] + generations["jobs"]
            failed = capture["failed"]
            finish_worker_run(conn, run_id, claimed, succeeded, failed)
            print(json.dumps({
                "capture": capture,
                "eligibility_count": eligibility_count,
                "generations": generations,
                "run_id": run_id,
                "elapsed_ms": round((time.monotonic() - started) * 1000, 2),
            }, sort_keys=True))
        except Exception as error:
            conn.rollback()
            finish_worker_run(conn, run_id, 0, 0, 1, str(error)[:2000])
            raise
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
