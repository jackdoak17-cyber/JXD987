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
import stat
import sys
import time
import uuid
from dataclasses import dataclass
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor


DATABASE_URL = os.environ.get("SUPABASE_DB_URL_SESSION") or os.environ.get("SUPABASE_DB_URL")
RELEASE_SHA = os.environ.get("LEADERBOARD_SHADOW_RELEASE_SHA", "development")
MAX_FIXTURES_PER_RUN = 250
MAX_SECONDS_PER_RUN = 30.0
LOCK_TIMEOUT_MS = 2_000
# Leave enough of the hard run deadline for the mandatory resource check and
# transaction cleanup.  Starting another fixture with only milliseconds left
# turns an otherwise healthy bounded run into a statement-timeout failure.
SAFETY_CHECK_RESERVE_SECONDS = 5.0
RESOURCE_EVIDENCE_FILE = os.environ.get(
    "LEADERBOARD_SHADOW_RESOURCE_EVIDENCE_FILE",
    "/var/lib/oddssearch/leaderboard-shadow-resource.json",
)


@dataclass(frozen=True)
class DirtyClaim:
    fixture_id: int
    dirty_version: int
    claim_token: str
    claim_fence: int


@dataclass
class RunBudget:
    fixture_limit: int
    max_seconds: float
    started_at: float
    processed: int = 0

    @classmethod
    def create(cls, fixture_limit: int, max_seconds: float) -> "RunBudget":
        if not 1 <= fixture_limit <= MAX_FIXTURES_PER_RUN:
            raise ValueError(f"fixture limit must be between 1 and {MAX_FIXTURES_PER_RUN}")
        if not 0 < max_seconds <= MAX_SECONDS_PER_RUN:
            raise ValueError(f"max seconds must be greater than 0 and at most {MAX_SECONDS_PER_RUN}")
        return cls(fixture_limit, max_seconds, time.monotonic())

    def remaining_seconds(self) -> float:
        return max(0.0, self.max_seconds - (time.monotonic() - self.started_at))

    def available(self) -> bool:
        return (
            self.processed < self.fixture_limit
            and self.remaining_seconds() > SAFETY_CHECK_RESERVE_SECONDS
        )

    def consume(self) -> None:
        self.processed += 1

    def apply_database_timeouts(self, conn) -> None:
        remaining_ms = max(1, int(self.remaining_seconds() * 1000))
        with conn.cursor() as cur:
            cur.execute("select set_config('lock_timeout', %s, false)", (f"{LOCK_TIMEOUT_MS}ms",))
            cur.execute("select set_config('statement_timeout', %s, false)", (f"{remaining_ms}ms",))


def configure_temp_file_limit_if_permitted(conn) -> bool:
    """Apply defense-in-depth only when the connected role may set the parameter.

    Supabase's production postgres login owns the application objects but is not
    a superuser and cannot set temp_file_limit. The attributed resource guard,
    statement deadline and bounded run remain mandatory in either case.
    """

    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "select has_parameter_privilege(current_user, 'temp_file_limit', 'SET')"
            )
            permitted = bool(cur.fetchone()[0])
            if permitted:
                cur.execute(
                    """
                    select set_config(
                      'temp_file_limit',
                      greatest(0, floor(max_temp_bytes / 1024.0))::bigint::text || 'kB',
                      false
                    )
                    from leaderboard_shadow_v1.pipeline_state where singleton
                    """
                )
    return permitted


def connect():
    if not DATABASE_URL:
        raise RuntimeError("SUPABASE_DB_URL_SESSION or SUPABASE_DB_URL is required")
    return psycopg2.connect(
        DATABASE_URL,
        connect_timeout=20,
        application_name=f"oddssearch-leaderboard-shadow/{RELEASE_SHA[:12]}",
    )


def isolated_test_database(conn) -> bool:
    with conn.cursor() as cur:
        cur.execute("select current_database(), inet_server_addr() is null")
        database_name, socket_only = cur.fetchone()
    return bool(socket_only and str(database_name).startswith("oddssearch_m3"))


def validate_activation_override(conn, isolated_test_override: bool) -> bool:
    if not isolated_test_override:
        return False
    if not isolated_test_database(conn):
        raise RuntimeError(
            "--isolated-test-override is restricted to socket-only oddssearch_m3* databases"
        )
    return True


def load_external_resource_evidence(conn, path: str = RESOURCE_EVIDENCE_FILE) -> dict[str, Any]:
    file_stat = os.stat(path, follow_symlinks=False)
    if not stat.S_ISREG(file_stat.st_mode) or file_stat.st_mode & 0o022:
        raise RuntimeError("resource evidence must be a regular, non-group/world-writable file")
    with open(path, encoding="utf-8") as evidence_file:
        evidence = json.load(evidence_file)
    required = {
        "disk_available_bytes",
        "source_ingestion_baseline_p95_ms",
        "source_ingestion_current_p95_ms",
        "observed_at",
    }
    if set(evidence) != required:
        raise RuntimeError("resource evidence has unexpected or missing fields")
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "select leaderboard_shadow_v1.set_external_resource_evidence(%s,%s,%s,%s)",
                (
                    int(evidence["disk_available_bytes"]),
                    float(evidence["source_ingestion_baseline_p95_ms"]),
                    float(evidence["source_ingestion_current_p95_ms"]),
                    str(evidence["observed_at"]),
                ),
            )
    return evidence


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


def enforce_resource_guard(conn, require_external: bool) -> dict[str, Any]:
    with conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "select leaderboard_shadow_v1.enforce_resource_guard(%s) as evidence",
                (require_external,),
            )
            evidence = cur.fetchone()["evidence"]
    if not evidence.get("ok"):
        raise RuntimeError(
            "leaderboard shadow resource guard paused processing: "
            + ",".join(evidence.get("breaches", []))
        )
    return evidence


def apply_recovery_timeouts(conn) -> None:
    """Give lease/error cleanup a small bound after the processing deadline."""
    conn.rollback()
    with conn:
        with conn.cursor() as cur:
            cur.execute("select set_config('lock_timeout', '2000ms', false)")
            cur.execute("select set_config('statement_timeout', '5000ms', false)")


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


def claim_dirty(
    conn,
    worker_id: str,
    lease_seconds: int = 60,
    league_id: int | None = None,
    season_id: int | None = None,
) -> DirtyClaim | None:
    token = f"{worker_id}:{uuid.uuid4().hex}"
    with conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                with candidate as (
                  select queue.fixture_id
                  from leaderboard_shadow_v1.fixture_capture_queue queue
                  left join leaderboard_shadow_v1.fixture_source_versions recorded
                    on recorded.fixture_id=queue.fixture_id
                  left join public.fixtures current_fixture
                    on current_fixture.id=queue.fixture_id
                  where (queue.claim_token is null
                         or queue.claim_until <= clock_timestamp())
                    and (
                      %s::bigint is null
                      or (recorded.league_id, recorded.season_id)=(%s,%s)
                      or (current_fixture.league_id, current_fixture.season_id)=(%s,%s)
                    )
                  order by queue.last_dirty_at, queue.fixture_id
                  for update of queue skip locked
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
                (
                    league_id,
                    league_id,
                    season_id,
                    league_id,
                    season_id,
                    token,
                    lease_seconds,
                ),
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


def process_capture_queue(
    conn,
    worker_id: str,
    limit: int,
    budget: RunBudget,
    require_external: bool,
    league_id: int | None = None,
    season_id: int | None = None,
) -> dict[str, int]:
    result = {"claimed": 0, "acknowledged": 0, "redirtied": 0, "failed": 0}
    for _ in range(min(limit, budget.fixture_limit - budget.processed)):
        if not budget.available():
            break
        budget.apply_database_timeouts(conn)
        enforce_resource_guard(conn, require_external)
        claim = claim_dirty(
            conn,
            worker_id,
            league_id=league_id,
            season_id=season_id,
        )
        if claim is None:
            break
        result["claimed"] += 1
        budget.consume()
        try:
            if process_dirty_claim(conn, claim):
                result["acknowledged"] += 1
            else:
                result["redirtied"] += 1
        except Exception as error:
            apply_recovery_timeouts(conn)
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
              -- A queued fixture may invalidate an older journal event after a
              -- snapshot-contract upgrade or a cohort transfer.  Defer both
              -- its recorded and current cohorts until capture acknowledges
              -- the exact dirty version; publication must never race ahead of
              -- canonical source capture.
              and not exists (
                select 1
                from leaderboard_shadow_v1.fixture_capture_queue queue
                left join leaderboard_shadow_v1.fixture_source_versions recorded
                  on recorded.fixture_id = queue.fixture_id
                left join public.fixtures current_fixture
                  on current_fixture.id = queue.fixture_id
                where (recorded.league_id, recorded.season_id)
                        = (cursor.league_id, cursor.season_id)
                   or (current_fixture.league_id, current_fixture.season_id)
                        = (cursor.league_id, cursor.season_id)
              )
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
        apply_recovery_timeouts(conn)
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


def process_generations(
    conn, worker_id: str, cohort_limit: int, job_limit: int,
    budget: RunBudget, require_external: bool,
) -> dict[str, int]:
    result = {"cohorts": 0, "jobs": 0, "published": 0}
    for league_id, season_id in pending_cohorts(conn, cohort_limit):
        if not budget.available():
            break
        budget.apply_database_timeouts(conn)
        enforce_resource_guard(conn, require_external)
        generation_id = ensure_generation(conn, league_id, season_id)
        if generation_id is None:
            continue
        result["cohorts"] += 1
        for _ in range(job_limit):
            if not budget.available():
                break
            budget.apply_database_timeouts(conn)
            enforce_resource_guard(conn, require_external)
            if not process_generation_job(conn, generation_id, worker_id):
                break
            result["jobs"] += 1
            budget.consume()
        if not budget.available():
            break
        budget.apply_database_timeouts(conn)
        enforce_resource_guard(conn, require_external)
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
    parser.add_argument("--isolated-test-override", action="store_true")
    parser.add_argument("--fixture-limit", type=int, default=MAX_FIXTURES_PER_RUN)
    parser.add_argument("--max-seconds", type=float, default=MAX_SECONDS_PER_RUN)
    parser.add_argument("--capture-limit", type=int, default=MAX_FIXTURES_PER_RUN)
    parser.add_argument("--cohort-limit", type=int, default=8)
    parser.add_argument("--job-limit", type=int, default=MAX_FIXTURES_PER_RUN)
    parser.add_argument("--league-id", type=int)
    parser.add_argument("--season-id", type=int)
    parser.add_argument("--batch-size", type=int, default=250)
    args = parser.parse_args()
    if (args.league_id is None) != (args.season_id is None):
        parser.error("league-id and season-id must be supplied together")
    worker_id = f"{socket.gethostname()}:{os.getpid()}"

    conn = connect()
    try:
        if args.command == "status":
            print(json.dumps(status(conn), default=str, sort_keys=True))
            return 0
        budget = RunBudget.create(args.fixture_limit, args.max_seconds)
        allow_disabled = validate_activation_override(conn, args.isolated_test_override)
        require_worker_enabled(conn, allow_disabled)
        require_external = not allow_disabled
        if require_external:
            load_external_resource_evidence(conn)
        temp_file_limit_enforced = configure_temp_file_limit_if_permitted(conn)
        budget.apply_database_timeouts(conn)
        resource_evidence = enforce_resource_guard(conn, require_external)
        if args.command in {"backfill", "backfill-all"}:
            require_backfill_enabled(conn, allow_disabled)
            if args.command == "backfill":
                if args.league_id is None or args.season_id is None:
                    parser.error("backfill requires --league-id and --season-id")
                cohorts = [(args.league_id, args.season_id)]
            else:
                cohorts = backfill_cohorts(conn, args.cohort_limit)
            counts = []
            for league_id, season_id in cohorts:
                if not budget.available():
                    break
                budget.apply_database_timeouts(conn)
                enforce_resource_guard(conn, require_external)
                batch_size = min(args.batch_size, budget.fixture_limit - budget.processed)
                enqueued = enqueue_backfill(conn, league_id, season_id, batch_size)
                budget.processed += enqueued
                counts.append({
                    "league_id": league_id,
                    "season_id": season_id,
                    "enqueued": enqueued,
                })
            print(json.dumps({
                "cohorts": counts,
                "enqueued": sum(row["enqueued"] for row in counts),
                "processed_fixtures": budget.processed,
                "elapsed_ms": round((time.monotonic() - budget.started_at) * 1000, 2),
                "temp_file_limit_enforced": temp_file_limit_enforced,
                "resource_guard": resource_evidence,
            }, default=str))
            return 0

        run_id = start_worker_run(conn, worker_id)
        try:
            eligibility_count = refresh_eligibility_if_due(conn)
            capture = process_capture_queue(
                conn,
                worker_id,
                args.capture_limit,
                budget,
                require_external,
                league_id=args.league_id,
                season_id=args.season_id,
            )
            generations = process_generations(
                conn, worker_id, args.cohort_limit, args.job_limit, budget, require_external,
            )
            claimed = capture["claimed"] + generations["jobs"]
            succeeded = capture["acknowledged"] + generations["jobs"]
            failed = capture["failed"]
            finish_worker_run(conn, run_id, claimed, succeeded, failed)
            print(json.dumps({
                "capture": capture,
                "eligibility_count": eligibility_count,
                "generations": generations,
                "run_id": run_id,
                "processed_fixtures": budget.processed,
                "budget_exhausted": not budget.available(),
                "elapsed_ms": round((time.monotonic() - budget.started_at) * 1000, 2),
                "temp_file_limit_enforced": temp_file_limit_enforced,
                "resource_guard": resource_evidence,
            }, sort_keys=True))
        except Exception as error:
            apply_recovery_timeouts(conn)
            finish_worker_run(conn, run_id, 0, 0, 1, str(error)[:2000])
            raise
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
