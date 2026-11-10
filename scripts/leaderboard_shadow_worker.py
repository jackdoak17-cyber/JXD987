#!/usr/bin/env python3
"""Replayable worker and bounded backfill coordinator for leaderboard shadow v1.

This worker performs database-only processing. It never calls Sportmonks or any
paid provider API. Production activation is separately gated by pipeline_state.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import socket
import stat
import sys
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable

import psycopg2
from psycopg2.extras import RealDictCursor


DATABASE_URL = os.environ.get("SUPABASE_DB_URL_SESSION") or os.environ.get("SUPABASE_DB_URL")
RELEASE_SHA = os.environ.get("LEADERBOARD_SHADOW_RELEASE_SHA", "development")
MAX_FIXTURES_PER_RUN = 250
MAX_SECONDS_PER_RUN = 30.0
LOCK_TIMEOUT_MS = 2_000
PREFLIGHT_STATEMENT_TIMEOUT_MS = 5_000
# Leave enough of the hard run deadline for the mandatory resource check and
# transaction cleanup.  Starting another fixture with only milliseconds left
# turns an otherwise healthy bounded run into a statement-timeout failure.
SAFETY_CHECK_RESERVE_SECONDS = 5.0
# The run is guarded before processing begins and publication is guarded again
# separately.  Recheck capture resource use in small bounded groups so a hard
# limit stops the run promptly without making the pg_stat_statements scan the
# dominant cost of every individual fixture.
RESOURCE_GUARD_FIXTURE_INTERVAL = 10
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


def apply_preflight_timeouts(conn) -> None:
    """Bound fail-closed evidence checks before the processing budget starts.

    The worker owns the shared database-heavy slot while these checks run. A
    congested statistics view must therefore defer quickly instead of delaying
    the higher-priority source importer at its next scheduled boundary.
    """

    with conn.cursor() as cur:
        cur.execute("select set_config('lock_timeout', %s, false)", (f"{LOCK_TIMEOUT_MS}ms",))
        cur.execute(
            "select set_config('statement_timeout', %s, false)",
            (f"{PREFLIGHT_STATEMENT_TIMEOUT_MS}ms",),
        )


def connect():
    if not DATABASE_URL:
        raise RuntimeError("SUPABASE_DB_URL_SESSION or SUPABASE_DB_URL is required")
    trial_token = os.environ.get("LEADERBOARD_FINITE_TRIAL_CONNECTION_TOKEN")
    if trial_token and not re.fullmatch(r"[0-9a-f]{16}", trial_token):
        raise RuntimeError("finite trial connection token is invalid")
    return psycopg2.connect(
        DATABASE_URL,
        connect_timeout=20,
        application_name=(
            f"oddssearch-finite-trial/{trial_token}" if trial_token
            else f"oddssearch-leaderboard-shadow/{RELEASE_SHA[:12]}"
        ),
    )


def isolated_test_database(conn) -> bool:
    with conn.cursor() as cur:
        cur.execute("select current_database(), inet_server_addr() is null")
        database_name, socket_only = cur.fetchone()
    return bool(socket_only and str(database_name).startswith("oddssearch_m3"))


def validate_production_trial_connection(conn, args) -> None:
    if args.isolated_test_override:
        raise RuntimeError("production finite trial rejects the general test override")
    if args.production_trial_isolated_test:
        if not isolated_test_database(conn):
            raise RuntimeError("production trial test path requires socket-only oddssearch_m3")
    else:
        expected_hash = os.environ.get("LEADERBOARD_FINITE_TRIAL_EXPECTED_DSN_SHA256", "")
        if not re.fullmatch(r"[0-9a-f]{64}", expected_hash) or not DATABASE_URL:
            raise RuntimeError("production finite trial requires a pinned database URL hash")
        if hashlib.sha256(DATABASE_URL.encode()).hexdigest() != expected_hash:
            raise RuntimeError("production finite trial database URL does not match its pin")
        with conn.cursor() as cur:
            cur.execute("select current_database(), inet_server_addr() is not null, current_user")
            database_name, network_connection, database_role = cur.fetchone()
        if database_name != "postgres" or not network_connection or database_role != "postgres":
            raise RuntimeError("production finite trial database identity is invalid")
    if not re.fullmatch(r"[0-9a-f]{40}", RELEASE_SHA):
        raise RuntimeError("production finite trial requires a complete release SHA")
    if os.environ.get("LEADERBOARD_FINITE_TRIAL_RELEASE_SHA") != RELEASE_SHA:
        raise RuntimeError("production finite trial worker release differs from its reviewed pin")
    if not re.fullmatch(
        r"[0-9a-f]{16}", os.environ.get("LEADERBOARD_FINITE_TRIAL_CONNECTION_TOKEN", "")
    ):
        raise RuntimeError("production finite trial requires a supervisor connection token")
    if not args.production_trial_isolated_test and (
        args.trial_test_pg_sleep_after_admission
        or args.trial_test_pg_sleep_after_capture
        or args.trial_test_pg_sleep_before_recovery
    ):
        raise RuntimeError("production finite trial rejects test sleep hooks")
    supervisor_pid = os.environ.get("LEADERBOARD_FINITE_TRIAL_SUPERVISOR_PID", "")
    if not supervisor_pid.isdecimal() or int(supervisor_pid) != os.getppid():
        raise RuntimeError("production finite trial requires its live supervisor parent")
    lock_specs = (
        ("LEADERBOARD_FINITE_TRIAL_WORKER_LOCK_FD", "LEADERBOARD_FINITE_TRIAL_WORKER_LOCK_PATH"),
        ("LEADERBOARD_FINITE_TRIAL_HEAVY_LOCK_FD", "LEADERBOARD_FINITE_TRIAL_HEAVY_LOCK_PATH"),
    )
    identities = []
    for fd_key, path_key in lock_specs:
        fd_text = os.environ.get(fd_key, "")
        path = os.environ.get(path_key, "")
        if not fd_text.isdecimal() or not path or not os.path.isabs(path):
            raise RuntimeError("production finite trial inherited lock identity is incomplete")
        fd = int(fd_text)
        try:
            held = os.fstat(fd)
            named = os.stat(path, follow_symlinks=False)
            if not stat.S_ISREG(held.st_mode) or not stat.S_ISREG(named.st_mode):
                raise RuntimeError("production finite trial lock is not a regular file")
            if (held.st_dev, held.st_ino) != (named.st_dev, named.st_ino):
                raise RuntimeError("production finite trial lock descriptor changed")
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (OSError, ValueError) as error:
            raise RuntimeError("production finite trial does not hold both host locks") from error
        identities.append((held.st_dev, held.st_ino))
    if identities[0] == identities[1]:
        raise RuntimeError("production finite trial host locks are not distinct")


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
        "observed_at",
    }
    if set(evidence) != required:
        raise RuntimeError("resource evidence has unexpected or missing fields")
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "select leaderboard_shadow_v1.refresh_external_resource_evidence_v2(%s,%s)",
                (
                    int(evidence["disk_available_bytes"]),
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
                "select nullif(current_setting('oddssearch.worker_run_id',true),'')::bigint "
                "as active_run_id"
            )
            active_run_id = cur.fetchone()["active_run_id"]
            if active_run_id is not None:
                cur.execute(
                    "select leaderboard_shadow_v1.checkpoint_worker_resource_window(%s,false)",
                    (active_run_id,),
                )
                cur.fetchone()
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


def mark_failed_statement_interval(conn) -> None:
    """Charge an interval with failed SQL by database WAL at its next checkpoint.

    A failed transaction is rolled back before this call. The marker belongs to
    this connection, survives subsequent commits, and is cleared by a
    successful checkpoint. Interrupted windows are marked during DB recovery.
    """
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "select set_config('oddssearch.worker_failed_statement','true',false)"
            )


def start_worker_run(conn, worker_id: str) -> int:
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "select set_config('oddssearch.worker_failed_statement','false',false)"
            )
            cur.execute(
                """
                insert into leaderboard_shadow_v1.worker_runs(worker_id, release_sha)
                values (%s,%s) returning run_id
                """,
                (worker_id, RELEASE_SHA),
            )
            run_id = int(cur.fetchone()[0])
            cur.execute(
                "select leaderboard_shadow_v1.begin_worker_resource_window(%s)",
                (run_id,),
            )
            cur.execute(
                "select set_config('oddssearch.worker_run_id',%s,false)",
                (str(run_id),),
            )
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


def recover_unfinished_worker_runs(conn) -> dict[str, Any]:
    """Close a prior killed worker's durable resource window before new work."""
    with conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "select leaderboard_shadow_v1."
                "recover_unfinished_worker_resource_windows() as evidence"
            )
            return cur.fetchone()["evidence"]


def finish_worker_run(
    conn, run_id: int, claimed: int, succeeded: int, failed: int,
    failure_summary: str | None = None,
) -> None:
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "select leaderboard_shadow_v1.checkpoint_worker_resource_window(%s,true)",
                (run_id,),
            )
            cur.fetchone()
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
            cur.execute("select set_config('oddssearch.worker_run_id','',false)")
            cur.execute(
                """
                update leaderboard_shadow_v1.pipeline_state
                set last_worker_heartbeat_at=clock_timestamp(), updated_at=clock_timestamp()
                where singleton
                """
            )


def finish_worker_run_with_resource_check(
    conn,
    run_id: int,
    claimed: int,
    succeeded: int,
    failed: int,
    require_external: bool,
) -> dict[str, Any]:
    """Record the run, then persist a resource snapshot after final writes.

    The post-finish snapshot makes a later pg_stat_statements attribution
    rollover lossless: no leaderboard worker transaction can sit between the
    last accepted resource evidence and the completed worker run.
    """

    finish_worker_run(conn, run_id, claimed, succeeded, failed)
    apply_recovery_timeouts(conn)
    return enforce_resource_guard(conn, require_external)


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
    audit_run_id: int | None = None,
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
            if row is not None and audit_run_id is not None:
                cur.execute(
                    "update leaderboard_shadow_v1.worker_runs "
                    "set claimed_count=claimed_count+1 where run_id=%s and finished_at is null",
                    (audit_run_id,),
                )
                if cur.rowcount != 1:
                    raise RuntimeError("finite trial audit run is not open")
    return DirtyClaim(**row) if row else None


def process_dirty_claim(
    conn, claim: DirtyClaim, audit_run_id: int | None = None,
) -> bool:
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
                if audit_run_id is not None:
                    cur.execute(
                        "update leaderboard_shadow_v1.worker_runs "
                        "set succeeded_count=succeeded_count+1 "
                        "where run_id=%s and finished_at is null",
                        (audit_run_id,),
                    )
                    if cur.rowcount != 1:
                        raise RuntimeError("finite trial audit run is not open")
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


def fail_dirty_claim(
    conn, claim: DirtyClaim, error: Exception, audit_run_id: int | None = None,
) -> None:
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
            if audit_run_id is not None:
                cur.execute(
                    "update leaderboard_shadow_v1.worker_runs "
                    "set failed_count=failed_count+1 "
                    "where run_id=%s and finished_at is null",
                    (audit_run_id,),
                )
                if cur.rowcount != 1:
                    raise RuntimeError("finite trial audit run is not open")


def process_capture_queue(
    conn,
    worker_id: str,
    limit: int,
    budget: RunBudget,
    require_external: bool,
    league_id: int | None = None,
    season_id: int | None = None,
    guard_check: Callable[[Any, bool], dict[str, Any]] | None = None,
    progress: dict[str, int] | None = None,
    audit_run_id: int | None = None,
    guard_interval: int = RESOURCE_GUARD_FIXTURE_INTERVAL,
) -> dict[str, int]:
    if not 1 <= guard_interval <= RESOURCE_GUARD_FIXTURE_INTERVAL:
        raise ValueError("capture guard interval is outside its bounded range")
    result = progress if progress is not None else {
        "claimed": 0, "acknowledged": 0, "redirtied": 0, "failed": 0,
    }
    guard_check = guard_check or enforce_resource_guard
    for _ in range(min(limit, budget.fixture_limit - budget.processed)):
        if not budget.available():
            break
        if result["claimed"] > 0 and (
            result["claimed"] % guard_interval == 0
        ):
            budget.apply_database_timeouts(conn)
            guard_check(conn, require_external)
        claim = claim_dirty(
            conn,
            worker_id,
            league_id=league_id,
            season_id=season_id,
            **({"audit_run_id": audit_run_id} if audit_run_id is not None else {}),
        )
        if claim is None:
            break
        result["claimed"] += 1
        budget.consume()
        try:
            if process_dirty_claim(
                conn, claim,
                **({"audit_run_id": audit_run_id} if audit_run_id is not None else {}),
            ):
                result["acknowledged"] += 1
            else:
                result["redirtied"] += 1
        except Exception as error:
            apply_recovery_timeouts(conn)
            mark_failed_statement_interval(conn)
            fail_dirty_claim(
                conn, claim, error,
                **({"audit_run_id": audit_run_id} if audit_run_id is not None else {}),
            )
            result["failed"] += 1
    if (
        result["claimed"] > 0
        and result["claimed"] % guard_interval != 0
        and budget.available()
    ):
        budget.apply_database_timeouts(conn)
        guard_check(conn, require_external)
    return result


def pending_cohorts(
    conn, limit: int, league_id: int | None = None, season_id: int | None = None,
) -> list[tuple[int, int]]:
    if (league_id is None) != (season_id is None):
        raise ValueError("cohort scope requires league and season together")
    scope_clause = (
        "and (cursor.league_id,cursor.season_id)=(%s,%s)"
        if league_id is not None else ""
    )
    params = (league_id, season_id, limit) if league_id is not None else (limit,)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            select cursor.league_id, cursor.season_id
            from leaderboard_shadow_v1.cohort_change_cursors cursor
            left join leaderboard_shadow_v1.cohort_publications publication
              using (league_id, season_id)
            where cursor.committed_version > coalesce(publication.published_cursor, 0)
              {scope_clause}
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
            params,
        )
        return [(int(row[0]), int(row[1])) for row in cur.fetchall()]


def ensure_generation(
    conn, league_id: int, season_id: int, *, max_materialized_jobs: int | None = None,
) -> int | None:
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "select leaderboard_shadow_v1.begin_generation(%s,%s)",
                (league_id, season_id),
            )
            generation_id = cur.fetchone()[0]
            if generation_id is not None:
                if max_materialized_jobs is not None:
                    if max_materialized_jobs < 1:
                        raise RuntimeError("generation job bound must be positive")
                    # begin_generation holds the cohort cursor lock until this
                    # transaction ends. Count only max+1 distinct fixtures from
                    # its fixed cursor range before materialize_generation can
                    # insert the entire manifest. A large existing generation
                    # is refused too. Raising rolls back a newly begun one.
                    cur.execute(
                        """
                        select generation.status, generation.expected_job_count,
                               (select count(*) from (
                                  select distinct event.fixture_id
                                  from leaderboard_shadow_v1.fixture_change_events event
                                  where event.league_id=generation.league_id
                                    and event.season_id=generation.season_id
                                    and event.cohort_version>generation.from_cursor
                                    and event.cohort_version<=generation.through_cursor
                                  limit %s
                               ) bounded) as distinct_jobs
                        from leaderboard_shadow_v1.cohort_pending_publications generation
                        where generation.generation_id=%s
                        """,
                        (max_materialized_jobs + 1, generation_id),
                    )
                    status, expected_jobs, distinct_jobs = cur.fetchone()
                    if (status in ("ready", "processing", "published")
                            and (expected_jobs is None
                                 or expected_jobs > max_materialized_jobs)):
                        raise RuntimeError("generation exceeds finite trial job bound")
                    if distinct_jobs > max_materialized_jobs:
                        raise RuntimeError("generation exceeds finite trial job bound")
                cur.execute(
                    "select leaderboard_shadow_v1.materialize_generation(%s)",
                    (generation_id,),
                )
                materialized_jobs = cur.fetchone()[0]
                if (max_materialized_jobs is not None
                        and materialized_jobs > max_materialized_jobs):
                    raise RuntimeError("generation exceeded finite trial job bound")
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
                    observed_hash = (
                        snapshot["source_hash"]
                        if snapshot else job["expected_source_hash"]
                    )
                else:
                    if not snapshot:
                        raise RuntimeError("canonical fixture source is missing")
                    facts = snapshot["facts"]
                    observed_hash = snapshot["source_hash"]
                if observed_hash != job["expected_source_hash"]:
                    # Historical publication keeps the automatic source
                    # triggers disabled. If authoritative source data changed
                    # after this immutable generation was materialized, record
                    # the consistent snapshot before locking the generation
                    # job. This preserves the cursor-first locking contract and
                    # lets the replacement generation use the newer version.
                    cur.execute(
                        "select leaderboard_shadow_v1.capture_fixture_source_changes(%s::bigint[])",
                        ([job["fixture_id"]],),
                    )
                    cur.fetchone()
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
                return bool(next(iter(cur.fetchone().values())))
    except Exception as error:
        apply_recovery_timeouts(conn)
        mark_failed_statement_interval(conn)
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
            select generation.status,
                   count(*) filter (where job.status in ('pending','processing')),
                   count(*) filter (where job.status='failed')
            from leaderboard_shadow_v1.cohort_pending_publications generation
            left join leaderboard_shadow_v1.cohort_generation_jobs job
              on job.generation_id=generation.generation_id
            where generation.generation_id=%s
            group by generation.status
            """,
            (generation_id,),
        )
        generation_status, outstanding, failed = cur.fetchone()
    # Source drift deliberately supersedes the immutable generation. Its
    # failed staging job is evidence that the stale snapshot was not exposed,
    # not an operational failure. The next bounded run creates a replacement
    # generation from the cohort's newer committed cursor.
    if generation_status == "superseded":
        return False
    if generation_status == "published":
        if failed or outstanding:
            raise RuntimeError(
                f"published generation {generation_id} has incomplete jobs"
            )
        # publish_generation() also returns true for an already-published
        # generation. Do not count that as a publication by this worker run.
        return False
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
    guard_check: Callable[[Any, bool], dict[str, Any]] | None = None,
    progress: dict[str, int] | None = None,
    max_materialized_jobs: int | None = None,
    league_id: int | None = None, season_id: int | None = None,
) -> dict[str, int]:
    if (league_id is None) != (season_id is None):
        raise ValueError("generation scope requires league and season together")
    result = progress if progress is not None else {"cohorts": 0, "jobs": 0, "published": 0}
    guard_check = guard_check or enforce_resource_guard
    cohorts = (
        pending_cohorts(conn, cohort_limit, league_id, season_id)
        if league_id is not None else pending_cohorts(conn, cohort_limit)
    )
    for cohort_league_id, cohort_season_id in cohorts:
        if not budget.available():
            break
        budget.apply_database_timeouts(conn)
        guard_check(conn, require_external)
        generation_id = (
            ensure_generation(conn, cohort_league_id, cohort_season_id)
            if max_materialized_jobs is None else
            ensure_generation(
                conn, cohort_league_id, cohort_season_id,
                max_materialized_jobs=min(
                    max_materialized_jobs,
                    budget.fixture_limit - budget.processed,
                ),
            )
        )
        if generation_id is None:
            continue
        result["cohorts"] += 1
        for _ in range(job_limit):
            if not budget.available():
                break
            budget.apply_database_timeouts(conn)
            guard_check(conn, require_external)
            if not process_generation_job(conn, generation_id, worker_id):
                break
            result["jobs"] += 1
            budget.consume()
        if not budget.available():
            break
        budget.apply_database_timeouts(conn)
        guard_check(conn, require_external)
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


TRIAL_STATEMENTS = {
    "isolated": {
        "begin": "select admission_proto.begin_bound_worker_trial_for_test(%s,%s,%s) as decision",
        "guard": "select admission_proto.check_bound_worker_trial_for_test(%s,%s,%s,%s) as evidence",
        "finish": "select admission_proto.finish_bound_worker_trial_with_counts_for_test(%s,%s,%s,%s,false,%s,%s,%s) as outcome",
        "fail": "select admission_proto.finish_bound_worker_trial_with_counts_for_test(%s,%s,%s,%s,true,%s,%s,%s)",
        "reservation": "select reservation_id, owner, worker_run_id, settled_at from admission_proto.reservations where policy_id=%s",
        "recover": "select admission_proto.recover_bound_worker_trial_for_test(%s,%s,%s,%s) as outcome",
    },
    "production": {
        "transition": "select leaderboard_trial_v1.create_finite_trial_transition(%s,%s,%s,%s) as transition",
        "begin": "select leaderboard_trial_v1.begin_bound_worker_trial(%s,%s,%s) as decision",
        "guard": "select leaderboard_trial_v1.check_bound_worker_trial(%s,%s,%s,%s) as evidence",
        "finish": "select leaderboard_trial_v1.finish_bound_worker_trial_with_counts(%s,%s,%s,%s,false,%s,%s,%s) as outcome",
        "fail": "select leaderboard_trial_v1.finish_bound_worker_trial_with_counts(%s,%s,%s,%s,true,%s,%s,%s)",
        "reservation": "select reservation_id, owner, worker_run_id, settled_at from leaderboard_trial_v1.reservations where policy_id=%s",
        "recover": "select leaderboard_trial_v1.recover_bound_worker_trial(%s,%s,%s,%s) as outcome",
    },
}


def run_finite_trial(
    conn, args, worker_id: str, *, production: bool,
    include_generations: bool = False,
) -> dict[str, Any]:
    """Run the same bounded capture/settlement flow against a fixed SQL contract."""
    if production and include_generations:
        raise RuntimeError("full-workload trial has no production entrypoint")
    if production:
        validate_production_trial_connection(conn, args)
    elif not validate_activation_override(conn, args.isolated_test_override):
        raise RuntimeError("trial-once requires the disposable database override")
    statements = TRIAL_STATEMENTS["production" if production else "isolated"]
    if not args.trial_policy_id:
        raise RuntimeError("trial-once requires --trial-policy-id")
    if not (1 <= args.fixture_limit <= 10 and 1 <= args.capture_limit <= 10):
        raise RuntimeError("trial-once is limited to ten fixture captures")
    if include_generations:
        if (args.cohort_limit != 1 or not 1 <= args.job_limit <= 10
                or args.capture_limit > args.fixture_limit):
            raise RuntimeError("isolated full-workload trial requires one cohort and at most ten jobs")
    elif args.cohort_limit != 0 or args.job_limit != 0:
        raise RuntimeError("trial-once cannot generate or publish leaderboards")
    if not (0 <= args.trial_test_pg_sleep_after_admission <= 120
            and 0 <= args.trial_test_pg_sleep_after_capture <= 120):
        raise RuntimeError("isolated trial sleep must be within 120 seconds")
    budget = RunBudget.create(args.fixture_limit, args.max_seconds)
    apply_preflight_timeouts(conn)
    temp_file_limit_enforced = configure_temp_file_limit_if_permitted(conn)
    budget.apply_database_timeouts(conn)
    with conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if production:
                note = os.environ.get("LEADERBOARD_FINITE_TRIAL_OPERATOR_NOTE", "")
                if not 1 <= len(note) <= 2000:
                    raise RuntimeError("production finite trial operator note is missing or invalid")
                cur.execute(
                    statements["transition"],
                    (args.trial_policy_id, RELEASE_SHA, 5368709120, note),
                )
                cur.fetchone()
            cur.execute(
                statements["begin"],
                (args.trial_policy_id, worker_id, RELEASE_SHA),
            )
            decision = cur.fetchone()["decision"]
            if production and decision.get("admitted") is not True:
                raise RuntimeError(f"finite trial admission refused: {decision.get('reason')}")
    if decision.get("admitted") is not True:
        raise RuntimeError(f"finite trial admission refused: {decision.get('reason')}")
    fence = int(decision["fence"])
    run_id = int(decision["run_id"])
    print(json.dumps({
        "event": "finite_trial_admitted" if production else "isolated_finite_trial_admitted",
        "policy_id": args.trial_policy_id,
        "owner": worker_id, "fence": fence, "run_id": run_id,
    }, sort_keys=True), flush=True)

    def check_trial_guard(connection, _require_external: bool) -> dict[str, Any]:
        with connection:
            with connection.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    statements["guard"],
                    (args.trial_policy_id, worker_id, fence, run_id),
                )
                evidence = cur.fetchone()["evidence"]
        if evidence.get("ok") is not True:
            raise RuntimeError(f"finite trial guard stopped work: {evidence.get('reason')}")
        return evidence

    closed = False
    capture = {"claimed": 0, "acknowledged": 0, "redirtied": 0, "failed": 0}
    generations = {"cohorts": 0, "jobs": 0, "published": 0}
    try:
        if args.trial_test_pg_sleep_after_admission:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "select pg_sleep(%s)",
                        (args.trial_test_pg_sleep_after_admission,),
                    )
        check_trial_guard(conn, False)
        budget.apply_database_timeouts(conn)
        eligibility_count = refresh_eligibility_if_due(conn)
        check_trial_guard(conn, False)
        budget.apply_database_timeouts(conn)
        capture = process_capture_queue(
            conn, worker_id, args.capture_limit, budget, False,
            league_id=args.league_id, season_id=args.season_id,
            guard_check=check_trial_guard,
            progress=capture, audit_run_id=run_id,
        )
        if include_generations:
            if capture["failed"]:
                raise RuntimeError("finite trial capture failed")
            check_trial_guard(conn, False)
            budget.apply_database_timeouts(conn)
            remaining_jobs = min(args.job_limit, budget.fixture_limit - budget.processed)
            if remaining_jobs > 0:
                process_generations(
                    conn, worker_id, args.cohort_limit, remaining_jobs, budget, False,
                    guard_check=check_trial_guard, progress=generations,
                    max_materialized_jobs=remaining_jobs,
                )
            if generations["published"] != 1:
                raise RuntimeError("full-workload trial did not publish one bounded generation")
        if args.trial_test_pg_sleep_after_capture:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "select pg_sleep(%s)",
                        (args.trial_test_pg_sleep_after_capture,),
                    )
        # A full ten-claim budget skips process_capture_queue's final periodic
        # check. Always checkpoint before closing or claiming success.
        check_trial_guard(conn, False)
        if capture["failed"]:
            raise RuntimeError("finite trial capture failed")
        apply_recovery_timeouts(conn)
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    statements["finish"],
                    (
                        args.trial_policy_id, worker_id, fence, run_id,
                        capture["claimed"] + generations["jobs"],
                        capture["acknowledged"] + generations["jobs"],
                        capture["failed"],
                    ),
                )
                outcome = cur.fetchone()["outcome"]
        closed = True
        if outcome.get("trial_succeeded") is not True:
            raise RuntimeError("finite trial settled but failed its final safety check")
        return {
            "policy_id": args.trial_policy_id, "owner": worker_id,
            "fence": fence, "run_id": run_id,
            "capture": capture, "eligibility_count": eligibility_count,
            **({"generations": generations} if include_generations else {}),
            "elapsed_ms": round((time.monotonic() - budget.started_at) * 1000, 2),
            "temp_file_limit_enforced": temp_file_limit_enforced,
            "outcome": outcome,
        }
    except Exception:
        if not closed:
            apply_recovery_timeouts(conn)
            mark_failed_statement_interval(conn)
            claimed = capture["claimed"] + generations["jobs"]
            succeeded = capture["acknowledged"] + generations["jobs"]
            failed = max(1, capture["failed"])
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        statements["fail"],
                        (
                            args.trial_policy_id, worker_id, fence, run_id,
                            claimed, succeeded, failed,
                        ),
                    )
                    cur.fetchone()
        raise


def recover_finite_trial(conn, args, *, production: bool) -> dict[str, Any]:
    """Settle a stopped trial while the supervisor holds both host locks."""
    if production:
        validate_production_trial_connection(conn, args)
    elif not validate_activation_override(conn, args.isolated_test_override):
        raise RuntimeError("trial-recover requires the disposable database override")
    statements = TRIAL_STATEMENTS["production" if production else "isolated"]
    if not args.trial_policy_id:
        raise RuntimeError("trial-recover requires --trial-policy-id")
    if not 0 <= args.trial_test_pg_sleep_before_recovery <= 120:
        raise RuntimeError("isolated recovery sleep must be within 120 seconds")
    if args.trial_test_pg_sleep_before_recovery:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "select pg_sleep(%s)",
                    (args.trial_test_pg_sleep_before_recovery,),
                )
    apply_preflight_timeouts(conn)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            statements["reservation"],
            (args.trial_policy_id,),
        )
        reservations = cur.fetchall()
    if len(reservations) > 1:
        raise RuntimeError("finite trial has multiple reservations")
    if not reservations:
        return {"state": "no_reservation", "policy_id": args.trial_policy_id}
    reservation = reservations[0]
    if reservation["settled_at"] is not None:
        return {"state": "already_settled", "policy_id": args.trial_policy_id}
    if reservation["worker_run_id"] is None:
        raise RuntimeError("finite trial has an unbound reservation")
    with conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                statements["recover"],
                (
                    args.trial_policy_id, reservation["owner"],
                    reservation["reservation_id"], reservation["worker_run_id"],
                ),
            )
            outcome = cur.fetchone()["outcome"]
    if outcome.get("resolution_state") != "settled" or outcome.get("settlement", {}).get("settled") is not True:
        raise RuntimeError(f"finite trial recovery remains unresolved: {outcome.get('reason') or outcome.get('resolution_state')}")
    return {"state": "recovered", "policy_id": args.trial_policy_id, "outcome": outcome}


def run_isolated_finite_trial(conn, args, worker_id: str) -> dict[str, Any]:
    return run_finite_trial(conn, args, worker_id, production=False)


def recover_isolated_finite_trial(conn, args) -> dict[str, Any]:
    return recover_finite_trial(conn, args, production=False)


ROLLING_STATEMENTS = {
    "begin": "select leaderboard_rolling_v1.begin_bound_worker(%s,%s,%s) as decision",
    "guard": "select leaderboard_rolling_v1.check_bound_worker(%s,%s,%s,%s) as evidence",
    "finish": "select leaderboard_rolling_v1.finish_bound_worker(%s,%s,%s,%s,%s,%s,%s,%s) as outcome",
    "recover": "select leaderboard_rolling_v1.recover_bound_worker(%s,%s,%s,%s) as outcome",
}


def run_rolling_worker(conn, args, worker_id: str) -> dict[str, Any]:
    """One supervised capture and publication run under a renewable reservation."""
    validate_production_trial_connection(conn, args)
    if not args.trial_policy_id:
        raise RuntimeError("rolling worker requires --trial-policy-id")
    if not (1 <= args.fixture_limit <= MAX_FIXTURES_PER_RUN
            and 0 <= args.capture_limit <= args.fixture_limit
            and 0 <= args.cohort_limit <= 8
            and 0 <= args.job_limit <= args.fixture_limit
            and (args.capture_limit or args.job_limit)):
        raise RuntimeError("rolling worker capture, cohort or job bound is invalid")
    if ((args.league_id is None) != (args.season_id is None)
            or (args.league_id is not None
                and (args.league_id <= 0 or args.season_id <= 0))):
        raise RuntimeError("rolling cohort scope requires positive league and season together")
    if args.production_trial_isolated_test and not isolated_test_database(conn):
        raise RuntimeError("rolling test path requires socket-only oddssearch_m3")
    if args.production_trial_isolated_test and args.trial_test_pg_sleep_after_capture:
        if not 0 < args.trial_test_pg_sleep_after_capture <= 120:
            raise RuntimeError("rolling post-capture test pause is outside its bound")
        with conn.cursor() as cur:
            cur.execute("""
                select current_database()='oddssearch_m3_rolling_worker'
                   and inet_server_addr() is null
                   and current_setting('data_directory') like '/tmp/oddssearch-m3-pg.%'
            """)
            if cur.fetchone()[0] is not True:
                raise RuntimeError("rolling post-capture pause requires exact disposable clone")
    budget = RunBudget.create(args.fixture_limit, args.max_seconds)
    apply_preflight_timeouts(conn)
    # The normal worker is paused, so it cannot upload the host's fresh disk
    # sample. Refresh it before admission; the installed function validates
    # both the sample age and current source identity without rearming debt.
    load_external_resource_evidence(conn)
    temp_file_limit_enforced = configure_temp_file_limit_if_permitted(conn)
    budget.apply_database_timeouts(conn)
    with conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                ROLLING_STATEMENTS["begin"],
                (args.trial_policy_id, worker_id, RELEASE_SHA),
            )
            decision = cur.fetchone()["decision"]
    if decision.get("admitted") is not True:
        raise RuntimeError(f"rolling admission refused: {decision.get('reason')}")
    fence, run_id = int(decision["fence"]), int(decision["run_id"])
    print(json.dumps({
        "event": "rolling_worker_admitted", "policy_id": args.trial_policy_id,
        "owner": worker_id, "fence": fence, "run_id": run_id,
    }, sort_keys=True), flush=True)

    def check_rolling_guard(connection, _require_external: bool) -> dict[str, Any]:
        with connection:
            with connection.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    ROLLING_STATEMENTS["guard"],
                    (args.trial_policy_id, worker_id, fence, run_id),
                )
                evidence = cur.fetchone()["evidence"]
        if evidence.get("ok") is not True:
            raise RuntimeError(f"rolling guard stopped work: {evidence.get('reason')}")
        return evidence

    capture = {"claimed": 0, "acknowledged": 0, "redirtied": 0, "failed": 0}
    generations = {"cohorts": 0, "jobs": 0, "published": 0}
    closed = False
    try:
        if args.production_trial_isolated_test and args.trial_test_pg_sleep_after_admission:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "select pg_sleep(%s)",
                        (args.trial_test_pg_sleep_after_admission,),
                    )
        check_rolling_guard(conn, False)
        budget.apply_database_timeouts(conn)
        eligibility_count = refresh_eligibility_if_due(conn)
        check_rolling_guard(conn, False)
        budget.apply_database_timeouts(conn)
        if args.capture_limit:
            process_capture_queue(
                conn, worker_id, args.capture_limit, budget, False,
                league_id=args.league_id, season_id=args.season_id,
                guard_check=check_rolling_guard, progress=capture,
                audit_run_id=run_id, guard_interval=1,
            )
        if args.production_trial_isolated_test and args.trial_test_pg_sleep_after_capture:
            if not capture["acknowledged"]:
                raise RuntimeError("rolling post-capture pause had no acknowledged capture")
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "select pg_sleep(%s)",
                        (args.trial_test_pg_sleep_after_capture,),
                    )
        if capture["failed"]:
            raise RuntimeError("rolling capture failed")
        remaining_jobs = min(
            args.job_limit, budget.fixture_limit - budget.processed
        )
        if args.cohort_limit and remaining_jobs and budget.available():
            check_rolling_guard(conn, False)
            budget.apply_database_timeouts(conn)
            process_generations(
                conn, worker_id, args.cohort_limit, remaining_jobs,
                budget, False, guard_check=check_rolling_guard,
                progress=generations, max_materialized_jobs=remaining_jobs,
                league_id=args.league_id, season_id=args.season_id,
            )
        check_rolling_guard(conn, False)
        apply_recovery_timeouts(conn)
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    ROLLING_STATEMENTS["finish"],
                    (args.trial_policy_id, worker_id, fence, run_id, False,
                     capture["claimed"] + generations["jobs"],
                     capture["acknowledged"] + generations["jobs"],
                     capture["failed"]),
                )
                outcome = cur.fetchone()["outcome"]
        closed = True
        if outcome.get("run_succeeded") is not True:
            raise RuntimeError("rolling worker closed but failed final safety check")
        return {
            "policy_id": args.trial_policy_id, "owner": worker_id,
            "fence": fence, "run_id": run_id, "capture": capture,
            "generations": generations, "eligibility_count": eligibility_count,
            "cohort_scope": (
                {"league_id": args.league_id, "season_id": args.season_id}
                if args.league_id is not None else None
            ),
            "elapsed_ms": round((time.monotonic() - budget.started_at) * 1000, 2),
            "temp_file_limit_enforced": temp_file_limit_enforced,
            "outcome": outcome,
        }
    except Exception:
        if not closed:
            apply_recovery_timeouts(conn)
            mark_failed_statement_interval(conn)
            with conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(
                        ROLLING_STATEMENTS["finish"],
                        (args.trial_policy_id, worker_id, fence, run_id, True,
                         capture["claimed"] + generations["jobs"],
                         capture["acknowledged"] + generations["jobs"],
                         max(1, capture["failed"])),
                    )
                    cur.fetchone()
        raise


def recover_rolling_worker(conn, args) -> dict[str, Any]:
    """Settle the single stopped run after its exact owner backend has drained."""
    validate_production_trial_connection(conn, args)
    if not args.trial_policy_id:
        raise RuntimeError("rolling recovery requires --trial-policy-id")
    apply_preflight_timeouts(conn)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "select reservation_id,owner,worker_run_id,closed_unsettled_at "
            "from leaderboard_rolling_v1.reservations "
            "where policy_id=%s and settled_at is null",
            (args.trial_policy_id,),
        )
        reservations = cur.fetchall()
    if not reservations:
        return {"state": "already_settled", "policy_id": args.trial_policy_id}
    if len(reservations) != 1:
        raise RuntimeError("rolling recovery found multiple unsettled reservations")
    reservation = reservations[0]
    if reservation["worker_run_id"] is None:
        raise RuntimeError("rolling recovery found an unbound reservation")
    if reservation["closed_unsettled_at"] is not None:
        raise RuntimeError("rolling recovery remains closed and unsettled")
    with conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                ROLLING_STATEMENTS["recover"],
                (args.trial_policy_id, reservation["owner"],
                 reservation["reservation_id"], reservation["worker_run_id"]),
            )
            outcome = cur.fetchone()["outcome"]
    if (outcome.get("resolution_state") != "settled"
            or outcome.get("settlement", {}).get("settled") is not True):
        raise RuntimeError(
            "rolling recovery remains unresolved: "
            + str(outcome.get("reason") or outcome.get("resolution_state"))
        )
    return {"state": "recovered", "policy_id": args.trial_policy_id,
            "outcome": outcome}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=(
        "run-once", "backfill", "backfill-all", "status",
        "trial-once", "trial-full-once", "trial-recover",
        "finite-once", "finite-recover", "rolling-once", "rolling-recover",
    ))
    parser.add_argument("--isolated-test-override", action="store_true")
    parser.add_argument("--production-trial-isolated-test", action="store_true")
    parser.add_argument("--fixture-limit", type=int, default=MAX_FIXTURES_PER_RUN)
    parser.add_argument("--max-seconds", type=float, default=MAX_SECONDS_PER_RUN)
    parser.add_argument("--capture-limit", type=int, default=MAX_FIXTURES_PER_RUN)
    parser.add_argument("--cohort-limit", type=int, default=8)
    parser.add_argument("--job-limit", type=int, default=MAX_FIXTURES_PER_RUN)
    parser.add_argument("--league-id", type=int)
    parser.add_argument("--season-id", type=int)
    parser.add_argument("--batch-size", type=int, default=250)
    parser.add_argument("--trial-policy-id")
    parser.add_argument("--trial-test-pg-sleep-after-admission", type=float, default=0)
    parser.add_argument("--trial-test-pg-sleep-after-capture", type=float, default=0)
    parser.add_argument("--trial-test-pg-sleep-before-recovery", type=float, default=0)
    args = parser.parse_args()
    if args.production_trial_isolated_test and args.command not in {
        "finite-once", "finite-recover", "rolling-once", "rolling-recover"
    }:
        parser.error("production isolated test flag requires a bound worker command")
    if (args.league_id is None) != (args.season_id is None):
        parser.error("league-id and season-id must be supplied together")
    worker_id = f"{socket.gethostname()}:{os.getpid()}"

    conn = connect()
    try:
        if args.command == "status":
            print(json.dumps(status(conn), default=str, sort_keys=True))
            return 0
        if args.command == "trial-once":
            print(json.dumps(
                run_isolated_finite_trial(conn, args, worker_id),
                default=str, sort_keys=True,
            ))
            return 0
        if args.command == "trial-full-once":
            print(json.dumps(
                run_finite_trial(
                    conn, args, worker_id, production=False,
                    include_generations=True,
                ),
                default=str, sort_keys=True,
            ))
            return 0
        if args.command == "trial-recover":
            print(json.dumps(
                recover_isolated_finite_trial(conn, args),
                default=str, sort_keys=True,
            ))
            return 0
        if args.command == "finite-once":
            print(json.dumps(
                run_finite_trial(conn, args, worker_id, production=True),
                default=str, sort_keys=True,
            ))
            return 0
        if args.command == "finite-recover":
            print(json.dumps(
                recover_finite_trial(conn, args, production=True),
                default=str, sort_keys=True,
            ))
            return 0
        if args.command == "rolling-once":
            print(json.dumps(
                run_rolling_worker(conn, args, worker_id),
                default=str, sort_keys=True,
            ))
            return 0
        if args.command == "rolling-recover":
            print(json.dumps(
                recover_rolling_worker(conn, args),
                default=str, sort_keys=True,
            ))
            return 0
        budget = RunBudget.create(args.fixture_limit, args.max_seconds)
        allow_disabled = validate_activation_override(conn, args.isolated_test_override)
        require_external = not allow_disabled
        apply_preflight_timeouts(conn)
        recovery_evidence = recover_unfinished_worker_runs(conn)
        require_worker_enabled(conn, allow_disabled)
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
                "interrupted_run_recovery": recovery_evidence,
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
            resource_evidence = finish_worker_run_with_resource_check(
                conn, run_id, claimed, succeeded, failed, require_external,
            )
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
                "interrupted_run_recovery": recovery_evidence,
            }, sort_keys=True))
        except Exception as error:
            apply_recovery_timeouts(conn)
            mark_failed_statement_interval(conn)
            # budget.processed advances only after a capture or generation job
            # committed successfully. Preserve that authoritative progress in
            # run evidence even when a later guard or publication check fails.
            finish_worker_run(
                conn,
                run_id,
                budget.processed,
                budget.processed,
                1,
                str(error)[:2000],
            )
            raise
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
