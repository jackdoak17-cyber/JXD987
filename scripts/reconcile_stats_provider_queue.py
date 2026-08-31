#!/usr/bin/env python3
"""Batch-reconcile completed stats fixtures from the serving queue.

The normal post-match worker is intentionally bounded and safe for a 15-minute
runtime. This companion command uses the same snapshot, validation, and ledger
contracts but exports a batch at once and rebuilds each affected season only
once. It is resumable: the serving delivery ledger is the queue cursor.
"""

from __future__ import annotations

import argparse
import atexit
from concurrent.futures import ThreadPoolExecutor, as_completed
import fcntl
import json
import logging
import os
import sqlite3
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine
import psycopg2

from jxd import SportMonksClient
from scripts.postmatch_fixture_detail_delivery import (
    TRACKED_PLAYER_STAT_TYPES,
    activate_provider_snapshot,
    assess_provider_payload,
    backoff_time,
    candidate_target_fixture_ids,
    compare_snapshots,
    ensure_ledger,
    iso,
    normalized_provider_hash,
    persist_provider_snapshot,
    provider_payload_hash,
    ProviderDetailIncompleteError,
    ProviderFixtureUnavailableError,
    publish_delivery_status,
    revalidation_time,
    source_connection,
    source_snapshot,
    is_non_competitive_provider_assessment,
    stable_provider_sparse_assessment,
    store_provider_detail,
    target_fixture_metadata,
    target_snapshot,
    ledger_attempt_start,
    update_ledger,
    mark_provider_unavailable,
    clear_provider_unavailable_exclusion,
    recover_stale_running,
    repair_legacy_ledger,
)
from scripts.fixture_detail_export_policy import (
    FixtureExportPartition,
    partition_export_candidates,
    retryable_delivery_status_is_due,
)


LOG = logging.getLogger("reconcile_stats_provider_queue")

DETAIL_INCLUDE = ";".join(
    [
        "participants",
        "scores",
        "state",
        "statistics",
        "statistics.type",
        "lineups.details",
        "lineups.position",
        "lineups.detailedposition",
        "lineups.player",
    ]
)
MAX_BULK_FIXTURE_IDS = 50
DEFAULT_MAX_COHORTS = 8


@dataclass(frozen=True)
class ExportCommandResult:
    fixture_ids: tuple[int, ...]
    returncode: int
    successful: bool
    failure_class: str | None = None
    stage: str | None = None
    message: str | None = None

    @classmethod
    def success(cls, fixture_ids: list[int] | tuple[int, ...]) -> "ExportCommandResult":
        return cls(tuple(int(value) for value in fixture_ids), 0, True)

    @classmethod
    def failure(
        cls,
        *,
        fixture_ids: list[int] | tuple[int, ...],
        failure_class: str,
        stage: str,
        message: str,
        returncode: int = 1,
    ) -> "ExportCommandResult":
        return cls(tuple(int(value) for value in fixture_ids), returncode, False, failure_class, stage, message)


@dataclass(frozen=True)
class ExportBatchResult:
    outcomes: tuple[ExportCommandResult, ...]

    @property
    def returncode(self) -> int:
        return 0 if all(outcome.returncode == 0 for outcome in self.outcomes) else 1

    @property
    def exported_ids(self) -> tuple[int, ...]:
        return tuple(
            fixture_id
            for outcome in self.outcomes
            if outcome.successful
            for fixture_id in outcome.fixture_ids
        )

    @property
    def failed_ids(self) -> tuple[int, ...]:
        return tuple(
            fixture_id
            for outcome in self.outcomes
            if not outcome.successful
            for fixture_id in outcome.fixture_ids
        )


@dataclass(frozen=True)
class ExportCandidatesResult:
    exported_ids: tuple[int, ...]
    retryable_ids: tuple[int, ...]
    unsafe_ids: tuple[int, ...]
    partition: FixtureExportPartition
    exporter_result: ExportCommandResult | ExportBatchResult | None = None


def export_candidates(
    candidates: list[dict[str, Any]],
    exporter: Any,
) -> ExportCandidatesResult:
    """Partition candidates before invoking a fixture-detail exporter."""
    partition = partition_export_candidates(candidates)
    if not partition.exportable:
        return ExportCandidatesResult((), partition.unsafe_ids, partition.unsafe_ids, partition)
    result = exporter(list(partition.exportable_ids))
    successful = bool(getattr(result, "successful", False))
    if isinstance(result, ExportBatchResult):
        exported_ids = result.exported_ids
        retryable_ids = result.failed_ids + partition.unsafe_ids
    elif successful:
        exported_ids = tuple(int(value) for value in result.fixture_ids)
        missing_ids = tuple(fixture_id for fixture_id in partition.exportable_ids if fixture_id not in exported_ids)
        retryable_ids = missing_ids + partition.unsafe_ids
    else:
        exported_ids = ()
        retryable_ids = partition.exportable_ids + partition.unsafe_ids
    return ExportCandidatesResult(exported_ids, retryable_ids, partition.unsafe_ids, partition, result)


def record_export_failure(
    conn: sqlite3.Connection,
    target_url: str,
    candidate: dict[str, Any],
    result: ExportCommandResult,
    *,
    now: datetime | None = None,
    target_conn: Any | None = None,
) -> None:
    """Keep an attempted fixture retryable without changing its attempt number."""
    fixture_id = int(candidate["fixture_id"])
    attempt = int(candidate.get("attempt") or 1)
    current = now or datetime.now(timezone.utc)
    message = result.message or "fixture-detail export failed"
    update_ledger(
        conn,
        fixture_id,
        "export_failed",
        attempt,
        assessment=candidate.get("assessment"),
        source=candidate.get("source"),
        error=f"{result.failure_class or 'exporter'}:{result.stage or 'unknown'}: {message}"[-4000:],
        next_attempt_at=backoff_time(attempt, current),
    )
    publish_delivery_status(target_url, conn, fixture_id, target_conn=target_conn)


def cohort_limited_fixture_ids(
    fixture_ids: list[int],
    target_metadata: dict[int, tuple[int | None, int | None, str | None, int, int]],
    max_cohorts: int,
) -> list[int]:
    """Keep a batch within a bounded number of projection cohorts.

    Candidate ordering already clusters fixtures by league/season. This
    defensive cap keeps a larger throughput batch from accidentally turning
    into a projection fan-out if the target queue later contains a different
    mix. Unknown target metadata is left untouched so a metadata defect cannot
    silently change queue fairness.
    """
    if max_cohorts <= 0 or not fixture_ids:
        return fixture_ids
    if any(fixture_id not in target_metadata for fixture_id in fixture_ids):
        return fixture_ids

    seen: set[tuple[int | None, int | None]] = set()
    for index, fixture_id in enumerate(fixture_ids):
        meta = target_metadata[fixture_id]
        cohort = (meta[0], meta[1])
        if cohort not in seen and len(seen) >= max_cohorts:
            return fixture_ids[:index]
        seen.add(cohort)
    return fixture_ids


def fetch_provider_fixture(fixture_id: int) -> tuple[dict[str, Any] | None, Exception | None, int]:
    """Fetch one fixture as the bounded fallback for a failed bulk request."""
    try:
        client = SportMonksClient()
        payload = client.request(
            "GET",
            f"fixtures/{fixture_id}",
            params={"include": DETAIL_INCLUDE},
        )
        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, dict) or not data:
            return None, RuntimeError("SportMonks returned no fixture data"), 1
        if int(data.get("id") or 0) != fixture_id:
            return None, RuntimeError("SportMonks returned the wrong fixture ID"), 1
        return data, None, 1
    except Exception as exc:  # classified by the existing attempt/error policy
        return None, exc, 1


def fetch_provider_fixture_batch(
    fixture_ids: list[int],
) -> tuple[dict[int, dict[str, Any]], dict[int, Exception], int]:
    """Fetch a bounded fixture batch using SportMonks' multi-ID endpoint.

    The endpoint returns full fixture objects with the same nested statistics
    and lineup includes as the single-fixture endpoint.  Missing IDs remain
    per-fixture errors so the existing retry/unavailable policy is preserved.
    The returned third value is the number of HTTP requests consumed (one).
    """
    try:
        client = SportMonksClient()
        payload = client.request(
            "GET",
            f"fixtures/multi/{','.join(str(value) for value in fixture_ids)}",
            params={"include": DETAIL_INCLUDE},
        )
        raw_data = payload.get("data") if isinstance(payload, dict) else None
        rows = raw_data if isinstance(raw_data, list) else [raw_data] if isinstance(raw_data, dict) else []
        fetched = {
            int(row["id"]): row
            for row in rows
            if isinstance(row, dict) and row.get("id") is not None and int(row["id"]) in fixture_ids
        }
        errors = {
            fixture_id: RuntimeError("SportMonks multi-fixture response omitted requested fixture")
            for fixture_id in fixture_ids
            if fixture_id not in fetched
        }
        return fetched, errors, 1
    except Exception as exc:  # classified by the existing attempt/error policy
        return {}, {fixture_id: exc for fixture_id in fixture_ids}, 1


def fetch_provider_fixtures(
    fixture_ids: list[int],
    fetch_concurrency: int,
    bulk_size: int,
) -> tuple[dict[int, dict[str, Any]], dict[int, Exception], int]:
    """Fetch all requested fixtures in bounded multi-ID HTTP batches.

    Only independent HTTP requests may run concurrently.  All persistence and
    verification remains in the caller's serial writer section.
    """
    chunks = [fixture_ids[index : index + bulk_size] for index in range(0, len(fixture_ids), bulk_size)]
    fetched: dict[int, dict[str, Any]] = {}
    errors: dict[int, Exception] = {}
    http_calls = 0
    with ThreadPoolExecutor(max_workers=min(fetch_concurrency, len(chunks))) as executor:
        futures = {executor.submit(fetch_provider_fixture_batch, chunk): chunk for chunk in chunks}
        for future in as_completed(futures):
            chunk = futures[future]
            try:
                batch_fetched, batch_errors, calls = future.result()
            except Exception as exc:  # defensive: worker function classifies normal errors
                batch_fetched, batch_errors, calls = {}, {fixture_id: exc for fixture_id in chunk}, 1
            http_calls += calls

            # A failed or incomplete bulk response must not quarantine every
            # requested fixture. Retry only affected IDs through the proven
            # single-fixture endpoint, keeping the fallback bounded by the
            # same worker concurrency.
            fallback_ids = sorted(batch_errors)
            if fallback_ids:
                with ThreadPoolExecutor(max_workers=min(fetch_concurrency, len(fallback_ids))) as fallback_executor:
                    fallback_futures = {
                        fallback_executor.submit(fetch_provider_fixture, fixture_id): fixture_id
                        for fixture_id in fallback_ids
                    }
                    for fallback_future in as_completed(fallback_futures):
                        fixture_id = fallback_futures[fallback_future]
                        data, error, fallback_calls = fallback_future.result()
                        http_calls += fallback_calls
                        if error is None and data is not None:
                            batch_fetched[fixture_id] = data
                            batch_errors.pop(fixture_id, None)
                        else:
                            batch_errors[fixture_id] = error or RuntimeError(
                                "SportMonks single-fixture fallback returned no data"
                            )
            fetched.update(batch_fetched)
            errors.update(batch_errors)
    return fetched, errors, http_calls


def acquire_process_lock() -> int | None:
    """Prevent concurrent workers from writing the shared SQLite spool."""
    # The VPS supervisor acquires the canonical lock around this worker so it
    # can enforce a bounded lease and hand the lock back to settlement before
    # the next tick. The inherited descriptor remains open in this process;
    # do not acquire a second flock on the same path.
    if os.environ.get("STATS_RECONCILE_LOCK_HELD") == "1":
        LOG.info("Using canonical SQLite spool lock held by the VPS supervisor")
        return 0
    # All production writers use the same lock because they share the SQLite
    # spool. Keep an override for isolated/manual runs, but make the
    # production default identical to the VPS wrappers.
    path = os.environ.get("STATS_RECONCILE_LOCK_PATH", "/var/lock/odds-sync.lock")
    fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(fd)
        return None
    os.ftruncate(fd, 0)
    os.write(fd, f"pid={os.getpid()}\n".encode())
    atexit.register(os.close, fd)
    return fd


def parse_csv_ints(value: str | None) -> list[int]:
    return [int(token.strip()) for token in (value or "").split(",") if token.strip()]


def export_batch(
    fixture_ids: list[int],
    leagues: list[int],
    report_path: str,
    snapshot_ids: dict[int, int] | None = None,
) -> ExportBatchResult:
    """Publish each safe fixture through an atomic target transaction."""
    outcomes: list[ExportCommandResult] = []
    snapshot_ids = snapshot_ids or {}
    for fixture_id in fixture_ids:
        command = [
            sys.executable,
            str(Path(__file__).with_name("export_to_supabase.py")),
            "--strict",
            "--leagues",
            ",".join(str(value) for value in leagues),
            "--fixture-ids",
            str(fixture_id),
            "--keep-all-seasons",
            "--protect-empty-detail",
            "--require-detail",
            "--atomic-fixture-detail",
            "--skip-odds-snapshots",
            "--skip-odds-outcomes",
            "--skip-prune",
            "--report-json",
            report_path,
        ]
        snapshot_id = snapshot_ids.get(int(fixture_id))
        if snapshot_id is not None:
            command.extend(["--provider-snapshot-id", str(snapshot_id)])
        completed = subprocess.run(command, text=True, capture_output=True, check=False)
        if completed.returncode == 0:
            outcomes.append(ExportCommandResult.success([fixture_id]))
        else:
            outcomes.append(
                ExportCommandResult.failure(
                    fixture_ids=[fixture_id],
                    failure_class="exporter",
                    stage="fixture_detail_publish",
                    message=(completed.stderr or completed.stdout or "fixture-detail export failed")[-4000:],
                    returncode=completed.returncode,
                )
            )
    return ExportBatchResult(tuple(outcomes))


def refresh_seasons(
    target_url: str,
    seasons: set[tuple[int, int]],
    eligible_fixture_ids: set[int] | None = None,
    target_conn: Any | None = None,
) -> dict[str, int]:
    rows: dict[str, int] = {}
    owns_target_conn = target_conn is None
    target = target_conn or psycopg2.connect(target_url, connect_timeout=20)
    try:
        with target.cursor() as cur:
            for league_id, season_id in sorted(seasons):
                cur.execute(
                    "select public.refresh_player_stats_season_eligible(%s, %s, null, %s)",
                    (league_id, season_id, sorted(eligible_fixture_ids) if eligible_fixture_ids is not None else None),
                )
                rows[f"{league_id}:{season_id}"] = int(cur.fetchone()[0] or 0)
        target.commit()
    except Exception:
        target.rollback()
        raise
    finally:
        if owns_target_conn:
            target.close()
    return rows


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--leagues", required=True)
    parser.add_argument("--season-ids", default=None)
    parser.add_argument("--batch-size", type=int, default=25)
    parser.add_argument(
        "--max-cohorts",
        type=int,
        default=None,
        help=f"maximum league-season projection cohorts per batch (default: STATS_RECONCILE_MAX_COHORTS or {DEFAULT_MAX_COHORTS})",
    )
    parser.add_argument(
        "--fetch-concurrency",
        type=int,
        default=None,
        help="bounded concurrent SportMonks fetches (default: STATS_RECONCILE_FETCH_CONCURRENCY or 4)",
    )
    parser.add_argument(
        "--bulk-size",
        type=int,
        default=None,
        help="fixture IDs per SportMonks multi-fixture request (default: STATS_RECONCILE_BULK_SIZE or 50)",
    )
    parser.add_argument("--max-batches", type=int, default=0, help="0 means drain the queue.")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--report-json", default="/tmp/reconcile_stats_provider_queue.json")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    if acquire_process_lock() is None:
        LOG.info("Another stats reconciliation worker owns the shared SQLite spool; exiting")
        return 0
    leagues = parse_csv_ints(args.leagues)
    season_ids = parse_csv_ints(args.season_ids)
    if not leagues:
        raise SystemExit("At least one league is required")

    configured_fetch_concurrency = args.fetch_concurrency
    if configured_fetch_concurrency is None:
        configured_fetch_concurrency = int(os.environ.get("STATS_RECONCILE_FETCH_CONCURRENCY", "4"))
    fetch_concurrency = max(1, min(configured_fetch_concurrency, 8))
    configured_bulk_size = args.bulk_size
    if configured_bulk_size is None:
        configured_bulk_size = int(os.environ.get("STATS_RECONCILE_BULK_SIZE", str(MAX_BULK_FIXTURE_IDS)))
    bulk_size = max(1, min(configured_bulk_size, MAX_BULK_FIXTURE_IDS))
    configured_max_cohorts = args.max_cohorts
    if configured_max_cohorts is None:
        configured_max_cohorts = int(
            os.environ.get("STATS_RECONCILE_MAX_COHORTS", str(DEFAULT_MAX_COHORTS))
        )
    max_cohorts = max(0, configured_max_cohorts)

    target_url = os.environ.get("SUPABASE_DB_URL_SESSION") or os.environ.get("SUPABASE_DB_URL")
    if not target_url:
        raise SystemExit("SUPABASE_DB_URL_SESSION or SUPABASE_DB_URL is required")

    conn = source_connection()
    ensure_ledger(conn)
    recovered_running = recover_stale_running(conn)
    if recovered_running:
        LOG.warning("Requeued %s stale running delivery rows after a prior worker handoff", recovered_running)
    repaired_ledger = repair_legacy_ledger(conn)
    repaired_ids = [
        *repaired_ledger["legacy_pending"],
        *repaired_ledger["handoff_failed"],
    ]
    if repaired_ids:
        LOG.warning(
            "Repaired %s legacy provider classifications (%s opaque pending, %s handoff failures)",
            len(repaired_ids),
            len(repaired_ledger["legacy_pending"]),
            len(repaired_ledger["handoff_failed"]),
        )
    source_path = str(Path(os.environ.get("JXD_DB_PATH", "data/jxd.sqlite")).resolve())
    engine = create_engine(f"sqlite:///{source_path}", future=True)
    # This client is retained for the sequential source-store path.  Provider
    # fetches use fresh clients in fetch_provider_fixture so bounded parallel
    # requests cannot race mutable retry/stat state.
    client = SportMonksClient()
    report: dict[str, Any] = {
        "summary": {
            "status": "running",
            "batches": 0,
            "fixtures_selected": 0,
            "fixtures_accepted": 0,
            "failed": 0,
            "provider_pending": 0,
        },
        "release_id": os.environ.get("RUNTIME_RELEASE_ID", "local"),
        "leagues": leagues,
        "season_ids": season_ids,
        "batches": 0,
        "fixtures_selected": 0,
        "fixtures_accepted": 0,
        "provider_pending": [],
        "provider_unavailable": [],
        "excluded": [],
        "provider_sparse": [],
        "failed": [],
        "projection_rows": {},
        "provider_calls": 0,
        "provider_fixture_attempts": 0,
        "provider_http_calls": 0,
        "fetch_concurrency": fetch_concurrency,
        "bulk_size": bulk_size,
        "max_cohorts": max_cohorts,
        "stage_seconds": {},
    }
    target_conn: Any | None = None

    def shared_target_connection() -> Any:
        nonlocal target_conn
        if target_conn is None or target_conn.closed:
            target_conn = psycopg2.connect(target_url, connect_timeout=20)
        return target_conn

    # Mirror the self-healing ledger transition immediately.  This uses one
    # target connection for the whole repair so the serving contract and the
    # authoritative source ledger cannot diverge merely because a worker
    # restarted before selecting the repaired fixture.
    for fixture_id in repaired_ids:
        publish_delivery_status(target_url, conn, fixture_id, target_conn=shared_target_connection())

    try:
        while True:
            if args.max_batches and report["batches"] >= args.max_batches:
                break
            fixture_ids = candidate_target_fixture_ids(
                target_url,
                leagues,
                max(args.batch_size, 1),
                args.force,
                season_ids or None,
            )
            if not fixture_ids:
                break
            target_metadata = target_fixture_metadata(target_url, fixture_ids)
            original_fixture_count = len(fixture_ids)
            fixture_ids = cohort_limited_fixture_ids(fixture_ids, target_metadata, max_cohorts)
            if len(fixture_ids) < original_fixture_count:
                LOG.info(
                    "Capped batch from %s to %s fixtures to stay within %s projection cohorts",
                    original_fixture_count,
                    len(fixture_ids),
                    max_cohorts,
                )
                target_metadata = {fixture_id: target_metadata[fixture_id] for fixture_id in fixture_ids}
            report["batches"] += 1
            report["fixtures_selected"] += len(fixture_ids)
            LOG.info("Processing stats reconciliation batch %s: %s fixtures", report["batches"], len(fixture_ids))

            # Mark the complete batch as running before fetching so a worker
            # handoff cannot leave a subset looking untouched.  The source
            # ledger is still updated serially because it is SQLite-backed.
            contexts: dict[int, dict[str, Any]] = {}
            for fixture_id in fixture_ids:
                source_meta_row = conn.execute(
                    "select league_id,season_id,starting_at from fixtures where id=?",
                    (fixture_id,),
                ).fetchone()
                target_meta = target_metadata.get(fixture_id)
                meta = source_meta_row or (target_meta[:3] if target_meta else None)
                prior = conn.execute(
                    "select provider_team_stat_count,provider_player_stat_count,last_normalized_hash,stable_fetch_count from fixture_detail_deliveries where fixture_id = ?",
                    (fixture_id,),
                ).fetchone()
                prior_team_count = int(prior[0] or 0) if prior else 0
                prior_player_count = int(prior[1] or 0) if prior else 0
                prior_hash = str(prior[2]) if prior and prior[2] else None
                prior_stable = int(prior[3] or 0) if prior else 0
                if target_meta:
                    prior_team_count = max(prior_team_count, target_meta[3])
                    prior_player_count = max(prior_player_count, target_meta[4])
                attempt = ledger_attempt_start(
                    conn,
                    fixture_id,
                    datetime.now(timezone.utc),
                    int(meta[0]) if meta and meta[0] is not None else None,
                    int(meta[1]) if meta and meta[1] is not None else None,
                )
                contexts[fixture_id] = {
                    "meta": meta,
                    "attempt": attempt,
                    "prior_team_count": prior_team_count,
                    "prior_player_count": prior_player_count,
                    "prior_hash": prior_hash,
                    "prior_stable": prior_stable,
                }

            stage_started = time.perf_counter()
            fetched, fetch_errors, http_calls = fetch_provider_fixtures(
                fixture_ids,
                fetch_concurrency,
                bulk_size,
            )
            report["stage_seconds"]["provider_fetch"] = round(time.perf_counter() - stage_started, 3)
            report["provider_calls"] += len(fixture_ids)
            report["provider_fixture_attempts"] += len(fixture_ids)
            report["provider_http_calls"] += http_calls

            accepted: list[dict[str, Any]] = []
            excluded_seasons: set[tuple[int, int]] = set()
            for fixture_id in fixture_ids:
                context = contexts[fixture_id]
                meta = context["meta"]
                attempt = context["attempt"]
                prior_team_count = context["prior_team_count"]
                prior_player_count = context["prior_player_count"]
                prior_hash = context["prior_hash"]
                prior_stable = context["prior_stable"]
                assessment = None
                payload_hash = None
                normalized_hash = None
                stable_count = prior_stable
                try:
                    if fixture_id in fetch_errors:
                        raise fetch_errors[fixture_id]
                    data = fetched.get(fixture_id)
                    if not isinstance(data, dict) or not data:
                        message = "SportMonks returned no fixture data"
                        if attempt >= 3:
                            raise ProviderFixtureUnavailableError(message)
                        raise RuntimeError(message)
                    assessment = assess_provider_payload(data)
                    payload_hash = provider_payload_hash(data)
                    normalized_hash = normalized_provider_hash(data)
                    snapshot_id = persist_provider_snapshot(
                        target_url,
                        fixture_id,
                        int(meta[0]) if meta and meta[0] is not None else None,
                        int(meta[1]) if meta and meta[1] is not None else None,
                        data,
                        assessment,
                        payload_hash,
                        normalized_hash,
                        target_conn=shared_target_connection(),
                    )
                    stable_count = prior_stable + 1 if prior_hash == normalized_hash else 1
                    if is_non_competitive_provider_assessment(assessment):
                        reason = (
                            f"SportMonks marks fixture status {assessment.fixture_status}; "
                            "fixture is excluded from stats as non-competitive"
                        )
                        review_at = mark_provider_unavailable(
                            target_url,
                            conn,
                            fixture_id,
                            attempt,
                            reason,
                            exclusion_reason=reason,
                            evidence={"provider_status": assessment.fixture_status, "assessment": asdict(assessment)},
                            assessment=assessment,
                            target_conn=shared_target_connection(),
                            reason_code="excluded",
                        )
                        if meta:
                            excluded_seasons.add((int(meta[0]), int(meta[1])))
                        report["excluded"].append({"fixture_id": fixture_id, "next_review_at": review_at, "reason": reason})
                        continue
                    terminal_sparse = stable_provider_sparse_assessment(assessment, stable_count)
                    if terminal_sparse is not None:
                        assessment = terminal_sparse
                    elif assessment.status == "provider_sparse":
                        assessment = replace(
                            assessment,
                            status="provider_pending",
                            error="optional provider stat types are absent; awaiting stable confirmation",
                        )
                    if assessment.status == "provider_pending":
                        next_at = backoff_time(attempt, datetime.now(timezone.utc))
                        update_ledger(conn, fixture_id, assessment.status, attempt, assessment, error=assessment.error, next_attempt_at=next_at, payload_hash=payload_hash, normalized_hash=normalized_hash, stable_fetch_count=stable_count)
                        publish_delivery_status(target_url, conn, fixture_id, target_conn=shared_target_connection())
                        report["provider_pending"].append({"fixture_id": fixture_id, "reason": assessment.error})
                        continue
                    candidate_shrank = (
                        (prior_team_count > 0 and assessment.team_stat_count < prior_team_count)
                        or (prior_player_count > 0 and assessment.player_stat_count < prior_player_count)
                    )
                    if not args.force and candidate_shrank and prior_hash != normalized_hash and stable_count < 2:
                        next_at = backoff_time(attempt, datetime.now(timezone.utc))
                        message = (
                            "provider detail collection shrank "
                            f"(team_stats {prior_team_count}->{assessment.team_stat_count}, "
                            f"player_stats {prior_player_count}->{assessment.player_stat_count}); "
                            "awaiting one identical confirmation fetch"
                        )
                        update_ledger(conn, fixture_id, "provider_pending", attempt, assessment, error=message, next_attempt_at=next_at, payload_hash=payload_hash, normalized_hash=normalized_hash, stable_fetch_count=stable_count)
                        publish_delivery_status(target_url, conn, fixture_id, target_conn=shared_target_connection())
                        report["provider_pending"].append({"fixture_id": fixture_id, "reason": message})
                        continue
                    source = store_provider_detail(engine, client, fixture_id, data, assessment)
                    clear_provider_unavailable_exclusion(
                        target_url,
                        fixture_id,
                        target_conn=shared_target_connection(),
                    )
                    if not meta:
                        meta = conn.execute("select league_id,season_id,starting_at from fixtures where id=?", (fixture_id,)).fetchone()
                    accepted.append({"fixture_id": fixture_id, "attempt": attempt, "assessment": assessment, "source": source, "snapshot_id": snapshot_id, "payload_hash": payload_hash, "normalized_hash": normalized_hash, "stable_count": stable_count, "meta": meta})
                except ProviderFixtureUnavailableError as exc:
                    message = str(exc)[-4000:]
                    review_at = mark_provider_unavailable(
                        target_url,
                        conn,
                        fixture_id,
                        attempt,
                        message,
                        target_conn=shared_target_connection(),
                    )
                    report["provider_unavailable"].append({"fixture_id": fixture_id, "next_review_at": review_at, "reason": message})
                except ProviderDetailIncompleteError as exc:
                    message = str(exc)[-4000:]
                    next_at = backoff_time(attempt, datetime.now(timezone.utc))
                    update_ledger(conn, fixture_id, "provider_pending", attempt, assessment, error=message, next_attempt_at=next_at, payload_hash=payload_hash, normalized_hash=normalized_hash, stable_fetch_count=stable_count)
                    publish_delivery_status(target_url, conn, fixture_id, target_conn=shared_target_connection())
                    report["provider_pending"].append({"fixture_id": fixture_id, "reason": message})
                except Exception as exc:
                    message = str(exc)[-4000:]
                    update_ledger(conn, fixture_id, "failed", attempt, error=message, next_attempt_at=backoff_time(attempt, datetime.now(timezone.utc)))
                    publish_delivery_status(target_url, conn, fixture_id, target_conn=shared_target_connection())
                    report["failed"].append({"fixture_id": fixture_id, "stage": "fetch_or_store", "error": message})

            if excluded_seasons:
                stage_started = time.perf_counter()
                refreshed = refresh_seasons(target_url, excluded_seasons, target_conn=shared_target_connection())
                report["stage_seconds"]["excluded_projection_refresh"] = round(time.perf_counter() - stage_started, 3)
                for key, count in refreshed.items():
                    report["projection_rows"][key] = report["projection_rows"].get(key, 0) + count

            if not accepted:
                continue
            stage_started = time.perf_counter()
            export_result = export_candidates(
                accepted,
                lambda fixture_ids: export_batch(
                    fixture_ids,
                    leagues,
                    "/tmp/reconcile_stats_provider_export.json",
                    {
                        int(item["fixture_id"]): int(item["snapshot_id"])
                        for item in accepted
                        if item.get("snapshot_id") is not None
                    },
                ),
            )
            report["stage_seconds"]["export"] = round(time.perf_counter() - stage_started, 3)
            for item in export_result.partition.unsafe:
                decision = export_result.partition.decisions[int(item["fixture_id"])]
                next_at = backoff_time(int(item.get("attempt") or 1), datetime.now(timezone.utc))
                update_ledger(
                    conn,
                    int(item["fixture_id"]),
                    "provider_pending",
                    int(item.get("attempt") or 1),
                    assessment=item.get("assessment"),
                    source=item.get("source"),
                    error=decision.reason,
                    next_attempt_at=next_at,
                )
                publish_delivery_status(
                    target_url,
                    conn,
                    int(item["fixture_id"]),
                    target_conn=shared_target_connection(),
                )
                report["provider_pending"].append({"fixture_id": int(item["fixture_id"]), "reason": decision.reason})

            failed_ids = set(export_result.retryable_ids).difference(export_result.unsafe_ids)
            if failed_ids:
                outcomes_by_fixture: dict[int, ExportCommandResult] = {}
                exporter_result = export_result.exporter_result
                if isinstance(exporter_result, ExportBatchResult):
                    outcomes_by_fixture = {
                        fixture_id: outcome
                        for outcome in exporter_result.outcomes
                        for fixture_id in outcome.fixture_ids
                    }
                elif isinstance(exporter_result, ExportCommandResult):
                    outcomes_by_fixture = {
                        fixture_id: exporter_result
                        for fixture_id in exporter_result.fixture_ids
                    }
                for item in accepted:
                    fixture_id = int(item["fixture_id"])
                    if fixture_id not in failed_ids:
                        continue
                    failure = outcomes_by_fixture.get(
                        fixture_id,
                        ExportCommandResult.failure(
                            fixture_ids=[fixture_id],
                            failure_class="exporter",
                            stage="fixture_detail_publish",
                            message="fixture was not included in the successful export result",
                        ),
                    )
                    record_export_failure(
                        conn,
                        target_url,
                        item,
                        failure,
                        target_conn=shared_target_connection(),
                    )
                    report["failed"].append(
                        {
                            "fixture_id": fixture_id,
                            "stage": failure.stage or "export",
                            "failure_class": failure.failure_class or "exporter",
                            "error": failure.message or "fixture-detail export failed",
                        }
                    )

            valid: list[dict[str, Any]] = []
            seasons: set[tuple[int, int]] = set()
            stage_started = time.perf_counter()
            for item in accepted:
                fixture_id = item["fixture_id"]
                if fixture_id not in export_result.exported_ids:
                    continue
                target = target_snapshot(shared_target_connection(), fixture_id)
                problems = compare_snapshots(item["source"], target)
                if problems:
                    message = "; ".join(problems)
                    attempt = int(item.get("attempt") or 1)
                    update_ledger(conn, fixture_id, "verification_failed", attempt, assessment=item["assessment"], source=item["source"], target=target, error=message, next_attempt_at=backoff_time(attempt, datetime.now(timezone.utc)))
                    publish_delivery_status(target_url, conn, fixture_id, target_conn=shared_target_connection())
                    report["failed"].append({"fixture_id": fixture_id, "stage": "verification", "error": message})
                    continue
                meta = item["meta"]
                if meta:
                    seasons.add((int(meta[0]), int(meta[1])))
                valid.append({**item, "target": target})
            report["stage_seconds"]["verification"] = round(time.perf_counter() - stage_started, 3)

            if seasons:
                stage_started = time.perf_counter()
                try:
                    refreshed = refresh_seasons(
                        target_url,
                        seasons,
                        eligible_fixture_ids={int(item["fixture_id"]) for item in valid},
                        target_conn=shared_target_connection(),
                    )
                except Exception as exc:
                    message = str(exc)[-4000:]
                    for item in valid:
                        attempt = int(item.get("attempt") or 1)
                        fixture_id = int(item["fixture_id"])
                        update_ledger(
                            conn,
                            fixture_id,
                            "projection_failed",
                            attempt,
                            assessment=item["assessment"],
                            source=item["source"],
                            target=item["target"],
                            error=message,
                            next_attempt_at=backoff_time(attempt, datetime.now(timezone.utc)),
                        )
                        publish_delivery_status(
                            target_url,
                            conn,
                            fixture_id,
                            target_conn=shared_target_connection(),
                        )
                        report["failed"].append(
                            {"fixture_id": fixture_id, "stage": "projection", "error": message}
                        )
                    valid = []
                    refreshed = {}
                report["stage_seconds"]["projection_refresh"] = round(time.perf_counter() - stage_started, 3)
                for key, count in refreshed.items():
                    report["projection_rows"][key] = report["projection_rows"].get(key, 0) + count

            stage_started = time.perf_counter()
            for item in valid:
                fixture_id = item["fixture_id"]
                activate_provider_snapshot(
                    target_url,
                    fixture_id,
                    item["snapshot_id"],
                    target_conn=shared_target_connection(),
                )
                status = "provider_sparse" if item["assessment"].status == "provider_sparse" else "verified"
                next_reval = revalidation_time(item["meta"][2] if item["meta"] else None, datetime.now(timezone.utc))
                update_ledger(conn, fixture_id, status, 0, assessment=item["assessment"], source=item["source"], target=item["target"], successful=True, payload_hash=item["payload_hash"], normalized_hash=item["normalized_hash"], accepted_snapshot_id=item["snapshot_id"], stable_fetch_count=item["stable_count"], next_revalidation_at=next_reval)
                publish_delivery_status(target_url, conn, fixture_id, target_conn=shared_target_connection())
                report["fixtures_accepted"] += 1
                if status == "provider_sparse":
                    report["provider_sparse"].append({"fixture_id": fixture_id})
            report["stage_seconds"]["activation"] = round(time.perf_counter() - stage_started, 3)
    finally:
        if target_conn is not None and not target_conn.closed:
            target_conn.close()
        conn.close()
        engine.dispose()

    report["status"] = "failed" if report["failed"] else "success"
    report["summary"].update(
        {
            "status": report["status"],
            "batches": report["batches"],
            "fixtures_selected": report["fixtures_selected"],
            "fixtures_accepted": report["fixtures_accepted"],
            "failed": len(report["failed"]),
            "provider_pending": len(report["provider_pending"]),
        }
    )
    Path(args.report_json).write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    print(json.dumps(report, default=str))
    return 1 if report["status"] == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
