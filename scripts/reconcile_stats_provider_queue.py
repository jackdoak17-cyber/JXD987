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
import fcntl
import json
import logging
import os
import sqlite3
import subprocess
import sys
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
    store_provider_detail,
    target_snapshot,
    ledger_attempt_start,
    update_ledger,
    mark_provider_unavailable,
    clear_provider_unavailable_exclusion,
)


LOG = logging.getLogger("reconcile_stats_provider_queue")


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


def export_batch(fixture_ids: list[int], leagues: list[int], report_path: str) -> subprocess.CompletedProcess[str]:
    command = [
        sys.executable,
        str(Path(__file__).with_name("export_to_supabase.py")),
        "--strict",
        "--leagues",
        ",".join(str(value) for value in leagues),
        "--fixture-ids",
        ",".join(str(value) for value in fixture_ids),
        "--keep-all-seasons",
        "--protect-empty-detail",
        "--require-detail",
        "--skip-odds-snapshots",
        "--skip-odds-outcomes",
        "--skip-prune",
        "--report-json",
        report_path,
    ]
    return subprocess.run(command, text=True, capture_output=True, check=False)


def refresh_seasons(target_url: str, seasons: set[tuple[int, int]]) -> dict[str, int]:
    rows: dict[str, int] = {}
    import psycopg2

    with psycopg2.connect(target_url, connect_timeout=20) as conn:
        with conn.cursor() as cur:
            for league_id, season_id in sorted(seasons):
                cur.execute(
                    "select public.refresh_player_stats_season(%s, %s, null)",
                    (league_id, season_id),
                )
                rows[f"{league_id}:{season_id}"] = int(cur.fetchone()[0] or 0)
    return rows


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--leagues", required=True)
    parser.add_argument("--season-ids", default=None)
    parser.add_argument("--batch-size", type=int, default=25)
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

    target_url = os.environ.get("SUPABASE_DB_URL_SESSION") or os.environ.get("SUPABASE_DB_URL")
    if not target_url:
        raise SystemExit("SUPABASE_DB_URL_SESSION or SUPABASE_DB_URL is required")

    conn = source_connection()
    ensure_ledger(conn)
    source_path = str(Path(os.environ.get("JXD_DB_PATH", "data/jxd.sqlite")).resolve())
    engine = create_engine(f"sqlite:///{source_path}", future=True)
    client = SportMonksClient()
    report: dict[str, Any] = {
        "release_id": os.environ.get("RUNTIME_RELEASE_ID", "local"),
        "leagues": leagues,
        "season_ids": season_ids,
        "batches": 0,
        "fixtures_selected": 0,
        "fixtures_accepted": 0,
        "provider_pending": [],
        "provider_unavailable": [],
        "provider_sparse": [],
        "failed": [],
        "projection_rows": {},
        "provider_calls": 0,
    }

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
            report["batches"] += 1
            report["fixtures_selected"] += len(fixture_ids)
            LOG.info("Processing stats reconciliation batch %s: %s fixtures", report["batches"], len(fixture_ids))

            accepted: list[dict[str, Any]] = []
            for fixture_id in fixture_ids:
                prior = conn.execute(
                    "select provider_player_stat_count,last_normalized_hash,stable_fetch_count from fixture_detail_deliveries where fixture_id = ?",
                    (fixture_id,),
                ).fetchone()
                prior_count = int(prior[0] or 0) if prior else 0
                prior_hash = str(prior[1]) if prior and prior[1] else None
                prior_stable = int(prior[2] or 0) if prior else 0
                attempt = ledger_attempt_start(conn, fixture_id, datetime.now(timezone.utc))
                try:
                    payload = client.request(
                        "GET",
                        f"fixtures/{fixture_id}",
                        params={
                            "include": ";".join([
                                "participants", "scores", "state", "statistics", "statistics.type",
                                "lineups.details", "lineups.position", "lineups.detailedposition", "lineups.player",
                            ])
                        },
                    )
                    report["provider_calls"] += 1
                    data = payload.get("data") if isinstance(payload, dict) else None
                    if not isinstance(data, dict) or not data:
                        message = "SportMonks returned no fixture data"
                        if attempt >= 3:
                            raise ProviderFixtureUnavailableError(message)
                        raise RuntimeError(message)
                    assessment = assess_provider_payload(data)
                    payload_hash = provider_payload_hash(data)
                    normalized_hash = normalized_provider_hash(data)
                    meta = conn.execute("select league_id,season_id,starting_at from fixtures where id=?", (fixture_id,)).fetchone()
                    if not meta:
                        # Historical fixtures may exist in the serving store
                        # before the SQLite spool has ever seen them.
                        with psycopg2.connect(target_url, connect_timeout=20) as target_conn:
                            with target_conn.cursor() as target_cur:
                                target_cur.execute(
                                    "select league_id,season_id,starting_at from public.fixtures where id=%s",
                                    (fixture_id,),
                                )
                                meta = target_cur.fetchone()
                    snapshot_id = persist_provider_snapshot(
                        target_url,
                        fixture_id,
                        int(meta[0]) if meta and meta[0] is not None else None,
                        int(meta[1]) if meta and meta[1] is not None else None,
                        data,
                        assessment,
                        payload_hash,
                        normalized_hash,
                    )
                    stable_count = prior_stable + 1 if prior_hash == normalized_hash else 1
                    if assessment.status == "provider_pending":
                        next_at = backoff_time(attempt, datetime.now(timezone.utc))
                        update_ledger(conn, fixture_id, assessment.status, attempt, assessment, error=assessment.error, next_attempt_at=next_at, payload_hash=payload_hash, normalized_hash=normalized_hash, stable_fetch_count=stable_count)
                        publish_delivery_status(target_url, conn, fixture_id)
                        report["provider_pending"].append({"fixture_id": fixture_id, "reason": assessment.error})
                        continue
                    if not args.force and assessment.status == "ready" and prior_count > 0 and assessment.player_stat_count < prior_count and prior_hash != normalized_hash:
                        next_at = backoff_time(attempt, datetime.now(timezone.utc))
                        message = f"provider player-detail collection shrank from {prior_count} to {assessment.player_stat_count}; awaiting confirmation"
                        update_ledger(conn, fixture_id, "provider_pending", attempt, assessment, error=message, next_attempt_at=next_at, payload_hash=payload_hash, normalized_hash=normalized_hash, stable_fetch_count=stable_count)
                        publish_delivery_status(target_url, conn, fixture_id)
                        report["provider_pending"].append({"fixture_id": fixture_id, "reason": message})
                        continue
                    source = store_provider_detail(engine, client, fixture_id, data, assessment)
                    clear_provider_unavailable_exclusion(target_url, fixture_id)
                    if not meta:
                        meta = conn.execute("select league_id,season_id,starting_at from fixtures where id=?", (fixture_id,)).fetchone()
                    accepted.append({"fixture_id": fixture_id, "assessment": assessment, "source": source, "snapshot_id": snapshot_id, "payload_hash": payload_hash, "normalized_hash": normalized_hash, "stable_count": stable_count, "meta": meta})
                except ProviderFixtureUnavailableError as exc:
                    message = str(exc)[-4000:]
                    review_at = mark_provider_unavailable(target_url, conn, fixture_id, attempt, message)
                    report["provider_unavailable"].append({"fixture_id": fixture_id, "next_review_at": review_at, "reason": message})
                except ProviderDetailIncompleteError as exc:
                    message = str(exc)[-4000:]
                    next_at = backoff_time(attempt, datetime.now(timezone.utc))
                    update_ledger(conn, fixture_id, "provider_pending", attempt, assessment, error=message, next_attempt_at=next_at, payload_hash=payload_hash, normalized_hash=normalized_hash, stable_fetch_count=stable_count)
                    publish_delivery_status(target_url, conn, fixture_id)
                    report["provider_pending"].append({"fixture_id": fixture_id, "reason": message})
                except Exception as exc:
                    message = str(exc)[-4000:]
                    update_ledger(conn, fixture_id, "failed", attempt, error=message, next_attempt_at=backoff_time(attempt, datetime.now(timezone.utc)))
                    publish_delivery_status(target_url, conn, fixture_id)
                    report["failed"].append({"fixture_id": fixture_id, "stage": "fetch_or_store", "error": message})

            if not accepted:
                continue
            accepted_ids = [item["fixture_id"] for item in accepted]
            export_result = export_batch(accepted_ids, leagues, "/tmp/reconcile_stats_provider_export.json")
            if export_result.returncode != 0:
                message = (export_result.stderr or export_result.stdout or "batch export failed")[-4000:]
                for item in accepted:
                    fixture_id = item["fixture_id"]
                    update_ledger(conn, fixture_id, "export_failed", 0, error=message, next_attempt_at=backoff_time(1, datetime.now(timezone.utc)))
                    publish_delivery_status(target_url, conn, fixture_id)
                    report["failed"].append({"fixture_id": fixture_id, "stage": "export", "error": message})
                continue

            valid: list[dict[str, Any]] = []
            seasons: set[tuple[int, int]] = set()
            for item in accepted:
                fixture_id = item["fixture_id"]
                with psycopg2.connect(target_url, connect_timeout=20) as target_conn:
                    target = target_snapshot(target_conn, fixture_id)
                problems = compare_snapshots(item["source"], target)
                if problems:
                    message = "; ".join(problems)
                    update_ledger(conn, fixture_id, "verification_failed", 0, assessment=item["assessment"], source=item["source"], target=target, error=message, next_attempt_at=backoff_time(1, datetime.now(timezone.utc)))
                    publish_delivery_status(target_url, conn, fixture_id)
                    report["failed"].append({"fixture_id": fixture_id, "stage": "verification", "error": message})
                    continue
                meta = item["meta"]
                if meta:
                    seasons.add((int(meta[0]), int(meta[1])))
                valid.append({**item, "target": target})

            if seasons:
                refreshed = refresh_seasons(target_url, seasons)
                for key, count in refreshed.items():
                    report["projection_rows"][key] = report["projection_rows"].get(key, 0) + count

            for item in valid:
                fixture_id = item["fixture_id"]
                activate_provider_snapshot(target_url, fixture_id, item["snapshot_id"])
                status = "provider_sparse" if item["assessment"].status == "provider_sparse" else "verified"
                next_reval = revalidation_time(item["meta"][2] if item["meta"] else None, datetime.now(timezone.utc))
                update_ledger(conn, fixture_id, status, 0, assessment=item["assessment"], source=item["source"], target=item["target"], successful=True, payload_hash=item["payload_hash"], normalized_hash=item["normalized_hash"], accepted_snapshot_id=item["snapshot_id"], stable_fetch_count=item["stable_count"], next_revalidation_at=next_reval)
                publish_delivery_status(target_url, conn, fixture_id)
                report["fixtures_accepted"] += 1
                if status == "provider_sparse":
                    report["provider_sparse"].append({"fixture_id": fixture_id})
    finally:
        conn.close()
        engine.dispose()

    report["status"] = "failed" if report["failed"] else "success"
    Path(args.report_json).write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    print(json.dumps(report, default=str))
    return 1 if report["status"] == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
