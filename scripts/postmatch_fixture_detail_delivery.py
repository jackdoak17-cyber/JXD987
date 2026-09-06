#!/usr/bin/env python3
"""Deliver completed SportMonks fixture detail to Supabase.

This is the authoritative post-match delivery worker.  It deliberately works
from explicit fixture IDs, keeps a durable per-fixture ledger in the source
database, retries provider-pending fixtures with backoff, and only publishes a
fixture after the provider payload and the Supabase result have both passed
validation.

The worker is intentionally separate from the large P3 reconciliation.  P3 is
the historical safety net; this bounded queue is the production path for data
that becomes available after a match finishes.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import os
import sqlite3
import subprocess
import sys
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence

import psycopg2
from psycopg2.extras import Json
from sqlalchemy import create_engine

from jxd import SportMonksClient, SyncService
from jxd.models import FixturePlayer, FixturePlayerStatistic, FixtureStatistic


LOG = logging.getLogger("postmatch_fixture_detail_delivery")
UTC = timezone.utc
SOURCE_DB = os.environ.get("JXD_DB_PATH", "data/jxd.sqlite")
FINISHED_STATUSES = {"FT", "AET", "PEN", "FT_PEN", "FINISHED", "ENDED"}
NON_COMPETITIVE_STATUSES = {"ABAN", "ABANDONED", "CANCELLED", "CANCELED", "POSTPONED"}

# These are the provider-backed metrics consumed by the Team Stats surface.
# They describe metric coverage, not fixture validity. SportMonks legitimately
# omits individual team-stat types by competition, fixture, or zero-event
# representation. A missing type is therefore recorded as NULL/coverage
# evidence and classified as provider-sparse; it must not hold a structurally
# valid completed fixture in an endless pending state.
TRACKED_TEAM_STAT_TYPES = frozenset({42, 45, 56, 57, 78, 83, 84, 85, 86, 100, 109, 581})

# Kept as a compatibility alias for scripts/tests that imported the old name.
# There is no provider type that is universally required for every supported
# competition; structural validity is checked per team below instead.
REQUIRED_TEAM_STAT_TYPES = frozenset()
PROVIDER_SPARSE_TEAM_STAT_TYPES = TRACKED_TEAM_STAT_TYPES
TRACKED_PLAYER_STAT_TYPES = {
    42, 52, 56, 57, 78, 79, 83, 84, 85, 86, 88, 96, 97, 100, 101,
    109, 117, 118, 27267, 580, 9706,
}

# These rows are generated locally from provider-backed facts. They are part
# of the serving read model and must be preserved, but they are not evidence
# that SportMonks returned additional provider detail. Shrink protection must
# compare provider-owned rows only, otherwise every revalidation sees these
# derived rows as a false provider shrink.
DERIVED_STAT_TYPE_IDS = frozenset({200001, 200010, 200011, 200012, 200013})

LEDGER_TABLE = "fixture_detail_deliveries"
TARGET_STATUS_TABLE = "fixture_detail_delivery_status"
DELIVERY_CONTRACT_VERSION = 2
DELIVERY_REASON_CODES = frozenset(
    {
        "new",
        "running",
        "provider_pending_structure",
        "provider_pending_identity",
        "provider_pending_optional_metrics",
        "provider_pending_shrink",
        "provider_unavailable",
        "accepted",
        "export_failed",
        "dependency_missing",
        "verification_failed",
        "projection_failed",
        "excluded",
        "legacy_unclassified",
        "unknown",
    }
)
LEGACY_PENDING_REASON = (
    "Legacy provider_pending record had no durable classification reason; "
    "provider revalidation required"
)
HANDOFF_REQUEUE_REASON = (
    "Controlled reconciliation worker handoff recovered; provider revalidation required"
)
DEFAULT_HOURS_BACK = 72
DEFAULT_LIMIT = 25
DEFAULT_GRACE_MINUTES = 60
DEFAULT_SOURCE_BUSY_TIMEOUT_MS = 30_000
DEFAULT_RECENT_REVALIDATION_HOURS = 6
DEFAULT_DAILY_REVALIDATION_HOURS = 24
DEFAULT_HISTORICAL_REVALIDATION_HOURS = 168
DEFAULT_PROVIDER_UNAVAILABLE_REVIEW_DAYS = 7
TARGET_NEW_FIXTURE_SHARE = 0.8


@dataclass(frozen=True)
class ProviderAssessment:
    status: str
    fixture_status: str | None
    finished: bool
    team_stat_types: dict[str, list[int]]
    missing_team_stat_type_ids: dict[str, list[int]]
    player_stat_types: dict[str, list[int]]
    missing_player_stat_type_ids: dict[str, list[int]]
    lineup_counts: dict[str, int]
    player_stat_counts: dict[str, int]
    team_stat_count: int
    player_stat_count: int
    lineup_count: int
    error: str | None = None


@dataclass(frozen=True)
class DetailSnapshot:
    fixture_id: int
    team_stat_count: int
    player_stat_count: int
    lineup_count: int
    team_stat_types: dict[str, list[int]]
    team_stat_values: dict[str, int | float | None]
    player_stat_values: dict[str, int | float | None]
    lineup_values: dict[str, tuple[object, ...]]


class ProviderDetailIncompleteError(RuntimeError):
    """SportMonks returned a finished fixture that cannot be safely published."""


class ProviderFixtureUnavailableError(RuntimeError):
    """SportMonks explicitly returned no fixture for a completed target ID."""


def parse_csv_ints(raw: str | None) -> list[int]:
    values: list[int] = []
    for token in (raw or "").replace("\n", ",").split(","):
        token = token.strip()
        if not token:
            continue
        try:
            value = int(token)
        except ValueError:
            continue
        if value not in values:
            values.append(value)
    return values


def utc_now() -> datetime:
    return datetime.now(UTC)


def iso(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_iso(value: object) -> datetime | None:
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def json_text(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def provider_payload_hash(data: dict[str, Any]) -> str:
    return hashlib.sha256(json_text(data).encode("utf-8")).hexdigest()


def normalized_provider_payload(data: dict[str, Any]) -> dict[str, Any]:
    """Canonicalize provider-owned collections before revision comparison."""
    normalized = dict(data)
    statistics = [item for item in data.get("statistics") or [] if isinstance(item, dict)]
    normalized["statistics"] = sorted(
        statistics,
        key=lambda item: (
            _int(item.get("participant_id") or item.get("team_id")) or 0,
            _int(item.get("type_id") or ((item.get("type") or {}).get("id") if isinstance(item.get("type"), dict) else None)) or 0,
            str(item.get("location") or ""),
            _int(item.get("id")) or 0,
        ),
    )
    lineups = []
    for lineup in data.get("lineups") or []:
        if not isinstance(lineup, dict):
            continue
        copy = dict(lineup)
        details = [item for item in lineup.get("details") or [] if isinstance(item, dict)]
        copy["details"] = sorted(
            details,
            key=lambda item: (
                _int(item.get("type_id") or ((item.get("type") or {}).get("id") if isinstance(item.get("type"), dict) else None)) or 0,
                _int(item.get("id")) or 0,
            ),
        )
        lineups.append(copy)
    normalized["lineups"] = sorted(
        lineups,
        key=lambda item: (
            _int(item.get("team_id") or item.get("participant_id")) or 0,
            _int(item.get("player_id")) or 0,
            _int(item.get("id")) or 0,
        ),
    )
    return normalized


def normalized_provider_hash(data: dict[str, Any]) -> str:
    return provider_payload_hash(normalized_provider_payload(data))


def revalidation_time(starting_at: object, now: datetime) -> str:
    started = parse_iso(starting_at)
    if started is None:
        return iso(now + timedelta(hours=DEFAULT_HISTORICAL_REVALIDATION_HOURS))
    age = now - started
    if age <= timedelta(hours=72):
        hours = DEFAULT_RECENT_REVALIDATION_HOURS
    elif age <= timedelta(days=30):
        hours = DEFAULT_DAILY_REVALIDATION_HOURS
    else:
        hours = DEFAULT_HISTORICAL_REVALIDATION_HOURS
    return iso(now + timedelta(hours=hours))


def source_connection() -> sqlite3.Connection:
    path = Path(SOURCE_DB)
    if not path.exists():
        raise SystemExit(f"Source database not found: {path}")
    conn = sqlite3.connect(
        str(path),
        timeout=max(float(os.environ.get("JXD_SQLITE_BUSY_TIMEOUT_SECONDS", "30")), 1.0),
    )
    conn.execute(
        "pragma busy_timeout = "
        + str(max(int(os.environ.get("JXD_SQLITE_BUSY_TIMEOUT_MS", str(DEFAULT_SOURCE_BUSY_TIMEOUT_MS))), 1000))
    )
    conn.row_factory = sqlite3.Row
    return conn


def source_engine(source_path: str):
    """Create the ORM connection with the same lock tolerance as the queue connection."""
    timeout_seconds = max(float(os.environ.get("JXD_SQLITE_BUSY_TIMEOUT_SECONDS", "30")), 1.0)
    return create_engine(
        f"sqlite:///{source_path}",
        connect_args={"timeout": timeout_seconds},
        future=True,
    )


def ensure_ledger(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        create table if not exists {LEDGER_TABLE} (
          fixture_id integer primary key,
          league_id integer,
          season_id integer,
          status text not null,
          attempts integer not null default 0,
          first_seen_at text not null,
          last_attempted_at text,
          next_attempt_at text,
          last_successful_at text,
          provider_status text,
          provider_finished integer not null default 0,
          provider_team_stat_count integer not null default 0,
          provider_player_stat_count integer not null default 0,
          provider_lineup_count integer not null default 0,
          provider_team_stat_types text,
          provider_missing_type_ids text,
          provider_player_stat_types text,
          provider_missing_player_type_ids text,
          source_snapshot text,
          target_snapshot text,
          last_error text,
          release_id text,
          updated_at text not null
        )
        """
    )
    existing_columns = {
        row[1]
        for row in conn.execute(f"pragma table_info({LEDGER_TABLE})").fetchall()
    }
    contract_columns_added = False
    for column, ddl in (
        ("provider_player_stat_types", "text"),
        ("provider_missing_player_type_ids", "text"),
        ("last_checked_at", "text"),
        ("next_revalidation_at", "text"),
        ("last_payload_hash", "text"),
        ("last_normalized_hash", "text"),
        ("stable_fetch_count", "integer not null default 0"),
        ("accepted_snapshot_id", "integer"),
        ("delivery_contract_version", "integer not null default 2"),
        ("reason_code", "text not null default 'legacy_unclassified'"),
        ("player_stat_parity", "integer"),
        ("lineup_parity", "integer"),
        ("target_player_stat_count", "integer not null default 0"),
        ("target_lineup_count", "integer not null default 0"),
        ("target_team_stat_count", "integer not null default 0"),
        ("parity_checked_at", "text"),
    ):
        if column not in existing_columns:
            conn.execute(f"alter table {LEDGER_TABLE} add column {column} {ddl}")
            contract_columns_added = True
    if contract_columns_added:
        # SQLite applies an ADD COLUMN default to pre-existing rows.  Those
        # rows have not passed the v2 producer/serving contract yet, so do not
        # let the default version 2 make Operations treat missing parity as a
        # genuine data failure. New rows are promoted to v2 by update_ledger.
        conn.execute(
            f"""
            update {LEDGER_TABLE}
               set delivery_contract_version = 1
             where reason_code = 'legacy_unclassified'
            """
        )
    conn.commit()


def recover_stale_running(
    conn: sqlite3.Connection,
    timeout_minutes: int = 30,
    now: datetime | None = None,
) -> int:
    """Requeue attempts left running after a bounded worker handoff.

    The worker checkpoints each fixture, but a lease timeout can occur between
    the running marker and the final classification. Requeue stale markers
    explicitly so the next attempt is observable and cannot remain an
    unexplained in-flight row indefinitely.
    """
    current = now or utc_now()
    current_text = iso(current)
    cutoff_text = iso(current - timedelta(minutes=max(timeout_minutes, 1)))
    cursor = conn.execute(
        f"""
        update {LEDGER_TABLE}
           set status = 'provider_pending',
               reason_code = 'provider_pending_structure',
               next_attempt_at = ?,
               last_error = 'Previous reconciliation worker exited before final classification; fixture requeued',
               updated_at = ?
         where status = 'running'
           and coalesce(last_attempted_at, updated_at) < ?
        """,
        (current_text, current_text, cutoff_text),
    )
    recovered = int(cursor.rowcount or 0)
    if recovered:
        conn.commit()
    return recovered


def repair_legacy_ledger(
    conn: sqlite3.Connection,
    now: datetime | None = None,
) -> dict[str, list[int]]:
    """Make pre-ledger classifications explicit and safely retryable.

    Older workers could leave provider-pending rows without a reason, and the
    bounded reconciliation handoff used to record controlled interruptions as
    ``failed``.  Neither state describes a provider failure.  Repair them at
    worker startup so the queue is self-healing, auditable, and cannot expose
    an opaque pending row indefinitely.  No provider or serving data is
    accepted by this migration; every repaired fixture still goes through the
    normal fetch, snapshot, comparison, and activation gates.
    """
    current_text = iso(now or utc_now())
    legacy_rows = [
        int(row[0])
        for row in conn.execute(
            f"""
            select fixture_id
              from {LEDGER_TABLE}
             where status = 'provider_pending'
               and (last_error is null or trim(last_error) = '')
            """
        ).fetchall()
    ]
    handoff_rows = [
        int(row[0])
        for row in conn.execute(
            f"""
            select fixture_id
              from {LEDGER_TABLE}
             where status = 'failed'
               and last_error = 'Controlled reconciliation worker handoff; fixture requeued'
            """
        ).fetchall()
    ]
    if legacy_rows:
        conn.execute(
            f"""
            update {LEDGER_TABLE}
               set last_error = ?,
                   reason_code = 'legacy_unclassified',
                   next_attempt_at = ?,
                   release_id = ?,
                   updated_at = ?
             where status = 'provider_pending'
               and (last_error is null or trim(last_error) = '')
            """,
            (LEGACY_PENDING_REASON, current_text, release_id(), current_text),
        )
    if handoff_rows:
        conn.execute(
            f"""
            update {LEDGER_TABLE}
               set status = 'provider_pending',
                   reason_code = 'legacy_unclassified',
                   next_attempt_at = ?,
                   last_error = ?,
                   release_id = ?,
                   updated_at = ?
             where status = 'failed'
               and last_error = 'Controlled reconciliation worker handoff; fixture requeued'
            """,
            (current_text, HANDOFF_REQUEUE_REASON, release_id(), current_text),
        )
    if legacy_rows or handoff_rows:
        conn.commit()
    return {"legacy_pending": legacy_rows, "handoff_failed": handoff_rows}


def release_id() -> str:
    explicit = os.environ.get("RUNTIME_RELEASE_ID") or os.environ.get("PIPELINE_RELEASE_ID")
    if explicit:
        return explicit
    manifest = Path(__file__).with_name("vps") / "runtime_manifest.sha1"
    if manifest.exists():
        return hashlib.sha1(manifest.read_bytes()).hexdigest()[:12]
    return "local"


def due(value: object, now: datetime) -> bool:
    parsed = parse_iso(value)
    return parsed is None or parsed <= now


def candidate_fixture_ids(
    conn: sqlite3.Connection,
    league_ids: Sequence[int],
    hours_back: int,
    limit: int,
    force: bool = False,
) -> list[int]:
    now = utc_now()
    cutoff = now - timedelta(hours=max(hours_back, 0))
    params: list[object] = [cutoff.strftime("%Y-%m-%d %H:%M:%S"), now.strftime("%Y-%m-%d %H:%M:%S")]
    league_clause = ""
    if league_ids:
        placeholders = ",".join("?" for _ in league_ids)
        league_clause = f"and f.league_id in ({placeholders})"
        params.extend(league_ids)
    rows = conn.execute(
        f"""
        select f.id, f.starting_at, d.status, d.next_attempt_at, d.updated_at,
               d.next_revalidation_at
          from fixtures f
          left join {LEDGER_TABLE} d on d.fixture_id = f.id
         where ((f.starting_at >= ? and f.starting_at <= ?)
            or d.next_revalidation_at is not null)
           {league_clause}
         order by
           case when f.starting_at >= ? then 0 else 1 end,
           f.starting_at asc,
           f.id asc
        """,
        [*params, cutoff.strftime("%Y-%m-%d %H:%M:%S")],
    ).fetchall()
    selected: list[int] = []
    now = utc_now()
    for row in rows:
        status = str(row[2] or "new")
        if not force and status == "running":
            running_at = parse_iso(row[4])
            if running_at and now - running_at < timedelta(minutes=30):
                continue
        if not force:
            if status == "verified":
                # Verified is deliberately non-terminal. Existing rows from
                # before revalidation was introduced have no schedule and
                # are rechecked while they remain in the recent window.
                revalidation_at = row[5]
                if revalidation_at is not None and not due(revalidation_at, now):
                    continue
                if revalidation_at is None and parse_iso(row[1]) and parse_iso(row[1]) < now - timedelta(hours=max(hours_back, 0)):
                    continue
            elif status == "provider_sparse" and not due(row[3], now):
                continue
            elif row[3] is not None and not due(row[3], now):
                continue
        selected.append(int(row[0]))
        if len(selected) >= max(limit, 0):
            break
    return selected


def candidate_target_fixture_ids(
    target_url: str,
    league_ids: Sequence[int],
    limit: int,
    force: bool = False,
    season_ids: Sequence[int] | None = None,
) -> list[int]:
    """Select completed target fixtures missing or due for provider detail.

    Historical backfills must make progress even when a large retry queue is
    eligible. Reserve most of every batch for fixtures with no delivery row,
    while retaining a bounded retry/revalidation lane. If either lane is
    smaller than its reservation, the spare slots are filled from the other
    lane. This prevents repeated shrink confirmations from starving the
    never-delivered population.
    """
    clauses = [
        "f.home_score is not null",
        "f.away_score is not null",
        "not exists (select 1 from public.fixture_stats_quality_exclusions x where x.fixture_id = f.id and (x.exclusion_type = 'duplicate' or x.next_review_at is null or x.next_review_at > now()))",
    ]
    params: list[object] = []
    if league_ids:
        clauses.append("f.league_id = any(%s)")
        params.append(list(league_ids))
    if season_ids:
        clauses.append("f.season_id = any(%s)")
        params.append(list(season_ids))
    requested = max(int(limit), 0)
    if requested == 0:
        return []

    def fetch_candidates(
        target_conn: Any,
        extra_clause: str,
        order_clause: str,
    ) -> list[int]:
        query = f"""
            select f.id
              from public.fixtures f
              left join public.fixture_detail_delivery_status d on d.fixture_id = f.id
             where {' and '.join([*clauses, extra_clause])}
             order by {order_clause}
             limit %s
        """
        with target_conn.cursor() as cur:
            cur.execute(query, [*params, requested])
            return [int(row[0]) for row in cur.fetchall()]

    with psycopg2.connect(target_url, connect_timeout=20) as target_conn:
        if force:
            return fetch_candidates(
                target_conn,
                "true",
                "f.starting_at desc, f.id desc",
            )

        new_quota, retry_quota = target_candidate_quotas(requested)
        new_candidates = fetch_candidates(
            target_conn,
            "d.fixture_id is null",
            # Keep one bounded batch within a small number of league-season
            # cohorts. Projection refresh is keyed by this pair; clustering
            # the queue avoids rebuilding 15-20 unrelated read models for a
            # 50-fixture batch while the fair lane still advances every
            # supported competition over time.
            "f.season_id desc, f.league_id, f.starting_at asc, f.id asc",
        )
        retry_candidates = fetch_candidates(
            target_conn,
            """
            d.fixture_id is not null
            and (
              (d.status in ('provider_pending', 'failed', 'export_failed', 'verification_failed', 'projection_failed')
                and (d.next_attempt_at is null or d.next_attempt_at <= now()))
              or (d.next_revalidation_at is not null and d.next_revalidation_at <= now())
              or (d.status = 'verified' and d.next_revalidation_at is null)
              or (
                d.status in ('verified', 'provider_sparse')
                and (
                  coalesce(d.delivery_contract_version, 1) < 2
                  or d.accepted_snapshot_id is null
                  or d.player_stat_parity is distinct from true
                  or d.lineup_parity is distinct from true
                  or coalesce(d.target_player_stat_count, 0) <= 0
                  or coalesce(d.target_lineup_count, 0) <= 0
                )
              )
            )
            """,
            """
            case
              when d.status in ('verified', 'provider_sparse')
               and (
                 coalesce(d.delivery_contract_version, 1) < 2
                 or d.accepted_snapshot_id is null
                 or d.player_stat_parity is distinct from true
                 or d.lineup_parity is distinct from true
                 or coalesce(d.target_player_stat_count, 0) <= 0
                 or coalesce(d.target_lineup_count, 0) <= 0
               )
              then 0
              else 1
            end,
            case when d.accepted_snapshot_id is null then 0 else 1 end,
            case
              when d.status = 'provider_pending'
               and d.last_error like 'Legacy provider_pending record%%'
              then 0
              when d.status = 'provider_pending'
               and nullif(btrim(d.last_error), '') is null
              then 0
              else 1
            end,
            coalesce(d.next_attempt_at, d.next_revalidation_at, d.updated_at, f.starting_at),
            f.season_id desc,
            f.league_id,
            f.starting_at asc,
            f.id asc
            """,
        )

    selected = new_candidates[:new_quota] + retry_candidates[:retry_quota]
    if len(selected) < requested:
        selected.extend(new_candidates[new_quota : new_quota + requested - len(selected)])
    if len(selected) < requested:
        selected.extend(retry_candidates[retry_quota : retry_quota + requested - len(selected)])
    return selected[:requested]


def target_candidate_quotas(limit: int, new_share: float = TARGET_NEW_FIXTURE_SHARE) -> tuple[int, int]:
    """Return the reserved new-fixture and retry slots for one target batch."""
    requested = max(int(limit), 0)
    if requested == 0:
        return 0, 0
    new_quota = min(requested, max(1, math.ceil(requested * new_share)))
    return new_quota, requested - new_quota


def excluded_target_fixture_ids(target_url: str, fixture_ids: Sequence[int]) -> set[int]:
    """Return quality-excluded IDs so source and target queues share one gate."""
    ids = [int(value) for value in fixture_ids]
    if not ids:
        return set()
    with psycopg2.connect(target_url, connect_timeout=20) as target_conn:
        with target_conn.cursor() as cur:
            cur.execute(
                "select fixture_id from public.fixture_stats_quality_exclusions where fixture_id = any(%s) and (exclusion_type = 'duplicate' or next_review_at is null or next_review_at > now())",
                (ids,),
            )
            return {int(row[0]) for row in cur.fetchall()}


def target_fixture_metadata(
    target_url: str,
    fixture_ids: Sequence[int],
) -> dict[int, tuple[int | None, int | None, str | None, int, int]]:
    """Load serving identity metadata for target-queue fixtures.

    Historical target rows can be absent from the SQLite spool. Keeping their
    league/season identity in the source ledger is required for scoped API
    coverage and for resumable backfill selection.
    """
    ids = [int(value) for value in fixture_ids]
    if not ids:
        return {}
    with psycopg2.connect(target_url, connect_timeout=20) as target_conn:
        with target_conn.cursor() as cur:
            cur.execute(
                """
                select f.id, f.league_id, f.season_id, f.starting_at,
                       (select count(distinct (fs.team_id, fs.type_id))
                          from public.fixture_statistics fs
                         where fs.fixture_id = f.id
                           and fs.type_id <> all(%s)) as team_stat_count,
                       (select count(distinct (fps.player_id, fps.team_id, fps.type_id))
                          from public.fixture_player_statistics fps
                         where fps.fixture_id = f.id
                           and fps.type_id <> all(%s)) as player_stat_count
                  from public.fixtures f
                 where f.id = any(%s)
                """,
                (list(DERIVED_STAT_TYPE_IDS), list(DERIVED_STAT_TYPE_IDS), ids),
            )
            return {
                int(row[0]): (
                    _int(row[1]),
                    _int(row[2]),
                    str(row[3]) if row[3] is not None else None,
                    int(row[4] or 0),
                    int(row[5] or 0),
                )
                for row in cur.fetchall()
            }


def ledger_attempt_start(
    conn: sqlite3.Connection,
    fixture_id: int,
    now: datetime,
    league_id: int | None = None,
    season_id: int | None = None,
) -> int:
    timestamp = iso(now)
    conn.execute(
        f"""
        insert into {LEDGER_TABLE} (
          fixture_id, league_id, season_id, status, attempts, first_seen_at, last_attempted_at,
          next_attempt_at, release_id, updated_at
        ) values (?, ?, ?, 'running', 1, ?, ?, null, ?, ?)
        on conflict(fixture_id) do update set
          league_id=coalesce(excluded.league_id, {LEDGER_TABLE}.league_id),
          season_id=coalesce(excluded.season_id, {LEDGER_TABLE}.season_id),
          status='running',
          attempts={LEDGER_TABLE}.attempts + 1,
          last_attempted_at=excluded.last_attempted_at,
          next_attempt_at=null,
          last_error=null,
          reason_code='running',
          release_id=excluded.release_id,
          updated_at=excluded.updated_at
        """,
        (fixture_id, league_id, season_id, timestamp, timestamp, release_id(), timestamp),
    )
    row = conn.execute(
        f"select attempts from {LEDGER_TABLE} where fixture_id = ?", (fixture_id,)
    ).fetchone()
    conn.commit()
    return int(row[0]) if row else 1


def backoff_time(attempt: int, now: datetime) -> str:
    minutes = (15, 30, 60, 180, 360, 720, 1440)[min(max(attempt - 1, 0), 6)]
    return iso(now + timedelta(minutes=minutes))


def delivery_reason_code(
    status: str,
    error: str | None = None,
    assessment: ProviderAssessment | None = None,
    explicit: str | None = None,
) -> str:
    """Return the stable machine-readable reason for one ledger transition."""
    if explicit in DELIVERY_REASON_CODES:
        return explicit
    if status == "new":
        return "new"
    if status == "running":
        return "running"
    if status in {"verified", "provider_sparse"}:
        return "accepted"
    if status == "excluded":
        return "provider_unavailable" if "no fixture" in (error or "").lower() else "excluded"
    if status == "provider_pending":
        detail = " ".join(
            value.lower()
            for value in (
                error or "",
                assessment.error if assessment else "",
            )
            if value
        )
        if "shrank" in detail or "shrink" in detail:
            return "provider_pending_shrink"
        if "optional provider" in detail or "optional metric" in detail:
            return "provider_pending_optional_metrics"
        if "identity" in detail or "lineup/player" in detail:
            return "provider_pending_identity"
        return "provider_pending_structure"
    if status == "export_failed" or status == "failed":
        detail = (error or "").lower()
        return "dependency_missing" if "foreign key" in detail or "dependency missing" in detail else "export_failed"
    if status == "verification_failed":
        return "verification_failed"
    if status == "projection_failed":
        return "projection_failed"
    return "unknown"


def update_ledger(
    conn: sqlite3.Connection,
    fixture_id: int,
    status: str,
    attempt: int,
    assessment: ProviderAssessment | None = None,
    source: DetailSnapshot | None = None,
    target: DetailSnapshot | None = None,
    error: str | None = None,
    next_attempt_at: str | None = None,
    successful: bool = False,
    payload_hash: str | None = None,
    normalized_hash: str | None = None,
    accepted_snapshot_id: int | None = None,
    stable_fetch_count: int | None = None,
    next_revalidation_at: str | None = None,
    reason_code: str | None = None,
) -> None:
    now = iso(utc_now())
    provider = assessment
    resolved_reason = delivery_reason_code(status, error, provider, reason_code)
    has_parity_evidence = source is not None and target is not None
    conn.execute(
        f"""
        update {LEDGER_TABLE}
           set status = ?,
               attempts = ?,
               next_attempt_at = ?,
               last_checked_at = ?,
               next_revalidation_at = coalesce(?, next_revalidation_at),
               last_successful_at = case when ? then ? else last_successful_at end,
               delivery_contract_version = ?,
               reason_code = ?,
               provider_status = coalesce(?, provider_status),
               provider_finished = coalesce(?, provider_finished),
               provider_team_stat_count = coalesce(?, provider_team_stat_count),
               provider_player_stat_count = coalesce(?, provider_player_stat_count),
               provider_lineup_count = coalesce(?, provider_lineup_count),
               provider_team_stat_types = coalesce(?, provider_team_stat_types),
               provider_missing_type_ids = coalesce(?, provider_missing_type_ids),
               provider_player_stat_types = coalesce(?, provider_player_stat_types),
               provider_missing_player_type_ids = coalesce(?, provider_missing_player_type_ids),
               source_snapshot = coalesce(?, source_snapshot),
               target_snapshot = coalesce(?, target_snapshot),
               last_error = ?,
               release_id = ?,
               updated_at = ?,
               last_payload_hash = coalesce(?, last_payload_hash),
               last_normalized_hash = coalesce(?, last_normalized_hash),
               stable_fetch_count = coalesce(?, stable_fetch_count),
               accepted_snapshot_id = coalesce(?, accepted_snapshot_id),
               player_stat_parity = coalesce(?, player_stat_parity),
               lineup_parity = coalesce(?, lineup_parity),
               target_player_stat_count = coalesce(?, target_player_stat_count),
               target_lineup_count = coalesce(?, target_lineup_count),
               target_team_stat_count = coalesce(?, target_team_stat_count),
               parity_checked_at = case when ? then ? else parity_checked_at end
         where fixture_id = ?
        """,
        (
            status,
            attempt,
            next_attempt_at,
            now,
            next_revalidation_at,
            1 if successful else 0,
            now,
            DELIVERY_CONTRACT_VERSION,
            resolved_reason,
            provider.fixture_status if provider else None,
            1 if provider and provider.finished else None,
            provider.team_stat_count if provider else None,
            provider.player_stat_count if provider else None,
            provider.lineup_count if provider else None,
            json_text(provider.team_stat_types) if provider else None,
            json_text(provider.missing_team_stat_type_ids) if provider else None,
            json_text(provider.player_stat_types) if provider else None,
            json_text(provider.missing_player_stat_type_ids) if provider else None,
            json_text(asdict(source)) if source else None,
            json_text(asdict(target)) if target else None,
            (error or (provider.error if provider else None) or "")[:4000] or None,
            release_id(),
            now,
            payload_hash,
            normalized_hash,
            stable_fetch_count,
            accepted_snapshot_id,
            (source.player_stat_values == target.player_stat_values) if has_parity_evidence else None,
            (source.lineup_values == target.lineup_values) if has_parity_evidence else None,
            target.player_stat_count if target else None,
            target.lineup_count if target else None,
            target.team_stat_count if target else None,
            has_parity_evidence,
            now,
            fixture_id,
        ),
    )
    conn.commit()


def _json_value(raw: object) -> Json | None:
    if raw is None or raw == "":
        return None
    try:
        return Json(json.loads(str(raw)))
    except (TypeError, ValueError, json.JSONDecodeError):
        return Json({"raw": str(raw)})


def mark_provider_unavailable(
    target_url: str,
    conn: sqlite3.Connection,
    fixture_id: int,
    attempt: int,
    error: str,
    exclusion_reason: str | None = None,
    evidence: dict[str, Any] | None = None,
    assessment: ProviderAssessment | None = None,
    target_conn: Any | None = None,
    reason_code: str | None = None,
) -> str:
    """Quarantine a provider-absent target while retaining a scheduled review."""
    review_at = iso(utc_now() + timedelta(days=DEFAULT_PROVIDER_UNAVAILABLE_REVIEW_DAYS))
    resolved_reason = exclusion_reason or "SportMonks returned no fixture data for a completed target ID"
    resolved_evidence = {"attempt": attempt, "error": error[:4000]}
    if evidence:
        resolved_evidence.update(evidence)
    meta = conn.execute(
        "select league_id, season_id from fixtures where id = ?",
        (fixture_id,),
    ).fetchone()
    owns_target_conn = target_conn is None
    target = target_conn or psycopg2.connect(target_url, connect_timeout=20)
    try:
        with target.cursor() as cur:
            if not meta:
                cur.execute(
                    "select league_id, season_id from public.fixtures where id = %s",
                    (fixture_id,),
                )
                meta = cur.fetchone()
            cur.execute(
                """
                insert into public.fixture_stats_quality_exclusions (
                  fixture_id, league_id, season_id, exclusion_type,
                  reason, next_review_at, evidence, last_checked_at, updated_at
                ) values (
                  %s, %s, %s, 'provider_unavailable', %s, %s::timestamptz,
                  %s::jsonb, now(), now()
                )
                on conflict (fixture_id) do update set
                  league_id = excluded.league_id,
                  season_id = excluded.season_id,
                  exclusion_type = case
                    when public.fixture_stats_quality_exclusions.exclusion_type = 'duplicate'
                    then public.fixture_stats_quality_exclusions.exclusion_type
                    else excluded.exclusion_type
                  end,
                  reason = case
                    when public.fixture_stats_quality_exclusions.exclusion_type = 'duplicate'
                    then public.fixture_stats_quality_exclusions.reason
                    else excluded.reason
                  end,
                  next_review_at = case
                    when public.fixture_stats_quality_exclusions.exclusion_type = 'duplicate'
                    then public.fixture_stats_quality_exclusions.next_review_at
                    else excluded.next_review_at
                  end,
                  evidence = case
                    when public.fixture_stats_quality_exclusions.exclusion_type = 'duplicate'
                    then public.fixture_stats_quality_exclusions.evidence
                    else excluded.evidence
                  end,
                  last_checked_at = now(),
                  updated_at = now()
                """,
                (
                    fixture_id,
                    int(meta[0]) if meta and meta[0] is not None else None,
                    int(meta[1]) if meta and meta[1] is not None else None,
                    resolved_reason,
                    review_at,
                    json.dumps(resolved_evidence),
                ),
            )
        target.commit()
    except Exception:
        target.rollback()
        raise
    finally:
        if owns_target_conn:
            target.close()
    update_ledger(
        conn,
        fixture_id,
        "excluded",
        attempt,
        assessment=assessment,
        error=f"{resolved_reason}; {error}; quarantined until {review_at}",
        next_attempt_at=review_at,
        reason_code=reason_code or ("excluded" if exclusion_reason else "provider_unavailable"),
    )
    publish_delivery_status(target_url, conn, fixture_id, target_conn=target_conn)
    return review_at


def clear_provider_unavailable_exclusion(
    target_url: str,
    fixture_id: int,
    target_conn: Any | None = None,
) -> None:
    """Re-open a quarantined fixture only after a complete provider payload passes validation."""
    owns_target_conn = target_conn is None
    target = target_conn or psycopg2.connect(target_url, connect_timeout=20)
    try:
        with target.cursor() as cur:
            cur.execute(
                "delete from public.fixture_stats_quality_exclusions where fixture_id = %s and exclusion_type = 'provider_unavailable'",
                (fixture_id,),
            )
        target.commit()
    except Exception:
        target.rollback()
        raise
    finally:
        if owns_target_conn:
            target.close()


def persist_provider_snapshot(
    target_url: str,
    fixture_id: int,
    league_id: int | None,
    season_id: int | None,
    data: dict[str, Any],
    assessment: ProviderAssessment,
    payload_hash: str,
    normalized_hash: str,
    target_conn: Any | None = None,
) -> int:
    """Persist every successful provider response before active facts publish."""
    owns_target_conn = target_conn is None
    target = target_conn or psycopg2.connect(target_url, connect_timeout=20)
    try:
        with target.cursor() as cur:
            cur.execute(
                """
                insert into public.fixture_detail_snapshots (
                  fixture_id, league_id, season_id, payload_hash,
                  normalized_hash, provider_status, quality_status,
                  payload, release_id, error, fetched_at, last_seen_at
                ) values (
                  %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s,
                  now(), now()
                )
                on conflict (fixture_id, payload_hash) do update set
                  league_id = excluded.league_id,
                  season_id = excluded.season_id,
                  normalized_hash = excluded.normalized_hash,
                  provider_status = excluded.provider_status,
                  quality_status = excluded.quality_status,
                  payload = excluded.payload,
                  release_id = excluded.release_id,
                  error = excluded.error,
                  last_seen_at = now()
                returning id
                """,
                (
                    fixture_id,
                    league_id,
                    season_id,
                    payload_hash,
                    normalized_hash,
                    assessment.fixture_status,
                    assessment.status,
                    json_text(data),
                    release_id(),
                    assessment.error,
                ),
            )
            row = cur.fetchone()
            if not row:
                raise RuntimeError(f"Provider snapshot was not persisted for fixture {fixture_id}")
            snapshot_id = int(row[0])
        target.commit()
        return snapshot_id
    except Exception:
        target.rollback()
        raise
    finally:
        if owns_target_conn:
            target.close()


def activate_provider_snapshot(
    target_url: str,
    fixture_id: int,
    snapshot_id: int,
    target_conn: Any | None = None,
) -> None:
    """Atomically mark the snapshot accepted and link all active fixture facts."""
    owns_target_conn = target_conn is None
    target = target_conn or psycopg2.connect(target_url, connect_timeout=20)
    try:
        with target.cursor() as cur:
            for table in ("fixture_players", "fixture_statistics", "fixture_player_statistics"):
                cur.execute(
                    f"update public.{table} set provider_snapshot_id = %s where fixture_id = %s",
                    (snapshot_id, fixture_id),
                )
            cur.execute(
                """
                update public.fixture_detail_snapshots
                   set accepted_at = now(), quality_status = 'accepted'
                 where id = %s and fixture_id = %s
                """,
                (snapshot_id, fixture_id),
            )
        target.commit()
    except Exception:
        target.rollback()
        raise
    finally:
        if owns_target_conn:
            target.close()


def publish_delivery_status(
    target_url: str,
    source_conn: sqlite3.Connection,
    fixture_id: int,
    target_conn: Any | None = None,
) -> None:
    """Mirror the durable source ledger into the serving database.

    The source SQLite ledger remains authoritative for retries.  This small
    serving-side projection lets the stats API explain whether a null metric
    is still pending ingestion or was explicitly classified as provider-sparse.
    """
    row = source_conn.execute(
        f"""
        select d.fixture_id,
               coalesce(d.league_id, f.league_id) as league_id,
               coalesce(d.season_id, f.season_id) as season_id,
               d.status, d.attempts, d.first_seen_at, d.last_attempted_at,
               d.next_attempt_at, d.last_checked_at, d.next_revalidation_at,
               d.last_successful_at, d.provider_status,
               d.provider_finished, d.provider_team_stat_count,
               d.provider_player_stat_count, d.provider_lineup_count,
               d.provider_team_stat_types, d.provider_missing_type_ids,
               d.provider_player_stat_types, d.provider_missing_player_type_ids,
               d.source_snapshot, d.target_snapshot, d.last_error,
               d.release_id, d.updated_at, d.last_payload_hash,
               d.last_normalized_hash, d.stable_fetch_count,
               d.accepted_snapshot_id, d.delivery_contract_version,
               d.reason_code, d.player_stat_parity, d.lineup_parity,
               d.target_player_stat_count, d.target_lineup_count,
               d.target_team_stat_count, d.parity_checked_at
          from {LEDGER_TABLE} d
          left join fixtures f on f.id = d.fixture_id
         where d.fixture_id = ?
        """,
        (fixture_id,),
    ).fetchone()
    if row is None:
        return

    values = (
        int(row["fixture_id"]),
        _int(row["league_id"]),
        _int(row["season_id"]),
        str(row["status"]),
        int(row["attempts"] or 0),
        row["first_seen_at"],
        row["last_attempted_at"],
        row["next_attempt_at"],
        row["last_checked_at"],
        row["next_revalidation_at"],
        row["last_successful_at"],
        row["provider_status"],
        bool(row["provider_finished"]),
        int(row["provider_team_stat_count"] or 0),
        int(row["provider_player_stat_count"] or 0),
        int(row["provider_lineup_count"] or 0),
        _json_value(row["provider_team_stat_types"]),
        _json_value(row["provider_missing_type_ids"]),
        _json_value(row["provider_player_stat_types"]),
        _json_value(row["provider_missing_player_type_ids"]),
        _json_value(row["source_snapshot"]),
        _json_value(row["target_snapshot"]),
        row["last_error"],
        row["release_id"],
        row["updated_at"],
        row["last_payload_hash"],
        row["last_normalized_hash"],
        int(row["stable_fetch_count"] or 0),
        row["accepted_snapshot_id"],
        int(row["delivery_contract_version"] or DELIVERY_CONTRACT_VERSION),
        row["reason_code"] or "legacy_unclassified",
        None if row["player_stat_parity"] is None else bool(row["player_stat_parity"]),
        None if row["lineup_parity"] is None else bool(row["lineup_parity"]),
        int(row["target_player_stat_count"] or 0),
        int(row["target_lineup_count"] or 0),
        int(row["target_team_stat_count"] or 0),
        row["parity_checked_at"],
    )
    owns_target_conn = target_conn is None
    target = target_conn or psycopg2.connect(target_url, connect_timeout=20)
    try:
        with target.cursor() as cur:
            cur.execute(
                f"""
                insert into public.{TARGET_STATUS_TABLE} (
                  fixture_id, league_id, season_id, status, attempts,
                  first_seen_at, last_attempted_at, next_attempt_at,
                  last_checked_at, next_revalidation_at, last_successful_at,
                  provider_status, provider_finished,
                  provider_team_stat_count, provider_player_stat_count,
                  provider_lineup_count, provider_team_stat_types,
                  provider_missing_type_ids, provider_player_stat_types,
                  provider_missing_player_type_ids, source_snapshot, target_snapshot,
                  last_error, release_id, updated_at, last_payload_hash,
                  last_normalized_hash, stable_fetch_count, accepted_snapshot_id,
                  delivery_contract_version, reason_code, player_stat_parity,
                  lineup_parity, target_player_stat_count, target_lineup_count,
                  target_team_stat_count, parity_checked_at
                ) values (
                  %s, %s, %s, %s, %s,
                  %s::timestamptz, %s::timestamptz, %s::timestamptz,
                  %s::timestamptz, %s::timestamptz, %s::timestamptz,
                  %s, %s,
                  %s, %s, %s, %s, %s, %s, %s, %s, %s,
                  %s, %s, %s::timestamptz, %s, %s, %s, %s,
                  %s, %s, %s, %s, %s, %s, %s, %s::timestamptz
                )
                on conflict (fixture_id) do update set
                  league_id = excluded.league_id,
                  season_id = excluded.season_id,
                  status = excluded.status,
                  attempts = excluded.attempts,
                  first_seen_at = excluded.first_seen_at,
                  last_attempted_at = excluded.last_attempted_at,
                  next_attempt_at = excluded.next_attempt_at,
                  last_checked_at = excluded.last_checked_at,
                  next_revalidation_at = excluded.next_revalidation_at,
                  last_successful_at = excluded.last_successful_at,
                  provider_status = excluded.provider_status,
                  provider_finished = excluded.provider_finished,
                  provider_team_stat_count = excluded.provider_team_stat_count,
                  provider_player_stat_count = excluded.provider_player_stat_count,
                  provider_lineup_count = excluded.provider_lineup_count,
                  provider_team_stat_types = excluded.provider_team_stat_types,
                  provider_missing_type_ids = excluded.provider_missing_type_ids,
                  provider_player_stat_types = excluded.provider_player_stat_types,
                  provider_missing_player_type_ids = excluded.provider_missing_player_type_ids,
                  source_snapshot = excluded.source_snapshot,
                  target_snapshot = excluded.target_snapshot,
                  last_error = excluded.last_error,
                  release_id = excluded.release_id,
                  updated_at = excluded.updated_at,
                  last_payload_hash = excluded.last_payload_hash,
                  last_normalized_hash = excluded.last_normalized_hash,
                  stable_fetch_count = excluded.stable_fetch_count,
                  accepted_snapshot_id = excluded.accepted_snapshot_id,
                  delivery_contract_version = excluded.delivery_contract_version,
                  reason_code = excluded.reason_code,
                  player_stat_parity = excluded.player_stat_parity,
                  lineup_parity = excluded.lineup_parity,
                  target_player_stat_count = excluded.target_player_stat_count,
                  target_lineup_count = excluded.target_lineup_count,
                  target_team_stat_count = excluded.target_team_stat_count,
                  parity_checked_at = excluded.parity_checked_at
                """,
                values,
            )
        target.commit()
    except Exception:
        target.rollback()
        raise
    finally:
        if owns_target_conn:
            target.close()


def _int(value: object) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def refresh_player_projection(target_url: str, fixture_id: int) -> int:
    """Refresh all affected player-season rows after verified fixture export."""
    with psycopg2.connect(target_url, connect_timeout=20) as target_conn:
        with target_conn.cursor() as cur:
            cur.execute(
                "select public.refresh_player_stats_for_fixture(%s)",
                (fixture_id,),
            )
            row = cur.fetchone()
            return int(row[0] or 0) if row else 0


def _provider_status(data: dict[str, Any]) -> str | None:
    state = data.get("state") or {}
    if not isinstance(state, dict):
        state = {}
    for value in (
        state.get("short_name"),
        state.get("developer_name"),
        state.get("state"),
        data.get("status"),
        data.get("status_code"),
    ):
        if value:
            return str(value).upper()
    return None


def _team_ids(data: dict[str, Any]) -> list[int]:
    values: list[int] = []
    for participant in data.get("participants") or []:
        if not isinstance(participant, dict):
            continue
        value = _int(participant.get("id") or participant.get("team_id"))
        if value and value not in values:
            values.append(value)
    for key in ("home_team_id", "away_team_id"):
        value = _int(data.get(key))
        if value and value not in values:
            values.append(value)
    return values


def assess_provider_payload(data: dict[str, Any]) -> ProviderAssessment:
    status = _provider_status(data)
    finished = status in FINISHED_STATUSES
    teams = _team_ids(data)
    stats = data.get("statistics")
    lineups = data.get("lineups")
    stats_list = stats if isinstance(stats, list) else []
    lineups_list = lineups if isinstance(lineups, list) else []
    team_types: dict[int, set[int]] = {team_id: set() for team_id in teams}
    team_stat_count = 0
    for stat in stats_list:
        if not isinstance(stat, dict):
            continue
        team_id = _int(stat.get("participant_id") or stat.get("team_id"))
        type_info = stat.get("type") or {}
        type_id = _int(stat.get("type_id") or (type_info.get("id") if isinstance(type_info, dict) else None))
        if team_id and type_id:
            team_types.setdefault(team_id, set()).add(type_id)
            team_stat_count += 1

    lineup_counts: dict[int, int] = {team_id: 0 for team_id in teams}
    lineup_identity_counts: dict[int, int] = {team_id: 0 for team_id in teams}
    player_stat_counts: dict[int, int] = {team_id: 0 for team_id in teams}
    player_types: dict[int, set[int]] = {team_id: set() for team_id in teams}
    lineup_count = 0
    player_stat_count = 0
    for lineup in lineups_list:
        if not isinstance(lineup, dict):
            continue
        team_id = _int(lineup.get("team_id") or lineup.get("participant_id") or (lineup.get("team") or {}).get("id"))
        if not team_id:
            continue
        lineup_counts[team_id] = lineup_counts.get(team_id, 0) + 1
        player_id = _int(lineup.get("player_id") or (lineup.get("player") or {}).get("id"))
        if player_id:
            lineup_identity_counts[team_id] = lineup_identity_counts.get(team_id, 0) + 1
        lineup_count += 1
        for detail in lineup.get("details") or []:
            if not isinstance(detail, dict):
                continue
            type_id = _int(detail.get("type_id") or (detail.get("type") or {}).get("id"))
            if type_id:
                player_stat_counts[team_id] = player_stat_counts.get(team_id, 0) + 1
                player_types.setdefault(team_id, set()).add(type_id)
                player_stat_count += 1

    type_output = {str(team_id): sorted(types) for team_id, types in team_types.items()}
    missing: dict[str, list[int]] = {}
    for team_id in teams:
        missing[str(team_id)] = sorted(TRACKED_TEAM_STAT_TYPES.difference(team_types.get(team_id, set())))
    player_type_output = {str(team_id): sorted(types) for team_id, types in player_types.items()}
    missing_player_types = {
        str(team_id): sorted(TRACKED_PLAYER_STAT_TYPES.difference(player_types.get(team_id, set())))
        for team_id in teams
    }

    assessment_error: str | None = None
    if not finished or len(teams) < 2:
        assessment_status = "provider_pending"
        assessment_error = (
            "provider fixture structure pending "
            f"(status={status or 'unknown'}, teams={len(teams)})"
        )
    elif not lineups_list or any(
        lineup_counts.get(team_id, 0) <= 0
        or lineup_identity_counts.get(team_id, 0) < lineup_counts.get(team_id, 0)
        or player_stat_counts.get(team_id, 0) <= 0
        for team_id in teams
    ):
        assessment_status = "provider_pending"
        incomplete_teams = [
            str(team_id)
            for team_id in teams
            if (
                lineup_counts.get(team_id, 0) <= 0
                or lineup_identity_counts.get(team_id, 0) < lineup_counts.get(team_id, 0)
                or player_stat_counts.get(team_id, 0) <= 0
            )
        ]
        assessment_error = (
            "provider lineup/player identity detail incomplete "
            f"for team ids {','.join(incomplete_teams) or 'unknown'}"
        )
    elif any(missing.get(str(team_id)) for team_id in teams):
        assessment_status = "provider_sparse"
    else:
        assessment_status = "ready"

    return ProviderAssessment(
        status=assessment_status,
        fixture_status=status,
        finished=finished,
        team_stat_types=type_output,
        missing_team_stat_type_ids=missing,
        player_stat_types=player_type_output,
        missing_player_stat_type_ids=missing_player_types,
        lineup_counts={str(team_id): count for team_id, count in lineup_counts.items()},
        player_stat_counts={str(team_id): count for team_id, count in player_stat_counts.items()},
        team_stat_count=team_stat_count,
        player_stat_count=player_stat_count,
        lineup_count=lineup_count,
        error=assessment_error,
    )


def stable_provider_sparse_assessment(
    assessment: ProviderAssessment,
    stable_fetch_count: int,
) -> ProviderAssessment | None:
    """Terminally classify a stable, structurally usable provider gap.

    SportMonks sometimes returns a finished fixture with team statistics,
    lineups, and player details, but omits identity/details for a subset of
    lineup rows.  That is provider-sparse data, not an ingestion failure. Two
    identical provider payloads are required before accepting the sparse
    revision so a temporary post-match response cannot shrink the serving
    facts.
    """
    sparse_optional_gap = assessment.status == "provider_sparse" and any(
        missing for missing in assessment.missing_team_stat_type_ids.values()
    )
    identity_gap = (
        assessment.status == "provider_pending"
        and (assessment.error or "").startswith(
            "provider lineup/player identity detail incomplete"
        )
    )
    if (
        not (sparse_optional_gap or identity_gap)
        or not assessment.finished
        or assessment.fixture_status not in FINISHED_STATUSES
        or assessment.team_stat_count <= 0
        or assessment.player_stat_count <= 0
        or assessment.lineup_count <= 0
        or stable_fetch_count < 2
        or any(count <= 0 for count in assessment.lineup_counts.values())
        or any(count <= 0 for count in assessment.player_stat_counts.values())
    ):
        return None
    return replace(
        assessment,
        status="provider_sparse",
        error=(
            f"{assessment.error or 'optional provider stat types are absent'}; provider-sparse classification confirmed "
            f"after {stable_fetch_count} identical finished payloads"
        ),
    )


def is_non_competitive_provider_assessment(assessment: ProviderAssessment) -> bool:
    """Return true only for explicit provider statuses that cannot be played."""
    return (
        assessment.status == "provider_pending"
        and assessment.fixture_status in NON_COMPETITIVE_STATUSES
    )


def _numeric(value: object) -> int | float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return int(parsed) if parsed.is_integer() else parsed


def source_snapshot(conn: sqlite3.Connection, fixture_id: int) -> DetailSnapshot:
    team_types: dict[str, set[int]] = {}
    team_values: dict[str, int | float | None] = {}
    player_values: dict[str, int | float | None] = {}
    lineup_values: dict[str, tuple[object, ...]] = {}

    rows = conn.execute(
        "select team_id, type_id, max(value) as value from fixture_statistics where fixture_id=? group by team_id,type_id",
        (fixture_id,),
    ).fetchall()
    for row in rows:
        team_id, type_id = _int(row[0]), _int(row[1])
        if team_id is None or type_id is None:
            continue
        team_types.setdefault(str(team_id), set()).add(type_id)
        team_values[f"{team_id}:{type_id}"] = _numeric(row[2])

    rows = conn.execute(
        "select player_id, team_id, type_id, max(value) as value from fixture_player_statistics where fixture_id=? group by player_id,team_id,type_id",
        (fixture_id,),
    ).fetchall()
    for row in rows:
        player_id, team_id, type_id = _int(row[0]), _int(row[1]), _int(row[2])
        if player_id is None or type_id is None:
            continue
        key = f"{player_id}:{team_id or 0}:{type_id}"
        player_values[key] = _numeric(row[3])

    rows = conn.execute(
        "select player_id, team_id, is_starter, minutes_played from fixture_players where fixture_id=?",
        (fixture_id,),
    ).fetchall()
    for row in rows:
        player_id = _int(row[0])
        if player_id is None:
            continue
        key = f"{player_id}:{_int(row[1]) or 0}"
        lineup_values[key] = (row[2], row[3])

    team_stat_count = len(team_values)
    player_stat_count = len(player_values)
    return DetailSnapshot(
        fixture_id=fixture_id,
        team_stat_count=team_stat_count,
        player_stat_count=player_stat_count,
        lineup_count=len(lineup_values),
        team_stat_types={key: sorted(value) for key, value in team_types.items()},
        team_stat_values=team_values,
        player_stat_values=player_values,
        lineup_values=lineup_values,
    )


def source_ready(snapshot: DetailSnapshot, assessment: ProviderAssessment) -> bool:
    for team_id in assessment.team_stat_types:
        # Team-stat rows are per-metric optional. A finished provider payload
        # can legitimately contain no team-stat rows at all, in which case
        # every tracked team metric remains NULL and is inventoried as sparse.
        # Lineups and player details remain mandatory because this delivery
        # pipeline also owns the player-stat read model.
        if snapshot.team_stat_types.get(team_id) and not any(
            key.startswith(f"{team_id}:") for key in snapshot.team_stat_values
        ):
            return False
    if snapshot.lineup_count <= 0 or snapshot.player_stat_count <= 0:
        return False
    return all(
        sum(1 for key in snapshot.lineup_values if key.endswith(f":{team_id}")) > 0
        for team_id in assessment.team_stat_types
    ) and all(
        sum(1 for key in snapshot.player_stat_values if key.split(":")[1] == str(team_id)) > 0
        for team_id in assessment.team_stat_types
    )


def target_snapshot(target_conn: Any, fixture_id: int) -> DetailSnapshot:
    team_types: dict[str, set[int]] = {}
    team_values: dict[str, int | float | None] = {}
    player_values: dict[str, int | float | None] = {}
    lineup_values: dict[str, tuple[object, ...]] = {}
    with target_conn.cursor() as cur:
        cur.execute(
            "select team_id, type_id, max(value) from public.fixture_statistics where fixture_id=%s group by team_id,type_id",
            (fixture_id,),
        )
        for row in cur.fetchall():
            team_id, type_id = _int(row[0]), _int(row[1])
            if team_id is None or type_id is None:
                continue
            team_types.setdefault(str(team_id), set()).add(type_id)
            team_values[f"{team_id}:{type_id}"] = _numeric(row[2])
        cur.execute(
            "select player_id, team_id, type_id, max(value) from public.fixture_player_statistics where fixture_id=%s group by player_id,team_id,type_id",
            (fixture_id,),
        )
        for row in cur.fetchall():
            player_id, team_id, type_id = _int(row[0]), _int(row[1]), _int(row[2])
            if player_id is None or type_id is None:
                continue
            player_values[f"{player_id}:{team_id or 0}:{type_id}"] = _numeric(row[3])
        cur.execute(
            "select player_id, team_id, is_starter, minutes_played from public.fixture_players where fixture_id=%s",
            (fixture_id,),
        )
        for row in cur.fetchall():
            player_id = _int(row[0])
            if player_id is None:
                continue
            lineup_values[f"{player_id}:{_int(row[1]) or 0}"] = (row[2], row[3])
    return DetailSnapshot(
        fixture_id=fixture_id,
        team_stat_count=len(team_values),
        player_stat_count=len(player_values),
        lineup_count=len(lineup_values),
        team_stat_types={key: sorted(value) for key, value in team_types.items()},
        team_stat_values=team_values,
        player_stat_values=player_values,
        lineup_values=lineup_values,
    )


def compare_snapshots(source: DetailSnapshot, target: DetailSnapshot) -> list[str]:
    problems: list[str] = []
    if source.team_stat_values != target.team_stat_values:
        missing = sorted(set(source.team_stat_values) - set(target.team_stat_values))
        extra = sorted(set(target.team_stat_values) - set(source.team_stat_values))
        changed = sorted(
            key for key in set(source.team_stat_values).intersection(target.team_stat_values)
            if source.team_stat_values[key] != target.team_stat_values[key]
        )
        if missing:
            problems.append(f"target missing team-stat keys: {missing[:20]}")
        if extra:
            problems.append(f"target has extra team-stat keys: {extra[:20]}")
        if changed:
            problems.append(f"team-stat values differ: {changed[:20]}")
    if source.player_stat_values != target.player_stat_values:
        problems.append(
            "player-stat parity differs "
            f"(source={len(source.player_stat_values)} target={len(target.player_stat_values)})"
        )
    if source.lineup_values != target.lineup_values:
        problems.append(
            "lineup parity differs "
            f"(source={len(source.lineup_values)} target={len(target.lineup_values)})"
        )
    return problems


def store_provider_detail(
    engine: Any,
    client: SportMonksClient,
    fixture_id: int,
    data: dict[str, Any],
    assessment: ProviderAssessment,
) -> DetailSnapshot:
    session = None
    try:
        from jxd.db import get_session

        session = get_session(engine)
        service = SyncService(client, session)
        service.ensure_schema()
        # The worker already persists row counts and parity evidence.  Avoid
        # emitting one log line per provider stat/player detail on every
        # 15-minute run.
        service._store_fixture_raw(data, log_changes=False, full_detail=True)
        session.flush()
        # Flush the SQLAlchemy session, then use its bind through a direct
        # connection so the validation sees exactly the rows being committed.
        snapshot = source_snapshot_from_session(session, fixture_id)
        if not source_ready(snapshot, assessment):
            raise ProviderDetailIncompleteError("provider payload was accepted but source detail rows are incomplete")
        session.commit()
        return snapshot
    except Exception:
        if session is not None:
            session.rollback()
        raise
    finally:
        if session is not None:
            session.close()


def source_snapshot_from_session(session: Any, fixture_id: int) -> DetailSnapshot:
    team_types: dict[str, set[int]] = {}
    team_values: dict[str, int | float | None] = {}
    player_values: dict[str, int | float | None] = {}
    lineup_values: dict[str, tuple[object, ...]] = {}
    for row in session.query(FixtureStatistic).filter(FixtureStatistic.fixture_id == fixture_id).all():
        team_id, type_id = _int(row.team_id), _int(row.type_id)
        if team_id is None or type_id is None:
            continue
        key = f"{team_id}:{type_id}"
        team_types.setdefault(str(team_id), set()).add(type_id)
        current = team_values.get(key)
        value = _numeric(row.value)
        if current is None or (value is not None and value > current):
            team_values[key] = value
    for row in session.query(FixturePlayerStatistic).filter(FixturePlayerStatistic.fixture_id == fixture_id).all():
        player_id, team_id, type_id = _int(row.player_id), _int(row.team_id), _int(row.type_id)
        if player_id is None or type_id is None:
            continue
        player_values[f"{player_id}:{team_id or 0}:{type_id}"] = _numeric(row.value)
    for row in session.query(FixturePlayer).filter(FixturePlayer.fixture_id == fixture_id).all():
        player_id = _int(row.player_id)
        if player_id is not None:
            lineup_values[f"{player_id}:{_int(row.team_id) or 0}"] = (row.is_starter, row.minutes_played)
    return DetailSnapshot(
        fixture_id=fixture_id,
        team_stat_count=len(team_values),
        player_stat_count=len(player_values),
        lineup_count=len(lineup_values),
        team_stat_types={key: sorted(value) for key, value in team_types.items()},
        team_stat_values=team_values,
        player_stat_values=player_values,
        lineup_values=lineup_values,
    )


def export_fixture(
    fixture_id: int,
    leagues: Sequence[int],
    report_path: str,
    snapshot_id: int | None = None,
) -> subprocess.CompletedProcess[str]:
    command = [
        sys.executable,
        str(Path(__file__).with_name("export_to_supabase.py")),
        "--strict",
        "--leagues",
        ",".join(str(value) for value in leagues),
        "--fixture-ids",
        str(fixture_id),
        "--protect-empty-detail",
        "--require-detail",
        "--atomic-fixture-detail",
        "--skip-odds-snapshots",
        "--skip-odds-outcomes",
        "--skip-prune",
        "--report-json",
        report_path,
    ]
    if snapshot_id is not None:
        command.extend(["--provider-snapshot-id", str(snapshot_id)])
    LOG.info("Exporting verified fixture detail: fixture_id=%s", fixture_id)
    return subprocess.run(command, text=True, capture_output=True, check=False)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--leagues", default=None)
    parser.add_argument("--fixture-ids", default=None, help="Explicit fixture IDs for repair/certification.")
    parser.add_argument(
        "--season-ids",
        default=None,
        help="Restrict target-queue selection to comma-separated season IDs.",
    )
    parser.add_argument("--hours-back", type=int, default=int(os.environ.get("POSTMATCH_DETAIL_HOURS_BACK", str(DEFAULT_HOURS_BACK))))
    parser.add_argument("--limit", type=int, default=int(os.environ.get("POSTMATCH_DETAIL_LIMIT", str(DEFAULT_LIMIT))))
    parser.add_argument("--grace-minutes", type=int, default=int(os.environ.get("POSTMATCH_DETAIL_GRACE_MINUTES", str(DEFAULT_GRACE_MINUTES))))
    parser.add_argument("--force", action="store_true", help="Reprocess fixtures even when the ledger says verified.")
    parser.add_argument(
        "--target-queue",
        action=argparse.BooleanOptionalAction,
        default=os.environ.get("POSTMATCH_DETAIL_TARGET_QUEUE", "1").lower() not in {"0", "false", "no"},
        help="Select missing/due completed fixtures from the serving database as well as the source queue.",
    )
    parser.add_argument("--no-fail-on-sla-breach", action="store_true")
    parser.add_argument("--report-json", default=None)
    return parser


def default_leagues() -> list[int]:
    for key in ("STATS_LEAGUES", "STATS_LEAGUE_IDS", "LEAGUE_IDS"):
        values = parse_csv_ints(os.environ.get(key))
        if values:
            return values
    path = Path("config/league_ids.txt")
    if path.exists():
        return parse_csv_ints(path.read_text(encoding="utf-8"))
    return []


def main() -> int:
    args = build_parser().parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    leagues = parse_csv_ints(args.leagues) if args.leagues else default_leagues()
    explicit_ids = parse_csv_ints(args.fixture_ids)
    if not leagues and not explicit_ids:
        raise SystemExit("No leagues or explicit fixture IDs selected")

    conn = source_connection()
    ensure_ledger(conn)
    target_url = os.environ.get("SUPABASE_DB_URL_SESSION") or os.environ.get("SUPABASE_DB_URL")
    if not target_url:
        raise SystemExit("SUPABASE_DB_URL_SESSION or SUPABASE_DB_URL is required for delivery verification")
    season_ids = parse_csv_ints(args.season_ids)
    if explicit_ids:
        fixture_ids = explicit_ids
    else:
        # A season-scoped run is an intentional historical drain. Do not let
        # unrelated recent source fixtures consume the bounded batch before
        # the requested target-season queue gets a turn. The normal live run
        # (without --season-ids) continues to combine source and target lanes.
        selected = []
        if args.target_queue:
            selected.extend(candidate_target_fixture_ids(target_url, leagues, args.limit, args.force, season_ids or None))
        if not season_ids:
            selected.extend(candidate_fixture_ids(conn, leagues, args.hours_back, args.limit, args.force))
        fixture_ids = list(dict.fromkeys(selected))
        excluded = excluded_target_fixture_ids(target_url, fixture_ids)
        fixture_ids = [fixture_id for fixture_id in fixture_ids if fixture_id not in excluded][: max(args.limit, 0)]
    target_metadata = target_fixture_metadata(target_url, fixture_ids)
    report: dict[str, Any] = {
        "release_id": release_id(),
        "leagues": leagues,
        "fixture_ids": fixture_ids,
        "fixtures_selected": len(fixture_ids),
        "verified": [],
        "provider_sparse": [],
        "provider_pending": [],
        "provider_unavailable": [],
        "excluded": [],
        "failed": [],
        "status_sync_failures": [],
        "sla_breaches": [],
        "provider_calls": 0,
        "projection_rows": 0,
        "projection_failures": [],
    }
    if not fixture_ids:
        report["status"] = "idle"
        if args.report_json:
            Path(args.report_json).write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(json.dumps(report))
        conn.close()
        return 0

    # Use an explicit SQLite engine so the sync session and the exporter read
    # the same source database even when JXD_DB_URL is configured for a
    # separate validation database.
    source_path = str(Path(SOURCE_DB).resolve())
    engine = source_engine(source_path)
    client = SportMonksClient()
    export_report_path = "/tmp/postmatch_fixture_detail_export_report.json"

    for fixture_id in fixture_ids:
        source_meta_row = conn.execute(
            "select league_id, season_id, starting_at from fixtures where id = ?",
            (fixture_id,),
        ).fetchone()
        target_meta = target_metadata.get(fixture_id)
        league_id = _int(source_meta_row[0]) if source_meta_row else (target_meta[0] if target_meta else None)
        season_id = _int(source_meta_row[1]) if source_meta_row else (target_meta[1] if target_meta else None)
        starting_at = source_meta_row[2] if source_meta_row else (target_meta[2] if target_meta else None)
        prior = conn.execute(
            f"select provider_team_stat_count, provider_player_stat_count, last_normalized_hash, stable_fetch_count "
            f"from {LEDGER_TABLE} where fixture_id = ?",
            (fixture_id,),
        ).fetchone()
        prior_team_count = int(prior[0] or 0) if prior else 0
        prior_player_count = int(prior[1] or 0) if prior else 0
        if target_meta:
            # A target-only fixture may have active facts from an earlier
            # pipeline even when its SQLite delivery ledger has no row. Use
            # the serving counts in the shrink guard so a sparse response
            # cannot delete richer facts on its first observation.
            prior_team_count = max(prior_team_count, target_meta[3])
            prior_player_count = max(prior_player_count, target_meta[4])
        prior_normalized_hash = str(prior[2]) if prior and prior[2] else None
        prior_stable_count = int(prior[3] or 0) if prior else 0
        attempt = ledger_attempt_start(conn, fixture_id, utc_now(), league_id, season_id)
        assessment: ProviderAssessment | None = None
        snapshot_id: int | None = None
        payload_hash: str | None = None
        normalized_hash: str | None = None
        try:
            payload = client.request(
                "GET",
                f"fixtures/{fixture_id}",
                params={
                    "include": ";".join(
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
            snapshot_id = persist_provider_snapshot(
                target_url,
                fixture_id,
                league_id,
                season_id,
                data,
                assessment,
                payload_hash,
                normalized_hash,
            )
            stable_fetch_count = prior_stable_count + 1 if prior_normalized_hash == normalized_hash else 1
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
                    reason_code="excluded",
                )
                report["excluded"].append({"fixture_id": fixture_id, "next_review_at": review_at, "reason": reason})
                try:
                    report["projection_rows"] += refresh_player_projection(target_url, fixture_id)
                except Exception as projection_exc:
                    report["projection_failures"].append({"fixture_id": fixture_id, "error": str(projection_exc)[-4000:]})
                    report["failed"].append({"fixture_id": fixture_id, "stage": "projection", "error": str(projection_exc)[-4000:]})
                continue
            terminal_sparse = stable_provider_sparse_assessment(assessment, stable_fetch_count)
            if terminal_sparse is not None:
                assessment = terminal_sparse
            elif assessment.status == "provider_sparse":
                assessment = replace(
                    assessment,
                    status="provider_pending",
                    error="optional provider stat types are absent; awaiting stable confirmation",
                )
            if assessment.status == "provider_pending":
                now = utc_now()
                next_at = backoff_time(attempt, now)
                update_ledger(
                    conn,
                    fixture_id,
                    assessment.status,
                    attempt,
                    assessment,
                    error=assessment.error,
                    next_attempt_at=next_at,
                    payload_hash=payload_hash,
                    normalized_hash=normalized_hash,
                    stable_fetch_count=stable_fetch_count,
                )
                report["provider_pending"].append({"fixture_id": fixture_id, "next_attempt_at": next_at, "assessment": asdict(assessment)})
                started = parse_iso(starting_at)
                if started and utc_now() - started > timedelta(minutes=max(args.grace_minutes, 0)):
                    report["sla_breaches"].append({"fixture_id": fixture_id, "status": assessment.status, "started_at": starting_at})
                continue

            # Never replace a richer accepted snapshot with a first, changed
            # response that has fewer team/player facts. A repeated identical
            # payload confirms a provider correction or a durable sparse
            # response; only then may the active facts shrink. This applies to
            # provider-sparse payloads as well as complete payloads.
            candidate_shrank = (
                (prior_team_count > 0 and assessment.team_stat_count < prior_team_count)
                or (prior_player_count > 0 and assessment.player_stat_count < prior_player_count)
            )
            if (
                not args.force
                and candidate_shrank
                and prior_normalized_hash != normalized_hash
                and stable_fetch_count < 2
            ):
                now = utc_now()
                next_at = backoff_time(attempt, now)
                message = (
                    "provider detail collection shrank "
                    f"(team_stats {prior_team_count}->{assessment.team_stat_count}, "
                    f"player_stats {prior_player_count}->{assessment.player_stat_count}); "
                    "awaiting one identical confirmation fetch"
                )
                update_ledger(
                    conn,
                    fixture_id,
                    "provider_pending",
                    attempt,
                    assessment,
                    error=message,
                    next_attempt_at=next_at,
                    payload_hash=payload_hash,
                    normalized_hash=normalized_hash,
                    stable_fetch_count=stable_fetch_count,
                )
                report["provider_pending"].append({"fixture_id": fixture_id, "next_attempt_at": next_at, "reason": message})
                continue

            source = store_provider_detail(engine, client, fixture_id, data, assessment)
            clear_provider_unavailable_exclusion(target_url, fixture_id)
            export_result = export_fixture(
                fixture_id,
                leagues,
                export_report_path,
                snapshot_id=snapshot_id,
            )
            if export_result.returncode != 0:
                message = (export_result.stderr or export_result.stdout or "export failed")[-4000:]
                update_ledger(conn, fixture_id, "export_failed", attempt, assessment, source=source, error=message, next_attempt_at=backoff_time(attempt, utc_now()))
                report["failed"].append({"fixture_id": fixture_id, "stage": "export", "error": message})
                continue

            target_conn = psycopg2.connect(target_url, connect_timeout=20)
            try:
                target = target_snapshot(target_conn, fixture_id)
            finally:
                target_conn.close()
            problems = compare_snapshots(source, target)
            if problems:
                message = "; ".join(problems)
                update_ledger(conn, fixture_id, "verification_failed", attempt, assessment, source=source, target=target, error=message, next_attempt_at=backoff_time(attempt, utc_now()))
                report["failed"].append({"fixture_id": fixture_id, "stage": "verification", "error": message})
                continue

            try:
                report["projection_rows"] += refresh_player_projection(target_url, fixture_id)
            except Exception as projection_exc:
                message = str(projection_exc)[-4000:]
                update_ledger(
                    conn,
                    fixture_id,
                    "projection_failed",
                    attempt,
                    assessment,
                    source=source,
                    target=target,
                    error=message,
                    next_attempt_at=backoff_time(attempt, utc_now()),
                )
                report["projection_failures"].append({"fixture_id": fixture_id, "error": message})
                report["failed"].append({"fixture_id": fixture_id, "stage": "projection", "error": message})
                continue

            if snapshot_id is None:
                raise RuntimeError(f"No provider snapshot ID for accepted fixture {fixture_id}")
            activate_provider_snapshot(target_url, fixture_id, snapshot_id)
            final_status = "provider_sparse" if assessment.status == "provider_sparse" else "verified"
            next_revalidation_at = revalidation_time(
                starting_at,
                utc_now(),
            )
            update_ledger(
                conn,
                fixture_id,
                final_status,
                attempt,
                assessment,
                source=source,
                target=target,
                next_attempt_at=None,
                successful=True,
                payload_hash=payload_hash,
                normalized_hash=normalized_hash,
                accepted_snapshot_id=snapshot_id,
                stable_fetch_count=stable_fetch_count,
                next_revalidation_at=next_revalidation_at,
            )
            if final_status == "provider_sparse":
                report["provider_sparse"].append({"fixture_id": fixture_id, "assessment": asdict(assessment), "next_revalidation_at": next_revalidation_at})
            else:
                report["verified"].append({"fixture_id": fixture_id, "assessment": asdict(assessment), "next_revalidation_at": next_revalidation_at})
        except ProviderFixtureUnavailableError as exc:
            message = str(exc)[-4000:]
            review_at = mark_provider_unavailable(target_url, conn, fixture_id, attempt, message)
            report["provider_unavailable"].append({"fixture_id": fixture_id, "next_review_at": review_at, "reason": message})
            LOG.warning("Provider returned no fixture for %s; quarantined until %s", fixture_id, review_at)
        except ProviderDetailIncompleteError as exc:
            message = str(exc)[-4000:]
            next_at = backoff_time(attempt, utc_now())
            update_ledger(
                conn,
                fixture_id,
                "provider_pending",
                attempt,
                assessment,
                error=message,
                next_attempt_at=next_at,
                payload_hash=payload_hash,
                normalized_hash=normalized_hash,
            )
            report["provider_pending"].append({"fixture_id": fixture_id, "next_attempt_at": next_at, "reason": message})
            LOG.warning("Provider detail incomplete for %s; retaining it in pending state", fixture_id)
        except Exception as exc:  # provider and storage errors are retried by the ledger
            message = str(exc)[-4000:]
            update_ledger(conn, fixture_id, "failed", attempt, assessment, error=message, next_attempt_at=backoff_time(attempt, utc_now()))
            report["failed"].append({"fixture_id": fixture_id, "stage": "fetch_or_store", "error": message})
            LOG.exception("Fixture detail delivery failed for %s", fixture_id)
        finally:
            try:
                publish_delivery_status(target_url, conn, fixture_id)
            except Exception as status_exc:
                message = f"delivery status projection failed: {status_exc}"[-4000:]
                report["status_sync_failures"].append({"fixture_id": fixture_id, "error": message})
                if not any(item.get("fixture_id") == fixture_id for item in report["failed"]):
                    report["failed"].append({"fixture_id": fixture_id, "stage": "status_projection", "error": message})
                try:
                    update_ledger(
                        conn,
                        fixture_id,
                        "failed",
                        attempt,
                        assessment,
                        error=message,
                        next_attempt_at=backoff_time(attempt, utc_now()),
                    )
                except Exception:
                    LOG.exception("Could not record status projection failure for %s", fixture_id)
                LOG.exception("Delivery status projection failed for %s", fixture_id)

    report["status"] = "failed" if report["failed"] else "success"
    if report["sla_breaches"] and not args.no_fail_on_sla_breach:
        report["status"] = "sla_breach"
    if args.report_json:
        path = Path(args.report_json)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    print(json.dumps(report, default=str))
    conn.close()
    engine.dispose()
    return 1 if report["status"] in {"failed", "sla_breach"} else 0


if __name__ == "__main__":
    raise SystemExit(main())
