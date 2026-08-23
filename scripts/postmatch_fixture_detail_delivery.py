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
import os
import sqlite3
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence

import psycopg2
from sqlalchemy import create_engine

from jxd import SportMonksClient, SyncService
from jxd.models import FixturePlayer, FixturePlayerStatistic, FixtureStatistic


LOG = logging.getLogger("postmatch_fixture_detail_delivery")
UTC = timezone.utc
SOURCE_DB = os.environ.get("JXD_DB_PATH", "data/jxd.sqlite")
FINISHED_STATUSES = {"FT", "AET", "PEN", "FT_PEN", "FINISHED", "ENDED"}

# These are the provider-backed metrics consumed by the Team Stats surface.
# Big Chances Missed (581) is known to be legitimately absent for some
# fixtures, so it is handled as provider-sparse rather than an ingestion
# failure. Card subtypes can also be absent when no cards of that subtype were
# recorded and are intentionally not delivery gates.
REQUIRED_TEAM_STAT_TYPES = {42, 45, 56, 57, 78, 86, 100, 109, 581}
PROVIDER_SPARSE_TEAM_STAT_TYPES = {581}

LEDGER_TABLE = "fixture_detail_deliveries"
DEFAULT_HOURS_BACK = 72
DEFAULT_LIMIT = 25
DEFAULT_GRACE_MINUTES = 60
DEFAULT_SOURCE_BUSY_TIMEOUT_MS = 30_000


@dataclass(frozen=True)
class ProviderAssessment:
    status: str
    fixture_status: str | None
    finished: bool
    team_stat_types: dict[str, list[int]]
    missing_required_type_ids: dict[str, list[int]]
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
          source_snapshot text,
          target_snapshot text,
          last_error text,
          release_id text,
          updated_at text not null
        )
        """
    )
    conn.commit()


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
        select f.id, f.starting_at, d.status, d.next_attempt_at, d.updated_at
          from fixtures f
          left join {LEDGER_TABLE} d on d.fixture_id = f.id
         where f.starting_at >= ?
           and f.starting_at <= ?
           {league_clause}
         order by f.starting_at asc, f.id asc
        """,
        params,
    ).fetchall()
    selected: list[int] = []
    now = utc_now()
    for row in rows:
        status = str(row[2] or "new")
        if not force and status == "verified":
            continue
        if not force and status == "running":
            running_at = parse_iso(row[4])
            if running_at and now - running_at < timedelta(minutes=30):
                continue
        if not force and status == "provider_sparse" and not due(row[3], now):
            continue
        if not force and row[3] is not None and not due(row[3], now):
            continue
        selected.append(int(row[0]))
        if len(selected) >= max(limit, 0):
            break
    return selected


def ledger_attempt_start(conn: sqlite3.Connection, fixture_id: int, now: datetime) -> int:
    timestamp = iso(now)
    conn.execute(
        f"""
        insert into {LEDGER_TABLE} (
          fixture_id, status, attempts, first_seen_at, last_attempted_at,
          next_attempt_at, release_id, updated_at
        ) values (?, 'running', 1, ?, ?, null, ?, ?)
        on conflict(fixture_id) do update set
          status='running',
          attempts={LEDGER_TABLE}.attempts + 1,
          last_attempted_at=excluded.last_attempted_at,
          next_attempt_at=null,
          last_error=null,
          release_id=excluded.release_id,
          updated_at=excluded.updated_at
        """,
        (fixture_id, timestamp, timestamp, release_id(), timestamp),
    )
    row = conn.execute(
        f"select attempts from {LEDGER_TABLE} where fixture_id = ?", (fixture_id,)
    ).fetchone()
    conn.commit()
    return int(row[0]) if row else 1


def backoff_time(attempt: int, now: datetime) -> str:
    minutes = (15, 30, 60, 180, 360, 720, 1440)[min(max(attempt - 1, 0), 6)]
    return iso(now + timedelta(minutes=minutes))


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
) -> None:
    now = iso(utc_now())
    provider = assessment
    conn.execute(
        f"""
        update {LEDGER_TABLE}
           set status = ?,
               attempts = ?,
               next_attempt_at = ?,
               last_successful_at = case when ? then ? else last_successful_at end,
               provider_status = ?,
               provider_finished = ?,
               provider_team_stat_count = ?,
               provider_player_stat_count = ?,
               provider_lineup_count = ?,
               provider_team_stat_types = ?,
               provider_missing_type_ids = ?,
               source_snapshot = ?,
               target_snapshot = ?,
               last_error = ?,
               release_id = ?,
               updated_at = ?
         where fixture_id = ?
        """,
        (
            status,
            attempt,
            next_attempt_at,
            1 if successful else 0,
            now,
            provider.fixture_status if provider else None,
            1 if provider and provider.finished else 0,
            provider.team_stat_count if provider else 0,
            provider.player_stat_count if provider else 0,
            provider.lineup_count if provider else 0,
            json_text(provider.team_stat_types) if provider else None,
            json_text(provider.missing_required_type_ids) if provider else None,
            json_text(asdict(source)) if source else None,
            json_text(asdict(target)) if target else None,
            (error or (provider.error if provider else None) or "")[:4000] or None,
            release_id(),
            now,
            fixture_id,
        ),
    )
    conn.commit()


def _int(value: object) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


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
    player_stat_counts: dict[int, int] = {team_id: 0 for team_id in teams}
    lineup_count = 0
    player_stat_count = 0
    for lineup in lineups_list:
        if not isinstance(lineup, dict):
            continue
        team_id = _int(lineup.get("team_id") or lineup.get("participant_id") or (lineup.get("team") or {}).get("id"))
        if not team_id:
            continue
        lineup_counts[team_id] = lineup_counts.get(team_id, 0) + 1
        lineup_count += 1
        for detail in lineup.get("details") or []:
            if not isinstance(detail, dict):
                continue
            type_id = _int(detail.get("type_id") or (detail.get("type") or {}).get("id"))
            if type_id:
                player_stat_counts[team_id] = player_stat_counts.get(team_id, 0) + 1
                player_stat_count += 1

    type_output = {str(team_id): sorted(types) for team_id, types in team_types.items()}
    missing: dict[str, list[int]] = {}
    for team_id in teams:
        missing[str(team_id)] = sorted(REQUIRED_TEAM_STAT_TYPES.difference(team_types.get(team_id, set())))

    if not finished or len(teams) < 2:
        assessment_status = "provider_pending"
    elif not stats_list or not lineups_list or any(
        lineup_counts.get(team_id, 0) <= 0 or player_stat_counts.get(team_id, 0) <= 0 for team_id in teams
    ):
        assessment_status = "provider_pending"
    elif any(
        set(missing.get(str(team_id), [])) - PROVIDER_SPARSE_TEAM_STAT_TYPES for team_id in teams
    ):
        assessment_status = "provider_pending"
    elif any(missing.get(str(team_id)) for team_id in teams):
        assessment_status = "provider_sparse"
    else:
        assessment_status = "ready"

    return ProviderAssessment(
        status=assessment_status,
        fixture_status=status,
        finished=finished,
        team_stat_types=type_output,
        missing_required_type_ids=missing,
        lineup_counts={str(team_id): count for team_id, count in lineup_counts.items()},
        player_stat_counts={str(team_id): count for team_id, count in player_stat_counts.items()},
        team_stat_count=team_stat_count,
        player_stat_count=player_stat_count,
        lineup_count=lineup_count,
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
        types = set(snapshot.team_stat_types.get(team_id, []))
        missing = REQUIRED_TEAM_STAT_TYPES.difference(types) - PROVIDER_SPARSE_TEAM_STAT_TYPES
        if missing:
            return False
        if not any(key.startswith(f"{team_id}:") for key in snapshot.team_stat_values):
            return False
    if snapshot.lineup_count <= 0 or snapshot.player_stat_count <= 0:
        return False
    return all(
        sum(1 for key in snapshot.lineup_values if key.endswith(f":{team_id}")) > 0
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
            raise RuntimeError("provider payload was accepted but source detail rows are incomplete")
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


def export_fixture(fixture_id: int, leagues: Sequence[int], report_path: str) -> subprocess.CompletedProcess[str]:
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
        "--skip-odds-snapshots",
        "--skip-odds-outcomes",
        "--skip-prune",
        "--report-json",
        report_path,
    ]
    LOG.info("Exporting verified fixture detail: fixture_id=%s", fixture_id)
    return subprocess.run(command, text=True, capture_output=True, check=False)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--leagues", default=None)
    parser.add_argument("--fixture-ids", default=None, help="Explicit fixture IDs for repair/certification.")
    parser.add_argument("--hours-back", type=int, default=int(os.environ.get("POSTMATCH_DETAIL_HOURS_BACK", str(DEFAULT_HOURS_BACK))))
    parser.add_argument("--limit", type=int, default=int(os.environ.get("POSTMATCH_DETAIL_LIMIT", str(DEFAULT_LIMIT))))
    parser.add_argument("--grace-minutes", type=int, default=int(os.environ.get("POSTMATCH_DETAIL_GRACE_MINUTES", str(DEFAULT_GRACE_MINUTES))))
    parser.add_argument("--force", action="store_true", help="Reprocess fixtures even when the ledger says verified.")
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
    fixture_ids = explicit_ids or candidate_fixture_ids(conn, leagues, args.hours_back, args.limit, args.force)
    report: dict[str, Any] = {
        "release_id": release_id(),
        "leagues": leagues,
        "fixture_ids": fixture_ids,
        "fixtures_selected": len(fixture_ids),
        "verified": [],
        "provider_sparse": [],
        "provider_pending": [],
        "failed": [],
        "sla_breaches": [],
        "provider_calls": 0,
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
    engine = create_engine(f"sqlite:///{source_path}", future=True)
    client = SportMonksClient()
    target_url = os.environ.get("SUPABASE_DB_URL_SESSION") or os.environ.get("SUPABASE_DB_URL")
    if not target_url:
        raise SystemExit("SUPABASE_DB_URL_SESSION or SUPABASE_DB_URL is required for delivery verification")
    export_report_path = "/tmp/postmatch_fixture_detail_export_report.json"

    for fixture_id in fixture_ids:
        attempt = ledger_attempt_start(conn, fixture_id, utc_now())
        assessment: ProviderAssessment | None = None
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
                raise RuntimeError("SportMonks returned no fixture data")
            assessment = assess_provider_payload(data)
            if assessment.status == "provider_pending":
                now = utc_now()
                next_at = backoff_time(attempt, now)
                update_ledger(conn, fixture_id, assessment.status, attempt, assessment, error=assessment.error, next_attempt_at=next_at)
                report["provider_pending"].append({"fixture_id": fixture_id, "next_attempt_at": next_at, "assessment": asdict(assessment)})
                starting_at = conn.execute("select starting_at from fixtures where id=?", (fixture_id,)).fetchone()
                started = parse_iso(starting_at[0]) if starting_at else None
                if started and utc_now() - started > timedelta(minutes=max(args.grace_minutes, 0)):
                    report["sla_breaches"].append({"fixture_id": fixture_id, "status": assessment.status, "started_at": starting_at[0]})
                continue

            source = store_provider_detail(engine, client, fixture_id, data, assessment)
            export_result = export_fixture(fixture_id, leagues, export_report_path)
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

            final_status = "provider_sparse" if assessment.status == "provider_sparse" else "verified"
            next_at = iso(utc_now() + timedelta(hours=24)) if final_status == "provider_sparse" else None
            update_ledger(conn, fixture_id, final_status, attempt, assessment, source=source, target=target, next_attempt_at=next_at, successful=True)
            if final_status == "provider_sparse":
                report["provider_sparse"].append({"fixture_id": fixture_id, "assessment": asdict(assessment)})
            else:
                report["verified"].append({"fixture_id": fixture_id, "assessment": asdict(assessment)})
        except Exception as exc:  # provider and storage errors are retried by the ledger
            message = str(exc)[-4000:]
            update_ledger(conn, fixture_id, "failed", attempt, assessment, error=message, next_attempt_at=backoff_time(attempt, utc_now()))
            report["failed"].append({"fixture_id": fixture_id, "stage": "fetch_or_store", "error": message})
            LOG.exception("Fixture detail delivery failed for %s", fixture_id)

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
