#!/usr/bin/env python3
"""Verify and repair fixture-detail delivery from JXD SQLite to Supabase.

The normal odds/stat export is intentionally date-windowed.  That is unsafe for
historical fixture-detail rows: a later source backfill can succeed locally
without ever being published to Supabase.  This job compares the two stores by
fixture and republishes only source-complete mismatches.

It is safe to run repeatedly.  Provider refreshes for source-sparse fixtures
are bounded and rate-limited by a small local attempt ledger; empty provider
payloads never cause production detail rows to be deleted.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import subprocess
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import psycopg2

from jxd import SportMonksClient, SyncService, get_engine, get_session


LOG = logging.getLogger("verify_fixture_detail_delivery")
UTC = timezone.utc
DEFAULT_COOLDOWN_HOURS = 24
DEFAULT_SOURCE_BUSY_TIMEOUT_MS = 30_000


@dataclass(frozen=True)
class DetailCounts:
    fixture_id: int
    team_id: int
    lineup_rows: int = 0
    appearances: int = 0
    player_stat_rows: int = 0
    team_stat_rows: int = 0


@dataclass(frozen=True)
class FixtureRecord:
    fixture_id: int
    league_id: int
    season_id: int | None
    starting_at: str | None
    home_team_id: int
    away_team_id: int


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


def default_leagues() -> list[int]:
    for key in ("STATS_LEAGUES", "STATS_LEAGUE_IDS", "LEAGUE_IDS"):
        values = parse_csv_ints(os.environ.get(key))
        if values:
            return values
    path = Path("config/league_ids.txt")
    if path.exists():
        return parse_csv_ints(path.read_text(encoding="utf-8"))
    raise SystemExit("No supported league IDs were configured")


def chunked(values: list[int], size: int) -> Iterable[list[int]]:
    for start in range(0, len(values), max(size, 1)):
        yield values[start : start + max(size, 1)]


def source_db_path() -> str:
    return os.environ.get("JXD_DB_PATH", "data/jxd.sqlite")


def open_source() -> sqlite3.Connection:
    conn = sqlite3.connect(
        source_db_path(),
        timeout=max(float(os.environ.get("JXD_SQLITE_BUSY_TIMEOUT_SECONDS", "30")), 1.0),
    )
    conn.execute(
        "pragma busy_timeout = "
        + str(
            max(
                int(
                    os.environ.get(
                        "JXD_SQLITE_BUSY_TIMEOUT_MS",
                        str(DEFAULT_SOURCE_BUSY_TIMEOUT_MS),
                    )
                ),
                1000,
            )
        )
    )
    conn.row_factory = sqlite3.Row
    return conn


def ensure_attempt_ledger(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        create table if not exists fixture_detail_delivery_attempts (
          fixture_id integer primary key,
          last_attempted_at text not null,
          attempts integer not null default 0,
          last_status text not null,
          last_error text null
        )
        """
    )
    conn.commit()


def load_fixtures(
    conn: sqlite3.Connection,
    leagues: list[int],
    season_ids: list[int] | None = None,
) -> dict[int, FixtureRecord]:
    placeholders = ",".join("?" for _ in leagues)
    season_clause = ""
    params: list[object] = list(leagues)
    if season_ids:
        season_placeholders = ",".join("?" for _ in season_ids)
        season_clause = f" and season_id in ({season_placeholders})"
        params.extend(season_ids)
    rows = conn.execute(
        f"""
        select id, league_id, season_id, starting_at, home_team_id, away_team_id
         from fixtures
         where league_id in ({placeholders})
           {season_clause}
           and home_team_id is not null
           and away_team_id is not null
           and starting_at <= datetime('now', '-3 hours')
         order by starting_at desc, id desc
        """,
        params,
    ).fetchall()
    return {
        int(row["id"]): FixtureRecord(
            fixture_id=int(row["id"]),
            league_id=int(row["league_id"]),
            season_id=int(row["season_id"]) if row["season_id"] is not None else None,
            starting_at=row["starting_at"],
            home_team_id=int(row["home_team_id"]),
            away_team_id=int(row["away_team_id"]),
        )
        for row in rows
    }


def empty_counts(fixture_ids: Iterable[int], fixture_map: dict[int, FixtureRecord]) -> dict[tuple[int, int], DetailCounts]:
    result: dict[tuple[int, int], DetailCounts] = {}
    for fixture_id in fixture_ids:
        fixture = fixture_map[fixture_id]
        for team_id in (fixture.home_team_id, fixture.away_team_id):
            result[(fixture_id, team_id)] = DetailCounts(fixture_id, team_id)
    return result


def load_source_counts(
    conn: sqlite3.Connection,
    fixture_ids: list[int],
    fixture_map: dict[int, FixtureRecord],
) -> dict[tuple[int, int], DetailCounts]:
    counts = empty_counts(fixture_ids, fixture_map)
    if not fixture_ids:
        return counts
    placeholders = ",".join("?" for _ in fixture_ids)

    for row in conn.execute(
        f"""
        select fixture_id, team_id, count(*) as lineup_rows,
               sum(case when coalesce(minutes_played, 0) > 0 then 1 else 0 end) as appearances
          from fixture_players
         where fixture_id in ({placeholders})
         group by fixture_id, team_id
        """,
        fixture_ids,
    ):
        key = (int(row["fixture_id"]), int(row["team_id"]))
        if key in counts:
            counts[key] = DetailCounts(
                counts[key].fixture_id,
                counts[key].team_id,
                int(row["lineup_rows"] or 0),
                int(row["appearances"] or 0),
                counts[key].player_stat_rows,
                counts[key].team_stat_rows,
            )

    for row in conn.execute(
        f"""
        select fixture_id, team_id, count(distinct player_id || ':' || type_id) as player_stat_rows
          from fixture_player_statistics
         where fixture_id in ({placeholders})
         group by fixture_id, team_id
        """,
        fixture_ids,
    ):
        key = (int(row["fixture_id"]), int(row["team_id"]))
        if key in counts:
            counts[key] = DetailCounts(
                counts[key].fixture_id,
                counts[key].team_id,
                counts[key].lineup_rows,
                counts[key].appearances,
                int(row["player_stat_rows"] or 0),
                counts[key].team_stat_rows,
            )

    for row in conn.execute(
        f"""
        select fixture_id, team_id, count(distinct type_id) as team_stat_rows
          from fixture_statistics
         where fixture_id in ({placeholders})
         group by fixture_id, team_id
        """,
        fixture_ids,
    ):
        key = (int(row["fixture_id"]), int(row["team_id"]))
        if key in counts:
            counts[key] = DetailCounts(
                counts[key].fixture_id,
                counts[key].team_id,
                counts[key].lineup_rows,
                counts[key].appearances,
                counts[key].player_stat_rows,
                int(row["team_stat_rows"] or 0),
            )
    return counts


def target_counts(
    conn: Any,
    fixture_ids: list[int],
    fixture_map: dict[int, FixtureRecord],
) -> dict[tuple[int, int], DetailCounts]:
    counts = empty_counts(fixture_ids, fixture_map)
    if not fixture_ids:
        return counts
    array = "{" + ",".join(str(value) for value in fixture_ids) + "}"
    queries = (
        (
            """
            select fixture_id, team_id, count(*) as lineup_rows,
                   count(*) filter (where coalesce(minutes_played, 0) > 0) as appearances
              from public.fixture_players
             where fixture_id = any(%s::bigint[])
             group by fixture_id, team_id
            """,
            "lineup",
        ),
        (
            """
            select fixture_id, team_id, count(distinct (player_id, type_id)) as player_stat_rows
              from public.fixture_player_statistics
             where fixture_id = any(%s::bigint[])
             group by fixture_id, team_id
            """,
            "player",
        ),
        (
            """
            select fixture_id, team_id, count(distinct type_id) as team_stat_rows
              from public.fixture_statistics
             where fixture_id = any(%s::bigint[])
             group by fixture_id, team_id
            """,
            "team",
        ),
    )
    for query, kind in queries:
        with conn.cursor() as cur:
            cur.execute(query, (array,))
            for row in cur.fetchall():
                key = (int(row[0]), int(row[1]))
                if key not in counts:
                    continue
                current = counts[key]
                if kind == "lineup":
                    counts[key] = DetailCounts(current.fixture_id, current.team_id, int(row[2]), int(row[3]), current.player_stat_rows, current.team_stat_rows)
                elif kind == "player":
                    counts[key] = DetailCounts(current.fixture_id, current.team_id, current.lineup_rows, current.appearances, int(row[2]), current.team_stat_rows)
                else:
                    counts[key] = DetailCounts(current.fixture_id, current.team_id, current.lineup_rows, current.appearances, current.player_stat_rows, int(row[2]))
    return counts


def source_is_complete(fixture: FixtureRecord, counts: dict[tuple[int, int], DetailCounts]) -> bool:
    return all(
        counts[(fixture.fixture_id, team_id)].lineup_rows > 0
        and counts[(fixture.fixture_id, team_id)].player_stat_rows > 0
        and counts[(fixture.fixture_id, team_id)].team_stat_rows > 0
        for team_id in (fixture.home_team_id, fixture.away_team_id)
    )


def differs(source: DetailCounts, target: DetailCounts) -> bool:
    return (
        source.lineup_rows != target.lineup_rows
        or source.appearances != target.appearances
        or source.player_stat_rows != target.player_stat_rows
        or source.team_stat_rows != target.team_stat_rows
    )


def classify(
    fixtures: dict[int, FixtureRecord],
    source: dict[tuple[int, int], DetailCounts],
    target: dict[tuple[int, int], DetailCounts],
) -> tuple[list[int], list[int], list[int]]:
    complete_gaps: list[int] = []
    source_sparse: list[int] = []
    healthy: list[int] = []
    for fixture_id, fixture in fixtures.items():
        source_complete = source_is_complete(fixture, source)
        target_gap = any(
            differs(source[(fixture_id, team_id)], target[(fixture_id, team_id)])
            for team_id in (fixture.home_team_id, fixture.away_team_id)
        )
        if not source_complete:
            source_sparse.append(fixture_id)
        elif target_gap:
            complete_gaps.append(fixture_id)
        else:
            healthy.append(fixture_id)
    return complete_gaps, source_sparse, healthy


def eligible_provider_refresh(
    conn: sqlite3.Connection,
    fixture_ids: list[int],
    limit: int,
    cooldown_hours: int,
) -> list[int]:
    if limit <= 0 or not fixture_ids:
        return []
    cutoff = datetime.now(UTC) - timedelta(hours=max(cooldown_hours, 0))
    selected: list[int] = []
    placeholders = ",".join("?" for _ in fixture_ids)
    rows = conn.execute(
        f"select fixture_id, last_attempted_at from fixture_detail_delivery_attempts where fixture_id in ({placeholders})",
        fixture_ids,
    ).fetchall()
    attempts = {int(row["fixture_id"]): row["last_attempted_at"] for row in rows}
    for fixture_id in fixture_ids:
        raw = attempts.get(fixture_id)
        if raw:
            try:
                attempted = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
            except ValueError:
                attempted = None
            if attempted is not None and attempted >= cutoff:
                continue
        selected.append(fixture_id)
        if len(selected) >= limit:
            break
    return selected


def refresh_provider(conn: sqlite3.Connection, fixture_ids: list[int]) -> dict[str, Any]:
    if not fixture_ids:
        return {"selected": 0, "reconciled": 0, "errors": []}
    now = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    for fixture_id in fixture_ids:
        conn.execute(
            "insert into fixture_detail_delivery_attempts(fixture_id,last_attempted_at,attempts,last_status) values(?,?,1,'running') on conflict(fixture_id) do update set last_attempted_at=excluded.last_attempted_at, attempts=fixture_detail_delivery_attempts.attempts+1, last_status='running', last_error=null",
            (fixture_id, now),
        )
    conn.commit()
    engine = get_engine()
    session = get_session(engine)
    errors: list[str] = []
    reconciled = 0
    try:
        service = SyncService(SportMonksClient(), session)
        reconciled = service.reconcile_fixtures(fixture_ids)
        status = "succeeded"
    except Exception as exc:  # provider failures are recorded per run and retried later
        status = "failed"
        errors.append(str(exc))
    finally:
        session.rollback()
        session.close()
        engine.dispose()
    for fixture_id in fixture_ids:
        conn.execute(
            "update fixture_detail_delivery_attempts set last_status=?, last_error=? where fixture_id=?",
            (status, "; ".join(errors)[:1000] if errors else None, fixture_id),
        )
    conn.commit()
    return {"selected": len(fixture_ids), "reconciled": reconciled, "errors": errors}


def export_fixture_ids(fixture_ids: list[int], leagues: list[int], require_detail: bool) -> dict[str, Any]:
    if not fixture_ids:
        return {"selected": 0, "returncode": 0, "stdout": ""}
    command = [
        sys.executable,
        str(Path(__file__).with_name("export_to_supabase.py")),
        "--strict",
        "--leagues",
        ",".join(str(value) for value in leagues),
        "--fixture-ids",
        ",".join(str(value) for value in fixture_ids),
        "--protect-empty-detail",
        "--skip-odds-snapshots",
        "--skip-odds-outcomes",
        "--skip-prune",
        "--report-json",
        "/tmp/fixture_detail_explicit_export_report.json",
    ]
    if require_detail:
        command.append("--require-detail")
    LOG.info("Exporting explicit fixture-detail batch (%s fixtures)", len(fixture_ids))
    result = subprocess.run(command, text=True, capture_output=True, check=False)
    if result.returncode != 0:
        LOG.error("Explicit fixture export failed: %s", result.stderr[-2000:])
    return {
        "selected": len(fixture_ids),
        "returncode": result.returncode,
        "stdout": result.stdout[-4000:],
        "stderr": result.stderr[-4000:],
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--leagues", default=None)
    parser.add_argument("--season-ids", default=None, help="Comma-separated season IDs to audit or repair.")
    parser.add_argument("--repair", action="store_true")
    parser.add_argument("--max-repair-fixtures", type=int, default=100)
    parser.add_argument("--provider-refresh-limit", type=int, default=25)
    parser.add_argument("--provider-refresh-cooldown-hours", type=int, default=DEFAULT_COOLDOWN_HOURS)
    parser.add_argument("--batch-size", type=int, default=250)
    parser.add_argument("--report-json", default=None)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    leagues = parse_csv_ints(args.leagues) if args.leagues else default_leagues()
    season_ids = parse_csv_ints(args.season_ids)
    if not leagues:
        raise SystemExit("No leagues selected")

    source_conn = open_source()
    # A plain audit must not mutate the source database.  The ledger is only
    # needed when a repair may schedule provider refreshes.
    if args.repair:
        ensure_attempt_ledger(source_conn)
    fixture_map = load_fixtures(source_conn, leagues, season_ids)
    fixture_ids = list(fixture_map)
    source = load_source_counts(source_conn, fixture_ids, fixture_map)
    target_url = os.environ.get("SUPABASE_DB_URL_SESSION") or os.environ.get("SUPABASE_DB_URL")
    if not target_url:
        raise SystemExit("SUPABASE_DB_URL_SESSION or SUPABASE_DB_URL is required")
    target_conn = psycopg2.connect(target_url, connect_timeout=20)
    target: dict[tuple[int, int], DetailCounts] = {}
    try:
        for batch in chunked(fixture_ids, args.batch_size):
            target.update(target_counts(target_conn, batch, fixture_map))
    finally:
        target_conn.close()

    complete_gaps, sparse, healthy = classify(fixture_map, source, target)
    report: dict[str, Any] = {
        "repair_requested": bool(args.repair),
        "leagues": leagues,
        "season_ids": season_ids,
        "fixtures_checked": len(fixture_ids),
        "healthy_fixtures": len(healthy),
        "source_sparse_fixtures": len(sparse),
        "source_complete_target_gaps": len(complete_gaps),
        "target_gap_fixture_ids": complete_gaps[:500],
        "source_sparse_fixture_ids": sparse[:500],
        "provider_refresh": {"selected": 0, "reconciled": 0, "errors": []},
        "exports": [],
    }

    if args.repair and sparse:
        refresh_ids = eligible_provider_refresh(
            source_conn,
            sorted(sparse, key=lambda value: fixture_map[value].starting_at or "", reverse=True),
            args.provider_refresh_limit,
            args.provider_refresh_cooldown_hours,
        )
        report["provider_refresh"] = refresh_provider(source_conn, refresh_ids)
        if refresh_ids:
            source_conn.close()
            source_conn = None
            last_locked_error: Exception | None = None
            for attempt in range(6):
                try:
                    source_conn = open_source()
                    fixture_map = load_fixtures(source_conn, leagues, season_ids)
                    fixture_ids = list(fixture_map)
                    source = load_source_counts(source_conn, fixture_ids, fixture_map)
                    last_locked_error = None
                    break
                except sqlite3.OperationalError as exc:
                    if "locked" not in str(exc).lower():
                        raise
                    last_locked_error = exc
                    if source_conn is not None:
                        source_conn.close()
                    source_conn = None
                    time.sleep(min(5 * (attempt + 1), 30))
            if last_locked_error is not None or source_conn is None:
                raise last_locked_error or RuntimeError("Unable to reopen source database after provider refresh")
            target_conn = psycopg2.connect(target_url, connect_timeout=20)
            target = {}
            try:
                for batch in chunked(fixture_ids, args.batch_size):
                    target.update(target_counts(target_conn, batch, fixture_map))
            finally:
                target_conn.close()
            complete_gaps, sparse, healthy = classify(fixture_map, source, target)

    if args.repair and complete_gaps:
        repair_limit = args.max_repair_fixtures if args.max_repair_fixtures > 0 else len(complete_gaps)
        for batch in chunked(complete_gaps[:repair_limit], args.batch_size):
            report["exports"].append(export_fixture_ids(batch, leagues, require_detail=True))

    # Re-read target after any export and fail only for source-complete data that
    # remains inconsistent. Provider-sparse fixtures are reported separately so
    # a provider omission does not silently look like a successful delivery.
    if args.repair and complete_gaps:
        target_conn = psycopg2.connect(target_url, connect_timeout=20)
        target = {}
        try:
            for batch in chunked(list(fixture_map), args.batch_size):
                target.update(target_counts(target_conn, batch, fixture_map))
        finally:
            target_conn.close()
        complete_gaps, sparse, healthy = classify(fixture_map, source, target)
        report["post_repair_source_complete_target_gaps"] = len(complete_gaps)
        report["post_repair_target_gap_fixture_ids"] = complete_gaps[:500]
        report["post_repair_source_sparse_fixtures"] = len(sparse)
    else:
        report["post_repair_source_complete_target_gaps"] = len(complete_gaps)
        report["post_repair_target_gap_fixture_ids"] = complete_gaps[:500]
        report["post_repair_source_sparse_fixtures"] = len(sparse)

    source_conn.close()
    if args.report_json:
        Path(args.report_json).write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    print(json.dumps(report, default=str))
    return 1 if report["post_repair_source_complete_target_gaps"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
