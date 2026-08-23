#!/usr/bin/env python3
"""
Refresh confirmed lineups for imminent fixtures and export them to Supabase.

This script intentionally avoids syncing predicted lineups. It only stores and exports
fixture lineups when SportMonks metadata says the lineup is officially confirmed.
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Sequence, Set

from sqlalchemy import text

from export_to_supabase import (
    FIXTURE_CORE_TABLES,
    delete_fixture_rows,
    ensure_tables_exist,
    fetch_players,
    fetch_teams,
    get_conn,
    require_env,
    upsert_table,
)
from jxd import SportMonksClient, SyncService
from jxd.db import get_engine, get_session
from jxd.models import Fixture, FixturePlayer

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

DETAIL_INCLUDES = [
    "participants",
    "scores",
    "state",
    "metadata",
    "formations",
    "lineups.details",
    "lineups.position",
    "lineups.detailedposition",
    "lineups.player",
]
FINISHED_STATUSES = {"FT", "AET", "PEN", "FT_PEN"}


def _parse_leagues(league_csv: str) -> List[int]:
    return [int(x) for x in league_csv.split(",") if x.strip()]


def _extract_lineup_confirmed(data: Dict) -> bool:
    metadata = data.get("metadata")
    if not isinstance(metadata, list):
        return False
    for item in metadata:
        if not isinstance(item, dict):
            continue
        if item.get("type_id") != 572:
            continue
        values = item.get("values")
        if not isinstance(values, dict):
            return False
        return values.get("confirmed") is True
    return False


def fetch_candidate_fixture_ids(
    hours_back: int,
    hours_forward: int,
    league_ids: Sequence[int],
    limit: int,
) -> List[int]:
    engine = get_engine()
    with engine.begin() as conn:
        if engine.dialect.name == "sqlite":
            cols = {
                row[1]
                for row in conn.exec_driver_sql("PRAGMA table_info(fixtures)").fetchall()
            }
            if "lineup_confirmed" not in cols:
                conn.exec_driver_sql("ALTER TABLE fixtures ADD COLUMN lineup_confirmed INTEGER")
    params: Dict[str, object] = {
        "window_start": datetime.utcnow() - timedelta(hours=max(hours_back, 0)),
        "window_end": datetime.utcnow() + timedelta(hours=max(hours_forward, 0)),
        "limit": limit,
    }
    league_clause = ""
    if league_ids:
        placeholders = ",".join(f":league_{index}" for index, _ in enumerate(league_ids))
        league_clause = f"and league_id in ({placeholders})"
        for index, league_id in enumerate(league_ids):
            params[f"league_{index}"] = league_id

    query = text(
        f"""
        select id
        from fixtures
        where starting_at >= :window_start
          and starting_at <= :window_end
          and coalesce(status, '') not in ('FT', 'AET', 'PEN', 'FT_PEN')
          {league_clause}
        order by starting_at asc
        limit :limit
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(query, params).fetchall()
    return [int(row[0]) for row in rows]


def sync_confirmed_lineups(fixture_ids: Sequence[int]) -> tuple[List[int], List[int]]:
    if not fixture_ids:
        return ([], [])

    engine = get_engine()
    session = get_session(engine)
    client = SportMonksClient()
    svc = SyncService(client, session)
    svc.ensure_schema()

    checked_ids: List[int] = []
    confirmed_ids: List[int] = []
    for fixture_id in fixture_ids:
        endpoint = f"fixtures/{fixture_id}"
        try:
            payload = client.request(
                "GET",
                endpoint,
                params={"include": ";".join(DETAIL_INCLUDES)},
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("Confirmed lineup fetch failed for fixture %s: %s", fixture_id, exc)
            continue
        data = payload.get("data") or {}
        if not data:
            continue
        checked_ids.append(fixture_id)
        if not _extract_lineup_confirmed(data):
            fixture = session.get(Fixture, fixture_id)
            if fixture:
                fixture.lineup_confirmed = False
            continue
        session.query(FixturePlayer).filter(
            FixturePlayer.fixture_id == fixture_id
        ).delete(synchronize_session=False)
        # This endpoint intentionally omits statistics. Preserve existing
        # player-stat rows; the full-detail worker owns their replacement.
        svc._store_fixture_raw(data, log_changes=True, full_detail=False)
        confirmed_ids.append(fixture_id)

    if checked_ids:
        session.commit()
    else:
        session.rollback()
    session.close()
    return checked_ids, confirmed_ids


def fetch_fixtures_by_ids(conn, fixture_ids: Sequence[int]) -> List[Dict]:
    if not fixture_ids:
        return []
    cur = conn.cursor()
    q = ",".join("?" for _ in fixture_ids)
    cur.execute(
        f"""
        select id, league_id, season_id, starting_at, status, status_code,
               home_team_id, away_team_id, home_score, away_score, lineup_confirmed
        from fixtures
        where id in ({q})
        """,
        fixture_ids,
    )
    return [
        {
            "id": row[0],
            "league_id": row[1],
            "season_id": row[2],
            "starting_at": row[3],
            "status": row[4],
            "status_code": row[5],
            "home_team_id": row[6],
            "away_team_id": row[7],
            "home_score": row[8],
            "away_score": row[9],
            "lineup_confirmed": bool(row[10]) if row[10] is not None else None,
        }
        for row in cur.fetchall()
    ]


def fetch_fixture_players_by_ids(conn, fixture_ids: Sequence[int]) -> List[Dict]:
    if not fixture_ids:
        return []
    cur = conn.cursor()
    q = ",".join("?" for _ in fixture_ids)
    cur.execute(
        f"""
        select fixture_id, player_id, team_id, name, position, lineup_type, jersey_number,
               is_starter, minutes_played, position_name, detailed_position_id,
               detailed_position_name, detailed_position_code, formation_field,
               formation_position, lineup_detailed_position_id,
               lineup_detailed_position_name, lineup_detailed_position_code, position_abbr
        from fixture_players
        where fixture_id in ({q})
        """,
        fixture_ids,
    )
    return [
        {
            "fixture_id": row[0],
            "player_id": row[1],
            "team_id": row[2],
            "name": row[3],
            "position": row[4],
            "lineup_type": row[5],
            "jersey_number": row[6],
            "is_starter": row[7],
            "minutes_played": row[8],
            "position_name": row[9],
            "detailed_position_id": row[10],
            "detailed_position_name": row[11],
            "detailed_position_code": row[12],
            "formation_field": row[13],
            "formation_position": row[14],
            "lineup_detailed_position_id": row[15],
            "lineup_detailed_position_name": row[16],
            "lineup_detailed_position_code": row[17],
            "position_abbr": row[18],
        }
        for row in cur.fetchall()
    ]


def export_confirmed_lineups(
    checked_fixture_ids: Sequence[int],
    confirmed_fixture_ids: Sequence[int],
    dry_run: bool,
) -> None:
    if not checked_fixture_ids:
        return

    require_env(dry_run)
    conn = get_conn()
    ensure_tables_exist(conn, [*FIXTURE_CORE_TABLES, "players", "fixture_players"])

    fixtures = fetch_fixtures_by_ids(conn, checked_fixture_ids)
    if not fixtures:
        return
    team_ids = sorted(
        {
            int(team_id)
            for fixture in fixtures
            for team_id in (fixture.get("home_team_id"), fixture.get("away_team_id"))
            if isinstance(team_id, int) and team_id > 0
        }
    )
    teams = fetch_teams(conn, team_ids)
    fixture_players = fetch_fixture_players_by_ids(conn, confirmed_fixture_ids)
    player_ids = sorted(
        {
            int(row["player_id"])
            for row in fixture_players
            if isinstance(row.get("player_id"), int) and row["player_id"] > 0
        }
    )
    players = fetch_players(conn, player_ids)

    log.info(
        "Exporting confirmed lineups to Supabase: fixtures=%s teams=%s players=%s fixture_players=%s",
        len(fixtures),
        len(teams),
        len(players),
        len(fixture_players),
    )

    if confirmed_fixture_ids:
        delete_fixture_rows("fixture_players", confirmed_fixture_ids, dry_run)
    upsert_table("teams", teams, "id", dry_run)
    upsert_table("fixtures", fixtures, "id", dry_run)
    if players:
        upsert_table("players", players, "id", dry_run)
    if fixture_players:
        upsert_table("fixture_players", fixture_players, "fixture_id,player_id", dry_run)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--leagues", default="", help="Comma-separated league IDs")
    parser.add_argument("--hours-back", type=int, default=2)
    parser.add_argument("--hours-forward", type=int, default=3)
    parser.add_argument("--limit", type=int, default=40)
    parser.add_argument("--dry-run", action="store_true", default=False)
    args = parser.parse_args()

    league_ids = _parse_leagues(args.leagues) if args.leagues else []
    fixture_ids = fetch_candidate_fixture_ids(
        hours_back=args.hours_back,
        hours_forward=args.hours_forward,
        league_ids=league_ids,
        limit=max(args.limit, 1),
    )
    if not fixture_ids:
        log.info("No imminent fixtures eligible for confirmed lineup refresh.")
        return

    log.info("Checking confirmed lineups for %s imminent fixtures", len(fixture_ids))
    checked_ids, confirmed_ids = sync_confirmed_lineups(fixture_ids)
    if not checked_ids:
        log.info("No fixture rows were updated in the current window.")
        return

    if confirmed_ids:
        log.info("Confirmed lineups found for fixtures: %s", ",".join(str(fid) for fid in confirmed_ids))
    else:
        log.info("No newly confirmed lineups found in the current window.")
    export_confirmed_lineups(checked_ids, confirmed_ids, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
