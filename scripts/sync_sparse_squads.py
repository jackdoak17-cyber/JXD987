#!/usr/bin/env python3
"""
Refresh current-season squads and export squad player rows to Supabase.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
from pathlib import Path
from typing import Dict, Iterable, List, Sequence

import requests
from sqlalchemy import bindparam, text

from jxd import SportMonksClient, SyncService
from jxd.db import get_engine, get_session
from scripts.export_to_supabase import (
    REST_PATH,
    SUPABASE_URL,
    require_env,
    rest_headers,
    upsert_table,
)


DB_PATH = os.environ.get("JXD_DB_PATH", "data/jxd.sqlite")


def parse_ids(raw: str | None) -> List[int]:
    if not raw:
        return []
    return [int(value) for value in raw.replace("\n", ",").split(",") if value.strip()]


def unique_ordered(values: Iterable[int]) -> List[int]:
    seen = set()
    ordered: List[int] = []
    for value in values:
        parsed = int(value)
        if parsed in seen:
            continue
        seen.add(parsed)
        ordered.append(parsed)
    return ordered


def current_season_team_ids(
    session,
    league_ids: Sequence[int],
    skip_large_leagues_threshold: int,
) -> List[int]:
    params: Dict[str, object] = {}
    league_filter = ""
    if league_ids:
        league_filter = "and f.league_id in :league_ids"
        params["league_ids"] = list(league_ids)
    stmt = text(
        f"""
        select distinct f.league_id, f.home_team_id as team_id
        from fixtures f
        join seasons s on s.id = f.season_id
        where coalesce(s.is_current, 0) = 1
          and f.home_team_id is not null
          {league_filter}
        union
        select distinct f.league_id, f.away_team_id as team_id
        from fixtures f
        join seasons s on s.id = f.season_id
        where coalesce(s.is_current, 0) = 1
          and f.away_team_id is not null
          {league_filter}
        """
    )
    if league_ids:
        stmt = stmt.bindparams(bindparam("league_ids", expanding=True))
    rows = session.execute(stmt, params).fetchall()
    league_team_counts: Dict[int, int] = {}
    for row in rows:
        league_team_counts[int(row.league_id)] = league_team_counts.get(int(row.league_id), 0) + 1
    return [
        int(row.team_id)
        for row in rows
        if skip_large_leagues_threshold <= 0
        or league_team_counts.get(int(row.league_id), 0) < skip_large_leagues_threshold
    ]


def team_player_counts(session, team_ids: Sequence[int]) -> Dict[int, int]:
    if not team_ids:
        return {}
    stmt = (
        text(
            """
            select team_id, count(*) as player_count
            from players
            where team_id in :team_ids
            group by team_id
            """
        )
        .bindparams(bindparam("team_ids", expanding=True))
    )
    rows = session.execute(stmt, {"team_ids": list(team_ids)}).fetchall()
    return {int(row.team_id): int(row.player_count) for row in rows if row.team_id is not None}


def fetch_players_for_teams(team_ids: Sequence[int]) -> List[Dict]:
    if not team_ids:
        return []
    conn = sqlite3.connect(DB_PATH)
    try:
        q = ",".join("?" for _ in team_ids)
        rows = conn.execute(
            f"""
            select id, name, display_name, short_name, common_name, team_id, team_updated_at, image_path
            from players
            where team_id in ({q})
            """,
            list(team_ids),
        ).fetchall()
    finally:
        conn.close()
    return [
        {
            "id": row[0],
            "name": row[1],
            "display_name": row[2],
            "short_name": row[3],
            "common_name": row[4],
            "team_id": row[5],
            "team_updated_at": row[6],
            "image_path": row[7],
        }
        for row in rows
    ]


def fetch_players_by_ids(player_ids: Sequence[int]) -> List[Dict]:
    if not player_ids:
        return []
    conn = sqlite3.connect(DB_PATH)
    try:
        q = ",".join("?" for _ in player_ids)
        rows = conn.execute(
            f"""
            select id, name, display_name, short_name, common_name, team_id, team_updated_at, image_path
            from players
            where id in ({q})
            """,
            list(player_ids),
        ).fetchall()
    finally:
        conn.close()
    return [
        {
            "id": row[0],
            "name": row[1],
            "display_name": row[2],
            "short_name": row[3],
            "common_name": row[4],
            "team_id": row[5],
            "team_updated_at": row[6],
            "image_path": row[7],
        }
        for row in rows
    ]


def detach_remote_players_missing_from_squads(
    team_ids: Sequence[int],
    current_players: Sequence[Dict],
    dry_run: bool,
) -> Dict[str, int]:
    """Remove remote team assignments not present in a successful local squad snapshot."""
    current_player_ids_by_team: Dict[int, set[int]] = {}
    for row in current_players:
        team_id = row.get("team_id")
        player_id = row.get("id")
        if team_id is None or player_id is None:
            continue
        current_player_ids_by_team.setdefault(int(team_id), set()).add(int(player_id))

    detached_by_team: Dict[str, int] = {}
    if dry_run:
        return detached_by_team

    for team_id in team_ids:
        response = requests.get(
            f"{SUPABASE_URL.rstrip()}{REST_PATH}/players",
            headers=rest_headers(),
            params={"select": "id", "team_id": f"eq.{team_id}", "limit": "1000"},
            timeout=60,
        )
        if not response.ok:
            raise SystemExit(
                f"Supabase player lookup for team {team_id} failed {response.status_code}: {response.text}"
            )
        remote_ids = {
            int(row["id"])
            for row in response.json()
            if isinstance(row, dict) and row.get("id") is not None
        }
        stale_ids = sorted(remote_ids - current_player_ids_by_team.get(int(team_id), set()))
        if not stale_ids:
            continue
        patch = requests.patch(
            f"{SUPABASE_URL.rstrip()}{REST_PATH}/players",
            headers=rest_headers(),
            params={
                # Keep the team predicate on the write as well as the lookup.
                # This makes the detach idempotent and prevents a stale player
                # ID from being cleared if another reconciliation moved it to
                # a different team between these two requests.
                "id": f"in.({','.join(str(player_id) for player_id in stale_ids)})",
                "team_id": f"eq.{team_id}",
            },
            json={"team_id": None},
            timeout=60,
        )
        if not patch.ok:
            raise SystemExit(
                f"Supabase player detach for team {team_id} failed {patch.status_code}: {patch.text}"
            )
        detached_by_team[str(team_id)] = len(stale_ids)
    return detached_by_team


def fetch_player_team_history(player_ids: Sequence[int]) -> List[Dict]:
    if not player_ids:
        return []
    conn = sqlite3.connect(DB_PATH)
    try:
        q = ",".join("?" for _ in player_ids)
        rows = conn.execute(
            f"""
            select id, player_id, team_id, source, effective_from, effective_to, created_at, updated_at
            from player_team_history
            where player_id in ({q})
            """,
            list(player_ids),
        ).fetchall()
    finally:
        conn.close()
    return [
        {
            "id": row[0],
            "player_id": row[1],
            "team_id": row[2],
            "source": row[3],
            "effective_from": row[4],
            "effective_to": row[5],
            "created_at": row[6],
            "updated_at": row[7],
        }
        for row in rows
    ]


def fetch_team_squad_snapshots(team_ids: Sequence[int]) -> List[Dict]:
    if not team_ids:
        return []
    conn = sqlite3.connect(DB_PATH)
    try:
        q = ",".join("?" for _ in team_ids)
        rows = conn.execute(
            f"""
            select id, team_id, source, status, observed_at, completed_at,
                   player_count, payload_hash, error, created_at
            from team_squad_snapshots
            where team_id in ({q})
            order by id
            """,
            list(team_ids),
        ).fetchall()
    finally:
        conn.close()
    return [
        {
            "id": row[0],
            "team_id": row[1],
            "source": row[2],
            "status": row[3],
            "observed_at": row[4],
            "completed_at": row[5],
            "player_count": row[6],
            "payload_hash": row[7],
            "error": row[8],
            "created_at": row[9],
        }
        for row in rows
    ]


def fetch_team_squad_memberships(team_ids: Sequence[int]) -> List[Dict]:
    if not team_ids:
        return []
    conn = sqlite3.connect(DB_PATH)
    try:
        q = ",".join("?" for _ in team_ids)
        rows = conn.execute(
            f"""
            select team_id, player_id, is_active, first_seen_at, last_seen_at, provider_started_at,
                   last_snapshot_id, source, created_at, updated_at
            from team_squad_memberships
            where team_id in ({q})
            """,
            list(team_ids),
        ).fetchall()
    finally:
        conn.close()
    return [
        {
            "team_id": row[0],
            "player_id": row[1],
            "is_active": bool(row[2]),
            "first_seen_at": row[3],
            "last_seen_at": row[4],
            "provider_started_at": row[5],
            "last_snapshot_id": row[6],
            "source": row[7],
            "created_at": row[8],
            "updated_at": row[9],
        }
        for row in rows
    ]


def write_report(path: str | None, report: Dict) -> None:
    if not path:
        return
    Path(path).write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--leagues", default=os.environ.get("LEAGUE_IDS", ""))
    parser.add_argument("--team-ids", default="")
    parser.add_argument("--minimum-players", type=int, default=int(os.environ.get("SQUAD_REFRESH_MIN_PLAYERS", "15")))
    parser.add_argument(
        "--refresh-all",
        action="store_true",
        help="Reconcile every selected current-season squad, including player departures.",
    )
    parser.add_argument(
        "--skip-large-leagues-threshold",
        type=int,
        default=int(os.environ.get("SQUAD_REFRESH_SKIP_LARGE_LEAGUES_THRESHOLD", "80")),
        help="Skip automatic sparse refresh in leagues with this many current-season teams; explicit team IDs still run.",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--report-json", default="")
    args = parser.parse_args()

    require_env(args.dry_run)

    league_ids = parse_ids(args.leagues)
    explicit_team_ids = parse_ids(args.team_ids)

    engine = get_engine()
    session = get_session(engine)
    client = SportMonksClient()
    service = SyncService(client, session)
    service.ensure_schema()

    candidate_team_ids = unique_ordered(
        [
            # A full reconciliation must never inherit the sparse-refresh guard.
            # Otherwise the largest leagues are silently omitted precisely when
            # we are trying to establish a complete current-squad baseline.
            *current_season_team_ids(
                session,
                league_ids,
                0 if args.refresh_all else args.skip_large_leagues_threshold,
            ),
            *explicit_team_ids,
        ]
    )
    before_counts = team_player_counts(session, candidate_team_ids)
    sparse_team_ids = (
        candidate_team_ids
        if args.refresh_all
        else [
            team_id
            for team_id in candidate_team_ids
            if team_id in explicit_team_ids or before_counts.get(team_id, 0) < args.minimum_players
        ]
    )
    previously_assigned_player_ids = [
        int(row["id"])
        for row in fetch_players_for_teams(sparse_team_ids)
        if row.get("id")
    ]

    if not args.dry_run:
        service.sync_squads_for_teams(sparse_team_ids)
    after_counts = team_player_counts(session, sparse_team_ids)

    current_players = fetch_players_for_teams(sparse_team_ids)
    players = fetch_players_by_ids(
        unique_ordered([
            *previously_assigned_player_ids,
            *(int(row["id"]) for row in current_players if row.get("id")),
        ])
    )
    player_ids = [int(row["id"]) for row in players if row.get("id")]
    player_team_history = fetch_player_team_history(player_ids)
    squad_snapshots = fetch_team_squad_snapshots(sparse_team_ids)
    squad_memberships = fetch_team_squad_memberships(sparse_team_ids)

    players_exported = 0
    history_exported = 0
    snapshots_exported = 0
    memberships_exported = 0
    if players:
        players_exported, _ = upsert_table("players", players, "id", args.dry_run)
    if player_team_history:
        history_exported, _ = upsert_table("player_team_history", player_team_history, "id", args.dry_run)
    if squad_snapshots:
        snapshots_exported, _ = upsert_table("team_squad_snapshots", squad_snapshots, "id", args.dry_run)
    if squad_memberships:
        memberships_exported, _ = upsert_table(
            "team_squad_memberships", squad_memberships, "team_id,player_id", args.dry_run
        )
    remote_players_detached = detach_remote_players_missing_from_squads(
        sparse_team_ids,
        current_players,
        args.dry_run,
    )

    report = {
        "league_ids": league_ids,
        "explicit_team_ids": explicit_team_ids,
        "minimum_players": args.minimum_players,
        "refresh_all": args.refresh_all,
        "skip_large_leagues_threshold": args.skip_large_leagues_threshold,
        "candidate_teams": len(candidate_team_ids),
        "teams_refreshed": len(sparse_team_ids),
        "team_ids_refreshed": sparse_team_ids,
        "before_counts": {str(team_id): before_counts.get(team_id, 0) for team_id in sparse_team_ids},
        "after_counts": {str(team_id): after_counts.get(team_id, 0) for team_id in sparse_team_ids},
        "players_exported": players_exported,
        "player_team_history_exported": history_exported,
        "squad_snapshots_exported": snapshots_exported,
        "squad_memberships_exported": memberships_exported,
        "remote_players_detached": remote_players_detached,
    }
    write_report(args.report_json, report)
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
