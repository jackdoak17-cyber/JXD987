#!/usr/bin/env python3
"""
Export a pruned subset of SQLite data to Supabase via REST.
- seasons: current + previous per league
- teams: only those referenced by exported fixtures
- fixtures: only with both scores and valid home/away teams
- players: only players referenced by exported fixture player stats/lineups
- fixture_players: only for exported fixtures
- fixture_statistics: only for exported fixtures
- fixture_player_statistics: only for exported fixtures

Supports --dry-run to print the payload counts without hitting Supabase.
"""

import argparse
import json
import os
import sqlite3
from typing import Dict, List, Sequence, Set, Tuple

import requests

DB_PATH = os.environ.get("JXD_DB_PATH", "data/jxd.sqlite")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
REST_PATH = "/rest/v1"

REQUIRED_TABLES = [
    "seasons",
    "teams",
    "fixtures",
    "players",
    "fixture_players",
    "fixture_statistics",
    "fixture_player_statistics",
]


def require_env(dry_run: bool) -> None:
    if dry_run:
        return
    missing = []
    if not SUPABASE_URL:
        missing.append("SUPABASE_URL")
    if not SUPABASE_KEY:
        missing.append("SUPABASE_SERVICE_ROLE_KEY")
    if missing:
        raise SystemExit(f"Missing env vars: {', '.join(missing)}")


def get_conn() -> sqlite3.Connection:
    if not os.path.exists(DB_PATH):
        raise SystemExit(f"SQLite DB not found at {DB_PATH}")
    return sqlite3.connect(DB_PATH)


def ensure_tables_exist(conn: sqlite3.Connection, tables: Sequence[str]) -> None:
    cur = conn.cursor()
    cur.execute("select name from sqlite_master where type='table'")
    existing = {row[0] for row in cur.fetchall()}
    missing = [t for t in tables if t not in existing]
    if missing:
        raise SystemExit(f"Missing required tables in SQLite: {', '.join(missing)}")


def choose_keep_seasons(conn: sqlite3.Connection) -> Set[int]:
    cur = conn.cursor()
    keep: Set[int] = set()
    cur.execute("select distinct league_id from seasons")
    leagues = [row[0] for row in cur.fetchall()]
    for league_id in leagues:
        cur.execute(
            """
            select id, is_current, end_date
            from seasons
            where league_id = ?
            order by is_current desc, end_date desc
            """,
            (league_id,),
        )
        rows = cur.fetchall()
        if not rows:
            continue
        current = next((r for r in rows if r[1]), None)
        if current:
            keep.add(current[0])
        for r in rows:
            if current and r[0] == current[0]:
                continue
            keep.add(r[0])
            break
    return keep


def fetch_seasons(conn: sqlite3.Connection, keep_ids: Sequence[int]) -> List[Dict]:
    cur = conn.cursor()
    q = ",".join("?" for _ in keep_ids)
    cur.execute(
        f"select id, league_id, name, start_date, end_date, is_current from seasons where id in ({q})",
        keep_ids,
    )
    return [
        {
            "id": r[0],
            "league_id": r[1],
            "name": r[2],
            "start_date": r[3],
            "end_date": r[4],
            "is_current": bool(r[5]),
        }
        for r in cur.fetchall()
    ]


def fetch_fixtures(conn: sqlite3.Connection, keep_ids: Sequence[int]) -> List[Dict]:
    cur = conn.cursor()
    q = ",".join("?" for _ in keep_ids)
    cur.execute(
        f"""
        select id, league_id, season_id, starting_at, status, status_code,
               home_team_id, away_team_id, home_score, away_score
        from fixtures
        where season_id in ({q})
          and home_score is not null
          and away_score is not null
          and home_team_id is not null
          and away_team_id is not null
        """,
        keep_ids,
    )
    fixtures = []
    for row in cur.fetchall():
        fixtures.append(
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
            }
        )
    return fixtures


def fetch_teams(conn: sqlite3.Connection, team_ids: Sequence[int]) -> List[Dict]:
    if not team_ids:
        return []
    cur = conn.cursor()
    q = ",".join("?" for _ in team_ids)
    cur.execute(f"select id, name, short_code from teams where id in ({q})", team_ids)
    return [{"id": r[0], "name": r[1], "short_code": r[2]} for r in cur.fetchall()]


def fetch_fixture_players(conn: sqlite3.Connection, fixture_ids: Sequence[int]) -> List[Dict]:
    if not fixture_ids:
        return []
    cur = conn.cursor()
    q = ",".join("?" for _ in fixture_ids)
    cur.execute(
        f"""
        select fixture_id, player_id, team_id, is_starter, minutes_played, position_name
        from fixture_players
        where fixture_id in ({q})
        """,
        fixture_ids,
    )
    return [
        {
            "fixture_id": r[0],
            "player_id": r[1],
            "team_id": r[2],
            "is_starter": r[3],
            "minutes_played": r[4],
            "position_name": r[5],
        }
        for r in cur.fetchall()
    ]


def fetch_fixture_statistics(conn: sqlite3.Connection, fixture_ids: Sequence[int]) -> List[Dict]:
    if not fixture_ids:
        return []
    cur = conn.cursor()
    q = ",".join("?" for _ in fixture_ids)
    cur.execute(
        f"""
        select fixture_id, team_id, type_id, value
        from fixture_statistics
        where fixture_id in ({q})
        """,
        fixture_ids,
    )
    return [
        {
            "fixture_id": r[0],
            "team_id": r[1],
            "type_id": r[2],
            "value": r[3],
        }
        for r in cur.fetchall()
    ]


def fetch_fixture_player_statistics(conn: sqlite3.Connection, fixture_ids: Sequence[int]) -> List[Dict]:
    if not fixture_ids:
        return []
    cur = conn.cursor()
    q = ",".join("?" for _ in fixture_ids)
    cur.execute(
        f"""
        select fixture_id, player_id, team_id, type_id, value
        from fixture_player_statistics
        where fixture_id in ({q})
        """,
        fixture_ids,
    )
    return [
        {
            "fixture_id": r[0],
            "player_id": r[1],
            "team_id": r[2],
            "type_id": r[3],
            "value": r[4],
        }
        for r in cur.fetchall()
    ]


def fetch_players(conn: sqlite3.Connection, player_ids: Sequence[int]) -> List[Dict]:
    if not player_ids:
        return []
    cur = conn.cursor()
    q = ",".join("?" for _ in player_ids)
    cur.execute(
        f"select id, name, short_name, common_name, team_id from players where id in ({q})",
        player_ids,
    )
    return [
        {
            "id": r[0],
            "name": r[1],
            "short_name": r[2],
            "common_name": r[3],
            "team_id": r[4],
        }
        for r in cur.fetchall()
    ]


def rest_headers() -> Dict[str, str]:
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal,resolution=merge-duplicates",
    }


def upsert_table(table: str, rows: List[Dict], on_conflict: str, dry_run: bool) -> int:
    if not rows:
        return 0
    if dry_run:
        return len(rows)

    url = SUPABASE_URL.rstrip("/") + REST_PATH + f"/{table}"
    total = 0
    chunk = 500
    headers = rest_headers()
    for i in range(0, len(rows), chunk):
        batch = rows[i : i + chunk]
        resp = requests.post(
            url,
            headers=headers,
            params={"on_conflict": on_conflict},
            data=json.dumps(batch),
            timeout=30,
        )
        if not resp.ok:
            raise SystemExit(
                f"Supabase upsert to {table} failed {resp.status_code}: {resp.text}"
            )
        total += len(batch)
    return total


def prune_fixtures(keep_ids: Sequence[int], dry_run: bool) -> int:
    if dry_run:
        return 0
    url = SUPABASE_URL.rstrip("/") + REST_PATH + "/fixtures"
    params = {"season_id": f"not.in.({','.join(str(x) for x in keep_ids)})"}
    resp = requests.delete(url, headers={**rest_headers(), "Prefer": "count=exact"}, params=params)
    if not resp.ok:
        raise SystemExit(f"Supabase prune failed {resp.status_code}: {resp.text}")
    try:
        content_range = resp.headers.get("Content-Range", "")
        total = content_range.split("/")[-1] if "/" in content_range else content_range
        return int(total) if total and total != "*" else 0
    except Exception:
        return 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--strict", action="store_true", default=True)
    parser.add_argument("--dry-run", action="store_true", help="Compute payload sizes without sending to Supabase")
    args = parser.parse_args()

    require_env(args.dry_run)

    conn = get_conn()
    ensure_tables_exist(conn, REQUIRED_TABLES)

    keep_ids = choose_keep_seasons(conn)
    if not keep_ids:
        raise SystemExit("No seasons to export")

    seasons = fetch_seasons(conn, list(keep_ids))
    fixtures = fetch_fixtures(conn, list(keep_ids))
    team_ids = {f["home_team_id"] for f in fixtures} | {f["away_team_id"] for f in fixtures}
    teams = fetch_teams(conn, list(team_ids))
    known_team_ids = {t["id"] for t in teams}
    filtered_fixtures = [f for f in fixtures if f["home_team_id"] in known_team_ids and f["away_team_id"] in known_team_ids]
    dropped = len(fixtures) - len(filtered_fixtures)
    if args.strict and dropped > 0:
        raise SystemExit(f"Strict export: dropping {dropped} fixtures with missing teams")
    fixtures = filtered_fixtures

    fixture_ids: Set[int] = {f["id"] for f in fixtures}

    fixture_players = fetch_fixture_players(conn, list(fixture_ids))
    fixture_stats = fetch_fixture_statistics(conn, list(fixture_ids))
    fixture_player_stats = fetch_fixture_player_statistics(conn, list(fixture_ids))

    player_ids: Set[int] = set()
    for fp in fixture_players:
        if fp.get("player_id"):
            player_ids.add(fp["player_id"])
    for fps in fixture_player_stats:
        if fps.get("player_id"):
            player_ids.add(fps["player_id"])

    players = fetch_players(conn, list(player_ids))

    exported = {
        "seasons": upsert_table("seasons", seasons, "id", args.dry_run),
        "teams": upsert_table("teams", teams, "id", args.dry_run),
        "fixtures": upsert_table("fixtures", fixtures, "id", args.dry_run),
        "players": upsert_table("players", players, "id", args.dry_run),
        "fixture_players": upsert_table(
            "fixture_players", fixture_players, "fixture_id,player_id", args.dry_run
        ),
        "fixture_statistics": upsert_table(
            "fixture_statistics", fixture_stats, "fixture_id,team_id,type_id", args.dry_run
        ),
        "fixture_player_statistics": upsert_table(
            "fixture_player_statistics",
            fixture_player_stats,
            "fixture_id,player_id,type_id",
            args.dry_run,
        ),
    }
    pruned = prune_fixtures(list(keep_ids), args.dry_run)

    summary = {
        "dry_run": args.dry_run,
        "keep_season_ids": list(keep_ids),
        "fixtures_exported": exported["fixtures"],
        "teams_exported": exported["teams"],
        "seasons_exported": exported["seasons"],
        "players_exported": exported["players"],
        "fixture_players_exported": exported["fixture_players"],
        "fixture_statistics_exported": exported["fixture_statistics"],
        "fixture_player_statistics_exported": exported["fixture_player_statistics"],
        "fixtures_dropped_missing_teams": dropped,
        "fixtures_pruned_other_seasons": pruned,
    }
    print(json.dumps(summary))


if __name__ == "__main__":
    main()
