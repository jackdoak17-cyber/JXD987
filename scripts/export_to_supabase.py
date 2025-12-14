#!/usr/bin/env python3
"""
Export a pruned subset of SQLite data to Supabase via REST:
- seasons: current + previous per league
- teams: only those referenced by exported fixtures
- fixtures: only with both scores and valid home/away teams
- prune Supabase fixtures not in kept seasons
"""

import argparse
import json
import os
import sqlite3
from typing import Dict, List, Sequence, Set

import requests

DB_PATH = os.environ.get("JXD_DB_PATH", "data/jxd.sqlite")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
REST_PATH = "/rest/v1"


def require_env() -> None:
    missing = []
    if not SUPABASE_URL:
        missing.append("SUPABASE_URL")
    if not SUPABASE_KEY:
        missing.append("SUPABASE_SERVICE_ROLE_KEY")
    if missing:
        raise SystemExit(f"Missing env vars: {', '.join(missing)}")


def get_conn() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)


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


def rest_headers() -> Dict[str, str]:
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal,resolution=merge-duplicates",
    }


def upsert(table: str, rows: List[Dict]) -> int:
    if not rows:
        return 0
    url = SUPABASE_URL.rstrip("/") + REST_PATH + f"/{table}"
    total = 0
    chunk = 500
    for i in range(0, len(rows), chunk):
        batch = rows[i : i + chunk]
        resp = requests.post(url, headers=rest_headers(), params={"on_conflict": "id"}, data=json.dumps(batch))
        if not resp.ok:
            raise SystemExit(f"Supabase upsert to {table} failed {resp.status_code}: {resp.text}")
        total += len(batch)
    return total


def prune_fixtures(keep_ids: Sequence[int]) -> int:
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
    args = parser.parse_args()

    require_env()

    conn = get_conn()
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

    exported = {
        "seasons": upsert("seasons", seasons),
        "teams": upsert("teams", teams),
        "fixtures": upsert("fixtures", fixtures),
    }
    pruned = prune_fixtures(list(keep_ids))

    summary = {
        "keep_season_ids": list(keep_ids),
        "fixtures_exported": exported["fixtures"],
        "teams_exported": exported["teams"],
        "seasons_exported": exported["seasons"],
        "fixtures_dropped_missing_teams": dropped,
        "fixtures_pruned_other_seasons": pruned,
    }
    print(json.dumps(summary))


if __name__ == "__main__":
    main()
