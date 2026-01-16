#!/usr/bin/env python3
"""
Export a pruned subset of SQLite data to Supabase via REST.
- seasons: current + previous per league
- teams: only those referenced by exported fixtures
- fixtures: finished fixtures plus upcoming scheduled fixtures (next 14 days)
- players: only players referenced by exported fixture player stats/lineups
- player_team_history: player team timeline from squad syncs
- fixture_players: only for exported fixtures
- fixture_statistics: only for exported fixtures
- fixture_player_statistics: only for exported fixtures
- odds_snapshots/odds_outcomes: only for exported fixtures

Supports --dry-run to print the payload counts without hitting Supabase.
"""

import argparse
import json
import logging
import os
import sqlite3
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Sequence, Set, Tuple

import requests

DB_PATH = os.environ.get("JXD_DB_PATH", "data/jxd.sqlite")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
REST_PATH = "/rest/v1"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger(__name__)
REQUIRED_TABLES = [
    "seasons",
    "teams",
    "fixtures",
    "players",
    "player_team_history",
    "fixture_players",
    "fixture_statistics",
    "fixture_player_statistics",
    "odds_snapshots",
    "odds_outcomes",
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


def choose_keep_seasons(conn: sqlite3.Connection, league_ids: Sequence[int] | None = None) -> Set[int]:
    cur = conn.cursor()
    keep: Set[int] = set()
    if league_ids:
        q = ",".join("?" for _ in league_ids)
        cur.execute(f"select distinct league_id from seasons where league_id in ({q})", league_ids)
    else:
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


def extract_half_time_scores(extra_raw: object) -> Tuple[Optional[int], Optional[int]]:
    if not extra_raw:
        return None, None
    extra = None
    if isinstance(extra_raw, (dict, list)):
        extra = extra_raw
    else:
        try:
            extra = json.loads(extra_raw)
        except Exception:
            extra = None
    if not isinstance(extra, dict):
        return None, None
    scores = extra.get("scores")
    if not isinstance(scores, list):
        return None, None
    home_ht = None
    away_ht = None
    for score in scores:
        if not isinstance(score, dict):
            continue
        if str(score.get("description") or "").upper() != "1ST_HALF":
            continue
        score_obj = score.get("score") or {}
        if not isinstance(score_obj, dict):
            continue
        goals = score_obj.get("goals")
        participant = score_obj.get("participant")
        try:
            goals_val = int(goals) if goals is not None else None
        except Exception:
            goals_val = None
        if participant == "home":
            home_ht = goals_val
        elif participant == "away":
            away_ht = goals_val
    return home_ht, away_ht


def fetch_fixtures(conn: sqlite3.Connection, keep_ids: Sequence[int], upcoming_days: int = 14) -> List[Dict]:
    cur = conn.cursor()
    q = ",".join("?" for _ in keep_ids)
    now = datetime.utcnow()
    upcoming_end = now + timedelta(days=upcoming_days)
    days_back = int(os.environ.get("EXPORT_DAYS_BACK", "0") or "0")
    finished_start = now - timedelta(days=days_back) if days_back > 0 else None
    now_iso = now.isoformat(sep=" ")
    upcoming_iso = upcoming_end.isoformat(sep=" ")
    finished_iso = finished_start.isoformat(sep=" ") if finished_start else None
    finished_clause = "home_score is not null and away_score is not null"
    if finished_iso:
        finished_clause += " and starting_at >= ?"
    cur.execute(
        f"""
        select id, league_id, season_id, starting_at, status, status_code,
               home_team_id, away_team_id, home_score, away_score, extra
        from fixtures
        where season_id in ({q})
          and home_team_id is not null
          and away_team_id is not null
          and (
            ({finished_clause})
            or (starting_at >= ? and starting_at <= ?)
          )
        """,
        [*keep_ids, *( [finished_iso] if finished_iso else []), now_iso, upcoming_iso],
    )
    fixtures = []
    for row in cur.fetchall():
        home_ht_score, away_ht_score = extract_half_time_scores(row[10])
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
                "home_ht_score": home_ht_score,
                "away_ht_score": away_ht_score,
            }
        )
    return fixtures


def fetch_teams(conn: sqlite3.Connection, team_ids: Sequence[int]) -> List[Dict]:
    if not team_ids:
        return []
    cur = conn.cursor()
    q = ",".join("?" for _ in team_ids)
    cur.execute(
        f"select id, name, short_code, image_path from teams where id in ({q})", team_ids
    )
    return [
        {"id": r[0], "name": r[1], "short_code": r[2], "image_path": r[3]}
        for r in cur.fetchall()
    ]


def fetch_fixture_players(conn: sqlite3.Connection, fixture_ids: Sequence[int]) -> List[Dict]:
    if not fixture_ids:
        return []
    cur = conn.cursor()
    q = ",".join("?" for _ in fixture_ids)
    cur.execute(
        f"""
        select fixture_id, player_id, team_id, is_starter, minutes_played, position_name,
               detailed_position_id, detailed_position_name, detailed_position_code,
               formation_field, formation_position,
               lineup_detailed_position_id, lineup_detailed_position_name, lineup_detailed_position_code,
               position_abbr
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
            "detailed_position_id": r[6],
            "detailed_position_name": r[7],
            "detailed_position_code": r[8],
            "formation_field": r[9],
            "formation_position": r[10],
            "lineup_detailed_position_id": r[11],
            "lineup_detailed_position_name": r[12],
            "lineup_detailed_position_code": r[13],
            "position_abbr": r[14],
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
        select fixture_id, team_id, type_id, max(value) as value
        from fixture_statistics
        where fixture_id in ({q})
        group by fixture_id, team_id, type_id
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


def fetch_odds_snapshots(conn: sqlite3.Connection, fixture_ids: Sequence[int]) -> List[Dict]:
    if not fixture_ids:
        return []
    cur = conn.cursor()
    q = ",".join("?" for _ in fixture_ids)
    cur.execute(
        f"""
        select id, fixture_id, bookmaker_id, pulled_at, raw
        from odds_snapshots
        where fixture_id in ({q})
        """,
        fixture_ids,
    )
    def parse_raw(raw_value):
        if raw_value is None:
            return None
        if isinstance(raw_value, (dict, list)):
            return raw_value
        try:
            return json.loads(raw_value)
        except Exception:
            return raw_value

    return [
        {
            "id": r[0],
            "fixture_id": r[1],
            "bookmaker_id": r[2],
            "pulled_at": r[3],
            "raw": parse_raw(r[4]),
        }
        for r in cur.fetchall()
    ]


def fetch_odds_outcomes(conn: sqlite3.Connection, fixture_ids: Sequence[int]) -> List[Dict]:
    if not fixture_ids:
        return []
    cur = conn.cursor()
    q = ",".join("?" for _ in fixture_ids)
    cur.execute(
        f"""
        select fixture_id, bookmaker_id, market_key, selection_key,
               participant_type, participant_id, line,
               price_decimal, price_american, last_updated_at
        from odds_outcomes
        where fixture_id in ({q})
        """,
        fixture_ids,
    )
    return [
        {
            "fixture_id": r[0],
            "bookmaker_id": r[1],
            "market_key": r[2],
            "selection_key": r[3],
            "participant_type": r[4],
            "participant_id": r[5],
            "line": r[6],
            "price_decimal": r[7],
            "price_american": r[8],
            "last_updated_at": r[9],
        }
        for r in cur.fetchall()
    ]


def fetch_players(conn: sqlite3.Connection, player_ids: Sequence[int]) -> List[Dict]:
    if not player_ids:
        return []
    cur = conn.cursor()
    q = ",".join("?" for _ in player_ids)
    cur.execute(
        f"select id, name, short_name, common_name, team_id, team_updated_at, image_path from players where id in ({q})",
        player_ids,
    )
    return [
        {
            "id": r[0],
            "name": r[1],
            "short_name": r[2],
            "common_name": r[3],
            "team_id": r[4],
            "team_updated_at": r[5],
            "image_path": r[6],
        }
        for r in cur.fetchall()
    ]


def fetch_player_team_history(conn: sqlite3.Connection, player_ids: Sequence[int]) -> List[Dict]:
    if not player_ids:
        return []
    cur = conn.cursor()
    q = ",".join("?" for _ in player_ids)
    cur.execute(
        f"""
        select id, player_id, team_id, source, effective_from, effective_to, created_at, updated_at
        from player_team_history
        where player_id in ({q})
        """,
        player_ids,
    )
    return [
        {
            "id": r[0],
            "player_id": r[1],
            "team_id": r[2],
            "source": r[3],
            "effective_from": r[4],
            "effective_to": r[5],
            "created_at": r[6],
            "updated_at": r[7],
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
    chunk = max(int(os.environ.get("SUPABASE_EXPORT_CHUNK", "50")), 1)
    pause = max(float(os.environ.get("SUPABASE_EXPORT_SLEEP", "0.5")), 0.0)
    max_retries = max(int(os.environ.get("SUPABASE_EXPORT_RETRIES", "3")), 0)
    headers = rest_headers()
    total_batches = (len(rows) + chunk - 1) // chunk
    for i in range(0, len(rows), chunk):
        batch_index = i // chunk + 1
        log.info("Upserting %s batch %s/%s (%s rows)", table, batch_index, total_batches, len(rows[i : i + chunk]))
        batch = rows[i : i + chunk]
        attempt = 0
        while True:
            try:
                resp = requests.post(
                    url,
                    headers=headers,
                    params={"on_conflict": on_conflict},
                    data=json.dumps(batch),
                    timeout=60,
                )
            except requests.RequestException as exc:
                if attempt >= max_retries:
                    raise SystemExit(f"Supabase upsert to {table} failed: {exc}") from exc
                attempt += 1
                time.sleep(pause * (2**attempt))
                continue
            if resp.ok:
                break
            if attempt >= max_retries:
                raise SystemExit(
                    f"Supabase upsert to {table} failed {resp.status_code}: {resp.text}"
                )
            attempt += 1
            time.sleep(pause * (2**attempt))
        total += len(batch)
        if pause:
            time.sleep(pause)
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
    parser.add_argument("--leagues", default=os.environ.get("LEAGUE_IDS", ""), help="Comma-separated league IDs")
    parser.add_argument(
        "--skip-odds-snapshots",
        action="store_true",
        default=False,
        help="Skip exporting odds_snapshots (reduces Supabase load)",
    )
    args = parser.parse_args()

    require_env(args.dry_run)

    conn = get_conn()
    ensure_tables_exist(conn, REQUIRED_TABLES)

    league_ids = [int(x) for x in args.leagues.split(",") if x.strip()] if args.leagues else []
    keep_ids = choose_keep_seasons(conn, league_ids if league_ids else None)
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
    odds_snapshots = [] if args.skip_odds_snapshots else fetch_odds_snapshots(conn, list(fixture_ids))
    odds_outcomes = fetch_odds_outcomes(conn, list(fixture_ids))

    player_ids: Set[int] = set()
    for fp in fixture_players:
        if fp.get("player_id"):
            player_ids.add(fp["player_id"])
    for fps in fixture_player_stats:
        if fps.get("player_id"):
            player_ids.add(fps["player_id"])

    players = fetch_players(conn, list(player_ids))
    player_team_history = fetch_player_team_history(conn, list(player_ids))

    log.info("Payload counts: seasons=%s teams=%s fixtures=%s players=%s", len(seasons), len(teams), len(fixtures), len(players))
    log.info(
        "Payload counts: fixture_players=%s fixture_statistics=%s fixture_player_statistics=%s",
        len(fixture_players),
        len(fixture_stats),
        len(fixture_player_stats),
    )
    log.info("Payload counts: player_team_history=%s", len(player_team_history))
    log.info("Payload counts: odds_snapshots=%s odds_outcomes=%s", len(odds_snapshots), len(odds_outcomes))

    exported: Dict[str, int] = {}
    exports = [
        ("seasons", seasons, "id"),
        ("teams", teams, "id"),
        ("fixtures", fixtures, "id"),
        ("players", players, "id"),
        ("player_team_history", player_team_history, "id"),
        ("fixture_players", fixture_players, "fixture_id,player_id"),
        ("fixture_statistics", fixture_stats, "fixture_id,team_id,type_id"),
        ("fixture_player_statistics", fixture_player_stats, "fixture_id,player_id,type_id"),
    ]

    for table, rows, on_conflict in exports:
        log.info("Exporting %s (%s rows)", table, len(rows))
        exported[table] = upsert_table(table, rows, on_conflict, args.dry_run)

    if args.skip_odds_snapshots:
        exported["odds_snapshots"] = 0
    else:
        log.info("Exporting odds_snapshots (%s rows)", len(odds_snapshots))
        exported["odds_snapshots"] = upsert_table("odds_snapshots", odds_snapshots, "id", args.dry_run)

    log.info("Exporting odds_outcomes (%s rows)", len(odds_outcomes))
    exported["odds_outcomes"] = upsert_table(
        "odds_outcomes",
        odds_outcomes,
        "fixture_id,bookmaker_id,market_key,selection_key,line",
        args.dry_run,
    )
    pruned = prune_fixtures(list(keep_ids), args.dry_run)

    summary = {
        "dry_run": args.dry_run,
        "keep_season_ids": list(keep_ids),
        "fixtures_exported": exported["fixtures"],
        "teams_exported": exported["teams"],
        "seasons_exported": exported["seasons"],
        "players_exported": exported["players"],
        "player_team_history_exported": exported["player_team_history"],
        "fixture_players_exported": exported["fixture_players"],
        "fixture_statistics_exported": exported["fixture_statistics"],
        "fixture_player_statistics_exported": exported["fixture_player_statistics"],
        "odds_snapshots_exported": exported["odds_snapshots"],
        "odds_outcomes_exported": exported["odds_outcomes"],
        "fixtures_dropped_missing_teams": dropped,
        "fixtures_pruned_other_seasons": pruned,
    }
    print(json.dumps(summary))


if __name__ == "__main__":
    main()
