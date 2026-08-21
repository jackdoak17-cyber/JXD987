#!/usr/bin/env python3
"""
Export a pruned subset of SQLite data to Supabase via REST.
- seasons: current + previous per league
- teams: only those referenced by exported fixtures
- fixtures: finished fixtures plus upcoming scheduled fixtures (next 45 days)
- players: only players referenced by exported fixture player stats/lineups
- player_team_history: player team timeline from squad syncs
- fixture_players: only for exported fixtures
- fixture_statistics: only for exported fixtures
- fixture_player_statistics: only for exported fixtures
- fixture_players/fixture_player_statistics: replace rows per fixture to avoid stale data
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

try:
    import psycopg2
except Exception:  # pragma: no cover - optional runtime dependency on some hosts
    psycopg2 = None

DB_PATH = os.environ.get("JXD_DB_PATH", "data/jxd.sqlite")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_DB_URL = os.environ.get("SUPABASE_DB_URL_SESSION") or os.environ.get("SUPABASE_DB_URL")
REST_PATH = "/rest/v1"
ODDS_MIN_PRICE = float(os.environ.get("ODDS_MIN_PRICE", "1.0"))
ODDS_MAX_PRICE = float(os.environ.get("ODDS_MAX_PRICE", "500"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger(__name__)
REQUIRED_TABLES = [
    "seasons",
    "rounds",
    "teams",
    "fixtures",
    "players",
    "sidelined_players",
    "player_team_history",
    "fixture_players",
    "fixture_statistics",
    "fixture_player_statistics",
    "odds_snapshots",
    "odds_outcomes",
]
FIXTURE_CORE_TABLES = [
    "seasons",
    "rounds",
    "teams",
    "fixtures",
]
FALLBACK_REMOTE_COLUMNS: Dict[str, Set[str]] = {
    "fixture_players": {
        "fixture_id",
        "player_id",
        "team_id",
        "is_starter",
        "minutes_played",
        "position_name",
        "detailed_position_id",
        "detailed_position_name",
        "detailed_position_code",
        "formation_field",
        "formation_position",
        "lineup_detailed_position_id",
        "lineup_detailed_position_name",
        "lineup_detailed_position_code",
        "position_abbr",
    },
}
REMOTE_TABLE_COLUMNS_CACHE: Dict[str, Optional[Set[str]]] = {}
REMOTE_TABLE_FILTER_LOGGED: Set[str] = set()


def _valid_odds_price(value: object) -> bool:
    try:
        price = float(value)
    except (TypeError, ValueError):
        return False
    return ODDS_MIN_PRICE < price <= ODDS_MAX_PRICE


def _odds_outcome_identity(row: Dict[str, object]) -> Tuple[object, ...]:
    return (
        row.get("fixture_id"),
        row.get("market_key"),
        row.get("bookmaker_id"),
        row.get("participant_type") or "",
        row.get("participant_id") if row.get("participant_id") is not None else -1,
        row.get("selection_key") or "",
        row.get("line") if row.get("line") is not None else -9999,
    )


def _odds_outcome_rank(row: Dict[str, object]) -> Tuple[str, int]:
    last_updated_at = row.get("last_updated_at")
    rank_time = str(last_updated_at or "")
    rank_rowid = int(row.get("_rowid") or 0)
    return (rank_time, rank_rowid)


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


def get_remote_table_columns(table: str) -> Optional[Set[str]]:
    if table in REMOTE_TABLE_COLUMNS_CACHE:
        return REMOTE_TABLE_COLUMNS_CACHE[table]

    columns: Optional[Set[str]] = None
    if SUPABASE_DB_URL and psycopg2 is not None:
        conn = None
        try:
            conn = psycopg2.connect(SUPABASE_DB_URL)
            cur = conn.cursor()
            cur.execute(
                """
                select column_name
                from information_schema.columns
                where table_schema = 'public'
                  and table_name = %s
                """,
                (table,),
            )
            fetched = {str(row[0]) for row in cur.fetchall() if row and row[0]}
            if fetched:
                columns = fetched
        except Exception as exc:  # pragma: no cover - best-effort schema discovery
            log.warning("Remote schema discovery failed for %s: %s", table, exc)
        finally:
            if conn is not None:
                conn.close()

    if not columns:
        fallback = FALLBACK_REMOTE_COLUMNS.get(table)
        if fallback:
            columns = set(fallback)

    REMOTE_TABLE_COLUMNS_CACHE[table] = columns
    return columns


def filter_rows_for_remote_schema(table: str, rows: List[Dict]) -> List[Dict]:
    columns = get_remote_table_columns(table)
    if not columns:
        return rows

    unsupported = sorted({key for row in rows for key in row.keys() if key not in columns})
    if unsupported and table not in REMOTE_TABLE_FILTER_LOGGED:
        log.info(
            "Filtering unsupported %s columns for Supabase export: %s",
            table,
            ",".join(unsupported),
        )
        REMOTE_TABLE_FILTER_LOGGED.add(table)

    filtered = [{key: value for key, value in row.items() if key in columns} for row in rows]
    return [row for row in filtered if row]


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


def ensure_fixture_columns(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(fixtures)")
    existing = {row[1] for row in cur.fetchall()}
    desired = {
        "lineup_confirmed": "INTEGER",
    }
    for name, ddl_type in desired.items():
        if name not in existing:
            cur.execute(f"ALTER TABLE fixtures ADD COLUMN {name} {ddl_type}")
    conn.commit()


def choose_keep_seasons(
    conn: sqlite3.Connection,
    league_ids: Sequence[int] | None = None,
    keep_all: bool = False,
) -> Set[int]:
    cur = conn.cursor()
    if keep_all:
        if league_ids:
            q = ",".join("?" for _ in league_ids)
            cur.execute(f"select distinct id from seasons where league_id in ({q})", league_ids)
        else:
            cur.execute("select distinct id from seasons")
        return {int(row[0]) for row in cur.fetchall()}

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


def fetch_rounds(conn: sqlite3.Connection, keep_ids: Sequence[int]) -> List[Dict]:
    cur = conn.cursor()
    q = ",".join("?" for _ in keep_ids)
    cur.execute(
        f"""
        select id, league_id, season_id, stage_id, name, starting_at, ending_at,
               is_current, games_in_current_week, finished
        from rounds
        where season_id in ({q})
        """,
        keep_ids,
    )
    return [
        {
            "id": r[0],
            "league_id": r[1],
            "season_id": r[2],
            "stage_id": r[3],
            "name": r[4],
            "starting_at": r[5],
            "ending_at": r[6],
            "is_current": bool(r[7]),
            "games_in_current_week": bool(r[8]),
            "finished": bool(r[9]),
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


def fetch_fixtures(
    conn: sqlite3.Connection,
    keep_ids: Sequence[int],
    upcoming_days: int = 45,
    days_back: Optional[int] = None,
) -> List[Dict]:
    cur = conn.cursor()
    q = ",".join("?" for _ in keep_ids)
    now = datetime.utcnow()
    upcoming_end = now + timedelta(days=max(upcoming_days, 0))
    if days_back is None:
        days_back = int(os.environ.get("EXPORT_DAYS_BACK", "0") or "0")
    days_back = max(days_back, 0)
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
               home_team_id, away_team_id, home_score, away_score, lineup_confirmed, extra
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
                "lineup_confirmed": bool(row[10]) if row[10] is not None else None,
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
        select fixture_id, player_id, team_id, name, position, lineup_type, jersey_number,
               is_starter, minutes_played, position_name,
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
            "name": r[3],
            "position": r[4],
            "lineup_type": r[5],
            "jersey_number": r[6],
            "is_starter": r[7],
            "minutes_played": r[8],
            "position_name": r[9],
            "detailed_position_id": r[10],
            "detailed_position_name": r[11],
            "detailed_position_code": r[12],
            "formation_field": r[13],
            "formation_position": r[14],
            "lineup_detailed_position_id": r[15],
            "lineup_detailed_position_name": r[16],
            "lineup_detailed_position_code": r[17],
            "position_abbr": r[18],
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
        select rowid, fixture_id, bookmaker_id, market_key, selection_key,
               participant_type, participant_id, line,
               price_decimal, price_american, last_updated_at
        from odds_outcomes
        where fixture_id in ({q})
        """,
        fixture_ids,
    )
    deduped_rows: Dict[Tuple[object, ...], Dict] = {}
    skipped_invalid = 0
    skipped_duplicates = 0
    skipped_samples: List[str] = []
    for r in cur.fetchall():
        if not _valid_odds_price(r[8]):
            skipped_invalid += 1
            if len(skipped_samples) < 5:
                skipped_samples.append(
                    f"fixture_id={r[1]} market_key={r[3]} selection_key={r[4]} price_decimal={r[8]}"
                )
            continue
        row = {
            "_rowid": r[0],
            "fixture_id": r[1],
            "bookmaker_id": r[2],
            "market_key": r[3],
            "selection_key": r[4],
            "participant_type": r[5],
            "participant_id": r[6],
            "line": r[7],
            "price_decimal": r[8],
            "price_american": r[9],
            "last_updated_at": r[10],
        }
        identity = _odds_outcome_identity(row)
        existing = deduped_rows.get(identity)
        if existing is not None:
            skipped_duplicates += 1
            if _odds_outcome_rank(row) <= _odds_outcome_rank(existing):
                continue
        deduped_rows[identity] = row
    if skipped_invalid:
        sample_text = "; ".join(skipped_samples)
        log.warning(
            "Skipped %s odds_outcomes rows outside allowed price range [%s, %s]. Samples: %s",
            skipped_invalid,
            ODDS_MIN_PRICE,
            ODDS_MAX_PRICE,
            sample_text,
        )
    if skipped_duplicates:
        log.warning(
            "Collapsed %s duplicate odds_outcomes rows to the latest row per natural key.",
            skipped_duplicates,
        )
    return [
        {key: value for key, value in row.items() if key != "_rowid"}
        for row in deduped_rows.values()
    ]


def fetch_players(conn: sqlite3.Connection, player_ids: Sequence[int]) -> List[Dict]:
    if not player_ids:
        return []
    cur = conn.cursor()
    q = ",".join("?" for _ in player_ids)
    cur.execute(
        f"""
        select id, name, display_name, short_name, common_name, team_id, team_updated_at, image_path
        from players
        where id in ({q})
        """,
        player_ids,
    )
    return [
        {
            "id": r[0],
            "name": r[1],
            "display_name": r[2],
            "short_name": r[3],
            "common_name": r[4],
            "team_id": r[5],
            "team_updated_at": r[6],
            "image_path": r[7],
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


def fetch_sidelined_players(conn: sqlite3.Connection, team_ids: Sequence[int]) -> List[Dict]:
    if not team_ids:
        return []
    cur = conn.cursor()
    q = ",".join("?" for _ in team_ids)
    cur.execute(
        f"""
        select id, player_id, team_id, category, type_id, season_id,
               start_date, end_date, games_missed, completed, updated_at, extra
        from sidelined_players
        where team_id in ({q})
        """,
        team_ids,
    )
    return [
        {
            "id": r[0],
            "player_id": r[1],
            "team_id": r[2],
            "category": r[3],
            "type_id": r[4],
            "season_id": r[5],
            "start_date": r[6],
            "end_date": r[7],
            "games_missed": r[8],
            "completed": r[9],
            "updated_at": r[10],
            "extra": r[11],
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


def delete_fixture_rows(table: str, fixture_ids: Sequence[int], dry_run: bool) -> int:
    if not fixture_ids or dry_run:
        return 0

    url = SUPABASE_URL.rstrip("/") + REST_PATH + f"/{table}"
    chunk = max(int(os.environ.get("SUPABASE_DELETE_CHUNK", os.environ.get("SUPABASE_EXPORT_CHUNK", "50"))), 1)
    pause = max(float(os.environ.get("SUPABASE_EXPORT_SLEEP", "0.5")), 0.0)
    max_retries = max(int(os.environ.get("SUPABASE_EXPORT_RETRIES", "3")), 0)
    headers = {**rest_headers(), "Prefer": "count=exact"}
    fixture_ids = list(fixture_ids)
    total_batches = (len(fixture_ids) + chunk - 1) // chunk
    total = 0
    for i in range(0, len(fixture_ids), chunk):
        batch_ids = fixture_ids[i : i + chunk]
        batch_index = i // chunk + 1
        log.info("Deleting %s batch %s/%s (%s fixtures)", table, batch_index, total_batches, len(batch_ids))
        attempt = 0
        while True:
            try:
                resp = requests.delete(
                    url,
                    headers=headers,
                    params={"fixture_id": f"in.({','.join(str(x) for x in batch_ids)})"},
                    timeout=60,
                )
            except requests.RequestException as exc:
                if attempt >= max_retries:
                    raise SystemExit(f"Supabase delete from {table} failed: {exc}") from exc
                attempt += 1
                time.sleep(pause * (2**attempt))
                continue
            if resp.ok:
                break
            if attempt >= max_retries:
                raise SystemExit(
                    f"Supabase delete from {table} failed {resp.status_code}: {resp.text}"
                )
            attempt += 1
            time.sleep(pause * (2**attempt))
        content_range = resp.headers.get("Content-Range", "")
        if "/" in content_range:
            count = content_range.split("/")[-1]
            if count and count != "*":
                try:
                    total += int(count)
                except ValueError:
                    pass
        if pause:
            time.sleep(pause)
    return total


def upsert_table(table: str, rows: List[Dict], on_conflict: str, dry_run: bool) -> int:
    if not rows:
        return 0
    rows = filter_rows_for_remote_schema(table, rows)
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
        "--days-back",
        type=int,
        default=None,
        help="Override the finished-fixture export lookback window in days.",
    )
    parser.add_argument(
        "--upcoming-days",
        type=int,
        default=int(os.environ.get("EXPORT_DAYS_FORWARD", "45") or "45"),
        help="Number of upcoming days to export for scheduled fixtures.",
    )
    parser.add_argument(
        "--fixture-core-only",
        action="store_true",
        default=False,
        help="Export only seasons, rounds, teams, and fixtures for lightweight refresh runs.",
    )
    parser.add_argument(
        "--skip-odds-snapshots",
        action="store_true",
        default=False,
        help="Skip exporting odds_snapshots (reduces Supabase load)",
    )
    parser.add_argument(
        "--skip-odds-outcomes",
        action="store_true",
        default=False,
        help="Skip exporting odds_outcomes (useful for fixture-only refresh runs)",
    )
    parser.add_argument(
        "--skip-prune",
        action="store_true",
        default=False,
        help="Skip pruning fixtures outside kept seasons.",
    )
    parser.add_argument(
        "--keep-all-seasons",
        action="store_true",
        default=False,
        help="Retain every season present in SQLite (for bounded team-history exports).",
    )
    parser.add_argument(
        "--report-json",
        default=None,
        help="Write the export summary JSON to this path as well as stdout.",
    )
    args = parser.parse_args()

    require_env(args.dry_run)

    conn = get_conn()
    ensure_tables_exist(conn, FIXTURE_CORE_TABLES if args.fixture_core_only else REQUIRED_TABLES)
    ensure_fixture_columns(conn)

    league_ids = [int(x) for x in args.leagues.split(",") if x.strip()] if args.leagues else []
    keep_ids = choose_keep_seasons(
        conn,
        league_ids if league_ids else None,
        keep_all=args.keep_all_seasons,
    )
    if not keep_ids:
        raise SystemExit("No seasons to export")

    seasons = fetch_seasons(conn, list(keep_ids))
    rounds = fetch_rounds(conn, list(keep_ids))
    fixtures = fetch_fixtures(
        conn,
        list(keep_ids),
        upcoming_days=args.upcoming_days,
        days_back=args.days_back,
    )
    team_ids = {f["home_team_id"] for f in fixtures} | {f["away_team_id"] for f in fixtures}
    teams = fetch_teams(conn, list(team_ids))
    known_team_ids = {t["id"] for t in teams}
    filtered_fixtures = [f for f in fixtures if f["home_team_id"] in known_team_ids and f["away_team_id"] in known_team_ids]
    dropped = len(fixtures) - len(filtered_fixtures)
    if args.strict and dropped > 0:
        raise SystemExit(f"Strict export: dropping {dropped} fixtures with missing teams")
    fixtures = filtered_fixtures

    fixture_ids: Set[int] = {f["id"] for f in fixtures}

    if args.fixture_core_only:
        fixture_players = []
        fixture_stats = []
        fixture_player_stats = []
        odds_snapshots = []
        odds_outcomes = []
        players = []
        player_team_history = []
        sidelined_players = []
    else:
        fixture_players = fetch_fixture_players(conn, list(fixture_ids))
        fixture_stats = fetch_fixture_statistics(conn, list(fixture_ids))
        fixture_player_stats = fetch_fixture_player_statistics(conn, list(fixture_ids))
        odds_snapshots = [] if args.skip_odds_snapshots else fetch_odds_snapshots(conn, list(fixture_ids))
        odds_outcomes = [] if args.skip_odds_outcomes else fetch_odds_outcomes(conn, list(fixture_ids))

        player_ids: Set[int] = set()
        for fp in fixture_players:
            if fp.get("player_id"):
                player_ids.add(fp["player_id"])
        for fps in fixture_player_stats:
            if fps.get("player_id"):
                player_ids.add(fps["player_id"])

        players = fetch_players(conn, list(player_ids))
        player_team_history = fetch_player_team_history(conn, list(player_ids))
        sidelined_players = fetch_sidelined_players(conn, list(team_ids))

    log.info("Payload counts: seasons=%s teams=%s fixtures=%s players=%s", len(seasons), len(teams), len(fixtures), len(players))
    log.info("Payload counts: rounds=%s", len(rounds))
    log.info(
        "Payload counts: fixture_players=%s fixture_statistics=%s fixture_player_statistics=%s",
        len(fixture_players),
        len(fixture_stats),
        len(fixture_player_stats),
    )
    log.info(
        "Payload counts: player_team_history=%s sidelined_players=%s",
        len(player_team_history),
        len(sidelined_players),
    )
    log.info("Payload counts: odds_snapshots=%s odds_outcomes=%s", len(odds_snapshots), len(odds_outcomes))

    if fixture_ids and not args.fixture_core_only:
        log.info("Deleting existing fixture-scoped rows before upsert")
        deleted_fixture_players = delete_fixture_rows("fixture_players", fixture_ids, args.dry_run)
        deleted_fixture_player_stats = delete_fixture_rows(
            "fixture_player_statistics", fixture_ids, args.dry_run
        )
        # Odds are maintained by the dedicated odds-ingestion pipeline. A
        # history/metadata export may not have any local odds rows at all;
        # deleting the remote rows in that case silently wipes valid odds.
        # Preserve them by default and require an explicit opt-in for a
        # destructive replacement.
        skip_odds_delete = os.environ.get("SUPABASE_SKIP_ODDS_OUTCOMES_DELETE", "1").strip().lower() not in {
            "0",
            "false",
            "no",
        }
        if args.skip_odds_outcomes or skip_odds_delete:
            deleted_odds_outcomes = 0
            if skip_odds_delete and not args.skip_odds_outcomes:
                log.info("Skipping odds_outcomes fixture-scoped delete (SUPABASE_SKIP_ODDS_OUTCOMES_DELETE enabled).")
        else:
            deleted_odds_outcomes = delete_fixture_rows("odds_outcomes", fixture_ids, args.dry_run)
        log.info(
            "Deleted rows: fixture_players=%s fixture_player_statistics=%s odds_outcomes=%s",
            deleted_fixture_players,
            deleted_fixture_player_stats,
            deleted_odds_outcomes,
        )

    exported: Dict[str, int] = {}
    exports = [
        ("seasons", seasons, "id"),
        ("rounds", rounds, "id"),
        ("teams", teams, "id"),
        ("fixtures", fixtures, "id"),
    ]
    if not args.fixture_core_only:
        exports.extend(
            [
                ("players", players, "id"),
                ("sidelined_players", sidelined_players, "id"),
                ("player_team_history", player_team_history, "id"),
                ("fixture_players", fixture_players, "fixture_id,player_id"),
                ("fixture_statistics", fixture_stats, "fixture_id,team_id,type_id"),
                ("fixture_player_statistics", fixture_player_stats, "fixture_id,player_id,type_id"),
            ]
        )

    for table, rows, on_conflict in exports:
        log.info("Exporting %s (%s rows)", table, len(rows))
        exported[table] = upsert_table(table, rows, on_conflict, args.dry_run)

    if args.fixture_core_only or args.skip_odds_snapshots:
        exported["odds_snapshots"] = 0
    else:
        log.info("Exporting odds_snapshots (%s rows)", len(odds_snapshots))
        exported["odds_snapshots"] = upsert_table("odds_snapshots", odds_snapshots, "id", args.dry_run)

    if args.fixture_core_only or args.skip_odds_outcomes:
        exported["odds_outcomes"] = 0
    else:
        log.info("Exporting odds_outcomes (%s rows)", len(odds_outcomes))
        exported["odds_outcomes"] = upsert_table(
            "odds_outcomes",
            odds_outcomes,
            "fixture_id,bookmaker_id,market_key,selection_key,line",
            args.dry_run,
        )
    pruned = 0 if args.skip_prune else prune_fixtures(list(keep_ids), args.dry_run)

    summary = {
        "dry_run": args.dry_run,
        "fixture_core_only": args.fixture_core_only,
        "keep_season_ids": list(keep_ids),
        "fixtures_exported": exported["fixtures"],
        "teams_exported": exported["teams"],
        "seasons_exported": exported["seasons"],
        "rounds_exported": exported["rounds"],
        "players_exported": exported.get("players", 0),
        "player_team_history_exported": exported.get("player_team_history", 0),
        "fixture_players_exported": exported.get("fixture_players", 0),
        "fixture_statistics_exported": exported.get("fixture_statistics", 0),
        "fixture_player_statistics_exported": exported.get("fixture_player_statistics", 0),
        "odds_snapshots_exported": exported["odds_snapshots"],
        "odds_outcomes_exported": exported["odds_outcomes"],
        "fixtures_dropped_missing_teams": dropped,
        "fixtures_pruned_other_seasons": pruned,
    }
    summary_json = json.dumps(summary)
    if args.report_json:
        report_path = os.path.abspath(args.report_json)
        report_dir = os.path.dirname(report_path)
        if report_dir:
            os.makedirs(report_dir, exist_ok=True)
        with open(report_path, "w", encoding="utf-8") as report_file:
            report_file.write(summary_json)
            report_file.write("\n")
        log.info("Wrote export report: %s", report_path)
    print(summary_json)


if __name__ == "__main__":
    main()
