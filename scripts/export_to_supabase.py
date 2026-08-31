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
from pathlib import Path
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


def _is_timeout_error_message(text: str) -> bool:
    msg = (text or "").lower()
    return (
        "statement timeout" in msg
        or "canceling statement due to statement timeout" in msg
        or '"code":"57014"' in msg
        or '"code": "57014"' in msg
        or "code=57014" in msg
    )


def _write_json_report(path_str: str, payload: Dict) -> None:
    if not path_str:
        return
    path = Path(path_str)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


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
        if keep_all:
            keep.update(row[0] for row in rows)
            continue
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
    finished_start = (
        datetime.combine((now - timedelta(days=days_back)).date(), datetime.min.time())
        if days_back > 0
        else None
    )
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
        home_ht_score, away_ht_score = extract_half_time_scores(row[11])
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


def fetch_fixtures_by_ids(conn: sqlite3.Connection, fixture_ids: Sequence[int]) -> List[Dict]:
    """Read an explicit fixture manifest without applying the rolling date window."""
    if not fixture_ids:
        return []
    cur = conn.cursor()
    q = ",".join("?" for _ in fixture_ids)
    cur.execute(
        f"""
        select id, league_id, season_id, starting_at, status, status_code,
               home_team_id, away_team_id, home_score, away_score, lineup_confirmed, extra
          from fixtures
         where id in ({q})
         order by starting_at desc, id desc
        """,
        list(fixture_ids),
    )
    fixtures: List[Dict] = []
    for row in cur.fetchall():
        home_ht_score, away_ht_score = extract_half_time_scores(row[11])
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
    found = {int(fixture["id"]) for fixture in fixtures}
    missing = sorted(set(int(value) for value in fixture_ids).difference(found))
    if missing:
        raise SystemExit(f"Explicit fixture export could not find source fixture IDs: {missing[:20]}")
    return fixtures


def validate_detail_payload(
    conn: sqlite3.Connection,
    fixtures: Sequence[Dict],
    require_player_stats: bool = False,
) -> List[int]:
    """Return completed fixtures whose source detail payload is unsafe to publish."""
    completed = [
        fixture
        for fixture in fixtures
        if fixture.get("home_score") is not None and fixture.get("away_score") is not None
    ]
    if not completed:
        return []
    fixture_ids = [int(fixture["id"]) for fixture in completed]
    q = ",".join("?" for _ in fixture_ids)
    lineup_rows = conn.execute(
        f"""
        select fixture_id, team_id, count(*) as row_count
          from fixture_players
         where fixture_id in ({q})
         group by fixture_id, team_id
        """,
        fixture_ids,
    ).fetchall()
    player_rows = conn.execute(
        f"""
        select fixture_id, team_id, count(distinct player_id || ':' || type_id) as row_count
          from fixture_player_statistics
         where fixture_id in ({q})
         group by fixture_id, team_id
        """,
        fixture_ids,
    ).fetchall()
    team_rows = conn.execute(
        f"""
        select fixture_id, team_id, count(distinct type_id) as row_count
          from fixture_statistics
         where fixture_id in ({q})
         group by fixture_id, team_id
        """,
        fixture_ids,
    ).fetchall()
    lineup_counts = {(int(row[0]), int(row[1])): int(row[2] or 0) for row in lineup_rows}
    player_counts = {(int(row[0]), int(row[1])): int(row[2] or 0) for row in player_rows}
    team_counts = {(int(row[0]), int(row[1])): int(row[2] or 0) for row in team_rows}
    unsafe: List[int] = []
    for fixture in completed:
        fixture_id = int(fixture["id"])
        teams = (int(fixture["home_team_id"]), int(fixture["away_team_id"]))
        if any(
            lineup_counts.get((fixture_id, team_id), 0) <= 0
            or team_counts.get((fixture_id, team_id), 0) <= 0
            for team_id in teams
        ):
            unsafe.append(fixture_id)
            continue
        if any(player_counts.get((fixture_id, team_id), 0) <= 0 for team_id in teams):
            unsafe.append(fixture_id)
    return sorted(set(unsafe))


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
               position_abbr, extra
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
            "extra": r[19],
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
        select fixture_id, team_id, type_id, max(code) as code, max(name) as name,
               max(location) as location, max(value) as value, max(extra) as extra
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
            "code": r[3],
            "name": r[4],
            "location": r[5],
            "value": r[6],
            "extra": r[7],
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
        select fixture_id, player_id, team_id, type_id, code, name, value, extra
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
            "code": r[4],
            "name": r[5],
            "value": r[6],
            "extra": r[7],
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
        "select 1 from sqlite_master where name = 'team_squad_memberships' "
        "and type in ('table', 'view') limit 1"
    )
    has_squad_memberships = cur.fetchone() is not None

    if has_squad_memberships:
        cur.execute("pragma table_info(team_squad_memberships)")
        squad_columns = {str(row[1]) for row in cur.fetchall()}
    else:
        squad_columns = set()

    required_squad_columns = {"player_id", "team_id", "is_active"}
    if required_squad_columns.issubset(squad_columns):
        seen_expression = "last_seen_at" if "last_seen_at" in squad_columns else "NULL"
        order_columns = []
        if "provider_started_at" in squad_columns:
            order_columns.append("provider_started_at desc")
        if "last_seen_at" in squad_columns:
            order_columns.append("last_seen_at desc")
        if "last_snapshot_id" in squad_columns:
            order_columns.append("last_snapshot_id desc")
        order_columns.append("team_id asc")
        cur.execute(
            f"""
            with current_assignment as (
                select player_id, team_id, {seen_expression} as last_seen_at,
                       row_number() over (
                           partition by player_id
                           order by {', '.join(order_columns)}
                       ) as assignment_rank
                from team_squad_memberships
                where is_active = 1
            )
            select p.id, p.name, p.display_name, p.short_name, p.common_name,
                   ca.team_id, coalesce(ca.last_seen_at, p.team_updated_at), p.image_path
            from players p
            left join current_assignment ca
              on ca.player_id = p.id and ca.assignment_rank = 1
            where p.id in ({q})
            """,
            player_ids,
        )
    else:
        # Older local databases may predate squad memberships. Keep the
        # exporter usable there, while current databases always use the
        # canonical active assignment above.
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


def delete_fixture_rows(table: str, fixture_ids: Sequence[int], dry_run: bool) -> Tuple[int, List[int]]:
    if not fixture_ids or dry_run:
        return 0, []

    url = SUPABASE_URL.rstrip("/") + REST_PATH + f"/{table}"
    chunk = max(int(os.environ.get("SUPABASE_DELETE_CHUNK", os.environ.get("SUPABASE_EXPORT_CHUNK", "50"))), 1)
    pause = max(float(os.environ.get("SUPABASE_EXPORT_SLEEP", "0.5")), 0.0)
    max_retries = max(int(os.environ.get("SUPABASE_EXPORT_RETRIES", "3")), 0)
    headers = {**rest_headers(), "Prefer": "count=exact"}
    fixture_ids = list(fixture_ids)
    total_batches = (len(fixture_ids) + chunk - 1) // chunk
    total = 0
    missed_fixture_ids: List[int] = []

    def run_delete_batch(batch_ids: List[int]) -> Tuple[bool, Optional[requests.Response], Optional[Exception]]:
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
                    return False, None, exc
                attempt += 1
                time.sleep(pause * (2**attempt))
                continue
            if resp.ok:
                return True, resp, None
            if attempt >= max_retries:
                return False, resp, None
            attempt += 1
            time.sleep(pause * (2**attempt))

    def delete_with_split(batch_ids: List[int]) -> None:
        nonlocal total
        ok, resp, exc = run_delete_batch(batch_ids)
        if ok and resp is not None:
            content_range = resp.headers.get("Content-Range", "")
            if "/" in content_range:
                count = content_range.split("/")[-1]
                if count and count != "*":
                    try:
                        total += int(count)
                    except ValueError:
                        pass
            return

        error_text = str(exc) if exc is not None else f"{resp.status_code}: {resp.text}" if resp is not None else "unknown"
        timeout_like = _is_timeout_error_message(error_text)

        if timeout_like and len(batch_ids) > 1:
            mid = len(batch_ids) // 2
            left = batch_ids[:mid]
            right = batch_ids[mid:]
            log.warning(
                "Delete timeout-like failure on %s (%s fixtures). Splitting into %s + %s.",
                table,
                len(batch_ids),
                len(left),
                len(right),
            )
            delete_with_split(left)
            delete_with_split(right)
            return

        if timeout_like:
            log.error("Delete failed after split to single fixture on %s fixture_id=%s: %s", table, batch_ids[0], error_text)
            missed_fixture_ids.extend(batch_ids)
            return

        raise SystemExit(f"Supabase delete from {table} failed: {error_text}")

    for i in range(0, len(fixture_ids), chunk):
        batch_ids = fixture_ids[i : i + chunk]
        batch_index = i // chunk + 1
        log.info("Deleting %s batch %s/%s (%s fixtures)", table, batch_index, total_batches, len(batch_ids))
        delete_with_split(batch_ids)
        if pause:
            time.sleep(pause)
    return total, sorted(set(missed_fixture_ids))


def atomic_fixture_detail_publish(
    target_conn,
    *,
    fixture_id: int,
    snapshot_id: int | None,
    fixture_players: Sequence[Dict],
    fixture_statistics: Sequence[Dict],
    fixture_player_statistics: Sequence[Dict],
) -> Dict:
    """Replace one fixture's raw detail through one target-side transaction."""
    def json_rows(rows: Sequence[Dict]) -> list[Dict]:
        normalized: list[Dict] = []
        for row in rows:
            value = dict(row)
            if isinstance(value.get("extra"), str):
                try:
                    value["extra"] = json.loads(value["extra"])
                except (TypeError, ValueError, json.JSONDecodeError):
                    pass
            normalized.append(value)
        return normalized

    payloads = (
        json.dumps(json_rows(fixture_players), default=str),
        json.dumps(json_rows(fixture_statistics), default=str),
        json.dumps(json_rows(fixture_player_statistics), default=str),
    )
    try:
        with target_conn.cursor() as cur:
            cur.execute(
                """
                select public.publish_fixture_detail_atomic(
                  %s, %s, %s::jsonb, %s::jsonb, %s::jsonb
                )
                """,
                (fixture_id, snapshot_id, *payloads),
            )
            row = cur.fetchone()
        target_conn.commit()
        return row[0] if row else {"fixture_id": fixture_id}
    except Exception:
        target_conn.rollback()
        raise


def upsert_table(table: str, rows: List[Dict], on_conflict: str, dry_run: bool) -> Tuple[int, int]:
    if not rows:
        return 0, 0
    rows = filter_rows_for_remote_schema(table, rows)
    if not rows:
        return 0, 0
    if dry_run:
        return len(rows), 0

    url = SUPABASE_URL.rstrip("/") + REST_PATH + f"/{table}"
    total = 0
    # Keep the REST exporter within the P3 SLA without weakening correctness.
    # The request is split recursively on a provider timeout, so a larger
    # starting batch is safe while avoiding thousands of 50-row requests.
    # Operators can still tune these explicitly for a constrained Supabase
    # project, but the production defaults are sized for the largest detail
    # table (fixture_player_statistics).
    chunk = max(int(os.environ.get("SUPABASE_EXPORT_CHUNK", "500")), 1)
    pause = max(float(os.environ.get("SUPABASE_EXPORT_SLEEP", "0.05")), 0.0)
    max_retries = max(int(os.environ.get("SUPABASE_EXPORT_RETRIES", "3")), 0)
    headers = rest_headers()
    total_batches = (len(rows) + chunk - 1) // chunk
    timeout_split_count = 0

    def run_upsert_batch(batch_rows: List[Dict]) -> Tuple[bool, Optional[requests.Response], Optional[Exception]]:
        attempt = 0
        while True:
            try:
                resp = requests.post(
                    url,
                    headers=headers,
                    params={"on_conflict": on_conflict},
                    data=json.dumps(batch_rows),
                    timeout=60,
                )
            except requests.RequestException as exc:
                if attempt >= max_retries:
                    return False, None, exc
                attempt += 1
                time.sleep(pause * (2**attempt))
                continue
            if resp.ok:
                return True, resp, None
            if attempt >= max_retries:
                return False, resp, None
            attempt += 1
            time.sleep(pause * (2**attempt))

    def upsert_with_split(batch_rows: List[Dict]) -> int:
        nonlocal timeout_split_count
        ok, resp, exc = run_upsert_batch(batch_rows)
        if ok:
            return len(batch_rows)

        error_text = str(exc) if exc is not None else f"{resp.status_code}: {resp.text}" if resp is not None else "unknown"
        timeout_like = _is_timeout_error_message(error_text)
        if timeout_like and len(batch_rows) > 1:
            timeout_split_count += 1
            mid = len(batch_rows) // 2
            left = batch_rows[:mid]
            right = batch_rows[mid:]
            log.warning(
                "Upsert timeout-like failure on %s (%s rows). Splitting into %s + %s.",
                table,
                len(batch_rows),
                len(left),
                len(right),
            )
            return upsert_with_split(left) + upsert_with_split(right)
        if timeout_like:
            raise SystemExit(f"Supabase upsert to {table} failed on single-row batch after timeout retries: {error_text}")
        raise SystemExit(f"Supabase upsert to {table} failed: {error_text}")

    for i in range(0, len(rows), chunk):
        batch_index = i // chunk + 1
        log.info("Upserting %s batch %s/%s (%s rows)", table, batch_index, total_batches, len(rows[i : i + chunk]))
        batch = rows[i : i + chunk]
        total += upsert_with_split(batch)
        if pause:
            time.sleep(pause)
    return total, timeout_split_count


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
        "--fixture-ids",
        default=None,
        help="Optional comma-separated explicit fixture IDs; bypasses the rolling date window.",
    )
    parser.add_argument(
        "--protect-empty-detail",
        action="store_true",
        help="Refuse to delete or publish completed fixture detail when either team has no source lineup rows.",
    )
    parser.add_argument(
        "--require-detail",
        action="store_true",
        help="Require non-empty source player statistics for both teams as well as lineups.",
    )
    parser.add_argument(
        "--atomic-fixture-detail",
        action="store_true",
        help="Publish each explicit fixture's detail through the target-side atomic publisher.",
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
        help="Retain every provider season for the selected leagues.",
    )
    parser.add_argument(
        "--report-json",
        default=os.environ.get("SUPABASE_EXPORT_REPORT_JSON", ""),
        help="Optional path to write detailed export report (including timeout splits/missed fixture deletes).",
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
    explicit_fixture_ids = [
        int(value.strip())
        for value in (args.fixture_ids or "").split(",")
        if value.strip()
    ]
    fixtures = (
        fetch_fixtures_by_ids(conn, explicit_fixture_ids)
        if explicit_fixture_ids
        else fetch_fixtures(
            conn,
            list(keep_ids),
            upcoming_days=args.upcoming_days,
            days_back=args.days_back,
        )
    )
    team_ids = {f["home_team_id"] for f in fixtures} | {f["away_team_id"] for f in fixtures}
    teams = fetch_teams(conn, list(team_ids))
    known_team_ids = {t["id"] for t in teams}
    filtered_fixtures = [f for f in fixtures if f["home_team_id"] in known_team_ids and f["away_team_id"] in known_team_ids]
    dropped = len(fixtures) - len(filtered_fixtures)
    if args.strict and dropped > 0:
        raise SystemExit(f"Strict export: dropping {dropped} fixtures with missing teams")
    fixtures = filtered_fixtures

    if args.protect_empty_detail and not args.fixture_core_only:
        unsafe_detail_ids = validate_detail_payload(
            conn,
            fixtures,
            require_player_stats=args.require_detail,
        )
        if unsafe_detail_ids:
            if explicit_fixture_ids or args.require_detail:
                raise SystemExit(
                    "Refusing fixture-detail export because source detail is empty for completed "
                    f"fixture IDs: {unsafe_detail_ids[:50]}"
                )
            log.warning(
                "Skipping %s completed fixtures with incomplete source detail during rolling export: %s",
                len(unsafe_detail_ids),
                unsafe_detail_ids[:50],
            )
            unsafe_set = set(unsafe_detail_ids)
            fixtures = [fixture for fixture in fixtures if int(fixture["id"]) not in unsafe_set]

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

    delete_stats: Dict[str, Dict[str, object]] = {}
    if fixture_ids and not args.fixture_core_only and not args.atomic_fixture_detail:
        log.info("Deleting existing fixture-scoped rows before upsert")
        deleted_fixture_players, missed_fixture_players = delete_fixture_rows("fixture_players", fixture_ids, args.dry_run)
        deleted_fixture_player_stats, missed_fixture_player_stats = delete_fixture_rows(
            "fixture_player_statistics", fixture_ids, args.dry_run
        )
        deleted_fixture_stats, missed_fixture_stats = delete_fixture_rows(
            "fixture_statistics", fixture_ids, args.dry_run
        )
        delete_stats["fixture_players"] = {
            "deleted": deleted_fixture_players,
            "missed_fixture_ids": missed_fixture_players,
        }
        delete_stats["fixture_player_statistics"] = {
            "deleted": deleted_fixture_player_stats,
            "missed_fixture_ids": missed_fixture_player_stats,
        }
        delete_stats["fixture_statistics"] = {
            "deleted": deleted_fixture_stats,
            "missed_fixture_ids": missed_fixture_stats,
        }
        deleted_odds_outcomes = 0
        missed_odds_outcomes: List[int] = []
        skip_odds_delete = os.environ.get("SUPABASE_SKIP_ODDS_OUTCOMES_DELETE", "1").strip().lower() not in {"0", "false", "no"}
        if not args.skip_odds_outcomes and not skip_odds_delete:
            deleted_odds_outcomes, missed_odds_outcomes = delete_fixture_rows("odds_outcomes", fixture_ids, args.dry_run)
        elif not args.skip_odds_outcomes and skip_odds_delete:
            log.info("Skipping odds_outcomes fixture-scoped delete (SUPABASE_SKIP_ODDS_OUTCOMES_DELETE enabled).")
        delete_stats["odds_outcomes"] = {
            "deleted": deleted_odds_outcomes,
            "missed_fixture_ids": missed_odds_outcomes,
            "delete_skipped": bool(skip_odds_delete),
        }
        missed_detail_deletes = sorted(
            set(missed_fixture_players)
            | set(missed_fixture_player_stats)
            | set(missed_fixture_stats)
        )
        if missed_detail_deletes and (explicit_fixture_ids or args.protect_empty_detail):
            raise SystemExit(
                "Refusing to publish fixture detail after incomplete target cleanup; "
                f"fixture IDs: {missed_detail_deletes[:50]}"
            )
        log.info(
            "Deleted rows: fixture_players=%s fixture_statistics=%s fixture_player_statistics=%s odds_outcomes=%s",
            deleted_fixture_players,
            deleted_fixture_stats,
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
            ]
        )

    if not args.fixture_core_only and not args.atomic_fixture_detail:
        exports.extend(
            [
                ("fixture_players", fixture_players, "fixture_id,player_id"),
                ("fixture_statistics", fixture_stats, "fixture_id,team_id,type_id"),
                ("fixture_player_statistics", fixture_player_stats, "fixture_id,player_id,type_id"),
            ]
        )

    atomic_published: Dict[str, int] = {}
    if args.atomic_fixture_detail and not args.dry_run:
        if psycopg2 is None or not SUPABASE_DB_URL:
            raise SystemExit("--atomic-fixture-detail requires SUPABASE_DB_URL and psycopg2")
        target_conn = psycopg2.connect(SUPABASE_DB_URL, connect_timeout=20)
        try:
            for fixture_id in sorted(fixture_ids):
                try:
                    atomic_fixture_detail_publish(
                        target_conn,
                        fixture_id=int(fixture_id),
                        snapshot_id=None,
                        fixture_players=[row for row in fixture_players if int(row["fixture_id"]) == int(fixture_id)],
                        fixture_statistics=[row for row in fixture_stats if int(row["fixture_id"]) == int(fixture_id)],
                        fixture_player_statistics=[
                            row for row in fixture_player_stats if int(row["fixture_id"]) == int(fixture_id)
                        ],
                    )
                    atomic_published[str(fixture_id)] = 1
                except Exception as exc:
                    failure = {
                        "status": "failed",
                        "failure_class": "database",
                        "stage": "atomic_fixture_detail_publish",
                        "fixture_id": int(fixture_id),
                        "error": str(exc)[-4000:],
                    }
                    _write_json_report(args.report_json, failure)
                    raise SystemExit(
                        f"Atomic fixture-detail publish failed for fixture {fixture_id}: {exc}"
                    ) from exc
        finally:
            target_conn.close()

    for table, rows, on_conflict in exports:
        log.info("Exporting %s (%s rows)", table, len(rows))
        exported_count, timeout_splits = upsert_table(table, rows, on_conflict, args.dry_run)
        exported[table] = exported_count
        if timeout_splits:
            log.warning("Timeout split recovery used for %s: %s split events", table, timeout_splits)

    if args.fixture_core_only or args.skip_odds_snapshots:
        exported["odds_snapshots"] = 0
    else:
        log.info("Exporting odds_snapshots (%s rows)", len(odds_snapshots))
        exported["odds_snapshots"], _ = upsert_table("odds_snapshots", odds_snapshots, "id", args.dry_run)

    if args.fixture_core_only or args.skip_odds_outcomes:
        exported["odds_outcomes"] = 0
    else:
        log.info("Exporting odds_outcomes (%s rows)", len(odds_outcomes))
        exported["odds_outcomes"], _ = upsert_table(
            "odds_outcomes",
            odds_outcomes,
            "fixture_id,bookmaker_id,market_key,selection_key,line",
            args.dry_run,
        )
    pruned = 0 if args.skip_prune else prune_fixtures(list(keep_ids), args.dry_run)

    summary = {
        "dry_run": args.dry_run,
        "fixture_core_only": args.fixture_core_only,
        "explicit_fixture_ids": explicit_fixture_ids,
        "fixtures_selected": len(fixtures),
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
        "delete_stats": delete_stats,
    }
    _write_json_report(args.report_json, summary)
    print(json.dumps(summary))


if __name__ == "__main__":
    main()
