import datetime as dt
import json
import sqlite3
from typing import Any, Optional


def scalar(conn: sqlite3.Connection, query: str, params: tuple = (), default: Any = None):
  row = conn.execute(query, params).fetchone()
  return row[0] if row and row[0] is not None else default


def parse_iso(ts: Optional[str]) -> Optional[dt.datetime]:
  if not ts:
    return None
  try:
    return dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))
  except Exception:
    return None


def main() -> int:
  conn = sqlite3.connect("data/jxd.sqlite")

  now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
  now_iso = now.replace(microsecond=0).isoformat() + "Z"
  thirty_days_ago = (now - dt.timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")

  teams_count = scalar(conn, "select count(*) from teams", default=0)
  teams_named = scalar(
    conn,
    "select count(*) from teams where name is not null and trim(name) != ''",
    default=0,
  )
  seasons_count = scalar(conn, "select count(*) from seasons", default=0)

  fixtures_total = scalar(conn, "select count(*) from fixtures", default=0)
  fixtures_scored_total = scalar(
    conn,
    "select count(*) from fixtures where home_score is not null and away_score is not null",
    default=0,
  )
  fixtures_scored_last_30d = scalar(
    conn,
    """
    select count(*) from fixtures
    where starting_at >= ?
      and home_score is not null
      and away_score is not null
    """,
    (thirty_days_ago,),
    default=0,
  )

  max_fixture_starting_at = scalar(
    conn, "select max(starting_at) from fixtures", default=None
  )
  max_scored_starting_at = scalar(
    conn,
    "select max(starting_at) from fixtures where home_score is not null and away_score is not null",
    default=None,
  )

  league_8 = {
    "fixtures_total": scalar(
      conn, "select count(*) from fixtures where league_id=8", default=0
    ),
    "fixtures_scored_total": scalar(
      conn,
      """
      select count(*) from fixtures
      where league_id=8 and home_score is not null and away_score is not null
      """,
      default=0,
    ),
    "fixtures_scored_since_2025_08_01": scalar(
      conn,
      """
      select count(*) from fixtures
      where league_id=8
        and starting_at >= '2025-08-01'
        and home_score is not null
        and away_score is not null
      """,
      default=0,
    ),
    "max_scored_starting_at": scalar(
      conn,
      """
      select max(starting_at) from fixtures
      where league_id=8 and home_score is not null and away_score is not null
      """,
      default=None,
    ),
  }

  team_19 = {
    "fixtures_scored_total": scalar(
      conn,
      """
      select count(*) from fixtures f
      join fixture_participants fp on fp.fixture_id = f.id
      where fp.team_id = 19
        and f.home_score is not null
        and f.away_score is not null
      """,
      default=0,
    ),
    "fixtures_scored_since_2025_08_01": scalar(
      conn,
      """
      select count(*) from fixtures f
      join fixture_participants fp on fp.fixture_id = f.id
      where fp.team_id = 19
        and f.starting_at >= '2025-08-01'
        and f.home_score is not null
        and f.away_score is not null
      """,
      default=0,
    ),
    "max_scored_starting_at": scalar(
      conn,
      """
      select max(f.starting_at) from fixtures f
      join fixture_participants fp on fp.fixture_id = f.id
      where fp.team_id = 19
        and f.home_score is not null
        and f.away_score is not null
      """,
      default=None,
    ),
  }

  report = {
    "now_utc": now_iso,
    "teams_count": teams_count,
    "teams_named": teams_named,
    "seasons_count": seasons_count,
    "fixtures_total": fixtures_total,
    "fixtures_scored_total": fixtures_scored_total,
    "fixtures_scored_last_30d": fixtures_scored_last_30d,
    "max_fixture_starting_at": max_fixture_starting_at,
    "max_scored_starting_at": max_scored_starting_at,
    "league_8": league_8,
    "team_19_arsenal": team_19,
  }

  print(json.dumps(report, indent=2))
  return 0


if __name__ == "__main__":
  raise SystemExit(main())
