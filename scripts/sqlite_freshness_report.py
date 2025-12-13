import datetime as dt
import json
import re
import sqlite3
from typing import Any, Dict, Optional


def to_int(value: Any) -> Optional[int]:
  if value is None:
    return None
  if isinstance(value, bool):
    return None
  if isinstance(value, (int, float)):
    return int(value)
  if isinstance(value, str):
    s = value.strip()
    if re.fullmatch(r"-?\d+", s):
      try:
        return int(s)
      except ValueError:
        return None
  return None


def parse_ts(ts: Optional[str]) -> Optional[dt.datetime]:
  if not ts:
    return None
  try:
    return dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))
  except Exception:
    try:
      # Fallback for "YYYY-MM-DD HH:MM:SS"
      return dt.datetime.fromisoformat(ts)
    except Exception:
      return None


def participant_location(location: Optional[str], extra: Optional[str]) -> str:
  loc = (location or "").lower()
  if loc:
    return loc
  if extra:
    try:
      data = json.loads(extra)
    except Exception:
      data = {}
    meta = {}
    if isinstance(data, dict):
      meta = data.get("meta") or {}
      if isinstance(meta, dict) and meta.get("location"):
        return str(meta.get("location")).lower()
      if data.get("location"):
        return str(data.get("location")).lower()
    if isinstance(meta, dict) and meta.get("location"):
      return str(meta.get("location")).lower()
  return ""


def main() -> int:
  conn = sqlite3.connect("data/jxd.sqlite")
  now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
  threshold = now - dt.timedelta(days=30)
  now_iso = now.replace(microsecond=0).isoformat() + "Z"

  fixtures_total = conn.execute("select count(*) from fixtures").fetchone()[0]
  fixtures_scored_in_fixtures = conn.execute(
    "select count(*) from fixtures where home_score is not null and away_score is not null"
  ).fetchone()[0]
  max_scored_starting_at_in_fixtures = conn.execute(
    "select max(starting_at) from fixtures where home_score is not null and away_score is not null"
  ).fetchone()[0]

  participants_rows = conn.execute(
    """
    select fp.fixture_id, fp.team_id, fp.location, fp.score, fp.extra,
           f.starting_at, f.league_id
    from fixture_participants fp
    join fixtures f on f.id = fp.fixture_id
    """
  ).fetchall()

  participants_map: Dict[int, Dict[str, Any]] = {}
  for row in participants_rows:
    fixture_id, team_id, loc_raw, score_raw, extra, starting_at, league_id = row
    loc = participant_location(loc_raw, extra)
    score_int = to_int(score_raw)
    entry = participants_map.setdefault(
      fixture_id,
      {
        "starting_at": starting_at,
        "league_id": league_id,
        "has_arsenal": False,
        "home_score": None,
        "away_score": None,
      },
    )
    if team_id == 19:
      entry["has_arsenal"] = True
    if loc == "home" and score_int is not None and entry["home_score"] is None:
      entry["home_score"] = score_int
    if loc == "away" and score_int is not None and entry["away_score"] is None:
      entry["away_score"] = score_int

  fixtures_with_participant_scores = 0
  max_scored_starting_at_from_participants: Optional[str] = None
  league_8_participant_scored = 0
  league_8_max_scored = None
  arsenal_participant_scored_since = 0

  for fixture_id, entry in participants_map.items():
    home_score = entry.get("home_score")
    away_score = entry.get("away_score")
    if home_score is None or away_score is None:
      continue
    fixtures_with_participant_scores += 1
    start_str = entry.get("starting_at")
    if start_str and (
      max_scored_starting_at_from_participants is None
      or parse_ts(start_str) > parse_ts(max_scored_starting_at_from_participants)
    ):
      max_scored_starting_at_from_participants = start_str
    if entry.get("league_id") == 8:
      league_8_participant_scored += 1
      if start_str and (
        league_8_max_scored is None
        or parse_ts(start_str) > parse_ts(league_8_max_scored)
      ):
        league_8_max_scored = start_str
    if entry.get("has_arsenal"):
      start_dt = parse_ts(start_str)
      if start_dt and start_dt >= dt.datetime(2025, 8, 1):
        arsenal_participant_scored_since += 1

  league_8_fixtures_scored_since = conn.execute(
    """
    select count(*) from fixtures
    where league_id=8
      and starting_at >= '2025-08-01'
      and home_score is not null and away_score is not null
    """
  ).fetchone()[0]

  arsenal_scored_since_fixtures = conn.execute(
    """
    select count(*) from fixtures f
    join fixture_participants fp on fp.fixture_id = f.id
    where fp.team_id = 19
      and f.starting_at >= '2025-08-01'
      and f.home_score is not null
      and f.away_score is not null
    """
  ).fetchone()[0]

  max_fix_ts = parse_ts(max_scored_starting_at_in_fixtures)
  max_part_ts = parse_ts(max_scored_starting_at_from_participants)
  stale = True
  if (max_fix_ts and max_fix_ts >= threshold) or (
    max_part_ts and max_part_ts >= threshold
  ):
    stale = False

  report = {
    "now_utc": now_iso,
    "fixtures_total": fixtures_total,
    "fixtures_scored_in_fixtures_table": fixtures_scored_in_fixtures,
    "max_scored_starting_at_in_fixtures_table": max_scored_starting_at_in_fixtures,
    "fixtures_with_participant_scores_both_sides": fixtures_with_participant_scores,
    "max_scored_starting_at_from_participants": max_scored_starting_at_from_participants,
    "league_8": {
      "fixtures_scored_total_in_fixtures_table": conn.execute(
        "select count(*) from fixtures where league_id=8 and home_score is not null and away_score is not null"
      ).fetchone()[0],
      "fixtures_scored_since_2025_08_01_in_fixtures_table": league_8_fixtures_scored_since,
      "fixtures_scored_total_from_participants": league_8_participant_scored,
      "max_scored_starting_at_from_participants": league_8_max_scored,
    },
    "team_19_arsenal": {
      "fixtures_scored_total_in_fixtures_table": conn.execute(
        """
        select count(*) from fixtures f
        join fixture_participants fp on fp.fixture_id = f.id
        where fp.team_id = 19
          and f.home_score is not null
          and f.away_score is not null
        """
      ).fetchone()[0],
      "arsenal_scored_since_2025_08_01_in_fixtures_table": arsenal_scored_since_fixtures,
      "arsenal_scored_since_2025_08_01_from_participants": arsenal_participant_scored_since,
    },
    "stale": stale,
  }

  print(json.dumps(report, indent=2))
  if stale:
    print("STALE=true")
    return 1
  return 0


if __name__ == "__main__":
  raise SystemExit(main())
