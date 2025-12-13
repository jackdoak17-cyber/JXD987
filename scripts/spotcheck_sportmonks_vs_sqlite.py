import datetime as dt
import json
import os
import re
import sqlite3
import sys
from typing import Any, Dict, List, Optional, Tuple

import requests


def require_env(name: str) -> str:
  val = os.getenv(name)
  if not val:
    raise RuntimeError(f"Missing env var: {name}")
  return val


def to_int(val: Any) -> Optional[int]:
  if val is None or isinstance(val, bool):
    return None
  if isinstance(val, (int, float)):
    return int(val)
  if isinstance(val, str):
    s = val.strip()
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
      return dt.datetime.fromisoformat(ts)
    except Exception:
      return None


def participant_location(location: Any, extra: Any) -> str:
  loc = (location or "").lower()
  if loc:
    return loc
  data = extra
  if isinstance(extra, str):
    try:
      data = json.loads(extra)
    except Exception:
      data = {}
  if isinstance(data, dict):
    meta = data.get("meta") or {}
    if isinstance(meta, dict) and meta.get("location"):
      return str(meta.get("location")).lower()
    if data.get("location"):
      return str(data.get("location")).lower()
  return ""


def is_finished(state: Any) -> bool:
  strings: List[str] = []
  if isinstance(state, dict):
    for key in ("state", "name", "short_name", "code", "type", "short_code"):
      val = state.get(key)
      if isinstance(val, str):
        strings.append(val.lower())
    if isinstance(state.get("data"), dict):
      for key, val in state["data"].items():
        if isinstance(val, str):
          strings.append(val.lower())
  finished_tokens = {"ft", "aet", "pen", "finished", "full time", "fulltime"}
  return any(any(tok in s for tok in finished_tokens) for s in strings)


def parse_fixture_scores_from_api(data: dict) -> Tuple[Optional[int], Optional[int], Optional[str]]:
  participants = data.get("participants")
  if isinstance(participants, dict) and "data" in participants:
    participants = participants.get("data")
  home_score = None
  away_score = None
  if isinstance(participants, list):
    for p in participants:
      if not isinstance(p, dict):
        continue
      loc = participant_location(p.get("location"), p.get("meta") or p.get("extra"))
      score_val = p.get("score")
      score_int = to_int(score_val)
      if loc == "home" and score_int is not None and home_score is None:
        home_score = score_int
      if loc == "away" and score_int is not None and away_score is None:
        away_score = score_int
  return home_score, away_score, data.get("starting_at")


def get_current_season_id(conn: sqlite3.Connection, league_id: int = 8) -> Optional[int]:
  row = conn.execute(
    """
    select id from seasons
    where league_id=?
    order by is_current desc, end_date desc
    limit 1
    """,
    (league_id,),
  ).fetchone()
  return row[0] if row else None


def derive_sqlite_scores(conn: sqlite3.Connection, fixture_id: int, valid_team_ids: set) -> Tuple[Optional[str], Optional[int], Optional[int]]:
  row = conn.execute(
    "select starting_at, home_score, away_score from fixtures where id=?",
    (fixture_id,),
  ).fetchone()
  if not row:
    return None, None, None
  starting_at, home_score, away_score = row
  if home_score is not None and away_score is not None:
    return starting_at, home_score, away_score

  participants = conn.execute(
    """
    select location, score, extra, team_id
    from fixture_participants
    where fixture_id=?
    """,
    (fixture_id,),
  ).fetchall()

  home = []
  away = []
  for loc_raw, score_raw, extra, team_id in participants:
    loc = participant_location(loc_raw, extra)
    if team_id not in valid_team_ids:
      continue
    if loc == "home":
      home.append(to_int(score_raw))
    if loc == "away":
      away.append(to_int(score_raw))

  if len(home) == 1 and len(away) == 1 and home[0] is not None and away[0] is not None:
    return starting_at, home[0], away[0]

  return starting_at, home_score, away_score


def main() -> int:
  token = require_env("SPORTMONKS_API_TOKEN")
  conn = sqlite3.connect("data/jxd.sqlite")
  season_id = get_current_season_id(conn, league_id=8)
  if not season_id:
    print("No season found for league 8 in SQLite.")
    return 1

  team_ids = {row[0] for row in conn.execute("select id from teams").fetchall()}

  sched_resp = requests.get(
    f"https://api.sportmonks.com/v3/football/schedules/seasons/{season_id}/teams/19",
    params={"api_token": token},
    timeout=20,
  )
  if sched_resp.status_code >= 300:
    print(f"Schedule fetch failed: {sched_resp.status_code} {sched_resp.text}")
    return 1

  schedule = sched_resp.json().get("data") or []
  items = []
  for item in schedule:
    if not isinstance(item, dict):
      continue
    fixture_id = item.get("fixture_id") or item.get("id")
    start = (
      item.get("starting_at")
      or item.get("start_at")
      or item.get("datetime")
      or item.get("date")
    )
    items.append(
      {
        "id": fixture_id,
        "starting_at": start,
        "start_dt": parse_ts(start),
      }
    )

  items = [i for i in items if i["id"]]
  items.sort(key=lambda x: x["start_dt"] or dt.datetime.min, reverse=True)

  scored_fixtures: List[Dict[str, Any]] = []

  for item in items:
    if len(scored_fixtures) >= 8:  # fetch a few extra then trim
      break
    fixture_id = item["id"]
    fx_resp = requests.get(
      f"https://api.sportmonks.com/v3/football/fixtures/{fixture_id}",
      params={"api_token": token, "include": "scores;participants;state"},
      timeout=15,
    )
    if fx_resp.status_code >= 300:
      print(f"Fixture fetch failed {fixture_id}: {fx_resp.status_code} {fx_resp.text}")
      return 1
    data = fx_resp.json().get("data", {})
    if not is_finished(data.get("state")):
      continue
    sm_home, sm_away, sm_start = parse_fixture_scores_from_api(data)
    if sm_home is None or sm_away is None:
      continue
    scored_fixtures.append(
      {
        "id": fixture_id,
        "sm_home": sm_home,
        "sm_away": sm_away,
        "starting_at": sm_start or item.get("starting_at"),
        "start_dt": parse_ts(sm_start) or item.get("start_dt"),
      }
    )

  scored_fixtures.sort(key=lambda x: x["start_dt"] or dt.datetime.min, reverse=True)
  scored_fixtures = scored_fixtures[:5]

  mismatches: List[Dict[str, Any]] = []
  checked = 0

  for fx in scored_fixtures:
    sqlite_start, sqlite_home, sqlite_away = derive_sqlite_scores(
      conn, fx["id"], team_ids
    )
    checked += 1
    if sqlite_home is None or sqlite_away is None:
      mismatches.append(
        {
          "fixture_id": fx["id"],
          "sportmonks_score": [fx["sm_home"], fx["sm_away"]],
          "sqlite_score": [sqlite_home, sqlite_away],
          "sqlite_start": sqlite_start,
        }
      )
      continue
    if sqlite_home != fx["sm_home"] or sqlite_away != fx["sm_away"]:
      mismatches.append(
        {
          "fixture_id": fx["id"],
          "sportmonks_score": [fx["sm_home"], fx["sm_away"]],
          "sqlite_score": [sqlite_home, sqlite_away],
          "sqlite_start": sqlite_start,
        }
      )

  summary: Dict[str, Any] = {
    "season_id": season_id,
    "checked": checked,
    "mismatches": len(mismatches),
    "details": mismatches[:10],
  }
  print(json.dumps(summary, indent=2))
  return 1 if mismatches else 0


if __name__ == "__main__":
  raise SystemExit(main())
