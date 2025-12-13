import json
import os
import sqlite3
import sys
import datetime as dt
from typing import Any, Dict, List

import requests


def require_env(name: str) -> str:
  val = os.getenv(name)
  if not val:
    raise RuntimeError(f"Missing env var: {name}")
  return val


def parse_scores(scores_field: Any) -> bool:
  # scores can arrive as list or {"data": [...]}
  scores = scores_field
  if isinstance(scores_field, dict) and "data" in scores_field:
    scores = scores_field["data"]
  if not isinstance(scores, list):
    return False
  for s in scores:
    if not isinstance(s, dict):
      continue
    val = s.get("score") or s.get("result") or ""
    if isinstance(val, str) and "-" in val and any(ch.isdigit() for ch in val):
      return True
    # Sometimes scores come as goals per participant
    if "participant_id" in s and any(k in s for k in ("score", "goals", "goals_scored")):
      return True
  return False


def state_strings(state: Any) -> List[str]:
  values: List[str] = []
  if isinstance(state, dict):
    for key in ("state", "name", "short_name", "code", "type"):
      if isinstance(state.get(key), str):
        values.append(state[key].lower())
    if isinstance(state.get("data"), dict):
      values.extend(state_strings(state["data"]))
  return values


def is_finished(state: Any) -> bool:
  strings = state_strings(state)
  finished_tokens = {"ft", "aet", "pen", "finished", "full time", "fulltime"}
  return any(any(token in s for token in finished_tokens) for s in strings)


def main() -> int:
  token = require_env("SPORTMONKS_API_TOKEN")
  conn = sqlite3.connect("data/jxd.sqlite")
  now_iso = dt.datetime.utcnow().isoformat()
  rows = conn.execute(
    """
    select id, home_score, away_score, starting_at
    from fixtures
    where league_id = 8 and starting_at <= ?
    order by starting_at desc
    limit 5
    """,
    (now_iso,),
  ).fetchall()

  fixture_rows = [
    {"id": r[0], "home_score": r[1], "away_score": r[2], "starting_at": r[3]}
    for r in rows
  ]

  mismatches: List[int] = []
  checked = 0

  for fx in fixture_rows:
    fixture_id = fx["id"]
    resp = requests.get(
      f"https://api.sportmonks.com/v3/football/fixtures/{fixture_id}",
      params={"api_token": token, "include": "scores;participants;state"},
      timeout=15,
    )
    if resp.status_code >= 300:
      print(f"SportMonks call failed for {fixture_id}: {resp.status_code} {resp.text}")
      return 1
    data = resp.json().get("data", {})
    finished = is_finished(data.get("state"))
    has_scores = parse_scores(data.get("scores"))
    sqlite_missing_scores = fx["home_score"] is None or fx["away_score"] is None
    if finished and has_scores and sqlite_missing_scores:
      mismatches.append(fixture_id)
    checked += 1

  summary: Dict[str, Any] = {
    "checked": checked,
    "mismatches": len(mismatches),
    "sample_mismatch_ids": mismatches[:5],
  }
  print(json.dumps(summary, indent=2))
  return 1 if mismatches else 0


if __name__ == "__main__":
  raise SystemExit(main())
