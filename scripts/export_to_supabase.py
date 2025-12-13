import argparse
import json
import os
import sqlite3
import sys
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import requests


def chunked(seq: Sequence[dict], size: int) -> Iterable[List[dict]]:
  for i in range(0, len(seq), size):
    yield list(seq[i : i + size])


def require_env(name: str) -> str:
  value = os.getenv(name)
  if not value:
    raise RuntimeError(f"Missing env var: {name}")
  return value


def get_keep_seasons(conn: sqlite3.Connection) -> Tuple[Dict[int, List[dict]], set]:
  rows = conn.execute(
    """
    SELECT id, league_id, name, start_date, end_date, is_current
    FROM seasons
    """
  ).fetchall()

  seasons_by_league: Dict[int, List[dict]] = {}
  for row in rows:
    season = {
      "id": row[0],
      "league_id": row[1],
      "name": row[2],
      "start_date": row[3],
      "end_date": row[4],
      "is_current": bool(row[5]),
    }
    seasons_by_league.setdefault(season["league_id"], []).append(season)

  keep_by_league: Dict[int, List[dict]] = {}
  keep_ids: set = set()

  for league_id, items in seasons_by_league.items():
    items_sorted = sorted(
      items,
      key=lambda s: (s["end_date"] or "", s["start_date"] or ""),
      reverse=True,
    )
    current = [s for s in items if s["is_current"]]
    keep: List[dict] = []

    if current:
      # If multiple flagged current, keep the most recent by end_date.
      current_sorted = sorted(
        current,
        key=lambda s: (s["end_date"] or "", s["start_date"] or ""),
        reverse=True,
      )
      keep.append(current_sorted[0])

    for season in items_sorted:
      if season["id"] not in {s["id"] for s in keep}:
        keep.append(season)
      if len(keep) >= 2:
        break

    keep_by_league[league_id] = keep
    for s in keep:
      keep_ids.add(s["id"])

  return keep_by_league, keep_ids


def fetch_fixture_participants(
  conn: sqlite3.Connection, fixture_ids: Sequence[int]
) -> Dict[int, List[dict]]:
  if not fixture_ids:
    return {}
  placeholders = ",".join("?" for _ in fixture_ids)
  query = f"""
    SELECT fixture_id, team_id, location, extra
    FROM fixture_participants
    WHERE fixture_id IN ({placeholders})
  """
  rows = conn.execute(query, list(fixture_ids)).fetchall()
  by_fixture: Dict[int, List[dict]] = {}
  for row in rows:
    try:
      extra = json.loads(row[3]) if row[3] else {}
    except json.JSONDecodeError:
      extra = {}
    by_fixture.setdefault(row[0], []).append(
      {
        "team_id": row[1],
        "location": row[2],
        "extra": extra,
      }
    )
  return by_fixture


def fetch_fixtures(conn: sqlite3.Connection, season_ids: Sequence[int]) -> List[dict]:
  if not season_ids:
    return []

  placeholders = ",".join("?" for _ in season_ids)
  query = f"""
    SELECT id, league_id, season_id, starting_at, status, status_code,
           home_team_id, away_team_id, home_score, away_score
    FROM fixtures
    WHERE season_id IN ({placeholders})
  """
  rows = conn.execute(query, list(season_ids)).fetchall()

  fixtures: List[dict] = []
  for row in rows:
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


def fetch_teams(conn: sqlite3.Connection, team_ids: Sequence[int]) -> List[dict]:
  if not team_ids:
    return []
  placeholders = ",".join("?" for _ in team_ids)
  query = f"SELECT id, name, short_code FROM teams WHERE id IN ({placeholders})"
  rows = conn.execute(query, list(team_ids)).fetchall()
  teams: List[dict] = []
  for row in rows:
    teams.append({"id": row[0], "name": row[1], "short_code": row[2]})
  return teams


def upsert_table(
  rest_url: str,
  table: str,
  rows: List[dict],
  headers: dict,
  dry_run: bool,
  batch_size: int = 500,
) -> int:
  if not rows:
    return 0

  if dry_run:
    return len(rows)

  total = 0
  for batch in chunked(rows, batch_size):
    resp = requests.post(
      f"{rest_url}/{table}",
      headers=headers,
      params={"on_conflict": "id"},
      data=json.dumps(batch),
    )
    if resp.status_code >= 300:
      raise RuntimeError(
        f"Upsert failed for {table}: {resp.status_code} {resp.text}"
      )
    total += len(batch)
  return total


def delete_out_of_scope_fixtures(
  rest_url: str,
  headers: dict,
  keep_by_league: Dict[int, List[dict]],
  dry_run: bool,
) -> int:
  deleted = 0
  for league_id, seasons in keep_by_league.items():
    keep_ids = [s["id"] for s in seasons]
    if not keep_ids:
      continue
    season_list = ",".join(str(i) for i in keep_ids)
    params = {
      "league_id": f"eq.{league_id}",
      "season_id": f"not.in.({season_list})",
    }
    if dry_run:
      continue
    resp = requests.delete(f"{rest_url}/fixtures", headers=headers, params=params)
    if resp.status_code >= 300:
      raise RuntimeError(
        f"Delete failed for league {league_id}: {resp.status_code} {resp.text}"
      )
    deleted += int(resp.headers.get("Content-Range", "0").split("/")[-1] or 0)
  return deleted


def repair_fixture_home_away(
  fixtures: List[dict], participants_by_fixture: Dict[int, List[dict]]
) -> int:
  repaired = 0
  for fx in fixtures:
    if fx.get("home_team_id") and fx.get("away_team_id"):
      continue
    participants = participants_by_fixture.get(fx["id"], [])
    home_id: Optional[int] = None
    away_id: Optional[int] = None
    for p in participants:
      loc = (p.get("location") or "").lower()
      if not home_id and loc == "home":
        home_id = p.get("team_id")
      if not away_id and loc == "away":
        away_id = p.get("team_id")
      extra_loc = (p.get("extra", {}).get("meta", {}) or {}).get("location")
      if not extra_loc and isinstance(p.get("extra"), dict):
        extra_loc = p["extra"].get("location")
      extra_loc = (extra_loc or "").lower()
      if not home_id and extra_loc == "home":
        home_id = p.get("team_id")
      if not away_id and extra_loc == "away":
        away_id = p.get("team_id")
    if home_id and away_id:
      fx["home_team_id"] = home_id
      fx["away_team_id"] = away_id
      repaired += 1
  return repaired


def main() -> int:
  parser = argparse.ArgumentParser(description="Export recent seasons to Supabase")
  parser.add_argument(
    "--dry-run",
    action="store_true",
    help="Print counts without writing to Supabase",
  )
  args = parser.parse_args()

  supabase_url = os.getenv("SUPABASE_URL")
  supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
  if not args.dry_run:
    supabase_url = require_env("SUPABASE_URL")
    supabase_key = require_env("SUPABASE_SERVICE_ROLE_KEY")

  rest_url = (
    f"{supabase_url.rstrip('/')}/rest/v1" if supabase_url and supabase_key else ""
  )
  headers = (
    {
      "apikey": supabase_key,
      "Authorization": f"Bearer {supabase_key}",
      "Content-Type": "application/json",
      "Prefer": "resolution=merge-duplicates,return=minimal",
    }
    if supabase_key
    else {}
  )

  conn = sqlite3.connect("data/jxd.sqlite")
  keep_by_league, keep_ids = get_keep_seasons(conn)
  fixtures = fetch_fixtures(conn, list(keep_ids))
  participants_by_fixture = fetch_fixture_participants(
    conn, [f["id"] for f in fixtures]
  )
  repaired = repair_fixture_home_away(fixtures, participants_by_fixture)

  team_ids: set = set()
  for fx in fixtures:
    if fx["home_team_id"]:
      team_ids.add(fx["home_team_id"])
    if fx["away_team_id"]:
      team_ids.add(fx["away_team_id"])

  teams = fetch_teams(conn, list(team_ids))

  # Upserts
  seasons_rows: List[dict] = []
  for seasons in keep_by_league.values():
    seasons_rows.extend(
      {
        "id": s["id"],
        "league_id": s["league_id"],
        "name": s["name"],
        "start_date": s["start_date"],
        "end_date": s["end_date"],
        "is_current": s["is_current"],
      }
      for s in seasons
    )

  seasons_exported = upsert_table(
    rest_url, "seasons", seasons_rows, headers, args.dry_run
  )
  teams_exported = upsert_table(
    rest_url, "teams", teams, headers, args.dry_run
  )
  fixtures_exported = upsert_table(
    rest_url, "fixtures", fixtures, headers, args.dry_run, batch_size=300
  )

  deleted = delete_out_of_scope_fixtures(
    rest_url, headers, keep_by_league, args.dry_run
  )

  print(
    json.dumps(
      {
        "dry_run": args.dry_run,
        "seasons_exported": seasons_exported,
        "teams_exported": teams_exported,
        "fixtures_exported": fixtures_exported,
        "fixtures_deleted": deleted,
        "leagues_processed": len(keep_by_league),
        "fixtures_repaired": repaired,
      },
      indent=2,
    )
  )
  return 0


if __name__ == "__main__":
  sys.exit(main())
