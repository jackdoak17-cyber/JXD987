#!/usr/bin/env python3
"""
Spot-check recent finished fixtures for Arsenal (team 19) in league 8.
Compares SportMonks scores vs SQLite.
"""

import json
import os
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from jxd import SportMonksClient

DB_PATH = os.environ.get("JXD_DB_PATH", "data/jxd.sqlite")
LEAGUE_ID = 8
TEAM_ID = 19
CHECK_COUNT = 5


def parse_dt(raw: Optional[str]) -> Optional[datetime]:
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("T", " ").replace("Z", ""))
    except Exception:
        return None


def extract_scores(scores_raw) -> Tuple[Optional[int], Optional[int]]:
    home = away = None
    if isinstance(scores_raw, list):
        for s in scores_raw:
            obj = s.get("score") if isinstance(s, dict) else {}
            participant = (obj or {}).get("participant") or s.get("participant")
            goals = (obj or {}).get("goals") or s.get("goals")
            if participant == "home" and home is None:
                try:
                    home = int(goals)
                except Exception:
                    pass
            if participant == "away" and away is None:
                try:
                    away = int(goals)
                except Exception:
                    pass
    elif isinstance(scores_raw, dict):
        try:
            home = int(scores_raw.get("localteam_score") or scores_raw.get("home"))
        except Exception:
            home = home
        try:
            away = int(scores_raw.get("visitorteam_score") or scores_raw.get("away"))
        except Exception:
            away = away
    return home, away


def current_season_id(conn: sqlite3.Connection) -> Optional[int]:
    cur = conn.cursor()
    cur.execute(
        """
        select id from seasons
        where league_id=?
        order by is_current desc, end_date desc
        limit 1
        """,
        (LEAGUE_ID,),
    )
    row = cur.fetchone()
    return row[0] if row else None


def fetch_recent_finished(client: SportMonksClient, season_id: int) -> List[Dict]:
    endpoint = f"schedules/seasons/{season_id}/teams/{TEAM_ID}"
    payload = client.request("GET", endpoint)
    rows = payload.get("data") if isinstance(payload, dict) else []
    fixtures = []
    for r in rows or []:
        start = parse_dt(r.get("starting_at"))
        status = (r.get("state") or {}).get("short_code") or r.get("state") or r.get("status")
        scores_raw = (r.get("scores") or r.get("score"))
        home, away = extract_scores(scores_raw)
        fixtures.append(
            {
                "id": r.get("id"),
                "starting_at": start,
                "status": status,
                "home_score": home,
                "away_score": away,
            }
        )
    fixtures = [f for f in fixtures if f["home_score"] is not None and f["away_score"] is not None and f["starting_at"]]
    fixtures.sort(key=lambda x: x["starting_at"], reverse=True)
    return fixtures[:CHECK_COUNT]


def sqlite_scores(conn: sqlite3.Connection, fixture_id: int) -> Optional[Tuple[int, int]]:
    cur = conn.cursor()
    cur.execute(
        "select home_score, away_score from fixtures where id=? and home_score is not null and away_score is not null",
        (fixture_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    return row[0], row[1]


def main():
    conn = sqlite3.connect(DB_PATH)
    season_id = current_season_id(conn)
    if not season_id:
        print(json.dumps({"error": "No season found for league 8"}))
        return 1

    client = SportMonksClient()
    fixtures = fetch_recent_finished(client, season_id)

    mismatches = []
    for fx in fixtures:
        local = sqlite_scores(conn, fx["id"])
        if not local or local != (fx["home_score"], fx["away_score"]):
            mismatches.append(
                {
                    "fixture_id": fx["id"],
                    "sportmonks": {"home": fx["home_score"], "away": fx["away_score"]},
                    "sqlite": local,
                }
            )

    summary = {
        "season_id": season_id,
        "checked": len(fixtures),
        "mismatches": len(mismatches),
        "mismatch_ids": [m["fixture_id"] for m in mismatches],
        "details": mismatches,
    }
    print(json.dumps(summary, default=str))
    return 1 if mismatches else 0


if __name__ == "__main__":
    raise SystemExit(main())
