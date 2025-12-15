#!/usr/bin/env python3
"""
Golden checks for Arsenal/Saka (League 8, Team 19 by default).
Supports SQLite (local) and Supabase REST (service role) backends.
- Results last 5 (scored fixtures only)
- Yellow cards last 5 (team stat type_id 84)
- Shots last 10 for a player (stat type_id 42) with started flag

If expected sequences are provided (env/args), exits non-zero when they mismatch.
"""
import argparse
import json
import os
import sys
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

import requests
import sqlite3

DEFAULT_LEAGUE_ID = int(os.environ.get("LEAGUE_ID", "8"))
DEFAULT_TEAM_ID = int(os.environ.get("TEAM_ID", "19"))
DEFAULT_PLAYER_NAME = os.environ.get("PLAYER_NAME", "Bukayo Saka")
DEFAULT_EXPECTED_SAKA_SHOTS_LAST10 = os.environ.get(
    "EXPECTED_SAKA_SHOTS_LAST10", "1,4,2,3,3,3,2,1,2,4"
)
DEFAULT_EXPECTED_ARSENAL_RESULTS_LAST5 = os.environ.get(
    "EXPECTED_ARSENAL_RESULTS_LAST5", "W 2-1,L 2-1,W 2-0,D 1-1,W 4-1"
)
DEFAULT_EXPECTED_ARSENAL_YELLOWS_LAST5 = os.environ.get(
    "EXPECTED_ARSENAL_YELLOWS_LAST5", "0,2,0,6,1"
)

PLAYER_SHOTS_TYPE = 42
TEAM_YELLOWS_TYPE = 84


@dataclass
class FixtureRow:
    id: int
    starting_at: Optional[str]
    home_team_id: int
    away_team_id: int
    home_score: Optional[int]
    away_score: Optional[int]


@dataclass
class AppearanceRow:
    fixture_id: int
    starting_at: Optional[str]
    lineup_type: Optional[str]


class BackendError(RuntimeError):
    pass


class SQLiteBackend:
    def __init__(self, path: str) -> None:
        self.path = path
        if not os.path.exists(path):
            raise BackendError(f"SQLite DB not found at {path}")
        self.conn = sqlite3.connect(path)

    def fixtures_for_team(self, team_id: int, limit: int, league_id: Optional[int] = None) -> List[FixtureRow]:
        cur = self.conn.cursor()
        where = ["(home_team_id = ? OR away_team_id = ?)", "home_score IS NOT NULL", "away_score IS NOT NULL"]
        params: List = [team_id, team_id]
        if league_id is not None:
            where.append("league_id = ?")
            params.append(league_id)
        sql = f"""
            SELECT id, starting_at, home_team_id, away_team_id, home_score, away_score
            FROM fixtures
            WHERE {' AND '.join(where)}
            ORDER BY starting_at DESC
            LIMIT ?
        """
        params.append(limit)
        rows = cur.execute(sql, params).fetchall()
        return [FixtureRow(*row) for row in rows]

    def yellow_cards_for_fixtures(self, fixture_ids: Sequence[int], team_id: int) -> Dict[int, Optional[int]]:
        if not fixture_ids:
            return {}
        cur = self.conn.cursor()
        q = ",".join("?" for _ in fixture_ids)
        sql = f"""
            SELECT fixture_id, value
            FROM fixture_statistics
            WHERE fixture_id IN ({q}) AND team_id = ? AND type_id = ?
        """
        rows = cur.execute(sql, [*fixture_ids, team_id, TEAM_YELLOWS_TYPE]).fetchall()
        out: Dict[int, Optional[int]] = {}
        for fid, val in rows:
            out[fid] = int(val) if val is not None else None
        return out

    def find_player_id(self, player_name: str, team_id: Optional[int]) -> int:
        cur = self.conn.cursor()
        params: List = []
        where = ["name LIKE ?"]
        params.append(f"%{player_name}%")
        if team_id:
            where.append("team_id = ?")
            params.append(team_id)
        sql = f"""
            SELECT player_id
            FROM fixture_players
            WHERE {' AND '.join(where)}
            ORDER BY fixture_id DESC
            LIMIT 1
        """
        row = cur.execute(sql, params).fetchone()
        if not row:
            raise BackendError(f"Player '{player_name}' not found in fixture_players")
        return int(row[0])

    def appearances(self, player_id: int, limit: int) -> List[AppearanceRow]:
        cur = self.conn.cursor()
        sql = """
            SELECT fp.fixture_id, f.starting_at, fp.lineup_type
            FROM fixture_players fp
            JOIN fixtures f ON f.id = fp.fixture_id
            WHERE fp.player_id = ?
            ORDER BY f.starting_at DESC
            LIMIT ?
        """
        rows = cur.execute(sql, (player_id, max(limit, 20))).fetchall()
        return [AppearanceRow(*row) for row in rows]

    def player_stats_for_fixtures(self, player_id: int, fixture_ids: Sequence[int]) -> Dict[int, Optional[int]]:
        if not fixture_ids:
            return {}
        cur = self.conn.cursor()
        q = ",".join("?" for _ in fixture_ids)
        sql = f"""
            SELECT fixture_id, value
            FROM fixture_player_statistics
            WHERE fixture_id IN ({q}) AND player_id = ? AND type_id = ?
        """
        rows = cur.execute(sql, [*fixture_ids, player_id, PLAYER_SHOTS_TYPE]).fetchall()
        out: Dict[int, Optional[int]] = {}
        for fid, val in rows:
            out[fid] = int(val) if val is not None else None
        return out


class SupabaseBackend:
    def __init__(self, url: str, key: str) -> None:
        if not url or not key:
            raise BackendError("Supabase URL/key required for supabase backend")
        self.base = url.rstrip("/") + "/rest/v1/"
        self.headers = {
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Accept": "application/json",
        }

    def _request(self, table: str, params: Dict[str, str]) -> List[Dict]:
        resp = requests.get(self.base + table, headers=self.headers, params=params, timeout=30)
        if resp.status_code == 404:
            raise BackendError(f"Supabase table '{table}' not found (404)")
        if not resp.ok:
            raise BackendError(f"Supabase request to {table} failed {resp.status_code}: {resp.text}")
        try:
            return resp.json()
        except Exception as exc:
            raise BackendError(f"Failed to parse Supabase response for {table}: {exc}") from exc

    def fixtures_for_team(self, team_id: int, limit: int, league_id: Optional[int] = None) -> List[FixtureRow]:
        params = {
            "select": "id,starting_at,home_team_id,away_team_id,home_score,away_score,league_id",
            "or": f"(home_team_id.eq.{team_id},away_team_id.eq.{team_id})",
            "home_score": "not.is.null",
            "away_score": "not.is.null",
            "order": "starting_at.desc",
            "limit": str(max(limit, 20)),
        }
        if league_id is not None:
            params["league_id"] = f"eq.{league_id}"
        rows = self._request("fixtures", params)
        fixtures: List[FixtureRow] = []
        for r in rows:
            hs = r.get("home_score")
            ascore = r.get("away_score")
            fixtures.append(
                FixtureRow(
                    id=int(r["id"]),
                    starting_at=r.get("starting_at"),
                    home_team_id=int(r["home_team_id"]),
                    away_team_id=int(r["away_team_id"]),
                    home_score=int(hs) if hs is not None else None,
                    away_score=int(ascore) if ascore is not None else None,
                )
            )
        return fixtures

    def yellow_cards_for_fixtures(self, fixture_ids: Sequence[int], team_id: int) -> Dict[int, Optional[int]]:
        if not fixture_ids:
            return {}
        ids = ",".join(str(i) for i in fixture_ids)
        params = {
            "select": "fixture_id,value",
            "fixture_id": f"in.({ids})",
            "team_id": f"eq.{team_id}",
            "type_id": f"eq.{TEAM_YELLOWS_TYPE}",
        }
        rows = self._request("fixture_statistics", params)
        out: Dict[int, Optional[int]] = {}
        for r in rows:
            val = r.get("value")
            out[int(r["fixture_id"])] = int(val) if val is not None else None
        return out

    def find_player_id(self, player_name: str, team_id: Optional[int]) -> int:
        params = {
            "select": "player_id",
            "name": f"ilike.%{player_name}%",
            "order": "fixture_id.desc",
            "limit": "1",
        }
        if team_id:
            params["team_id"] = f"eq.{team_id}"
        rows = self._request("fixture_players", params)
        if not rows:
            raise BackendError(f"Player '{player_name}' not found in fixture_players (Supabase)")
        return int(rows[0]["player_id"])

    def appearances(self, player_id: int, limit: int) -> List[AppearanceRow]:
        params = {
            "select": "fixture_id,lineup_type",
            "player_id": f"eq.{player_id}",
            "order": "fixture_id.desc",
            "limit": str(max(limit, 40)),
        }
        rows = self._request("fixture_players", params)
        fixture_ids = [int(r["fixture_id"]) for r in rows]
        if not fixture_ids:
            return []
        ids = ",".join(str(i) for i in fixture_ids)
        fixture_rows = self._request(
            "fixtures",
            {
                "select": "id,starting_at",
                "id": f"in.({ids})",
            },
        )
        start_map = {int(r["id"]): r.get("starting_at") for r in fixture_rows}
        appearances = [
            AppearanceRow(fixture_id=fid, starting_at=start_map.get(fid), lineup_type=r.get("lineup_type"))
            for fid, r in zip(fixture_ids, rows)
        ]
        appearances.sort(key=lambda x: x.starting_at or "", reverse=True)
        return appearances[:limit]

    def player_stats_for_fixtures(self, player_id: int, fixture_ids: Sequence[int]) -> Dict[int, Optional[int]]:
        if not fixture_ids:
            return {}
        ids = ",".join(str(i) for i in fixture_ids)
        params = {
            "select": "fixture_id,value",
            "fixture_id": f"in.({ids})",
            "player_id": f"eq.{player_id}",
            "type_id": f"eq.{PLAYER_SHOTS_TYPE}",
        }
        rows = self._request("fixture_player_statistics", params)
        out: Dict[int, Optional[int]] = {}
        for r in rows:
            val = r.get("value")
            out[int(r["fixture_id"])] = int(val) if val is not None else None
        return out


def is_starter(lineup_type: Optional[str]) -> bool:
    if lineup_type is None:
        return False
    normalized = str(lineup_type).lower()
    return normalized in {"11", "lineup", "starting", "starter", "1"}


def format_results_sequence(fixtures: List[Dict]) -> str:
    return ",".join(f"{f['result']} {f['score']}" for f in fixtures)


def format_numeric_sequence(values: List[Optional[int]]) -> str:
    return ",".join("n/a" if v is None else str(v) for v in values)


def compute_results(fixture_rows: List[FixtureRow], team_id: int, limit: int = 5) -> List[Dict]:
    rows = fixture_rows[:limit]
    results = []
    for f in rows:
        is_home = f.home_team_id == team_id
        gf_raw = f.home_score if is_home else f.away_score
        ga_raw = f.away_score if is_home else f.home_score
        gf = int(gf_raw) if gf_raw is not None else None
        ga = int(ga_raw) if ga_raw is not None else None
        if gf is None or ga is None:
            result = "?"
        elif gf > ga:
            result = "W"
        elif gf < ga:
            result = "L"
        else:
            result = "D"
        results.append(
            {
                "fixture_id": f.id,
                "date": f.starting_at,
                "home_team_id": f.home_team_id,
                "away_team_id": f.away_team_id,
                "score": f"{f.home_score}-{f.away_score}",
                "result": result,
            }
        )
    return results


def compute_yellows(
    fixture_rows: List[FixtureRow],
    yellows_map: Dict[int, Optional[int]],
    limit: int = 5,
) -> List[Dict]:
    rows = fixture_rows[:limit]
    out = []
    for f in rows:
        out.append(
            {
                "fixture_id": f.id,
                "date": f.starting_at,
                "yellow_cards": yellows_map.get(f.id),
            }
        )
    return out


def compute_player_shots(
    appearances: List[AppearanceRow], stats_map: Dict[int, Optional[int]], limit: int = 10
) -> List[Dict]:
    rows = appearances[:limit]
    out = []
    for app in rows:
        out.append(
            {
                "fixture_id": app.fixture_id,
                "date": app.starting_at,
                "shots": stats_map.get(app.fixture_id),
                "started": is_starter(app.lineup_type),
            }
        )
    return out


def parse_expected_list(raw: Optional[str]) -> Optional[List[str]]:
    if raw is None or raw == "":
        return None
    return [part.strip() for part in raw.split(",")]


def main():
    parser = argparse.ArgumentParser(description="Golden checks for Arsenal/Saka data")
    parser.add_argument("--backend", choices=["sqlite", "supabase"], default="sqlite")
    parser.add_argument("--db-path", default=os.environ.get("JXD_DB_PATH", "data/jxd.sqlite"))
    parser.add_argument("--league-id", type=int, default=DEFAULT_LEAGUE_ID)
    parser.add_argument("--team-id", type=int, default=DEFAULT_TEAM_ID)
    parser.add_argument("--player-name", default=DEFAULT_PLAYER_NAME)
    parser.add_argument("--expected-saka-shots", default=DEFAULT_EXPECTED_SAKA_SHOTS_LAST10)
    parser.add_argument("--expected-arsenal-results", default=DEFAULT_EXPECTED_ARSENAL_RESULTS_LAST5)
    parser.add_argument("--expected-arsenal-yellows", default=DEFAULT_EXPECTED_ARSENAL_YELLOWS_LAST5)
    args = parser.parse_args()

    expected_shots = parse_expected_list(args.expected_saka_shots)
    expected_results = parse_expected_list(args.expected_arsenal_results)
    expected_yellows = parse_expected_list(args.expected_arsenal_yellows)

    backend = None
    if args.backend == "sqlite":
        backend = SQLiteBackend(args.db_path)
    else:
        supabase_url = os.environ.get("SUPABASE_URL")
        supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        backend = SupabaseBackend(supabase_url, supabase_key)

    team_fixtures = backend.fixtures_for_team(args.team_id, limit=20, league_id=args.league_id)
    if not team_fixtures:
        raise SystemExit("No fixtures found for team")

    results = compute_results(team_fixtures, args.team_id)
    yellows_map = backend.yellow_cards_for_fixtures([f.id for f in team_fixtures], args.team_id)
    yellows = compute_yellows(team_fixtures, yellows_map)

    player_id = backend.find_player_id(args.player_name, args.team_id)
    appearances = backend.appearances(player_id, limit=15)
    stats_map = backend.player_stats_for_fixtures(player_id, [a.fixture_id for a in appearances])
    shots = compute_player_shots(appearances, stats_map)

    sequences = {
        "results": format_results_sequence(results),
        "yellows": format_numeric_sequence([r["yellow_cards"] for r in yellows]),
        "shots": format_numeric_sequence([s["shots"] for s in shots]),
    }

    checks = []
    success = True
    if expected_results is not None:
        match = expected_results == sequences["results"].split(",")
        checks.append({"name": "arsenal_results_last5", "expected": expected_results, "actual": sequences["results"].split(","), "match": match})
        success = success and match
    if expected_yellows is not None:
        match = expected_yellows == sequences["yellows"].split(",")
        checks.append({"name": "arsenal_yellows_last5", "expected": expected_yellows, "actual": sequences["yellows"].split(","), "match": match})
        success = success and match
    if expected_shots is not None:
        match = expected_shots == sequences["shots"].split(",")
        checks.append({"name": "saka_shots_last10", "expected": expected_shots, "actual": sequences["shots"].split(","), "match": match})
        success = success and match

    output = {
        "ok": success,
        "backend": args.backend,
        "league_id": args.league_id,
        "team_id": args.team_id,
        "player_name": args.player_name,
        "player_id": player_id,
        "sequences": sequences,
        "results_last5": results,
        "yellows_last5": yellows,
        "shots_last10": shots,
        "checks": checks,
    }
    print(json.dumps(output, indent=2))
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    try:
        main()
    except BackendError as exc:
        print(json.dumps({"ok": False, "error": str(exc)}))
        sys.exit(1)
