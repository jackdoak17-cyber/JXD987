"""
Player 1+ shot overs with opponent context and odds filter.

Rules (default env-driven):
- Require odds from Bet365 player shots market (market_id=268) with decimal_odds >= ODDS_MIN (default 1.25).
- Player hit-rate: % of last N games (sample sizes) with >=1 shot must be >= PLAYER_MIN_PCT.
- Opponent concede: % of last N games allowing shots_against >= OPP_SHOTS_AGAINST_LINE must be >= OPP_MIN_PCT.
- Window: fixtures between now-LOOKBACK_DAYS and now+LOOKAHEAD_DAYS.

Env:
- JXD_DB_PATH (default data/jxd.sqlite)
- PLAYER_MIN_PCT (default 0.8)
- OPP_MIN_PCT (default 0.7)
- SAMPLE_SIZES (comma, default 5,10)
- LOOKAHEAD_DAYS (default 14)
- LOOKBACK_DAYS (default 2)
- ODDS_MIN (default 1.25)
"""

import json
import os
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

DB_PATH = os.environ.get("JXD_DB_PATH", "data/jxd.sqlite")
PLAYER_MIN_PCT = float(os.environ.get("PLAYER_MIN_PCT", "0.8"))
OPP_MIN_PCT = float(os.environ.get("OPP_MIN_PCT", "0.7"))
SAMPLE_SIZES: Sequence[int] = tuple(int(x) for x in os.environ.get("SAMPLE_SIZES", "5,10").split(","))
LOOKAHEAD_DAYS = int(os.environ.get("LOOKAHEAD_DAYS", "14"))
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "2"))
ODDS_MIN = float(os.environ.get("ODDS_MIN", "1.25"))
PLAYER_SHOTS_MARKET_ID = 268
MIN_PLAYER_SAMPLES = int(os.environ.get("MIN_PLAYER_SAMPLES", "5"))
MIN_OPP_SAMPLES = int(os.environ.get("MIN_OPP_SAMPLES", "5"))


def connect() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)


def parse_dt(raw: Optional[str]) -> Optional[datetime]:
    if not raw:
        return None
    raw_clean = raw.replace("Z", "").replace("T", " ")
    try:
        return datetime.fromisoformat(raw_clean)
    except Exception:
        return None


def load_fixture_team_map(cur: sqlite3.Cursor, require_window: bool = True) -> Dict[int, Dict[str, Any]]:
    params: List[Any] = []
    where = ""
    if require_window:
        where = "WHERE f.starting_at BETWEEN datetime('now', ?) AND datetime('now', ?)"
        params = [f"-{LOOKBACK_DAYS} days", f"+{LOOKAHEAD_DAYS} days"]
    cur.execute(
        f"""
        SELECT f.id, f.starting_at, home.team_id as home_id, away.team_id as away_id
        FROM fixtures f
        JOIN fixture_participants home ON home.fixture_id=f.id AND home.location='home'
        JOIN fixture_participants away ON away.fixture_id=f.id AND away.location='away'
        {where}
        """,
        params,
    )
    out: Dict[int, Dict[str, Any]] = {}
    for fid, start, hid, aid in cur.fetchall():
        out[fid] = {"start": start, "home": hid, "away": aid}
    return out


def load_team_fixtures(cur: sqlite3.Cursor) -> Dict[int, List[Dict[str, Any]]]:
    cur.execute(
        """
        SELECT ts.fixture_id, ts.team_id, ts.stats, f.starting_at
        FROM team_stats ts
        JOIN fixtures f ON f.id = ts.fixture_id
        WHERE f.starting_at IS NOT NULL
        """
    )
    stats_map: Dict[int, Dict[int, float]] = {}
    dates: Dict[int, datetime] = {}
    for fid, tid, stats_raw, started in cur.fetchall():
        dates[fid] = parse_dt(started) or datetime.min
        shots = None
        try:
            stats = json.loads(stats_raw) if stats_raw else []
        except Exception:
            stats = []
        if isinstance(stats, list):
            for row in stats:
                t = row.get("type") if isinstance(row, dict) else None
                name = (t.get("developer_name") or t.get("name") or "").lower() if t else ""
                data = row.get("data") if isinstance(row, dict) else None
                val = None
                if isinstance(data, dict) and "value" in data:
                    try:
                        val = float(data.get("value") or 0)
                    except Exception:
                        val = None
                elif isinstance(data, (int, float)):
                    val = float(data)
                if val is not None and ("shots_total" in name or "shots total" in name):
                    shots = val
                    break
        stats_map.setdefault(fid, {})[tid] = shots

    team_fixtures: Dict[int, List[Dict[str, Any]]] = {}
    for fid, teams in stats_map.items():
        if len(teams) < 2:
            continue
        for tid, shots_for in teams.items():
            opp_id = next((k for k in teams if k != tid), None)
            shots_against = teams.get(opp_id)
            team_fixtures.setdefault(tid, []).append(
                {
                    "fixture_id": fid,
                    "started_at": dates.get(fid),
                    "shots_for": shots_for,
                    "shots_against": shots_against,
                }
            )
    for items in team_fixtures.values():
        items.sort(key=lambda r: r["started_at"], reverse=True)
    return team_fixtures


def rate_over(vals: List[Optional[float]], line: float, min_samples: int) -> Optional[Dict[str, Any]]:
    clean = [v for v in vals if v is not None]
    required = min_samples if min_samples > 0 else 1
    required = min(required, len(clean))
    if len(clean) < required:
        return None
    hits = sum(1 for v in clean if v >= line)
    pct = hits / len(clean) if clean else 0.0
    return {
        "pct": pct,
        "total": len(clean),
        "avg": sum(clean) / len(clean) if clean else 0.0,
        "seq": ",".join(str(int(v)) for v in clean),
    }


def best_rate(fixtures: Dict[int, List[Dict[str, Any]]], entity_id: int, key: str, line: float, min_samples: int) -> Optional[Dict[str, Any]]:
    best = None
    for sample in SAMPLE_SIZES:
        vals = [fx.get(key) for fx in fixtures.get(entity_id, [])[:sample]]
        res = rate_over(vals, line, min_samples=min_samples)
        if not res:
            continue
        res["sample_size"] = sample
        if best is None or res["pct"] > best["pct"]:
            best = res
    return best


def load_player_forms(cur: sqlite3.Cursor, player_ids: List[int]) -> Dict[int, Dict[int, List[int]]]:
    placeholders = ",".join("?" for _ in player_ids)
    cur.execute(
        f"""
        SELECT player_id, sample_size, raw_fixtures
        FROM player_forms
        WHERE player_id IN ({placeholders})
        """,
        player_ids,
    )
    out: Dict[int, Dict[int, List[int]]] = {}
    for pid, sample, raw in cur.fetchall():
        try:
            fixtures = json.loads(raw) if raw else []
        except Exception:
            fixtures = []
        shots_seq: List[int] = []
        for fx in fixtures or []:
            if isinstance(fx, dict):
                val = fx.get("shots")
                try:
                    shots_seq.append(int(val) if val is not None else 0)
                except Exception:
                    shots_seq.append(0)
        out.setdefault(pid, {})[sample] = shots_seq
    return out


def load_player_odds(cur: sqlite3.Cursor) -> List[Dict[str, Any]]:
    cur.execute(
        """
        SELECT fixture_id, player_id, market_id, line, decimal_odds
        FROM player_odds
        WHERE market_id = ?
        """,
        (PLAYER_SHOTS_MARKET_ID,),
    )
    rows = []
    for fid, pid, mid, line, odds in cur.fetchall():
        try:
            line_val = float(line) if line is not None else 1.0
        except Exception:
            line_val = 1.0
        rows.append({"fixture_id": fid, "player_id": pid, "line": line_val, "odds": odds})
    return rows


def load_player_names(cur: sqlite3.Cursor, player_ids: List[int]) -> Dict[int, str]:
    if not player_ids:
        return {}
    placeholders = ",".join("?" for _ in player_ids)
    cur.execute(f"SELECT id, display_name FROM players WHERE id IN ({placeholders})", player_ids)
    return {pid: name for pid, name in cur.fetchall()}


def main() -> None:
    conn = connect()
    cur = conn.cursor()

    fixture_map = load_fixture_team_map(cur, require_window=True)
    if not fixture_map:
        print("No fixtures in window.")
        return

    team_fixtures = load_team_fixtures(cur)
    if not team_fixtures:
        print("No team stats found; ensure with-details fixtures are synced.")
        return

    odds_rows = load_player_odds(cur)
    odds_rows = [r for r in odds_rows if r["fixture_id"] in fixture_map and (r.get("odds") or 0) >= ODDS_MIN]
    if not odds_rows:
        print("No player shot odds in window meeting ODDS_MIN.")
        return

    player_ids = list({r["player_id"] for r in odds_rows})
    player_forms = load_player_forms(cur, player_ids)
    player_names = load_player_names(cur, player_ids)

    results: List[Dict[str, Any]] = []
    for row in odds_rows:
        fid = row["fixture_id"]
        pid = row["player_id"]
        line = row["line"]
        odds = row["odds"]
        fx = fixture_map.get(fid)
        if not fx:
            continue
        cur.execute("SELECT team_id FROM fixture_participants WHERE fixture_id=? AND (team_id=? OR team_id=?)", (fid, fx["home"], fx["away"]))
        teams_present = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT current_team_id FROM players WHERE id = ?", (pid,))
        row_team = cur.fetchone()
        team_id = row_team[0] if row_team else None
        if team_id not in teams_present:
            continue
        opp_id = fx["away"] if team_id == fx["home"] else fx["home"]

        player_seq = []
        best_player = None
        for sample in SAMPLE_SIZES:
            seq = player_forms.get(pid, {}).get(sample)
            if not seq:
                continue
            player_seq = seq
            res = rate_over(seq, 1, min_samples=MIN_PLAYER_SAMPLES)
            if not res:
                continue
            res["sample_size"] = sample
            if best_player is None or res["pct"] > best_player["pct"]:
                best_player = res
        if not best_player or best_player["pct"] < PLAYER_MIN_PCT:
            continue

        opp_rate = None
        for sample in SAMPLE_SIZES:
            vals = [fx_row.get("shots_against") for fx_row in team_fixtures.get(opp_id, [])[:sample]]
            res = rate_over(vals, line, min_samples=MIN_OPP_SAMPLES)
            if not res:
                continue
            res["sample_size"] = sample
            if opp_rate is None or res["pct"] > opp_rate["pct"]:
                opp_rate = res
        if not opp_rate or opp_rate["pct"] < OPP_MIN_PCT:
            continue

        results.append(
            {
                "start": fx["start"],
                "player": player_names.get(pid, f"Player {pid}"),
                "team_id": team_id,
                "opp_id": opp_id,
                "line": line,
                "odds": odds,
                "player_pct": best_player["pct"],
                "opp_pct": opp_rate["pct"],
                "player_avg": best_player["avg"],
                "opp_avg": opp_rate["avg"],
                "player_seq": best_player["seq"],
                "opp_seq": opp_rate["seq"],
                "player_sample": best_player["sample_size"],
                "opp_sample": opp_rate["sample_size"],
            }
        )

    if not results:
        print("No candidates found with odds and thresholds.")
        return

    results.sort(key=lambda r: (min(r["player_pct"], r["opp_pct"]), r["odds"]), reverse=True)
    for r in results[:50]:
        print(
            f"{r['start']} | {r['player']} | over {r['line']} @ {r['odds']:.2f} | "
            f"hit {r['player_pct']:.0%} (avg {r['player_avg']:.1f}, n@{r['player_sample']}) | "
            f"opp concede {r['opp_pct']:.0%} (avg {r['opp_avg']:.1f}, n@{r['opp_sample']}) | "
            f"seq {r['player_seq']} | opp {r['opp_seq']}"
        )


if __name__ == "__main__":
    main()
