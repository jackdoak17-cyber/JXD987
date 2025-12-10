"""
Fixture-level over candidates for match shots and team shots, with opponent context.

Rules:
- Use team form sequences (shots_for/against, match_shots) to compute hit rates for both teams.
- Keep rows where:
    * Team's over hit-rate >= TEAM_VALUE_MIN_PCT, and
    * Opponent's concede rate (team shots against) >= TEAM_VALUE_MIN_PCT (for team shots), or
    * Combined match_shots over hit-rate >= TEAM_VALUE_MIN_PCT (for match shots).
- Attach Bet365 odds if present (odds_latest: market_id 292 = match shots, 285 = team shots).

Configuration (env):
- JXD_DB_PATH: path to SQLite DB (default: data/jxd.sqlite)
- TEAM_VALUE_MIN_PCT: minimum hit rate to surface (default: 0.8)
- MIN_SAMPLES: minimum non-null samples per side (default: 8)

Assumes:
- `team_forms` populated via `python -m jxd.cli compute-forms` (stores raw_fixtures with shots_for/against).
- `team_stats` + `fixtures` + `fixture_participants` to map teams to fixtures.
- Odds for Bet365 are present in odds_latest.
"""

import json
import os
import sqlite3
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

DB_PATH = os.environ.get("JXD_DB_PATH", "data/jxd.sqlite")
SAMPLE_SIZES: Sequence[int] = (10, 20)
MATCH_SHOT_LINES: Sequence[float] = (22.5, 24.5, 26.5, 28.5, 30.5)
TEAM_SHOT_LINES: Sequence[float] = (8.5, 9.5, 10.5, 11.5, 12.5, 13.5)
MIN_PCT = float(os.environ.get("TEAM_VALUE_MIN_PCT", "0.8"))
MIN_SAMPLES = int(os.environ.get("TEAM_VALUE_MIN_SAMPLES", str(8)))
MAX_ROWS = 80
LOOKAHEAD_DAYS = int(os.environ.get("TEAM_VALUE_LOOKAHEAD_DAYS", "14"))
LOOKBACK_DAYS = int(os.environ.get("TEAM_VALUE_LOOKBACK_DAYS", "2"))
REQUIRE_ODDS = os.environ.get("TEAM_VALUE_REQUIRE_ODDS", "1") not in ("0", "false", "False")


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


def parse_shot_stats(raw_stats: Any) -> Dict[str, Optional[float]]:
    """
    Extract shot totals/on-target from a team_stats.stats payload.
    """
    stats = raw_stats
    if isinstance(raw_stats, str):
        try:
            stats = json.loads(raw_stats)
        except Exception:
            stats = []
    if not isinstance(stats, list):
        return {"shots": None, "sot": None}

    shots = sot = None
    for row in stats:
        stat_type = row.get("type") if isinstance(row, dict) else None
        name = (stat_type.get("developer_name") or stat_type.get("name") or "").lower() if stat_type else ""
        data = row.get("data") if isinstance(row, dict) else None
        val: Optional[float] = None
        if isinstance(data, dict) and "value" in data:
            try:
                val = float(data.get("value") or 0)
            except Exception:
                val = None
        elif isinstance(data, (int, float)):
            val = float(data)
        if val is None:
            continue
        if "shots_total" in name or "shots total" in name:
            shots = val
        elif "shots_on_target" in name or "shots on target" in name:
            sot = val
    return {"shots": shots, "sot": sot}


def parse_line_val(raw: Optional[str]) -> Optional[float]:
    if raw is None:
        return None
    txt = str(raw).strip()
    # Strip leading Over/Under labels if present
    low = txt.lower()
    for token in ("over", "under"):
        if low.startswith(token):
            txt = txt[len(token) :].strip()
            break
    try:
        return float(txt)
    except Exception:
        return None


def load_team_fixtures(cur: sqlite3.Cursor, fixture_team_map: Dict[int, Dict[str, int]]) -> Dict[int, List[Dict[str, Any]]]:
    """
    Build per-team fixture stats with shots for/against and match totals from team_stats.
    """
    cur.execute(
        """
        SELECT ts.fixture_id, ts.team_id, ts.location, ts.stats, f.starting_at
        FROM team_stats ts
        JOIN fixtures f ON f.id = ts.fixture_id
        WHERE f.starting_at IS NOT NULL
        """
    )
    fixtures: Dict[int, Dict[int, Dict[str, Any]]] = {}
    fixture_dates: Dict[int, datetime] = {}
    for fixture_id, team_id, location, stats_raw, started_at in cur.fetchall():
        dt = parse_dt(started_at)
        if not dt:
            continue
        parsed = parse_shot_stats(stats_raw)
        fixtures.setdefault(fixture_id, {})[team_id] = {
            "shots": parsed["shots"],
            "location": (location or "").lower(),
        }
        fixture_dates[fixture_id] = dt

    team_fixtures: Dict[int, List[Dict[str, Any]]] = {}
    for fixture_id, teams in fixtures.items():
        started_at = fixture_dates.get(fixture_id)
        if not started_at or len(teams) < 2:
            continue
        team_map = fixture_team_map.get(fixture_id) or {}
        for team_id, data in teams.items():
            # determine opponent
            opp_id = None
            if team_map:
                if team_map.get("home") == team_id:
                    opp_id = team_map.get("away")
                elif team_map.get("away") == team_id:
                    opp_id = team_map.get("home")
            if opp_id is None:
                opp_id = next((tid for tid in teams.keys() if tid != team_id), None)
            opp = teams.get(opp_id) if opp_id else None
            shots_for = data.get("shots")
            shots_against = opp.get("shots") if opp else None
            match_shots = (
                (shots_for or 0) + (shots_against or 0) if shots_for is not None and shots_against is not None else None
            )
            team_fixtures.setdefault(team_id, []).append(
                {
                    "fixture_id": fixture_id,
                    "started_at": started_at,
                    "location": data.get("location") or "",
                    "opp_id": opp_id,
                    "shots_for": shots_for,
                    "shots_against": shots_against,
                    "match_shots": match_shots,
                }
            )
    for items in team_fixtures.values():
        items.sort(key=lambda r: r["started_at"], reverse=True)
    return team_fixtures


def fetch_fixture_team_map(cur: sqlite3.Cursor) -> Dict[int, Dict[str, int]]:
    """
    fixture_id -> {"home": team_id, "away": team_id}
    """
    cur.execute("SELECT fixture_id, team_id, location FROM fixture_participants")
    out: Dict[int, Dict[str, int]] = {}
    for fixture_id, team_id, loc in cur.fetchall():
        loc_key = (loc or "").lower()
        if loc_key not in ("home", "away"):
            continue
        out.setdefault(fixture_id, {})[loc_key] = team_id
    return out


def load_odds(cur: sqlite3.Cursor, fixture_team_map: Dict[int, Dict[str, int]]) -> Tuple[
    Dict[int, Dict[float, float]], Dict[int, Dict[str, Dict[float, float]]]
]:
    """
    Returns:
    - match_over_odds: fixture_id -> line -> odds
    - team_over_odds: fixture_id -> {home|away: {line -> odds}}
    """
    match_over_odds: Dict[int, Dict[float, float]] = {}
    team_over_odds: Dict[int, Dict[str, Dict[float, float]]] = {}
    cur.execute(
        """
        SELECT fixture_id, market_id, selection, line, decimal_odds, raw
        FROM odds_latest
        WHERE bookmaker_id = 2 AND market_id IN (292, 285)
        """
    )
    for fixture_id, market_id, selection, line, decimal_odds, raw_json in cur.fetchall():
        try:
            raw = json.loads(raw_json) if raw_json else {}
        except Exception:
            raw = {}
        total_str = raw.get("total") or selection or line
        line_val = parse_line_val(total_str)
        if line_val is None:
            continue
        sel_text = (selection or "").lower()
        total_text = (total_str or "").lower()
        if "under" in sel_text or "under" in total_text:
            continue
        if market_id == 292:
            match_over_odds.setdefault(fixture_id, {})[line_val] = decimal_odds
        elif market_id == 285:
            label = str(raw.get("label") or "").lower()
            loc = "home" if label == "1" else "away" if label == "2" else None
            if not loc:
                continue
            team_over_odds.setdefault(fixture_id, {}).setdefault(loc, {})[line_val] = decimal_odds
    return match_over_odds, team_over_odds


def truncate_seq(seq: Iterable[float], limit: int) -> str:
    vals = [str(int(v)) for v in seq if v is not None][:limit]
    return ",".join(vals)


def rate_over(values: List[Optional[float]], line: float, min_samples: int) -> Optional[Dict[str, Any]]:
    clean = [v for v in values if v is not None]
    if len(clean) < min_samples:
        return None
    over = sum(1 for v in clean if v >= line)
    pct = over / len(clean)
    avg = sum(clean) / len(clean) if clean else 0.0
    return {"pct": pct, "total": len(clean), "avg": avg, "seq": truncate_seq(clean, len(clean))}


def select_best_rate(team_fixtures: Dict[int, List[Dict[str, Any]]], team_id: int, sample_sizes: Sequence[int], key: str, line: float) -> Optional[Dict[str, Any]]:
    best: Optional[Dict[str, Any]] = None
    for sample_size in sample_sizes:
        fixtures = team_fixtures.get(team_id, [])
        vals = [fx.get(key) for fx in fixtures[:sample_size]]
        res = rate_over(vals, line, MIN_SAMPLES)
        if not res:
            continue
        res["sample_size"] = sample_size
        if best is None or res["pct"] > best["pct"]:
            best = res
    return best


def load_fixture_info(cur: sqlite3.Cursor, require_odds: bool) -> Dict[int, Dict[str, Any]]:
    """
    Pull fixtures in the date window; optionally require odds_latest entries for shot markets.
    """
    cur.execute("SELECT datetime('now')")  # ensure sqlite now() available
    odds_join = ""
    odds_where = ""
    if require_odds:
        odds_join = "JOIN odds_latest o ON o.fixture_id = f.id AND o.market_id IN (285,292) AND o.bookmaker_id = 2"
        odds_where = "AND o.fixture_id IS NOT NULL"
    cur.execute(
        f"""
        SELECT f.id, f.starting_at, fp_home.team_id AS home_id, fp_away.team_id AS away_id
        FROM fixtures f
        JOIN fixture_participants fp_home ON fp_home.fixture_id = f.id AND fp_home.location = 'home'
        JOIN fixture_participants fp_away ON fp_away.fixture_id = f.id AND fp_away.location = 'away'
        {odds_join}
        WHERE f.starting_at BETWEEN datetime('now', ?) AND datetime('now', ?)
        {odds_where}
        """,
        (f"-{LOOKBACK_DAYS} days", f"+{LOOKAHEAD_DAYS} days"),
    )
    out: Dict[int, Dict[str, Any]] = {}
    for fid, start, home_id, away_id in cur.fetchall():
        out[fid] = {"start": start, "home": home_id, "away": away_id}
    return out


def build_fixture_rows(
    teams: Dict[int, str],
    team_fixtures: Dict[int, List[Dict[str, Any]]],
    fixtures: Dict[int, Dict[str, Any]],
    match_odds: Dict[int, Dict[float, float]],
    team_odds: Dict[int, Dict[str, Dict[float, float]]],
    sample_sizes: Sequence[int],
    min_pct: float,
    min_samples: int,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for fixture_id, info in fixtures.items():
        home_id = info["home"]
        away_id = info["away"]
        start = info["start"]
    cur.execute(
        """
        SELECT f.id, f.starting_at, fp_home.team_id AS home_id, fp_away.team_id AS away_id
        FROM fixtures f
        JOIN fixture_participants fp_home ON fp_home.fixture_id = f.id AND fp_home.location = 'home'
        JOIN fixture_participants fp_away ON fp_away.fixture_id = f.id AND fp_away.location = 'away'
        WHERE f.starting_at IS NOT NULL
        """
    )
    out: Dict[int, Dict[str, Any]] = {}
    for fid, start, home_id, away_id in cur.fetchall():
        out[fid] = {"start": start, "home": home_id, "away": away_id}
    return out


def build_fixture_rows(
    teams: Dict[int, str],
    team_fixtures: Dict[int, List[Dict[str, Any]]],
    fixtures: Dict[int, Dict[str, Any]],
    match_odds: Dict[int, Dict[float, float]],
    team_odds: Dict[int, Dict[str, Dict[float, float]]],
    sample_sizes: Sequence[int],
    min_pct: float,
    min_samples: int,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for fixture_id, info in fixtures.items():
        home_id = info["home"]
        away_id = info["away"]
        start = info["start"]
        # Team shots overs (home/away)
        team_lines = team_odds.get(fixture_id, {})
        sides = ["home", "away"]
        for side in sides:
            lines = team_lines.get(side, {}) if REQUIRE_ODDS else team_lines.get(side, {})
            line_keys = list(lines.keys()) if lines else ([] if REQUIRE_ODDS else list(TEAM_SHOT_LINES))
            team_id = home_id if side == "home" else away_id
            opp_id = away_id if side == "home" else home_id
            for line in line_keys:
                odds = lines.get(line)
                team_rate = select_best_rate(team_fixtures, team_id, sample_sizes, "shots_for", line)
                opp_concede = select_best_rate(team_fixtures, opp_id, sample_sizes, "shots_against", line)
                if not team_rate or not opp_concede:
                    continue
                if team_rate["pct"] < min_pct or opp_concede["pct"] < min_pct:
                    continue
                rows.append(
                    {
                        "fixture_id": fixture_id,
                        "market": "team shots",
                        "side": side,
                        "start": start,
                        "team": teams.get(team_id, f"Team {team_id}"),
                        "opp": teams.get(opp_id, f"Team {opp_id}"),
                        "line": line,
                        "team_pct": team_rate["pct"],
                        "opp_concede_pct": opp_concede["pct"],
                        "team_avg": team_rate["avg"],
                        "opp_concede_avg": opp_concede["avg"],
                        "odds": odds,
                        "team_samples": team_rate["total"],
                        "opp_samples": opp_concede["total"],
                        "team_sample_size": team_rate["sample_size"],
                        "opp_sample_size": opp_concede["sample_size"],
                        "team_seq": team_rate["seq"],
                        "opp_seq": opp_concede["seq"],
                    }
                )
        # Match shots overs
        match_lines = match_odds.get(fixture_id, {})
        lines_to_check = list(match_lines.keys()) if match_lines else ([] if REQUIRE_ODDS else list(MATCH_SHOT_LINES))
        for line in lines_to_check:
            odds = match_lines.get(line)
            home_rate = select_best_rate(team_fixtures, home_id, sample_sizes, "match_shots", line)
            away_rate = select_best_rate(team_fixtures, away_id, sample_sizes, "match_shots", line)
            # match_shots should align for both, but require both to have coverage
            if not home_rate or not away_rate:
                continue
            min_pct_pair = min(home_rate["pct"], away_rate["pct"])
            if min_pct_pair < min_pct:
                continue
            rows.append(
                {
                    "fixture_id": fixture_id,
                    "market": "match shots",
                    "start": start,
                    "line": line,
                    "team": teams.get(home_id, f"Team {home_id}"),
                    "opp": teams.get(away_id, f"Team {away_id}"),
                    "team_pct": home_rate["pct"],
                    "opp_concede_pct": away_rate["pct"],  # symmetric for match total
                    "team_avg": home_rate["avg"],
                    "opp_concede_avg": away_rate["avg"],
                    "odds": odds,
                    "team_samples": home_rate["total"],
                    "opp_samples": away_rate["total"],
                    "team_sample_size": home_rate["sample_size"],
                    "opp_sample_size": away_rate["sample_size"],
                    "team_seq": home_rate["seq"],
                    "opp_seq": away_rate["seq"],
                }
            )
    rows.sort(
        key=lambda r: (
            min(r["team_pct"], r["opp_concede_pct"]),
            r.get("odds") or 0,
            -r["line"],
        ),
        reverse=True,
    )
    return rows


def fetch_team_names(cur: sqlite3.Cursor) -> Dict[int, str]:
    cur.execute("SELECT id, name FROM teams")
    return {row[0]: row[1] for row in cur.fetchall()}


def print_rows(rows: List[Dict[str, Any]], title: str, limit: int) -> None:
    print(f"\n{title}")
    if not rows:
        print("  (no candidates found)")
        return
    for row in rows[:limit]:
        pct_team = row["team_pct"]
        pct_opp = row["opp_concede_pct"]
        odds_str = f"@{row['odds']:.2f}" if row.get("odds") else "odds n/a"
        date_str = row.get("start") or "n/a"
        if row["market"] == "team shots":
            print(
                f"- {date_str} | {row['team']} vs {row['opp']} | team shots over {row['line']:.1f} "
                f"team {pct_team:.0%} (avg {row['team_avg']:.1f}, n={row['team_samples']}@{row['team_sample_size']}) "
                f"| opp concedes {pct_opp:.0%} (avg {row['opp_concede_avg']:.1f}, n={row['opp_samples']}@{row['opp_sample_size']}) "
                f"| {odds_str} | seq team {row['team_seq']} | opp {row['opp_seq']}"
            )
        else:
            print(
                f"- {date_str} | {row['team']} vs {row['opp']} | match shots over {row['line']:.1f} "
                f"{pct_team:.0%}/{pct_opp:.0%} (avg {row['team_avg']:.1f}/{row['opp_concede_avg']:.1f}) "
                f"| {odds_str} | seq {row['team_seq']} / {row['opp_seq']}"
            )


def main() -> None:
    conn = connect()
    cur = conn.cursor()
    teams = fetch_team_names(cur)
    fixture_team_map = fetch_fixture_team_map(cur)
    fixtures = load_fixture_info(cur, require_odds=REQUIRE_ODDS)
    team_fixtures = load_team_fixtures(cur, fixture_team_map)
    if not team_fixtures or not fixtures:
        raise SystemExit("No fixtures with required odds in window. Sync odds or relax window/REQUIRE_ODDS.")
    match_odds, team_odds = load_odds(cur, fixture_team_map)
    rows = build_fixture_rows(
        teams=teams,
        team_fixtures=team_fixtures,
        fixtures=fixtures,
        match_odds=match_odds,
        team_odds=team_odds,
        sample_sizes=SAMPLE_SIZES,
        min_pct=MIN_PCT,
        min_samples=MIN_SAMPLES,
    )
    print_rows(rows, title=f"Fixture candidates (min {int(MIN_PCT*100)}% team+opp hit)", limit=MAX_ROWS)


if __name__ == "__main__":
    main()
