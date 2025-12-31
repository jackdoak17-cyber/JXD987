#!/usr/bin/env python3
"""
Validate canonical odds market coverage and mapping correctness in Supabase.

Reads SUPABASE_DB_URL (or SUPABASE_DB_URL_SESSION) and samples fixtures
per league in the next N days, then checks that canonical markets are present
and mapped consistently.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor


CANONICAL_MARKETS: Dict[str, Dict[str, str]] = {
    "moneyline": {"type": "team"},
    "double_chance": {"type": "team"},
    "draw_no_bet": {"type": "team"},
    "handicap": {"type": "team"},
    "card_handicap": {"type": "team"},
    "goals_over_under": {"type": "match"},
    "btts": {"type": "match"},
    "corners_over_under": {"type": "match"},
    "total_offsides": {"type": "match"},
    "match_shots": {"type": "match"},
    "match_shots_on_target": {"type": "match"},
    "match_cards": {"type": "match"},
    "team_shots": {"type": "team"},
    "team_shots_on_target": {"type": "team"},
    "team_cards": {"type": "team"},
    "team_corners": {"type": "team"},
    "team_most_cards": {"type": "team"},
    "team_most_corners": {"type": "team"},
    "team_most_shots": {"type": "team"},
    "team_most_shots_on_target": {"type": "team"},
    "to_score_a_penalty": {"type": "team"},
    "1st_goal_scorer": {"type": "player"},
    "player_to_score": {"type": "player"},
    "player_to_assist": {"type": "player"},
    "player_to_score_or_assist": {"type": "player"},
    "player_card": {"type": "player"},
    "player_shots": {"type": "player"},
    "player_shots_on_target": {"type": "player"},
    "player_goalkeeper_saves": {"type": "player"},
}


TEAM_DRAW_SELECTIONS = {"draw", "tie"}
DOUBLE_CHANCE_HOME = {"home_or_draw"}
DOUBLE_CHANCE_AWAY = {"draw_or_away"}
DOUBLE_CHANCE_NEUTRAL = {"home_or_away"}


def get_db_url() -> str:
    db_url = os.environ.get("SUPABASE_DB_URL") or os.environ.get("SUPABASE_DB_URL_SESSION")
    if db_url:
        return db_url
    fallback = Path("/tmp/supabase_db_url")
    if fallback.exists():
        return fallback.read_text(encoding="utf-8").strip()
    raise SystemExit("Missing SUPABASE_DB_URL (or /tmp/supabase_db_url)")


def fetch_fixtures(
    conn,
    league_id: int,
    days_forward: int,
    sample_size: int,
) -> List[Dict]:
    query = """
    select id, home_team_id, away_team_id
    from public.fixtures
    where league_id = %s
      and starting_at >= (now() at time zone 'utc')
      and starting_at <  (now() at time zone 'utc') + interval %s
    order by starting_at
    limit %s;
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, (league_id, f"{days_forward} days", sample_size))
        return cur.fetchall()


def fetch_outcomes(conn, fixture_ids: List[int]) -> List[Dict]:
    if not fixture_ids:
        return []
    market_keys = list(CANONICAL_MARKETS.keys())
    query = """
    select fixture_id, market_key, selection_key, participant_type, participant_id, line
    from public.odds_outcomes
    where fixture_id = any(%s)
      and market_key = any(%s);
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, (fixture_ids, market_keys))
        return cur.fetchall()


def outcome_expected_team(
    market_key: str,
    selection_key: Optional[str],
    participant_type: Optional[str],
    participant_id: Optional[int],
    home_team_id: Optional[int],
    away_team_id: Optional[int],
) -> Tuple[bool, str]:
    selection_key = selection_key or ""
    if selection_key in TEAM_DRAW_SELECTIONS:
        return participant_type is None, "draw_selection"
    if market_key == "double_chance":
        if selection_key in DOUBLE_CHANCE_HOME:
            return participant_type == "team" and participant_id == home_team_id, "double_chance_home"
        if selection_key in DOUBLE_CHANCE_AWAY:
            return participant_type == "team" and participant_id == away_team_id, "double_chance_away"
        if selection_key in DOUBLE_CHANCE_NEUTRAL:
            return participant_type is None, "double_chance_neutral"

    if selection_key == "home":
        return participant_type == "team" and participant_id == home_team_id, "home_selection"
    if selection_key == "away":
        return participant_type == "team" and participant_id == away_team_id, "away_selection"

    # For team totals and other team markets without explicit side keys,
    # participant_id should still be one of the fixture teams.
    if participant_type == "team" and participant_id in {home_team_id, away_team_id}:
        return True, "team_generic"
    return False, "team_generic"


def outcome_expected_match(
    participant_type: Optional[str],
    participant_id: Optional[int],
) -> Tuple[bool, str]:
    return participant_type is None and participant_id in {None, 0}, "match"


def outcome_expected_player(
    participant_type: Optional[str],
    participant_id: Optional[int],
) -> Tuple[bool, str]:
    return participant_type == "player" and participant_id is not None, "player"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--leagues", default="8,9,72,82,301,384,387,444,501,564,567,600")
    parser.add_argument("--days-forward", type=int, default=14)
    parser.add_argument("--sample-size", type=int, default=10)
    parser.add_argument("--out-json", default="core_markets_report.json")
    parser.add_argument("--out-md", default="core_markets_report.md")
    args = parser.parse_args()

    db_url = get_db_url()
    league_ids = [int(x) for x in args.leagues.split(",") if x.strip()]

    conn = psycopg2.connect(db_url)
    report = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "days_forward": args.days_forward,
        "sample_size": args.sample_size,
        "leagues": [],
    }

    for league_id in league_ids:
        fixtures = fetch_fixtures(conn, league_id, args.days_forward, args.sample_size)
        fixture_ids = [f["id"] for f in fixtures]
        fixture_map = {f["id"]: f for f in fixtures}
        outcomes = fetch_outcomes(conn, fixture_ids)

        availability = {key: 0 for key in CANONICAL_MARKETS}
        fixture_market_presence = {fid: set() for fid in fixture_ids}

        correctness = {key: {"total": 0, "correct": 0} for key in CANONICAL_MARKETS}
        errors: Dict[str, List[Dict]] = {key: [] for key in CANONICAL_MARKETS}

        for row in outcomes:
            market_key = row.get("market_key")
            fixture_id = row.get("fixture_id")
            if market_key not in CANONICAL_MARKETS or fixture_id not in fixture_map:
                continue
            fixture_market_presence[fixture_id].add(market_key)
            fixture = fixture_map[fixture_id]
            home_team_id = fixture.get("home_team_id")
            away_team_id = fixture.get("away_team_id")

            participant_type = row.get("participant_type")
            participant_id = row.get("participant_id")
            selection_key = row.get("selection_key")

            correctness[market_key]["total"] += 1
            market_type = CANONICAL_MARKETS[market_key]["type"]

            if market_type == "team":
                ok, reason = outcome_expected_team(
                    market_key,
                    selection_key,
                    participant_type,
                    participant_id,
                    home_team_id,
                    away_team_id,
                )
            elif market_type == "player":
                ok, reason = outcome_expected_player(participant_type, participant_id)
            else:
                ok, reason = outcome_expected_match(participant_type, participant_id)

            if ok:
                correctness[market_key]["correct"] += 1
            else:
                if len(errors[market_key]) < 10:
                    errors[market_key].append(
                        {
                            "fixture_id": fixture_id,
                            "selection_key": selection_key,
                            "participant_type": participant_type,
                            "participant_id": participant_id,
                            "reason": reason,
                        }
                    )

        for fixture_id in fixture_ids:
            for market_key in fixture_market_presence[fixture_id]:
                availability[market_key] += 1

        league_entry = {
            "league_id": league_id,
            "fixtures_sampled": len(fixture_ids),
            "availability": {},
            "mapping": {},
            "errors": {k: v for k, v in errors.items() if v},
        }
        for market_key in CANONICAL_MARKETS:
            avail = availability[market_key]
            total_fixtures = len(fixture_ids) or 1
            avail_pct = (avail / total_fixtures) * 100.0
            total_outcomes = correctness[market_key]["total"]
            correct_outcomes = correctness[market_key]["correct"]
            correct_pct = (correct_outcomes / total_outcomes) * 100.0 if total_outcomes else 0.0
            league_entry["availability"][market_key] = {
                "fixtures_with_market": avail,
                "availability_pct": round(avail_pct, 2),
            }
            league_entry["mapping"][market_key] = {
                "total_outcomes": total_outcomes,
                "correct_outcomes": correct_outcomes,
                "correct_pct": round(correct_pct, 2),
            }

        report["leagues"].append(league_entry)

    conn.close()

    out_json = Path(args.out_json)
    out_json.write_text(json.dumps(report, indent=2), encoding="utf-8")

    lines = []
    lines.append("# Core Markets Validation Report")
    lines.append("")
    for league_entry in report["leagues"]:
        lines.append(f"## League {league_entry['league_id']}")
        lines.append("")
        lines.append("| Market | Availability % | Mapping % |")
        lines.append("|---|---:|---:|")
        for market_key in CANONICAL_MARKETS:
            availability_pct = league_entry["availability"][market_key]["availability_pct"]
            mapping_pct = league_entry["mapping"][market_key]["correct_pct"]
            lines.append(f"| {market_key} | {availability_pct:.2f} | {mapping_pct:.2f} |")
        lines.append("")
        if league_entry["errors"]:
            lines.append("Top errors (sample):")
            for market_key, entries in league_entry["errors"].items():
                lines.append(f"- {market_key}: {entries[:3]}")
        lines.append("")

    out_md = Path(args.out_md)
    out_md.write_text("\\n".join(lines), encoding="utf-8")
    print(f"Wrote {out_json} and {out_md}")


if __name__ == "__main__":
    main()
