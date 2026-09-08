#!/usr/bin/env python3
"""Fail fast when Odds-API's live football market contract drifts."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable

from jxd.odds_api_client import OddsApiClient, OddsApiError
from scripts.sync_odds import resolve_market_key


REQUIRED_MARKETS: Dict[str, tuple[str, str]] = {
    "ML": ("moneyline", "moneyline"),
    "Double Chance": ("outcomes", "double_chance"),
    "Draw No Bet": ("moneyline", "draw_no_bet"),
    "Totals": ("line", "goals_over_under"),
    "Totals HT": ("line", "goals_over_under_first_half"),
    "Both Teams To Score": ("yesno", "btts"),
    "Total Shots": ("line", "match_shots"),
    "Total Shots on Target": ("line", "match_shots_on_target"),
    "Total Shots Home": ("line", "team_shots_home"),
    "Total Shots Away": ("line", "team_shots_away"),
    "Total Shots on Target Home": ("line", "team_shots_on_target_home"),
    "Total Shots on Target Away": ("line", "team_shots_on_target_away"),
    "Team Total Home": ("line", "team_total_goals_home"),
    "Team Total Away": ("line", "team_total_goals_away"),
    "Player Shots": ("labelled", "player_shots"),
    "Player Shots on Target": ("labelled", "player_shots_on_target"),
    "Player Fouls": ("labelled", "player_fouls_committed"),
    "Player To Be Fouled": ("labelled", "player_fouls_drawn"),
    "Anytime Goalscorer": ("labelled", "player_to_score"),
    "Player To Assist": ("labelled", "player_to_assist"),
    "Player To Score or Assist": ("labelled", "player_to_score_or_assist"),
    "Player to be Booked": ("labelled", "player_card"),
    "Player Tackles": ("labelled", "player_tackles"),
    "Goalkeeper Saves": ("labelled", "player_goalkeeper_saves"),
}

RETIRED_FOOTBALL_MARKETS = {
    "Half Time Result",
    "1st Half Handicap",
    "Corners 2-Way",
    "Team Corners Home",
    "Team Corners Away",
    "Team Cards Home",
    "Team Cards Away",
    "Match Shots",
    "Team Shots Home",
    "Team Shots Away",
    "Match Shots on Target",
    "Team Shots on Target Home",
    "Team Shots on Target Away",
    "Match Offsides",
    "Team Offsides Home",
    "Team Offsides Away",
    "Match Tackles",
    "Team Tackles Home",
    "Team Tackles Away",
    "Alternative Asian Handicap",
    "Alternative Goal Line",
    "Alternative Total Goals",
    "Alternative 1st Half Asian Handicap",
    "Alternative 1st Half Goal Line",
}


def validate_catalog(items: Iterable[object]) -> dict[str, object]:
    catalog = {
        str(item.get("name")): item
        for item in items
        if isinstance(item, dict) and item.get("name")
    }
    missing = sorted(set(REQUIRED_MARKETS).difference(catalog))
    shape_mismatches = []
    parser_mismatches = []
    for name, (expected_shape, expected_key) in REQUIRED_MARKETS.items():
        item = catalog.get(name)
        if item is None:
            continue
        actual_shape = str(item.get("shape") or "")
        if actual_shape != expected_shape:
            shape_mismatches.append(
                {"market": name, "expected": expected_shape, "actual": actual_shape}
            )
        actual_key = resolve_market_key(name)
        if actual_key != expected_key:
            parser_mismatches.append(
                {"market": name, "expected": expected_key, "actual": actual_key}
            )
    retired_present = sorted(RETIRED_FOOTBALL_MARKETS.intersection(catalog))
    return {
        "ok": not missing and not shape_mismatches and not parser_mismatches,
        "market_count": len(catalog),
        "required_market_count": len(REQUIRED_MARKETS),
        "missing_required_markets": missing,
        "shape_mismatches": shape_mismatches,
        "parser_mismatches": parser_mismatches,
        "retired_markets_still_present": retired_present,
        "generic_player_props_available": "Player Props" in catalog,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sport", default="football")
    parser.add_argument("--report-out", default="")
    args = parser.parse_args()

    try:
        payload = OddsApiClient().request("markets", {"sport": args.sport})
    except OddsApiError as exc:
        raise SystemExit(f"Odds API market catalogue unavailable: {exc}") from exc
    if isinstance(payload, dict):
        items = payload.get("markets") or payload.get(args.sport) or []
    else:
        items = payload
    if not isinstance(items, list):
        raise SystemExit("Odds API market catalogue returned an invalid payload")

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "endpoint": "markets",
        "sport": args.sport,
        **validate_catalog(items),
    }
    if args.report_out:
        Path(args.report_out).write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, sort_keys=True))
    if not report["ok"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
