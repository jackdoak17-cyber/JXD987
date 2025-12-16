#!/usr/bin/env python3
import json
import os
import sys
from typing import Any, Dict, List

import requests

FIXTURE_ID = 19427606
PRIMARY_INCLUDE = "lineups.details;lineups.position;lineups.player;participants;scores;state"
FALLBACK_INCLUDE = "lineups.details;participants;scores;state"
BASE_URL = "https://api.sportmonks.com/v3/football/fixtures/{}"

def fetch_fixture(token: str) -> Dict[str, Any]:
    url = BASE_URL.format(FIXTURE_ID)
    for include in [PRIMARY_INCLUDE, FALLBACK_INCLUDE]:
        params = {"api_token": token, "include": include}
        resp = requests.get(url, params=params, timeout=30)
        if resp.ok:
            return {"data": resp.json(), "include": include}
        # if include was primary and failed, try fallback
    resp.raise_for_status()
    return {}


def first_keys(obj: Dict[str, Any]) -> List[str]:
    return list(obj.keys())


def main():
    token = os.environ.get("SPORTMONKS_API_TOKEN")
    if not token:
        print("Missing SPORTMONKS_API_TOKEN", file=sys.stderr)
        sys.exit(1)

    result = fetch_fixture(token)
    payload = result.get("data", {})
    include_used = result.get("include")
    data = payload.get("data", {}) if isinstance(payload, dict) else {}

    lineups = data.get("lineups") or []
    lineup_count = len(lineups)

    saka = None
    for l in lineups:
        pid = l.get("player_id") or (l.get("player") or {}).get("id")
        if pid == 16827155:
            saka = l
            break

    first_lineup = lineups[0] if lineups else None
    first_details = first_lineup.get("details") if isinstance(first_lineup, dict) else []

    output = {
        "include_used": include_used,
        "top_level_keys": first_keys(data) if isinstance(data, dict) else [],
        "lineup_count": lineup_count,
        "saka_raw_keys": list(saka.keys()) if isinstance(saka, dict) else None,
        "saka_position_fields": {
            "detailed_position_id": saka.get("detailed_position_id") if saka else None,
            "detailed_position": saka.get("detailed_position") if saka else None,
            "position_id": saka.get("position_id") if saka else None,
            "position": saka.get("position") if saka else None,
            "formation_field": saka.get("formation_field") if saka else None,
            "formation_position": saka.get("formation_position") if saka else None,
        },
        "saka_lineup": saka,
        "first_lineup_keys": list(first_lineup.keys()) if isinstance(first_lineup, dict) else [],
        "first_details_sample": first_details[:10] if isinstance(first_details, list) else [],
    }

    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
