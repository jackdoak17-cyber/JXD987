import os
import requests

TOKEN = os.environ.get("SPORTMONKS_API_TOKEN")
if not TOKEN:
    raise SystemExit("SPORTMONKS_API_TOKEN required")

BASE = os.environ.get("SPORTMONKS_BASE_URL", "https://api.sportmonks.com/v3")
HEADERS = {"Accept": "application/json"}
COMMON = {"api_token": TOKEN}

endpoints = [
    ("fixtures", "football/fixtures", {"per_page": 1}),
    ("livescores", "football/livescores", {"per_page": 1}),
    ("injuries", "football/injuries", {"per_page": 1}),
    ("news", "football/news", {"per_page": 1}),
    ("expected_teams", "football/expected/teams", {"per_page": 1}),
    ("expected_players", "football/expected/players", {"per_page": 1}),
    ("predictions_probabilities", "football/predictions/probabilities", {"per_page": 1}),
    ("predictions_valuebets", "football/predictions/valuebets", {"per_page": 1}),
    ("inplay_odds_sample", "football/odds/inplay/fixtures/464", {}),
    ("inplay_odds_latest", "football/odds/inplay/latest", {"per_page": 1}),
    ("match_facts", "football/match-facts", {"per_page": 1}),
    ("premium_odds", "football/odds/premium/pre-match", {"per_page": 1}),
]

results = []
for label, path, params in endpoints:
    url = f"{BASE}/{path}"
    qp = {**COMMON, **params}
    try:
        r = requests.get(url, headers=HEADERS, params=qp, timeout=20)
        status = r.status_code
        try:
            payload = r.json()
        except Exception:
            payload = None
        if status == 200:
            data = payload.get("data") if isinstance(payload, dict) else None
            note = f"ok len={len(data) if data else 0} keys={list(data[0].keys())[:5] if data else []}"
        else:
            note = payload if payload else r.text
    except Exception as exc:
        status = None
        note = str(exc)
    results.append((label, status, note))

for label, status, note in results:
    print(f"{label}: {status} -> {note}")
