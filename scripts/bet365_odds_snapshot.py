#!/usr/bin/env python3
"""
Quick Bet365 odds fetcher.

Usage:
  python scripts/bet365_odds_snapshot.py 12345 67890
  export SPORTMONKS_API_TOKEN=...
  export BOOKMAKER_ID=2           # optional; defaults to Bet365

Outputs a JSON blob per fixture to stdout.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import List

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from jxd.api import SportMonksClient
from jxd.config import settings


def main(fixture_ids: List[int]) -> int:
    if not fixture_ids:
        print("Usage: python scripts/bet365_odds_snapshot.py <fixture_id> [<fixture_id> ...]", file=sys.stderr)
        return 1
    token = settings.sportmonks_api_token
    if not token:
        print("SPORTMONKS_API_TOKEN is required.", file=sys.stderr)
        return 1
    bookmaker_id = int(os.getenv("BOOKMAKER_ID", settings.bookmaker_id))
    client = SportMonksClient(
        api_token=token,
        requests_per_hour=settings.requests_per_hour,
        base_url=settings.sportmonks_base_url,
        use_filters_populate=settings.use_filters_populate,
    )
    for fid in fixture_ids:
        payload = client.get_raw(
            "football/odds/pre-match/fixtures/{}".format(fid),
            params={"filter": f"bookmakers:{bookmaker_id}"},
        )
        rows = payload.get("data") if isinstance(payload, dict) else payload
        out = {
            "fixture_id": fid,
            "bookmaker_id": bookmaker_id,
            "odds_rows": rows,
        }
        print(json.dumps(out, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    ids = []
    for arg in sys.argv[1:]:
        try:
            ids.append(int(arg))
        except ValueError:
            continue
    sys.exit(main(ids))
