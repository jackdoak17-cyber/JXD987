#!/usr/bin/env python3
"""
Sync Bet365 odds for all configured leagues.

- Reads config/league_ids.txt
- Refreshes upcoming fixtures (next N days)
- Fetches odds into SQLite (odds_snapshots/odds_outcomes)
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def load_league_ids(path: Path) -> str:
    ids = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        ids.append(line)
    if not ids:
        raise SystemExit(f"No league ids found in {path}")
    return ",".join(ids)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days-forward", type=int, default=14)
    parser.add_argument("--bookmaker-id", type=int, default=2)
    parser.add_argument("--sleep", type=float, default=0.05)
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    leagues_file = Path("config/league_ids.txt")
    league_ids = load_league_ids(leagues_file)

    cmd = [
        sys.executable,
        "scripts/sync_odds.py",
        "--leagues",
        league_ids,
        "--days-forward",
        str(args.days_forward),
        "--bookmaker-id",
        str(args.bookmaker_id),
        "--sleep",
        str(args.sleep),
        "--refresh-upcoming",
    ]
    if args.limit and args.limit > 0:
        cmd += ["--limit", str(args.limit)]

    raise SystemExit(subprocess.call(cmd))


if __name__ == "__main__":
    main()
