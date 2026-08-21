#!/usr/bin/env python3
"""
Sync odds for odds-enabled leagues (plus leagues with fixtures in the odds window).

- Reads config/odds_api_leagues.json and preserves config/league_ids.txt order
- Uses Odds-API.io events + odds endpoints
- Stores allowlisted markets in odds_outcomes (SQLite)
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path


def load_league_ids(path: Path) -> list[int]:
    ids: list[int] = []
    if not path.exists():
        return ids
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        try:
            ids.append(int(line))
        except ValueError as exc:
            raise SystemExit(f"Invalid league id in {path}: {line}") from exc
    return ids


def load_excluded_league_ids(repo_root: Path) -> set[int]:
    path = repo_root / "config" / "odds_api_sync_excluded_leagues.json"
    if not path.exists():
        return set()
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise SystemExit(f"Expected a JSON array in {path}")
    return {int(value) for value in raw}


def load_odds_league_ids(repo_root: Path, include_excluded: bool = False) -> list[int]:
    config_ids = load_league_ids(repo_root / "config" / "league_ids.txt")
    odds_map_path = repo_root / "config" / "odds_api_leagues.json"
    raw = json.loads(odds_map_path.read_text(encoding="utf-8"))
    excluded = set() if include_excluded else load_excluded_league_ids(repo_root)
    odds_ids = {int(value) for value in raw if int(value) not in excluded}
    ordered_ids = [league_id for league_id in config_ids if league_id in odds_ids]
    extra_ids = sorted(odds_ids.difference(ordered_ids))
    return [*ordered_ids, *extra_ids]


def window_bounds(days_forward: int) -> tuple[str, str]:
    start_dt = datetime.utcnow()
    end_dt = start_dt + timedelta(days=days_forward)
    fmt = "%Y-%m-%d %H:%M:%S"
    return start_dt.strftime(fmt), end_dt.strftime(fmt)


def load_fixture_league_ids(db_path: Path, start_dt: str, end_dt: str) -> list[int]:
    if not db_path.exists():
        return []
    try:
        conn = sqlite3.connect(str(db_path))
        rows = conn.execute(
            """
            select distinct league_id
            from fixtures
            where league_id is not null
              and datetime(starting_at) >= ?
              and datetime(starting_at) < ?
            """,
            (start_dt, end_dt),
        ).fetchall()
    except sqlite3.Error as exc:
        print(f"Warning: could not read fixture leagues from {db_path}: {exc}", file=sys.stderr)
        return []
    finally:
        try:
            conn.close()
        except Exception:
            pass
    return [int(row[0]) for row in rows if row and row[0] is not None]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days-forward", type=int, default=14)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument(
        "--include-excluded",
        action="store_true",
        help="Explicitly include leagues listed in odds_api_sync_excluded_leagues.json.",
    )
    parser.add_argument(
        "--bookmakers",
        default=os.environ.get("ODDS_BOOKMAKERS", "Bet365,Paddy Power"),
        help="Comma-separated bookmaker names",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent.parent
    db_path = Path(os.environ.get("JXD_DB_PATH", str(repo_root / "data" / "jxd.sqlite")))
    start_dt, end_dt = window_bounds(args.days_forward)

    excluded_ids = load_excluded_league_ids(repo_root) if not args.include_excluded else set()
    config_ids = load_odds_league_ids(repo_root, include_excluded=args.include_excluded)
    fixture_ids = load_fixture_league_ids(db_path, start_dt, end_dt)
    league_ids = sorted({*config_ids, *(league_id for league_id in fixture_ids if league_id not in excluded_ids)})
    if not league_ids:
        raise SystemExit(f"No odds-enabled league ids found (config={repo_root / 'config' / 'odds_api_leagues.json'}, db={db_path})")

    league_ids_csv = ",".join(str(x) for x in league_ids)

    cmd = [
        sys.executable,
        "scripts/sync_odds.py",
        "--leagues",
        league_ids_csv,
        "--days-forward",
        str(args.days_forward),
        "--bookmakers",
        args.bookmakers,
    ]
    if args.limit and args.limit > 0:
        cmd += ["--limit", str(args.limit)]
    if args.include_excluded:
        cmd.append("--include-excluded")

    raise SystemExit(subprocess.call(cmd))


if __name__ == "__main__":
    main()
