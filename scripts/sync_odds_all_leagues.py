#!/usr/bin/env python3
"""
Sync Bet365 odds for all configured leagues (plus leagues with fixtures in the odds window).

- Reads config/league_ids.txt
- Refreshes upcoming fixtures (next N days)
- Fetches odds into SQLite (odds_snapshots/odds_outcomes)
"""

from __future__ import annotations

import argparse
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
    parser.add_argument("--bookmaker-id", type=int, default=2)
    parser.add_argument("--sleep", type=float, default=0.05)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument(
        "--refresh-squads",
        action="store_true",
        help="Refresh team squads for upcoming fixtures to improve player mapping",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent.parent
    leagues_file = repo_root / "config" / "league_ids.txt"
    db_path = Path(os.environ.get("JXD_DB_PATH", str(repo_root / "data" / "jxd.sqlite")))
    start_dt, end_dt = window_bounds(args.days_forward)

    config_ids = load_league_ids(leagues_file)
    fixture_ids = load_fixture_league_ids(db_path, start_dt, end_dt)
    league_ids = sorted({*config_ids, *fixture_ids})
    if not league_ids:
        raise SystemExit(f"No league ids found (config={leagues_file}, db={db_path})")

    league_ids_csv = ",".join(str(x) for x in league_ids)

    cmd = [
        sys.executable,
        "scripts/sync_odds.py",
        "--leagues",
        league_ids_csv,
        "--days-forward",
        str(args.days_forward),
        "--bookmaker-id",
        str(args.bookmaker_id),
        "--sleep",
        str(args.sleep),
        "--refresh-upcoming",
    ]
    if args.refresh_squads:
        cmd.append("--refresh-squads")
    if args.limit and args.limit > 0:
        cmd += ["--limit", str(args.limit)]

    raise SystemExit(subprocess.call(cmd))


if __name__ == "__main__":
    main()
