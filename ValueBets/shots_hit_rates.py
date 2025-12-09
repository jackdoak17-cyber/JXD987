"""
Report players who hit >=1 shot in high percentages of recent games.

Rules:
- Check sample sizes: 11, 14, 15, 20.
- Keep players meeting any of:
  * 9/11
  * 12/14
  * 13/15
  * 16/20
- Output: player name, team name, sample size, hits/games, per-game shot sequence.
- Odds are shown as "n/a" because player-prop odds are not present in the DB.

Relies on `player_forms.raw_fixtures` populated by `python -m jxd.cli compute-forms`.
"""

import json
import os
import sqlite3
from typing import Dict, List, Tuple

DB_PATH = os.environ.get("JXD_DB_PATH", "data/jxd.sqlite")

# sample_size -> required hits
THRESHOLDS: Dict[int, int] = {11: 9, 14: 12, 15: 13, 20: 16}


def connect() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)


def fetch_player_forms(cur: sqlite3.Cursor) -> List[Tuple[int, int, int, str]]:
    """
    Return (player_id, team_id, sample_size, raw_fixtures_json).
    """
    cur.execute(
        """
        SELECT player_id, team_id, sample_size, raw_fixtures
        FROM player_forms
        WHERE raw_fixtures IS NOT NULL AND sample_size >= 11
        """
    )
    return cur.fetchall()


def evaluate(forms: List[Tuple[int, int, int, str]]) -> List[Tuple]:
    results: List[Tuple] = []
    for player_id, team_id, sample_size, raw_json in forms:
        try:
            fixtures = json.loads(raw_json)
        except Exception:
            continue
        if not isinstance(fixtures, list) or not fixtures:
            continue
        # Use fixtures in listed order (assumed most recent first)
        # We evaluate for each target sample size independently.
        for sample, required in THRESHOLDS.items():
            if len(fixtures) < sample:
                continue
            subset = fixtures[:sample]
            shots = []
            for fx in subset:
                val = fx.get("shots") if isinstance(fx, dict) else None
                try:
                    shots.append(int(val) if val is not None else 0)
                except Exception:
                    shots.append(0)
            hits = sum(1 for v in shots if v >= 1)
            if hits >= required:
                seq = ",".join(str(v) for v in shots)
                results.append((player_id, team_id, sample, hits, seq))
    return results


def enrich(cur: sqlite3.Cursor, rows: List[Tuple]) -> List[str]:
    out: List[str] = []
    for player_id, team_id, sample, hits, seq in rows:
        cur.execute("SELECT display_name FROM players WHERE id = ?", (player_id,))
        player_name = cur.fetchone()
        cur.execute("SELECT name FROM teams WHERE id = ?", (team_id,))
        team_name = cur.fetchone()
        pname = player_name[0] if player_name and player_name[0] else f"Player {player_id}"
        tname = team_name[0] if team_name and team_name[0] else f"Team {team_id}"
        out.append(f"{pname} ({tname}) - {hits}/{sample} games with 1+ shot | seq: {seq} | odds: n/a")
    return out


def main() -> None:
    conn = connect()
    cur = conn.cursor()
    forms = fetch_player_forms(cur)
    rows = evaluate(forms)
    if not rows:
        raise SystemExit("No matches found. Ensure player_forms is populated via compute-forms.")
    # Sort by highest hit rate then sample size
    rows.sort(key=lambda r: (r[3] / r[2], r[2]), reverse=True)
    lines = enrich(cur, rows)
    for line in lines:
        print(line)


if __name__ == "__main__":
    main()
