"""
Quick ranker for shots on target outside the box (last N games).

This relies on:
- A populated `types` table (via `python -m jxd.cli sync-types`) so we can
  locate the SportMonks type IDs for outside-box/on-target shots.
- Player stat rows in `player_stats.stats` that include those type IDs.

If the required type IDs are missing, the script will exit with a clear message.
Odds are omitted unless a player-prop market is present in `odds_latest`.
"""

import os
import sqlite3
from typing import List, Optional, Sequence, Tuple

DB_PATH = os.environ.get("JXD_DB_PATH", "data/jxd.sqlite")


def connect() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)


def find_type_ids(cur: sqlite3.Cursor) -> List[int]:
    """
    Try to find SportMonks type IDs whose name mentions outside box + target + shot.
    """
    cur.execute(
        """
        SELECT id
        FROM types
        WHERE lower(name) LIKE '%outside%'
          AND lower(name) LIKE '%shot%'
          AND (lower(name) LIKE '%target%' OR lower(name) LIKE '%on target%')
        """
    )
    rows = cur.fetchall()
    return [r[0] for r in rows]


def compute(cur: sqlite3.Cursor, type_ids: Sequence[int], sample: int, limit: int) -> List[Tuple]:
    """
    Aggregate outside-box SOT per player over their most recent fixtures.
    """
    placeholders = ",".join("?" for _ in type_ids)
    cur.execute(
        f"""
        WITH events AS (
            SELECT
                ps.player_id,
                ps.team_id,
                f.id AS fixture_id,
                f.starting_at AS started_at,
                CAST(json_extract(e.value,'$.data.value') AS REAL) AS val
            FROM player_stats ps
            JOIN fixtures f ON f.id = ps.fixture_id
            JOIN json_each(ps.stats) e
              ON json_extract(e.value,'$.type_id') IN ({placeholders})
        ),
        ranked AS (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY started_at DESC, fixture_id DESC) AS rn
            FROM events
        ),
        trimmed AS (
            SELECT * FROM ranked WHERE rn <= ?
        ),
        agg AS (
            SELECT
                player_id,
                team_id,
                COUNT(*) AS samples,
                SUM(CASE WHEN val >= 1 THEN 1 ELSE 0 END) AS hit_games,
                GROUP_CONCAT(CAST(val AS INT), '-') AS seq
            FROM trimmed
            GROUP BY player_id, team_id
        )
        SELECT
            p.display_name,
            t.name,
            a.hit_games,
            a.samples,
            a.seq
        FROM agg a
        JOIN players p ON p.id = a.player_id
        JOIN teams t ON t.id = a.team_id
        WHERE a.samples >= 1
        ORDER BY a.hit_games DESC, a.samples DESC
        LIMIT ?
        """,
        [*type_ids, sample, limit],
    )
    return cur.fetchall()


def main(sample: int = 10, limit: int = 50) -> None:
    conn = connect()
    cur = conn.cursor()
    # Ensure required tables exist
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name in ('types','player_stats','fixtures')"
    )
    present = {r[0] for r in cur.fetchall()}
    if "types" not in present:
        raise SystemExit("types table missing; run `python -m jxd.cli sync-types` first.")
    type_ids = find_type_ids(cur)
    if not type_ids:
        raise SystemExit("No type IDs found for outside-box on-target shots. Sync types and stats first.")
    rows = compute(cur, type_ids=type_ids, sample=sample, limit=limit)
    if not rows:
        raise SystemExit("No rows found. Ensure player stats with the required type IDs are present.")
    for name, team, hits, samples, seq in rows:
        seq_str = seq or ""
        print(f"{name} ({team}) - outside-box SOT {hits}/{samples} | values: {seq_str}")


if __name__ == "__main__":
    main()
