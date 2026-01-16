#!/usr/bin/env python3
"""
Backfill player_team_history from fixture_players + fixtures in SQLite.
This preserves historical team membership per player based on match appearances.
"""

import argparse
import sqlite3
from datetime import datetime

DB_PATH_DEFAULT = "data/jxd.sqlite"


def ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        create table if not exists player_team_history (
          id integer primary key autoincrement,
          player_id integer not null,
          team_id integer not null,
          source text not null default 'lineup',
          effective_from datetime not null,
          effective_to datetime,
          created_at datetime not null,
          updated_at datetime not null
        )
        """
    )
    conn.commit()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=DB_PATH_DEFAULT)
    parser.add_argument("--dry-run", action="store_true", default=False)
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    ensure_table(conn)

    cur = conn.cursor()
    cur.execute(
        """
        select fp.player_id, fp.team_id, f.starting_at
        from fixture_players fp
        join fixtures f on f.id = fp.fixture_id
        where fp.player_id is not null
          and fp.team_id is not null
          and f.starting_at is not null
        order by fp.player_id, f.starting_at asc, f.id asc
        """
    )
    rows = cur.fetchall()

    now = datetime.utcnow().isoformat(sep=" ", timespec="seconds")
    history = []
    last_player = None
    last_team = None
    last_start = None

    for row in rows:
        player_id = row["player_id"]
        team_id = row["team_id"]
        starting_at = row["starting_at"]
        if last_player != player_id:
            if last_player is not None and last_team is not None and last_start is not None:
                history.append(
                    (
                        last_player,
                        last_team,
                        "lineup",
                        last_start,
                        None,
                        now,
                        now,
                    )
                )
            last_player = player_id
            last_team = team_id
            last_start = starting_at
            continue

        if team_id != last_team:
            history.append(
                (
                    player_id,
                    last_team,
                    "lineup",
                    last_start,
                    starting_at,
                    now,
                    now,
                )
            )
            last_team = team_id
            last_start = starting_at

    if last_player is not None and last_team is not None and last_start is not None:
        history.append(
            (
                last_player,
                last_team,
                "lineup",
                last_start,
                None,
                now,
                now,
            )
        )

    if args.dry_run:
        print(f"Would insert {len(history)} history rows.")
        return

    cur.execute("delete from player_team_history")
    cur.executemany(
        """
        insert into player_team_history (
          player_id, team_id, source, effective_from, effective_to, created_at, updated_at
        ) values (?, ?, ?, ?, ?, ?, ?)
        """,
        history,
    )
    conn.commit()
    print(f"Inserted {len(history)} history rows.")


if __name__ == "__main__":
    main()
