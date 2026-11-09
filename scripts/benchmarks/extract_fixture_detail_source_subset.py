#!/usr/bin/env python3
"""Copy a bounded fixture-detail source slice from SQLite without source writes.

The output is a standalone SQLite database for isolated exporter comparisons.
Use --output - to stream bytes over SSH without writing a file on the source
host. This is a diagnostic tool, not part of the VPS runtime manifest.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from pathlib import Path


TABLES = (
    "seasons", "rounds", "teams", "fixtures", "players",
    "sidelined_players", "player_team_history", "fixture_players",
    "fixture_statistics", "fixture_player_statistics", "odds_snapshots",
    "odds_outcomes",
)


def copy_source(source_path: Path, fixture_ids: list[int]) -> bytes:
    if not 1 <= len(fixture_ids) <= 20 or len(set(fixture_ids)) != len(fixture_ids):
        raise ValueError("provide 1..20 distinct fixture IDs")
    if any(value <= 0 for value in fixture_ids):
        raise ValueError("fixture IDs must be positive")

    source = sqlite3.connect(source_path.resolve().as_uri() + "?mode=ro", uri=True, timeout=1)
    source.execute("pragma query_only=on")
    source.execute("begin")
    target = sqlite3.connect(":memory:")
    target.execute("pragma foreign_keys=off")

    def query_for(table: str, column: str, values: list[int]):
        if not values:
            return f"select * from {table} where 0", ()
        marks = ",".join("?" for _ in values)
        return f"select * from {table} where {column} in ({marks})", tuple(values)

    def keys(table: str, column: str, where_column: str, values: list[int]) -> list[int]:
        marks = ",".join("?" for _ in values)
        return [row[0] for row in source.execute(
            f"select distinct {column} from {table} where {where_column} in ({marks})",
            values,
        ) if row[0] is not None]

    fixtures = list(source.execute(
        "select id,league_id,season_id,home_team_id,away_team_id "
        "from fixtures where id in (" + ",".join("?" for _ in fixture_ids) + ")",
        fixture_ids,
    ))
    if len(fixtures) != len(fixture_ids):
        raise ValueError("a requested fixture is missing from source")
    leagues = sorted({row[1] for row in fixtures})
    seasons = keys("seasons", "id", "league_id", leagues)
    teams = sorted({row[3] for row in fixtures} | {row[4] for row in fixtures})
    players = sorted(set(keys("fixture_players", "player_id", "fixture_id", fixture_ids)) |
                     set(keys("fixture_player_statistics", "player_id", "fixture_id", fixture_ids)))
    queries = {
        "seasons": query_for("seasons", "league_id", leagues),
        "rounds": query_for("rounds", "season_id", seasons),
        "teams": query_for("teams", "id", teams),
        "fixtures": query_for("fixtures", "id", fixture_ids),
        "players": query_for("players", "id", players),
        "sidelined_players": query_for("sidelined_players", "team_id", teams),
        "player_team_history": query_for("player_team_history", "player_id", players),
        "fixture_players": query_for("fixture_players", "fixture_id", fixture_ids),
        "fixture_statistics": query_for("fixture_statistics", "fixture_id", fixture_ids),
        "fixture_player_statistics": query_for("fixture_player_statistics", "fixture_id", fixture_ids),
        "odds_snapshots": ("select * from odds_snapshots where 0", ()),
        "odds_outcomes": ("select * from odds_outcomes where 0", ()),
    }
    for table in TABLES:
        query, params = queries[table]
        schema = source.execute(
            "select sql from sqlite_master where type='table' and name=?", (table,)
        ).fetchone()
        if not schema or not schema[0]:
            raise ValueError(f"missing source table: {table}")
        target.execute(schema[0])
        cursor = source.execute(query, params)
        count = 0
        while rows := cursor.fetchmany(2000):
            count += len(rows)
            if count > 200000:
                raise ValueError(f"source row cap exceeded: {table}")
            target.executemany(
                f"insert into {table} values(" + ",".join("?" for _ in cursor.description) + ")",
                rows,
            )
        for (index_sql,) in source.execute(
            "select sql from sqlite_master where type='index' and tbl_name=? "
            "and sql is not null", (table,)
        ):
            target.execute(index_sql)
    target.commit()
    source.rollback()
    return target.serialize()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-db", required=True, type=Path)
    parser.add_argument("--fixture-ids", required=True)
    parser.add_argument("--output", default="-")
    args = parser.parse_args()
    ids = [int(value) for value in args.fixture_ids.split(",")]
    payload = copy_source(args.source_db, ids)
    if args.output == "-":
        sys.stdout.buffer.write(payload)
    else:
        output = Path(args.output)
        descriptor = os.open(output, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)


if __name__ == "__main__":
    main()
