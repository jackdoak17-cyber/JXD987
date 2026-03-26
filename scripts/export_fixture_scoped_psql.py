#!/usr/bin/env python3
"""
Bulk export fixture-scoped tables from SQLite to Supabase using PostgreSQL COPY.

This is intended for large fixture/player-stat restores where the REST exporter is too slow.
It restores the same fixture window used by export_to_supabase.py but writes the heavy
fixture-scoped tables through a temp-stage + COPY path.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sqlite3
import tempfile
from pathlib import Path
from typing import Iterable, Sequence

import psycopg2
from psycopg2.extras import execute_values

from export_to_supabase import (
    SUPABASE_DB_URL,
    choose_keep_seasons,
    ensure_fixture_columns,
    ensure_tables_exist,
    fetch_fixture_player_statistics,
    fetch_fixture_players,
    fetch_fixture_statistics,
    fetch_fixtures,
    fetch_odds_outcomes,
    fetch_rounds,
    fetch_seasons,
    fetch_teams,
    filter_rows_for_remote_schema,
    get_conn,
    require_env,
)

REQUIRED_TABLES = [
    "seasons",
    "rounds",
    "teams",
    "fixtures",
    "fixture_players",
    "fixture_statistics",
    "fixture_player_statistics",
    "odds_outcomes",
]


def csv_value(value):
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, separators=(",", ":"), ensure_ascii=False)
    return value


def write_csv(rows: Sequence[dict], columns: Sequence[str], path: Path) -> int:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        for row in rows:
            writer.writerow([csv_value(row.get(column)) for column in columns])
    return len(rows)


def load_fixture_ids(cur, fixture_ids: Sequence[int]) -> None:
    cur.execute("drop table if exists fixture_ids_stage")
    cur.execute("create temporary table fixture_ids_stage (fixture_id bigint primary key) on commit drop")
    execute_values(
        cur,
        "insert into fixture_ids_stage (fixture_id) values %s",
        [(fixture_id,) for fixture_id in fixture_ids],
        page_size=1000,
    )


def copy_table(conn, table: str, rows: Sequence[dict], columns: Sequence[str], fixture_ids: Sequence[int]) -> int:
    if not rows:
        with conn.cursor() as cur:
            load_fixture_ids(cur, fixture_ids)
            cur.execute(f"delete from public.{table} t using fixture_ids_stage f where t.fixture_id = f.fixture_id")
        conn.commit()
        return 0

    with tempfile.NamedTemporaryFile(prefix=f"{table}_", suffix=".csv", delete=False) as tmp:
        tmp_path = Path(tmp.name)
    try:
        write_csv(rows, columns, tmp_path)
        with conn.cursor() as cur:
            load_fixture_ids(cur, fixture_ids)
            stage_name = f"{table}_stage"
            cur.execute(f"drop table if exists {stage_name}")
            cur.execute(f"create temporary table {stage_name} (like public.{table} including defaults) on commit drop")
            with tmp_path.open("r", encoding="utf-8") as handle:
                cur.copy_expert(
                    f"copy {stage_name} ({','.join(columns)}) from stdin with (format csv)",
                    handle,
                )
            cur.execute(f"delete from public.{table} t using fixture_ids_stage f where t.fixture_id = f.fixture_id")
            cur.execute(
                f"insert into public.{table} ({','.join(columns)}) "
                f"select {','.join(columns)} from {stage_name}"
            )
        conn.commit()
        return len(rows)
    finally:
        tmp_path.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--leagues", default=os.environ.get("LEAGUE_IDS", ""))
    parser.add_argument("--days-back", type=int, default=None)
    parser.add_argument(
        "--upcoming-days",
        type=int,
        default=int(os.environ.get("EXPORT_DAYS_FORWARD", "45") or "45"),
    )
    args = parser.parse_args()

    require_env(False)
    if not SUPABASE_DB_URL:
        raise SystemExit("Missing SUPABASE_DB_URL")

    sqlite_conn = get_conn()
    ensure_tables_exist(sqlite_conn, REQUIRED_TABLES)
    ensure_fixture_columns(sqlite_conn)

    league_ids = [int(value) for value in args.leagues.split(",") if value.strip()] if args.leagues else []
    keep_ids = choose_keep_seasons(sqlite_conn, league_ids if league_ids else None)
    if not keep_ids:
        raise SystemExit("No seasons to export")

    fixtures = fetch_fixtures(
        sqlite_conn,
        list(keep_ids),
        upcoming_days=args.upcoming_days,
        days_back=args.days_back,
    )
    team_ids = {fixture["home_team_id"] for fixture in fixtures} | {fixture["away_team_id"] for fixture in fixtures}
    teams = fetch_teams(sqlite_conn, list(team_ids))
    known_team_ids = {team["id"] for team in teams}
    fixtures = [
        fixture
        for fixture in fixtures
        if fixture["home_team_id"] in known_team_ids and fixture["away_team_id"] in known_team_ids
    ]
    fixture_ids = sorted({fixture["id"] for fixture in fixtures})
    if not fixture_ids:
        raise SystemExit("No fixtures to export")

    fixture_players = filter_rows_for_remote_schema(
        "fixture_players",
        fetch_fixture_players(sqlite_conn, fixture_ids),
    )
    fixture_statistics = filter_rows_for_remote_schema(
        "fixture_statistics",
        fetch_fixture_statistics(sqlite_conn, fixture_ids),
    )
    fixture_player_statistics = filter_rows_for_remote_schema(
        "fixture_player_statistics",
        fetch_fixture_player_statistics(sqlite_conn, fixture_ids),
    )
    odds_outcomes = filter_rows_for_remote_schema(
        "odds_outcomes",
        fetch_odds_outcomes(sqlite_conn, fixture_ids),
    )

    pg_conn = psycopg2.connect(SUPABASE_DB_URL)
    try:
        with pg_conn.cursor() as cur:
            cur.execute("set statement_timeout = 0")
            cur.execute("set lock_timeout = 0")
            cur.execute("set idle_in_transaction_session_timeout = 0")
        pg_conn.commit()
        exported_fixture_players = copy_table(
            pg_conn,
            "fixture_players",
            fixture_players,
            list(fixture_players[0].keys()) if fixture_players else [],
            fixture_ids,
        )
        exported_fixture_statistics = copy_table(
            pg_conn,
            "fixture_statistics",
            fixture_statistics,
            list(fixture_statistics[0].keys()) if fixture_statistics else [],
            fixture_ids,
        )
        exported_fixture_player_statistics = copy_table(
            pg_conn,
            "fixture_player_statistics",
            fixture_player_statistics,
            list(fixture_player_statistics[0].keys()) if fixture_player_statistics else [],
            fixture_ids,
        )
        exported_odds_outcomes = copy_table(
            pg_conn,
            "odds_outcomes",
            odds_outcomes,
            list(odds_outcomes[0].keys()) if odds_outcomes else [],
            fixture_ids,
        )
    finally:
        pg_conn.close()
        sqlite_conn.close()

    print(
        json.dumps(
            {
                "fixtures": len(fixture_ids),
                "fixture_players_exported": exported_fixture_players,
                "fixture_statistics_exported": exported_fixture_statistics,
                "fixture_player_statistics_exported": exported_fixture_player_statistics,
                "odds_outcomes_exported": exported_odds_outcomes,
            }
        )
    )


if __name__ == "__main__":
    main()
