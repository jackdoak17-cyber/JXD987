#!/usr/bin/env python3
"""
Deduplicate legacy odds_outcomes rows in SQLite.

This targets historical rows that slipped past the nullable-line uniqueness hole in the
local SQLite table. It keeps the latest row per intended local write key:

  fixture_id, bookmaker_id, market_key, selection_key, line

The script is intended for controlled maintenance runs, not the normal cron chain.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
from typing import Optional, Sequence

DB_PATH_DEFAULT = os.environ.get("JXD_DB_PATH", "data/jxd.sqlite")
NULL_LINE_SENTINEL = -999999.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Deduplicate odds_outcomes rows in SQLite.")
    parser.add_argument("--db", default=DB_PATH_DEFAULT, help="Path to SQLite DB.")
    parser.add_argument(
        "--league-ids",
        default="",
        help="Optional comma-separated league ids to scope the cleanup.",
    )
    parser.add_argument(
        "--days-forward",
        type=int,
        default=0,
        help="Optional future fixture window in days. Used only with --league-ids.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Report duplicate counts only.")
    parser.add_argument("--vacuum", action="store_true", help="Run VACUUM after deleting rows.")
    return parser.parse_args()


def parse_league_ids(raw: str) -> list[int]:
    values: list[int] = []
    for chunk in (raw or "").split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        values.append(int(chunk))
    return values


def fixture_scope_sql(league_ids: Sequence[int], days_forward: int) -> tuple[str, list[object]]:
    if not league_ids:
        return "", []
    placeholders = ",".join("?" for _ in league_ids)
    sql = f"""
        where fixture_id in (
            select id
            from fixtures
            where league_id in ({placeholders})
        """
    params: list[object] = list(league_ids)
    if days_forward > 0:
        sql += """
              and starting_at >= datetime('now')
              and starting_at < datetime('now', ?)
        """
        params.append(f"+{days_forward} days")
    sql += "\n        )"
    return sql, params


def duplicate_count(conn: sqlite3.Connection, scope_sql: str, params: Sequence[object]) -> tuple[int, int]:
    sql = f"""
        with ranked as (
            select
                id,
                row_number() over (
                    partition by
                        fixture_id,
                        bookmaker_id,
                        market_key,
                        selection_key,
                        coalesce(round(line, 6), {NULL_LINE_SENTINEL})
                    order by coalesce(last_updated_at, '') desc, id desc
                ) as rn
            from odds_outcomes
            {scope_sql}
        )
        select
            sum(case when rn > 1 then 1 else 0 end) as duplicate_rows,
            count(distinct case when rn > 1 then id end) as duplicate_ids
        from ranked
    """
    row = conn.execute(sql, params).fetchone()
    if not row:
        return 0, 0
    duplicate_rows = int(row[0] or 0)
    duplicate_ids = int(row[1] or 0)
    return duplicate_rows, duplicate_ids


def delete_duplicates(conn: sqlite3.Connection, scope_sql: str, params: Sequence[object]) -> int:
    sql = f"""
        delete from odds_outcomes
        where id in (
            with ranked as (
                select
                    id,
                    row_number() over (
                        partition by
                            fixture_id,
                            bookmaker_id,
                            market_key,
                            selection_key,
                            coalesce(round(line, 6), {NULL_LINE_SENTINEL})
                        order by coalesce(last_updated_at, '') desc, id desc
                    ) as rn
                from odds_outcomes
                {scope_sql}
            )
            select id
            from ranked
            where rn > 1
        )
    """
    cursor = conn.execute(sql, params)
    return int(cursor.rowcount or 0)


def main() -> int:
    args = parse_args()
    league_ids = parse_league_ids(args.league_ids)
    scope_sql, params = fixture_scope_sql(league_ids, args.days_forward)

    conn = sqlite3.connect(args.db)
    try:
        duplicate_rows, duplicate_ids = duplicate_count(conn, scope_sql, params)
        summary = {
            "db": args.db,
            "league_ids": league_ids,
            "days_forward": args.days_forward,
            "duplicate_rows": duplicate_rows,
            "duplicate_ids": duplicate_ids,
            "dry_run": args.dry_run,
            "deleted_rows": 0,
            "vacuum": False,
        }
        if duplicate_rows == 0 or args.dry_run:
            print(json.dumps(summary))
            return 0

        with conn:
            deleted_rows = delete_duplicates(conn, scope_sql, params)
        summary["deleted_rows"] = deleted_rows

        if args.vacuum:
            conn.execute("vacuum")
            summary["vacuum"] = True

        print(json.dumps(summary))
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
