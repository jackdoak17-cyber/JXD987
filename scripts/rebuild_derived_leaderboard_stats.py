#!/usr/bin/env python3
"""Rebuild leaderboard-only derived stats in local SQLite."""

from __future__ import annotations

import argparse
import logging
import os
import sqlite3
from typing import Sequence


DB_PATH = os.environ.get("JXD_DB_PATH", "data/jxd.sqlite")

SHOT_ACCURACY_PERCENT_TYPE_ID = 200010
SHOT_ACCURACY_PERCENT_CODE = "shot_accuracy_percent"
SHOT_ACCURACY_PERCENT_NAME = "Shot Accuracy %"

INSIDE_BOX_SHOT_SHARE_PERCENT_TYPE_ID = 200011
INSIDE_BOX_SHOT_SHARE_PERCENT_CODE = "inside_box_shot_share_percent"
INSIDE_BOX_SHOT_SHARE_PERCENT_NAME = "Inside Box Shot Share %"

CROSS_ACCURACY_PERCENT_TYPE_ID = 200012
CROSS_ACCURACY_PERCENT_CODE = "cross_accuracy_percent"
CROSS_ACCURACY_PERCENT_NAME = "Cross Accuracy %"

DEFENSIVE_INVOLVEMENT_TYPE_ID = 200013
DEFENSIVE_INVOLVEMENT_CODE = "defensive_involvement"
DEFENSIVE_INVOLVEMENT_NAME = "Defensive Involvement"

SHOTS_TOTAL_TYPE_ID = 42
SHOTS_ON_TARGET_TYPE_ID = 86
SHOTS_INSIDEBOX_TYPE_ID = 49
TOTAL_CROSSES_TYPE_ID = 98
ACCURATE_CROSSES_TYPE_ID = 99
TACKLES_TYPE_ID = 78
INTERCEPTIONS_TYPE_ID = 100
CLEARANCES_TYPE_ID = 101
BLOCKED_SHOTS_TYPE_ID = 97
BALL_RECOVERY_TYPE_ID = 27271

PLAYER_RATIO_STATS = (
    (
        SHOT_ACCURACY_PERCENT_TYPE_ID,
        SHOT_ACCURACY_PERCENT_CODE,
        SHOT_ACCURACY_PERCENT_NAME,
        SHOTS_ON_TARGET_TYPE_ID,
        SHOTS_TOTAL_TYPE_ID,
    ),
    (
        CROSS_ACCURACY_PERCENT_TYPE_ID,
        CROSS_ACCURACY_PERCENT_CODE,
        CROSS_ACCURACY_PERCENT_NAME,
        ACCURATE_CROSSES_TYPE_ID,
        TOTAL_CROSSES_TYPE_ID,
    ),
)

TEAM_RATIO_STATS = (
    (
        SHOT_ACCURACY_PERCENT_TYPE_ID,
        SHOT_ACCURACY_PERCENT_CODE,
        SHOT_ACCURACY_PERCENT_NAME,
        SHOTS_ON_TARGET_TYPE_ID,
        SHOTS_TOTAL_TYPE_ID,
    ),
    (
        INSIDE_BOX_SHOT_SHARE_PERCENT_TYPE_ID,
        INSIDE_BOX_SHOT_SHARE_PERCENT_CODE,
        INSIDE_BOX_SHOT_SHARE_PERCENT_NAME,
        SHOTS_INSIDEBOX_TYPE_ID,
        SHOTS_TOTAL_TYPE_ID,
    ),
    (
        CROSS_ACCURACY_PERCENT_TYPE_ID,
        CROSS_ACCURACY_PERCENT_CODE,
        CROSS_ACCURACY_PERCENT_NAME,
        ACCURATE_CROSSES_TYPE_ID,
        TOTAL_CROSSES_TYPE_ID,
    ),
)

ALL_DERIVED_TYPE_IDS = (
    SHOT_ACCURACY_PERCENT_TYPE_ID,
    INSIDE_BOX_SHOT_SHARE_PERCENT_TYPE_ID,
    CROSS_ACCURACY_PERCENT_TYPE_ID,
    DEFENSIVE_INVOLVEMENT_TYPE_ID,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger(__name__)


def build_fixture_filter(column: str, fixture_ids: Sequence[int]) -> tuple[str, list[int]]:
    if not fixture_ids:
        return "", []
    placeholders = ",".join("?" for _ in fixture_ids)
    return f" and {column} in ({placeholders})", list(fixture_ids)


def delete_existing_rows(conn: sqlite3.Connection, fixture_ids: Sequence[int]) -> None:
    fixture_filter, fixture_params = build_fixture_filter("fixture_id", fixture_ids)
    type_placeholders = ",".join("?" for _ in ALL_DERIVED_TYPE_IDS)
    params = list(ALL_DERIVED_TYPE_IDS) + fixture_params
    for table in ("fixture_player_statistics", "fixture_statistics"):
        conn.execute(
            f"delete from {table} where type_id in ({type_placeholders}){fixture_filter}",
            params,
        )


def insert_player_ratio_stat(
    conn: sqlite3.Connection,
    type_id: int,
    code: str,
    name: str,
    numerator_type_id: int,
    denominator_type_id: int,
    fixture_ids: Sequence[int],
) -> None:
    fixture_filter, fixture_params = build_fixture_filter("fixture_id", fixture_ids)
    params = [
        numerator_type_id,
        denominator_type_id,
        *fixture_params,
        numerator_type_id,
        denominator_type_id,
        type_id,
        code,
        name,
        numerator_type_id,
        denominator_type_id,
    ]
    conn.execute(
        f"""
        insert into fixture_player_statistics (
          fixture_id, player_id, team_id, type_id, code, name, value, extra
        )
        with base as (
          select
            fixture_id,
            player_id,
            max(team_id) as team_id,
            type_id,
            max(cast(value as real)) as value
          from fixture_player_statistics
          where type_id in (?, ?){fixture_filter}
          group by fixture_id, player_id, type_id
        ),
        agg as (
          select
            fixture_id,
            player_id,
            max(team_id) as team_id,
            sum(case when type_id = ? then value else 0 end) as numerator_value,
            sum(case when type_id = ? then value else 0 end) as denominator_value
          from base
          group by fixture_id, player_id
        )
        select
          fixture_id,
          player_id,
          team_id,
          ?,
          ?,
          ?,
          round((numerator_value / denominator_value) * 100.0, 4),
          json_object(
            'source', 'derived',
            'formula', 'ratio_percent',
            'numerator_type_id', ?,
            'denominator_type_id', ?,
            'numerator_value', numerator_value,
            'denominator_value', denominator_value
          )
        from agg
        where denominator_value > 0
        """,
        params,
    )


def insert_team_ratio_stat(
    conn: sqlite3.Connection,
    type_id: int,
    code: str,
    name: str,
    numerator_type_id: int,
    denominator_type_id: int,
    fixture_ids: Sequence[int],
) -> None:
    fixture_filter, fixture_params = build_fixture_filter("fixture_id", fixture_ids)
    params = [
        numerator_type_id,
        denominator_type_id,
        *fixture_params,
        numerator_type_id,
        denominator_type_id,
        type_id,
        code,
        name,
        numerator_type_id,
        denominator_type_id,
    ]
    conn.execute(
        f"""
        insert into fixture_statistics (
          fixture_id, team_id, type_id, code, name, location, value, extra
        )
        with base as (
          select
            fixture_id,
            team_id,
            type_id,
            max(cast(value as real)) as value
          from fixture_statistics
          where type_id in (?, ?){fixture_filter}
          group by fixture_id, team_id, type_id
        ),
        agg as (
          select
            fixture_id,
            team_id,
            sum(case when type_id = ? then value else 0 end) as numerator_value,
            sum(case when type_id = ? then value else 0 end) as denominator_value
          from base
          group by fixture_id, team_id
        )
        select
          fixture_id,
          team_id,
          ?,
          ?,
          ?,
          null,
          round((numerator_value / denominator_value) * 100.0, 4),
          json_object(
            'source', 'derived',
            'formula', 'ratio_percent',
            'numerator_type_id', ?,
            'denominator_type_id', ?,
            'numerator_value', numerator_value,
            'denominator_value', denominator_value
          )
        from agg
        where denominator_value > 0
        """,
        params,
    )


def insert_defensive_involvement(conn: sqlite3.Connection, fixture_ids: Sequence[int]) -> None:
    component_type_ids = (
        TACKLES_TYPE_ID,
        INTERCEPTIONS_TYPE_ID,
        CLEARANCES_TYPE_ID,
        BALL_RECOVERY_TYPE_ID,
        BLOCKED_SHOTS_TYPE_ID,
    )
    fixture_filter, fixture_params = build_fixture_filter("fixture_id", fixture_ids)
    component_placeholders = ",".join("?" for _ in component_type_ids)
    params = [
        *component_type_ids,
        *fixture_params,
        DEFENSIVE_INVOLVEMENT_TYPE_ID,
        DEFENSIVE_INVOLVEMENT_CODE,
        DEFENSIVE_INVOLVEMENT_NAME,
        *component_type_ids,
    ]
    conn.execute(
        f"""
        insert into fixture_player_statistics (
          fixture_id, player_id, team_id, type_id, code, name, value, extra
        )
        with base as (
          select
            fixture_id,
            player_id,
            max(team_id) as team_id,
            type_id,
            max(cast(value as real)) as value
          from fixture_player_statistics
          where type_id in ({component_placeholders}){fixture_filter}
          group by fixture_id, player_id, type_id
        ),
        agg as (
          select
            fixture_id,
            player_id,
            max(team_id) as team_id,
            sum(value) as total_value,
            json_group_object(cast(type_id as text), value) as component_values
          from base
          group by fixture_id, player_id
        )
        select
          fixture_id,
          player_id,
          team_id,
          ?,
          ?,
          ?,
          total_value,
          json_object(
            'source', 'derived',
            'formula', 'sum',
            'component_type_ids', json_array({component_placeholders}),
            'component_values', json(component_values)
          )
        from agg
        where total_value > 0
        """,
        params,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Rebuild leaderboard-only derived stats in SQLite.")
    parser.add_argument(
        "--fixture-ids",
        default="",
        help="Optional comma-separated fixture IDs to rebuild. Defaults to all fixtures.",
    )
    args = parser.parse_args()

    fixture_ids = [
        int(chunk.strip())
        for chunk in args.fixture_ids.split(",")
        if chunk.strip()
    ]

    if not os.path.exists(DB_PATH):
        raise SystemExit(f"SQLite DB not found at {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    try:
        delete_existing_rows(conn, fixture_ids)
        for stat in PLAYER_RATIO_STATS:
            insert_player_ratio_stat(conn, *stat, fixture_ids)
        for stat in TEAM_RATIO_STATS:
            insert_team_ratio_stat(conn, *stat, fixture_ids)
        insert_defensive_involvement(conn, fixture_ids)
        conn.commit()

        player_counts = conn.execute(
            f"select type_id, count(*) from fixture_player_statistics where type_id in ({','.join('?' for _ in ALL_DERIVED_TYPE_IDS)}) group by type_id order by type_id",
            ALL_DERIVED_TYPE_IDS,
        ).fetchall()
        team_counts = conn.execute(
            f"select type_id, count(*) from fixture_statistics where type_id in ({','.join('?' for _ in ALL_DERIVED_TYPE_IDS)}) group by type_id order by type_id",
            ALL_DERIVED_TYPE_IDS,
        ).fetchall()
        log.info("Player derived rows: %s", player_counts)
        log.info("Team derived rows: %s", team_counts)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
