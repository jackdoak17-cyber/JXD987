#!/usr/bin/env python3
"""Publish validated SportMonks standings to the shared Supabase database.

The customer website never calls SportMonks. This bounded worker fetches the
provider table, validates it as a complete season snapshot, and replaces the
canonical season and compatibility cache in one transaction. A failed fetch
or validation leaves the last-known-good table untouched.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import psycopg2
from psycopg2.extras import Json, execute_values

from jxd import SportMonksClient


LOG = logging.getLogger("sync_official_standings")
UTC = timezone.utc
DETAIL_TYPES = {
    "played": 129,
    "wins": 130,
    "draws": 131,
    "losses": 132,
    "goals_for": 133,
    "goals_against": 134,
    "goal_diff": 179,
}


@dataclass(frozen=True)
class SeasonTarget:
    league_id: int
    season_id: int
    start_date: str | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--leagues", help="comma-separated league IDs; defaults to config/league_ids.txt")
    parser.add_argument("--season-id", type=int, action="append", default=[])
    parser.add_argument("--current-only", action="store_true", help="refresh only the latest started season per league")
    parser.add_argument(
        "--expect-standing",
        action="append",
        default=[],
        metavar="SEASON:TEAM:POSITION:POINTS",
        help="fail unless the provider snapshot contains the exact standing",
    )
    parser.add_argument("--report-out")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def database_url() -> str:
    value = (
        os.getenv("SUPABASE_DB_URL_SESSION")
        or os.getenv("SUPABASE_DB_URL_POOLER")
        or os.getenv("SUPABASE_DB_URL")
    )
    if not value:
        raise RuntimeError("Missing SUPABASE_DB_URL_SESSION / SUPABASE_DB_URL_POOLER / SUPABASE_DB_URL")
    return value


def parse_leagues(value: str | None) -> list[int]:
    if value:
        raw = value.split(",")
    else:
        raw = Path("config/league_ids.txt").read_text(encoding="utf-8").splitlines()
    leagues = sorted({int(item.strip()) for item in raw if item.strip()})
    if not leagues or any(item <= 0 for item in leagues):
        raise ValueError("at least one positive league ID is required")
    return leagues


def required_int(value: Any, field: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be an integer; got {value!r}") from exc
    return parsed


def optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return required_int(value, "optional integer")


def load_targets(
    cur,
    leagues: list[int],
    season_ids: list[int],
    *,
    current_only: bool = False,
) -> list[SeasonTarget]:
    if season_ids:
        cur.execute(
            """
            select s.league_id, s.id, s.start_date::text
              from public.seasons s
             where s.league_id = any(%s)
               and s.id = any(%s)
             order by s.league_id, s.start_date, s.id
            """,
            (leagues, season_ids),
        )
    elif current_only:
        cur.execute(
            """
            with candidates as (
              select
                s.league_id,
                s.id,
                s.start_date,
                row_number() over (
                  partition by s.league_id
                  order by s.start_date desc nulls last, s.id desc
                ) as season_rank
              from public.seasons s
              where s.league_id = any(%s)
                and (s.start_date is null or s.start_date <= current_date)
                and exists (
                  select 1
                    from public.fixtures f
                   where f.league_id = s.league_id
                     and f.season_id = s.id
                )
            )
            select c.league_id, c.id, c.start_date::text
              from candidates c
             where c.season_rank = 1
             order by c.league_id, c.start_date, c.id
            """,
            (leagues,),
        )
    else:
        # Refresh the latest started season for every supported league and
        # backfill any older season that the website can source from fixtures.
        cur.execute(
            """
            with candidates as (
              select
                s.league_id,
                s.id,
                s.start_date,
                row_number() over (
                  partition by s.league_id
                  order by s.start_date desc nulls last, s.id desc
                ) as season_rank
              from public.seasons s
              where s.league_id = any(%s)
                and (s.start_date is null or s.start_date <= current_date)
                and exists (
                  select 1
                    from public.fixtures f
                   where f.league_id = s.league_id
                     and f.season_id = s.id
                )
            )
            select c.league_id, c.id, c.start_date::text
              from candidates c
             where c.season_rank = 1
                or not exists (
                  select 1
                    from public.official_team_standings o
                   where o.league_id = c.league_id
                     and o.season_id = c.id
                )
             order by c.league_id, c.start_date, c.id
            """,
            (leagues,),
        )
    rows = [SeasonTarget(int(row[0]), int(row[1]), row[2]) for row in cur.fetchall()]
    missing = set(season_ids) - {row.season_id for row in rows}
    if missing:
        raise RuntimeError(f"requested seasons are not available for the selected leagues: {sorted(missing)}")
    return rows


def parse_expectations(values: list[str]) -> dict[tuple[int, int], tuple[int, int]]:
    result: dict[tuple[int, int], tuple[int, int]] = {}
    for value in values:
        parts = value.split(":")
        if len(parts) != 4:
            raise ValueError(f"invalid --expect-standing {value!r}")
        season_id, team_id, position, points = (required_int(part, "expectation") for part in parts)
        result[(season_id, team_id)] = (position, points)
    return result


def verify_expectations(
    target: SeasonTarget,
    standings: list[dict[str, Any]],
    expectations: dict[tuple[int, int], tuple[int, int]],
) -> None:
    by_team = {int(row["team_id"]): row for row in standings}
    for (season_id, team_id), (position, points) in expectations.items():
        if season_id != target.season_id:
            continue
        actual = by_team.get(team_id)
        if actual is None:
            raise ValueError(f"expected team {team_id} missing from season {season_id}")
        actual_values = (int(actual["position"]), int(actual["points"]))
        if actual_values != (position, points):
            raise ValueError(
                f"standing expectation failed season={season_id} team={team_id}: "
                f"expected={(position, points)} actual={actual_values}"
            )


def fetch_rows(client: SportMonksClient, endpoint: str, **params: object) -> list[dict[str, Any]]:
    return [
        row
        for row in client.fetch_collection(endpoint, params=params or None, per_page=100)
        if isinstance(row, dict)
    ]


def selected_stage(stages: list[dict[str, Any]], standings: list[dict[str, Any]]) -> int:
    by_stage: dict[int, list[dict[str, Any]]] = {}
    for row in standings:
        stage_id = required_int(row.get("stage_id"), "standing.stage_id")
        by_stage.setdefault(stage_id, []).append(row)
    if not by_stage:
        raise ValueError("SportMonks returned no standings")

    stage_candidates = sorted(
        stages,
        key=lambda row: (
            bool(row.get("is_current")),
            not bool(row.get("pending")),
            required_int(row.get("sort_order") or 0, "stage.sort_order"),
            required_int(row.get("id"), "stage.id"),
        ),
        reverse=True,
    )
    for stage in stage_candidates:
        stage_id = required_int(stage.get("id"), "stage.id")
        if stage_id in by_stage:
            return stage_id
    if len(by_stage) == 1:
        return next(iter(by_stage))
    raise ValueError(
        "standings contain stages that are absent from the season stage response: "
        f"{sorted(by_stage)}"
    )


def detail_values(row: dict[str, Any]) -> dict[int, int]:
    values: dict[int, int] = {}
    for detail in row.get("details") or []:
        if not isinstance(detail, dict):
            continue
        type_id = optional_int(detail.get("type_id"))
        if type_id is None:
            continue
        values[type_id] = required_int(detail.get("value"), f"standing detail {type_id}")
    return values


def normalize_standings(
    target: SeasonTarget,
    stage_id: int,
    rows: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        if required_int(row.get("stage_id"), "standing.stage_id") != stage_id:
            continue
        if required_int(row.get("league_id"), "standing.league_id") != target.league_id:
            raise ValueError("SportMonks standing league does not match the requested league")
        if required_int(row.get("season_id"), "standing.season_id") != target.season_id:
            raise ValueError("SportMonks standing season does not match the requested season")
        participant = row.get("participant")
        if not isinstance(participant, dict):
            raise ValueError("SportMonks standing is missing the requested participant include")
        team_id = required_int(row.get("participant_id"), "standing.participant_id")
        if required_int(participant.get("id"), "standing.participant.id") != team_id:
            raise ValueError(f"participant identity mismatch for team {team_id}")
        team_name = str(participant.get("name") or "").strip()
        if not team_name:
            raise ValueError(f"participant name missing for team {team_id}")
        details = detail_values(row)
        missing_details = [name for name, type_id in DETAIL_TYPES.items() if type_id not in details]
        if missing_details:
            raise ValueError(f"standing {row.get('id')} missing details: {missing_details}")
        normalized.append(
            {
                "league_id": target.league_id,
                "season_id": target.season_id,
                "stage_id": stage_id,
                "group_id": optional_int(row.get("group_id")),
                "team_id": team_id,
                "provider_standing_id": required_int(row.get("id"), "standing.id"),
                "standing_rule_id": optional_int(row.get("standing_rule_id")),
                "round_id": optional_int(row.get("round_id")),
                "position": required_int(row.get("position"), "standing.position"),
                "points": required_int(row.get("points"), "standing.points"),
                **{name: details[type_id] for name, type_id in DETAIL_TYPES.items()},
                "result": str(row.get("result")) if row.get("result") is not None else None,
                "team_name": team_name,
                "team_short_code": participant.get("short_code"),
                "team_image_path": participant.get("image_path"),
            }
        )
    validate_table(normalized)
    return sorted(normalized, key=lambda row: (row["group_id"] or 0, row["position"], row["team_id"]))


def validate_table(rows: list[dict[str, Any]]) -> None:
    if len(rows) < 2:
        raise ValueError(f"refusing implausibly small standings table with {len(rows)} row(s)")
    team_ids: set[int] = set()
    positions: set[tuple[int, int]] = set()
    for row in rows:
        team_id = int(row["team_id"])
        position_key = (int(row["group_id"] or 0), int(row["position"]))
        if team_id in team_ids:
            raise ValueError(f"duplicate team {team_id} in selected standings stage")
        if position_key in positions:
            raise ValueError(f"duplicate position {position_key} in selected standings stage")
        team_ids.add(team_id)
        positions.add(position_key)
        if row["position"] <= 0 or row["played"] < 0:
            raise ValueError(f"invalid position/played values for team {team_id}")
        if row["played"] != row["wins"] + row["draws"] + row["losses"]:
            raise ValueError(f"P/W/D/L mismatch for team {team_id}")
        if row["goal_diff"] != row["goals_for"] - row["goals_against"]:
            raise ValueError(f"goal-difference mismatch for team {team_id}")


def normalize_corrections(
    target: SeasonTarget,
    rows: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[int] = set()
    for row in rows:
        correction_id = required_int(row.get("id"), "correction.id")
        if correction_id in seen:
            raise ValueError(f"duplicate correction {correction_id}")
        seen.add(correction_id)
        season_id = required_int(row.get("season_id"), "correction.season_id")
        if season_id != target.season_id:
            raise ValueError(f"correction {correction_id} belongs to season {season_id}")
        result.append(
            {
                "provider_correction_id": correction_id,
                "league_id": target.league_id,
                "season_id": target.season_id,
                "stage_id": required_int(row.get("stage_id"), "correction.stage_id"),
                "group_id": optional_int(row.get("group_id")),
                "team_id": required_int(row.get("participant_id"), "correction.participant_id"),
                "type_id": optional_int(row.get("type_id")),
                "value": required_int(row.get("value"), "correction.value"),
                "calc_type": str(row.get("calc_type")) if row.get("calc_type") is not None else None,
                "active": bool(row.get("active")),
            }
        )
    return result


def payload_hash(stages: Any, standings: Any, corrections: Any) -> str:
    payload = json.dumps(
        {"stages": stages, "standings": standings, "corrections": corrections},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def fetch_season(client: SportMonksClient, target: SeasonTarget) -> dict[str, Any]:
    stages = fetch_rows(client, f"stages/seasons/{target.season_id}")
    standings = list(
        client.fetch_collection(
            f"standings/seasons/{target.season_id}",
            includes=["details", "participant"],
            per_page=100,
        )
    )
    corrections = fetch_rows(client, f"standings/corrections/seasons/{target.season_id}")
    stage_id = selected_stage(stages, standings)
    normalized_standings = normalize_standings(target, stage_id, standings)
    normalized_corrections = normalize_corrections(target, corrections)
    return {
        "stage_id": stage_id,
        "standings": normalized_standings,
        "corrections": normalized_corrections,
        "payload_hash": payload_hash(stages, standings, corrections),
    }


def publish_season(cur, target: SeasonTarget, snapshot: dict[str, Any], fetched_at: datetime) -> None:
    standings = snapshot["standings"]
    corrections = snapshot["corrections"]
    digest = str(snapshot["payload_hash"])
    cur.execute("select pg_advisory_xact_lock(hashtext(%s))", (f"official-standings:{target.season_id}",))
    cur.execute(
        "select count(*) from public.official_team_standings where league_id = %s and season_id = %s",
        (target.league_id, target.season_id),
    )
    previous_count = int(cur.fetchone()[0])
    if previous_count and len(standings) < previous_count:
        raise RuntimeError(
            f"refusing standings shrink for league={target.league_id} season={target.season_id}: "
            f"previous={previous_count} received={len(standings)}"
        )

    cur.execute(
        "delete from public.official_team_standings where league_id = %s and season_id = %s",
        (target.league_id, target.season_id),
    )
    execute_values(
        cur,
        """
        insert into public.official_team_standings (
          league_id, season_id, stage_id, group_id, team_id,
          provider_standing_id, standing_rule_id, round_id, position, points,
          played, wins, draws, losses, goals_for, goals_against, goal_diff,
          result, team_name, team_short_code, team_image_path,
          source_payload_hash, fetched_at
        ) values %s
        """,
        [
            (
                row["league_id"], row["season_id"], row["stage_id"], row["group_id"], row["team_id"],
                row["provider_standing_id"], row["standing_rule_id"], row["round_id"], row["position"], row["points"],
                row["played"], row["wins"], row["draws"], row["losses"], row["goals_for"], row["goals_against"], row["goal_diff"],
                row["result"], row["team_name"], row["team_short_code"], row["team_image_path"], digest, fetched_at,
            )
            for row in standings
        ],
        page_size=100,
    )

    cur.execute(
        "delete from public.official_standing_corrections where league_id = %s and season_id = %s",
        (target.league_id, target.season_id),
    )
    if corrections:
        execute_values(
            cur,
            """
            insert into public.official_standing_corrections (
              provider_correction_id, league_id, season_id, stage_id, group_id,
              team_id, type_id, value, calc_type, active, source_payload_hash, fetched_at
            ) values %s
            """,
            [
                (
                    row["provider_correction_id"], row["league_id"], row["season_id"], row["stage_id"], row["group_id"],
                    row["team_id"], row["type_id"], row["value"], row["calc_type"], row["active"], digest, fetched_at,
                )
                for row in corrections
            ],
            page_size=100,
        )

    # Existing website consumers keep their stable read contract while the
    # cache becomes an atomic projection of the official provider table.
    cur.execute(
        "delete from public.team_standings_cache where league_id = %s and season_id = %s",
        (target.league_id, target.season_id),
    )
    execute_values(
        cur,
        """
        insert into public.team_standings_cache (
          league_id, season_id, team_id, rank, points, played,
          goals_for, goals_against, goal_diff, updated_at
        ) values %s
        """,
        [
            (
                row["league_id"], row["season_id"], row["team_id"], row["position"], row["points"], row["played"],
                row["goals_for"], row["goals_against"], row["goal_diff"], fetched_at,
            )
            for row in standings
        ],
        page_size=100,
    )
    cur.execute(
        """
        insert into public.official_standings_sync_runs (
          league_id, season_id, status, rows_received, corrections_received,
          selected_stage_id, source_payload_hash, evidence, finished_at
        ) values (%s, %s, 'succeeded', %s, %s, %s, %s, %s, now())
        """,
        (
            target.league_id,
            target.season_id,
            len(standings),
            len(corrections),
            snapshot["stage_id"],
            digest,
            Json({"previous_rows": previous_count, "published_rows": len(standings)}),
        ),
    )


def record_failure(conn, target: SeasonTarget, error: Exception) -> None:
    conn.rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.official_standings_sync_runs (
              league_id, season_id, status, error, finished_at
            ) values (%s, %s, 'failed', %s, now())
            """,
            (target.league_id, target.season_id, str(error)[:4000]),
        )
    conn.commit()


def write_report(path: str | None, report: dict[str, Any]) -> None:
    if not path:
        return
    Path(path).write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    leagues = parse_leagues(args.leagues)
    expectations = parse_expectations(args.expect_standing)
    report: dict[str, Any] = {
        "started_at": datetime.now(UTC).isoformat(),
        "leagues": leagues,
        "dry_run": bool(args.dry_run),
        "seasons": [],
    }
    conn = psycopg2.connect(database_url())
    conn.autocommit = False
    client = SportMonksClient()
    try:
        with conn.cursor() as cur:
            targets = load_targets(cur, leagues, args.season_id, current_only=bool(args.current_only))
        conn.rollback()
        if not targets:
            raise RuntimeError("no standings seasons matched the requested scope")
        for target in targets:
            LOG.info("Fetching official standings league=%s season=%s", target.league_id, target.season_id)
            try:
                snapshot = fetch_season(client, target)
                verify_expectations(target, snapshot["standings"], expectations)
                if not args.dry_run:
                    with conn.cursor() as cur:
                        publish_season(cur, target, snapshot, datetime.now(UTC))
                    conn.commit()
                report["seasons"].append(
                    {
                        "league_id": target.league_id,
                        "season_id": target.season_id,
                        "stage_id": snapshot["stage_id"],
                        "rows": len(snapshot["standings"]),
                        "corrections": len(snapshot["corrections"]),
                        "payload_hash": snapshot["payload_hash"],
                        "status": "validated" if args.dry_run else "published",
                    }
                )
            except Exception as exc:  # noqa: BLE001
                if not args.dry_run:
                    record_failure(conn, target, exc)
                report["seasons"].append(
                    {"league_id": target.league_id, "season_id": target.season_id, "status": "failed", "error": str(exc)}
                )
                report["status"] = "failed"
                report["finished_at"] = datetime.now(UTC).isoformat()
                write_report(args.report_out, report)
                raise
        report["status"] = "succeeded"
        report["finished_at"] = datetime.now(UTC).isoformat()
        report["sportmonks_calls"] = client.stats
        write_report(args.report_out, report)
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
