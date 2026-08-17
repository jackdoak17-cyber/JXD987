#!/usr/bin/env python3
"""Fail the pipeline if a current-season squad is stale or diverges in Supabase.

The local database is the ingestion workspace; the public database is the
contract.  This check reads both so a successful provider fetch is not treated
as success until the reconciled snapshot has reached Supabase.
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Set

import requests
from sqlalchemy import bindparam, text

from jxd.db import get_engine, get_session
from scripts.export_to_supabase import REST_PATH, SUPABASE_URL, require_env, rest_headers
from scripts.sync_sparse_squads import current_season_team_ids, parse_ids


def _write_report(path: str, report: Dict) -> None:
    if path:
        Path(path).write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")


def _fetch_remote(table: str, params: Dict[str, str]) -> List[Dict]:
    response = requests.get(
        f"{SUPABASE_URL.rstrip()}{REST_PATH}/{table}",
        headers=rest_headers(),
        params=params,
        timeout=60,
    )
    if not response.ok:
        raise SystemExit(f"Supabase {table} verification query failed {response.status_code}: {response.text}")
    payload = response.json()
    if not isinstance(payload, list):
        raise SystemExit(f"Supabase {table} verification returned a non-list response")
    return [row for row in payload if isinstance(row, dict)]


def _chunks(values: Sequence[int], size: int = 100) -> Iterable[Sequence[int]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


def _remote_rows(table: str, team_ids: Sequence[int], select_columns: str, extra: Dict[str, str] | None = None) -> List[Dict]:
    rows: List[Dict] = []
    order_by = {
        "players": "id.asc",
        "team_squad_memberships": "team_id.asc,player_id.asc",
        "team_squad_snapshots": "team_id.asc,id.asc",
    }.get(table, "team_id.asc")
    for chunk in _chunks(team_ids):
        offset = 0
        while True:
            # Supabase commonly caps REST responses at 1,000 rows even when a
            # higher limit is requested.  Paginate so fleet verification never
            # drops memberships from larger leagues and reports a false alarm.
            params = {
                "select": select_columns,
                "team_id": f"in.({','.join(str(value) for value in chunk)})",
                "limit": "1000",
                "offset": str(offset),
                "order": order_by,
            }
            if extra:
                params.update(extra)
            page = _fetch_remote(table, params)
            rows.extend(page)
            if len(page) < 1000:
                break
            offset += len(page)
    return rows


def _latest_successful_snapshot(rows: Sequence[Dict], now: datetime) -> Dict[int, Dict]:
    latest: Dict[int, Dict] = {}
    for row in rows:
        if row.get("status") != "success":
            continue
        team_id = row.get("team_id")
        observed_at = row.get("observed_at")
        if team_id is None or not observed_at:
            continue
        try:
            parsed_team_id = int(team_id)
            parsed_at = datetime.fromisoformat(str(observed_at).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            continue
        if parsed_at.tzinfo is None:
            parsed_at = parsed_at.replace(tzinfo=timezone.utc)
        if parsed_at > now + timedelta(minutes=5):
            continue
        existing = latest.get(parsed_team_id)
        if existing is None or str(observed_at) > str(existing["observed_at"]):
            latest[parsed_team_id] = row
    return latest


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--leagues", default=os.environ.get("LEAGUE_IDS", ""))
    parser.add_argument("--max-age-hours", type=float, default=float(os.environ.get("SQUAD_FRESHNESS_MAX_HOURS", "12")))
    parser.add_argument("--report-json", default="")
    parser.add_argument("--dry-run", action="store_true", help="Validate local snapshot freshness only.")
    args = parser.parse_args()
    if args.max_age_hours <= 0:
        raise SystemExit("--max-age-hours must be positive")
    require_env(args.dry_run)

    session = get_session(get_engine())
    team_ids = current_season_team_ids(session, parse_ids(args.leagues), 0)
    team_ids = sorted(set(team_ids))
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=args.max_age_hours)

    local_rows = session.execute(
        text(
            """
            select team_id, status, observed_at, player_count
            from team_squad_snapshots
            where team_id in :team_ids
            order by observed_at desc
            """
        ).bindparams(bindparam("team_ids", expanding=True)),
        {"team_ids": team_ids},
    ).mappings().all() if team_ids else []
    local_latest = _latest_successful_snapshot(local_rows, now)

    failures: Dict[str, List[Dict]] = {"missing_local_snapshot": [], "stale_local_snapshot": []}
    for team_id in team_ids:
        row = local_latest.get(team_id)
        if row is None:
            failures["missing_local_snapshot"].append({"team_id": team_id})
            continue
        observed_at = datetime.fromisoformat(str(row["observed_at"]).replace("Z", "+00:00"))
        if observed_at.tzinfo is None:
            observed_at = observed_at.replace(tzinfo=timezone.utc)
        if observed_at < cutoff:
            failures["stale_local_snapshot"].append({"team_id": team_id, "observed_at": str(row["observed_at"])})

    if not args.dry_run:
        remote_snapshots = _remote_rows(
            "team_squad_snapshots", team_ids, "id,team_id,status,observed_at,player_count"
        )
        remote_memberships = _remote_rows(
            "team_squad_memberships", team_ids, "team_id,player_id,is_active,last_snapshot_id"
        )
        remote_players = _remote_rows("players", team_ids, "id,team_id")
        remote_latest = _latest_successful_snapshot(remote_snapshots, now)
        failures.update({"missing_remote_snapshot": [], "stale_remote_snapshot": [], "snapshot_membership_count_mismatch": [], "player_assignment_mismatch": []})
        active_members_by_team: Dict[int, Set[int]] = {}
        members_by_snapshot: Dict[tuple[int, int], Set[int]] = {}
        for row in remote_memberships:
            if row.get("team_id") is not None and row.get("player_id") is not None:
                team_id = int(row["team_id"])
                player_id = int(row["player_id"])
                snapshot_id = row.get("last_snapshot_id")
                if snapshot_id is not None:
                    members_by_snapshot.setdefault((team_id, int(snapshot_id)), set()).add(player_id)
                if row.get("is_active"):
                    active_members_by_team.setdefault(team_id, set()).add(player_id)
        players_by_team: Dict[int, Set[int]] = {}
        for row in remote_players:
            if row.get("team_id") is not None and row.get("id") is not None:
                players_by_team.setdefault(int(row["team_id"]), set()).add(int(row["id"]))
        for team_id in team_ids:
            snapshot = remote_latest.get(team_id)
            if snapshot is None:
                failures["missing_remote_snapshot"].append({"team_id": team_id})
                continue
            observed_at = datetime.fromisoformat(str(snapshot["observed_at"]).replace("Z", "+00:00"))
            if observed_at.tzinfo is None:
                observed_at = observed_at.replace(tzinfo=timezone.utc)
            if observed_at < cutoff:
                failures["stale_remote_snapshot"].append({"team_id": team_id, "observed_at": str(snapshot["observed_at"])})
            member_ids = active_members_by_team.get(team_id, set())
            snapshot_member_ids = members_by_snapshot.get((team_id, int(snapshot["id"])), set())
            expected_count = int(snapshot.get("player_count") or 0)
            if len(snapshot_member_ids) != expected_count:
                failures["snapshot_membership_count_mismatch"].append(
                    {"team_id": team_id, "snapshot_id": snapshot["id"], "expected": expected_count, "actual": len(snapshot_member_ids)}
                )
            assigned_ids = players_by_team.get(team_id, set())
            if assigned_ids != member_ids:
                failures["player_assignment_mismatch"].append(
                    {"team_id": team_id, "only_in_players": sorted(assigned_ids - member_ids), "only_in_membership": sorted(member_ids - assigned_ids)}
                )

    failing_groups = {name: rows for name, rows in failures.items() if rows}
    report = {
        "checked_at": now.isoformat(),
        "max_age_hours": args.max_age_hours,
        "current_season_team_count": len(team_ids),
        "local_latest_successful_snapshots": len(local_latest),
        "dry_run": args.dry_run,
        "failures": failing_groups,
        "ok": not failing_groups,
    }
    _write_report(args.report_json, report)
    print(json.dumps(report, sort_keys=True))
    if failing_groups:
        raise SystemExit("Squad freshness verification failed")


if __name__ == "__main__":
    main()
