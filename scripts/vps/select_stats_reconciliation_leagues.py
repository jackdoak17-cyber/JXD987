#!/usr/bin/env python3
"""Persist bounded, release-scoped reconciliation attempts and their outcomes."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def parse_leagues(raw: str) -> list[int]:
    leagues: list[int] = []
    for token in raw.split(","):
        token = token.strip()
        if not token or not token.isascii() or not token.isdecimal():
            raise ValueError("league IDs must be a nonempty comma-separated list of positive integers")
        league_id = int(token)
        if league_id <= 0 or league_id in leagues:
            raise ValueError("league IDs must be positive and unique")
        leagues.append(league_id)
    return leagues


def groups_for(leagues: list[int], group_size: int) -> list[list[int]]:
    if not leagues or group_size < 1:
        raise ValueError("at least one league and a positive group size are required")
    return [leagues[i : i + group_size] for i in range(0, len(leagues), group_size)]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _signature(leagues: list[int], group_size: int) -> str:
    return hashlib.sha256(
        json.dumps([leagues, group_size], separators=(",", ":")).encode("ascii")
    ).hexdigest()


def _new_state(leagues: list[int], group_size: int, release_id: str, previous: dict[str, Any] | None = None) -> dict[str, Any]:
    retained = {str(league) for league in leagues}
    unresolved = dict(previous.get("unresolved", {})) if previous else {}
    failures = dict(previous.get("last_failure", {})) if previous else {}
    pending = previous.get("pending") if previous else None
    if isinstance(pending, dict):
        for league in pending.get("leagues", []):
            if str(league) in retained:
                event = {"outcome": "interrupted", "at": _now(), "attempt_id": pending.get("attempt_id", "unknown")}
                unresolved[str(league)] = event
                failures[str(league)] = event
    return {
        "version": 2, "signature": _signature(leagues, group_size),
        "release_id": release_id, "epoch": uuid.uuid4().hex,
        "leagues": leagues, "group_size": group_size, "next_group": 0,
        "pending": None, "last_completed": {},
        "unresolved": {key: value for key, value in unresolved.items() if key in retained},
        "last_failure": {key: value for key, value in failures.items() if key in retained},
        "empty_streak": 0, "empty_sweep_at": None,
    }


def _validate(state: Any) -> None:
    if not isinstance(state, dict) or state.get("version") != 2:
        raise ValueError("invalid reconciliation scope state version")
    leagues = state.get("leagues")
    group_size = state.get("group_size")
    if (not isinstance(leagues, list) or not leagues
            or any(type(league) is not int or league <= 0 for league in leagues)
            or len(set(leagues)) != len(leagues)
            or type(group_size) is not int or group_size < 1):
        raise ValueError("invalid saved reconciliation league configuration")
    groups = groups_for(leagues, group_size)
    if state.get("signature") != _signature(leagues, group_size):
        raise ValueError("invalid saved reconciliation scope signature")
    if not isinstance(state.get("signature"), str) or not isinstance(state.get("release_id"), str):
        raise ValueError("invalid reconciliation scope identity")
    if not isinstance(state.get("epoch"), str) or not state["epoch"]:
        raise ValueError("invalid reconciliation scope epoch")
    if type(state.get("next_group")) is not int or not 0 <= state["next_group"] < len(groups):
        raise ValueError("invalid reconciliation next group")
    if type(state.get("empty_streak")) is not int or not 0 <= state["empty_streak"] <= len(groups):
        raise ValueError("invalid reconciliation empty streak")
    for field in ("last_completed", "unresolved", "last_failure"):
        value = state.get(field)
        if not isinstance(value, dict) or any(
            not key.isdecimal() or int(key) not in leagues or not isinstance(entry, dict)
            for key, entry in value.items()
        ):
            raise ValueError(f"invalid reconciliation {field}")
    for completion in state["last_completed"].values():
        try:
            completed_at = datetime.fromisoformat(completion["at"])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError("invalid saved reconciliation completion time") from error
        if (completed_at.tzinfo is None
                or completion.get("outcome") not in ("empty", "processed")
                or type(completion.get("selected")) is not int
                or type(completion.get("accepted")) is not int
                or completion["selected"] < 0
                or completion["accepted"] < 0
                or completion["accepted"] > completion["selected"]
                or not isinstance(completion.get("attempt_id"), str)
                or not completion["attempt_id"]):
            raise ValueError("invalid saved reconciliation completion")
    pending = state.get("pending")
    if pending is not None:
        if not isinstance(pending, dict) or not isinstance(pending.get("attempt_id"), str):
            raise ValueError("invalid reconciliation pending attempt")
        index = pending.get("group_index")
        if type(index) is not int or not 0 <= index < len(groups) or pending.get("leagues") != groups[index]:
            raise ValueError("invalid reconciliation pending scope")
        if pending.get("epoch") != state["epoch"] or pending.get("release_id") != state["release_id"]:
            raise ValueError("invalid reconciliation pending identity")


def _load(leagues: list[int], group_size: int, release_id: str, cursor_file: Path) -> dict[str, Any]:
    if not release_id:
        raise ValueError("release ID is required")
    if not cursor_file.exists():
        return _new_state(leagues, group_size, release_id)
    raw = json.loads(cursor_file.read_text(encoding="utf-8"))
    if isinstance(raw, dict) and raw.get("version") == 1:
        # The first draft was never deployed, but a local cursor can be safely
        # upgraded without treating its scheduling position as health evidence.
        if raw.get("signature") != _signature(leagues, group_size):
            return _new_state(leagues, group_size, release_id)
        state = _new_state(leagues, group_size, release_id)
        old_index = raw.get("next_group")
        if type(old_index) is not int or not 0 <= old_index < len(groups_for(leagues, group_size)):
            raise ValueError("invalid legacy reconciliation cursor index")
        state["next_group"] = old_index
        return state
    _validate(raw)
    if raw["signature"] != _signature(leagues, group_size) or raw["release_id"] != release_id:
        return _new_state(leagues, group_size, release_id, raw)
    if raw.get("leagues") != leagues or raw.get("group_size") != group_size:
        raise ValueError("reconciliation scope signature does not match configuration")
    return raw


def _save(state: dict[str, Any], cursor_file: Path) -> None:
    cursor_file.parent.mkdir(parents=True, exist_ok=True)
    temporary: str | None = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=cursor_file.parent,
                                         prefix=cursor_file.name + ".", delete=False) as handle:
            temporary = handle.name
            os.fchmod(handle.fileno(), 0o600)
            json.dump(state, handle, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, cursor_file)
        directory = os.open(cursor_file.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary and os.path.exists(temporary):
            os.unlink(temporary)


def _mark_unresolved(state: dict[str, Any], pending: dict[str, Any], outcome: str) -> None:
    event = {"outcome": outcome, "at": _now(), "attempt_id": pending["attempt_id"]}
    for league in pending["leagues"]:
        state["unresolved"][str(league)] = event
        state["last_failure"][str(league)] = event
    state["empty_streak"] = 0
    state["empty_sweep_at"] = None


def next_group(leagues: list[int], group_size: int, cursor_file: Path, release_id: str = "local") -> dict[str, Any]:
    groups = groups_for(leagues, group_size)
    state = _load(leagues, group_size, release_id, cursor_file)
    if state["pending"] is not None:
        _mark_unresolved(state, state["pending"], "interrupted")
    index = state["next_group"]
    pending = {
        "attempt_id": uuid.uuid4().hex, "group_index": index,
        "leagues": groups[index], "started_at": _now(),
        "epoch": state["epoch"], "release_id": release_id,
    }
    state["pending"] = pending
    state["next_group"] = (index + 1) % len(groups)
    _save(state, cursor_file)
    return pending


def _report_outcome(report_file: Path, pending: dict[str, Any]) -> tuple[str, int, int]:
    if not report_file.exists():
        return "incomplete", 0, 0
    report = json.loads(report_file.read_text(encoding="utf-8"))
    if not isinstance(report, dict) or any(report.get(key) != pending[key] for key in ("release_id", "leagues")):
        return "invalid_report", 0, 0
    if report.get("scope_attempt_id") != pending["attempt_id"] or report.get("scope_epoch") != pending["epoch"]:
        return "invalid_report", 0, 0
    if report.get("status") != "success":
        return "failed", 0, 0
    batches = report.get("batches")
    selected = report.get("fixtures_selected")
    accepted = report.get("fixtures_accepted")
    if any(type(value) is not int or value < 0 for value in (batches, selected, accepted)) or accepted > selected:
        return "invalid_report", 0, 0
    if batches == 0 and selected == 0:
        return "empty", 0, 0
    if batches > 0 and selected > 0:
        return "processed", selected, accepted
    return "invalid_report", 0, 0


def _aggregate(state: dict[str, Any], max_scope_age_seconds: int) -> dict[str, Any]:
    if max_scope_age_seconds < 1:
        raise ValueError("scope age limit must be positive")
    now = datetime.now(timezone.utc)
    missing = sorted(league for league in state["leagues"] if str(league) not in state["last_completed"])
    stale: list[int] = []
    for league in state["leagues"]:
        completion = state["last_completed"].get(str(league))
        if completion is not None:
            try:
                age = (now - datetime.fromisoformat(completion["at"])).total_seconds()
            except (TypeError, ValueError, KeyError) as error:
                raise ValueError("invalid saved reconciliation completion time") from error
            if age < -30 or age > max_scope_age_seconds:
                stale.append(league)
    unresolved = sorted(int(key) for key in state["unresolved"])
    if unresolved or stale:
        aggregate_state = "degraded"
    elif missing or state["pending"] is not None:
        aggregate_state = "unknown"
    else:
        aggregate_state = "healthy"
    return {
        "state": aggregate_state, "epoch": state["epoch"],
        "release_id": state["release_id"], "observed_at": _now(),
        "leagues": state["leagues"], "missing_leagues": missing,
        "stale_leagues": stale, "unresolved_leagues": unresolved,
        "scope_max_age_seconds": max_scope_age_seconds,
        "empty_sweep": aggregate_state == "healthy" and state["empty_streak"] >= len(groups_for(state["leagues"], state["group_size"])),
        "empty_sweep_at": state["empty_sweep_at"],
    }


def finish_attempt(leagues: list[int], group_size: int, cursor_file: Path, release_id: str,
                   attempt_id: str, exit_status: int, report_file: Path,
                   max_scope_age_seconds: int = 43200) -> dict[str, Any]:
    state = _load(leagues, group_size, release_id, cursor_file)
    pending = state["pending"]
    if pending is None or pending["attempt_id"] != attempt_id or pending["release_id"] != release_id:
        raise ValueError("reconciliation attempt identity mismatch")
    if exit_status == 0:
        outcome, selected, accepted = _report_outcome(report_file, pending)
    else:
        outcome, selected, accepted = ("incomplete" if exit_status == 2 else "failed"), 0, 0
    if outcome in ("empty", "processed"):
        for league in pending["leagues"]:
            key = str(league)
            state["last_completed"][key] = {"at": _now(), "outcome": outcome,
                                             "selected": selected, "accepted": accepted,
                                             "attempt_id": attempt_id}
            state["unresolved"].pop(key, None)
        if outcome == "empty":
            state["empty_streak"] = min(state["empty_streak"] + 1, len(groups_for(leagues, group_size)))
            if state["empty_streak"] >= len(groups_for(leagues, group_size)):
                state["empty_sweep_at"] = _now()
        else:
            state["empty_streak"] = 0
            state["empty_sweep_at"] = None
    else:
        _mark_unresolved(state, pending, outcome)
    state["pending"] = None
    aggregate = _aggregate(state, max_scope_age_seconds)
    _save(state, cursor_file)
    return {
        "attempt_id": attempt_id, "scope": pending["leagues"], "outcome": outcome,
        "selected": selected, "accepted": accepted,
        "aggregate": aggregate,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("count", "next", "finish"))
    parser.add_argument("--leagues", required=True)
    parser.add_argument("--group-size", type=int, required=True)
    parser.add_argument("--cursor-file", type=Path)
    parser.add_argument("--release-id")
    parser.add_argument("--attempt-id")
    parser.add_argument("--exit-status", type=int)
    parser.add_argument("--report-file", type=Path)
    parser.add_argument("--max-scope-age-seconds", type=int, default=43200)
    args = parser.parse_args()
    try:
        leagues = parse_leagues(args.leagues)
        groups = groups_for(leagues, args.group_size)
        if args.action == "count":
            print(len(groups))
        else:
            if args.cursor_file is None or not args.release_id:
                parser.error("--cursor-file and --release-id are required")
            if args.action == "next":
                print(json.dumps(next_group(leagues, args.group_size, args.cursor_file, args.release_id)))
            else:
                if not args.attempt_id or args.exit_status is None or args.report_file is None:
                    parser.error("--attempt-id, --exit-status and --report-file are required")
                print(json.dumps(finish_attempt(leagues, args.group_size, args.cursor_file, args.release_id,
                                                args.attempt_id, args.exit_status, args.report_file,
                                                args.max_scope_age_seconds)))
    except (ValueError, OSError, json.JSONDecodeError) as error:
        parser.error(str(error))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
