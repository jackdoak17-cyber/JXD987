#!/usr/bin/env python3
"""Manage the durable dirty state for fixture-delivery publication."""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DEFAULT_STATE_PATH = "/var/lib/oddssearch/fixture-delivery/dirty-state.json"
UTC = timezone.utc


def now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def state_path(raw: str | None = None) -> Path:
    return Path(raw or os.environ.get("FIXTURE_DELIVERY_DIRTY_STATE_PATH") or DEFAULT_STATE_PATH)


@contextlib.contextmanager
def locked(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_suffix(path.suffix + ".lock")
    with lock_path.open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def read_state(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"dirty state is not an object: {path}")
    return payload


def write_state(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, path)
    finally:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(tmp_name)


def remove_state(path: Path) -> None:
    with contextlib.suppress(FileNotFoundError):
        path.unlink()


def load_json_file(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    candidate = Path(path)
    if not candidate.exists():
        return {}
    with candidate.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return payload if isinstance(payload, dict) else {}


def int_value(payload: dict[str, Any], key: str) -> int:
    try:
        return int(payload.get(key) or 0)
    except (TypeError, ValueError):
        return 0


def fixture_delivery_relevant_change(reconcile: dict[str, Any], export: dict[str, Any]) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    reconciled = int_value(reconcile, "fixtures_reconciled")
    if reconciled > 0:
        reasons.append(f"fixtures_reconciled={reconciled}")
    exported = int_value(export, "fixtures_exported")
    if exported > 0:
        reasons.append(f"fixtures_exported={exported}")
    dropped = int_value(export, "fixtures_dropped_missing_teams")
    if dropped > 0:
        reasons.append(f"fixtures_dropped_missing_teams={dropped}")
    return bool(reasons), reasons


def mark(path: Path, reason: str, source: str, metadata: dict[str, Any]) -> dict[str, Any]:
    previous = read_state(path)
    timestamp = now_iso()
    payload = {
        "dirty": True,
        "first_marked_at": previous.get("first_marked_at") if previous else timestamp,
        "last_marked_at": timestamp,
        "mark_count": int(previous.get("mark_count", 0)) + 1 if previous else 1,
        "reason": reason,
        "source": source,
        "metadata": metadata,
    }
    if previous and previous.get("last_publish_attempt_at"):
        payload["last_publish_attempt_at"] = previous["last_publish_attempt_at"]
    if previous and previous.get("last_publish_error"):
        payload["last_publish_error"] = previous["last_publish_error"]
    write_state(path, payload)
    return payload


def note_attempt(path: Path) -> dict[str, Any] | None:
    payload = read_state(path)
    if not payload:
        return None
    payload["last_publish_attempt_at"] = now_iso()
    write_state(path, payload)
    return payload


def note_failure(path: Path, error: str) -> dict[str, Any] | None:
    payload = read_state(path)
    if not payload:
        return None
    payload["last_publish_error"] = error[-1000:]
    payload["last_publish_failed_at"] = now_iso()
    write_state(path, payload)
    return payload


def command_from_reports(args: argparse.Namespace) -> int:
    path = state_path(args.state_path)
    reconcile = load_json_file(args.reconcile_report)
    export = load_json_file(args.export_report)
    changed, reasons = fixture_delivery_relevant_change(reconcile, export)
    report = {
        "changed": changed,
        "reasons": reasons,
        "state_path": str(path),
    }
    if changed:
        metadata = {
            "reconcile_report": reconcile,
            "export_report": export,
        }
        with locked(path):
            report["state"] = mark(path, "; ".join(reasons), args.source, metadata)
    if args.report_out:
        write_state(Path(args.report_out), report)
    print(json.dumps(report, sort_keys=True))
    return 0


def command_status(args: argparse.Namespace) -> int:
    path = state_path(args.state_path)
    with locked(path):
        payload = read_state(path)
    print(json.dumps({"dirty": bool(payload), "state_path": str(path), "state": payload}, sort_keys=True))
    return 0 if payload else 2


def command_clear(args: argparse.Namespace) -> int:
    path = state_path(args.state_path)
    with locked(path):
        remove_state(path)
    print(json.dumps({"dirty": False, "cleared": True, "state_path": str(path)}, sort_keys=True))
    return 0


def command_attempt(args: argparse.Namespace) -> int:
    path = state_path(args.state_path)
    with locked(path):
        payload = note_attempt(path)
    print(json.dumps({"dirty": bool(payload), "state_path": str(path), "state": payload}, sort_keys=True))
    return 0 if payload else 2


def command_failure(args: argparse.Namespace) -> int:
    path = state_path(args.state_path)
    with locked(path):
        payload = note_failure(path, args.error)
    print(json.dumps({"dirty": bool(payload), "state_path": str(path), "state": payload}, sort_keys=True))
    return 0 if payload else 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-path", default=None)
    sub = parser.add_subparsers(dest="command", required=True)

    from_reports = sub.add_parser("mark-from-reports")
    from_reports.add_argument("--reconcile-report", required=True)
    from_reports.add_argument("--export-report", required=True)
    from_reports.add_argument("--source", default="postmatch-settlement")
    from_reports.add_argument("--report-out", default=None)
    from_reports.set_defaults(func=command_from_reports)

    status = sub.add_parser("status")
    status.set_defaults(func=command_status)

    clear = sub.add_parser("clear")
    clear.set_defaults(func=command_clear)

    attempt = sub.add_parser("attempt")
    attempt.set_defaults(func=command_attempt)

    failure = sub.add_parser("failure")
    failure.add_argument("--error", required=True)
    failure.set_defaults(func=command_failure)

    return parser


def main() -> int:
    args = build_parser().parse_args()
    return int(args.func(args) or 0)


if __name__ == "__main__":
    raise SystemExit(main())
