#!/usr/bin/env python3
"""Rotate small stats-reconciliation league scopes on the existing worker."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from pathlib import Path


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


def next_group(leagues: list[int], group_size: int, cursor_file: Path) -> list[int]:
    groups = groups_for(leagues, group_size)
    signature = hashlib.sha256(
        json.dumps([leagues, group_size], separators=(",", ":")).encode("ascii")
    ).hexdigest()
    index = 0
    if cursor_file.exists():
        state = json.loads(cursor_file.read_text(encoding="utf-8"))
        if not isinstance(state, dict) or state.get("version") != 1:
            raise ValueError("invalid reconciliation league cursor")
        if state.get("signature") == signature:
            saved_index = state.get("next_group")
            if type(saved_index) is not int or not 0 <= saved_index < len(groups):
                raise ValueError("invalid reconciliation league cursor index")
            index = saved_index

    cursor_file.parent.mkdir(parents=True, exist_ok=True)
    payload = {"version": 1, "signature": signature, "next_group": (index + 1) % len(groups)}
    temp_name: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=cursor_file.parent,
            prefix=cursor_file.name + ".", delete=False,
        ) as handle:
            temp_name = handle.name
            os.fchmod(handle.fileno(), 0o600)
            json.dump(payload, handle, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, cursor_file)
    finally:
        if temp_name and os.path.exists(temp_name):
            os.unlink(temp_name)
    return groups[index]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("count", "next"))
    parser.add_argument("--leagues", required=True)
    parser.add_argument("--group-size", type=int, required=True)
    parser.add_argument("--cursor-file", type=Path)
    args = parser.parse_args()
    try:
        leagues = parse_leagues(args.leagues)
        groups = groups_for(leagues, args.group_size)
        if args.action == "count":
            print(len(groups))
        else:
            if args.cursor_file is None:
                parser.error("--cursor-file is required for next")
            print(",".join(str(value) for value in next_group(leagues, args.group_size, args.cursor_file)))
    except (ValueError, OSError, json.JSONDecodeError) as error:
        parser.error(str(error))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
