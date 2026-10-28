"""Opt-in transactional capture hook for the player leaderboard shadow model."""

from __future__ import annotations

import os
from collections.abc import Iterable
from typing import Any


def capture_enabled() -> bool:
    return os.environ.get("LEADERBOARD_SHADOW_CAPTURE_ENABLED", "").strip().lower() in {
        "1", "true", "yes", "on",
    }


def capture_fixture_ids(cur: Any, fixture_ids: Iterable[int]) -> None:
    """Capture canonical source state inside the caller's open transaction.

    The feature is inert unless explicitly enabled. Once enabled, a capture
    failure is intentionally fatal so authoritative source writes and their
    journal evidence cannot commit independently.
    """

    if not capture_enabled():
        return
    ids = sorted({int(fixture_id) for fixture_id in fixture_ids})
    if not ids:
        return
    cur.execute(
        "select leaderboard_shadow_v1.capture_fixture_source_changes(%s::bigint[])",
        (ids,),
    )
    cur.fetchone()


def capture_fixture_id(cur: Any, fixture_id: int) -> None:
    capture_fixture_ids(cur, [fixture_id])
