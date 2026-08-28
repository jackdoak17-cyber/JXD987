"""Pure per-fixture delivery policy shared by batch and single-fixture workers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Iterable


RETRYABLE_DELIVERY_STATUSES = frozenset(
    {
        "provider_pending",
        "failed",
        "export_failed",
        "verification_failed",
        "projection_failed",
    }
)

WEBSITE_PENDING_DELIVERY_STATUSES = frozenset(
    {
        "new",
        "running",
        *RETRYABLE_DELIVERY_STATUSES,
    }
)


@dataclass(frozen=True)
class FixtureSafetyDecision:
    fixture_id: int
    exportable: bool
    reason_code: str
    reason: str


@dataclass(frozen=True)
class FixtureExportPartition:
    exportable: tuple[dict[str, Any], ...]
    unsafe: tuple[dict[str, Any], ...]
    decisions: dict[int, FixtureSafetyDecision]

    @property
    def exportable_ids(self) -> tuple[int, ...]:
        return tuple(int(candidate["fixture_id"]) for candidate in self.exportable)

    @property
    def unsafe_ids(self) -> tuple[int, ...]:
        return tuple(int(candidate["fixture_id"]) for candidate in self.unsafe)


def _value(candidate: dict[str, Any], key: str, default: Any = None) -> Any:
    return candidate.get(key, default)


def _team_ids(assessment: Any) -> tuple[str, ...]:
    values: set[str] = set()
    for field in ("team_stat_types", "lineup_counts", "player_stat_counts"):
        values.update(str(team_id) for team_id in getattr(assessment, field, {}) or {})
    return tuple(sorted(values))


def _count_for_team(snapshot: Any, field: str, team_id: str) -> int:
    values = getattr(snapshot, field, {}) or {}
    if field == "team_stat_types":
        return len(values.get(team_id, ()) or ())
    if field == "lineup_values":
        return sum(1 for key in values if key.endswith(f":{team_id}"))
    if field == "player_stat_values":
        return sum(1 for key in values if len(key.split(":")) >= 2 and key.split(":")[1] == team_id)
    return 0


def classify_fixture_candidate(candidate: dict[str, Any]) -> FixtureSafetyDecision:
    """Classify one accepted candidate before any multi-fixture export starts."""
    fixture_id = int(candidate["fixture_id"])
    assessment = _value(candidate, "assessment")
    source = _value(candidate, "source")
    if assessment is None:
        return FixtureSafetyDecision(fixture_id, False, "provider_pending", "provider assessment is missing")

    status = str(getattr(assessment, "status", "provider_pending"))
    if status == "provider_pending":
        return FixtureSafetyDecision(
            fixture_id,
            False,
            "provider_pending",
            str(getattr(assessment, "error", None) or "provider detail is not complete"),
        )
    if status not in {"ready", "provider_sparse"}:
        return FixtureSafetyDecision(
            fixture_id,
            False,
            status or "provider_pending",
            f"provider assessment status is not exportable: {status or 'unknown'}",
        )
    if status == "provider_sparse" and int(_value(candidate, "stable_count", 0) or 0) < 2:
        return FixtureSafetyDecision(
            fixture_id,
            False,
            "provider_pending",
            "provider-sparse detail requires two stable confirmations",
        )
    if source is None:
        return FixtureSafetyDecision(fixture_id, False, "source_incomplete", "source detail snapshot is missing")

    if int(getattr(source, "lineup_count", 0) or 0) <= 0:
        return FixtureSafetyDecision(fixture_id, False, "source_incomplete", "source lineup detail is empty")
    if int(getattr(source, "player_stat_count", 0) or 0) <= 0:
        return FixtureSafetyDecision(fixture_id, False, "source_incomplete", "source player-stat detail is empty")

    team_ids = _team_ids(assessment)
    if len(team_ids) < 2:
        return FixtureSafetyDecision(fixture_id, False, "source_incomplete", "provider assessment has fewer than two teams")
    for team_id in team_ids:
        if _count_for_team(source, "lineup_values", team_id) <= 0:
            return FixtureSafetyDecision(fixture_id, False, "source_incomplete", f"source lineup detail is empty for team {team_id}")
        if _count_for_team(source, "player_stat_values", team_id) <= 0:
            return FixtureSafetyDecision(fixture_id, False, "source_incomplete", f"source player-stat detail is empty for team {team_id}")

    return FixtureSafetyDecision(fixture_id, True, "ready", "source detail is structurally safe to export")


def partition_export_candidates(candidates: Iterable[dict[str, Any]]) -> FixtureExportPartition:
    exportable: list[dict[str, Any]] = []
    unsafe: list[dict[str, Any]] = []
    decisions: dict[int, FixtureSafetyDecision] = {}
    for candidate in candidates:
        decision = classify_fixture_candidate(candidate)
        fixture_id = int(candidate["fixture_id"])
        decisions[fixture_id] = decision
        (exportable if decision.exportable else unsafe).append(candidate)
    return FixtureExportPartition(tuple(exportable), tuple(unsafe), decisions)


def _parse_timestamp(value: str | datetime | None) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def retryable_delivery_status_is_due(
    *,
    status: str,
    accepted_snapshot_id: int | None,
    next_attempt_at: str | datetime | None,
    next_revalidation_at: str | datetime | None,
    now: datetime,
) -> bool:
    """Return whether a retryable delivery row is due, regardless of snapshot history."""
    del accepted_snapshot_id, next_revalidation_at
    if status not in RETRYABLE_DELIVERY_STATUSES:
        return False
    due_at = _parse_timestamp(next_attempt_at)
    current = now if now.tzinfo else now.replace(tzinfo=timezone.utc)
    return due_at is None or due_at <= current


def apply_export_outcome(
    candidates: Iterable[dict[str, Any]],
    exporter: Callable[[list[int]], Any],
) -> tuple[tuple[int, ...], tuple[int, ...], tuple[int, ...]]:
    """Small pure adapter used by tests and callers that need safe partitioning."""
    partition = partition_export_candidates(candidates)
    if not partition.exportable:
        return (), partition.unsafe_ids, partition.unsafe_ids
    result = exporter(list(partition.exportable_ids))
    exported_ids = tuple(int(value) for value in getattr(result, "fixture_ids", ()) if getattr(result, "success", False))
    if getattr(result, "returncode", 0) != 0 or not getattr(result, "success", True):
        exported_ids = ()
        retryable_ids = partition.exportable_ids + partition.unsafe_ids
    else:
        retryable_ids = partition.unsafe_ids
    return exported_ids, retryable_ids, partition.unsafe_ids
