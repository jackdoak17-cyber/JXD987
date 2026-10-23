#!/usr/bin/env python3
"""Bounded, aggregate-only provider-fetch history for the signed Odds matcher.

These counters describe *actual provider responses*, not committed or verified
canonical prices. Supabase quote-match coverage is a separate downstream metric.
This module never calls a provider or writes odds.
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
from typing import Any, Iterable, Mapping

TIERS = frozenset({"p1", "p2", "p3"})
RETENTION_DAYS = 30


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_utc(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def summarize_fetch(
    *,
    run_id: str,
    tier: str,
    fixtures_in_scope: int,
    coverage: Iterable[Mapping[str, Any]],
    observations: Iterable[Mapping[str, Any]],
    api_calls: int,
    errors: Iterable[str],
) -> dict[str, Any]:
    """Explicitly distinguish missing provider evidence from downstream joins."""
    if tier not in TIERS or not run_id.startswith(f"{tier}-"):
        raise ValueError("invalid tier or source run id")
    rows = list(coverage)
    quote_rows = list(observations)
    failures = list(errors)
    statuses = Counter(str(row.get("odds_response_status") or "not_reported") for row in rows)
    matching = Counter(str(row.get("matching_status") or "not_reported") for row in rows)
    missing_side_count = 0
    for row in rows:
        if row.get("matching_status") != "matched":
            continue
        sides_by_book = row.get("moneyline_sides_by_bookmaker") or {}
        # Count absent 1/X/2 sides only for bookmakers actually present in
        # the provider response; a missing bookmaker is not a returned price.
        for sides in sides_by_book.values():
            if isinstance(sides, (list, tuple)):
                missing_side_count += len({"home", "draw", "away"} - set(sides))
    return {
        "schema_version": 1,
        "source": "provider_fetch_observation_summary",
        "run_id": run_id,
        "tier": tier,
        "recorded_at": _utc_now().isoformat(),
        "provider_fetch_status": "partial_failure" if failures else "completed",
        "error_count": len(failures),
        "errors": [str(e)[:300] for e in failures[:10]],
        "fixtures_in_scope": int(fixtures_in_scope),
        "fixtures_with_coverage_evidence": len(rows),
        "matching_status_counts": dict(matching),
        "response_status_counts": dict(statuses),
        "returned_moneyline_side_gaps": missing_side_count,
        "normalized_observations": len(quote_rows),
        "api_calls": int(api_calls),
        # Not known until a separate canonical-quote database match. Never
        # use these observation counts to claim customer-price verification.
        "canonical_quotes_matched": None,
        "canonical_quotes_unmatched": None,
    }


def write_run_metric(directory: Path, record: Mapping[str, Any], *, now: datetime | None = None) -> Path:
    """Persist one immutable run file atomically; prune only our old metric files."""
    now = now or _utc_now()
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    run_id = str(record["run_id"])
    if not run_id or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-" for c in run_id):
        raise ValueError("unsafe run id")
    target = directory / f"{run_id}.json"
    if target.exists():
        existing = json.loads(target.read_text(encoding="utf-8"))
        if existing != record:
            raise ValueError("conflicting metrics for existing run id")
        return target
    temporary = directory / f".{run_id}.{os.getpid()}.tmp"
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(record, handle, sort_keys=True, separators=(",", ":"))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)
    cutoff = now - timedelta(days=RETENTION_DAYS)
    for file in directory.glob("*.json"):
        if datetime.fromtimestamp(file.stat().st_mtime, timezone.utc) < cutoff:
            file.unlink()
    return target


def summarize_history(directory: Path, *, days: int = 14, now: datetime | None = None) -> dict[str, Any]:
    if days < 1 or days > RETENTION_DAYS:
        raise ValueError("history window must be 1-30 days")
    cutoff = (now or _utc_now()) - timedelta(days=days)
    by_tier: dict[str, dict[str, int]] = {
        tier: {"runs": 0, "partial_failures": 0, "zero_observation_runs": 0,
               "normalized_observations": 0, "fixtures_in_scope": 0,
               "fixtures_missing_event": 0, "api_calls": 0}
        for tier in ("p1", "p2", "p3")
    }
    if directory.exists():
        for path in directory.glob("*.json"):
            event = json.loads(path.read_text(encoding="utf-8"))
            if event.get("source") != "provider_fetch_observation_summary":
                continue
            tier = event.get("tier")
            if tier not in by_tier or _parse_utc(event["recorded_at"]) < cutoff:
                continue
            bucket = by_tier[tier]
            bucket["runs"] += 1
            bucket["partial_failures"] += int(event["provider_fetch_status"] == "partial_failure")
            bucket["zero_observation_runs"] += int(event["normalized_observations"] == 0)
            bucket["normalized_observations"] += int(event["normalized_observations"])
            bucket["fixtures_in_scope"] += int(event["fixtures_in_scope"])
            statuses = event.get("matching_status_counts") or {}
            bucket["fixtures_missing_event"] += sum(int(count) for status, count in statuses.items() if status != "matched")
            bucket["api_calls"] += int(event["api_calls"])
    return {"window_days": days, "scope": "provider_fetch_only_not_canonical_verification", "tiers": by_tier}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["summary"])
    parser.add_argument("--directory", required=True)
    parser.add_argument("--days", type=int, default=14)
    args = parser.parse_args()
    print(json.dumps(summarize_history(Path(args.directory), days=args.days), sort_keys=True))


if __name__ == "__main__":
    main()
