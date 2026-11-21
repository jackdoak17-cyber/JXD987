"""Decide whether a staged P3 provider report can survive publication and validation."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path


def reusable(
    report_path: Path,
    *,
    max_age_hours: float,
    remaining_runtime_seconds: int,
    safety_seconds: int,
    now: datetime | None = None,
) -> bool:
    if max_age_hours <= 0 or remaining_runtime_seconds < 0 or safety_seconds < 0:
        return False
    try:
        report = json.loads(report_path.read_text(encoding="utf-8"))
        generated_at = datetime.fromisoformat(report["generated_at"].replace("Z", "+00:00"))
    except (OSError, ValueError, TypeError, KeyError, AttributeError):
        return False
    if generated_at.tzinfo is None:
        return False
    observed_at = now or datetime.now(timezone.utc)
    age_seconds = (observed_at - generated_at).total_seconds()
    # Allow two minutes of clock skew, but never reuse evidence that can age out
    # before the bounded pipeline reaches its provider-aware moneyline guard.
    return -120 <= age_seconds and (
        age_seconds + remaining_runtime_seconds + safety_seconds < max_age_hours * 3600
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--max-age-hours", type=float, required=True)
    parser.add_argument("--remaining-runtime-seconds", type=int, required=True)
    parser.add_argument("--safety-seconds", type=int, default=60)
    args = parser.parse_args()
    return 0 if reusable(
        args.report,
        max_age_hours=args.max_age_hours,
        remaining_runtime_seconds=args.remaining_runtime_seconds,
        safety_seconds=args.safety_seconds,
    ) else 1


if __name__ == "__main__":
    raise SystemExit(main())
