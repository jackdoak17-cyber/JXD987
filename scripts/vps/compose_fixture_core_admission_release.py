#!/usr/bin/env python3
"""Compose the reviewed admission protocol into a known immutable release.

This is an offline staging helper. It never connects to a host or copies a
runtime manifest; the caller must rebuild and verify that manifest afterward.
"""

from __future__ import annotations

import argparse
import hashlib
import re
from pathlib import Path


KNOWN_BASELINES = {
    # Active odds matcher, whose live-settlement guard is always enabled.
    "805ad500057338fd9e24f4174a1d13ae5cd49199": "matcher",
    # Active fixture core, website odds delivery and squad reconciliation.
    "fab07d5e8b5f8735eddc53e0412ebbc2a5042384": "normal",
    # Active long-lived leaderboard stats reconciliation supervisor.
    "4b9e4af514ede779eb21f088aa411b38ab3afd76": "normal",
    # Locally composed, verified #80 fixture-core release candidate.
    "4e654c04de7512e8e40ed39346745ed1a5401e7d": "normal",
}

FUNCTIONS = (
    "run_recorded_pipeline_job",
    "run_with_global_lock_and_retry",
    "run_with_global_lock_and_timeout",
)


def function_pattern(name: str) -> re.Pattern[str]:
    return re.compile(rf"(?ms)^{re.escape(name)}\(\) \{{\n.*?^\}}\n")


def only_function(source: str, name: str) -> str:
    matches = list(function_pattern(name).finditer(source))
    if len(matches) != 1:
        raise ValueError(f"expected exactly one {name}, found {len(matches)}")
    return matches[0].group()


def compose(base: str, current: str, kind: str) -> str:
    if "fixture_core_reservation_file()" in base:
        raise ValueError("baseline already contains a reservation protocol")
    for name in FUNCTIONS:
        replacement = only_function(current, name)
        if name == "run_with_global_lock_and_timeout" and kind == "matcher":
            # The existing matcher always enforces the live-settlement guard.
            replacement = replacement.replace(
                'local live_schedule_enabled="${ODDS_SYNC_LIVE_SCHEDULE_ENABLED:-true}"',
                "local live_schedule_enabled=true",
            )
        base = function_pattern(name).sub(lambda _: replacement, base, count=1)
    reservation_start = current.index("# The fixture-core source must be admitted")
    reservation_end = current.index("run_with_global_lock_and_timeout() {", reservation_start)
    reservation_helpers = current[reservation_start:reservation_end]
    marker = "run_with_global_lock_and_timeout() {"
    if base.count(marker) != 1:
        raise ValueError("global writer function missing or duplicated")
    return base.replace(marker, reservation_helpers + marker, 1)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-common", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    source_bytes = args.baseline_common.read_bytes()
    digest = hashlib.sha1(source_bytes).hexdigest()
    kind = KNOWN_BASELINES.get(digest)
    if not kind:
        raise SystemExit(f"unreviewed common.sh baseline SHA-1: {digest}")
    current = Path(__file__).with_name("common.sh").read_text()
    output = compose(source_bytes.decode(), current, kind)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(output)
    print(f"baseline={digest} kind={kind} output_sha1={hashlib.sha1(output.encode()).hexdigest()}")


if __name__ == "__main__":
    main()
