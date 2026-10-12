from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from dataclasses import replace

from psycopg2.extras import RealDictCursor

from .config import ARTIFACTS_DIR, get_settings
from .db_read import build_feature_rows, match_public_targets, readonly_connection
from .fit_model1 import write_fit_report
from .public_targets import PublicTarget, fetch_model1_targets


def _date_strings(days_back: int) -> list[str]:
    now = datetime.now(timezone.utc).date()
    return [(now - timedelta(days=offset)).isoformat() for offset in range(days_back, -1, -1)]


def write_csv(rows: list[dict], path: Path) -> None:
    if not rows:
        path.write_text("")
        return
    fieldnames = sorted({key for row in rows for key in row.keys()})
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reverse-engineer Statshub Model 1 using a date range sample (reduces overEdge-only sampling bias)."
    )
    parser.add_argument("--days-back", type=int, default=10, help="How many days back to sample (inclusive)")
    parser.add_argument("--pages-per-day", type=int, default=6, help="Max pages to fetch for each day")
    parser.add_argument("--sort-by", default="matchDate", help="Public API sort key (default: matchDate)")
    parser.add_argument("--sort-order", default="asc", help="Public API sort order (default: asc)")
    parser.add_argument("--max-odd", type=float, default=4.0)
    parser.add_argument(
        "--sot-plus-goals",
        action="store_true",
        help="Experiment: treat SOT stat as (shots on target + goals) when building windows/conceded stats",
    )
    parser.add_argument(
        "--sot-use-shots-total",
        action="store_true",
        help="Experiment: treat SOT market windows as total shots windows (tests provider/stat mapping mismatch)",
    )
    parser.add_argument(
        "--sot-dual-features",
        action="store_true",
        help="Experiment: for SOT rows, also compute a second feature set derived from shots (prefixed with shots_*)",
    )
    parser.add_argument(
        "--all-appearances",
        action="store_true",
        help="Build player windows from all appearances (not starts-only)",
    )
    parser.add_argument(
        "--force-statshub-windows",
        action="store_true",
        help="Force player_window=60 and opponent_window=60 during parameter search (matches Statshub lastXMatches=60)",
    )
    parser.add_argument(
        "--include-started",
        action="store_true",
        help="Include rows whose fixture had already started at fetch time",
    )
    args = parser.parse_args()

    settings = get_settings()
    if args.all_appearances:
        settings = replace(settings, started_only=False)
    if args.sot_plus_goals:
        settings = replace(settings, sot_plus_goals=True)
    if args.sot_use_shots_total:
        settings = replace(settings, sot_use_shots_total=True)
    if args.sot_dual_features:
        settings = replace(settings, sot_dual_features=True)
    if args.force_statshub_windows:
        settings = replace(settings, force_statshub_windows=True)
    all_targets: list[PublicTarget] = []
    for date_str in _date_strings(args.days_back):
        targets = fetch_model1_targets(
            date_filter=date_str,
            max_pages=args.pages_per_day,
            future_only=not args.include_started,
            sort_by=args.sort_by,
            sort_order=args.sort_order,
            max_odd=args.max_odd,
        )
        all_targets.extend(targets)
        print(f"date={date_str} targets_fetched={len(targets)}", flush=True)

    # Dedupe by group key.
    deduped: dict[str, PublicTarget] = {t.public_group_key: t for t in all_targets}
    targets = list(deduped.values())
    print(f"targets_total={len(all_targets)} targets_deduped={len(targets)}", flush=True)

    with readonly_connection(settings) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            matched_targets = match_public_targets(cur, targets)
            print(f"targets_matched={len(matched_targets)}", flush=True)
            feature_rows = build_feature_rows(cur, matched_targets, settings, verbose=True)
            print(f"feature_rows_built={len(feature_rows)}", flush=True)

    stem = f"model1_range_{args.days_back}d"
    matched_path = ARTIFACTS_DIR / f"{stem}_matched_targets.json"
    dataset_json_path = ARTIFACTS_DIR / f"{stem}_dataset.json"
    dataset_csv_path = ARTIFACTS_DIR / f"{stem}_dataset.csv"

    matched_path.write_text(json.dumps([target.__dict__ for target in matched_targets], indent=2, default=str))
    dataset_json_path.write_text(json.dumps(feature_rows, indent=2, default=str))
    write_csv(feature_rows, dataset_csv_path)

    report = write_fit_report(feature_rows, stem, settings=settings)
    print("fit_complete=true", flush=True)
    print(
        json.dumps(
            {
                "targets_fetched": len(all_targets),
                "targets_deduped": len(targets),
                "targets_matched": len(matched_targets),
                "feature_rows": len(feature_rows),
                "dataset_csv": str(dataset_csv_path),
                "report_json": str((ARTIFACTS_DIR.parent / "reports" / f"{stem}_fit_report.json").resolve()),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
