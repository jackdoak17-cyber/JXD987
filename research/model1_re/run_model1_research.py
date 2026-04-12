from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

from psycopg2.extras import RealDictCursor

from .config import ARTIFACTS_DIR, get_settings
from .db_read import build_feature_rows, match_public_targets, readonly_connection
from .fit_model1 import write_fit_report
from .public_targets import fetch_model1_targets


def write_csv(rows: list[dict], path: Path) -> None:
    if not rows:
        path.write_text("")
        return
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Reverse-engineer Statshub Model 1 in an isolated workspace.")
    parser.add_argument("--date", default="today", help="Public API date filter, e.g. today or all")
    parser.add_argument("--max-pages", type=int, default=None, help="Optional page cap for target fetch")
    parser.add_argument(
        "--include-started",
        action="store_true",
        help="Include targets whose fixture had already started at fetch time",
    )
    args = parser.parse_args()

    settings = get_settings()
    targets = fetch_model1_targets(
        date_filter=args.date,
        max_pages=args.max_pages,
        future_only=not args.include_started,
    )
    print(f"targets_fetched={len(targets)}", flush=True)
    with readonly_connection(settings) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            matched_targets = match_public_targets(cur, targets)
            print(f"targets_matched={len(matched_targets)}", flush=True)
            feature_rows = build_feature_rows(cur, matched_targets, settings, verbose=True)
            print(f"feature_rows_built={len(feature_rows)}", flush=True)

    stem = f"model1_{args.date or 'all'}"
    matched_path = ARTIFACTS_DIR / f"{stem}_matched_targets.json"
    dataset_json_path = ARTIFACTS_DIR / f"{stem}_dataset.json"
    dataset_csv_path = ARTIFACTS_DIR / f"{stem}_dataset.csv"

    matched_path.write_text(json.dumps([target.__dict__ for target in matched_targets], indent=2, default=str))
    dataset_json_path.write_text(json.dumps(feature_rows, indent=2, default=str))
    write_csv(feature_rows, dataset_csv_path)

    report = write_fit_report(feature_rows, stem)
    print("fit_complete=true", flush=True)
    summary = {
        "targets_fetched": len(targets),
        "targets_matched": len(matched_targets),
        "feature_rows": len(feature_rows),
        "dataset_csv": str(dataset_csv_path),
        "report_json": str((ARTIFACTS_DIR.parent / "reports" / f"{stem}_fit_report.json").resolve()),
        "report": report,
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
