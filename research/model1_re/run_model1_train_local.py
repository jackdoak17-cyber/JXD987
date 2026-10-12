from __future__ import annotations

import argparse
import csv
import json
from dataclasses import replace
from pathlib import Path

from psycopg2.extras import RealDictCursor

from .config import ARTIFACTS_DIR, get_settings
from .db_read import build_local_training_feature_rows, load_local_training_targets, readonly_connection
from .fit_model1_outcomes import write_fit_report


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
    parser = argparse.ArgumentParser(description="Train Model 1 locally from Supabase outcomes (no Statshub).")
    parser.add_argument("--days-back", type=int, default=180)
    parser.add_argument("--bookmaker-id", type=int, default=2)
    parser.add_argument("--min-odd", type=float, default=1.01)
    parser.add_argument("--max-odd", type=float, default=10.0)
    parser.add_argument("--limit", type=int, default=25000, help="Cap the number of training odds rows to load")
    parser.add_argument(
        "--all-appearances",
        action="store_true",
        help="Build player windows from all appearances (not starts-only)",
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    settings = get_settings()
    if args.all_appearances:
        settings = replace(settings, started_only=False)

    with readonly_connection(settings) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            targets = load_local_training_targets(
                cur,
                days_back=args.days_back,
                bookmaker_id=args.bookmaker_id,
                min_odd=args.min_odd,
                max_odd=args.max_odd,
                limit=args.limit,
            )
            print(f"local_training_targets_loaded={len(targets)}", flush=True)
            rows = build_local_training_feature_rows(cur, targets, settings=settings, verbose=args.verbose)
            print(f"local_training_feature_rows_built={len(rows)}", flush=True)

    stem = "model1_local"
    dataset_json_path = ARTIFACTS_DIR / f"{stem}_dataset.json"
    dataset_csv_path = ARTIFACTS_DIR / f"{stem}_dataset.csv"
    dataset_json_path.write_text(json.dumps(rows, indent=2, default=str))
    write_csv(rows, dataset_csv_path)

    report = write_fit_report(rows, stem, settings=settings)
    print("fit_complete=true", flush=True)
    print(
        json.dumps(
            {
                "targets": len(targets),
                "feature_rows": len(rows),
                "dataset_csv": str(dataset_csv_path),
                "report_json": str((ARTIFACTS_DIR.parent / "reports" / f"{stem}_fit_report.json").resolve()),
                "report": report,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
