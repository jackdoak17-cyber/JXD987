from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path

from psycopg2.extras import RealDictCursor

from .config import ARTIFACTS_DIR, REPORTS_DIR, get_settings
from .db_read import build_upcoming_feature_rows, load_upcoming_over_candidates, readonly_connection
from .formula import FormulaParams, predict_fair_odds


def write_csv(rows: list[dict], path: Path) -> None:
    if not rows:
        path.write_text("")
        return
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def load_fit_params(report_path: Path) -> dict[str, dict[str, FormulaParams]]:
    payload = json.loads(report_path.read_text())
    return {
        "shots": {
            "global": FormulaParams(**payload["shots"]["params"]),
            "line": {line: FormulaParams(**params) for line, params in payload["shots"].get("line_params", {}).items()},
        },
        "onTargetScoringAttempt": {
            "global": FormulaParams(**payload["onTargetScoringAttempt"]["params"]),
            "line": {
                line: FormulaParams(**params)
                for line, params in payload["onTargetScoringAttempt"].get("line_params", {}).items()
            },
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate future Model 1 candidates without Statshub.")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--bookmaker-id", type=int, default=2)
    parser.add_argument("--min-edge", type=float, default=0.0)
    parser.add_argument("--verbose", action="store_true", help="Print progress while building feature rows")
    parser.add_argument(
        "--fit-report",
        default=str(REPORTS_DIR / "model1_all_fit_report.json"),
        help="Path to a fit report produced by run_model1_research.py",
    )
    args = parser.parse_args()

    settings = get_settings()
    fit_params = load_fit_params(Path(args.fit_report))

    with readonly_connection(settings) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            candidates = load_upcoming_over_candidates(cur, days=args.days, bookmaker_id=args.bookmaker_id)
            print(f"upcoming_candidates_loaded={len(candidates)}", flush=True)
            generation_cutoff = datetime.now(timezone.utc)
            feature_rows = build_upcoming_feature_rows(
                cur,
                candidates,
                settings,
                cutoff_timestamp=generation_cutoff,
                verbose=args.verbose,
            )
            print(f"upcoming_feature_rows_built={len(feature_rows)}", flush=True)

    generated: list[dict] = []
    for row in feature_rows:
        market_bundle = fit_params[row["public_market_type"]]
        line_key = f"{float(row['public_line']):.1f}"
        params = market_bundle["line"].get(line_key, market_bundle["global"])
        prediction = predict_fair_odds(row, params)
        fair_over = prediction["fair_over_odds"]
        if fair_over is None:
            continue
        edge_pct = ((row["best_over_odds"] - fair_over) / fair_over) * 100.0
        if edge_pct < args.min_edge:
            continue
        selected_player_key = f"player_{params.player_source}_{params.player_window}"
        generated.append(
            {
                "fixture_timestamp": row["fixture_timestamp"],
                "home_team_name": row["home_team_name"],
                "away_team_name": row["away_team_name"],
                "player_name_db": row["player_name_db"],
                "market_type": row["public_market_type"],
                "line": row["public_line"],
                "best_over_odds": row["best_over_odds"],
                "predicted_mu": prediction["mu"],
                "predicted_fair_over_odds": fair_over,
                "predicted_fair_under_odds": prediction["fair_under_odds"],
                "predicted_over_edge_pct": edge_pct,
                "selected_player_window": params.player_window,
                "selected_player_source": params.player_source,
                "selected_opponent_window": params.opponent_window,
                "selected_ratio_source": params.ratio_source,
                "selected_distribution": params.distribution,
                "selected_player_baseline": row.get(selected_player_key),
                "player_avg_60": row["player_avg_60"],
                "player_games_60": row.get("player_games_60"),
                "player_hits_60": row.get("player_hits_60"),
                "player_hit_rate_60": row.get("player_hit_rate_60"),
                "player_per90_60": row.get("player_per90_60"),
                "opp_ratio_10": row.get("opp_ratio_10"),
                "opp_ratio_20": row.get("opp_ratio_20"),
                "opp_ratio_40": row.get("opp_ratio_40"),
                "opp_ratio_60": row.get("opp_ratio_60"),
                "opp_hit_ratio_10": row.get("opp_hit_ratio_10"),
                "opp_hit_ratio_20": row.get("opp_hit_ratio_20"),
                "opp_hit_ratio_40": row.get("opp_hit_ratio_40"),
                "opp_hit_ratio_60": row.get("opp_hit_ratio_60"),
                "resolution_source": row["resolution_source"],
                "selection_key": row["selection_key"],
            }
        )

    generated.sort(key=lambda row: row["predicted_over_edge_pct"], reverse=True)
    stem = f"model1_future_{args.days}d"
    csv_path = ARTIFACTS_DIR / f"{stem}.csv"
    json_path = ARTIFACTS_DIR / f"{stem}.json"
    write_csv(generated, csv_path)
    json_path.write_text(json.dumps(generated, indent=2, default=str))
    print(
        json.dumps(
            {
                "candidates_loaded": len(candidates),
                "feature_rows": len(feature_rows),
                "generated_rows": len(generated),
                "csv_path": str(csv_path),
                "json_path": str(json_path),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
