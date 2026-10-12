from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from dataclasses import replace
from pathlib import Path
from statistics import mean, median
from typing import Any

from psycopg2.extras import RealDictCursor

from .config import REPORTS_DIR, get_settings
from .db_read import (
    MatchedTarget,
    UpcomingCandidate,
    build_upcoming_feature_rows,
    match_public_targets,
    readonly_connection,
)
from .formula import FormulaParams, predict_fair_odds
from .public_targets import MODEL_NAME_BY_MARKET, fetch_model1_targets, load_targets


def _safe_median(values: list[float]) -> float | None:
    if not values:
        return None
    return float(median(values))


def _safe_mean(values: list[float]) -> float | None:
    if not values:
        return None
    return float(mean(values))


def _load_fit_params(report_path: Path) -> dict[str, dict[str, Any]]:
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


def _to_candidate(target: MatchedTarget) -> UpcomingCandidate:
    market_key = "player_shots" if target.public_market_type == "shots" else "player_shots_on_target"
    # Preserve the Statshub group key so we can join predictions back to targets after feature building.
    selection_key = target.public_group_key
    best_over_odds = float(target.public_best_over_odds or 0.0)
    return UpcomingCandidate(
        market_key=market_key,
        public_market_type=target.public_market_type,
        fixture_id=target.fixture_id,
        fixture_timestamp=target.fixture_timestamp,
        league_id=target.league_id,
        season_id=target.season_id,
        home_team_id=target.team_id if target.team_is_home else target.opponent_team_id,
        away_team_id=target.opponent_team_id if target.team_is_home else target.team_id,
        home_team_name=target.public_home_team_name,
        away_team_name=target.public_away_team_name,
        player_id=target.player_id,
        player_name_db=target.player_name_db,
        selection_key=selection_key,
        line=float(target.public_line),
        best_over_odds=best_over_odds,
        team_id=target.team_id,
        opponent_team_id=target.opponent_team_id,
        team_is_home=target.team_is_home,
        resolution_source="statshub_match",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare local Model 1 predictions vs Statshub for the upcoming week.")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--max-pages", type=int, default=80)
    parser.add_argument("--max-odd", type=float, default=4.0)
    parser.add_argument("--min-odd", type=float, default=1.01)
    parser.add_argument("--sort-by", default="overEdgePercent", help="Public API sort key")
    parser.add_argument("--sort-order", default="desc", help="Public API sort order")
    parser.add_argument(
        "--targets-path",
        default=None,
        help="Optional path to a saved public targets JSON (freezes the comparison target set)",
    )
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
        "--fit-report",
        default=str(REPORTS_DIR / "model1_all_fit_report.json"),
        help="Fit report to use for local predictions",
    )
    args = parser.parse_args()

    now = datetime.now(timezone.utc)
    horizon = now + timedelta(days=args.days)
    fit_params = _load_fit_params(Path(args.fit_report))

    if args.targets_path:
        public_targets = load_targets(Path(args.targets_path))
    else:
        public_targets = fetch_model1_targets(
            date_filter="all",
            max_pages=args.max_pages,
            future_only=True,
            sort_by=args.sort_by,
            sort_order=args.sort_order,
            max_odd=args.max_odd,
        )
    public_targets = [t for t in public_targets if now.timestamp() < t.public_match_timestamp <= horizon.timestamp()]

    settings = get_settings()
    if args.sot_plus_goals:
        settings = replace(settings, sot_plus_goals=True)
    if args.sot_use_shots_total:
        settings = replace(settings, sot_use_shots_total=True)
    if args.sot_dual_features:
        settings = replace(settings, sot_dual_features=True)
    with readonly_connection(settings) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            matched = match_public_targets(cur, public_targets)
            candidates = [_to_candidate(t) for t in matched]
            feature_rows = build_upcoming_feature_rows(cur, candidates, settings=settings, cutoff_timestamp=now, verbose=False)

    # Index statshub targets by group key for quick lookup.
    target_by_key: dict[str, MatchedTarget] = {t.public_group_key: t for t in matched}

    comparisons: list[dict[str, Any]] = []
    abs_diffs_by_market: dict[str, list[float]] = {"shots": [], "onTargetScoringAttempt": []}
    abs_diffs_by_market_line: dict[tuple[str, str], list[float]] = {}

    for row in feature_rows:
        group_key = row.get("selection_key")
        if not group_key or group_key not in target_by_key:
            continue
        target = target_by_key[group_key]
        target_fair = target.public_model_fair_over_odds
        best_over_odds = float(target.public_best_over_odds or 0.0)
        if best_over_odds < args.min_odd or best_over_odds > args.max_odd:
            continue
        market = target.public_market_type
        line_key = f"{float(target.public_line):.1f}"
        bundle = fit_params[market]
        params = bundle["line"].get(line_key, bundle["global"])
        pred = predict_fair_odds(row, params)
        our_fair = pred.get("fair_over_odds")
        if target_fair is None or our_fair is None:
            continue
        abs_diff = abs(float(our_fair) - float(target_fair))
        abs_diffs_by_market[market].append(abs_diff)
        abs_diffs_by_market_line.setdefault((market, line_key), []).append(abs_diff)

        statshub_edge = ((best_over_odds - float(target_fair)) / float(target_fair)) * 100.0 if target_fair else None
        our_edge = ((best_over_odds - float(our_fair)) / float(our_fair)) * 100.0 if our_fair else None

        # Feature slices for diagnosing mismatches.
        player_window = int(params.player_window)
        feature_slice = {
            "started_last_game": bool(row.get("started_last_game", False)),
            "minutes_last_game": row.get("minutes_last_game"),
            "likely_lineup_confidence": float(row.get("likely_lineup_confidence") or 0.0),
            f"player_avg_{player_window}": row.get(f"player_avg_{player_window}"),
            f"player_per90_{player_window}": row.get(f"player_per90_{player_window}"),
            f"player_hit_rate_{player_window}": row.get(f"player_hit_rate_{player_window}"),
            f"player_team_share_{player_window}": row.get(f"player_team_share_{player_window}"),
            f"opp_ratio_{params.opponent_window}": row.get(f"opp_ratio_{params.opponent_window}"),
            f"opp_hit_ratio_{params.opponent_window}": row.get(f"opp_hit_ratio_{params.opponent_window}"),
        }

        comparisons.append(
            {
                "public_group_key": group_key,
                "fixture_timestamp": str(target.fixture_timestamp),
                "home_team": target.public_home_team_name,
                "away_team": target.public_away_team_name,
                "player": target.public_player_name,
                "market_type": market,
                "line": float(target.public_line),
                "best_over_odds": best_over_odds,
                "statshub_model": MODEL_NAME_BY_MARKET[market],
                "statshub_fair_over_odds": float(target_fair),
                "our_fair_over_odds": float(our_fair),
                "abs_fair_over_diff": abs_diff,
                "statshub_edge_pct": float(statshub_edge) if statshub_edge is not None else None,
                "our_edge_pct": float(our_edge) if our_edge is not None else None,
                "model_params": {
                    "player_window": params.player_window,
                    "opponent_window": params.opponent_window,
                    "player_source": params.player_source,
                    "ratio_source": params.ratio_source,
                    "distribution": params.distribution,
                    "dispersion": params.dispersion,
                    "calibration_mode": params.calibration_mode,
                },
                "feature_slice": feature_slice,
            }
        )

    # Pick overlap: top N by edge on the matched set.
    def top_keys(rows: list[dict[str, Any]], key: str, n: int = 50) -> list[str]:
        filtered = [r for r in rows if r.get(key) is not None]
        filtered.sort(key=lambda r: float(r[key]), reverse=True)
        return [r["public_group_key"] for r in filtered[:n]]

    top_statshub = set(top_keys(comparisons, "statshub_edge_pct", n=50))
    top_ours = set(top_keys(comparisons, "our_edge_pct", n=50))
    overlap = sorted(top_statshub & top_ours)

    summary = {
        "generated_at_utc": now.isoformat(),
        "horizon_utc": horizon.isoformat(),
        "targets_fetched": len(public_targets),
        "targets_matched": len(matched),
        "feature_rows": len(feature_rows),
        "compared_rows": len(comparisons),
        "abs_fair_over_diff": {
            "shots": {"mean": _safe_mean(abs_diffs_by_market["shots"]), "median": _safe_median(abs_diffs_by_market["shots"])},
            "onTargetScoringAttempt": {
                "mean": _safe_mean(abs_diffs_by_market["onTargetScoringAttempt"]),
                "median": _safe_median(abs_diffs_by_market["onTargetScoringAttempt"]),
            },
        },
        "abs_fair_over_diff_by_line": {
            f"{market}:{line}": {"count": len(vals), "mean": _safe_mean(vals), "median": _safe_median(vals)}
            for (market, line), vals in sorted(abs_diffs_by_market_line.items(), key=lambda kv: (kv[0][0], float(kv[0][1])))
        },
        "top50_overlap_count": len(overlap),
        "top50_overlap_keys": overlap[:100],
    }

    comparisons.sort(key=lambda r: float(r["abs_fair_over_diff"]), reverse=True)
    worst = comparisons[:50]
    best = sorted(comparisons, key=lambda r: float(r["abs_fair_over_diff"]))[:50]

    report = {
        "summary": summary,
        "worst_50_by_abs_fair_over_diff": worst,
        "best_50_by_abs_fair_over_diff": best,
    }

    out_stem = f"compare_week_{now.strftime('%Y%m%d')}"
    json_path = REPORTS_DIR / f"{out_stem}.json"
    md_path = REPORTS_DIR / f"{out_stem}.md"
    json_path.write_text(json.dumps(report, indent=2))
    md_path.write_text(
        "\n".join(
            [
                "# Statshub vs local Model 1 (upcoming week)",
                "",
                f"- generated_at_utc: {summary['generated_at_utc']}",
                f"- horizon_utc: {summary['horizon_utc']}",
                f"- targets_fetched: {summary['targets_fetched']}",
                f"- targets_matched: {summary['targets_matched']}",
                f"- compared_rows: {summary['compared_rows']}",
                f"- shots abs diff: mean={summary['abs_fair_over_diff']['shots']['mean']} median={summary['abs_fair_over_diff']['shots']['median']}",
                f"- SOT abs diff: mean={summary['abs_fair_over_diff']['onTargetScoringAttempt']['mean']} median={summary['abs_fair_over_diff']['onTargetScoringAttempt']['median']}",
                f"- top50 overlap count: {summary['top50_overlap_count']}",
            ]
        )
    )
    print(json.dumps({"json": str(json_path), "md": str(md_path), "summary": summary}, indent=2))


if __name__ == "__main__":
    main()
