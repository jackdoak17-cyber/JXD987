from __future__ import annotations

import itertools
import json
from dataclasses import asdict
from pathlib import Path
from statistics import mean
from typing import Any

import numpy as np
from scipy.optimize import least_squares

from .config import REPORTS_DIR, get_settings
from .formula import FormulaParams, predict_fair_odds
from .utils import log_error


def _row_error(row: dict[str, Any], prediction: dict[str, float | None]) -> float | None:
    errors = [
        log_error(prediction["fair_over_odds"], row.get("target_fair_over_odds")),
        log_error(prediction["fair_under_odds"], row.get("target_fair_under_odds")),
    ]
    usable = [err for err in errors if err is not None]
    if not usable:
        return None
    return mean(usable)


def _row_abs_over_error(row: dict[str, Any], prediction: dict[str, float | None]) -> float | None:
    target = row.get("target_fair_over_odds")
    predicted = prediction.get("fair_over_odds")
    if target is None or predicted is None:
        return None
    return abs(predicted - target)


def _summary_for_rows(rows: list[dict[str, Any]], params: FormulaParams, market_type: str) -> dict[str, Any]:
    market_rows = [row for row in rows if row["public_market_type"] == market_type]
    log_errors: list[float] = []
    abs_over_errors: list[float] = []
    for row in market_rows:
        prediction = predict_fair_odds(row, params)
        log_error_value = _row_error(row, prediction)
        abs_over_error = _row_abs_over_error(row, prediction)
        if log_error_value is not None:
            log_errors.append(log_error_value)
        if abs_over_error is not None:
            abs_over_errors.append(abs_over_error)
    if not log_errors or not abs_over_errors:
        raise RuntimeError(f"No usable predictions for market_type={market_type}")
    return {
        "market_type": market_type,
        "rows": len(abs_over_errors),
        "mean_log_error": mean(log_errors),
        "mean_abs_over_error": mean(abs_over_errors),
        "median_abs_over_error": float(np.median(np.array(abs_over_errors))),
        "params": asdict(params),
    }


def _summary_with_line_overrides(
    rows: list[dict[str, Any]],
    market_type: str,
    global_params: FormulaParams,
    line_params: dict[str, dict[str, Any]],
) -> dict[str, float | int]:
    market_rows = [row for row in rows if row["public_market_type"] == market_type]
    log_errors: list[float] = []
    abs_over_errors: list[float] = []
    for row in market_rows:
        line_key = f"{float(row['public_line']):.1f}"
        params = global_params
        if line_key in line_params:
            params = FormulaParams(**line_params[line_key])
        prediction = predict_fair_odds(row, params)
        log_error_value = _row_error(row, prediction)
        abs_over_error = _row_abs_over_error(row, prediction)
        if log_error_value is not None:
            log_errors.append(log_error_value)
        if abs_over_error is not None:
            abs_over_errors.append(abs_over_error)
    return {
        "effective_rows": len(abs_over_errors),
        "effective_mean_log_error": mean(log_errors),
        "effective_mean_abs_over_error": mean(abs_over_errors),
        "effective_median_abs_over_error": float(np.median(np.array(abs_over_errors))),
    }


def _fit_log_odds_calibration(rows: list[dict[str, Any]], base_params: FormulaParams) -> dict[str, float]:
    market_rows = rows
    initial = np.array([0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0], dtype=float)
    lower = np.array([-5.0, -2.0, -1.0, 0.0, -2.0, -3.0, -3.0, -2.0], dtype=float)
    upper = np.array([5.0, 2.0, 1.0, 2.0, 2.0, 3.0, 3.0, 2.0], dtype=float)
    names = ["intercept", "line", "line_sq", "log_base", "log_emp", "log_opp_avg", "log_opp_hit", "is_home"]

    def build_params(vector: np.ndarray) -> FormulaParams:
        calibration_coeffs = {name: float(value) for name, value in zip(names, vector, strict=False)}
        payload = asdict(base_params)
        payload["calibration_mode"] = "log_odds_linear"
        payload["calibration_coeffs"] = calibration_coeffs
        return FormulaParams(**payload)

    def residuals(vector: np.ndarray) -> np.ndarray:
        params = build_params(vector)
        values: list[float] = []
        for row in market_rows:
            prediction = predict_fair_odds(row, params)
            target = row.get("target_fair_over_odds")
            predicted = prediction.get("fair_over_odds")
            if target is None or predicted is None:
                continue
            values.append(predicted - target)
        return np.array(values, dtype=float)

    result = least_squares(
        residuals,
        initial,
        bounds=(lower, upper),
        loss="soft_l1",
        f_scale=0.25,
        max_nfev=300,
    )
    return {name: float(value) for name, value in zip(names, result.x, strict=False)}


def _search_best_params(market_rows: list[dict[str, Any]], market_type: str) -> tuple[FormulaParams, dict[str, Any]]:
    settings = get_settings()
    scales = [0.9, 1.0, 1.1, 1.2]
    biases = [-0.05, 0.0, 0.05, 0.1]
    shrinks = [0.25, 0.5, 0.75]
    gammas = [1.0, 1.25, 1.5]
    player_sources = ["avg", "per90", "rec_avg", "rec_per90", "venue_avg", "venue_per90"]
    ratio_sources = ["avg", "hit"]
    distributions = [
        ("poisson", 10.0),
        ("negative_binomial", 1.5),
        ("negative_binomial", 2.0),
        ("negative_binomial", 3.0),
        ("negative_binomial", 5.0),
        ("negative_binomial", 10.0),
    ]
    best_params: FormulaParams | None = None
    best_summary: dict[str, Any] | None = None

    for player_window, player_source, opponent_window, scale, bias, shrink, gamma, ratio_source in itertools.product(
        settings.player_windows,
        player_sources,
        settings.opponent_windows,
        scales,
        biases,
        shrinks,
        gammas,
        ratio_sources,
    ):
        for distribution, dispersion in distributions:
            params = FormulaParams(
                player_window=player_window,
                opponent_window=opponent_window,
                player_source=player_source,
                scale=scale,
                bias=bias,
                shrink=shrink,
                gamma=gamma,
                model_type="distribution",
                ratio_source=ratio_source,
                distribution=distribution,
                dispersion=dispersion,
            )
            summary = _summary_for_rows(market_rows, params, market_type)
            if (
                best_summary is None
                or summary["mean_abs_over_error"] < best_summary["mean_abs_over_error"]
                or (
                    summary["mean_abs_over_error"] == best_summary["mean_abs_over_error"]
                    and summary["mean_log_error"] < best_summary["mean_log_error"]
                )
            ):
                best_params = params
                best_summary = summary

    if best_params is None or best_summary is None:
        raise RuntimeError(f"Unable to fit any candidate for market_type={market_type}")
    return best_params, best_summary


def fit_market_rows(rows: list[dict[str, Any]], market_type: str) -> dict[str, Any]:
    market_rows = [row for row in rows if row["public_market_type"] == market_type]
    if not market_rows:
        raise RuntimeError(f"No rows for market_type={market_type}")

    base_params, base_summary = _search_best_params(market_rows, market_type)

    calibration_coeffs = _fit_log_odds_calibration(market_rows, base_params)
    calibrated_payload = asdict(base_params)
    calibrated_payload["calibration_mode"] = "log_odds_linear"
    calibrated_payload["calibration_coeffs"] = calibration_coeffs
    calibrated_params = FormulaParams(**calibrated_payload)
    calibrated_summary = _summary_for_rows(market_rows, calibrated_params, market_type)

    selected_params = base_params
    selected_summary = base_summary
    if (
        calibrated_summary["mean_abs_over_error"] < base_summary["mean_abs_over_error"]
        and calibrated_summary["median_abs_over_error"] <= base_summary["median_abs_over_error"] + 0.05
    ):
        selected_params = calibrated_params
        selected_summary = calibrated_summary

    line_params: dict[str, dict[str, Any]] = {}
    line_metrics: dict[str, dict[str, Any]] = {}
    line_counts: dict[float, int] = {}
    for row in market_rows:
        line_counts[row["public_line"]] = line_counts.get(row["public_line"], 0) + 1

    for line_value, count in sorted(line_counts.items()):
        if count < 8:
            continue
        line_rows = [row for row in market_rows if row["public_line"] == line_value]
        line_best_params, line_best_summary = _search_best_params(line_rows, market_type)
        line_selected_params = line_best_params
        line_selected_summary = line_best_summary
        line_calibration_coeffs = _fit_log_odds_calibration(line_rows, line_best_params)
        line_calibrated_payload = asdict(line_best_params)
        line_calibrated_payload["calibration_mode"] = "log_odds_linear"
        line_calibrated_payload["calibration_coeffs"] = line_calibration_coeffs
        line_calibrated_params = FormulaParams(**line_calibrated_payload)
        line_calibrated_summary = _summary_for_rows(line_rows, line_calibrated_params, market_type)
        if (
            line_calibrated_summary["mean_abs_over_error"] < line_best_summary["mean_abs_over_error"]
            and line_calibrated_summary["median_abs_over_error"] <= line_best_summary["median_abs_over_error"] + 0.05
        ):
            line_selected_params = line_calibrated_params
            line_selected_summary = line_calibrated_summary
        selected_line_summary = _summary_for_rows(line_rows, selected_params, market_type)
        if line_selected_summary["mean_abs_over_error"] + 1e-9 < selected_line_summary["mean_abs_over_error"]:
            line_key = f"{line_value:.1f}"
            line_params[line_key] = asdict(line_selected_params)
            line_metrics[line_key] = {
                "rows": count,
                "line_mean_abs_over_error": line_selected_summary["mean_abs_over_error"],
                "global_mean_abs_over_error": selected_line_summary["mean_abs_over_error"],
            }

    selected_summary["base_params"] = asdict(base_params)
    selected_summary["base_mean_log_error"] = base_summary["mean_log_error"]
    selected_summary["base_mean_abs_over_error"] = base_summary["mean_abs_over_error"]
    selected_summary["base_median_abs_over_error"] = base_summary["median_abs_over_error"]
    selected_summary["line_params"] = line_params
    selected_summary["line_metrics"] = line_metrics
    selected_summary.update(_summary_with_line_overrides(market_rows, market_type, selected_params, line_params))
    return selected_summary


def write_fit_report(rows: list[dict[str, Any]], output_stem: str) -> dict[str, Any]:
    report = {
        "shots": fit_market_rows(rows, "shots"),
        "onTargetScoringAttempt": fit_market_rows(rows, "onTargetScoringAttempt"),
        "rows_total": len(rows),
    }
    json_path = REPORTS_DIR / f"{output_stem}_fit_report.json"
    md_path = REPORTS_DIR / f"{output_stem}_fit_report.md"
    json_path.write_text(json.dumps(report, indent=2))
    md_path.write_text(
        "\n".join(
            [
                "# Model 1 reverse-engineering fit report",
                "",
                f"- rows_total: {report['rows_total']}",
                f"- shots best: {report['shots']['params']} | mean_log_error={report['shots']['mean_log_error']:.4f} | mean_abs_over_error={report['shots']['mean_abs_over_error']:.4f} | effective_mean_abs_over_error={report['shots']['effective_mean_abs_over_error']:.4f}",
                f"- shots line models: {list(report['shots'].get('line_params', {}).keys())}",
                f"- SOT best: {report['onTargetScoringAttempt']['params']} | mean_log_error={report['onTargetScoringAttempt']['mean_log_error']:.4f} | mean_abs_over_error={report['onTargetScoringAttempt']['mean_abs_over_error']:.4f} | effective_mean_abs_over_error={report['onTargetScoringAttempt']['effective_mean_abs_over_error']:.4f}",
                f"- SOT line models: {list(report['onTargetScoringAttempt'].get('line_params', {}).keys())}",
            ]
        )
    )
    return report
