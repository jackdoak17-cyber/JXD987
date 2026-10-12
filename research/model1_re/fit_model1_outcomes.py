from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from statistics import mean
from typing import Any

import numpy as np
from scipy.optimize import minimize

from .config import REPORTS_DIR, get_settings
from .formula import FormulaParams, predict_over_probability


def _clamp(p: float) -> float:
    return max(1e-6, min(1 - 1e-6, float(p)))


def _log_loss(y: int, p: float) -> float:
    p = _clamp(p)
    return -(y * np.log(p) + (1 - y) * np.log(1 - p))


def _score_log_loss(rows: list[dict[str, Any]], params: FormulaParams) -> float:
    losses: list[float] = []
    for row in rows:
        y = row.get("target_hit")
        if y is None:
            continue
        prob = predict_over_probability(row, params)
        if prob is None:
            continue
        losses.append(float(_log_loss(1 if y else 0, float(prob))))
    if not losses:
        return float("inf")
    return float(mean(losses))


def _fit_log_odds_calibration_coeffs(rows: list[dict[str, Any]], base_params: FormulaParams) -> dict[str, float]:
    """
    Fit calibration coeffs by minimizing log-loss on locally-labeled outcomes.

    Uses the same calibrated probability mapping as formula._predict_calibrated_probability().
    """
    names = [
        "intercept",
        "line",
        "line_sq",
        "log_base",
        "log_emp",
        "log_opp_avg",
        "log_opp_hit",
        "is_home",
        "started_last_game",
        "likely_conf",
        "team_share",
    ]
    x0 = np.array([0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], dtype=float)
    bounds = [
        (-5.0, 5.0),  # intercept
        (-2.0, 2.0),  # line
        (-1.0, 1.0),  # line_sq
        (0.0, 2.0),  # log_base
        (-2.0, 2.0),  # log_emp
        (-3.0, 3.0),  # log_opp_avg
        (-3.0, 3.0),  # log_opp_hit
        (-2.0, 2.0),  # is_home
        (-2.0, 2.0),  # started_last_game
        (-2.0, 2.0),  # likely_conf
        (-2.0, 2.0),  # team_share
    ]

    def build(vec: np.ndarray) -> FormulaParams:
        payload = asdict(base_params)
        payload["calibration_mode"] = "logit_linear"
        payload["calibration_coeffs"] = {name: float(value) for name, value in zip(names, vec, strict=False)}
        return FormulaParams(**payload)

    def objective(vec: np.ndarray) -> float:
        params = build(vec)
        losses: list[float] = []
        for row in rows:
            y = row.get("target_hit")
            if y is None:
                continue
            prob = predict_over_probability(row, params)
            if prob is None:
                continue
            losses.append(float(_log_loss(1 if y else 0, float(prob))))
        if not losses:
            return float("inf")
        # Light L2 to reduce overfitting.
        l2 = 0.02
        penalty = l2 * float(np.dot(vec, vec))
        return float(mean(losses)) + penalty

    result = minimize(objective, x0, bounds=bounds, method="L-BFGS-B", options={"maxiter": 200})
    vec = result.x if result.success else x0
    return {name: float(value) for name, value in zip(names, vec, strict=False)}


def _search_best_base_params(
    rows: list[dict[str, Any]],
    market_type: str,
    settings=None,
) -> FormulaParams:
    settings = settings or get_settings()
    rng = np.random.default_rng(1)

    # Keep the search space deliberately small to avoid long runtimes.
    # We can expand once the "no Statshub" baseline is stable.
    player_windows = (60,)
    opponent_windows = (60,)
    player_sources = ["avg", "per90", "rec_per90", "avg_m60", "per90_m60", "rec_per90_m60"]
    ratio_sources = ["avg", "avg_mw", "hit", "hit_mw"]
    scales = [0.9, 1.0, 1.1]
    biases = [0.0, 0.05, 0.1]
    shrinks = [0.25, 0.5, 0.75]
    gammas = [1.0, 1.25, 1.5]
    distributions = [
        ("poisson", 0.0),
        ("negative_binomial", 2.0),
        ("negative_binomial", 3.0),
        ("negative_binomial", 5.0),
    ]

    scored_rows = rows
    if len(rows) > 2500:
        idx = rng.choice(len(rows), size=2500, replace=False)
        scored_rows = [rows[int(i)] for i in idx]

    best: FormulaParams | None = None
    best_loss: float | None = None

    trials = 900
    for _ in range(trials):
        player_window = int(rng.choice(player_windows))
        opponent_window = int(rng.choice(opponent_windows))
        player_source = str(rng.choice(player_sources))
        ratio_source = str(rng.choice(ratio_sources))
        scale = float(rng.choice(scales))
        bias = float(rng.choice(biases))
        shrink = float(rng.choice(shrinks))
        gamma = float(rng.choice(gammas))
        distribution, dispersion = distributions[int(rng.integers(0, len(distributions)))]
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
            dispersion=float(dispersion),
            calibration_mode="none",
            calibration_coeffs=None,
        )
        loss = _score_log_loss(scored_rows, params)
        if best_loss is None or loss < best_loss:
            best = params
            best_loss = loss

    if best is None:
        raise RuntimeError(f"Unable to fit base params for market_type={market_type}")
    return best


def fit_market(rows: list[dict[str, Any]], market_type: str, settings=None) -> dict[str, Any]:
    settings = settings or get_settings()
    market_rows = [r for r in rows if r.get("public_market_type") == market_type and r.get("target_hit") is not None]
    if not market_rows:
        raise RuntimeError(f"No training rows for market_type={market_type}")

    base_params = _search_best_base_params(market_rows, market_type, settings=settings)
    base_loss = _score_log_loss(market_rows, base_params)

    # Fit calibration coefficients to improve calibration on outcomes.
    #
    # Guardrail: very small training sets (common for SOT) massively overfit and
    # can produce absurd probabilities (e.g. implied fair odds ~1.10 for lines
    # that historically hit ~15-30%). For small N, we keep the baseline
    # distribution model, which is far more stable/conservative.
    calibrated_params = None
    calibrated_loss = base_loss
    calibration_skipped_reason = None
    if len(market_rows) < 200:
        calibration_skipped_reason = f"insufficient_rows({len(market_rows)})"
    else:
        coeffs = _fit_log_odds_calibration_coeffs(market_rows, base_params)
        payload = asdict(base_params)
        payload["calibration_mode"] = "logit_linear"
        payload["calibration_coeffs"] = coeffs
        calibrated_params = FormulaParams(**payload)
        calibrated_loss = _score_log_loss(market_rows, calibrated_params)

    selected_params = base_params
    selected_loss = float(base_loss)
    if calibrated_params is not None and calibrated_loss + 1e-12 < base_loss:
        selected_params = calibrated_params
        selected_loss = float(calibrated_loss)

    # Basic diagnostics.
    probs: list[float] = []
    ys: list[int] = []
    for row in market_rows:
        prob = predict_over_probability(row, selected_params)
        if prob is None:
            continue
        probs.append(float(prob))
        ys.append(1 if row["target_hit"] else 0)
    mean_p = float(mean(probs)) if probs else None
    mean_y = float(mean(ys)) if ys else None

    return {
        "market_type": market_type,
        "rows": len(market_rows),
        "selected_log_loss": float(selected_loss),
        "base_log_loss": float(base_loss),
        "calibrated_log_loss": float(calibrated_loss),
        "mean_pred_prob": mean_p,
        "mean_hit_rate": mean_y,
        "calibration_skipped_reason": calibration_skipped_reason,
        "params": asdict(selected_params),
    }


def write_fit_report(rows: list[dict[str, Any]], output_stem: str, settings=None) -> dict[str, Any]:
    settings = settings or get_settings()
    report = {
        "shots": fit_market(rows, "shots", settings=settings),
        "onTargetScoringAttempt": fit_market(rows, "onTargetScoringAttempt", settings=settings),
        "rows_total": len(rows),
        "label_source": "local_db",
    }
    json_path = REPORTS_DIR / f"{output_stem}_fit_report.json"
    md_path = REPORTS_DIR / f"{output_stem}_fit_report.md"
    json_path.write_text(json.dumps(report, indent=2))
    md_path.write_text(
        "\n".join(
            [
                "# Model 1 local fit report (no Statshub)",
                "",
                f"- rows_total: {report['rows_total']}",
                f"- shots: rows={report['shots']['rows']} log_loss={report['shots']['selected_log_loss']:.4f} mean_p={report['shots']['mean_pred_prob']} mean_y={report['shots']['mean_hit_rate']}",
                f"- sot: rows={report['onTargetScoringAttempt']['rows']} log_loss={report['onTargetScoringAttempt']['selected_log_loss']:.4f} mean_p={report['onTargetScoringAttempt']['mean_pred_prob']} mean_y={report['onTargetScoringAttempt']['mean_hit_rate']}",
            ]
        )
    )
    return report
