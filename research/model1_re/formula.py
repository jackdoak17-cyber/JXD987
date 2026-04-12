from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

from .utils import (
    negative_binomial_over_probability,
    poisson_over_probability,
    probability_to_decimal,
)


@dataclass(frozen=True)
class FormulaParams:
    player_window: int
    opponent_window: int
    player_source: str
    scale: float
    bias: float
    shrink: float
    gamma: float
    model_type: str = "distribution"
    ratio_source: str = "avg"
    distribution: str = "poisson"
    dispersion: float = 10.0
    prior_alpha: float = 1.0
    prior_beta: float = 1.0
    calibration_mode: str = "none"
    calibration_coeffs: dict[str, float] | None = None


def _ratio_for_row(row: dict[str, Any], params: FormulaParams) -> float:
    if params.ratio_source == "hit":
        return row.get(f"opp_hit_ratio_{params.opponent_window}") or 1.0
    return row.get(f"opp_ratio_{params.opponent_window}") or 1.0


def _player_baseline_for_row(row: dict[str, Any], params: FormulaParams) -> float | None:
    key = f"player_{params.player_source}_{params.player_window}"
    value = row.get(key)
    if value is None:
        return None
    return float(value)


def predict_mu(row: dict[str, Any], params: FormulaParams) -> float:
    baseline = _player_baseline_for_row(row, params)
    if baseline is None:
        return 0.01
    ratio = _ratio_for_row(row, params)
    adjusted_ratio = 1.0 + params.shrink * (ratio - 1.0)
    adjusted_ratio = max(0.1, adjusted_ratio)
    mu = params.bias + params.scale * baseline * (adjusted_ratio**params.gamma)
    return max(0.01, mu)


def _predict_empirical_probability(row: dict[str, Any], params: FormulaParams) -> float | None:
    games = row.get(f"player_games_{params.player_window}")
    hits = row.get(f"player_hits_{params.player_window}")
    if games in (None, 0) or hits is None:
        return None
    smoothed_base = (hits + params.prior_alpha) / (games + params.prior_alpha + params.prior_beta)
    return max(1e-6, min(1 - 1e-6, smoothed_base))


def _predict_distribution_probability(row: dict[str, Any], params: FormulaParams) -> float | None:
    mu = predict_mu(row, params)
    if params.distribution == "negative_binomial":
        return negative_binomial_over_probability(mu, row["public_line"], params.dispersion)
    return poisson_over_probability(mu, row["public_line"])


def _clamp_probability(probability: float) -> float:
    return max(1e-6, min(1 - 1e-6, probability))


def _predict_calibrated_probability(row: dict[str, Any], params: FormulaParams) -> float | None:
    coeffs = params.calibration_coeffs or {}
    base_probability = _predict_distribution_probability(row, params)
    empirical_probability = _predict_empirical_probability(row, params)
    if base_probability is None or empirical_probability is None:
        return None
    base_odds = probability_to_decimal(_clamp_probability(base_probability))
    empirical_odds = probability_to_decimal(_clamp_probability(empirical_probability))
    if base_odds is None or empirical_odds is None:
        return None
    line = float(row["public_line"])
    opp_ratio = row.get(f"opp_ratio_{params.opponent_window}") or 1.0
    opp_hit_ratio = row.get(f"opp_hit_ratio_{params.opponent_window}") or 1.0
    z = (
        coeffs.get("intercept", 0.0)
        + coeffs.get("line", 0.0) * line
        + coeffs.get("line_sq", 0.0) * (line**2)
        + coeffs.get("log_base", 1.0) * math.log(max(base_odds, 1.000001))
        + coeffs.get("log_emp", 0.0) * math.log(max(empirical_odds, 1.000001))
        + coeffs.get("log_opp_avg", 0.0) * math.log(max(opp_ratio, 1e-6))
        + coeffs.get("log_opp_hit", 0.0) * math.log(max(opp_hit_ratio, 1e-6))
        + coeffs.get("is_home", 0.0) * (1.0 if row.get("team_is_home") else 0.0)
    )
    calibrated_over_odds = max(1.000001, math.exp(z))
    probability = _clamp_probability(1.0 / calibrated_over_odds)
    return probability


def predict_fair_odds(row: dict[str, Any], params: FormulaParams) -> dict[str, float | None]:
    mu = predict_mu(row, params)
    if params.calibration_mode == "log_odds_linear":
        over_probability = _predict_calibrated_probability(row, params)
    elif params.model_type == "empirical":
        over_probability = _predict_empirical_probability(row, params)
    else:
        over_probability = _predict_distribution_probability(row, params)
    if over_probability is None:
        return {
            "mu": mu,
            "fair_over_odds": None,
            "fair_under_odds": None,
        }
    under_probability = max(0.0, min(1.0, 1.0 - over_probability))
    return {
        "mu": mu,
        "fair_over_odds": probability_to_decimal(over_probability),
        "fair_under_odds": probability_to_decimal(under_probability),
    }
