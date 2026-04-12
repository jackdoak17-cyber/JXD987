from __future__ import annotations

import math
import re
import unicodedata
from difflib import SequenceMatcher


def normalize_text(value: str | None) -> str:
    if not value:
        return ""
    ascii_value = (
        unicodedata.normalize("NFKD", value)
        .encode("ascii", "ignore")
        .decode("ascii")
        .lower()
    )
    ascii_value = ascii_value.replace("&", " and ")
    ascii_value = re.sub(r"[^a-z0-9]+", " ", ascii_value)
    return re.sub(r"\s+", " ", ascii_value).strip()


def alias_similarity(left: str | None, right: str | None) -> float:
    a = normalize_text(left)
    b = normalize_text(right)
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    if a in b or b in a:
        shorter = min(len(a), len(b))
        longer = max(len(a), len(b))
        return 0.92 + 0.06 * (shorter / longer)
    a_tokens = set(a.split())
    b_tokens = set(b.split())
    if a_tokens and a_tokens.issubset(b_tokens):
        return 0.95
    if b_tokens and b_tokens.issubset(a_tokens):
        return 0.95
    return SequenceMatcher(None, a, b).ratio()


def poisson_over_probability(lmbda: float, line: float) -> float:
    if lmbda <= 0:
        return 0.0
    threshold = int(math.floor(line)) + 1
    cumulative = 0.0
    for k in range(threshold):
        cumulative += math.exp(-lmbda) * (lmbda**k) / math.factorial(k)
    return max(0.0, min(1.0, 1.0 - cumulative))


def negative_binomial_over_probability(mean_value: float, line: float, dispersion: float) -> float:
    if mean_value <= 0:
        return 0.0
    if dispersion <= 0:
        return poisson_over_probability(mean_value, line)
    threshold = int(math.floor(line)) + 1
    p = dispersion / (dispersion + mean_value)
    cumulative = 0.0
    for k in range(threshold):
        log_prob = (
            math.lgamma(k + dispersion)
            - math.lgamma(dispersion)
            - math.lgamma(k + 1)
            + dispersion * math.log(p)
            + k * math.log(1 - p)
        )
        cumulative += math.exp(log_prob)
    return max(0.0, min(1.0, 1.0 - cumulative))


def probability_to_decimal(probability: float) -> float | None:
    if probability <= 0:
        return None
    return 1.0 / probability


def recency_weighted_average(values: list[float], decay: float = 0.9) -> float | None:
    if not values:
        return None
    weighted_sum = 0.0
    total_weight = 0.0
    for index, value in enumerate(values):
        weight = decay**index
        weighted_sum += value * weight
        total_weight += weight
    if total_weight <= 0:
        return None
    return weighted_sum / total_weight


def recency_weighted_per90(values: list[float], minutes: list[float], decay: float = 0.9) -> float | None:
    if not values or not minutes or len(values) != len(minutes):
        return None
    weighted_stat_sum = 0.0
    weighted_minutes_sum = 0.0
    for index, (value, minute_value) in enumerate(zip(values, minutes, strict=False)):
        weight = decay**index
        weighted_stat_sum += value * weight
        weighted_minutes_sum += minute_value * weight
    if weighted_minutes_sum <= 0:
        return None
    return (weighted_stat_sum * 90.0) / weighted_minutes_sum


def venue_average(
    values: list[float],
    is_home_flags: list[bool],
    target_is_home: bool,
    fallback: float,
    minimum_games: int = 3,
) -> float:
    filtered = [value for value, is_home in zip(values, is_home_flags, strict=False) if is_home == target_is_home]
    if len(filtered) < minimum_games:
        return fallback
    return sum(filtered) / len(filtered)


def venue_per90(
    values: list[float],
    minutes: list[float],
    is_home_flags: list[bool],
    target_is_home: bool,
    fallback: float | None,
    minimum_games: int = 3,
) -> float | None:
    filtered = [(value, minute_value) for value, minute_value, is_home in zip(values, minutes, is_home_flags, strict=False) if is_home == target_is_home]
    if len(filtered) < minimum_games:
        return fallback
    total_value = sum(value for value, _ in filtered)
    total_minutes = sum(minute_value for _, minute_value in filtered)
    if total_minutes <= 0:
        return fallback
    return (total_value * 90.0) / total_minutes


def safe_ratio(numerator: float | None, denominator: float | None, default: float = 1.0) -> float:
    if numerator is None or denominator in (None, 0):
        return default
    return numerator / denominator


def log_error(predicted: float | None, actual: float | None) -> float | None:
    if predicted is None or actual is None or predicted <= 0 or actual <= 0:
        return None
    return abs(math.log(predicted / actual))
