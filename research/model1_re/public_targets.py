from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from .config import ARTIFACTS_DIR, get_settings


MODEL_NAME_BY_MARKET = {
    "shots": "Shots opp_vs_leag",
    "onTargetScoringAttempt": "SOT opp_vs_leag",
}


@dataclass
class PublicTarget:
    public_group_key: str
    public_player_name: str
    public_team_name: str
    public_home_team_name: str
    public_away_team_name: str
    public_match_timestamp: int
    public_market_type: str
    public_line: float
    public_best_over_odds: float | None
    public_best_over_edge: float | None
    public_model_name: str
    public_model_fair_over_odds: float | None
    public_model_fair_under_odds: float | None
    raw_row: dict[str, Any]


def _fetch_page(session: requests.Session, params: dict[str, Any]) -> dict[str, Any]:
    settings = get_settings()
    response = session.get(settings.public_value_bets_url, params=params, timeout=settings.request_timeout_seconds)
    response.raise_for_status()
    return response.json()


def fetch_model1_targets(
    date_filter: str = "today",
    max_pages: int | None = None,
    future_only: bool = True,
) -> list[PublicTarget]:
    session = requests.Session()
    fetch_started_at = datetime.now(timezone.utc)
    fetch_started_ts = int(fetch_started_at.timestamp())
    params: dict[str, Any] = {
        "page": 1,
        "line": "all",
        "marketType": "all",
        "sortBy": "overEdgePercent",
        "sortOrder": "desc",
        "maxOdd": 4,
    }
    if date_filter:
        params["date"] = date_filter

    targets: list[PublicTarget] = []
    total_pages = 1

    while True:
        payload = _fetch_page(session, params)
        total_pages = int(payload.get("pagination", {}).get("totalPages", params["page"]))
        for row in payload.get("data", []):
            if future_only and int(row.get("matchDate") or 0) <= fetch_started_ts:
                continue
            model_name = MODEL_NAME_BY_MARKET.get(row.get("marketType"))
            if not model_name:
                continue
            model_entry = next((m for m in row.get("models", []) if m.get("modelName") == model_name), None)
            if not model_entry:
                continue
            targets.append(
                PublicTarget(
                    public_group_key=row["groupKey"],
                    public_player_name=row["playerName"],
                    public_team_name=row["teamName"],
                    public_home_team_name=row["homeTeamName"],
                    public_away_team_name=row["awayTeamName"],
                    public_match_timestamp=int(row["matchDate"]),
                    public_market_type=row["marketType"],
                    public_line=float(row["line"]),
                    public_best_over_odds=row.get("bestOverOdds"),
                    public_best_over_edge=row.get("bestOverEdge"),
                    public_model_name=model_entry["modelName"],
                    public_model_fair_over_odds=model_entry.get("fairOverOdds"),
                    public_model_fair_under_odds=model_entry.get("fairUnderOdds"),
                    raw_row=row,
                )
            )
        if not payload.get("pagination", {}).get("hasNextPage"):
            break
        if max_pages is not None and params["page"] >= max_pages:
            break
        params["page"] += 1

    metadata = {
        "date_filter": date_filter,
        "future_only": future_only,
        "fetch_started_at_utc": fetch_started_at.isoformat(),
        "pages_fetched": params["page"],
        "total_pages_reported": total_pages,
        "target_rows": len(targets),
    }
    raw_path = ARTIFACTS_DIR / f"public_model1_targets_{date_filter or 'all'}.json"
    raw_path.write_text(json.dumps({"metadata": metadata, "targets": [asdict(t) for t in targets]}, indent=2))
    return targets


def load_targets(path: Path) -> list[PublicTarget]:
    payload = json.loads(path.read_text())
    return [PublicTarget(**target) for target in payload["targets"]]
