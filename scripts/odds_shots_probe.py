#!/usr/bin/env python3
"""
Probe SportMonks odds coverage for shot markets using paginated bookmaker requests.

For each fixture in the requested windows, we call:
- /odds/pre-match/fixtures/{fixture_id}/bookmakers/{bookmaker_id} (paginated)
- the same endpoint with filters=markets:<shot_market_ids>

We output per-fixture coverage and raw shot market IDs/names.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from jxd import SportMonksClient
from jxd.sportmonks_client import SportMonksError

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import sync_odds as odds  # noqa: E402

SHOT_TOTAL_KEYS = {
    "team_shots",
    "team_shots_on_target",
    "match_shots",
    "match_shots_on_target",
}

PLAYER_SHOT_KEYS = {"player_shots", "player_shots_on_target"}


def parse_dt(text: str) -> Optional[datetime]:
    if not text:
        return None
    text_val = text.replace("T", " ").replace("Z", "")
    try:
        return datetime.fromisoformat(text_val)
    except Exception:
        return None


def discover_shot_market_ids(client: SportMonksClient) -> Dict[int, str]:
    targets = {
        "team shots",
        "team shots on target",
        "match shots",
        "match shots on target",
    }
    results: Dict[int, str] = {}
    try:
        for row in client.fetch_collection("markets", per_page=50):
            market_id = row.get("id") or row.get("market_id")
            if market_id is None:
                continue
            name = (
                row.get("name")
                or row.get("description")
                or row.get("market")
                or row.get("market_description")
                or ""
            )
            name_lower = str(name).lower()
            if name_lower in targets:
                results[int(market_id)] = str(name)
    except SportMonksError as exc:
        print(f"Market discovery failed: {exc}. Using fallback IDs.", flush=True)
    return results


def fetch_fixtures_between(
    client: SportMonksClient,
    league_id: int,
    start_dt: datetime,
    end_dt: datetime,
) -> List[Dict[str, object]]:
    endpoint = f"fixtures/between/{start_dt.date().isoformat()}/{end_dt.date().isoformat()}"
    params = {"filters": f"fixtureLeagues:{league_id}"}
    fixtures = []
    for row in client.fetch_collection(endpoint, params=params, includes=["participants"], per_page=50):
        fixtures.append(
            {
                "id": row.get("id"),
                "starting_at": row.get("starting_at"),
            }
        )
    return fixtures


def analyze_rows(rows: Iterable[Dict]) -> Dict[str, object]:
    keys = set()
    raw_shot_markets: List[Dict[str, object]] = []
    for row in rows:
        market_key = odds.resolve_market_key(row)
        keys.add(market_key)
        market_id = row.get("market_id")
        market_desc = row.get("market_description") or row.get("market") or ""
        selection_text = odds.merge_selection_text(str(row.get("name") or ""), str(row.get("label") or ""))
        desc_lower = str(market_desc).lower()
        selection_lower = selection_text.lower()
        if "shot" in desc_lower or "shot" in selection_lower or "on target" in desc_lower or "on target" in selection_lower:
            raw_shot_markets.append(
                {
                    "market_id": market_id,
                    "market_description": str(market_desc),
                    "market_key": market_key,
                }
            )
    summary = {
        "has_player_shots": any(key in keys for key in PLAYER_SHOT_KEYS),
        "has_team_shots": "team_shots" in keys,
        "has_team_sot": "team_shots_on_target" in keys,
        "has_match_shots": "match_shots" in keys,
        "has_match_sot": "match_shots_on_target" in keys,
        "raw_shot_markets": raw_shot_markets,
    }
    return summary


def summarize(fixtures: List[Dict[str, object]], key: str) -> Dict[str, int]:
    counts = {
        "player_shots": 0,
        "team_shots": 0,
        "team_sot": 0,
        "match_shots": 0,
        "match_sot": 0,
    }
    for fixture in fixtures:
        block = fixture.get(key, {}) or {}
        if block.get("has_player_shots"):
            counts["player_shots"] += 1
        if block.get("has_team_shots"):
            counts["team_shots"] += 1
        if block.get("has_team_sot"):
            counts["team_sot"] += 1
        if block.get("has_match_shots"):
            counts["match_shots"] += 1
        if block.get("has_match_sot"):
            counts["match_sot"] += 1
    return counts


def run_window(
    client: SportMonksClient,
    league_id: int,
    label: str,
    start_dt: datetime,
    end_dt: datetime,
    bookmaker_id: int,
    market_ids: Optional[List[int]],
    per_page: int,
    sleep_seconds: float,
    out_dir: Path,
    max_fixtures: int,
    progress_every: int,
    skip_unfiltered: bool,
    skip_filtered: bool,
    fixture_ids: Optional[List[int]] = None,
    rate_limit_sleep: float = 30.0,
    rate_limit_retries: int = 3,
) -> Dict[str, object]:
    if fixture_ids:
        fixtures = [{"id": fixture_id, "starting_at": None} for fixture_id in fixture_ids]
    else:
        fixtures = fetch_fixtures_between(client, league_id, start_dt, end_dt)
    if max_fixtures:
        fixtures = fixtures[:max_fixtures]
    output_rows: List[Dict[str, object]] = []
    start_ts = time.time()
    for fixture in fixtures:
        fixture_id = int(fixture["id"])
        starting_at = fixture.get("starting_at")
        print(f"[{league_id} {label}] fetching fixture {fixture_id}", flush=True)
        unfiltered_rows: List[Dict] = []
        filtered_rows: List[Dict] = []
        unfiltered_error = None
        filtered_error = None
        if not skip_unfiltered:
            for attempt in range(rate_limit_retries):
                try:
                    unfiltered_rows = odds.fetch_odds_for_fixture(
                        client,
                        fixture_id,
                        bookmaker_id,
                        market_ids=None,
                        per_page=per_page,
                    )
                    break
                except SportMonksError as exc:
                    if exc.status_code == 429 and attempt < rate_limit_retries - 1:
                        time.sleep(rate_limit_sleep)
                        continue
                    unfiltered_error = str(exc)
                    break
                except Exception as exc:
                    unfiltered_error = str(exc)
                    break
        if market_ids and not skip_filtered:
            for attempt in range(rate_limit_retries):
                try:
                    filtered_rows = odds.fetch_odds_for_fixture(
                        client,
                        fixture_id,
                        bookmaker_id,
                        market_ids=market_ids,
                        per_page=per_page,
                    )
                    break
                except SportMonksError as exc:
                    if exc.status_code == 429 and attempt < rate_limit_retries - 1:
                        time.sleep(rate_limit_sleep)
                        continue
                    filtered_error = str(exc)
                    break
                except Exception as exc:
                    filtered_error = str(exc)
                    break
        unfiltered_summary = analyze_rows(unfiltered_rows)
        filtered_summary = analyze_rows(filtered_rows)
        output_rows.append(
            {
                "fixture_id": fixture_id,
                "starting_at": starting_at,
                "unfiltered": unfiltered_summary,
                "filtered": filtered_summary,
                "errors": {
                    "unfiltered": unfiltered_error,
                    "filtered": filtered_error,
                },
                "missing_in_unfiltered": {
                    "team_shots": filtered_summary["has_team_shots"] and not unfiltered_summary["has_team_shots"],
                    "team_sot": filtered_summary["has_team_sot"] and not unfiltered_summary["has_team_sot"],
                    "match_shots": filtered_summary["has_match_shots"] and not unfiltered_summary["has_match_shots"],
                    "match_sot": filtered_summary["has_match_sot"] and not unfiltered_summary["has_match_sot"],
                },
            }
        )
        if sleep_seconds:
            time.sleep(sleep_seconds)
        if progress_every and len(output_rows) % progress_every == 0:
            elapsed = time.time() - start_ts
            print(
                f"[{league_id} {label}] processed {len(output_rows)}/{len(fixtures)} fixtures "
                f"elapsed={elapsed:.1f}s",
                flush=True,
            )

    summary = {
        "league_id": league_id,
        "window": label,
        "fixtures_count": len(fixtures),
        "unfiltered_counts": summarize(output_rows, "unfiltered"),
        "filtered_counts": summarize(output_rows, "filtered"),
    }
    payload = {
        "league_id": league_id,
        "window": label,
        "start": start_dt.isoformat() + "Z",
        "end": end_dt.isoformat() + "Z",
        "fixtures_count": len(fixtures),
        "market_ids": market_ids,
        "fixtures": output_rows,
        "summary": summary,
    }
    out_path = out_dir / f"odds_shots_probe_{league_id}_{label}.json"
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--leagues", default="8,9", help="Comma-separated league IDs")
    parser.add_argument("--bookmaker-id", type=int, default=2)
    parser.add_argument("--hours-forward", type=int, default=48)
    parser.add_argument("--days-forward", type=int, default=14)
    parser.add_argument("--market-ids", default="", help="Comma-separated market IDs for shot totals.")
    parser.add_argument("--per-page", type=int, default=50)
    parser.add_argument("--sleep", type=float, default=0.2)
    parser.add_argument("--timeout", type=int, default=20)
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--max-fixtures", type=int, default=0)
    parser.add_argument("--progress-every", type=int, default=5)
    parser.add_argument("--skip-unfiltered", action="store_true")
    parser.add_argument("--skip-filtered", action="store_true")
    parser.add_argument("--fixture-ids", default="", help="Comma-separated fixture IDs (overrides windows).")
    parser.add_argument("--rate-limit-sleep", type=float, default=30.0)
    parser.add_argument("--rate-limit-retries", type=int, default=3)
    parser.add_argument("--out-dir", default="/tmp")
    args = parser.parse_args()

    league_ids = [int(x) for x in args.leagues.split(",") if x.strip()]
    fixture_ids: List[int] = []
    if args.fixture_ids:
        fixture_ids = [int(x) for x in args.fixture_ids.split(",") if x.strip()]
    client = SportMonksClient(timeout=args.timeout, max_retries=args.max_retries)
    market_ids: List[int] = []
    if args.market_ids:
        market_ids = [int(x) for x in args.market_ids.split(",") if x.strip()]
    else:
        discovered = discover_shot_market_ids(client)
        if discovered:
            market_ids = sorted(discovered.keys())
            print(f"Discovered shot market IDs: {discovered}")
        else:
            market_ids = [284, 285, 291, 292]
            print(f"Fallback shot market IDs: {market_ids}")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    now = datetime.utcnow()
    end_48h = now + timedelta(hours=args.hours_forward)
    end_14d = now + timedelta(days=args.days_forward)

    summaries = []
    for league_id in league_ids:
        summaries.append(
            run_window(
                client,
                league_id,
                "48h",
                now,
                end_48h,
                args.bookmaker_id,
                market_ids,
                args.per_page,
                args.sleep,
                out_dir,
                args.max_fixtures,
                args.progress_every,
                args.skip_unfiltered,
                args.skip_filtered,
                fixture_ids if fixture_ids else None,
                args.rate_limit_sleep,
                args.rate_limit_retries,
            )
        )
        summaries.append(
            run_window(
                client,
                league_id,
                "14d",
                now,
                end_14d,
                args.bookmaker_id,
                market_ids,
                args.per_page,
                args.sleep,
                out_dir,
                args.max_fixtures,
                args.progress_every,
                args.skip_unfiltered,
                args.skip_filtered,
                fixture_ids if fixture_ids else None,
                args.rate_limit_sleep,
                args.rate_limit_retries,
            )
        )

    print("Summary:")
    for summary in summaries:
        league_id = summary["league_id"]
        window = summary["window"]
        fixtures_count = summary["fixtures_count"]
        unfiltered = summary["unfiltered_counts"]
        filtered = summary["filtered_counts"]
        print(
            f"league={league_id} window={window} fixtures={fixtures_count} "
            f"unfiltered team_shots={unfiltered['team_shots']} match_shots={unfiltered['match_shots']} "
            f"filtered team_shots={filtered['team_shots']} match_shots={filtered['match_shots']}"
        )


if __name__ == "__main__":
    main()
