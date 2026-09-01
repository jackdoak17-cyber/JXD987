#!/usr/bin/env python3
"""
Validate complete moneyline coverage for upcoming visible fixtures in Supabase.

This script is intended to run after odds ingest. It checks the same class of
home/draw/away prices the fixtures page needs and fails when a league drops
below a configured coverage threshold inside the user-facing window.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor


MONEYLINE_MARKET_KEYS = [
    "moneyline",
    "match_result",
    "match_winner",
    "match_winner_90",
    "match_winner_90_min",
    "full_time_result",
    "full_time_result_90",
    "1x2",
    "home_draw_away",
]

HIDDEN_FIXTURE_STATUSES = [
    "POSTP",
    "POSTPONED",
    "CANCL",
    "CANCELLED",
    "CANCELED",
    "ABANDONED",
    "SUSPENDED",
    "INTERRUPTED",
]

DEFAULT_EXCLUDED_LEAGUE_IDS = {24, 27, 109, 307, 390, 570}


def load_excluded_league_ids() -> List[int]:
    """Return competitions excluded from paid Odds-API ingestion."""
    path = Path(__file__).resolve().parent.parent / "config" / "odds_api_sync_excluded_leagues.json"
    if not path.exists():
        return sorted(DEFAULT_EXCLUDED_LEAGUE_IDS)
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise SystemExit(f"Expected a JSON array in {path}")
    return sorted({int(value) for value in raw})


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def get_db_url() -> str:
    db_url = os.environ.get("SUPABASE_DB_URL") or os.environ.get("SUPABASE_DB_URL_SESSION")
    if db_url:
        return db_url
    fallback = Path("/tmp/supabase_db_url")
    if fallback.exists():
        return fallback.read_text(encoding="utf-8").strip()
    raise SystemExit("Missing SUPABASE_DB_URL (or /tmp/supabase_db_url)")


def parse_league_ids(raw: str) -> List[int]:
    return [int(value) for value in raw.split(",") if value.strip()]


def parse_report_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def load_provider_evidence(
    report_paths: List[str],
    max_age_hours: float,
    now: datetime | None = None,
) -> Tuple[Dict[int, Dict[str, object]], List[str]]:
    """Load fresh, compact per-fixture evidence emitted by sync_odds.py.

    Evidence is intentionally fail-closed.  A validator must never turn an
    absent, malformed, stale, or conflicting report into an accepted provider
    gap.  When multiple reports contain a fixture, the newest report wins only
    when the evidence is identical; conflicting observations are rejected.
    """
    evidence_by_fixture: Dict[int, Dict[str, object]] = {}
    errors: List[str] = []
    observed_at = now or datetime.now(timezone.utc)
    if observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=timezone.utc)
    observed_at = observed_at.astimezone(timezone.utc)

    for raw_path in report_paths:
        path = Path(raw_path)
        if not path.exists():
            errors.append(f"provider evidence report is missing: {path}")
            continue
        try:
            report = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            errors.append(f"provider evidence report cannot be read: {path}: {exc}")
            continue
        if not isinstance(report, dict):
            errors.append(f"provider evidence report is not an object: {path}")
            continue

        generated_at = parse_report_timestamp(report.get("generated_at"))
        if generated_at is None:
            errors.append(f"provider evidence report has no valid generated_at: {path}")
            continue
        age_hours = (observed_at - generated_at).total_seconds() / 3600
        if age_hours > max_age_hours:
            errors.append(
                f"provider evidence report is stale ({age_hours:.2f}h > {max_age_hours:.2f}h): {path}"
            )
            continue
        if age_hours < -0.25:
            errors.append(f"provider evidence report is from the future: {path}")
            continue

        rows = report.get("moneyline_coverage")
        if not isinstance(rows, list):
            errors.append(f"provider evidence report has no moneyline_coverage array: {path}")
            continue

        for row in rows:
            if not isinstance(row, dict):
                errors.append(f"provider evidence report contains a non-object row: {path}")
                continue
            try:
                fixture_id = int(row["fixture_id"])
            except (KeyError, TypeError, ValueError):
                errors.append(f"provider evidence row has no valid fixture_id: {path}")
                continue
            if fixture_id <= 0:
                errors.append(f"provider evidence row has invalid fixture_id={fixture_id}: {path}")
                continue

            normalized = dict(row)
            normalized["fixture_id"] = fixture_id
            normalized["_generated_at"] = generated_at.isoformat()
            normalized["_report_path"] = str(path)
            existing = evidence_by_fixture.get(fixture_id)
            if existing is None:
                evidence_by_fixture[fixture_id] = normalized
                continue

            comparable_existing = {
                key: value
                for key, value in existing.items()
                if not key.startswith("_")
            }
            comparable_new = {
                key: value
                for key, value in normalized.items()
                if not key.startswith("_")
            }
            if comparable_existing != comparable_new:
                errors.append(
                    f"conflicting provider evidence for fixture {fixture_id}: "
                    f"{existing.get('_report_path')} vs {path}"
                )
                evidence_by_fixture.pop(fixture_id, None)
                continue

            existing_generated = parse_report_timestamp(existing.get("_generated_at"))
            if existing_generated is None or generated_at > existing_generated:
                evidence_by_fixture[fixture_id] = normalized

    return evidence_by_fixture, errors


def fetch_league_coverage(
    conn,
    league_ids: List[int],
    days_forward: int,
    min_price: float,
    max_price: float,
) -> List[Dict[str, object]]:
    excluded_league_ids = load_excluded_league_ids()
    league_ids = [league_id for league_id in league_ids if league_id not in excluded_league_ids]
    league_filter = ""
    params: List[object] = [f"{days_forward} days"]
    if league_ids:
        league_filter = "and f.league_id = any(%s)"
        params.append(league_ids)

    params.extend(
        [
            excluded_league_ids,
            HIDDEN_FIXTURE_STATUSES,
            HIDDEN_FIXTURE_STATUSES,
            min_price,
            max_price,
            min_price,
            max_price,
            MONEYLINE_MARKET_KEYS,
        ]
    )

    query = f"""
with visible_fixtures as (
  select
    f.id,
    f.league_id,
    f.starting_at,
    f.home_team_id,
    f.away_team_id
  from public.fixtures f
  where f.starting_at >= (now() at time zone 'utc')
    and f.starting_at < (now() at time zone 'utc') + interval %s
    {league_filter}
    and f.league_id <> all(%s)
    and upper(regexp_replace(coalesce(f.status, ''), '[^A-Z0-9]+', '_', 'g')) <> all(%s)
    and upper(regexp_replace(coalesce(f.status_code, ''), '[^A-Z0-9]+', '_', 'g')) <> all(%s)
), moneyline_by_fixture as (
  select
    o.fixture_id,
    bool_or(
      o.participant_type = 'team'
      and o.participant_id = vf.home_team_id
      and o.price_decimal > %s
      and o.price_decimal <= %s
    ) as has_home,
    bool_or(
      o.participant_type = 'team'
      and o.participant_id = vf.away_team_id
      and o.price_decimal > %s
      and o.price_decimal <= %s
    ) as has_away,
    bool_or(
      (
        coalesce(o.participant_type, 'match') = 'match'
        and lower(regexp_replace(coalesce(o.selection_key, ''), '[^a-z0-9]+', '_', 'g')) in ('draw', 'x')
      )
      or lower(coalesce(o.selection_key, '')) like '%%draw%%'
    ) as has_draw
  from public.odds_outcomes o
  join visible_fixtures vf on vf.id = o.fixture_id
  where o.market_key = any(%s)
  group by o.fixture_id
), fixture_coverage as (
  select
    vf.league_id,
    vf.id as fixture_id,
    vf.starting_at,
    coalesce(m.has_home, false) as has_home,
    coalesce(m.has_draw, false) as has_draw,
    coalesce(m.has_away, false) as has_away,
    (
      coalesce(m.has_home, false)
      and coalesce(m.has_draw, false)
      and coalesce(m.has_away, false)
    ) as has_complete_moneyline
  from visible_fixtures vf
  left join moneyline_by_fixture m on m.fixture_id = vf.id
)
select
  league_id,
  count(*)::bigint as fixtures_in_window,
  count(*) filter (where has_complete_moneyline)::bigint as fixtures_with_complete_moneyline,
  coalesce(
    round(
      100.0 * count(*) filter (where has_complete_moneyline)
      / nullif(count(*), 0),
      2
    ),
    0
  ) as coverage_pct,
  coalesce(
    array_agg(fixture_id order by starting_at) filter (where not has_complete_moneyline),
    array[]::bigint[]
  ) as missing_fixture_ids,
  min(starting_at) filter (where not has_complete_moneyline) as first_missing_starting_at
from fixture_coverage
group by league_id
order by league_id;
"""

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, params)
        rows = cur.fetchall()

    normalized: List[Dict[str, object]] = []
    for row in rows:
        missing_fixture_ids = [int(value) for value in (row.get("missing_fixture_ids") or [])]
        first_missing = row.get("first_missing_starting_at")
        normalized.append(
            {
                "league_id": int(row["league_id"]),
                "fixtures_in_window": int(row["fixtures_in_window"] or 0),
                "fixtures_with_complete_moneyline": int(row["fixtures_with_complete_moneyline"] or 0),
                "coverage_pct": float(row["coverage_pct"] or 0),
                "missing_fixture_ids": missing_fixture_ids,
                "first_missing_starting_at": first_missing.isoformat() if first_missing else None,
            }
        )
    return normalized


def evaluate_failures(leagues: List[Dict[str, object]], fail_below_pct: float) -> List[Dict[str, object]]:
    failures: List[Dict[str, object]] = []
    for league in leagues:
        fixtures_in_window = int(league.get("fixtures_in_window") or 0)
        coverage_pct = float(league.get("coverage_pct") or 0)
        if fixtures_in_window == 0:
            continue
        if coverage_pct + 1e-9 < fail_below_pct:
            failures.append(
                {
                    "league_id": int(league["league_id"]),
                    "fixtures_in_window": fixtures_in_window,
                    "fixtures_with_complete_moneyline": int(
                        league.get("fixtures_with_complete_moneyline") or 0
                    ),
                    "coverage_pct": coverage_pct,
                    "missing_fixture_ids": list(league.get("missing_fixture_ids") or []),
                    "first_missing_starting_at": league.get("first_missing_starting_at"),
                }
            )
    return failures


def classify_provider_evidence(
    fixture_id: int,
    evidence: Dict[int, Dict[str, object]],
) -> Tuple[str, str, Dict[str, object] | None]:
    """Classify one database-missing fixture using provider facts.

    Only a matched event with a valid odds response and zero usable supported
    moneyline bookmakers is an accepted provider gap.  Everything else is a
    hard failure category so matcher, transport, and ingestion defects remain
    visible.
    """
    row = evidence.get(fixture_id)
    if row is None:
        return (
            "EVIDENCE_MISSING",
            "no fresh provider evidence was supplied for this fixture",
            None,
        )
    if row.get("matching_status") != "matched":
        return (
            "UPSTREAM_UNMATCHED",
            "the canonical provider run did not match an upstream event to this fixture",
            row,
        )
    try:
        event_id = int(row.get("event_id"))
    except (TypeError, ValueError):
        return (
            "EVIDENCE_INVALID",
            "matched provider evidence did not contain a valid event_id",
            row,
        )
    if event_id <= 0:
        return (
            "EVIDENCE_INVALID",
            "matched provider evidence did not contain a positive event_id",
            row,
        )
    if row.get("odds_response_status") != "received":
        return (
            "ODDS_RESPONSE_INCOMPLETE",
            "the matched event did not have a valid, returned odds response",
            row,
        )
    supported = row.get("supported_moneyline_bookmakers")
    if not isinstance(supported, list) or not all(isinstance(value, str) for value in supported):
        return (
            "EVIDENCE_INVALID",
            "provider evidence did not contain a supported_moneyline_bookmakers list",
            row,
        )
    if supported:
        return (
            "PIPELINE_FAILURE",
            "the provider returned usable supported moneyline odds but the database is incomplete",
            row,
        )
    return (
        "PROVIDER_GAP",
        "the provider returned a valid event response with no usable supported moneyline odds",
        row,
    )


def evaluate_provider_aware_failures(
    leagues: List[Dict[str, object]],
    evidence: Dict[int, Dict[str, object]],
    fail_below_pct: float,
) -> Tuple[List[Dict[str, object]], List[Dict[str, object]], List[Dict[str, object]], List[Dict[str, object]]]:
    """Evaluate coverage while preserving hard failures and provider gaps."""
    failures: List[Dict[str, object]] = []
    provider_gaps: List[Dict[str, object]] = []
    pipeline_failures: List[Dict[str, object]] = []
    unresolved: List[Dict[str, object]] = []

    for league in leagues:
        fixtures_in_window = int(league.get("fixtures_in_window") or 0)
        if fixtures_in_window == 0:
            continue
        missing_fixture_ids = [int(value) for value in (league.get("missing_fixture_ids") or [])]
        hard_missing: List[int] = []
        provider_gap_ids: List[int] = []
        for fixture_id in missing_fixture_ids:
            classification, reason, row = classify_provider_evidence(fixture_id, evidence)
            audit = {
                "fixture_id": fixture_id,
                "league_id": int(league["league_id"]),
                "classification": classification,
                "reason": reason,
                "evidence": row,
            }
            if classification == "PROVIDER_GAP":
                provider_gap_ids.append(fixture_id)
                provider_gaps.append(audit)
            else:
                hard_missing.append(fixture_id)
                if classification == "PIPELINE_FAILURE":
                    pipeline_failures.append(audit)
                else:
                    unresolved.append(audit)

        effective_complete = fixtures_in_window - len(hard_missing)
        effective_coverage = 100.0 * effective_complete / fixtures_in_window
        if effective_coverage + 1e-9 < fail_below_pct:
            failures.append(
                {
                    "league_id": int(league["league_id"]),
                    "fixtures_in_window": fixtures_in_window,
                    "fixtures_with_complete_moneyline": int(
                        league.get("fixtures_with_complete_moneyline") or 0
                    ),
                    "coverage_pct": float(league.get("coverage_pct") or 0),
                    "effective_coverage_pct": effective_coverage,
                    "missing_fixture_ids": hard_missing,
                    "provider_gap_fixture_ids": provider_gap_ids,
                    "first_missing_starting_at": league.get("first_missing_starting_at"),
                }
            )

    return failures, provider_gaps, pipeline_failures, unresolved


def build_markdown_report(report: Dict[str, object]) -> str:
    lines = [
        "# Moneyline Coverage Report",
        "",
        f"Generated: {report['generated_at']}",
        f"Days forward: {report['days_forward']}",
        f"Fail below coverage %: {report['fail_below_pct']}",
        f"Provider evidence: {'enabled' if report.get('provider_evidence_enabled') else 'not supplied (strict database-only mode)'}",
        f"Status: {'PASS' if report['ok'] else 'FAIL'}",
        "",
        "| League | Fixtures | Complete moneyline | Coverage % | Effective % | Missing fixture IDs |",
        "|---|---:|---:|---:|---:|---|",
    ]
    for league in report["leagues"]:
        missing_fixture_ids = ", ".join(str(value) for value in league["missing_fixture_ids"]) or "-"
        lines.append(
            f"| {league['league_id']} | {league['fixtures_in_window']} | "
            f"{league['fixtures_with_complete_moneyline']} | {league['coverage_pct']:.2f} | "
            f"{float(league.get('effective_coverage_pct', league['coverage_pct'])):.2f} | "
            f"{missing_fixture_ids} |"
        )
    if report["failures"]:
        lines.extend(["", "## Failures", ""])
        for failure in report["failures"]:
            lines.append(
                f"- League {failure['league_id']}: "
                f"{failure['fixtures_with_complete_moneyline']}/{failure['fixtures_in_window']} "
                f"fixtures with complete moneyline "
                f"({failure['coverage_pct']:.2f}%)."
            )
    if report.get("provider_gaps"):
        lines.extend(["", "## Accepted provider gaps", ""])
        for gap in report["provider_gaps"]:
            lines.append(f"- Fixture {gap['fixture_id']} in league {gap['league_id']}: {gap['reason']}.")
    if report.get("unresolved"):
        lines.extend(["", "## Unresolved evidence", ""])
        for item in report["unresolved"]:
            lines.append(f"- Fixture {item['fixture_id']} in league {item['league_id']}: {item['reason']}.")
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--leagues", default="")
    parser.add_argument(
        "--days-forward",
        type=int,
        default=int(os.environ.get("MONEYLINE_COVERAGE_DAYS_FORWARD", "7")),
    )
    parser.add_argument(
        "--fail-below-pct",
        type=float,
        default=float(os.environ.get("MONEYLINE_COVERAGE_MIN_PCT", "100")),
    )
    parser.add_argument("--out-json", default="")
    parser.add_argument("--out-md", default="")
    parser.add_argument(
        "--provider-report",
        action="append",
        default=[],
        help="Fresh sync_odds JSON evidence report; may be supplied more than once.",
    )
    parser.add_argument(
        "--provider-report-max-age-hours",
        type=float,
        default=float(os.environ.get("MONEYLINE_PROVIDER_REPORT_MAX_AGE_HOURS", "6")),
    )
    args = parser.parse_args()

    db_url = get_db_url()
    league_ids = parse_league_ids(args.leagues)
    excluded_league_ids = load_excluded_league_ids()
    league_ids = [league_id for league_id in league_ids if league_id not in excluded_league_ids]
    min_price = float(os.environ.get("ODDS_MIN_PRICE", "1.0"))
    max_price = float(os.environ.get("ODDS_MAX_PRICE", "500"))

    conn = psycopg2.connect(db_url)
    try:
        leagues = fetch_league_coverage(conn, league_ids, args.days_forward, min_price, max_price)
    finally:
        conn.close()

    provider_evidence_enabled = bool(args.provider_report)
    provider_evidence: Dict[int, Dict[str, object]] = {}
    provider_evidence_errors: List[str] = []
    provider_gaps: List[Dict[str, object]] = []
    provider_pipeline_failures: List[Dict[str, object]] = []
    unresolved: List[Dict[str, object]] = []
    if provider_evidence_enabled:
        provider_evidence, provider_evidence_errors = load_provider_evidence(
            args.provider_report,
            args.provider_report_max_age_hours,
        )
        (
            failures,
            provider_gaps,
            provider_pipeline_failures,
            unresolved,
        ) = evaluate_provider_aware_failures(
            leagues,
            provider_evidence,
            args.fail_below_pct,
        )
    else:
        failures = evaluate_failures(leagues, args.fail_below_pct)

    if provider_evidence_errors:
        failures = list(failures)
        failures.append(
            {
                "league_id": None,
                "fixtures_in_window": 0,
                "fixtures_with_complete_moneyline": 0,
                "coverage_pct": 0,
                "effective_coverage_pct": 0,
                "missing_fixture_ids": [],
                "provider_gap_fixture_ids": [],
                "first_missing_starting_at": None,
                "reason": "provider evidence errors",
            }
        )
    report = {
        "generated_at": utc_now_iso(),
        "days_forward": args.days_forward,
        "fail_below_pct": args.fail_below_pct,
        "league_ids": league_ids,
        "excluded_league_ids": excluded_league_ids,
        "leagues": leagues,
        "failures": failures,
        "provider_evidence_enabled": provider_evidence_enabled,
        "provider_report_paths": args.provider_report,
        "provider_report_max_age_hours": args.provider_report_max_age_hours,
        "provider_evidence_fixture_count": len(provider_evidence),
        "provider_evidence_errors": provider_evidence_errors,
        "provider_gaps": provider_gaps,
        "provider_pipeline_failures": provider_pipeline_failures,
        "unresolved": unresolved,
        "ok": len(failures) == 0 and not provider_evidence_errors,
    }

    if args.out_json:
        Path(args.out_json).write_text(json.dumps(report, indent=2), encoding="utf-8")
    if args.out_md:
        Path(args.out_md).write_text(build_markdown_report(report), encoding="utf-8")

    if failures:
        raise SystemExit("moneyline coverage validation failed")


if __name__ == "__main__":
    main()
