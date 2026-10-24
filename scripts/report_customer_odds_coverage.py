#!/usr/bin/env python3
"""Read-only customer-eligibility report, separate from provider/pipeline success.

Never calls Odds-API and never writes production data. Queries the current
published Fixtures release, not the canonical fixture or bookmaker universe.
A provider gap is counted as unavailable customer coverage, even if a pipeline
validator correctly classified it as a non-actionable upstream gap.
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor

from scripts.validate_moneyline_coverage import get_db_url

QUERY = """
with publication as (
  select p.release_id,r.published_at
  from public.fixture_delivery_current_publication p
  join public.fixture_delivery_releases r on r.id=p.release_id
  where p.publication_key='fixtures' and r.status='published'
),
scope as (
  select s.release_id,s.fixture_id,s.league_id,s.starting_at
  from public.fixture_delivery_schedule s
  join publication p on p.release_id=s.release_id
  where s.starting_at > now() and s.starting_at <= now()+interval '14 days'
    and s.league_id not in (24,27,109,307,390,570)
),
quotes as (
  select d.fixture_id,d.selection_key,d.bookmaker_id,
    v.last_verified_at,
    case
      when s.starting_at <= now()+interval '2 hours' then interval '2 hours'
      when s.starting_at <= now()+interval '24 hours' then interval '3 hours'
      else interval '8 hours'
    end as max_age
  from scope s
  join public.fixture_delivery_odds d
    on d.release_id=s.release_id and d.fixture_id=s.fixture_id
    and d.market_key='moneyline' and d.bookmaker_id in (2,4,5,8)
    and d.selection_key in ('home','draw','away')
    and d.price_decimal>1 and d.price_decimal<=500
  left join public.odds_quote_verifications v
    on v.fixture_id=d.fixture_id and v.bookmaker_id=d.bookmaker_id
    and v.market_key=d.market_key and v.selection_key=d.selection_key
    and v.participant_type=coalesce(d.participant_type,'team')
    and v.participant_id=coalesce(d.participant_id,0)
    and v.line_key=coalesce(d.line,-9999)
    and v.price_decimal=d.price_decimal
    and v.source_last_updated_at is not distinct from d.source_last_updated_at
),
coverage as (
  select s.fixture_id,s.league_id,
    count(q.bookmaker_id)>0 as any_stored,
    bool_or(q.selection_key='home' and q.last_verified_at >= now()-q.max_age
      and q.last_verified_at <= now()+interval '2 minutes') as home_verified,
    bool_or(q.selection_key='draw' and q.last_verified_at >= now()-q.max_age
      and q.last_verified_at <= now()+interval '2 minutes') as draw_verified,
    bool_or(q.selection_key='away' and q.last_verified_at >= now()-q.max_age
      and q.last_verified_at <= now()+interval '2 minutes') as away_verified
  from scope s left join quotes q on q.fixture_id=s.fixture_id
  group by s.fixture_id,s.league_id
)
select c.league_id,count(*)::integer as fixtures,
  count(*) filter(where c.any_stored)::integer as with_stored_odds,
  count(*) filter(where coalesce(c.home_verified,false)
     or coalesce(c.draw_verified,false) or coalesce(c.away_verified,false))::integer as with_any_verified_price,
  count(*) filter(where coalesce(c.home_verified,false)
     and coalesce(c.draw_verified,false) and coalesce(c.away_verified,false))::integer as with_complete_verified_moneyline,
  count(*) filter(where not c.any_stored)::integer as without_provider_prices,
  count(*) filter(where c.any_stored and not(
     coalesce(c.home_verified,false) or coalesce(c.draw_verified,false)
     or coalesce(c.away_verified,false)))::integer as with_prices_but_no_current_verification,
  (select min(p.published_at) from publication p) as published_at,
  now() as checked_at
from coverage c group by c.league_id order by c.league_id
"""

def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    fields = ("fixtures", "with_stored_odds", "with_any_verified_price",
              "with_complete_verified_moneyline", "without_provider_prices",
              "with_prices_but_no_current_verification")
    totals = {key: sum(int(row.get(key) or 0) for row in rows) for key in fields}
    fixtures = totals["fixtures"]
    def pct(value: int) -> float:
        return round(100.0 * value / fixtures, 2) if fixtures else 0.0
    return {
        "totals": totals,
        "customer_any_price_coverage_pct": pct(totals["with_any_verified_price"]),
        "customer_complete_moneyline_coverage_pct": pct(totals["with_complete_verified_moneyline"]),
        "published_price_coverage_pct": pct(totals["with_stored_odds"]),
        "by_league": [{**{k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in row.items()},
                       "customer_complete_pct": round(100.0 * int(row.get("with_complete_verified_moneyline") or 0)
                                                     / int(row["fixtures"]), 2) if int(row["fixtures"]) else 0.0}
                      for row in rows],
    }

def provider_summary(path: str, checked_at: datetime) -> dict[str, Any]:
    if not path:
        return {"state": "not_supplied"}
    try:
        report = json.loads(Path(path).read_text(encoding="utf-8"))
        generated_at = datetime.fromisoformat(str(report["generated_at"]).replace("Z", "+00:00"))
        if generated_at.tzinfo is None:
            generated_at = generated_at.replace(tzinfo=timezone.utc)
        age_hours = (checked_at - generated_at).total_seconds() / 3600
        if age_hours < -0.25 or age_hours > 6:
            return {"state": "stale", "generated_at": generated_at.isoformat(), "age_hours": round(age_hours, 2)}
        statuses: dict[str, int] = {}
        for row in report.get("moneyline_coverage", []):
            status = str(row.get("matching_status", "missing"))
            response = str(row.get("odds_response_status", "unknown"))
            key = status + ":" + response
            statuses[key] = statuses.get(key, 0) + 1
        return {"state": "observed", "generated_at": generated_at.isoformat(),
                "age_hours": round(age_hours, 2), "fixture_evidence": sum(statuses.values()),
                "by_matching_and_response": statuses, "errors": len(report.get("errors") or [])}
    except (OSError, ValueError, TypeError, KeyError) as exc:
        return {"state": "invalid", "detail": type(exc).__name__}

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--provider-report", default="")
    parser.add_argument("--out-json", default="")
    args = parser.parse_args()
    with psycopg2.connect(get_db_url(), connect_timeout=10) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(QUERY)
            rows = [dict(row) for row in cursor.fetchall()]
    output = summarize(rows)
    checked_at = datetime.now(timezone.utc)
    published = rows[0].get("published_at") if rows else None
    output["checked_at"] = checked_at.isoformat()
    output["publication"] = {
        "published_at": published.isoformat() if published else None,
        "age_minutes": round((checked_at - published).total_seconds() / 60, 1) if published else None,
    }
    output["provider"] = provider_summary(args.provider_report, checked_at)
    body = json.dumps(output, indent=2, sort_keys=True)
    if args.out_json:
        path = Path(args.out_json)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body + "\n", encoding="utf-8")
    print(body)

if __name__ == "__main__":
    main()
