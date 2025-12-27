#!/usr/bin/env bash
set -euo pipefail

DB_URL="${SUPABASE_DB_URL:?SUPABASE_DB_URL not set}"
MIN_DISTINCT="${ODDS_COVERAGE_MIN_DISTINCT:-20}"
MIN_PCT="${ODDS_PLAYER_MAPPING_MIN_PCT:-80}"

distinct_sot=$(psql "$DB_URL" -At -v ON_ERROR_STOP=1 -c "
with fixtures_in_range as (
  select id
  from public.fixtures
  where league_id = 8
    and (starting_at at time zone 'Europe/London')::date >= current_date
    and (starting_at at time zone 'Europe/London')::date < (current_date + interval '14 days')
)
select count(distinct o.participant_id)
from public.odds_outcomes o
join fixtures_in_range f on f.id = o.fixture_id
where o.participant_type = 'player'
  and o.market_key = 'player_shots_on_target'
  and o.line = 0.5
  and o.participant_id is not null;
")

mapping_pct=$(psql "$DB_URL" -At -v ON_ERROR_STOP=1 -c "
with fixtures_in_range as (
  select id
  from public.fixtures
  where league_id = 8
    and (starting_at at time zone 'Europe/London')::date >= current_date
    and (starting_at at time zone 'Europe/London')::date < (current_date + interval '14 days')
), scoped as (
  select o.participant_id
  from public.odds_outcomes o
  join fixtures_in_range f on f.id = o.fixture_id
  where o.participant_type = 'player'
)
select coalesce(round(100.0 * count(*) filter (where participant_id is not null) / nullif(count(*),0), 2), 0)
from scoped;
")

cat > /tmp/odds_coverage_report.json <<JSON
{
  "window_days": 14,
  "distinct_sot_05_players": ${distinct_sot:-0},
  "mapped_pct_player_outcomes": ${mapping_pct:-0},
  "thresholds": {
    "distinct_min": ${MIN_DISTINCT},
    "mapped_pct_min": ${MIN_PCT}
  }
}
JSON

python3 - <<PY
import json, os, sys

with open("/tmp/odds_coverage_report.json", "r") as f:
    report = json.load(f)

distinct_val = float(report.get("distinct_sot_05_players", 0) or 0)
mapped_pct = float(report.get("mapped_pct_player_outcomes", 0) or 0)
min_distinct = float(report.get("thresholds", {}).get("distinct_min", 0))
min_pct = float(report.get("thresholds", {}).get("mapped_pct_min", 0))

print("Odds coverage report:", json.dumps(report))

if distinct_val < min_distinct:
    print(f"FAIL: distinct_sot_05_players {distinct_val} < {min_distinct}")
    sys.exit(1)

if mapped_pct < min_pct:
    print(f"FAIL: mapped_pct_player_outcomes {mapped_pct} < {min_pct}")
    sys.exit(1)
PY
