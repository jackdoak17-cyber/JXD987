#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=./common.sh
source "${SCRIPT_DIR}/common.sh"
verify_runtime_manifest_or_exit "$0"
require_runtime_manifest_entries_or_exit "$0" \
  "config/league_ids.txt" \
  "config/odds_api_leagues.json" \
  "jxd/__init__.py" \
  "jxd/db.py" \
  "jxd/models.py" \
  "jxd/odds_api_client.py" \
  "jxd/sportmonks_client.py" \
  "jxd/sync.py" \
  "scripts/sync_odds.py" \
  "scripts/export_odds_to_supabase_psql.py" \
  "scripts/sync_confirmed_lineups.py"

export REPO_ROOT
export STATS_LEAGUES="${STATS_LEAGUE_IDS:-${LEAGUE_IDS:-$(default_league_csv)}}"
export ODDS_LEAGUES="${ODDS_LEAGUE_IDS:-$(odds_league_csv)}"
export DAYS_FORWARD="${ODDS_DAYS_FORWARD:-14}"
export ODDS_BOOKMAKERS="${ODDS_BOOKMAKERS:-Bet365,Paddy Power,Unibet,BetMGM,Betfair Exchange}"
export ODDS_FAST_INGEST_DAYS_BACK="${ODDS_FAST_INGEST_DAYS_BACK:-1}"
export ODDS_FAST_INGEST_DAYS_FORWARD="${ODDS_FAST_INGEST_DAYS_FORWARD:-1}"
export ODDS_FAST_INGEST_MAX_RUNTIME_MINUTES="${ODDS_FAST_INGEST_MAX_RUNTIME_MINUTES:-10}"
export LINEUP_SYNC_HOURS_BACK="${LINEUP_SYNC_HOURS_BACK:-2}"
export LINEUP_SYNC_HOURS_FORWARD="${LINEUP_SYNC_HOURS_FORWARD:-3}"
export LINEUP_SYNC_LIMIT="${LINEUP_SYNC_LIMIT:-40}"

CHAIN_COMMAND=$(cat <<'CHAIN'
set -euo pipefail

cd "${REPO_ROOT}"
source .venv/bin/activate
export PYTHONPATH="${REPO_ROOT}"

if [[ -f ./.env ]]; then
  set -a
  source ./.env
  set +a
fi

python scripts/sync_odds.py \
  --leagues "${ODDS_LEAGUES}" \
  --days-forward "${DAYS_FORWARD}" \
  --priority p2 \
  --bookmakers "${ODDS_BOOKMAKERS}" \
  --report-out "/tmp/odds_sync_report_p2.json" \
  --unmatched-out "/tmp/unmatched_players_p2.json"

# P2 covers the 2–24 hour window. Export it here so a price becomes visible
# as soon as the provider releases it, rather than waiting for P3.
python scripts/export_odds_to_supabase_psql.py \
  --leagues "${ODDS_LEAGUES}" \
  --days-back "${ODDS_FAST_INGEST_DAYS_BACK}" \
  --days-forward "${ODDS_FAST_INGEST_DAYS_FORWARD}" \
  --no-include-fixture-leagues \
  --max-runtime-minutes "${ODDS_FAST_INGEST_MAX_RUNTIME_MINUTES}" \
  --skip-coverage \
  --skip-verification \
  --skip-retention \
  --skip-retention-snapshots \
  --report-out "/tmp/odds_ingest_report_p2.json"

# Best-effort confirmed-lineup refresh for imminent fixtures.
if [[ -n "${SPORTMONKS_API_TOKEN:-}" && -n "${SUPABASE_URL:-}" && -n "${SUPABASE_SERVICE_ROLE_KEY:-}" ]]; then
  if ! python scripts/sync_confirmed_lineups.py \
    --leagues "${STATS_LEAGUES}" \
    --hours-back "${LINEUP_SYNC_HOURS_BACK}" \
    --hours-forward "${LINEUP_SYNC_HOURS_FORWARD}" \
    --limit "${LINEUP_SYNC_LIMIT}"; then
    echo "Confirmed lineup refresh failed; continuing odds pipeline" >&2
  fi
else
  echo "Skipping confirmed lineup refresh; missing SportMonks or Supabase REST env" >&2
fi
CHAIN
)

status=0
RUN_STARTED_AT="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
RUN_STARTED_EPOCH="$(date -u +"%s")"
run_with_global_lock_and_timeout "${CHAIN_COMMAND}" || status=$?
RUN_FINISHED_AT="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
RUN_FINISHED_EPOCH="$(date -u +"%s")"
record_pipeline_job_run \
  "run_p2" \
  "P2 reconciliation" \
  "${status}" \
  "${RUN_STARTED_AT}" \
  "${RUN_FINISHED_AT}" \
  "$(((RUN_FINISHED_EPOCH - RUN_STARTED_EPOCH) * 1000))"
finalize_with_healthcheck "${status}" "${HEALTHCHECK_PING_URL_P2:-${HEALTHCHECK_PING_URL:-}}"
