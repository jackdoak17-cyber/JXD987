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
  "scripts/export_odds_to_supabase_psql.py"

export REPO_ROOT
export ODDS_LEAGUES="${ODDS_LEAGUE_IDS:-$(odds_league_csv)}"
export DAYS_FORWARD="${ODDS_DAYS_FORWARD:-14}"
export ODDS_BOOKMAKERS="${ODDS_BOOKMAKERS:-Bet365,Paddy Power,Unibet,BetMGM,Betfair Exchange}"
export ODDS_FAST_INGEST_DAYS_BACK="${ODDS_FAST_INGEST_DAYS_BACK:-1}"
export ODDS_FAST_INGEST_DAYS_FORWARD="${ODDS_FAST_INGEST_DAYS_FORWARD:-1}"
export ODDS_FAST_INGEST_MAX_RUNTIME_MINUTES="${ODDS_FAST_INGEST_MAX_RUNTIME_MINUTES:-10}"

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
  --priority p1 \
  --bookmakers "${ODDS_BOOKMAKERS}" \
  --report-out "/tmp/odds_sync_report_p1.json" \
  --unmatched-out "/tmp/unmatched_players_p1.json"

# P1 is the near-kickoff refresh. Publish its newly fetched prices immediately;
# waiting for P3 leaves the website stale even though SQLite has the latest odds.
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
  --report-out "/tmp/odds_ingest_report_p1.json"
CHAIN
)

status=0
RUN_STARTED_AT="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
RUN_STARTED_EPOCH="$(date -u +"%s")"
run_with_global_lock_and_timeout "${CHAIN_COMMAND}" || status=$?
RUN_FINISHED_AT="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
RUN_FINISHED_EPOCH="$(date -u +"%s")"
record_pipeline_job_run \
  "run_p1" \
  "P1 odds fetch" \
  "${status}" \
  "${RUN_STARTED_AT}" \
  "${RUN_FINISHED_AT}" \
  "$(((RUN_FINISHED_EPOCH - RUN_STARTED_EPOCH) * 1000))"
finalize_with_healthcheck "${status}" "${HEALTHCHECK_PING_URL_P1:-${HEALTHCHECK_PING_URL:-}}"
