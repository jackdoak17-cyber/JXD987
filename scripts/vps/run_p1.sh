#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=./common.sh
source "${SCRIPT_DIR}/common.sh"
verify_runtime_manifest_or_exit "$0"
require_runtime_manifest_entries_or_exit "$0" \
  "config/league_ids.txt" \
  "config/odds_api_leagues.json" \
  "config/odds_api_sync_excluded_leagues.json" \
  "config/odds_api_bookmakers.json" \
  "jxd/__init__.py" \
  "jxd/db.py" \
  "jxd/models.py" \
  "jxd/odds_api_client.py" \
  "jxd/sportmonks_client.py" \
  "jxd/sync.py" \
  "scripts/sync_odds.py"

export REPO_ROOT
export ODDS_LEAGUES="${ODDS_LEAGUE_IDS:-$(odds_league_csv)}"
export DAYS_FORWARD="${ODDS_DAYS_FORWARD:-14}"
export ODDS_BOOKMAKERS="${ODDS_BOOKMAKERS:-$(odds_bookmaker_csv)}"
# A cron tick is a scheduling opportunity, not proof that the shared writer
# lock was available.  Retry the bounded, idempotent P1 chain across a
# settlement handoff so a long detail delivery cannot starve odds ingestion.
# The retry budget is finite: a persistent lock or repeated lease handoff is
# still recorded as skipped and remains visible to Operations.
export ODDS_SYNC_LOCK_RETRY_ATTEMPTS="${ODDS_SYNC_LOCK_RETRY_ATTEMPTS:-${ODDS_SYNC_P1_LOCK_RETRY_ATTEMPTS:-60}}"
export ODDS_SYNC_LOCK_RETRY_DELAY_SECONDS="${ODDS_SYNC_LOCK_RETRY_DELAY_SECONDS:-${ODDS_SYNC_P1_LOCK_RETRY_DELAY_SECONDS:-15}}"

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
CHAIN
)

status=0
run_recorded_pipeline_job \
  "run_p1" \
  "P1 odds fetch" \
  "${CHAIN_COMMAND}" \
  "/tmp/odds_sync_report_p1.json" || status=$?
finalize_with_healthcheck "${status}" "${HEALTHCHECK_PING_URL_P1:-${HEALTHCHECK_PING_URL:-}}"
