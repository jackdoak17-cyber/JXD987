#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=./common.sh
source "${SCRIPT_DIR}/common.sh"

if [[ -f "${REPO_ROOT}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${REPO_ROOT}/.env"
  set +a
fi

export REPO_ROOT
export STATS_LEAGUES="${STATS_LEAGUE_IDS:-${LEAGUE_IDS:-$(default_league_csv)}}"
export SETTLEMENT_HOURS_BACK="${SETTLEMENT_HOURS_BACK:-48}"
export SETTLEMENT_MAX_RUNTIME_SECONDS="${SETTLEMENT_MAX_RUNTIME_SECONDS:-900}"
export SETTLEMENT_EXPORT_DAYS_BACK="${SETTLEMENT_EXPORT_DAYS_BACK:-2}"

CHAIN_COMMAND=$(cat <<'CHAIN'
set -euo pipefail

cd "${REPO_ROOT}"
source .venv/bin/activate
export PYTHONPATH="${REPO_ROOT}"

python scripts/reconcile_recent_fixtures.py \
  --leagues "${STATS_LEAGUES}" \
  --completed-hours-back "${SETTLEMENT_HOURS_BACK}" \
  --report-json "/tmp/postmatch_settlement_reconcile.json"

python scripts/export_to_supabase.py \
  --strict \
  --leagues "${STATS_LEAGUES}" \
  --days-back "${SETTLEMENT_EXPORT_DAYS_BACK}" \
  --upcoming-days 0 \
  --fixture-core-only \
  --skip-prune \
  --report-json "/tmp/postmatch_settlement_export.json"
CHAIN
)

status=0
RUN_STARTED_AT="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
RUN_STARTED_EPOCH="$(date -u +"%s")"
ODDS_SYNC_P3_MAX_DURATION_SECONDS="${SETTLEMENT_MAX_RUNTIME_SECONDS}" \
  run_with_global_lock_and_timeout "${CHAIN_COMMAND}" || status=$?
RUN_FINISHED_AT="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
RUN_FINISHED_EPOCH="$(date -u +"%s")"
record_pipeline_job_run \
  "run_postmatch_settlement" \
  "Post-match settlement" \
  "${status}" \
  "${RUN_STARTED_AT}" \
  "${RUN_FINISHED_AT}" \
  "$(((RUN_FINISHED_EPOCH - RUN_STARTED_EPOCH) * 1000))"

finalize_with_healthcheck "${status}" "${HEALTHCHECK_PING_URL_SETTLEMENT:-${HEALTHCHECK_PING_URL:-}}"
