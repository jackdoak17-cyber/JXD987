#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=./common.sh
source "${SCRIPT_DIR}/common.sh"
verify_runtime_manifest_or_exit "$0"
require_runtime_manifest_entries_or_exit "$0" \
  "config/league_ids.txt" \
  "config/odds_api_sync_excluded_leagues.json" \
  "jxd/__init__.py" \
  "jxd/db.py" \
  "jxd/models.py" \
  "jxd/sportmonks_client.py" \
  "jxd/sync.py" \
  "scripts/reconcile_recent_fixtures.py" \
  "scripts/export_to_supabase.py" \
  "scripts/refresh_fixture_delivery.py" \
  "scripts/postmatch_fixture_detail_delivery.py"

if [[ -f "${REPO_ROOT}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${REPO_ROOT}/.env"
  set +a
fi

export REPO_ROOT
export STATS_LEAGUES="${FIXTURE_LEAGUE_IDS:-${STATS_LEAGUE_IDS:-$(supported_league_csv)}}"
validate_supported_leagues "${STATS_LEAGUES}"
export SETTLEMENT_HOURS_BACK="${SETTLEMENT_HOURS_BACK:-48}"
export SETTLEMENT_MAX_RUNTIME_SECONDS="${SETTLEMENT_MAX_RUNTIME_SECONDS:-1200}"
export SETTLEMENT_EXPORT_DAYS_BACK="${SETTLEMENT_EXPORT_DAYS_BACK:-2}"
export SETTLEMENT_DELIVERY_DAYS_BACK="${SETTLEMENT_DELIVERY_DAYS_BACK:-2}"
export SETTLEMENT_DELIVERY_DAYS_FORWARD="${SETTLEMENT_DELIVERY_DAYS_FORWARD:-2}"
export POSTMATCH_DETAIL_HOURS_BACK="${POSTMATCH_DETAIL_HOURS_BACK:-72}"
export POSTMATCH_DETAIL_LIMIT="${POSTMATCH_DETAIL_LIMIT:-25}"
export POSTMATCH_DETAIL_GRACE_MINUTES="${POSTMATCH_DETAIL_GRACE_MINUTES:-60}"
export PIPELINE_EVIDENCE_FILE="${PIPELINE_EVIDENCE_FILE:-/tmp/postmatch_fixture_detail_delivery_report.json}"
export FIXTURE_SETTLEMENT_LOCK_FILE="${FIXTURE_SETTLEMENT_LOCK_FILE:-/var/lock/fixture-settlement.lock}"

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

python scripts/refresh_fixture_delivery.py \
  --start-date "$(date -u -d "-${SETTLEMENT_DELIVERY_DAYS_BACK} days" +%F)" \
  --end-date "$(date -u -d "+${SETTLEMENT_DELIVERY_DAYS_FORWARD} days" +%F)" \
  --leagues "${STATS_LEAGUES}" \
  --report-out "/tmp/postmatch_settlement_delivery.json"

python scripts/postmatch_fixture_detail_delivery.py \
  --leagues "${STATS_LEAGUES}" \
  --hours-back "${POSTMATCH_DETAIL_HOURS_BACK}" \
  --limit "${POSTMATCH_DETAIL_LIMIT}" \
  --grace-minutes "${POSTMATCH_DETAIL_GRACE_MINUTES}" \
  --report-json "/tmp/postmatch_fixture_detail_delivery_report.json"
CHAIN
)

status=0
RUN_STARTED_AT="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
RUN_STARTED_EPOCH="$(date -u +"%s")"
ODDS_SYNC_LOCK_FILE="${FIXTURE_SETTLEMENT_LOCK_FILE}" \
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
