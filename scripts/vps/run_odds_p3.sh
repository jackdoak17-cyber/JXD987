#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=./common.sh
source "${SCRIPT_DIR}/common.sh"
verify_runtime_manifest_or_exit "$0"
require_runtime_manifest_entries_or_exit "$0" \
  "config/fixture_core_contract.json" \
  "scripts/fixture_core_contract.py" \
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
  "scripts/sync_odds.py" \
  "scripts/export_odds_to_supabase_psql.py" \
  "scripts/odds_retention_psql.py" \
  "scripts/validate_moneyline_coverage.py"

export REPO_ROOT
if [[ -f "${REPO_ROOT}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${REPO_ROOT}/.env"
  set +a
fi

export FIXTURE_CORE_CONTRACT_PATH="${FIXTURE_CORE_CONTRACT_PATH:-${REPO_ROOT}/config/fixture_core_contract.json}"
contract_value() {
  python3 "${REPO_ROOT}/scripts/fixture_core_contract.py" \
    --contract "${FIXTURE_CORE_CONTRACT_PATH}" \
    --field "$1"
}

export ODDS_LEAGUES="${ODDS_LEAGUE_IDS:-$(odds_league_csv)}"
export SETTLED_HISTORY_DAYS="$(contract_value history_window_days)"
export ODDS_SYNC_DAYS_BACK="${ODDS_SYNC_DAYS_BACK:-0}"
export DAYS_FORWARD="$(contract_value odds_window_days)"
export ODDS_BOOKMAKERS="${ODDS_BOOKMAKERS:-$(odds_bookmaker_csv)}"
export INGEST_MAX_RUNTIME_MINUTES="${ODDS_INGEST_MAX_RUNTIME_MINUTES:-25}"
export ODDS_EXPORT_DAYS_BACK="${SETTLED_HISTORY_DAYS}"
export RETENTION_DAYS_BACK="${RETENTION_DAYS_BACK:-${SETTLED_HISTORY_DAYS}}"
export RETENTION_DAYS_FORWARD="$(contract_value odds_window_days)"
export RETENTION_SNAPSHOT_DAYS="${RETENTION_SNAPSHOT_DAYS:-30}"
export MONEYLINE_COVERAGE_DAYS_FORWARD="${MONEYLINE_COVERAGE_DAYS_FORWARD:-7}"
export MONEYLINE_COVERAGE_MIN_PCT="${MONEYLINE_COVERAGE_MIN_PCT:-100}"
export MONEYLINE_REPAIR_ATTEMPTS="${MONEYLINE_REPAIR_ATTEMPTS:-1}"
export PIPELINE_EVIDENCE_FILE="${PIPELINE_EVIDENCE_FILE:-/tmp/odds_ingest_report_p3.json}"

if ! [[ "${RETENTION_DAYS_BACK}" =~ ^[0-9]+$ ]]; then
  echo "RETENTION_DAYS_BACK must be a non-negative integer" >&2
  exit 1
fi
if (( RETENTION_DAYS_BACK < SETTLED_HISTORY_DAYS )); then
  echo "RETENTION_DAYS_BACK=${RETENTION_DAYS_BACK} cannot be less than settled history window ${SETTLED_HISTORY_DAYS}" >&2
  exit 1
fi

# This is an odds-only lane. Fixture identity/detail publication has separate
# owners, so the variable-cost detail worker cannot starve the +7d odds lane.
# A finite retry budget makes a cron tick a bounded queue trigger while still
# preserving settlement priority and truthful skipped/failure heartbeats.
# The timeout covers the complete history sync, +7d sync, CSV build, Supabase
# export, retention, and moneyline validation chain. Three hundred seconds was
# shorter than a full provider sync on the current production fixture set, so
# the process could be killed after fetching data but before committing it.
# Keep the documented 600-second safety bound as the default while allowing a
# controlled operator override for a larger production-shaped recovery run.
export ODDS_SYNC_LOCK_RETRY_ATTEMPTS="${ODDS_SYNC_LOCK_RETRY_ATTEMPTS:-${ODDS_P3_LOCK_RETRY_ATTEMPTS:-60}}"
export ODDS_SYNC_LOCK_RETRY_DELAY_SECONDS="${ODDS_SYNC_LOCK_RETRY_DELAY_SECONDS:-${ODDS_P3_LOCK_RETRY_DELAY_SECONDS:-15}}"
export ODDS_SYNC_P3_MAX_DURATION_SECONDS="${ODDS_P3_ODDS_MAX_RUNTIME_SECONDS:-600}"
export ODDS_SYNC_MIN_NORMAL_LEASE_SECONDS="${ODDS_P3_MIN_NORMAL_LEASE_SECONDS:-180}"

if [[ "${RUN_COVERAGE:-false}" == "true" || "${RUN_COVERAGE:-false}" == "1" ]]; then
  export COVERAGE_ARGS=""
else
  export COVERAGE_ARGS="--skip-coverage --skip-verification"
fi

CHAIN_COMMAND=$(cat <<'CHAIN'
set -euo pipefail

build_moneyline_provider_report_args() {
  local primary_report="$1"
  MONEYLINE_PROVIDER_REPORT_ARGS=(--provider-report "${primary_report}")
  # The coverage window spans the P1/P2/P3 lanes. Include the freshest evidence
  # from the other lanes so a P3 validation cannot misclassify an imminent
  # fixture simply because it is owned by P2 or P1.
  local report_path
  for report_path in /tmp/odds_sync_report_p2.json /tmp/odds_sync_report_p1.json; do
    if [[ -f "${report_path}" ]]; then
      MONEYLINE_PROVIDER_REPORT_ARGS+=(--provider-report "${report_path}")
    fi
  done
}

cd "${REPO_ROOT}"
source .venv/bin/activate
export PYTHONPATH="${REPO_ROOT}"

export SUPABASE_DB_URL_SESSION="${SUPABASE_DB_URL_SESSION:-${SUPABASE_DB_URL:-}}"
export PGSSLMODE="${PGSSLMODE:-require}"

# Settled odds use the exact previous-calendar-day contract and are immutable.
python scripts/sync_odds.py \
  --leagues "${ODDS_LEAGUES}" \
  --days-back "${SETTLED_HISTORY_DAYS}" \
  --days-forward 0 \
  --priority settled-history \
  --refresh-history \
  --bookmakers "${ODDS_BOOKMAKERS}" \
  --report-out "/tmp/odds_sync_report_history_p3.json" \
  --unmatched-out "/tmp/unmatched_players_history_p3.json"

# The fixture-core lane owns identity refresh. This lane consumes that
# canonical fixture set and owns the long-range odds snapshots/outcomes.
python scripts/sync_odds.py \
  --leagues "${ODDS_LEAGUES}" \
  --days-back "${ODDS_SYNC_DAYS_BACK}" \
  --days-forward "${DAYS_FORWARD}" \
  --priority p3 \
  --bookmakers "${ODDS_BOOKMAKERS}" \
  --report-out "/tmp/odds_sync_report_p3.json" \
  --unmatched-out "/tmp/unmatched_players_p3.json"

export ODDS_SYNC_REPORT_PATH="/tmp/odds_sync_report_p3.json"
python scripts/export_odds_to_supabase_psql.py \
  --leagues "${ODDS_LEAGUES}" \
  --days-back "${ODDS_EXPORT_DAYS_BACK}" \
  --days-forward "${DAYS_FORWARD}" \
  --calendar-window \
  --csv-out "/tmp/odds_outcomes_export_p3.csv" \
  --no-include-fixture-leagues \
  --progress-rows 10000 \
  --progress-fixtures 100 \
  --max-runtime-minutes "${INGEST_MAX_RUNTIME_MINUTES}" \
  --report-out "/tmp/odds_ingest_report_p3.json" \
  --skip-retention \
  --skip-retention-snapshots \
  ${COVERAGE_ARGS}

python scripts/odds_retention_psql.py \
  --days-back "${RETENTION_DAYS_BACK}" \
  --days-forward "${RETENTION_DAYS_FORWARD}" \
  --calendar-window \
  --snapshot-days "${RETENTION_SNAPSHOT_DAYS}" \
  --report-out "/tmp/odds_retention_report_p3.json"

set +e
build_moneyline_provider_report_args "/tmp/odds_sync_report_p3.json"
python scripts/validate_moneyline_coverage.py \
  --leagues "${ODDS_LEAGUES}" \
  --days-forward "${MONEYLINE_COVERAGE_DAYS_FORWARD}" \
  --fail-below-pct "${MONEYLINE_COVERAGE_MIN_PCT}" \
  "${MONEYLINE_PROVIDER_REPORT_ARGS[@]}" \
  --out-json "/tmp/moneyline_coverage_report_p3.json" \
  --out-md "/tmp/moneyline_coverage_report_p3.md"
MONEYLINE_VALIDATION_STATUS=$?
set -e

if [[ "${MONEYLINE_VALIDATION_STATUS}" -ne 0 ]]; then
  echo "Moneyline fidelity is red; running the bounded P3 odds repair attempt." >&2
  repair_attempt=0
  while [[ "${MONEYLINE_VALIDATION_STATUS}" -ne 0 && "${repair_attempt}" -lt "${MONEYLINE_REPAIR_ATTEMPTS}" ]]; do
    repair_attempt=$((repair_attempt + 1))
    set +e
    python scripts/sync_odds.py \
      --leagues "${ODDS_LEAGUES}" \
      --days-back "${ODDS_SYNC_DAYS_BACK}" \
      --days-forward "${DAYS_FORWARD}" \
      --priority p3 \
      --bookmakers "${ODDS_BOOKMAKERS}" \
      --report-out "/tmp/odds_sync_report_p3_repair_${repair_attempt}.json" \
      --unmatched-out "/tmp/unmatched_players_p3_repair_${repair_attempt}.json"
    repair_sync_status=$?
    if [[ "${repair_sync_status}" -eq 0 ]]; then
      python scripts/export_odds_to_supabase_psql.py \
        --leagues "${ODDS_LEAGUES}" \
        --days-back "${ODDS_EXPORT_DAYS_BACK}" \
        --days-forward "${DAYS_FORWARD}" \
        --calendar-window \
        --csv-out "/tmp/odds_outcomes_export_p3_repair_${repair_attempt}.csv" \
        --no-include-fixture-leagues \
        --progress-rows 10000 \
        --progress-fixtures 100 \
        --max-runtime-minutes "${INGEST_MAX_RUNTIME_MINUTES}" \
        --report-out "/tmp/odds_ingest_report_p3_repair_${repair_attempt}.json" \
        --skip-retention \
        --skip-retention-snapshots \
        ${COVERAGE_ARGS}
      repair_export_status=$?
    else
      repair_export_status=${repair_sync_status}
    fi
    if [[ "${repair_sync_status}" -eq 0 && "${repair_export_status}" -eq 0 ]]; then
      build_moneyline_provider_report_args "/tmp/odds_sync_report_p3_repair_${repair_attempt}.json"
      python scripts/validate_moneyline_coverage.py \
        --leagues "${ODDS_LEAGUES}" \
        --days-forward "${MONEYLINE_COVERAGE_DAYS_FORWARD}" \
        --fail-below-pct "${MONEYLINE_COVERAGE_MIN_PCT}" \
        "${MONEYLINE_PROVIDER_REPORT_ARGS[@]}" \
        --out-json "/tmp/moneyline_coverage_report_p3.json" \
        --out-md "/tmp/moneyline_coverage_report_p3.md"
      MONEYLINE_VALIDATION_STATUS=$?
    else
      MONEYLINE_VALIDATION_STATUS=1
    fi
    set -e
  done
fi

if [[ "${MONEYLINE_VALIDATION_STATUS}" -ne 0 ]]; then
  echo "Moneyline fidelity remains red after bounded repair attempts." >&2
  exit "${MONEYLINE_VALIDATION_STATUS}"
fi
CHAIN
)

status=0
run_recorded_pipeline_job \
  "run_p3" \
  "P3 Supabase ingest" \
  "${CHAIN_COMMAND}" \
  "${PIPELINE_EVIDENCE_FILE}" || status=$?
finalize_with_healthcheck "${status}" "${HEALTHCHECK_PING_URL_P3:-${HEALTHCHECK_PING_URL:-}}"
