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
  "scripts/validate_odds_api_market_catalog.py" \
  "scripts/export_odds_to_supabase_psql.py" \
  "scripts/odds_retention_psql.py" \
  "scripts/validate_moneyline_coverage.py"

export REPO_ROOT
# Cron/operator values are the runtime contract.  Capture them before loading
# shared defaults so a value in .env cannot silently shorten a recovery run or
# redirect an immutable release to a different SQLite spool.
runtime_jxd_db_path="${JXD_DB_PATH:-}"
runtime_lock_retry_attempts="${ODDS_SYNC_LOCK_RETRY_ATTEMPTS:-}"
if [[ -f "${REPO_ROOT}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${REPO_ROOT}/.env"
  set +a
fi
if [[ -n "${runtime_jxd_db_path}" ]]; then
  export JXD_DB_PATH="${runtime_jxd_db_path}"
fi
if [[ -n "${runtime_lock_retry_attempts}" ]]; then
  export ODDS_SYNC_LOCK_RETRY_ATTEMPTS="${runtime_lock_retry_attempts}"
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
export PIPELINE_EVIDENCE_FILE="${PIPELINE_EVIDENCE_FILE:-/tmp/odds_ingest_report_p3.json}"
export ODDS_P3_STAGE_DIR="${ODDS_P3_STAGE_DIR:-/var/lib/odds-sync/p3-staging}"
export ODDS_P3_STAGE_MAX_AGE_MINUTES="${ODDS_P3_STAGE_MAX_AGE_MINUTES:-480}"
export ODDS_P3_PIPELINE_MAX_DURATION_SECONDS="${ODDS_P3_PIPELINE_MAX_DURATION_SECONDS:-1800}"
export ODDS_P3_PIPELINE_LOCK_FILE="${ODDS_P3_PIPELINE_LOCK_FILE:-/var/lock/odds-p3-pipeline.lock}"

if ! [[ "${RETENTION_DAYS_BACK}" =~ ^[0-9]+$ ]]; then
  echo "RETENTION_DAYS_BACK must be a non-negative integer" >&2
  exit 1
fi
if (( RETENTION_DAYS_BACK < SETTLED_HISTORY_DAYS )); then
  echo "RETENTION_DAYS_BACK=${RETENTION_DAYS_BACK} cannot be less than settled history window ${SETTLED_HISTORY_DAYS}" >&2
  exit 1
fi
if ! [[ "${ODDS_P3_STAGE_MAX_AGE_MINUTES}" =~ ^[1-9][0-9]*$ ]]; then
  echo "ODDS_P3_STAGE_MAX_AGE_MINUTES must be a positive integer" >&2
  exit 1
fi

# Provider fetch/parsing and Supabase publication do not need the shared SQLite
# writer lock. They run under a dedicated P3 overlap lock. Only the complete,
# validated mutation bundles are applied under the settlement-aware writer
# lease, so a normal handoff never repeats an eight-minute provider fetch.
export ODDS_SYNC_LOCK_RETRY_ATTEMPTS="${ODDS_SYNC_LOCK_RETRY_ATTEMPTS:-${ODDS_P3_LOCK_RETRY_ATTEMPTS:-60}}"
export ODDS_SYNC_LOCK_RETRY_DELAY_SECONDS="${ODDS_SYNC_LOCK_RETRY_DELAY_SECONDS:-${ODDS_P3_LOCK_RETRY_DELAY_SECONDS:-15}}"
export ODDS_SYNC_P3_MAX_DURATION_SECONDS="${ODDS_P3_APPLY_MAX_RUNTIME_SECONDS:-300}"
export ODDS_SYNC_MIN_NORMAL_LEASE_SECONDS="${ODDS_P3_MIN_NORMAL_LEASE_SECONDS:-60}"

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
source "${REPO_ROOT}/scripts/vps/common.sh"

export SUPABASE_DB_URL_SESSION="${SUPABASE_DB_URL_SESSION:-${SUPABASE_DB_URL:-}}"
export PGSSLMODE="${PGSSLMODE:-require}"
mkdir -p "${ODDS_P3_STAGE_DIR}"
HISTORY_STAGE="${ODDS_P3_STAGE_DIR}/history.ndjson.gz"
P3_STAGE="${ODDS_P3_STAGE_DIR}/p3.ndjson.gz"
HISTORY_REPORT="${ODDS_P3_STAGE_DIR}/history-report.json"
P3_REPORT="${ODDS_P3_STAGE_DIR}/p3-report.json"

# Detect provider catalogue renames and response-shape changes explicitly,
# before a missing market can be misreported as local data loss.
python scripts/validate_odds_api_market_catalog.py \
  --sport football \
  --report-out "/tmp/odds_api_market_catalog_p3.json"

# A previous interrupted publish leaves complete stages in place. Reuse them
# within the bounded age contract instead of charging the provider twice.
stage_invalid=0
for stage_path in "${HISTORY_STAGE}" "${P3_STAGE}"; do
  if [[ -f "${stage_path}" ]]; then
    gzip -t "${stage_path}" || stage_invalid=1
    if [[ ! -f "${stage_path}.sha256" ]] || ! (cd "$(dirname "${stage_path}")" && sha256sum -c "$(basename "${stage_path}").sha256" >/dev/null); then
      stage_invalid=1
    fi
    if [[ -n "$(find "${stage_path}" -mmin "+${ODDS_P3_STAGE_MAX_AGE_MINUTES}" -print -quit)" ]]; then
      stage_invalid=1
    fi
  fi
done
if [[ "${stage_invalid}" -ne 0 ]]; then
  log_info "discarding invalid or expired P3 stages before provider fetch"
  rm -f "${HISTORY_STAGE}" "${P3_STAGE}" "${HISTORY_STAGE}.sha256" "${P3_STAGE}.sha256"
fi

if [[ ! -f "${HISTORY_STAGE}" || ! -f "${P3_STAGE}" || ! -f "${HISTORY_REPORT}" || ! -f "${P3_REPORT}" ]]; then
  rm -f "${HISTORY_STAGE}" "${P3_STAGE}" "${HISTORY_STAGE}.sha256" "${P3_STAGE}.sha256" "${HISTORY_REPORT}" "${P3_REPORT}"
  # Fixture-core and post-match settlement own fixture identity/status. This
  # odds-only lane consumes their canonical snapshot and stages no SQLite write.
  python scripts/sync_odds.py \
    --leagues "${ODDS_LEAGUES}" \
    --days-back "${SETTLED_HISTORY_DAYS}" \
    --days-forward 0 \
    --priority settled-history \
    --bookmakers "${ODDS_BOOKMAKERS}" \
    --stage-out "${HISTORY_STAGE}" \
    --report-out "${HISTORY_REPORT}" \
    --unmatched-out "/tmp/unmatched_players_history_p3.json"

  python scripts/sync_odds.py \
    --leagues "${ODDS_LEAGUES}" \
    --days-back "${ODDS_SYNC_DAYS_BACK}" \
    --days-forward "${DAYS_FORWARD}" \
    --priority p3 \
    --bookmakers "${ODDS_BOOKMAKERS}" \
    --stage-out "${P3_STAGE}" \
    --report-out "${P3_REPORT}" \
    --unmatched-out "/tmp/unmatched_players_p3.json"
else
  log_info "reusing complete P3 stages after an interrupted publish"
fi

APPLY_COMMAND=$(printf \
  'python scripts/sync_odds.py --apply-stage %q --stage-max-age-minutes %q && python scripts/sync_odds.py --apply-stage %q --stage-max-age-minutes %q' \
  "${HISTORY_STAGE}" "${ODDS_P3_STAGE_MAX_AGE_MINUTES}" \
  "${P3_STAGE}" "${ODDS_P3_STAGE_MAX_AGE_MINUTES}")
run_with_global_lock_and_retry "${APPLY_COMMAND}"

export ODDS_SYNC_REPORT_PATH="${P3_REPORT}"
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

build_moneyline_provider_report_args "${P3_REPORT}"
python scripts/validate_moneyline_coverage.py \
  --leagues "${ODDS_LEAGUES}" \
  --days-forward "${MONEYLINE_COVERAGE_DAYS_FORWARD}" \
  --fail-below-pct "${MONEYLINE_COVERAGE_MIN_PCT}" \
  "${MONEYLINE_PROVIDER_REPORT_ARGS[@]}" \
  --out-json "/tmp/moneyline_coverage_report_p3.json" \
  --out-md "/tmp/moneyline_coverage_report_p3.md"

# Only discard resumable stages after publication and validation both succeed.
rm -f "${HISTORY_STAGE}" "${P3_STAGE}" "${HISTORY_STAGE}.sha256" "${P3_STAGE}.sha256"
CHAIN
)

started_at="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
started_epoch="$(date -u +%s)"
status=0
run_with_dedicated_lock_and_timeout \
  "${CHAIN_COMMAND}" \
  "${ODDS_P3_PIPELINE_LOCK_FILE}" \
  "${ODDS_P3_PIPELINE_MAX_DURATION_SECONDS}" \
  "P3 odds pipeline" || status=$?
finished_at="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
finished_epoch="$(date -u +%s)"
record_pipeline_job_run \
  "run_p3" \
  "P3 Supabase ingest" \
  "${status}" \
  "${started_at}" \
  "${finished_at}" \
  "$(((finished_epoch - started_epoch) * 1000))"
finalize_with_healthcheck "${status}" "${HEALTHCHECK_PING_URL_P3:-${HEALTHCHECK_PING_URL:-}}"
