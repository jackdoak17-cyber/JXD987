#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=./common.sh
source "${SCRIPT_DIR}/common.sh"
verify_runtime_manifest_or_exit "$0"
require_runtime_manifest_entries_or_exit "$0" \
  "config/fixture_core_contract.json" \
  "config/league_ids.txt" \
  "config/odds_api_sync_excluded_leagues.json" \
  "jxd/__init__.py" \
  "jxd/db.py" \
  "jxd/models.py" \
  "jxd/sportmonks_client.py" \
  "jxd/sync.py" \
  "scripts/fixture_core_contract.py" \
  "scripts/sync_odds.py" \
  "scripts/export_to_supabase.py" \
  "scripts/refresh_fixture_delivery.py"

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

# These values are contract-owned. Environment variables may select a
# different contract file for a controlled rehearsal, but may not silently
# override its horizons or freshness semantics.
export FIXTURE_CORE_IDENTITY_DAYS="$(contract_value identity_window_days)"
export FIXTURE_CORE_HISTORY_DAYS="$(contract_value history_window_days)"
export FIXTURE_CORE_SOURCE_DAYS_FORWARD="$(contract_value source_window_days_forward)"
export FIXTURE_CORE_DELIVERY_DAYS_FORWARD="$(contract_value delivery_window_days)"
export FIXTURE_CORE_JOB_ID="$(contract_value job_id)"
export FIXTURE_CORE_JOB_MAX_AGE_MINUTES="$(contract_value max_job_age_minutes)"
export FIXTURE_CORE_SOURCE_BUFFER_DAYS="$(contract_value source_buffer_days)"

export STATS_LEAGUES="${FIXTURE_LEAGUE_IDS:-${STATS_LEAGUE_IDS:-$(supported_league_csv)}}"
validate_supported_leagues "${STATS_LEAGUES}"
export FIXTURE_CORE_MAX_RUNTIME_SECONDS="${FIXTURE_CORE_MAX_RUNTIME_SECONDS:-900}"
# The complete identity refresh has measured at roughly fifty seconds on the
# production-shaped dataset. Admit a one-minute lease so a late settlement
# handoff can still use the final safe minute before the next tick; the shared
# timeout remains the hard safety boundary if the refresh runs long.
export FIXTURE_CORE_MIN_NORMAL_LEASE_SECONDS="${FIXTURE_CORE_MIN_NORMAL_LEASE_SECONDS:-60}"
# The detail writer can legitimately hold the canonical lock for several
# minutes after its quarter-hour tick. Keep this identity-refresh lane
# resumable across that handoff rather than losing its six-hour cron tick.
# A six-hour lane may start during the settlement writer's quarter-hour
# reservation. Thirty minutes is finite, exceeds the settlement timeout, and
# spans the next normal handoff window without allowing an unbounded process.
export ODDS_SYNC_LOCK_RETRY_ATTEMPTS="${FIXTURE_CORE_LOCK_RETRY_ATTEMPTS:-120}"
export ODDS_SYNC_LOCK_RETRY_DELAY_SECONDS="${FIXTURE_CORE_LOCK_RETRY_DELAY_SECONDS:-15}"
# The exporter treats zero as an unbounded completed-fixture selection. Keep
# the historical side of this identity refresh bounded by the shared contract
# as well; the future identity contract is still controlled by the source
# window.
export FIXTURE_CORE_REFRESH_DAYS_BACK="${FIXTURE_CORE_HISTORY_DAYS}"
export FIXTURE_DELIVERY_TIMEOUT_SECONDS="${FIXTURE_DELIVERY_TIMEOUT_SECONDS:-1800}"
export PIPELINE_EVIDENCE_FILE="${PIPELINE_EVIDENCE_FILE:-/tmp/fixture_core_export_report.json}"

CHAIN_COMMAND=$(cat <<'CHAIN'
set -euo pipefail

cd "${REPO_ROOT}"
source .venv/bin/activate
export PYTHONPATH="${REPO_ROOT}"

if [[ -f ./.env ]]; then
  set -a
  # shellcheck disable=SC1091
  source ./.env
  set +a
fi

required_env_missing=()
for required_env in SPORTMONKS_API_TOKEN SUPABASE_URL SUPABASE_SERVICE_ROLE_KEY; do
  if [[ -z "${!required_env:-}" ]]; then
    required_env_missing+=("${required_env}")
  fi
done
if [[ -z "${SUPABASE_DB_URL_SESSION:-}" && -z "${SUPABASE_DB_URL_POOLER:-}" && -z "${SUPABASE_DB_URL:-}" ]]; then
  required_env_missing+=("SUPABASE_DB_URL_SESSION, SUPABASE_DB_URL_POOLER, or SUPABASE_DB_URL")
fi
if [[ "${#required_env_missing[@]}" -gt 0 ]]; then
  printf 'Fixture-core preflight missing: %s\n' "${required_env_missing[*]}" >&2
  exit 1
fi

# Refresh only the provider fixture/metadata source. In particular, do not
# expand the paid Odds-API window or opportunistically refresh squads and
# sidelined data as a side effect of this identity-fidelity job.
python scripts/sync_odds.py \
  --leagues "${STATS_LEAGUES}" \
  --days-back "${FIXTURE_CORE_REFRESH_DAYS_BACK}" \
  --days-forward "${FIXTURE_CORE_SOURCE_DAYS_FORWARD}" \
  --refresh-upcoming \
  --no-refresh-squads-missing \
  --no-refresh-sidelined-window \
  --refresh-only \
  --report-out "/tmp/fixture_core_refresh_report.json"

# Strictly export the complete fixture identity source for the contract
# window. Pruning is intentionally disabled because this is a rolling source
# refresh, not season-retention maintenance.
python scripts/export_to_supabase.py \
  --strict \
  --leagues "${STATS_LEAGUES}" \
  --days-back "${FIXTURE_CORE_REFRESH_DAYS_BACK}" \
  --upcoming-days "${FIXTURE_CORE_SOURCE_DAYS_FORWARD}" \
  --fixture-core-only \
  --skip-odds-snapshots \
  --skip-odds-outcomes \
  --skip-prune \
  --report-json "${PIPELINE_EVIDENCE_FILE}"

# Publish with the existing release-aware, schema-preflighted delivery path.
# London date boundaries match the public fixture-card contract; the source
# identity contract itself remains UTC/date-only.
python scripts/refresh_fixture_delivery.py \
  --start-date "$(TZ=Europe/London date -d "-${FIXTURE_CORE_HISTORY_DAYS} days" +%F)" \
  --end-date "$(TZ=Europe/London date -d "+${FIXTURE_CORE_DELIVERY_DAYS_FORWARD} days" +%F)" \
  --leagues "${STATS_LEAGUES}" \
  --report-out "/tmp/fixture_core_delivery_report.json"
CHAIN
)

status=0
ODDS_SYNC_P3_MAX_DURATION_SECONDS="${FIXTURE_CORE_MAX_RUNTIME_SECONDS}" \
ODDS_SYNC_MIN_NORMAL_LEASE_SECONDS="${FIXTURE_CORE_MIN_NORMAL_LEASE_SECONDS}" \
ODDS_SYNC_JOB_PRIORITY="normal" \
  run_recorded_pipeline_job \
    "${FIXTURE_CORE_JOB_ID}" \
    "Fixture core export" \
    "${CHAIN_COMMAND}" \
    "${PIPELINE_EVIDENCE_FILE}" || status=$?

finalize_with_healthcheck "${status}" "${HEALTHCHECK_PING_URL_FIXTURE_CORE:-${HEALTHCHECK_PING_URL:-}}"
