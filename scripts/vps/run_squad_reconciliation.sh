#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=./common.sh
source "${SCRIPT_DIR}/common.sh"
verify_runtime_manifest_or_exit "$0"
require_runtime_manifest_entries_or_exit "$0" \
  "config/league_ids.txt" \
  "jxd/__init__.py" \
  "jxd/db.py" \
  "jxd/models.py" \
  "jxd/sportmonks_client.py" \
  "jxd/sync.py" \
  "scripts/export_to_supabase.py" \
  "scripts/sync_sparse_squads.py" \
  "scripts/verify_squad_freshness.py"

export REPO_ROOT
if [[ -f "${REPO_ROOT}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${REPO_ROOT}/.env"
  set +a
fi

export SQUAD_LEAGUES="${SQUAD_LEAGUE_IDS:-${STATS_LEAGUE_IDS:-${LEAGUE_IDS:-$(default_league_csv)}}}"
export SQUAD_BATCH_SIZE="${SQUAD_BATCH_SIZE:-100}"
export SQUAD_FRESHNESS_MAX_HOURS="${SQUAD_FRESHNESS_MAX_HOURS:-12}"
export SQUAD_OFFSET_FILE="${SQUAD_OFFSET_FILE:-/var/lib/odds-sync/squad-reconciliation-offset}"
export SQUAD_SUPERVISOR_LOCK_FILE="${SQUAD_SUPERVISOR_LOCK_FILE:-/var/lock/odds-sync-squad-reconciliation.lock}"
export SQUAD_REPORT_FILE="${SQUAD_REPORT_FILE:-/tmp/sparse_squad_reconciliation_report.json}"
export SQUAD_FRESHNESS_REPORT_FILE="${SQUAD_FRESHNESS_REPORT_FILE:-/tmp/squad_freshness_report.json}"
export SQUAD_RECONCILIATION_MAX_RUNTIME_SECONDS="${SQUAD_RECONCILIATION_MAX_RUNTIME_SECONDS:-2400}"

if [[ ! "${SQUAD_BATCH_SIZE}" =~ ^[1-9][0-9]*$ ]]; then
  log_error "SQUAD_BATCH_SIZE must be a positive integer"
  exit 1
fi
if [[ ! "${SQUAD_RECONCILIATION_MAX_RUNTIME_SECONDS}" =~ ^[1-9][0-9]*$ ]]; then
  log_error "SQUAD_RECONCILIATION_MAX_RUNTIME_SECONDS must be a positive integer"
  exit 1
fi

mkdir -p "$(dirname "${SQUAD_OFFSET_FILE}")" "$(dirname "${SQUAD_SUPERVISOR_LOCK_FILE}")"
umask 077
exec 8>"${SQUAD_SUPERVISOR_LOCK_FILE}"
if ! flock --nonblock 8; then
  log_info "[SKIPPED] squad supervisor lock unavailable, will retry next tick"
  exit 2
fi

team_offset=0
if [[ -f "${SQUAD_OFFSET_FILE}" ]]; then
  read -r team_offset < "${SQUAD_OFFSET_FILE}" || team_offset=0
fi
if [[ ! "${team_offset}" =~ ^[0-9]+$ ]]; then
  log_error "invalid squad reconciliation offset in ${SQUAD_OFFSET_FILE}: ${team_offset}"
  exit 1
fi
export SQUAD_TEAM_OFFSET="${team_offset}"
export ODDS_SYNC_P3_MAX_DURATION_SECONDS="${SQUAD_RECONCILIATION_MAX_RUNTIME_SECONDS}"

CHAIN_COMMAND=$(cat <<'CHAIN'
set -euo pipefail

cd "${REPO_ROOT}"
source .venv/bin/activate
export PYTHONPATH="${REPO_ROOT}"

python scripts/sync_sparse_squads.py \
  --leagues "${SQUAD_LEAGUES}" \
  --refresh-all \
  --team-offset "${SQUAD_TEAM_OFFSET}" \
  --max-teams "${SQUAD_BATCH_SIZE}" \
  --report-json "${SQUAD_REPORT_FILE}"

# A freshness check is meaningful only after the complete deterministic team
# list has been reconciled. Running it under the same global writer lock keeps
# the local/export read pair from observing a half-written batch.
if jq -e '.has_more_teams == false' "${SQUAD_REPORT_FILE}" >/dev/null; then
  python scripts/verify_squad_freshness.py \
    --leagues "${SQUAD_LEAGUES}" \
    --max-age-hours "${SQUAD_FRESHNESS_MAX_HOURS}" \
    --report-json "${SQUAD_FRESHNESS_REPORT_FILE}"
fi
CHAIN
)

status=0
run_with_global_lock_and_timeout "${CHAIN_COMMAND}" || status=$?
if [[ "${status}" -eq 0 ]]; then
  next_offset="$(jq -r '.next_team_offset // 0' "${SQUAD_REPORT_FILE}")"
  if [[ ! "${next_offset}" =~ ^[0-9]+$ ]]; then
    log_error "squad report returned an invalid next offset: ${next_offset}"
    exit 1
  fi
  next_offset_tmp="${SQUAD_OFFSET_FILE}.tmp.$$"
  printf '%s\n' "${next_offset}" > "${next_offset_tmp}"
  mv "${next_offset_tmp}" "${SQUAD_OFFSET_FILE}"
  log_info "squad reconciliation batch completed offset=${team_offset} next_offset=${next_offset}"
fi

finalize_with_healthcheck "${status}" "${HEALTHCHECK_PING_URL_SQUADS:-${HEALTHCHECK_PING_URL:-}}"
