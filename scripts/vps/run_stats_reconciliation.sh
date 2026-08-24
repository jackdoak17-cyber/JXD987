#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=./common.sh
source "${SCRIPT_DIR}/common.sh"
verify_runtime_manifest_or_exit "$0"
require_runtime_manifest_entries_or_exit "$0" \
  "config/league_ids.txt" \
  "scripts/reconcile_stats_provider_queue.py"

if [[ -f "${REPO_ROOT}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${REPO_ROOT}/.env"
  set +a
fi

export PYTHONPATH="${REPO_ROOT}"
export STATS_RECONCILE_LEAGUES="${STATS_RECONCILE_LEAGUES:-$(default_league_csv)}"
# Keep each historical lock hold below the live settlement cadence, then leave
# a real handoff window for the scheduled writers. Operators can raise these
# values for an isolated maintenance window, but the production defaults are
# deliberately live-safe.
export STATS_RECONCILE_BATCH_SIZE="${STATS_RECONCILE_BATCH_SIZE:-50}"
export STATS_RECONCILE_SLEEP_SECONDS="${STATS_RECONCILE_SLEEP_SECONDS:-120}"
export STATS_RECONCILE_REPORT="${STATS_RECONCILE_REPORT:-/tmp/stats_reconcile_provider_batch.json}"
export STATS_RECONCILE_RUN_LOG="${STATS_RECONCILE_RUN_LOG:-/tmp/stats_reconcile_provider_batch.log}"
export STATS_RECONCILE_SUPERVISOR_LOCK="${STATS_RECONCILE_SUPERVISOR_LOCK:-/var/lock/stats-reconciliation-supervisor.lock}"

mkdir -p "$(dirname "${STATS_RECONCILE_SUPERVISOR_LOCK}")"
exec 9>"${STATS_RECONCILE_SUPERVISOR_LOCK}"
if ! flock --nonblock 9; then
  log_info "[SKIPPED] stats reconciliation supervisor already running"
  exit 0
fi

while true; do
  rm -f "${STATS_RECONCILE_REPORT}" "${STATS_RECONCILE_RUN_LOG}"
  set +e
  "${REPO_ROOT}/.venv/bin/python" \
    "${REPO_ROOT}/scripts/reconcile_stats_provider_queue.py" \
    --leagues "${STATS_RECONCILE_LEAGUES}" \
    --batch-size "${STATS_RECONCILE_BATCH_SIZE}" \
    --max-batches 1 \
    --report-json "${STATS_RECONCILE_REPORT}" \
    >"${STATS_RECONCILE_RUN_LOG}" 2>&1
  status=$?
  set -e

  if [[ ! -f "${STATS_RECONCILE_REPORT}" ]]; then
    # The Python worker exits without a report when another SQLite writer owns
    # the canonical lock. Wait and retry without contending or spinning.
    sleep "${STATS_RECONCILE_SLEEP_SECONDS}"
    continue
  fi

  batches="$(${REPO_ROOT}/.venv/bin/python - "${STATS_RECONCILE_REPORT}" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    report = json.load(handle)
print(int(report.get("batches", 0)))
PY
)"

  if [[ "${status}" -ne 0 ]]; then
    tail -n 20 "${STATS_RECONCILE_RUN_LOG}" >&2 || true
    sleep "${STATS_RECONCILE_SLEEP_SECONDS}"
    continue
  fi

  if [[ "${batches}" -eq 0 ]]; then
    log_info "stats reconciliation queue drained"
    exit 0
  fi

  tail -n 8 "${STATS_RECONCILE_RUN_LOG}" || true
  sleep 2
done
