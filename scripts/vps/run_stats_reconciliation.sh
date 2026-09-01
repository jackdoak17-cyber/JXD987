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
export REPO_ROOT
export STATS_RECONCILE_LEAGUES="${STATS_RECONCILE_LEAGUES:-$(supported_league_csv)}"
# Keep each historical lock hold below the live settlement cadence, then leave
# a real handoff window for the scheduled writers. Operators can raise these
# values for an isolated maintenance window, but the production defaults are
# deliberately live-safe. Provider fetch is bulked, and candidate ordering
# plus the cohort cap keeps the larger batch within a bounded projection
# fan-out. The global wrapper still refuses to start when the remaining lease
# is too short.
export STATS_RECONCILE_BATCH_SIZE="${STATS_RECONCILE_BATCH_SIZE:-50}"
export STATS_RECONCILE_MAX_COHORTS="${STATS_RECONCILE_MAX_COHORTS:-5}"
# Retry delay after a live writer owns the lock. The scheduler gate below
# decides whether a new batch may begin; this delay is not the handoff policy.
export STATS_RECONCILE_SLEEP_SECONDS="${STATS_RECONCILE_SLEEP_SECONDS:-60}"
export STATS_RECONCILE_LIVE_TICK_SECONDS="${STATS_RECONCILE_LIVE_TICK_SECONDS:-900}"
export ODDS_SYNC_LOCK_RETRY_ATTEMPTS="${STATS_RECONCILE_LOCK_RETRY_ATTEMPTS:-60}"
export ODDS_SYNC_LOCK_RETRY_DELAY_SECONDS="${STATS_RECONCILE_LOCK_RETRY_DELAY_SECONDS:-15}"
# The live settlement normally owns the spool for several minutes after the
# quarter-hour tick. Do not race that work; begin historical reconciliation
# after this guard and let the canonical lease cap the run before the next
# tick.
export STATS_RECONCILE_LIVE_SETTLEMENT_GUARD_SECONDS="${STATS_RECONCILE_LIVE_SETTLEMENT_GUARD_SECONDS:-420}"
export STATS_RECONCILE_LIVE_GRACE_SECONDS="${STATS_RECONCILE_LIVE_GRACE_SECONDS:-60}"
export STATS_RECONCILE_MAX_HOLD_SECONDS="${STATS_RECONCILE_MAX_HOLD_SECONDS:-600}"
# A cohort-clustered 50-fixture batch has measured at up to roughly four
# minutes in the production path. Do not start a new batch when the live
# handoff window cannot accommodate that bounded attempt with a real buffer.
export ODDS_SYNC_MIN_NORMAL_LEASE_SECONDS="${STATS_RECONCILE_MIN_LEASE_SECONDS:-270}"
export STATS_RECONCILE_REPORT="${STATS_RECONCILE_REPORT:-/tmp/stats_reconcile_provider_batch.json}"
export STATS_RECONCILE_RUN_LOG="${STATS_RECONCILE_RUN_LOG:-/tmp/stats_reconcile_provider_batch.log}"
export STATS_RECONCILE_SUPERVISOR_LOCK="${STATS_RECONCILE_SUPERVISOR_LOCK:-/var/lock/stats-reconciliation-supervisor.lock}"
export RUNTIME_RELEASE_ID="${RUNTIME_RELEASE_ID:-$(runtime_release_id)}"

mkdir -p "$(dirname "${STATS_RECONCILE_SUPERVISOR_LOCK}")"
exec 9>"${STATS_RECONCILE_SUPERVISOR_LOCK}"
if ! flock --nonblock 9; then
  log_info "[SKIPPED] stats reconciliation supervisor already running"
  exit 0
fi

# Let the canonical wrapper own the shared spool lock. This gives the
# provider worker the same deadline-aware lease as every other normal writer;
# Python inherits descriptor 9 and therefore must not flock the path twice.
export STATS_RECONCILE_LOCK_HELD=1
export ODDS_SYNC_JOB_PRIORITY=normal
export ODDS_SYNC_P3_MAX_DURATION_SECONDS="${STATS_RECONCILE_MAX_HOLD_SECONDS}"
export ODDS_SYNC_LIVE_TICK_SECONDS="${STATS_RECONCILE_LIVE_TICK_SECONDS}"
export ODDS_SYNC_LIVE_GRACE_SECONDS="${STATS_RECONCILE_LIVE_GRACE_SECONDS}"

RECONCILIATION_COMMAND=$(cat <<'CHAIN'
exec "${REPO_ROOT}/.venv/bin/python" \
  "${REPO_ROOT}/scripts/reconcile_stats_provider_queue.py" \
  --leagues "${STATS_RECONCILE_LEAGUES}" \
  --batch-size "${STATS_RECONCILE_BATCH_SIZE}" \
  --max-cohorts "${STATS_RECONCILE_MAX_COHORTS}" \
  --max-batches 1 \
  --report-json "${STATS_RECONCILE_REPORT}" \
  >"${STATS_RECONCILE_RUN_LOG}" 2>&1
CHAIN
)

wait_for_live_window() {
  local now phase delay tick guard grace
  tick="${STATS_RECONCILE_LIVE_TICK_SECONDS}"
  guard="${STATS_RECONCILE_LIVE_SETTLEMENT_GUARD_SECONDS}"
  grace="${STATS_RECONCILE_LIVE_GRACE_SECONDS}"
  now="$(date -u +%s)"
  phase=$((now % tick))
  delay=0

  # Do not start while the live settlement is expected to own the spool. Start
  # only after the measured post-tick guard, and stop starting new batches once
  # there is not enough time to hand the bounded lease back before the next
  # tick. The canonical wrapper also caps an already-running lease.
  if (( phase < guard )); then
    delay=$((guard - phase))
  elif (( phase >= tick - grace )); then
    delay=$((tick - phase + guard))
  fi

  if (( delay > 0 )); then
    log_info "waiting ${delay}s for the next live-safe reconciliation window"
    sleep "${delay}" 9>&-
  fi
}

while true; do
  wait_for_live_window
  # The supervisor is intentionally long-lived. Refresh the release label for
  # every bounded batch so hot deployments cannot leave delivery reports and
  # ledger rows attributed to the release that started the parent shell.
  export RUNTIME_RELEASE_ID="$(runtime_release_id)"
  rm -f "${STATS_RECONCILE_REPORT}" "${STATS_RECONCILE_RUN_LOG}"
  RUN_STARTED_AT="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  RUN_STARTED_EPOCH="$(date -u +"%s")"
  set +e
  run_with_global_lock_and_retry "${RECONCILIATION_COMMAND}"
  status=$?
  set -e
  RUN_FINISHED_AT="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  RUN_FINISHED_EPOCH="$(date -u +"%s")"
  PIPELINE_EVIDENCE_FILE="${STATS_RECONCILE_REPORT}" record_pipeline_job_run \
    "run_stats_reconciliation" \
    "Stats reconciliation" \
    "${status}" \
    "${RUN_STARTED_AT}" \
    "${RUN_FINISHED_AT}" \
    "$(((RUN_FINISHED_EPOCH - RUN_STARTED_EPOCH) * 1000))"

  if [[ ! -f "${STATS_RECONCILE_REPORT}" ]]; then
    # The Python worker exits without a report when another SQLite writer owns
    # the canonical lock. Wait and retry without contending or spinning.
    sleep "${STATS_RECONCILE_SLEEP_SECONDS}" 9>&-
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
    sleep "${STATS_RECONCILE_SLEEP_SECONDS}" 9>&-
    continue
  fi

  if [[ "${batches}" -eq 0 ]]; then
    log_info "stats reconciliation queue drained"
    exit 0
  fi

  tail -n 8 "${STATS_RECONCILE_RUN_LOG}" || true
done
