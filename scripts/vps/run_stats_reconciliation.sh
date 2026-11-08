#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=./common.sh
source "${SCRIPT_DIR}/common.sh"
verify_runtime_manifest_or_exit "$0"
require_runtime_manifest_entries_or_exit "$0" \
  "config/league_ids.txt" \
  "scripts/reconcile_stats_provider_queue.py" \
  "scripts/vps/select_stats_reconciliation_leagues.py"

if [[ -f "${REPO_ROOT}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${REPO_ROOT}/.env"
  set +a
fi

export PYTHONPATH="${REPO_ROOT}"
export REPO_ROOT
export STATS_RECONCILE_LEAGUES="${STATS_RECONCILE_LEAGUES:-$(supported_league_csv)}"
if [[ "${STATS_RECONCILE_LEAGUES}" != "$(supported_league_csv)" ]]; then
  log_error "supervised reconciliation must cover the complete supported league list"
  exit 1
fi
# One league per attempt prevents the two-cohort batch cap from repeatedly
# starving a second league in the same group. Scope age is an inspection
# freshness guard, separate from accepted data and publication freshness.
export STATS_RECONCILE_LEAGUE_GROUP_SIZE="${STATS_RECONCILE_LEAGUE_GROUP_SIZE:-1}"
if [[ "${STATS_RECONCILE_LEAGUE_GROUP_SIZE}" != "1" ]]; then
  log_error "supervised reconciliation requires one league per attempt until grouped fairness is certified"
  exit 1
fi
export STATS_RECONCILE_SCOPE_MAX_AGE_SECONDS="${STATS_RECONCILE_SCOPE_MAX_AGE_SECONDS:-43200}"
export STATS_RECONCILE_LEAGUE_CURSOR_FILE="${STATS_RECONCILE_LEAGUE_CURSOR_FILE:-/var/lib/odds-sync/stats-reconciliation-league-cursor.json}"
STATS_RECONCILE_GROUP_COUNT="$("${REPO_ROOT}/.venv/bin/python" \
  "${REPO_ROOT}/scripts/vps/select_stats_reconciliation_leagues.py" count \
  --leagues "${STATS_RECONCILE_LEAGUES}" \
  --group-size "${STATS_RECONCILE_LEAGUE_GROUP_SIZE}")"
# Keep each historical lock hold below the live settlement cadence, then leave
# a real handoff window for the scheduled writers. Operators can raise these
# values for an isolated maintenance window, but the production defaults are
# deliberately live-safe. Provider fetch is bulked, and candidate ordering
# plus the cohort cap keeps the larger batch within a bounded projection
# fan-out. The global wrapper still refuses to start when the remaining lease
# is too short.
export STATS_RECONCILE_BATCH_SIZE="${STATS_RECONCILE_BATCH_SIZE:-20}"
export STATS_RECONCILE_MAX_COHORTS="${STATS_RECONCILE_MAX_COHORTS:-2}"
# Retry delay after a live writer owns the lock. The scheduler gate below
# decides whether a new batch may begin; this delay is not the handoff policy.
export STATS_RECONCILE_SLEEP_SECONDS="${STATS_RECONCILE_SLEEP_SECONDS:-60}"
# A foreground job polls for the canonical lock while this historical worker
# drains. Leave a short deterministic handoff after every successful batch so
# the foreground waiter can acquire it instead of being starved by this loop.
export STATS_RECONCILE_SUCCESS_HANDOFF_SECONDS="${STATS_RECONCILE_SUCCESS_HANDOFF_SECONDS:-75}"
export STATS_RECONCILE_LIVE_TICK_SECONDS="${STATS_RECONCILE_LIVE_TICK_SECONDS:-900}"
# The live settlement normally owns the spool for several minutes after the
# quarter-hour tick. Do not race that work; begin historical reconciliation
# after this guard and let the canonical lease cap the run before the next
# tick.
export STATS_RECONCILE_LIVE_SETTLEMENT_GUARD_SECONDS="${STATS_RECONCILE_LIVE_SETTLEMENT_GUARD_SECONDS:-420}"
export STATS_RECONCILE_LIVE_GRACE_SECONDS="${STATS_RECONCILE_LIVE_GRACE_SECONDS:-60}"
export STATS_RECONCILE_MAX_HOLD_SECONDS="${STATS_RECONCILE_MAX_HOLD_SECONDS:-600}"
# Production evidence showed a 50-fixture batch could exceed the available
# lease and repeatedly reacquire the spool before a waiting P3 publisher. A
# 20-fixture, two-cohort batch fits the measured live window, and the handoff
# is longer than four foreground 15-second retry intervals.
export ODDS_SYNC_MIN_NORMAL_LEASE_SECONDS="${STATS_RECONCILE_MIN_LEASE_SECONDS:-270}"
export STATS_RECONCILE_REPORT="${STATS_RECONCILE_REPORT:-/tmp/stats_reconcile_provider_batch.json}"
export STATS_RECONCILE_RUN_LOG="${STATS_RECONCILE_RUN_LOG:-/tmp/stats_reconcile_provider_batch.log}"
export STATS_RECONCILE_ATTEMPT_RECEIPT="${STATS_RECONCILE_ATTEMPT_RECEIPT:-/tmp/stats_reconcile_attempt.$$.json}"
export STATS_RECONCILE_OUTCOME_FILE="${STATS_RECONCILE_OUTCOME_FILE:-/tmp/stats_reconcile_outcome.$$.json}"
export STATS_RECONCILE_TIMING_FILE="${STATS_RECONCILE_TIMING_FILE:-/tmp/stats_reconcile_provider_batch.timing.$$}"
export STATS_RECONCILE_SUPERVISOR_LOCK="${STATS_RECONCILE_SUPERVISOR_LOCK:-/var/lock/stats-reconciliation-supervisor.lock}"
# Existing source reconciliation is the priority workload. If a bounded
# leaderboard publication is already in flight, wait just beyond its 30-second
# ceiling; while waiting we do not hold the canonical SQLite writer lock.
export STATS_RECONCILE_DB_HEAVY_LOCK_WAIT_SECONDS="${STATS_RECONCILE_DB_HEAVY_LOCK_WAIT_SECONDS:-35}"
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
set -euo pipefail
selection_json="$("${REPO_ROOT}/.venv/bin/python" \
  "${REPO_ROOT}/scripts/vps/select_stats_reconciliation_leagues.py" next \
  --leagues "${STATS_RECONCILE_LEAGUES}" \
  --group-size "${STATS_RECONCILE_LEAGUE_GROUP_SIZE}" \
  --cursor-file "${STATS_RECONCILE_LEAGUE_CURSOR_FILE}" \
  --release-id "${RUNTIME_RELEASE_ID}" \
  2>>"${STATS_RECONCILE_RUN_LOG}")"
printf '%s\n' "${selection_json}" >"${STATS_RECONCILE_ATTEMPT_RECEIPT}"
read -r STATS_RECONCILE_SCOPE_ATTEMPT_ID selected_leagues STATS_RECONCILE_SCOPE_EPOCH < <(
  "${REPO_ROOT}/.venv/bin/python" -c 'import json,sys; p=json.loads(sys.argv[1]); print(p["attempt_id"], ",".join(map(str,p["leagues"])), p["epoch"])' "${selection_json}")
export STATS_RECONCILE_SCOPE_ATTEMPT_ID STATS_RECONCILE_SCOPE_EPOCH
printf 'stats reconciliation selected leagues=%s\n' "${selected_leagues}" >>"${STATS_RECONCILE_RUN_LOG}"
printf 'stats reconciliation selected leagues=%s\n' "${selected_leagues}" >&2
exec "${REPO_ROOT}/.venv/bin/python" \
  "${REPO_ROOT}/scripts/reconcile_stats_provider_queue.py" \
  --leagues "${selected_leagues}" \
  --batch-size "${STATS_RECONCILE_BATCH_SIZE}" \
  --max-cohorts "${STATS_RECONCILE_MAX_COHORTS}" \
  --max-batches 1 \
  --report-json "${STATS_RECONCILE_REPORT}" \
  >>"${STATS_RECONCILE_RUN_LOG}" 2>&1
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

record_scope_health() {
  local health_state health_evidence health_status
  if [[ -f "${STATS_RECONCILE_OUTCOME_FILE}" ]]; then
    read -r health_state health_evidence < <("${REPO_ROOT}/.venv/bin/python" -c '
import json,sys
p=json.load(open(sys.argv[1], encoding="utf-8")); a=p["aggregate"]
e={"scope_health":a["state"],"epoch":a["epoch"],"observed_at":a["observed_at"],
   "missing":len(a["missing_leagues"]),"stale":len(a["stale_leagues"]),
   "unresolved":a["unresolved_leagues"],"empty_sweep":a["empty_sweep"],
   "scope_max_age_seconds":a["scope_max_age_seconds"]}
print(a["state"],json.dumps(e,separators=(",",":")))' "${STATS_RECONCILE_OUTCOME_FILE}")
  elif [[ "${status}" -ne 0 && "${status}" -ne 2 ]]; then
    health_state="unknown"
    health_evidence='{"scope_health":"unknown","reason":"attempt_outcome_missing"}'
  else
    return 0
  fi
  health_status=1
  if [[ "${health_state}" == "healthy" ]]; then
    health_status=0
  fi
  record_pipeline_job_run \
    "run_stats_reconciliation_scope_health" "Stats reconciliation scope health" "${health_status}" \
    "${RUN_STARTED_AT}" "${RUN_FINISHED_AT}" "${TOTAL_DURATION_MS}" \
    "" "" "" "" "" "${health_evidence}" "1"
}

while true; do
  wait_for_live_window
  # The supervisor is intentionally long-lived. Refresh the release label for
  # every bounded batch so hot deployments cannot leave delivery reports and
  # ledger rows attributed to the release that started the parent shell.
  export RUNTIME_RELEASE_ID="$(runtime_release_id)"
  rm -f "${STATS_RECONCILE_REPORT}" "${STATS_RECONCILE_RUN_LOG}" "${STATS_RECONCILE_TIMING_FILE}" "${STATS_RECONCILE_ATTEMPT_RECEIPT}" "${STATS_RECONCILE_OUTCOME_FILE}"
  RUN_STARTED_AT="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  RUN_STARTED_EPOCH_MS="$(epoch_milliseconds)"
  set +e
  run_with_database_heavy_lock_timed "${STATS_RECONCILE_DB_HEAVY_LOCK_WAIT_SECONDS}" \
    "${STATS_RECONCILE_TIMING_FILE}" \
    run_with_global_lock_and_timeout "${RECONCILIATION_COMMAND}"
  status=$?
  set -e
  if [[ "${status}" -eq 75 ]]; then
    log_info "stats reconciliation deferred until database-heavy slot is available"
    sleep "${STATS_RECONCILE_SLEEP_SECONDS}" 9>&-
    continue
  fi
  scope_outcome="not_started"
  empty_sweep="false"
  if [[ -f "${STATS_RECONCILE_ATTEMPT_RECEIPT}" ]]; then
    attempt_id="$("${REPO_ROOT}/.venv/bin/python" -c 'import json,sys; print(json.load(open(sys.argv[1]))["attempt_id"])' "${STATS_RECONCILE_ATTEMPT_RECEIPT}")"
    if "${REPO_ROOT}/.venv/bin/python" \
      "${REPO_ROOT}/scripts/vps/select_stats_reconciliation_leagues.py" finish \
      --leagues "${STATS_RECONCILE_LEAGUES}" \
      --group-size "${STATS_RECONCILE_LEAGUE_GROUP_SIZE}" \
      --cursor-file "${STATS_RECONCILE_LEAGUE_CURSOR_FILE}" \
      --release-id "${RUNTIME_RELEASE_ID}" \
      --attempt-id "${attempt_id}" \
      --exit-status "${status}" \
      --report-file "${STATS_RECONCILE_REPORT}" \
      --max-scope-age-seconds "${STATS_RECONCILE_SCOPE_MAX_AGE_SECONDS}" \
      >"${STATS_RECONCILE_OUTCOME_FILE}"; then
      read -r scope_outcome empty_sweep < <("${REPO_ROOT}/.venv/bin/python" -c \
        'import json,sys; p=json.load(open(sys.argv[1])); print(p["outcome"], str(p["aggregate"]["empty_sweep"]).lower())' \
        "${STATS_RECONCILE_OUTCOME_FILE}")
    else
      log_error "stats reconciliation scope completion could not be validated"
      status=1
      scope_outcome="invalid_state"
    fi
  fi
  if [[ "${status}" -eq 0 && "${scope_outcome}" != "empty" && "${scope_outcome}" != "processed" ]]; then
    status=1
  fi
  RUN_FINISHED_AT="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  RUN_FINISHED_EPOCH_MS="$(epoch_milliseconds)"
  TOTAL_DURATION_MS=$((RUN_FINISHED_EPOCH_MS - RUN_STARTED_EPOCH_MS))

  QUEUE_WAIT_MS=""
  EXECUTION_DURATION_MS=""
  if [[ -f "${STATS_RECONCILE_TIMING_FILE}" ]]; then
    timing_acquired_ms="$(sed -n '1p' "${STATS_RECONCILE_TIMING_FILE}")"
    timing_finished_ms="$(sed -n '2p' "${STATS_RECONCILE_TIMING_FILE}")"
    if [[ "${timing_acquired_ms}" =~ ^[0-9]+$ && "${timing_finished_ms}" =~ ^[0-9]+$ ]]; then
      QUEUE_WAIT_MS=$((timing_acquired_ms - RUN_STARTED_EPOCH_MS))
      EXECUTION_DURATION_MS=$((timing_finished_ms - timing_acquired_ms))
    fi
  fi

  if [[ ! -f "${STATS_RECONCILE_REPORT}" || "${scope_outcome}" != "empty" && "${scope_outcome}" != "processed" ]]; then
    # The worker may fail before it can write a JSON report. Preserve a bounded
    # error excerpt (20 lines, at most 8 KiB) before the next loop deletes the
    # temporary run log; the ledger's status/duration alone cannot name the
    # failing stage.
    if [[ "${status}" -ne 0 ]]; then
      log_error "stats reconciliation exited status=${status} before report release=${RUNTIME_RELEASE_ID}"
    else
      log_info "stats reconciliation returned without report release=${RUNTIME_RELEASE_ID}"
    fi
    if [[ -f "${STATS_RECONCILE_RUN_LOG}" ]]; then
      tail -n 20 "${STATS_RECONCILE_RUN_LOG}" | tail -c 8192 >&2 || true
    else
      log_error "stats reconciliation run log missing before report release=${RUNTIME_RELEASE_ID}"
    fi
    PIPELINE_EVIDENCE_FILE="${STATS_RECONCILE_OUTCOME_FILE}" record_pipeline_job_run \
      "run_stats_reconciliation" "Stats reconciliation" "${status}" \
      "${RUN_STARTED_AT}" "${RUN_FINISHED_AT}" "${TOTAL_DURATION_MS}" \
      "${QUEUE_WAIT_MS}" "${EXECUTION_DURATION_MS}" "" "" "" "" "1"
    record_scope_health
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

  read -r work_units provider_duration_ms < <(${REPO_ROOT}/.venv/bin/python - "${STATS_RECONCILE_REPORT}" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    report = json.load(handle)
provider_ms = round(float(report.get("stage_seconds", {}).get("provider_fetch", 0)) * 1000)
print(int(report.get("fixtures_selected", 0)), provider_ms)
PY
)
  processing_duration_ms=""
  if [[ "${EXECUTION_DURATION_MS}" =~ ^[0-9]+$ && "${provider_duration_ms}" =~ ^[0-9]+$ ]]; then
    processing_duration_ms=$((EXECUTION_DURATION_MS - provider_duration_ms))
    if (( processing_duration_ms < 0 )); then
      processing_duration_ms=0
    fi
  fi

  PIPELINE_EVIDENCE_FILE="${STATS_RECONCILE_OUTCOME_FILE}" record_pipeline_job_run \
    "run_stats_reconciliation" \
    "Stats reconciliation" \
    "${status}" \
    "${RUN_STARTED_AT}" \
    "${RUN_FINISHED_AT}" \
    "${TOTAL_DURATION_MS}" \
    "${QUEUE_WAIT_MS}" \
    "${EXECUTION_DURATION_MS}" \
    "${work_units}" \
    "${provider_duration_ms}" \
    "${processing_duration_ms}" \
    "" "1"
  record_scope_health

  if [[ "${status}" -ne 0 ]]; then
    tail -n 20 "${STATS_RECONCILE_RUN_LOG}" >&2 || true
    sleep "${STATS_RECONCILE_SLEEP_SECONDS}" 9>&-
    continue
  fi

  if [[ "${batches}" -eq 0 ]]; then
    if [[ "${empty_sweep}" == "true" ]]; then
      log_info "stats reconciliation queue drained across all ${STATS_RECONCILE_GROUP_COUNT} league groups"
      exit 0
    fi
    log_info "stats reconciliation scope empty; checking next league"
    sleep "${STATS_RECONCILE_SUCCESS_HANDOFF_SECONDS}" 9>&-
    continue
  fi

  tail -n 8 "${STATS_RECONCILE_RUN_LOG}" || true
  sleep "${STATS_RECONCILE_SUCCESS_HANDOFF_SECONDS}" 9>&-
done
