#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
RUNTIME_MANIFEST_PATH="${REPO_ROOT}/scripts/vps/runtime_manifest.sha1"

log_info() {
  printf '%s [INFO] %s\n' "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" "$*"
}

log_error() {
  printf '%s [ERROR] %s\n' "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" "$*" >&2
}

# The finite trial and both scheduled database-heavy entrants must share these
# lock identities. Fail closed if a sourced or inherited environment changes
# either path; otherwise a quarantine marker could protect the wrong file.
require_default_trial_lock_paths_or_exit() {
  if [[ "${LEADERBOARD_SHADOW_LOCK_FILE:-/var/lock/oddssearch-leaderboard-shadow.lock}" != "/var/lock/oddssearch-leaderboard-shadow.lock" \
     || "${ODDSSEARCH_DB_HEAVY_LOCK_FILE:-/var/lock/oddssearch-db-heavy.lock}" != "/var/lock/oddssearch-db-heavy.lock" ]]; then
    log_error "finite-trial lock paths must use the shared production defaults"
    exit 1
  fi
}

hash_file() {
  local path="$1"
  if command -v shasum >/dev/null 2>&1; then
    shasum "${path}" | awk '{print $1}'
    return 0
  fi
  if command -v sha1sum >/dev/null 2>&1; then
    sha1sum "${path}" | awk '{print $1}'
    return 0
  fi
  return 1
}

runtime_release_id() {
  if [[ ! -f "${RUNTIME_MANIFEST_PATH}" ]]; then
    printf 'manifest-missing'
    return 0
  fi
  local digest
  digest="$(hash_file "${RUNTIME_MANIFEST_PATH}" 2>/dev/null || true)"
  if [[ -z "${digest}" ]]; then
    printf 'manifest-unhashed'
    return 0
  fi
  printf '%s' "${digest:0:12}"
}

verify_runtime_manifest() {
  if [[ ! -f "${RUNTIME_MANIFEST_PATH}" ]]; then
    log_error "runtime manifest missing: ${RUNTIME_MANIFEST_PATH}"
    return 1
  fi

  if ! command -v shasum >/dev/null 2>&1 && ! command -v sha1sum >/dev/null 2>&1; then
    log_error "runtime verification requires shasum or sha1sum"
    return 1
  fi

  local expected relpath abs actual mismatches=0
  while read -r expected relpath; do
    [[ -n "${expected}" ]] || continue
    [[ "${expected}" == \#* ]] && continue
    if [[ -z "${relpath}" ]]; then
      log_error "invalid runtime manifest row: ${expected}"
      mismatches=1
      continue
    fi
    abs="${REPO_ROOT}/${relpath}"
    if [[ ! -f "${abs}" ]]; then
      log_error "runtime file missing: ${relpath}"
      mismatches=1
      continue
    fi
    actual="$(hash_file "${abs}" 2>/dev/null || true)"
    if [[ -z "${actual}" ]]; then
      log_error "unable to hash runtime file: ${relpath}"
      mismatches=1
      continue
    fi
    if [[ "${actual}" != "${expected}" ]]; then
      log_error "runtime drift detected for ${relpath}: expected=${expected} actual=${actual}"
      mismatches=1
    fi
  done < "${RUNTIME_MANIFEST_PATH}"

  [[ "${mismatches}" -eq 0 ]]
}

verify_runtime_manifest_or_exit() {
  local entrypoint="${1:-unknown}"
  if ! verify_runtime_manifest; then
    log_error "runtime manifest verification failed for $(basename "${entrypoint}")"
    exit 1
  fi
  log_info "runtime manifest verified release=$(runtime_release_id) entrypoint=$(basename "${entrypoint}")"
}

require_runtime_manifest_entries_or_exit() {
  local entrypoint="${1:-unknown}"
  shift || true

  if [[ ! -f "${RUNTIME_MANIFEST_PATH}" ]]; then
    log_error "runtime manifest missing while checking required entries for $(basename "${entrypoint}")"
    exit 1
  fi

  local manifest_paths=""
  manifest_paths="$(awk '{print $2}' "${RUNTIME_MANIFEST_PATH}" 2>/dev/null || true)"
  if [[ -z "${manifest_paths}" ]]; then
    log_error "runtime manifest unreadable while checking required entries for $(basename "${entrypoint}")"
    exit 1
  fi

  local missing=0
  local relpath
  for relpath in "$@"; do
    # Avoid piping into grep -q while pipefail is enabled: grep may exit early
    # after finding a match, causing printf to receive SIGPIPE and producing a
    # false missing-file result.
    if ! grep -Fqx -- "${relpath}" <<<"${manifest_paths}"; then
      log_error "runtime manifest missing required file for $(basename "${entrypoint}"): ${relpath}"
      missing=1
    fi
  done

  if [[ "${missing}" -ne 0 ]]; then
    exit 1
  fi
}

default_league_csv() {
  paste -sd, "${REPO_ROOT}/config/league_ids.txt"
}

# Return a stable, de-duplicated CSV union for wrappers that combine the
# supported SportMonks and Odds API league sets.
union_csv() {
  python3 -c 'import sys; seen=set(); ordered=[]; [((lambda v: (None if not v or v in seen else (seen.add(v), ordered.append(v))))(v.strip())) for raw in sys.argv[1:] for v in str(raw).replace("\n", " ,").split(",")] ; print(",".join(ordered))' "$@"
}

supported_league_csv() {
  python3 - "${REPO_ROOT}/config/league_ids.txt" "${REPO_ROOT}/config/odds_api_sync_excluded_leagues.json" <<'PY'
import json
import sys
from pathlib import Path

configured = []
for raw in Path(sys.argv[1]).read_text(encoding="utf-8").splitlines():
    value = raw.strip()
    if not value or value.startswith("#"):
        continue
    configured.append(int(value))

excluded_path = Path(sys.argv[2])
excluded = set(json.loads(excluded_path.read_text(encoding="utf-8"))) if excluded_path.exists() else set()
print(",".join(str(value) for value in configured if value not in excluded))
PY
}

validate_supported_leagues() {
  python3 - "${REPO_ROOT}/config/odds_api_sync_excluded_leagues.json" "$1" <<'PY'
import json
import sys
from pathlib import Path

excluded_path = Path(sys.argv[1])
excluded = set(json.loads(excluded_path.read_text(encoding="utf-8"))) if excluded_path.exists() else set()
requested = {int(value.strip()) for value in sys.argv[2].split(",") if value.strip()}
blocked = sorted(requested.intersection(excluded))
if blocked:
    print(f"Unsupported cup league IDs in fixture pipeline: {blocked}", file=sys.stderr)
    raise SystemExit(1)
PY
}

odds_league_csv() {
python3 - "${REPO_ROOT}/config/league_ids.txt" "${REPO_ROOT}/config/odds_api_leagues.json" "${REPO_ROOT}/config/odds_api_sync_excluded_leagues.json" <<'PY'
import json
import sys
from pathlib import Path

league_ids_path = Path(sys.argv[1])
odds_map_path = Path(sys.argv[2])
excluded_path = Path(sys.argv[3])

configured_ids = []
for line in league_ids_path.read_text(encoding="utf-8").splitlines():
    value = line.strip()
    if not value or value.startswith("#"):
        continue
    configured_ids.append(int(value))

odds_map = json.loads(odds_map_path.read_text(encoding="utf-8"))
excluded_ids = {
    int(value)
    for value in json.loads(excluded_path.read_text(encoding="utf-8"))
} if excluded_path.exists() else set()
odds_ids = {int(value) for value in odds_map if int(value) not in excluded_ids}
print(",".join(str(league_id) for league_id in configured_ids if league_id in odds_ids))
PY
}

pipeline_job_status_name() {
  local status="$1"
  case "${status}" in
    0) printf 'success' ;;
    2) printf 'skipped' ;;
    *) printf 'failure' ;;
  esac
}

record_pipeline_job_run() {
  local job_id="$1"
  local job_name="$2"
  local status_code="$3"
  local started_at="$4"
  local finished_at="$5"
  local duration_ms="$6"
  local queue_wait_ms="${7:-}"
  local execution_duration_ms="${8:-}"
  local work_units="${9:-}"
  local provider_duration_ms="${10:-}"
  local processing_duration_ms="${11:-}"

  if [[ -z "${OPERATIONS_CHECK_RUNNER_DATABASE_URL:-}" && -f "${REPO_ROOT}/.env" ]]; then
    set -a
    # shellcheck disable=SC1091
    source "${REPO_ROOT}/.env"
    set +a
  fi
  if [[ -z "${OPERATIONS_CHECK_RUNNER_DATABASE_URL:-}" ]]; then
    log_error "pipeline heartbeat skipped for ${job_id}; missing OPERATIONS_CHECK_RUNNER_DATABASE_URL"
    return 0
  fi
  if ! command -v psql >/dev/null 2>&1; then
    log_error "pipeline heartbeat skipped for ${job_id}; psql unavailable"
    return 0
  fi

  local run_status release_id evidence evidence_file evidence_payload
  run_status="$(pipeline_job_status_name "${status_code}")"
  release_id="$(runtime_release_id)"
  evidence="exit status: ${status_code}; completion status: ${run_status}"
  if [[ -n "${queue_wait_ms}" || -n "${execution_duration_ms}" || -n "${work_units}" ]]; then
    evidence="${evidence}; queue_wait_ms=${queue_wait_ms:-unknown}; execution_duration_ms=${execution_duration_ms:-unknown}; provider_duration_ms=${provider_duration_ms:-unknown}; processing_duration_ms=${processing_duration_ms:-unknown}; work_units=${work_units:-unknown}"
  fi
  evidence_file="${PIPELINE_EVIDENCE_FILE:-}"
  if [[ -n "${evidence_file}" && -f "${evidence_file}" ]]; then
    # Do not use a truncating pipeline here. With pipefail enabled, `tr | head`
    # can return 141 when the report is large, aborting the wrapper before it
    # records the completion heartbeat. Read and truncate in one process.
    evidence_payload="$(python3 - "${evidence_file}" <<'PY'
import pathlib
import sys

payload = pathlib.Path(sys.argv[1]).read_text(encoding="utf-8", errors="replace")
sys.stdout.write(payload.replace("\n", " ")[:3500])
PY
)"
    if [[ -n "${evidence_payload}" ]]; then
      evidence="${evidence}; report: ${evidence_payload}"
    fi
  fi
  # operations.pipeline_job_runs.evidence_summary is intentionally bounded so
  # a verbose JSON report can never make an otherwise completed run fail while
  # recording its heartbeat.
  evidence="${evidence:0:500}"

  if ! psql "${OPERATIONS_CHECK_RUNNER_DATABASE_URL}" \
    -v ON_ERROR_STOP=1 \
    -v job_id="${job_id}" \
    -v job_name="${job_name}" \
    -v run_status="${run_status}" \
    -v started_at="${started_at}" \
    -v finished_at="${finished_at}" \
    -v duration_ms="${duration_ms}" \
    -v queue_wait_ms="${queue_wait_ms}" \
    -v execution_duration_ms="${execution_duration_ms}" \
    -v work_units="${work_units}" \
    -v provider_duration_ms="${provider_duration_ms}" \
    -v processing_duration_ms="${processing_duration_ms}" \
    -v release_id="${release_id}" \
    -v evidence="${evidence}" <<'SQL' >/dev/null
insert into operations.pipeline_job_runs (
  job_id, job_name, status, started_at, finished_at, duration_ms,
  release_id, evidence_summary, queue_wait_ms, execution_duration_ms, work_units,
  provider_duration_ms, processing_duration_ms
)
values (
  :'job_id', :'job_name', :'run_status', :'started_at'::timestamptz,
  :'finished_at'::timestamptz, :'duration_ms'::integer, :'release_id', :'evidence',
  nullif(:'queue_wait_ms', '')::integer,
  nullif(:'execution_duration_ms', '')::integer,
  nullif(:'work_units', '')::integer,
  nullif(:'provider_duration_ms', '')::integer,
  nullif(:'processing_duration_ms', '')::integer
);
SQL
  then
    log_error "pipeline heartbeat write failed for ${job_id}"
  fi
}

run_recorded_pipeline_job() {
  local job_id="$1"
  local job_name="$2"
  local chain_command="$3"
  local evidence_file="${4:-}"
  local started_at started_epoch finished_at finished_epoch status=0
  local evidence_was_set=0 previous_evidence_file=""

  if [[ -n "${PIPELINE_EVIDENCE_FILE+x}" ]]; then
    evidence_was_set=1
    previous_evidence_file="${PIPELINE_EVIDENCE_FILE}"
  fi
  if [[ -n "${evidence_file}" ]]; then
    export PIPELINE_EVIDENCE_FILE="${evidence_file}"
  else
    unset PIPELINE_EVIDENCE_FILE || true
  fi

  started_at="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  started_epoch="$(date -u +%s)"
  if [[ "${ODDS_SYNC_LOCK_RETRY_ATTEMPTS:-0}" =~ ^[1-9][0-9]*$ ]]; then
    run_with_global_lock_and_retry "${chain_command}" || status=$?
  else
    run_with_global_lock_and_timeout "${chain_command}" || status=$?
  fi
  # A caller may explicitly disable retries. The settlement handoff still
  # means admitted work was stopped deliberately, not that the run failed.
  if [[ "${status}" -eq 76 && "${ODDS_SYNC_STOP_RETRY_AFTER_LEASE_HANDOFF:-false}" == "true" ]]; then
    status=2
  fi
  # Deferral before admission also ends this invocation's claim. Drop it
  # before the database heartbeat, just as admitted work does on exit.
  if [[ "${ODDS_SYNC_JOB_PRIORITY:-normal}" == "fixture_core" && -n "${FIXTURE_CORE_RESERVATION_OWNER:-}" ]]; then
    fixture_core_reservation_release "${FIXTURE_CORE_RESERVATION_OWNER}" || {
      log_error "fixture-core reservation release failed before heartbeat"
      status=1
    }
  fi
  finished_at="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  finished_epoch="$(date -u +%s)"

  # Record the outcome before restoring the caller's evidence context. This
  # makes lock skips and timeout handoffs observable just like hard failures.
  record_pipeline_job_run \
    "${job_id}" \
    "${job_name}" \
    "${status}" \
    "${started_at}" \
    "${finished_at}" \
    "$(((finished_epoch - started_epoch) * 1000))"

  if [[ "${evidence_was_set}" -eq 1 ]]; then
    export PIPELINE_EVIDENCE_FILE="${previous_evidence_file}"
  else
    unset PIPELINE_EVIDENCE_FILE || true
  fi
  return "${status}"
}

run_with_global_lock_and_retry() {
  local chain_command="$1"
  local retry_attempts="${ODDS_SYNC_LOCK_RETRY_ATTEMPTS:-0}"
  local retry_delay_seconds="${ODDS_SYNC_LOCK_RETRY_DELAY_SECONDS:-15}"
  local retry_deadline_seconds="${ODDS_SYNC_LOCK_RETRY_DEADLINE_SECONDS:-0}"
  local retry_started_epoch
  local attempt=0 status=2

  if [[ ! "${retry_attempts}" =~ ^[0-9]+$ ]]; then
    log_error "ODDS_SYNC_LOCK_RETRY_ATTEMPTS must be a non-negative integer"
    return 1
  fi
  if [[ ! "${retry_delay_seconds}" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    log_error "ODDS_SYNC_LOCK_RETRY_DELAY_SECONDS must be a non-negative number"
    return 1
  fi
  if [[ ! "${retry_deadline_seconds}" =~ ^[0-9]+$ ]]; then
    log_error "ODDS_SYNC_LOCK_RETRY_DEADLINE_SECONDS must be a non-negative integer"
    return 1
  fi
  retry_started_epoch="${ODDS_SYNC_LOCK_RETRY_STARTED_EPOCH:-$(date -u +%s)}"

  while (( attempt <= retry_attempts )); do
    if (( retry_deadline_seconds > 0 && $(date -u +%s) - retry_started_epoch >= retry_deadline_seconds )); then
      log_info "[SKIPPED] writer-lock retry deadline reached"
      return 2
    fi
    ODDS_SYNC_LOCK_RETRY_STARTED_EPOCH="${retry_started_epoch}" \
      run_with_global_lock_and_timeout "${chain_command}" && return 0
    status=$?
    if [[ "${status}" -eq 76 && "${ODDS_SYNC_STOP_RETRY_AFTER_LEASE_HANDOFF:-false}" == "true" ]]; then
      # The job already acquired the canonical lock and began provider work.
      # A later settlement handoff is a skip, but should not repeat that work
      # inside this same invocation when the caller opts out of such retries.
      return 2
    fi
    if [[ "${status}" -ne 2 ]]; then
      return "${status}"
    fi
    attempt=$((attempt + 1))
    if (( attempt > retry_attempts )); then
      return 2
    fi
    if (( retry_deadline_seconds > 0 && $(date -u +%s) - retry_started_epoch >= retry_deadline_seconds )); then
      log_info "[SKIPPED] writer-lock retry deadline reached"
      return 2
    fi
    log_info "[RETRY] lock handoff; retry ${attempt}/${retry_attempts} in ${retry_delay_seconds}s"
    sleep "${retry_delay_seconds}"
  done
  return "${status}"
}

healthcheck_ping() {
  local url="${1:-}"
  if [[ -z "${url}" ]]; then
    return 0
  fi
  if ! curl -fsS --max-time 10 --retry 2 "${url}" >/dev/null; then
    log_error "Healthcheck ping failed for ${url}"
    return 1
  fi
  return 0
}

# Serialise database-heavy maintenance that is safe independently but causes
# measurable ingestion regressions when it overlaps. This lock is deliberately
# separate from ODDS_SYNC_LOCK_FILE: the latter protects the canonical SQLite
# writer, while this slot protects Supabase-heavy reconciliation/publication.
#
# Return 75 (EX_TEMPFAIL) when the slot is unavailable so callers can defer
# without recording a failed data run. The file descriptor is owned by the
# subshell and is therefore released on normal exit, error, timeout, or signal.
run_with_database_heavy_lock() {
  local wait_seconds="$1"
  shift
  local lock_file="${ODDSSEARCH_DB_HEAVY_LOCK_FILE:-/var/lock/oddssearch-db-heavy.lock}"

  if [[ ! "${wait_seconds}" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    log_error "database-heavy lock wait must be a non-negative number"
    return 1
  fi
  if [[ "$#" -eq 0 ]]; then
    log_error "database-heavy lock command is required"
    return 1
  fi

  mkdir -p "$(dirname "${lock_file}")"
  (
    exec 8>"${lock_file}"
    if ! flock --wait "${wait_seconds}" 8; then
      log_info "[SKIPPED] database-heavy maintenance slot unavailable"
      exit 75
    fi
    if [[ -e "${LEADERBOARD_SHADOW_LOCK_FILE:-/var/lock/oddssearch-leaderboard-shadow.lock}.quarantine.heavy" \
       || -L "${LEADERBOARD_SHADOW_LOCK_FILE:-/var/lock/oddssearch-leaderboard-shadow.lock}.quarantine.heavy" ]]; then
      log_info "[SKIPPED] finite-trial quarantine blocks database-heavy maintenance"
      exit 75
    fi
    "$@"
  )
}

epoch_milliseconds() {
  python3 -c 'import time; print(time.time_ns() // 1_000_000)'
}

# Acquire the same importer-priority slot while recording only the time spent
# after admission as execution. The caller keeps end-to-end duration separately
# so scheduler/lock delay remains visible without masquerading as slower work.
run_with_database_heavy_lock_timed() {
  local wait_seconds="$1"
  local timing_file="$2"
  shift 2
  local lock_file="${ODDSSEARCH_DB_HEAVY_LOCK_FILE:-/var/lock/oddssearch-db-heavy.lock}"

  if [[ ! "${wait_seconds}" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    log_error "database-heavy lock wait must be a non-negative number"
    return 1
  fi
  if [[ "$#" -eq 0 ]]; then
    log_error "database-heavy lock command is required"
    return 1
  fi

  mkdir -p "$(dirname "${lock_file}")" "$(dirname "${timing_file}")"
  (
    local status acquired_ms finished_ms
    exec 8>"${lock_file}"
    if ! flock --wait "${wait_seconds}" 8; then
      log_info "[SKIPPED] database-heavy maintenance slot unavailable"
      exit 75
    fi
    if [[ -e "${LEADERBOARD_SHADOW_LOCK_FILE:-/var/lock/oddssearch-leaderboard-shadow.lock}.quarantine.heavy" \
       || -L "${LEADERBOARD_SHADOW_LOCK_FILE:-/var/lock/oddssearch-leaderboard-shadow.lock}.quarantine.heavy" ]]; then
      log_info "[SKIPPED] finite-trial quarantine blocks database-heavy maintenance"
      exit 75
    fi
    acquired_ms="$(epoch_milliseconds)"
    printf '%s\n' "${acquired_ms}" >"${timing_file}"
    set +e
    "$@"
    status=$?
    set -e
    finished_ms="$(epoch_milliseconds)"
    printf '%s\n' "${finished_ms}" >>"${timing_file}"
    exit "${status}"
  )
}

# The fixture-core source must be admitted before it ages out, but the
# quarter-hour settlement still has priority. A short-lived reservation keeps
# newly arriving normal writers from overtaking an already waiting fixture
# core. Both files are derived from the canonical writer lock path, so an
# unrelated lock domain (for example Models) is unaffected.
fixture_core_reservation_file() {
  printf '%s.fixture-core-reservation' "${ODDS_SYNC_LOCK_FILE:-/var/lock/odds-sync.lock}"
}

fixture_core_reservation_active() {
  local reservation_file="$1" owner expires now
  [[ -f "${reservation_file}" ]] || return 1
  read -r owner expires <"${reservation_file}" || return 1
  if [[ ! "${owner}" =~ ^[0-9a-f-]{36}$ || ! "${expires}" =~ ^[0-9]+$ ]]; then
    log_error "ignoring malformed fixture-core reservation at ${reservation_file}"
    return 1
  fi
  now="$(date -u +%s)"
  (( expires > now ))
}

fixture_core_reservation_owned_for() {
  local reservation_file="$1" expected_owner="$2" minimum_seconds="$3"
  local owner expires now
  [[ -f "${reservation_file}" ]] || return 1
  read -r owner expires <"${reservation_file}" || return 1
  [[ "${owner}" == "${expected_owner}" && "${expires}" =~ ^[0-9]+$ ]] || return 1
  now="$(date -u +%s)"
  (( expires > now + minimum_seconds ))
}

fixture_core_reservation_claim() {
  local reservation_file guard_file ttl
  reservation_file="$(fixture_core_reservation_file)"
  guard_file="${reservation_file}.guard"
  ttl="${FIXTURE_CORE_RESERVATION_TTL_SECONDS:-3600}"
  if [[ ! "${ttl}" =~ ^[1-9][0-9]*$ ]] || (( ttl > 3600 )); then
    log_error "fixture-core reservation TTL must be between 1 and 3600 seconds"
    return 1
  fi
  mkdir -p "$(dirname "${reservation_file}")" || return 1
  (
    local owner expires tmp
    umask 077
    flock -w 2 -x 7 || exit 2
    if fixture_core_reservation_active "${reservation_file}"; then
      exit 2
    fi
    owner="$(python3 -c 'import uuid; print(uuid.uuid4())')" || exit 1
    expires=$(( $(date -u +%s) + ttl ))
    tmp="$(mktemp "${reservation_file}.tmp.XXXXXXXX")" || exit 1
    if ! printf '%s %s\n' "${owner}" "${expires}" >"${tmp}" ||
       ! mv -f "${tmp}" "${reservation_file}"; then
      rm -f "${tmp}"
      exit 1
    fi
    printf '%s\n' "${owner}"
  ) 7>"${guard_file}"
}

fixture_core_reservation_release() {
  local expected_owner="$1" reservation_file guard_file owner expires
  reservation_file="$(fixture_core_reservation_file)"
  guard_file="${reservation_file}.guard"
  [[ -f "${reservation_file}" ]] || return 0
  (
    flock -w 2 -x 7 || exit 1
    [[ -f "${reservation_file}" ]] || exit 0
    read -r owner expires <"${reservation_file}" || exit 1
    if [[ "${owner}" == "${expected_owner}" ]]; then
      rm -f "${reservation_file}"
    fi
  ) 7>"${guard_file}"
}

run_with_global_lock_and_timeout() {
  local chain_command="$1"
  local lock_file="${ODDS_SYNC_LOCK_FILE:-/var/lock/odds-sync.lock}"
  local max_runtime="${ODDS_SYNC_P3_MAX_DURATION_SECONDS:-900}"
  local job_priority="${ODDS_SYNC_JOB_PRIORITY:-normal}"
  local lock_wait_seconds="${ODDS_SYNC_LOCK_WAIT_SECONDS:-0}"
  local lock_poll_seconds="${ODDS_SYNC_LOCK_POLL_SECONDS:-5}"
  local live_tick_seconds="${ODDS_SYNC_LIVE_TICK_SECONDS:-900}"
  local live_reserve_seconds="${ODDS_SYNC_LIVE_RESERVE_SECONDS:-180}"
  local live_grace_seconds="${ODDS_SYNC_LIVE_GRACE_SECONDS:-120}"
  local min_normal_lease_seconds="${ODDS_SYNC_MIN_NORMAL_LEASE_SECONDS:-0}"
  local live_schedule_enabled="${ODDS_SYNC_LIVE_SCHEDULE_ENABLED:-true}"
  local reservation_file reservation_owner="${FIXTURE_CORE_RESERVATION_OWNER:-}"
  local retry_started_epoch="${ODDS_SYNC_LOCK_RETRY_STARTED_EPOCH:-0}"
  local retry_deadline_seconds="${ODDS_SYNC_LOCK_RETRY_DEADLINE_SECONDS:-0}"

  if [[ ! "${retry_started_epoch}" =~ ^[0-9]+$ || ! "${retry_deadline_seconds}" =~ ^[0-9]+$ ]]; then
    log_error "writer-lock retry deadline requires numeric start and duration"
    return 1
  fi

  mkdir -p "$(dirname "${lock_file}")"

  (
    # The quarter-hour settlement is the critical stats writer. Keep normal
    # writers from starting shortly before/after its tick, while allowing the
    # settlement writer to wait for a writer that was already in flight.
    now_epoch="$(date -u +%s)"
    live_phase=$((now_epoch % live_tick_seconds))
    if [[ "${live_schedule_enabled}" == "true" && "${job_priority}" != "settlement" ]] && {
      (( live_phase >= live_tick_seconds - live_reserve_seconds )) ||
      (( live_phase < live_grace_seconds ));
    }; then
      log_info "[SKIPPED] live settlement reservation active (phase=${live_phase}s)"
      exit 2
    fi

    # Use the same short guard as fixture-core reservation creation. A normal
    # writer admitted before a reservation claim may finish; one arriving
    # afterward yields before it can take the canonical writer lock.
    if [[ "${job_priority}" == "normal" ]]; then
      reservation_file="$(fixture_core_reservation_file)"
      exec 7>"${reservation_file}.guard"
      if ! flock -w 2 -x 7; then
        log_info "[SKIPPED] fixture-core admission guard unavailable"
        exit 2
      fi
      if fixture_core_reservation_active "${reservation_file}"; then
        log_info "[SKIPPED] fixture-core reservation active"
        exit 2
      fi
    fi

    if [[ "${job_priority}" == "fixture_core" && -z "${reservation_owner}" ]]; then
      log_error "fixture-core admission requires a reservation owner"
      exit 1
    fi

    wait_started="$(date -u +%s)"
    while ! flock --nonblock 9; do
      if [[ "${job_priority}" != "settlement" ]]; then
        log_info "[SKIPPED] lock unavailable, will retry next tick"
        exit 2
      fi
      waited=$(( $(date -u +%s) - wait_started ))
      if (( waited >= lock_wait_seconds )); then
        log_info "[SKIPPED] settlement lock unavailable after ${waited}s; will retry next tick"
        exit 2
      fi
      sleep "${lock_poll_seconds}" 9>&-
    done
    if [[ "${job_priority}" == "normal" ]]; then
      flock -u 7
      exec 7>&-
    fi

    local effective_runtime="${max_runtime}"
    if [[ "${live_schedule_enabled}" == "true" && "${job_priority}" != "settlement" ]]; then
      # A normal writer may start outside the reservation window but still
      # overrun the next settlement tick if it consumes its full timeout.
      # Cap the in-flight lease so the canonical lock is released before the
      # settlement grace period. Every normal chain is resumable/idempotent,
      # so a planned handoff is safer than allowing a long writer to starve
      # the critical settlement path.
      now_epoch="$(date -u +%s)"
      live_phase=$((now_epoch % live_tick_seconds))
      seconds_to_tick=$((live_tick_seconds - live_phase))
      available_runtime=$((seconds_to_tick - live_grace_seconds))
      if (( available_runtime <= 0 )); then
        log_info "[SKIPPED] no normal-writer lease before settlement grace window (phase=${live_phase}s)"
        exit 2
      fi
      if (( min_normal_lease_seconds > 0 && available_runtime < min_normal_lease_seconds )); then
        log_info "[SKIPPED] normal-writer lease too short for bounded job (available=${available_runtime}s minimum=${min_normal_lease_seconds}s)"
        exit 2
      fi
      if (( available_runtime < effective_runtime )); then
        effective_runtime="${available_runtime}"
        log_info "normal-writer lease capped at ${effective_runtime}s before settlement grace window"
      fi
    fi

    if [[ "${job_priority}" == "fixture_core" ]]; then
      # Recheck the exact owner immediately before provider work. Its expiry
      # must outlast this lease, including timeout's five-second kill grace.
      reservation_file="$(fixture_core_reservation_file)"
      exec 7>"${reservation_file}.guard"
      if ! flock -w 2 -x 7; then
        log_info "[SKIPPED] fixture-core owner guard unavailable"
        exit 2
      fi
      if [[ "${live_schedule_enabled}" == "true" ]]; then
        # Owner validation must use the lease left after acquiring this guard,
        # not the longer lease calculated before the wait.
        now_epoch="$(date -u +%s)"
        live_phase=$((now_epoch % live_tick_seconds))
        available_runtime=$((live_tick_seconds - live_phase - live_grace_seconds))
        if (( available_runtime <= 0 ||
              (min_normal_lease_seconds > 0 && available_runtime < min_normal_lease_seconds) )); then
          log_info "[SKIPPED] fixture-core lease too short after owner guard"
          exit 2
        fi
        if (( available_runtime < effective_runtime )); then
          effective_runtime="${available_runtime}"
        fi
      fi
      if ! fixture_core_reservation_owned_for "${reservation_file}" "${reservation_owner}" "$((effective_runtime + 10))"; then
        log_info "[SKIPPED] fixture-core reservation owner expired or changed before admission"
        exit 2
      fi
      flock -u 7
      exec 7>&-
    fi

    # Guard acquisition and lock handoff can advance the clock after the
    # initial check. Recheck immediately before the work command starts.
    now_epoch="$(date -u +%s)"
    if (( retry_deadline_seconds > 0 && retry_started_epoch > 0 && now_epoch - retry_started_epoch >= retry_deadline_seconds )); then
      log_info "[SKIPPED] writer-lock retry deadline reached before admission"
      exit 2
    fi
    live_phase=$((now_epoch % live_tick_seconds))
    if [[ "${live_schedule_enabled}" == "true" && "${job_priority}" != "settlement" ]] && {
      (( live_phase >= live_tick_seconds - live_reserve_seconds )) ||
      (( live_phase < live_grace_seconds ));
    }; then
      log_info "[SKIPPED] live settlement reservation active before admission (phase=${live_phase}s)"
      exit 2
    fi
    if [[ "${live_schedule_enabled}" == "true" && "${job_priority}" != "settlement" ]]; then
      # The owner-guard wait may consume part of the lease calculated above.
      # Recompute from the actual admission instant before starting the chain.
      available_runtime=$((live_tick_seconds - live_phase - live_grace_seconds))
      if (( available_runtime <= 0 )); then
        log_info "[SKIPPED] no writer lease before settlement grace window after admission wait"
        exit 2
      fi
      if (( min_normal_lease_seconds > 0 && available_runtime < min_normal_lease_seconds )); then
        log_info "[SKIPPED] writer lease too short after admission wait (available=${available_runtime}s minimum=${min_normal_lease_seconds}s)"
        exit 2
      fi
      if (( available_runtime < effective_runtime )); then
        effective_runtime="${available_runtime}"
        log_info "writer lease shortened to ${effective_runtime}s after admission wait"
      fi
    fi

    # The full chain must execute as one subshell so timeout covers every step.
    timeout --signal=TERM --kill-after=5s "${effective_runtime}" bash -lc "${chain_command}"
    local status=$?
    if [[ "${job_priority}" == "fixture_core" ]]; then
      # Release priority at the end of admitted work, before the wrapper's
      # database heartbeat. A stalled heartbeat must not block normal writers.
      fixture_core_reservation_release "${reservation_owner}" || {
        log_error "fixture-core reservation release failed after admitted work"
        exit 1
      }
    fi
    if [[ ${status} -eq 124 || ${status} -eq 137 ]]; then
      if [[ "${job_priority}" != "settlement" && "${effective_runtime}" -lt "${max_runtime}" ]]; then
        log_info "[SKIPPED] normal-writer lease ended for settlement handoff after ${effective_runtime}s"
        if [[ "${ODDS_SYNC_STOP_RETRY_AFTER_LEASE_HANDOFF:-false}" == "true" ]]; then
          exit 76
        fi
        exit 2
      fi
      log_error "process killed after ${max_runtime}s overrun"
      exit 1
    fi
    if [[ ${status} -eq 2 ]]; then
      log_error "process exited with usage/status code 2"
      exit 1
    fi
    exit ${status}
  ) 9>"${lock_file}"
}

finalize_with_healthcheck() {
  local status="$1"
  local ping_url="${2:-}"

  case "${status}" in
    0)
      log_info "run completed successfully"
      healthcheck_ping "${ping_url}" || true
      ;;
    1)
      log_error "run failed"
      ;;
    2)
      log_info "run skipped due to lock contention"
      ;;
    *)
      log_error "unexpected status ${status}; treating as failure"
      status=1
      ;;
  esac

  exit "${status}"
}
