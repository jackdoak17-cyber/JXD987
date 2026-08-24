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
    if ! printf '%s\n' "${manifest_paths}" | grep -Fxq "${relpath}"; then
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
  evidence_file="${PIPELINE_EVIDENCE_FILE:-}"
  if [[ -n "${evidence_file}" && -f "${evidence_file}" ]]; then
    evidence_payload="$(tr '\n' ' ' < "${evidence_file}" | head -c 3500)"
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
    -v release_id="${release_id}" \
    -v evidence="${evidence}" <<'SQL' >/dev/null
insert into operations.pipeline_job_runs (
  job_id, job_name, status, started_at, finished_at, duration_ms,
  release_id, evidence_summary
)
values (
  :'job_id', :'job_name', :'run_status', :'started_at'::timestamptz,
  :'finished_at'::timestamptz, :'duration_ms'::integer, :'release_id', :'evidence'
);
SQL
  then
    log_error "pipeline heartbeat write failed for ${job_id}"
  fi
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

run_with_global_lock_and_timeout() {
  local chain_command="$1"
  local lock_file="${ODDS_SYNC_LOCK_FILE:-/var/lock/odds-sync.lock}"
  local max_runtime="${ODDS_SYNC_P3_MAX_DURATION_SECONDS:-900}"

  mkdir -p "$(dirname "${lock_file}")"

  (
    flock --nonblock 9 || {
      log_info "[SKIPPED] lock unavailable, will retry next tick"
      exit 2
    }

    # The full chain must execute as one subshell so timeout covers every step.
    timeout "${max_runtime}" bash -lc "${chain_command}"
    local status=$?
    if [[ ${status} -eq 124 || ${status} -eq 137 ]]; then
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
