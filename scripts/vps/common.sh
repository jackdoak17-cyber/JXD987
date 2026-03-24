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
