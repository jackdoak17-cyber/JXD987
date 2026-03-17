#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

log_info() {
  printf '%s [INFO] %s\n' "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" "$*"
}

log_error() {
  printf '%s [ERROR] %s\n' "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" "$*" >&2
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
