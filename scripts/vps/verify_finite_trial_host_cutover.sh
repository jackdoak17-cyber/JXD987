#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "$ROOT/scripts/vps/common.sh"
mode="${1:-active}"
install_crontab=false
if [[ $# -eq 5 && ( "$mode" == prepare || "$mode" == revert ) && "$2" == --install ]]; then
  install_crontab=true
elif [[ $# -gt 1 || ( "$mode" != active && "$mode" != prepare && "$mode" != revert ) ]]; then
  echo "usage: verify_finite_trial_host_cutover.sh [active|prepare|revert [--install frozen-sha256 target-sha256 target-file]]" >&2
  exit 2
fi
verify_runtime_manifest_or_exit "$0"
require_runtime_manifest_entries_or_exit "$0" \
  "scripts/vps/verify_finite_trial_host_cutover.sh" \
  "scripts/vps/run_stats_reconciliation.sh" \
  "scripts/vps/run_leaderboard_shadow_worker.sh" \
  "scripts/vps/run_leaderboard_finite_trial.sh" \
  "scripts/vps/common.sh"
if [[ "$install_crontab" == true ]]; then
  require_runtime_manifest_entries_or_exit "$0" "scripts/vps/install_leaderboard_crontab.py"
  if [[ ! -f "$TRIAL_LOCK_PINS" || -L "$TRIAL_LOCK_PINS" || ! -O "$TRIAL_LOCK_PINS" ]]; then
    echo "finite trial cron transition requires the existing owned lock pin file" >&2
    exit 1
  fi
  exec 9<"$TRIAL_LOCK_PINS"
  if ! flock -n 9; then
    echo "finite trial cron transition is already running" >&2
    exit 1
  fi
fi

# Each entrant must use this exact archived common.sh. The statistics
# supervisor is long-lived, so a cron edit alone cannot retire its old copy.
schedule="$(crontab -l)"
active_schedule="$(printf '%s\n' "$schedule" | sed '/^[[:space:]]*#/d')"
if [[ "$active_schedule" == *LEADERBOARD_SHADOW_LOCK_FILE* \
   || "$active_schedule" == *ODDSSEARCH_DB_HEAVY_LOCK_FILE* \
   || "$active_schedule" == *STATS_RECONCILE_SUPERVISOR_LOCK* \
   || "$active_schedule" == *LEADERBOARD_RESTRICTED_HOST_PIN_FILE* ]]; then
  echo "finite trial host cutover rejects cron lock-path overrides" >&2
  exit 1
fi
for entrypoint in run_stats_reconciliation.sh run_leaderboard_shadow_worker.sh; do
  matches="$(printf '%s\n' "$active_schedule" | grep -F "/scripts/vps/$entrypoint" || true)"
  exact="cd $ROOT && $ROOT/scripts/vps/$entrypoint"
  if [[ "$mode" != active ]]; then
    if [[ -n "$matches" ]]; then
      echo "finite trial $mode requires both scheduled entrants frozen" >&2
      exit 1
    fi
  elif [[ "$matches" == *$'\n'* || " $matches " != *" $exact "* ]]; then
    echo "finite trial host cutover requires one exact $entrypoint cron entry in this release" >&2
    exit 1
  fi
done
require_default_trial_lock_paths_or_exit

check_legacy_quarantine() {
  if has_quarantine_marker "$LEGACY_TRIAL_WORKER_LOCK"; then
    echo "finite trial host cutover requires resolved legacy quarantine" >&2
    exit 1
  fi
}

check_legacy_quarantine
if [[ "$mode" != active ]]; then
  for lock in "$LEGACY_TRIAL_WORKER_LOCK" "$LEGACY_TRIAL_HEAVY_LOCK" "$STATS_SUPERVISOR_LOCK"; do
    if [[ ! -f "$lock" || -L "$lock" || "$(stat -c %u "$lock")" != "$EUID" ]]; then
      echo "finite trial $mode requires the existing owned legacy lock files" >&2
      exit 1
    fi
  done
  exec 3<"$LEGACY_TRIAL_WORKER_LOCK" 4<"$LEGACY_TRIAL_HEAVY_LOCK"
  exec 5<"$TRIAL_WORKER_LOCK" 6<"$TRIAL_HEAVY_LOCK"
  exec 7<"$STATS_SUPERVISOR_LOCK"
  check_trial_lock_fd worker "$TRIAL_WORKER_LOCK" 5
  check_trial_lock_fd heavy "$TRIAL_HEAVY_LOCK" 6
  for fd in 7 3 4 5 6; do
    if ! flock -n "$fd"; then
      echo "finite trial $mode found an occupied legacy or protected lock" >&2
      exit 1
    fi
  done
fi

# Check the actual values each entrant will see after its own env source.
# A textual assignment scan misses exported values and shell expressions.
(
  if [[ -f "$ROOT/.env" ]]; then
    set -a
    source "$ROOT/.env"
    set +a
  fi
  require_default_trial_lock_paths_or_exit
  [[ "${STATS_RECONCILE_SUPERVISOR_LOCK:-$STATS_SUPERVISOR_LOCK}" == "$STATS_SUPERVISOR_LOCK" ]]
)
(
  worker_env="${LEADERBOARD_SHADOW_ENV_FILE:-/etc/oddssearch/leaderboard-shadow.env}"
  if [[ ! -r "$worker_env" ]]; then
    echo "finite trial host cutover requires readable leaderboard environment" >&2
    exit 1
  fi
  set -a
  source "$worker_env"
  set +a
  require_default_trial_lock_paths_or_exit
)

# The old supervisor is long-lived and can leave a Python child after its
# wrapper exits. Require both the wrapper and every reconciliation child to
# belong to this release in two stable snapshots, with unchanged cron text.
check_process_releases() {
  local pid ppid weekday month day clock year process cwd snapshot
  snapshot="$(ps -eo pid=,ppid=,lstart=,args=)" || {
    echo "finite trial host cutover could not inspect process ancestry" >&2
    return 1
  }
  [[ -n "$snapshot" ]] || {
    echo "finite trial host cutover received an empty process snapshot" >&2
    return 1
  }
  while read -r pid ppid weekday month day clock year process; do
    [[ "$pid" =~ ^[0-9]+$ ]] || continue
    if [[ "$process" == *"/scripts/vps/run_stats_reconciliation.sh"* \
       || "$process" == *"/scripts/vps/run_leaderboard_shadow_worker.sh"* ]]; then
      if [[ "$mode" != active || " $process " != *" $ROOT/scripts/vps/run_stats_reconciliation.sh "* &&
            " $process " != *" $ROOT/scripts/vps/run_leaderboard_shadow_worker.sh "* ]]; then
        echo "finite trial host cutover found old wrapper pid=$pid ppid=$ppid started=$weekday-$month-$day-$clock-$year command=$process" >&2
        return 1
      fi
    fi
    if [[ "$process" == *"reconcile_stats_provider_queue.py"* || "$process" == *"leaderboard_shadow_worker.py"* ]]; then
      if [[ "$mode" != active || " $process " != *" $ROOT/scripts/reconcile_stats_provider_queue.py "* &&
            " $process " != *" $ROOT/scripts/leaderboard_shadow_worker.py "* ]]; then
        echo "finite trial host cutover found old reconciliation child pid=$pid ppid=$ppid started=$weekday-$month-$day-$clock-$year command=$process" >&2
        return 1
      fi
      if [[ -d "/proc/$pid" ]]; then
        cwd="$(readlink "/proc/$pid/cwd" 2>/dev/null || true)"
        if [[ -z "$cwd" && -d "/proc/$pid" ]]; then
          echo "finite trial host cutover could not inspect reconciliation child cwd pid=$pid ppid=$ppid" >&2
          return 1
        fi
        if [[ -n "$cwd" && "$cwd" != "$ROOT" ]]; then
          echo "finite trial host cutover found reconciliation child in another release pid=$pid ppid=$ppid started=$weekday-$month-$day-$clock-$year cwd=$cwd" >&2
          return 1
        fi
      fi
    fi
  done <<<"$snapshot"
}
check_process_releases
sleep 2
[[ "$(crontab -l)" == "$schedule" ]] || {
  echo "finite trial host cutover cron changed during process drain proof" >&2
  exit 1
}
check_process_releases

check_legacy_quarantine
if [[ "$mode" == active ]]; then
  echo "finite trial host cutover verified for both scheduled entrants and descendants"
else
  if has_quarantine_marker "$TRIAL_WORKER_LOCK"; then
    echo "finite trial $mode requires resolved protected quarantine" >&2
    exit 1
  fi
  echo "finite trial $mode verified frozen schedules, drained processes and both lock pairs"
  if [[ "$install_crontab" == true ]]; then
    python3 -I "$ROOT/scripts/vps/install_leaderboard_crontab.py" "$mode" "$ROOT" "$3" "$4" "$5"
  fi
fi
