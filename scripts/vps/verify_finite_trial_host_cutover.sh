#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "$ROOT/scripts/vps/common.sh"
require_default_trial_lock_paths_or_exit
verify_runtime_manifest_or_exit "$0"
require_runtime_manifest_entries_or_exit "$0" \
  "scripts/vps/verify_finite_trial_host_cutover.sh" \
  "scripts/vps/run_stats_reconciliation.sh" \
  "scripts/vps/run_leaderboard_shadow_worker.sh" \
  "scripts/vps/run_leaderboard_finite_trial.sh" \
  "scripts/vps/common.sh"

# Each entrant must use this exact archived common.sh. The statistics
# supervisor is long-lived, so a cron edit alone cannot retire its old copy.
schedule="$(crontab -l)"
active_schedule="$(printf '%s\n' "$schedule" | sed '/^[[:space:]]*#/d')"
if [[ "$active_schedule" == *LEADERBOARD_SHADOW_LOCK_FILE* \
   || "$active_schedule" == *ODDSSEARCH_DB_HEAVY_LOCK_FILE* ]]; then
  echo "finite trial host cutover rejects cron lock-path overrides" >&2
  exit 1
fi
for entrypoint in run_stats_reconciliation.sh run_leaderboard_shadow_worker.sh; do
  matches="$(printf '%s\n' "$active_schedule" | grep -F "/scripts/vps/$entrypoint" || true)"
  if [[ "$(printf '%s\n' "$matches" | sed '/^$/d' | wc -l | tr -d ' ')" != 1 \
     || "$matches" != *"cd $ROOT && $ROOT/scripts/vps/$entrypoint"* ]]; then
    echo "finite trial host cutover requires one exact $entrypoint cron entry in this release" >&2
    exit 1
  fi
done

# Check the actual values each entrant will see after its own env source.
# A textual assignment scan misses exported values and shell expressions.
(
  if [[ -f "$ROOT/.env" ]]; then
    set -a
    source "$ROOT/.env"
    set +a
  fi
  require_default_trial_lock_paths_or_exit
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
      if [[ "$process" != *"$ROOT/scripts/vps/"* ]]; then
        echo "finite trial host cutover found old wrapper pid=$pid ppid=$ppid started=$weekday-$month-$day-$clock-$year command=$process" >&2
        return 1
      fi
    fi
    if [[ "$process" == *"reconcile_stats_provider_queue.py"* ]]; then
      if [[ "$process" != *"$ROOT/scripts/reconcile_stats_provider_queue.py"* ]]; then
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

echo "finite trial host cutover verified for both scheduled entrants and descendants"
