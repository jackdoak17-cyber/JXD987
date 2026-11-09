#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "$ROOT/scripts/vps/common.sh"
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
for entrypoint in run_stats_reconciliation.sh run_leaderboard_shadow_worker.sh; do
  matches="$(printf '%s\n' "$schedule" | grep -F "/scripts/vps/$entrypoint" || true)"
  if [[ "$(printf '%s\n' "$matches" | sed '/^$/d' | wc -l | tr -d ' ')" != 1 \
     || "$matches" != *"cd $ROOT && $ROOT/scripts/vps/$entrypoint"* ]]; then
    echo "finite trial host cutover requires one exact $entrypoint cron entry in this release" >&2
    exit 1
  fi
done

for file in /etc/oddssearch/leaderboard-shadow.env "$ROOT/.env"; do
  if [[ -f "$file" ]] && grep -Eq \
    '^[[:space:]]*(export[[:space:]]+)?(LEADERBOARD_SHADOW_LOCK_FILE|ODDSSEARCH_DB_HEAVY_LOCK_FILE)=' \
    "$file"; then
    echo "finite trial host cutover requires the shared default lock paths" >&2
    exit 1
  fi
done

while IFS= read -r process; do
  if [[ "$process" == *"/scripts/vps/run_stats_reconciliation.sh"* \
     || "$process" == *"/scripts/vps/run_leaderboard_shadow_worker.sh"* ]]; then
    if [[ "$process" != *"$ROOT/scripts/vps/"* ]]; then
      echo "finite trial host cutover found an old scheduled process" >&2
      exit 1
    fi
  fi
done < <(ps -eo args=)

echo "finite trial host cutover verified for both scheduled entrants"
