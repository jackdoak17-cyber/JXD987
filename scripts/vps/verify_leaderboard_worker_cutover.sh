#!/usr/bin/env bash
# Worker-only cron transition. The source supervisor and its schedule stay live.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "$ROOT/scripts/vps/common.sh"
verify_runtime_manifest_or_exit "$0"
require_runtime_manifest_entries_or_exit "$0" \
  "scripts/vps/verify_leaderboard_worker_cutover.sh" \
  "scripts/vps/install_leaderboard_worker_crontab.py" \
  "scripts/vps/run_leaderboard_shadow_worker.sh" \
  "scripts/vps/run_stats_reconciliation.sh" \
  "scripts/vps/common.sh"
require_default_trial_lock_paths_or_exit
require_no_cron_transition
if [[ $# -ne 6 || ( "$1" != install && "$1" != recover ) ]]; then
  echo "usage: $0 install|recover frozen-sha256 target-sha256 private-crontab from-root to-root" >&2
  exit 2
fi
exec python3 -I "$ROOT/scripts/vps/install_leaderboard_worker_crontab.py" "$@"
