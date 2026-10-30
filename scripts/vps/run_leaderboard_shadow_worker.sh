#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/scripts/vps/common.sh"
verify_runtime_manifest_or_exit "$0"
require_runtime_manifest_entries_or_exit "$0" \
  "scripts/leaderboard_shadow_capture.py" \
  "scripts/leaderboard_shadow_worker.py" \
  "scripts/export_to_supabase.py" \
  "scripts/postmatch_fixture_detail_delivery.py"
LOCK_FILE="${LEADERBOARD_SHADOW_LOCK_FILE:-/var/lock/oddssearch-leaderboard-shadow.lock}"
ENV_FILE="${LEADERBOARD_SHADOW_ENV_FILE:-/etc/oddssearch/leaderboard-shadow.env}"
DB_HEAVY_LOCK_WAIT_SECONDS="${LEADERBOARD_SHADOW_DB_HEAVY_LOCK_WAIT_SECONDS:-0}"

exec 9>"$LOCK_FILE"
flock -n 9 || exit 0

if [[ ! -r "$ENV_FILE" ]]; then
  echo "leaderboard shadow environment file is unavailable" >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

export LEADERBOARD_SHADOW_RELEASE_SHA="${LEADERBOARD_SHADOW_RELEASE_SHA:-$(git -C "$ROOT" rev-parse HEAD)}"
set +e
run_with_database_heavy_lock "${DB_HEAVY_LOCK_WAIT_SECONDS}" \
  python3 "$ROOT/scripts/leaderboard_shadow_worker.py" run-once \
  --fixture-limit 250 --max-seconds 30 \
  --capture-limit 250 --job-limit 250
status=$?
set -e

# A source reconciliation batch has priority. Deferral is healthy and the next
# scheduled worker run resumes from the durable queue/generation state.
if [[ "${status}" -eq 75 ]]; then
  exit 0
fi
exit "${status}"
