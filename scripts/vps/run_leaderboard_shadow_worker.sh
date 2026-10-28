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
exec python3 "$ROOT/scripts/leaderboard_shadow_worker.py" run-once
