#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/scripts/vps/common.sh"
verify_runtime_manifest_or_exit "$0"
require_default_trial_lock_paths_or_exit
require_no_cron_transition
require_runtime_manifest_entries_or_exit "$0" \
  "scripts/leaderboard_shadow_capture.py" \
  "scripts/leaderboard_shadow_worker.py" \
  "scripts/export_to_supabase.py" \
  "scripts/postmatch_fixture_detail_delivery.py"
LOCK_FILE="$TRIAL_WORKER_LOCK"
ENV_FILE="${LEADERBOARD_SHADOW_ENV_FILE:-/etc/oddssearch/leaderboard-shadow.env}"
DB_HEAVY_LOCK_WAIT_SECONDS="${LEADERBOARD_SHADOW_DB_HEAVY_LOCK_WAIT_SECONDS:-0}"

exec 9<"$LOCK_FILE"
check_trial_lock_fd worker "$LOCK_FILE" 9
flock -n 9 || exit 0
if has_quarantine_marker "$LOCK_FILE"; then
  echo "leaderboard worker blocked by unresolved finite-trial quarantine" >&2
  exit 1
fi

if [[ ! -r "$ENV_FILE" ]]; then
  echo "leaderboard shadow environment file is unavailable" >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a
require_default_trial_lock_paths_or_exit

export LEADERBOARD_SHADOW_RELEASE_SHA="${LEADERBOARD_SHADOW_RELEASE_SHA:-$(git -C "$ROOT" rev-parse HEAD)}"

# The provider may repeat an old disk observation in a newly written file.
# Check its own timestamp after the worker and database-heavy locks are held.
# Python still refreshes the evidence in SQL and runs every full resource guard.
run_shadow_if_fresh() {
  local evidence_path="${LEADERBOARD_SHADOW_RESOURCE_EVIDENCE_FILE:-/var/lib/oddssearch/leaderboard-shadow-resource.json}"
  local evidence_status=0
  python3 - "$evidence_path" <<'PY' || evidence_status=$?
import datetime
import json
import os
import stat
import sys

try:
    fd = os.open(sys.argv[1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, encoding="utf-8") as source:
        info = os.fstat(source.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o022:
            raise ValueError("untrusted evidence file")
        evidence = json.load(source)
    if not isinstance(evidence, dict) or set(evidence) != {"disk_available_bytes", "observed_at"}:
        raise ValueError("invalid evidence fields")
    if type(evidence["disk_available_bytes"]) is not int or evidence["disk_available_bytes"] <= 0:
        raise ValueError("invalid disk measurement")
    if not isinstance(evidence["observed_at"], str):
        raise ValueError("invalid observation time")
    observed = datetime.datetime.fromisoformat(evidence["observed_at"].replace("Z", "+00:00"))
    if observed.tzinfo is None:
        raise ValueError("observation time has no timezone")
    age = (datetime.datetime.now(datetime.timezone.utc) - observed).total_seconds()
    if age < 0 or age > 210:
        raise ValueError("disk observation is outside admission margin")
except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
    raise SystemExit(75)
PY
  if (( evidence_status == 75 )); then
    echo "[SKIPPED] fresh provider disk observation unavailable" >&2
    return 75
  elif (( evidence_status != 0 )); then
    return "$evidence_status"
  fi
  python3 "$ROOT/scripts/leaderboard_shadow_worker.py" run-once \
    --fixture-limit 250 --max-seconds 30 \
    --capture-limit 250 --job-limit 250
}
set +e
run_with_database_heavy_lock "${DB_HEAVY_LOCK_WAIT_SECONDS}" run_shadow_if_fresh
status=$?
set -e

# A source reconciliation batch has priority. Deferral is healthy and the next
# scheduled worker run resumes from the durable queue/generation state.
if [[ "${status}" -eq 75 ]]; then
  exit 0
fi
exit "${status}"
