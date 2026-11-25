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
PUBLISHER_TICK_SECONDS=900
PUBLISHER_WINDOW_START_SECONDS=120
PUBLISHER_WINDOW_END_SECONDS=720
PUBLISHER_WORK_RESERVE_SECONDS=45

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

# Keep scheduled work inside the same publisher-safe quarter-hour window used
# by the activation controllers.  Recheck after taking the database-heavy slot
# so lock contention cannot admit a run after the window has closed.  The
# canonical lock is only probed: an already-admitted source writer wins, while
# the fixture-core reservation guard prevents racing its protected settlement.
publisher_window_available() {
  local now phase reservation_file canonical_lock
  now=$(date -u +%s) || return 1
  phase=$((now % PUBLISHER_TICK_SECONDS))
  if (( phase < PUBLISHER_WINDOW_START_SECONDS ||
        phase + PUBLISHER_WORK_RESERVE_SECONDS >= PUBLISHER_WINDOW_END_SECONDS )); then
    echo "[SKIPPED] publisher-safe window unavailable (phase=${phase}s)" >&2
    return 75
  fi

  reservation_file="$(fixture_core_reservation_file)"
  exec 7>"${reservation_file}.guard"
  if ! flock -n 7; then
    exec 7>&-
    echo "[SKIPPED] fixture-core reservation guard unavailable" >&2
    return 75
  fi
  if fixture_core_reservation_active "$reservation_file"; then
    flock -u 7
    exec 7>&-
    echo "[SKIPPED] fixture-core reservation active" >&2
    return 75
  fi

  canonical_lock="${ODDS_SYNC_LOCK_FILE:-/var/lock/odds-sync.lock}"
  if [[ ! -f "$canonical_lock" || -L "$canonical_lock" ]]; then
    flock -u 7
    exec 7>&-
    echo "canonical writer lock is unavailable or untrusted" >&2
    return 1
  fi
  exec 6<"$canonical_lock"
  if ! python3 - "$canonical_lock" 6 <<'PY'
import os
import sys

path_info = os.stat(sys.argv[1], follow_symlinks=False)
fd_info = os.fstat(int(sys.argv[2]))
if (path_info.st_dev, path_info.st_ino) != (fd_info.st_dev, fd_info.st_ino):
    raise SystemExit(1)
PY
  then
    exec 6>&-
    flock -u 7
    exec 7>&-
    echo "canonical writer lock identity changed" >&2
    return 1
  fi
  if ! flock -n 6; then
    exec 6>&-
    flock -u 7
    exec 7>&-
    echo "[SKIPPED] canonical writer slot unavailable" >&2
    return 75
  fi
  flock -u 6
  exec 6>&-
  flock -u 7
  exec 7>&-
}

# The provider may repeat an old disk observation in a newly written file.
# Check its own timestamp after the worker and database-heavy locks are held.
# Python still refreshes the evidence in SQL and runs every full resource guard.
run_shadow_if_fresh() {
  local evidence_path="${LEADERBOARD_SHADOW_RESOURCE_EVIDENCE_FILE:-/var/lib/oddssearch/leaderboard-shadow-resource.json}"
  local evidence_status=0
  publisher_window_available || return $?
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
  publisher_window_available || return $?
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
