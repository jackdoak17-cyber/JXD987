#!/usr/bin/env bash
set -euo pipefail
umask 077

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "$ROOT/scripts/vps/common.sh"
verify_runtime_manifest_or_exit "$0"
require_default_trial_lock_paths_or_exit
require_no_cron_transition
require_runtime_manifest_entries_or_exit "$0" \
  "scripts/leaderboard_resource_meter.py" \
  "scripts/leaderboard_dedicated_lane_check.py" \
  "scripts/leaderboard_shadow_worker.py"

ENV_FILE="${LEADERBOARD_SHADOW_ENV_FILE:-/etc/oddssearch/leaderboard-shadow.env}"
PLATFORM_ENV_FILE="${LEADERBOARD_DEDICATED_PLATFORM_ENV_FILE:-/opt/odds-sync/JXD987/.env}"
STATE_DIRECTORY="${LEADERBOARD_DEDICATED_LANE_STATE_DIR:-/var/lib/oddssearch/leaderboard-dedicated-lane}"
RECEIPT_DIRECTORY="${LEADERBOARD_DEDICATED_LANE_RECEIPT_DIR:-/var/lib/oddssearch/leaderboard-dedicated-lane/receipts}"
METRICS_STATE="$STATE_DIRECTORY/metrics.json"
LAST_RUN_STATE="$STATE_DIRECTORY/last-run.json"
LANE_QUARANTINE="$STATE_DIRECTORY/quarantine.json"
PUBLISHER_TICK_SECONDS=900
PUBLISHER_WINDOW_START_SECONDS=120
# The source reconciliation can use its final bounded batch through minute 11
# of each quarter-hour. Admit the leaderboard after that handoff; the 195s
# reservation still requires release before the next live publisher tick.
PUBLISHER_WINDOW_END_SECONDS=900
PUBLISHER_WORK_RESERVE_SECONDS=195

[[ $(id -u) == 0 ]] || { echo "dedicated lane requires root" >&2; exit 1; }
[[ -r "$ENV_FILE" && -r "$PLATFORM_ENV_FILE" ]] || {
  echo "dedicated lane environment is unavailable" >&2; exit 1;
}
mkdir -p -m 0700 "$STATE_DIRECTORY" "$RECEIPT_DIRECTORY"
[[ ! -L "$STATE_DIRECTORY" && ! -L "$RECEIPT_DIRECTORY" ]] || exit 1

# Load the platform key first, then restore the worker's reviewed database and
# release settings from its narrower environment.
set -a
# shellcheck disable=SC1090
source "$PLATFORM_ENV_FILE"
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a
require_default_trial_lock_paths_or_exit
: "${SUPABASE_URL:?Supabase metrics URL is required}"
: "${SUPABASE_SERVICE_ROLE_KEY:?Supabase metrics key is required}"
: "${LEADERBOARD_SHADOW_RELEASE_SHA:?reviewed worker release is required}"
[[ "$LEADERBOARD_SHADOW_RELEASE_SHA" =~ ^[0-9a-f]{40}$ ]] || exit 1
export LEADERBOARD_DEDICATED_LANE_RELEASE_SHA="$LEADERBOARD_SHADOW_RELEASE_SHA"

scope_args=()
if [[ -n ${LEADERBOARD_DEDICATED_LANE_COHORT:-} ]]; then
  if [[ ! $LEADERBOARD_DEDICATED_LANE_COHORT =~ ^([1-9][0-9]*)/([1-9][0-9]*)$ ]]; then
    echo "dedicated lane cohort scope must be league/season" >&2
    exit 1
  fi
  scope_args=(
    --league-id "${BASH_REMATCH[1]}"
    --season-id "${BASH_REMATCH[2]}"
    --cohort-limit 1
  )
fi

exec 9<"$TRIAL_WORKER_LOCK"
check_trial_lock_fd worker "$TRIAL_WORKER_LOCK" 9
flock -n 9 || exit 0
if has_quarantine_marker "$TRIAL_WORKER_LOCK"; then
  echo "dedicated lane blocked by quarantine" >&2
  exit 1
fi
if [[ -e "$LANE_QUARANTINE" || -L "$LANE_QUARANTINE" ]]; then
  echo "dedicated lane retained its quarantine after an incomplete run" >&2
  exit 1
fi

if [[ -e "$LAST_RUN_STATE" ]]; then
  python3 - "$LAST_RUN_STATE" <<'PY' || exit $?
import datetime, json, os, stat, sys
fd=os.open(sys.argv[1],os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
with os.fdopen(fd,encoding='utf-8') as source:
    info=os.fstat(source.fileno())
    if not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or info.st_mode & 0o077:
        raise SystemExit(1)
    value=json.load(source)
finished=datetime.datetime.fromisoformat(value['finished_at'].replace('Z','+00:00'))
age=(datetime.datetime.now(datetime.timezone.utc)-finished).total_seconds()
raise SystemExit(0 if age >= 90 else 75)
PY
fi

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
  flock -n 7 || { exec 7>&-; return 75; }
  if fixture_core_reservation_active "$reservation_file"; then
    flock -u 7; exec 7>&-; return 75
  fi
  canonical_lock="${ODDS_SYNC_LOCK_FILE:-/var/lock/odds-sync.lock}"
  [[ -f "$canonical_lock" && ! -L "$canonical_lock" ]] || return 1
  exec 6<"$canonical_lock"
  [[ "$canonical_lock" -ef /proc/self/fd/6 ]] || return 1
  flock -n 6 || { exec 6>&-; flock -u 7; exec 7>&-; return 75; }
  flock -u 6; exec 6>&-
  flock -u 7; exec 7>&-
}

receipt_id="$(date -u +%Y%m%dT%H%M%SZ)-$(openssl rand -hex 8)"
receipt="$RECEIPT_DIRECTORY/$receipt_id.json"
work="$(mktemp -d "$STATE_DIRECTORY/.run.XXXXXX")"
cleanup() { rm -r -- "$work" 2>/dev/null || true; }
trap cleanup EXIT

publisher_window_available
meter_status=0
"$ROOT/.venv/bin/python3" "$ROOT/scripts/leaderboard_resource_meter.py" \
  --state "$METRICS_STATE" --mode admission >"$work/metrics-before.json" || meter_status=$?
if (( meter_status == 75 )); then
  echo "[SKIPPED] dedicated lane has no green fresh metrics interval" >&2
  exit 75
elif (( meter_status != 0 )); then
  exit "$meter_status"
fi
"$ROOT/.venv/bin/python3" "$ROOT/scripts/leaderboard_dedicated_lane_check.py" before \
  >"$work/database-before.json"

run_one() {
  publisher_window_available || return $?
  python3 - "$LANE_QUARANTINE" "$receipt_id" <<'PY'
import json,os,sys
fd=os.open(sys.argv[1],os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
with os.fdopen(fd,'w') as target:
    json.dump({'receipt_id':sys.argv[2],'state':'worker_started'},target,sort_keys=True)
    target.write('\n');target.flush();os.fsync(target.fileno())
PY
  export LEADERBOARD_DEDICATED_LANE_TOKEN
  LEADERBOARD_DEDICATED_LANE_TOKEN="$(openssl rand -hex 16)"
  "$ROOT/.venv/bin/python3" "$ROOT/scripts/leaderboard_shadow_worker.py" run-once \
    --allow-source-p95-yellow --fixture-limit 250 --max-seconds 180 \
    --capture-limit 250 --cohort-limit 8 --job-limit 250 \
    "${scope_args[@]}" >"$work/worker.json"
}

set +e
run_with_database_heavy_lock 0 run_one
worker_status=$?
set -e
if (( worker_status == 75 )); then
  echo "[SKIPPED] source or database-heavy lane has priority" >&2
  exit 75
elif (( worker_status != 0 )); then
  exit "$worker_status"
fi

run_id="$(python3 - "$work/worker.json" <<'PY'
import json,sys
value=json.load(open(sys.argv[1],encoding='utf-8'))
run_id=value.get('run_id')
if type(run_id) is not int or run_id <= 0: raise SystemExit(1)
print(run_id)
PY
)"
"$ROOT/.venv/bin/python3" "$ROOT/scripts/leaderboard_dedicated_lane_check.py" after \
  --run-id "$run_id" >"$work/database-after.json"

# The shared heavy lock is already released. This wait obtains a genuine
# post-run host interval without delaying the source importer or publisher.
sleep 60
"$ROOT/.venv/bin/python3" "$ROOT/scripts/leaderboard_resource_meter.py" \
  --state "$METRICS_STATE" --mode post >"$work/metrics-after.json"

python3 - "$receipt_id" "$run_id" "$work" "$receipt" "$LAST_RUN_STATE" <<'PY'
import datetime,json,os,pathlib,sys,tempfile
receipt_id,run_id,work,receipt,last=sys.argv[1:]
work=pathlib.Path(work)
value={
  'ok':True,'receipt_id':receipt_id,'run_id':int(run_id),
  'finished_at':datetime.datetime.now(datetime.timezone.utc).isoformat().replace('+00:00','Z'),
  'metrics_before':json.loads((work/'metrics-before.json').read_text()),
  'database_before':json.loads((work/'database-before.json').read_text()),
  'worker':json.loads((work/'worker.json').read_text()),
  'database_after':json.loads((work/'database-after.json').read_text()),
  'metrics_after':json.loads((work/'metrics-after.json').read_text()),
}
for target,document in ((pathlib.Path(receipt),value),(pathlib.Path(last),{
    'receipt_id':receipt_id,'run_id':int(run_id),'finished_at':value['finished_at']})):
  fd,temp=tempfile.mkstemp(prefix='.'+target.name+'.',dir=target.parent)
  os.fchmod(fd,0o600)
  with os.fdopen(fd,'w') as stream:
    json.dump(document,stream,sort_keys=True);stream.write('\n');stream.flush();os.fsync(stream.fileno())
  os.replace(temp,target)
  directory=os.open(target.parent,os.O_RDONLY|os.O_DIRECTORY)
  try: os.fsync(directory)
  finally: os.close(directory)
PY

# Removal is the final acknowledgement. If it fails, the lane stays fenced
# even though the database run itself completed successfully.
rm -- "$LANE_QUARANTINE"
sync "$STATE_DIRECTORY"

echo "dedicated leaderboard lane PASS receipt=$receipt run_id=$run_id"
