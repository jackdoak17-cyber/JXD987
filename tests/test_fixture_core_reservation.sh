#!/usr/bin/env bash
set -euo pipefail

source scripts/vps/common.sh
tmp="$(mktemp -d)"
trap 'rm -rf "${tmp}"' EXIT
export ODDS_SYNC_LOCK_FILE="${tmp}/odds-sync.lock"
export ODDS_SYNC_LIVE_SCHEDULE_ENABLED=false
export ODDS_SYNC_P3_MAX_DURATION_SECONDS=5
export FIXTURE_CORE_RESERVATION_TTL_SECONDS=30
export MARKER="${tmp}/marker"

set +e
FIXTURE_CORE_RESERVATION_TTL_SECONDS=999999 fixture_core_reservation_claim >"${tmp}/invalid-ttl" 2>&1
invalid_ttl_status=$?
set -e
[[ "${invalid_ttl_status}" -eq 1 ]]
[[ ! -e "$(fixture_core_reservation_file)" ]]
owner="$(fixture_core_reservation_claim)"
export FIXTURE_CORE_RESERVATION_OWNER="${owner}"
[[ -n "${owner}" ]]
set +e
fixture_core_reservation_claim >"${tmp}/duplicate" 2>&1
duplicate_status=$?
ODDS_SYNC_JOB_PRIORITY=normal run_with_global_lock_and_timeout 'touch "${MARKER}"' >"${tmp}/normal" 2>&1
normal_status=$?
set -e
[[ "${duplicate_status}" -eq 2 ]]
[[ "${normal_status}" -eq 2 ]]
[[ ! -e "${MARKER}" ]]
if declare -F run_with_database_heavy_lock >/dev/null; then
  export ODDSSEARCH_DB_HEAVY_LOCK_FILE="${tmp}/db-heavy.lock"
  set +e
  ODDS_SYNC_JOB_PRIORITY=normal run_with_database_heavy_lock 1 \
    run_with_global_lock_and_timeout 'touch "${MARKER}"' >"${tmp}/db-heavy" 2>&1
  db_heavy_status=$?
  set -e
  [[ "${db_heavy_status}" -eq 2 ]]
  [[ ! -e "${MARKER}" ]]
fi
ODDS_SYNC_LOCK_FILE="${tmp}/other-writer.lock" ODDS_SYNC_JOB_PRIORITY=normal \
  run_with_global_lock_and_timeout 'touch "${MARKER}"'
[[ -e "${MARKER}" ]]
rm "${MARKER}"
ODDS_SYNC_JOB_PRIORITY=settlement run_with_global_lock_and_timeout 'touch "${MARKER}"'
[[ -e "${MARKER}" ]]
rm "${MARKER}"
fixture_core_reservation_release "00000000-0000-4000-8000-000000000000"
[[ -f "$(fixture_core_reservation_file)" ]]
fixture_core_reservation_release "${owner}"
ODDS_SYNC_JOB_PRIORITY=normal run_with_global_lock_and_timeout 'touch "${MARKER}"'
[[ -e "${MARKER}" ]]
rm "${MARKER}"

# An incumbent normal writer is allowed to drain, but newcomers must yield.
export INCUMBENT_MARKER="${tmp}/incumbent"
ODDS_SYNC_JOB_PRIORITY=normal run_with_global_lock_and_timeout 'touch "${INCUMBENT_MARKER}"; sleep 1' >"${tmp}/incumbent.log" 2>&1 &
incumbent_pid=$!
for _ in $(seq 1 100); do
  [[ -e "${INCUMBENT_MARKER}" ]] && break
  sleep 0.02
done
[[ -e "${INCUMBENT_MARKER}" ]]
owner="$(fixture_core_reservation_claim)"
export FIXTURE_CORE_RESERVATION_OWNER="${owner}"
set +e
ODDS_SYNC_JOB_PRIORITY=normal run_with_global_lock_and_timeout 'touch "${MARKER}"' >"${tmp}/late-normal" 2>&1
late_status=$?
set -e
[[ "${late_status}" -eq 2 ]]
[[ ! -e "${MARKER}" ]]
export ODDS_SYNC_LOCK_RETRY_ATTEMPTS=30
export ODDS_SYNC_LOCK_RETRY_DELAY_SECONDS=0.05
ODDS_SYNC_JOB_PRIORITY=fixture_core run_with_global_lock_and_retry 'touch "${MARKER}"'
wait "${incumbent_pid}"
[[ -e "${MARKER}" ]]
rm "${MARKER}"
fixture_core_reservation_release "${owner}"
ODDS_SYNC_JOB_PRIORITY=normal run_with_global_lock_and_timeout 'touch "${MARKER}"'
[[ -e "${MARKER}" ]]
rm "${MARKER}"

# A crash leaves only bounded priority. An old owner cannot clear a newer one.
export FIXTURE_CORE_RESERVATION_TTL_SECONDS=2
old_owner="$(fixture_core_reservation_claim)"
sleep 3
export FIXTURE_CORE_RESERVATION_TTL_SECONDS=30
new_owner="$(fixture_core_reservation_claim)"
fixture_core_reservation_release "${old_owner}"
[[ -f "$(fixture_core_reservation_file)" ]]
set +e
FIXTURE_CORE_RESERVATION_OWNER="${old_owner}" ODDS_SYNC_JOB_PRIORITY=fixture_core \
  run_with_global_lock_and_timeout 'touch "${MARKER}"' >"${tmp}/old-core" 2>&1
old_core_status=$?
set -e
[[ "${old_core_status}" -eq 2 ]]
[[ "$(cat "$(fixture_core_reservation_file)")" == "${new_owner} "* ]]
[[ ! -e "${MARKER}" ]]
set +e
ODDS_SYNC_JOB_PRIORITY=normal run_with_global_lock_and_timeout 'touch "${MARKER}"' >"${tmp}/new-owner" 2>&1
new_owner_status=$?
set -e
[[ "${new_owner_status}" -eq 2 ]]
FIXTURE_CORE_RESERVATION_OWNER="${new_owner}" ODDS_SYNC_JOB_PRIORITY=fixture_core \
  run_with_global_lock_and_timeout 'touch "${MARKER}"'
[[ -e "${MARKER}" ]]
[[ ! -f "$(fixture_core_reservation_file)" ]]
rm "${MARKER}"
ODDS_SYNC_JOB_PRIORITY=normal run_with_global_lock_and_timeout 'touch "${MARKER}"'
[[ -e "${MARKER}" ]]

# The existing live-settlement guard still blocks core admission at its tick.
export ODDS_SYNC_LIVE_SCHEDULE_ENABLED=true
export ODDS_SYNC_LIVE_TICK_SECONDS=900
export ODDS_SYNC_LIVE_RESERVE_SECONDS=900
owner="$(fixture_core_reservation_claim)"
export FIXTURE_CORE_RESERVATION_OWNER="${owner}"
set +e
ODDS_SYNC_JOB_PRIORITY=fixture_core run_with_global_lock_and_timeout 'true' >"${tmp}/core-live" 2>&1
core_live_status=$?
set -e
[[ "${core_live_status}" -eq 2 ]]
ODDS_SYNC_JOB_PRIORITY=settlement run_with_global_lock_and_timeout 'true'
fixture_core_reservation_release "${owner}"
export ODDS_SYNC_LOCK_RETRY_ATTEMPTS=100
export ODDS_SYNC_LOCK_RETRY_DELAY_SECONDS=0.05
export ODDS_SYNC_LOCK_RETRY_DEADLINE_SECONDS=1
retry_started="$(date -u +%s)"
set +e
ODDS_SYNC_JOB_PRIORITY=fixture_core run_with_global_lock_and_retry 'true' >"${tmp}/deadline" 2>&1
deadline_status=$?
set -e
retry_elapsed=$(( $(date -u +%s) - retry_started ))
[[ "${deadline_status}" -eq 2 ]]
[[ "${retry_elapsed}" -le 3 ]]
grep -q 'retry deadline reached' "${tmp}/deadline"
set +e
ODDS_SYNC_LOCK_RETRY_DEADLINE_SECONDS=invalid run_with_global_lock_and_retry 'true' >"${tmp}/invalid-deadline" 2>&1
invalid_deadline_status=$?
set -e
[[ "${invalid_deadline_status}" -eq 1 ]]

# The admitted source releases priority before its database heartbeat. A slow
# heartbeat must not keep ordinary writer work paused after the source exits.
export ODDS_SYNC_LIVE_SCHEDULE_ENABLED=false
export ODDS_SYNC_LIVE_RESERVE_SECONDS=0
export ODDS_SYNC_LIVE_GRACE_SECONDS=0
export FIXTURE_CORE_RESERVATION_TTL_SECONDS=30
export ODDS_SYNC_LOCK_RETRY_ATTEMPTS=0
export FIXTURE_CORE_RESERVATION_OWNER="$(fixture_core_reservation_claim)"
export NORMAL_HEARTBEAT_MARKER="${tmp}/normal-during-heartbeat"
rm -f "${MARKER}"
record_pipeline_job_run() { touch "${tmp}/heartbeat-start"; sleep 2; }
ODDS_SYNC_JOB_PRIORITY=fixture_core run_recorded_pipeline_job \
  test_fixture_core 'test fixture core' 'touch "${MARKER}"' '' >"${tmp}/heartbeat.log" 2>&1 &
core_pid=$!
for _ in $(seq 1 100); do
  [[ -e "${tmp}/heartbeat-start" ]] && break
  sleep 0.02
done
[[ -e "${tmp}/heartbeat-start" ]]
[[ ! -f "$(fixture_core_reservation_file)" ]]
ODDS_SYNC_JOB_PRIORITY=normal run_with_global_lock_and_timeout 'touch "${NORMAL_HEARTBEAT_MARKER}"'
[[ -e "${NORMAL_HEARTBEAT_MARKER}" ]]
wait "${core_pid}"

# A core job that never reaches provider work must likewise release priority
# before its outcome heartbeat can stall.
export ODDS_SYNC_LOCK_RETRY_STARTED_EPOCH="$(( $(date -u +%s) - 2 ))"
export ODDS_SYNC_LOCK_RETRY_DEADLINE_SECONDS=1
export FIXTURE_CORE_RESERVATION_OWNER="$(fixture_core_reservation_claim)"
rm -f "${tmp}/heartbeat-start" "${MARKER}"
ODDS_SYNC_JOB_PRIORITY=fixture_core run_recorded_pipeline_job \
  test_fixture_core 'test fixture core' 'touch "${MARKER}"' '' >"${tmp}/deferred-heartbeat.log" 2>&1 &
core_pid=$!
for _ in $(seq 1 100); do
  [[ -e "${tmp}/heartbeat-start" ]] && break
  sleep 0.02
done
[[ -e "${tmp}/heartbeat-start" ]]
[[ ! -f "$(fixture_core_reservation_file)" ]]
[[ ! -e "${MARKER}" ]]
export ODDS_SYNC_LOCK_RETRY_STARTED_EPOCH=0
export ODDS_SYNC_LOCK_RETRY_DEADLINE_SECONDS=0
ODDS_SYNC_JOB_PRIORITY=normal run_with_global_lock_and_timeout 'touch "${NORMAL_HEARTBEAT_MARKER}"'
wait "${core_pid}" || [[ "$?" -eq 2 ]]

# A guard delay counts toward admission's deadline. It cannot permit work to
# start after the deadline just because the first check happened earlier.
export ODDS_SYNC_LOCK_RETRY_DEADLINE_SECONDS=1
export ODDS_SYNC_LOCK_RETRY_STARTED_EPOCH="$(date -u +%s)"
export FIXTURE_CORE_RESERVATION_OWNER="$(fixture_core_reservation_claim)"
rm -f "${MARKER}"
(
  exec 7>"$(fixture_core_reservation_file).guard"
  flock -x 7
  touch "${tmp}/guard-held"
  sleep 1.5
) &
guard_pid=$!
for _ in $(seq 1 100); do
  [[ -e "${tmp}/guard-held" ]] && break
  sleep 0.02
done
[[ -e "${tmp}/guard-held" ]]
set +e
ODDS_SYNC_JOB_PRIORITY=fixture_core run_with_global_lock_and_timeout 'touch "${MARKER}"' >"${tmp}/late-admission" 2>&1
late_admission_status=$?
set -e
wait "${guard_pid}"
[[ "${late_admission_status}" -eq 2 ]]
[[ ! -e "${MARKER}" ]]
grep -q 'retry deadline reached before admission' "${tmp}/late-admission"
fixture_core_reservation_release "${FIXTURE_CORE_RESERVATION_OWNER}"
export ODDS_SYNC_LOCK_RETRY_DEADLINE_SECONDS=0
export ODDS_SYNC_LOCK_RETRY_STARTED_EPOCH=0

# A settlement window can begin while the normal writer waits for its guard.
# The final admission check must catch the changed clock.
mkdir -p "${tmp}/clock-bin"
cat >"${tmp}/clock-bin/date" <<'DATE'
#!/usr/bin/env bash
if [[ "$#" -eq 2 && "$1" == "-u" && "$2" == "+%s" ]]; then
  n=0
  [[ ! -f "${FAKE_DATE_COUNT}" ]] || n="$(cat "${FAKE_DATE_COUNT}")"
  n=$((n + 1))
  printf '%s\n' "${n}" >"${FAKE_DATE_COUNT}"
  if (( n <= 2 )); then printf '700\n'; else printf '850\n'; fi
else
  exec /bin/date "$@"
fi
DATE
chmod +x "${tmp}/clock-bin/date"
export FAKE_DATE_COUNT="${tmp}/date-count"
export ODDS_SYNC_LOCK_FILE="${tmp}/settlement-clock.lock"
export ODDS_SYNC_LIVE_SCHEDULE_ENABLED=true
export ODDS_SYNC_LIVE_TICK_SECONDS=900
export ODDS_SYNC_LIVE_RESERVE_SECONDS=100
export ODDS_SYNC_LIVE_GRACE_SECONDS=0
rm -f "${MARKER}"
set +e
PATH="${tmp}/clock-bin:${PATH}" ODDS_SYNC_JOB_PRIORITY=normal \
  run_with_global_lock_and_timeout 'touch "${MARKER}"' >"${tmp}/late-settlement" 2>&1
late_settlement_status=$?
set -e
[[ "${late_settlement_status}" -eq 2 ]]
[[ ! -e "${MARKER}" ]]
grep -q 'live settlement reservation active before admission' "${tmp}/late-settlement"

# Claim and cleanup use the same bounded guard; neither can stall forever
# behind a paused contender.
export ODDS_SYNC_LOCK_FILE="${tmp}/bounded-guard.lock"
export ODDS_SYNC_LIVE_SCHEDULE_ENABLED=false
export ODDS_SYNC_LIVE_RESERVE_SECONDS=0
export ODDS_SYNC_LIVE_GRACE_SECONDS=0
guard_file="$(fixture_core_reservation_file).guard"
(
  exec 7>"${guard_file}"
  flock -x 7
  touch "${tmp}/claim-guard-held"
  sleep 3
) &
guard_pid=$!
for _ in $(seq 1 100); do
  [[ -e "${tmp}/claim-guard-held" ]] && break
  sleep 0.02
done
[[ -e "${tmp}/claim-guard-held" ]]
claim_started="$(date -u +%s)"
set +e
fixture_core_reservation_claim >"${tmp}/claim-timeout" 2>&1
claim_status=$?
set -e
claim_elapsed=$(( $(date -u +%s) - claim_started ))
[[ "${claim_status}" -eq 2 && "${claim_elapsed}" -le 3 ]]
wait "${guard_pid}"
owner="$(fixture_core_reservation_claim)"
(
  exec 7>"${guard_file}"
  flock -x 7
  touch "${tmp}/release-guard-held"
  sleep 3
) &
guard_pid=$!
for _ in $(seq 1 100); do
  [[ -e "${tmp}/release-guard-held" ]] && break
  sleep 0.02
done
[[ -e "${tmp}/release-guard-held" ]]
release_started="$(date -u +%s)"
set +e
fixture_core_reservation_release "${owner}" >"${tmp}/release-timeout" 2>&1
release_status=$?
set -e
release_elapsed=$(( $(date -u +%s) - release_started ))
[[ "${release_status}" -eq 1 && "${release_elapsed}" -le 3 ]]
wait "${guard_pid}"
fixture_core_reservation_release "${owner}"

# The owner check can advance the clock after the first settlement lease is
# computed. The actual timeout must use the shorter lease at final admission.
export ODDS_SYNC_LOCK_FILE="${tmp}/recomputed-lease.lock"
export ODDS_SYNC_LIVE_SCHEDULE_ENABLED=true
export ODDS_SYNC_LIVE_TICK_SECONDS=900
export ODDS_SYNC_LIVE_RESERVE_SECONDS=0
export ODDS_SYNC_LIVE_GRACE_SECONDS=0
export ODDS_SYNC_P3_MAX_DURATION_SECONDS=300
export FIXTURE_CORE_RESERVATION_TTL_SECONDS=3600
export FIXTURE_CORE_RESERVATION_OWNER="$(fixture_core_reservation_claim)"
cat >"${tmp}/clock-bin/date" <<'DATE'
#!/usr/bin/env bash
if [[ "$#" -eq 2 && "$1" == "-u" && "$2" == "+%s" ]]; then
  n=0
  [[ ! -f "${FAKE_DATE_COUNT}" ]] || n="$(cat "${FAKE_DATE_COUNT}")"
  n=$((n + 1))
  printf '%s\n' "${n}" >"${FAKE_DATE_COUNT}"
  if (( n <= 3 )); then printf '700\n'; else printf '750\n'; fi
else
  exec /bin/date "$@"
fi
DATE
cat >"${tmp}/clock-bin/timeout" <<'TIMEOUT'
#!/usr/bin/env bash
printf '%s\n' "$3" >"${FAKE_TIMEOUT_RUNTIME}"
shift 3
"$@"
TIMEOUT
chmod +x "${tmp}/clock-bin/date" "${tmp}/clock-bin/timeout"
export FAKE_DATE_COUNT="${tmp}/lease-date-count"
export FAKE_TIMEOUT_RUNTIME="${tmp}/actual-lease"
rm -f "${MARKER}"
PATH="${tmp}/clock-bin:${PATH}" ODDS_SYNC_JOB_PRIORITY=fixture_core \
  run_with_global_lock_and_timeout 'touch "${MARKER}"'
[[ -e "${MARKER}" ]]
[[ "$(cat "${FAKE_TIMEOUT_RUNTIME}")" == "150" ]]
[[ ! -f "$(fixture_core_reservation_file)" ]]
echo "fixture-core reservation contention checks passed"
