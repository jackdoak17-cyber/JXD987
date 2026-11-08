#!/usr/bin/env bash
set -euo pipefail

source scripts/vps/common.sh
tmp="$(mktemp -d)"
trap 'rm -rf "${tmp}"' EXIT
export ODDS_SYNC_LOCK_FILE="${tmp}/odds-sync.lock"
export ODDS_SYNC_LIVE_SCHEDULE_ENABLED=false
export ODDS_SYNC_P3_MAX_DURATION_SECONDS=5
export FIXTURE_CORE_RESERVATION_TTL_SECONDS=10
export MARKER="${tmp}/marker"

set +e
FIXTURE_CORE_RESERVATION_TTL_SECONDS=999999 fixture_core_reservation_claim >"${tmp}/invalid-ttl" 2>&1
invalid_ttl_status=$?
set -e
[[ "${invalid_ttl_status}" -eq 1 ]]
[[ ! -e "$(fixture_core_reservation_file)" ]]
owner="$(fixture_core_reservation_claim)"
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
export ODDSSEARCH_DB_HEAVY_LOCK_FILE="${tmp}/db-heavy.lock"
set +e
ODDS_SYNC_JOB_PRIORITY=normal run_with_database_heavy_lock 1 \
  run_with_global_lock_and_timeout 'touch "${MARKER}"' >"${tmp}/db-heavy" 2>&1
db_heavy_status=$?
set -e
[[ "${db_heavy_status}" -eq 2 ]]
[[ ! -e "${MARKER}" ]]
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
new_owner="$(fixture_core_reservation_claim)"
fixture_core_reservation_release "${old_owner}"
[[ -f "$(fixture_core_reservation_file)" ]]
set +e
ODDS_SYNC_JOB_PRIORITY=normal run_with_global_lock_and_timeout 'touch "${MARKER}"' >"${tmp}/new-owner" 2>&1
new_owner_status=$?
set -e
[[ "${new_owner_status}" -eq 2 ]]
fixture_core_reservation_release "${new_owner}"
ODDS_SYNC_JOB_PRIORITY=normal run_with_global_lock_and_timeout 'touch "${MARKER}"'
[[ -e "${MARKER}" ]]

# The existing live-settlement guard still blocks core admission at its tick.
export ODDS_SYNC_LIVE_SCHEDULE_ENABLED=true
export ODDS_SYNC_LIVE_TICK_SECONDS=900
export ODDS_SYNC_LIVE_RESERVE_SECONDS=900
owner="$(fixture_core_reservation_claim)"
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
echo "fixture-core reservation contention checks passed"
