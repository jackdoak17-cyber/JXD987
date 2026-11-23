#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
source "$ROOT/scripts/vps/common.sh"
verify_runtime_manifest

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
export ODDS_SYNC_LOCK_FILE="$tmp/writer.lock"
export ODDS_SYNC_LIVE_SCHEDULE_ENABLED=false
export ODDS_SYNC_P3_MAX_DURATION_SECONDS=2
export ODDS_SYNC_JOB_PRIORITY=normal
reservation="$(fixture_core_reservation_file)"
owner="$(fixture_core_reservation_claim)"
[[ "$owner" =~ ^[0-9a-f-]{36}$ && -f "$reservation" ]]

set +e
run_with_global_lock_and_timeout "touch '$tmp/normal-ran'" >/dev/null
status=$?
set -e
[[ "$status" -eq 2 && ! -e "$tmp/normal-ran" ]] || {
  echo "normal writer overtook an active fixture-core reservation" >&2
  exit 1
}

ODDS_SYNC_JOB_PRIORITY=settlement run_with_global_lock_and_timeout "touch '$tmp/settlement-ran'" >/dev/null
[[ -e "$tmp/settlement-ran" && -f "$reservation" ]]
FIXTURE_CORE_RESERVATION_OWNER="$owner" ODDS_SYNC_JOB_PRIORITY=fixture_core \
  run_with_global_lock_and_timeout "touch '$tmp/fixture-core-ran'" >/dev/null
[[ -e "$tmp/fixture-core-ran" && ! -e "$reservation" ]]
run_with_global_lock_and_timeout "touch '$tmp/normal-ran'" >/dev/null
[[ -e "$tmp/normal-ran" ]]

export LEADERBOARD_SHADOW_LOCK_FILE="$tmp/leaderboard.lock"
export ODDSSEARCH_DB_HEAVY_LOCK_FILE="$tmp/heavy.lock"
run_with_database_heavy_lock 0 touch "$tmp/heavy-ran" >/dev/null
[[ -e "$tmp/heavy-ran" ]]
rm "$tmp/heavy-ran"
touch "$LEADERBOARD_SHADOW_LOCK_FILE.quarantine.heavy"
set +e
run_with_database_heavy_lock 0 touch "$tmp/heavy-ran" >/dev/null
ordinary_status=$?
run_with_database_heavy_lock_timed 0 "$tmp/timing" touch "$tmp/heavy-ran" >/dev/null
timed_status=$?
set -e
[[ "$ordinary_status" -eq 75 && "$timed_status" -eq 75 && ! -e "$tmp/heavy-ran" && ! -e "$tmp/timing" ]] || {
  echo "heavy quarantine did not defer both database-heavy entrants" >&2
  exit 1
}
rm "$LEADERBOARD_SHADOW_LOCK_FILE.quarantine.heavy"
run_with_database_heavy_lock_timed 0 "$tmp/timing" touch "$tmp/heavy-ran" >/dev/null
[[ -e "$tmp/heavy-ran" && -s "$tmp/timing" ]]
unset LEADERBOARD_SHADOW_LOCK_FILE ODDSSEARCH_DB_HEAVY_LOCK_FILE

mkdir "$tmp/bin"
cat >"$tmp/bin/crontab" <<'SH'
#!/usr/bin/env bash
cat "$TEST_CRONTAB_FILE"
SH
cat >"$tmp/bin/ps" <<'SH'
#!/usr/bin/env bash
printf '%s\n' '1 0 Sun Nov 8 00:00:00 2026 /sbin/init'
cat "$TEST_PROCESS_FILE"
SH
cat >"$tmp/bin/sleep" <<'SH'
#!/usr/bin/env bash
exit 0
SH
chmod +x "$tmp/bin/"*
export TEST_CRONTAB_FILE="$tmp/crontab"
export TEST_PROCESS_FILE="$tmp/processes"
export LEADERBOARD_SHADOW_ENV_FILE="$tmp/leaderboard.env"
export PATH="$tmp/bin:$PATH"
cat >"$TEST_CRONTAB_FILE" <<EOF
*/5 * * * * cd $ROOT && $ROOT/scripts/vps/run_stats_reconciliation.sh
1-4 * * * * cd $ROOT && $ROOT/scripts/vps/run_leaderboard_shadow_worker.sh
EOF
: >"$TEST_PROCESS_FILE"
: >"$LEADERBOARD_SHADOW_ENV_FILE"

verify="$ROOT/scripts/vps/verify_finite_trial_host_cutover.sh"
"$verify" >/dev/null

expect_rejected() {
  local label="$1"
  if "$verify" >"$tmp/verify.out" 2>&1; then
    echo "host verifier accepted $label" >&2
    exit 1
  fi
}

LEADERBOARD_SHADOW_LOCK_FILE="$tmp/inherited.lock" expect_rejected 'inherited lock override'
printf 'export ODDSSEARCH_DB_HEAVY_LOCK_FILE=%q\n' "$tmp/sourced.lock" >"$LEADERBOARD_SHADOW_ENV_FILE"
expect_rejected 'sourced lock override'
: >"$LEADERBOARD_SHADOW_ENV_FILE"
printf '%s\n' 'LEADERBOARD_SHADOW_LOCK_FILE=/tmp/other.lock' >>"$TEST_CRONTAB_FILE"
expect_rejected 'cron lock override'
sed -i '/LEADERBOARD_SHADOW_LOCK_FILE=/d' "$TEST_CRONTAB_FILE"
printf '%s\n' "123 1 Sun Nov 8 15:55:00 2026 python3 /opt/old/scripts/reconcile_stats_provider_queue.py" >"$TEST_PROCESS_FILE"
expect_rejected 'old orphan reconciliation child'
printf '%s\n' "124 1 Sun Nov 8 15:55:00 2026 bash /opt/old/scripts/vps/run_stats_reconciliation.sh" >"$TEST_PROCESS_FILE"
expect_rejected 'old statistics supervisor'
: >"$TEST_PROCESS_FILE"
"$verify" >/dev/null

TEST_PROCESS_FILE="$tmp/missing-process-list" expect_rejected 'unavailable process snapshot'

echo 'finite-trial host composition: reservation, quarantine and verifier PASS'
