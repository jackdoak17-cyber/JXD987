#!/usr/bin/env bash
set -euo pipefail

source scripts/vps/common.sh
tmp="$(mktemp -d)"
trap 'rm -rf "${tmp}"' EXIT
mkdir -p "${tmp}/bin"
cat >"${tmp}/bin/date" <<'DATE'
#!/usr/bin/env bash
if [[ "$#" -eq 2 && "$1" == "-u" && "$2" == "+%s" ]]; then
  printf '800\n'
else
  exec /bin/date "$@"
fi
DATE
chmod +x "${tmp}/bin/date"
export PATH="${tmp}/bin:${PATH}"
export CHAIN_MARKER="${tmp}/provider-work"
export ODDS_SYNC_LOCK_FILE="${tmp}/odds-sync.lock"
export ODDS_SYNC_LIVE_SCHEDULE_ENABLED=true
export ODDS_SYNC_LIVE_TICK_SECONDS=900
export ODDS_SYNC_LIVE_RESERVE_SECONDS=0
export ODDS_SYNC_LIVE_GRACE_SECONDS=98
export ODDS_SYNC_MIN_NORMAL_LEASE_SECONDS=1
export ODDS_SYNC_P3_MAX_DURATION_SECONDS=5
export ODDS_SYNC_JOB_PRIORITY=normal
export ODDS_SYNC_LOCK_RETRY_ATTEMPTS=5
export ODDS_SYNC_LOCK_RETRY_DELAY_SECONDS=0.05
export ODDS_SYNC_STOP_RETRY_AFTER_LEASE_HANDOFF=true

set +e
run_with_global_lock_and_retry 'printf x >> "${CHAIN_MARKER}"; sleep 3' >"${tmp}/run.log" 2>&1
retry_status=$?
set -e
[[ "${retry_status}" -eq 2 ]]
[[ "$(wc -c <"${CHAIN_MARKER}")" -eq 1 ]]
grep -q 'normal-writer lease ended for settlement handoff' "${tmp}/run.log"
echo "fixture-core admitted-work retry guard passed"
