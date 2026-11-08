#!/usr/bin/env bash
set -euo pipefail

# Run on Linux with GNU timeout, against the staged runtime archive. This
# executes the wrapper's actual timeout helper without calling the publisher.
root="${1:?staged runtime archive path required}"
wrapper="${root}/scripts/vps/run_fixture_delivery_publisher.sh"
[[ -f "$wrapper" && -f "${root}/scripts/fixture_delivery_dirty_state.py" ]]
command -v timeout >/dev/null
helper="$(sed -n '/^run_dirty_state() {/,/^}/p' "$wrapper")"
validator="$(sed -n '/^require_positive_seconds() {/,/^}/p' "$wrapper")"
[[ -n "$helper" && -n "$validator" ]]

set +e
VALIDATOR_SOURCE="$validator" bash -c 'log_error() { :; }; eval "$VALIDATOR_SOURCE"; require_positive_seconds FIXTURE_DELIVERY_PUBLISHER_MAX_RUNTIME_SECONDS 0' >/dev/null 2>&1
zero_status=$?
set -e
[[ "$zero_status" -ne 0 ]] \
  || { echo "FAIL: zero runtime was accepted as an unlimited timeout" >&2; exit 1; }

work="$(mktemp -d /tmp/odds-publisher-lock-budget.XXXXXX)"
holder_pid=""
cleanup() {
  if [[ -n "$holder_pid" ]]; then
    kill "$holder_pid" >/dev/null 2>&1 || true
    wait "$holder_pid" >/dev/null 2>&1 || true
  fi
  rm -rf "$work"
}
trap cleanup EXIT
state="${work}/dirty-state.json"
lock="${state}.lock"
ready="${work}/ready"
python3 - "$lock" "$ready" <<'PY' &
import fcntl
import pathlib
import sys
import time
lock, ready = map(pathlib.Path, sys.argv[1:])
with lock.open('a+') as handle:
    fcntl.flock(handle, fcntl.LOCK_EX)
    ready.touch()
    time.sleep(8)
PY
holder_pid=$!
for _ in {1..60}; do
  [[ -f "$ready" ]] && break
  sleep 0.05
done
[[ -f "$ready" ]] || { echo "FAIL: dirty-state lock holder did not start" >&2; exit 1; }

eval "$helper"
export FIXTURE_DELIVERY_DIRTY_STATE_TIMEOUT_SECONDS=1
cd "$root"
started="$(date +%s)"
set +e
run_dirty_state --state-path "$state" status >"${work}/status.json" 2>"${work}/status.err"
status=$?
set -e
elapsed=$(( $(date +%s) - started ))
[[ "$status" -eq 124 && "$elapsed" -le 7 ]] \
  || { echo "FAIL: held dirty-state lock was not bounded (status=${status} elapsed=${elapsed}s)" >&2; exit 1; }
kill -0 "$holder_pid"
echo "PASS: publisher preparation rejects zero runtime and bounds a held dirty-state lock"
