#!/usr/bin/env bash
set -euo pipefail
# Run from an immutable release. The caller supplies the private environment.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON="${REFEREE_PYTHON:-/opt/odds-sync/JXD987/.venv/bin/python}"
REPORTS="${REFEREE_REPORT_DIR:-/opt/odds-sync/reports/referee-delivery}"
MODE="${1:-${REFEREE_DELIVERY_MODE:-frequent}}"

if [[ "$MODE" != "frequent" && "$MODE" != "hourly" ]]; then
  echo "Usage: $(basename "$0") [frequent|hourly]" >&2
  exit 2
fi

mkdir -p "$REPORTS"
exec 9>"$REPORTS/delivery.lock"
flock -n 9 || exit 0

if [[ "$MODE" == "frequent" ]]; then
  "$PYTHON" "$ROOT/scripts/sync_fixture_referees.py" --days-back 0 --days-forward 14 --limit-fixtures 50 --write-batch-size 10 --report-json "$REPORTS/assignments.json"
  "$PYTHON" "$ROOT/scripts/sync_fixture_referee_stats.py" --days-back 0 --days-forward 14 --refresh-mode frequent --report-json "$REPORTS/stats.json"
else
  "$PYTHON" "$ROOT/scripts/hydrate_referee_history.py" --seed-days-back 0 --seed-days-forward 14 --report-json "$REPORTS/history.json"
  "$PYTHON" "$ROOT/scripts/sync_fixture_referee_stats.py" --days-back 0 --days-forward 14 --refresh-mode full --report-json "$REPORTS/stats.json"
fi
