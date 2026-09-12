#!/usr/bin/env bash
set -euo pipefail
# Run from an immutable release. The caller supplies the private environment.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON="${REFEREE_PYTHON:-/opt/odds-sync/JXD987/.venv/bin/python}"
REPORTS="${REFEREE_REPORT_DIR:-/opt/odds-sync/reports/referee-delivery}"
mkdir -p "$REPORTS"
exec 9>"$REPORTS/delivery.lock"
flock -n 9 || exit 0
"$PYTHON" "$ROOT/scripts/sync_fixture_referees.py" --days-back 0 --days-forward 14 --limit-fixtures 50 --write-batch-size 10 --report-json "$REPORTS/assignments.json"
"$PYTHON" "$ROOT/scripts/hydrate_referee_history.py" --seed-days-back 0 --seed-days-forward 14 --report-json "$REPORTS/history.json"
"$PYTHON" "$ROOT/scripts/sync_fixture_referee_stats.py" --days-back 0 --days-forward 14 --report-json "$REPORTS/stats.json"
