#!/usr/bin/env bash
set -euo pipefail
# Publish the local odds collected by P1/P2 without waiting for six-hour P3.
# Use the verified production exporter, including its settled-fixture guards.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ODDS_DELIVERY_RUNTIME="$(cd "${SCRIPT_DIR}/../.." && pwd)"
source "${ODDS_DELIVERY_RUNTIME}/scripts/vps/common.sh"
verify_runtime_manifest_or_exit "${ODDS_DELIVERY_RUNTIME}/scripts/vps/run_odds_p3.sh"
require_runtime_manifest_entries_or_exit "$0" "scripts/export_odds_to_supabase_psql.py"
require_runtime_manifest_entries_or_exit "$0" "scripts/reconcile_fixture_delivery_odds.py"
export REPO_ROOT
export ODDS_LEAGUES="$(odds_league_csv)"
if [[ "${ODDS_DELIVERY_VERIFY_ONLY:-false}" == "true" ]]; then
  cd "${REPO_ROOT}"
  for required_path in .env .venv/bin/activate data/jxd.sqlite; do
    if [[ ! -e "${required_path}" ]]; then
      log_error "odds delivery startup dependency missing: ${REPO_ROOT}/${required_path}"
      exit 1
    fi
  done
  source .venv/bin/activate
  set -a
  source .env
  set +a
  PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="${REPO_ROOT}/scripts" python - <<'PY'
import ast
from pathlib import Path

for path in (
    Path("scripts/export_odds_to_supabase_psql.py"),
    Path("scripts/reconcile_fixture_delivery_odds.py"),
):
    ast.parse(path.read_text(encoding="utf-8"), filename=str(path))

import reconcile_fixture_delivery_odds  # noqa: F401, E402
PY
  log_info "odds delivery startup verified runtime=${REPO_ROOT} exporter=scripts/export_odds_to_supabase_psql.py reconciler=scripts/reconcile_fixture_delivery_odds.py"
  exit 0
fi
export ODDS_SYNC_LOCK_RETRY_ATTEMPTS=20
export ODDS_SYNC_LOCK_RETRY_DELAY_SECONDS=15
CHAIN_COMMAND=$(cat <<'CHAIN'
set -euo pipefail
cd "${REPO_ROOT}"
source .venv/bin/activate
set -a
source .env
set +a
export ODDS_LOCK_TIMEOUT=15000
export ODDS_STATEMENT_TIMEOUT=180000
export ODDS_IDLE_TX_TIMEOUT=180000
export ODDS_USE_ADVISORY_LOCK=true
# Do not perform retention or fill from unrelated fixture leagues in this job.
python "${ODDS_DELIVERY_EXPORTER:-scripts/export_odds_to_supabase_psql.py}" \
  --leagues "${ODDS_LEAGUES}" --days-back 0 --days-forward 14 \
  --calendar-window --no-include-fixture-leagues \
  --csv-out /tmp/odds_outcomes_delivery.csv \
  --report-out /tmp/odds_delivery_report.json \
  --max-runtime-minutes 5 --skip-retention --skip-retention-snapshots
# Only a successfully committed canonical odds delivery reaches this step.
# A busy fixture publisher lock defers reconciliation until the next run.
python scripts/reconcile_fixture_delivery_odds.py \
  --report-out /tmp/fixture_delivery_odds_sync_report.json \
  --delivery-report /tmp/odds_delivery_report.json
CHAIN
)
run_recorded_pipeline_job "run_odds_delivery" "Website odds delivery" \
  "${CHAIN_COMMAND}" "/tmp/odds_delivery_report.json"
