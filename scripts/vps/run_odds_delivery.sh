#!/usr/bin/env bash
set -euo pipefail
# Publish the local odds collected by P1/P2 without waiting for six-hour P3.
# Use the verified production exporter, including its settled-fixture guards.
ODDS_DELIVERY_RUNTIME="${ODDS_DELIVERY_RUNTIME:-/opt/odds-sync/JXD987-odds-matcher-5f50ff2-release}"
source "${ODDS_DELIVERY_RUNTIME}/scripts/vps/common.sh"
verify_runtime_manifest_or_exit "${ODDS_DELIVERY_RUNTIME}/scripts/vps/run_odds_p3.sh"
require_runtime_manifest_entries_or_exit "$0" "scripts/export_odds_to_supabase_psql.py"
export REPO_ROOT
export ODDS_LEAGUES="$(odds_league_csv)"
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
CHAIN
)
run_recorded_pipeline_job "run_odds_delivery" "Website odds delivery" \
  "${CHAIN_COMMAND}" "/tmp/odds_delivery_report.json"
