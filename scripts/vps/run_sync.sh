#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=./common.sh
source "${SCRIPT_DIR}/common.sh"

export REPO_ROOT
export LEAGUES="${LEAGUE_IDS:-$(default_league_csv)}"
export DAYS_FORWARD="${ODDS_DAYS_FORWARD:-14}"
export INGEST_MAX_RUNTIME_MINUTES="${ODDS_INGEST_MAX_RUNTIME_MINUTES:-25}"
export RETENTION_DAYS_BACK="${RETENTION_DAYS_BACK:-1}"
export RETENTION_DAYS_FORWARD="${RETENTION_DAYS_FORWARD:-14}"
export RETENTION_SNAPSHOT_DAYS="${RETENTION_SNAPSHOT_DAYS:-30}"
export ODDS_BOOKMAKERS="${ODDS_BOOKMAKERS:-Bet365,Kambi,Paddy Power}"
export RUN_COVERAGE="${RUN_COVERAGE:-false}"

if [[ "${RUN_COVERAGE}" == "true" || "${RUN_COVERAGE}" == "1" ]]; then
  export COVERAGE_ARGS=""
else
  export COVERAGE_ARGS="--skip-coverage --skip-verification"
fi

CHAIN_COMMAND=$(cat <<'CHAIN'
set -euo pipefail

cd "${REPO_ROOT}"
source .venv/bin/activate
export PYTHONPATH="${REPO_ROOT}"

if [[ -f ./.env ]]; then
  set -a
  source ./.env
  set +a
fi

export SUPABASE_DB_URL_SESSION="${SUPABASE_DB_URL_SESSION:-${SUPABASE_DB_URL:-}}"
export PGSSLMODE="${PGSSLMODE:-require}"

for league_id in ${LEAGUES//,/ }; do
  python scripts/preflight_supabase_psql.py \
    --league-id "${league_id}" \
    --report-out "/tmp/odds_preflight_${league_id}.json" \
    --stderr-out "/tmp/odds_preflight_err_${league_id}.txt"

  if [[ -n "${SUPABASE_URL:-}" && -n "${SUPABASE_SERVICE_ROLE_KEY:-}" ]]; then
    python scripts/rest_preflight.py \
      --path "/rest/v1/odds_outcomes?select=fixture_id&limit=1" \
      --env-out "/tmp/odds_rest_env_${league_id}.txt"
  fi

  python scripts/sync_odds.py \
    --leagues "${league_id}" \
    --days-forward "${DAYS_FORWARD}" \
    --refresh-upcoming \
    --bookmakers "${ODDS_BOOKMAKERS}" \
    --report-out "/tmp/odds_sync_report_${league_id}.json" \
    --unmatched-out "/tmp/unmatched_players_${league_id}.json"

  python scripts/export_odds_to_supabase_psql.py \
    --leagues "${league_id}" \
    --days-forward "${DAYS_FORWARD}" \
    --csv-out "/tmp/odds_outcomes_export_${league_id}.csv" \
    --no-include-fixture-leagues \
    --progress-rows 10000 \
    --progress-fixtures 100 \
    --max-runtime-minutes "${INGEST_MAX_RUNTIME_MINUTES}" \
    --report-out "/tmp/odds_ingest_report_${league_id}.json" \
    --skip-retention \
    --skip-retention-snapshots \
    ${COVERAGE_ARGS}
done

python scripts/odds_retention_psql.py \
  --days-back "${RETENTION_DAYS_BACK}" \
  --days-forward "${RETENTION_DAYS_FORWARD}" \
  --snapshot-days "${RETENTION_SNAPSHOT_DAYS}" \
  --report-out "/tmp/odds_retention_report.json"
CHAIN
)

status=0
run_with_global_lock_and_timeout "${CHAIN_COMMAND}" || status=$?
finalize_with_healthcheck "${status}" "${HEALTHCHECK_PING_URL:-}"
