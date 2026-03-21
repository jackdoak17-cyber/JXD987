#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=./common.sh
source "${SCRIPT_DIR}/common.sh"

export REPO_ROOT
export LEAGUES="${LEAGUE_IDS:-$(default_league_csv)}"
export DAYS_FORWARD="${ODDS_DAYS_FORWARD:-14}"
export ODDS_BOOKMAKERS="${ODDS_BOOKMAKERS:-Bet365,Kambi,Paddy Power}"
export INGEST_MAX_RUNTIME_MINUTES="${ODDS_INGEST_MAX_RUNTIME_MINUTES:-25}"
export RETENTION_DAYS_BACK="${RETENTION_DAYS_BACK:-1}"
export RETENTION_DAYS_FORWARD="${RETENTION_DAYS_FORWARD:-14}"
export RETENTION_SNAPSHOT_DAYS="${RETENTION_SNAPSHOT_DAYS:-30}"
export FIXTURE_REFRESH_DAYS_BACK="${FIXTURE_REFRESH_DAYS_BACK:-2}"
export FIXTURE_REFRESH_DAYS_FORWARD="${FIXTURE_REFRESH_DAYS_FORWARD:-3}"
export FIXTURE_EXPORT_DAYS_BACK="${FIXTURE_EXPORT_DAYS_BACK:-2}"
export FIXTURE_EXPORT_DAYS_FORWARD="${FIXTURE_EXPORT_DAYS_FORWARD:-3}"
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

# Step 1: SportMonks refresh (inside P3 chain, no separate cron)
python scripts/sync_odds.py \
  --leagues "${LEAGUES}" \
  --days-forward "${DAYS_FORWARD}" \
  --refresh-upcoming \
  --refresh-only \
  --report-out "/tmp/odds_refresh_report_p3.json"

# Step 2: Odds fetch scoped to P3 fixtures only
python scripts/sync_odds.py \
  --leagues "${LEAGUES}" \
  --days-forward "${DAYS_FORWARD}" \
  --priority p3 \
  --bookmakers "${ODDS_BOOKMAKERS}" \
  --report-out "/tmp/odds_sync_report_p3.json" \
  --unmatched-out "/tmp/unmatched_players_p3.json"

# Step 3: Ingest full window (Path B)
python scripts/export_odds_to_supabase_psql.py \
  --leagues "${LEAGUES}" \
  --days-forward "${DAYS_FORWARD}" \
  --csv-out "/tmp/odds_outcomes_export_p3.csv" \
  --no-include-fixture-leagues \
  --progress-rows 10000 \
  --progress-fixtures 100 \
  --max-runtime-minutes "${INGEST_MAX_RUNTIME_MINUTES}" \
  --report-out "/tmp/odds_ingest_report_p3.json" \
  --skip-retention \
  --skip-retention-snapshots \
  ${COVERAGE_ARGS}

# Step 4: Retention only on P3
python scripts/odds_retention_psql.py \
  --days-back "${RETENTION_DAYS_BACK}" \
  --days-forward "${RETENTION_DAYS_FORWARD}" \
  --snapshot-days "${RETENTION_SNAPSHOT_DAYS}" \
  --report-out "/tmp/odds_retention_report_p3.json"

# Step 5: Best-effort recent fixture refresh/export.
if [[ -n "${SPORTMONKS_API_TOKEN:-}" && -n "${SUPABASE_URL:-}" && -n "${SUPABASE_SERVICE_ROLE_KEY:-}" ]]; then
  if python scripts/reconcile_recent_fixtures.py \
    --leagues "${LEAGUES}" \
    --days-back "${FIXTURE_REFRESH_DAYS_BACK}" \
    --days-forward "${FIXTURE_REFRESH_DAYS_FORWARD}"; then
    if ! python scripts/export_to_supabase.py \
      --strict \
      --leagues "${LEAGUES}" \
      --days-back "${FIXTURE_EXPORT_DAYS_BACK}" \
      --upcoming-days "${FIXTURE_EXPORT_DAYS_FORWARD}" \
      --fixture-core-only \
      --skip-prune; then
      echo "Recent fixture export failed; continuing odds pipeline" >&2
    fi
  else
    echo "Recent fixture refresh failed; continuing odds pipeline" >&2
  fi
else
  echo "Skipping recent fixture refresh/export; missing SportMonks or Supabase REST env" >&2
fi
CHAIN
)

status=0
run_with_global_lock_and_timeout "${CHAIN_COMMAND}" || status=$?
finalize_with_healthcheck "${status}" "${HEALTHCHECK_PING_URL_P3:-${HEALTHCHECK_PING_URL:-}}"
