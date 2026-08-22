#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=./common.sh
source "${SCRIPT_DIR}/common.sh"
verify_runtime_manifest_or_exit "$0"
require_runtime_manifest_entries_or_exit "$0" \
  "config/league_ids.txt" \
  "config/odds_api_sync_excluded_leagues.json" \
  "jxd/__init__.py" \
  "jxd/db.py" \
  "jxd/models.py" \
  "jxd/odds_api_client.py" \
  "jxd/sportmonks_client.py" \
  "jxd/sync.py" \
  "config/odds_api_leagues.json" \
  "scripts/sync_odds.py" \
  "scripts/export_odds_to_supabase_psql.py" \
  "scripts/odds_retention_psql.py" \
  "scripts/reconcile_recent_fixtures.py" \
  "scripts/export_to_supabase.py" \
  "scripts/refresh_fixture_delivery.py"

export REPO_ROOT
export STATS_LEAGUES="${STATS_LEAGUE_IDS:-${LEAGUE_IDS:-$(default_league_csv)}}"
export ODDS_LEAGUES="${ODDS_LEAGUE_IDS:-$(odds_league_csv)}"
export ODDS_SYNC_DAYS_BACK="${ODDS_SYNC_DAYS_BACK:-2}"
export DAYS_FORWARD="${ODDS_DAYS_FORWARD:-14}"
export ODDS_BOOKMAKERS="${ODDS_BOOKMAKERS:-Bet365,Paddy Power}"
export INGEST_MAX_RUNTIME_MINUTES="${ODDS_INGEST_MAX_RUNTIME_MINUTES:-25}"
export ODDS_EXPORT_DAYS_BACK="${ODDS_EXPORT_DAYS_BACK:-2}"
export RETENTION_DAYS_BACK="${RETENTION_DAYS_BACK:-1}"
export RETENTION_DAYS_FORWARD="${RETENTION_DAYS_FORWARD:-14}"
export RETENTION_SNAPSHOT_DAYS="${RETENTION_SNAPSHOT_DAYS:-30}"
export FIXTURE_REFRESH_DAYS_BACK="${FIXTURE_REFRESH_DAYS_BACK:-2}"
export FIXTURE_REFRESH_DAYS_FORWARD="${FIXTURE_REFRESH_DAYS_FORWARD:-3}"
export FIXTURE_EXPORT_DAYS_BACK="${FIXTURE_EXPORT_DAYS_BACK:-2}"
export FIXTURE_EXPORT_DAYS_FORWARD="${FIXTURE_EXPORT_DAYS_FORWARD:-3}"
export FIXTURE_DELIVERY_DAYS_FORWARD="${FIXTURE_DELIVERY_DAYS_FORWARD:-14}"
export FIXTURE_DELIVERY_TIMEOUT_SECONDS="${FIXTURE_DELIVERY_TIMEOUT_SECONDS:-1800}"
export RUN_COVERAGE="${RUN_COVERAGE:-false}"
export MONEYLINE_COVERAGE_DAYS_FORWARD="${MONEYLINE_COVERAGE_DAYS_FORWARD:-7}"
export MONEYLINE_COVERAGE_MIN_PCT="${MONEYLINE_COVERAGE_MIN_PCT:-100}"
export MONEYLINE_REPAIR_ATTEMPTS="${MONEYLINE_REPAIR_ATTEMPTS:-1}"

if [[ "${RUN_COVERAGE}" == "true" || "${RUN_COVERAGE}" == "1" ]]; then
  export COVERAGE_ARGS=""
else
  export COVERAGE_ARGS="--skip-coverage --skip-verification"
fi

# Opportunistic publish of betting picks (Models -> Supabase).
# This ensures picks are refreshed after the heaviest ingestion step completes,
# even if the standalone `run_models.sh` cron tick was skipped due to the lock.
export RUN_MODELS_PUBLISH="${RUN_MODELS_PUBLISH:-false}"
export MODELS_PUBLISH_AFTER_P3="${MODELS_PUBLISH_AFTER_P3:-false}"
export MODELS_REPO_ROOT="${MODELS_REPO_ROOT:-/opt/odds-sync/Models}"
export MODELS_ENV_PATH="${MODELS_ENV_PATH:-${REPO_ROOT}/.env}"
export MODELS_TOP="${MODELS_TOP:-50}"
export MODELS_FIXTURE_LIMIT="${MODELS_FIXTURE_LIMIT:-20}"
export MODELS_PLAYERS_LIMIT="${MODELS_PLAYERS_LIMIT:-20}"
export MODELS_PLAYER_REQUIRE_POSITIVE_EV="${MODELS_PLAYER_REQUIRE_POSITIVE_EV:-true}"
export MODELS_PLAYER_VALUE_ODDS_MIN="${MODELS_PLAYER_VALUE_ODDS_MIN:-1.0}"
export MODELS_PLAYER_HIGH_ODDS_MIN="${MODELS_PLAYER_HIGH_ODDS_MIN:-1.0}"
export MODELS_PLAYER_HIGH_PROB_MIN="${MODELS_PLAYER_HIGH_PROB_MIN:-0.8}"
export MODELS_PLAYER_HIGH_HIT_RATE_MIN="${MODELS_PLAYER_HIGH_HIT_RATE_MIN:-0.8}"
export MODELS_SKIP_PLAYER_AI="${MODELS_SKIP_PLAYER_AI:-false}"
export MODELS_SKIP_TEAM_AI="${MODELS_SKIP_TEAM_AI:-false}"

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
  --leagues "${STATS_LEAGUES}" \
  --days-back "${ODDS_SYNC_DAYS_BACK}" \
  --days-forward "${DAYS_FORWARD}" \
  --refresh-upcoming \
  --refresh-only \
  --report-out "/tmp/odds_refresh_report_p3.json"

# Step 2: Odds fetch scoped to P3 fixtures only
python scripts/sync_odds.py \
  --leagues "${ODDS_LEAGUES}" \
  --days-back "${ODDS_SYNC_DAYS_BACK}" \
  --days-forward "${DAYS_FORWARD}" \
  --priority p3 \
  --bookmakers "${ODDS_BOOKMAKERS}" \
  --report-out "/tmp/odds_sync_report_p3.json" \
  --unmatched-out "/tmp/unmatched_players_p3.json"

# Step 3: Ingest full window (Path B)
python scripts/export_odds_to_supabase_psql.py \
  --leagues "${ODDS_LEAGUES}" \
  --days-back "${ODDS_EXPORT_DAYS_BACK}" \
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
    --leagues "${STATS_LEAGUES}" \
    --days-back "${FIXTURE_REFRESH_DAYS_BACK}" \
    --days-forward "${FIXTURE_REFRESH_DAYS_FORWARD}" \
    --with-details; then
    if ! python scripts/export_to_supabase.py \
      --strict \
      --leagues "${STATS_LEAGUES}" \
      --days-back "${FIXTURE_EXPORT_DAYS_BACK}" \
      --upcoming-days "${FIXTURE_EXPORT_DAYS_FORWARD}" \
      --skip-odds-snapshots \
      --skip-odds-outcomes \
      --skip-prune; then
      echo "Recent fixture export failed; continuing odds pipeline" >&2
    fi
  else
    echo "Recent fixture refresh failed; continuing odds pipeline" >&2
  fi
else
  echo "Skipping recent fixture refresh/export; missing SportMonks or Supabase REST env" >&2
fi

# Step 6: Publish the persistent Fixtures Data Delivery v2 read models.
# This is the only user-facing fixture delivery source after cutover. A failed
# refresh fails P3 instead of hiding a stale or incomplete read model.
FIXTURE_DELIVERY_STATUS=0
if [[ -n "${SUPABASE_DB_URL_SESSION:-${SUPABASE_DB_URL:-}}" ]]; then
  python scripts/refresh_fixture_delivery.py \
    --start-date "$(date -u +%F)" \
    --end-date "$(date -u -d "+${FIXTURE_DELIVERY_DAYS_FORWARD} days" +%F)" \
    --leagues "${STATS_LEAGUES}" \
    --report-out /tmp/fixture_delivery_v2_report.json || FIXTURE_DELIVERY_STATUS=$?
else
  echo "Skipping Fixtures Data Delivery v2 refresh; missing Supabase DB URL" >&2
  FIXTURE_DELIVERY_STATUS=1
fi

# Step 7: Hard guard for the user-facing fixtures window.
set +e
python scripts/validate_moneyline_coverage.py \
  --leagues "${ODDS_LEAGUES}" \
  --days-forward "${MONEYLINE_COVERAGE_DAYS_FORWARD}" \
  --fail-below-pct "${MONEYLINE_COVERAGE_MIN_PCT}" \
  --out-json "/tmp/moneyline_coverage_report_p3.json" \
  --out-md "/tmp/moneyline_coverage_report_p3.md"
MONEYLINE_VALIDATION_STATUS=$?
set -e
if [[ "${MONEYLINE_VALIDATION_STATUS}" -ne 0 ]]; then
  echo "Moneyline fidelity report is red (exit=${MONEYLINE_VALIDATION_STATUS}); running bounded autonomous repair attempts before final alert." >&2
  repair_attempt=0
  while [[ "${MONEYLINE_VALIDATION_STATUS}" -ne 0 && "${repair_attempt}" -lt "${MONEYLINE_REPAIR_ATTEMPTS}" ]]; do
    repair_attempt=$((repair_attempt + 1))
    echo "Moneyline repair attempt ${repair_attempt}/${MONEYLINE_REPAIR_ATTEMPTS}: refetching P3 events and exporting the complete odds window." >&2
    set +e
    python scripts/sync_odds.py \
      --leagues "${ODDS_LEAGUES}" \
      --days-back "${ODDS_SYNC_DAYS_BACK}" \
      --days-forward "${DAYS_FORWARD}" \
      --priority p3 \
      --bookmakers "${ODDS_BOOKMAKERS}" \
      --report-out "/tmp/odds_sync_report_p3_repair_${repair_attempt}.json" \
      --unmatched-out "/tmp/unmatched_players_p3_repair_${repair_attempt}.json"
    repair_sync_status=$?
    if [[ "${repair_sync_status}" -eq 0 ]]; then
      python scripts/export_odds_to_supabase_psql.py \
        --leagues "${ODDS_LEAGUES}" \
        --days-back "${ODDS_EXPORT_DAYS_BACK}" \
        --days-forward "${DAYS_FORWARD}" \
        --csv-out "/tmp/odds_outcomes_export_p3_repair_${repair_attempt}.csv" \
        --no-include-fixture-leagues \
        --progress-rows 10000 \
        --progress-fixtures 100 \
        --max-runtime-minutes "${INGEST_MAX_RUNTIME_MINUTES}" \
        --report-out "/tmp/odds_ingest_report_p3_repair_${repair_attempt}.json" \
        --skip-retention \
        --skip-retention-snapshots \
        ${COVERAGE_ARGS}
      repair_export_status=$?
    else
      repair_export_status=${repair_sync_status}
    fi
    if [[ "${repair_sync_status}" -eq 0 && "${repair_export_status}" -eq 0 ]]; then
      python scripts/validate_moneyline_coverage.py \
        --leagues "${ODDS_LEAGUES}" \
        --days-forward "${MONEYLINE_COVERAGE_DAYS_FORWARD}" \
        --fail-below-pct "${MONEYLINE_COVERAGE_MIN_PCT}" \
        --out-json "/tmp/moneyline_coverage_report_p3.json" \
        --out-md "/tmp/moneyline_coverage_report_p3.md"
      MONEYLINE_VALIDATION_STATUS=$?
    else
      MONEYLINE_VALIDATION_STATUS=1
    fi
    set -e
  done
  if [[ "${MONEYLINE_VALIDATION_STATUS}" -ne 0 ]]; then
    echo "Moneyline fidelity remains red after ${repair_attempt} repair attempt(s); final P3 status will fail." >&2
  else
    echo "Moneyline fidelity passed after ${repair_attempt} autonomous repair attempt(s)." >&2
  fi
fi

# Step 8: Best-effort betting picks publish (uses odds already ingested into Supabase).
if [[ "${RUN_MODELS_PUBLISH}" == "true" || "${RUN_MODELS_PUBLISH}" == "1" ]]; then
  if [[ "${MODELS_PUBLISH_AFTER_P3}" == "true" || "${MODELS_PUBLISH_AFTER_P3}" == "1" ]]; then
    if [[ -d "${MODELS_REPO_ROOT}" ]]; then
      cd "${MODELS_REPO_ROOT}"
      if [[ -f .venv/bin/activate ]]; then
        # shellcheck disable=SC1091
        source .venv/bin/activate
      fi
      node scripts/create_betting_picks_tables.mjs --env "${MODELS_ENV_PATH}"
      python3 ml/publish_betting_picks_to_supabase.py \
        --env "${MODELS_ENV_PATH}" \
        --top "${MODELS_TOP}" \
        --fixtureLimit "${MODELS_FIXTURE_LIMIT}" \
        --playersLimit "${MODELS_PLAYERS_LIMIT}" \
        --playerRequirePositiveEv "${MODELS_PLAYER_REQUIRE_POSITIVE_EV}" \
        --playerValueOddsMin "${MODELS_PLAYER_VALUE_ODDS_MIN}" \
        --playerHighOddsMin "${MODELS_PLAYER_HIGH_ODDS_MIN}" \
        --playerHighProbMin "${MODELS_PLAYER_HIGH_PROB_MIN}" \
        --playerHighHitRateMin "${MODELS_PLAYER_HIGH_HIT_RATE_MIN}"
    else
      echo "Skipping models publish; Models repo missing at ${MODELS_REPO_ROOT}" >&2
    fi
  fi
fi

if [[ "${MONEYLINE_VALIDATION_STATUS:-0}" -ne 0 ]]; then
  exit "${MONEYLINE_VALIDATION_STATUS}"
fi
if [[ "${FIXTURE_DELIVERY_STATUS}" -ne 0 ]]; then
  echo "Fixtures Data Delivery v2 refresh failed; see /tmp/fixture_delivery_v2_report.json" >&2
  exit "${FIXTURE_DELIVERY_STATUS}"
fi
CHAIN
)

status=0
run_with_global_lock_and_timeout "${CHAIN_COMMAND}" || status=$?
finalize_with_healthcheck "${status}" "${HEALTHCHECK_PING_URL_P3:-${HEALTHCHECK_PING_URL:-}}"
