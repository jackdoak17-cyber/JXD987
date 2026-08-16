#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=./common.sh
source "${SCRIPT_DIR}/common.sh"
verify_runtime_manifest_or_exit "$0"
require_runtime_manifest_entries_or_exit "$0" \
  "config/league_ids.txt" \
  "jxd/__init__.py" \
  "jxd/db.py" \
  "jxd/models.py" \
  "jxd/odds_api_client.py" \
  "jxd/sportmonks_client.py" \
  "jxd/sync.py" \
  "config/odds_api_leagues.json" \
  "scripts/sync_odds.py" \
  "scripts/sync_sportmonks_metadata.py" \
  "scripts/sync_sparse_squads.py" \
  "scripts/export_odds_to_supabase_psql.py" \
  "scripts/odds_retention_psql.py" \
  "scripts/reconcile_recent_fixtures.py" \
  "scripts/validate_moneyline_coverage.py" \
  "scripts/validate_player_prop_odds_feed.py" \
  "scripts/export_to_supabase.py"

export REPO_ROOT
export STATS_LEAGUES="${STATS_LEAGUE_IDS:-${LEAGUE_IDS:-$(default_league_csv)}}"
export ODDS_LEAGUES="${ODDS_LEAGUE_IDS:-$(odds_league_csv)}"
export FIXTURE_LEAGUES="${FIXTURE_LEAGUE_IDS:-$(union_csv "${STATS_LEAGUES}" "${ODDS_LEAGUES}")}"
export ODDS_SYNC_DAYS_BACK="${ODDS_SYNC_DAYS_BACK:-2}"
export DAYS_FORWARD="${ODDS_DAYS_FORWARD:-14}"
export FIXTURE_SYNC_DAYS_FORWARD="${FIXTURE_SYNC_DAYS_FORWARD:-31}"
export ODDS_BOOKMAKERS="${ODDS_BOOKMAKERS:-Bet365,Paddy Power,Unibet,BetMGM,Betfair Exchange}"
export INGEST_MAX_RUNTIME_MINUTES="${ODDS_INGEST_MAX_RUNTIME_MINUTES:-25}"
export ODDS_EXPORT_DAYS_BACK="${ODDS_EXPORT_DAYS_BACK:-2}"
export RETENTION_DAYS_BACK="${RETENTION_DAYS_BACK:-1}"
export RETENTION_DAYS_FORWARD="${RETENTION_DAYS_FORWARD:-14}"
export RETENTION_SNAPSHOT_DAYS="${RETENTION_SNAPSHOT_DAYS:-30}"
export FIXTURE_REFRESH_DAYS_BACK="${FIXTURE_REFRESH_DAYS_BACK:-30}"
export FIXTURE_REFRESH_DAYS_FORWARD="${FIXTURE_REFRESH_DAYS_FORWARD:-0}"
export FIXTURE_EXPORT_DAYS_BACK="${FIXTURE_EXPORT_DAYS_BACK:-30}"
export FIXTURE_EXPORT_DAYS_FORWARD="${FIXTURE_EXPORT_DAYS_FORWARD:-31}"
export LATE_FIXTURE_RECONCILE_HOURS_BACK="${LATE_FIXTURE_RECONCILE_HOURS_BACK:-48}"
export LATE_FIXTURE_RECONCILE_EXPORT_DAYS_BACK="${LATE_FIXTURE_RECONCILE_EXPORT_DAYS_BACK:-2}"
export SQUAD_REFRESH_MIN_PLAYERS="${SQUAD_REFRESH_MIN_PLAYERS:-15}"
export RUN_COVERAGE="${RUN_COVERAGE:-false}"
export MONEYLINE_COVERAGE_DAYS_FORWARD="${MONEYLINE_COVERAGE_DAYS_FORWARD:-7}"
export MONEYLINE_COVERAGE_MIN_PCT="${MONEYLINE_COVERAGE_MIN_PCT:-100}"
export PLAYER_PROP_VALIDATE="${PLAYER_PROP_VALIDATE:-true}"
export PLAYER_PROP_VALIDATE_DAYS_BACK="${PLAYER_PROP_VALIDATE_DAYS_BACK:-${ODDS_EXPORT_DAYS_BACK}}"
export PLAYER_PROP_VALIDATE_DAYS_FORWARD="${PLAYER_PROP_VALIDATE_DAYS_FORWARD:-${DAYS_FORWARD}}"
export ODDS_SYNC_P3_MAX_DURATION_SECONDS="${ODDS_SYNC_P3_MAX_DURATION_SECONDS:-5400}"
export FIXTURE_CORE_EXPORT_CHUNK="${FIXTURE_CORE_EXPORT_CHUNK:-500}"
export FIXTURE_DETAIL_EXPORT_CHUNK="${FIXTURE_DETAIL_EXPORT_CHUNK:-500}"

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
export MODELS_PLAYER_HIGH_PROB_MIN="${MODELS_PLAYER_HIGH_PROB_MIN:-0.0}"
export MODELS_PLAYER_HIGH_HIT_RATE_MIN="${MODELS_PLAYER_HIGH_HIT_RATE_MIN:-0.8}"
export MODELS_TEAM_VALUE_HIT_RATE_MIN="${MODELS_TEAM_VALUE_HIT_RATE_MIN:-0.7}"
export MODELS_TEAM_HIGH_HIT_RATE_MIN="${MODELS_TEAM_HIGH_HIT_RATE_MIN:-0.8}"
export MODELS_TEAM_HIGH_ODDS_MIN="${MODELS_TEAM_HIGH_ODDS_MIN:-1.0}"
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
# shellcheck source=./scripts/vps/common.sh
source "${REPO_ROOT}/scripts/vps/common.sh"

run_isolated_pipeline_job() {
  local job_id="$1"
  local job_name="$2"
  local command="$3"
  local max_runtime="${4:-${PIPELINE_ISOLATED_JOB_TIMEOUT_SECONDS:-1800}}"
  local started_at finished_at started_epoch finished_epoch status

  started_at="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  started_epoch="$(date -u +"%s")"
  set +e
  timeout "${max_runtime}" bash -lc "${command}"
  status=$?
  set -e
  finished_at="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  finished_epoch="$(date -u +"%s")"
  record_pipeline_job_run \
    "${job_id}" \
    "${job_name}" \
    "${status}" \
    "${started_at}" \
    "${finished_at}" \
    "$(((finished_epoch - started_epoch) * 1000))"
  if [[ "${status}" -ne 0 ]]; then
    if [[ "${status}" -eq 124 || "${status}" -eq 137 ]]; then
      echo "${job_name} exceeded ${max_runtime}s; continuing odds pipeline" >&2
    else
      echo "${job_name} failed with status ${status}; continuing odds pipeline" >&2
    fi
  fi
  return 0
}

# Step 1: Fixture-core refresh/export for the full user-facing league contract.
if [[ -n "${SPORTMONKS_API_TOKEN:-}" && -n "${SUPABASE_URL:-}" && -n "${SUPABASE_SERVICE_ROLE_KEY:-}" ]]; then
  run_isolated_pipeline_job \
    "run_p3_fixture_core_export" \
    "P3 fixture-core export" \
    "cd \"${REPO_ROOT}\" && source .venv/bin/activate && export PYTHONPATH=\"${REPO_ROOT}\" && python scripts/sync_sportmonks_metadata.py --leagues \"${FIXTURE_LEAGUES}\" && python scripts/sync_odds.py --leagues \"${FIXTURE_LEAGUES}\" --days-back \"${ODDS_SYNC_DAYS_BACK}\" --days-forward \"${FIXTURE_SYNC_DAYS_FORWARD}\" --refresh-upcoming --refresh-only --no-refresh-squads-missing --no-refresh-sidelined-window --report-out \"/tmp/odds_refresh_report_p3.json\" && SUPABASE_EXPORT_CHUNK=\"${FIXTURE_CORE_EXPORT_CHUNK}\" python scripts/export_to_supabase.py --strict --fixture-core-only --leagues \"${FIXTURE_LEAGUES}\" --days-back 0 --upcoming-days \"${FIXTURE_EXPORT_DAYS_FORWARD}\" --skip-prune --report-json \"/tmp/fixture_core_export_report_p3.json\"" \
    "${P3_FIXTURE_CORE_EXPORT_TIMEOUT_SECONDS:-1200}"
else
  echo "Skipping fixture-core refresh/export; missing SportMonks or Supabase REST env" >&2
fi

# Step 2: Refresh/export sparse current-season squads.
if [[ -n "${SPORTMONKS_API_TOKEN:-}" && -n "${SUPABASE_URL:-}" && -n "${SUPABASE_SERVICE_ROLE_KEY:-}" ]]; then
  run_isolated_pipeline_job \
    "run_p3_sparse_squad_refresh" \
    "P3 sparse squad refresh" \
    "cd \"${REPO_ROOT}\" && source .venv/bin/activate && export PYTHONPATH=\"${REPO_ROOT}\" && python scripts/sync_sparse_squads.py --leagues \"${STATS_LEAGUES}\" --minimum-players \"${SQUAD_REFRESH_MIN_PLAYERS}\" --report-json \"/tmp/sparse_squad_refresh_report_p3.json\"" \
    "${P3_SPARSE_SQUAD_REFRESH_TIMEOUT_SECONDS:-900}"
else
  echo "Skipping sparse squad refresh; missing SportMonks or Supabase REST env" >&2
fi

# Step 3: Odds fetch scoped to P3 fixtures only
python scripts/sync_odds.py \
  --leagues "${ODDS_LEAGUES}" \
  --days-back "${ODDS_SYNC_DAYS_BACK}" \
  --days-forward "${DAYS_FORWARD}" \
  --priority p3 \
  --bookmakers "${ODDS_BOOKMAKERS}" \
  --report-out "/tmp/odds_sync_report_p3.json" \
  --unmatched-out "/tmp/unmatched_players_p3.json"

# Step 4: Ingest full window (Path B)
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

# Step 5: Retention only on P3
python scripts/odds_retention_psql.py \
  --days-back "${RETENTION_DAYS_BACK}" \
  --days-forward "${RETENTION_DAYS_FORWARD}" \
  --snapshot-days "${RETENTION_SNAPSHOT_DAYS}" \
  --report-out "/tmp/odds_retention_report_p3.json"

# Step 6: Report the user-facing fixtures moneyline window.
# Keep this close to the odds sync/export so the live API comparison is not
# invalidated by provider updates that arrive during the long stats exports.
# This validator is intentionally non-blocking: it records a red fidelity
# result for Operations, while P3 continues to independent SportMonks and
# model work after odds ingestion has already completed.
set +e
python scripts/validate_moneyline_coverage.py \
  --leagues "${ODDS_LEAGUES}" \
  --days-forward "${MONEYLINE_COVERAGE_DAYS_FORWARD}" \
  --priority p3 \
  --fail-below-pct "${MONEYLINE_COVERAGE_MIN_PCT}" \
  --out-json "/tmp/moneyline_coverage_report_p3.json" \
  --out-md "/tmp/moneyline_coverage_report_p3.md"
MONEYLINE_VALIDATION_STATUS=$?
set -e
if [[ "${MONEYLINE_VALIDATION_STATUS}" -ne 0 ]]; then
  echo "Moneyline fidelity report is red (exit=${MONEYLINE_VALIDATION_STATUS}); continuing P3 downstream work." >&2
fi

# Step 7: Best-effort recent fixture detail refresh/export.
if [[ -n "${SPORTMONKS_API_TOKEN:-}" && -n "${SUPABASE_URL:-}" && -n "${SUPABASE_SERVICE_ROLE_KEY:-}" ]]; then
  run_isolated_pipeline_job \
    "run_p3_stats_detail_export" \
    "P3 stats-detail export" \
    "cd \"${REPO_ROOT}\" && source .venv/bin/activate && export PYTHONPATH=\"${REPO_ROOT}\" && python scripts/sync_sportmonks_metadata.py --leagues \"${STATS_LEAGUES}\" && python scripts/reconcile_recent_fixtures.py --leagues \"${STATS_LEAGUES}\" --days-back \"${FIXTURE_REFRESH_DAYS_BACK}\" --days-forward \"${FIXTURE_REFRESH_DAYS_FORWARD}\" --with-details && SUPABASE_EXPORT_CHUNK=\"${FIXTURE_DETAIL_EXPORT_CHUNK}\" python scripts/export_to_supabase.py --strict --leagues \"${STATS_LEAGUES}\" --days-back \"${FIXTURE_EXPORT_DAYS_BACK}\" --upcoming-days 0 --skip-odds-snapshots --skip-odds-outcomes --skip-prune --report-json \"/tmp/stats_detail_export_report_p3.json\"" \
    "${P3_STATS_DETAIL_EXPORT_TIMEOUT_SECONDS:-2400}"
else
  echo "Skipping recent fixture refresh/export; missing SportMonks or Supabase REST env" >&2
fi

# Step 8: Late post-match reconciliation for provider updates that land after the main stats export.
if [[ -n "${SPORTMONKS_API_TOKEN:-}" && -n "${SUPABASE_URL:-}" && -n "${SUPABASE_SERVICE_ROLE_KEY:-}" ]]; then
  run_isolated_pipeline_job \
    "run_p3_late_fixture_detail_reconcile" \
    "P3 late fixture-detail reconcile" \
    "cd \"${REPO_ROOT}\" && source .venv/bin/activate && export PYTHONPATH=\"${REPO_ROOT}\" && python scripts/reconcile_recent_fixtures.py --leagues \"${STATS_LEAGUES}\" --completed-hours-back \"${LATE_FIXTURE_RECONCILE_HOURS_BACK}\" --with-details --report-json \"/tmp/late_fixture_detail_reconcile_report_p3.json\" && SUPABASE_EXPORT_CHUNK=\"${FIXTURE_DETAIL_EXPORT_CHUNK}\" python scripts/export_to_supabase.py --strict --leagues \"${STATS_LEAGUES}\" --days-back \"${LATE_FIXTURE_RECONCILE_EXPORT_DAYS_BACK}\" --upcoming-days 0 --skip-odds-snapshots --skip-odds-outcomes --skip-prune --report-json \"/tmp/late_fixture_detail_export_report_p3.json\"" \
    "${P3_LATE_FIXTURE_RECONCILE_TIMEOUT_SECONDS:-1200}"
else
  echo "Skipping late fixture-detail reconcile; missing SportMonks or Supabase REST env" >&2
fi

# Step 9: Hard guard for the player-prop odds feed used by betting models.
if [[ "${PLAYER_PROP_VALIDATE}" == "true" || "${PLAYER_PROP_VALIDATE}" == "1" ]]; then
  python scripts/validate_player_prop_odds_feed.py \
    --days-back "${PLAYER_PROP_VALIDATE_DAYS_BACK}" \
    --days-forward "${PLAYER_PROP_VALIDATE_DAYS_FORWARD}" \
    --out-json "/tmp/player_prop_odds_feed_report_p3.json" \
    --out-md "/tmp/player_prop_odds_feed_report_p3.md"
fi

# Step 10: Best-effort betting picks publish (uses odds already ingested into Supabase).
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
CHAIN
)

status=0
RUN_STARTED_AT="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
RUN_STARTED_EPOCH="$(date -u +"%s")"
run_with_global_lock_and_timeout "${CHAIN_COMMAND}" || status=$?
RUN_FINISHED_AT="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
RUN_FINISHED_EPOCH="$(date -u +"%s")"
record_pipeline_job_run \
  "run_p3" \
  "P3 Supabase ingest" \
  "${status}" \
  "${RUN_STARTED_AT}" \
  "${RUN_FINISHED_AT}" \
  "$(((RUN_FINISHED_EPOCH - RUN_STARTED_EPOCH) * 1000))"
finalize_with_healthcheck "${status}" "${HEALTHCHECK_PING_URL_P3:-${HEALTHCHECK_PING_URL:-}}"
