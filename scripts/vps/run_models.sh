#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=./common.sh
source "${SCRIPT_DIR}/common.sh"
verify_runtime_manifest_or_exit "$0"
require_runtime_manifest_entries_or_exit "$0" \
  "scripts/vps/common.sh" \
  "scripts/vps/run_models.sh"

# This wrapper publishes betting picks into Supabase (and optionally R2) by running the
# publisher inside the Models repo. The public feed uses the global odds-sync lock;
# experimental-only output uses a dedicated private-ledger lock so model scoring
# cannot starve production odds/settlement writers.
#
# Expected VPS layout (defaults):
# - JXD987 repo:  /opt/odds-sync/JXD987
# - Models repo: /opt/odds-sync/Models
#
# Override paths via env:
# - MODELS_REPO_ROOT
# - MODELS_ENV_PATH
#
# Runtime knobs:
# - MODELS_MAX_DURATION_SECONDS (defaults to 900)
# - MODELS_TOP (defaults to 50)
# - MODELS_PUBLISH_R2 (true/false, defaults to true)

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
export MODELS_PUBLISH_R2="${MODELS_PUBLISH_R2:-true}"
export MODELS_EXPERIMENTAL_ONLY="${MODELS_EXPERIMENTAL_ONLY:-false}"
export MODELS_EXPERIMENTAL_LOCK_FILE="${MODELS_EXPERIMENTAL_LOCK_FILE:-/var/lock/models-experimental.lock}"

# Reuse the global lock helper, but allow a separate timeout for model publishing.
export ODDS_SYNC_P3_MAX_DURATION_SECONDS="${MODELS_MAX_DURATION_SECONDS:-900}"

CHAIN_COMMAND=$(cat <<'CHAIN'
set -euo pipefail

if [[ ! -d "${MODELS_REPO_ROOT}" ]]; then
  echo "Models repo not found at ${MODELS_REPO_ROOT}. Set MODELS_REPO_ROOT." >&2
  exit 1
fi

cd "${MODELS_REPO_ROOT}"

# Optional venv (recommended on VPS).
if [[ -f .venv/bin/activate ]]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
fi

python3 -V >/dev/null
node -v >/dev/null

if [[ -f ./.env ]]; then
  set -a
  # shellcheck disable=SC1091
  source ./.env
  set +a
fi

if [[ "${MODELS_EXPERIMENTAL_ONLY}" == "true" || "${MODELS_EXPERIMENTAL_ONLY}" == "1" ]]; then
  # Keep the research-only production lane behind the same bounded canonical
  # lock and heartbeat as the primary publisher. The external VPS helper used
  # to acquire the lock itself, which allowed a long model run to starve the
  # settlement writer and never recorded run_models.
  MODELS_PIPELINE_KEY="${MODELS_PIPELINE_KEY:-vps-experimental-$(date -u +%F)}" \
  MODELS_PLAYER_AI_OUT_DATE="${MODELS_PLAYER_AI_OUT_DATE:-}" \
  MODELS_TEAM_AI_OUT_DATE="${MODELS_TEAM_AI_OUT_DATE:-}" \
  MODELS_PLAYER_HIGH_PROB_MIN="${MODELS_EXPERIMENTAL_PLAYER_HIGH_PROB_MIN:-0.0}" \
    ./scripts/run_experimental_models.sh "${MODELS_ENV_PATH}"

  python3 ml/settle_experimental_bets.py --env "${MODELS_ENV_PATH}"
else
  # Ensure the publisher tables exist and grants are applied.
  node scripts/create_betting_picks_tables.mjs --env "${MODELS_ENV_PATH}"

  # Publish the latest picks into Supabase (primary feed).
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

  # Optional: publish to R2 as a fallback/archive if credentials are present.
  if [[ "${MODELS_PUBLISH_R2}" == "true" || "${MODELS_PUBLISH_R2}" == "1" ]]; then
    if [[ -n "${CLOUDFLARE_R2_BUCKET:-}" && -n "${CLOUDFLARE_R2_ACCOUNT_ID:-}" && -n "${CLOUDFLARE_R2_ACCESS_KEY_ID:-}" && -n "${CLOUDFLARE_R2_SECRET_ACCESS_KEY:-}" ]]; then
      python3 ml/publish_betting_picks_to_r2.py \
        --bucket "${CLOUDFLARE_R2_BUCKET}" \
        --prefix betting-picks \
        --top "${MODELS_TOP}" \
        --fixtureLimit "${MODELS_FIXTURE_LIMIT}" \
        --playersLimit "${MODELS_PLAYERS_LIMIT}"
    else
      echo "Skipping R2 publish; missing CLOUDFLARE_R2_* env vars." >&2
    fi
  fi
fi
CHAIN
)

status=0
RUN_STARTED_AT="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
RUN_STARTED_EPOCH="$(date -u +"%s")"
if [[ "${MODELS_EXPERIMENTAL_ONLY}" == "true" || "${MODELS_EXPERIMENTAL_ONLY}" == "1" ]]; then
  run_with_dedicated_lock_and_timeout \
    "${CHAIN_COMMAND}" \
    "${MODELS_EXPERIMENTAL_LOCK_FILE}" \
    "${MODELS_MAX_DURATION_SECONDS:-900}" || status=$?
else
  run_with_global_lock_and_timeout "${CHAIN_COMMAND}" || status=$?
fi
RUN_FINISHED_AT="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
RUN_FINISHED_EPOCH="$(date -u +"%s")"
record_pipeline_job_run \
  "run_models" \
  "Models publish" \
  "${status}" \
  "${RUN_STARTED_AT}" \
  "${RUN_FINISHED_AT}" \
  "$(((RUN_FINISHED_EPOCH - RUN_STARTED_EPOCH) * 1000))"
finalize_with_healthcheck "${status}" "${HEALTHCHECK_PING_URL_MODELS:-${HEALTHCHECK_PING_URL:-}}"
