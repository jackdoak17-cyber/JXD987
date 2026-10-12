#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=./common.sh
source "${SCRIPT_DIR}/common.sh"
verify_runtime_manifest_or_exit "$0"
require_runtime_manifest_entries_or_exit "$0" \
  "scripts/vps/common.sh" \
  "scripts/vps/run_models_train_weekly.sh"

export MODELS_REPO_ROOT="${MODELS_REPO_ROOT:-/opt/odds-sync/Models}"
export MODELS_ENV_PATH="${MODELS_ENV_PATH:-${MODELS_REPO_ROOT}/.env}"
export MODELS_TOP="${MODELS_TOP:-50}"
export MODELS_PUBLISH_R2="${MODELS_PUBLISH_R2:-true}"
export MODELS_PLAYER_FEATURE_SET="${MODELS_PLAYER_FEATURE_SET:-core}"
export MODELS_TRAIN_TIMEOUT_SECONDS="${MODELS_TRAIN_TIMEOUT_SECONDS:-14400}" # 4h

# Share global lock with sync jobs.
export ODDS_SYNC_P3_MAX_DURATION_SECONDS="${MODELS_TRAIN_TIMEOUT_SECONDS}"

CHAIN_COMMAND=$(cat <<'CHAIN'
set -euo pipefail

if [[ ! -d "${MODELS_REPO_ROOT}" ]]; then
  echo "Models repo not found at ${MODELS_REPO_ROOT}. Set MODELS_REPO_ROOT." >&2
  exit 1
fi

cd "${MODELS_REPO_ROOT}"

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

out_date="$(date -u +%F-%H%M)"
player_ai_out_date="${out_date}-player-ai"
team_ai_out_date="${out_date}-team-ai"

node scripts/create_betting_picks_tables.mjs --env "${MODELS_ENV_PATH}"

python3 ml/train_sidecar_models.py \
  --env "${MODELS_ENV_PATH}" \
  --outDate "${player_ai_out_date}" \
  --featureSet "${MODELS_PLAYER_FEATURE_SET}" \
  --withOdds \
  --oddsDays 45 \
  --oddsSnapshotType first_eval

python3 ml/build_team_training_dataset.py \
  --env "${MODELS_ENV_PATH}" \
  --prefix team-ml-dataset \
  --shardRows 5000

python3 ml/train_team_count_models.py \
  --env3 "${MODELS_ENV_PATH}" \
  --r2Prefix team-ml-dataset/ \
  --outDate "${team_ai_out_date}" \
  --valFrac 0.2

python3 ml/publish_betting_picks_to_supabase.py \
  --env "${MODELS_ENV_PATH}" \
  --top "${MODELS_TOP}" \
  --playerMlFeatureSet "${MODELS_PLAYER_FEATURE_SET}" \
  --playerAiOutDate "${player_ai_out_date}" \
  --teamAiOutDate "${team_ai_out_date}"

if [[ "${MODELS_PUBLISH_R2}" == "true" || "${MODELS_PUBLISH_R2}" == "1" ]]; then
  if [[ -n "${CLOUDFLARE_R2_BUCKET:-}" && -n "${CLOUDFLARE_R2_ACCOUNT_ID:-}" && -n "${CLOUDFLARE_R2_ACCESS_KEY_ID:-}" && -n "${CLOUDFLARE_R2_SECRET_ACCESS_KEY:-}" ]]; then
    python3 ml/publish_betting_picks_to_r2.py \
      --bucket "${CLOUDFLARE_R2_BUCKET}" \
      --prefix betting-picks \
      --top "${MODELS_TOP}" \
      --playerAiOutDate "${player_ai_out_date}" \
      --teamAiOutDate "${team_ai_out_date}"
  else
    echo "Skipping R2 publish; missing CLOUDFLARE_R2_* env vars." >&2
  fi
fi
CHAIN
)

status=0
run_with_global_lock_and_timeout "${CHAIN_COMMAND}" || status=$?
finalize_with_healthcheck "${status}" "${HEALTHCHECK_PING_URL_MODELS_TRAIN:-${HEALTHCHECK_PING_URL_MODELS:-${HEALTHCHECK_PING_URL:-}}}"
