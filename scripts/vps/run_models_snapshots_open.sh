#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=./common.sh
source "${SCRIPT_DIR}/common.sh"
verify_runtime_manifest_or_exit "$0"
require_runtime_manifest_entries_or_exit "$0" \
  "scripts/vps/common.sh" \
  "scripts/vps/run_models_snapshots_open.sh"

export MODELS_REPO_ROOT="${MODELS_REPO_ROOT:-/opt/odds-sync/Models}"
export MODELS_ENV_PATH="${MODELS_ENV_PATH:-${MODELS_REPO_ROOT}/.env}"
export MODELS_SNAPSHOT_TIMEOUT_SECONDS="${MODELS_SNAPSHOT_TIMEOUT_SECONDS:-1200}"
export ODDS_SYNC_P3_MAX_DURATION_SECONDS="${MODELS_SNAPSHOT_TIMEOUT_SECONDS}"

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

if [[ -f ./.env ]]; then
  set -a
  # shellcheck disable=SC1091
  source ./.env
  set +a
fi

python3 ml/r2_odds_snapshot_logger.py \
  --env "${MODELS_ENV_PATH}" \
  --snapshotType first_eval \
  --days 7 \
  --limit 60 \
  --playersLimit 25 \
  --minHoursToKickoff 0 \
  --maxHoursToKickoff 168
CHAIN
)

status=0
run_with_global_lock_and_timeout "${CHAIN_COMMAND}" || status=$?
finalize_with_healthcheck "${status}" "${HEALTHCHECK_PING_URL_MODELS_SNAPSHOT_OPEN:-${HEALTHCHECK_PING_URL_MODELS:-${HEALTHCHECK_PING_URL:-}}}"
