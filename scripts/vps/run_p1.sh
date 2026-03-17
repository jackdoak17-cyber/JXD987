#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=./common.sh
source "${SCRIPT_DIR}/common.sh"

export REPO_ROOT
export LEAGUES="${LEAGUE_IDS:-$(default_league_csv)}"
export DAYS_FORWARD="${ODDS_DAYS_FORWARD:-14}"
export ODDS_BOOKMAKERS="${ODDS_BOOKMAKERS:-Bet365,Kambi,Paddy Power}"

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

python scripts/sync_odds.py \
  --leagues "${LEAGUES}" \
  --days-forward "${DAYS_FORWARD}" \
  --priority p1 \
  --bookmakers "${ODDS_BOOKMAKERS}" \
  --report-out "/tmp/odds_sync_report_p1.json" \
  --unmatched-out "/tmp/unmatched_players_p1.json"
CHAIN
)

status=0
run_with_global_lock_and_timeout "${CHAIN_COMMAND}" || status=$?
finalize_with_healthcheck "${status}" "${HEALTHCHECK_PING_URL_P1:-${HEALTHCHECK_PING_URL:-}}"
