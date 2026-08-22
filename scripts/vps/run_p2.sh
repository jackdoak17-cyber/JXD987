#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=./common.sh
source "${SCRIPT_DIR}/common.sh"
verify_runtime_manifest_or_exit "$0"
require_runtime_manifest_entries_or_exit "$0" \
  "config/league_ids.txt" \
  "config/odds_api_leagues.json" \
  "config/odds_api_sync_excluded_leagues.json" \
  "jxd/__init__.py" \
  "jxd/db.py" \
  "jxd/models.py" \
  "jxd/odds_api_client.py" \
  "jxd/sportmonks_client.py" \
  "jxd/sync.py" \
  "scripts/sync_odds.py" \
  "scripts/sync_confirmed_lineups.py"

export REPO_ROOT
export STATS_LEAGUES="${FIXTURE_LEAGUE_IDS:-${STATS_LEAGUE_IDS:-$(supported_league_csv)}}"
validate_supported_leagues "${STATS_LEAGUES}"
export ODDS_LEAGUES="${ODDS_LEAGUE_IDS:-$(odds_league_csv)}"
export DAYS_FORWARD="${ODDS_DAYS_FORWARD:-14}"
export ODDS_BOOKMAKERS="${ODDS_BOOKMAKERS:-Bet365,Paddy Power}"
export LINEUP_SYNC_HOURS_BACK="${LINEUP_SYNC_HOURS_BACK:-2}"
export LINEUP_SYNC_HOURS_FORWARD="${LINEUP_SYNC_HOURS_FORWARD:-3}"
export LINEUP_SYNC_LIMIT="${LINEUP_SYNC_LIMIT:-40}"

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
  --leagues "${ODDS_LEAGUES}" \
  --days-forward "${DAYS_FORWARD}" \
  --priority p2 \
  --bookmakers "${ODDS_BOOKMAKERS}" \
  --report-out "/tmp/odds_sync_report_p2.json" \
  --unmatched-out "/tmp/unmatched_players_p2.json"

# Best-effort confirmed-lineup refresh for imminent fixtures.
if [[ -n "${SPORTMONKS_API_TOKEN:-}" && -n "${SUPABASE_URL:-}" && -n "${SUPABASE_SERVICE_ROLE_KEY:-}" ]]; then
  if ! python scripts/sync_confirmed_lineups.py \
    --leagues "${STATS_LEAGUES}" \
    --hours-back "${LINEUP_SYNC_HOURS_BACK}" \
    --hours-forward "${LINEUP_SYNC_HOURS_FORWARD}" \
    --limit "${LINEUP_SYNC_LIMIT}"; then
    echo "Confirmed lineup refresh failed; continuing odds pipeline" >&2
  fi
else
  echo "Skipping confirmed lineup refresh; missing SportMonks or Supabase REST env" >&2
fi
CHAIN
)

status=0
run_with_global_lock_and_timeout "${CHAIN_COMMAND}" || status=$?
finalize_with_healthcheck "${status}" "${HEALTHCHECK_PING_URL_P2:-${HEALTHCHECK_PING_URL:-}}"
