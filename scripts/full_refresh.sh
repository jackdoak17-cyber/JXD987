#!/usr/bin/env bash
set -euo pipefail

# End-to-end refresh:
# 1) Sync reference + deep history (with stats/lineups)
# 2) Sync odds (Bet365 by default)
# 3) Sync player odds (Bet365 shots/SOT props)
# 4) Recompute forms (5/10/15/20/25/50) and availability (last 5)
# 5) Normalize odds snapshot
# 6) Write compressed SQLite dump to data/jxd_dump.sql.xz
#
# Required env:
#   SPORTMONKS_API_TOKEN
#
# Optional env overrides:
#   LEAGUE_IDS (comma-separated)
#   DAYS_BACK (default 450)
#   DAYS_FORWARD (default 14)
#   HISTORY_LIMIT (cap fixtures; default 4000)
#   ODDS_LIMIT (cap fixtures for odds; default 800)
#   PLAYER_ODDS_LIMIT (cap fixtures for player odds; default 400)
#   PLAYER_ODDS_DAYS_FORWARD (default 7)
#   BOOKMAKER_ID (default 2 = Bet365)

LEAGUE_IDS="${LEAGUE_IDS:-8,9,72,82,181,208,244,271,301,384,387,444,453,462,501,564,567,573,591,600}"
DAYS_BACK="${DAYS_BACK:-450}"
DAYS_FORWARD="${DAYS_FORWARD:-14}"
HISTORY_LIMIT="${HISTORY_LIMIT:-4000}"
ODDS_LIMIT="${ODDS_LIMIT:-800}"
PLAYER_ODDS_LIMIT="${PLAYER_ODDS_LIMIT:-400}"
PLAYER_ODDS_DAYS_FORWARD="${PLAYER_ODDS_DAYS_FORWARD:-7}"
BOOKMAKER_ID="${BOOKMAKER_ID:-2}"

if [[ -z "${SPORTMONKS_API_TOKEN:-}" ]]; then
  echo "SPORTMONKS_API_TOKEN is required" >&2
  exit 1
fi

echo "=== Sync static reference data ==="
python3 -m jxd.cli sync-static

echo "=== Sync league teams for configured leagues ==="
python3 -m jxd.cli sync-league-teams --league-ids "${LEAGUE_IDS}"

echo "=== Sync deep history (back ${DAYS_BACK}d, forward ${DAYS_FORWARD}d) with details ==="
python3 -m jxd.cli sync-history \
  --days-back "${DAYS_BACK}" \
  --days-forward "${DAYS_FORWARD}" \
  --with-details \
  --league-ids "${LEAGUE_IDS}" \
  --limit "${HISTORY_LIMIT}"

echo "=== Sync odds (bookmaker ${BOOKMAKER_ID}) ==="
python3 -m jxd.cli sync-odds \
  --bookmaker-id "${BOOKMAKER_ID}" \
  --league-ids "${LEAGUE_IDS}" \
  --limit "${ODDS_LIMIT}"

echo "=== Sync player odds (bookmaker ${BOOKMAKER_ID}) ==="
python3 -m jxd.cli sync-player-odds \
  --days-forward "${PLAYER_ODDS_DAYS_FORWARD}" \
  --bookmaker-id "${BOOKMAKER_ID}" \
  --league-ids "${LEAGUE_IDS}" \
  --limit "${PLAYER_ODDS_LIMIT}"

echo "=== Compute forms (5/10/15/20/25/50) and availability (5) ==="
python3 -m jxd.cli compute-forms --samples "5,10,15,20,25,50" --availability-sample 5

echo "=== Normalize odds snapshot ==="
python3 -m jxd.cli normalize-odds

echo "=== Write compressed SQLite dump (data/jxd_dump.sql.xz) ==="
mkdir -p data
sqlite3 data/jxd.sqlite ".dump" | xz -9 -T0 > data/jxd_dump.sql.xz
echo "Done."
