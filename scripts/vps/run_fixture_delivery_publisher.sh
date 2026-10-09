#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=./common.sh
source "${SCRIPT_DIR}/common.sh"
verify_runtime_manifest_or_exit "$0"
require_runtime_manifest_entries_or_exit "$0" \
  "config/fixture_core_contract.json" \
  "config/league_ids.txt" \
  "config/odds_api_sync_excluded_leagues.json" \
  "scripts/fixture_core_contract.py" \
  "scripts/fixture_delivery_dirty_state.py" \
  "scripts/refresh_fixture_delivery.py"

if [[ -f "${REPO_ROOT}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${REPO_ROOT}/.env"
  set +a
fi

export REPO_ROOT
export FIXTURE_CORE_CONTRACT_PATH="${FIXTURE_CORE_CONTRACT_PATH:-${REPO_ROOT}/config/fixture_core_contract.json}"
contract_value() {
  python3 "${REPO_ROOT}/scripts/fixture_core_contract.py" \
    --contract "${FIXTURE_CORE_CONTRACT_PATH}" \
    --field "$1"
}
export STATS_LEAGUES="${FIXTURE_LEAGUE_IDS:-${STATS_LEAGUES:-$(supported_league_csv)}}"
validate_supported_leagues "${STATS_LEAGUES}"
export FIXTURE_DELIVERY_DAYS_BACK="${FIXTURE_DELIVERY_DAYS_BACK:-$(contract_value history_window_days)}"
export FIXTURE_DELIVERY_DAYS_FORWARD="${FIXTURE_DELIVERY_DAYS_FORWARD:-$(contract_value delivery_window_days)}"
export FIXTURE_DELIVERY_DIRTY_STATE_PATH="${FIXTURE_DELIVERY_DIRTY_STATE_PATH:-/var/lib/oddssearch/fixture-delivery/dirty-state.json}"
export FIXTURE_DELIVERY_PUBLISH_MIN_INTERVAL_SECONDS="${FIXTURE_DELIVERY_PUBLISH_MIN_INTERVAL_SECONDS:-3600}"
export FIXTURE_DELIVERY_PUBLISHER_MAX_RUNTIME_SECONDS="${FIXTURE_DELIVERY_PUBLISHER_MAX_RUNTIME_SECONDS:-1800}"
export PIPELINE_EVIDENCE_FILE="${PIPELINE_EVIDENCE_FILE:-/tmp/fixture_delivery_publisher_report.json}"

cd "${REPO_ROOT}"
source .venv/bin/activate
export PYTHONPATH="${REPO_ROOT}"

publisher_status=0
RUN_STARTED_AT="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
RUN_STARTED_EPOCH="$(date -u +%s)"
rm -f "${PIPELINE_EVIDENCE_FILE}"

if ! python scripts/fixture_delivery_dirty_state.py status > /tmp/fixture_delivery_dirty_status.json; then
  python3 - <<'PY' "${PIPELINE_EVIDENCE_FILE}" "${FIXTURE_DELIVERY_DIRTY_STATE_PATH}"
import json, sys
with open(sys.argv[1], "w", encoding="utf-8") as fh:
    json.dump({"status": "skipped", "reason": "nothing_dirty", "dirty_state_path": sys.argv[2]}, fh)
PY
  publisher_status=2
else
  python scripts/fixture_delivery_dirty_state.py attempt >/tmp/fixture_delivery_dirty_attempt.json
  dirty_revision="$(python3 - <<'PY'
import json
with open('/tmp/fixture_delivery_dirty_attempt.json', encoding='utf-8') as handle:
    payload = json.load(handle)
print(int(payload['state']['revision']))
PY
)"
  if timeout --signal=TERM --kill-after=5s "${FIXTURE_DELIVERY_PUBLISHER_MAX_RUNTIME_SECONDS}" \
    python scripts/refresh_fixture_delivery.py \
      --start-date "$(TZ=Europe/London date -d "-${FIXTURE_DELIVERY_DAYS_BACK} days" +%F)" \
      --end-date "$(TZ=Europe/London date -d "+${FIXTURE_DELIVERY_DAYS_FORWARD} days" +%F)" \
      --leagues "${STATS_LEAGUES}" \
      --report-out "${PIPELINE_EVIDENCE_FILE}" \
      --skip-if-publication-active \
      --min-publish-interval-seconds "${FIXTURE_DELIVERY_PUBLISH_MIN_INTERVAL_SECONDS}"; then
    python scripts/fixture_delivery_dirty_state.py clear \
      --expected-revision "${dirty_revision}" \
      >/tmp/fixture_delivery_dirty_clear.json
    publisher_status=0
  else
    publisher_status=$?
    if [[ "${publisher_status}" -eq 2 ]]; then
      # Guarded skip: no duplicate build, recent enough publication, or another publisher active.
      true
    else
      python scripts/fixture_delivery_dirty_state.py failure --error "refresh_fixture_delivery exit status ${publisher_status}" >/tmp/fixture_delivery_dirty_failure.json || true
    fi
  fi
fi

RUN_FINISHED_AT="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
RUN_FINISHED_EPOCH="$(date -u +%s)"
record_pipeline_job_run \
  "run_fixture_delivery_publisher" \
  "Fixture delivery guarded publisher" \
  "${publisher_status}" \
  "${RUN_STARTED_AT}" \
  "${RUN_FINISHED_AT}" \
  "$(((RUN_FINISHED_EPOCH - RUN_STARTED_EPOCH) * 1000))"

if [[ "${publisher_status}" -eq 2 ]]; then
  exit 0
fi
exit "${publisher_status}"
