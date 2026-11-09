#!/usr/bin/env bash
set -euo pipefail

if [[ $# -gt 1 || ( $# -eq 1 && "$1" != reconcile ) ]]; then
  echo "usage: run_leaderboard_rolling_worker.sh [reconcile]" >&2
  exit 2
fi
if [[ $# -eq 1 ]]; then set -- --reconcile-quarantine; else set --; fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/scripts/vps/common.sh"
verify_runtime_manifest_or_exit "$0"
require_runtime_manifest_entries_or_exit "$0" \
  "scripts/vps/run_leaderboard_rolling_worker.sh" \
  "scripts/vps/verify_finite_trial_host_cutover.sh" \
  "scripts/leaderboard_finite_trial_supervisor.py" \
  "scripts/leaderboard_shadow_worker.py" \
  "scripts/leaderboard_shadow_capture.py" \
  "scripts/export_to_supabase.py" \
  "scripts/postmatch_fixture_detail_delivery.py"

ENV_FILE="${LEADERBOARD_SHADOW_ENV_FILE:-/etc/oddssearch/leaderboard-shadow.env}"
if [[ ! -r "$ENV_FILE" ]]; then
  echo "leaderboard rolling environment file is unavailable" >&2
  exit 1
fi
set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a
require_default_trial_lock_paths_or_exit

: "${LEADERBOARD_ROLLING_POLICY_ID:?reviewed rolling policy is required}"
: "${LEADERBOARD_ROLLING_EXPECTED_DSN_SHA256:?reviewed database URL pin is required}"
: "${LEADERBOARD_ROLLING_RELEASE_SHA:?reviewed worker release SHA is required}"
: "${LEADERBOARD_ROLLING_RUNTIME_MANIFEST_SHA256:?reviewed manifest digest is required}"
: "${LEADERBOARD_ROLLING_FIXTURE_LIMIT:?reviewed fixture limit is required}"
: "${LEADERBOARD_ROLLING_CAPTURE_LIMIT:?reviewed capture limit is required}"
: "${LEADERBOARD_ROLLING_COHORT_LIMIT:?reviewed cohort limit is required}"
: "${LEADERBOARD_ROLLING_JOB_LIMIT:?reviewed job limit is required}"
if [[ ! "$LEADERBOARD_ROLLING_POLICY_ID" =~ ^[a-z0-9-]{8,80}$ \
   || ! "$LEADERBOARD_ROLLING_EXPECTED_DSN_SHA256" =~ ^[0-9a-f]{64}$ \
   || ! "$LEADERBOARD_ROLLING_RELEASE_SHA" =~ ^[0-9a-f]{40}$ \
   || ! "$LEADERBOARD_ROLLING_RUNTIME_MANIFEST_SHA256" =~ ^[0-9a-f]{64}$ \
   || ! "$LEADERBOARD_ROLLING_FIXTURE_LIMIT" =~ ^[0-9]{1,3}$ \
   || ! "$LEADERBOARD_ROLLING_CAPTURE_LIMIT" =~ ^[0-9]{1,3}$ \
   || ! "$LEADERBOARD_ROLLING_COHORT_LIMIT" =~ ^[0-9]$ \
   || ! "$LEADERBOARD_ROLLING_JOB_LIMIT" =~ ^[0-9]{1,3}$ ]]; then
  echo "leaderboard rolling policy, release or numeric pin is invalid" >&2
  exit 1
fi
actual_manifest_sha256="$(shasum -a 256 "$ROOT/scripts/vps/runtime_manifest.sha1" | awk '{print $1}')"
if [[ "$actual_manifest_sha256" != "$LEADERBOARD_ROLLING_RUNTIME_MANIFEST_SHA256" ]]; then
  echo "leaderboard rolling manifest differs from reviewed release" >&2
  exit 1
fi
if [[ -e "$ROOT/.git" \
   && "$(git -C "$ROOT" rev-parse HEAD 2>/dev/null)" != "$LEADERBOARD_ROLLING_RELEASE_SHA" ]]; then
  echo "leaderboard rolling checkout differs from reviewed release" >&2
  exit 1
fi

"$ROOT/scripts/vps/verify_finite_trial_host_cutover.sh"

# The worker's production identity check is shared with the finite supervisor.
export LEADERBOARD_SHADOW_RELEASE_SHA="$LEADERBOARD_ROLLING_RELEASE_SHA"
export LEADERBOARD_FINITE_TRIAL_EXPECTED_DSN_SHA256="$LEADERBOARD_ROLLING_EXPECTED_DSN_SHA256"
export LEADERBOARD_FINITE_TRIAL_RELEASE_SHA="$LEADERBOARD_ROLLING_RELEASE_SHA"
exec python3 "$ROOT/scripts/leaderboard_finite_trial_supervisor.py" \
  --production --rolling "$@" \
  --worker-path "$ROOT/scripts/leaderboard_shadow_worker.py" \
  --policy-id "$LEADERBOARD_ROLLING_POLICY_ID" \
  --worker-lock-file "${LEADERBOARD_SHADOW_LOCK_FILE:-/var/lock/oddssearch-leaderboard-shadow.lock}" \
  --database-heavy-lock-file "${ODDSSEARCH_DB_HEAVY_LOCK_FILE:-/var/lock/oddssearch-db-heavy.lock}" \
  --fixture-limit "$LEADERBOARD_ROLLING_FIXTURE_LIMIT" \
  --capture-limit "$LEADERBOARD_ROLLING_CAPTURE_LIMIT" \
  --cohort-limit "$LEADERBOARD_ROLLING_COHORT_LIMIT" \
  --job-limit "$LEADERBOARD_ROLLING_JOB_LIMIT" \
  --max-seconds 30 --deadline-seconds 45 --grace-seconds 5 \
  --drain-seconds 10 --recovery-seconds 15
