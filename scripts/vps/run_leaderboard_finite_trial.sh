#!/usr/bin/env bash
set -euo pipefail

if [[ $# -gt 1 || ( $# -eq 1 && "$1" != reconcile ) ]]; then
  echo "usage: run_leaderboard_finite_trial.sh [reconcile]" >&2
  exit 2
fi
if [[ $# -eq 1 ]]; then set -- --reconcile-quarantine; else set --; fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT}/scripts/vps/common.sh"
verify_runtime_manifest_or_exit "$0"
require_runtime_manifest_entries_or_exit "$0" \
  "scripts/vps/run_leaderboard_finite_trial.sh" \
  "scripts/leaderboard_finite_trial_supervisor.py" \
  "scripts/leaderboard_shadow_worker.py" \
  "scripts/leaderboard_fixture_assembly.py" \
  "scripts/leaderboard_shadow_capture.py" \
  "scripts/export_to_supabase.py" \
  "scripts/postmatch_fixture_detail_delivery.py"

ENV_FILE="${LEADERBOARD_SHADOW_ENV_FILE:-/etc/oddssearch/leaderboard-shadow.env}"
if [[ ! -r "$ENV_FILE" ]]; then
  echo "leaderboard finite trial environment file is unavailable" >&2
  exit 1
fi
set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

: "${LEADERBOARD_FINITE_TRIAL_POLICY_ID:?named finite trial policy is required}"
: "${LEADERBOARD_FINITE_TRIAL_EXPECTED_DSN_SHA256:?reviewed database URL pin is required}"
: "${LEADERBOARD_FINITE_TRIAL_RELEASE_SHA:?reviewed worker release SHA is required}"
: "${LEADERBOARD_FINITE_TRIAL_RUNTIME_MANIFEST_SHA256:?reviewed runtime manifest digest is required}"
: "${LEADERBOARD_FINITE_TRIAL_OPERATOR_NOTE:?reviewed transition note is required}"
if [[ ! "$LEADERBOARD_FINITE_TRIAL_POLICY_ID" =~ ^[a-z0-9-]{8,80}$ \
   || ! "$LEADERBOARD_FINITE_TRIAL_EXPECTED_DSN_SHA256" =~ ^[0-9a-f]{64}$ \
   || ! "$LEADERBOARD_FINITE_TRIAL_RELEASE_SHA" =~ ^[0-9a-f]{40}$ \
   || ! "$LEADERBOARD_FINITE_TRIAL_RUNTIME_MANIFEST_SHA256" =~ ^[0-9a-f]{64}$ \
   || ${#LEADERBOARD_FINITE_TRIAL_OPERATOR_NOTE} -gt 2000 ]]; then
  echo "leaderboard finite trial policy, database or release pin is invalid" >&2
  exit 1
fi
actual_manifest_sha256="$(shasum -a 256 "$ROOT/scripts/vps/runtime_manifest.sha1" | awk '{print $1}')"
if [[ "$actual_manifest_sha256" != "$LEADERBOARD_FINITE_TRIAL_RUNTIME_MANIFEST_SHA256" ]]; then
  echo "leaderboard finite trial runtime manifest differs from reviewed release" >&2
  exit 1
fi
if [[ -e "$ROOT/.git" \
   && "$(git -C "$ROOT" rev-parse HEAD 2>/dev/null)" != "$LEADERBOARD_FINITE_TRIAL_RELEASE_SHA" ]]; then
  echo "leaderboard finite trial checkout differs from the reviewed release" >&2
  exit 1
fi

"$ROOT/scripts/vps/verify_finite_trial_host_cutover.sh"

export LEADERBOARD_SHADOW_RELEASE_SHA="$LEADERBOARD_FINITE_TRIAL_RELEASE_SHA"
exec python3 "$ROOT/scripts/leaderboard_finite_trial_supervisor.py" \
  --production \
  "$@" \
  --worker-path "$ROOT/scripts/leaderboard_shadow_worker.py" \
  --policy-id "$LEADERBOARD_FINITE_TRIAL_POLICY_ID" \
  --worker-lock-file "$TRIAL_WORKER_LOCK" \
  --database-heavy-lock-file "$TRIAL_HEAVY_LOCK" \
  --deadline-seconds 45 --grace-seconds 5 --drain-seconds 10 \
  --recovery-seconds 15
