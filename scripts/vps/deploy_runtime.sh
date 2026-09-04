#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
RUNTIME_FILE_LIST="${SCRIPT_DIR}/runtime_files.txt"
RUNTIME_MANIFEST="${SCRIPT_DIR}/runtime_manifest.sha1"

TARGET_HOST="${1:-${VPS_HOST:-}}"
TARGET_REPO_ROOT="${2:-${VPS_REPO_ROOT:-}}"
TARGET_USER="${VPS_USER:-root}"
SSH_KEY="${VPS_SSH_KEY:-}"
TARGET_VENV_PATH="${VPS_VENV_PATH:-/opt/odds-sync/JXD987/.venv}"
TARGET_ENV_PATH="${VPS_ENV_PATH:-/opt/odds-sync/JXD987/.env}"

if [[ -z "${TARGET_HOST}" || -z "${TARGET_REPO_ROOT}" ]]; then
  echo "Usage: $(basename "$0") <host> <remote_repo_root>" >&2
  echo "Or set VPS_HOST and VPS_REPO_ROOT (plus VPS_USER / VPS_SSH_KEY)." >&2
  exit 1
fi

if [[ "${TARGET_VENV_PATH}" != /* ]]; then
  echo "VPS_VENV_PATH must be an absolute path: ${TARGET_VENV_PATH}" >&2
  exit 1
fi
if [[ "${TARGET_ENV_PATH}" != /* ]]; then
  echo "VPS_ENV_PATH must be an absolute path: ${TARGET_ENV_PATH}" >&2
  exit 1
fi

if [[ ! -f "${RUNTIME_FILE_LIST}" ]]; then
  echo "Missing runtime file list: ${RUNTIME_FILE_LIST}" >&2
  exit 1
fi

# The stats reconciliation worker is a production dependency. Fail before
# touching the VPS if a stale/alternate checkout supplies an incomplete list.
required_runtime_entries=(
  "scripts/refresh_fixture_delivery.py"
  "scripts/vps/run_p3.sh"
  "scripts/vps/run_postmatch_settlement.sh"
  "scripts/reconcile_stats_provider_queue.py"
  "scripts/vps/run_stats_reconciliation.sh"
)
for required_entry in "${required_runtime_entries[@]}"; do
  if [[ ! -f "${REPO_ROOT}/${required_entry}" ]]; then
    echo "Runtime source missing required production entry: ${required_entry}" >&2
    exit 1
  fi
  if ! grep -Fqx -- "${required_entry}" < <(sed -e '/^[[:space:]]*#/d' -e '/^[[:space:]]*$/d' "${RUNTIME_FILE_LIST}"); then
    echo "Runtime file list missing required production entry: ${required_entry}" >&2
    exit 1
  fi
done

"${SCRIPT_DIR}/update_runtime_manifest.sh"

ssh_cmd=(ssh)
rsync_rsh=(ssh)
if [[ -n "${SSH_KEY}" ]]; then
  ssh_cmd+=(-i "${SSH_KEY}")
  rsync_rsh+=(-i "${SSH_KEY}")
fi
ssh_cmd+=("${TARGET_USER}@${TARGET_HOST}")

files_to_sync=("scripts/vps/runtime_files.txt")
while IFS= read -r relpath || [[ -n "${relpath}" ]]; do
  [[ -n "${relpath}" ]] || continue
  [[ "${relpath}" == \#* ]] && continue
  files_to_sync+=("${relpath}")
done < "${RUNTIME_FILE_LIST}"

(
  cd "${REPO_ROOT}"
  rsync -avR -e "$(printf '%q ' "${rsync_rsh[@]}")" "${files_to_sync[@]}" "${TARGET_USER}@${TARGET_HOST}:${TARGET_REPO_ROOT}/"
)

# Publish the manifest last. During a runtime update, wrappers either see the
# previous complete release or fail closed on a manifest mismatch; they never
# execute a partially copied release as valid.
(
  cd "${REPO_ROOT}"
  rsync -avR -e "$(printf '%q ' "${rsync_rsh[@]}")" \
    scripts/vps/runtime_manifest.sha1 "${TARGET_USER}@${TARGET_HOST}:${TARGET_REPO_ROOT}/"
)

"${ssh_cmd[@]}" "set -e; venv_target='${TARGET_REPO_ROOT}/.venv'; venv_shared='${TARGET_VENV_PATH}'; if [[ -e \"\${venv_target}\" || -L \"\${venv_target}\" ]]; then if [[ ! -L \"\${venv_target}\" || \"\$(readlink \"\${venv_target}\")\" != \"\${venv_shared}\" ]]; then echo \"Existing .venv does not point to \${venv_shared}: \${venv_target}\" >&2; exit 1; fi; else test -d \"\${venv_shared}\"; ln -s \"\${venv_shared}\" \"\${venv_target}\"; fi; test -f \"\${venv_target}/bin/activate\"; env_target='${TARGET_REPO_ROOT}/.env'; env_shared='${TARGET_ENV_PATH}'; if [[ -e \"\${env_target}\" || -L \"\${env_target}\" ]]; then if [[ ! -L \"\${env_target}\" || \"\$(readlink \"\${env_target}\")\" != \"\${env_shared}\" ]]; then echo \"Existing .env does not point to \${env_shared}: \${env_target}\" >&2; exit 1; fi; else test -f \"\${env_shared}\"; ln -s \"\${env_shared}\" \"\${env_target}\"; fi; test -f \"\${env_target}\"; cd '${TARGET_REPO_ROOT}' && shasum -c scripts/vps/runtime_manifest.sha1 && grep -Fqx -- 'scripts/refresh_fixture_delivery.py' < <(awk '{print \$2}' scripts/vps/runtime_manifest.sha1) && grep -Fqx -- 'scripts/vps/run_p3.sh' < <(awk '{print \$2}' scripts/vps/runtime_manifest.sha1) && grep -Fqx -- 'scripts/vps/run_postmatch_settlement.sh' < <(awk '{print \$2}' scripts/vps/runtime_manifest.sha1) && grep -Fqx -- 'scripts/reconcile_stats_provider_queue.py' < <(awk '{print \$2}' scripts/vps/runtime_manifest.sha1) && grep -Fqx -- 'scripts/vps/run_stats_reconciliation.sh' < <(awk '{print \$2}' scripts/vps/runtime_manifest.sha1)"
echo "Runtime deployed and verified on ${TARGET_USER}@${TARGET_HOST}:${TARGET_REPO_ROOT}"
