#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
RUNTIME_FILE_LIST="${SCRIPT_DIR}/runtime_files.txt"
RUNTIME_MANIFEST="${SCRIPT_DIR}/runtime_manifest.sha1"

TARGET_HOST="${1:-${VPS_HOST:-}}"
TARGET_REPO_ROOT="${2:-${VPS_REPO_ROOT:-/opt/odds-sync/JXD987}}"
TARGET_USER="${VPS_USER:-root}"
SSH_KEY="${VPS_SSH_KEY:-}"

if [[ -z "${TARGET_HOST}" ]]; then
  echo "Usage: $(basename "$0") <host> [remote_repo_root]" >&2
  echo "Or set VPS_HOST / VPS_REPO_ROOT / VPS_USER / VPS_SSH_KEY." >&2
  exit 1
fi

if [[ ! -f "${RUNTIME_FILE_LIST}" ]]; then
  echo "Missing runtime file list: ${RUNTIME_FILE_LIST}" >&2
  exit 1
fi

"${SCRIPT_DIR}/update_runtime_manifest.sh"

ssh_cmd=(ssh)
rsync_rsh=(ssh)
if [[ -n "${SSH_KEY}" ]]; then
  ssh_cmd+=(-i "${SSH_KEY}")
  rsync_rsh+=(-i "${SSH_KEY}")
fi
ssh_cmd+=("${TARGET_USER}@${TARGET_HOST}")

files_to_sync=("scripts/vps/runtime_manifest.sha1" "scripts/vps/runtime_files.txt")
while IFS= read -r relpath || [[ -n "${relpath}" ]]; do
  [[ -n "${relpath}" ]] || continue
  [[ "${relpath}" == \#* ]] && continue
  files_to_sync+=("${relpath}")
done < "${RUNTIME_FILE_LIST}"

(
  cd "${REPO_ROOT}"
  rsync -avR -e "$(printf '%q ' "${rsync_rsh[@]}")" "${files_to_sync[@]}" "${TARGET_USER}@${TARGET_HOST}:${TARGET_REPO_ROOT}/"
)

"${ssh_cmd[@]}" "cd '${TARGET_REPO_ROOT}' && shasum -c scripts/vps/runtime_manifest.sha1"
echo "Runtime deployed and verified on ${TARGET_USER}@${TARGET_HOST}:${TARGET_REPO_ROOT}"
