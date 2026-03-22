#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
RUNTIME_FILE_LIST="${SCRIPT_DIR}/runtime_files.txt"
RUNTIME_MANIFEST="${SCRIPT_DIR}/runtime_manifest.sha1"

if [[ ! -f "${RUNTIME_FILE_LIST}" ]]; then
  echo "Missing runtime file list: ${RUNTIME_FILE_LIST}" >&2
  exit 1
fi

hash_file() {
  local path="$1"
  if command -v shasum >/dev/null 2>&1; then
    shasum "${path}" | awk '{print $1}'
    return 0
  fi
  if command -v sha1sum >/dev/null 2>&1; then
    sha1sum "${path}" | awk '{print $1}'
    return 0
  fi
  echo "Requires shasum or sha1sum" >&2
  exit 1
}

tmp_manifest="$(mktemp)"
trap 'rm -f "${tmp_manifest}"' EXIT

while IFS= read -r relpath || [[ -n "${relpath}" ]]; do
  [[ -n "${relpath}" ]] || continue
  [[ "${relpath}" == \#* ]] && continue
  abs="${REPO_ROOT}/${relpath}"
  if [[ ! -f "${abs}" ]]; then
    echo "Missing runtime file: ${relpath}" >&2
    exit 1
  fi
  printf '%s  %s\n' "$(hash_file "${abs}")" "${relpath}" >> "${tmp_manifest}"
done < "${RUNTIME_FILE_LIST}"

mv "${tmp_manifest}" "${RUNTIME_MANIFEST}"
echo "Updated ${RUNTIME_MANIFEST}"
