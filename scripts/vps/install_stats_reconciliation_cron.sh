#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="${1:-/opt/odds-sync/JXD987}"
LOG_PATH="${2:-/var/log/stats-reconciliation-supervisor.log}"
ENTRY="*/5 * * * * cd ${REPO_ROOT} && ${REPO_ROOT}/scripts/vps/run_stats_reconciliation.sh >> ${LOG_PATH} 2>&1"
MARKER="# OddsSearch stats reconciliation supervisor"

current="$(crontab -l 2>/dev/null || true)"
if printf '%s\n' "${current}" | grep -Fqx "${ENTRY}"; then
  printf '%s\n' "Stats reconciliation cron already installed."
  exit 0
fi

{
  if [[ -n "${current}" ]]; then
    printf '%s\n' "${current}"
  fi
  printf '%s\n%s\n' "${MARKER}" "${ENTRY}"
} | crontab -
printf '%s\n' "Installed stats reconciliation cron: ${ENTRY}"
