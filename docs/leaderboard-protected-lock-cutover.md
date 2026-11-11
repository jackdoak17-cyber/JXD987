# Leaderboard protected lock cutover

The composed release preserves the live statistics reconciler and exporter from JXD main `42bc790`. Its schedules share two existing regular files under `/var/lib/oddssearch/leaderboard-locks`. The source supervisor continues to hold `/var/lock/stats-reconciliation-supervisor.lock`. The independent SQLite matcher/spool lock and its priority rules still apply.

## Stage the release

1. Stage a new immutable release with the current statistics archive's shared `.env`, virtual environment and data links. Preserve the old source and leaderboard archives and their protected environment. Run the existing manifest and odds-delivery verification against the shared matcher database before changing schedules. Keep that database at its existing absolute path.
2. Provision the root-owned mode-0700 protected directory on the VPS. Create `oddssearch-leaderboard-shadow.lock` and `oddssearch-db-heavy.lock` once as root-owned mode-0600 regular files. Record each absolute path, canonical directory, device and inode under the keys `worker` and `heavy` in root-owned mode-0600 `pins.json` in that directory. Preserve those files on retries and refuse changed identities. The wrappers and their acquired descriptors check the same pins.
3. Save the exact full root crontab and its digest. Inspect other cron users, system timers, manual worker commands and actual process and file descriptor holders for both legacy locks and the source supervisor lock. The current root cron has two affected entries. Preserve all other schedule lines, paths, environments and launch order.

## Switch both scheduled entrants

1. At the source handoff window, freeze both affected lines in one crontab update. Retain the frozen crontab and digest. Let any in-flight source batch finish. Stop its old supervisor during handoff only after matching PID, process start, release path and descendants. Leave the schedule frozen if any old launcher, child, lock or quarantine remains.
2. From the staged release run `scripts/vps/verify_finite_trial_host_cutover.sh prepare` while both lines are frozen. It checks the archive, protected descriptors and environment, holds the old and new lock pairs and the source supervisor lock during two process scans, and refuses either quarantine namespace. Recheck that the crontab digest still matches the frozen snapshot immediately before installing the reviewed new crontab. Keep manual entrants stopped through the switch.
3. Install both new paths in one crontab update using the original schedule expressions and log destinations. Run `scripts/vps/verify_finite_trial_host_cutover.sh active` and check the first bounded source batch, its visible delivery and the newly attributed importer samples. The new manifest changes the source release ID. Establish the comparable pinned baseline using the existing debt-preserving procedure before a canary. No old-release sample may stand in for the new manifest.

## Reverse the source-only switch

Freeze the same two lines together, let the new source batch drain and verify the process and lock-holder inventory again. Run the staged verifier with `revert`. It requires both schedules frozen and both quarantine namespaces resolved. Confirm the database has no unresolved reservation or prepared interval and the expected receipts and accounting remain durable. Restore the exact saved crontab and protected environment only after those checks. Verify that both original schedules, the source delivery and the independent matcher resume. Preserve the new archive and pins for inspection.

If a candidate reservation or either quarantine marker still exists, keep the compatible archive and lock identities available. Resolve the liability using the reviewed restricted operator path before reversal. A heavy quarantine defers both database-heavy entrants, while a worker-only marker blocks another worker launch. The empty SQL inverse is available only before activation and durable candidate evidence.
