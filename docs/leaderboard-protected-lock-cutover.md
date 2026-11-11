# Leaderboard protected lock cutover

The composed release preserves the live statistics reconciler and exporter from JXD main `42bc790`. Its schedules share two existing regular files under `/var/lib/oddssearch/leaderboard-locks`. The source supervisor continues to hold `/var/lock/stats-reconciliation-supervisor.lock`. The independent SQLite matcher/spool lock and its priority rules still apply.

## Stage the release

1. Stage a new immutable release with the current statistics archive's shared `.env`, virtual environment and data links. Preserve the old source and leaderboard archives and their protected environment. Run the existing manifest and odds-delivery verification against the shared matcher database before changing schedules. Keep that database at its existing absolute path.
2. Provision the root-owned mode-0700 protected directory on the VPS. Create `oddssearch-leaderboard-shadow.lock` and `oddssearch-db-heavy.lock` once as root-owned mode-0600 regular files. Record each absolute path, canonical directory, device and inode under the keys `worker` and `heavy` in root-owned mode-0600 `pins.json` in that directory. Preserve those files on retries and refuse changed identities. The wrappers and their acquired descriptors check the same pins.
3. Save the exact full root crontab and its digest. Inspect other cron users, system timers, manual worker commands and actual process and file descriptor holders for both legacy locks and the source supervisor lock. The current root cron has two affected entries. Preserve all other schedule lines, paths, environments and launch order.

## Switch both scheduled entrants

1. At the source handoff window, freeze both affected lines in one crontab update. Retain the frozen crontab and digest. Let any in-flight source batch finish. Stop its old supervisor during handoff only after matching PID, process start, release path and descendants. Leave the schedule frozen if any old launcher, child, lock or quarantine remains.
2. From the staged release run `scripts/vps/verify_finite_trial_host_cutover.sh prepare --install FROZEN_OLD_SHA ACTIVE_NEW_SHA ACTIVE_NEW_FILE`. Keep manual entrants stopped through the switch. The command checks the archive, protected descriptors, environment, and both quarantine namespaces. It keeps both lock pairs and the source-supervisor lock held through process scans, the whole-crontab write, and exact readback. A standalone `prepare` is a dry preflight that releases the locks when it exits.
3. Run `scripts/vps/verify_finite_trial_host_cutover.sh active` and check the first bounded source batch, its delivery and the newly attributed importer samples. The new manifest changes the source release ID. Establish the comparable pinned baseline using genuine new-release samples and the existing debt-preserving procedure before a canary.

If installation stops with a transition hold, keep it in place and inspect the actual crontab digest. Use `prepare --recover FROZEN_OLD_SHA ACTIVE_NEW_SHA FROZEN_OLD_FILE` for a recognized switch state. Recovery restores the exact frozen schedule. Evaluate any return to the legacy schedule as a separate reversal.

## Reverse the source-only switch

Freeze the same two lines together, let the new source batch drain and verify the process and lock-holder inventory again. Confirm the database baseline still points to the old source release, there is no unresolved reservation or prepared interval, and the expected receipts and accounting remain durable. If the new baseline was already pinned, first review the required database transition. Restore the exact original schedule with the staged verifier's `revert --install FROZEN_NEW_SHA ORIGINAL_SHA ORIGINAL_FILE` while both quarantine namespaces are resolved. Verify the original protected environment, both old schedules, the source delivery and the independent matcher. Preserve the new archive and pins for inspection.

If a candidate reservation or either quarantine marker still exists, keep the compatible archive and lock identities available. Resolve the liability using the reviewed restricted operator path before reversal. A heavy quarantine defers both database-heavy entrants, while a worker-only marker blocks another worker launch. The empty SQL inverse is available only before activation and durable candidate evidence.
