# Fixture-core #80 release and rollback record — 8 November 2026

Jack separately approved this no-added-cost production cutover. It was
installed at 14:42 UTC on the existing VPS. His earlier approval covered only
the one-time publication at 14:13 UTC.

## Exact candidate

| Item | Value |
| --- | --- |
| Untouched production source and rollback | `/opt/odds-sync/JXD987-fixture-core-900d1ca-release` |
| Source runtime manifest SHA-1 | `7607d31bedf8` |
| Installed immutable target | `/opt/odds-sync/JXD987-fixture-core-80-aae97d-release` |
| Candidate runtime manifest SHA-1 | `aae97d98942b5c17a1c1094f34e86bde82dada31` |
| Prepared local archive | `fixture-core-80-aae97d98942b.tar.gz` |
| Archive SHA-256 | `3f07af9651127761da93295740d0e0f58cde68dd12ff55d2830d816637250a89` |
| Reviewed source PR | #80 at `22e0532` |

The archive starts with an exact read-only copy of the active fixture-core
runtime, excluding credential and virtual-environment links. It overlays
#80's `refresh_fixture_delivery.py`, `run_fixture_delivery_publisher.sh` and
`run_p3_fixture_core.sh`. It applies only #80's lease-handoff hunks to that
release's `common.sh`, retaining the active release's other job-evidence
behavior. Its 40-entry runtime manifest was regenerated and verified after
composition. The archive contains no `.env`, `.venv`, SQLite data or secrets.

## Local verdict

- The untouched source and freshly extracted archive both passed their
  complete runtime-manifest checks.
- Changed shell entrypoints passed `bash -n`; the Python publisher passed
  compilation.
- The exact composed candidate passed 48 targeted fixture publisher,
  release, source and wrapper tests. The Linux `flock`/`timeout` handoff test
  passed with the candidate `common.sh`, including zero-retry skip recording.
- The broader current-repository fixture suite was also attempted against
  this older production runtime. It reported five unrelated source-version
  expectation failures (odds quote recorder and later settlement/DB-heavy
  additions); that is **not** a full-suite pass for this composed candidate.

## Executed cutover, 14:42 UTC

1. Rechecked the active source manifest (`7607d31bedf8`), archive SHA-256,
   exact existing cron entries, absent target path and no running fixture-core
   or publisher invocation.
2. Installed the archive at the immutable target, linked `.env` and `.venv`
   to the same targets as the old release, set root ownership and verified the
   40-entry manifest (`aae97d98942b`) and shell syntax on the VPS.
3. Backed up the root crontab to
   `/root/crontab-before-fixture-core80-20261108T144247Z.txt`. Atomically
   replaced only the core entry `4 */6` with `17 */6` on the new release and
   the hourly publisher entry `11 *` with its new-release path. Read back both
   exact entries. Settlement and all other jobs remained on their previous
   paths and schedules.
4. A read-only source check at 14:43 UTC selected genuine fallback evidence
   from 07:56:25 UTC, age 24,419 seconds, within the existing 540-minute
   limit. The source timestamp and scope overrides were unset. We ran the new
   publisher once. It verified its runtime manifest and returned a guarded
   `recent_publication_satisfies_guard` skip because the 14:13 publication was
   only 1,808 seconds old against the existing 3,600-second minimum. We did
   not override that safety interval. The database's default release resolver
   remained `current` for release
   `bda3d455-74bf-455b-aee7-6db158843a77` at 14:44 UTC.

The 15:11 UTC scheduled publisher ran from the new release, verified its
manifest and skipped under the same guard: 3,463 seconds had elapsed, short
of the 3,600-second minimum. The 16:11 run has a narrow window before the
current 16:13:24 pin expires and before fallback evidence becomes too old at
16:56:25. The next regular primary source attempt is 18:17. A genuinely fresh
source is therefore still needed before 16:56; the GitHub fallback schedule
has been irregular and is not a guaranteed bridge. Do not infer sustained
Fixtures availability from this cutover alone.

Minute `17` avoids the observed direct collision with website odds delivery
at minute `4`. P1 also starts at `17` and has recently held the writer lock
for seconds; the new core job retries after that. This is a mitigation, not
guaranteed priority. The stronger reservation in stacked draft #81 requires
a separate coordinated five-checkout rollout.

## Rollback

This section applies only to rolling back the installed **#80 cutover itself**.
Rollback of stacked [draft #81](https://github.com/jackdoak17-cyber/JXD987/pull/81)
preserves #80's minute `17` schedule and uses that draft's separate two-stage
drain procedure.

Restore the exact source-release fixture-core entry at minute `4` and source
publisher entry at minute `11`; verify both. Keep the old release untouched.
Do not delete a target while an invocation is using it. An already published
fixture release remains subject to its recorded expiry; rollback does not
make stale source evidence fresh. No disk, compute, spend-cap, service or job
frequency change is part of this candidate.
