# Fixture-core #80 prepared release candidate — 8 November 2026

This is a staged, **unapplied** existing-resource recovery option. Jack's
approval of the separate one-time publication did not approve this cutover.

## Exact candidate

| Item | Value |
| --- | --- |
| Untouched production source and rollback | `/opt/odds-sync/JXD987-fixture-core-900d1ca-release` |
| Source runtime manifest SHA-1 | `7607d31bedf8` |
| Proposed immutable target | `/opt/odds-sync/JXD987-fixture-core-80-aae97d-release` |
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

## Forward cutover, only after explicit owner approval

1. Recheck the active source manifest, exact archive hash, production cron
   entries and effective environment. Verify the target path is absent.
2. Unpack the archive to the new immutable target. Recreate `.env` and `.venv`
   links to the **same targets** used by the source release. Match the source
   ownership/permissions and verify the staged manifest on the VPS.
3. Replace only the six-hour fixture-core cron entry with the target at minute
   `17`, and the hourly fixture publisher entry with the target at its
   existing minute `11`. Leave settlement on the old, untouched source
   release. Keep all other writer jobs and their schedules unchanged.
4. Check both exact cron entries and runtime IDs. Observe the next primary
   source attempt and hourly publisher, then check the verified source age,
   default resolver, pin expiry and authenticated fixture schedule/details.

Minute `17` avoids the observed direct collision with website odds delivery
at minute `4`. P1 also starts at `17` and has recently held the writer lock
for seconds; the new core job retries after that. This is a mitigation, not
guaranteed priority. The stronger reservation in stacked draft #81 requires
a separate coordinated five-checkout rollout.

## Rollback

Restore the exact source-release fixture-core entry at minute `4` and source
publisher entry at minute `11`; verify both. Keep the old release untouched.
Do not delete a target while an invocation is using it. An already published
fixture release remains subject to its recorded expiry; rollback does not
make stale source evidence fresh. No disk, compute, spend-cap, service or job
frequency change is part of this candidate.
