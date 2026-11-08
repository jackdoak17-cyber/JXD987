# Guarded fixture publisher runway correction

Status: draft, not deployed. Customer release `d892bbe4-8612-4558-bb0f-02f25c2160a1`
was renewed by the existing publisher at 21:13:28 UTC and pinned through
23:13:27 UTC. This one-time renewal is separate from the code candidate.

## Failure evidence

The existing hourly publisher starts at minute `:11`. Its 21:11:07 run
returned `recent_publication_satisfies_guard` because the previous completed
publication was less than 3,600 seconds old. The current lease then ended
at 22:11:08, about one second after the next scheduled start. The last three
successful full publications took roughly 35–39 seconds, so an ordinary next
run would leave a customer availability gap. The skip predicate required only
45 minutes of current lease runway; it did not cover the next hourly run.

The live publisher source SHA-256 `f0b55940899274d523266fb301c6ec9b01ffac4036f4cafe6cd13a3fcd958c8c`
and wrapper SHA-256 `b08b5393f0677e52f508adc26e0acf08a27a693b02607bd22b91ae48a56dd222`
match the local base. Other runtime files, including `common.sh`, do **not**
all match this source branch, so deployment must start from the exact active
VPS archive and overlay only the reviewed publisher source and wrapper.

## Correction and acceptance

The guarded recent-publication skip now requires a declared minimum runway.
The hourly VPS wrapper supplies 3,600 seconds to the next scheduled start,
the existing 1,800-second maximum publisher runtime, and a 300-second margin:
5,700 seconds total. Missing runway disables only the recent-publication
skip, so a verified no-op or a new release can renew the lease. The other
guards against an older source, a shorter proposed pin and a concurrent
publisher remain intact. The source freshness check still precedes a renewal.

The 21:11 boundary is covered by a deterministic unit test, together with
the exact 5,700/5,699-second boundary and a missing-runway case. The fixture
delivery suite passed 48 tests with 13 skips, including 19 release tests and
three wrapper contract tests. The runtime manifest checksum, shell syntax and
`git diff --check` pass. The test suite does not reproduce the full VPS cron
environment; stage the copied archive and verify its manifest before a cutover.

## No-cost cutover and rollback

1. Confirm the current customer release, genuine source age, disk headroom,
   the three active cron entries, no affected publisher process and no active
   fixture-delivery publication lock. Save the current crontab and SHA-256 of
   the active archive manifest. Do not alter the core or settlement entries.
2. Copy the **active** 1.2 MB VPS archive to a new immutable release directory,
   preserving its `.venv` symlink. Overlay only
   `scripts/refresh_fixture_delivery.py` and
   `scripts/vps/run_fixture_delivery_publisher.sh` from the reviewed commit.
   Rebuild the 40-entry SHA-1 runtime manifest in the new archive and run
   `shasum -c`/`sha1sum -c` there. Compare all paths and hashes except those
   two changed files and the manifest against the original archive.
3. Replace only the hourly publisher crontab path with the staged archive;
   preserve the other lines byte for byte. Observe the next scheduled run and
   the database resolver/pin. A guarded skip is acceptable only with at least
   5,700 seconds of remaining pin at the decision time. A successful no-op or
   publication must show genuine source evidence and a renewed live pin.
4. If the staged wrapper/manifest check, cron verification or live publication
   fails, restore the saved publisher cron entry to the original archive under
   the same no-process barrier. Keep the staged archive for diagnosis. The
   active database release is never rolled back by changing the wrapper path.

This changes no provider subscription, paid service, compute tier, provisioned
disk, spend cap, or job frequency. It may cause a normal hourly publisher to
validate and renew a release that the prior guard would have skipped; measure
its database work and customer lease continuity under the existing resources.
