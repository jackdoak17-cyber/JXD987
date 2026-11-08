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
two bounded dirty-state calls of at most 60 seconds plus 5 seconds termination
grace each, the existing 1,800-second maximum publisher runtime plus its
5-second termination grace, and a 300-second margin: **5,835 seconds total**.
All effective time settings must be positive whole seconds at most 86,400;
zero cannot silently make `timeout` unlimited. Missing runway disables only the recent-publication
skip, so a verified no-op or a new release can renew the lease. The other
guards against an older source, a shorter proposed pin and a concurrent
publisher remain intact. The source freshness check still precedes a renewal.

The 21:11 boundary is covered by a deterministic unit test, together with
the exact 5,835/5,834-second boundary and a missing-runway case. The fixture
delivery suite passed 48 tests with 13 skips, including 19 release tests and
three wrapper contract tests before the bounded-preparation correction.
The wrapper now bounds each dirty-state call with a 60-second timeout and
5-second termination grace; a held-lock test on the staged Linux runtime is
required before cutover. The runtime manifest checksum, shell syntax and
`git diff --check` pass. The test suite does not reproduce the full VPS cron
environment; stage the copied archive and verify its manifest before a cutover.

At 21:20 UTC the **active VPS archive**, rather than this source branch, had
40 manifest paths. The staged copy has exactly the same 40 paths and the
same `.env`, `.venv` and `data` symlink targets. Only the publisher source
and hourly wrapper had changed entry hashes before the bounded-preparation
revision; regenerate and repeat the comparison at the exact final head.
The earlier 43-entry v4 count refers to a different package checkpoint.

## No-cost cutover and rollback

1. Confirm the current customer release, genuine source age, disk headroom,
   the three active cron entries and the active archive. Save the full
   crontab, its SHA-256, and the active manifest SHA-256. Do not alter the
   core or settlement entries.
2. Copy the **active** 1.2 MB VPS archive to a new immutable release directory,
   preserving its `.venv` symlink. Overlay only
   `scripts/refresh_fixture_delivery.py` and
   `scripts/vps/run_fixture_delivery_publisher.sh` from the reviewed commit.
   Rebuild the runtime manifest from the **active package's actual path set**
   and run `shasum -c`/`sha1sum -c` there. Compare all paths and hashes except
   those two changed files and the manifest against the original archive.
   Verify the external `.env`, `.venv` and `data` links, the installed Python
   CLI, and the executable held-dirty-lock deadline test before handoff.
3. Freeze **only** the hourly publisher cron entry while retaining all other
   lines byte for byte. Verify the frozen crontab, wait until the old hourly
   invocation and publication lock are drained, then replace that one entry
   with the staged path. Verify the complete installed crontab against the
   saved file with only this path substitution. Do not rely on a pre-staging
   no-process check as the handoff barrier.
4. Observe the next scheduled run and the database resolver/pin. Only
   `recent_publication_satisfies_guard` certifies the new 5,835-second runway;
   `publication_active`, `newer_current_source` and
   `would_shorten_current_pin` are distinct deferrals and must be checked
   against their own live resolver/source evidence. A successful no-op or
   publication must show genuine source evidence and a renewed live pin.
5. If the staged wrapper/manifest check, cron verification or live renewal
   fails, freeze only the hourly publisher entry again, drain its process and
   publication lock, restore the saved publisher entry to the original
   archive, and verify byte-for-byte preservation of all other lines. Keep
   the staged archive for diagnosis. The active database release is never
   rolled back by changing the wrapper path.

This changes no provider subscription, paid service, compute tier, provisioned
disk, spend cap, or job frequency. It may cause a normal hourly publisher to
validate and renew a release that the prior guard would have skipped; measure
its database work and customer lease continuity under the existing resources.
