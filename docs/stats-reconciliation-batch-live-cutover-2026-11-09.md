# Statistics reconciliation batch export: live source cutover

Date: 2026-11-09. This is a source-only release on the existing VPS. It does
not enable the leaderboard worker or its customer reader, reset resource debt,
change the Supabase disk/compute tier or Spend Cap, or buy a service.

## Exact release and evidence before activation

- Candidate source branch `codex/batched-fixture-detail-export` at `88125da`;
  active prior source archive `3028e0f`.
- The candidate merges `3028e0f` and changes only the bounded statistics
  reconciliation/export path among runtime files. Runtime manifest has 49
  verified entries, SHA-256
  `617534da51cb8f0b63d109067eba176cb853afb6e53d84ebea78295f722b053c`.
  Its new runtime release ID is `368b97679c37`; the prior source release was
  `d025f6607f67`. The separately pinned importer baseline remains
  `375f1f1aaf25`.
- Exact candidate tests: 158 passed, one existing skip, 12 deprecation
  warnings; Python compilation, manifest verification and diff check passed.
  Linux host composition passed in a network-disabled container. See
  [candidate analysis](stats-reconciliation-batched-export-candidate.md) for
  real-source isolated old/batch parity, parent-call, elapsed and WAL evidence
  and its limits.
- The new immutable archive was staged at
  `/opt/odds-sync/JXD987-stats-batch-88125da-release`, with `.env`, `.venv`
  and `data` linked to the same existing runtime dependencies as `3028e0f`.
  It occupied 977,848 directory bytes, excluding those symlink targets. The
  deployment verifier confirmed the full manifest and odds-delivery startup;
  the VPS had 5,370,601,472 free bytes on `/` at 19:27 UTC.
- At the last preflight, the old source supervisor had no reconciliation or
  exporter child. The 19:24 old-source run succeeded with three selected,
  one accepted, two provider-pending and no failure. Its export stage took
  5.342 seconds. The old 19:25 cron entrant skipped because the old
  supervisor still held its lock.

## Activation and rollback boundary

The exact prior crontab is backed up root-only at
`/root/odds-stats-cron-before-88125da-20261109`. The first stop script sent
TERM to the old supervisor and then treated `ps` exit 1 (the expected
already-exited result) as an error; it stopped **before** changing cron.
A readback proved the crontab still matched the backup, with no old supervisor
or source child. A second guarded step replaced only the two archive-path
occurrences in the single statistics cron line and proved exact readback;
all other cron entries were retained. The new supervisor started at 19:26:42
UTC and held PID 1464919. It initially skipped because only 138 seconds of
normal-writer lease remained against its 270-second minimum; this is the
expected protection before the next settlement handoff, not a source failure.

The first genuine new-release batch is recorded below. The cutover was rolled
back because it preserved correctness but did not reduce export time. For any
future rollback, allow any in-flight bounded child to exit or terminate it
under the existing lease, then stop the new supervisor. Restore
**only the statistics cron line** from the saved prior entry after verifying
the current line still points to this candidate; preserve any later unrelated
cron edits. Verify no new child or supervisor remains, restart the prior
manifest-verified archive, and check one ordinary source result and target
delivery status. Do not change the old source pin or resource debt on rollback.

The new release starts its own performance observation sequence. No old
samples are relabeled. Pinning requires the installed sample, freshness and
p95 limits; this source-only cutover does not create a renewable leaderboard
worker allowance or authorize customer reader activation.

## First genuine run and conditional rollback

At 19:37 UTC, candidate release `368b97679c37` completed one genuine
lease-bounded source batch: 20 selected, 13 accepted, seven provider-pending,
zero failed. All 13 accepted target delivery rows had the accepted snapshot
linked, equal source/target snapshots, and true player-stat and lineup parity.
The source-store, export, projection-refresh and activation stages took
21.226, 55.564, 8.354 and 8.811 seconds respectively. The final exporter
report named one explicit fixture and had no batch-outcome field, proving
that the **individual** exporter path ran. The candidate's all-or-nothing
batch gate was disabled by at least one retry in the same accepted set; the
isolated replay had not covered this mixed-attempt production shape. Correct
delivery is established for this batch, but a live speed improvement is not.

After the batch finished and no candidate child remained, the candidate
supervisor was stopped. The current statistics cron entry was checked to
point to this candidate, then only that entry was restored from the root-only
prior backup; unrelated cron entries were retained. The inactive candidate
entry was saved at `/root/odds-stats-cron-candidate-88125da-20261109`. The
prior `3028e0f` supervisor restarted at 19:40:30 UTC; its first checks were
expected lease and settlement deferrals, and PID 1469933 was still running at
19:44:50. No old source pin, resource debt/cap, leaderboard worker, customer
reader, disk/compute tier, Spend Cap or paid service was changed. A later
genuine old-source result remains to be checked.

## Mixed-attempt correction in development

The next candidate groups eligible first attempts into one bounded exporter
call while sending retries from the same source run through the existing
individual commands. If only one first attempt is eligible, all exports stay
individual. It preserves per-fixture outcome order, fails closed on an
ambiguous batch report, and records batch versus individual fixture counts in
the source report. The mixed-success/failure and one-first-attempt cases now
have focused tests; the combined suite passed 160 with one existing skip after
regenerating the 49-entry runtime manifest. Its manifest SHA-256 is
`b5ff7da6e23d0eb546f1087574689fa09fb6edc731bc459c8001e2006f60a4bc`
and release ID is `3df6bb130cb9` (the manifest SHA-1 prefix). The immutable
archive `/opt/odds-sync/JXD987-stats-batch-mixed-24d0a94-release` was staged
and its 49-entry manifest and odds-delivery startup verified at 19:51 UTC.
This candidate is **not active** and has not had a live performance trial.
Another immutable archive and guarded source-only cutover require a fresh
preflight, exact test/manifest readback and old-source result. No old sample
may be counted toward the new release's source pin.
