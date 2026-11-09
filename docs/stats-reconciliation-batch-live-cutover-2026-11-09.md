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

The first genuine new-release batch, source-performance sample, delivery
parity and customer freshness must be observed before calling the cutover
successful. If the source fails, allow any in-flight bounded child to exit or
terminate it under the existing lease, then stop the new supervisor. Restore
**only the statistics cron line** from the saved prior entry after verifying
the current line still points to this candidate; preserve any later unrelated
cron edits. Verify no new child or supervisor remains, restart the prior
manifest-verified archive, and check one ordinary source result and target
delivery status. Do not change the old source pin or resource debt on rollback.

The new release starts its own performance observation sequence. No old
samples are relabeled. Pinning requires the installed sample, freshness and
p95 limits; this source-only cutover does not create a renewable leaderboard
worker allowance or authorize customer reader activation.
