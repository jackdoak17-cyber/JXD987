# Codex senior review of R50 and the live finite trial — 9 November 2026

**Verdict:** R50's narrow source-composition conclusion is supported by independent checks. The live host cutover and one-use trial are durably settled and have no active quarantine. The trial was performed before the first new-release source batch, so its clean guard used fresh **old-release** importer evidence. The 3,608,050-byte capture WAL measurement remains valid; the trial does **not** prove new-release importer health or renewable leaderboard operation. No second trial is authorised by the consumed policy.

## Checks performed without relying on R50's sign-off

| Check | Direct result |
| --- | --- |
| Actual old host versus reviewed source | The saved 05:58 VPS `common.sh` SHA-256 `0cf88d6c...e1603fcf0` exactly matches PR81 commit `305a5cb`'s file. |
| Candidate composition | A local diff against PR81 showed only the default lock-identity helper and two `.quarantine.heavy` gates. Candidate SHA-256 is `164dcc62...73fe502`; manifest SHA-256 is `951230c6...a79dc32`. |
| Focused Linux behavior | Re-ran `tests/test_finite_trial_host_composition.sh` inside the cached PostgreSQL 17 Linux image; exit 0, reservation/settlement/owner-release, quarantine and verifier checks passed. These use synthetic cron/process inventories. |
| Live host | At 07:04 UTC, the actual VPS verifier exited 0 with both existing cron entrants on the same new archive and no old reconciliation descendant. The installed manifest SHA-256 matched `951230c6...a79dc32`. |
| Durable database outcome | At 07:04 UTC: zero unsettled trial reservations and zero unfinished worker runs; worker/backfill false, capture true, resource paused, unchanged 5,368,709,120-byte WAL and 536,870,912-byte temp caps. The earlier direct reservation read showed consumed policy, settled WAL 3,608,050, temp 0, run 2047 finished with claimed/succeeded/failed 10/10/0 and exact reviewed release binding. |
| Customer check | Authenticated selected-date Fixtures after the cutover reported 2/2 with zero issues on the same published release. This is a narrow observation, not broad schedule or capacity certification. |

R50 explicitly limited its independent claims to source, shell behavior and a synthetic host test; it did not claim to inspect the VPS or execute the trial. That scope statement was sound. The live verification above is Codex's separate evidence.

## Gap found in the action-time sequence

The new statistics supervisor started at 06:45 and waited for its normal safe window. At 06:46 the installed database guard had no hard breaches because the latest genuine source batch was the **old archive's** successful 06:38:20 run, and the pinned baseline was also old release `563d92e85772`. The one-use capture trial ran and settled at 06:47. The new archive's first source batch did not finish until 06:52:28, under manifest release `375f1f1aaf25`.

This was an operator sequencing error: the reviewed procedure called for observing the new source outcome before trial eligibility, and the guard did not bind the live cron archive to the latest importer evidence before the first new run. Later success cannot retroactively supply that preflight evidence. The trial's measured database work, exact settlement and clean cleanup remain evidenced; claims about new-release importer health, a renewable budget or customer leaderboard freshness do not follow from it.

The new source batch succeeded with 10 selected, two accepted, eight provider-pending and zero failed. Its performance view initially had one comparable new-release sample, while the pinned baseline still referred to old release/workload 20. Keep the worker/backfill disabled and the old WAL pause/caps. A future activation must require a completed source run from the active manifest and genuine same-release comparable baseline evidence. This is a technical evidence gate, not a calendar wait or permission request.

## Host dependency missed by the reviewed runbook

The VPS system `python3` does not have `psycopg2`; the existing shared `.venv/bin/python3` does. The reviewed finite wrapper calls `python3` through `PATH`. Its invocation therefore used the shared virtual environment on `PATH`, with unchanged reviewed runtime bytes. This prevented a startup failure, but the R50 runbook did not state the dependency. It is now explicit in the historical cutover procedure. An unprepared invocation would fail before useful work; the five consumed-trial invocation pins have since been removed from the active protected environment to prevent accidental replay.

## Spending and remaining proof

No paid compute tier, provisioned disk, Spend Cap, added service or customer reader flag was changed by these actions. This audit does not independently reconcile the billing page after its reporting lag, so it does not assert a final invoice amount. The one trial had `temp_file_limit_enforced=false` in worker output but measured zero temp; the 16 MiB logical reservation is not a physical maximum. Complete catalogue/data accuracy, cold reader speed, normal publication freshness and realistic 1,000-user capacity remain open.

— Codex
