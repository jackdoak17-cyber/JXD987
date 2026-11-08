# Fixture-core #81 coordinated admission rollout candidate — 8 November 2026

This is a prepared, **unapplied** five-checkout change. Jack approved and we
executed the narrower #80 cutover at 14:42 UTC. That approval does not cover
this broader reservation cutover. No additional infrastructure or spend is
proposed.

## Why all five releases must move together

The reservation is a sidecar file derived from `/var/lock/odds-sync.lock`.
The fixture-core caller claims it before attempting the canonical writer
lock; normal writers must check it at admission. An old writer checkout would
ignore the reservation and could overtake fixture core. A partial rollout
therefore cannot establish the intended priority. Settlement retains its
special priority and must run from the compatible core release. The existing
leaderboard stats reconciliation supervisor has held its shell process open
for days; its `common.sh` functions are loaded in memory and changing the
cron path alone cannot update them.

## Exact offline-composed packages

All five packages were composed from read-only copies of the active VPS
releases. The offline composer in `scripts/vps/compose_fixture_core_admission_release.py`
accepts only the inspected baseline `common.sh` hashes and replaces only the
three canonical admission/recording functions plus reservation helpers. It
preserves each checkout's unrelated functions, including the matcher's
always-enabled live-settlement guard and the leaderboard release's database
heavy lock. The core package starts with the installed #80 release and adds
the #81 core wrapper. No archive contains `.env`, `.venv`, tests, SQLite data
or credentials. The existing credential and virtual-environment links must
be recreated to their **unchanged** targets at installation.

| Writer checkout | Current rollback release | Candidate release | Manifest SHA-1 | Archive SHA-256 |
| --- | --- | --- | --- | --- |
| Fixture core, hourly publisher and post-match settlement | `JXD987-fixture-core-80-aae97d-release` for core/publisher; `JXD987-fixture-core-900d1ca-release` for settlement | `JXD987-fixture-core-81-d3aa9c-release` | `d3aa9ced4752448768ae44604e5f47415c960368` | `96d4c2eb184ba1829df431dbcbab814f0fd9360d436981e71d5d0caa80a72d2a` |
| Odds matcher P1/P2/P3 | `JXD987-odds-matcher-48d251e-release` | `JXD987-odds-matcher-81-9653c9-release` | `9653c9e3eab9a73352db426ef2442ec778ad2bb6` | `e59c26146199a0579ba06faae97b6d5ffb4dfb8f53e19ab5bc02e633b54b63c0` |
| Leaderboard stats reconciliation | `JXD987-leaderboard-65e105b-release` | `JXD987-leaderboard-81-9227a0-release` | `9227a0f3eec117397f7ef586333f42895cc874d7` | `62fb3926015eaccbc6c364bb94caeb4e6d866d0f09f97d58a81f549964bd92fa` |
| Website odds delivery | `odds-delivery-5b315fb-release` | `odds-delivery-81-721e96-release` | `721e9620f49acefaa7eb23acaa5ed3c47224c497` | `443107374208e9b082744df06e40b9425ce77f58dbdc8c4b0cce5960d69dd7d2` |
| Squad reconciliation | `JXD987-runtime-7e74d46-release` | `JXD987-runtime-81-af2053-release` | `af205351fbb694ba43e3a3b3c8f452cf7ae70bf5` | `003c6b1e5d82048b0d64f3f3cb6b57e894610996e92c79919b53dc186e40112d` |

The five corresponding archives are in the local `release-artifacts/`
directory with names `fixture-core-81-{core,matcher,leaderboard,delivery,runtime}-<manifest-prefix>.tar.gz`.
Each archive was extracted into an empty directory and every manifest entry
was hash-verified. Each composed release's full manifest then passed the
Linux runtime verifier. The executable Linux reservation/settlement,
ownership-expiry, lock-order, bounded-wait, heartbeat-stall and post-wait
lease tests passed on **all five** compositions, as did the admitted-work
handoff test. This is focused admission evidence, not complete end-to-end
certification of each older release's unrelated jobs.

## Forward map for a separately approved rollout

1. Recheck all active releases and manifests, archive hashes, symlink targets,
   cron entries and the currently held locks. Confirm no candidate target
   path already exists. Do not start the rollout during a fixture-core source
   run, settlement batch or data-heavy maintenance hold.
2. Install the five immutable packages side by side, restore each release's
   existing `.env` and `.venv` symlink targets, and verify their entire
   manifests on the VPS. Keep all rollback directories untouched.
3. Back up the complete root crontab. Pause new starts of the long-lived
   stats supervisor, then wait for an observed batch handoff or natural queue
   drain. The current supervisor has **no graceful drain command**. A signal
   is acceptable only after checking that it has no active child and does not
   hold the canonical or database-heavy lock; verify all descendants are
   gone before starting the new supervisor. If that safe state cannot be
   established, postpone the cutover. Merely repointing cron leaves the old
   in-memory admission functions running.
4. In one crontab replacement, repoint P1 (`17 *`), P2 (`37 */2`) and odds
   P3 (`19 */6`) to the matcher package; fixture core (`17 */6`), hourly
   publisher (`11 *`) and settlement (`*/15`) to the core package; stats
   supervisor (`*/5`) to the leaderboard package; website odds delivery
   (`4,19,34,49 *`) to the delivery package; and squad reconciliation
   (`20,50 *`) to the runtime package. Preserve every existing environment
   prefix, argument, frequency and log path. Leave unrelated Models,
   Operations and other lock domains untouched.
5. Read back each exact cron entry, verify the new supervisor starts from the
   new release, and inspect the canonical lock and reservation owner. Observe
   a real fixture-core attempt while normal odds delivery and reconciliation
   are active; confirm one admissible source refresh, ordinary writer
   resumption, settlement continuity, an hourly guarded publisher renewal,
   a current release resolver, and authenticated customer fixtures. Watch at
   least the next two scheduled six-hour core runs. The first observation
   does not establish long-run capacity.

## Rollback and partial-cutover safety

Restore the backed-up **entire** crontab, including #80's `17 */6` core and
its hourly publisher, and the old settlement path. Pause the new supervisor
and hand it off only at a verified idle boundary as above; verify that the old
supervisor is again the only holder of its own supervisor lock. Do not delete
any release directory while a process uses it. An old normal writer ignores
the reservation, so do not leave a mixed protocol operating and describe it
as a successful rollback. If a reservation exists, identify its owner and
expiry, let an admitted core invocation drain, and clear only that owner after
it exits; otherwise wait for its bounded expiry. Then verify normal writers,
settlement and the current release resolver. Rollback cannot make expired
fixture source evidence fresh or extend an expired release pin.

The #81 cutover requires a separate owner approval and an operationally safe
supervisor handoff. Neither has occurred.
