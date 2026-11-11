# Rolling successor selection candidate — 9 November 2026

Status: **isolated integration verified; production inactive**. Pairs with the
private successor ledger in website draft PR #103. This report is an independent
worker review, not a production activation record.

## Change and review

- The rolling wrapper keeps one reviewed root policy ID. After acquiring both
  existing host locks, the supervisor reads its append-only successor chain and
  selects the sole tip. It requires every policy in the chain to retain the
  pinned worker release and source release. Database admission still decides
  whether that tip is safe to run; a new successor committed between selection
  and admission causes the old policy to be denied, not silently redirected.
- A quarantine marker records the exact policy used by an interrupted run.
  Reconciliation from the reviewed root accepts only a policy in that lineage,
  drains its recorded sessions and exact reservation backend, and settles that
  policy. It rejects a foreign marker or a pair of markers with different
  policy IDs or session tokens, leaving both markers in place for review.
- I traced the lock order, fixed-release checks, worker subprocess arguments,
  and recovery path in the exact candidate. The website successor schema has
  one successor per predecessor and an append-only succession record. The
  worker's 32-link traversal limit fails closed if exceeded.
- This closes the previous fixed-ID wrapper gap. The worker release pin and
  runtime manifest must be generated and reviewed together before any future
  production install; changing this draft changes the eventual release SHA.

## Verification

- `python3 -m pytest tests/test_leaderboard_shadow_pipeline.py -q`:
  **43 passed, 1 skipped** (existing optional integration skip).
- `shasum -c scripts/vps/runtime_manifest.sha1`: all entries passed.
- Disposable PostgreSQL 17 website Milestone 3 runner with
  `M3_ROLLING_LEDGER_TEST=1`, `M3_ROLLING_GUARDED_TEST=1`,
  `M3_ROLLING_BOUND_TEST=1`, `M3_ROLLING_SUCCESSOR_TEST=1`, and the exact JXD
  worker/supervisor paths: **exit 0**. It covered real capture, generation,
  publication, a timed-out successor run, backend drain and conservative
  settlement. It then rejected a foreign marker, rejected matching-policy
  markers with different tokens, and reconciled a valid successor marker.
  The full runner ended with `PASS: Milestone 3 isolated source integration`.

## Remaining release gates

- The numeric allowances in these tests are synthetic. They establish ledger
  and selection behavior, not a safe six-hour/day production resource budget,
  catch-up rate, customer accuracy, or capacity for 1,000 concurrent users.
- A committed but closed, unsettled predecessor still needs an explicit
  successor rejection test. The isolated rollback/race and open-liability tests
  cover related paths but not that exact state.
- Before any activation: review current physical, source, and host disk
  evidence; create a measured policy; verify the precise JXD archive and VPS
  settings; finish independent source-to-browser statistical certification and
  customer-journey checks. The customer leaderboard reader remains disabled.
- No paid capacity, disk, Spend Cap, production worker, database migration,
  cron entry, or reader setting changed in this checkpoint.
