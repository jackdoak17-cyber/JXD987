# Supervised rolling leaderboard worker candidate — 9 November 2026

Status: **isolated integration verified; production inactive**. This draft pairs
with the private rolling SQL in website PR #103. It is not an activation record.

## Change

- A rolling run uses `begin_bound_worker`, `check_bound_worker`, and
  `finish_bound_worker` against one release-pinned policy. The existing paused
  worker state is left untouched. Capture, eligibility refresh, generation
  jobs, and publication share one 30-second deadline and at most 250 operations.
- Capture checks the rolling guard after every claim. Generation checks it
  before each job and publication. A generation manifest larger than the
  remaining operation budget is refused before materialization.
- The finite supervisor now supports `--rolling` while retaining its two host
  locks, process-group deadline, exact backend drain, durable quarantine, and
  post-crash whole-run recovery. Quarantine markers identify their mode, so a
  finite reconciliation cannot clear a rolling reservation.
- The production wrapper requires exact database, worker release, runtime
  manifest, policy, and numeric limit pins before invoking the supervisor.
  It also runs the existing source/worker cron and descendant cutover check.
- The rolling worker uploads the host's fresh disk sample before admission.
  The paused normal worker cannot upload it; the installed database function
  rejects invalid or stale samples.

## Verification

- `python3 -m pytest tests/test_leaderboard_shadow_pipeline.py -q`:
  **43 passed, 1 skipped** (the skip is an existing optional integration).
- `shasum -c scripts/vps/runtime_manifest.sha1`: all listed runtime files
  matched.
- Website Milestone 3 disposable PostgreSQL 17 runner with
  `M3_ROLLING_LEDGER_TEST=1`, `M3_ROLLING_GUARDED_TEST=1`,
  `M3_ROLLING_BOUND_TEST=1`, and the worker/supervisor paths set: exit 0.
  Its new test `73_python_rolling_supervised_full_workload.sh` captured a
  real fixture, uploaded disk evidence, executed a generation job, published, and settled the bound
  reservation. A second run deliberately timed out after admission; the
  supervisor drained the owner backend, recovered the reservation using the
  physical WAL fallback, and removed both quarantine markers. The old paused
  worker settings remained unchanged.

## Remaining gates

- The isolated workload is small. It does not establish a safe production
  allowance or catch-up rate for the current 1,336 queued fixtures across 24
  cohorts, and it does not prove a six-hour/day resource window.
- A read-only production measurement at 10:49 UTC found at most 44 existing
  event jobs and 107 queued fixtures per cohort. The largest conservative
  combined bound was 151 jobs, below 250. This is a moving count; a future
  cohort over the ceiling still needs bounded incremental generation.
- The strict source-health gate currently refuses genuine low-volume runs:
  the latest success at 10:39 had 5 work units, while the last comparable
  20-unit run was at 10:23. The host disk file was fresh at 10:50, but the
  database disk observation had aged since 06:44 while the normal worker was
  disabled. The new disk preflight closes the latter gap only. Source-health
  semantics need a reviewed fail-closed solution for sparse batches.
- Review the exact website SQL and worker release together, set a numeric
  rolling policy from measured WAL/temp/source headroom, and verify the VPS
  archive and current source evidence. Keep the customer leaderboard reader
  disabled until source-to-browser statistical certification and release gates
  pass.
- No paid capacity, disk, Spend Cap, production worker, or cron setting was
  changed by this draft and its isolated tests.
