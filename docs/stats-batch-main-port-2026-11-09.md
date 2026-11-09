# Main-line port of the live bounded statistics export

**9 November 2026. Source-control candidate only.** This branch starts at JXD `main` `aa9304e` and carries only the bounded statistics export code, its tests, and the regenerated runtime manifest. It does not merge or activate the separately staged leaderboard finite/rolling worker, alter VPS cron, pin a source baseline, run a production worker or change paid capacity.

## Why this port exists

The production source-only archive based on `24d0a94` is currently using a corrected first-attempt batch exporter while later attempts remain individual. JXD `main` does not yet contain that code. Directly merging the active branch would also bring in the draft leaderboard supervisor, locks and worker changes. The two copied source files in this main-based branch are byte-identical to `24d0a94` and to the files in the live source-only archive. This preserves the known source behavior for future reviewed integration without implying that this branch is a deployable replacement for the currently active archive.

`scripts/export_to_supabase.py` validates a bounded fixture-to-snapshot map and reports each atomic fixture outcome; `scripts/reconcile_stats_provider_queue.py` batches only eligible first attempts, retains individual retries and fails closed when the batch report cannot be trusted. Existing single-fixture and provider-pending behavior remains. The parent fixture core and odds writers are outside this change. The active archive's separate host exclusion and rolling worker files are **not** part of this main-line port; an archive built from this branch must not replace the live archive until those host dependencies are reconciled and the exact combined release is reviewed.

## Verification

- `python3 -m pytest -q tests/test_stats_reconciliation_batched_export.py tests/test_postponed_fixture_lifecycle.py`: **54 passed**, 12 existing deprecation warnings, no failures. The suite includes partial outcome/report refusal, mixed first-attempt/retry selection and a disposable PostgreSQL two-fixture snapshot-lineage replay.
- `python3 -m py_compile scripts/export_to_supabase.py scripts/reconcile_stats_provider_queue.py`: passed.
- `shasum -c scripts/vps/runtime_manifest.sha1`: all listed runtime files passed.
- `git diff --check`: passed. The two production source files match `24d0a94` byte for byte.

Regenerating the manifest changed the expected hashes for the two source files and also corrected an **already stale** `scripts/leaderboard_shadow_worker.py` entry on `main`: the file at `aa9304e` hashes to `b670e9b45ed9bcccf4bb3c40e0f62ba85022cf7e`, whereas its committed manifest listed `e5c676f8d40932ea43444f87e12add076c7018b2`. The worker file itself is unchanged by this branch. This is a manifest integrity correction, not a worker release.

## Live boundary

The corrected source release ID is `3df6bb130cb9` in the active VPS archive. At 21:05 UTC its Supabase performance view had two comparable 10–20-unit successes of the required 20, and the old `375f1f1aaf25` source baseline remained pinned; the leaderboard worker/backfill were off with capture on and the WAL safety pause intact. The previous [source runbook](https://github.com/jackdoak17-cyber/JXD987/blob/9c11cb9/docs/stats-reconciliation-batch-live-cutover-2026-11-09.md) records four successful corrected-release batches and direct target parity for the latest 19 accepted fixtures. This port neither reuses those samples under a different manifest identity nor changes that live state. A later complete host integration must match the exact active release, test its source and worker code together, and preserve the source health and rollback gates before any VPS cutover.
