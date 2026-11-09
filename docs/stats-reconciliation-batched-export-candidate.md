# Bounded statistics detail export candidate

Date: 2026-11-09. Source branch `codex/batched-fixture-detail-export` starts
from JXD `main` `aa9304e`. The active VPS statistics archive is instead
`3028e0f`; its rolling-worker changes must be integrated explicitly before
any release. This branch is **not deployed or scheduled** and its runtime
manifest is unchanged.

## Measured reason for this candidate

The current reconciliation source invokes `export_to_supabase.py` once for
each accepted fixture. In a 20-fixture successful run, the old pinned release
accepted a mean 13.13 fixtures and spent 51.665 seconds in export; the current
release accepted a mean 16.11 and spent 62.304 seconds. The selected-fixture
performance guard currently fails, but this comparison alone does not prove
an importer code regression. The three source processing files are identical
between the checked prior and active VPS worker archives. A read-only serving
database check found 174 of 282 fixtures with a successful current-release
delivery since 14:15 UTC still linked to a provider snapshot accepted more
than an hour before that success. Re-exporting unchanged snapshots may be
avoidable, but this candidate **does not skip any fixture**: snapshot identity
does not by itself prove core, ancillary and target detail parity.

The latest single-fixture exporter report had one fixture, 72 season upserts,
two team upserts, one fixture upsert and 46 player-team-history upserts. A
read-only `--help` launch on the VPS took 0.23–0.24 seconds in three trials;
process startup is a small part of the measured export time. Batching aims to
avoid repeated source reads, parent REST calls and connection setup without
changing which fixtures are published.

## Candidate contract

For two or more first-attempt, partition-approved fixtures with validated
snapshot IDs, the reconciliation worker now invokes one exporter process for
at most 20 fixture IDs. It supplies a protected fixture-to-snapshot JSON map.
The exporter validates the exact map and guarded flags before database work,
loads the source batch once, then calls the existing atomic target publisher
separately for each fixture. Each publisher call still commits or rolls back
one fixture. Ancillary rows are exported only for fixtures whose atomic detail
publication succeeded. The structured report identifies every success and
failure; the worker rejects missing, duplicate, extra or ambiguous outcomes.

If a batch fails without a complete report, every requested fixture remains
retryable. The next attempt uses the existing individual exporter path because
the ledger attempt number has increased. If the batch reports a partial
result, successful fixtures continue through the existing verification and
activation path; failed fixtures are marked retryable. Single-fixture and
later-attempt exports use the old command unchanged. The normal post-match
single-fixture path is unchanged.

## Local verification and open gates

`python3 -m pytest -q tests/test_postmatch_fixture_detail_delivery.py
tests/test_stats_reconciliation_batched_export.py
tests/test_fixture_detail_export_policy.py tests/test_export_to_supabase.py
tests/test_postponed_fixture_lifecycle.py` passed **92/92**. New tests cover
strict map validation, one-process invocation, partial success, ambiguous
report refusal, retry-mode individual commands and a Unix-socket-only
disposable PostgreSQL replay with two real atomic fixture publications and
distinct snapshot IDs. That replay mocked the parent REST upserts; it proves
the existing atomic SQL path and result lineage, not full network behavior or
production speed. Python compilation and `git diff --check` passed.

Before a production release, compare exact old/batched results and failure
recovery under the same isolated source shape, measure parent REST calls,
wall time and WAL under a representative normal batch, and verify the maximum
lease and source identity change against the live `3028e0f` archive. The
current source pin remains on `375f1f1aaf25`; the new source has only 17/20
comparable samples and still exceeds the installed p95 guard as last checked
at 18:52:38 UTC. Neither this branch nor a new manifest may be installed to
bypass that guard. Keep the leaderboard worker paused, capture enabled, old
resource debt/caps intact and the customer reader unchanged. No added cost or
production change occurred in this candidate.
