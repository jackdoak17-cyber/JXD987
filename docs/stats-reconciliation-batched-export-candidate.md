# Bounded statistics detail export candidate

Date: 2026-11-09. Source branch `codex/batched-fixture-detail-export` starts
from JXD `main` `aa9304e` and now merges the exact active VPS statistics
archive `3028e0f` at merge commit `d78bc48`. The candidate manifest was
regenerated for the two changed source files; its SHA-256 is
`617534da51cb8f0b63d109067eba176cb853afb6e53d84ebea78295f722b053c`.
All 49 entries verify. This branch is **not deployed or scheduled**. A new
manifest identity would reset the source-performance observation sequence, so
this merge is not permission to cut over while the current pin gate refuses.

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
tests/test_postponed_fixture_lifecycle.py tests/test_reconcile_stats_provider_queue.py
tests/test_leaderboard_shadow_pipeline.py` passed **157**, with one existing
skip and 12 deprecation warnings. New tests cover
strict map validation, one-process invocation, partial success, ambiguous
report refusal, retry-mode individual commands and a Unix-socket-only
disposable PostgreSQL replay with two real atomic fixture publications and
distinct snapshot IDs. A wrong-snapshot failure committed only its successful
sibling; a legacy single-fixture retry then restored exact source/target
parity. The parent REST upserts were mocked.

A separate 20-fixture disposable replay produced the same 1,340 target
player-stat rows and snapshot lineage with old and batch paths. The old path
performed 20 fixture-player source reads and 40 nonempty parent-upsert call
sites; batch performed one and two. One local, in-process comparison took
0.123/0.038 seconds old/batch and wrote 439,352/439,192 PostgreSQL WAL bytes.
The paths were run in that order, without process-start cost or actual parent
HTTP traffic, against a synthetic source with 22 lineups per fixture. These
numbers demonstrate parity and lower duplicated source/parent work in this
isolation, **not** production speed, live WAL or network savings.

The combined suite, Python compilation, 49-entry manifest verification and
`git diff --check` passed. The host composition shell test passed in an
existing Linux Docker image with networking disabled and the worktree mounted
read-only; the Mac host lacks `flock` and cannot run it natively.

Before a production release, compare actual subprocess old/batch exports on a
representative normal source with isolated target and real parent HTTP
behavior; measure whole-stage wall time, target WAL, lease duration and retry
outcomes. Review the merged manifest and exact host release identity. The
current production source pin remains on `375f1f1aaf25`; new source
`d025f6607f67` had 18/20 comparable samples, latest20-unit success at
18:54:03 UTC and normalized total p95 156,658.3 ms against a 148,617.4 ms
ceiling as read at 19:04 UTC. Neither this branch nor a new manifest may be
installed to bypass that guard. Keep the leaderboard worker paused, capture
enabled, old resource debt/caps intact and the customer reader unchanged. No
added cost or production change occurred in this candidate.
