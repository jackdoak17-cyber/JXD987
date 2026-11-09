# Bounded statistics detail export candidate

Date: 2026-11-09. Source branch `codex/batched-fixture-detail-export` starts
from JXD `main` `aa9304e` and now merges the exact active VPS statistics
archive `3028e0f` at merge commit `d78bc48`. The candidate manifest was
regenerated for the two changed source files; its SHA-256 is
`617534da51cb8f0b63d109067eba176cb853afb6e53d84ebea78295f722b053c`
for the first live candidate. All 49 entries verified. That candidate was
briefly scheduled for a source-only trial and then rolled back; the corrected
mixed-attempt revision below is **not deployed or scheduled**. A new manifest
identity resets the source-performance observation sequence and does not
bypass the existing source-pin gate.

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
tests/test_leaderboard_shadow_pipeline.py` passed **158**, with one existing
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

An actual-process replay then used the same 20-fixture source, a fresh target
detail state for each path, a Unix-socket-only disposable PostgreSQL server,
and a local HTTP server that accepted parent upserts without writing them.
The old/batch paths produced identical 1,340 target player-stat rows and
snapshot lineage. Old used 20 exporter processes, 40 local parent HTTP
requests carrying 60 rows, 2.872 seconds and 442,448 target WAL bytes;
batch used one process, two local parent HTTP requests carrying three rows,
0.251 seconds and 441,792 target WAL bytes. The old path ran first, and both
used synthetic 22-player fixtures; HTTP response behavior and target schema
were simpler than production. This proves the candidate removes repeated
process, source and parent-request work in that isolation. It does not establish
the production gain, full parent-write WAL, or a production-shaped lease bound.

## Representative source replay

The read-only source slice was extracted from the active VPS SQLite file with
`scripts/benchmarks/extract_fixture_detail_source_subset.py`. It contains 20
recently accepted fixture IDs across league82 and league387: `19735139,
19735136,19735135,19735133,19735157,19735156,19735129,19735127,19735153,
19735137,19735131,19735130,19735152,19735126,19735118,19735160,19735155,
19735154,19735138,19768813`. The 2.7MB local slice had six seasons, 18
teams, 410 players, 408 player-team-history rows, 806 lineups, 1,800 team
statistics and **17,944** player statistics. Its SHA-256 was
`68435c3b829eb9c949e366b48ec9457926b3b9209239a178686581f78c3fb202`.
The data file is local and uncommitted; the extractor streams to stdout so a
repeat can avoid writing a copy on the VPS. The benchmark test accepts this
slice through `ODDS_REPRESENTATIVE_SOURCE_PATH`.

The first representative actual-process replay kept parent HTTP as a local
no-write endpoint. Old/batch delivered identical 17,944 target detail rows
and snapshot lineage; used 20/1 exporter processes and 60/3 parent requests
carrying 971/432 rows; took 3.120/0.338 seconds; and wrote
4,043,336/4,042,616 target WAL bytes. The disposable PostgreSQL cluster
initially used SQL_ASCII and rejected real accented player names. Changing
its `initdb` encoding to UTF-8 resolved that test-environment error; no
product data was altered.

With `ODDS_PARENT_WRITE_REPLAY=1`, the local HTTP endpoint wrote every parent
payload into a generic JSONB upsert table, committing each request. Exact
detail, snapshot lineage and all **432 distinct parent payloads** matched
between paths. In old-first order, old/batch took 3.158/0.351 seconds and
wrote 4,462,976/4,261,168 WAL bytes. In batch-first order, batch/old took
0.356/3.113 seconds and wrote 4,261,128/4,463,016 WAL bytes. Both runs had
60/3 parent HTTP requests and 971/432 submitted parent rows. The generic
table captures duplicate-write direction but does not reproduce the real
PostgREST schemas, indexes, authorization, network latency or concurrent
production load. The source slice is real; the target and HTTP layer remain
isolated substitutes. These results do not establish the live whole-job
improvement or a production-safe cutover.

The initial combined suite, Python compilation, 49-entry manifest verification and
`git diff --check` passed. The host composition shell test passed in an
existing Linux Docker image with networking disabled and the worktree mounted
read-only; the Mac host lacks `flock` and cannot run it natively.

Before a production release, verify a normal source run's full provider,
source-store, export, verification and projection stages under the live lease;
check real PostgREST parent behavior, target WAL and partial-failure recovery
with bounded observation. Review the merged manifest and exact host release
identity. The
current production source pin remains on `375f1f1aaf25`; new source
`d025f6607f67` had 18/20 comparable samples, latest20-unit success at
18:54:03 UTC and normalized total p95 156,658.3 ms against a 148,617.4 ms
ceiling as read at 19:04 UTC. Neither this branch nor a new manifest may be
installed to bypass that guard. Keep the leaderboard worker paused, capture
enabled, old resource debt/caps intact and the customer reader unchanged.

## Live trial correction

The exact `88125da` runtime was installed in an immutable VPS archive and
briefly scheduled as the source-only statistics worker. Its first genuine
19:37 UTC batch accepted13 of20 selected with no failure; all13 target
delivery rows passed snapshot, parity and accepted-link checks. Export still
took55.564 seconds. The final exporter report listed one explicit fixture and
no batch outcome, proving individual calls ran. Inspection of the worker's
batch gate found that one later-attempt fixture disabled batching for the
**entire** accepted set. The local old/batch benchmarks were valid for their
isolated first-attempt workload but did not predict this mixed production
run. The old statistics cron/supervisor were conditionally restored at19:40;
details and limits are in the [live runbook](stats-reconciliation-batch-live-cutover-2026-11-09.md).

The corrected candidate separates first-attempt and retry IDs within one
accepted set. Eligible first attempts use one atomic-per-fixture batch;
retries retain the individual exporter path, with original outcome order
restored before verification. The source report counts each export path.
Mixed partial failure and a one-first-attempt fallback have focused tests.
The combined suite passed **160**, with one existing skip and12 deprecation
warnings. Its regenerated49-entry manifest SHA-256 is
`b5ff7da6e23d0eb546f1087574689fa09fb6edc731bc459c8001e2006f60a4bc`
and release ID `b5ff7da6e23d`. It still needs a genuine live source trial
under the installed lease, target parity check, full-job timing comparison,
and normal old-source run verification before any renewed cutover can be
called successful. No added capacity, Spend Cap change or paid service was
used.
