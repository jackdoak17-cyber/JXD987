# Prop Search history refresh worker candidate

Date: 2026-11-09. This branch starts from JXD `main` at `aa9304e` and is
**not deployed or scheduled**. It pairs with the experimental website SQL at
`statswebsite-web` branch `codex/prop-search-maintained-history`.

`scripts/refresh_prop_search_history.py` is read-only by default. `--apply`
requires an exact installed `refresh_fixture(bigint)` function MD5, a positive
WAL ceiling, and hard work-loop limits: at most 20 fixtures, 60 seconds, 8 MiB
accounted WAL, 10,000 statistic rows and 500 appearance rows per fixture.
Defaults are stricter: 5 fixtures, 30 seconds, 5,000 statistics and 150
appearances. Connection establishment has a separate five-second timeout;
catalog and count preflights have five-second statement limits. The worker
takes a session advisory lock, processes dirty
fixtures before incomplete backfill fixtures, and commits one fixture at a
time. It rolls back a fixture if its measured WAL plus commit reserve would
cross the run ceiling. The WAL LSN measures all concurrent database writes
during the transaction, so it is conservative and cannot attribute a cost
to this worker alone. An abort can itself generate a small amount of WAL.

The worker never promotes the projection as ready and never changes the
customer reader. It does not call a paid source API. It writes a small JSON
report atomically when requested. The exact SQL hash ensures a later schema
revision cannot silently change what the worker executes.

## Local evidence

`python3 -m pytest -q tests/test_prop_search_history_refresher.py` passed
6/6 against a disposable PostgreSQL server. The cases cover argument bounds,
read-only default, schema-hash mismatch, dirty-first ordering, fixture cap,
WAL rollback, single-session lock and statistic-row ceiling. This test uses
a minimal function with the same database interface; the website repository
separately tests the full SQL calculation.

The paired integration command also passed:

```bash
bash scripts/test_prop_search_history_integration.sh "/absolute/path/to/statswebsite-web-prototype"
```

It created a socket-only disposable database, loaded the exact website SQL
with the test runner's explicit commit flag, changed one statistic, verified
the worker's default dry-run observed one dirty fixture, then ran one bounded
refresh with the installed function's exact MD5. The private dirty marker
cleared, the metric changed, and the guarded count reader matched the
installed calculation. The first integration attempt had an incorrectly
spelled function signature in the test's MD5 lookup; this yielded an empty
hash and was fixed before the passing run. This is an exact paired SQL/worker
test on synthetic data, not production-scale backfill or normal ingestion.

## Release gates

Do not add this to the runtime manifest, deploy it, or schedule it yet.
The private projection schema is absent from production. Its 17-million-row
source statistics table and normal overlapping jobs have not been exercised
with the additional triggers or a bounded backfill. The full-data backfill,
normal ingestion latency/WAL, cold all-league customer response and a
renewable six-hour/day operating allowance are unproved. The current
leaderboard worker is separately paused at its WAL limit; this candidate
does not override that resource state. Jack alone can approve added spending.
