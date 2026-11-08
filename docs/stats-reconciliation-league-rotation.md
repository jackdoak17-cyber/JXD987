# Bounded league scopes for stats reconciliation

**State:** draft, local verification only. No worker, database or paid service
has been changed in production.

## Evidence and intended effect

A live 2026-11-08 11:39–11:40 UTC reconciliation attempt failed with a PostgreSQL
statement timeout while selecting revalidation fixture IDs. The existing
selector issues six sequential queries against all 30 configured leagues for
each batch. A non-executing production `EXPLAIN` of that exact revalidation
shape estimated cost 69,262 for all 30 leagues, 25,537 for five, 9,264 for
leagues 8 and 9, and 4,129 for league 8 alone. The two-league plan starts from
the existing fixtures league index and estimates about 1,740 qualifying rows
before the final sort, versus about 18,079 in the all-league plan. These costs
and row counts are planner estimates, not execution measurements or proof that
this change alone restores the website.

The supervisor now passes two configured leagues per bounded reconciliation
attempt. The worker's scored/rediscovered eligibility, exclusions, due and
quality conditions, six lane queries, quotas and final selection stay
unchanged *within that scope*. A small file under `/var/lib/odds-sync` records
the next league group across supervisor and deployment restarts. The cursor is
advanced only after both production locks admit the attempt. A failed attempt
advances to the next group; it can be retried on the next cycle. The cursor is
written before the Python worker acquires its separate source-store lock, so a
worker refusal or crash can also skip that group until the next rotation. An empty group
does not end the supervisor until every group has been checked without a
successful batch between them.

This intentionally changes priority between leagues: work in a different
group waits for its turn. With 30 configured leagues and the default group size
of two, a full pass requires 15 admitted attempts. Actual revisit time depends
on batch durations, live settlement reservations and lock contention; there is
no proven wall-clock guarantee. The current worker fails before making a
batch, so this is a bounded recovery candidate, not a claim that freshness or
the 24-hour delivery SLA is already met.

## Local evidence and release boundary

- The helper's 12 focused tests cover full-cycle coverage, restart persistence,
  changed configuration, invalid input, cursor handling and the selected scope
  passed to the supervisor. The related fixture-core, settlement and
  reconciliation suites passed 56 tests with one skip.
- Shell syntax and the complete runtime manifest hash check passed.
- No production runtime or data query was executed with the new scheduler.
- This change is stacked on the unmerged reconciliation failure-log draft. A
  release must include both runtime manifest entries and the independent
  leaderboard lock-order change, then pass exact integrated worker checks.

After an approved release, compare actual candidate-selection duration,
successful/failed batches, selected leagues, fixture backlog and publication
freshness. If it does not improve progress or causes unacceptable league delay,
restore the previous supervisor and manifest. The cursor file can remain
unused; rollback needs no database migration. Any production deployment or
worker restart still needs Jack's approval under the website delivery protocol.
