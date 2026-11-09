# Durable league-scoped stats reconciliation

**State:** draft on existing VPS resources. The worker, database, website and paid
infrastructure are unchanged in production. This draft requires the paired
website aggregate-health migration and Jack's production approval before use.

## Evidence and scheduling choice

A genuine 8 November 11:39–11:40 UTC reconciliation attempt timed out in the
revalidation fixture selector before producing a batch. A non-executing
production plan estimated cost 69,262 for all 30 leagues and 4,129 for league
8 alone. These are planner estimates, not measured speedups. A previous
materialized-CTE rewrite was slower in a local replay and was discarded.

The supervisor now passes **one supported league per admitted attempt**. This
prevents the current two-cohort post-selection cap from repeatedly admitting
both cohorts of one league while another league in the same group receives no
work. It does not prove every season within a league progresses or that the
full rotation has enough throughput. The supervised path requires the exact
supported league list and group size one; manual, season-specific and force
calls retain their existing behaviour. The existing candidate rules, urgency
probes, quotas, exclusions and ordering remain unchanged *within the selected
league*. Global priority between leagues changes intentionally.

## Durable state and health

A private cursor under `/var/lib/odds-sync` contains the release/configuration
fingerprint, coverage epoch, next league, a unique admitted attempt and its
scope, last validated completion by league, unresolved failures and the last
failure event. The supervisor writes a pending attempt after acquiring the
existing database-heavy and canonical locks. The Python worker inherits the
canonical lock; it does not acquire a second source-store process lock. The
worker report carries the attempt and epoch IDs. A completion only counts
when its report matches that pending identity, league and release. A missing,
stale or malformed report cannot mark the scope successful.

Failures, planned-lease interruptions and crashes remain unresolved for the
affected league while other leagues continue. A later valid completion for
that same league clears its unresolved state; the last failure remains in the
file for diagnosis. An interrupted attempt is detected on the next admitted
claim. A release or league-list change starts an unknown coverage epoch and
retains unresolved failures for leagues still configured. Cursor and outcome
updates use a private temporary file, file and directory sync, and atomic
replacement while the supervisor lock is held.

An empty league never terminates the supervisor by itself. Only a complete
sequence of empty scopes across all configured leagues, without intervening
work or unresolved failure, produces an `empty_sweep`. The state also marks
coverage unknown until every league has a valid completion, or degraded if a
league is failed, interrupted or more than twelve hours since inspection.
This **inspection** age is separate from accepted provider data, cohort
publication and the customer delivery SLA; those must be measured separately.

Actual attempts remain `run_stats_reconciliation` ledger rows with real
status, selected work and timing. A distinct
`run_stats_reconciliation_scope_health` ledger row records aggregate unknown,
degraded or healthy state and complete-empty-sweep evidence. The paired
website migration makes the leaderboard health consumer read this aggregate
record. It does not add synthetic rows to the actual performance sample. A
successful league B cannot clear an unresolved failure in league A, and an
empty B cannot mark the whole importer idle. The existing release-baseline
transition remains required when the runtime manifest changes.

## Local evidence and release boundary

- The helper tests cover wrap, missing/failed scope, restart, interruption,
  stale or wrong report identity, release/list changes, and selected work with
  zero accepted fixtures. A Linux container with no network executes the real
  supervisor, lock wrapper and stub worker for mixed empty, failed and
  interrupted sequences. Its stub ledger shows real attempts and separate
  aggregate outcomes.
- The paired website SQL migration and local tests verify aggregate-health
  checks without adding aggregate rows to performance samples. The exact
  migration must be integrated with the worker release and retested together.
- No production run with a scoped selector has occurred. The complete
  rotation's selector time, admission delay, useful accepted data, published
  cohorts and oldest due fixture age remain unmeasured. There is no proven
  wall-clock revisit or 24-hour freshness guarantee.

After an approved release, compare full-rotation throughput, per-league
selected/accepted/published progress, overdue age, normal customer results and
leaderboard publication. If the scheduler harms freshness or does not recover
progress, restore the prior supervisor and manifest. The cursor file becomes
unused; rollback needs no data migration. The aggregate-health SQL should be
rolled back only with the coordinated worker release, because its guard
intentionally rejects a worker that emits no aggregate row.
