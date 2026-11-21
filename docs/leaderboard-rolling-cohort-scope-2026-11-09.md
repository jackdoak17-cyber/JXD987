# Rolling worker cohort scope — 9 November 2026

Status: local and disposable integration verified; production still runs the
previous manifest-verified archive with the rolling worker disabled. No rolling
policy, reservation or customer reader has been activated.

## Why the first measurement plan changed

At 13:58:41 UTC, a read-only production query found 1,578 dirty fixture
captures. The then-reviewed rolling worker took the oldest 250 globally before
checking publication eligibility. No unpublished cohort had all its queued
fixtures among those 250, including fixtures linked by either their recorded
or current cohort. The proposed unscoped 30-second run therefore could not
measure publication even if it reached its maximum capture count. The smallest
unpublished queue was 29 fixtures. A separate 13:59:46 read found league
271/season 27897 with 30 queued fixtures and seven distinct change-event
fixtures at the current cursor. These are dated observations, not a fixed
production target or a measured resource allowance.

## Focused correction

The rolling wrapper now accepts a positive league/season pair in its protected
environment and passes it through the supervisor to the child. The child uses
the same pair for dirty capture and pending-generation selection. An omitted
pair preserves the previous global ordering and normal worker behavior.
Incomplete or nonpositive pairs fail before database work. The result records
the effective scope for audit. Existing source reconciliation and shared host
lock code remain unchanged.

This makes a real capture-to-publication measurement possible for a cohort
whose queue and generation workload fit the bounded run. It does not establish
that any currently observed cohort will complete in 30 seconds or within the
proposed WAL/temp allowance. The target and limits need a fresh preflight; a
refusal or partial capture remains fully accounted, never a reason to increase
the allowance automatically.

## Verification and release impact

- Python syntax and wrapper shell syntax passed.
- Focused worker suite: 46 passed, one existing skip. Added tests cover
  scoped generation selection, the exact SQL cohort filter, and a copied
  manifest-verified archive whose wrapper passes the complete pair and rejects
  an incomplete pair before starting its child. The previous unscoped path
  retains its original parameters and ordering.
- Full socket-only PostgreSQL 17 M3 suite with rolling ledger, guarded,
  bounded and successor flags passed. Its supervised run supplied the cohort
  pair, captured and published a generation, settled one reservation, and
  reported the same scope. Recovery, successor and rollback checks also passed.
- Runtime manifest SHA-1 prefix after this change: `d025f6607f67`; SHA-256
  of the manifest: `155dbe8da0628c649a1809b0b199e27d06bf6b3f8f63269e5bbd386304ae7bc6`.

The current source job release is `ad5dc85da158`, while this candidate's
whole-manifest source identity would be `d025f6607f67`. An eventual cutover
of both scheduled entrants must drain the old source supervisor, verify the
new archive and preserve the old resource debt. It must then collect a fresh
genuine comparable source baseline before opening a rolling policy; the old
source jobs must not be relabeled. A future source-specific identity can avoid
this worker-only coupling, but needs its own auditable transition proof.

The production website runtime, Supabase settings, paid capacity, spend cap,
normal ingestion schedule, rolling policy and customer reader have not changed.
