#!/usr/bin/env python3
"""One transaction for canonical odds, readable release odds and observations.

The caller holds the installed global writer flock through CSV preparation.
P3 releases that flock only after this context has acquired the odds DB lock
and prepared the existing pending evidence. The database transaction remains
open until all publication checks pass.
"""

from __future__ import annotations

import fcntl
import json
import os
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import psycopg2

try:
    from .odds_delivery_quote_verification import (
        CUSTOMER_MARKETS, archive_claimed, claim_pending, prepare_claimed,
        record_claimed, restore_claimed,
    )
    from .reconcile_fixture_delivery_odds import reconcile
except ImportError:  # Direct script execution in the installed VPS runtime.
    from odds_delivery_quote_verification import (
        CUSTOMER_MARKETS, archive_claimed, claim_pending, prepare_claimed,
        record_claimed, restore_claimed,
    )
    from reconcile_fixture_delivery_odds import reconcile


def moneyline_module():
    try:
        from . import validate_moneyline_coverage
    except ImportError:  # Direct script execution in the installed VPS runtime.
        import validate_moneyline_coverage
    return validate_moneyline_coverage


class PublicationPhaseTrace:
    """Record bounded phase evidence without SQL text, parameters or credentials."""

    def __init__(self) -> None:
        self.timings: list[dict[str, Any]] = []
        self.failed_phase: str | None = None

    @contextmanager
    def step(self, name: str):
        started = time.monotonic()
        status = "ok"
        try:
            yield
        except BaseException:
            status = "failed"
            self.failed_phase = name
            raise
        finally:
            elapsed_ms = round((time.monotonic() - started) * 1000, 1)
            self.timings.append({"phase": name, "status": status, "elapsed_ms": elapsed_ms})
            try:
                print(f"Odds publication phase {name}: {status} in {elapsed_ms}ms", flush=True)
            except OSError:
                # A closed log pipe must not mask a database error or turn a
                # committed transaction into an apparent publication failure.
                pass


class AtomicOddsPublication:
    def __init__(
        self, mode: str, db_url: str, sqlite_db: Path, league_ids: list[int],
        spool_dir: Path,
    ) -> None:
        if mode not in {"p3", "delivery"}:
            raise ValueError(f"unknown atomic odds mode: {mode}")
        self.mode = mode
        self.sqlite_db = sqlite_db
        self.league_ids = league_ids
        self.spool_dir = spool_dir
        self.connection = psycopg2.connect(db_url, connect_timeout=10)
        self.claims: list[tuple[Path, Path]] = []
        self.prepared: list[tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]] = []
        self.committed = False
        self.moneyline_report: dict[str, Any] | None = None
        self.release_report: dict[str, Any] | None = None
        self.quote_report: dict[str, int] | None = None
        self.phase_trace = PublicationPhaseTrace()

    def phase_step(self, name: str):
        return self.phase_trace.step(name)

    def prepare(self) -> None:
        """Acquire odds after writer admission, then freeze current evidence."""
        self.verify_writer_fd()
        with self.connection.cursor() as cur:
            for name, env_name, default in (
                ("statement_timeout", "ODDS_STATEMENT_TIMEOUT", "180000"),
                ("lock_timeout", "ODDS_LOCK_TIMEOUT", "15000"),
                ("idle_in_transaction_session_timeout", "ODDS_IDLE_TX_TIMEOUT", "180000"),
            ):
                value = os.environ.get(env_name, default)
                cur.execute("select set_config(%s, %s, true)", (name, value + "ms"))
            cur.execute("select pg_advisory_xact_lock(%s)", (int(os.environ.get("ODDS_ADVISORY_LOCK_KEY", "982374")),))
        self.claims = claim_pending(self.spool_dir)
        self.prepared = prepare_claimed(self.claims, self.sqlite_db, self.league_ids)
        if self.mode == "p3":
            self.release_p3_writer_lock()

    @staticmethod
    def verify_writer_fd() -> None:
        lock_path = os.path.realpath(os.environ.get("ODDS_SYNC_LOCK_FILE", "/var/lock/odds-sync.lock"))
        try:
            held = os.fstat(9)
            expected = os.stat(lock_path)
        except OSError as exc:
            raise RuntimeError("P3 atomic publisher lacks inherited writer-lock fd 9") from exc
        if (held.st_dev, held.st_ino) != (expected.st_dev, expected.st_ino):
            raise RuntimeError("atomic publisher lacks inherited writer-lock fd 9")

    @staticmethod
    def release_p3_writer_lock() -> None:
        AtomicOddsPublication.verify_writer_fd()
        fcntl.flock(9, fcntl.LOCK_UN)

    def capture_pre_usable(self) -> None:
        """Remember exact customer quotes usable under the current kickoff tier."""
        markets = sorted(CUSTOMER_MARKETS)
        with self.connection.cursor() as cur:
            cur.execute(
                """
                create temp table odds_pre_usable on commit drop as
                with future_raw as materialized (
                  select o.fixture_id, o.bookmaker_id, o.market_key, o.selection_key,
                         coalesce(o.line, -9999::numeric) as line_key,
                         o.price_decimal, o.last_updated_at, f.starting_at,
                         coalesce(o.participant_type, 'team') as participant_type,
                         coalesce(o.participant_id, 0) as participant_id
                  from public.odds_outcomes o
                  join public.fixtures f on f.id = o.fixture_id
                  where f.starting_at > (statement_timestamp() at time zone 'utc')
                    and o.market_key = any(%s)
                    and o.price_decimal > 1 and o.price_decimal <= 500
                ),
                recent_verifications as materialized (
                  select v.fixture_id, v.bookmaker_id, v.market_key, v.selection_key,
                         v.participant_type, v.participant_id, v.line_key, v.price_decimal,
                         v.source_last_updated_at, v.last_verified_at
                  from public.odds_quote_verifications v
                  where v.market_key = any(%s)
                    and v.last_verified_at >= statement_timestamp() - interval '8 hours'
                    and v.last_verified_at <= statement_timestamp() + interval '2 minutes'
                )
                select o.fixture_id, o.bookmaker_id, o.market_key, o.selection_key,
                       o.line_key, o.price_decimal, o.last_updated_at, o.starting_at,
                       o.participant_type, o.participant_id
                from future_raw o
                join recent_verifications v
                  on v.fixture_id = o.fixture_id
                 and v.bookmaker_id = o.bookmaker_id
                 and v.market_key = o.market_key
                 and v.selection_key = o.selection_key
                 and v.participant_type = o.participant_type
                 and v.participant_id = o.participant_id
                 and v.line_key = o.line_key
                 and v.price_decimal = o.price_decimal
                 and v.source_last_updated_at is not distinct from o.last_updated_at
                where v.last_verified_at <= statement_timestamp() + interval '2 minutes'
                  and v.last_verified_at >= statement_timestamp() -
                    case
                      when o.starting_at <= (statement_timestamp() at time zone 'utc') + interval '2 hours'
                        then interval '2 hours'
                      when o.starting_at <= (statement_timestamp() at time zone 'utc') + interval '1 day'
                        then interval '3 hours'
                      else interval '8 hours'
                    end
                """,
                (markets, markets),
            )

    def _assert_prior_usable_remains_exact(self) -> None:
        # A deliberately removed/rebound participant key is an absence, not a
        # surviving price. For a surviving player/team identity, require fresh
        # exact evidence before changing the price or source timestamp.
        with self.connection.cursor() as cur:
            cur.execute(
                """
                select count(*)::bigint
                  from odds_pre_usable p
                  join public.odds_outcomes o
                    on o.fixture_id = p.fixture_id
                   and o.bookmaker_id = p.bookmaker_id
                   and o.market_key = p.market_key
                   and o.selection_key = p.selection_key
                   and coalesce(o.line, -9999::numeric) = p.line_key
                   and coalesce(o.participant_type, 'team') = p.participant_type
                   and coalesce(o.participant_id, 0) = p.participant_id
                  left join public.odds_quote_verifications v
                    on v.fixture_id = o.fixture_id
                   and v.bookmaker_id = o.bookmaker_id
                   and v.market_key = o.market_key
                   and v.selection_key = o.selection_key
                   and v.participant_type = coalesce(o.participant_type, 'team')
                   and v.participant_id = coalesce(o.participant_id, 0)
                   and v.line_key = coalesce(o.line, -9999::numeric)
                 where (o.price_decimal, o.last_updated_at,
                        coalesce(o.participant_type, 'team'), coalesce(o.participant_id, 0))
                       is distinct from
                       (p.price_decimal, p.last_updated_at, p.participant_type, p.participant_id)
                   and (
                     v.fixture_id is null
                     or v.price_decimal is distinct from o.price_decimal
                     or v.source_last_updated_at is distinct from o.last_updated_at
                     or v.last_verified_at > statement_timestamp() + interval '2 minutes'
                     or v.last_verified_at < statement_timestamp() -
                       case
                         when p.starting_at <= (statement_timestamp() at time zone 'utc') + interval '2 hours'
                           then interval '2 hours'
                         when p.starting_at <= (statement_timestamp() at time zone 'utc') + interval '1 day'
                           then interval '3 hours'
                         else interval '8 hours'
                       end
                   )
                """
            )
            if int(cur.fetchone()[0]):
                raise RuntimeError("publication would replace a usable quote without fresh exact evidence")

    def _validate_p3_moneyline(self) -> None:
        moneyline = moneyline_module()
        excluded = moneyline.load_excluded_league_ids()
        leagues = [league for league in self.league_ids if league not in excluded]
        days_forward = int(os.environ.get("MONEYLINE_COVERAGE_DAYS_FORWARD", "7"))
        fail_below_pct = float(os.environ.get("MONEYLINE_COVERAGE_MIN_PCT", "100"))
        min_price = float(os.environ.get("ODDS_MIN_PRICE", "1.0"))
        max_price = float(os.environ.get("ODDS_MAX_PRICE", "500"))
        reports = [os.environ.get("ODDS_SYNC_REPORT_PATH", "")]
        reports += [path for path in ("/tmp/odds_sync_report_p2.json", "/tmp/odds_sync_report_p1.json") if Path(path).exists()]
        reports = [path for path in reports if path]
        coverage = moneyline.fetch_league_coverage(self.connection, leagues, days_forward, min_price, max_price)
        evidence, errors = moneyline.load_provider_evidence(
            reports, float(os.environ.get("MONEYLINE_PROVIDER_REPORT_MAX_AGE_HOURS", "6"))
        )
        failures, gaps, pipeline_failures, unresolved = moneyline.evaluate_provider_aware_failures(
            coverage, evidence, fail_below_pct
        )
        self.moneyline_report = {
            "generated_at": moneyline.utc_now_iso(),
            "days_forward": days_forward,
            "fail_below_pct": fail_below_pct,
            "league_ids": leagues,
            "excluded_league_ids": excluded,
            "leagues": coverage,
            "failures": failures,
            "provider_evidence_enabled": True,
            "provider_report_paths": reports,
            "provider_report_max_age_hours": float(os.environ.get("MONEYLINE_PROVIDER_REPORT_MAX_AGE_HOURS", "6")),
            "provider_evidence_fixture_count": len(evidence),
            "provider_evidence_errors": errors,
            "provider_gaps": gaps,
            "provider_pipeline_failures": pipeline_failures,
            "unresolved": unresolved,
            "ok": moneyline.validation_succeeded(failures, errors),
        }
        if not self.moneyline_report["ok"]:
            raise RuntimeError("moneyline coverage validation failed")

    def before_commit(self) -> None:
        if self.mode == "p3":
            with self.phase_step("moneyline_validation"):
                self._validate_p3_moneyline()
        with self.phase_step("release_reconcile"):
            self.release_report = reconcile(self.connection, commit=False)
        # The reconciler narrows its own statements to 60 s. Restore the
        # exporter's existing bound for the quote RPC and final identity read.
        with self.phase_step("restore_statement_timeout"):
            with self.connection.cursor() as cur:
                cur.execute(
                    "select set_config('statement_timeout', %s, true)",
                    (os.environ.get("ODDS_STATEMENT_TIMEOUT", "180000") + "ms",),
                )
        with self.phase_step("quote_record"):
            self.quote_report = record_claimed(self.connection, self.prepared)
        with self.phase_step("prior_usable_assert"):
            self._assert_prior_usable_remains_exact()

    def mark_committed(self) -> None:
        self.committed = True

    def _write_moneyline_report(self) -> None:
        if self.moneyline_report is None:
            return
        self.moneyline_report["publication_committed"] = self.committed
        out_json = Path("/tmp/moneyline_coverage_report_p3.json")
        out_md = Path("/tmp/moneyline_coverage_report_p3.md")
        out_json.write_text(json.dumps(self.moneyline_report, indent=2, default=str), encoding="utf-8")
        out_md.write_text(moneyline_module().build_markdown_report(self.moneyline_report), encoding="utf-8")

    def finalize(self) -> None:
        if not self.committed:
            raise RuntimeError("cannot finalize uncommitted odds publication")
        try:
            archive_claimed(self.claims, self.spool_dir)
        except Exception:
            # The DB commit is durable; replaying a still-claimed file is safe.
            restore_claimed(self.claims)
            raise
        if self.mode == "p3":
            self._write_moneyline_report()
        if self.mode == "delivery" and self.release_report is not None:
            Path("/tmp/fixture_delivery_odds_sync_report.json").write_text(
                json.dumps(self.release_report, indent=2, default=str), encoding="utf-8"
            )

    def close(self) -> None:
        if not self.committed:
            try:
                self.connection.rollback()
            except psycopg2.Error:
                # A disconnected server already discarded the open transaction.
                pass
            finally:
                restore_claimed(self.claims)
            if self.moneyline_report is not None:
                self._write_moneyline_report()
            if self.release_report is not None:
                self.release_report = {
                    "status": "rolled_back",
                    "attempted": self.release_report,
                }
        self.connection.close()
