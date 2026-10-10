from __future__ import annotations

import copy
import concurrent.futures
import json
import multiprocessing
import os
import fcntl
import signal
import subprocess
import selectors
import sqlite3
import socket
import sys
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
import requests

from scripts import postmatch_fixture_detail_delivery as delivery
from scripts import reconcile_stats_provider_queue as queue
from scripts import review_postponement_status as checker


NOW = datetime(2026, 10, 9, 16, tzinfo=timezone.utc)


def quarantine(fixture_id=19745046):
    return dict(fixture_id=fixture_id, exclusion_type="provider_unavailable",
                first_identified_at="2026-10-03T16:37:11Z", last_checked_at="2026-10-03T16:37:11Z",
                evidence={"provider_status": "POSTPONED"}, reason="postponed",
                next_review_at="2026-10-10T16:37:11Z", home_team_id=758, away_team_id=238115)


def capture(status="FT", fixture_id=19745046):
    # Controlled independent status observation, not a claimed latest live response.
    payload = {"id": fixture_id, "state": {"short_name": status},
               "participants": [{"id": 758}, {"id": 238115}]}
    return dict(source="fixture_core_provider_response", observed_at="2026-10-09T15:00:00Z",
                payload=payload, payload_sha256=delivery.provider_payload_hash(payload))


@pytest.mark.parametrize("status", ["POST", "POSTPONED", "CANC", "CANCELLED", "ABAN", "ABANDONED", "NS", "INPLAY_1ST_HALF"])
def test_non_final_evidence_does_not_reopen(status):
    assert not delivery.postponement_review_eligible(quarantine(), capture(status), NOW)


def test_historical_19745046_status_blocker_is_reviewable_not_export_ready():
    assert delivery.postponement_review_eligible(quarantine(), capture(), NOW)
    assert delivery.assess_provider_payload(capture()["payload"]).status == "provider_pending"


@pytest.mark.parametrize("field,value", [("source", "untrusted"), ("payload_sha256", "changed"),
    ("observed_at", "2026-10-03T16:00:00Z"), ("observed_at", "2026-10-07T15:00:00Z"),
    ("observed_at", "2026-10-10T15:00:00Z"), ("observed_at", "not-a-time"),
    ("observed_at", "2026-10-09T15:00:00")])
def test_stale_untrusted_or_future_capture_is_rejected(field, value):
    evidence = capture()
    evidence[field] = value
    assert not delivery.postponement_review_eligible(quarantine(), evidence, NOW)


@pytest.mark.parametrize("kind,status", [("duplicate", "POST"), ("provider_unavailable", "CANC"),
    ("provider_unavailable", "ABAN"), ("provider_unavailable", None)])
def test_unrelated_quarantines_stay_protected(kind, status):
    old = quarantine()
    old.update(exclusion_type=kind, evidence={"provider_status": status})
    assert not delivery.postponement_review_eligible(old, capture(), NOW)


def test_identity_mismatch_is_rejected():
    assert not delivery.postponement_review_eligible(quarantine(9000), capture(), NOW)
    old = quarantine()
    old["away_team_id"] = 99
    assert not delivery.postponement_review_eligible(old, capture(), NOW)


def test_claim_is_durable_idempotent_and_exhausts_three_attempts(tmp_path):
    path = tmp_path / "reviews.sqlite"
    first, second = sqlite3.connect(path), sqlite3.connect(path)
    original = quarantine()
    for day in range(3):
        now = NOW + timedelta(days=day)
        claim = {}
        assert delivery.claim_postponement_review(first, original, capture(), now, claim)
        assert not delivery.claim_postponement_review(second, original, capture(), now)
        assert not delivery.claim_postponement_review(second, original, capture(), now + timedelta(hours=23))
        delivery.finish_postponement_review(first, claim, "blocked_or_failed")
    assert not delivery.claim_postponement_review(second, original, capture(), NOW + timedelta(days=30))
    rows = first.execute("select original_exclusion, evidence, outcome from fixture_postponement_reviews").fetchall()
    assert len(rows) == 3
    assert all(json.loads(row[0]) == original for row in rows)
    first.close()
    second.close()


def test_successful_review_cannot_be_reclaimed():
    db = sqlite3.connect(":memory:")
    claim = {}
    assert delivery.claim_postponement_review(db, quarantine(), capture(), NOW, claim)
    delivery.finish_postponement_review(db, claim, "verified")
    assert not delivery.claim_postponement_review(db, quarantine(), capture(), NOW + timedelta(days=2))


def test_native_postgres_timestamps_are_preserved_in_audit():
    db = sqlite3.connect(":memory:")
    old = quarantine()
    for key in ("first_identified_at", "last_checked_at", "next_review_at"):
        old[key] = delivery.parse_iso(old[key])
    assert delivery.postponement_review_eligible(old, capture(), NOW)
    assert delivery.claim_postponement_review(db, old, capture(), NOW)
    saved = json.loads(db.execute("select original_exclusion from fixture_postponement_reviews").fetchone()[0])
    assert delivery.parse_iso(saved["first_identified_at"]) == old["first_identified_at"]


def test_finishing_retry_does_not_rewrite_crashed_attempt_evidence():
    db = sqlite3.connect(":memory:")
    assert delivery.claim_postponement_review(db, quarantine(), capture(), NOW)
    claim = {}
    assert delivery.claim_postponement_review(db, quarantine(), capture(), NOW + timedelta(days=1), claim)
    delivery.finish_postponement_review(db, claim, "verified")
    assert db.execute("select outcome from fixture_postponement_reviews order by attempt").fetchall() == [("claimed",), ("verified",)]


def full_detail(status="FT"):
    payload = capture(status)["payload"]
    payload["statistics"] = [{"participant_id": team, "type_id": stat, "data": {"value": 1}}
                             for team in (758, 238115) for stat in delivery.TRACKED_TEAM_STAT_TYPES]
    payload["lineups"] = [{"team_id": team, "player_id": player,
                           "details": [{"type_id": 119, "data": {"value": 90}}]}
                          for team, player in ((758, 11), (238115, 22))]
    return payload



@pytest.mark.parametrize("budget", [0, -1, 4, 100])
def test_budget_validation_precedes_target_access(budget):
    with pytest.raises(ValueError): delivery.select_postponement_reviews(sqlite3.connect(":memory:"), "unused", [capture()], budget)


@pytest.mark.parametrize("status", [200, 301, 429, 503])
def test_actual_http_transport_never_retries_or_redirects(monkeypatch, status):
    calls = []
    def request(*args, **kwargs):
        calls.append(kwargs)
        return SimpleNamespace(status_code=status, json=lambda: {"data": {"id": 19745046}})
    monkeypatch.setattr(delivery.requests, "request", request)
    client = SimpleNamespace(base_url="https://provider.invalid/", api_token="offline", timeout=90)
    if status == 200:
        assert delivery.postponement_provider_request(client, "GET", "fixtures/19745046", {})["data"]["id"] == 19745046
    else:
        with pytest.raises(RuntimeError): delivery.postponement_provider_request(client, "GET", "fixtures/19745046", {})
    assert len(calls) == 1 and calls[0]["timeout"] == 20 and not calls[0]["allow_redirects"]


def test_timeout_consumes_exactly_one_http_attempt_without_secret_error(monkeypatch):
    calls = []
    def request(*a, **kw):
        calls.append(1)
        raise requests.Timeout("secret-url-and-token")
    monkeypatch.setattr(delivery.requests, "request", request)
    client = SimpleNamespace(base_url="https://provider.invalid/", api_token="offline", timeout=20)
    with pytest.raises(RuntimeError, match="reserved attempt consumed") as error:
        delivery.postponement_provider_request(client, "GET", "fixtures/19745046", {})
    assert len(calls) == 1 and "secret" not in str(error.value)



def test_real_selection_obeys_budget_and_does_not_scan_normal_blocked_queue(monkeypatch):
    db = sqlite3.connect(":memory:")
    exclusions = [quarantine(fixture) for fixture in (19745046, 9001, 9002)]
    keys = ("fixture_id", "exclusion_type", "first_identified_at", "last_checked_at", "evidence", "reason", "next_review_at", "home_team_id", "away_team_id")
    class Cursor:
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def execute(self, query, params):
            assert "where x.fixture_id = any(%s)" in query and len(params[0]) == 3
        def fetchall(self): return [tuple(exclusion[key] for key in keys) for exclusion in exclusions]
    class Target:
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def cursor(self): return Cursor()
    monkeypatch.setattr(delivery.psycopg2, "connect", lambda *a, **kw: Target())
    monkeypatch.setattr(delivery, "utc_now", lambda: NOW)
    evidence = [capture(fixture_id=fixture) for fixture in (19745046, 9001, 9002)]
    assert delivery.select_postponement_reviews(db, "offline", evidence, 1) == [19745046]
    assert db.execute("select count(*) from fixture_postponement_reviews").fetchone()[0] == 1
    assert delivery.select_postponement_reviews(db, "offline", evidence, 2) == [9001, 9002]
    assert delivery.select_postponement_reviews(db, "offline", evidence, 3) == []


@pytest.fixture(autouse=True)
def deny_unmocked_provider_access(monkeypatch):
    monkeypatch.setattr(requests, "request", lambda *a, **kw: pytest.fail("Unmocked provider access forbidden"))


def run_read_only(monkeypatch, tmp_path, evidence, *, repeats=1, extra_flags=()):
    """Real main and SELECT boundary; any indirect mutator is an assertion failure."""
    state = {"snapshot": {"id": 123, "quality_status": "accepted", "payload": "trusted"},
             "quarantine": quarantine(), "attempts": [], "claims": [], "leases": []}
    before = copy.deepcopy(state)
    calls = []
    def forbidden(name):
        def call(*a, **kw):
            calls.append(name)
            raise AssertionError(f"Review-only reached mutating/provider boundary: {name}")
        return call
    for name in ("source_connection", "ensure_ledger", "claim_postponement_review", "finish_postponement_review",
                 "source_engine", "SportMonksClient", "postponement_provider_request", "persist_provider_snapshot",
                 "store_provider_detail", "export_fixture", "clear_provider_unavailable_exclusion",
                 "activate_provider_snapshot", "update_ledger",
                 "publish_delivery_status", "mark_provider_unavailable", "refresh_player_projection"):
        monkeypatch.setattr(delivery, name, forbidden(name))
    monkeypatch.setattr(delivery, "acquire_postponement_lock", forbidden("lock"))
    class Target:
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def cursor(self): return self
        def execute(self, sql, params):
            calls.append("SELECT")
            assert sql.strip().lower().startswith("select")
            assert "fixture_stats_quality_exclusions" in sql
        def fetchall(self):
            keys = ("fixture_id", "exclusion_type", "first_identified_at", "last_checked_at", "evidence",
                    "reason", "next_review_at", "home_team_id", "away_team_id")
            return [tuple(state["quarantine"][key] for key in keys) + (46,)]
        def commit(self): raise AssertionError("Unexpected explicit commit")
        def rollback(self): raise AssertionError("Unexpected explicit rollback")
    def connect(*args, **kwargs):
        assert kwargs["options"] == "-c default_transaction_read_only=on"
        return Target()
    monkeypatch.setattr(delivery.psycopg2, "connect", connect)
    monkeypatch.setattr(delivery, "utc_now", lambda: NOW)
    monkeypatch.setenv("SUPABASE_DB_URL_SESSION", "offline")
    path = tmp_path / "capture.json"
    path.write_text(json.dumps([evidence]))
    monkeypatch.setattr(sys, "argv", ["delivery", "--postponement-status-evidence", str(path),
                                    *extra_flags])
    results = [checker.main() for _ in range(repeats)]
    assert state == before
    assert all(call == "SELECT" for call in calls)
    assert sorted(p.name for p in tmp_path.iterdir()) == ["capture.json"]
    return results, calls


def test_read_only_real_main_preserves_accepted_snapshot_and_all_persistent_state(monkeypatch, tmp_path, capsys):
    results, calls = run_read_only(monkeypatch, tmp_path, capture(), repeats=3)
    assert results == [0, 0, 0] and calls == ["SELECT"] * 3
    reports = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert all(report["provider_calls"] == 0 for report in reports)
    assert all(report["postponement_revalidation"]["observations"][0]["detail_completeness"] == "not_assessed" for report in reports)


@pytest.mark.parametrize("case", ["conflict", "postponed", "cancelled", "abandoned", "unknown", "stale", "missing", "malformed"])
def test_read_only_bad_evidence_fails_closed_without_mutation(monkeypatch, tmp_path, capsys, case):
    evidence = capture()
    if case == "conflict": evidence["payload"]["state"] = {"developer_name": "FINISHED", "short_name": "POST"}
    if case in {"postponed", "cancelled", "abandoned", "unknown"}:
        evidence["payload"]["state"] = {"short_name": {"postponed": "POST", "cancelled": "CANC", "abandoned": "ABAN", "unknown": "UNKNOWN"}[case]}
    if case == "stale": evidence["observed_at"] = "2026-10-07T15:00:00Z"
    if case == "missing": evidence["payload"].pop("state")
    if case == "malformed": evidence["payload"]["state"] = "FINISHED"
    evidence["payload_sha256"] = delivery.provider_payload_hash(evidence["payload"])
    assert run_read_only(monkeypatch, tmp_path, evidence)[0] == [1]
    report = json.loads(capsys.readouterr().out)
    assert report["failed"] and not report["postponement_revalidation"]["observations"]
    if case == "conflict": assert "Contradictory" in report["failed"][0]["error"]


def test_review_only_rejects_report_file_before_any_side_effect(monkeypatch, tmp_path):
    with pytest.raises(SystemExit) as rejected:
        run_read_only(monkeypatch, tmp_path, capture(), extra_flags=("--report-json", str(tmp_path / "report.json")))
    assert rejected.value.code == 2
    assert not (tmp_path / "report.json").exists()



def test_conflicting_capture_never_claims_or_requests(monkeypatch):
    evidence = capture()
    evidence["payload"]["state"] = {"developer_name": "FINISHED", "short_name": "POST"}
    evidence["payload_sha256"] = delivery.provider_payload_hash(evidence["payload"])
    class Target:
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def cursor(self): return self
        def execute(self, *a): pass
        def fetchall(self):
            old = quarantine()
            keys = ("fixture_id", "exclusion_type", "first_identified_at", "last_checked_at", "evidence", "reason", "next_review_at", "home_team_id", "away_team_id")
            return [tuple(old[key] for key in keys) + (46,)]
    monkeypatch.setattr(delivery.psycopg2, "connect", lambda *a, **kw: Target())
    monkeypatch.setattr(delivery, "utc_now", lambda: NOW)
    db = sqlite3.connect(":memory:")
    assert delivery.select_postponement_reviews(db, "offline", [evidence], 1) == []
    assert db.execute("select name from sqlite_master").fetchall() == []


def test_actual_snapshot_SQL_preserves_accepted_evidence_for_active_revalidation():
    trusted = {"quality_status": "accepted", "payload": "original", "release_id": "trusted-release"}
    before = copy.deepcopy(trusted)
    calls = []
    class Target:
        def cursor(self): return self
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def execute(self, sql, params):
            calls.append(sql)
            if sql.strip().startswith("insert"):
                assert "case when %s and fixture_detail_snapshots.quality_status = 'accepted'" in sql
                assert params[-1] is True
                self.row = None
            else:
                assert sql.strip().startswith("select id")
                self.row = (123,)
        def fetchone(self): return self.row
        def commit(self): pass
        def rollback(self): pytest.fail("unexpected rollback")
    payload = full_detail()
    assert delivery.persist_provider_snapshot("offline", 19745046, 567, 28479, payload,
        delivery.assess_provider_payload(payload), delivery.provider_payload_hash(payload),
        delivery.normalized_provider_hash(payload), target_conn=Target(), preserve_accepted=True) == 123
    assert trusted == before and len(calls) == 2


def test_active_claims_survive_concurrent_reopened_connections(tmp_path):
    path = tmp_path / "reviews.sqlite"
    first = sqlite3.connect(path)
    assert delivery.claim_postponement_review(first, quarantine(), capture(), NOW)
    first.close()
    def claim(day):
        db = sqlite3.connect(path, timeout=20)
        try: return delivery.claim_postponement_review(db, quarantine(), capture(), NOW + timedelta(days=day))
        finally: db.close()
    for day, expected in ((0, 0), (1, 1), (2, 1), (3, 0), (30, 0)):
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            assert sum(pool.map(claim, [day] * 8)) == expected
    with sqlite3.connect(path) as db:
        assert db.execute("select count(*) from fixture_postponement_reviews").fetchone()[0] == 3


def test_original_review_snapshot_probe_now_reads_only_and_preserves_real_audit(monkeypatch):
    db = sqlite3.connect(":memory:")
    delivery.claim_postponement_review(db, quarantine(), capture(), NOW)
    audit_before = db.execute("select * from fixture_postponement_reviews").fetchall()
    snapshot = {"quality_status": "accepted", "payload": "trusted-original"}
    before = copy.deepcopy(snapshot)
    statements = []
    class Target:
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def cursor(self): return self
        def execute(self, sql, params):
            statements.append(sql.strip().lower())
            if sql.strip().lower().startswith("insert"):
                snapshot["quality_status"] = params[6]
        def fetchone(self): return (123,)
        def fetchall(self):
            old = quarantine()
            keys = ("fixture_id", "exclusion_type", "first_identified_at", "last_checked_at", "evidence",
                    "reason", "next_review_at", "home_team_id", "away_team_id")
            return [tuple(old[key] for key in keys) + (46,)]
        def commit(self): statements.append("commit")
        def rollback(self): statements.append("rollback")
        def close(self): pass
    monkeypatch.setattr(delivery.psycopg2, "connect", lambda *a, **kw: Target())
    monkeypatch.setattr(delivery, "utc_now", lambda: NOW)
    monkeypatch.setattr(delivery, "SportMonksClient", lambda: SimpleNamespace(timeout=90))
    monkeypatch.setattr(delivery, "postponement_provider_request", lambda *a, **kw: {"data": full_detail()})
    evidence = capture()
    evidence["payload"] = full_detail()
    evidence["payload_sha256"] = delivery.provider_payload_hash(evidence["payload"])
    report = {"fixture_ids": [19745046], "provider_calls": 0, "failed": [], "postponement_revalidation": {}}
    for _ in range(3):
        delivery.review_postponement_status_only(db, "offline", [evidence], report)
    assert snapshot == before
    assert db.execute("select * from fixture_postponement_reviews").fetchall() == audit_before
    assert not report["failed"] and report["provider_calls"] == 0
    assert len(statements) == 3 and all(sql.startswith("select") for sql in statements)


def run_observation(monkeypatch, tmp_path, payload, *, captures=None, budget=1, repeats=1, changed_quarantine=False,
                    metadata_updates=None, preserve_response_id=False):
    captures = captures or [capture()]
    state = {"snapshot": {"quality_status": "accepted", "payload": "trusted"},
             "quarantines": {item["payload"]["id"]: quarantine(item["payload"]["id"]) for item in captures}}
    if metadata_updates:
        for original in state["quarantines"].values(): original.update(metadata_updates)
    original_snapshot = copy.deepcopy(state["snapshot"])
    calls, sql = [], []
    path = tmp_path / "source.sqlite"
    monkeypatch.setattr(delivery, "SOURCE_DB", str(path))
    db = sqlite3.connect(path)
    db.execute("create table football_facts(fixture_id integer,value integer)")
    db.execute("insert into football_facts values(19745046,16)")
    db.commit()
    db.close()
    def forbidden(name):
        def call(*a, **kw):
            calls.append(name)
            raise AssertionError(f"Status observation reached forbidden helper: {name}")
        return call
    for name in ("ensure_ledger", "source_engine", "store_provider_detail", "export_fixture",
                 "clear_provider_unavailable_exclusion", "refresh_player_projection", "refresh_player_projection_season",
                 "activate_provider_snapshot", "persist_provider_snapshot", "publish_delivery_status",
                 "mark_provider_unavailable", "ledger_attempt_start", "update_ledger", "target_fixture_metadata"):
        monkeypatch.setattr(delivery, name, forbidden(name))
    def source():
        connection = sqlite3.connect(path)
        connection.set_trace_callback(sql.append)
        return connection
    monkeypatch.setattr(delivery, "source_connection", source)
    monkeypatch.setattr(delivery, "acquire_postponement_lock", lambda: calls.append("lock") or
                        os.open(tmp_path / "test.lock", os.O_CREAT | os.O_RDWR, 0o600))
    monkeypatch.setattr(delivery, "utc_now", lambda: NOW)
    class Target:
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def cursor(self): return self
        def execute(self, query, params):
            assert query.strip().lower().startswith("select")
            assert len(params[0]) <= 3
            calls.append("target_SELECT")
            self.ids = params[0]
        def fetchall(self):
            keys = ("fixture_id", "exclusion_type", "first_identified_at", "last_checked_at", "evidence",
                    "reason", "next_review_at", "home_team_id", "away_team_id")
            return [tuple(state["quarantines"][fixture][key] for key in keys) for fixture in self.ids]
        def commit(self): pytest.fail("No explicit target write commit allowed")
    def connect(*a, **kw):
        assert kw["options"] == "-c default_transaction_read_only=on"
        return Target()
    monkeypatch.setattr(delivery.psycopg2, "connect", connect)
    monkeypatch.setattr(delivery, "SportMonksClient", lambda: SimpleNamespace(
        timeout=90, base_url="https://provider.invalid/", api_token="offline"))
    def transport(method, url, **kwargs):
        calls.append("mock_HTTP")
        assert kwargs["params"]["include"] == "participants;state"
        assert kwargs["timeout"] == 20 and not kwargs["allow_redirects"]
        fixture = int(url.rsplit("/", 1)[1])
        with sqlite3.connect(path) as check:
            claimed = check.execute("select count(*) from fixture_postponement_reviews where fixture_id=?", (fixture,)).fetchone()[0]
            assert claimed == 1
        if changed_quarantine:
            state["quarantines"][fixture]["evidence"] = {"provider_status": "CANCELLED"}
        if isinstance(payload, Exception): raise payload
        data = copy.deepcopy(payload)
        if not preserve_response_id: data["id"] = fixture
        return SimpleNamespace(status_code=200, json=lambda: {"data": data})
    monkeypatch.setattr(delivery.requests, "request", transport)
    monkeypatch.setenv("SUPABASE_DB_URL_SESSION", "offline")
    evidence_file = tmp_path / "capture.json"
    evidence_file.write_text(json.dumps(captures))
    monkeypatch.setattr(sys, "argv", ["delivery", "--postponement-status-evidence", str(evidence_file),
                                    "--postponement-observe-status", "--postponement-provider-budget", str(budget)])
    results = [checker.main() for _ in range(repeats)]
    with sqlite3.connect(path) as check:
        tables = {row[0] for row in check.execute("select name from sqlite_master where type='table'")}
        rows = check.execute("select fixture_id,attempt,outcome,observation from fixture_postponement_reviews order by fixture_id").fetchall() if "fixture_postponement_reviews" in tables else []
        assert check.execute("select * from football_facts").fetchall() == [(19745046, 16)]
        assert tables.issubset({"football_facts", "fixture_postponement_reviews"})
    assert state["snapshot"] == original_snapshot
    assert not any("football_facts" in query.lower() for query in sql)
    assert all(name in {"lock", "target_SELECT", "mock_HTTP"} for name in calls)
    return results, calls, rows, state


def test_final_observation_is_local_evidence_only_not_publication(monkeypatch, tmp_path, capsys):
    results, calls, rows, state = run_observation(monkeypatch, tmp_path, full_detail())
    assert results == [0] and calls.count("mock_HTTP") == 1
    assert rows[0][2] == "final_status_observed"
    observation = json.loads(rows[0][3])
    assert observation["candidate_for_separate_repair"] and observation["detail_completeness"] == "not_assessed"
    assert not observation["publication_authorized"] and not observation["quarantine_modified"]
    assert state["quarantines"][19745046] == quarantine()
    report = json.loads(capsys.readouterr().out)
    assert report["status"] == "status_observations_recorded_locally"
    assert "verified" not in report and "assessment" not in observation
    assert "clear_revalidated_postponement" not in vars(delivery)


def test_changed_quarantine_cannot_trigger_replacement(monkeypatch, tmp_path):
    results, calls, rows, state = run_observation(monkeypatch, tmp_path, full_detail(), changed_quarantine=True)
    assert results == [0] and rows[0][2] == "final_status_observed"
    assert state["quarantines"][19745046]["evidence"] == {"provider_status": "CANCELLED"}
    assert set(calls) == {"lock", "target_SELECT", "mock_HTTP"}


@pytest.mark.parametrize("unused_stage", ["refresh_player_projection", "activate_provider_snapshot"])
def test_projection_and_activation_failures_are_structurally_irrelevant(monkeypatch, tmp_path, unused_stage):
    results, calls, _, _ = run_observation(monkeypatch, tmp_path, full_detail())
    # Both helpers raise if invoked by the real main orchestration.
    assert results == [0] and unused_stage not in calls


@pytest.mark.parametrize("field", ["developer_name", "state", "short_name", "status", "status_code"])
def test_conflicting_fresh_observation_is_audited_but_never_published(monkeypatch, tmp_path, field):
    payload = capture()["payload"]
    payload["state"] = {"developer_name": "FINISHED", "short_name": "FT"}
    if field in {"developer_name", "state", "short_name"}: payload["state"][field] = "POST"
    else: payload[field] = "POST"
    results, calls, rows, state = run_observation(monkeypatch, tmp_path, payload)
    observation = json.loads(rows[0][3])
    assert results == [1] and calls.count("mock_HTTP") == 1
    assert observation["classification"] == "blocked_or_failed" and "Contradictory" in observation["diagnostic"]
    assert not observation["candidate_for_separate_repair"] and state["quarantines"][19745046] == quarantine()


@pytest.mark.parametrize("status,classification", [("POST", "postponed"), ("CANC", "cancelled"),
    ("ABAN", "abandoned"), ("UNKNOWN", "unknown_or_non_final")])
def test_nonfinal_observation_retains_quarantine_with_distinct_diagnostic(monkeypatch, tmp_path, status, classification):
    results, _, rows, state = run_observation(monkeypatch, tmp_path, capture(status)["payload"])
    assert results == [1] and rows[0][2] == classification
    assert not json.loads(rows[0][3])["candidate_for_separate_repair"]
    assert state["quarantines"][19745046] == quarantine()


@pytest.mark.parametrize("malformation", ["missing_status", "bad_state", "wrong_team", "timeout"])
def test_failed_observation_consumes_one_attempt_and_only_records_local_diagnostics(monkeypatch, tmp_path, malformation):
    payload = capture()["payload"]
    if malformation == "missing_status": payload.pop("state")
    if malformation == "bad_state": payload["state"] = []
    if malformation == "wrong_team": payload["participants"] = [{"id": 99}, {"id": 238115}]
    if malformation == "timeout": payload = requests.Timeout("secret-token")
    results, calls, rows, _ = run_observation(monkeypatch, tmp_path, payload)
    assert results == [1] and calls.count("mock_HTTP") == 1 and rows[0][1] == 1
    assert rows[0][2] == "blocked_or_failed" and "secret-token" not in rows[0][3]


@pytest.mark.parametrize("budget", [1, 2, 3])
def test_real_observation_orchestration_caps_requests_and_repeats_do_not_rewrite(monkeypatch, tmp_path, budget):
    captures = [capture(fixture_id=fixture) for fixture in (19745046, 9001, 9002)]
    results, calls, rows, _ = run_observation(monkeypatch, tmp_path, capture()["payload"], captures=captures, budget=budget)
    assert results == [0] and calls.count("mock_HTTP") == budget and len(rows) == budget


def test_repeated_active_invocation_consumes_no_unchanged_extra_claim(monkeypatch, tmp_path, capsys):
    results, calls, rows, _ = run_observation(monkeypatch, tmp_path, capture()["payload"], repeats=2)
    assert results == [0, 0] and calls.count("mock_HTTP") == 1 and len(rows) == 1
    reports = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert reports[1]["provider_calls"] == 0 and not reports[1]["fixture_ids"]
    assert "24-hour" in reports[1]["postponement_revalidation"]["rejections"][0]["reason"]


@pytest.mark.parametrize("case", ["stale", "conflict", "malformed", "missing", "unknown", "postponed", "cancelled", "abandoned"])
def test_ineligible_active_input_cannot_claim_or_request(monkeypatch, tmp_path, capsys, case):
    evidence = capture()
    if case == "stale": evidence["observed_at"] = "2026-10-07T15:00:00Z"
    if case == "conflict": evidence["payload"]["state"] = {"developer_name": "FINISHED", "short_name": "POST"}
    if case == "malformed": evidence["payload"]["state"] = []
    if case == "missing": evidence["payload"].pop("state")
    if case in {"unknown", "postponed", "cancelled", "abandoned"}:
        evidence["payload"]["state"] = {"short_name": {"unknown": "UNKNOWN", "postponed": "POST", "cancelled": "CANC", "abandoned": "ABAN"}[case]}
    evidence["payload_sha256"] = delivery.provider_payload_hash(evidence["payload"])
    _, calls, rows, state = run_observation(monkeypatch, tmp_path, full_detail(), captures=[evidence])
    assert calls == [] and rows == [] and state["quarantines"][19745046] == quarantine()
    report = json.loads(capsys.readouterr().out)
    assert report["postponement_revalidation"]["rejections"] and not report["postponement_revalidation"]["observations"]


@pytest.mark.parametrize("flags", [
    ["--postponement-publish-details"],
    ["--postponement-status-evidence", "unused", "--postponement-publish-details"],
    ["--postponement-status-evidence", "unused", "--postponement-publish-details", "--postponement-provider-budget", "1"],
    ["--postponement-observe-status", "--postponement-provider-budget", "1"],
    ["--postponement-status-evidence", "unused", "--postponement-provider-budget", "1"],
    ["--postponement-status-evidence", "unused", "--postponement-observe-status"],
    ["--postponement-status-evidence", "unused", "--postponement-observe-status", "--postponement-provider-budget", "4"],
    ["--postponement-status-evidence", "unused", "--force"],
    ["--postponement-status-evidence", "unused", "--batch-projection"],
    ["--postponement-status-evidence", "unused", "--fixture-ids", "19745046"]])
def test_withdrawn_publication_and_invalid_modes_reject_before_all_access(monkeypatch, flags):
    monkeypatch.setattr(sys, "argv", ["delivery", *flags])
    monkeypatch.setenv("SUPABASE_DB_URL_SESSION", "offline")
    monkeypatch.setattr(delivery, "source_connection", lambda: pytest.fail("Unexpected source access"))
    monkeypatch.setattr(delivery.psycopg2, "connect", lambda *a, **kw: pytest.fail("Unexpected target access"))
    monkeypatch.setattr(queue, "acquire_process_lock", lambda: pytest.fail("Unexpected lock access"))
    with pytest.raises(SystemExit): delivery.main()


def test_observation_busy_lock_prevents_claim_or_request(monkeypatch, tmp_path):
    evidence = tmp_path / "capture.json"
    evidence.write_text(json.dumps([capture()]))
    monkeypatch.setattr(sys, "argv", ["delivery", "--postponement-status-evidence", str(evidence),
        "--postponement-observe-status", "--postponement-provider-budget", "1"])
    monkeypatch.setenv("SUPABASE_DB_URL_SESSION", "offline")
    monkeypatch.setattr(delivery, "SOURCE_DB", str(tmp_path / "source.sqlite"))
    monkeypatch.setattr(delivery, "postponement_exclusions", lambda *a: {19745046: quarantine()})
    monkeypatch.setattr(delivery, "acquire_postponement_lock", lambda: None)
    monkeypatch.setattr(delivery, "source_connection", lambda: pytest.fail("Unexpected source access"))
    with pytest.raises(SystemExit, match="lock is busy"): checker.main()


def test_feature_disabled_by_default():
    args = delivery.build_parser().parse_args([])
    assert args.postponement_status_evidence is None and args.postponement_provider_budget == 0
    assert not args.postponement_observe_status and not args.postponement_publish_details


@pytest.mark.parametrize("flags", [
    ["--postponement-status-evidence="],
    ["--postponement-status-evidence", "   "],
    ["--postponement-status-evidence", "missing-file"],
    ["--postponement-status-evidence", "{malformed}"],
    [], ["--postponement-observe-status"],
    ["--postponement-status-evidence", "unused", "--force"],
    ["--postponement-status-evidence", "unused", "--postponement-publish-details"],
    ["--postponement-status-evidence", "unused", "--postponement-provider-budget", "1"],
    ["--postponement-status-evidence", "unused", "--postponement-observe-status", "--postponement-provider-budget", "0"],
    ["--postponement-status-evidence", "unused", "--postponement-observe-status", "--postponement-provider-budget", "4"],
    ["--postponement-status-evidence", "unused", "--execution-seconds", "121"],
])
def test_dedicated_invalid_command_rejects_before_all_access(monkeypatch, tmp_path, flags):
    if "{malformed}" in flags:
        path = tmp_path / "malformed.json"
        path.write_text("{malformed}")
        flags = [str(path) if arg == "{malformed}" else arg for arg in flags]
    deny = lambda *a, **kw: pytest.fail("Invalid invocation reached a side-effect boundary")
    for name in ("source_connection", "ensure_ledger", "acquire_postponement_lock", "SportMonksClient"):
        monkeypatch.setattr(delivery, name, deny)
    monkeypatch.setattr(delivery.psycopg2, "connect", deny)
    monkeypatch.setenv("SUPABASE_DB_URL_SESSION", "offline")
    monkeypatch.setattr(sys, "argv", ["status", *flags])
    try:
        assert checker.main() != 0
    except SystemExit as exc:
        assert exc.code != 0


@pytest.mark.parametrize("flag", ["--postponement-status-evidence=", "--postponement-provider-budget=0",
                                  "--postponement-publish-details", "--postponement-observe-status"])
def test_ingestion_rejects_status_arguments_before_write_path(monkeypatch, flag):
    monkeypatch.setattr(sys, "argv", ["delivery", flag, "--leagues", "567"])
    monkeypatch.setattr(delivery, "source_connection", lambda: pytest.fail("Ordinary write path reached"))
    with pytest.raises(SystemExit, match="status-only"):
        delivery.main()


def test_normal_ingestion_entry_is_unchanged(monkeypatch):
    class OrdinaryPath(Exception): pass
    def source(): raise OrdinaryPath()
    monkeypatch.setattr(sys, "argv", ["delivery", "--leagues", "567"])
    monkeypatch.setattr(delivery, "source_connection", source)
    monkeypatch.setattr(delivery, "run_postponement_revalidation", lambda *a: pytest.fail("Wrong entry point"))
    with pytest.raises(OrdinaryPath): delivery.main()


def test_inherited_lock_observation_rejected_before_access(monkeypatch):
    monkeypatch.setenv("STATS_RECONCILE_LOCK_HELD", "1")
    monkeypatch.setattr(sys, "argv", ["status", "--postponement-status-evidence", "unused",
        "--postponement-observe-status", "--postponement-provider-budget", "1"])
    monkeypatch.setattr(delivery, "SOURCE_DB", "/tmp/offline-unused.sqlite")
    with pytest.raises(SystemExit) as rejected: checker.main()
    assert rejected.value.code != 0


HUNG_COMMAND = r'''
import json, os, socket, sqlite3, sys, time
from datetime import datetime, timezone
from types import SimpleNamespace
from scripts import postmatch_fixture_detail_delivery as d
from scripts import review_postponement_status as command
deny=lambda *a,**kw: (_ for _ in ()).throw(AssertionError('Real network/publication forbidden'))
socket.socket.connect=socket.create_connection=deny
d.requests.request=deny
for name in ('store_provider_detail','export_fixture','clear_provider_unavailable_exclusion',
             'persist_provider_snapshot','activate_provider_snapshot','refresh_player_projection',
             'publish_delivery_status','ensure_ledger','source_engine','update_ledger'):
    setattr(d,name,deny)
d.utc_now=lambda: datetime(2026,10,9,16,tzinfo=timezone.utc)
d.SOURCE_DB=os.environ['TEST_SPOOL']
stage=os.environ['HANG_STAGE']
def hang():
    print('HUNG',flush=True)
    time.sleep(3600)
class Target:
    def __enter__(self): return self
    def __exit__(self,*a): pass
    def cursor(self): return self
    def execute(self,sql,params):
        assert sql.strip().lower().startswith('select')
        if stage=='database': hang()
    def fetchall(self):
        return [(19745046,'provider_unavailable','2026-10-03T16:37:11Z',
                 '2026-10-03T16:37:11Z',{'provider_status':'POSTPONED'},'postponed',
                 '2026-10-10T16:37:11Z',758,238115)]
def connect(*a,**kw):
    assert kw['options']=='-c default_transaction_read_only=on'
    return Target()
d.psycopg2.connect=connect
class Source(sqlite3.Connection):
    def close(self):
        if stage=='cleanup': hang()
        super().close()
def source():
    if stage=='sqlite': hang()
    return sqlite3.connect(os.environ['TEST_SPOOL'],factory=Source)
d.source_connection=source
d.SportMonksClient=lambda: SimpleNamespace(timeout=20,base_url='https://offline.invalid/',api_token='offline')
def request(*a,**kw):
    if stage=='provider': hang()
    return SimpleNamespace(status_code=200,json=lambda:{'data':json.load(open(sys.argv[2]))[0]['payload']})
d.requests.request=request
if stage=='audit': d.finish_postponement_review=lambda *a,**kw: hang()
raise SystemExit(command.main())
'''


@pytest.mark.parametrize("stage", ["database", "sqlite", "provider", "audit", "cleanup"])
def test_actual_status_command_hard_kill_releases_lock_and_preserves_claim(tmp_path, stage):
    evidence = tmp_path / "capture.json"
    evidence.write_text(json.dumps([capture()]))
    lock = tmp_path / "canonical.lock"
    spool = tmp_path / "audit.sqlite"
    env = {**os.environ, "SUPABASE_DB_URL_SESSION": "offline", "HANG_STAGE": stage,
           "TEST_SPOOL": str(spool), "ODDS_SYNC_LOCK_FILE": str(lock)}
    env.pop("STATS_RECONCILE_LOCK_HELD", None)
    env.pop("STATS_RECONCILE_LOCK_PATH", None)
    args = [sys.executable, "-B", "-c", HUNG_COMMAND, "--postponement-status-evidence", str(evidence),
            "--postponement-observe-status", "--postponement-provider-budget", "1", "--execution-seconds", "1"]
    child = subprocess.Popen(args, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(child.stdout, selectors.EVENT_READ)
            assert selector.select(timeout=5), "Worker did not reach hung operation"
        assert child.stdout.readline().strip() == "HUNG"
        fd = os.open(lock, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            with pytest.raises(BlockingIOError): fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            stdout, stderr = child.communicate(timeout=5)
            assert child.returncode == -signal.SIGALRM, (stdout, stderr)
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally: os.close(fd)
        if stage in {"provider", "audit", "cleanup"}:
            with sqlite3.connect(spool) as db:
                assert db.execute("select attempt from fixture_postponement_reviews").fetchall() == [(1,)]
                assert not delivery.claim_postponement_review(db, quarantine(), capture(), NOW)
    finally:
        if child.poll() is None:
            child.kill()
            child.communicate(timeout=5)


def claim_in_process(path, day):
    deny = lambda *a, **kw: (_ for _ in ()).throw(AssertionError("Offline worker: network forbidden"))
    requests.request = deny
    socket.socket.connect = socket.create_connection = deny
    delivery.psycopg2.connect = deny
    db = sqlite3.connect(path, timeout=30)
    try: return delivery.claim_postponement_review(db, quarantine(), capture(), NOW + timedelta(days=day))
    finally: db.close()


def test_claim_limits_survive_concurrent_processes_and_legacy_audit_upgrade(tmp_path):
    path = tmp_path / "legacy.sqlite"
    with sqlite3.connect(path) as db:
        db.execute("""create table fixture_postponement_reviews(fixture_id integer,exclusion_key text,attempt integer,
            claimed_at text,evidence text,original_exclusion text,outcome text default 'claimed',
            primary key(fixture_id,exclusion_key,attempt))""")
    with concurrent.futures.ProcessPoolExecutor(max_workers=4, mp_context=multiprocessing.get_context("spawn")) as pool:
        for day, expected in ((0, 1), (0, 0), (1, 1), (2, 1), (30, 0)):
            assert sum(pool.map(claim_in_process, [str(path)] * 4, [day] * 4)) == expected
    with sqlite3.connect(path) as db:
        assert db.execute("select count(*) from fixture_postponement_reviews").fetchone()[0] == 3
        assert "observation" in {row[1] for row in db.execute("pragma table_info(fixture_postponement_reviews)")}


@pytest.mark.parametrize("value", [758.9, 758.0, True, False, None, 0, -1, "758", "758.9", "bad"])
@pytest.mark.parametrize("location", ["fixture", "team", "metadata"])
def test_exact_integer_identity_rejected_before_active_writes(monkeypatch, tmp_path, value, location):
    evidence = capture()
    if location == "fixture": evidence["payload"]["id"] = value
    if location == "team": evidence["payload"]["participants"][0]["id"] = value
    evidence["payload_sha256"] = delivery.provider_payload_hash(evidence["payload"])
    if location == "metadata":
        old = quarantine()
        old["home_team_id"] = value
        with pytest.raises(ValueError): delivery.strict_status_metadata(old)
        results, calls, rows, _ = run_observation(monkeypatch, tmp_path, full_detail(), metadata_updates={"home_team_id": value})
        assert results == [1] and calls == ["lock", "target_SELECT"] and rows == []
        return
    deny = lambda *a, **kw: pytest.fail("Malformed identity reached active side effect")
    for name in ("acquire_postponement_lock", "source_connection", "SportMonksClient"):
        monkeypatch.setattr(delivery, name, deny)
    monkeypatch.setattr(delivery.psycopg2, "connect", deny)
    monkeypatch.setenv("SUPABASE_DB_URL_SESSION", "offline")
    file = tmp_path / "evidence.json"
    file.write_text(json.dumps([evidence]))
    monkeypatch.setattr(sys, "argv", ["status", "--postponement-status-evidence", str(file),
        "--postponement-observe-status", "--postponement-provider-budget", "1"])
    assert checker.main() == 1


@pytest.mark.parametrize("value", [758.9, 758.0, True, None, 0, -1, "758", "bad"])
def test_current_provider_invalid_team_cannot_become_candidate(monkeypatch, tmp_path, capsys, value):
    payload = full_detail()
    payload["participants"][0]["id"] = value
    results, calls, rows, _ = run_observation(monkeypatch, tmp_path, payload)
    assert results == [1] and calls.count("mock_HTTP") == 1 and rows[0][2] == "blocked_or_failed"
    report = json.loads(capsys.readouterr().out)
    assert not report["postponement_revalidation"]["observations"][0]["candidate_for_separate_repair"]


@pytest.mark.parametrize("value", [19745046.0, 19745046.9, True, None, 0, -1, "19745046"])
def test_current_provider_fixture_id_is_not_coerced(monkeypatch, tmp_path, value):
    payload = full_detail()
    payload["id"] = value
    results, _, rows, _ = run_observation(monkeypatch, tmp_path, payload, preserve_response_id=True)
    assert results == [1] and rows[0][2] == "blocked_or_failed"


@pytest.mark.parametrize("kind", ["duplicate", "missing", "inconsistent", "reversed"])
def test_duplicate_or_inconsistent_team_identity_is_rejected(kind):
    payload = capture()["payload"]
    if kind == "duplicate": payload["participants"][1]["id"] = 758
    if kind == "missing": payload["participants"][0].pop("id")
    if kind == "inconsistent": payload["participants"][0]["team_id"] = 99
    if kind == "reversed": payload.update(home_team_id=238115, away_team_id=758)
    with pytest.raises(ValueError): delivery.strict_status_identity(payload, quarantine())


PRODUCTION_LOCK_PROCESS = r'''
import fcntl,os,sys,time
fd=os.open(os.environ['ODDS_SYNC_LOCK_FILE'],os.O_CREAT|os.O_RDWR,0o600)
os.dup2(fd,9)
try: fcntl.flock(9,fcntl.LOCK_EX|fcntl.LOCK_NB)
except BlockingIOError: raise SystemExit(75)
print('LOCKED',flush=True)
if len(sys.argv)>1: time.sleep(3600)
'''


def status_process_args(tmp_path, stage="provider"):
    file = tmp_path / "capture.json"
    file.write_text(json.dumps([capture()]))
    env = {**os.environ, "HANG_STAGE": stage, "SUPABASE_DB_URL_SESSION": "offline",
           "TEST_SPOOL": str(tmp_path / "audit.sqlite"), "ODDS_SYNC_LOCK_FILE": str(tmp_path / "canonical.lock")}
    env.pop("STATS_RECONCILE_LOCK_HELD", None)
    env.pop("STATS_RECONCILE_LOCK_PATH", None)
    args = [sys.executable, "-B", "-c", HUNG_COMMAND, "--postponement-status-evidence", str(file),
        "--postponement-observe-status", "--postponement-provider-budget", "1", "--execution-seconds", "2"]
    return args, env


def await_marker(process, expected):
    with selectors.DefaultSelector() as selector:
        selector.register(process.stdout, selectors.EVENT_READ)
        assert selector.select(timeout=5)
    assert process.stdout.readline().strip() == expected


def test_real_production_lock_refuses_observation_without_claim_or_request(tmp_path):
    args, env = status_process_args(tmp_path)
    writer = subprocess.Popen([sys.executable, "-B", "-c", PRODUCTION_LOCK_PROCESS, "hold"],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        await_marker(writer, "LOCKED")
        observer = subprocess.run(args, env=env, capture_output=True, text=True, timeout=5)
        assert observer.returncode == 1 and "lock is busy" in observer.stderr
        assert "HUNG" not in observer.stdout and not (tmp_path / "audit.sqlite").exists()
    finally:
        writer.kill(); writer.communicate(timeout=5)


@pytest.mark.parametrize("termination", ["timeout", "crash"])
def test_real_observation_refuses_production_then_releases_lock(tmp_path, termination):
    args, env = status_process_args(tmp_path)
    observer = subprocess.Popen(args, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        await_marker(observer, "HUNG")
        writer = subprocess.run([sys.executable, "-B", "-c", PRODUCTION_LOCK_PROCESS], env=env,
            capture_output=True, text=True, timeout=5)
        assert writer.returncode == 75
        if termination == "crash": observer.kill()
        observer.communicate(timeout=5)
        assert observer.returncode == (-signal.SIGKILL if termination == "crash" else -signal.SIGALRM)
        writer = subprocess.run([sys.executable, "-B", "-c", PRODUCTION_LOCK_PROCESS], env=env,
            capture_output=True, text=True, timeout=5)
        assert writer.returncode == 0
        with sqlite3.connect(tmp_path / "audit.sqlite") as db:
            assert db.execute("select attempt,outcome from fixture_postponement_reviews").fetchall() == [(1, "claimed")]
            assert not delivery.claim_postponement_review(db, quarantine(), capture(), NOW)
    finally:
        if observer.poll() is None: observer.kill(); observer.communicate(timeout=5)


@pytest.mark.parametrize("stage", ["success", "provider"])
def test_wrapper_inherited_fd9_is_verified_without_self_deadlock(tmp_path, stage):
    args, env = status_process_args(tmp_path, stage)
    env.update(STATS_RECONCILE_LOCK_HELD="1", ODDS_SYNC_P3_MAX_DURATION_SECONDS="120")
    wrapper = r'''
import fcntl,json,os,subprocess,sys
fd=os.open(os.environ['ODDS_SYNC_LOCK_FILE'],os.O_CREAT|os.O_RDWR,0o600)
os.dup2(fd,9); os.set_inheritable(9,True); fcntl.flock(9,fcntl.LOCK_EX|fcntl.LOCK_NB)
try:
    result=subprocess.run(json.loads(sys.argv[1]),pass_fds=(9,),capture_output=True,text=True,timeout=5)
    print(result.stdout,end=''); print(result.stderr,file=sys.stderr,end='')
    raise SystemExit(result.returncode)
finally: os.close(9)
'''
    result = subprocess.run([sys.executable, "-B", "-c", wrapper, json.dumps(args)], env=env,
        capture_output=True, text=True, timeout=8)
    if stage == "success":
        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout)["provider_calls"] == 1
    else:
        assert result.returncode != 0 and "HUNG" in result.stdout
        with sqlite3.connect(tmp_path / "audit.sqlite") as db:
            assert db.execute("select attempt from fixture_postponement_reviews").fetchall() == [(1,)]
    writer = subprocess.run([sys.executable, "-B", "-c", PRODUCTION_LOCK_PROCESS], env=env,
        capture_output=True, text=True, timeout=5)
    assert writer.returncode == 0


@pytest.mark.parametrize("configuration", ["relative", "conflicting", "false_inherited"])
def test_unsafe_lock_configuration_is_rejected(tmp_path, monkeypatch, configuration):
    monkeypatch.setenv("ODDS_SYNC_LOCK_FILE", str(tmp_path / "canonical.lock"))
    monkeypatch.delenv("STATS_RECONCILE_LOCK_HELD", raising=False)
    monkeypatch.delenv("STATS_RECONCILE_LOCK_PATH", raising=False)
    if configuration == "relative": monkeypatch.setenv("ODDS_SYNC_LOCK_FILE", "relative.lock")
    if configuration == "conflicting": monkeypatch.setenv("STATS_RECONCILE_LOCK_PATH", str(tmp_path / "other.lock"))
    if configuration == "false_inherited": monkeypatch.setenv("STATS_RECONCILE_LOCK_HELD", "1")
    with pytest.raises(SystemExit): delivery.acquire_postponement_lock()


def test_changed_cwd_and_symlink_alias_share_canonical_inode(tmp_path):
    args, env = status_process_args(tmp_path)
    canonical = tmp_path / "canonical.lock"
    canonical.touch()
    alias = tmp_path / "alias.lock"
    alias.symlink_to(canonical)
    env["STATS_RECONCILE_LOCK_PATH"] = str(alias)
    writer = subprocess.Popen([sys.executable, "-B", "-c", PRODUCTION_LOCK_PROCESS, "hold"], env=env,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        await_marker(writer, "LOCKED")
        # Absolute module location keeps the controlled command importable in another cwd.
        env["PYTHONPATH"] = str(__import__("pathlib").Path(__file__).resolve().parents[1])
        result = subprocess.run(args, cwd=tmp_path, env=env, capture_output=True, text=True, timeout=5)
        assert result.returncode == 1 and "lock is busy" in result.stderr
        assert not (tmp_path / "audit.sqlite").exists()
    finally: writer.kill(); writer.communicate(timeout=5)


def test_relative_spool_cannot_reset_audit_through_changed_cwd(tmp_path):
    args, env = status_process_args(tmp_path)
    env["TEST_SPOOL"] = "relative.sqlite"
    result = subprocess.run(args, env=env, capture_output=True, text=True, timeout=5)
    assert result.returncode != 0 and "absolute persistent JXD_DB_PATH" in result.stderr
    assert not (tmp_path / "canonical.lock").exists()


@pytest.mark.parametrize("kind", ["reversed", "duplicate", "unknown", "malformed"])
def test_provider_participant_locations_must_match_trusted_sides(monkeypatch, tmp_path, kind):
    payload = full_detail()
    payload["participants"][0]["meta"] = {"location": "away" if kind == "reversed" else "home"}
    payload["participants"][1]["meta"] = {"location": "home" if kind == "reversed" else "away"}
    if kind == "duplicate": payload["participants"][1]["meta"]["location"] = "home"
    if kind == "unknown": payload["participants"][1]["meta"]["location"] = "unknown"
    if kind == "malformed": payload["participants"][1]["meta"] = []
    results, _, rows, _ = run_observation(monkeypatch, tmp_path, payload)
    assert results == [1] and rows[0][2] == "blocked_or_failed"


def test_metadata_is_read_only_after_canonical_lock(monkeypatch, tmp_path):
    results, calls, _, _ = run_observation(monkeypatch, tmp_path, full_detail())
    assert results == [0] and calls[:2] == ["lock", "target_SELECT"]


EPISODE_FORMS = ["2026-10-03T16:37:11Z", "2026-10-03T16:37:11+00:00",
                 "2026-10-03T17:37:11+01:00", "2026-10-03T18:37:11+02:00",
                 "2026-10-03T19:37:11+03:00"]


def legacy_audit(db, forms, times):
    db.execute("""create table fixture_postponement_reviews (
        fixture_id integer, exclusion_key text, attempt integer, claimed_at text,
        evidence text, original_exclusion text, outcome text default 'claimed', observation text,
        primary key(fixture_id, exclusion_key, attempt))""")
    for index, (form, claimed) in enumerate(zip(forms, times)):
        original = quarantine()
        original["first_identified_at"] = form
        db.execute("insert into fixture_postponement_reviews values(?,?,?,?,?,?,?,?)",
                   (19745046, form, index + 1, claimed.isoformat(), json.dumps(capture()),
                    json.dumps(original), "claimed", None))
    db.commit()


@pytest.mark.parametrize("form", EPISODE_FORMS)
def test_equivalent_episode_same_instant_cannot_claim_again(form):
    db = sqlite3.connect(":memory:")
    assert delivery.claim_postponement_review(db, quarantine(), capture(), NOW)
    other = quarantine()
    other["first_identified_at"] = form
    assert not delivery.claim_postponement_review(db, other, capture(), NOW)
    assert not delivery.claim_postponement_review(db, other, capture(), NOW + timedelta(hours=23))
    assert db.execute("select count(*) from fixture_postponement_reviews").fetchone()[0] == 1


@pytest.mark.parametrize("form", EPISODE_FORMS)
@pytest.mark.parametrize("attempts", [1, 3])
def test_legacy_equivalent_attempts_preserved_and_counted(form, attempts):
    db = sqlite3.connect(":memory:")
    legacy_audit(db, EPISODE_FORMS[:attempts], [NOW + timedelta(days=i) for i in range(attempts)])
    before = db.execute("select * from fixture_postponement_reviews").fetchall()
    other = quarantine()
    other["first_identified_at"] = form
    assert not delivery.claim_postponement_review(db, other, capture(), NOW + timedelta(days=attempts-1, hours=23))
    result = delivery.claim_postponement_review(db, other, capture(), NOW + timedelta(days=10))
    assert result == (attempts < 3)
    after = db.execute("select * from fixture_postponement_reviews").fetchall()
    assert after[:attempts] == before
    if result:
        assert after[-1][1:3] == ("2026-10-03T16:37:11.000000+00:00", 2)


@pytest.mark.parametrize("bad", [None, True, 123, "2026-10-03T16:37:11", "bad",
    "2026-10-03T16:37:11.1234567Z", "2026-10-03T16:37:11-00:00", "2026-10-03T16:37:11+00:60"])
def test_unsafe_episode_identity_rejected_without_claim(bad):
    db = sqlite3.connect(":memory:")
    old = quarantine()
    old["first_identified_at"] = bad
    with pytest.raises(ValueError): delivery.claim_postponement_review(db, old, capture(), NOW)
    assert not db.execute("select name from sqlite_master where type='table'").fetchall()


@pytest.mark.parametrize("bad", ["bad", "2026-10-03T16:37:11", "2026-10-03T16:37:11.1234567Z"])
def test_unreconciled_legacy_history_fails_closed_without_rewrite(bad):
    db = sqlite3.connect(":memory:")
    legacy_audit(db, [bad], [NOW])
    before = db.execute("select * from fixture_postponement_reviews").fetchall()
    assert not delivery.claim_postponement_review(db, quarantine(), capture(), NOW + timedelta(days=10))
    assert db.execute("select * from fixture_postponement_reviews").fetchall() == before


def test_exact_claim_completion_cannot_complete_another_episode():
    db = sqlite3.connect(":memory:")
    first, second = {}, {}
    assert delivery.claim_postponement_review(db, quarantine(), capture(), NOW, first)
    other = quarantine()
    other["first_identified_at"] = "2026-10-03T16:37:11.000001Z"
    assert delivery.claim_postponement_review(db, other, capture(), NOW, second)
    delivery.finish_postponement_review(db, first, "blocked_or_failed")
    rows = db.execute("select exclusion_key,outcome from fixture_postponement_reviews order by rowid").fetchall()
    assert rows == [(first["exclusion_key"], "blocked_or_failed"), (second["exclusion_key"], "claimed")]
    with pytest.raises(ValueError): delivery.finish_postponement_review(db, first, "verified")
    assert db.execute("select outcome from fixture_postponement_reviews where exclusion_key=?", (second["exclusion_key"],)).fetchone() == ("claimed",)


def test_spacing_preserves_subsecond_precision():
    db = sqlite3.connect(":memory:")
    now = NOW.replace(microsecond=999999)
    assert delivery.claim_postponement_review(db, quarantine(), capture(), now)
    assert not delivery.claim_postponement_review(db, quarantine(), capture(), NOW + timedelta(days=1))
    assert delivery.claim_postponement_review(db, quarantine(), capture(), now + timedelta(days=1))


def equivalent_claim_in_process(path, form, day):
    deny = lambda *a, **kw: (_ for _ in ()).throw(AssertionError("Offline network forbidden"))
    requests.request = socket.socket.connect = socket.create_connection = delivery.psycopg2.connect = deny
    old = quarantine()
    old["first_identified_at"] = form
    with sqlite3.connect(path, timeout=30) as db:
        return delivery.claim_postponement_review(db, old, capture(), NOW + timedelta(days=day))


def test_concurrent_equivalent_episode_claims_share_one_durable_budget(tmp_path):
    path = str(tmp_path / "equivalent.sqlite")
    with concurrent.futures.ProcessPoolExecutor(max_workers=5, mp_context=multiprocessing.get_context("spawn")) as pool:
        for day, expected in [(0, 1), (0, 0), (1, 1), (2, 1), (20, 0)]:
            assert sum(pool.map(equivalent_claim_in_process, [path]*5, EPISODE_FORMS, [day]*5)) == expected
    with sqlite3.connect(path) as db:
        assert db.execute("select attempt from fixture_postponement_reviews order by attempt").fetchall() == [(1,), (2,), (3,)]


@pytest.mark.parametrize("inherited", [False, True])
def test_blocked_sigalrm_command_dies_releases_flock_and_preserves_claim(tmp_path, inherited):
    args, env = status_process_args(tmp_path)
    args[-1] = "1"
    args[3] = args[3].replace("raise SystemExit(command.main())",
        "signal.pthread_sigmask(signal.SIG_BLOCK,{signal.SIGALRM}); raise SystemExit(command.main())")
    # HUNG_COMMAND imports signal only in this adversarial launcher.
    args[3] = "import signal\n" + args[3]
    wrapper = r'''
import fcntl,json,os,signal,sys
fd=os.open(os.environ['ODDS_SYNC_LOCK_FILE'],os.O_CREAT|os.O_RDWR,0o600)
os.dup2(fd,9); os.set_inheritable(9,True); os.close(fd)
fcntl.flock(9,fcntl.LOCK_EX|fcntl.LOCK_NB)
signal.pthread_sigmask(signal.SIG_BLOCK,{signal.SIGALRM})
args=json.loads(sys.argv[1]); os.execv(args[0],args)
'''
    if inherited:
        env.update(STATS_RECONCILE_LOCK_HELD="1", ODDS_SYNC_P3_MAX_DURATION_SECONDS="120")
        args = [sys.executable, "-B", "-c", wrapper, json.dumps(args)]
    child = subprocess.Popen(args, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        await_marker(child, "HUNG")
        refused = subprocess.run([sys.executable,"-B","-c",PRODUCTION_LOCK_PROCESS], env=env,
                                 capture_output=True, text=True, timeout=5)
        assert refused.returncode == 75
        stdout, stderr = child.communicate(timeout=4)
        assert child.returncode == -signal.SIGALRM, (stdout, stderr)
        acquired = subprocess.run([sys.executable,"-B","-c",PRODUCTION_LOCK_PROCESS], env=env,
                                  capture_output=True,text=True,timeout=5)
        assert acquired.returncode == 0
        with sqlite3.connect(tmp_path / "audit.sqlite") as db:
            assert db.execute("select attempt,outcome from fixture_postponement_reviews").fetchall() == [(1,"claimed")]
            assert not delivery.claim_postponement_review(db, quarantine(), capture(), NOW)
    finally:
        if child.poll() is None: child.kill(); child.communicate(timeout=5)


def test_signal_unblock_failure_stops_before_any_active_work(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["status", "--postponement-status-evidence", "unused",
        "--postponement-observe-status", "--postponement-provider-budget", "1"])
    monkeypatch.setattr(signal, "pthread_sigmask", lambda *a: {signal.SIGALRM})
    monkeypatch.setattr(delivery, "run_postponement_revalidation", lambda *a: pytest.fail("Deadline failure reached active work"))
    with pytest.raises(SystemExit, match="Cannot establish hard observation deadline"): checker.main()
