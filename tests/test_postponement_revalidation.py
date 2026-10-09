from __future__ import annotations

import copy
import concurrent.futures
import json
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
import requests

from scripts import postmatch_fixture_detail_delivery as delivery
from scripts import reconcile_stats_provider_queue as queue


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
        assert delivery.claim_postponement_review(first, original, capture(), now)
        assert not delivery.claim_postponement_review(second, original, capture(), now)
        assert not delivery.claim_postponement_review(second, original, capture(), now + timedelta(hours=23))
        delivery.finish_postponement_review(first, original["fixture_id"], "blocked_or_failed")
    assert not delivery.claim_postponement_review(second, original, capture(), NOW + timedelta(days=30))
    rows = first.execute("select original_exclusion, evidence, outcome from fixture_postponement_reviews").fetchall()
    assert len(rows) == 3
    assert all(json.loads(row[0]) == original for row in rows)
    first.close()
    second.close()


def test_successful_review_cannot_be_reclaimed():
    db = sqlite3.connect(":memory:")
    assert delivery.claim_postponement_review(db, quarantine(), capture(), NOW)
    delivery.finish_postponement_review(db, 19745046, "verified")
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
    assert delivery.claim_postponement_review(db, quarantine(), capture(), NOW + timedelta(days=1))
    delivery.finish_postponement_review(db, 19745046, "verified")
    assert db.execute("select outcome from fixture_postponement_reviews order by attempt").fetchall() == [("claimed",), ("verified",)]


def full_detail(status="FT"):
    payload = capture(status)["payload"]
    payload["statistics"] = [{"participant_id": team, "type_id": stat, "data": {"value": 1}}
                             for team in (758, 238115) for stat in delivery.TRACKED_TEAM_STAT_TYPES]
    payload["lineups"] = [{"team_id": team, "player_id": player,
                           "details": [{"type_id": 119, "data": {"value": 90}}]}
                          for team, player in ((758, 11), (238115, 22))]
    return payload


def run_main(monkeypatch, tmp_path, payload, *, export_failure=False, parity_failure=False, store_failure=False, publish=True,
             local_stats=0, local_lineups=0, check_snapshot_protection=False):
    events = []
    db = sqlite3.connect(":memory:")
    db.execute("create table fixtures(id integer, league_id integer, season_id integer, starting_at text)")
    db.execute("insert into fixtures values(19745046,567,28479,'2026-10-04 19:00:00')")
    db.execute("create table fixture_statistics(fixture_id integer,team_id integer,type_id integer,value numeric)")
    db.execute("create table fixture_player_statistics(fixture_id integer,player_id integer,team_id integer,type_id integer,value numeric)")
    db.execute("create table fixture_players(fixture_id integer,player_id integer,team_id integer,is_starter integer,minutes_played integer)")
    for player in range(local_stats):
        db.execute("insert into fixture_player_statistics values(19745046,?,758,42,1)", (player + 1,))
    for player in range(local_lineups):
        db.execute("insert into fixture_players values(19745046,?,758,1,90)", (player + 1,))
    if publish:
        delivery.ensure_ledger(db)
        assert delivery.claim_postponement_review(db, quarantine(), capture(), NOW)
    monkeypatch.setattr(delivery, "utc_now", lambda: NOW)
    monkeypatch.setattr(delivery, "source_connection", lambda: db)
    monkeypatch.setattr(queue, "acquire_process_lock", lambda: 0)
    monkeypatch.setattr(delivery, "select_postponement_reviews", lambda *a: [19745046])
    monkeypatch.setattr(delivery, "target_fixture_metadata", lambda *a: {})
    monkeypatch.setattr(delivery, "source_engine", lambda *a: SimpleNamespace(dispose=lambda: None))
    evidence = tmp_path / "status.json"
    evidence.write_text(json.dumps([capture()]))
    monkeypatch.setattr(sys, "argv", ["delivery", "--leagues", "567", "--postponement-status-evidence", str(evidence),
                                    "--postponement-provider-budget", "1", "--no-fail-on-sla-breach"]
                        + (["--postponement-publish-details"] if publish else []))
    monkeypatch.setenv("SUPABASE_DB_URL_SESSION", "offline-test")
    class Client:
        timeout = 90
        max_retries = 5
        rate_limit_retries = 5
        def request(self, *args, **kwargs):
            assert self.max_retries == self.rate_limit_retries == 1 and self.timeout == 20
            events.append("request")
            if isinstance(payload, Exception): raise payload
            return {"data": payload}
    monkeypatch.setattr(delivery, "SportMonksClient", Client)
    monkeypatch.setattr(delivery, "postponement_provider_request", lambda client, *a, **kw: client.request(*a, **kw))
    def snapshot_writer(*args, **kwargs):
        if check_snapshot_protection: assert kwargs.get("preserve_accepted") is True
        events.append("snapshot")
        return 1
    monkeypatch.setattr(delivery, "persist_provider_snapshot", snapshot_writer)
    def store(*args):
        events.append("store")
        if store_failure: raise delivery.ProviderDetailIncompleteError("partial source")
        return "source"
    monkeypatch.setattr(delivery, "store_provider_detail", store)
    monkeypatch.setattr(delivery, "export_fixture", lambda *a, **kw: events.append("export") or
                        SimpleNamespace(returncode=int(export_failure), stderr="offline failure", stdout=""))
    class Target:
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def close(self): pass
        def cursor(self): return self
        def execute(self, sql, params): assert sql.strip().lower().startswith("select")
        def fetchall(self):
            old = quarantine()
            return [tuple(old[key] for key in ("fixture_id", "exclusion_type", "first_identified_at", "last_checked_at", "evidence",
                "reason", "next_review_at", "home_team_id", "away_team_id")) + (0,)]
    monkeypatch.setattr(delivery.psycopg2, "connect", lambda *a, **kw: Target())
    monkeypatch.setattr(delivery, "target_snapshot", lambda *a: "target")
    monkeypatch.setattr(delivery, "compare_snapshots", lambda *a: events.append("parity") or (["mismatch"] if parity_failure else []))
    monkeypatch.setattr(delivery, "clear_provider_unavailable_exclusion", lambda *a: events.append("clear"))
    monkeypatch.setattr(delivery, "clear_revalidated_postponement", lambda *a: events.append("clear"))
    monkeypatch.setattr(delivery, "refresh_player_projection", lambda *a: events.append("projection") or 0)
    monkeypatch.setattr(delivery, "activate_provider_snapshot", lambda *a: events.append("activate"))
    monkeypatch.setattr(delivery, "publish_delivery_status", lambda *a: None)
    monkeypatch.setattr(delivery, "update_ledger", lambda *a, **kw: events.append("ledger"))
    monkeypatch.setattr(delivery, "mark_provider_unavailable", lambda *a, **kw: events.append("quarantine") or "later")
    result = delivery.main()
    return result, events


@pytest.mark.parametrize("scenario", ["postponed", "cancelled", "abandoned", "incomplete", "timeout", "partial", "wrong_fixture"])
def test_actual_runner_never_exports_failed_or_incomplete_review(monkeypatch, tmp_path, scenario):
    payload = full_detail()
    if scenario in {"postponed", "cancelled", "abandoned"}:
        payload["state"]["short_name"] = {"postponed": "POST", "cancelled": "CANC", "abandoned": "ABAN"}[scenario]
    if scenario == "incomplete": payload["lineups"] = []
    if scenario == "timeout": payload = TimeoutError("offline timeout")
    if scenario == "wrong_fixture": payload["id"] = 9000
    _, events = run_main(monkeypatch, tmp_path, payload, store_failure=scenario == "partial")
    assert events.count("request") == 1
    assert "export" not in events and "clear" not in events


@pytest.mark.parametrize("failure", ["export", "parity"])
def test_publication_failure_retains_quarantine(monkeypatch, tmp_path, failure):
    result, events = run_main(monkeypatch, tmp_path, full_detail(), export_failure=failure == "export", parity_failure=failure == "parity")
    assert result == 1
    assert "clear" not in events and "activate" not in events


def test_complete_review_uses_existing_checks_before_clearing(monkeypatch, tmp_path):
    result, events = run_main(monkeypatch, tmp_path, full_detail())
    assert result == 0
    assert events.index("snapshot") < events.index("store") < events.index("export") < events.index("parity") < events.index("clear")
    assert events.index("clear") < events.index("activate")


@pytest.mark.parametrize("baseline", [{"local_stats": 10}, {"local_lineups": 3}])
def test_richer_unpublished_local_facts_require_shrink_confirmation(monkeypatch, tmp_path, baseline):
    _, events = run_main(monkeypatch, tmp_path, full_detail(), **baseline)
    assert "snapshot" in events and "store" not in events and "export" not in events and "clear" not in events


def test_provider_review_alone_cannot_authorize_data_repair(monkeypatch, tmp_path):
    result, events = run_main(monkeypatch, tmp_path, full_detail(), publish=False)
    assert result == 0 and not events


@pytest.mark.parametrize("payload", [full_detail("POST"), full_detail("CANC"), capture()["payload"], TimeoutError("offline")])
def test_review_only_does_not_mutate_delivery_state_even_on_failure(monkeypatch, tmp_path, payload):
    _, events = run_main(monkeypatch, tmp_path, copy.deepcopy(payload), publish=False)
    assert not events


def test_busy_canonical_lock_stops_before_database_or_provider(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["delivery", "--postponement-status-evidence", "unused", "--postponement-provider-budget", "1",
                                    "--postponement-publish-details"])
    monkeypatch.setattr(queue, "acquire_process_lock", lambda: None)
    monkeypatch.setattr(delivery, "source_connection", lambda: pytest.fail("unexpected database access"))
    with pytest.raises(SystemExit, match="lock is busy"): delivery.main()


def test_default_disabled_and_conflicting_flags_fail_before_access(monkeypatch):
    args = delivery.build_parser().parse_args([])
    assert args.postponement_status_evidence is None and args.postponement_provider_budget == 0
    assert not args.postponement_publish_details
    monkeypatch.setattr(delivery, "source_connection", lambda: pytest.fail("unexpected database access"))
    for flags in (["--postponement-status-evidence", "unused"],
                  ["--postponement-provider-budget", "1"],
                  ["--postponement-status-evidence", "unused", "--postponement-provider-budget", "4"],
                  ["--postponement-status-evidence", "unused", "--postponement-provider-budget", "1", "--force"]):
        monkeypatch.setattr(sys, "argv", ["delivery", *flags])
        with pytest.raises(SystemExit): delivery.main()


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


@pytest.mark.parametrize("rowcount", [0, 1])
def test_clear_requires_exact_original_quarantine_and_fixture(monkeypatch, rowcount):
    db = sqlite3.connect(":memory:")
    delivery.claim_postponement_review(db, quarantine(), capture(), NOW)
    class Cursor:
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def execute(self, sql, params):
            assert "fixture_id = %s" in sql and params[0] == 19745046
            assert "first_identified_at =" in sql and "last_checked_at =" in sql and "evidence =" in sql
            assert "exclusion_type = 'provider_unavailable'" in sql
        def __init__(self): self.rowcount = rowcount
    class Target:
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def cursor(self): return Cursor()
    monkeypatch.setattr(delivery.psycopg2, "connect", lambda *a, **kw: Target())
    if rowcount:
        delivery.clear_revalidated_postponement("offline", db, 19745046)
    else:
        with pytest.raises(delivery.ProviderDetailIncompleteError, match="Quarantine changed"):
            delivery.clear_revalidated_postponement("offline", db, 19745046)


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
                 "clear_revalidated_postponement", "activate_provider_snapshot", "update_ledger",
                 "publish_delivery_status", "mark_provider_unavailable", "refresh_player_projection"):
        monkeypatch.setattr(delivery, name, forbidden(name))
    monkeypatch.setattr(queue, "acquire_process_lock", forbidden("lock"))
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
                                    "--postponement-provider-budget", "1", *extra_flags])
    results = [delivery.main() for _ in range(repeats)]
    assert state == before
    assert all(call == "SELECT" for call in calls)
    assert sorted(p.name for p in tmp_path.iterdir()) == ["capture.json"]
    return results, calls


def test_read_only_real_main_preserves_accepted_snapshot_and_all_persistent_state(monkeypatch, tmp_path, capsys):
    results, calls = run_read_only(monkeypatch, tmp_path, capture(), repeats=3)
    assert results == [0, 0, 0] and calls == ["SELECT"] * 3
    reports = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert all(report["provider_calls"] == 0 for report in reports)
    assert all(report["postponement_revalidation"]["observations"][0]["assessment"]["status"] == "provider_pending" for report in reports)


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
    with pytest.raises(SystemExit, match="writes no report files"):
        run_read_only(monkeypatch, tmp_path, capture(), extra_flags=("--report-json", str(tmp_path / "report.json")))
    assert not (tmp_path / "report.json").exists()


@pytest.mark.parametrize("field", ["developer_name", "state", "short_name", "status", "status_code"])
def test_actual_active_runner_rejects_conflicting_fresh_status_before_snapshot_or_export(monkeypatch, tmp_path, capsys, field):
    payload = full_detail()
    payload["state"] = {"developer_name": "FINISHED", "short_name": "FT"}
    if field in {"developer_name", "state", "short_name"}: payload["state"][field] = "POST"
    else: payload[field] = "POST"
    result, events = run_main(monkeypatch, tmp_path, payload)
    assert result == 0 and events.count("request") == 1
    assert not {"snapshot", "store", "export", "clear", "activate", "projection"}.intersection(events)
    report = json.loads(capsys.readouterr().out)
    assert "Contradictory" in report["provider_pending"][0]["reason"]


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
                assert "where not (%s and fixture_detail_snapshots.quality_status = 'accepted')" in sql
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


def test_consistent_final_aliases_do_not_bypass_detail_completeness(monkeypatch, tmp_path):
    payload = capture()["payload"]
    payload["state"] = {"developer_name": "FINISHED", "short_name": "FT"}
    _, events = run_main(monkeypatch, tmp_path, payload)
    assert events.count("request") == 1 and "snapshot" in events
    assert not {"store", "export", "clear", "activate"}.intersection(events)


def test_actual_active_orchestration_enables_accepted_snapshot_protection(monkeypatch, tmp_path):
    result, events = run_main(monkeypatch, tmp_path, full_detail(), check_snapshot_protection=True)
    assert result == 0 and "snapshot" in events and "activate" in events
