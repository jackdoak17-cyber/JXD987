from __future__ import annotations

import copy
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
             local_stats=0, local_lineups=0):
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
    monkeypatch.setattr(delivery, "persist_provider_snapshot", lambda *a, **kw: events.append("snapshot") or 1)
    def store(*args):
        events.append("store")
        if store_failure: raise delivery.ProviderDetailIncompleteError("partial source")
        return "source"
    monkeypatch.setattr(delivery, "store_provider_detail", store)
    monkeypatch.setattr(delivery, "export_fixture", lambda *a, **kw: events.append("export") or
                        SimpleNamespace(returncode=int(export_failure), stderr="offline failure", stdout=""))
    monkeypatch.setattr(delivery.psycopg2, "connect", lambda *a, **kw: SimpleNamespace(close=lambda: None))
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
    assert result == 0 and events.count("request") == 1 and "snapshot" in events
    assert not {"store", "export", "clear", "activate", "projection", "ledger", "quarantine"}.intersection(events)


@pytest.mark.parametrize("payload", [full_detail("POST"), full_detail("CANC"), capture()["payload"], TimeoutError("offline")])
def test_review_only_does_not_mutate_delivery_state_even_on_failure(monkeypatch, tmp_path, payload):
    _, events = run_main(monkeypatch, tmp_path, copy.deepcopy(payload), publish=False)
    assert events.count("request") == 1
    assert not {"store", "export", "clear", "activate", "projection", "ledger", "quarantine"}.intersection(events)


def test_busy_canonical_lock_stops_before_database_or_provider(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["delivery", "--postponement-status-evidence", "unused", "--postponement-provider-budget", "1"])
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
