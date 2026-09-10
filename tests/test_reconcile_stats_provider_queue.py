from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

from scripts import reconcile_stats_provider_queue as queue
from scripts import postmatch_fixture_detail_delivery as postmatch
from scripts.postmatch_fixture_detail_delivery import (
    HANDOFF_REQUEUE_REASON,
    LEGACY_PENDING_REASON,
    ensure_ledger,
    hydrate_missing_source_delivery_history,
    recover_stale_running,
    repair_legacy_ledger,
    interleave_candidate_lanes,
    interleave_retry_lanes,
    target_candidate_quotas,
    target_retry_lane_quotas,
)


class FakeSportMonksClient:
    calls: list[str] = []
    responses: dict[str, list[int]] = {}

    def __init__(self) -> None:
        pass

    def request(self, method: str, endpoint: str, params: dict[str, object]) -> dict[str, object]:
        assert method == "GET"
        assert params["include"] == queue.DETAIL_INCLUDE
        self.calls.append(endpoint)
        ids = [int(value) for value in endpoint.rsplit("/", 1)[1].split(",")]
        returned = self.responses.get(endpoint, ids)
        rows = [{"id": fixture_id, "statistics": [], "lineups": []} for fixture_id in returned]
        return {"data": rows if "/multi/" in endpoint else rows[0]}


def test_target_candidate_quotas_reserve_historical_progress() -> None:
    assert target_candidate_quotas(50) == (40, 10)
    assert target_candidate_quotas(50, urgent_retry_count=30) == (20, 30)
    assert target_candidate_quotas(50, urgent_retry_count=100) == (10, 40)
    assert target_candidate_quotas(1) == (1, 0)
    assert target_candidate_quotas(0) == (0, 0)


def test_retry_lane_quotas_reserve_each_failure_category() -> None:
    assert target_retry_lane_quotas(10) == (3, 4, 3)
    assert target_retry_lane_quotas(10, urgent_pending_count=7) == (1, 8, 1)
    assert target_retry_lane_quotas(2) == (1, 1, 0)
    assert target_retry_lane_quotas(0) == (0, 0, 0)


def test_retry_categories_are_interleaved_before_cohort_cap() -> None:
    assert interleave_retry_lanes(
        [101, 102, 103],
        [201, 202, 203, 204],
        [301, 302, 303],
    ) == [101, 201, 301, 102, 202, 302, 103, 203, 303, 204]


def test_candidate_lanes_put_retries_before_a_later_cohort_cap() -> None:
    assert interleave_candidate_lanes(
        list(range(1, 9)),
        [101, 102],
    ) == [101, 1, 2, 3, 4, 102, 5, 6, 7, 8]


def test_target_selection_requeues_legacy_accepted_rows_for_v2_evidence(
    monkeypatch,
) -> None:
    queries: list[str] = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, statement, params):
            del params
            queries.append(statement)

        def fetchall(self):
            return []

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def cursor(self):
            return Cursor()

    monkeypatch.setattr(postmatch.psycopg2, "connect", lambda *args, **kwargs: Connection())

    assert postmatch.candidate_target_fixture_ids("postgres://target", [8], 10) == []
    retry_queries = [query for query in queries if "next_attempt_at" in query]
    assert retry_queries
    assert any("coalesce(d.delivery_contract_version, 1) < 2" in query for query in queries)
    assert any("d.player_stat_parity is distinct from true" in query for query in queries)
    assert any("d.lineup_parity is distinct from true" in query for query in queries)
    assert any("d.status in ('failed', 'export_failed'" in query for query in queries)
    assert any("d.status = 'running'" in query for query in queries)
    assert any("now() - interval '30 minutes'" in query for query in queries)
    assert any("d.status = 'excluded'" in query for query in queries)
    assert any("d.next_attempt_at <= now()" in query for query in queries)
    assert any("d.status = 'provider_pending'" in query for query in queries)
    pending_query = next(query for query in queries if "d.status = 'provider_pending'" in query)
    assert "d.stable_fetch_count = 1" in pending_query
    assert "now() - interval '15 minutes'" in pending_query
    assert "d.first_seen_at <= now() - interval '24 hours'" in pending_query
    assert "f.starting_at >= date_trunc('day', now()) - interval '30 days'" in pending_query
    assert pending_query.index("coalesce(d.next_attempt_at") < pending_query.index("f.season_id desc")


def test_ledger_upgrade_does_not_promote_pre_contract_rows_to_v2() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "create table fixture_detail_deliveries ("
        "fixture_id integer primary key, status text not null, "
        "first_seen_at text not null, updated_at text not null)"
    )
    conn.execute(
        "insert into fixture_detail_deliveries values (1, 'verified', '2026-08-24T10:00:00Z', '2026-08-24T10:00:00Z')"
    )

    ensure_ledger(conn)

    row = conn.execute(
        "select delivery_contract_version,reason_code from fixture_detail_deliveries where fixture_id=1"
    ).fetchone()
    assert row == (1, "legacy_unclassified")


def test_cohort_limit_preserves_clustered_prefix() -> None:
    metadata = {
        1: (8, 25583, None, 0, 0),
        2: (8, 25583, None, 0, 0),
        3: (387, 26164, None, 0, 0),
        4: (648, 23265, None, 0, 0),
    }

    assert queue.cohort_limited_fixture_ids([1, 2, 3, 4], metadata, 2) == [1, 2, 3]
    assert queue.cohort_limited_fixture_ids([1, 4, 2, 3], metadata, 2) == [1, 4, 2]
    assert queue.cohort_limited_fixture_ids([1, 2, 3, 4], metadata, 0) == [1, 2, 3, 4]


def test_stale_running_rows_are_requeued_with_a_reason() -> None:
    conn = sqlite3.connect(":memory:")
    ensure_ledger(conn)
    now = datetime(2026, 8, 24, 18, 0, tzinfo=timezone.utc)
    conn.execute(
        "insert into fixture_detail_deliveries(fixture_id,status,first_seen_at,last_attempted_at,updated_at) "
        "values (1,'running',?,?,?)",
        ("2026-08-24T17:00:00Z", "2026-08-24T17:00:00Z", "2026-08-24T17:00:00Z"),
    )
    conn.execute(
        "insert into fixture_detail_deliveries(fixture_id,status,first_seen_at,last_attempted_at,updated_at) "
        "values (2,'running',?,?,?)",
        ("2026-08-24T17:58:00Z", "2026-08-24T17:58:00Z", "2026-08-24T17:58:00Z"),
    )
    conn.commit()

    assert recover_stale_running(conn, now=now) == 1
    stale = conn.execute(
        "select status,next_attempt_at,last_error,reason_code from fixture_detail_deliveries where fixture_id=1"
    ).fetchone()
    fresh = conn.execute(
        "select status from fixture_detail_deliveries where fixture_id=2"
    ).fetchone()
    assert stale[0] == "provider_pending"
    assert stale[1] == "2026-08-24T18:00:00Z"
    assert "requeued" in stale[2]
    assert stale[3] == "provider_pending_structure"
    assert fresh[0] == "running"


def test_missing_source_ledger_preserves_target_retry_age_and_attempts() -> None:
    conn = sqlite3.connect(":memory:")
    ensure_ledger(conn)
    target_meta = (
        567,
        28479,
        "2026-09-05 16:30:00+00:00",
        12,
        40,
        4,
        "2026-09-05 16:52:05+00:00",
        "2026-09-06 19:44:56+00:00",
    )

    assert hydrate_missing_source_delivery_history(
        conn,
        19745097,
        target_meta,
        now=datetime(2026, 9, 9, 4, 0, tzinfo=timezone.utc),
    )
    row = conn.execute(
        "select league_id,season_id,status,attempts,first_seen_at,last_attempted_at,reason_code "
        "from fixture_detail_deliveries where fixture_id=19745097"
    ).fetchone()
    assert row == (
        567,
        28479,
        "provider_pending",
        4,
        "2026-09-05 16:52:05+00:00",
        "2026-09-06 19:44:56+00:00",
        "legacy_unclassified",
    )
    assert not hydrate_missing_source_delivery_history(conn, 19745097, target_meta)


def test_legacy_provider_pending_rows_are_repaired_and_due() -> None:
    conn = sqlite3.connect(":memory:")
    ensure_ledger(conn)
    now = datetime(2026, 8, 24, 18, 0, tzinfo=timezone.utc)
    conn.execute(
        "insert into fixture_detail_deliveries(fixture_id,status,first_seen_at,updated_at) "
        "values (1,'provider_pending',?,?)",
        ("2026-08-24T17:00:00Z", "2026-08-24T17:00:00Z"),
    )
    conn.execute(
        "insert into fixture_detail_deliveries(fixture_id,status,first_seen_at,last_error,updated_at) "
        "values (2,'failed',?,?,?)",
        (
            "2026-08-24T17:00:00Z",
            "Controlled reconciliation worker handoff; fixture requeued",
            "2026-08-24T17:00:00Z",
        ),
    )
    conn.commit()

    repaired = repair_legacy_ledger(conn, now=now)

    assert repaired == {"legacy_pending": [1], "handoff_failed": [2]}
    rows = conn.execute(
        "select fixture_id,status,next_attempt_at,last_error,reason_code,delivery_contract_version "
        "from fixture_detail_deliveries order by fixture_id"
    ).fetchall()
    assert rows == [
        (1, "provider_pending", "2026-08-24T18:00:00Z", LEGACY_PENDING_REASON, "legacy_unclassified", 2),
        (2, "provider_pending", "2026-08-24T18:00:00Z", HANDOFF_REQUEUE_REASON, "legacy_unclassified", 2),
    ]


def test_bulk_fetch_preserves_each_fixture_and_reports_one_http_call(monkeypatch) -> None:
    FakeSportMonksClient.calls = []
    FakeSportMonksClient.responses = {}
    monkeypatch.setattr(queue, "SportMonksClient", FakeSportMonksClient)

    fetched, errors, calls = queue.fetch_provider_fixture_batch([101, 202])

    assert sorted(fetched) == [101, 202]
    assert errors == {}
    assert calls == 1
    assert FakeSportMonksClient.calls == ["fixtures/multi/101,202"]


def test_bulk_fetch_keeps_provider_omissions_per_fixture(monkeypatch) -> None:
    FakeSportMonksClient.calls = []
    FakeSportMonksClient.responses = {"fixtures/multi/101,202": [101]}
    monkeypatch.setattr(queue, "SportMonksClient", FakeSportMonksClient)

    fetched, errors, calls = queue.fetch_provider_fixture_batch([101, 202])

    assert sorted(fetched) == [101]
    assert list(errors) == [202]
    assert "omitted requested fixture" in str(errors[202])
    assert calls == 1


def test_bulk_fetch_splits_batches_and_counts_http_requests(monkeypatch) -> None:
    FakeSportMonksClient.calls = []
    FakeSportMonksClient.responses = {}
    monkeypatch.setattr(queue, "SportMonksClient", FakeSportMonksClient)

    fetched, errors, calls = queue.fetch_provider_fixtures([1, 2, 3, 4, 5], 2, 2)

    assert sorted(fetched) == [1, 2, 3, 4, 5]
    assert errors == {}
    assert calls == 3
    assert sorted(FakeSportMonksClient.calls) == [
        "fixtures/multi/1,2",
        "fixtures/multi/3,4",
        "fixtures/multi/5",
    ]


def test_bulk_omission_falls_back_to_single_fixture(monkeypatch) -> None:
    FakeSportMonksClient.calls = []
    FakeSportMonksClient.responses = {"fixtures/multi/101,202": [101]}
    monkeypatch.setattr(queue, "SportMonksClient", FakeSportMonksClient)

    fetched, errors, calls = queue.fetch_provider_fixtures([101, 202], 2, 50)

    assert sorted(fetched) == [101, 202]
    assert errors == {}
    assert calls == 2
    assert sorted(FakeSportMonksClient.calls) == ["fixtures/202", "fixtures/multi/101,202"]


def test_recent_revalidation_starts_with_confirmation_headroom() -> None:
    from datetime import timedelta
    now = datetime(2026, 9, 10, 2, 0, tzinfo=timezone.utc)
    scheduled = postmatch.parse_iso(postmatch.revalidation_time(now - timedelta(days=12), now))
    assert scheduled == now + timedelta(hours=12)
    assert scheduled + timedelta(minutes=15) < now + timedelta(hours=24)
    assert postmatch.parse_iso(postmatch.revalidation_time(now - timedelta(hours=24), now)) == now + timedelta(hours=6)
    assert postmatch.parse_iso(postmatch.revalidation_time(now - timedelta(days=60), now)) == now + timedelta(days=7)


def test_due_rechecks_receive_capacity_without_starving_other_work() -> None:
    new, retry = target_candidate_quotas(50, urgent_retry_count=100)
    assert new >= 10
    recovery, pending, revalidation = target_retry_lane_quotas(
        retry, urgent_pending_count=2, urgent_revalidation_count=98,
    )
    assert recovery >= 1 and pending >= 1
    assert pending > retry * 0.8
    assert recovery + pending + revalidation == retry
    assert target_retry_lane_quotas(retry, 0, 98)[2] > retry * 0.8
    for size in range(3, 51):
        for pending_count, revalidation_count in [(0, 100), (100, 1), (50, 50)]:
            quotas = target_retry_lane_quotas(size, pending_count, revalidation_count)
            assert sum(quotas) == size
            assert min(quotas) >= 1


def test_due_confirmations_survive_large_revalidation_backlog_and_cohort_cap() -> None:
    recovery, pending, revalidation = target_retry_lane_quotas(200, 55, 250)
    due_ids = list(range(1, 56))
    recheck_ids = list(range(101, 351))
    selected = interleave_retry_lanes([], due_ids[:pending], recheck_ids[:revalidation])
    selected += recheck_ids[revalidation:]
    metadata = {i: (8 + (i % 4), 2026, None, 0, 0) for i in due_ids}
    metadata.update({i: (82, 2026, None, 0, 0) for i in recheck_ids})
    batch = queue.cohort_limited_fixture_ids(selected, metadata, 5)[:50]
    assert sum(i in due_ids for i in batch) == 49
    assert len(set(batch)) == 50
