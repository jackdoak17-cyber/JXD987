from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

from scripts import reconcile_stats_provider_queue as queue
from scripts.postmatch_fixture_detail_delivery import (
    HANDOFF_REQUEUE_REASON,
    LEGACY_PENDING_REASON,
    ensure_ledger,
    recover_stale_running,
    repair_legacy_ledger,
    target_candidate_quotas,
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
    assert target_candidate_quotas(1) == (1, 0)
    assert target_candidate_quotas(0) == (0, 0)


def test_cohort_limit_preserves_clustered_prefix() -> None:
    metadata = {
        1: (8, 25583, None, 0, 0),
        2: (8, 25583, None, 0, 0),
        3: (387, 26164, None, 0, 0),
        4: (648, 23265, None, 0, 0),
    }

    assert queue.cohort_limited_fixture_ids([1, 2, 3, 4], metadata, 2) == [1, 2, 3]
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
        "select status,next_attempt_at,last_error from fixture_detail_deliveries where fixture_id=1"
    ).fetchone()
    fresh = conn.execute(
        "select status from fixture_detail_deliveries where fixture_id=2"
    ).fetchone()
    assert stale[0] == "provider_pending"
    assert stale[1] == "2026-08-24T18:00:00Z"
    assert "requeued" in stale[2]
    assert fresh[0] == "running"


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
        "select fixture_id,status,next_attempt_at,last_error from fixture_detail_deliveries order by fixture_id"
    ).fetchall()
    assert rows == [
        (1, "provider_pending", "2026-08-24T18:00:00Z", LEGACY_PENDING_REASON),
        (2, "provider_pending", "2026-08-24T18:00:00Z", HANDOFF_REQUEUE_REASON),
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
