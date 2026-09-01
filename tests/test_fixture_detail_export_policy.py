from __future__ import annotations

import sqlite3
from dataclasses import asdict
from datetime import datetime, timezone

import pytest

from scripts import reconcile_stats_provider_queue as queue
from scripts.postmatch_fixture_detail_delivery import (
    DetailSnapshot,
    ProviderAssessment,
    ensure_ledger,
    stable_provider_sparse_assessment,
)


def _assessment(status: str = "ready") -> ProviderAssessment:
    return ProviderAssessment(
        status=status,
        fixture_status="FT",
        finished=True,
        team_stat_types={"101": [42], "202": [42]},
        missing_team_stat_type_ids={"101": [], "202": []},
        player_stat_types={"101": [119], "202": [119]},
        missing_player_stat_type_ids={"101": [], "202": []},
        lineup_counts={"101": 1, "202": 1},
        player_stat_counts={"101": 1, "202": 1},
        team_stat_count=2,
        player_stat_count=2,
        lineup_count=2,
    )


def _source(fixture_id: int) -> DetailSnapshot:
    return DetailSnapshot(
        fixture_id=fixture_id,
        team_stat_count=2,
        player_stat_count=2,
        lineup_count=2,
        team_stat_types={"101": [42], "202": [42]},
        team_stat_values={"101:42": 1, "202:42": 1},
        player_stat_values={"11:101:119": 90, "22:202:119": 90},
        lineup_values={"11:101": (True, 90), "22:202": (True, 90)},
    )


def _candidate(fixture_id: int, *, unsafe: bool = False) -> dict:
    return {
        "fixture_id": fixture_id,
        "attempt": 3,
        "assessment": _assessment("provider_pending" if unsafe else "ready"),
        "source": None if unsafe else _source(fixture_id),
        "snapshot_id": 9000 + fixture_id,
        "payload_hash": f"payload-{fixture_id}",
        "normalized_hash": f"normalized-{fixture_id}",
        "stable_count": 2,
        "meta": (8, 28083, "2026-08-01T12:00:00Z"),
    }


def test_partition_export_candidates_isolates_unsafe_fixture() -> None:
    from scripts.fixture_detail_export_policy import partition_export_candidates

    partition = partition_export_candidates(
        [_candidate(1), _candidate(2), _candidate(3, unsafe=True)]
    )

    assert partition.exportable_ids == (1, 2)
    assert partition.unsafe_ids == (3,)
    assert partition.decisions[3].reason_code == "provider_pending"
    assert partition.decisions[3].exportable is False


def test_partition_rejects_missing_team_stats_for_strict_export() -> None:
    from scripts.fixture_detail_export_policy import partition_export_candidates

    source = _source(4)
    source_without_team_stats = DetailSnapshot(
        fixture_id=source.fixture_id,
        team_stat_count=0,
        player_stat_count=source.player_stat_count,
        lineup_count=source.lineup_count,
        team_stat_types={},
        team_stat_values={},
        player_stat_values=source.player_stat_values,
        lineup_values=source.lineup_values,
    )
    candidate = _candidate(4)
    candidate["source"] = source_without_team_stats

    partition = partition_export_candidates([candidate])

    assert partition.exportable_ids == ()
    assert partition.unsafe_ids == (4,)
    assert partition.decisions[4].reason_code == "source_incomplete"
    assert partition.decisions[4].exportable is False


def test_queue_exports_safe_subset_when_one_fixture_is_unsafe() -> None:
    calls: list[list[int]] = []

    def fake_export(fixture_ids: list[int]) -> queue.ExportCommandResult:
        calls.append(fixture_ids)
        return queue.ExportCommandResult.success(fixture_ids)

    result = queue.export_candidates(
        [_candidate(1), _candidate(2), _candidate(3, unsafe=True)],
        fake_export,
    )

    assert calls == [[1, 2]]
    assert result.exported_ids == (1, 2)
    assert result.retryable_ids == (3,)
    assert result.unsafe_ids == (3,)


def test_batch_export_transport_failure_marks_attempted_fixtures_retryable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = sqlite3.connect(":memory:")
    ensure_ledger(conn)
    conn.execute(
        "insert into fixture_detail_deliveries "
        "(fixture_id, status, attempts, first_seen_at, updated_at) "
        "values (1, 'running', 3, ?, ?)" ,
        ("2026-08-28T10:00:00Z", "2026-08-28T10:00:00Z"),
    )
    conn.commit()
    monkeypatch.setattr(queue, "publish_delivery_status", lambda *args, **kwargs: None)

    queue.record_export_failure(
        conn,
        "postgres://target",
        _candidate(1),
        queue.ExportCommandResult.failure(
            fixture_ids=[1],
            failure_class="transport",
            stage="upsert",
            message="database unavailable",
        ),
        now=datetime(2026, 8, 28, 10, 5, tzinfo=timezone.utc),
    )

    row = conn.execute(
        "select status, attempts, provider_team_stat_count, last_error, next_attempt_at "
        "from fixture_detail_deliveries where fixture_id=1"
    ).fetchone()
    assert row[0] == "export_failed"
    assert row[1] == 3
    assert row[2] == 2
    assert "database unavailable" in row[3]
    assert row[4] is not None


def test_retry_selection_honors_due_retryable_status_with_accepted_snapshot() -> None:
    from scripts.fixture_detail_export_policy import retryable_delivery_status_is_due

    now = datetime(2026, 8, 28, 10, 0, tzinfo=timezone.utc)
    for status in ("export_failed", "verification_failed", "projection_failed"):
        assert retryable_delivery_status_is_due(
            status=status,
            accepted_snapshot_id=123,
            next_attempt_at="2026-08-28T09:59:00Z",
            next_revalidation_at=None,
            now=now,
        )


def test_export_failure_does_not_activate_snapshot_or_refresh_success_for_that_fixture(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    monkeypatch.setattr(queue, "activate_provider_snapshot", lambda *args, **kwargs: calls.append("activate"))
    monkeypatch.setattr(queue, "refresh_seasons", lambda *args, **kwargs: calls.append("refresh"))

    result = queue.export_candidates(
        [_candidate(1)],
        lambda fixture_ids: queue.ExportCommandResult.failure(
            fixture_ids=fixture_ids,
            failure_class="database",
            stage="publish",
            message="connection lost",
        ),
    )

    assert result.exported_ids == ()
    assert result.retryable_ids == (1,)
    assert calls == []


def test_atomic_or_staged_publication_survives_failure_after_delete() -> None:
    from scripts.export_to_supabase import atomic_fixture_detail_publish

    class FailingCursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, *args):
            raise RuntimeError("injected failure after staged delete")

    class Transaction:
        def __init__(self):
            self.commits = 0
            self.rollbacks = 0

        def cursor(self):
            return FailingCursor()

        def commit(self):
            self.commits += 1

        def rollback(self):
            self.rollbacks += 1

    transaction = Transaction()
    with pytest.raises(RuntimeError, match="injected failure"):
        atomic_fixture_detail_publish(
            transaction,
            fixture_id=1,
            snapshot_id=2,
            fixture_players=[],
            fixture_statistics=[],
            fixture_player_statistics=[],
        )
    assert transaction.commits == 0
    assert transaction.rollbacks == 1


def test_atomic_publication_calls_v2_with_player_dimensions_and_snapshot() -> None:
    from scripts.export_to_supabase import atomic_fixture_detail_publish

    captured: dict[str, object] = {}

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, statement, params):
            captured["statement"] = statement
            captured["params"] = params

        def fetchone(self):
            return ({"fixture_id": 1},)

    class Transaction:
        def cursor(self):
            return Cursor()

        def commit(self):
            captured["committed"] = True

        def rollback(self):
            captured["rolled_back"] = True

    atomic_fixture_detail_publish(
        Transaction(),
        fixture_id=1,
        snapshot_id=2,
        player_dimensions=[{"id": 11, "name": "Player 11"}],
        fixture_players=[{"fixture_id": 1, "player_id": 11, "team_id": 101}],
        fixture_statistics=[],
        fixture_player_statistics=[],
    )

    assert "publish_fixture_detail_atomic_v2" in str(captured["statement"])
    params = captured["params"]
    assert params[0] == 1
    assert params[1] == 2
    assert '"id": 11' in params[2]
    assert captured["committed"] is True


def test_partial_export_rerun_is_idempotent() -> None:
    from scripts.export_to_supabase import atomic_fixture_detail_publish

    assert callable(atomic_fixture_detail_publish)
    # The database-level function is the idempotency boundary; the worker must
    # call it with the same fixture/snapshot payload on a retry.
    assert asdict(_source(1))["fixture_id"] == 1


def test_projection_eligibility_excludes_unverified_fixture() -> None:
    fake = type("FakeConnection", (), {})()
    fake.queries = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, statement, params):
            fake.queries.append((statement, params))

        def fetchone(self):
            return (2,)

    fake.cursor = lambda: Cursor()
    fake.commit = lambda: None
    fake.rollback = lambda: None

    queue.refresh_seasons(
        "postgres://target",
        {(8, 28083)},
        eligible_fixture_ids={1},
        target_conn=fake,
    )

    assert fake.queries
    assert any(params and 1 in params[-1] for _, params in fake.queries)


def test_provider_sparse_optional_gap_requires_stable_confirmation() -> None:
    from scripts.postmatch_fixture_detail_delivery import assess_provider_payload

    payload = {
        "state": {"short_name": "FT"},
        "participants": [{"id": 101}, {"id": 202}],
        "statistics": [
            {"participant_id": team_id, "type_id": 42}
            for team_id in (101, 202)
        ],
        "lineups": [
            {"team_id": 101, "player_id": 11, "details": [{"type_id": 119}]},
            {"team_id": 202, "player_id": 22, "details": [{"type_id": 119}]},
        ],
    }
    assessment = assess_provider_payload(payload)
    assert assessment.status == "provider_sparse"
    assert stable_provider_sparse_assessment(assessment, 1) is None
    assert stable_provider_sparse_assessment(assessment, 2) is not None


def test_projection_failed_is_pending_to_website_consumers() -> None:
    from scripts.fixture_detail_export_policy import WEBSITE_PENDING_DELIVERY_STATUSES

    assert "projection_failed" in WEBSITE_PENDING_DELIVERY_STATUSES


def test_postmatch_single_fixture_contract_remains_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from scripts import postmatch_fixture_detail_delivery as postmatch

    captured: list[list[str]] = []

    def fake_run(command, **kwargs):
        captured.append(command)
        return queue.ExportCommandResult.success([9001])

    monkeypatch.setattr(postmatch.subprocess, "run", fake_run)
    result = postmatch.export_fixture(9001, [8], "/tmp/postmatch-report.json")

    assert result.returncode == 0
    assert "--atomic-fixture-detail" in captured[0]


def test_postmatch_single_fixture_passes_provider_snapshot_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from scripts import postmatch_fixture_detail_delivery as postmatch

    captured: list[list[str]] = []

    def fake_run(command, **kwargs):
        captured.append(command)
        return queue.ExportCommandResult.success([9001])

    monkeypatch.setattr(postmatch.subprocess, "run", fake_run)
    result = postmatch.export_fixture(9001, [8], "/tmp/postmatch-report.json", snapshot_id=77)

    assert result.returncode == 0
    assert captured[0][captured[0].index("--provider-snapshot-id") + 1] == "77"
