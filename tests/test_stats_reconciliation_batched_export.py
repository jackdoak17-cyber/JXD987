from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from scripts import export_to_supabase as exporter
from scripts import reconcile_stats_provider_queue as queue


def test_snapshot_map_requires_exact_unique_positive_ids(tmp_path: Path) -> None:
    mapping = tmp_path / "snapshots.json"
    mapping.write_text('{"1":101,"2":202}', encoding="utf-8")
    assert exporter.load_batch_snapshot_ids(str(mapping), "1,2") == {1: 101, 2: 202}

    for content, fixture_ids in (
        ('{"1":101}', "1,2"),
        ('{"1":101,"1":102}', "1"),
        ('{"1":true}', "1"),
        ('{"1":0}', "1"),
        ('{"1":101}', "1,1"),
        ('{"1":101}', "0"),
    ):
        mapping.write_text(content, encoding="utf-8")
        with pytest.raises(ValueError):
            exporter.load_batch_snapshot_ids(str(mapping), fixture_ids)


def test_queue_uses_one_batch_and_preserves_partial_outcomes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    report_path = tmp_path / "export-report.json"
    calls: list[list[str]] = []

    def fake_run(command: list[str], **_kwargs):
        calls.append(command)
        fixture_arg = command[command.index("--fixture-ids") + 1]
        snapshot_file = Path(command[command.index("--provider-snapshot-ids-json") + 1])
        assert fixture_arg == "1,2"
        assert json.loads(snapshot_file.read_text()) == {"1": 101, "2": 202}
        report_path.write_text(json.dumps({
            "explicit_fixture_ids": [1, 2],
            "atomic_published_fixture_ids": [1],
            "atomic_failed_fixtures": [{"fixture_id": 2, "error": "fixture 2 failed"}],
        }), encoding="utf-8")
        return subprocess.CompletedProcess(command, 1, "", "")

    monkeypatch.setattr(queue.subprocess, "run", fake_run)
    result = queue.export_batch([1, 2], [8], str(report_path), {1: 101, 2: 202})

    assert len(calls) == 1
    assert result.exported_ids == (1,)
    assert result.failed_ids == (2,)
    assert result.outcomes[1].stage == "atomic_fixture_detail_publish"


def test_batch_missing_or_untrusted_report_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    report_path = tmp_path / "export-report.json"
    report_path.write_text("stale", encoding="utf-8")
    calls = 0

    def fake_run(command: list[str], **_kwargs):
        nonlocal calls
        calls += 1
        return subprocess.CompletedProcess(command, 1, "", "failed before report")

    monkeypatch.setattr(queue.subprocess, "run", fake_run)
    result = queue.export_batch([1, 2], [8], str(report_path), {1: 101, 2: 202})

    assert calls == 1
    assert not report_path.exists()
    assert result.exported_ids == ()
    assert result.failed_ids == (1, 2)
    assert {item.stage for item in result.outcomes} == {"batch_outcome_unverified"}


def test_batch_report_rejects_ambiguous_fixture_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    report_path = tmp_path / "export-report.json"

    def fake_run(command: list[str], **_kwargs):
        report_path.write_text(json.dumps({
            "explicit_fixture_ids": [1, 2],
            "atomic_published_fixture_ids": [True, 2],
            "atomic_failed_fixtures": [],
        }), encoding="utf-8")
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(queue.subprocess, "run", fake_run)
    result = queue.export_batch([1, 2], [8], str(report_path), {1: 101, 2: 202})
    assert result.exported_ids == ()
    assert result.failed_ids == (1, 2)


def test_retry_mode_uses_individual_exports(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    commands: list[list[str]] = []

    def fake_run(command: list[str], **_kwargs):
        commands.append(command)
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(queue.subprocess, "run", fake_run)
    result = queue.export_batch(
        [1, 2], [8], str(tmp_path / "report.json"), {1: 101, 2: 202},
        allow_batch=False,
    )

    assert result.exported_ids == (1, 2)
    assert len(commands) == 2
    assert [command[command.index("--fixture-ids") + 1] for command in commands] == ["1", "2"]
    assert all("--provider-snapshot-ids-json" not in command for command in commands)


def test_mixed_first_attempts_batch_while_retry_stays_individual(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    report_path = tmp_path / "mixed-report.json"
    commands: list[list[str]] = []

    def fake_run(command: list[str], **_kwargs):
        commands.append(command)
        if "--provider-snapshot-ids-json" in command:
            assert command[command.index("--fixture-ids") + 1] == "1,2"
            report_path.write_text(json.dumps({
                "explicit_fixture_ids": [1, 2],
                "atomic_published_fixture_ids": [1],
                "atomic_failed_fixtures": [{"fixture_id": 2, "error": "isolated failure"}],
            }), encoding="utf-8")
            return subprocess.CompletedProcess(command, 1, "", "")
        assert command[command.index("--fixture-ids") + 1] == "3"
        assert "--provider-snapshot-id" in command
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(queue.subprocess, "run", fake_run)
    result = queue.export_batch(
        [1, 3, 2], [8], str(report_path), {1: 101, 2: 202, 3: 303},
        attempt_by_id={1: 1, 3: 2, 2: 1},
    )

    assert len(commands) == 2
    assert result.batched_fixture_ids == (1, 2)
    assert [outcome.fixture_ids for outcome in result.outcomes] == [(1,), (3,), (2,)]
    assert result.exported_ids == (1, 3)
    assert result.failed_ids == (2,)
    assert result.outcomes[2].stage == "atomic_fixture_detail_publish"


def test_mixed_with_only_one_first_attempt_keeps_individual_exports(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    commands: list[list[str]] = []

    def fake_run(command: list[str], **_kwargs):
        commands.append(command)
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(queue.subprocess, "run", fake_run)
    result = queue.export_batch(
        [1, 2, 3], [8], str(tmp_path / "report.json"), {1: 101, 2: 202, 3: 303},
        attempt_by_id={1: 1, 2: 2, 3: 3},
    )

    assert [command[command.index("--fixture-ids") + 1] for command in commands] == ["1", "2", "3"]
    assert all("--provider-snapshot-ids-json" not in command for command in commands)
    assert result.batched_fixture_ids == ()
    assert result.exported_ids == (1, 2, 3)


def test_exporter_reports_partial_atomic_results_and_filters_ancillary_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    mapping = tmp_path / "snapshots.json"
    mapping.write_text('{"1":101,"2":202}', encoding="utf-8")
    report_path = tmp_path / "result.json"
    monkeypatch.setattr(sys, "argv", [
        "export_to_supabase.py", "--fixture-ids", "1,2", "--leagues", "8",
        "--keep-all-seasons", "--protect-empty-detail", "--require-detail",
        "--atomic-fixture-detail", "--skip-odds-snapshots", "--skip-odds-outcomes",
        "--skip-prune", "--provider-snapshot-ids-json", str(mapping),
        "--report-json", str(report_path),
    ])
    monkeypatch.setattr(exporter, "require_env", lambda *_: None)
    monkeypatch.setattr(exporter, "SUPABASE_DB_URL", "postgres://disposable")
    monkeypatch.setattr(exporter, "get_conn", lambda: object())
    monkeypatch.setattr(exporter, "ensure_tables_exist", lambda *_: None)
    monkeypatch.setattr(exporter, "ensure_fixture_columns", lambda *_: None)
    monkeypatch.setattr(exporter, "choose_keep_seasons", lambda *_args, **_kwargs: {10})
    monkeypatch.setattr(exporter, "fetch_seasons", lambda *_: [])
    monkeypatch.setattr(exporter, "fetch_rounds", lambda *_: [])
    fixtures = [
        {"id": 1, "home_team_id": 11, "away_team_id": 12},
        {"id": 2, "home_team_id": 21, "away_team_id": 22},
    ]
    monkeypatch.setattr(exporter, "fetch_fixtures_by_ids", lambda *_: fixtures)
    monkeypatch.setattr(exporter, "fetch_teams", lambda _conn, ids: [{"id": value} for value in ids])
    monkeypatch.setattr(exporter, "validate_detail_payload", lambda *_args, **_kwargs: [])
    players = [
        {"fixture_id": 1, "player_id": 1011, "team_id": 11},
        {"fixture_id": 2, "player_id": 2021, "team_id": 21},
    ]
    monkeypatch.setattr(exporter, "fetch_fixture_players", lambda *_: players)
    monkeypatch.setattr(exporter, "fetch_fixture_statistics", lambda *_: [])
    monkeypatch.setattr(exporter, "fetch_fixture_player_statistics", lambda *_: [])
    monkeypatch.setattr(exporter, "fetch_players", lambda _conn, ids: [{"id": value} for value in ids])
    monkeypatch.setattr(exporter, "fetch_player_team_history", lambda *_: [
        {"id": 1, "player_id": 1011}, {"id": 2, "player_id": 2021},
    ])
    monkeypatch.setattr(exporter, "fetch_sidelined_players", lambda *_: [
        {"id": 1, "team_id": 11}, {"id": 2, "team_id": 21},
    ])
    monkeypatch.setattr(exporter, "filter_rows_for_remote_schema", lambda _table, rows: rows)
    exported_rows: dict[str, list[dict]] = {}

    def fake_upsert(table, rows, *_args):
        exported_rows[table] = list(rows)
        return len(rows), 0

    monkeypatch.setattr(exporter, "upsert_table", fake_upsert)

    class DummyTarget:
        def close(self):
            pass

    monkeypatch.setattr(exporter.psycopg2, "connect", lambda *_args, **_kwargs: DummyTarget())
    published: list[tuple[int, int]] = []

    def fake_atomic(_target, *, fixture_id, snapshot_id, **_kwargs):
        published.append((fixture_id, snapshot_id))
        if fixture_id == 2:
            raise RuntimeError("fixture 2 failed")
        return {"fixture_id": fixture_id}

    monkeypatch.setattr(exporter, "atomic_fixture_detail_publish", fake_atomic)
    with pytest.raises(SystemExit) as raised:
        exporter.main()

    assert raised.value.code == 1
    assert published == [(1, 101), (2, 202)]
    assert exported_rows["player_team_history"] == [{"id": 1, "player_id": 1011}]
    assert exported_rows["sidelined_players"] == [{"id": 1, "team_id": 11}]
    report = json.loads(report_path.read_text())
    assert report["atomic_published_fixture_ids"] == [1]
    assert [item["fixture_id"] for item in report["atomic_failed_fixtures"]] == [2]
