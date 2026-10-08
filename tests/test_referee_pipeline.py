import os
import subprocess
from pathlib import Path

import scripts.hydrate_referee_history as history
import scripts.sync_fixture_referee_stats as stats
from scripts.sync_fixture_referees import (
    ProviderRateLimited,
    extract_referee_items,
    fetch_fixture_ids,
    fetch_json_with_retry,
    normalize_assignment,
    payload_hash,
)


class _FakeResponse:
    def __init__(self, status_code, headers=None, payload=None):
        self.status_code = status_code
        self.headers = headers or {}
        self._payload = payload or {}

    def raise_for_status(self):
        raise AssertionError(f"unexpected response status={self.status_code}")

    def json(self):
        return self._payload


class _FakeSession:
    def __init__(self, response):
        self.response = response

    def get(self, url, timeout):
        return self.response


class _FakeCursor:
    def __init__(self):
        self.sql = ""
        self.params = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, sql, params):
        self.sql = sql
        self.params = params

    def fetchall(self):
        return [(101,), (202,)]


class _FakeConnection:
    def __init__(self):
        self.cursor_value = _FakeCursor()

    def cursor(self):
        return self.cursor_value


class _MutationConnection:
    def __init__(self):
        self.cursor_value = _FakeCursor()
        self.commits = 0

    def cursor(self):
        return self.cursor_value

    def commit(self):
        self.commits += 1


def test_fixture_referee_payload_maps_primary_and_supporting_officials():
    payload = {
        "data": {
            "id": 19722197,
            "referees": [
                {"id": 6024882, "fixture_id": 19722197, "referee_id": 14814, "type_id": 6},
                {"id": 6024883, "fixture_id": 19722197, "referee_id": 12090, "type_id": 7},
                {"id": 6024884, "fixture_id": 19722197, "referee_id": 24135, "type_id": 8},
                {"id": 6024885, "fixture_id": 19722197, "referee_id": 13537, "type_id": 9},
            ],
        }
    }

    rows = [normalize_assignment(19722197, item) for item in extract_referee_items(payload)]

    assert len(rows) == 4
    assert rows[0]["referee_id"] == 14814
    assert rows[0]["role"] == "main"
    assert rows[0]["is_primary"] is True
    assert [row["role"] for row in rows[1:]] == ["assistant_1", "assistant_2", "fourth_official"]
    assert all(row["source"] == "sportmonks" for row in rows)


def test_relation_payload_without_referees_is_empty_and_retryable():
    assert extract_referee_items({"data": {"id": 19722197, "referees": []}}) == []
    assert extract_referee_items({"data": {"id": 19722197}}) == []


def test_payload_hash_is_stable_for_key_order():
    assert payload_hash({"b": 2, "a": 1}) == payload_hash({"a": 1, "b": 2})


def test_rate_limit_stops_without_retrying_for_provider_cooldown():
    session = _FakeSession(_FakeResponse(429, headers={"Retry-After": "740"}))

    try:
        fetch_json_with_retry(session=session, url="https://example.test/fixtures/101?api_token=secret", timeout=1)
    except ProviderRateLimited as exc:
        assert exc.retry_after_seconds == 740
    else:
        raise AssertionError("expected ProviderRateLimited")


def test_force_window_bypasses_state_filter_but_normal_run_keeps_it():
    normal_conn = _FakeConnection()
    fetch_fixture_ids(
        normal_conn,
        days_back=30,
        days_forward=31,
        resync_hours=12,
        fixture_id=0,
        limit_fixtures=10,
        force_window=False,
    )
    assert "state.status in ('pending', 'no_assignment', 'error')" in normal_conn.cursor_value.sql
    assert normal_conn.cursor_value.params[-2] == 12
    assert normal_conn.cursor_value.params[-1] == 10

    force_conn = _FakeConnection()
    fetch_fixture_ids(
        force_conn,
        days_back=30,
        days_forward=31,
        resync_hours=12,
        fixture_id=0,
        limit_fixtures=10,
        force_window=True,
    )
    assert "state.status in ('pending', 'no_assignment', 'error')" not in force_conn.cursor_value.sql
    assert force_conn.cursor_value.params[-1] == 10


def test_nested_referee_profile_keeps_real_name():
    from scripts.sync_fixture_referees import normalize_referee_profile
    row = normalize_referee_profile({'referee_id':74959,'referee':{'id':74959,'name':'Andy Madley'}})
    assert row['id'] == 74959
    assert row['name'] == 'Andy Madley'


def test_due_order_prevents_empty_assignments_starving_the_backlog():
    conn = _FakeConnection()
    fetch_fixture_ids(conn, days_back=0, days_forward=14, resync_hours=12, fixture_id=0, limit_fixtures=50, force_window=False)
    assert "order by coalesce(state.next_attempt_at" in conn.cursor_value.sql
    assert "interval '12 hours'" not in conn.cursor_value.sql


def test_frequent_stats_query_selects_only_missing_reassigned_or_recent_targets():
    conn = _FakeConnection()
    conn.cursor_value.description = [("fixture_id",)]
    conn.cursor_value.fetchall = lambda: [(101,)]

    rows = stats.fetch_fixture_referee_metrics(
        conn,
        days_back=0,
        days_forward=14,
        fixture_id=0,
        limit_fixtures=0,
        refresh_mode="frequent",
        recent_history_hours=48,
    )

    assert rows == [{"fixture_id": 101}]
    assert "current_stats.fixture_id is null" in conn.cursor_value.sql
    assert "current_stats.referee_id is distinct from candidate.referee_id" in conn.cursor_value.sql
    assert "recent_fixture.starting_at >= (now() - make_interval(hours => %s))" in conn.cursor_value.sql
    assert conn.cursor_value.params[4:7] == ["frequent", 48, stats.FINISHED_STATUSES]


def test_full_stats_mode_preserves_the_complete_target_scope():
    conn = _FakeConnection()
    conn.cursor_value.description = [("fixture_id",)]
    conn.cursor_value.fetchall = lambda: [(101,), (202,)]

    rows = stats.fetch_fixture_referee_metrics(
        conn,
        days_back=0,
        days_forward=14,
        fixture_id=0,
        limit_fixtures=0,
        refresh_mode="full",
    )

    assert rows == [{"fixture_id": 101}, {"fixture_id": 202}]
    assert conn.cursor_value.params[4] == "full"


def test_stats_upsert_reports_insert_update_and_semantic_noop(monkeypatch):
    conn = _MutationConnection()
    captured = {}

    def fake_execute_values(cur, sql, values, *, page_size, fetch):
        captured.update(sql=sql, values=values, page_size=page_size, fetch=fetch)
        return [(True,), (False,)]

    monkeypatch.setattr(stats, "execute_values", fake_execute_values)
    rows = [
        {"fixture_id": 101, "referee_id": 1, "referee_name": "One"},
        {"fixture_id": 202, "referee_id": 2, "referee_name": "Two"},
        {"fixture_id": 303, "referee_id": 3, "referee_name": "Three"},
    ]

    mutations = stats.upsert_fixture_referee_stats(conn, rows)

    assert mutations == stats.MutationCounts(inserted=1, updated=1)
    assert conn.commits == 1
    assert "is distinct from" in captured["sql"]
    assert "returning (xmax = 0) as inserted" in captured["sql"]
    assert captured["fetch"] is True


def test_history_upsert_preserves_refresh_metadata_for_semantic_noops(monkeypatch):
    conn = _MutationConnection()
    captured = {}

    def fake_execute_values(cur, sql, values, *, page_size, fetch):
        captured.update(sql=sql, values=values, page_size=page_size, fetch=fetch)
        return [(False,)]

    monkeypatch.setattr(history, "execute_values", fake_execute_values)
    rows = [
        {
            "fixture_id": 101,
            "referee_id": 1,
            "role": "main",
            "is_primary": True,
            "source": "sportmonks",
            "extra": {"fixture_id": 101, "referee_id": 1, "type_id": 6},
        },
        {
            "fixture_id": 202,
            "referee_id": 2,
            "role": "main",
            "is_primary": True,
            "source": "sportmonks",
            "extra": {"fixture_id": 202, "referee_id": 2, "type_id": 6},
        },
    ]

    mutations = history.upsert_assignments(conn, rows)

    assert mutations == history.MutationCounts(inserted=0, updated=1)
    assert "updated_at = now()" in captured["sql"]
    assert "last_synced_at = now()" in captured["sql"]
    assert "is distinct from" in captured["sql"]
    assert conn.commits == 1


def test_referee_runner_uses_one_lock_and_separates_frequent_from_hourly(tmp_path):
    repo_root = Path(__file__).resolve().parents[1]
    calls = tmp_path / "calls.txt"
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_flock = fake_bin / "flock"
    fake_flock.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    fake_flock.chmod(0o755)
    fake_python = tmp_path / "python"
    fake_python.write_text(
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$REFEREE_TEST_CALLS\"\n",
        encoding="utf-8",
    )
    fake_python.chmod(0o755)
    env = {
        **os.environ,
        "REFEREE_PYTHON": str(fake_python),
        "REFEREE_REPORT_DIR": str(tmp_path / "reports"),
        "REFEREE_TEST_CALLS": str(calls),
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
    }

    subprocess.run(
        ["bash", str(repo_root / "scripts/run_referee_delivery.sh"), "frequent"],
        check=True,
        env=env,
    )
    frequent_calls = calls.read_text(encoding="utf-8").splitlines()
    assert len(frequent_calls) == 2
    assert "sync_fixture_referees.py" in frequent_calls[0]
    assert "hydrate_referee_history.py" not in "\n".join(frequent_calls)
    assert "sync_fixture_referee_stats.py" in frequent_calls[1]
    assert "--refresh-mode frequent" in frequent_calls[1]

    calls.write_text("", encoding="utf-8")
    subprocess.run(
        ["bash", str(repo_root / "scripts/run_referee_delivery.sh"), "hourly"],
        check=True,
        env=env,
    )
    hourly_calls = calls.read_text(encoding="utf-8").splitlines()
    assert len(hourly_calls) == 2
    assert "sync_fixture_referees.py" not in "\n".join(hourly_calls)
    assert "hydrate_referee_history.py" in hourly_calls[0]
    assert "sync_fixture_referee_stats.py" in hourly_calls[1]
    assert "--refresh-mode full" in hourly_calls[1]


def test_referee_runner_rejects_unknown_mode(tmp_path):
    repo_root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        ["bash", str(repo_root / "scripts/run_referee_delivery.sh"), "surprise"],
        check=False,
        capture_output=True,
        text=True,
        env={**os.environ, "REFEREE_REPORT_DIR": str(tmp_path)},
    )
    assert result.returncode == 2
    assert "frequent|hourly" in result.stderr


def test_referee_runner_lock_collision_skips_without_starting_a_job(tmp_path):
    repo_root = Path(__file__).resolve().parents[1]
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_flock = fake_bin / "flock"
    fake_flock.write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")
    fake_flock.chmod(0o755)
    calls = tmp_path / "calls.txt"
    fake_python = tmp_path / "python"
    fake_python.write_text(
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$REFEREE_TEST_CALLS\"\n",
        encoding="utf-8",
    )
    fake_python.chmod(0o755)

    result = subprocess.run(
        ["bash", str(repo_root / "scripts/run_referee_delivery.sh"), "hourly"],
        check=False,
        env={
            **os.environ,
            "REFEREE_PYTHON": str(fake_python),
            "REFEREE_REPORT_DIR": str(tmp_path / "reports"),
            "REFEREE_TEST_CALLS": str(calls),
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
        },
    )

    assert result.returncode == 0
    assert not calls.exists()
