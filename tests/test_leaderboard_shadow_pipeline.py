import os
import json
import hashlib
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts.leaderboard_finite_trial_supervisor import acquire_lock
import scripts.leaderboard_finite_trial_supervisor as supervisor_module
import scripts.leaderboard_shadow_worker as worker_module
from scripts.leaderboard_shadow_capture import capture_fixture_ids
from scripts.leaderboard_shadow_worker import (
    MAX_FIXTURES_PER_RUN,
    MAX_SECONDS_PER_RUN,
    RESOURCE_GUARD_FIXTURE_INTERVAL,
    SAFETY_CHECK_RESERVE_SECONDS,
    PREFLIGHT_STATEMENT_TIMEOUT_MS,
    DirtyClaim,
    RunBudget,
    apply_preflight_timeouts,
    configure_temp_file_limit_if_permitted,
    claim_dirty,
    finish_generation,
    finish_worker_run,
    finish_worker_run_with_resource_check,
    load_external_resource_evidence,
    pending_cohorts,
    process_generation_job,
    process_dirty_claim,
    process_capture_queue,
    recover_unfinished_worker_runs,
    start_worker_run,
)


ROOT = Path(__file__).resolve().parents[1]


def test_finite_trial_wrapper_rejects_unreviewed_release_before_connection(tmp_path):
    environment_file = tmp_path / "finite-trial.env"
    environment_file.write_text(
        "LEADERBOARD_FINITE_TRIAL_POLICY_ID=local-policy\n"
        "LEADERBOARD_FINITE_TRIAL_EXPECTED_DSN_SHA256=" + "0" * 64 + "\n"
        "LEADERBOARD_FINITE_TRIAL_RELEASE_SHA=" + "1" * 40 + "\n"
        "LEADERBOARD_FINITE_TRIAL_RUNTIME_MANIFEST_SHA256="
        + hashlib.sha256((ROOT / "scripts/vps/runtime_manifest.sha1").read_bytes()).hexdigest()
        + "\n"
        "LEADERBOARD_FINITE_TRIAL_OPERATOR_NOTE='local wrapper rejection'\n"
    )
    environment = os.environ.copy()
    environment["LEADERBOARD_SHADOW_ENV_FILE"] = str(environment_file)
    environment.pop("SUPABASE_DB_URL", None)
    environment.pop("SUPABASE_DB_URL_SESSION", None)
    process = subprocess.run(
        ["bash", str(ROOT / "scripts/vps/run_leaderboard_finite_trial.sh")],
        env=environment, capture_output=True, text=True, timeout=10, check=False,
    )
    assert process.returncode != 0
    assert "checkout differs from the reviewed release" in process.stderr


def test_finite_trial_wrapper_accepts_pinned_archive_without_git(tmp_path):
    archive = tmp_path / "runtime-archive"
    manifest = ROOT / "scripts/vps/runtime_manifest.sha1"
    for line in manifest.read_text().splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        relative = line.split(maxsplit=1)[1]
        destination = archive / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / relative, destination)
    archive_manifest = archive / "scripts/vps/runtime_manifest.sha1"
    shutil.copy2(manifest, archive_manifest)
    environment_file = tmp_path / "archive-trial.env"
    environment_file.write_text(
        "LEADERBOARD_FINITE_TRIAL_POLICY_ID=local-policy\n"
        "LEADERBOARD_FINITE_TRIAL_EXPECTED_DSN_SHA256=" + "0" * 64 + "\n"
        "LEADERBOARD_FINITE_TRIAL_RELEASE_SHA=" + "1" * 40 + "\n"
        "LEADERBOARD_FINITE_TRIAL_RUNTIME_MANIFEST_SHA256="
        + hashlib.sha256(archive_manifest.read_bytes()).hexdigest() + "\n"
        "LEADERBOARD_FINITE_TRIAL_OPERATOR_NOTE='archive identity proof'\n"
    )
    environment = os.environ.copy()
    environment["LEADERBOARD_SHADOW_ENV_FILE"] = str(environment_file)
    environment.pop("SUPABASE_DB_URL", None)
    environment.pop("SUPABASE_DB_URL_SESSION", None)
    result = subprocess.run(
        ["bash", str(archive / "scripts/vps/run_leaderboard_finite_trial.sh")],
        env=environment, capture_output=True, text=True, timeout=15, check=False,
    )
    assert result.returncode != 0
    assert "runtime manifest verified" in result.stdout
    assert "trial database URL is required" in result.stderr


def test_finite_trial_commands_reject_a_non_disposable_database(monkeypatch):
    monkeypatch.setattr(worker_module, "isolated_test_database", lambda _conn: False)
    args = SimpleNamespace(isolated_test_override=True)
    with pytest.raises(RuntimeError, match="socket-only oddssearch_m3"):
        worker_module.run_isolated_finite_trial(object(), args, "worker")
    with pytest.raises(RuntimeError, match="socket-only oddssearch_m3"):
        worker_module.recover_isolated_finite_trial(object(), args)


def test_finite_trial_supervisor_lock_excludes_and_releases(tmp_path):
    path = str(tmp_path / "worker.lock")
    first = acquire_lock(path)
    assert first is not None
    try:
        assert acquire_lock(path) is None
    finally:
        first.close()
    again = acquire_lock(path)
    assert again is not None
    again.close()


def test_finite_trial_child_keeps_flock_after_parent_exit(tmp_path):
    path = str(tmp_path / "inherited.lock")
    parent_code = (
        "import fcntl,os,subprocess,sys; "
        "lock=open(sys.argv[1],'a'); "
        "fcntl.flock(lock,fcntl.LOCK_EX); "
        "subprocess.Popen([sys.executable,'-c','import time; time.sleep(3)'], "
        "stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,"
        "stderr=subprocess.DEVNULL,pass_fds=(lock.fileno(),)); "
        "os._exit(0)"
    )
    parent = subprocess.run(
        [sys.executable, "-c", parent_code, path],
        capture_output=True, text=True, timeout=5, check=False,
    )
    assert parent.returncode == 0
    assert acquire_lock(path) is None
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        released = acquire_lock(path)
        if released is not None:
            released.close()
            break
        time.sleep(0.05)
    else:
        pytest.fail("orphaned child did not release inherited flock")


@pytest.mark.parametrize("signal_during_spawn", [1, 2])
def test_supervisor_delivers_signal_arriving_before_child_assignment(
    tmp_path, monkeypatch, signal_during_spawn,
):
    worker = tmp_path / "fake_worker.py"
    worker.write_text(
        "import json,sys,time\n"
        f"if (sys.argv[1] == 'trial-once') == {signal_during_spawn == 1}:\n"
        "    time.sleep(10)\n"
        "else:\n"
        "    print(json.dumps({'state':'no_reservation'}), flush=True)\n"
    )
    monkeypatch.setattr(supervisor_module, "database_url", lambda: "disposable")
    monkeypatch.setattr(supervisor_module, "assert_disposable_database", lambda _url: None)
    monkeypatch.setattr(supervisor_module, "drain_token_backends", lambda *_args: None)
    monkeypatch.setattr(supervisor_module, "drain_exact_backend", lambda *_args, **_kw: None)
    real_popen = subprocess.Popen
    children = []

    def inject_signal(*args, **kwargs):
        child = real_popen(*args, **kwargs)
        children.append(child)
        if len(children) == signal_during_spawn:
            signal.raise_signal(signal.SIGTERM)
        return child

    monkeypatch.setattr(supervisor_module.subprocess, "Popen", inject_signal)
    args = SimpleNamespace(
        production=False, production_trial_isolated_test=False,
        worker_lock_file=str(tmp_path / "worker.lock"),
        database_heavy_lock_file=str(tmp_path / "heavy.lock"),
        worker_path=str(worker), policy_id="local-signal",
        test_pg_sleep_after_admission=0, test_pg_sleep_after_capture=0,
        test_recovery_pg_sleep=0, test_force_uncertain_drain=False,
        deadline_seconds=3, grace_seconds=1, drain_seconds=1,
        recovery_seconds=3,
    )
    started = time.monotonic()
    with pytest.raises(RuntimeError, match="finite trial liability remains|finite trial recovery failed"):
        supervisor_module.run_trial(args)
    assert time.monotonic() - started < 3
    assert children[signal_during_spawn - 1].poll() is not None
    assert (tmp_path / "worker.lock.quarantine").exists()


def test_trial_capture_progress_survives_a_later_guard_failure(monkeypatch):
    claims = iter([DirtyClaim(2001, 1, "worker:1", 1), None])
    monkeypatch.setattr(worker_module, "claim_dirty", lambda *_args, **_kwargs: next(claims))
    monkeypatch.setattr(worker_module, "process_dirty_claim", lambda *_args, **_kwargs: True)
    progress = {"claimed": 0, "acknowledged": 0, "redirtied": 0, "failed": 0}

    def stopped_guard(*_args):
        raise RuntimeError("guard stopped after one committed capture")

    with pytest.raises(RuntimeError, match="guard stopped"):
        process_capture_queue(
            FakeConnection(FakeCursor()), "worker", 10,
            RunBudget.create(10, MAX_SECONDS_PER_RUN), False,
            guard_check=stopped_guard, progress=progress, audit_run_id=11,
        )
    assert progress == {"claimed": 1, "acknowledged": 1, "redirtied": 0, "failed": 0}


class FakeCursor:
    def __init__(self, delete_count=1):
        self.executed = []
        self.rowcount = 0
        self.delete_count = delete_count

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, query, params=None):
        normalized = " ".join(query.split())
        self.executed.append((normalized, params))
        if normalized.startswith("delete from leaderboard_shadow_v1.fixture_capture_queue"):
            self.rowcount = self.delete_count
        elif normalized.startswith("update leaderboard_shadow_v1.fixture_capture_queue"):
            self.rowcount = 1

    def fetchone(self):
        return ({"ok": True},)


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def cursor(self, **_kwargs):
        return self._cursor


class CohortCursor(FakeCursor):
    def fetchall(self):
        return [(271, 27897)]


class ClaimCursor(FakeCursor):
    def fetchone(self):
        return {
            "fixture_id": 1001,
            "dirty_version": 4,
            "claim_token": "worker:token",
            "claim_fence": 7,
        }


class GenerationFinishCursor(FakeCursor):
    def __init__(self, row):
        super().__init__()
        self.row = row

    def fetchone(self):
        return self.row


class GenerationJobCursor(FakeCursor):
    def __init__(self, staged, snapshot_hash="fresh-hash"):
        super().__init__()
        rows = [
            {
                "fixture_id": 19729028,
                "expected_source_hash": "stale-hash",
                "change_kind": "upsert",
                "fencing_number": 4,
            },
            {"snapshot": {
                "source_hash": snapshot_hash,
                "facts": [{"player_id": 1}],
            }},
        ]
        if snapshot_hash != "stale-hash":
            rows.append({"capture_fixture_source_changes": [{
                "fixture_id": 19729028,
                "source_version": 2,
            }]})
        rows.append({"stage_fixture_result": staged})
        self.rows = iter(rows)

    def fetchone(self):
        return next(self.rows)


class GenerationJobConnection(FakeConnection):
    def __init__(self, cursor):
        super().__init__(cursor)
        self.isolation_levels = []

    def set_session(self, isolation_level):
        self.isolation_levels.append(isolation_level)


class WorkerAccountingCursor(FakeCursor):
    def __init__(self, rows):
        super().__init__()
        self.rows = iter(rows)

    def fetchone(self):
        return next(self.rows)


def test_capture_hook_is_inert_by_default(monkeypatch):
    monkeypatch.delenv("LEADERBOARD_SHADOW_CAPTURE_ENABLED", raising=False)
    cursor = FakeCursor()
    capture_fixture_ids(cursor, [3, 2, 2])
    assert cursor.executed == []


def test_capture_hook_is_sorted_and_fails_with_authoritative_transaction(monkeypatch):
    monkeypatch.setenv("LEADERBOARD_SHADOW_CAPTURE_ENABLED", "true")
    cursor = FakeCursor()
    capture_fixture_ids(cursor, [3, 2, 2])
    assert len(cursor.executed) == 1
    assert "capture_fixture_source_changes" in cursor.executed[0][0]
    assert cursor.executed[0][1] == ([2, 3],)

    class FailingCursor(FakeCursor):
        def execute(self, *_args, **_kwargs):
            raise RuntimeError("capture failed")

    with pytest.raises(RuntimeError, match="capture failed"):
        capture_fixture_ids(FailingCursor(), [2])


def test_dirty_acknowledgement_is_fenced_to_claimed_version():
    claim = DirtyClaim(1001, 4, "worker:token", 7)
    acknowledged = FakeCursor(delete_count=1)
    assert process_dirty_claim(FakeConnection(acknowledged), claim) is True
    assert not any(query.startswith("update leaderboard_shadow_v1.fixture_capture_queue") for query, _ in acknowledged.executed)

    redirtied = FakeCursor(delete_count=0)
    assert process_dirty_claim(FakeConnection(redirtied), claim) is False
    assert any(query.startswith("update leaderboard_shadow_v1.fixture_capture_queue") for query, _ in redirtied.executed)
    delete_params = next(params for query, params in redirtied.executed if query.startswith("delete from"))
    assert delete_params == (1001, 4, "worker:token", 7)


def test_dirty_claim_can_be_bounded_to_recorded_or_current_cohort():
    cursor = ClaimCursor()
    claim = claim_dirty(
        FakeConnection(cursor),
        "worker",
        league_id=27,
        season_id=25654,
    )
    assert claim == DirtyClaim(1001, 4, "worker:token", 7)
    query, params = cursor.executed[0]
    assert "fixture_source_versions recorded" in query
    assert "public.fixtures current_fixture" in query
    assert "for update of queue skip locked" in query
    assert params[:5] == (27, 27, 25654, 27, 25654)


def test_unscoped_dirty_claim_preserves_global_queue_ordering():
    cursor = ClaimCursor()
    claim_dirty(FakeConnection(cursor), "worker")
    query, params = cursor.executed[0]
    assert "order by queue.last_dirty_at, queue.fixture_id" in query
    assert params[:5] == (None, None, None, None, None)


def test_capture_resource_guard_is_checked_in_bounded_groups(monkeypatch):
    claims = iter([
        DirtyClaim(fixture_id, 1, f"worker:{fixture_id}", 1)
        for fixture_id in range(1, RESOURCE_GUARD_FIXTURE_INTERVAL * 2 + 2)
    ] + [None])
    guard_calls = []
    monkeypatch.setattr(worker_module, "claim_dirty", lambda *_args, **_kwargs: next(claims))
    monkeypatch.setattr(worker_module, "process_dirty_claim", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        worker_module,
        "enforce_resource_guard",
        lambda *_args, **_kwargs: guard_calls.append(True) or {"ok": True},
    )
    budget = RunBudget.create(MAX_FIXTURES_PER_RUN, MAX_SECONDS_PER_RUN)
    result = process_capture_queue(
        FakeConnection(FakeCursor()),
        "worker",
        MAX_FIXTURES_PER_RUN,
        budget,
        True,
    )
    assert result == {
        "claimed": RESOURCE_GUARD_FIXTURE_INTERVAL * 2 + 1,
        "acknowledged": RESOURCE_GUARD_FIXTURE_INTERVAL * 2 + 1,
        "redirtied": 0,
        "failed": 0,
    }
    assert len(guard_calls) == 3


def test_failed_capture_marks_interval_before_claim_cleanup(monkeypatch):
    claims = iter([DirtyClaim(1001, 1, "worker:1", 1), None])
    calls = []
    monkeypatch.setattr(worker_module, "claim_dirty", lambda *_args, **_kwargs: next(claims))
    monkeypatch.setattr(
        worker_module, "process_dirty_claim",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("failed write")),
    )
    monkeypatch.setattr(worker_module, "apply_recovery_timeouts", lambda *_args: calls.append("rollback"))
    monkeypatch.setattr(worker_module, "mark_failed_statement_interval", lambda *_args: calls.append("mark"))
    monkeypatch.setattr(worker_module, "fail_dirty_claim", lambda *_args: calls.append("cleanup"))
    monkeypatch.setattr(worker_module, "enforce_resource_guard", lambda *_args: {"ok": True})

    result = process_capture_queue(
        FakeConnection(FakeCursor()), "worker", 2,
        RunBudget.create(MAX_FIXTURES_PER_RUN, MAX_SECONDS_PER_RUN), True,
    )

    assert result["failed"] == 1
    assert calls == ["rollback", "mark", "cleanup"]


def test_worker_finish_is_included_in_final_resource_evidence(monkeypatch):
    calls = []
    monkeypatch.setattr(
        worker_module,
        "finish_worker_run",
        lambda *_args, **_kwargs: calls.append("finish"),
    )
    monkeypatch.setattr(
        worker_module,
        "apply_recovery_timeouts",
        lambda *_args, **_kwargs: calls.append("timeouts"),
    )
    monkeypatch.setattr(
        worker_module,
        "enforce_resource_guard",
        lambda *_args, **_kwargs: calls.append("guard") or {"ok": True},
    )

    evidence = finish_worker_run_with_resource_check(
        object(), 7, 11, 11, 0, True,
    )

    assert evidence == {"ok": True}
    assert calls == ["finish", "timeouts", "guard"]


def test_resource_guard_checkpoints_an_active_run_before_enforcement():
    class GuardCursor(FakeCursor):
        def __init__(self):
            super().__init__()
            self.rows = iter([
                {"active_run_id": 73},
                ({"ok": True},),
                {"evidence": {"ok": True, "breaches": []}},
            ])

        def fetchone(self):
            return next(self.rows)

    cursor = GuardCursor()
    evidence = worker_module.enforce_resource_guard(FakeConnection(cursor), False)

    assert evidence == {"ok": True, "breaches": []}
    assert "checkpoint_worker_resource_window" in cursor.executed[1][0]
    assert cursor.executed[1][1] == (73,)
    assert "enforce_resource_guard" in cursor.executed[2][0]


def test_worker_run_opens_and_closes_the_durable_resource_window():
    cursor = WorkerAccountingCursor([(73,), ({"ok": True},)])
    connection = FakeConnection(cursor)

    assert start_worker_run(connection, "worker:test") == 73
    finish_worker_run(connection, 73, 4, 4, 0)

    statements = [query for query, _ in cursor.executed]
    assert any("begin_worker_resource_window" in query for query in statements)
    assert "set_config('oddssearch.worker_failed_statement','false',false)" in statements[0]
    assert any("set_config('oddssearch.worker_run_id'" in query for query in statements)
    assert any("checkpoint_worker_resource_window" in query for query in statements)
    assert any("set_config('oddssearch.worker_run_id',''" in query for query in statements)
    assert statements.index(next(q for q in statements if "begin_worker_resource_window" in q)) \
        < statements.index(next(q for q in statements if "checkpoint_worker_resource_window" in q))


def test_interrupted_windows_are_recovered_before_new_work():
    evidence = {
        "ok": True,
        "recovered_count": 1,
        "database_window_fallback_count": 1,
    }
    cursor = WorkerAccountingCursor([{"evidence": evidence}])

    assert recover_unfinished_worker_runs(FakeConnection(cursor)) == evidence
    assert "recover_unfinished_worker_resource_windows" in cursor.executed[0][0]


def test_preflight_is_bounded_before_the_processing_budget_starts():
    cursor = FakeCursor()

    apply_preflight_timeouts(FakeConnection(cursor))

    assert cursor.executed == [
        (
            "select set_config('lock_timeout', %s, false)",
            ("2000ms",),
        ),
        (
            "select set_config('statement_timeout', %s, false)",
            (f"{PREFLIGHT_STATEMENT_TIMEOUT_MS}ms",),
        ),
    ]
    assert PREFLIGHT_STATEMENT_TIMEOUT_MS == 5_000


def test_superseded_generation_defers_to_a_replacement_without_worker_failure():
    cursor = GenerationFinishCursor(("superseded", 138, 1))

    assert finish_generation(FakeConnection(cursor), 1000016) is False
    assert len(cursor.executed) == 1
    assert "cohort_pending_publications generation" in cursor.executed[0][0]


def test_source_drift_is_recaptured_before_replacement_generation():
    cursor = GenerationJobCursor(staged=False)
    connection = GenerationJobConnection(cursor)

    assert process_generation_job(connection, 1000023, "worker") is False

    queries = [query for query, _params in cursor.executed]
    assert "claim_generation_job" in queries[0]
    assert "fixture_source_snapshot" in queries[1]
    assert "capture_fixture_source_changes" in queries[2]
    assert "stage_fixture_result" in queries[3]
    assert cursor.executed[2][1] == ([19729028],)
    assert connection.isolation_levels == ["REPEATABLE READ", "READ COMMITTED"]


def test_successful_staging_does_not_create_a_duplicate_source_event():
    cursor = GenerationJobCursor(staged=True, snapshot_hash="stale-hash")
    connection = GenerationJobConnection(cursor)

    assert process_generation_job(connection, 1000023, "worker") is True

    queries = [query for query, _params in cursor.executed]
    assert not any("capture_fixture_source_changes" in query for query in queries)


def test_failed_generation_marks_interval_before_failure_record(monkeypatch):
    class FailedStageCursor(GenerationJobCursor):
        def execute(self, query, params=None):
            if "stage_fixture_result" in query:
                raise RuntimeError("failed write")
            super().execute(query, params)

    calls = []
    monkeypatch.setattr(worker_module, "apply_recovery_timeouts", lambda *_args: calls.append("rollback"))
    monkeypatch.setattr(worker_module, "mark_failed_statement_interval", lambda *_args: calls.append("mark"))
    cursor = FailedStageCursor(staged=True, snapshot_hash="stale-hash")

    with pytest.raises(RuntimeError, match="failed write"):
        process_generation_job(GenerationJobConnection(cursor), 1000023, "worker")

    assert calls == ["rollback", "mark"]
    assert any("fail_generation_job" in query for query, _ in cursor.executed)


def test_failed_active_generation_remains_fatal():
    cursor = GenerationFinishCursor(("processing", 0, 1))

    with pytest.raises(RuntimeError, match="generation 1000016 has 1 failed jobs"):
        finish_generation(FakeConnection(cursor), 1000016)


def test_pending_cohorts_defer_both_recorded_and_current_dirty_cohorts():
    cursor = CohortCursor()
    assert pending_cohorts(FakeConnection(cursor), 3) == [(271, 27897)]
    query, params = cursor.executed[0]
    assert params == (3,)
    assert "fixture_capture_queue queue" in query
    assert "fixture_source_versions recorded" in query
    assert "public.fixtures current_fixture" in query
    assert (
        "(recorded.league_id, recorded.season_id) = "
        "(cursor.league_id, cursor.season_id)"
    ) in query
    assert (
        "(current_fixture.league_id, current_fixture.season_id) = "
        "(cursor.league_id, cursor.season_id)"
    ) in query


def test_worker_and_capture_hooks_are_pinned_in_the_vps_runtime_manifest():
    runtime_files = {
        line.strip()
        for line in (ROOT / "scripts/vps/runtime_files.txt").read_text().splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }
    assert {
        "scripts/leaderboard_shadow_capture.py",
        "scripts/leaderboard_shadow_worker.py",
        "scripts/vps/run_leaderboard_shadow_worker.sh",
        "scripts/export_to_supabase.py",
        "scripts/postmatch_fixture_detail_delivery.py",
    } <= runtime_files
    wrapper = (ROOT / "scripts/vps/run_leaderboard_shadow_worker.sh").read_text()
    assert "verify_runtime_manifest_or_exit" in wrapper
    assert "require_runtime_manifest_entries_or_exit" in wrapper
    assert "--fixture-limit 250" in wrapper
    assert "--max-seconds 30" in wrapper
    assert "--isolated-test-override" not in wrapper
    assert "run_with_database_heavy_lock" in wrapper
    assert "LEADERBOARD_SHADOW_DB_HEAVY_LOCK_WAIT_SECONDS:-0" in wrapper


def test_database_heavy_lock_defers_overlap_then_releases(tmp_path):
    if shutil.which("flock") is None:
        pytest.skip("Linux flock is unavailable on this host")

    lock_file = tmp_path / "database-heavy.lock"
    ready_file = tmp_path / "holder-ready"
    env = {
        **os.environ,
        "ODDSSEARCH_DB_HEAVY_LOCK_FILE": str(lock_file),
        "COMMON_SH": str(ROOT / "scripts/vps/common.sh"),
        "READY_FILE": str(ready_file),
    }
    holder = subprocess.Popen(
        [
            "bash",
            "-lc",
            'source "$COMMON_SH"; run_with_database_heavy_lock 0 '
            'bash -c \'printf ready > "$READY_FILE"; sleep 1\'',
        ],
        env=env,
    )
    try:
        deadline = time.monotonic() + 3
        while not ready_file.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        assert ready_file.exists(), "lock holder did not start"

        deferred = subprocess.run(
            [
                "bash",
                "-lc",
                'source "$COMMON_SH"; run_with_database_heavy_lock 0 true',
            ],
            env=env,
            capture_output=True,
            text=True,
        )
        assert deferred.returncode == 75
        assert "database-heavy maintenance slot unavailable" in deferred.stdout

        waited = subprocess.run(
            [
                "bash",
                "-lc",
                'source "$COMMON_SH"; run_with_database_heavy_lock 2 true',
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=4,
        )
        assert waited.returncode == 0, waited.stderr
        assert holder.wait(timeout=2) == 0

        released = subprocess.run(
            [
                "bash",
                "-lc",
                'source "$COMMON_SH"; run_with_database_heavy_lock 0 true',
            ],
            env=env,
            capture_output=True,
            text=True,
        )
        assert released.returncode == 0
    finally:
        if holder.poll() is None:
            holder.terminate()
            holder.wait(timeout=2)


def test_database_heavy_gates_refuse_durable_trial_quarantine(tmp_path):
    # macOS lacks flock(1); a no-op command still exercises the post-lock
    # quarantine decision. The Linux lock test above verifies real exclusion.
    fake_bin = tmp_path / "bin"
    if shutil.which("flock") is None:
        fake_bin.mkdir()
        fake_flock = fake_bin / "flock"
        fake_flock.write_text("#!/bin/sh\nexit 0\n")
        fake_flock.chmod(0o755)
    leaderboard_lock = tmp_path / "leaderboard.lock"
    marker = Path(str(leaderboard_lock) + ".quarantine")
    marker.write_text('{"policy_id":"local-test"}')
    launched = tmp_path / "launched"
    timing = tmp_path / "timing"
    env = {
        **os.environ,
        "ODDSSEARCH_DB_HEAVY_LOCK_FILE": str(tmp_path / "heavy.lock"),
        "LEADERBOARD_SHADOW_LOCK_FILE": str(leaderboard_lock),
        "COMMON_SH": str(ROOT / "scripts/vps/common.sh"),
        "LAUNCHED": str(launched),
        "TIMING": str(timing),
        "PATH": str(fake_bin) + os.pathsep + os.environ.get("PATH", ""),
    }
    for gate in (
        'run_with_database_heavy_lock 0 bash -c \'touch "$LAUNCHED"\'',
        'run_with_database_heavy_lock_timed 0 "$TIMING" bash -c \'touch "$LAUNCHED"\'',
    ):
        result = subprocess.run(
            ["bash", "-lc", f'source "$COMMON_SH"; {gate}'],
            env=env, capture_output=True, text=True, timeout=5, check=False,
        )
        assert result.returncode == 75, result.stderr
        assert "finite-trial quarantine blocks" in result.stdout
        assert not launched.exists()
        assert not timing.exists()
    marker.unlink()
    result = subprocess.run(
        ["bash", "-lc", 'source "$COMMON_SH"; run_with_database_heavy_lock 0 bash -c \'touch "$LAUNCHED"\''],
        env=env, capture_output=True, text=True, timeout=5, check=False,
    )
    assert result.returncode == 0, result.stderr
    assert launched.exists()


def test_run_budget_rejects_unbounded_fixture_and_time_limits():
    assert RunBudget.create(MAX_FIXTURES_PER_RUN, MAX_SECONDS_PER_RUN).available()
    with pytest.raises(ValueError, match="fixture limit"):
        RunBudget.create(MAX_FIXTURES_PER_RUN + 1, MAX_SECONDS_PER_RUN)
    with pytest.raises(ValueError, match="max seconds"):
        RunBudget.create(MAX_FIXTURES_PER_RUN, MAX_SECONDS_PER_RUN + 0.01)


def test_run_budget_stops_at_250_fixture_operations():
    budget = RunBudget.create(MAX_FIXTURES_PER_RUN, MAX_SECONDS_PER_RUN)
    for _ in range(MAX_FIXTURES_PER_RUN):
        assert budget.available()
        budget.consume()
    assert budget.processed == 250
    assert not budget.available()


def test_temp_file_limit_is_applied_only_with_parameter_privilege():
    class CapabilityCursor(FakeCursor):
        def __init__(self, permitted):
            super().__init__()
            self.permitted = permitted

        def fetchone(self):
            return (self.permitted,)

    denied = CapabilityCursor(False)
    assert configure_temp_file_limit_if_permitted(FakeConnection(denied)) is False
    assert sum("temp_file_limit" in query for query, _ in denied.executed) == 1

    permitted = CapabilityCursor(True)
    assert configure_temp_file_limit_if_permitted(FakeConnection(permitted)) is True
    statements = "\n".join(statement for statement, _ in permitted.executed)
    assert "leaderboard_shadow_v1.pipeline_state" in statements
    assert sum("temp_file_limit" in query for query, _ in permitted.executed) == 2


def test_run_budget_stops_when_30_second_deadline_is_reached(monkeypatch):
    clock = iter([
        100.0,
        100.0 + MAX_SECONDS_PER_RUN - SAFETY_CHECK_RESERVE_SECONDS - 0.01,
        100.0 + MAX_SECONDS_PER_RUN - SAFETY_CHECK_RESERVE_SECONDS,
    ])
    monkeypatch.setattr(worker_module.time, "monotonic", lambda: next(clock))
    budget = RunBudget.create(MAX_FIXTURES_PER_RUN, MAX_SECONDS_PER_RUN)
    assert budget.available()
    assert not budget.available()


def test_run_budget_reserves_time_for_mandatory_safety_checks(monkeypatch):
    near_deadline = 200.0 + MAX_SECONDS_PER_RUN - 0.1
    clock = iter([200.0, near_deadline, near_deadline])
    monkeypatch.setattr(worker_module.time, "monotonic", lambda: next(clock))
    budget = RunBudget.create(MAX_FIXTURES_PER_RUN, MAX_SECONDS_PER_RUN)
    assert budget.remaining_seconds() == pytest.approx(0.1)
    assert not budget.available()


def test_external_resource_evidence_is_exact_and_protected(tmp_path):
    path = tmp_path / "resource.json"
    path.write_text(json.dumps({
        "disk_available_bytes": 30 * 1024**3,
        "observed_at": "2026-10-28T18:00:00Z",
    }))
    path.chmod(0o600)
    cursor = FakeCursor()
    evidence = load_external_resource_evidence(FakeConnection(cursor), str(path))
    assert evidence["disk_available_bytes"] == 30 * 1024**3
    assert "refresh_external_resource_evidence_v2" in cursor.executed[0][0]
    assert cursor.executed[0][1] == (
        30 * 1024**3, "2026-10-28T18:00:00Z",
    )

    path.chmod(0o622)
    with pytest.raises(RuntimeError, match="non-group/world-writable"):
        load_external_resource_evidence(FakeConnection(FakeCursor()), str(path))


def test_external_resource_evidence_rejects_unknown_fields(tmp_path):
    path = tmp_path / "resource.json"
    path.write_text(json.dumps({
        "disk_available_bytes": 30 * 1024**3,
        "observed_at": "2026-10-28T18:00:00Z",
        "unreviewed_override": True,
    }))
    path.chmod(0o600)
    with pytest.raises(RuntimeError, match="unexpected or missing"):
        load_external_resource_evidence(FakeConnection(FakeCursor()), str(path))
