"""Exercise secret isolation at the actual supervisor subprocess boundary."""

import json
import os
import re
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

import scripts.leaderboard_finite_trial_supervisor as supervisor


WORKER_SETTINGS = {
    "PATH": os.defpath,
    "LANG": "C.UTF-8",
    "LC_ALL": "C",
    "LC_CTYPE": "C",
    "TZ": "UTC",
    "LEADERBOARD_SHADOW_RELEASE_SHA": "1" * 40,
    "LEADERBOARD_SHADOW_RESOURCE_EVIDENCE_FILE": "/reviewed/resource-evidence.json",
    "LEADERBOARD_FINITE_TRIAL_EXPECTED_DSN_SHA256": "2" * 64,
    "LEADERBOARD_FINITE_TRIAL_RELEASE_SHA": "1" * 40,
    "LEADERBOARD_FINITE_TRIAL_OPERATOR_NOTE": "reviewed transition",
}
WORKER_DSN = "postgresql://worker:worker-password@localhost/oddssearch_m3_env"
PARENT_SECRETS = {
    key: "parent-secret-" + key for key in (
        "SUPABASE_SERVICE_ROLE_KEY", "SUPABASE_ACCESS_TOKEN",
        "SUPABASE_DB_URL_OPERATOR", "SUPABASE_DB_URL_ADMIN",
        "OPERATOR_DATABASE_URL", "ADMIN_DATABASE_URL", "DATABASE_URL",
        "LEADERBOARD_DRAIN_DB_URL", "LEADERBOARD_FINITE_TRIAL_OPERATOR_DB_URL",
        "LEADERBOARD_FINITE_TRIAL_ADMIN_DB_URL",
        "LEADERBOARD_ROLLING_OPERATOR_DB_URL",
        "LEADERBOARD_SHADOW_OPERATOR_DB_URL",
        "LEADERBOARD_FINITE_TRIAL_FUTURE_SECRET",
        "LEADERBOARD_SHADOW_FUTURE_SECRET", "LEADERBOARD_ROLLING_FUTURE_SECRET",
        "LEADERBOARD_SHADOW_ENV_FILE", "SPORTMONKS_API_TOKEN",
        "AWS_SECRET_ACCESS_KEY", "PGPASSWORD", "PGPASSFILE", "PGSERVICE",
        "PGSERVICEFILE", "PGOPTIONS", "PGHOST", "PGPORT", "PGUSER",
        "PGDATABASE", "PGSSLKEY", "PGSSLCERT", "PGSSLROOTCERT", "HOME",
        "PYTHONPATH", "PYTHONHOME", "PYTHONSTARTUP", "LD_PRELOAD",
        "DYLD_INSERT_LIBRARIES",
    )
}
# An unused fallback DSN may have broader privileges than the selected DSN.
PARENT_SECRETS["SUPABASE_DB_URL"] = "postgresql://admin:admin-password@localhost/postgres"
STALE_METADATA = {
    "LEADERBOARD_FINITE_TRIAL_" + key: "stale-" + key for key in (
        "CONNECTION_TOKEN", "SUPERVISOR_PID", "WORKER_LOCK_FD",
        "WORKER_LOCK_PATH", "HEAVY_LOCK_FD", "HEAVY_LOCK_PATH",
    )
}


@pytest.mark.parametrize("production,rolling,prefix", [
    (False, False, "trial"), (True, False, "finite"), (True, True, "rolling"),
])
@pytest.mark.parametrize("reconcile", [False, True])
def test_all_child_launches_isolate_secrets_and_retain_worker_inputs(
    tmp_path, monkeypatch, production, rolling, prefix, reconcile,
):
    parent_environment = {
        **WORKER_SETTINGS, **PARENT_SECRETS, **STALE_METADATA,
        "SUPABASE_DB_URL_SESSION": WORKER_DSN,
    }
    monkeypatch.setattr(supervisor.os, "environ", parent_environment.copy())
    database_calls = []

    def database_check(url, *_args, **_kwargs):
        database_calls.append(url)
        # Filtering must not strip the supervisor's own operator credentials.
        assert dict(os.environ) == parent_environment

    monkeypatch.setattr(supervisor, "assert_disposable_database", database_check)
    monkeypatch.setattr(supervisor, "drain_token_backends", database_check)
    monkeypatch.setattr(supervisor, "drain_exact_backend", database_check)

    # Use real child processes, and import the real worker to verify its
    # import-time configuration still resolves without ambient Python paths.
    worker = tmp_path / "probe_worker.py"
    worker.write_text(
        "import json,os,sys\n"
        "from pathlib import Path\n"
        f"sys.path.insert(0, {str(Path(supervisor.__file__).parent)!r})\n"
        "import leaderboard_shadow_worker as worker\n"
        "locks = []\n"
        "for kind in ('WORKER', 'HEAVY'):\n"
        "    prefix = 'LEADERBOARD_FINITE_TRIAL_' + kind + '_LOCK_'\n"
        "    inherited = os.fstat(int(os.environ[prefix + 'FD']))\n"
        "    named = os.stat(os.environ[prefix + 'PATH'])\n"
        "    locks.append((inherited.st_dev, inherited.st_ino) == "
        "(named.st_dev, named.st_ino))\n"
        "record = {'command': sys.argv[1], 'environment': dict(os.environ),\n"
        "          'database_url': worker.DATABASE_URL, 'release': worker.RELEASE_SHA,\n"
        "          'evidence': worker.RESOURCE_EVIDENCE_FILE, 'locks': locks,\n"
        "          'parent_pid': os.getppid()}\n"
        "with open(Path(__file__).with_suffix('.jsonl'), 'a') as output:\n"
        "    output.write(json.dumps(record) + '\\n')\n"
        "print(json.dumps({'state': 'already_settled'}), flush=True)\n"
    )
    args = SimpleNamespace(
        production=production, rolling=rolling,
        production_trial_isolated_test=production, follow_rolling_successors=False,
        worker_lock_file=str(tmp_path / "worker.lock"),
        database_heavy_lock_file=str(tmp_path / "heavy.lock"),
        worker_path=str(worker), policy_id="local-env-policy",
        fixture_limit=10, capture_limit=10, cohort_limit=0, job_limit=0,
        max_seconds=30, test_pg_sleep_after_admission=0,
        test_pg_sleep_after_capture=0, test_recovery_pg_sleep=0,
        test_force_uncertain_drain=False, deadline_seconds=5,
        grace_seconds=1, drain_seconds=1, recovery_seconds=5,
    )
    if reconcile:
        for suffix in (".quarantine", ".quarantine.heavy"):
            supervisor.write_quarantine_marker(
                Path(args.worker_lock_file + suffix), args.policy_id,
                "inflight", ["a" * 16], rolling=rolling,
            )
        assert supervisor.reconcile_quarantine(args) == 0
    else:
        assert supervisor.run_trial(args) == 0

    records = [json.loads(line) for line in worker.with_suffix(".jsonl").read_text().splitlines()]
    expected_commands = [prefix + "-recover"]
    if not reconcile:
        expected_commands.insert(0, prefix + "-once")
    assert [record["command"] for record in records] == expected_commands
    tokens = []
    for record in records:
        environment = record["environment"]
        assert {key: environment[key] for key in WORKER_SETTINGS} == WORKER_SETTINGS
        assert record["database_url"] == environment["SUPABASE_DB_URL_SESSION"] == WORKER_DSN
        assert record["release"] == WORKER_SETTINGS["LEADERBOARD_SHADOW_RELEASE_SHA"]
        assert record["evidence"] == WORKER_SETTINGS["LEADERBOARD_SHADOW_RESOURCE_EVIDENCE_FILE"]
        assert record["locks"] == [True, True]
        assert record["parent_pid"] == os.getpid()
        assert environment["LEADERBOARD_FINITE_TRIAL_SUPERVISOR_PID"] == str(os.getpid())
        assert environment["LEADERBOARD_FINITE_TRIAL_WORKER_LOCK_PATH"] == args.worker_lock_file
        assert environment["LEADERBOARD_FINITE_TRIAL_HEAVY_LOCK_PATH"] == args.database_heavy_lock_file
        token = environment["LEADERBOARD_FINITE_TRIAL_CONNECTION_TOKEN"]
        assert re.fullmatch(r"[0-9a-f]{16}", token)
        assert token != "a" * 16
        tokens.append(token)
        assert not (PARENT_SECRETS.keys() & environment.keys())
        assert not (set(PARENT_SECRETS.values()) & set(environment.values()))
        assert not (set(STALE_METADATA.values()) & set(environment.values()))
        # Python itself adds this encoding hint on macOS, even with an empty env.
        runtime_keys = {"__CF_USER_TEXT_ENCODING"} if sys.platform == "darwin" else set()
        assert environment.keys() - runtime_keys == WORKER_SETTINGS.keys() | STALE_METADATA.keys() | {
            "SUPABASE_DB_URL_SESSION",
        }
    assert len(tokens) == len(set(tokens))
    assert database_calls and set(database_calls) == {WORKER_DSN}
    assert dict(os.environ) == parent_environment
    assert not Path(args.worker_lock_file + ".quarantine").exists()
    assert not Path(args.worker_lock_file + ".quarantine.heavy").exists()


@pytest.mark.parametrize("session_url", [None, ""])
def test_child_environment_keeps_fallback_worker_dsn_without_ambient_defaults(monkeypatch, session_url):
    environment = {"SUPABASE_DB_URL": WORKER_DSN, "PGPASSWORD": "operator-password"}
    if session_url is not None:
        environment["SUPABASE_DB_URL_SESSION"] = session_url
    monkeypatch.setattr(supervisor.os, "environ", environment)
    child = supervisor.child_environment("b" * 16, (10, 11), "/worker.lock", "/heavy.lock")
    assert child["SUPABASE_DB_URL_SESSION"] == WORKER_DSN
    assert "SUPABASE_DB_URL" not in child
    assert "PGPASSWORD" not in child
    assert child.keys() == STALE_METADATA.keys() | {"SUPABASE_DB_URL_SESSION"}


def test_child_environment_requires_explicit_worker_dsn(monkeypatch):
    monkeypatch.setattr(supervisor.os, "environ", PARENT_SECRETS | {"SUPABASE_DB_URL": ""})
    with pytest.raises(RuntimeError, match="trial database URL is required"):
        supervisor.child_environment("b" * 16, (10, 11), "/worker.lock", "/heavy.lock")
