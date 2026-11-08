"""Exercise the real Linux supervisor and lock wrapper with an isolated stub worker."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
IMAGE = "public.ecr.aws/supabase/postgres:17.6.1.075"

STUB_WORKER = '''#!/usr/bin/env python3
import argparse, json, os, pathlib, sys
p=argparse.ArgumentParser()
p.add_argument('--leagues', required=True)
p.add_argument('--batch-size'); p.add_argument('--max-cohorts'); p.add_argument('--max-batches')
p.add_argument('--report-json', required=True)
a=p.parse_args()
sequence=pathlib.Path('/repo/sequence.json')
items=json.loads(sequence.read_text())
item=items.pop(0) if items else 'empty'
sequence.write_text(json.dumps(items))
with pathlib.Path('/repo/visits.txt').open('a') as out: out.write(a.leagues+' '+item+'\\n')
if item == 'fail':
    print('stub candidate selector failed', file=sys.stderr)
    sys.exit(1)
selected=1 if item=='work' else 0
report={'release_id':os.environ['RUNTIME_RELEASE_ID'],
 'scope_attempt_id':os.environ['STATS_RECONCILE_SCOPE_ATTEMPT_ID'],
 'scope_epoch':os.environ['STATS_RECONCILE_SCOPE_EPOCH'],
 'leagues':[int(v) for v in a.leagues.split(',')],
 'status':'success','batches':selected,'fixtures_selected':selected,
 'fixtures_accepted':0,'stage_seconds':{'provider_fetch':0}}
pathlib.Path(a.report_json).write_text(json.dumps(report))
'''
STUB_PSQL = '''#!/usr/bin/env python3
import json, pathlib, sys
if pathlib.Path('/repo/psql_fail').exists():
    print('stub heartbeat write failed', file=sys.stderr)
    sys.exit(1)
values={}
for i,arg in enumerate(sys.argv[:-1]):
    if arg=='-v' and '=' in sys.argv[i+1]:
        key,value=sys.argv[i+1].split('=',1);values[key]=value
with pathlib.Path('/repo/ledger.jsonl').open('a') as out:
    out.write(json.dumps(values)+'\\n')
'''


def docker_available() -> bool:
    if not shutil.which("docker"):
        return False
    result = subprocess.run(["docker", "image", "inspect", IMAGE],
                            env={**os.environ, "DOCKER_CONTEXT": "colima-odds-launch"},
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return result.returncode == 0


@pytest.fixture
def linux_repo() -> tuple[Path, str]:
    if not docker_available():
        pytest.skip("local Linux image with flock and timeout is unavailable")
    (ROOT / "tmp").mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="reconcile-supervisor-", dir=ROOT / "tmp") as directory:
        repo = Path(directory)
        (repo / "scripts/vps").mkdir(parents=True)
        (repo / "config").mkdir()
        (repo / "test-bin").mkdir()
        (repo / "test-bin/psql").write_text(STUB_PSQL)
        (repo / "test-bin/psql").chmod(0o755)
        (repo / ".venv/bin").mkdir(parents=True)
        (repo / ".venv/bin/python").symlink_to("/usr/bin/python3")
        files = {
            "scripts/vps/common.sh": (ROOT / "scripts/vps/common.sh").read_bytes(),
            "scripts/vps/run_stats_reconciliation.sh": (ROOT / "scripts/vps/run_stats_reconciliation.sh").read_bytes(),
            "scripts/vps/select_stats_reconciliation_leagues.py": (ROOT / "scripts/vps/select_stats_reconciliation_leagues.py").read_bytes(),
            "scripts/reconcile_stats_provider_queue.py": STUB_WORKER.encode(),
            "config/league_ids.txt": b"8\n9\n12\n",
        }
        for name, content in files.items():
            target = repo / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
        manifest = "".join(f"{hashlib.sha1(content).hexdigest()}  {name}\n" for name, content in files.items())
        (repo / "scripts/vps/runtime_manifest.sha1").write_text(manifest)
        name = "odds-reconcile-test-" + uuid.uuid4().hex[:12]
        docker_env = {**os.environ, "DOCKER_CONTEXT": "colima-odds-launch"}
        subprocess.run(["docker", "run", "-d", "--name", name, "--network", "none",
                        "--entrypoint", "/usr/bin/bash", IMAGE, "-lc", "mkdir -p /repo && sleep infinity"],
                       env=docker_env, check=True, capture_output=True, text=True)
        try:
            subprocess.run(["docker", "cp", str(repo) + "/.", name + ":/repo"],
                           env=docker_env, check=True, capture_output=True, text=True)
            yield repo, name
        finally:
            subprocess.run(["docker", "rm", "-f", name], env=docker_env,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def run_supervisor(linux_repo: tuple[Path, str], sequence: list[str], *, sleep_seconds: str = "0") -> subprocess.CompletedProcess[str]:
    repo, name = linux_repo
    (repo / "sequence.json").write_text(json.dumps(sequence))
    docker_env = {**os.environ, "DOCKER_CONTEXT": "colima-odds-launch"}
    subprocess.run(["docker", "cp", str(repo / "sequence.json"), name + ":/repo/sequence.json"],
                   env=docker_env, check=True, capture_output=True, text=True)
    env = {
        "DOCKER_CONTEXT": "colima-odds-launch",
        "PATH": "/repo/test-bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "OPERATIONS_CHECK_RUNNER_DATABASE_URL": "local://stub-only",
        "STATS_RECONCILE_LEAGUES": "8,9,12",
        "STATS_RECONCILE_LEAGUE_GROUP_SIZE": "1",
        "STATS_RECONCILE_LEAGUE_CURSOR_FILE": "/repo/cursor.json",
        "STATS_RECONCILE_REPORT": "/repo/report.json",
        "STATS_RECONCILE_RUN_LOG": "/repo/run.log",
        "STATS_RECONCILE_ATTEMPT_RECEIPT": "/repo/receipt.json",
        "STATS_RECONCILE_OUTCOME_FILE": "/repo/outcome.json",
        "STATS_RECONCILE_TIMING_FILE": "/repo/timing",
        "STATS_RECONCILE_SUPERVISOR_LOCK": "/repo/supervisor.lock",
        "ODDSSEARCH_DB_HEAVY_LOCK_FILE": "/repo/heavy.lock",
        "ODDS_SYNC_LOCK_FILE": "/repo/canonical.lock",
        "ODDS_SYNC_LIVE_SCHEDULE_ENABLED": "false",
        "STATS_RECONCILE_LIVE_SETTLEMENT_GUARD_SECONDS": "0",
        "STATS_RECONCILE_LIVE_GRACE_SECONDS": "0",
        "STATS_RECONCILE_SUCCESS_HANDOFF_SECONDS": "0",
        "STATS_RECONCILE_SLEEP_SECONDS": sleep_seconds,
        "STATS_RECONCILE_MAX_HOLD_SECONDS": "10",
        "STATS_RECONCILE_MIN_LEASE_SECONDS": "0",
        "STATS_RECONCILE_DB_HEAVY_LOCK_WAIT_SECONDS": "0",
    }
    command = ["docker", "exec", "-w", "/repo"]
    for key, value in env.items():
        if key != "DOCKER_CONTEXT":
            command.extend(["-e", f"{key}={value}"])
    command.extend([name, "/usr/bin/bash", "scripts/vps/run_stats_reconciliation.sh"])
    result = subprocess.run(command, env=docker_env, text=True, capture_output=True, timeout=25)
    for filename in ("visits.txt", "cursor.json", "ledger.jsonl"):
        subprocess.run(["docker", "cp", name + ":/repo/" + filename, str(repo / filename)],
                       env=docker_env, capture_output=True, text=True)
    return result


def test_empty_first_scope_reaches_work_later_and_requires_complete_empty_sweep(linux_repo: tuple[Path, str]) -> None:
    repo, _ = linux_repo
    result = run_supervisor(linux_repo, ["empty", "work", "empty", "empty", "empty"])
    assert result.returncode == 0, result.stdout + result.stderr
    visits = (repo / "visits.txt").read_text().splitlines()
    assert visits == ["8 empty", "9 work", "12 empty", "8 empty", "9 empty"]
    assert "queue drained across all 3 league groups" in result.stdout
    state = json.loads((repo / "cursor.json").read_text())
    assert state["empty_streak"] == 3
    assert state["unresolved"] == {}
    ledger = [json.loads(line) for line in (repo / "ledger.jsonl").read_text().splitlines()]
    actual = [row for row in ledger if row["job_id"] == "run_stats_reconciliation"]
    aggregate = [row for row in ledger if row["job_id"] == "run_stats_reconciliation_scope_health"]
    assert len(actual) == len(aggregate) == len(visits)
    assert [row["work_units"] for row in actual] == ["0", "1", "0", "0", "0"]
    assert all(row["work_units"] == "" for row in aggregate)
    assert [json.loads(row["evidence"])["scope_health"] for row in aggregate] == ["unknown", "unknown", "healthy", "healthy", "healthy"]
    assert json.loads(aggregate[-1]["evidence"])["empty_sweep"] is True


def test_failed_scope_remains_visible_through_other_empty_scopes_and_recovers(linux_repo: tuple[Path, str]) -> None:
    repo, _ = linux_repo
    result = run_supervisor(linux_repo, ["empty", "fail", "empty", "empty", "empty", "empty"])
    assert result.returncode == 0, result.stdout + result.stderr
    visits = (repo / "visits.txt").read_text().splitlines()
    assert visits[:4] == ["8 empty", "9 fail", "12 empty", "8 empty"]
    assert "9 empty" in visits
    state = json.loads((repo / "cursor.json").read_text())
    assert state["unresolved"] == {}
    assert state["last_failure"]["9"]["outcome"] == "failed"
    assert state["empty_streak"] == 3
    ledger = [json.loads(line) for line in (repo / "ledger.jsonl").read_text().splitlines()]
    aggregate = [json.loads(row["evidence"]) for row in ledger if row["job_id"] == "run_stats_reconciliation_scope_health"]
    assert aggregate[1]["scope_health"] == "degraded"
    assert aggregate[2]["scope_health"] == "degraded"
    assert aggregate[2]["unresolved"] == [9]
    assert aggregate[-1]["scope_health"] == "healthy"


def test_interrupted_claim_is_retained_during_supervisor_restart(linux_repo: tuple[Path, str]) -> None:
    repo, name = linux_repo
    claim = subprocess.run(["docker", "exec", "-w", "/repo", name, "/usr/bin/python3",
                            "scripts/vps/select_stats_reconciliation_leagues.py", "next",
                            "--leagues", "8,9,12", "--group-size", "1", "--cursor-file", "/repo/cursor.json",
                            "--release-id", "test-release"],
                           env={**os.environ, "DOCKER_CONTEXT": "colima-odds-launch"},
                           text=True, capture_output=True, timeout=10)
    assert claim.returncode == 0, claim.stderr
    # A changed release starts a new unknown epoch and retains the prior pending
    # scope as interrupted; the supervisor then visits every configured league.
    result = run_supervisor(linux_repo, ["empty", "empty", "empty"])
    assert result.returncode == 0, result.stdout + result.stderr
    state = json.loads((repo / "cursor.json").read_text())
    assert state["last_failure"]["8"]["outcome"] == "interrupted"
    assert state["unresolved"] == {}
    assert (repo / "visits.txt").read_text().splitlines() == ["8 empty", "9 empty", "12 empty"]


def test_heavy_lock_deferral_does_not_claim_or_skip_first_league(linux_repo: tuple[Path, str]) -> None:
    repo, name = linux_repo
    docker_env = {**os.environ, "DOCKER_CONTEXT": "colima-odds-launch"}
    subprocess.run(["docker", "exec", "-d", name, "/usr/bin/bash", "-lc",
                    "exec 8>/repo/heavy.lock; flock 8; touch /repo/held; sleep 0.5"],
                   env=docker_env, check=True, capture_output=True, text=True)
    for _ in range(50):
        ready = subprocess.run(["docker", "exec", name, "test", "-f", "/repo/held"],
                               env=docker_env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if ready.returncode == 0:
            break
        time.sleep(0.02)
    else:
        pytest.fail("stub heavy lock was not acquired")
    result = run_supervisor(linux_repo, ["empty", "empty", "empty"], sleep_seconds="0.05")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "deferred until database-heavy slot is available" in result.stdout
    assert (repo / "visits.txt").read_text().splitlines() == ["8 empty", "9 empty", "12 empty"]


def test_missing_heartbeat_stops_before_publishing_healthy_aggregate(linux_repo: tuple[Path, str]) -> None:
    repo, name = linux_repo
    docker_env = {**os.environ, "DOCKER_CONTEXT": "colima-odds-launch"}
    subprocess.run(["docker", "exec", name, "touch", "/repo/psql_fail"],
                   env=docker_env, check=True, capture_output=True, text=True)
    result = run_supervisor(linux_repo, ["empty", "empty", "empty"])
    assert result.returncode != 0
    assert "pipeline heartbeat write failed for run_stats_reconciliation" in result.stderr
    assert (repo / "visits.txt").read_text().splitlines() == ["8 empty"]
    assert not (repo / "ledger.jsonl").exists()
