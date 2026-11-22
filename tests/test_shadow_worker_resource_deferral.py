#!/usr/bin/env python3
"""Exercise the protected wrapper with local locks and a fake worker, never a DB."""

from datetime import datetime, timedelta, timezone
from pathlib import Path
import fcntl
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile


ROOT = Path(__file__).resolve().parents[1]
WRAPPER = ROOT / "scripts/vps/run_leaderboard_shadow_worker.sh"
WORKER = ROOT / "scripts/leaderboard_shadow_worker.py"
EXPECTED_ARGS = [
    "run-once", "--fixture-limit", "250", "--max-seconds", "30",
    "--capture-limit", "250", "--job-limit", "250",
]
assert '"LEADERBOARD_SHADOW_RESOURCE_EVIDENCE_FILE"' in WORKER.read_text()
assert '"/var/lib/oddssearch/leaderboard-shadow-resource.json"' in WORKER.read_text()


def exercise(name: str, evidence, *, expected_worker: bool, new_mtime=False,
             hold_heavy=False, file_kind="regular"):
    with tempfile.TemporaryDirectory(prefix="shadow-resource-deferral-") as directory:
        home = Path(directory)
        release = home / "release"
        vps = release / "scripts/vps"
        vps.mkdir(parents=True)
        shutil.copy2(WRAPPER, vps / WRAPPER.name)
        (release / "scripts/leaderboard_shadow_worker.py").write_text("# fake worker target\n")
        worker_lock = home / "worker.lock"
        heavy_lock = home / "heavy.lock"
        worker_lock.touch()
        heavy_lock.touch()
        (vps / "common.sh").write_text(
            f'TRIAL_WORKER_LOCK="{worker_lock}"\n'
            f'TRIAL_HEAVY_LOCK="{heavy_lock}"\n'
            'verify_runtime_manifest_or_exit() { :; }\n'
            'require_default_trial_lock_paths_or_exit() { :; }\n'
            'require_no_cron_transition() { :; }\n'
            'require_runtime_manifest_entries_or_exit() { :; }\n'
            'check_trial_lock_fd() { :; }\n'
            'has_quarantine_marker() { return 1; }\n'
            'run_with_database_heavy_lock() {\n'
            '  local wait="$1"; shift\n'
            '  ( exec 8<"$TRIAL_HEAVY_LOCK"; flock -n 8 || exit 75; "$@" )\n'
            '}\n'
        )
        observation = home / "selected-provider-evidence.json"
        if file_kind == "fifo":
            os.mkfifo(observation)
        elif file_kind == "symlink":
            target = home / "actual-provider-evidence.json"
            target.write_text(json.dumps(sample(30)))
            observation.symlink_to(target)
        elif file_kind == "directory":
            observation.mkdir()
        elif evidence is not None:
            observation.write_text(evidence if isinstance(evidence, str) else json.dumps(evidence))
            if new_mtime:
                os.utime(observation, None)
        envfile = home / "shadow.env"
        envfile.write_text(
            f'LEADERBOARD_SHADOW_RESOURCE_EVIDENCE_FILE="{observation}"\n'
            'LEADERBOARD_SHADOW_RELEASE_SHA=77a2c1a702e991db66eda813cbc3c1564872f902\n'
        )
        bin_dir = home / "bin"
        bin_dir.mkdir()
        args_record = home / "worker-args.json"
        fake_python = bin_dir / "python3"
        fake_python.write_text(
            f'#!{sys.executable}\n'
            'import json, os, sys\n'
            'from pathlib import Path\n'
            f'if sys.argv[1] == "-": os.execv({sys.executable!r}, [{sys.executable!r}, *sys.argv[1:]])\n'
            f'Path({str(args_record)!r}).write_text(json.dumps(sys.argv[1:]))\n'
        )
        fake_python.chmod(0o755)
        fake_flock = bin_dir / "flock"
        fake_flock.write_text(
            f'#!{sys.executable}\n'
            'import fcntl, sys\n'
            'try: fcntl.flock(int(sys.argv[-1]), fcntl.LOCK_EX | fcntl.LOCK_NB)\n'
            'except BlockingIOError: raise SystemExit(1)\n'
        )
        fake_flock.chmod(0o755)
        env = dict(os.environ,
                   PATH=f"{bin_dir}:{os.environ['PATH']}",
                   LEADERBOARD_SHADOW_ENV_FILE=str(envfile))
        held_fd = os.open(heavy_lock, os.O_RDONLY) if hold_heavy else None
        try:
            if held_fd is not None:
                fcntl.flock(held_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            process = subprocess.Popen(["bash", str(vps / WRAPPER.name)], env=env,
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                       text=True, start_new_session=True)
            try:
                stdout, stderr = process.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.communicate(timeout=5)
                raise AssertionError(f"{name} blocked past the local timeout")
        finally:
            if held_fd is not None:
                os.close(held_fd)
        assert process.returncode == 0, (name, stdout, stderr)
        if expected_worker:
            assert args_record.exists(), (name, "worker was skipped", stderr)
            args = json.loads(args_record.read_text())
            assert args[1:] == EXPECTED_ARGS, (name, args)
            assert args[0] == str(release / "scripts/leaderboard_shadow_worker.py")
        else:
            assert not args_record.exists(), (name, "worker was called")
            if not hold_heavy:
                assert "[SKIPPED] fresh provider disk observation unavailable" in stderr
        print(f"PASS {name}")


def sample(seconds_ago: float):
    return {
        "disk_available_bytes": 91_600_000_000,
        "observed_at": (datetime.now(timezone.utc) - timedelta(seconds=seconds_ago)).isoformat(),
    }


exercise("genuine fresh sample runs unchanged 250/30 worker", sample(30), expected_worker=True)
exercise("near 210-second boundary admitted", sample(208), expected_worker=True)
exercise("past 210-second margin deferred", sample(212), expected_worker=False)
exercise("old provider time with new file mtime deferred", sample(370), expected_worker=False, new_mtime=True)
exercise("future provider time deferred", sample(-30), expected_worker=False)
exercise("missing evidence deferred", None, expected_worker=False)
exercise("malformed evidence deferred", "{broken", expected_worker=False)
exercise("wrong evidence shape deferred", {"disk_available_bytes": 100}, expected_worker=False)
exercise("FIFO without writer defers without blocking", None, expected_worker=False, file_kind="fifo")
exercise("symlink evidence defers", None, expected_worker=False, file_kind="symlink")
exercise("directory evidence defers before read", None, expected_worker=False, file_kind="directory")
exercise("busy source-priority heavy lock defers before worker", sample(30),
         expected_worker=False, hold_heavy=True)
