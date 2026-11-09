#!/usr/bin/env python3
"""Disposable-only whole-process trial supervisor; not a production entrypoint."""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import psycopg2


def database_url() -> str:
    url = os.environ.get("SUPABASE_DB_URL_SESSION") or os.environ.get("SUPABASE_DB_URL")
    if not url:
        raise RuntimeError("trial database URL is required")
    return url


def bounded_connection(url: str):
    return psycopg2.connect(
        url, connect_timeout=5,
        options="-c statement_timeout=5000 -c lock_timeout=2000",
    )


def assert_disposable_database(url: str) -> None:
    with bounded_connection(url) as conn:
        with conn.cursor() as cur:
            cur.execute("select current_database(), inet_server_addr() is null")
            name, socket_only = cur.fetchone()
    if not socket_only or not str(name).startswith("oddssearch_m3"):
        raise RuntimeError("finite trial supervisor requires a socket-only oddssearch_m3 database")


def acquire_lock(path: str):
    if not path or not Path(path).parent.is_dir():
        raise RuntimeError("finite trial lock directory is missing")
    handle = open(path, "a", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        return None
    return handle


def stop_process_group(process: subprocess.Popen, grace_seconds: float) -> tuple[str, str]:
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return process.communicate(timeout=grace_seconds)
    try:
        return process.communicate(timeout=grace_seconds)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        return process.communicate(timeout=grace_seconds)


def drain_exact_backend(url: str, policy_id: str, seconds: float) -> None:
    deadline = time.monotonic() + seconds
    conn = bounded_connection(url)
    try:
        conn.autocommit = True
        while True:
            with conn.cursor() as cur:
                cur.execute("select pg_catalog.pg_stat_clear_snapshot()")
                # The client process has exited while both host locks remain
                # held. Terminate only the still-open reservation's exact
                # backend PID/start identity, never another database process.
                cur.execute(
                    """
                    select pg_terminate_backend(activity.pid)
                    from admission_proto.reservations reservation
                    join pg_catalog.pg_stat_activity activity
                      on activity.pid=reservation.owner_backend_pid
                     and activity.backend_start=reservation.owner_backend_started_at
                    where reservation.policy_id=%s and reservation.settled_at is null
                    """,
                    (policy_id,),
                )
                terminated = cur.fetchall()
            if not terminated:
                return
            if time.monotonic() >= deadline:
                raise RuntimeError("finite trial owner backend did not drain")
            time.sleep(0.1)
    finally:
        conn.close()


def run_trial(args) -> int:
    url = database_url()
    assert_disposable_database(url)
    worker_lock = acquire_lock(args.worker_lock_file)
    if worker_lock is None:
        return 75
    try:
        heavy_lock = acquire_lock(args.database_heavy_lock_file)
        if heavy_lock is None:
            return 75
        try:
            worker = [
                sys.executable, args.worker_path, "trial-once",
                "--isolated-test-override", "--trial-policy-id", args.policy_id,
                "--fixture-limit", "10", "--capture-limit", "10",
                "--cohort-limit", "0", "--job-limit", "0",
                "--max-seconds", "30",
            ]
            if args.test_pg_sleep_after_admission:
                worker += [
                    "--trial-test-pg-sleep-after-admission",
                    str(args.test_pg_sleep_after_admission),
                ]
            if args.test_pg_sleep_after_capture:
                worker += [
                    "--trial-test-pg-sleep-after-capture",
                    str(args.test_pg_sleep_after_capture),
                ]
            process = subprocess.Popen(
                worker, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True, start_new_session=True,
            )
            timed_out = False
            try:
                output, errors = process.communicate(timeout=args.deadline_seconds)
            except subprocess.TimeoutExpired:
                timed_out = True
                output, errors = stop_process_group(process, args.grace_seconds)
            if output:
                print(output, end="")
            if errors:
                print(errors, end="", file=sys.stderr)
            # A killed client can leave its backend executing a statement.
            # Never recover or release the exclusion locks until it is gone.
            drain_exact_backend(url, args.policy_id, args.drain_seconds)
            recovered = subprocess.run(
                [
                    sys.executable, args.worker_path, "trial-recover",
                    "--isolated-test-override", "--trial-policy-id", args.policy_id,
                ],
                capture_output=True, text=True,
                timeout=args.recovery_seconds, check=False,
            )
            if recovered.returncode != 0:
                raise RuntimeError(
                    "finite trial recovery failed: " + recovered.stderr[-1000:]
                )
            state = json.loads(recovered.stdout)["state"]
            if state not in {"recovered", "already_settled"}:
                raise RuntimeError(f"finite trial liability remains: {state}")
            print(json.dumps({
                "event": "isolated_finite_trial_supervised",
                "policy_id": args.policy_id, "worker_exit": process.returncode,
                "timed_out": timed_out, "recovery_state": state,
            }, sort_keys=True), flush=True)
            if timed_out:
                return 124
            if process.returncode != 0 or state != "already_settled":
                return 1
            return 0
        finally:
            heavy_lock.close()
    finally:
        worker_lock.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker-path", required=True)
    parser.add_argument("--policy-id", required=True)
    parser.add_argument("--worker-lock-file", required=True)
    parser.add_argument("--database-heavy-lock-file", required=True)
    parser.add_argument("--deadline-seconds", type=float, default=45)
    parser.add_argument("--grace-seconds", type=float, default=5)
    parser.add_argument("--drain-seconds", type=float, default=10)
    parser.add_argument("--recovery-seconds", type=float, default=15)
    parser.add_argument("--test-pg-sleep-after-admission", type=float, default=0)
    parser.add_argument("--test-pg-sleep-after-capture", type=float, default=0)
    args = parser.parse_args()
    if not (1 <= args.deadline_seconds <= 60 and 1 <= args.grace_seconds <= 10
            and 1 <= args.drain_seconds <= 30 and 1 <= args.recovery_seconds <= 30
            and 0 <= args.test_pg_sleep_after_admission <= 120
            and 0 <= args.test_pg_sleep_after_capture <= 120):
        parser.error("finite trial time bounds are invalid")
    if not Path(args.worker_path).is_file():
        parser.error("finite trial worker path is missing")
    try:
        return run_trial(args)
    except Exception as error:
        print(f"finite trial supervisor stopped: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
