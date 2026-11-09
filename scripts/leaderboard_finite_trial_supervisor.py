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
import uuid
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


def write_quarantine_marker(path: Path, policy_id: str, state: str) -> None:
    payload = json.dumps({
        "policy_id": policy_id, "state": state, "recorded_at": time.time(),
    }, sort_keys=True).encode("utf-8")
    temporary = path.with_name(path.name + ".tmp-" + uuid.uuid4().hex)
    try:
        with open(temporary, "xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


def clear_quarantine_marker(path: Path) -> None:
    path.unlink()
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def drain_token_backends(url: str, token: str, seconds: float) -> None:
    deadline = time.monotonic() + seconds
    application_name = f"oddssearch-finite-trial/{token}"
    conn = bounded_connection(url)
    try:
        conn.autocommit = True
        while True:
            with conn.cursor() as cur:
                cur.execute("select pg_catalog.pg_stat_clear_snapshot()")
                cur.execute(
                    """
                    select pg_terminate_backend(pid)
                    from pg_catalog.pg_stat_activity
                    where datname=current_database() and application_name=%s
                    """,
                    (application_name,),
                )
                remaining = cur.fetchall()
            if not remaining:
                return
            if time.monotonic() >= deadline:
                raise RuntimeError("finite trial child backend did not drain")
            time.sleep(0.1)
    finally:
        conn.close()


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
            marker = Path(args.worker_lock_file + ".quarantine")
            if marker.exists():
                raise RuntimeError("finite trial quarantine marker requires operator reconciliation")
            main_token = uuid.uuid4().hex[:16]
            recovery_token = uuid.uuid4().hex[:16]
            write_quarantine_marker(marker, args.policy_id, "inflight")
            active_process: subprocess.Popen | None = None
            active_token: str | None = None
            interrupted_signal: int | None = None

            def on_signal(signum, _frame):
                nonlocal interrupted_signal
                interrupted_signal = signum
                if active_process is not None and active_process.poll() is None:
                    try:
                        os.killpg(active_process.pid, signal.SIGTERM)
                    except ProcessLookupError:
                        pass

            prior_term = signal.signal(signal.SIGTERM, on_signal)
            prior_int = signal.signal(signal.SIGINT, on_signal)
            lock_fds = (worker_lock.fileno(), heavy_lock.fileno())
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
            try:
                child_env = os.environ.copy()
                child_env["LEADERBOARD_FINITE_TRIAL_CONNECTION_TOKEN"] = main_token
                active_process = subprocess.Popen(
                    worker, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    text=True, start_new_session=True, pass_fds=lock_fds,
                    env=child_env,
                )
                active_token = main_token
                timed_out = False
                try:
                    output, errors = active_process.communicate(
                        timeout=args.deadline_seconds
                    )
                except subprocess.TimeoutExpired:
                    timed_out = True
                    output, errors = stop_process_group(
                        active_process, args.grace_seconds
                    )
                worker_exit = active_process.returncode
                active_process = None
                active_token = None
                if output:
                    print(output, end="")
                if errors:
                    print(errors, end="", file=sys.stderr)
                if args.test_force_uncertain_drain:
                    raise RuntimeError("forced uncertain original-owner drain")
                # The token finds even a preflight/admission backend that never
                # committed a reservation. Keep both flocks until it is gone.
                drain_token_backends(url, main_token, args.drain_seconds)
                drain_exact_backend(url, args.policy_id, args.drain_seconds)

                recovery = [
                    sys.executable, args.worker_path, "trial-recover",
                    "--isolated-test-override", "--trial-policy-id", args.policy_id,
                ]
                if args.test_recovery_pg_sleep:
                    recovery += [
                        "--trial-test-pg-sleep-before-recovery",
                        str(args.test_recovery_pg_sleep),
                    ]
                recovery_env = os.environ.copy()
                recovery_env["LEADERBOARD_FINITE_TRIAL_CONNECTION_TOKEN"] = recovery_token
                active_process = subprocess.Popen(
                    recovery, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    text=True, start_new_session=True, pass_fds=lock_fds,
                    env=recovery_env,
                )
                active_token = recovery_token
                recovery_timed_out = False
                try:
                    recovered_output, recovered_errors = active_process.communicate(
                        timeout=args.recovery_seconds
                    )
                except subprocess.TimeoutExpired:
                    recovery_timed_out = True
                    recovered_output, recovered_errors = stop_process_group(
                        active_process, args.grace_seconds
                    )
                recovery_exit = active_process.returncode
                active_process = None
                active_token = None
                drain_token_backends(url, recovery_token, args.drain_seconds)
                if recovery_timed_out or recovery_exit != 0:
                    raise RuntimeError(
                        "finite trial recovery failed or timed out: "
                        + recovered_errors[-1000:]
                    )
                state = json.loads(recovered_output)["state"]
                if state not in {"recovered", "already_settled"}:
                    if state != "no_reservation" or worker_exit == 0:
                        raise RuntimeError(f"finite trial liability remains: {state}")
                clear_quarantine_marker(marker)
                print(json.dumps({
                    "event": "isolated_finite_trial_supervised",
                    "policy_id": args.policy_id, "worker_exit": worker_exit,
                    "timed_out": timed_out, "recovery_state": state,
                    "interrupted_signal": interrupted_signal,
                }, sort_keys=True), flush=True)
                if interrupted_signal is not None:
                    return 128 + interrupted_signal
                if timed_out:
                    return 124
                return 0 if worker_exit == 0 and state == "already_settled" else 1
            except BaseException as error:
                # A failure before authoritative drain or settlement leaves a
                # durable marker. Future trials and the normal worker wrapper
                # refuse it; inherited lock FDs cover a crashed parent while
                # either child remains alive.
                try:
                    write_quarantine_marker(marker, args.policy_id, str(error)[:200])
                except Exception:
                    pass
                if active_process is not None:
                    try:
                        stop_process_group(active_process, args.grace_seconds)
                        if active_token is not None:
                            drain_token_backends(url, active_token, args.drain_seconds)
                    except Exception:
                        pass
                raise
            finally:
                signal.signal(signal.SIGTERM, prior_term)
                signal.signal(signal.SIGINT, prior_int)
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
    parser.add_argument("--test-force-uncertain-drain", action="store_true")
    parser.add_argument("--test-recovery-pg-sleep", type=float, default=0)
    args = parser.parse_args()
    if not (1 <= args.deadline_seconds <= 60 and 1 <= args.grace_seconds <= 10
            and 1 <= args.drain_seconds <= 30 and 1 <= args.recovery_seconds <= 30
            and 0 <= args.test_pg_sleep_after_admission <= 120
            and 0 <= args.test_pg_sleep_after_capture <= 120
            and 0 <= args.test_recovery_pg_sleep <= 120):
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
