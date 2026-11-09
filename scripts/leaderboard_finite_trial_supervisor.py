#!/usr/bin/env python3
"""Bounded whole-process finite trial supervisor and explicit recovery entrypoint."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
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


def assert_production_database(url: str) -> None:
    expected_hash = os.environ.get("LEADERBOARD_FINITE_TRIAL_EXPECTED_DSN_SHA256", "")
    if not re.fullmatch(r"[0-9a-f]{64}", expected_hash):
        raise RuntimeError("production finite trial requires a pinned database URL hash")
    if hashlib.sha256(url.encode()).hexdigest() != expected_hash:
        raise RuntimeError("production finite trial database URL does not match its pin")
    with bounded_connection(url) as conn:
        with conn.cursor() as cur:
            cur.execute("select current_database(), inet_server_addr() is not null, current_user")
            database_name, network_connection, database_role = cur.fetchone()
    if database_name != "postgres" or not network_connection or database_role != "postgres":
        raise RuntimeError("production finite trial database identity is invalid")


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


def write_quarantine_marker(path: Path, policy_id: str, state: str,
                            tokens: list[str], *, rolling: bool = False) -> None:
    if not tokens or len(tokens) != len(set(tokens)) or any(
        not re.fullmatch(r"[0-9a-f]{16}", token) for token in tokens
    ):
        raise RuntimeError("finite trial quarantine tokens are invalid")
    payload = json.dumps({
        "policy_id": policy_id, "state": state, "tokens": tokens,
        "mode": "rolling" if rolling else "finite",
        "recorded_at": time.time(),
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


def read_quarantine_marker(path: Path, policy_id: str, *, rolling: bool = False) -> list[str]:
    if path.is_symlink() or not path.is_file():
        raise RuntimeError("finite trial quarantine marker is missing or invalid")
    marker = json.loads(path.read_text(encoding="utf-8"))
    tokens = marker.get("tokens")
    expected_mode = "rolling" if rolling else "finite"
    if (marker.get("policy_id") != policy_id
            or marker.get("mode", "finite") != expected_mode
            or not isinstance(tokens, list)
            or not tokens or len(tokens) != len(set(tokens))
            or any(not isinstance(token, str) or
                   not re.fullmatch(r"[0-9a-f]{16}", token) for token in tokens)):
        raise RuntimeError("finite trial quarantine marker identity is invalid")
    return tokens


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


def drain_exact_backend(url: str, policy_id: str, seconds: float, *,
                        production: bool, rolling: bool = False) -> None:
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
                query = (
                    """
                    select pg_terminate_backend(activity.pid)
                    from leaderboard_rolling_v1.reservations reservation
                    join pg_catalog.pg_stat_activity activity
                      on activity.pid=reservation.owner_backend_pid
                     and activity.backend_start=reservation.owner_backend_started_at
                    where reservation.policy_id=%s and reservation.settled_at is null
                    """ if rolling else
                    """
                    select pg_terminate_backend(activity.pid)
                    from leaderboard_trial_v1.reservations reservation
                    join pg_catalog.pg_stat_activity activity
                      on activity.pid=reservation.owner_backend_pid
                     and activity.backend_start=reservation.owner_backend_started_at
                    where reservation.policy_id=%s and reservation.settled_at is null
                    """ if production else """
                    select pg_terminate_backend(activity.pid)
                    from admission_proto.reservations reservation
                    join pg_catalog.pg_stat_activity activity
                      on activity.pid=reservation.owner_backend_pid
                     and activity.backend_start=reservation.owner_backend_started_at
                    where reservation.policy_id=%s and reservation.settled_at is null
                    """
                )
                cur.execute(query, (policy_id,))
                terminated = cur.fetchall()
            if not terminated:
                return
            if time.monotonic() >= deadline:
                raise RuntimeError("finite trial owner backend did not drain")
            time.sleep(0.1)
    finally:
        conn.close()


def reconcile_quarantine(args) -> int:
    """Explicit operator action; never reopens a policy with no reservation."""
    url = database_url()
    if args.production:
        if args.production_trial_isolated_test:
            assert_disposable_database(url)
        else:
            assert_production_database(url)
    else:
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
            heavy_marker = Path(args.worker_lock_file + ".quarantine.heavy")
            rolling = getattr(args, "rolling", False)
            tokens = read_quarantine_marker(marker, args.policy_id, rolling=rolling)
            for token in tokens:
                drain_token_backends(url, token, args.drain_seconds)
            drain_exact_backend(url, args.policy_id, args.drain_seconds,
                                production=args.production, rolling=rolling)
            recovery_token = uuid.uuid4().hex[:16]
            tokens.append(recovery_token)
            write_quarantine_marker(marker, args.policy_id,
                                    "operator_recovery_inflight", tokens,
                                    rolling=rolling)
            write_quarantine_marker(heavy_marker, args.policy_id,
                                    "operator_recovery_inflight", tokens,
                                    rolling=rolling)
            lock_fds = (worker_lock.fileno(), heavy_lock.fileno())
            environment = os.environ.copy()
            environment.update({
                "LEADERBOARD_FINITE_TRIAL_CONNECTION_TOKEN": recovery_token,
                "LEADERBOARD_FINITE_TRIAL_SUPERVISOR_PID": str(os.getpid()),
                "LEADERBOARD_FINITE_TRIAL_WORKER_LOCK_FD": str(lock_fds[0]),
                "LEADERBOARD_FINITE_TRIAL_WORKER_LOCK_PATH": args.worker_lock_file,
                "LEADERBOARD_FINITE_TRIAL_HEAVY_LOCK_FD": str(lock_fds[1]),
                "LEADERBOARD_FINITE_TRIAL_HEAVY_LOCK_PATH": args.database_heavy_lock_file,
            })
            recovery = [
                sys.executable, args.worker_path,
                "rolling-recover" if rolling else
                ("finite-recover" if args.production else "trial-recover"),
                "--trial-policy-id", args.policy_id,
            ]
            if args.production_trial_isolated_test:
                recovery.append("--production-trial-isolated-test")
            elif not args.production:
                recovery.append("--isolated-test-override")
            process = subprocess.Popen(
                recovery, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True, start_new_session=True, pass_fds=lock_fds,
                env=environment,
            )
            try:
                output, errors = process.communicate(timeout=args.recovery_seconds)
            except subprocess.TimeoutExpired:
                output, errors = stop_process_group(process, args.grace_seconds)
                raise RuntimeError("finite trial operator recovery timed out")
            for token in tokens:
                drain_token_backends(url, token, args.drain_seconds)
            drain_exact_backend(url, args.policy_id, args.drain_seconds,
                                production=args.production, rolling=rolling)
            clear_quarantine_marker(heavy_marker)
            if process.returncode != 0:
                raise RuntimeError("finite trial operator recovery failed: " + errors[-1000:])
            state = json.loads(output)["state"]
            if state not in {"recovered", "already_settled"}:
                raise RuntimeError(f"finite trial liability remains: {state}")
            clear_quarantine_marker(marker)
            print(json.dumps({"event": "finite_trial_quarantine_reconciled",
                              "policy_id": args.policy_id,
                              "recovery_state": state}, sort_keys=True), flush=True)
            return 0
        finally:
            heavy_lock.close()
    finally:
        worker_lock.close()


def run_trial(args) -> int:
    url = database_url()
    rolling = getattr(args, "rolling", False)
    if rolling and not args.production:
        raise RuntimeError("rolling supervisor requires production identity checks")
    if args.production:
        if args.production_trial_isolated_test:
            assert_disposable_database(url)
        else:
            assert_production_database(url)
            if (args.test_pg_sleep_after_admission or args.test_pg_sleep_after_capture
                    or args.test_recovery_pg_sleep or args.test_force_uncertain_drain):
                raise RuntimeError("production finite trial rejects test fault hooks")
    else:
        if args.production_trial_isolated_test:
            raise RuntimeError("production trial test mode requires --production")
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
            heavy_marker = Path(args.worker_lock_file + ".quarantine.heavy")
            if os.path.lexists(marker) or os.path.lexists(heavy_marker):
                raise RuntimeError("finite trial quarantine marker requires operator reconciliation")
            main_token = uuid.uuid4().hex[:16]
            recovery_token = uuid.uuid4().hex[:16]
            marker_tokens = [main_token, recovery_token]
            write_quarantine_marker(
                marker, args.policy_id, "inflight", marker_tokens, rolling=rolling
            )
            write_quarantine_marker(heavy_marker, args.policy_id,
                                    "backend_drain_unproved", marker_tokens,
                                    rolling=rolling)
            active_process: subprocess.Popen | None = None
            active_token: str | None = None
            interrupted_signal: int | None = None
            pending_signal_delivery = False

            def on_signal(signum, _frame):
                nonlocal interrupted_signal, pending_signal_delivery
                interrupted_signal = signum
                if active_process is not None and active_process.poll() is None:
                    try:
                        os.killpg(active_process.pid, signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                else:
                    pending_signal_delivery = True

            def deliver_pending_signal() -> None:
                nonlocal pending_signal_delivery
                if pending_signal_delivery and active_process is not None:
                    pending_signal_delivery = False
                    if active_process.poll() is None:
                        try:
                            os.killpg(active_process.pid, signal.SIGTERM)
                        except ProcessLookupError:
                            pass

            prior_term = signal.signal(signal.SIGTERM, on_signal)
            prior_int = signal.signal(signal.SIGINT, on_signal)
            lock_fds = (worker_lock.fileno(), heavy_lock.fileno())
            def child_environment(token: str) -> dict[str, str]:
                environment = os.environ.copy()
                environment.update({
                    "LEADERBOARD_FINITE_TRIAL_CONNECTION_TOKEN": token,
                    "LEADERBOARD_FINITE_TRIAL_SUPERVISOR_PID": str(os.getpid()),
                    "LEADERBOARD_FINITE_TRIAL_WORKER_LOCK_FD": str(lock_fds[0]),
                    "LEADERBOARD_FINITE_TRIAL_WORKER_LOCK_PATH": args.worker_lock_file,
                    "LEADERBOARD_FINITE_TRIAL_HEAVY_LOCK_FD": str(lock_fds[1]),
                    "LEADERBOARD_FINITE_TRIAL_HEAVY_LOCK_PATH": args.database_heavy_lock_file,
                })
                return environment
            worker = [
                sys.executable, args.worker_path,
                "rolling-once" if rolling else
                ("finite-once" if args.production else "trial-once"),
                "--trial-policy-id", args.policy_id,
                "--fixture-limit", str(args.fixture_limit if rolling else 10),
                "--capture-limit", str(args.capture_limit if rolling else 10),
                "--cohort-limit", str(args.cohort_limit if rolling else 0),
                "--job-limit", str(args.job_limit if rolling else 0),
                "--max-seconds", str(args.max_seconds if rolling else 30),
            ]
            if args.production:
                if args.production_trial_isolated_test:
                    worker.append("--production-trial-isolated-test")
            else:
                worker.append("--isolated-test-override")
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
                active_process = subprocess.Popen(
                    worker, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    text=True, start_new_session=True, pass_fds=lock_fds,
                    env=child_environment(main_token),
                )
                active_token = main_token
                deliver_pending_signal()
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
                drain_exact_backend(
                    url, args.policy_id, args.drain_seconds,
                    production=args.production, rolling=rolling,
                )

                recovery = [
                    sys.executable, args.worker_path,
                    "rolling-recover" if rolling else
                    ("finite-recover" if args.production else "trial-recover"),
                    "--trial-policy-id", args.policy_id,
                ]
                if args.production:
                    if args.production_trial_isolated_test:
                        recovery.append("--production-trial-isolated-test")
                else:
                    recovery.append("--isolated-test-override")
                if args.test_recovery_pg_sleep:
                    recovery += [
                        "--trial-test-pg-sleep-before-recovery",
                        str(args.test_recovery_pg_sleep),
                    ]
                active_process = subprocess.Popen(
                    recovery, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    text=True, start_new_session=True, pass_fds=lock_fds,
                    env=child_environment(recovery_token),
                )
                active_token = recovery_token
                deliver_pending_signal()
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
                drain_exact_backend(
                    url, args.policy_id, args.drain_seconds,
                    production=args.production, rolling=rolling,
                )
                # Backend absence is now proved. Keep the trial-only marker
                # until settlement is known, but let the unrelated source
                # reconciler resume after the host locks are released.
                clear_quarantine_marker(heavy_marker)
                if recovery_timed_out or recovery_exit != 0:
                    raise RuntimeError(
                        "finite trial recovery failed or timed out: "
                        + recovered_errors[-1000:]
                    )
                state = json.loads(recovered_output)["state"]
                if state not in {"recovered", "already_settled"}:
                    raise RuntimeError(f"finite trial liability remains: {state}")
                clear_quarantine_marker(marker)
                print(json.dumps({
                    "event": "rolling_worker_supervised" if rolling else
                             ("finite_trial_supervised" if args.production
                              else "isolated_finite_trial_supervised"),
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
                    write_quarantine_marker(
                        marker, args.policy_id, str(error)[:200], marker_tokens,
                        rolling=rolling,
                    )
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
    parser.add_argument("--production", action="store_true")
    parser.add_argument("--rolling", action="store_true")
    parser.add_argument("--fixture-limit", type=int, default=10)
    parser.add_argument("--capture-limit", type=int, default=10)
    parser.add_argument("--cohort-limit", type=int, default=0)
    parser.add_argument("--job-limit", type=int, default=0)
    parser.add_argument("--max-seconds", type=float, default=30)
    parser.add_argument("--reconcile-quarantine", action="store_true")
    parser.add_argument("--production-trial-isolated-test", action="store_true")
    parser.add_argument("--deadline-seconds", type=float, default=45)
    parser.add_argument("--grace-seconds", type=float, default=5)
    parser.add_argument("--drain-seconds", type=float, default=10)
    parser.add_argument("--recovery-seconds", type=float, default=15)
    parser.add_argument("--test-pg-sleep-after-admission", type=float, default=0)
    parser.add_argument("--test-pg-sleep-after-capture", type=float, default=0)
    parser.add_argument("--test-force-uncertain-drain", action="store_true")
    parser.add_argument("--test-recovery-pg-sleep", type=float, default=0)
    args = parser.parse_args()
    if args.production_trial_isolated_test and not args.production:
        parser.error("production trial isolated test mode requires --production")
    if args.rolling and (
        not args.production or not 1 <= args.fixture_limit <= 250
        or not 0 <= args.capture_limit <= args.fixture_limit
        or not 0 <= args.cohort_limit <= 8
        or not 0 <= args.job_limit <= args.fixture_limit
        or not (args.capture_limit or args.job_limit)
        or not 0 < args.max_seconds <= 30
    ):
        parser.error("rolling worker mode or limits are invalid")
    if not (1 <= args.deadline_seconds <= 60 and 1 <= args.grace_seconds <= 10
            and 1 <= args.drain_seconds <= 30 and 1 <= args.recovery_seconds <= 30
            and 0 <= args.test_pg_sleep_after_admission <= 120
            and 0 <= args.test_pg_sleep_after_capture <= 120
            and 0 <= args.test_recovery_pg_sleep <= 120):
        parser.error("finite trial time bounds are invalid")
    if not Path(args.worker_path).is_file():
        parser.error("finite trial worker path is missing")
    try:
        return reconcile_quarantine(args) if args.reconcile_quarantine else run_trial(args)
    except Exception as error:
        print(f"finite trial supervisor stopped: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
