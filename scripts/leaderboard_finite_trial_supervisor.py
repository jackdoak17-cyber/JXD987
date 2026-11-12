#!/usr/bin/env python3
"""Bounded whole-process finite trial supervisor and explicit recovery entrypoint."""

from __future__ import annotations

import argparse
from contextlib import closing
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
if __package__:
    from .leaderboard_hosted_boundary import (
        TARGET_ENV,
        acquire_hosted_lock,
        connect_hosted,
        require_hosted_worker_storage,
        reviewed_canary_envelope,
    )
else:
    from leaderboard_hosted_boundary import (
        TARGET_ENV,
        acquire_hosted_lock,
        connect_hosted,
        require_hosted_worker_storage,
        reviewed_canary_envelope,
    )


def database_url() -> str:
    url = os.environ.get("SUPABASE_DB_URL_SESSION") or os.environ.get("SUPABASE_DB_URL")
    if not url:
        raise RuntimeError("trial database URL is required")
    return url


CHILD_ENVIRONMENT_ALLOWLIST = frozenset({
    "PATH", "LANG", "LC_ALL", "LC_CTYPE", "TZ",
    "LEADERBOARD_SHADOW_RELEASE_SHA",
    "LEADERBOARD_SHADOW_RESOURCE_EVIDENCE_FILE",
    "LEADERBOARD_FINITE_TRIAL_EXPECTED_DSN_SHA256",
    "LEADERBOARD_FINITE_TRIAL_RELEASE_SHA",
    "LEADERBOARD_FINITE_TRIAL_OPERATOR_NOTE",
})


def child_environment(token: str, lock_fds: tuple[int, int],
                      worker_lock_path: str, heavy_lock_path: str, *,
                      hosted_worker_url: str | None = None) -> dict[str, str]:
    """Pass only worker inputs, never the supervisor's ambient credentials.

    Normalize the selected worker DSN to one key so an unused fallback URL
    cannot expose a second credential. libpq defaults, Python injection knobs,
    operator/admin URLs and provider keys are deliberately not inherited.
    The operator note is required worker audit input, not an operator credential.
    """
    environment = {
        key: os.environ[key] for key in CHILD_ENVIRONMENT_ALLOWLIST
        if key in os.environ
    }
    environment.update({
        "SUPABASE_DB_URL_SESSION": hosted_worker_url or database_url(),
        "LEADERBOARD_FINITE_TRIAL_CONNECTION_TOKEN": token,
        "LEADERBOARD_FINITE_TRIAL_SUPERVISOR_PID": str(os.getpid()),
        "LEADERBOARD_FINITE_TRIAL_WORKER_LOCK_FD": str(lock_fds[0]),
        "LEADERBOARD_FINITE_TRIAL_WORKER_LOCK_PATH": worker_lock_path,
        "LEADERBOARD_FINITE_TRIAL_HEAVY_LOCK_FD": str(lock_fds[1]),
        "LEADERBOARD_FINITE_TRIAL_HEAVY_LOCK_PATH": heavy_lock_path,
    })
    if hosted_worker_url:
        environment.update({key: os.environ[key] for key in TARGET_ENV})
    return environment


def bounded_connection(url: str):
    return psycopg2.connect(
        url, connect_timeout=5,
        options="-c statement_timeout=5000 -c lock_timeout=2000",
    )


def assert_disposable_database(url: str) -> None:
    with bounded_connection(url) as conn:
        with conn.cursor() as cur:
            cur.execute("select current_database(), inet_server_addr() is null, session_user")
            name, socket_only, login = cur.fetchone()
    if login in {"m3_role_worker", "m3_role_supervisor"}:
        raise RuntimeError("restricted logins require restricted supervisor mode")
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
                            tokens: list[str], *, rolling: bool = False,
                            restricted: bool = False) -> None:
    if not tokens or len(tokens) != len(set(tokens)) or any(
        not re.fullmatch(r"[0-9a-f]{16}", token) for token in tokens
    ):
        raise RuntimeError("finite trial quarantine tokens are invalid")
    payload = json.dumps({
        "policy_id": policy_id, "state": state, "tokens": tokens,
        "mode": "rolling" if rolling else "finite",
        "recorded_at": time.time(),
        **({"restricted": True} if restricted else {}),
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


def rolling_policy_chain(url: str, root_policy_id: str,
                         expected_worker_sha: str) -> list[str]:
    """Read the reviewed root's immutable successor chain under host locks."""
    if not re.fullmatch(r"[a-z0-9-]{8,80}", root_policy_id):
        raise RuntimeError("rolling root policy identity is invalid")
    if not re.fullmatch(r"[0-9a-f]{40}", expected_worker_sha):
        raise RuntimeError("rolling worker release identity is invalid")
    with bounded_connection(url) as conn:
        with conn.cursor() as cur:
            cur.execute("set transaction read only")
            cur.execute(
                """
                with recursive chain(policy_id,depth,path) as (
                  select policy_id,0,array[policy_id]::text[]
                  from leaderboard_rolling_v1.policies where policy_id=%s
                  union all
                  select succession.successor_policy_id,chain.depth+1,
                         chain.path||succession.successor_policy_id
                  from chain
                  join leaderboard_rolling_v1.successions succession
                    on succession.predecessor_policy_id=chain.policy_id
                  where chain.depth<32
                    and not succession.successor_policy_id=any(chain.path)
                )
                select chain.policy_id,chain.depth,policy.worker_release_sha,
                       policy.source_release_id,
                       exists(select 1 from leaderboard_rolling_v1.successions next
                              where next.predecessor_policy_id=chain.policy_id)
                from chain join leaderboard_rolling_v1.policies policy
                  on policy.policy_id=chain.policy_id
                order by chain.depth
                """,
                (root_policy_id,),
            )
            rows = cur.fetchall()
    if (not rows or len(rows)>33 or rows[-1][4]
            or any(row[2] != expected_worker_sha for row in rows)
            or len({row[3] for row in rows}) != 1
            or any(row[1] != index for index, row in enumerate(rows))):
        raise RuntimeError("rolling successor chain or release is invalid")
    return [row[0] for row in rows]


def rolling_marker_policy_id(path: Path) -> str:
    """Read a stopped run's exact policy before selecting a newer tip."""
    if path.is_symlink() or not path.is_file():
        raise RuntimeError("rolling quarantine marker is missing or invalid")
    marker = json.loads(path.read_text(encoding="utf-8"))
    policy_id = marker.get("policy_id")
    if not isinstance(policy_id, str) or not re.fullmatch(r"[a-z0-9-]{8,80}", policy_id):
        raise RuntimeError("rolling quarantine policy identity is invalid")
    read_quarantine_marker(path, policy_id, rolling=True)
    return policy_id


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


def restricted_drain_url(worker_url: str, hosted: bool = False) -> str:
    drain_url = os.environ.get("LEADERBOARD_DRAIN_DB_URL")
    if not drain_url or drain_url == worker_url:
        raise RuntimeError("restricted supervisor needs distinct worker and drain database URLs")
    if hosted:
        with closing(connect_hosted(worker_url, "m3_role_worker")) as worker:
            require_hosted_worker_storage(worker)
        with closing(connect_hosted(drain_url, "m3_role_supervisor")):
            pass
        return drain_url
    identities = []
    for url, role in ((worker_url, "m3_role_worker"), (drain_url, "m3_role_supervisor")):
        conn = bounded_connection(url)
        try:
            with conn, conn.cursor() as cur:
                cur.execute("""
                    select current_database(),inet_server_addr() is null,
                           current_user,session_user
                """)
                name, socket_only, current_role, login = cur.fetchone()
                if (name != "oddssearch_m3_rolling_worker" or not socket_only
                        or not conn.info.host.startswith("/tmp/oddssearch-m3-socket.")
                        or current_role != role or login != role):
                    raise RuntimeError("restricted supervisor database identity is invalid")
                identities.append((name, conn.info.host, conn.info.port))
        finally:
            conn.close()
    if identities[0] != identities[1]:
        raise RuntimeError("worker and drain URLs must target the same disposable database")
    return drain_url


def drain_restricted_worker(url: str, policy_id: str, token: str, seconds: float, *, hosted: bool = False) -> dict:
    """Only the checked database function can signal a stored backend binding."""
    deadline = time.monotonic() + seconds
    conn = connect_hosted(url, "m3_role_supervisor") if hosted else bounded_connection(url)
    try:
        conn.autocommit = True
        terminate = False
        signal_requested = False
        while True:
            with conn.cursor() as cur:
                cur.execute("""
                    select leaderboard_role_boundary_v1.drain_policy_worker(%s,%s,%s)
                """, (policy_id, f"oddssearch-finite-trial/{token}", terminate))
                evidence = cur.fetchone()[0]
            if evidence.get("state") == "exited":
                return evidence
            if evidence.get("state") not in {"active", "signal_requested"}:
                raise RuntimeError("restricted drain returned an invalid state")
            if time.monotonic() >= deadline:
                raise RuntimeError("restricted worker backend exit remains unproved")
            # Probe first, signal at most once, and require fresh exit proof.
            signal_requested |= terminate
            terminate = not signal_requested
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
            if rolling and args.follow_rolling_successors:
                marker_policy_id = rolling_marker_policy_id(marker)
                chain = rolling_policy_chain(
                    url,args.policy_id,
                    os.environ.get("LEADERBOARD_FINITE_TRIAL_RELEASE_SHA", ""),
                )
                if marker_policy_id not in chain:
                    raise RuntimeError("rolling quarantine policy is outside reviewed lineage")
                if os.path.lexists(heavy_marker):
                    if (rolling_marker_policy_id(heavy_marker) != marker_policy_id
                            or read_quarantine_marker(
                                heavy_marker, marker_policy_id, rolling=True
                            ) != read_quarantine_marker(
                                marker, marker_policy_id, rolling=True
                            )):
                        raise RuntimeError("rolling quarantine markers disagree on identity or tokens")
                args.policy_id = marker_policy_id
            tokens = read_quarantine_marker(marker, args.policy_id, rolling=rolling)
            if rolling:
                for path in (marker, heavy_marker):
                    if os.path.lexists(path):
                        read_quarantine_marker(path, args.policy_id, rolling=True)
                        identity = json.loads(path.read_text(encoding="utf-8"))
                        if (identity.get("restricted") is True
                                or identity.get("state") in ("prepared_pending", "drained_unsettled")):
                            raise RuntimeError("restricted quarantine requires committed-state release")
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
                env=child_environment(
                    recovery_token, lock_fds, args.worker_lock_file,
                    args.database_heavy_lock_file,
                ),
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
    hosted = getattr(args, "production_restricted", False)
    url = os.environ.get("LEADERBOARD_WORKER_DB_URL", "") if hosted else database_url()
    rolling = getattr(args, "rolling", False)
    restricted = getattr(args, "restricted_worker", False)
    if restricted and (
        not rolling or not args.production or not (args.production_trial_isolated_test or hosted)
        or not getattr(args, "require_bound_publication", False)
        or getattr(args, "league_id", None) is None or args.follow_rolling_successors
    ):
        raise RuntimeError("restricted supervisor requires scoped rolling and bound publication in disposable mode")
    if hosted and (not restricted or args.production_trial_isolated_test or not url
                   or not reviewed_canary_envelope(args)
                   or any((args.test_pg_sleep_after_admission, args.test_pg_sleep_after_capture,
                           args.test_recovery_pg_sleep, args.test_force_uncertain_drain))):
        raise RuntimeError("hosted restricted supervisor requires the reviewed canary envelope")
    drain_url = restricted_drain_url(url, hosted=hosted) if restricted else None
    if rolling and not args.production:
        raise RuntimeError("rolling supervisor requires production identity checks")
    if args.production:
        if args.production_trial_isolated_test:
            if not restricted:
                assert_disposable_database(url)
        elif not hosted:
            assert_production_database(url)
            if (args.test_pg_sleep_after_admission or args.test_pg_sleep_after_capture
                    or args.test_recovery_pg_sleep or args.test_force_uncertain_drain):
                raise RuntimeError("production finite trial rejects test fault hooks")
    else:
        if args.production_trial_isolated_test:
            raise RuntimeError("production trial test mode requires --production")
        assert_disposable_database(url)
    worker_lock = (acquire_hosted_lock(args.worker_lock_file, "worker") if hosted
                   else acquire_lock(args.worker_lock_file))
    if worker_lock is None:
        return 75
    try:
        heavy_lock = (acquire_hosted_lock(args.database_heavy_lock_file, "heavy") if hosted
                      else acquire_lock(args.database_heavy_lock_file))
        if heavy_lock is None:
            return 75
        try:
            marker = Path(args.worker_lock_file + ".quarantine")
            heavy_marker = Path(args.worker_lock_file + ".quarantine.heavy")
            if os.path.lexists(marker) or os.path.lexists(heavy_marker):
                raise RuntimeError("finite trial quarantine marker requires operator reconciliation")
            if rolling and args.follow_rolling_successors:
                args.policy_id = rolling_policy_chain(
                    url,args.policy_id,
                    os.environ.get("LEADERBOARD_FINITE_TRIAL_RELEASE_SHA", ""),
                )[-1]
            main_token = uuid.uuid4().hex[:16]
            recovery_token = uuid.uuid4().hex[:16]
            marker_tokens = [main_token] if restricted else [main_token, recovery_token]
            write_quarantine_marker(
                marker, args.policy_id, "inflight", marker_tokens, rolling=rolling,
                restricted=restricted,
            )
            write_quarantine_marker(heavy_marker, args.policy_id,
                                    "backend_drain_unproved", marker_tokens,
                                    rolling=rolling, restricted=restricted)
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
            if rolling and getattr(args, "league_id", None) is not None:
                worker += [
                    "--league-id", str(args.league_id),
                    "--season-id", str(args.season_id),
                ]
            if rolling and getattr(args, "require_bound_publication", False):
                worker.append("--require-bound-publication")
            if restricted:
                worker.append("--restricted-worker")
            if hosted:
                worker.append("--production-restricted")
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
                    env=child_environment(
                        main_token, lock_fds, args.worker_lock_file,
                        args.database_heavy_lock_file,
                        **({"hosted_worker_url": url} if hosted else {}),
                    ),
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
                if restricted:
                    drained = drain_restricted_worker(
                        drain_url, args.policy_id, main_token, args.drain_seconds,
                        **({"hosted": True} if hosted else {}),
                    )
                    # A drained worker still has accounting liability. Keep the
                    # trial marker and never launch a recovery child in this mode.
                    clear_quarantine_marker(heavy_marker)
                    reports = [json.loads(line) for line in output.splitlines() if line.strip()]
                    report = reports[-1] if reports else {}
                    prepared = (
                        worker_exit == 0 and drained.get("prepared_pending") is True
                        and report.get("state") == "prepared_pending"
                        and report.get("fence") == drained["fence"]
                        and report.get("run_id") == drained["run_id"]
                    )
                    state = "prepared_pending" if prepared else "drained_unsettled"
                    write_quarantine_marker(marker, args.policy_id, state, marker_tokens,
                                            rolling=True, restricted=True)
                    print(json.dumps({
                        "event": "restricted_rolling_worker_supervised",
                        "policy_id": args.policy_id, "worker_exit": worker_exit,
                        "timed_out": timed_out, "state": state, "drain": drained,
                        "interrupted_signal": interrupted_signal,
                    }, sort_keys=True), flush=True)
                    if interrupted_signal is not None:
                        return 128 + interrupted_signal
                    if timed_out:
                        return 124
                    return 0 if prepared else 1
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
                    env=child_environment(
                        recovery_token, lock_fds, args.worker_lock_file,
                        args.database_heavy_lock_file,
                    ),
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
                        rolling=rolling, restricted=restricted,
                    )
                except Exception:
                    pass
                if active_process is not None:
                    try:
                        stop_process_group(active_process, args.grace_seconds)
                        if active_token is not None:
                            if restricted:
                                drain_restricted_worker(drain_url, args.policy_id, active_token, args.drain_seconds,
                                                        **({"hosted": True} if hosted else {}))
                            else:
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
    parser.add_argument("--require-bound-publication", action="store_true")
    parser.add_argument("--restricted-worker", action="store_true")
    parser.add_argument("--production-restricted", action="store_true")
    parser.add_argument("--follow-rolling-successors", action="store_true")
    parser.add_argument("--fixture-limit", type=int, default=10)
    parser.add_argument("--capture-limit", type=int, default=10)
    parser.add_argument("--cohort-limit", type=int, default=0)
    parser.add_argument("--job-limit", type=int, default=0)
    parser.add_argument("--league-id", type=int)
    parser.add_argument("--season-id", type=int)
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
    if args.restricted_worker and (
        not args.rolling or not args.production or not (args.production_trial_isolated_test or args.production_restricted)
        or not args.require_bound_publication or args.league_id is None
        or args.follow_rolling_successors or args.test_recovery_pg_sleep
        or args.reconcile_quarantine
    ):
        parser.error("restricted supervisor requires scoped disposable rolling with bound publication and no recovery")
    if args.production_restricted and (not args.restricted_worker or args.production_trial_isolated_test):
        parser.error("hosted restricted supervisor requires exclusive restricted mode")
    if args.require_bound_publication and not args.rolling:
        parser.error("bound publication requires rolling mode")
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
    if args.follow_rolling_successors and not args.rolling:
        parser.error("successor selection requires rolling mode")
    if (args.league_id is None) != (args.season_id is None):
        parser.error("rolling cohort scope requires league and season together")
    if args.league_id is not None and (
        not args.rolling or args.league_id <= 0 or args.season_id <= 0
    ):
        parser.error("rolling cohort scope requires positive IDs and rolling mode")
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
