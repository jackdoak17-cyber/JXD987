#!/usr/bin/env python3
"""Release one committed restricted-worker quarantine in the disposable clone."""

from __future__ import annotations

import argparse
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys

import psycopg2
from psycopg2.extensions import parse_dsn

from leaderboard_finite_trial_supervisor import acquire_lock, bounded_connection, clear_quarantine_marker


def marker_snapshot(path: Path):
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        return None
    with os.fdopen(fd, "rb") as marker:
        info = os.fstat(marker.fileno())
        if not stat.S_ISREG(info.st_mode):
            raise RuntimeError("quarantine marker is not a regular file")
        payload = marker.read(4097)
    if len(payload) > 4096:
        raise RuntimeError("quarantine marker is too large")
    return (info.st_dev, info.st_ino, payload)


def confirm_committed(args) -> None:
    dsn = os.environ.get("LEADERBOARD_OPERATOR_DB_URL", "")
    pin = os.environ.get("LEADERBOARD_OPERATOR_EXPECTED_DSN_SHA256", "")
    if not dsn or hashlib.sha256(dsn.encode()).hexdigest() != pin:
        raise RuntimeError("operator database URL is missing or does not match its pin")
    parameters = parse_dsn(dsn)
    if (parameters.get("dbname") != "oddssearch_m3_rolling_worker"
            or parameters.get("user") != "m3_role_operator"
            or not re.fullmatch(r"/tmp/oddssearch-m3-socket\.[a-zA-Z0-9]+", parameters.get("host", ""))
            or not re.fullmatch(r"[0-9]{1,5}", parameters.get("port", ""))
            or any(key in parameters for key in ("hostaddr", "service"))
            or any(os.environ.get(key) for key in ("PGHOSTADDR", "PGSERVICE", "PGSERVICEFILE"))):
        raise RuntimeError("disposable operator database identity is invalid")
    with closing(bounded_connection(dsn)) as connection, connection, connection.cursor() as cursor:
        cursor.execute("set transaction read only")
        cursor.execute("""
            select current_database(), inet_server_addr() is null, session_user, current_user
        """)
        database, local, login, role = cursor.fetchone()
        if (database != "oddssearch_m3_rolling_worker" or not local
                or login != "m3_role_operator" or role != login):
            raise RuntimeError("disposable operator database identity is invalid")
        cursor.execute("""
            select leaderboard_role_boundary_v1.confirm_quarantine_release(%s,%s,%s,%s,%s)
        """, (args.policy_id, args.reservation_id, args.evidence_id, args.token, args.marker_sha256))
        if cursor.fetchone() != (True,):
            raise RuntimeError("no matching committed quarantine settlement")


def release(args) -> int:
    marker = Path(args.worker_lock_file + ".quarantine")
    heavy_marker = Path(args.worker_lock_file + ".quarantine.heavy")
    worker_lock = acquire_lock(args.worker_lock_file)
    if worker_lock is None:
        return 75
    with worker_lock:
        heavy_lock = acquire_lock(args.database_heavy_lock_file)
        if heavy_lock is None:
            return 75
        with heavy_lock:
            if os.path.lexists(heavy_marker):
                raise RuntimeError("heavy quarantine marker requires separate reconciliation")
            original = marker_snapshot(marker)
            if original is not None:
                payload = original[2]
                if hashlib.sha256(payload).hexdigest() != args.marker_sha256:
                    raise RuntimeError("quarantine marker digest does not match")
                identity = json.loads(payload)
                if (not isinstance(identity, dict)
                        or identity.get("state") != "prepared_pending" or identity.get("mode") != "rolling"
                        or identity.get("policy_id") != args.policy_id or identity.get("tokens") != [args.token]):
                    raise RuntimeError("quarantine marker identity does not match")
            confirm_committed(args)
            if os.path.lexists(heavy_marker) or marker_snapshot(marker) != original:
                raise RuntimeError("quarantine changed during committed settlement verification")
            if original is not None:
                clear_quarantine_marker(marker)
            else:
                # Also make the no-op retry durable if the prior process
                # stopped between unlink and the directory fsync.
                fd = os.open(marker.parent, os.O_RDONLY)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
    print(json.dumps({
        "event": "restricted_rolling_quarantine_release",
        "state": "released" if original is not None else "already_absent",
        "policy_id": args.policy_id,
        "reservation_id": args.reservation_id,
        "evidence_id": args.evidence_id,
    }, sort_keys=True))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker-lock-file", required=True)
    parser.add_argument("--database-heavy-lock-file", required=True)
    parser.add_argument("--policy-id", required=True)
    parser.add_argument("--reservation-id", required=True, type=int)
    parser.add_argument("--evidence-id", required=True, type=int)
    parser.add_argument("--token", required=True)
    parser.add_argument("--marker-sha256", required=True)
    args = parser.parse_args()
    if (not re.fullmatch(r"[a-z0-9-]{8,80}", args.policy_id)
            or not re.fullmatch(r"[0-9a-f]{16}", args.token)
            or not re.fullmatch(r"[0-9a-f]{64}", args.marker_sha256)
            or not (0 < args.reservation_id < 2**63 and 0 < args.evidence_id < 2**63)):
        parser.error("exact quarantine identity is required")
    try:
        return release(args)
    except psycopg2.Error as error:
        print(f"quarantine release stopped: database check failed ({error.pgcode or 'connection'})",
              file=sys.stderr)
    except (OSError, ValueError, RuntimeError) as error:
        print(f"quarantine release stopped: {error}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
