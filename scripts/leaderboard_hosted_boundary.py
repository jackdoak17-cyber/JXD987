"""Reviewed target and pre-existing lock identities for the hosted canary."""
from __future__ import annotations

import fcntl
import json
import os
from pathlib import Path
import stat

import psycopg2

if __package__:
    from .leaderboard_restricted_runtime import validate_restricted_connection, validate_restricted_dsn
else:
    from leaderboard_restricted_runtime import validate_restricted_connection, validate_restricted_dsn


TARGET_ENV = (
    "LEADERBOARD_RESTRICTED_DB_HOST", "LEADERBOARD_RESTRICTED_DB_PORT",
    "LEADERBOARD_RESTRICTED_DB_CONTRACT_SHA256", "LEADERBOARD_RESTRICTED_HOST_PIN_FILE",
)
ROLE_PINS = {
    "m3_role_worker": "LEADERBOARD_FINITE_TRIAL_EXPECTED_DSN_SHA256",
    "m3_role_supervisor": "LEADERBOARD_DRAIN_EXPECTED_DSN_SHA256",
    "m3_role_operator": "LEADERBOARD_OPERATOR_EXPECTED_DSN_SHA256",
}


def hosted_target(dsn, role):
    return validate_restricted_dsn(
        dsn, expected_role=role, expected_dsn_sha256=os.environ.get(ROLE_PINS[role], ""),
        expected_host=os.environ.get(TARGET_ENV[0], ""), expected_port=os.environ.get(TARGET_ENV[1], ""),
        expected_contract_sha256=os.environ.get(TARGET_ENV[2], ""),
    )


def connect_hosted(dsn, role, **kwargs):
    target = hosted_target(dsn, role)
    if target.sslrootcert != "system":
        certificate = Path(target.sslrootcert)
        if not trusted_file(os.stat(trusted_parent(certificate) / certificate.name, follow_symlinks=False)):
            raise RuntimeError("hosted restricted TLS root certificate is not protected")
    conn = psycopg2.connect(
        dsn, connect_timeout=5, gssencmode="disable",
        options=("-c statement_timeout=5000 -c lock_timeout=2000 -c work_mem=3500kB "
                 "-c max_parallel_workers_per_gather=0 -c max_parallel_maintenance_workers=0"),
        **kwargs,
    )
    try:
        validate_restricted_connection(conn, target)
    except BaseException:
        conn.close()
        raise
    return conn


def trusted_file(info):
    return (stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and info.st_uid in (0, os.geteuid())
            and not info.st_mode & 0o022)


def trusted_parent(path):
    if not path.is_absolute():
        raise RuntimeError("hosted restricted paths must be absolute")
    parent = path.parent.resolve(strict=True)
    for directory in (parent, *parent.parents):
        info = directory.stat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid not in (0, os.geteuid()) or info.st_mode & 0o022:
            raise RuntimeError("hosted restricted directory is not protected")
    return parent


def read_lock_pins():
    path = Path(os.environ.get(TARGET_ENV[3], ""))
    parent = trusted_parent(path)
    fd = os.open(parent / path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as stream:
        if not trusted_file(os.fstat(stream.fileno())):
            raise RuntimeError("hosted restricted pin file is not protected")
        content = stream.read(4097)
    if len(content) > 4096:
        raise RuntimeError("hosted restricted pin file is too large")
    data = json.loads(content)
    if not isinstance(data, dict) or set(data) != {"worker", "heavy"}:
        raise RuntimeError("hosted restricted lock pins are incomplete")
    for pin in data.values():
        if (not isinstance(pin, dict) or set(pin) != {"path", "directory", "device", "inode"}
                or not all(isinstance(pin[key], str) for key in ("path", "directory"))
                or not all(type(pin[key]) is int for key in ("device", "inode"))):
            raise RuntimeError("hosted restricted lock pin is invalid")
    if (data["worker"]["device"], data["worker"]["inode"]) == (data["heavy"]["device"], data["heavy"]["inode"]):
        raise RuntimeError("hosted restricted lock identities alias")
    return data


def acquire_hosted_lock(path, kind):
    pin = read_lock_pins()[kind]
    location = Path(path)
    parent = trusted_parent(location)
    if (path, str(parent)) != (pin["path"], pin["directory"]):
        raise RuntimeError("hosted restricted lock path differs from its pin")
    fd = os.open(parent / location.name, os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        info = os.fstat(fd)
        if not trusted_file(info) or (info.st_dev, info.st_ino) != (pin["device"], pin["inode"]):
            raise RuntimeError("hosted restricted lock identity differs from its pin")
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return os.fdopen(fd, "rb")
    except BlockingIOError:
        os.close(fd)
        return None
    except BaseException:
        os.close(fd)
        raise
