#!/usr/bin/env python3
"""Install a reviewed two-entry cron switch under the host verifier's locks."""

import fcntl
import hashlib
import os
from pathlib import Path
import re
import stat
import subprocess
import sys


FROZEN_PREFIX = b"# oddssearch-cutover "
ENTRIES = {b"run_stats_reconciliation.sh", b"run_leaderboard_shadow_worker.sh"}
LINE = re.compile(rb"((?:[^\s#]+[ \t]+){5}cd )(/[a-zA-Z0-9_./-]+)"
                  rb" && \2/scripts/vps/(run_stats_reconciliation\.sh|run_leaderboard_shadow_worker\.sh)"
                  rb"([ \t][^\r\n]*\n|\n)")
MAX_BYTES = 256 * 1024
BARRIER = {
    3: "/var/lock/oddssearch-leaderboard-shadow.lock",
    4: "/var/lock/oddssearch-db-heavy.lock",
    5: "/var/lib/oddssearch/leaderboard-locks/oddssearch-leaderboard-shadow.lock",
    6: "/var/lib/oddssearch/leaderboard-locks/oddssearch-db-heavy.lock",
    7: "/var/lock/stats-reconciliation-supervisor.lock",
    9: "/var/lib/oddssearch/leaderboard-locks/pins.json",
}
HOLD = Path(BARRIER[9]).with_name("cron-transition.hold")


def digest(value):
    return hashlib.sha256(value).hexdigest()


def reviewed_bytes(path):
    location = Path(path)
    parent = location.parent.resolve(strict=True)
    if not location.is_absolute() or parent != location.parent:
        raise ValueError("reviewed crontab must have a canonical absolute path")
    for directory in (parent, *parent.parents):
        info = directory.stat()
        if info.st_uid not in (0, os.geteuid()) or info.st_mode & 0o022:
            raise ValueError("reviewed crontab parent is not protected")
    fd = os.open(location, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
            raise ValueError("reviewed crontab must be an owned private regular file")
        return stream.read(MAX_BYTES + 1)


def validate_switch(frozen, destination, mode, root):
    for schedule in (frozen, destination):
        if len(schedule) > MAX_BYTES or not schedule.endswith(b"\n") or b"\r" in schedule or b"\0" in schedule:
            raise ValueError("crontab snapshot is not bounded newline-terminated text")
    before = frozen.splitlines(keepends=True)
    after = destination.splitlines(keepends=True)
    if len(before) != len(after):
        raise ValueError("reviewed crontab changes the number of lines")
    changed = set()
    for old, new in zip(before, after, strict=True):
        if not old.startswith(FROZEN_PREFIX):
            if old != new or (not old.lstrip().startswith(b"#") and any(name in old for name in ENTRIES)):
                raise ValueError("reviewed crontab changes unrelated or unfrozen lines")
            continue
        source = LINE.fullmatch(old.removeprefix(FROZEN_PREFIX))
        target = LINE.fullmatch(new)
        if not source or not target or any(source[group] != target[group] for group in (1, 3, 4)):
            raise ValueError("reviewed crontab changes an entrant's cadence or command arguments")
        if target[3] in changed or sum(new.count(name) for name in ENTRIES) != 1:
            raise ValueError("reviewed crontab duplicates a scheduled entrant")
        if (target[2] == root) != (mode == "prepare") or (source[2] == root) != (mode == "revert"):
            raise ValueError("reviewed crontab points an entrant at the wrong release")
        changed.add(target[3])
    if changed != ENTRIES:
        raise ValueError("reviewed crontab must switch both scheduled entrants")


def check_barrier():
    identities = set()
    for fd, path in BARRIER.items():
        opened = os.fstat(fd)
        named = os.stat(path, follow_symlinks=False)
        identity = (opened.st_dev, opened.st_ino)
        if (not stat.S_ISREG(named.st_mode) or named.st_uid != os.geteuid() or named.st_mode & 0o022
                or identity != (named.st_dev, named.st_ino) or identity in identities):
            raise ValueError("cron transition barrier descriptor identity differs")
        identities.add(identity)
        # A separate open must encounter the existing lock, and the inherited
        # descriptor must already own that lock.
        other = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            try:
                fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            else:
                raise ValueError("cron transition barrier was not held")
        finally:
            os.close(other)


def sync_hold_directory():
    fd = os.open(HOLD.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_hold(evidence):
    fd = os.open(HOLD, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(evidence)
        stream.flush()
        os.fsync(stream.fileno())
    sync_hold_directory()


def clear_hold(evidence):
    if reviewed_bytes(HOLD) != evidence:
        raise ValueError("cron transition hold identity differs")
    HOLD.unlink()
    sync_hold_directory()


def read_crontab():
    return subprocess.check_output(["crontab", "-l"], timeout=8)


def write_crontab(schedule):
    try:
        status = subprocess.run(["crontab", "-"], input=schedule, timeout=8).returncode
    except subprocess.TimeoutExpired:
        status = 124
    return status, read_crontab()


def install(mode, root, expected_frozen, expected_target, path, evidence):
    destination = reviewed_bytes(path)
    if digest(destination) != expected_target:
        raise ValueError("reviewed destination crontab digest differs")
    frozen = read_crontab()
    if digest(frozen) != expected_frozen:
        raise ValueError("current frozen crontab digest differs")
    validate_switch(frozen, destination, mode, os.fsencode(root))
    check_barrier()
    write_hold(evidence)
    if read_crontab() != frozen:
        raise ValueError("current crontab changed before installation")
    # Bash retains the transition lock and all five barrier descriptors while
    # this child installs and reads back the exact reviewed bytes.
    status, actual = write_crontab(destination)
    if status or actual != destination:
        if actual == frozen:
            clear_hold(evidence)
        state = "still frozen" if actual == frozen else "requires operator inspection"
        raise ValueError(f"crontab installation did not verify ({state})")


def finish(mode, expected_target, evidence):
    check_barrier()
    if digest(read_crontab()) != expected_target:
        raise ValueError("installed crontab changed during post-install verification")
    clear_hold(evidence)
    print(f"finite trial {mode} installed and verified crontab sha256={expected_target}")


def recover(mode, root, expected_frozen, expected_target, path, evidence):
    check_barrier()
    if reviewed_bytes(HOLD) != evidence:
        raise ValueError("cron transition hold identity differs")
    frozen = reviewed_bytes(path)
    if digest(frozen) != expected_frozen:
        raise ValueError("reviewed frozen crontab digest differs")
    current = read_crontab()
    if current != frozen:
        if digest(current) != expected_target:
            raise ValueError("unrecognized current crontab; protected transition hold retained")
        validate_switch(frozen, current, mode, os.fsencode(root))
        if read_crontab() != current:
            raise ValueError("current crontab changed before recovery")
        status, actual = write_crontab(frozen)
        if status or actual != frozen:
            raise ValueError("frozen crontab restoration did not verify; protected transition hold retained")
    clear_hold(evidence)
    print(f"finite trial {mode} recovered frozen crontab sha256={expected_frozen}")


if __name__ == "__main__":
    try:
        action, mode, root, before_sha, after_sha, path = sys.argv[1:]
        if mode not in ("prepare", "revert") or not all(
            re.fullmatch(r"[a-f0-9]{64}", value) for value in (before_sha, after_sha)
        ):
            raise ValueError("invalid cron transition mode or digest")
        evidence = f"{mode} {before_sha} {after_sha}\n".encode("ascii")
        if action == "install":
            install(mode, root, before_sha, after_sha, path, evidence)
        elif action == "finish":
            finish(mode, after_sha, evidence)
        elif action == "recover":
            recover(mode, root, before_sha, after_sha, path, evidence)
        else:
            raise ValueError("invalid cron transition action")
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        sys.exit(f"finite trial crontab stopped: {error}")
