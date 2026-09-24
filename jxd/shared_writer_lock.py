"""Helpers for briefly yielding the shared pipeline writer lock around I/O."""

from __future__ import annotations

from contextlib import contextmanager
import fcntl
import os
import time
from typing import Iterator


class SharedWriterLockUnavailable(SystemExit):
    """The shared writer lock could not be reacquired before its bounded wait."""

    def __init__(self, message: str):
        self.message = message
        super().__init__(75)

    def __str__(self) -> str:
        return self.message


def _settlement_writer_is_waiting() -> bool:
    if os.environ.get("ODDS_SYNC_JOB_PRIORITY") == "settlement":
        return False
    path = os.environ.get("ODDS_SYNC_SETTLEMENT_PRIORITY_FILE")
    if not path:
        return False
    try:
        probe_fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    except OSError:
        return False
    try:
        try:
            fcntl.flock(probe_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        fcntl.flock(probe_fd, fcntl.LOCK_UN)
        return False
    finally:
        os.close(probe_fd)


@contextmanager
def release_shared_writer_lock() -> Iterator[None]:
    """Release an inherited wrapper lock while doing non-mutating external I/O.

    Production wrappers pass their already-held lock descriptor to child
    processes. Calls made outside those wrappers remain unchanged. The lock is
    always reacquired before returning to code that may read-modify-write the
    shared SQLite spool or publish overlapping serving rows.
    """

    raw_fd = os.environ.get("ODDS_SYNC_LOCK_FD")
    if raw_fd is None:
        yield
        return

    try:
        fd = int(raw_fd)
        os.fstat(fd)
    except (OSError, ValueError) as exc:
        raise SharedWriterLockUnavailable(
            "configured shared writer lock descriptor is unavailable"
        ) from exc

    try:
        fcntl.flock(fd, fcntl.LOCK_UN)
        yield
    finally:
        try:
            wait_seconds = max(
                0.0,
                float(os.environ.get("ODDS_SYNC_LOCK_REACQUIRE_WAIT_SECONDS", "300")),
            )
        except ValueError as exc:
            raise SharedWriterLockUnavailable(
                "ODDS_SYNC_LOCK_REACQUIRE_WAIT_SECONDS must be numeric"
            ) from exc
        deadline = time.monotonic() + wait_seconds
        while True:
            remaining = deadline - time.monotonic()
            if _settlement_writer_is_waiting():
                if remaining <= 0:
                    raise SharedWriterLockUnavailable(
                        "shared writer lock was not reacquired before the bounded wait"
                    )
                time.sleep(min(0.25, remaining))
                continue
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return
            except BlockingIOError:
                if remaining <= 0:
                    raise SharedWriterLockUnavailable(
                        "shared writer lock was not reacquired before the bounded wait"
                    )
                time.sleep(min(0.25, max(0.01, remaining)))
