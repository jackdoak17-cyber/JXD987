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


def _try_reacquire_shared_writer_lock(fd: int) -> bool:
    """Acquire the data lock without jumping ahead of settlement priority."""
    priority_path = os.environ.get("ODDS_SYNC_SETTLEMENT_PRIORITY_FILE")
    is_settlement = os.environ.get("ODDS_SYNC_JOB_PRIORITY") == "settlement"
    priority_fd: int | None = None
    if priority_path and not is_settlement:
        try:
            priority_fd = os.open(priority_path, os.O_CREAT | os.O_RDWR, 0o600)
        except OSError as exc:
            raise SharedWriterLockUnavailable(
                "settlement priority gate is unavailable"
            ) from exc
        try:
            fcntl.flock(priority_fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(priority_fd)
            return False
        except OSError as exc:
            os.close(priority_fd)
            raise SharedWriterLockUnavailable(
                "settlement priority gate could not be acquired"
            ) from exc

    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except BlockingIOError:
            return False
    finally:
        if priority_fd is not None:
            fcntl.flock(priority_fd, fcntl.LOCK_UN)
            os.close(priority_fd)


@contextmanager
def release_shared_writer_lock() -> Iterator[None]:
    """Yield the global lock for non-conflicting I/O or independent writes.

    Production wrappers pass their already-held lock descriptor to child
    processes. Calls made outside those wrappers remain unchanged. The lock is
    always reacquired before returning to code that may read-modify-write the
    shared SQLite spool or publish rows that overlap another writer's contract.
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
            if _try_reacquire_shared_writer_lock(fd):
                return
            if remaining <= 0:
                raise SharedWriterLockUnavailable(
                    "shared writer lock reacquisition timed out"
                )
            time.sleep(min(0.25, max(0.01, remaining)))
