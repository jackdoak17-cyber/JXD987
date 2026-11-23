#!/usr/bin/env python3
"""Atomic worker-only cron cutover; the long-lived source supervisor is untouched."""
from contextlib import ExitStack
from dataclasses import dataclass
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import uuid


WORKER = b"run_leaderboard_shadow_worker.sh"
SOURCE = b"run_stats_reconciliation.sh"
LINE = re.compile(rb"((?:[^\s#]+[ \t]+){5}cd )(/[a-zA-Z0-9_./-]+)"
                  rb" && \2/scripts/vps/(run_stats_reconciliation\.sh|run_leaderboard_shadow_worker\.sh)"
                  rb"([ \t][^\r\n]*\n|\n)")
SOURCE_LINE = re.compile(
    rb"((?:[^\s#]+[ \t]+){5}cd )(/[a-zA-Z0-9_./-]+)"
    rb' && STATS_TX_URL="\$\(cat /etc/oddssearch/stats-export-transaction\.url\)"'
    rb' && test -n "\$STATS_TX_URL" && SUPABASE_DB_URL_SESSION="\$STATS_TX_URL" '
    rb"\2/scripts/vps/run_stats_reconciliation\.sh([ \t][^\r\n]*\n|\n)"
)
MAX_BYTES = 256 * 1024


@dataclass(frozen=True)
class Paths:
    cert: Path
    pins: Path
    legacy_worker: Path
    legacy_heavy: Path
    worker: Path
    heavy: Path
    canonical: Path

    @property
    def marker(self):
        return Path(str(self.worker) + ".quarantine")

    @property
    def heavy_marker(self):
        return Path(str(self.worker) + ".quarantine.heavy")

    @property
    def legacy_marker(self):
        return Path(str(self.legacy_worker) + ".quarantine")

    @property
    def legacy_heavy_marker(self):
        return Path(str(self.legacy_worker) + ".quarantine.heavy")


HOST = Paths(
    Path("/var/lock/operations-certification.lock"),
    Path("/var/lib/oddssearch/leaderboard-locks/pins.json"),
    Path("/var/lock/oddssearch-leaderboard-shadow.lock"),
    Path("/var/lock/oddssearch-db-heavy.lock"),
    Path("/var/lib/oddssearch/leaderboard-locks/oddssearch-leaderboard-shadow.lock"),
    Path("/var/lib/oddssearch/leaderboard-locks/oddssearch-db-heavy.lock"),
    Path("/var/lock/odds-sync.lock"),
)


def digest(value):
    return hashlib.sha256(value).hexdigest()


def protected_file(path, owner, *, private=False, legacy_lock=False):
    if not path.is_absolute() or path.is_symlink():
        raise ValueError("protected path is not absolute regular file")
    parent = path.parent.stat()
    # /var/lock is a root-owned sticky 1777 directory on the installed host.
    # Only the four existing legacy lock leaves may use that layout.
    sticky_legacy = (legacy_lock and stat.S_ISDIR(parent.st_mode)
                     and stat.S_IMODE(parent.st_mode) == 0o1777)
    if parent.st_uid != owner or (parent.st_mode & 0o022 and not sticky_legacy):
        raise ValueError("protected directory owner or mode changed")
    named = path.stat(follow_symlinks=False)
    if (not stat.S_ISREG(named.st_mode) or named.st_nlink != 1 or named.st_uid != owner
            or named.st_mode & (0o077 if private else 0o022)):
        raise ValueError("protected file owner or mode changed")
    return named


def open_lock(stack, path, owner, *, private=False, legacy_lock=False):
    named = protected_file(path, owner, private=private, legacy_lock=legacy_lock)
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    stack.callback(os.close, fd)
    held = os.fstat(fd)
    if (held.st_dev, held.st_ino) != (named.st_dev, named.st_ino):
        raise ValueError("lock path changed after open")
    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    return fd


def acquire_barrier(stack, paths, owner):
    # Certification first. Never acquire the permanently held source-supervisor lock.
    names = ("cert", "pins", "legacy_worker", "legacy_heavy", "worker", "heavy", "canonical")
    legacy_names = ("cert", "legacy_worker", "legacy_heavy", "canonical")
    held = {name: open_lock(stack, getattr(paths, name), owner, private=name == "pins",
                            legacy_lock=name in legacy_names)
            for name in names}
    identities = [(os.fstat(fd).st_dev, os.fstat(fd).st_ino) for fd in held.values()]
    if len(set(identities)) != len(identities):
        raise ValueError("transition locks alias")
    os.lseek(held["pins"], 0, os.SEEK_SET)
    with os.fdopen(os.dup(held["pins"]), "rb") as source:
        raw = source.read(4097)
    if len(raw) > 4096:
        raise ValueError("protected pins too large")
    pins = json.loads(raw)
    if not isinstance(pins, dict) or set(pins) != {"worker", "heavy"}:
        raise ValueError("protected pins incomplete")
    for kind in ("worker", "heavy"):
        path = getattr(paths, kind)
        pin = pins[kind]
        if (not isinstance(pin, dict) or set(pin) != {"path", "directory", "device", "inode"}
                or pin["path"] != str(path) or pin["directory"] != str(path.parent.resolve(strict=True))
                or type(pin["device"]) is not int or type(pin["inode"]) is not int
                or (pin["device"], pin["inode"]) != identities[names.index(kind)]):
            raise ValueError("protected lock identity differs from pin")
    return held


def recheck_barrier(held, paths, owner):
    for name, fd in held.items():
        path = getattr(paths, name)
        named = protected_file(path, owner, private=name == "pins",
                               legacy_lock=name in ("cert", "legacy_worker", "legacy_heavy", "canonical"))
        opened = os.fstat(fd)
        if (opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino):
            raise ValueError("held transition lock path changed")
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    os.lseek(held["pins"], 0, os.SEEK_SET)
    with os.fdopen(os.dup(held["pins"]), "rb") as source:
        pins = json.loads(source.read(4097))
    if not isinstance(pins, dict) or set(pins) != {"worker", "heavy"}:
        raise ValueError("protected pins changed")
    for kind in ("worker", "heavy"):
        path = getattr(paths, kind)
        info = os.fstat(held[kind])
        pin = pins[kind]
        if (not isinstance(pin, dict) or set(pin) != {"path", "directory", "device", "inode"}
                or pin != {"path": str(path), "directory": str(path.parent.resolve(strict=True)),
                           "device": info.st_dev, "inode": info.st_ino}):
            raise ValueError("protected lock pin changed")


def read_private(path, owner):
    if path.parent.resolve(strict=True) != path.parent:
        raise ValueError("reviewed private path parent is not canonical")
    named = protected_file(path, owner, private=True)
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as source:
        held = os.fstat(source.fileno())
        if (not stat.S_ISREG(held.st_mode) or held.st_nlink != 1
                or (held.st_dev, held.st_ino) != (named.st_dev, named.st_ino)):
            raise ValueError("private file is nonregular")
        value = source.read(MAX_BYTES + 1)
    if len(value) > MAX_BYTES:
        raise ValueError("private file too large")
    return value


def validate_switch(before, after, from_root, to_root):
    for text in (before, after):
        if len(text) > MAX_BYTES or not text.endswith(b"\n") or b"\r" in text or b"\0" in text:
            raise ValueError("invalid crontab text")
    old_lines = before.splitlines(keepends=True)
    new_lines = after.splitlines(keepends=True)
    if len(old_lines) != len(new_lines):
        raise ValueError("crontab line count changed")
    worker_count = source_count = 0
    source_root = None
    for old, new in zip(old_lines, new_lines, strict=True):
        if WORKER in old or WORKER in new:
            source = LINE.fullmatch(old)
            target = LINE.fullmatch(new)
            if (not source or not target or source[3] != WORKER or target[3] != WORKER
                    or source[2] != from_root or target[2] != to_root
                    or any(source[group] != target[group] for group in (1, 4))):
                raise ValueError("worker switch changes cadence, args or release")
            worker_count += 1
        elif old != new:
            raise ValueError("unrelated crontab byte changed")
        elif not old.lstrip().startswith(b"#"):
            if SOURCE in old:
                source = SOURCE_LINE.fullmatch(old)
                if not source:
                    raise ValueError("source entry is not canonical")
                source_count += 1
                source_root = source[2]
    if worker_count != 1 or source_count != 1:
        raise ValueError("worker or unchanged source entry is missing or duplicated")
    return source_root


def verify_endpoint(root):
    manifest = root / "scripts/vps/runtime_manifest.sha1"
    if not manifest.is_file() or manifest.resolve(strict=True) != manifest:
        raise ValueError("release manifest missing or linked")
    entries = manifest.read_text().splitlines()
    found = set()
    for entry in entries:
        match = re.fullmatch(r"([a-f0-9]{40})  ([a-zA-Z0-9_./-]+)", entry)
        if not match:
            raise ValueError("invalid release manifest entry")
        relative = Path(match[2])
        if relative.is_absolute() or ".." in relative.parts or relative in found:
            raise ValueError("unsafe release manifest path")
        found.add(relative)
        path = root / relative
        if (not path.is_file() or path.resolve(strict=True) != path
                or hashlib.sha1(path.read_bytes()).hexdigest() != match[1]):
            raise ValueError("release manifest bytes differ")
    required = {Path(name) for name in ("scripts/vps/run_leaderboard_shadow_worker.sh",
               "scripts/vps/run_stats_reconciliation.sh", "scripts/vps/common.sh",
               "scripts/leaderboard_shadow_worker.py", "scripts/reconcile_stats_provider_queue.py")}
    if not required.issubset(found):
        raise ValueError("release manifest omits critical runtime files")


def verify_source_bytes(source_root, package_root):
    source = Path(os.fsdecode(source_root))
    if source.resolve(strict=True) != source:
        raise ValueError("unchanged source release path is not canonical")
    for relative in ("scripts/vps/run_stats_reconciliation.sh", "scripts/vps/common.sh",
                     "scripts/reconcile_stats_provider_queue.py"):
        old = source / relative
        packaged = package_root / relative
        if (not old.is_file() or old.is_symlink() or not packaged.is_file()
                or packaged.is_symlink() or digest(old.read_bytes()) != digest(packaged.read_bytes())):
            raise ValueError("unchanged source runtime bytes differ from reviewed package")


def sync_directory(path):
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def create_marker(path, before_sha, after_sha, owner):
    if path.exists() or path.is_symlink():
        raise ValueError("worker quarantine already exists")
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(fd)
        evidence = (f"worker-cron {before_sha} {after_sha} {uuid.uuid4()} "
                    f"{info.st_dev} {info.st_ino}\n").encode()
        os.write(fd, evidence)
        os.fsync(fd)
    finally:
        os.close(fd)
    sync_directory(path)
    verify_marker(path, before_sha, after_sha, owner)
    return evidence


def verify_marker(path, before_sha, after_sha, owner):
    evidence = read_private(path, owner)
    parts = evidence.decode("ascii").strip().split(" ")
    if (len(parts) != 6 or parts[:3] != ["worker-cron", before_sha, after_sha]
            or str(uuid.UUID(parts[3])) != parts[3]):
        raise ValueError("worker quarantine owner differs")
    info = path.stat(follow_symlinks=False)
    if (str(info.st_dev), str(info.st_ino)) != tuple(parts[4:6]) or info.st_nlink != 1:
        raise ValueError("worker quarantine inode changed")
    return evidence


def clear_marker(path, evidence, before_sha, after_sha, owner):
    if verify_marker(path, before_sha, after_sha, owner) != evidence:
        raise ValueError("worker quarantine bytes changed")
    path.unlink()
    sync_directory(path)


def read_crontab():
    return subprocess.check_output(["crontab", "-l"], timeout=8)


def write_crontab(text):
    result = subprocess.run(["crontab", "-"], input=text, timeout=8)
    return result.returncode, read_crontab()


def check_processes(source_root, proc_root=Path("/proc")):
    if not proc_root.is_dir():
        raise ValueError("process inspection unavailable")
    expected_source_wrapper = os.fsdecode(source_root) + "/scripts/vps/run_stats_reconciliation.sh"
    expected_source_child = os.fsdecode(source_root) + "/scripts/reconcile_stats_provider_queue.py"
    for proc in proc_root.iterdir():
        if not proc.name.isdigit():
            continue
        try:
            args = (proc / "cmdline").read_bytes().split(b"\0")
            args = [os.fsdecode(value) for value in args if value]
            if not args:
                continue
            cwd = os.readlink(proc / "cwd")
        except FileNotFoundError:
            continue  # Process exited during the snapshot.
        script = None
        for arg in args[:4]:
            if arg in ("-c", "-m"):
                break  # Do not mistake arbitrary shell text for a script argv.
            if Path(arg).name in ("run_leaderboard_shadow_worker.sh", "leaderboard_shadow_worker.py",
                                  "run_stats_reconciliation.sh", "reconcile_stats_provider_queue.py"):
                script = (Path(arg) if Path(arg).is_absolute() else Path(cwd) / arg).resolve()
                break
        if script is None:
            continue
        if script.name in ("run_leaderboard_shadow_worker.sh", "leaderboard_shadow_worker.py"):
            raise ValueError("shadow worker process remains active")
        if str(script) not in (expected_source_wrapper, expected_source_child) or cwd != os.fsdecode(source_root):
            raise ValueError("source supervisor or child path/cwd differs")


def execute(action, before_sha, after_sha, reviewed_path, from_root, to_root,
            *, paths=HOST, owner=None, read=read_crontab, write=write_crontab,
            check_process=check_processes, package_root=None):
    owner = os.geteuid() if owner is None else owner
    package_root = Path(__file__).resolve().parents[2] if package_root is None else package_root
    if action not in ("install", "recover") or not all(re.fullmatch(r"[a-f0-9]{64}", x)
                                                   for x in (before_sha, after_sha)):
        raise ValueError("invalid worker transition action or digest")
    if (not from_root.is_absolute() or not to_root.is_absolute()
            or from_root == to_root or str(from_root.resolve(strict=True)) != str(from_root)
            or str(to_root.resolve(strict=True)) != str(to_root)):
        raise ValueError("worker release paths are not distinct canonical directories")
    if package_root not in (from_root, to_root):
        raise ValueError("reviewed helper package is not a transition endpoint")
    verify_endpoint(from_root)
    verify_endpoint(to_root)
    if paths.marker.exists() or paths.marker.is_symlink():
        if action != "recover":
            raise ValueError("preexisting worker quarantine is not owned by this transition")
    for marker in (paths.heavy_marker, paths.legacy_marker, paths.legacy_heavy_marker):
        if marker.exists() or marker.is_symlink():
            raise ValueError("legacy or heavy quarantine must be resolved")
    with ExitStack() as stack:
        held = acquire_barrier(stack, paths, owner)
        for marker in (paths.heavy_marker, paths.legacy_marker, paths.legacy_heavy_marker):
            if marker.exists() or marker.is_symlink():
                raise ValueError("legacy or heavy quarantine appeared during admission")
        if action == "install":
            destination = read_private(reviewed_path, owner)
            if digest(destination) != after_sha:
                raise ValueError("reviewed destination digest differs")
            frozen = read()
            if digest(frozen) != before_sha:
                raise ValueError("frozen crontab digest differs")
            source_root = validate_switch(frozen, destination, os.fsencode(from_root), os.fsencode(to_root))
            verify_source_bytes(source_root, package_root)
            check_process(source_root)
            evidence = create_marker(paths.marker, before_sha, after_sha, owner)
            recheck_barrier(held, paths, owner)
            if read() != frozen:
                raise ValueError("crontab changed before installation")
            recheck_barrier(held, paths, owner)
            status, actual = write(destination)
            if status or actual != destination:
                raise ValueError("crontab installation uncertain; owned quarantine retained")
            check_process(source_root)
            recheck_barrier(held, paths, owner)
            if read() != destination:
                raise ValueError("crontab changed after installation")
            recheck_barrier(held, paths, owner)
            clear_marker(paths.marker, evidence, before_sha, after_sha, owner)
            return "installed"
        evidence = verify_marker(paths.marker, before_sha, after_sha, owner)
        frozen = read_private(reviewed_path, owner)
        if digest(frozen) != before_sha:
            raise ValueError("reviewed frozen crontab digest differs")
        current = read()
        if current != frozen:
            if digest(current) != after_sha:
                raise ValueError("unrecognized crontab; owned quarantine retained")
            source_root = validate_switch(frozen, current, os.fsencode(from_root), os.fsencode(to_root))
            verify_source_bytes(source_root, package_root)
            check_process(source_root)
            recheck_barrier(held, paths, owner)
            status, actual = write(frozen)
            if status or actual != frozen:
                raise ValueError("crontab restoration uncertain; owned quarantine retained")
        if current == frozen:
            # Recovering an interrupted pre-write attempt still validates the unchanged source.
            source_root = next((LINE.fullmatch(line)[2] for line in frozen.splitlines(keepends=True)
                                if LINE.fullmatch(line) and LINE.fullmatch(line)[3] == SOURCE), None)
            if source_root is None:
                raise ValueError("unchanged source entry missing during recovery")
            verify_source_bytes(source_root, package_root)
        check_process(source_root)
        recheck_barrier(held, paths, owner)
        if read() != frozen:
            raise ValueError("crontab changed during recovery")
        recheck_barrier(held, paths, owner)
        clear_marker(paths.marker, evidence, before_sha, after_sha, owner)
        return "recovered"


if __name__ == "__main__":
    try:
        if os.geteuid() != 0 or len(sys.argv) != 7:
            raise ValueError("root and six transition arguments required")
        action, before_sha, after_sha, file_path, from_root, to_root = sys.argv[1:]
        print(execute(action, before_sha, after_sha, Path(file_path), Path(from_root), Path(to_root)))
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        sys.exit(f"worker-only cron transition stopped: {error}")
