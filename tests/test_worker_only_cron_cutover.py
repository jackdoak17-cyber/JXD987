#!/usr/bin/env python3
"""Disposable file-lock/crontab tests; no host crontab, source or database access."""
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import tempfile
from unittest.mock import patch


MODULE = Path(__file__).resolve().parents[1] / "scripts/vps/install_leaderboard_worker_crontab.py"
spec = importlib.util.spec_from_file_location("worker_cutover", MODULE)
cutover = importlib.util.module_from_spec(spec)
import sys
sys.modules[spec.name] = cutover
spec.loader.exec_module(cutover)


def fixture():
    temp = tempfile.TemporaryDirectory(prefix="worker-only-cutover-")
    root = Path(temp.name).resolve()
    lockdir = root / "locks"
    lockdir.mkdir(mode=0o700)
    legacy_dir = root / "legacy-locks"
    legacy_dir.mkdir()
    legacy_dir.chmod(0o1777)
    paths = cutover.Paths(
        legacy_dir / "cert", lockdir / "pins", legacy_dir / "legacy-worker",
        legacy_dir / "legacy-heavy", lockdir / "worker", lockdir / "heavy",
        legacy_dir / "canonical")
    for name in ("cert", "legacy_worker", "legacy_heavy", "worker", "heavy", "canonical"):
        path = getattr(paths, name)
        path.touch(mode=0o600)
        path.chmod(0o644 if name in ("cert", "legacy_worker", "legacy_heavy", "canonical") else 0o600)
    pins = {}
    for name in ("worker", "heavy"):
        path = getattr(paths, name)
        info = path.stat()
        pins[name] = {"path": str(path), "directory": str(path.parent),
                      "device": info.st_dev, "inode": info.st_ino}
    paths.pins.write_text(json.dumps(pins))
    paths.pins.chmod(0o600)
    old = root / "old-release"
    new = root / "new-release"
    source = root / "source-release"
    for path in (old, new, source):
        path.mkdir()
    runtime_names = ("scripts/vps/run_stats_reconciliation.sh", "scripts/vps/common.sh",
                     "scripts/reconcile_stats_provider_queue.py",
                     "scripts/vps/run_leaderboard_shadow_worker.sh", "scripts/leaderboard_shadow_worker.py")
    for relative in runtime_names:
        for release in (old, new, source):
            target = release / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text("unchanged source bytes\n" if relative in runtime_names[:3]
                              else f"{release.name} worker bytes\n")
    for release in (old, new):
        manifest = release / "scripts/vps/runtime_manifest.sha1"
        manifest.write_text("".join(
            f"{hashlib.sha1((release / name).read_bytes()).hexdigest()}  {name}\n"
            for name in runtime_names
        ))
    worker_old = f"1-4 * * * * cd {old} && {old}/scripts/vps/run_leaderboard_shadow_worker.sh\n".encode()
    worker_new = f"1-4 * * * * cd {new} && {new}/scripts/vps/run_leaderboard_shadow_worker.sh\n".encode()
    source_line = f"*/5 * * * * cd {source} && {source}/scripts/vps/run_stats_reconciliation.sh\n".encode()
    frozen = source_line + worker_old + b"# unrelated settings stay identical\n"
    target = source_line + worker_new + b"# unrelated settings stay identical\n"
    reviewed = root / "reviewed-cron"
    reviewed.write_bytes(target)
    reviewed.chmod(0o600)
    state = {"cron": frozen}
    def read(): return state["cron"]
    def write(value):
        state["cron"] = value
        return 0, state["cron"]
    def run(action="install", *, before=frozen, after=target, file=reviewed,
            from_root=old, to_root=new, read_fn=read, write_fn=write,
            process=lambda source_root: None):
        return cutover.execute(action, cutover.digest(before), cutover.digest(after), file,
                               from_root, to_root, paths=paths, read=read_fn,
                               write=write_fn, check_process=process, package_root=new)
    return temp, paths, old, new, source, source_line, worker_old, worker_new, frozen, target, reviewed, state, run


def case(name, callback):
    values = fixture()
    try:
        callback(*values[1:])
        print(f"PASS {name}")
    finally:
        values[0].cleanup()


def positive(paths, old, new, source, source_line, worker_old, worker_new, frozen, target, reviewed, state, run):
    # The supervisor lock is intentionally NOT in the worker-only barrier.
    supervisor = paths.cert.parent / "stats-supervisor"
    supervisor.touch()
    fd = os.open(supervisor, os.O_RDONLY)
    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        assert run() == "installed" and state["cron"] == target and not paths.marker.exists()
        assert state["cron"].startswith(source_line)
        reverse_file = paths.cert.parent.parent / "reverse-reviewed"
        reverse_file.write_bytes(source_line + worker_old + b"# unrelated settings stay identical\n")
        reverse_file.chmod(0o600)
        assert run(before=target, after=reverse_file.read_bytes(), file=reverse_file,
                   from_root=new, to_root=old) == "installed"
        assert state["cron"] == reverse_file.read_bytes() and not paths.marker.exists()
    finally:
        os.close(fd)


def rejects(name, mutate):
    def check(paths, old, new, source, source_line, worker_old, worker_new,
              frozen, target, reviewed, state, run):
        mutate(paths, old, new, source_line, frozen, target, reviewed, state)
        try:
            run(after=reviewed.read_bytes())
        except (OSError, ValueError, BlockingIOError):
            assert state["cron"] == frozen
        else:
            raise AssertionError(f"{name} was accepted")
    case(name, check)


case("one worker line switches and reverses while source supervisor lock is held", positive)


def nonsticky_legacy(paths, old, new, source, source_line, worker_old, worker_new,
                     frozen, target, reviewed, state, run):
    paths.cert.parent.chmod(0o777)
    try: run()
    except ValueError: pass
    else: raise AssertionError("nonsticky world-writable legacy lock directory passed")
    assert state["cron"] == frozen and not paths.marker.exists()
case("nonsticky 0777 legacy parent rejected", nonsticky_legacy)


def sticky_protected(paths, old, new, source, source_line, worker_old, worker_new,
                     frozen, target, reviewed, state, run):
    paths.worker.parent.chmod(0o1777)
    try: run()
    except ValueError: pass
    else: raise AssertionError("sticky world-writable protected lock directory passed")
    assert state["cron"] == frozen and not paths.marker.exists()
case("protected lock parent remains private", sticky_protected)


def foreign_legacy_leaf(paths, old, new, source, source_line, worker_old, worker_new,
                        frozen, target, reviewed, state, run):
    original_stat = Path.stat
    def foreign_stat(path, *args, **kwargs):
        result = original_stat(path, *args, **kwargs)
        if path == paths.cert:
            fields = list(result)
            fields[4] = os.geteuid() + 1
            return os.stat_result(fields)
        return result
    with patch.object(Path, "stat", foreign_stat):
        try: run()
        except ValueError: pass
        else: raise AssertionError("foreign-owned legacy lock leaf passed")
    assert state["cron"] == frozen and not paths.marker.exists()
case("foreign-owned legacy leaf rejected", foreign_legacy_leaf)


rejects("foreign worker marker untouched", lambda p,o,n,s,b,t,f,x: p.marker.write_text("foreign"))
rejects("symlink worker marker untouched", lambda p,o,n,s,b,t,f,x: p.marker.symlink_to(p.worker))
rejects("legacy worker marker blocks switch", lambda p,o,n,s,b,t,f,x: p.legacy_marker.write_text("foreign"))
rejects("legacy heavy marker blocks switch", lambda p,o,n,s,b,t,f,x: p.legacy_heavy_marker.write_text("foreign"))
rejects("changed source line rejected", lambda p,o,n,s,b,t,f,x: f.write_bytes(t.replace(s, s.replace(b"*/5", b"*/6"))))
rejects("unrelated byte change rejected", lambda p,o,n,s,b,t,f,x: f.write_bytes(t.replace(b"unrelated", b"changed")))
rejects("duplicate source entry rejected", lambda p,o,n,s,b,t,f,x: f.write_bytes(t.replace(s, s+s)))
rejects("wrong release target rejected", lambda p,o,n,s,b,t,f,x: f.write_bytes(t.replace(os.fsencode(n), os.fsencode(o))))
rejects("pin inode mismatch rejected", lambda p,o,n,s,b,t,f,x: p.pins.write_text(p.pins.read_text().replace(str(p.worker.stat().st_ino), "0")))
rejects("changed source runtime bytes rejected", lambda p,o,n,s,b,t,f,x: (p.cert.parent.parent / "source-release/scripts/vps/common.sh").write_text("changed source bytes\n"))
rejects("unattested target wrapper rejected", lambda p,o,n,s,b,t,f,x: (n / "scripts/vps/run_leaderboard_shadow_worker.sh").write_text("changed target bytes\n"))


def unattested_package(paths, old, new, source, source_line, worker_old, worker_new,
                       frozen, target, reviewed, state, run):
    try:
        cutover.execute("install", cutover.digest(frozen), cutover.digest(target), reviewed,
                        old, new, paths=paths, read=lambda: state["cron"],
                        write=lambda text: (0, text), check_process=lambda _: None,
                        package_root=source)
    except ValueError: pass
    else: raise AssertionError("helper package outside endpoints passed")
    assert state["cron"] == frozen and not paths.marker.exists()
case("helper package outside endpoints rejected", unattested_package)


def busy(paths, old, new, source, source_line, worker_old, worker_new, frozen, target, reviewed, state, run):
    fd = os.open(paths.heavy, os.O_RDONLY)
    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        try: run()
        except BlockingIOError: pass
        else: raise AssertionError("active source heavy lock was ignored")
        assert state["cron"] == frozen and not paths.marker.exists()
    finally: os.close(fd)
case("active source heavy lock defers", busy)


def canonical_busy(paths, old, new, source, source_line, worker_old, worker_new,
                   frozen, target, reviewed, state, run):
    fd = os.open(paths.canonical, os.O_RDONLY)
    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        try: run()
        except BlockingIOError: pass
        else: raise AssertionError("active canonical writer lock was ignored")
        assert state["cron"] == frozen and not paths.marker.exists()
    finally: os.close(fd)
case("active canonical writer lock defers", canonical_busy)


def uncertain(paths, old, new, source, source_line, worker_old, worker_new, frozen, target, reviewed, state, run):
    def write_then_fail(value):
        state["cron"] = value
        raise OSError("simulated crash after whole-crontab write")
    try: run(write_fn=write_then_fail)
    except OSError: pass
    else: raise AssertionError("uncertain write passed")
    assert state["cron"] == target and paths.marker.exists()
    recovery_file = paths.cert.parent.parent / "recovery-frozen"
    recovery_file.write_bytes(frozen)
    recovery_file.chmod(0o600)
    assert run("recover", file=recovery_file) == "recovered"
    assert state["cron"] == frozen and not paths.marker.exists()
case("uncertain write retains owned marker then exact recovery restores bytes", uncertain)


def inode_changed(paths, old, new, source, source_line, worker_old, worker_new,
                  frozen, target, reviewed, state, run):
    def write_then_fail(value):
        state["cron"] = value
        raise OSError("simulated crash")
    try: run(write_fn=write_then_fail)
    except OSError: pass
    else: raise AssertionError("simulated crash passed")
    recovery_file = paths.cert.parent.parent / "recovery-inode-frozen"
    recovery_file.write_bytes(frozen)
    recovery_file.chmod(0o600)
    marker_bytes = paths.marker.read_bytes()
    replacement = paths.marker.with_name("replacement-marker")
    replacement.write_bytes(marker_bytes)
    replacement.chmod(0o600)
    os.replace(replacement, paths.marker)
    try: run("recover", file=recovery_file)
    except ValueError: pass
    else: raise AssertionError("replaced owned marker inode passed recovery")
    assert paths.marker.exists() and state["cron"] == target
case("replaced marker inode blocks recovery", inode_changed)


def wrong_source_process(paths, old, new, source, source_line, worker_old, worker_new,
                         frozen, target, reviewed, state, run):
    def fail_source(_): raise ValueError("unexpected source process path")
    try: run(process=fail_source)
    except ValueError: pass
    else: raise AssertionError("unexpected source process passed")
    assert state["cron"] == frozen and not paths.marker.exists()
case("unexpected source process blocks switch", wrong_source_process)


def lock_replaced(paths, old, new, source, source_line, worker_old, worker_new,
                  frozen, target, reviewed, state, run):
    def replace_after_process(_):
        replacement = paths.worker.with_name("replacement-worker-lock")
        replacement.touch(mode=0o600)
        replacement.chmod(0o600)
        os.replace(replacement, paths.worker)
    try: run(process=replace_after_process)
    except ValueError: pass
    else: raise AssertionError("changed held lock path passed")
    assert paths.marker.exists() and state["cron"] == frozen
case("held lock path replaced before write retains marker", lock_replaced)


def concurrent(paths, old, new, source, source_line, worker_old, worker_new, frozen, target, reviewed, state, run):
    calls = 0
    def changed_read():
        nonlocal calls
        calls += 1
        return frozen if calls == 1 else frozen + b"# concurrent\n"
    try: run(read_fn=changed_read)
    except ValueError: pass
    else: raise AssertionError("concurrent crontab change passed")
    assert paths.marker.exists() and state["cron"] == frozen
case("concurrent crontab mutation retains owned marker", concurrent)


def postwrite_mismatch(paths, old, new, source, source_line, worker_old, worker_new,
                       frozen, target, reviewed, state, run):
    def wrong_readback(value):
        state["cron"] = value + b"# concurrent postwrite mutation\n"
        return 0, state["cron"]
    try: run(write_fn=wrong_readback)
    except ValueError: pass
    else: raise AssertionError("postwrite crontab mismatch passed")
    assert paths.marker.exists() and state["cron"] != target
case("postwrite crontab mismatch retains marker", postwrite_mismatch)


def relative_process(paths, old, new, source, source_line, worker_old, worker_new,
                     frozen, target, reviewed, state, run):
    procroot = paths.cert.parent.parent / "proc"
    procroot.mkdir()
    item = procroot / "123"
    item.mkdir()
    (item / "cwd").symlink_to(old)
    (item / "cmdline").write_bytes(b"python3\0scripts/leaderboard_shadow_worker.py\0run-once\0")
    try: cutover.check_processes(os.fsencode(source), procroot)
    except ValueError: pass
    else: raise AssertionError("relative worker child was missed")
    (item / "cmdline").write_bytes(b"bash\0-c\0echo scripts/leaderboard_shadow_worker.py\0")
    cutover.check_processes(os.fsencode(source), procroot)
    (item / "cwd").unlink()
    (item / "cwd").symlink_to(source)
    (item / "cmdline").write_bytes(b"bash\0scripts/vps/run_stats_reconciliation.sh\0")
    cutover.check_processes(os.fsencode(source), procroot)
case("relative worker argv caught without shell-text false positive", relative_process)
