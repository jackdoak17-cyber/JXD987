#!/usr/bin/env python3
"""Fetch and evaluate the Supabase host metrics used by the dedicated lane."""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import json
import math
import os
from pathlib import Path
import re
import stat
import sys
import tempfile
import urllib.request


MIB = 1024 * 1024
METRIC_LINE = re.compile(
    r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+([-+0-9.eE]+)$'
)
LABEL = re.compile(r'(\w+)="((?:[^"\\]|\\.)*)"')


def parse_prometheus(raw: str) -> list[tuple[str, dict[str, str], float]]:
    values = []
    for line in raw.splitlines():
        if not line or line.startswith("#"):
            continue
        match = METRIC_LINE.fullmatch(line)
        if not match:
            continue
        value = float(match.group(3))
        if not math.isfinite(value):
            continue
        labels = {key: bytes(value, "utf-8").decode("unicode_escape")
                  for key, value in LABEL.findall(match.group(2) or "")}
        if labels.get("service_type") not in (None, "db"):
            continue
        values.append((match.group(1), labels, value))
    return values


def build_snapshot(raw: str, observed_at: dt.datetime | None = None) -> dict:
    rows = parse_prometheus(raw)
    observed = observed_at or dt.datetime.now(dt.timezone.utc)
    cpu: dict[str, float] = {}
    disks: dict[str, float] = {}
    gauges: dict[str, float] = {}
    wanted = {
        "node_memory_MemAvailable_bytes",
        "node_memory_MemTotal_bytes",
        "node_memory_SwapFree_bytes",
        "node_memory_SwapTotal_bytes",
    }
    for name, labels, value in rows:
        if name == "node_cpu_seconds_total":
            mode = labels.get("mode")
            if mode:
                cpu[mode] = cpu.get(mode, 0.0) + value
        elif name == "node_disk_io_time_seconds_total":
            device = labels.get("device")
            if device:
                disks[device] = value
        elif name in wanted:
            gauges[name] = value
    if not cpu or not disks or set(gauges) != wanted:
        raise ValueError("required Supabase host metrics are missing")
    return {
        "observed_at": observed.isoformat().replace("+00:00", "Z"),
        "cpu_seconds": cpu,
        "disk_io_seconds": disks,
        "memory": gauges,
    }


def evaluate(previous: dict, current: dict, *, mode: str) -> dict:
    before = dt.datetime.fromisoformat(previous["observed_at"].replace("Z", "+00:00"))
    after = dt.datetime.fromisoformat(current["observed_at"].replace("Z", "+00:00"))
    elapsed = (after - before).total_seconds()
    if elapsed < 45 or elapsed > 180:
        raise ValueError("metrics interval is outside 45-180 seconds")

    cpu_delta = {}
    for name, value in current["cpu_seconds"].items():
        prior = previous["cpu_seconds"].get(name)
        if prior is None or value < prior:
            raise ValueError("CPU counter reset or disappeared")
        cpu_delta[name] = value - prior
    total_cpu = sum(cpu_delta.values())
    if total_cpu <= 0:
        raise ValueError("CPU interval is empty")
    busy_pct = 100 * max(0.0, total_cpu - cpu_delta.get("idle", 0.0)
                         - cpu_delta.get("iowait", 0.0)) / total_cpu
    iowait_pct = 100 * cpu_delta.get("iowait", 0.0) / total_cpu

    common_devices = set(previous["disk_io_seconds"]) & set(current["disk_io_seconds"])
    if not common_devices:
        raise ValueError("disk counters have no stable device")
    disk_busy = {}
    for device in sorted(common_devices):
        delta = current["disk_io_seconds"][device] - previous["disk_io_seconds"][device]
        if delta < 0:
            raise ValueError("disk counter regressed")
        disk_busy[device] = 100 * delta / elapsed

    memory = current["memory"]
    total_memory = memory["node_memory_MemTotal_bytes"]
    available_memory = memory["node_memory_MemAvailable_bytes"]
    if total_memory <= 0 or not 0 <= available_memory <= total_memory:
        raise ValueError("memory gauges are invalid")
    available_pct = 100 * available_memory / total_memory
    swap_total = memory["node_memory_SwapTotal_bytes"]
    swap_free = memory["node_memory_SwapFree_bytes"]
    if swap_total < 0 or not 0 <= swap_free <= swap_total:
        raise ValueError("swap gauges are invalid")

    breaches = []
    if mode == "admission":
        if busy_pct >= 50: breaches.append("cpu_busy_gte_50pct")
        if iowait_pct >= 20: breaches.append("cpu_iowait_gte_20pct")
        if max(disk_busy.values()) >= 70: breaches.append("disk_busy_gte_70pct")
        if available_pct < 25 or available_memory < 384 * MIB:
            breaches.append("memory_available_lt_25pct_or_384mib")
    elif mode == "post":
        if busy_pct >= 70: breaches.append("cpu_busy_gte_70pct")
        if iowait_pct >= 30: breaches.append("cpu_iowait_gte_30pct")
        if max(disk_busy.values()) >= 80: breaches.append("disk_busy_gte_80pct")
        if available_pct < 20 or available_memory < 256 * MIB:
            breaches.append("memory_available_lt_20pct_or_256mib")
    else:
        raise ValueError("unknown resource-meter mode")
    return {
        "ok": not breaches,
        "mode": mode,
        "interval_seconds": elapsed,
        "breaches": breaches,
        "cpu_busy_pct": round(busy_pct, 3),
        "cpu_iowait_pct": round(iowait_pct, 3),
        "disk_busy_pct": {key: round(value, 3) for key, value in disk_busy.items()},
        "memory_available_bytes": int(available_memory),
        "memory_total_bytes": int(total_memory),
        "memory_available_pct": round(available_pct, 3),
        "swap_used_pct": round(100 * (swap_total - swap_free) / swap_total, 3)
            if swap_total else 0.0,
        "observed_at": current["observed_at"],
    }


def read_private_json(path: Path) -> dict | None:
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        return None
    with os.fdopen(descriptor, encoding="utf-8") as source:
        info = os.fstat(source.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_mode & 0o077:
            raise ValueError("metrics state file is untrusted")
        return json.load(source)


def write_private_json(path: Path, value: dict) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as target:
            json.dump(value, target, sort_keys=True)
            target.write("\n")
            target.flush()
            os.fsync(target.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def fetch_metrics(url: str, service_key: str) -> str:
    endpoint = url.rstrip("/") + "/customer/v1/privileged/metrics"
    token = base64.b64encode(f"service_role:{service_key}".encode()).decode()
    request = urllib.request.Request(endpoint, headers={"Authorization": f"Basic {token}"})
    with urllib.request.urlopen(request, timeout=10) as response:
        if response.status != 200:
            raise RuntimeError(f"Supabase metrics returned HTTP {response.status}")
        return response.read(16 * 1024 * 1024).decode("utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", required=True)
    parser.add_argument("--mode", choices=("admission", "post"), required=True)
    args = parser.parse_args()
    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
    if not url or not key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required")
    state_path = Path(args.state)
    previous = read_private_json(state_path)
    current = build_snapshot(fetch_metrics(url, key))
    write_private_json(state_path, current)
    if previous is None:
        print(json.dumps({"ok": False, "reason": "metrics_baseline_seeded",
                          "observed_at": current["observed_at"]}, sort_keys=True))
        return 75
    result = evaluate(previous, current, mode=args.mode)
    print(json.dumps(result, sort_keys=True))
    return 0 if result["ok"] else 75


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as error:
        print(json.dumps({"ok": False, "reason": str(error)[:240]}, sort_keys=True))
        sys.exit(75)
