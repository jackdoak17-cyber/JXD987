#!/usr/bin/env python3
"""
Verbose Supabase DB preflight for CI.

Prints host/port only, resolves DNS, retries with backoff, and writes
JSON + stderr artifacts for summary jobs.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import time
import urllib.parse
from typing import Dict, List, Optional, Tuple


def parse_db_url(raw: str) -> Dict[str, Optional[str]]:
    parsed = urllib.parse.urlparse(raw)
    dbname = parsed.path.lstrip("/") or "postgres"
    return {
        "host": parsed.hostname,
        "port": str(parsed.port or 5432),
        "user": urllib.parse.unquote(parsed.username) if parsed.username else None,
        "password": urllib.parse.unquote(parsed.password) if parsed.password else None,
        "dbname": dbname,
    }


def resolve_dns(host: str) -> Tuple[List[str], List[str]]:
    ipv4 = []
    ipv6 = []
    try:
        infos = socket.getaddrinfo(host, None)
    except socket.gaierror:
        return ipv4, ipv6
    for family, _, _, _, sockaddr in infos:
        if family == socket.AF_INET:
            addr = sockaddr[0]
            if addr not in ipv4:
                ipv4.append(addr)
        elif family == socket.AF_INET6:
            addr = sockaddr[0]
            if addr not in ipv6:
                ipv6.append(addr)
    return ipv4, ipv6


def run_psql(
    db_url: str,
    sql: str,
    stderr_path: str,
    env: Dict[str, str],
) -> Tuple[int, str]:
    cmd = ["psql", db_url, "-v", "ON_ERROR_STOP=1", "-XtAc", sql]
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if proc.stderr:
        with open(stderr_path, "a", encoding="utf-8") as f:
            f.write(proc.stderr)
            if not proc.stderr.endswith("\n"):
                f.write("\n")
    return proc.returncode, proc.stderr.strip()


def run_psql_hostaddr(
    info: Dict[str, Optional[str]],
    hostaddr: str,
    sql: str,
    stderr_path: str,
    env: Dict[str, str],
) -> Tuple[int, str]:
    parts = [
        f"host={info.get('host') or ''}",
        f"hostaddr={hostaddr}",
        f"port={info.get('port') or '5432'}",
        f"dbname={info.get('dbname') or 'postgres'}",
    ]
    if info.get("user"):
        parts.append(f"user={info['user']}")
    conn = " ".join(parts)
    env = dict(env)
    if info.get("password"):
        env["PGPASSWORD"] = info["password"]
    cmd = ["psql", conn, "-v", "ON_ERROR_STOP=1", "-XtAc", sql]
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if proc.stderr:
        with open(stderr_path, "a", encoding="utf-8") as f:
            f.write(proc.stderr)
            if not proc.stderr.endswith("\n"):
                f.write("\n")
    return proc.returncode, proc.stderr.strip()


def classify_error(stderr_text: str) -> str:
    text = stderr_text.lower()
    if "network is unreachable" in text:
        return "network_unreachable"
    if "could not translate host name" in text or "name or service not known" in text:
        return "dns"
    if "no route to host" in text:
        return "no_route"
    if "ssl" in text:
        return "ssl"
    if "timeout" in text:
        return "timeout"
    return "psql_error"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--report-out", required=True)
    parser.add_argument("--stderr-out", required=True)
    parser.add_argument("--league-id", default=None)
    parser.add_argument("--retries", type=int, default=5)
    parser.add_argument("--initial-backoff", type=int, default=5)
    parser.add_argument("--sql", default="select now(), inet_client_addr(), inet_server_addr();")
    args = parser.parse_args()

    db_url = os.environ.get("SUPABASE_DB_URL_SESSION") or os.environ.get("SUPABASE_DB_URL")
    if not db_url:
        report = {
            "league_id": args.league_id,
            "host": None,
            "port": None,
            "ok": False,
            "error_class": "missing_env",
            "error_snippet": "Missing SUPABASE_DB_URL_SESSION",
        }
        with open(args.report_out, "w", encoding="utf-8") as f:
            f.write(json.dumps(report, indent=2))
        raise SystemExit("Missing SUPABASE_DB_URL_SESSION")

    info = parse_db_url(db_url)
    host = info.get("host") or ""
    port = info.get("port") or "5432"
    print(f"Supabase DB host={host} port={port}", flush=True)

    ipv4, ipv6 = resolve_dns(host)
    print(f"DNS A={ipv4 or 'none'}", flush=True)
    print(f"DNS AAAA={ipv6 or 'none'}", flush=True)
    if ipv6 and not ipv4:
        print("IPv6-only DNS detected", flush=True)

    report: Dict[str, Optional[object]] = {
        "league_id": int(args.league_id) if args.league_id is not None else None,
        "host": host,
        "port": int(port) if port else 0,
        "ok": False,
        "error_class": None,
        "error_snippet": None,
        "attempts": 0,
        "used_hostaddr": None,
        "ipv4": ipv4,
        "ipv6": ipv6,
    }

    env = dict(os.environ)
    env.setdefault("PGSSLMODE", "require")

    backoff = args.initial_backoff
    stderr_snippet = ""
    for attempt in range(1, args.retries + 1):
        print(f"Preflight attempt {attempt}/{args.retries}", flush=True)
        report["attempts"] = attempt
        code, stderr_text = run_psql(db_url, args.sql, args.stderr_out, env)
        stderr_snippet = stderr_text[:500] if stderr_text else ""
        if code == 0:
            report["ok"] = True
            report["error_class"] = None
            report["error_snippet"] = None
            with open(args.report_out, "w", encoding="utf-8") as f:
                f.write(json.dumps(report, indent=2))
            return

        error_class = classify_error(stderr_text)
        report["error_class"] = error_class
        report["error_snippet"] = stderr_snippet

        if error_class == "network_unreachable" and host.startswith("db."):
            print(
                "Direct DB endpoint is IPv6; GitHub runners are IPv4-only. "
                "Use Supavisor Session pooler URL (aws-*.pooler.supabase.com:5432) "
                "or buy IPv4 add-on.",
                flush=True,
            )

        # If IPv6 is present and we have IPv4, try forcing hostaddr.
        if error_class in {"network_unreachable", "no_route"} and ipv4:
            hostaddr = ipv4[0]
            print(f"Retrying with hostaddr={hostaddr}", flush=True)
            report["used_hostaddr"] = hostaddr
            code2, stderr_text2 = run_psql_hostaddr(info, hostaddr, args.sql, args.stderr_out, env)
            if code2 == 0:
                report["ok"] = True
                report["error_class"] = None
                report["error_snippet"] = None
                with open(args.report_out, "w", encoding="utf-8") as f:
                    f.write(json.dumps(report, indent=2))
                return
            stderr_snippet = stderr_text2[:500] if stderr_text2 else ""
            report["error_class"] = classify_error(stderr_text2)
            report["error_snippet"] = stderr_snippet

        if attempt < args.retries:
            print(f"Preflight failed; sleeping {backoff}s before retry", flush=True)
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)

    with open(args.report_out, "w", encoding="utf-8") as f:
        f.write(json.dumps(report, indent=2))
    raise SystemExit(f"Preflight failed: {report.get('error_class')}")


if __name__ == "__main__":
    main()
