#!/usr/bin/env python3
"""
Prune odds_outcomes rows in Supabase to an allowlisted market set.
Use with care: this is intended for one-time cleanup.
"""

from __future__ import annotations

import argparse
import os
import subprocess
from typing import Iterable, List

from scripts.export_odds_to_supabase_psql import (
    load_market_allowlist,
    normalize_market_key,
    redact_db_url,
)

DB_URL = os.environ.get("SUPABASE_DB_URL") or os.environ.get("SUPABASE_DB_URL_SESSION")


def sql_text_array(values: Iterable[str]) -> str:
    items = []
    for value in values:
        escaped = value.replace("'", "''")
        items.append(f"'{escaped}'")
    if not items:
        return "array[]::text[]"
    return f"array[{','.join(items)}]::text[]"


def run_psql(sql: str, label: str) -> str:
    if not DB_URL:
        raise SystemExit("Missing SUPABASE_DB_URL")
    cmd = [
        "psql",
        DB_URL,
        "-v",
        "ON_ERROR_STOP=1",
        "--echo-errors",
        "-v",
        "VERBOSITY=verbose",
        "-v",
        "SHOW_CONTEXT=always",
        "-At",
        "-F",
        "\t",
        "-c",
        sql,
    ]
    env = dict(os.environ)
    env.setdefault("PGCONNECT_TIMEOUT", "15")
    try:
        proc = subprocess.run(cmd, check=True, capture_output=True, text=True, env=env)
    except subprocess.CalledProcessError as exc:
        safe_cmd = [redact_db_url(c) if c == DB_URL else c for c in cmd]
        print(f"psql failed ({label}) cmd: {' '.join(safe_cmd)}")
        print("stdout:\n", exc.stdout or "")
        print("stderr:\n", exc.stderr or "")
        raise
    return (proc.stdout or "").strip()


def parse_allowlist(raw: str) -> List[str]:
    if not raw:
        return []
    items = [normalize_market_key(item) for item in raw.split(",") if item.strip()]
    return [item for item in items if item]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--allowlist",
        default="",
        help="Comma-separated market keys. Defaults to ODDS_MARKET_ALLOWLIST or built-in core list.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Only report rows to delete.")
    parser.add_argument("--vacuum", action="store_true", help="Run VACUUM (ANALYZE) after delete.")
    parser.add_argument("--reindex", action="store_true", help="Run REINDEX TABLE after delete.")
    args = parser.parse_args()

    allowlist = []
    if args.allowlist:
        if args.allowlist.strip().lower() in {"all", "*"}:
            raise SystemExit("Refusing to prune with allowlist=all.")
        allowlist = parse_allowlist(args.allowlist)
    else:
        env_allowlist = load_market_allowlist()
        if env_allowlist is None:
            raise SystemExit("Refusing to prune: allowlist disabled (ODDS_MARKET_ALLOWLIST=all).")
        allowlist = sorted(env_allowlist)

    if not allowlist:
        raise SystemExit("Allowlist is empty; refusing to prune.")

    allowlist_sql = sql_text_array(allowlist)
    print(f"Allowlist size={len(allowlist)}")

    if args.dry_run:
        sql = (
            "select count(*)::bigint from public.odds_outcomes "
            f"where not (market_key = any({allowlist_sql}));"
        )
        count = run_psql(sql, label="dry_run")
        print(f"Rows outside allowlist: {count}")
        return

    delete_sql = (
        "with deleted as (\n"
        "  delete from public.odds_outcomes\n"
        f"  where not (market_key = any({allowlist_sql}))\n"
        "  returning 1\n"
        ")\n"
        "select count(*)::bigint from deleted;"
    )
    deleted = run_psql(delete_sql, label="prune")
    print(f"Deleted rows: {deleted}")

    if args.vacuum:
        run_psql("vacuum (analyze) public.odds_outcomes;", label="vacuum")
        print("VACUUM completed.")

    if args.reindex:
        run_psql("reindex table public.odds_outcomes;", label="reindex")
        print("REINDEX completed.")


if __name__ == "__main__":
    main()
