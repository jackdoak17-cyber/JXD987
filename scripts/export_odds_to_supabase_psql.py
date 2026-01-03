#!/usr/bin/env python3
"""
Export odds from SQLite to Supabase via psql COPY + upsert.

- Builds a CSV for odds_outcomes within the fixture window.
- Loads into a staging table via COPY.
- Upserts into public.odds_outcomes with a change-only WHERE clause.
- Writes a JSON report for workflow artifacts.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sqlite3
import subprocess
import urllib.parse
import tempfile
import time
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

DB_PATH = os.environ.get("JXD_DB_PATH", "data/jxd.sqlite")
DB_URL = os.environ.get("SUPABASE_DB_URL") or os.environ.get("SUPABASE_DB_URL_SESSION")


def redact_db_url(db_url: str) -> str:
    if not db_url:
        return db_url
    if "://" in db_url:
        parsed = urllib.parse.urlparse(db_url)
        if parsed.password:
            netloc = f"{parsed.username}:***@{parsed.hostname}"
            if parsed.port:
                netloc += f":{parsed.port}"
            return parsed._replace(netloc=netloc).geturl()
        return db_url
    if "password=" in db_url:
        parts = []
        for part in db_url.split():
            if part.startswith("password="):
                parts.append("password=***")
            else:
                parts.append(part)
        return " ".join(parts)
    return db_url


def append_output(path: str, label: str, content: str) -> None:
    if not path:
        return
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"--- {label} ---\n")
        f.write(content or "")
        if content and not content.endswith("\n"):
            f.write("\n")


def tail_text(path: str, max_lines: int = 200) -> str:
    try:
        text = Path(path).read_text(encoding="utf-8")
    except OSError:
        return ""
    lines = text.splitlines()
    if not lines:
        return ""
    return "\n".join(lines[-max_lines:])


def parse_league_ids(raw: str) -> List[int]:
    return [int(x) for x in raw.split(",") if x.strip()]


def sqlite_window_bounds(days_forward: int) -> Tuple[str, str]:
    start_dt = datetime.utcnow()
    end_dt = start_dt + timedelta(days=days_forward)
    fmt = "%Y-%m-%d %H:%M:%S"
    return start_dt.strftime(fmt), end_dt.strftime(fmt)


def fetch_fixture_league_ids(conn: sqlite3.Connection, days_forward: int) -> List[int]:
    start_dt, end_dt = sqlite_window_bounds(days_forward)
    cur = conn.cursor()
    cur.execute(
        """
        select distinct league_id
        from fixtures
        where league_id is not null
          and datetime(starting_at) >= ? and datetime(starting_at) < ?
        """,
        (start_dt, end_dt),
    )
    return [row[0] for row in cur.fetchall()]


def sql_array(values: List[int]) -> str:
    if not values:
        return "array[]::int[]"
    items = ",".join(str(v) for v in values)
    return f"array[{items}]::int[]"


def run_psql(
    db_url: str,
    sql: str,
    from_file: bool = False,
    label: str = "psql",
    err_path: Optional[str] = None,
    out_path: Optional[str] = None,
) -> str:
    cmd = [
        "psql",
        db_url,
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
    ]
    if from_file:
        cmd.extend(["-f", sql])
    else:
        cmd.extend(["-c", sql])
    env = dict(os.environ)
    env.setdefault("PGCONNECT_TIMEOUT", "15")
    retry_limit = int(os.environ.get("PSQL_RETRIES", "3"))
    retry_sleep = float(os.environ.get("PSQL_RETRY_SLEEP", "2.0"))
    attempt = 0
    while True:
        attempt += 1
        try:
            proc = subprocess.run(cmd, check=True, capture_output=True, text=True, env=env)
            append_output(out_path, f"{label} stdout", proc.stdout or "")
            append_output(err_path, f"{label} stderr", proc.stderr or "")
            return (proc.stdout or "").strip()
        except subprocess.CalledProcessError as exc:
            safe_cmd = [redact_db_url(c) if c == db_url else c for c in cmd]
            header = (
                f"psql failed ({label}) exit={exc.returncode}\n"
                f"psql cmd: {' '.join(safe_cmd)}\n"
                f"attempt: {attempt}/{retry_limit}\n"
            )
            stderr = (exc.stderr or "").lower()
            stdout = (exc.stdout or "").lower()
            retryable = any(
                token in stderr or token in stdout
                for token in (
                    "connection to server",
                    "timeout expired",
                    "server closed the connection",
                    "lock timeout",
                    "canceling statement due to lock timeout",
                    "could not connect",
                    "ssl sycall error",
                    "terminating connection",
                    "too many connections",
                    "connection not open",
                )
            )
            print(header)
            print("psql stdout:\n", exc.stdout or "")
            print("psql stderr:\n", exc.stderr or "")
            append_output(out_path, f"{label} stdout", (exc.stdout or ""))
            append_output(err_path, f"{label} stderr", (exc.stderr or ""))
            append_output(err_path, f"{label} header", header)
            if retryable and attempt < retry_limit:
                sleep_seconds = retry_sleep * attempt
                print(f"Retrying psql in {sleep_seconds:.1f}s...", flush=True)
                time.sleep(sleep_seconds)
                continue
            raise


def build_outcomes_csv(
    conn: sqlite3.Connection,
    league_ids: Iterable[int],
    days_forward: int,
    out_path: Path,
    progress_every: int,
    progress_fixtures: int,
    total_rows_estimate: int,
    total_fixtures_estimate: int,
    max_runtime_seconds: int,
) -> Tuple[int, bool, Optional[int]]:
    start_dt, end_dt = sqlite_window_bounds(days_forward)
    params: List[object] = [start_dt, end_dt]
    league_clause = ""
    if league_ids:
        placeholders = ",".join("?" for _ in league_ids)
        league_clause = f"and f.league_id in ({placeholders})"
        params.extend(league_ids)

    cur = conn.cursor()
    cur.execute(
        f"""
        select o.fixture_id,
               o.bookmaker_id,
               o.market_key,
               o.selection_key,
               o.line,
               o.price_decimal,
               o.price_american,
               o.participant_type,
               o.participant_id,
               o.last_updated_at
        from odds_outcomes o
        join fixtures f on f.id = o.fixture_id
        where datetime(f.starting_at) >= ? and datetime(f.starting_at) < ?
          {league_clause}
        order by o.fixture_id
        """,
        params,
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "fixture_id",
                "bookmaker_id",
                "market_key",
                "selection_key",
                "line",
                "price_decimal",
                "price_american",
                "participant_type",
                "participant_id",
                "last_updated_at",
            ]
        )

        total_rows = 0
        total_fixtures = 0
        last_report_rows = 0
        last_report_fixtures = 0
        last_fixture_id: Optional[int] = None
        partial_run = False
        start = time.time()
        while True:
            rows = cur.fetchmany(2000)
            if not rows:
                break
            for row in rows:
                fixture_id = row[0]
                if fixture_id != last_fixture_id:
                    total_fixtures += 1
                    last_fixture_id = fixture_id
                writer.writerow(row)
                total_rows += 1
            elapsed = time.time() - start
            should_log_rows = progress_every and (total_rows - last_report_rows) >= progress_every
            should_log_fixtures = progress_fixtures and (total_fixtures - last_report_fixtures) >= progress_fixtures
            if should_log_rows or should_log_fixtures:
                rows_per_sec = total_rows / elapsed if elapsed > 0 else 0.0
                fixtures_per_sec = total_fixtures / elapsed if elapsed > 0 else 0.0
                eta_seconds = None
                if total_rows_estimate > 0 and rows_per_sec > 0:
                    eta_seconds = max(0.0, (total_rows_estimate - total_rows) / rows_per_sec)
                elif total_fixtures_estimate > 0 and fixtures_per_sec > 0:
                    eta_seconds = max(0.0, (total_fixtures_estimate - total_fixtures) / fixtures_per_sec)
                eta_text = f"{eta_seconds:.0f}" if eta_seconds is not None else "n/a"
                print(
                    "CSV progress "
                    f"rows={total_rows} fixtures={total_fixtures} "
                    f"rows_per_sec={rows_per_sec:.1f} fixtures_per_sec={fixtures_per_sec:.2f} "
                    f"elapsed_sec={elapsed:.0f} eta_sec={eta_text} "
                    f"last_fixture_id={last_fixture_id}",
                    flush=True,
                )
                if should_log_rows:
                    last_report_rows = total_rows
                if should_log_fixtures:
                    last_report_fixtures = total_fixtures
            if max_runtime_seconds and elapsed > max_runtime_seconds:
                partial_run = True
                break
            if partial_run:
                break

    return total_rows, partial_run, last_fixture_id


def write_csv_head(csv_path: Path, out_path: Path, max_lines: int = 200) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("r", encoding="utf-8") as src, out_path.open("w", encoding="utf-8") as dst:
        for idx, line in enumerate(src, start=1):
            dst.write(line)
            if idx >= max_lines:
                break


def write_csv_summary(csv_path: Path, out_path: Path, top_n: int = 50) -> None:
    counts: Dict[str, int] = {}
    mapped: Dict[str, int] = {}
    unmatched_keys: Dict[str, int] = {}
    unmatched_team_keys: Dict[str, int] = {}
    line_missing_keys: Dict[str, int] = {}
    team_unmapped_keys: Dict[str, int] = {}
    total_rows = 0
    line_markets = {
        "team_shots",
        "team_shots_on_target",
        "match_shots",
        "match_shots_on_target",
    }
    with csv_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total_rows += 1
            ptype = (row.get("participant_type") or "").strip() or "null"
            counts[ptype] = counts.get(ptype, 0) + 1
            pid = (row.get("participant_id") or "").strip()
            if pid:
                mapped[ptype] = mapped.get(ptype, 0) + 1
            if ptype == "player" and not pid:
                selection_key = (row.get("selection_key") or "").strip()
                if selection_key:
                    unmatched_keys[selection_key] = unmatched_keys.get(selection_key, 0) + 1
            if ptype == "team" and not pid:
                selection_key = (row.get("selection_key") or "").strip()
                if selection_key:
                    unmatched_team_keys[selection_key] = unmatched_team_keys.get(selection_key, 0) + 1
            market_key = (row.get("market_key") or "").strip()
            if market_key in line_markets:
                selection_key = (row.get("selection_key") or "").strip()
                line_val = (row.get("line") or "").strip()
                if not line_val:
                    line_missing_keys[selection_key] = line_missing_keys.get(selection_key, 0) + 1
                if market_key.startswith("team_") and not pid:
                    team_unmapped_keys[selection_key] = team_unmapped_keys.get(selection_key, 0) + 1
    top_unmatched = sorted(unmatched_keys.items(), key=lambda item: item[1], reverse=True)[:top_n]
    top_team_unmatched = sorted(unmatched_team_keys.items(), key=lambda item: item[1], reverse=True)[:30]
    top_line_missing = sorted(line_missing_keys.items(), key=lambda item: item[1], reverse=True)[:50]
    top_team_unmapped = sorted(team_unmapped_keys.items(), key=lambda item: item[1], reverse=True)[:50]
    payload = {
        "total_rows": total_rows,
        "counts_by_participant_type": counts,
        "mapped_by_participant_type": mapped,
        "unmatched_player_selection_keys": [
            {"selection_key": key, "count": count} for key, count in top_unmatched
        ],
        "unmatched_team_selection_keys": [
            {"selection_key": key, "count": count} for key, count in top_team_unmatched
        ],
        "line_missing_selection_keys": [
            {"selection_key": key, "count": count} for key, count in top_line_missing
        ],
        "team_unmapped_selection_keys": [
            {"selection_key": key, "count": count} for key, count in top_team_unmapped
        ],
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def stage_and_upsert(
    db_url: str,
    csv_path: Path,
    league_label: str,
    league_ids: List[int],
    days_forward: int,
    keep_sql: bool,
    err_path: Optional[str],
    out_path: Optional[str],
) -> Dict[str, int]:
    cols = [
        "fixture_id",
        "bookmaker_id",
        "market_key",
        "selection_key",
        "line",
        "price_decimal",
        "price_american",
        "participant_type",
        "participant_id",
        "last_updated_at",
    ]
    cols_sql = ", ".join(cols)
    csv_path_sql = csv_path.as_posix().replace("'", "''")
    copy_line = (
        f"\\copy odds_outcomes_stage ({cols_sql}) "
        f"from '{csv_path_sql}' with (format csv, header true);"
    )
    lock_timeout = os.environ.get("ODDS_LOCK_TIMEOUT", "0")
    statement_timeout = os.environ.get("ODDS_STATEMENT_TIMEOUT", "0")
    advisory_lock_key = os.environ.get("ODDS_ADVISORY_LOCK_KEY", "982374")
    idle_tx_timeout = os.environ.get("ODDS_IDLE_TX_TIMEOUT", "0")
    use_advisory_lock = os.environ.get("ODDS_USE_ADVISORY_LOCK", "").lower() in {"1", "true", "yes"}
    league_filter = ""
    if league_ids:
        league_filter = f"and f.league_id = any({sql_array(league_ids)})"
    fixture_window_sql = (
        f"with fixture_window as (\n"
        f"  select f.id, f.home_team_id, f.away_team_id\n"
        f"  from public.fixtures f\n"
        f"  where f.starting_at >= (now() at time zone 'utc')\n"
        f"    and f.starting_at < (now() at time zone 'utc') + interval '{days_forward} days'\n"
        f"    {league_filter}\n"
        f")"
    )
    match_delete_sql = f"""
{fixture_window_sql},
parsed_match as (
  select
    o.ctid as ctid,
    o.fixture_id,
    o.bookmaker_id,
    o.market_key,
    regexp_replace(o.selection_key, '^([0-9]+)_([0-9]+)_(over|under)$', '\\3') as new_sel,
    regexp_replace(o.selection_key, '^([0-9]+)_([0-9]+)_(over|under)$', '\\1.\\2')::numeric as new_line,
    o.price_decimal,
    o.price_american,
    o.last_updated_at
  from public.odds_outcomes o
  join fixture_window fw on fw.id = o.fixture_id
  where o.market_key in ('match_shots','match_shots_on_target')
    and o.line is null
    and o.selection_key ~ '^[0-9]+_[0-9]+_(over|under)$'
  union all
  select
    o.ctid as ctid,
    o.fixture_id,
    o.bookmaker_id,
    o.market_key,
    regexp_replace(o.selection_key, '^(over|under)_([0-9]+)_([0-9]+)$', '\\1') as new_sel,
    regexp_replace(o.selection_key, '^(over|under)_([0-9]+)_([0-9]+)$', '\\2.\\3')::numeric as new_line,
    o.price_decimal,
    o.price_american,
    o.last_updated_at
  from public.odds_outcomes o
  join fixture_window fw on fw.id = o.fixture_id
  where o.market_key in ('match_shots','match_shots_on_target')
    and o.line is null
    and o.selection_key ~ '^(over|under)_[0-9]+_[0-9]+$'
),
match_merge as (
  update public.odds_outcomes o
  set price_decimal = p.price_decimal,
      price_american = p.price_american,
      last_updated_at = p.last_updated_at
  from parsed_match p
  where o.fixture_id = p.fixture_id
    and o.bookmaker_id = p.bookmaker_id
    and o.market_key = p.market_key
    and o.selection_key = p.new_sel
    and o.line = p.new_line
    and (
      o.last_updated_at is null
      or (p.last_updated_at is not null and p.last_updated_at > o.last_updated_at)
    )
  returning 1
)
delete from public.odds_outcomes o
using parsed_match p
where o.ctid = p.ctid
  and exists (
    select 1
    from public.odds_outcomes o2
    where o2.fixture_id = p.fixture_id
      and o2.bookmaker_id = p.bookmaker_id
      and o2.market_key = p.market_key
      and o2.selection_key = p.new_sel
      and o2.line = p.new_line
  );
"""
    match_update_sql = f"""
{fixture_window_sql},
parsed_match as (
  select
    o.ctid as ctid,
    regexp_replace(o.selection_key, '^([0-9]+)_([0-9]+)_(over|under)$', '\\3') as new_sel,
    regexp_replace(o.selection_key, '^([0-9]+)_([0-9]+)_(over|under)$', '\\1.\\2')::numeric as new_line
  from public.odds_outcomes o
  join fixture_window fw on fw.id = o.fixture_id
  where o.market_key in ('match_shots','match_shots_on_target')
    and o.line is null
    and o.selection_key ~ '^[0-9]+_[0-9]+_(over|under)$'
  union all
  select
    o.ctid as ctid,
    regexp_replace(o.selection_key, '^(over|under)_([0-9]+)_([0-9]+)$', '\\1') as new_sel,
    regexp_replace(o.selection_key, '^(over|under)_([0-9]+)_([0-9]+)$', '\\2.\\3')::numeric as new_line
  from public.odds_outcomes o
  join fixture_window fw on fw.id = o.fixture_id
  where o.market_key in ('match_shots','match_shots_on_target')
    and o.line is null
    and o.selection_key ~ '^(over|under)_[0-9]+_[0-9]+$'
)
update public.odds_outcomes o
set line = p.new_line,
    selection_key = p.new_sel,
    participant_type = null,
    participant_id = null
from parsed_match p
where o.ctid = p.ctid;
"""
    team_delete_sql = f"""
{fixture_window_sql},
parsed_team as (
  select
    o.ctid as ctid,
    o.fixture_id,
    o.bookmaker_id,
    o.market_key,
    regexp_replace(o.selection_key, '^(over|under)_([0-9]+)_([0-9]+)_(?:team_)?(1|2)$', '\\1') as new_sel,
    regexp_replace(o.selection_key, '^(over|under)_([0-9]+)_([0-9]+)_(?:team_)?(1|2)$', '\\2.\\3')::numeric as new_line,
    case
      when regexp_replace(o.selection_key, '^.*_(?:team_)?(1|2)$', '\\1') = '1' then fw.home_team_id
      else fw.away_team_id
    end as new_participant_id,
    o.price_decimal,
    o.price_american,
    o.last_updated_at
  from public.odds_outcomes o
  join fixture_window fw on fw.id = o.fixture_id
  where o.market_key in ('team_shots','team_shots_on_target')
    and o.selection_key ~ '^(over|under)_[0-9]+_[0-9]+_(?:team_)?[12]$'
  union all
  select
    o.ctid as ctid,
    o.fixture_id,
    o.bookmaker_id,
    o.market_key,
    regexp_replace(o.selection_key, '^([0-9]+)_([0-9]+)_(over|under)_(?:team_)?(1|2)$', '\\3') as new_sel,
    regexp_replace(o.selection_key, '^([0-9]+)_([0-9]+)_(over|under)_(?:team_)?(1|2)$', '\\1.\\2')::numeric as new_line,
    case
      when regexp_replace(o.selection_key, '^.*_(?:team_)?(1|2)$', '\\1') = '1' then fw.home_team_id
      else fw.away_team_id
    end as new_participant_id,
    o.price_decimal,
    o.price_american,
    o.last_updated_at
  from public.odds_outcomes o
  join fixture_window fw on fw.id = o.fixture_id
  where o.market_key in ('team_shots','team_shots_on_target')
    and o.selection_key ~ '^[0-9]+_[0-9]+_(over|under)_(?:team_)?[12]$'
),
team_merge as (
  update public.odds_outcomes o
  set price_decimal = p.price_decimal,
      price_american = p.price_american,
      last_updated_at = p.last_updated_at
  from parsed_team p
  where o.fixture_id = p.fixture_id
    and o.bookmaker_id = p.bookmaker_id
    and o.market_key = p.market_key
    and o.selection_key = p.new_sel
    and o.line = p.new_line
    and (
      o.last_updated_at is null
      or (p.last_updated_at is not null and p.last_updated_at > o.last_updated_at)
    )
  returning 1
)
delete from public.odds_outcomes o
using parsed_team p
where o.ctid = p.ctid
  and exists (
    select 1
    from public.odds_outcomes o2
    where o2.fixture_id = p.fixture_id
      and o2.bookmaker_id = p.bookmaker_id
      and o2.market_key = p.market_key
      and o2.selection_key = p.new_sel
      and o2.line = p.new_line
  );
"""
    team_update_sql = f"""
{fixture_window_sql},
parsed_team as (
  select
    o.ctid as ctid,
    regexp_replace(o.selection_key, '^(over|under)_([0-9]+)_([0-9]+)_(?:team_)?(1|2)$', '\\1') as new_sel,
    regexp_replace(o.selection_key, '^(over|under)_([0-9]+)_([0-9]+)_(?:team_)?(1|2)$', '\\2.\\3')::numeric as new_line,
    case
      when regexp_replace(o.selection_key, '^.*_(?:team_)?(1|2)$', '\\1') = '1' then fw.home_team_id
      else fw.away_team_id
    end as new_participant_id
  from public.odds_outcomes o
  join fixture_window fw on fw.id = o.fixture_id
  where o.market_key in ('team_shots','team_shots_on_target')
    and o.selection_key ~ '^(over|under)_[0-9]+_[0-9]+_(?:team_)?[12]$'
  union all
  select
    o.ctid as ctid,
    regexp_replace(o.selection_key, '^([0-9]+)_([0-9]+)_(over|under)_(?:team_)?(1|2)$', '\\3') as new_sel,
    regexp_replace(o.selection_key, '^([0-9]+)_([0-9]+)_(over|under)_(?:team_)?(1|2)$', '\\1.\\2')::numeric as new_line,
    case
      when regexp_replace(o.selection_key, '^.*_(?:team_)?(1|2)$', '\\1') = '1' then fw.home_team_id
      else fw.away_team_id
    end as new_participant_id
  from public.odds_outcomes o
  join fixture_window fw on fw.id = o.fixture_id
  where o.market_key in ('team_shots','team_shots_on_target')
    and o.selection_key ~ '^[0-9]+_[0-9]+_(over|under)_(?:team_)?[12]$'
)
update public.odds_outcomes o
set line = p.new_line,
    selection_key = p.new_sel,
    participant_type = 'team',
    participant_id = p.new_participant_id
from parsed_team p
where o.ctid = p.ctid
  and p.new_participant_id is not null;
"""
    cleanup_match_sql = f"""
{fixture_window_sql}
delete from public.odds_outcomes o
using fixture_window fw
where o.fixture_id = fw.id
  and o.market_key in ('match_shots','match_shots_on_target')
  and o.line is null
  and o.selection_key in ('over','under');
"""
    cleanup_team_sql = f"""
{fixture_window_sql}
delete from public.odds_outcomes o
using fixture_window fw
where o.fixture_id = fw.id
  and o.market_key in ('team_shots','team_shots_on_target')
  and o.line in (1,2)
  and o.selection_key in ('over','under');
"""
    sql_lines = [
        "\\set ON_ERROR_STOP on",
        f"set statement_timeout = '{statement_timeout}';",
        f"set lock_timeout = '{lock_timeout}';",
        f"set idle_in_transaction_session_timeout = '{idle_tx_timeout}';",
        "begin;",
    ]
    if use_advisory_lock:
        sql_lines.append(f"select pg_advisory_xact_lock({advisory_lock_key});")
    sql_lines += [
        "create temp table odds_outcomes_stage",
        "  (like public.odds_outcomes including defaults) on commit drop;",
        copy_line,
        "",
        "select 'stage_count', count(*)::bigint from odds_outcomes_stage;",
        "",
        """
with src as (
  select distinct on (fixture_id, bookmaker_id, market_key, selection_key, line)
    fixture_id, bookmaker_id, market_key, selection_key, line,
    price_decimal, price_american, participant_type, participant_id, last_updated_at
  from odds_outcomes_stage
  order by fixture_id, bookmaker_id, market_key, selection_key, line,
           last_updated_at desc nulls last
)
insert into public.odds_outcomes as o (
  fixture_id, bookmaker_id, market_key, selection_key, line,
  price_decimal, price_american, participant_type, participant_id, last_updated_at
)
select
  fixture_id, bookmaker_id, market_key, selection_key, line,
  price_decimal, price_american, participant_type, participant_id, last_updated_at
from src
on conflict (fixture_id, bookmaker_id, market_key, selection_key, line)
do update set
  price_decimal = excluded.price_decimal,
  price_american = excluded.price_american,
  participant_type = coalesce(excluded.participant_type, o.participant_type),
  participant_id = coalesce(excluded.participant_id, o.participant_id),
  last_updated_at = coalesce(excluded.last_updated_at, o.last_updated_at)
where
  o.price_decimal is distinct from excluded.price_decimal
  or o.price_american is distinct from excluded.price_american
  or o.participant_type is distinct from coalesce(excluded.participant_type, o.participant_type)
  or o.participant_id is distinct from coalesce(excluded.participant_id, o.participant_id)
  or o.last_updated_at is distinct from coalesce(excluded.last_updated_at, o.last_updated_at);
""",
        match_delete_sql,
        match_update_sql,
        team_delete_sql,
        team_update_sql,
        cleanup_match_sql,
        cleanup_team_sql,
        "commit;",
    ]
    sql = "\n".join(sql_lines)
    sql_path: Optional[str] = None
    if keep_sql or os.environ.get("KEEP_SQL") == "1":
        sql_path = f"/tmp/odds_psql_sql_{league_label}.sql"
        Path(sql_path).write_text(sql, encoding="utf-8")
        print(f"psql sql path: {sql_path}", flush=True)
    else:
        with tempfile.NamedTemporaryFile("w", delete=False, suffix=".sql") as f:
            f.write(sql)
            sql_path = f.name

    guard_pattern = re.compile(r"::bigint\\s+filter", re.IGNORECASE)
    sql_text = Path(sql_path).read_text(encoding="utf-8")
    guard_match = guard_pattern.search(sql_text)
    if guard_match:
        lines = sql_text.splitlines()
        line_no = sql_text[: guard_match.start()].count("\n") + 1
        start = max(1, line_no - 3)
        end = min(len(lines), line_no + 3)
        context = "\n".join(
            f"{idx + 1:4d}: {lines[idx]}"
            for idx in range(start - 1, end)
        )
        message = (
            "SQL guard failed: detected forbidden '::bigint filter' pattern.\n"
            f"Context:\n{context}\n"
        )
        print(message, flush=True)
        append_output(err_path, "sql_guard", message)
        raise SystemExit("SQL guard failed; refusing to run psql.")

    lines = sql_text.splitlines()
    snippet_lines = []
    for idx, line in enumerate(lines, start=1):
        if "select 'inserted'" in line or "select 'updated'" in line:
            start = max(1, idx - 2)
            end = min(len(lines), idx + 2)
            snippet_lines = [
                f"{line_no:4d}: {lines[line_no - 1]}" for line_no in range(start, end + 1)
            ]
            break
    if snippet_lines:
        snippet = "\n".join(snippet_lines)
        print("SQL snippet:\n" + snippet, flush=True)
        append_output(out_path, "sql_snippet", snippet)

    output = run_psql(
        db_url,
        sql_path,
        from_file=True,
        label="stage_upsert",
        err_path=err_path,
        out_path=out_path,
    )
    if not keep_sql and os.environ.get("KEEP_SQL") != "1":
        try:
            os.unlink(sql_path)
        except OSError:
            pass

    counts = {"stage_count": 0, "src_count": 0, "upserted_total": 0, "inserted": 0, "updated": 0}
    for line in output.splitlines():
        parts = line.split("\t")
        if len(parts) >= 2 and parts[0] in counts:
            try:
                counts[parts[0]] = int(parts[1])
            except ValueError:
                counts[parts[0]] = 0
    if counts["src_count"] == 0 and counts["stage_count"] > 0:
        counts["src_count"] = counts["stage_count"]
    counts["unchanged"] = max(0, counts["src_count"] - counts["upserted_total"])
    return counts


def coverage_query(days_forward: int, league_ids: List[int]) -> str:
    league_filter = ""
    if league_ids:
        league_filter = f"league_id = any({sql_array(league_ids)}) and"
    return f"""
with fixtures_in_range as (
  select id
  from public.fixtures
  where {league_filter}
    starting_at >= (now() at time zone 'utc')
    and starting_at < (now() at time zone 'utc') + interval '{days_forward} days'
), scoped as (
  select o.participant_id
  from public.odds_outcomes o
  join fixtures_in_range f on f.id = o.fixture_id
  where o.participant_type = 'player'
)
select
  coalesce(count(*), 0)::bigint as total,
  coalesce(count(*) filter (where participant_id is not null), 0)::bigint as mapped,
  coalesce(round(100.0 * count(*) filter (where participant_id is not null) / nullif(count(*),0), 2), 0) as mapped_pct
from scoped;
"""


def coverage_baseline_query(days_back: int, league_ids: List[int]) -> str:
    league_filter = ""
    if league_ids:
        league_filter = f"f.league_id = any({sql_array(league_ids)}) and"
    return f"""
with days as (
  select generate_series(current_date - interval '{days_back} days', current_date, interval '1 day')::date as day
), counts as (
  select
    d.day,
    coalesce((
      select count(*)
      from public.odds_outcomes o
      join public.fixtures f on f.id = o.fixture_id
      where {league_filter}
        o.participant_type = 'player'
        and (coalesce(o.last_updated_at, now()) at time zone 'Europe/London')::date = d.day
    ), 0) as total,
    coalesce((
      select count(*)
      from public.odds_outcomes o
      join public.fixtures f on f.id = o.fixture_id
      where {league_filter}
        o.participant_type = 'player'
        and o.participant_id is not null
        and (coalesce(o.last_updated_at, now()) at time zone 'Europe/London')::date = d.day
    ), 0) as mapped
  from days d
)
select coalesce(
  percentile_cont(0.5) within group (
    order by case when c.total = 0 then 0 else 100.0 * c.mapped / c.total end
  ),
  0
) as median_mapped_pct
from counts c;
"""


def verification_queries(days_forward: int) -> List[str]:
    queries = []
    queries.append(
        f"""
select
  count(*) as total,
  count(*) filter (where participant_type='player' and participant_id is not null) as mapped_players
from public.odds_outcomes o
join public.fixtures f on f.id=o.fixture_id
where f.starting_at >= (now() at time zone 'utc')
  and f.starting_at < (now() at time zone 'utc') + interval '{days_forward} days';
"""
    )
    queries.append(
        f"""
select market_key, line,
       count(distinct participant_id) filter (where participant_id is not null) as distinct_players
from public.odds_outcomes o
join public.fixtures f on f.id=o.fixture_id
where market_key in ('player_shots','player_shots_on_target')
  and f.starting_at >= (now() at time zone 'utc')
  and f.starting_at < (now() at time zone 'utc') + interval '{days_forward} days'
group by market_key, line
order by market_key, distinct_players desc
limit 20;
"""
    )
    return queries


def retention_cleanup_query(days_back: int, days_forward: int) -> str:
    return f"""
with deleted as (
  delete from public.odds_outcomes o
  using public.fixtures f
  where f.id = o.fixture_id
    and (
      f.starting_at < (now() at time zone 'utc') - interval '{days_back} days'
      or f.starting_at >= (now() at time zone 'utc') + interval '{days_forward} days'
    )
  returning 1
)
select count(*)::bigint from deleted;
"""


def retention_snapshots_query(days_back: int, days_forward: int, max_age_days: int) -> str:
    return f"""
with deleted as (
  delete from public.odds_snapshots s
  where s.pulled_at < (now() at time zone 'utc') - interval '{max_age_days} days'
     or exists (
       select 1
       from public.fixtures f
       where f.id = s.fixture_id
         and (
           f.starting_at < (now() at time zone 'utc') - interval '{days_back} days'
           or f.starting_at >= (now() at time zone 'utc') + interval '{days_forward} days'
         )
     )
  returning 1
)
select count(*)::bigint from deleted;
"""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--leagues", default="8,384", help="Comma-separated league IDs")
    parser.add_argument("--days-forward", type=int, default=14)
    parser.add_argument("--db", default=DB_PATH)
    parser.add_argument("--csv-out", default="/tmp/odds_outcomes_export.csv")
    parser.add_argument("--report-out", default="/tmp/odds_ingest_report.json")
    parser.add_argument("--progress-rows", type=int, default=10000)
    parser.add_argument("--progress-fixtures", type=int, default=100)
    parser.add_argument("--max-runtime-minutes", type=int, default=25)
    parser.add_argument("--keep-sql", action="store_true", help="Keep generated SQL file for debugging")
    parser.add_argument("--skip-coverage", action="store_true", help="Skip coverage queries after ingest")
    parser.add_argument("--skip-verification", action="store_true", help="Skip verification queries after ingest")
    parser.add_argument("--skip-retention", action="store_true", help="Skip retention cleanup after ingest")
    parser.add_argument("--retention-days-back", type=int, default=2)
    parser.add_argument("--retention-days-forward", type=int, default=14)
    parser.add_argument("--skip-retention-snapshots", action="store_true")
    parser.add_argument("--retention-snapshots-days", type=int, default=30)
    parser.add_argument(
        "--no-include-fixture-leagues",
        dest="include_fixture_leagues",
        action="store_false",
        help="Only use --leagues list (default includes leagues from upcoming fixtures).",
    )
    parser.set_defaults(include_fixture_leagues=True)
    args = parser.parse_args()

    if not DB_URL:
        raise SystemExit("Missing SUPABASE_DB_URL")
    if not os.path.exists(args.db):
        raise SystemExit(f"SQLite DB not found at {args.db}")

    start_time = time.time()
    start_iso = datetime.utcnow().isoformat() + "Z"
    max_runtime_seconds = max(0, int(args.max_runtime_minutes) * 60)

    conn = sqlite3.connect(args.db)
    league_ids = parse_league_ids(args.leagues)
    fixture_league_ids = fetch_fixture_league_ids(conn, args.days_forward) if args.include_fixture_leagues else []
    effective_leagues = sorted({*league_ids, *fixture_league_ids})

    fixture_count = 0
    if effective_leagues:
        placeholders = ",".join("?" for _ in effective_leagues)
        start_dt, end_dt = sqlite_window_bounds(args.days_forward)
        fixture_count = conn.execute(
            f"""
            select count(*)
            from fixtures
            where datetime(starting_at) >= ? and datetime(starting_at) < ?
              and league_id in ({placeholders})
            """,
            [start_dt, end_dt, *effective_leagues],
        ).fetchone()[0]

    total_rows_estimate = 0
    if effective_leagues:
        placeholders = ",".join("?" for _ in effective_leagues)
        start_dt, end_dt = sqlite_window_bounds(args.days_forward)
        total_rows_estimate = conn.execute(
            f"""
            select count(*)
            from odds_outcomes o
            join fixtures f on f.id = o.fixture_id
            where datetime(f.starting_at) >= ? and datetime(f.starting_at) < ?
              and f.league_id in ({placeholders})
            """,
            [start_dt, end_dt, *effective_leagues],
        ).fetchone()[0]

    print(
        f"Exporting odds_outcomes fixtures={fixture_count} leagues={len(effective_leagues)} window_days={args.days_forward}",
        flush=True,
    )

    total_rows, partial_run, last_fixture_id = build_outcomes_csv(
        conn,
        effective_leagues,
        args.days_forward,
        Path(args.csv_out),
        args.progress_rows,
        args.progress_fixtures,
        total_rows_estimate,
        fixture_count,
        max_runtime_seconds,
    )
    conn.close()

    print(f"CSV build complete rows={total_rows} partial_run={partial_run}", flush=True)
    if partial_run:
        print(
            f"Runtime guard hit (>{args.max_runtime_minutes} min). "
            f"Last fixture_id exported: {last_fixture_id}",
            flush=True,
        )

    league_label = str(effective_leagues[0]) if len(effective_leagues) == 1 else "multi"
    err_path = f"/tmp/psql_err_{league_label}.txt"
    out_path = f"/tmp/psql_out_{league_label}.txt"
    Path(err_path).touch(exist_ok=True)
    Path(out_path).touch(exist_ok=True)
    csv_head_path = Path(f"/tmp/odds_outcomes_head_{league_label}.csv")
    csv_summary_path = Path(f"/tmp/odds_outcomes_summary_{league_label}.json")
    csv_summary_data: Optional[Dict[str, object]] = None
    try:
        write_csv_head(Path(args.csv_out), csv_head_path, max_lines=200)
        write_csv_summary(Path(args.csv_out), csv_summary_path, top_n=50)
        print(f"Wrote CSV head to {csv_head_path}", flush=True)
        print(f"Wrote CSV summary to {csv_summary_path}", flush=True)
        csv_summary_data = json.loads(Path(csv_summary_path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        print(f"Failed to write CSV proof artifacts: {exc}", flush=True)

    counts = {"stage_count": 0, "src_count": 0, "upserted_total": 0, "inserted": 0, "updated": 0, "unchanged": 0}
    ingest_ok = True
    error_stage: Optional[str] = None
    error_message: Optional[str] = None
    psql_err_tail = ""
    psql_out_tail = ""

    try:
        counts = stage_and_upsert(
            DB_URL,
            Path(args.csv_out),
            league_label,
            effective_leagues,
            args.days_forward,
            args.keep_sql,
            err_path,
            out_path,
        )
        print(
            f"Stage count={counts['stage_count']} src_count={counts['src_count']} inserted={counts['inserted']} "
            f"updated={counts['updated']} unchanged={counts['unchanged']}",
            flush=True,
        )
    except Exception as exc:
        ingest_ok = False
        error_stage = "stage_upsert"
        error_message = str(exc)
        psql_err_tail = tail_text(err_path)
        psql_out_tail = tail_text(out_path)
        print(f"Stage upsert failed: {error_message}", flush=True)

    retention_deleted = 0
    if ingest_ok and not args.skip_retention:
        retention_sql = retention_cleanup_query(
            args.retention_days_back,
            args.retention_days_forward,
        )
        try:
            retention_out = run_psql(
                DB_URL,
                retention_sql,
                label="retention_cleanup",
                err_path=err_path,
                out_path=out_path,
            )
            retention_deleted = int(retention_out.strip()) if retention_out else 0
        except subprocess.CalledProcessError as exc:
            print(f"retention cleanup failed; continuing: {exc}", flush=True)
        except ValueError:
            retention_deleted = 0

    snapshots_deleted = 0
    if ingest_ok and not args.skip_retention_snapshots:
        snapshots_sql = retention_snapshots_query(
            args.retention_days_back,
            args.retention_days_forward,
            args.retention_snapshots_days,
        )
        try:
            snapshots_out = run_psql(
                DB_URL,
                snapshots_sql,
                label="retention_snapshots",
                err_path=err_path,
                out_path=out_path,
            )
            snapshots_deleted = int(snapshots_out.strip()) if snapshots_out else 0
        except subprocess.CalledProcessError as exc:
            print(f"retention snapshots failed; continuing: {exc}", flush=True)
        except ValueError:
            snapshots_deleted = 0

    coverage_total = 0
    coverage_mapped = 0
    coverage_pct = 0.0
    if ingest_ok and not args.skip_coverage:
        coverage_sql = coverage_query(args.days_forward, effective_leagues)
        try:
            coverage_out = run_psql(
                DB_URL,
                coverage_sql,
                label="coverage",
                err_path=err_path,
                out_path=out_path,
            )
            coverage_parts = coverage_out.split("\t") if coverage_out else ["0", "0", "0"]
            try:
                coverage_total = int(coverage_parts[0])
            except ValueError:
                coverage_total = 0
            try:
                coverage_mapped = int(coverage_parts[1])
            except ValueError:
                coverage_mapped = 0
            try:
                coverage_pct = float(coverage_parts[2])
            except ValueError:
                coverage_pct = 0.0
        except subprocess.CalledProcessError as exc:
            print(f"coverage failed; continuing: {exc}", flush=True)

    coverage_baseline_pct = 0.0
    if ingest_ok and not args.skip_coverage:
        baseline_sql = coverage_baseline_query(6, effective_leagues)
        try:
            baseline_out = run_psql(
                DB_URL,
                baseline_sql,
                label="coverage_baseline",
                err_path=err_path,
                out_path=out_path,
            )
            coverage_baseline_pct = float(baseline_out.strip()) if baseline_out else 0.0
        except subprocess.CalledProcessError as exc:
            print(f"coverage_baseline failed; continuing: {exc}", flush=True)
        except ValueError:
            coverage_baseline_pct = 0.0

    print(
        f"Coverage total={coverage_total} mapped={coverage_mapped} "
        f"pct={coverage_pct} baseline_pct={coverage_baseline_pct}",
        flush=True,
    )

    verification_outputs: List[str] = []
    if ingest_ok and not args.skip_verification:
        for idx, query in enumerate(verification_queries(args.days_forward), start=1):
            print(f"Verification query {idx} output:", flush=True)
            try:
                out = run_psql(
                    DB_URL,
                    query,
                    label=f"verification_{idx}",
                    err_path=err_path,
                    out_path=out_path,
                )
                verification_outputs.append(out)
                print(out, flush=True)
            except subprocess.CalledProcessError as exc:
                print(f"verification {idx} failed; continuing: {exc}", flush=True)
                verification_outputs.append("")

    end_time = time.time()
    end_iso = datetime.utcnow().isoformat() + "Z"

    report = {
        "ok": ingest_ok,
        "error_stage": error_stage,
        "error_message": error_message,
        "psql_err_tail": psql_err_tail,
        "psql_out_tail": psql_out_tail,
        "csv_summary": csv_summary_data,
        "start_time": start_iso,
        "end_time": end_iso,
        "runtime_seconds": round(end_time - start_time, 2),
        "window_days": args.days_forward,
        "fixture_count": fixture_count,
        "league_ids": effective_leagues,
        "league_id": effective_leagues[0] if len(effective_leagues) == 1 else None,
        "sqlite_rows_exported": total_rows,
        "rows_exported_csv": total_rows,
        "copy_rows": counts.get("stage_count", 0),
        "rows_copied": counts.get("stage_count", 0),
        "inserted_rows": counts.get("inserted", 0),
        "rows_inserted": counts.get("inserted", 0),
        "updated_rows": counts.get("updated", 0),
        "rows_updated": counts.get("updated", 0),
        "unchanged_rows": counts.get("unchanged", 0),
        "rows_unchanged_skipped": counts.get("unchanged", 0),
        "partial_run": partial_run,
        "last_fixture_id": last_fixture_id,
        "psql_ok": os.environ.get("PSQL_OK"),
        "coverage": {
            "total": coverage_total,
            "mapped": coverage_mapped,
            "mapped_pct": coverage_pct,
            "baseline_pct": coverage_baseline_pct,
        },
        "retention": {
            "days_back": args.retention_days_back,
            "days_forward": args.retention_days_forward,
            "deleted_rows": retention_deleted,
        },
        "snapshots_retention": {
            "max_age_days": args.retention_snapshots_days,
            "deleted_rows": snapshots_deleted,
        },
        "csv_head_path": str(csv_head_path),
        "csv_summary_path": str(csv_summary_path),
        "psql_err_path": err_path,
        "psql_out_path": out_path,
        "rest_ok": os.environ.get("REST_OK"),
        "rest_http": os.environ.get("REST_HTTP"),
        "verification": verification_outputs,
    }

    Path(args.report_out).write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(
        "Ingest summary: rows_exported="
        f"{total_rows} inserted={counts.get('inserted', 0)} "
        f"updated={counts.get('updated', 0)} unchanged={counts.get('unchanged', 0)} "
        f"runtime_sec={report['runtime_seconds']}",
        flush=True,
    )

    if not ingest_ok:
        error_path = f"/tmp/odds_ingest_error_{league_label}.txt"
        error_payload = [
            f"stage={error_stage or 'unknown'}",
            f"error={error_message or 'unknown'}",
        ]
        if psql_err_tail:
            error_payload.append("psql_err_tail:")
            error_payload.append(psql_err_tail)
        if psql_out_tail:
            error_payload.append("psql_out_tail:")
            error_payload.append(psql_out_tail)
        Path(error_path).write_text("\n".join(error_payload), encoding="utf-8")
        raise SystemExit("odds ingest failed")


if __name__ == "__main__":
    main()
