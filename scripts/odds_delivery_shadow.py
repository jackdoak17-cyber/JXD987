#!/usr/bin/env python3
"""Read-only equivalence telemetry for odds delivery.

The production exporter remains the authoritative writer. This module loads the
same natural CSV into temporary tables, compares it with the current target,
and rolls the transaction back without touching persistent data.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


ODDS_COLUMNS = (
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
)
KEY_COLUMNS = ODDS_COLUMNS[:5]
MUTABLE_COLUMNS = ODDS_COLUMNS[5:]


def _line_key(value: object) -> str:
    return "-9999" if value in (None, "") else str(value)


def canonical_key(row: Mapping[str, object]) -> Tuple[str, ...]:
    return (
        str(row.get("fixture_id")),
        str(row.get("bookmaker_id")),
        str(row.get("market_key")),
        str(row.get("selection_key")),
        _line_key(row.get("line")),
    )


def scope_key(row: Mapping[str, object]) -> Tuple[str, str, str]:
    return (
        str(row.get("fixture_id")),
        str(row.get("bookmaker_id")),
        str(row.get("market_key")),
    )


def canonicalize_stage_rows(
    rows: Iterable[Mapping[str, object]],
    min_price: float = 1.0,
    max_price: float = 500.0,
) -> List[Dict[str, object]]:
    """Mirror the exporter's price filter and latest-row deduplication."""
    latest: Dict[Tuple[str, ...], Dict[str, object]] = {}
    for raw in rows:
        row = {column: raw.get(column) for column in ODDS_COLUMNS}
        price = row.get("price_decimal")
        if price not in (None, "") and not (min_price < float(price) <= max_price):
            continue
        key = canonical_key(row)
        previous = latest.get(key)
        current_ts = str(row.get("last_updated_at") or "")
        previous_ts = str(previous.get("last_updated_at") or "") if previous else ""
        if previous is None or current_ts > previous_ts:
            latest[key] = row
    return list(latest.values())


def simulate_current_replacement(
    target_rows: Sequence[Mapping[str, object]],
    raw_stage_rows: Sequence[Mapping[str, object]],
    min_price: float = 1.0,
    max_price: float = 500.0,
) -> List[Dict[str, object]]:
    """Model current touched-scope delete followed by canonical stage insert."""
    touched = {scope_key(row) for row in raw_stage_rows}
    untouched = [dict(row) for row in target_rows if scope_key(row) not in touched]
    return untouched + canonicalize_stage_rows(raw_stage_rows, min_price, max_price)


def simulate_incremental_diff(
    target_rows: Sequence[Mapping[str, object]],
    raw_stage_rows: Sequence[Mapping[str, object]],
    min_price: float = 1.0,
    max_price: float = 500.0,
) -> List[Dict[str, object]]:
    """Model exact diff semantics, including removals from raw touched scopes."""
    touched = {scope_key(row) for row in raw_stage_rows}
    source = {
        canonical_key(row): dict(row)
        for row in canonicalize_stage_rows(raw_stage_rows, min_price, max_price)
    }
    result = {
        canonical_key(row): dict(row)
        for row in target_rows
        if scope_key(row) not in touched
    }
    # Exact equivalence requires NULLs and timestamps to replace target values,
    # rather than the legacy coalescing behavior of the unused upsert helper.
    result.update(source)
    return list(result.values())


def normalized_dataset(rows: Iterable[Mapping[str, object]]) -> List[Tuple[object, ...]]:
    return sorted(tuple(row.get(column) for column in ODDS_COLUMNS) for row in rows)


def build_target_snapshot_sql() -> str:
    """Read only the target rows belonging to raw touched scopes."""
    return """
select
  o.fixture_id, o.bookmaker_id, o.market_key, o.selection_key, o.line,
  o.price_decimal, o.price_american, o.participant_type, o.participant_id,
  o.last_updated_at
from public.odds_outcomes o
join odds_outcomes_shadow_scopes s
  on s.fixture_id = o.fixture_id
 and s.bookmaker_id = o.bookmaker_id
 and s.market_key = o.market_key;
"""


def _optional_int(value: object) -> Optional[int]:
    return None if value in (None, "") else int(value)


def _optional_decimal(value: object) -> Optional[Decimal]:
    return None if value in (None, "") else Decimal(str(value)).normalize()


def _optional_timestamp(value: object) -> Optional[str]:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value).strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError:
            return raw
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()


def normalize_row(row: Mapping[str, object]) -> Dict[str, object]:
    return {
        "fixture_id": int(row["fixture_id"]),
        "bookmaker_id": int(row["bookmaker_id"]),
        "market_key": str(row["market_key"]),
        "selection_key": str(row["selection_key"]),
        "line": _optional_decimal(row.get("line")),
        "price_decimal": _optional_decimal(row.get("price_decimal")),
        "price_american": _optional_int(row.get("price_american")),
        "participant_type": None if row.get("participant_type") in (None, "") else str(row["participant_type"]),
        "participant_id": _optional_int(row.get("participant_id")),
        "last_updated_at": _optional_timestamp(row.get("last_updated_at")),
    }


def _row_values(row: Mapping[str, object]) -> Tuple[object, ...]:
    return tuple(row.get(column) for column in ODDS_COLUMNS)


def _json_value(value: object) -> object:
    if isinstance(value, Decimal):
        return format(value, "f")
    return value


def _group_hashes(
    rows: Mapping[Tuple[object, ...], Mapping[str, object]],
    group_columns: Sequence[str],
) -> Dict[Tuple[object, ...], str]:
    grouped: Dict[Tuple[object, ...], List[Tuple[object, ...]]] = {}
    for row in rows.values():
        group = tuple(row[column] for column in group_columns)
        grouped.setdefault(group, []).append(_row_values(row))
    hashes: Dict[Tuple[object, ...], str] = {}
    for group, values in grouped.items():
        payload = json.dumps(
            [[_json_value(value) for value in row] for row in sorted(values, key=str)],
            separators=(",", ":"),
            sort_keys=False,
        )
        hashes[group] = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return hashes


def _different_hash_count(left: Mapping[object, str], right: Mapping[object, str]) -> int:
    return sum(1 for key in set(left) | set(right) if left.get(key) != right.get(key))


def compare_canonical_maps(
    stage_map: Mapping[Tuple[object, ...], Mapping[str, object]],
    target_map: Mapping[Tuple[object, ...], Mapping[str, object]],
    raw_count: int,
    invalid_count: int,
    touched_scope_count: int,
) -> Dict[str, object]:
    """Classify field changes and independently apply the proposed diff."""
    stage_keys = set(stage_map)
    target_keys = set(target_map)
    common_keys = stage_keys & target_keys
    new_keys = stage_keys - target_keys
    removed_keys = target_keys - stage_keys
    price_changes = set()
    participant_changes = set()
    timestamp_changes = set()
    unchanged = set()
    for key in common_keys:
        source = stage_map[key]
        target = target_map[key]
        if (source["price_decimal"], source["price_american"]) != (
            target["price_decimal"], target["price_american"]
        ):
            price_changes.add(key)
        if (source["participant_type"], source["participant_id"]) != (
            target["participant_type"], target["participant_id"]
        ):
            participant_changes.add(key)
        if source["last_updated_at"] != target["last_updated_at"]:
            timestamp_changes.add(key)
        if _row_values(source) == _row_values(target):
            unchanged.add(key)
    updated_keys = price_changes | participant_changes | timestamp_changes
    timestamp_only = timestamp_changes - price_changes - participant_changes
    participant_only = participant_changes - price_changes

    current_result = dict(stage_map)
    proposed_result = dict(target_map)
    for key in removed_keys:
        proposed_result.pop(key, None)
    for key in new_keys | updated_keys:
        proposed_result[key] = stage_map[key]
    differing_keys = {
        key
        for key in set(current_result) | set(proposed_result)
        if current_result.get(key) != proposed_result.get(key)
    }
    current_fixture_hashes = _group_hashes(current_result, ("fixture_id",))
    proposed_fixture_hashes = _group_hashes(proposed_result, ("fixture_id",))
    current_scope_hashes = _group_hashes(
        current_result, ("fixture_id", "bookmaker_id", "market_key")
    )
    proposed_scope_hashes = _group_hashes(
        proposed_result, ("fixture_id", "bookmaker_id", "market_key")
    )
    metrics = {
        "stage_rows_raw": raw_count,
        "filtered_invalid_price_rows": invalid_count,
        "duplicate_stage_rows": max(0, raw_count - invalid_count - len(stage_map)),
        "touched_scopes": touched_scope_count,
        "staged_canonical_rows": len(stage_map),
        "target_rows_touched": len(target_map),
        "new_keys": len(new_keys),
        "removed_keys": len(removed_keys),
        "price_changes": len(price_changes),
        "participant_mapping_changes": len(participant_changes),
        "last_updated_at_changes": len(timestamp_changes),
        "timestamp_only_changes": len(timestamp_only),
        "participant_only_changes": len(participant_only),
        "other_mutable_field_changes": 0,
        "completely_unchanged": len(unchanged),
        "updated_keys": len(updated_keys),
        "predicted_current_persistent_writes": len(target_map) + len(stage_map),
        "predicted_diff_persistent_writes": len(new_keys) + len(removed_keys) + len(updated_keys),
    }
    return {
        "metrics": metrics,
        "differing_canonical_rows": len(differing_keys),
        "differing_fixture_hashes": _different_hash_count(
            current_fixture_hashes, proposed_fixture_hashes
        ),
        "differing_scope_hashes": _different_hash_count(
            current_scope_hashes, proposed_scope_hashes
        ),
    }


def run_shadow_validation(
    db_url: str,
    csv_path: Path,
    partial_run: bool,
    min_price: float = 1.0,
    max_price: float = 500.0,
) -> Dict[str, object]:
    """Run bounded read-only shadow analysis and return JSON-safe telemetry."""
    import psycopg2

    started = time.monotonic()
    started_at = datetime.now(timezone.utc).isoformat()
    timeout = os.environ.get("ODDS_SHADOW_STATEMENT_TIMEOUT", "45000")
    lock_timeout = os.environ.get("ODDS_SHADOW_LOCK_TIMEOUT", "15000")
    connect_timeout = int(os.environ.get("ODDS_SHADOW_CONNECT_TIMEOUT", "8"))
    advisory_lock_key = os.environ.get("ODDS_ADVISORY_LOCK_KEY", "982374")
    use_lock = os.environ.get("ODDS_USE_ADVISORY_LOCK", "").lower() in {"1", "true", "yes"}
    digest = hashlib.sha256()
    with csv_path.open("rb") as stage_file:
        for chunk in iter(lambda: stage_file.read(1024 * 1024), b""):
            digest.update(chunk)
    result: Dict[str, object] = {
        "ok": False,
        "partial_run": bool(partial_run),
        "started_at": started_at,
        "stage_sha256": digest.hexdigest(),
    }
    raw_count = 0
    invalid_count = 0
    touched_scopes = set()
    stage_map: Dict[Tuple[object, ...], Dict[str, object]] = {}
    with csv_path.open("r", encoding="utf-8", newline="") as stage_file:
        for raw in csv.DictReader(stage_file):
            raw_count += 1
            normalized = normalize_row(raw)
            touched_scopes.add(scope_key(normalized))
            price = normalized["price_decimal"]
            if price is not None and not (Decimal(str(min_price)) < price <= Decimal(str(max_price))):
                invalid_count += 1
                continue
            key = canonical_key(normalized)
            previous = stage_map.get(key)
            if previous is None or (normalized["last_updated_at"] or "") > (previous["last_updated_at"] or ""):
                stage_map[key] = normalized

    conn = psycopg2.connect(
        db_url,
        connect_timeout=connect_timeout,
        application_name="odds_delivery_shadow",
    )
    try:
        conn.autocommit = False
        cur = conn.cursor()
        cur.execute(f"set statement_timeout = '{timeout}';")
        cur.execute(f"set lock_timeout = '{lock_timeout}';")
        if use_lock:
            cur.execute(f"select pg_advisory_xact_lock({advisory_lock_key});")
        cur.execute(
            "create temp table odds_outcomes_shadow_scopes ("
            "fixture_id bigint not null, bookmaker_id bigint not null, market_key text not null, "
            "primary key (fixture_id, bookmaker_id, market_key)) on commit drop;"
        )
        scope_csv = io.StringIO()
        scope_writer = csv.writer(scope_csv, lineterminator="\n")
        scope_writer.writerows(sorted(touched_scopes))
        scope_csv.seek(0)
        cur.copy_expert(
            "COPY odds_outcomes_shadow_scopes (fixture_id, bookmaker_id, market_key) "
            "FROM STDIN WITH (FORMAT csv)",
            scope_csv,
        )
        cur.execute("analyze odds_outcomes_shadow_scopes;")

        target_map: Dict[Tuple[object, ...], Dict[str, object]] = {}
        target_cur = conn.cursor(name="odds_shadow_target")
        target_cur.itersize = 10000
        target_cur.execute(build_target_snapshot_sql())
        for values in target_cur:
            normalized = normalize_row(dict(zip(ODDS_COLUMNS, values)))
            target_map[canonical_key(normalized)] = normalized
        target_cur.close()

        comparison = compare_canonical_maps(
            stage_map,
            target_map,
            raw_count=raw_count,
            invalid_count=invalid_count,
            touched_scope_count=len(touched_scopes),
        )
        result.update(
            {
                "ok": True,
                **comparison,
            }
        )
        return result
    finally:
        conn.rollback()
        conn.close()
        result["runtime_seconds"] = round(time.monotonic() - started, 3)


def append_shadow_log(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")
