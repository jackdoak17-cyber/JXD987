#!/usr/bin/env python3
"""Publish exact provider quote-observation evidence after canonical export."""

from __future__ import annotations

import argparse
import gzip
import json
import os
import re
import socket
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

SCHEMA_VERSION = 1
SOURCE = "odds_api_provider_response"
RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{5,179}$")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def load_document(path: Path) -> Dict[str, Any]:
    if ".json.gz" in path.name:
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            return json.load(handle)
    return json.loads(path.read_text(encoding="utf-8"))


def validate_document(document: Dict[str, Any]) -> List[Dict[str, Any]]:
    if (
        document.get("schema_version") != SCHEMA_VERSION
        or document.get("source") != SOURCE
        or not isinstance(document.get("run_id"), str)
        or not RUN_ID_RE.fullmatch(document["run_id"])
        or not isinstance(document.get("quotes"), list)
    ):
        raise ValueError("invalid provider-observation document")
    now = utc_now()
    identities = set()
    quotes: List[Dict[str, Any]] = []
    for index, quote in enumerate(document["quotes"]):
        try:
            observed = datetime.fromisoformat(str(quote["observed_at"]).replace("Z", "+00:00"))
            if observed.tzinfo is None:
                observed = observed.replace(tzinfo=timezone.utc)
            fixture_id = int(quote["fixture_id"])
            bookmaker_id = int(quote["bookmaker_id"])
            price = float(quote["price_decimal"])
            market_key = str(quote["market_key"])
            selection_key = str(quote["selection_key"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"malformed quote observation at index {index}") from exc
        participant_id = quote.get("participant_id")
        line = quote.get("line")
        if (
            fixture_id < 1 or bookmaker_id < 1
            or not 1 <= len(market_key) <= 100
            or not 1 <= len(selection_key) <= 300
            or not 1 < price <= 500
            or observed < now - timedelta(days=7)
            or observed > now + timedelta(minutes=2)
            or (participant_id is not None and int(participant_id) < 0)
        ):
            raise ValueError(f"invalid quote observation at index {index}")
        identity = (
            fixture_id, bookmaker_id, market_key, selection_key,
            quote.get("participant_type") or "team",
            int(participant_id) if participant_id is not None else 0,
            float(line) if line is not None else -9999.0,
        )
        if identity in identities:
            raise ValueError(f"duplicate normalized quote identity at index {index}")
        identities.add(identity)
        quotes.append(quote)
    return quotes


def record_document(connection, document: Dict[str, Any]) -> Dict[str, int]:
    from psycopg2.extras import Json

    quotes = validate_document(document)
    totals = {"supplied": 0, "matched": 0, "written": 0}
    with connection.cursor() as cursor:
        for offset in range(0, len(quotes), 500):
            batch = quotes[offset : offset + 500]
            cursor.execute(
                "select * from public.odds_quote_verifications_record(%s::text, %s::jsonb)",
                (document["run_id"], Json(batch)),
            )
            supplied, matched, written = cursor.fetchone()
            totals["supplied"] += int(supplied)
            totals["matched"] += int(matched)
            totals["written"] += int(written)
    if totals["supplied"] != len(quotes):
        raise RuntimeError("verification writer supplied-count mismatch")
    return totals


def write_observation_spool(spool_dir: Path, priority: str, observations: List[Dict[str, Any]]) -> Path:
    """Atomically spool actual provider-response observations, never publication times."""
    if priority not in {"p1", "p2", "p3"}:
        raise ValueError("invalid observation priority")
    pending = spool_dir / "pending"
    pending.mkdir(mode=0o700, parents=True, exist_ok=True)
    run_id = f"{priority}-{utc_now().strftime('%Y%m%dT%H%M%S')}-{os.getpid()}"
    unique: Dict[tuple, Dict[str, Any]] = {}
    for row in observations:
        key = (row["fixture_id"], row["bookmaker_id"], row["market_key"],
               row["selection_key"], row.get("participant_type") or "team",
               row.get("participant_id") or 0, row.get("line") if row.get("line") is not None else -9999)
        if key not in unique or row["observed_at"] > unique[key]["observed_at"]:
            unique[key] = row
    document = {"schema_version": SCHEMA_VERSION, "source": SOURCE,
                "run_id": run_id, "quotes": list(unique.values())}
    validate_document(document)
    target = pending / f"{run_id}.json.gz"
    staging = pending / f".{run_id}.writing"
    try:
        with staging.open("wb") as raw:
            with gzip.GzipFile(fileobj=raw, mode="wb") as zipped:
                zipped.write(json.dumps(document, separators=(",", ":")).encode("utf-8"))
            raw.flush()
            os.fsync(raw.fileno())
        os.replace(staging, target)
    finally:
        staging.unlink(missing_ok=True)
    return target


def append_run_metrics(path: Path, document: Dict[str, Any], stats: Dict[str, int]) -> None:
    """Small durable JSONL history. Readers deduplicate replayed run_id values."""
    times = sorted(str(x["observed_at"]) for x in document["quotes"])
    record = {"run_id": document["run_id"],
              "tier": document["run_id"].split("-", 1)[0],
              "recorded_at": utc_now().isoformat(),
              "observed_first_at": times[0] if times else None,
              "observed_last_at": times[-1] if times else None,
              "supplied": stats["supplied"], "matched": stats["matched"],
              "written": stats["written"], "unmatched": stats["supplied"] - stats["matched"]}
    fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
    try:
        os.write(fd, (json.dumps(record, separators=(",", ":")) + "\n").encode("utf-8"))
        os.fsync(fd)
    finally:
        os.close(fd)


def record_spool(spool_dir: Path, database_url: str) -> Dict[str, int]:
    import psycopg2

    pending = spool_dir / "pending"
    processed = spool_dir / "processed"
    pending.mkdir(parents=True, exist_ok=True)
    processed.mkdir(parents=True, exist_ok=True)
    stale_claim_cutoff = utc_now() - timedelta(minutes=30)
    for claimed in pending.glob(".*.processing-*"):
        if datetime.fromtimestamp(claimed.stat().st_mtime, timezone.utc) >= stale_claim_cutoff:
            continue
        original_name = claimed.name[1:].split(".processing-", 1)[0]
        original = pending / original_name
        if not original.exists():
            os.replace(claimed, original)
    totals = {"files": 0, "supplied": 0, "matched": 0, "written": 0}
    for path in sorted((*pending.glob("*.json"), *pending.glob("*.json.gz"))):
        claimed = path.with_name(f".{path.name}.processing-{socket.gethostname()}-{os.getpid()}")
        try:
            os.replace(path, claimed)
        except FileNotFoundError:
            continue
        try:
            document = load_document(claimed)
            with psycopg2.connect(database_url, connect_timeout=10) as connection:
                result = record_document(connection, document)
            append_run_metrics(spool_dir / "run-metrics.jsonl", document, result)
            os.replace(claimed, processed / path.name)
            totals["files"] += 1
            for key in ("supplied", "matched", "written"):
                totals[key] += result[key]
        except Exception:
            os.replace(claimed, path)
            raise
    cutoff = utc_now() - timedelta(hours=48)
    for path in (*processed.glob("*.json"), *processed.glob("*.json.gz")):
        if datetime.fromtimestamp(path.stat().st_mtime, timezone.utc) < cutoff:
            path.unlink()
    return totals


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["record"])
    parser.add_argument("--spool-dir", required=True)
    args = parser.parse_args()
    database_url = (
        os.environ.get("ODDS_QUOTE_VERIFICATION_DATABASE_URL")
        or os.environ.get("SUPABASE_DB_URL_SESSION")
        or os.environ.get("SUPABASE_DB_URL")
    )
    if not database_url:
        raise SystemExit("a dedicated quote-verification database URL is required")
    print(json.dumps(record_spool(Path(args.spool_dir), database_url), sort_keys=True))


if __name__ == "__main__":
    main()
