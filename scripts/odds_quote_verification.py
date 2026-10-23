#!/usr/bin/env python3
"""Persist and publish direct provider quote-observation evidence."""

from __future__ import annotations

import argparse
import gzip
import json
import os
import re
import socket
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List

SCHEMA_VERSION = 1
SOURCE = "odds_api_provider_response"
RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{5,179}$")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def observation_from_row(row: Dict[str, Any], observed_at: str) -> Dict[str, Any]:
    return {
        "fixture_id": int(row["fixture_id"]),
        "bookmaker_id": int(row["bookmaker_id"]),
        "market_key": str(row["market_key"]),
        "selection_key": str(row["selection_key"]),
        "participant_type": row.get("participant_type"),
        "participant_id": row.get("participant_id"),
        "line": row.get("line"),
        "price_decimal": float(row["price_decimal"]),
        "source_last_updated_at": iso_utc(row.get("last_updated_at")),
        "observed_at": iso_utc(observed_at),
    }


def deduplicate_observations(quotes: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    latest: Dict[tuple[Any, ...], Dict[str, Any]] = {}
    for quote in quotes:
        identity = (
            quote.get("fixture_id"), quote.get("bookmaker_id"),
            quote.get("market_key"), quote.get("selection_key"),
            quote.get("participant_type") or "team",
            quote.get("participant_id") if quote.get("participant_id") is not None else 0,
            quote.get("line") if quote.get("line") is not None else -9999,
        )
        current = latest.get(identity)
        if current is None or str(quote.get("observed_at")) > str(current.get("observed_at")):
            latest[identity] = quote
    return list(latest.values())


def write_observation_document(
    path: Path,
    *,
    run_id: str,
    lane: str,
    quotes: Iterable[Dict[str, Any]],
    errors: Iterable[str] = (),
) -> None:
    if not RUN_ID_RE.fullmatch(run_id):
        raise ValueError("invalid verification run id")
    quote_list = deduplicate_observations(quotes)
    document = {
        "schema_version": SCHEMA_VERSION,
        "source": SOURCE,
        "run_id": run_id,
        "lane": lane,
        "generated_at": iso_utc(utc_now()),
        "quotes": quote_list,
        "errors": list(errors),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    if path.suffix == ".gz":
        with gzip.open(temporary, "wt", encoding="utf-8") as handle:
            json.dump(document, handle, separators=(",", ":"), sort_keys=True)
    else:
        temporary.write_text(json.dumps(document, separators=(",", ":"), sort_keys=True), encoding="utf-8")
    os.replace(temporary, path)


def _load(path: Path) -> Dict[str, Any]:
    if ".json.gz" in path.name:
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            return json.load(handle)
    return json.loads(path.read_text(encoding="utf-8"))


def _validate(document: Dict[str, Any]) -> List[Dict[str, Any]]:
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
    normalized: List[Dict[str, Any]] = []
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
        if (
            fixture_id < 1
            or bookmaker_id < 1
            or not 1 <= len(market_key) <= 100
            or not 1 <= len(selection_key) <= 300
            or not 1 < price <= 500
            or observed < now - timedelta(days=7)
            or observed > now + timedelta(minutes=2)
        ):
            raise ValueError(f"invalid quote observation at index {index}")
        participant_id = quote.get("participant_id")
        if participant_id is not None and int(participant_id) < 0:
            raise ValueError(f"invalid participant at index {index}")
        line = quote.get("line")
        identity = (
            fixture_id,
            bookmaker_id,
            market_key,
            selection_key,
            quote.get("participant_type") or "team",
            int(participant_id) if participant_id is not None else 0,
            float(line) if line is not None else -9999.0,
        )
        if identity in identities:
            raise ValueError(f"duplicate normalized quote identity at index {index}")
        identities.add(identity)
        normalized.append(quote)
    return normalized


def _record_document(connection, document: Dict[str, Any]) -> Dict[str, int]:
    from psycopg2.extras import Json

    quotes = _validate(document)
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
            document = _load(claimed)
            with psycopg2.connect(database_url, connect_timeout=10) as connection:
                result = _record_document(connection, document)
            target = processed / path.name
            os.replace(claimed, target)
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
    result = record_spool(Path(args.spool_dir), database_url)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
