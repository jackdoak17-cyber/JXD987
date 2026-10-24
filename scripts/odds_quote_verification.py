#!/usr/bin/env python3
"""Publish exact provider quote-observation evidence after canonical export."""

from __future__ import annotations

import argparse
import gzip
import json
import os
import re
import sqlite3
import socket
from decimal import Decimal, InvalidOperation
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

SCHEMA_VERSION = 1
SOURCE = "odds_api_provider_response"
RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{5,179}$")
CUSTOMER_MARKETS = {
    "moneyline", "double_chance", "draw_no_bet", "goals_over_under",
    "goals_over_under_first_half", "btts", "match_shots",
    "match_shots_on_target", "team_shots", "team_shots_on_target",
    "team_total_goals", "player_shots", "player_shots_on_target",
    "player_fouls_committed", "player_fouls_drawn", "player_to_score",
    "player_to_assist", "player_to_score_or_assist", "player_card",
    "player_tackles", "player_goalkeeper_saves",
}


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


def _decimal_key(value: Any) -> str:
    try:
        return format(Decimal(str(value)).normalize(), "f")
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"invalid decimal identity value: {value!r}") from exc


def _timestamp_key(value: Any) -> str | None:
    if value in (None, ""):
        return None
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat(timespec="microseconds")


def _quote_key(quote: Dict[str, Any]) -> tuple[Any, ...]:
    return (
        int(quote["fixture_id"]), int(quote["bookmaker_id"]), str(quote["market_key"]),
        str(quote["selection_key"]), quote.get("participant_type") or "team",
        int(quote["participant_id"]) if quote.get("participant_id") is not None else 0,
        _decimal_key(quote["line"]) if quote.get("line") is not None else "-9999",
        _decimal_key(quote["price_decimal"]), _timestamp_key(quote.get("source_last_updated_at")),
    )


def current_customer_quotes(
    sqlite_db: Path,
    quotes: List[Dict[str, Any]],
    league_ids: List[int],
) -> List[Dict[str, Any]]:
    """Keep only observations that are still the local canonical customer quote."""
    fixture_ids = sorted({int(quote["fixture_id"]) for quote in quotes})
    canonical: set[tuple[Any, ...]] = set()
    uri = f"file:{sqlite_db.resolve(strict=True)}?mode=ro"
    with sqlite3.connect(uri, uri=True) as connection:
        for offset in range(0, len(fixture_ids), 400):
            chunk = fixture_ids[offset : offset + 400]
            fixture_slots = ",".join("?" for _ in chunk)
            league_slots = ",".join("?" for _ in league_ids)
            league_clause = f"and f.league_id in ({league_slots})" if league_ids else ""
            rows = connection.execute(
                f"""
                select o.fixture_id, o.bookmaker_id, o.market_key, o.selection_key,
                       coalesce(nullif(o.participant_type, ''), 'team'),
                       coalesce(o.participant_id, 0), o.line, o.price_decimal,
                       o.last_updated_at
                from odds_outcomes o
                join fixtures f on f.id = o.fixture_id
                where o.fixture_id in ({fixture_slots})
                  {league_clause}
                """,
                [*chunk, *league_ids],
            )
            for row in rows:
                if row[2] not in CUSTOMER_MARKETS:
                    continue
                canonical.add((
                    int(row[0]), int(row[1]), str(row[2]), str(row[3]), row[4] or "team",
                    int(row[5] or 0), _decimal_key(row[6]) if row[6] is not None else "-9999",
                    _decimal_key(row[7]), _timestamp_key(row[8]),
                ))
    return [quote for quote in quotes if quote.get("market_key") in CUSTOMER_MARKETS and _quote_key(quote) in canonical]


def partition_customer_quotes(
    sqlite_db: Path,
    quotes: List[Dict[str, Any]],
    league_ids: List[int],
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Separate mutable future quotes from fixtures whose publication is frozen."""
    eligible = current_customer_quotes(sqlite_db, quotes, league_ids)
    fixture_ids = sorted({int(quote["fixture_id"]) for quote in eligible})
    kickoffs: Dict[int, datetime] = {}
    uri = f"file:{sqlite_db.resolve(strict=True)}?mode=ro"
    with sqlite3.connect(uri, uri=True) as connection:
        for offset in range(0, len(fixture_ids), 400):
            chunk = fixture_ids[offset : offset + 400]
            slots = ",".join("?" for _ in chunk)
            for fixture_id, starting_at in connection.execute(
                f"select id, starting_at from fixtures where id in ({slots})", chunk,
            ):
                try:
                    parsed = datetime.fromisoformat(str(starting_at).replace("Z", "+00:00"))
                    if parsed.tzinfo is None:
                        parsed = parsed.replace(tzinfo=timezone.utc)
                    kickoffs[int(fixture_id)] = parsed.astimezone(timezone.utc)
                except (TypeError, ValueError):
                    # Unknown kickoff stays in the strict future group.
                    continue
    now = utc_now()
    frozen_fixture_ids = {
        fixture_id for fixture_id, kickoff in kickoffs.items() if kickoff <= now
    }
    frozen = [quote for quote in eligible if int(quote["fixture_id"]) in frozen_fixture_ids]
    future = [quote for quote in eligible if int(quote["fixture_id"]) not in frozen_fixture_ids]
    return future, frozen


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


def record_spool(
    spool_dir: Path,
    database_url: str,
    sqlite_db: Path,
    league_ids: List[int],
) -> Dict[str, int]:
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
    totals = {"files": 0, "observed": 0, "superseded": 0, "supplied": 0, "matched": 0, "written": 0}
    for path in sorted((*pending.glob("*.json"), *pending.glob("*.json.gz"))):
        claimed = path.with_name(f".{path.name}.processing-{socket.gethostname()}-{os.getpid()}")
        try:
            os.replace(path, claimed)
        except FileNotFoundError:
            continue
        try:
            document = load_document(claimed)
            observed = validate_document(document)
            future, frozen = partition_customer_quotes(sqlite_db, observed, league_ids)
            with psycopg2.connect(database_url, connect_timeout=10) as connection:
                result = {"supplied": 0, "matched": 0, "written": 0}
                if future:
                    future_result = record_document(connection, {**document, "quotes": future})
                    if future_result["matched"] != future_result["supplied"]:
                        raise RuntimeError(
                            "future canonical quote verification incomplete: "
                            f"matched {future_result['matched']} of {future_result['supplied']}"
                        )
                    for key in result:
                        result[key] += future_result[key]
                if frozen:
                    frozen_result = record_document(connection, {**document, "quotes": frozen})
                    for key in result:
                        result[key] += frozen_result[key]
            os.replace(claimed, processed / path.name)
            totals["files"] += 1
            totals["observed"] += len(observed)
            totals["superseded"] += len(observed) - len(future) - result["matched"]
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
    parser.add_argument("--sqlite-db", required=True, type=Path)
    parser.add_argument("--leagues", default="")
    args = parser.parse_args()
    database_url = (
        os.environ.get("ODDS_QUOTE_VERIFICATION_DATABASE_URL")
        or os.environ.get("SUPABASE_DB_URL_SESSION")
        or os.environ.get("SUPABASE_DB_URL")
    )
    if not database_url:
        raise SystemExit("a dedicated quote-verification database URL is required")
    league_ids = [int(value) for value in args.leagues.split(",") if value.strip()]
    print(json.dumps(record_spool(
        Path(args.spool_dir), database_url, args.sqlite_db, league_ids,
    ), sort_keys=True))


if __name__ == "__main__":
    main()
