#!/usr/bin/env python3
"""Audit canonical metadata coverage in Supabase/Postgres."""

from __future__ import annotations

import json
import os
from typing import Dict, Any

import psycopg2


def get_db_url() -> str:
    value = (
        os.getenv("SUPABASE_DB_URL_SESSION")
        or os.getenv("SUPABASE_DB_URL_POOLER")
        or os.getenv("SUPABASE_DB_URL")
    )
    if not value:
        raise RuntimeError("Missing SUPABASE_DB_URL_SESSION / SUPABASE_DB_URL_POOLER / SUPABASE_DB_URL")
    return value


def pct(with_value: int, total: int) -> str:
    if total <= 0:
        return "0.0%"
    return f"{(with_value * 100.0 / total):.1f}%"


def build_report(raw: Dict[str, int]) -> Dict[str, Any]:
    teams_total = int(raw.get("teams_total", 0) or 0)
    teams_with_badge = int(raw.get("teams_with_badge", 0) or 0)
    teams_with_country = int(raw.get("teams_with_country", 0) or 0)

    players_total = int(raw.get("players_total", 0) or 0)
    players_with_nationality = int(raw.get("players_with_nationality", 0) or 0)

    leagues_total = int(raw.get("leagues_total", 0) or 0)
    leagues_with_badge = int(raw.get("leagues_with_badge", 0) or 0)

    countries_total = int(raw.get("countries_total", 0) or 0)
    countries_with_flag = int(raw.get("countries_with_flag", 0) or 0)

    return {
        "coverage": {
            "teams": {
                "total": teams_total,
                "withBadge": teams_with_badge,
                "badgePct": pct(teams_with_badge, teams_total),
                "withCountry": teams_with_country,
                "countryPct": pct(teams_with_country, teams_total),
            },
            "players": {
                "total": players_total,
                "withNationality": players_with_nationality,
                "nationalityPct": pct(players_with_nationality, players_total),
            },
            "leagues": {
                "total": leagues_total,
                "withBadge": leagues_with_badge,
                "badgePct": pct(leagues_with_badge, leagues_total),
            },
            "countries": {
                "total": countries_total,
                "withFlag": countries_with_flag,
                "flagPct": pct(countries_with_flag, countries_total),
            },
        }
    }


def main() -> None:
    db_url = get_db_url()

    query = """
        select
          (select count(*)::int from public.teams) as teams_total,
          (select count(*)::int from public.teams where image_path is not null) as teams_with_badge,
          (select count(*)::int from public.teams where country_id is not null) as teams_with_country,
          (select count(*)::int from public.players) as players_total,
          (select count(*)::int from public.players where nationality_country_id is not null) as players_with_nationality,
          (select count(*)::int from public.leagues) as leagues_total,
          (select count(*)::int from public.leagues where image_path is not null) as leagues_with_badge,
          (select count(*)::int from public.countries) as countries_total,
          (select count(*)::int from public.countries where image_path is not null) as countries_with_flag
    """

    conn = psycopg2.connect(db_url, sslmode="require")
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            row = cur.fetchone()
            columns = [desc[0] for desc in cur.description]
        raw = dict(zip(columns, row))
        report = build_report(raw)
        print(json.dumps(report, indent=2))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
