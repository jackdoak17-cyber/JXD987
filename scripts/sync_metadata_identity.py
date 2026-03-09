#!/usr/bin/env python3
"""
Sync canonical metadata into Supabase:
- countries (flags)
- leagues (badges + country link)
- teams (badge + country link)
- players (nationality country)
- team_competitions (derived from fixtures/seasons)

This script is idempotent and safe to rerun.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Iterable, List, Optional, Tuple

import psycopg2
from psycopg2.extras import execute_values
import requests


DEFAULT_BASE_URL = "https://api.sportmonks.com/v3/football"
DEFAULT_PER_PAGE = 100

SCHEMA_SQL = """
create table if not exists public.countries (
  id bigint primary key,
  name text not null,
  official_name text,
  fifa_name text,
  iso2 text,
  iso3 text,
  image_path text,
  flag_storage_path text,
  source text not null default 'sportmonks',
  updated_at timestamptz not null default now()
);

create table if not exists public.leagues (
  id bigint primary key,
  name text not null,
  country_id bigint references public.countries(id) on delete set null,
  image_path text,
  badge_storage_path text,
  active boolean not null default true,
  source text not null default 'sportmonks',
  updated_at timestamptz not null default now()
);

create table if not exists public.team_competitions (
  team_id bigint not null references public.teams(id) on delete cascade,
  league_id bigint not null references public.leagues(id) on delete cascade,
  season_id bigint not null references public.seasons(id) on delete cascade,
  is_current boolean not null default false,
  source text not null default 'fixtures',
  updated_at timestamptz not null default now(),
  primary key (team_id, league_id, season_id)
);

alter table public.teams
  add column if not exists country_id bigint,
  add column if not exists country_updated_at timestamptz;

alter table public.players
  add column if not exists nationality_country_id bigint,
  add column if not exists nationality_updated_at timestamptz;

do $$
begin
  if not exists (
    select 1 from pg_constraint where conname = 'teams_country_id_fkey'
  ) then
    alter table public.teams
      add constraint teams_country_id_fkey
      foreign key (country_id)
      references public.countries(id)
      on delete set null;
  end if;

  if not exists (
    select 1 from pg_constraint where conname = 'players_nationality_country_id_fkey'
  ) then
    alter table public.players
      add constraint players_nationality_country_id_fkey
      foreign key (nationality_country_id)
      references public.countries(id)
      on delete set null;
  end if;
end
$$;

create index if not exists countries_name_idx
  on public.countries (lower(name));
create index if not exists leagues_country_id_idx
  on public.leagues (country_id);
create index if not exists leagues_name_idx
  on public.leagues (lower(name));
create index if not exists team_competitions_league_season_idx
  on public.team_competitions (league_id, season_id, is_current);
create index if not exists team_competitions_team_idx
  on public.team_competitions (team_id, updated_at desc);
create index if not exists teams_country_id_idx
  on public.teams (country_id);
create index if not exists players_nationality_country_id_idx
  on public.players (nationality_country_id);
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--player-mode", choices=["squads", "all", "off"], default="squads")
    parser.add_argument("--max-player-pages", type=int, default=0, help="0 = all")
    parser.add_argument("--team-concurrency", type=int, default=6)
    parser.add_argument("--player-id-concurrency", type=int, default=12)
    parser.add_argument("--player-id-limit", type=int, default=0, help="0 = all missing")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def get_db_url() -> str:
    value = (
        os.getenv("SUPABASE_DB_URL_SESSION")
        or os.getenv("SUPABASE_DB_URL_POOLER")
        or os.getenv("SUPABASE_DB_URL")
    )
    if not value:
        raise RuntimeError("Missing SUPABASE_DB_URL_SESSION / SUPABASE_DB_URL_POOLER / SUPABASE_DB_URL")
    return value


def get_api_token() -> str:
    value = os.getenv("SPORTMONKS_API_TOKEN") or os.getenv("ODDS_API_KEY")
    if not value:
        raise RuntimeError("Missing SPORTMONKS_API_TOKEN (or ODDS_API_KEY)")
    return value


def get_base_url() -> str:
    return (os.getenv("FOOTBALL_API_BASE_URL") or DEFAULT_BASE_URL).rstrip("/")


def fetch_json_with_retry(url: str, session: requests.Session, max_attempts: int = 5) -> Dict[str, Any]:
    last_exc: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        try:
            response = session.get(url, timeout=30)
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                sleep_secs = int(retry_after) if retry_after and retry_after.isdigit() else attempt
                time.sleep(max(1, sleep_secs))
                continue
            response.raise_for_status()
            return response.json()
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt >= max_attempts:
                break
            time.sleep(min(10, attempt))
    raise RuntimeError(f"Request failed after retries: {url}") from last_exc


def fetch_all_pages(
    *,
    session: requests.Session,
    base_url: str,
    token: str,
    endpoint: str,
    include: Optional[str] = None,
    max_pages: int = 0,
) -> List[Dict[str, Any]]:
    page = 1
    rows: List[Dict[str, Any]] = []
    while True:
        if max_pages > 0 and page > max_pages:
            break
        params = [f"api_token={token}", f"page={page}", f"per_page={DEFAULT_PER_PAGE}"]
        if include:
            params.append(f"include={include}")
        url = f"{base_url}/{endpoint}?{'&'.join(params)}"
        payload = fetch_json_with_retry(url, session=session)
        page_rows = payload.get("data")
        if isinstance(page_rows, list):
            rows.extend([r for r in page_rows if isinstance(r, dict)])
        has_more = bool(((payload.get("pagination") or {}).get("has_more")))
        if not has_more:
            break
        page += 1
    return rows


def to_positive_int(value: Any) -> Optional[int]:
    try:
        number = int(value)
    except Exception:  # noqa: BLE001
        return None
    return number if number > 0 else None


def non_empty(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    value = value.strip()
    return value or None


def normalize_country(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    cid = to_positive_int(row.get("id"))
    if not cid:
        return None
    return {
        "id": cid,
        "name": non_empty(row.get("name")) or f"Country {cid}",
        "official_name": non_empty(row.get("official_name")),
        "fifa_name": non_empty(row.get("fifa_name")),
        "iso2": non_empty(row.get("iso2")),
        "iso3": non_empty(row.get("iso3")),
        "image_path": non_empty(row.get("image_path")),
    }


def normalize_league(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    lid = to_positive_int(row.get("id"))
    if not lid:
        return None
    return {
        "id": lid,
        "name": non_empty(row.get("name")) or f"League {lid}",
        "country_id": to_positive_int(row.get("country_id")),
        "image_path": non_empty(row.get("image_path")),
    }


def normalize_team(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    tid = to_positive_int(row.get("id"))
    if not tid:
        return None
    return {
        "id": tid,
        "name": non_empty(row.get("name")) or f"Team {tid}",
        "short_code": non_empty(row.get("short_code")),
        "country_id": to_positive_int(row.get("country_id")),
        "image_path": non_empty(row.get("image_path")),
    }


def normalize_player(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    pid = to_positive_int(row.get("id"))
    if not pid:
        return None
    return {
        "id": pid,
        "name": non_empty(row.get("name")),
        "display_name": non_empty(row.get("display_name")),
        "short_name": non_empty(row.get("short_name")),
        "common_name": non_empty(row.get("common_name")),
        "country_id": to_positive_int(row.get("country_id")),
    }


def chunked(values: List[Any], size: int) -> Iterable[List[Any]]:
    for idx in range(0, len(values), size):
        yield values[idx : idx + size]


def upsert_countries(cur: Any, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    sql = """
        insert into public.countries
          (id,name,official_name,fifa_name,iso2,iso3,image_path,flag_storage_path,source,updated_at)
        values %s
        on conflict (id) do update
        set name = excluded.name,
            official_name = excluded.official_name,
            fifa_name = excluded.fifa_name,
            iso2 = excluded.iso2,
            iso3 = excluded.iso3,
            image_path = excluded.image_path,
            flag_storage_path = excluded.flag_storage_path,
            source = excluded.source,
            updated_at = excluded.updated_at
    """
    payload = [
        (
            row["id"],
            row["name"],
            row["official_name"],
            row["fifa_name"],
            row["iso2"],
            row["iso3"],
            row["image_path"],
            row["image_path"],
            "sportmonks",
        )
        for row in rows
    ]
    execute_values(cur, sql, payload, page_size=500, template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,now())")


def upsert_leagues(cur: Any, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    sql = """
        insert into public.leagues
          (id,name,country_id,image_path,badge_storage_path,active,source,updated_at)
        values %s
        on conflict (id) do update
        set name = excluded.name,
            country_id = excluded.country_id,
            image_path = excluded.image_path,
            badge_storage_path = excluded.badge_storage_path,
            active = excluded.active,
            source = excluded.source,
            updated_at = excluded.updated_at
    """
    payload = [
        (
            row["id"],
            row["name"],
            row["country_id"],
            row["image_path"],
            row["image_path"],
            True,
            "sportmonks",
        )
        for row in rows
    ]
    execute_values(cur, sql, payload, page_size=500, template="(%s,%s,%s,%s,%s,%s,%s,now())")


def upsert_teams(cur: Any, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    sql = """
        insert into public.teams
          (id,name,short_code,image_path,country_id,country_updated_at)
        values %s
        on conflict (id) do update
        set name = excluded.name,
            short_code = coalesce(excluded.short_code, public.teams.short_code),
            image_path = coalesce(excluded.image_path, public.teams.image_path),
            country_id = coalesce(excluded.country_id, public.teams.country_id),
            country_updated_at = excluded.country_updated_at
    """
    payload = [
        (
            row["id"],
            row["name"],
            row["short_code"],
            row["image_path"],
            row["country_id"],
        )
        for row in rows
    ]
    execute_values(cur, sql, payload, page_size=500, template="(%s,%s,%s,%s,%s,now())")


def upsert_players(cur: Any, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    sql = """
        insert into public.players
          (id,name,display_name,short_name,common_name,nationality_country_id,nationality_updated_at,updated_at)
        values %s
        on conflict (id) do update
        set name = coalesce(excluded.name, public.players.name),
            display_name = coalesce(excluded.display_name, public.players.display_name),
            short_name = coalesce(excluded.short_name, public.players.short_name),
            common_name = coalesce(excluded.common_name, public.players.common_name),
            nationality_country_id = coalesce(excluded.nationality_country_id, public.players.nationality_country_id),
            nationality_updated_at = excluded.nationality_updated_at,
            updated_at = excluded.updated_at
    """
    payload = [
        (
            row["id"],
            row["name"],
            row["display_name"],
            row["short_name"],
            row["common_name"],
            row["country_id"],
        )
        for row in rows
    ]
    execute_values(cur, sql, payload, page_size=500, template="(%s,%s,%s,%s,%s,%s,now(),now())")


def refresh_team_competitions(cur: Any) -> None:
    cur.execute(
        """
        insert into public.team_competitions
          (team_id, league_id, season_id, is_current, source, updated_at)
        select
          rows.team_id,
          rows.league_id,
          rows.season_id,
          bool_or(rows.is_current) as is_current,
          'fixtures' as source,
          now() as updated_at
        from (
          select
            f.home_team_id as team_id,
            f.league_id,
            f.season_id,
            coalesce(s.is_current, false) as is_current
          from public.fixtures f
          left join public.seasons s on s.id = f.season_id
          where f.home_team_id is not null

          union all

          select
            f.away_team_id as team_id,
            f.league_id,
            f.season_id,
            coalesce(s.is_current, false) as is_current
          from public.fixtures f
          left join public.seasons s on s.id = f.season_id
          where f.away_team_id is not null
        ) rows
        where rows.team_id is not null and rows.league_id is not null and rows.season_id is not null
        group by rows.team_id, rows.league_id, rows.season_id
        on conflict (team_id, league_id, season_id) do update
          set is_current = excluded.is_current,
              source = excluded.source,
              updated_at = excluded.updated_at
        """
    )


def load_coverage(cur: Any) -> Dict[str, Any]:
    cur.execute(
        """
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
    )
    row = cur.fetchone()
    columns = [desc[0] for desc in cur.description]
    return dict(zip(columns, row))


def fetch_team_squad_player_data(
    *,
    session: requests.Session,
    base_url: str,
    token: str,
    team_id: int,
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]], List[Tuple[Dict[str, Any], Dict[str, Any]]]]:
    url = f"{base_url}/teams/{team_id}?api_token={token}&include=players.player.country"
    payload = fetch_json_with_retry(url, session=session, max_attempts=4)
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        return None, None, []

    team = normalize_team(data)
    team_country = normalize_country(data.get("country") if isinstance(data.get("country"), dict) else {})

    pairs: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
    players = data.get("players")
    if isinstance(players, list):
        for entry in players:
            if not isinstance(entry, dict):
                continue
            player_raw = entry.get("player")
            if not isinstance(player_raw, dict):
                continue
            player = normalize_player(player_raw)
            country = normalize_country(player_raw.get("country") if isinstance(player_raw.get("country"), dict) else {})
            if player:
                pairs.append((player, country or {}))
    return team, team_country, pairs


def fetch_player_country(
    *,
    session: requests.Session,
    base_url: str,
    token: str,
    player_id: int,
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    url = f"{base_url}/players/{player_id}?api_token={token}&include=country"
    payload = fetch_json_with_retry(url, session=session, max_attempts=4)
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        return None, None
    player = normalize_player(data)
    country = normalize_country(data.get("country") if isinstance(data.get("country"), dict) else {})
    return player, country


def main() -> None:
    args = parse_args()
    db_url = get_db_url()
    token = get_api_token()
    base_url = get_base_url()

    verify_tls = os.getenv("ALLOW_INSECURE_TLS") != "1"
    session = requests.Session()
    session.verify = verify_tls
    session.headers.update({"Accept": "application/json"})

    print(
        json.dumps(
            {
                "stage": "start",
                "playerMode": args.player_mode,
                "maxPlayerPages": args.max_player_pages,
                "teamConcurrency": args.team_concurrency,
                "playerIdConcurrency": args.player_id_concurrency,
                "playerIdLimit": args.player_id_limit,
                "dryRun": args.dry_run,
            },
            indent=2,
        )
    )

    countries_by_id: Dict[int, Dict[str, Any]] = {}
    leagues_by_id: Dict[int, Dict[str, Any]] = {}
    teams_by_id: Dict[int, Dict[str, Any]] = {}
    players_by_id: Dict[int, Dict[str, Any]] = {}

    conn = psycopg2.connect(db_url, sslmode="require")
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
            conn.commit()

        league_rows, team_rows = (
            fetch_all_pages(session=session, base_url=base_url, token=token, endpoint="leagues", include="country"),
            fetch_all_pages(session=session, base_url=base_url, token=token, endpoint="teams", include="country"),
        )

        for row in league_rows:
            league = normalize_league(row)
            if league:
                leagues_by_id[league["id"]] = league
            country = normalize_country(row.get("country") if isinstance(row.get("country"), dict) else {})
            if country:
                countries_by_id[country["id"]] = country

        for row in team_rows:
            team = normalize_team(row)
            if team:
                teams_by_id[team["id"]] = team
            country = normalize_country(row.get("country") if isinstance(row.get("country"), dict) else {})
            if country:
                countries_by_id[country["id"]] = country

        with conn.cursor() as cur:
            cur.execute("select id from public.players")
            known_player_ids = {int(row[0]) for row in cur.fetchall() if row and row[0] is not None}

        if args.player_mode == "all":
            player_rows = fetch_all_pages(
                session=session,
                base_url=base_url,
                token=token,
                endpoint="players",
                include="country",
                max_pages=args.max_player_pages,
            )
            for row in player_rows:
                player = normalize_player(row)
                if not player or player["id"] not in known_player_ids:
                    continue
                players_by_id[player["id"]] = player
                country = normalize_country(row.get("country") if isinstance(row.get("country"), dict) else {})
                if country:
                    countries_by_id[country["id"]] = country

        elif args.player_mode == "squads":
            with conn.cursor() as cur:
                cur.execute("select id from public.teams")
                team_ids = [int(row[0]) for row in cur.fetchall() if row and row[0] is not None]

            with ThreadPoolExecutor(max_workers=max(1, args.team_concurrency)) as executor:
                futures = {
                    executor.submit(
                        fetch_team_squad_player_data,
                        session=session,
                        base_url=base_url,
                        token=token,
                        team_id=team_id,
                    ): team_id
                    for team_id in team_ids
                }
                for future in as_completed(futures):
                    team_id = futures[future]
                    try:
                        team, team_country, player_country_pairs = future.result()
                    except Exception as exc:  # noqa: BLE001
                        print(f"team {team_id} squad lookup failed: {exc}")
                        continue

                    if team:
                        teams_by_id[team["id"]] = team
                    if team_country:
                        countries_by_id[team_country["id"]] = team_country
                    for player, country in player_country_pairs:
                        if player["id"] not in known_player_ids:
                            continue
                        players_by_id[player["id"]] = player
                        if country and country.get("id"):
                            countries_by_id[country["id"]] = country

        if args.player_mode != "off":
            with conn.cursor() as cur:
                cur.execute(
                    "select id from public.players where nationality_country_id is null order by id"
                )
                missing_ids = [
                    int(row[0])
                    for row in cur.fetchall()
                    if row and row[0] is not None and int(row[0]) not in players_by_id
                ]
            if args.player_id_limit > 0:
                missing_ids = missing_ids[: args.player_id_limit]

            with ThreadPoolExecutor(max_workers=max(1, args.player_id_concurrency)) as executor:
                futures = {
                    executor.submit(
                        fetch_player_country,
                        session=session,
                        base_url=base_url,
                        token=token,
                        player_id=player_id,
                    ): player_id
                    for player_id in missing_ids
                }
                for future in as_completed(futures):
                    player_id = futures[future]
                    try:
                        player, country = future.result()
                    except Exception as exc:  # noqa: BLE001
                        print(f"player {player_id} nationality lookup failed: {exc}")
                        continue
                    if player and player["id"] in known_player_ids:
                        players_by_id[player["id"]] = player
                    if country and country.get("id"):
                        countries_by_id[country["id"]] = country

        if not args.dry_run:
            with conn.cursor() as cur:
                upsert_countries(cur, list(countries_by_id.values()))
                upsert_leagues(cur, list(leagues_by_id.values()))
                upsert_teams(cur, list(teams_by_id.values()))
                upsert_players(cur, list(players_by_id.values()))
                refresh_team_competitions(cur)
                conn.commit()

        with conn.cursor() as cur:
            coverage = load_coverage(cur)

        print(
            json.dumps(
                {
                    "stage": "complete",
                    "dryRun": args.dry_run,
                    "provider": {
                        "leaguesFetched": len(leagues_by_id),
                        "teamsFetched": len(teams_by_id),
                        "countriesCollected": len(countries_by_id),
                        "playersMatchedForNationality": len(players_by_id),
                    },
                    "coverage": coverage,
                },
                indent=2,
            )
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
