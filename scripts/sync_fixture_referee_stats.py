#!/usr/bin/env python3
"""
Build fixture-level referee card metrics from database stats only.

- Uses assigned primary/main referee per fixture
- Uses historical completed fixtures officiated by the same referee
- Derives card metrics from fixture_statistics type IDs:
    84 = yellow cards
    83 = red cards
- Produces Last 5 / Last 10 / Last 20 windows per fixture
- Upserts into public.fixture_referee_stats (including windows JSON)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from typing import Any, Dict, Iterable, List, Optional, Tuple

import psycopg2
from psycopg2.extras import Json, execute_values

FINISHED_STATUSES = ["FT", "AET", "PEN"]

logger = logging.getLogger("sync_fixture_referee_stats")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync fixture_referee_stats from database card history")
    parser.add_argument("--days-back", type=int, default=30)
    parser.add_argument("--days-forward", type=int, default=14)
    parser.add_argument("--limit-fixtures", type=int, default=0)
    # Kept for workflow CLI compatibility; no external API calls are made.
    parser.add_argument("--sleep-seconds", type=float, default=0.0)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--report-json", type=str, default="")
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


def to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        number = float(value)
    except Exception:  # noqa: BLE001
        return None
    return number if number == number else None


def to_int(value: Any) -> int:
    try:
        return int(value)
    except Exception:  # noqa: BLE001
        return 0


def write_report(path: str, report: Dict[str, Any]) -> None:
    if not path:
        return
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, ensure_ascii=False)


def fetch_fixture_referee_metrics(
    conn: psycopg2.extensions.connection,
    *,
    days_back: int,
    days_forward: int,
    limit_fixtures: int,
) -> List[Dict[str, Any]]:
    limit_clause = ""
    params: List[Any] = [max(0, days_back), max(0, days_forward), FINISHED_STATUSES]
    if limit_fixtures > 0:
        limit_clause = "\n          limit %s"
        params.append(limit_fixtures)

    sql = f"""
      with target as (
        select
          f.id::bigint as fixture_id,
          f.starting_at as fixture_starting_at,
          pick.referee_id::bigint as referee_id,
          coalesce(r.name, 'Referee ' || pick.referee_id::text) as referee_name
        from public.fixtures f
        join lateral (
          select fr.referee_id
          from public.fixture_referees fr
          where fr.fixture_id = f.id
          order by
            fr.is_primary desc,
            case when lower(coalesce(fr.role, '')) in ('main', 'referee') then 0 else 1 end,
            fr.updated_at desc
          limit 1
        ) pick on true
        left join public.referees r
          on r.id = pick.referee_id
        where f.starting_at >= (now() - make_interval(days => %s))
          and f.starting_at <= (now() + make_interval(days => %s))
        order by f.starting_at asc
        {limit_clause}
      ),
      referee_history as (
        select
          t.fixture_id,
          h.id::bigint as history_fixture_id,
          h.starting_at as history_starting_at,
          coalesce(sum(case when fs.type_id = 84 then fs.value else 0 end), 0)::float8 as yellow_cards,
          coalesce(sum(case when fs.type_id = 83 then fs.value else 0 end), 0)::float8 as red_cards,
          count(fs.type_id)::int as card_stat_points
        from target t
        join public.fixture_referees frh
          on frh.referee_id = t.referee_id
         and (frh.is_primary = true or lower(coalesce(frh.role, '')) in ('main', 'referee'))
        join public.fixtures h
          on h.id = frh.fixture_id
         and h.starting_at < t.fixture_starting_at
         and (h.status = any(%s::text[]) or (h.home_score is not null and h.away_score is not null))
        left join public.fixture_statistics fs
          on fs.fixture_id = h.id
         and fs.type_id in (83, 84)
        group by t.fixture_id, h.id, h.starting_at
      ),
      ranked as (
        select
          fixture_id,
          history_fixture_id,
          history_starting_at,
          yellow_cards,
          red_cards,
          row_number() over (
            partition by fixture_id
            order by history_starting_at desc, history_fixture_id desc
          ) as rn
        from referee_history
        where card_stat_points > 0
      ),
      agg as (
        select
          fixture_id,
          count(*) filter (where rn <= 5)::int as sample_5,
          count(*) filter (where rn <= 10)::int as sample_10,
          count(*) filter (where rn <= 20)::int as sample_20,

          avg(yellow_cards) filter (where rn <= 5)::float8 as avg_cards_5,
          avg(yellow_cards) filter (where rn <= 10)::float8 as avg_cards_10,
          avg(yellow_cards) filter (where rn <= 20)::float8 as avg_cards_20,

          avg(yellow_cards + red_cards) filter (where rn <= 5)::float8 as avg_total_cards_5,
          avg(yellow_cards + red_cards) filter (where rn <= 10)::float8 as avg_total_cards_10,
          avg(yellow_cards + red_cards) filter (where rn <= 20)::float8 as avg_total_cards_20,

          avg(red_cards) filter (where rn <= 5)::float8 as avg_red_cards_5,
          avg(red_cards) filter (where rn <= 10)::float8 as avg_red_cards_10,
          avg(red_cards) filter (where rn <= 20)::float8 as avg_red_cards_20,

          avg(case when yellow_cards + red_cards >= 3 then 100.0 else 0.0 end) filter (where rn <= 5)::float8 as pct_3plus_5,
          avg(case when yellow_cards + red_cards >= 3 then 100.0 else 0.0 end) filter (where rn <= 10)::float8 as pct_3plus_10,
          avg(case when yellow_cards + red_cards >= 3 then 100.0 else 0.0 end) filter (where rn <= 20)::float8 as pct_3plus_20,

          avg(case when yellow_cards + red_cards >= 4 then 100.0 else 0.0 end) filter (where rn <= 5)::float8 as pct_4plus_5,
          avg(case when yellow_cards + red_cards >= 4 then 100.0 else 0.0 end) filter (where rn <= 10)::float8 as pct_4plus_10,
          avg(case when yellow_cards + red_cards >= 4 then 100.0 else 0.0 end) filter (where rn <= 20)::float8 as pct_4plus_20,

          avg(case when yellow_cards + red_cards >= 5 then 100.0 else 0.0 end) filter (where rn <= 5)::float8 as pct_5plus_5,
          avg(case when yellow_cards + red_cards >= 5 then 100.0 else 0.0 end) filter (where rn <= 10)::float8 as pct_5plus_10,
          avg(case when yellow_cards + red_cards >= 5 then 100.0 else 0.0 end) filter (where rn <= 20)::float8 as pct_5plus_20,

          avg(case when red_cards > 0 then 100.0 else 0.0 end) filter (where rn <= 5)::float8 as pct_red_5,
          avg(case when red_cards > 0 then 100.0 else 0.0 end) filter (where rn <= 10)::float8 as pct_red_10,
          avg(case when red_cards > 0 then 100.0 else 0.0 end) filter (where rn <= 20)::float8 as pct_red_20
        from ranked
        where rn <= 20
        group by fixture_id
      )
      select
        t.fixture_id,
        t.referee_id,
        t.referee_name,

        coalesce(a.sample_5, 0) as sample_5,
        coalesce(a.sample_10, 0) as sample_10,
        coalesce(a.sample_20, 0) as sample_20,

        a.avg_cards_5,
        a.avg_cards_10,
        a.avg_cards_20,

        a.avg_total_cards_5,
        a.avg_total_cards_10,
        a.avg_total_cards_20,

        a.avg_red_cards_5,
        a.avg_red_cards_10,
        a.avg_red_cards_20,

        a.pct_3plus_5,
        a.pct_3plus_10,
        a.pct_3plus_20,

        a.pct_4plus_5,
        a.pct_4plus_10,
        a.pct_4plus_20,

        a.pct_5plus_5,
        a.pct_5plus_10,
        a.pct_5plus_20,

        a.pct_red_5,
        a.pct_red_10,
        a.pct_red_20
      from target t
      left join agg a
        on a.fixture_id = t.fixture_id
      order by t.fixture_id asc
    """

    with conn.cursor() as cur:
        cur.execute(sql, params)
        column_names = [desc[0] for desc in cur.description]
        rows = cur.fetchall()

    output: List[Dict[str, Any]] = []
    for row in rows:
        output.append(dict(zip(column_names, row)))
    return output


def build_windows_payload(row: Dict[str, Any]) -> Dict[str, Any]:
    def pack(window: int) -> Dict[str, Any]:
        return {
            "sample": to_int(row.get(f"sample_{window}")),
            "avg_cards": to_float(row.get(f"avg_cards_{window}")),
            "avg_total_cards": to_float(row.get(f"avg_total_cards_{window}")),
            "avg_red_cards": to_float(row.get(f"avg_red_cards_{window}")),
            "games_with_3plus_cards_pct": to_float(row.get(f"pct_3plus_{window}")),
            "games_with_4plus_cards_pct": to_float(row.get(f"pct_4plus_{window}")),
            "games_with_5plus_cards_pct": to_float(row.get(f"pct_5plus_{window}")),
            "games_with_red_card_pct": to_float(row.get(f"pct_red_{window}")),
        }

    return {
        "5": pack(5),
        "10": pack(10),
        "20": pack(20),
    }


def upsert_fixture_referee_stats(conn: psycopg2.extensions.connection, rows: Iterable[Dict[str, Any]]) -> int:
    values: List[Tuple[Any, ...]] = []

    for row in rows:
        windows_payload = build_windows_payload(row)
        sample_5 = to_int(row.get("sample_5"))
        sample_10 = to_int(row.get("sample_10"))
        sample_20 = to_int(row.get("sample_20"))

        values.append(
            (
                to_int(row.get("fixture_id")),
                to_int(row.get("referee_id")),
                (str(row.get("referee_name") or "").strip() or f"Referee {to_int(row.get('referee_id'))}"),
                to_float(row.get("avg_cards_20")),
                None,  # avg_fouls deprecated for referee panel
                to_float(row.get("pct_3plus_20")),
                to_float(row.get("pct_red_20")),
                None,  # avg_corners deprecated for referee panel
                sample_20,
                "db_derived",
                to_float(row.get("avg_total_cards_20")),
                to_float(row.get("pct_4plus_20")),
                to_float(row.get("pct_5plus_20")),
                sample_5,
                sample_10,
                sample_20,
                Json(windows_payload),
            )
        )

    if not values:
        return 0

    sql = """
      insert into public.fixture_referee_stats (
        fixture_id,
        referee_id,
        referee_name,
        avg_yellow_cards,
        avg_fouls,
        games_with_3plus_cards_pct,
        games_with_red_card_pct,
        avg_corners,
        sample,
        source,
        avg_total_cards,
        games_with_4plus_cards_pct,
        games_with_5plus_cards_pct,
        sample_5,
        sample_10,
        sample_20,
        windows
      )
      values %s
      on conflict (fixture_id) do update
      set
        referee_id = excluded.referee_id,
        referee_name = excluded.referee_name,
        avg_yellow_cards = excluded.avg_yellow_cards,
        avg_fouls = excluded.avg_fouls,
        games_with_3plus_cards_pct = excluded.games_with_3plus_cards_pct,
        games_with_red_card_pct = excluded.games_with_red_card_pct,
        avg_corners = excluded.avg_corners,
        sample = excluded.sample,
        source = excluded.source,
        avg_total_cards = excluded.avg_total_cards,
        games_with_4plus_cards_pct = excluded.games_with_4plus_cards_pct,
        games_with_5plus_cards_pct = excluded.games_with_5plus_cards_pct,
        sample_5 = excluded.sample_5,
        sample_10 = excluded.sample_10,
        sample_20 = excluded.sample_20,
        windows = excluded.windows,
        updated_at = now()
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, values, page_size=500)
    conn.commit()
    return len(values)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_args()

    db_url = get_db_url()
    conn = psycopg2.connect(db_url)
    try:
        rows = fetch_fixture_referee_metrics(
            conn,
            days_back=args.days_back,
            days_forward=args.days_forward,
            limit_fixtures=args.limit_fixtures,
        )
    finally:
        conn.close()

    report: Dict[str, Any] = {
        "ok": True,
        "dry_run": bool(args.dry_run),
        "rows_to_upsert": len(rows),
        "fixtures_with_ref_assignment": sum(1 for row in rows if to_int(row.get("referee_id")) > 0),
        "fixtures_with_history_sample_5": sum(1 for row in rows if to_int(row.get("sample_5")) > 0),
        "fixtures_with_history_sample_10": sum(1 for row in rows if to_int(row.get("sample_10")) > 0),
        "fixtures_with_history_sample_20": sum(1 for row in rows if to_int(row.get("sample_20")) > 0),
        "sample_fixture_ids": [to_int(row.get("fixture_id")) for row in rows[:10]],
        "sample_referee_ids": [to_int(row.get("referee_id")) for row in rows[:10]],
    }

    if args.dry_run:
        print(json.dumps(report, indent=2, ensure_ascii=False))
        write_report(args.report_json, report)
        return 0

    conn_write = psycopg2.connect(db_url)
    try:
        upserted = upsert_fixture_referee_stats(conn_write, rows)
    finally:
        conn_write.close()

    report["upserted"] = upserted
    print(json.dumps(report, indent=2, ensure_ascii=False))
    write_report(args.report_json, report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
