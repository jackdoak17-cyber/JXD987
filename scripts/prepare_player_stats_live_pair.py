"""Print, but never execute, a bounded read-only live parity check.

Run `fingerprint` first and inspect the current function fingerprint and
Premier League season. Supply both observed values to `pair` only after a
coordinated validation slot has been assigned.
"""

import argparse
from pathlib import Path
import re


ROOT = Path(__file__).resolve().parents[1]
CANDIDATE = ROOT / "supabase/migrations/20261121232000_player_stats_season_page_before_metrics.sql"
SIGNATURE = "public.player_stats_season_table(integer,integer,bigint,text,numeric,text,text,text,boolean,integer,integer)"


def fingerprint_sql():
    return f"""begin transaction isolation level repeatable read read only;
set local statement_timeout = '5s';
set local lock_timeout = '1s';
select md5(pg_get_functiondef('{SIGNATURE}'::regprocedure)) as live_function_fingerprint,
       length(pg_get_functiondef('{SIGNATURE}'::regprocedure)) as function_characters,
       (select id from public.seasons
         where league_id = 8 and start_date <= current_date
         order by start_date desc, id desc limit 1) as current_pl_season_id;
rollback;
"""


def candidate_select(season_id):
    text = CANDIDATE.read_text()
    body = text.split("as $$\n", 1)[1].rsplit("$$;", 1)[0].strip().removesuffix(";")
    values = {
        "p_league_id": "8::integer",
        "p_season_id": f"{season_id}::integer",
        "p_team_id": "null::bigint",
        "p_position_group": "null::text",
        "p_min_minutes": "0::numeric",
        "p_search": "null::text",
        "p_sort_key": "'goals'::text",
        "p_sort_measure": "'total'::text",
        "p_sort_desc": "true::boolean",
        "p_limit": "50::integer",
        "p_offset": "0::integer",
    }
    for name, value in values.items():
        body, count = re.subn(rf"\b{name}\b", value, body)
        if not count:
            raise ValueError(f"Missing candidate parameter {name}")
    if re.search(r"\bp_[a-z_]+\b", body):
        raise ValueError("Unbound candidate parameter")
    return body


def pair_sql(season_id, fingerprint):
    if season_id <= 0:
        raise ValueError("season_id must be positive")
    if not re.fullmatch(r"[0-9a-f]{32}", fingerprint):
        raise ValueError("expected_fingerprint must be a 32-character lowercase MD5")

    candidate = candidate_select(season_id)
    return f"""-- Execute only in the lead-coordinated validation slot.
-- One REPEATABLE READ snapshot; two separate 5-second statements.
-- Comparing JSONB arrays compares every column and nested metric in rank order.
begin transaction isolation level repeatable read read only;
set local statement_timeout = '5s';
set local lock_timeout = '1s';
select 1 / case when
  md5(pg_get_functiondef('{SIGNATURE}'::regprocedure)) = '{fingerprint}'
  then 1 else 0 end as unchanged_live_function_guard;

select set_config('oddsstats.started', clock_timestamp()::text, true);
with old_payload as materialized (
  select coalesce(jsonb_agg(to_jsonb(row) order by row.rank), '[]'::jsonb) as payload
  from public.player_stats_season_table(
    p_league_id => 8, p_season_id => {season_id},
    p_team_id => null, p_position_group => null, p_min_minutes => 0,
    p_search => null, p_sort_key => 'goals', p_sort_measure => 'total',
    p_sort_desc => true, p_limit => 50, p_offset => 0
  ) row
), saved as materialized (
  select set_config('oddsstats.old_json', payload::text, true) as full_rows
  from old_payload
)
select jsonb_array_length(full_rows::jsonb) as old_rows,
       set_config('oddsstats.old_ms',
         (extract(epoch from clock_timestamp() -
           current_setting('oddsstats.started')::timestamptz) * 1000)::text, true)
         as old_elapsed_ms
from saved;

select set_config('oddsstats.started', clock_timestamp()::text, true);
with candidate_rows (
  rank, player_id, player_name, player_display_name, player_common_name,
  player_short_name, player_image_path, team_id, team_name, team_short_code,
  team_image_path, position_group, appearances, starts, minutes, metrics
) as materialized (
{candidate}
), candidate_payload as materialized (
  select coalesce(jsonb_agg(to_jsonb(row) order by row.rank), '[]'::jsonb) as payload
  from candidate_rows row
)
select current_setting('oddsstats.old_ms')::numeric as old_elapsed_ms,
       round((extract(epoch from clock_timestamp() -
         current_setting('oddsstats.started')::timestamptz) * 1000)::numeric, 2)
         as candidate_elapsed_ms,
       jsonb_array_length(current_setting('oddsstats.old_json')::jsonb) as old_rows,
       jsonb_array_length(payload) as candidate_rows,
       payload = current_setting('oddsstats.old_json')::jsonb as exact_ordered_full_rows_equal,
       md5(current_setting('oddsstats.old_json')) as old_digest,
       md5(payload::text) as candidate_digest
from candidate_payload;
rollback;
"""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="action", required=True)
    subparsers.add_parser("fingerprint")
    pair = subparsers.add_parser("pair")
    pair.add_argument("--season-id", type=int, required=True)
    pair.add_argument("--expected-fingerprint", required=True)
    args = parser.parse_args()
    print(fingerprint_sql() if args.action == "fingerprint" else
          pair_sql(args.season_id, args.expected_fingerprint))


if __name__ == "__main__":
    main()
