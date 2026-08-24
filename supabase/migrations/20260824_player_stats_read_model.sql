-- Canonical player-season read model for the customer-facing player stats page.
-- Raw fixture tables remain the source of truth. These tables make the
-- aggregation semantics explicit, incremental refreshes idempotent, and API
-- queries independent of the provider's sparse event-row representation.

create table if not exists public.player_stats_metric_contract (
  metric_key text primary key,
  label text not null,
  group_name text not null check (group_name in ('offensive', 'defensive', 'overall')),
  value_semantics text not null check (value_semantics in ('count', 'rating')),
  aggregation text not null check (aggregation in ('sum', 'average')),
  source_type_ids integer[] not null,
  position_groups text[] not null default array['GK', 'DEF', 'MID', 'FWD'],
  zero_safe boolean not null default false,
  supports_per90 boolean not null default true,
  featured boolean not null default false,
  enabled boolean not null default true,
  updated_at timestamptz not null default now()
);

insert into public.player_stats_metric_contract (
  metric_key, label, group_name, value_semantics, aggregation,
  source_type_ids, position_groups, zero_safe, supports_per90, featured
) values
  ('goals', 'Goals', 'offensive', 'count', 'sum', array[52], default, true, true, true),
  ('assists', 'Assists', 'offensive', 'count', 'sum', array[79], default, true, true, true),
  -- Goal contributions are deterministically derived from goals + assists.
  ('goal_contributions', 'Goal contributions', 'offensive', 'count', 'sum', array[52, 79], default, true, true, true),
  ('shots', 'Shots', 'offensive', 'count', 'sum', array[42], default, true, true, true),
  ('shots_on_target', 'Shots on target', 'offensive', 'count', 'sum', array[86], default, true, true, true),
  ('big_chances_created', 'Big chances created', 'offensive', 'count', 'sum', array[580], default, true, true, true),
  ('successful_dribbles', 'Successful dribbles', 'offensive', 'count', 'sum', array[109], default, true, true, true),
  ('key_passes', 'Key passes', 'offensive', 'count', 'sum', array[117], default, true, true, true),
  ('chances_created', 'Chances created', 'offensive', 'count', 'sum', array[9706], default, true, true, true),
  ('fouls_drawn', 'Fouls drawn', 'offensive', 'count', 'sum', array[96], default, true, true, true),
  ('tackles', 'Tackles', 'defensive', 'count', 'sum', array[78], default, true, true, true),
  ('tackles_won', 'Tackles won', 'defensive', 'count', 'sum', array[27267], default, true, true, true),
  ('interceptions', 'Interceptions', 'defensive', 'count', 'sum', array[100], default, true, true, true),
  ('clearances', 'Clearances', 'defensive', 'count', 'sum', array[101], default, true, true, true),
  ('blocked_shots', 'Blocked shots', 'defensive', 'count', 'sum', array[97], default, true, true, true),
  ('fouls_committed', 'Fouls committed', 'defensive', 'count', 'sum', array[56], default, true, true, true),
  ('cards', 'Cards', 'defensive', 'count', 'sum', array[83, 84, 85], default, true, true, true),
  ('saves', 'Saves', 'defensive', 'count', 'sum', array[57], array['GK'], true, true, true),
  ('goals_conceded', 'Goals conceded', 'defensive', 'count', 'sum', array[88], default, false, true, true),
  ('rating', 'Rating', 'overall', 'rating', 'average', array[118], default, false, false, true)
on conflict (metric_key) do update set
  label = excluded.label,
  group_name = excluded.group_name,
  value_semantics = excluded.value_semantics,
  aggregation = excluded.aggregation,
  source_type_ids = excluded.source_type_ids,
  position_groups = excluded.position_groups,
  zero_safe = excluded.zero_safe,
  supports_per90 = excluded.supports_per90,
  featured = excluded.featured,
  enabled = excluded.enabled,
  updated_at = now();

create table if not exists public.player_stats_season_summary (
  league_id bigint not null,
  season_id bigint not null,
  player_id bigint not null,
  team_id bigint not null,
  position_group text not null,
  appearances integer not null check (appearances >= 0),
  starts integer not null check (starts >= 0),
  minutes numeric not null check (minutes >= 0),
  last_appearance_at timestamptz,
  source_revision text not null,
  updated_at timestamptz not null default now(),
  primary key (league_id, season_id, player_id, team_id)
);

create table if not exists public.player_stats_season_metric (
  league_id bigint not null,
  season_id bigint not null,
  player_id bigint not null,
  team_id bigint not null,
  metric_key text not null references public.player_stats_metric_contract(metric_key),
  total_value numeric,
  average_value numeric,
  per90_value numeric,
  eligible_appearances integer not null default 0 check (eligible_appearances >= 0),
  reported_appearances integer not null default 0 check (reported_appearances >= 0),
  zero_safe_appearances integer not null default 0 check (zero_safe_appearances >= 0),
  unknown_appearances integer not null default 0 check (unknown_appearances >= 0),
  not_applicable_appearances integer not null default 0 check (not_applicable_appearances >= 0),
  data_state text not null check (data_state in ('ready', 'partial', 'unavailable')),
  source_revision text not null,
  updated_at timestamptz not null default now(),
  primary key (league_id, season_id, player_id, team_id, metric_key)
);

create index if not exists player_stats_summary_scope_idx
  on public.player_stats_season_summary (league_id, season_id, team_id, position_group, minutes desc, player_id);
create index if not exists player_stats_summary_player_idx
  on public.player_stats_season_summary (league_id, season_id, player_id, last_appearance_at desc);
create index if not exists player_stats_metric_scope_idx
  on public.player_stats_season_metric (league_id, season_id, metric_key, data_state, player_id);

alter table public.player_stats_metric_contract enable row level security;
alter table public.player_stats_season_summary enable row level security;
alter table public.player_stats_season_metric enable row level security;
revoke all on table public.player_stats_metric_contract from public, anon, authenticated;
revoke all on table public.player_stats_season_summary from public, anon, authenticated;
revoke all on table public.player_stats_season_metric from public, anon, authenticated;
grant select, insert, update, delete on table public.player_stats_metric_contract to service_role;
grant select, insert, update, delete on table public.player_stats_season_summary to service_role;
grant select, insert, update, delete on table public.player_stats_season_metric to service_role;

create or replace function public.refresh_player_stats_season(
  p_league_id integer,
  p_season_id integer,
  p_player_id bigint default null
)
returns integer
language plpgsql
volatile
set search_path to 'pg_catalog', 'public'
as $$
declare
  v_rows integer := 0;
begin
  delete from public.player_stats_season_metric
   where league_id = p_league_id
     and season_id = p_season_id
     and (p_player_id is null or player_id = p_player_id);
  delete from public.player_stats_season_summary
   where league_id = p_league_id
     and season_id = p_season_id
     and (p_player_id is null or player_id = p_player_id);

  with eligible_appearances as (
    select
      f.league_id,
      f.season_id,
      f.id as fixture_id,
      f.starting_at,
      case when fp.team_id = f.home_team_id then f.away_score else f.home_score end::numeric as team_goals_conceded,
      fp.player_id,
      fp.team_id,
      coalesce(fp.minutes_played, 0)::numeric as minutes_played,
      coalesce(fp.is_starter, false) as is_starter,
      case
        when upper(coalesce(fp.position_abbr, '')) = 'GK'
          or lower(coalesce(fp.detailed_position_code, fp.detailed_position_name, fp.position_name, '')) like '%goalkeeper%'
          then 'GK'
        when lower(coalesce(fp.detailed_position_code, fp.detailed_position_name, fp.position_name, '')) like '%defend%'
          or upper(coalesce(fp.position_abbr, '')) in ('CB', 'LB', 'RB', 'LWB', 'RWB')
          then 'DEF'
        when lower(coalesce(fp.detailed_position_code, fp.detailed_position_name, fp.position_name, '')) like '%mid%'
          or upper(coalesce(fp.position_abbr, '')) in ('LM', 'RM', 'CM', 'DM', 'AM')
          then 'MID'
        else 'FWD'
      end as position_group
    from public.fixtures f
    join public.fixture_players fp on fp.fixture_id = f.id
    where f.league_id = p_league_id
      and f.season_id = p_season_id
      and f.home_score is not null
      and f.away_score is not null
      and fp.team_id is not null
      and coalesce(fp.minutes_played, 0) > 0
      and (p_player_id is null or fp.player_id = p_player_id)
  ),
  summary_rows as (
    select
      league_id,
      season_id,
      player_id,
      team_id,
      (array_agg(position_group order by starting_at desc, fixture_id desc))[1] as position_group,
      count(*)::integer as appearances,
      count(*) filter (where is_starter)::integer as starts,
      sum(minutes_played) as minutes,
      max(starting_at) as last_appearance_at,
      md5(concat(max(starting_at), ':', count(*), ':', sum(minutes_played))) as source_revision
    from eligible_appearances
    group by league_id, season_id, player_id, team_id
  )
  insert into public.player_stats_season_summary (
    league_id, season_id, player_id, team_id, position_group,
    appearances, starts, minutes, last_appearance_at, source_revision, updated_at
  )
  select league_id, season_id, player_id, team_id, position_group,
         appearances, starts, minutes, last_appearance_at, source_revision, now()
    from summary_rows;

  with eligible_appearances as (
    select
      f.league_id,
      f.season_id,
      f.id as fixture_id,
      f.starting_at,
      case when fp.team_id = f.home_team_id then f.away_score else f.home_score end::numeric as team_goals_conceded,
      fp.player_id,
      fp.team_id,
      coalesce(fp.minutes_played, 0)::numeric as minutes_played,
      case
        when upper(coalesce(fp.position_abbr, '')) = 'GK'
          or lower(coalesce(fp.detailed_position_code, fp.detailed_position_name, fp.position_name, '')) like '%goalkeeper%'
          then 'GK'
        when lower(coalesce(fp.detailed_position_code, fp.detailed_position_name, fp.position_name, '')) like '%defend%'
          or upper(coalesce(fp.position_abbr, '')) in ('CB', 'LB', 'RB', 'LWB', 'RWB')
          then 'DEF'
        when lower(coalesce(fp.detailed_position_code, fp.detailed_position_name, fp.position_name, '')) like '%mid%'
          or upper(coalesce(fp.position_abbr, '')) in ('LM', 'RM', 'CM', 'DM', 'AM')
          then 'MID'
        else 'FWD'
      end as position_group
    from public.fixtures f
    join public.fixture_players fp on fp.fixture_id = f.id
    where f.league_id = p_league_id
      and f.season_id = p_season_id
      and f.home_score is not null
      and f.away_score is not null
      and fp.team_id is not null
      and coalesce(fp.minutes_played, 0) > 0
      and (p_player_id is null or fp.player_id = p_player_id)
  ),
  joined_values as (
    select
      a.*,
      c.metric_key,
      c.aggregation,
      c.zero_safe,
      c.position_groups,
      count(fps.type_id) > 0 as source_present,
      coalesce(sum(fps.value), 0)::numeric as source_sum,
      max(fps.value)::numeric as source_max
    from eligible_appearances a
    cross join public.player_stats_metric_contract c
    left join public.fixture_player_statistics fps
      on fps.fixture_id = a.fixture_id
     and fps.player_id = a.player_id
     and fps.type_id = any(c.source_type_ids)
    where c.enabled
    group by a.league_id, a.season_id, a.fixture_id, a.starting_at,
             a.team_goals_conceded, a.player_id, a.team_id, a.minutes_played,
             a.position_group, c.metric_key, c.aggregation, c.zero_safe,
             c.position_groups
  ),
  states as (
    select
      j.*,
      case
        when not (j.position_groups @> array[j.position_group]) then 'not_applicable'
        when j.metric_key = 'goals_conceded' and not j.source_present and j.team_goals_conceded = 0 then 'zero_safe'
        when j.source_present then 'reported'
        when j.zero_safe then 'zero_safe'
        else 'unknown'
      end as value_state,
      case
        when not (j.position_groups @> array[j.position_group]) then null
        when j.source_present and j.aggregation = 'average' then j.source_max
        when j.source_present then j.source_sum
        when j.metric_key = 'goals_conceded' and j.team_goals_conceded = 0 then 0::numeric
        when j.zero_safe then 0::numeric
        else null
      end as value
    from joined_values j
  ),
  metric_rows as (
    select
      league_id,
      season_id,
      player_id,
      team_id,
      metric_key,
      sum(value) filter (where value_state in ('reported', 'zero_safe')) as total_value,
      count(*) filter (where value_state in ('reported', 'zero_safe'))::integer as observed_appearances,
      count(*) filter (where value_state not in ('not_applicable'))::integer as eligible_appearances,
      count(*) filter (where value_state = 'reported')::integer as reported_appearances,
      count(*) filter (where value_state = 'zero_safe')::integer as zero_safe_appearances,
      count(*) filter (where value_state = 'unknown')::integer as unknown_appearances,
      count(*) filter (where value_state = 'not_applicable')::integer as not_applicable_appearances,
      md5(concat(max(starting_at), ':', count(*), ':', sum(minutes_played))) as source_revision
    from states
    group by league_id, season_id, player_id, team_id, metric_key
  )
  insert into public.player_stats_season_metric (
    league_id, season_id, player_id, team_id, metric_key,
    total_value, average_value, per90_value,
    eligible_appearances, reported_appearances, zero_safe_appearances,
    unknown_appearances, not_applicable_appearances, data_state,
    source_revision, updated_at
  )
  select
    m.league_id,
    m.season_id,
    m.player_id,
    m.team_id,
    m.metric_key,
    m.total_value,
    case when m.observed_appearances > 0 and m.unknown_appearances = 0
      then m.total_value / m.observed_appearances else null end,
    case when m.observed_appearances > 0 and m.unknown_appearances = 0 and s.minutes > 0
      then m.total_value / s.minutes * 90 else null end,
    m.eligible_appearances,
    m.reported_appearances,
    m.zero_safe_appearances,
    m.unknown_appearances,
    m.not_applicable_appearances,
    case
      when m.eligible_appearances = 0 or m.observed_appearances = 0 then 'unavailable'
      when m.unknown_appearances > 0 then 'partial'
      else 'ready'
    end,
    m.source_revision,
    now()
  from metric_rows m
  join public.player_stats_season_summary s
    on s.league_id = m.league_id
   and s.season_id = m.season_id
   and s.player_id = m.player_id
   and s.team_id = m.team_id;

  get diagnostics v_rows = row_count;
  return v_rows;
end;
$$;

create or replace function public.refresh_player_stats_for_fixture(p_fixture_id bigint)
returns integer
language plpgsql
volatile
set search_path to 'pg_catalog', 'public'
as $$
declare
  v_league_id integer;
  v_season_id integer;
  v_rows integer := 0;
begin
  select f.league_id::integer, f.season_id::integer
    into v_league_id, v_season_id
    from public.fixtures f
   where f.id = p_fixture_id;
  if v_league_id is null or v_season_id is null then
    return 0;
  end if;
  -- The authoritative fixture export replaces the fixture's lineup rows.
  -- Rebuilding the complete season is intentional: it removes projections for
  -- players that a provider correction removed from this fixture, while also
  -- preserving transfer stints and making the operation idempotent.
  v_rows := public.refresh_player_stats_season(v_league_id, v_season_id, null);
  return v_rows;
end;
$$;

create or replace function public.player_stats_season_table(
  p_league_id integer,
  p_season_id integer,
  p_team_id bigint default null,
  p_position_group text default null,
  p_min_minutes numeric default 90,
  p_search text default null,
  p_sort_key text default 'goals',
  p_sort_measure text default 'per90',
  p_sort_desc boolean default true,
  p_limit integer default 50,
  p_offset integer default 0
)
returns table (
  rank bigint,
  player_id bigint,
  player_name text,
  player_display_name text,
  player_common_name text,
  player_short_name text,
  player_image_path text,
  team_id bigint,
  team_name text,
  team_short_code text,
  team_image_path text,
  position_group text,
  appearances integer,
  starts integer,
  minutes numeric,
  metrics jsonb
)
language sql
stable
set search_path to 'pg_catalog', 'public'
as $$
  with scoped_summary as (
    select
      s.player_id,
      sum(s.appearances)::integer as appearances,
      sum(s.starts)::integer as starts,
      sum(s.minutes) as minutes,
      max(s.last_appearance_at) as last_appearance_at
    from public.player_stats_season_summary s
    where s.league_id = p_league_id
      and s.season_id = p_season_id
      and (p_team_id is null or s.team_id = p_team_id)
      and (p_position_group is null or s.position_group = p_position_group)
    group by s.player_id
    having sum(s.minutes) >= greatest(coalesce(p_min_minutes, 0), 0)
  ),
  latest_team as (
    select distinct on (s.player_id)
      s.player_id, s.team_id, s.position_group
    from public.player_stats_season_summary s
    join scoped_summary ss on ss.player_id = s.player_id
    where s.league_id = p_league_id
      and s.season_id = p_season_id
      and (p_team_id is null or s.team_id = p_team_id)
      and (p_position_group is null or s.position_group = p_position_group)
    order by s.player_id, s.last_appearance_at desc, s.team_id desc
  ),
  metric_scope as materialized (
    -- Materialize the bounded season/team slice before joining it to the
    -- player scope. This avoids a generic-plan nested loop over the metric
    -- index when the optional team filter is null.
    select m.*
    from public.player_stats_season_metric m
    where m.league_id = p_league_id
      and m.season_id = p_season_id
      and (p_team_id is null or m.team_id = p_team_id)
  ),
  metric_aggregate as materialized (
    select
      m.player_id,
      m.metric_key,
      c.supports_per90,
      sum(m.total_value) as total_value,
      sum(m.eligible_appearances)::integer as eligible_appearances,
      sum(m.reported_appearances)::integer as reported_appearances,
      sum(m.zero_safe_appearances)::integer as zero_safe_appearances,
      sum(m.unknown_appearances)::integer as unknown_appearances,
      sum(m.not_applicable_appearances)::integer as not_applicable_appearances,
      case when sum(m.unknown_appearances) > 0 then 'partial'
           when sum(m.eligible_appearances) = 0 or sum(m.reported_appearances + m.zero_safe_appearances) = 0 then 'unavailable'
           else 'ready' end as data_state,
      case when sum(m.unknown_appearances) = 0 and sum(m.reported_appearances + m.zero_safe_appearances) > 0
        then sum(m.total_value) / nullif(sum(m.reported_appearances + m.zero_safe_appearances), 0) end as average_value,
      case when c.supports_per90 and sum(m.unknown_appearances) = 0 and sum(m.reported_appearances + m.zero_safe_appearances) > 0 and ss.minutes > 0
        then sum(m.total_value) / ss.minutes * 90 end as per90_value
    from metric_scope m
    join public.player_stats_metric_contract c on c.metric_key = m.metric_key
    join scoped_summary ss on ss.player_id = m.player_id
    group by m.player_id, m.metric_key, c.supports_per90, ss.minutes
  ),
  shaped as (
    select
      ss.player_id,
      lt.team_id,
      lt.position_group,
      ss.appearances,
      ss.starts,
      ss.minutes,
      ss.last_appearance_at,
      jsonb_object_agg(
        ma.metric_key,
        jsonb_build_object(
          'total', ma.total_value,
          'average', ma.average_value,
          'per90', ma.per90_value,
          'eligibleAppearances', ma.eligible_appearances,
          'reportedAppearances', ma.reported_appearances,
          'zeroSafeAppearances', ma.zero_safe_appearances,
          'unknownAppearances', ma.unknown_appearances,
          'notApplicableAppearances', ma.not_applicable_appearances,
          'status', ma.data_state
        )
      ) as metrics,
      case p_sort_key
        when 'appearances' then ss.appearances::numeric
        when 'starts' then ss.starts::numeric
        when 'minutes' then ss.minutes
        else null
      end as sort_base,
      max(ma.per90_value) filter (where ma.metric_key = p_sort_key and p_sort_measure = 'per90') as sort_per90,
      max(ma.average_value) filter (where ma.metric_key = p_sort_key and p_sort_measure = 'average') as sort_average,
      max(ma.total_value) filter (where ma.metric_key = p_sort_key and p_sort_measure = 'total') as sort_total
    from scoped_summary ss
    join latest_team lt on lt.player_id = ss.player_id
    join metric_aggregate ma on ma.player_id = ss.player_id
    group by ss.player_id, lt.team_id, lt.position_group, ss.appearances,
             ss.starts, ss.minutes, ss.last_appearance_at
  ),
  filtered as (
    select sh.*
      from shaped sh
      join public.players p on p.id = sh.player_id
     where p_search is null or trim(p_search) = '' or
       lower(coalesce(p.display_name, p.name, p.common_name, p.short_name, '')) like '%' || lower(trim(p_search)) || '%'
  ),
  ranked as (
    select
      row_number() over (
        order by
          case when p_sort_desc and p_sort_measure = 'per90' then sort_per90 end desc nulls last,
          case when p_sort_desc and p_sort_measure = 'average' then sort_average end desc nulls last,
          case when p_sort_desc and p_sort_measure = 'total' then sort_total end desc nulls last,
          case when p_sort_desc and p_sort_measure = 'base' then sort_base end desc nulls last,
          case when not p_sort_desc and p_sort_measure = 'per90' then sort_per90 end asc nulls last,
          case when not p_sort_desc and p_sort_measure = 'average' then sort_average end asc nulls last,
          case when not p_sort_desc and p_sort_measure = 'total' then sort_total end asc nulls last,
          case when not p_sort_desc and p_sort_measure = 'base' then sort_base end asc nulls last,
          minutes desc,
          player_id
      ) as rank,
      f.*
    from filtered f
  )
  select
    r.rank,
    r.player_id,
    p.name,
    p.display_name,
    p.common_name,
    p.short_name,
    p.image_path,
    r.team_id,
    t.name,
    t.short_code,
    t.image_path,
    r.position_group,
    r.appearances,
    r.starts,
    r.minutes,
    r.metrics
  from ranked r
  join public.players p on p.id = r.player_id
  left join public.teams t on t.id = r.team_id
  where r.rank > greatest(coalesce(p_offset, 0), 0)
    and r.rank <= greatest(coalesce(p_offset, 0), 0) + least(greatest(coalesce(p_limit, 50), 1), 100)
  order by r.rank;
$$;

create or replace function public.player_stats_metric_catalog()
returns table (
  metric_key text,
  label text,
  group_name text,
  value_semantics text,
  supports_per90 boolean,
  featured boolean,
  source_type_ids integer[],
  position_groups text[]
)
language sql
stable
set search_path to 'pg_catalog', 'public'
as $$
  select metric_key, label, group_name, value_semantics,
         supports_per90, featured, source_type_ids, position_groups
    from public.player_stats_metric_contract
   where enabled
   order by case group_name when 'overall' then 1 when 'offensive' then 2 when 'defensive' then 3 else 4 end,
            featured desc, label;
$$;
