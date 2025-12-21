create or replace function public.team_clean_sheets(
  p_league_id integer,
  p_season_id integer default null,
  p_last_n integer default null,
  p_home_only boolean default false,
  p_away_only boolean default false,
  p_team_id integer default null,
  p_limit integer default 20
)
returns table (
  team_id integer,
  team_name text,
  team_short_code text,
  team_image_path text,
  games integer,
  clean_sheets integer,
  clean_sheet_pct numeric,
  last_fixture_at timestamptz,
  last_flags boolean[]
)
language sql
stable
as $$
  with fixtures_base as (
    select
      f.id,
      f.starting_at,
      f.home_team_id,
      f.away_team_id,
      f.home_score,
      f.away_score
    from fixtures f
    where f.league_id = p_league_id
      and (p_season_id is null or f.season_id = p_season_id)
      and f.home_score is not null
      and f.away_score is not null
  ),
  appearances as (
    select
      fixtures_base.id as fixture_id,
      fixtures_base.starting_at,
      fixtures_base.home_team_id as team_id,
      (fixtures_base.away_score = 0) as clean_sheet,
      true as is_home
    from fixtures_base
    union all
    select
      fixtures_base.id as fixture_id,
      fixtures_base.starting_at,
      fixtures_base.away_team_id as team_id,
      (fixtures_base.home_score = 0) as clean_sheet,
      false as is_home
    from fixtures_base
  ),
  filtered as (
    select *
    from appearances
    where (not p_home_only or is_home)
      and (not p_away_only or not is_home)
      and (p_team_id is null or team_id = p_team_id)
  ),
  ranked as (
    select
      filtered.*,
      row_number() over (
        partition by team_id
        order by starting_at desc, fixture_id desc
      ) as rn
    from filtered
  ),
  windowed as (
    select *
    from ranked
    where p_last_n is null or rn <= p_last_n
  ),
  agg as (
    select
      team_id,
      count(*) as games,
      sum(case when clean_sheet then 1 else 0 end) as clean_sheets,
      array_agg(clean_sheet order by starting_at desc, fixture_id desc) as last_flags,
      max(starting_at) as last_fixture_at
    from windowed
    group by team_id
  )
  select
    agg.team_id::integer,
    t.name::text as team_name,
    t.short_code::text as team_short_code,
    t.image_path::text as team_image_path,
    agg.games::integer as games,
    agg.clean_sheets::integer as clean_sheets,
    case when agg.games > 0
      then agg.clean_sheets::numeric / agg.games
      else 0
    end as clean_sheet_pct,
    agg.last_fixture_at as last_fixture_at,
    agg.last_flags::boolean[] as last_flags
  from agg
  left join teams t on t.id = agg.team_id
  where p_last_n is null or agg.games = p_last_n
  order by agg.clean_sheets desc, clean_sheet_pct desc
  limit p_limit;
$$;

create or replace function public.team_clean_sheets_meta(
  p_league_id integer,
  p_season_id integer default null,
  p_last_n integer default null,
  p_home_only boolean default false,
  p_away_only boolean default false,
  p_team_id integer default null,
  p_required integer default null
)
returns table (
  candidate_teams_count integer,
  complete_windows_count integer,
  qualified_teams_count integer
)
language sql
stable
as $$
  with fixtures_base as (
    select
      f.id,
      f.starting_at,
      f.home_team_id,
      f.away_team_id,
      f.home_score,
      f.away_score
    from fixtures f
    where f.league_id = p_league_id
      and (p_season_id is null or f.season_id = p_season_id)
      and f.home_score is not null
      and f.away_score is not null
  ),
  appearances as (
    select
      fixtures_base.id as fixture_id,
      fixtures_base.starting_at,
      fixtures_base.home_team_id as team_id,
      (fixtures_base.away_score = 0) as clean_sheet,
      true as is_home
    from fixtures_base
    union all
    select
      fixtures_base.id as fixture_id,
      fixtures_base.starting_at,
      fixtures_base.away_team_id as team_id,
      (fixtures_base.home_score = 0) as clean_sheet,
      false as is_home
    from fixtures_base
  ),
  filtered as (
    select *
    from appearances
    where (not p_home_only or is_home)
      and (not p_away_only or not is_home)
      and (p_team_id is null or team_id = p_team_id)
  ),
  candidate_teams as (
    select team_id
    from filtered
    group by team_id
    having p_last_n is null or count(*) >= p_last_n
  ),
  ranked as (
    select
      filtered.*,
      row_number() over (
        partition by team_id
        order by starting_at desc, fixture_id desc
      ) as rn
    from filtered
  ),
  windowed as (
    select *
    from ranked
    where p_last_n is null or rn <= p_last_n
  ),
  agg as (
    select
      team_id,
      count(*) as games,
      sum(case when clean_sheet then 1 else 0 end) as clean_sheets
    from windowed
    group by team_id
  ),
  complete as (
    select *
    from agg
    where p_last_n is null or games = p_last_n
  )
  select
    (select count(*) from candidate_teams)::integer as candidate_teams_count,
    (select count(*) from complete)::integer as complete_windows_count,
    (select count(*) from complete where p_required is null or clean_sheets >= p_required)::integer
      as qualified_teams_count;
$$;
