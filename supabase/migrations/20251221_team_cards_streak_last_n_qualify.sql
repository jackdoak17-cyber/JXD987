create or replace function public.team_cards_streak_last_n(
  p_league_id integer,
  p_n integer,
  p_threshold numeric,
  p_required integer,
  p_season_id integer default null,
  p_limit integer default 20
)
returns table (
  team_id integer,
  team_name text,
  team_short_code text,
  team_image_path text,
  games integer,
  games_hit integer,
  avg_value numeric,
  last_values numeric[]
)
language sql
stable
as $$
  with cards_per_team_fixture as (
    select
      fs.fixture_id as fixture_id,
      fs.team_id as team_id,
      f.starting_at as starting_at,
      sum(
        case
          when fs.type_id in (83, 84, 85) then fs.value::numeric
          else 0
        end
      ) as cards
    from fixture_statistics fs
    join fixtures f on f.id = fs.fixture_id
    where f.league_id = p_league_id
      and (p_season_id is null or f.season_id = p_season_id)
      and f.home_score is not null
      and f.away_score is not null
    group by fs.fixture_id, fs.team_id, f.starting_at
  ),
  ranked as (
    select
      ctf.fixture_id,
      ctf.team_id,
      ctf.starting_at,
      ctf.cards,
      row_number() over (
        partition by ctf.team_id
        order by ctf.starting_at desc, ctf.fixture_id desc
      ) as rn
    from cards_per_team_fixture ctf
  ),
  windowed as (
    select
      ranked.fixture_id,
      ranked.team_id,
      ranked.starting_at,
      ranked.cards
    from ranked
    where ranked.rn <= p_n
  ),
  agg as (
    select
      windowed.team_id as team_id,
      count(*) as games,
      sum(case when windowed.cards >= p_threshold then 1 else 0 end) as games_hit,
      avg(windowed.cards) as avg_value,
      array_agg(
        windowed.cards
        order by windowed.starting_at desc, windowed.fixture_id desc
      ) as last_values
    from windowed
    group by windowed.team_id
  )
  select
    agg.team_id::integer as team_id,
    t.name::text as team_name,
    t.short_code::text as team_short_code,
    t.image_path::text as team_image_path,
    agg.games::integer as games,
    agg.games_hit::integer as games_hit,
    agg.avg_value::numeric as avg_value,
    agg.last_values::numeric[] as last_values
  from agg
  join teams t on t.id = agg.team_id
  where agg.games = p_n
    and agg.games_hit >= p_required
  order by agg.games_hit desc, agg.avg_value desc
  limit p_limit;
$$;
