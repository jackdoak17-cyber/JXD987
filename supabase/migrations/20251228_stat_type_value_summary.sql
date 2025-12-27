create or replace function public.player_stat_type_value_summary(
  p_league_id integer
)
returns table (
  type_id integer,
  row_count bigint,
  zero_count bigint,
  decimal_count bigint
)
language sql
stable
as $$
  select
    fps.type_id,
    count(*)::bigint as row_count,
    count(*) filter (where fps.value = 0)::bigint as zero_count,
    count(*) filter (where fps.value is not null and fps.value <> floor(fps.value))::bigint as decimal_count
  from fixture_player_statistics fps
  join fixtures f on f.id = fps.fixture_id
  where f.league_id = p_league_id
    and f.home_score is not null
    and f.away_score is not null
  group by fps.type_id;
$$;

create or replace function public.team_stat_type_value_summary(
  p_league_id integer
)
returns table (
  type_id integer,
  row_count bigint,
  zero_count bigint,
  decimal_count bigint
)
language sql
stable
as $$
  select
    fs.type_id,
    count(*)::bigint as row_count,
    count(*) filter (where fs.value = 0)::bigint as zero_count,
    count(*) filter (where fs.value is not null and fs.value <> floor(fs.value))::bigint as decimal_count
  from fixture_statistics fs
  join fixtures f on f.id = fs.fixture_id
  where f.league_id = p_league_id
    and f.home_score is not null
    and f.away_score is not null
  group by fs.type_id;
$$;
