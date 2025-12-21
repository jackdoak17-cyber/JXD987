create or replace function public.search_players(
  p_league_id integer,
  p_query text,
  p_limit integer default 10
)
returns table (
  player_id integer,
  player_name text,
  player_common_name text,
  player_short_name text,
  team_id integer,
  team_name text,
  team_short_code text,
  last_fixture_at timestamptz
)
language sql
stable
as $$
  with input as (
    select trim(p_query) as q
  ),
  tokens as (
    select
      array_remove(regexp_split_to_array(lower((select q from input)), '\s+'), '') as tokens
  ),
  latest_app as (
    select
      fp.player_id,
      fp.team_id,
      f.starting_at,
      f.id as fixture_id,
      row_number() over (
        partition by fp.player_id
        order by f.starting_at desc, f.id desc
      ) as rn
    from fixture_players fp
    join fixtures f on f.id = fp.fixture_id
    where f.league_id = p_league_id
      and f.home_score is not null
      and f.away_score is not null
  ),
  latest as (
    select
      player_id,
      team_id,
      starting_at as last_fixture_at
    from latest_app
    where rn = 1
  ),
  matched as (
    select
      p.id as player_id,
      p.name as player_name,
      p.common_name as player_common_name,
      p.short_name as player_short_name,
      l.team_id as team_id,
      t.name as team_name,
      t.short_code as team_short_code,
      l.last_fixture_at,
      lower(p.short_name) = lower((select q from input)) as exact_short,
      lower(p.common_name) = lower((select q from input)) as exact_common,
      lower(p.name) = lower((select q from input)) as exact_name,
      case
        when coalesce(array_length(tokens.tokens, 1), 0) = 0 then false
        else (
          select bool_and(lower(p.name) like '%' || tok || '%')
          from unnest(tokens.tokens) tok
        )
      end as name_token_match,
      case
        when p.common_name is null or coalesce(array_length(tokens.tokens, 1), 0) = 0 then false
        else (
          select bool_and(lower(p.common_name) like '%' || tok || '%')
          from unnest(tokens.tokens) tok
        )
      end as common_token_match
    from players p
    cross join tokens
    join latest l on l.player_id = p.id
    left join teams t on t.id = l.team_id
    where (select q from input) <> ''
  )
  select
    player_id::integer,
    player_name::text,
    player_common_name::text,
    player_short_name::text,
    team_id::integer,
    team_name::text,
    team_short_code::text,
    last_fixture_at
  from matched
  where exact_short
     or exact_common
     or exact_name
     or name_token_match
     or common_token_match
  order by
    case
      when exact_short then 1
      when exact_common then 2
      when exact_name then 3
      when name_token_match then 4
      when common_token_match then 5
      else 6
    end,
    last_fixture_at desc nulls last
  limit p_limit;
$$;
