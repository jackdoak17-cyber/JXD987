-- Allow full-season rankings when p_window <= 0.
create or replace function public.get_team_stat_rankings(
  p_league_id int,
  p_type_id int,
  p_window int default 10,
  p_season_id int default null,
  p_ranking_type text default 'defensive' -- 'defensive' (least conceded) or 'attacking' (most scored)
)
returns table (
  team_id int,
  team_name text,
  stat_rank int,
  avg_value numeric,
  total_value numeric,
  games_played int
)
language sql stable
as $$
  with team_stats as (
    select
      case
        when p_ranking_type = 'defensive' then
          case
            when f.home_team_id = t.id then f.away_team_id
            else f.home_team_id
          end
        else t.id
      end as team_id,
      f.id as fixture_id,
      f.starting_at,
      coalesce(
        case
          when p_ranking_type = 'defensive' then
            -- Get opponent's stat (what this team conceded)
            case
              when f.home_team_id = t.id then
                (select sum(coalesce(fps.value, 0))
                 from public.fixture_player_statistics fps
                 join public.fixture_players fp on fp.player_id = fps.player_id and fp.fixture_id = fps.fixture_id
                 where fps.fixture_id = f.id and fps.type_id = p_type_id and fp.team_id = f.away_team_id)
              else
                (select sum(coalesce(fps.value, 0))
                 from public.fixture_player_statistics fps
                 join public.fixture_players fp on fp.player_id = fps.player_id and fp.fixture_id = fps.fixture_id
                 where fps.fixture_id = f.id and fps.type_id = p_type_id and fp.team_id = f.home_team_id)
            end
          else
            -- Get this team's stat (what this team scored)
            (select sum(coalesce(fps.value, 0))
             from public.fixture_player_statistics fps
             join public.fixture_players fp on fp.player_id = fps.player_id and fp.fixture_id = fps.fixture_id
             where fps.fixture_id = f.id and fps.type_id = p_type_id and fp.team_id = t.id)
        end
      , 0) as stat_value
    from public.teams t
    cross join lateral (
      select f.id, f.starting_at, f.home_team_id, f.away_team_id, f.home_score, f.away_score
      from public.fixtures f
      where f.league_id = p_league_id
        and (f.home_team_id = t.id or f.away_team_id = t.id)
        and f.home_score is not null
        and f.away_score is not null
        and (p_season_id is null or f.season_id = p_season_id)
      order by f.starting_at desc
      limit case when p_window <= 0 then 1000 else p_window end
    ) f
    where exists (
      select 1 from public.fixtures fx
      where fx.league_id = p_league_id
        and (fx.home_team_id = t.id or fx.away_team_id = t.id)
    )
  ),
  team_aggregates as (
    select
      ts.team_id,
      count(distinct ts.fixture_id)::int as games_played,
      sum(ts.stat_value) as total_value,
      avg(ts.stat_value) as avg_value
    from team_stats ts
    group by ts.team_id
    having count(distinct ts.fixture_id) >= case when p_window <= 0 then 1 else (p_window * 0.5)::int end
  ),
  ranked_teams as (
    select
      ta.team_id,
      ta.total_value,
      ta.avg_value,
      ta.games_played,
      case
        when p_ranking_type = 'defensive' then
          rank() over (order by ta.avg_value asc, ta.total_value asc) -- Best defense = lowest conceded
        else
          rank() over (order by ta.avg_value desc, ta.total_value desc) -- Best attack = highest scored
      end as stat_rank
    from team_aggregates ta
  )
  select
    rt.team_id::int,
    t.name::text as team_name,
    rt.stat_rank::int,
    round(rt.avg_value, 2) as avg_value,
    rt.total_value,
    rt.games_played
  from ranked_teams rt
  join public.teams t on t.id = rt.team_id
  order by rt.stat_rank asc;
$$;

grant execute on function public.get_team_stat_rankings(int, int, int, int, text) to anon, authenticated;
