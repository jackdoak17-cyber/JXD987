-- Rank the same eligible player set before building full metric JSON.
-- The source projection and the website v2 wrapper are unchanged.

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
  eligible as (
    select ss.*, lt.team_id, lt.position_group
    from scoped_summary ss
    join latest_team lt on lt.player_id = ss.player_id
    join public.players player on player.id = ss.player_id
    where (p_search is null or trim(p_search) = '' or
      lower(coalesce(player.display_name, player.name, player.common_name, player.short_name, ''))
        like '%' || lower(trim(p_search)) || '%')
      and exists (
        -- The legacy inner join omitted players with no contracted metric.
        select 1
        from public.player_stats_season_metric m
        join public.player_stats_metric_contract c on c.metric_key = m.metric_key
        where m.league_id = p_league_id
          and m.season_id = p_season_id
          and m.player_id = ss.player_id
          and (p_team_id is null or m.team_id = p_team_id)
      )
  ),
  sort_metric_scope as materialized (
    select m.*
    from public.player_stats_season_metric m
    where m.league_id = p_league_id
      and m.season_id = p_season_id
      and (p_team_id is null or m.team_id = p_team_id)
      and m.metric_key = p_sort_key
      and p_sort_measure in ('per90', 'average', 'total')
  ),
  sort_metric as materialized (
    select
      m.player_id,
      sum(m.total_value) as total_value,
      case when sum(m.unknown_appearances) = 0
                 and sum(m.reported_appearances + m.zero_safe_appearances) > 0
        then sum(m.total_value) /
          nullif(sum(m.reported_appearances + m.zero_safe_appearances), 0)
      end as average_value,
      case when c.supports_per90
                 and sum(m.unknown_appearances) = 0
                 and sum(m.reported_appearances + m.zero_safe_appearances) > 0
                 and e.minutes > 0
        then sum(m.total_value) / e.minutes * 90
      end as per90_value
    from sort_metric_scope m
    join eligible e on e.player_id = m.player_id
    join public.player_stats_metric_contract c on c.metric_key = m.metric_key
    group by m.player_id, c.supports_per90, e.minutes
  ),
  ranked as (
    select
      row_number() over (
        order by
          case when p_sort_desc and p_sort_measure = 'per90' then sm.per90_value end desc nulls last,
          case when p_sort_desc and p_sort_measure = 'average' then sm.average_value end desc nulls last,
          case when p_sort_desc and p_sort_measure = 'total' then sm.total_value end desc nulls last,
          case when p_sort_desc and p_sort_measure = 'base' then
            case p_sort_key
              when 'appearances' then e.appearances::numeric
              when 'starts' then e.starts::numeric
              when 'minutes' then e.minutes
              else null
            end
          end desc nulls last,
          case when not p_sort_desc and p_sort_measure = 'per90' then sm.per90_value end asc nulls last,
          case when not p_sort_desc and p_sort_measure = 'average' then sm.average_value end asc nulls last,
          case when not p_sort_desc and p_sort_measure = 'total' then sm.total_value end asc nulls last,
          case when not p_sort_desc and p_sort_measure = 'base' then
            case p_sort_key
              when 'appearances' then e.appearances::numeric
              when 'starts' then e.starts::numeric
              when 'minutes' then e.minutes
              else null
            end
          end asc nulls last,
          e.minutes desc,
          e.player_id
      ) as rank,
      e.*
    from eligible e
    left join sort_metric sm on sm.player_id = e.player_id
  ),
  paged as materialized (
    select * from ranked r
    where r.rank > greatest(coalesce(p_offset, 0), 0)
      and r.rank <= greatest(coalesce(p_offset, 0), 0) +
        least(greatest(coalesce(p_limit, 50), 1), 100)
  ),
  metric_aggregate as (
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
      case when c.supports_per90 and sum(m.unknown_appearances) = 0 and sum(m.reported_appearances + m.zero_safe_appearances) > 0 and pg.minutes > 0
        then sum(m.total_value) / pg.minutes * 90 end as per90_value
    from paged pg
    join public.player_stats_season_metric m
      on m.league_id = p_league_id
     and m.season_id = p_season_id
     and m.player_id = pg.player_id
     and (p_team_id is null or m.team_id = p_team_id)
    join public.player_stats_metric_contract c on c.metric_key = m.metric_key
    group by m.player_id, m.metric_key, c.supports_per90, pg.minutes
  ),
  shaped as (
    select
      pg.rank,
      pg.player_id,
      pg.team_id,
      pg.position_group,
      pg.appearances,
      pg.starts,
      pg.minutes,
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
      ) as metrics
    from paged pg
    join metric_aggregate ma on ma.player_id = pg.player_id
    group by pg.rank, pg.player_id, pg.team_id, pg.position_group,
             pg.appearances, pg.starts, pg.minutes
  )
  select
    sh.rank,
    sh.player_id,
    player.name,
    player.display_name,
    player.common_name,
    player.short_name,
    player.image_path,
    sh.team_id,
    team.name,
    team.short_code,
    team.image_path,
    sh.position_group,
    sh.appearances,
    sh.starts,
    sh.minutes,
    sh.metrics
  from shaped sh
  join public.players player on player.id = sh.player_id
  left join public.teams team on team.id = sh.team_id
  order by sh.rank;
$$;
