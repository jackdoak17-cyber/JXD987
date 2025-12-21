create table if not exists public.team_likely_players (
  team_id integer not null,
  player_id integer not null,
  confidence numeric not null default 0,
  last_seen_at timestamptz not null,
  source_fixture_id bigint not null,
  updated_at timestamptz not null default now(),
  primary key (team_id, player_id)
);

create index if not exists team_likely_players_team_id_idx
on public.team_likely_players (team_id);

create index if not exists team_likely_players_player_id_idx
on public.team_likely_players (player_id);

create index if not exists team_likely_players_last_seen_at_idx
on public.team_likely_players (last_seen_at);

create or replace function public.refresh_team_likely_players(
  p_league_id integer,
  p_recent_days integer default 30,
  p_last_n integer default 1
)
returns integer
language plpgsql
as $$
declare
  v_count integer := 0;
begin
  with team_fixtures as (
    select
      fp.team_id,
      f.id as fixture_id,
      f.starting_at,
      row_number() over (
        partition by fp.team_id
        order by f.starting_at desc, f.id desc
      ) as rn
    from fixture_players fp
    join fixtures f on f.id = fp.fixture_id
    where f.league_id = p_league_id
      and f.home_score is not null
      and f.away_score is not null
      and (
        p_recent_days is null
        or f.starting_at >= (now() - (p_recent_days || ' days')::interval)
      )
    group by fp.team_id, f.id, f.starting_at
  ),
  recent_team_fixtures as (
    select *
    from team_fixtures
    where rn <= greatest(p_last_n, 1)
  ),
  candidate_players as (
    select
      rtf.team_id,
      fp.player_id,
      rtf.fixture_id,
      rtf.starting_at,
      case
        when fp.is_starter is true then 1.0
        when fp.minutes_played is not null and fp.minutes_played > 0 then 0.6
        else null
      end as confidence
    from recent_team_fixtures rtf
    join fixture_players fp
      on fp.fixture_id = rtf.fixture_id
     and fp.team_id = rtf.team_id
  ),
  ranked as (
    select distinct on (team_id, player_id)
      team_id,
      player_id,
      confidence,
      starting_at as last_seen_at,
      fixture_id as source_fixture_id
    from candidate_players
    where confidence is not null
    order by team_id, player_id, starting_at desc, confidence desc
  ),
  upserted as (
    insert into public.team_likely_players (
      team_id,
      player_id,
      confidence,
      last_seen_at,
      source_fixture_id,
      updated_at
    )
    select
      ranked.team_id,
      ranked.player_id,
      ranked.confidence,
      ranked.last_seen_at,
      ranked.source_fixture_id,
      now()
    from ranked
    on conflict (team_id, player_id) do update set
      confidence = excluded.confidence,
      last_seen_at = excluded.last_seen_at,
      source_fixture_id = excluded.source_fixture_id,
      updated_at = excluded.updated_at
    returning 1
  )
  select count(*) into v_count from upserted;

  return v_count;
end;
$$;
