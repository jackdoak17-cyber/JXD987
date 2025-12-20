create or replace function public.stat_type_usage()
returns table (
  type_id integer,
  key text,
  name text,
  is_player boolean,
  is_team boolean
)
language sql
stable
as $$
  with player as (
    select distinct type_id
    from fixture_player_statistics
    where type_id is not null
  ),
  team as (
    select distinct type_id
    from fixture_statistics
    where type_id is not null
  )
  select
    t.id as type_id,
    t.code as key,
    t.name as name,
    (p.type_id is not null) as is_player,
    (tm.type_id is not null) as is_team
  from types t
  left join player p on p.type_id = t.id
  left join team tm on tm.type_id = t.id
  where p.type_id is not null or tm.type_id is not null;
$$;
