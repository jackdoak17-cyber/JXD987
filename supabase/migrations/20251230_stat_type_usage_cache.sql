create table if not exists public.stat_type_usage_cache (
  type_id integer primary key,
  key text,
  name text,
  is_player boolean,
  is_team boolean
);

insert into public.stat_type_usage_cache (type_id, key, name, is_player, is_team)
select
  t.id as type_id,
  t.code as key,
  t.name as name,
  (p.type_id is not null) as is_player,
  (tm.type_id is not null) as is_team
from types t
left join (
  select type_id
  from fixture_player_statistics
  where type_id is not null
  group by type_id
) p on p.type_id = t.id
left join (
  select type_id
  from fixture_statistics
  where type_id is not null
  group by type_id
) tm on tm.type_id = t.id
where p.type_id is not null or tm.type_id is not null
on conflict (type_id) do update
set key = excluded.key,
    name = excluded.name,
    is_player = excluded.is_player,
    is_team = excluded.is_team;

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
  select type_id, key, name, is_player, is_team
  from public.stat_type_usage_cache;
$$;

create or replace function public.debug_statement_timeout()
returns text
language sql
stable
as $$
  select current_setting('statement_timeout');
$$;
