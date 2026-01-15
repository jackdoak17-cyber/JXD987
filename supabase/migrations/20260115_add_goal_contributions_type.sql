insert into public.types (id, name, code, developer_name, model_type, stat_group)
values (200001, 'Goal Contributions', 'goal_contributions', 'GOAL_CONTRIBUTIONS', 'player', 'player')
on conflict (id) do update
set name = excluded.name,
    code = excluded.code,
    developer_name = excluded.developer_name,
    model_type = excluded.model_type,
    stat_group = excluded.stat_group;

insert into public.stat_type_usage_cache (type_id, key, name, is_player, is_team)
values (200001, 'goal_contributions', 'Goal Contributions', true, false)
on conflict (type_id) do update
set key = excluded.key,
    name = excluded.name,
    is_player = excluded.is_player,
    is_team = excluded.is_team;
