insert into public.types (id, name, code, developer_name, model_type, stat_group)
values
  (200010, 'Shot Accuracy %', 'shot_accuracy_percent', 'SHOT_ACCURACY_PERCENT', 'both', 'Shooting'),
  (200011, 'Inside Box Shot Share %', 'inside_box_shot_share_percent', 'INSIDE_BOX_SHOT_SHARE_PERCENT', 'team', 'Shooting'),
  (200012, 'Cross Accuracy %', 'cross_accuracy_percent', 'CROSS_ACCURACY_PERCENT', 'both', 'Shooting'),
  (200013, 'Defensive Involvement', 'defensive_involvement', 'DEFENSIVE_INVOLVEMENT', 'player', 'Defense')
on conflict (id) do update
set name = excluded.name,
    code = excluded.code,
    developer_name = excluded.developer_name,
    model_type = excluded.model_type,
    stat_group = excluded.stat_group;

insert into public.stat_type_usage_cache (type_id, key, name, is_player, is_team)
values
  (200010, 'shot_accuracy_percent', 'Shot Accuracy %', true, true),
  (200011, 'inside_box_shot_share_percent', 'Inside Box Shot Share %', false, true),
  (200012, 'cross_accuracy_percent', 'Cross Accuracy %', true, true),
  (200013, 'defensive_involvement', 'Defensive Involvement', true, false)
on conflict (type_id) do update
set key = excluded.key,
    name = excluded.name,
    is_player = excluded.is_player,
    is_team = excluded.is_team;
