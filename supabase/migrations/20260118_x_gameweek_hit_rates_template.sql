insert into public.marketing_templates
  (name, type, platform, template_body, rules, is_active)
select
  'X Gameweek Hit Rates',
  'x_gameweek_hit_rates',
  'x',
  '{{content}}',
  '{}'::jsonb,
  true
where not exists (
  select 1
  from public.marketing_templates
  where type = 'x_gameweek_hit_rates'
    and platform = 'x'
);
