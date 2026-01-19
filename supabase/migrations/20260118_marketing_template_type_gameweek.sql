alter table public.marketing_templates
  drop constraint if exists marketing_templates_type_check;

alter table public.marketing_templates
  add constraint marketing_templates_type_check
  check (
    type = any (
      array[
        'stat_text',
        'promo_text',
        'x_multi_stat_daily',
        'x_streak_significance',
        'x_active_streaks',
        'x_gameweek_hit_rates'
      ]
    )
  );
