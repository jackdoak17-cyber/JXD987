-- Limit player_streak_playing_on_date to recent fixtures early to avoid statement timeouts.
do $$
declare
  fn regprocedure;
  def text;
begin
  for fn in
    select p.oid::regprocedure
    from pg_proc p
    join pg_namespace n on n.oid = p.pronamespace
    where n.nspname = 'public'
      and p.proname in (
        'player_streak_playing_on_date',
        'player_streak_playing_on_date_counts'
      )
  loop
    select pg_get_functiondef(fn) into def;
    def := replace(
      def,
      'where fp.player_id in (select player_id from candidates)',
      'where fp.player_id in (select player_id from candidates)
      and (p_recent_days is null or (f.starting_at at time zone ''Europe/London'')::date >=
        ((now() at time zone ''Europe/London'')::date - (greatest(p_recent_days, 1) - 1)))'
    );
    def := replace(
      def,
      'where fp.player_id in (select player_id from candidates_played)',
      'where fp.player_id in (select player_id from candidates_played)
      and (p_recent_days is null or (f.starting_at at time zone ''Europe/London'')::date >=
        ((now() at time zone ''Europe/London'')::date - (greatest(p_recent_days, 1) - 1)))'
    );
    def := replace(
      def,
      'where fp.player_id in (select player_id from candidates)',
      'where fp.player_id in (select player_id from candidates)
      and (p_recent_days is null or (f.starting_at at time zone ''Europe/London'')::date >=
        ((now() at time zone ''Europe/London'')::date - (greatest(p_recent_days, 1) - 1)))'
    );
    execute def;
  end loop;
end $$;
