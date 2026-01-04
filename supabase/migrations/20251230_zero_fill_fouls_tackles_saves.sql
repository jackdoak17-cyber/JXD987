DO $$
DECLARE
  fn regprocedure;
  def text;
BEGIN
  FOR fn IN
    SELECT p.oid::regprocedure
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = 'public'
      AND p.proname IN (
        'player_streak_base',
        'player_streak_meta',
        'player_streak_playing_on_date',
        'player_streak_playing_on_date_counts'
      )
  LOOP
    SELECT pg_get_functiondef(fn) INTO def;
    def := replace(def, 'p_type_id IN (42, 86)', 'p_type_id IN (42, 86, 56, 96, 78, 57)');
    def := replace(def, 'p_type_id IN (42,86)', 'p_type_id IN (42, 86, 56, 96, 78, 57)');
    EXECUTE def;
  END LOOP;
END $$;
