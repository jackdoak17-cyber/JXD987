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
      AND p.prokind = 'f'
      AND pg_get_functiondef(p.oid) ILIKE '%p_min_minutes%fp.minutes_played%'
  LOOP
    SELECT pg_get_functiondef(fn) INTO def;
    def := replace(
      def,
      'and (p_min_minutes is null or fp.minutes_played >= p_min_minutes)',
      'and (p_min_minutes is null or fp.minutes_played >= p_min_minutes or fp.is_starter is true)'
    );
    def := replace(
      def,
      'AND (p_min_minutes IS NULL OR fp.minutes_played >= p_min_minutes)',
      'AND (p_min_minutes IS NULL OR fp.minutes_played >= p_min_minutes OR fp.is_starter IS TRUE)'
    );
    EXECUTE def;
  END LOOP;
END $$;
