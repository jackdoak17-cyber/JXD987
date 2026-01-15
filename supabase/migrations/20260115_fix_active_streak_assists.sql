DO $$
DECLARE
  def text;
BEGIN
  SELECT pg_get_functiondef(p.oid)
    INTO def
  FROM pg_proc p
  JOIN pg_namespace n ON n.oid = p.pronamespace
  WHERE n.nspname = 'public'
    AND p.proname = 'player_active_streaks'
  LIMIT 1;

  IF def IS NULL THEN
    RAISE NOTICE 'player_active_streaks not found';
    RETURN;
  END IF;

  def := replace(def, 'where fps.type_id = 84', 'where fps.type_id = 79');
  def := replace(def, 'where fps.type_id = 84', 'where fps.type_id = 79');
  EXECUTE def;
END $$;
