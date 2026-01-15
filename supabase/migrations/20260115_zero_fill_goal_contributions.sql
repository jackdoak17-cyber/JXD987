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
      AND pg_get_functiondef(p.oid) ILIKE '%p_type_id in (42%'
  LOOP
    SELECT pg_get_functiondef(fn) INTO def;
    def := replace(
      def,
      'p_type_id in (42, 86, 52, 56, 96, 78, 57, 79, 84)',
      'p_type_id in (42, 86, 52, 56, 96, 78, 57, 79, 84, 200001)'
    );
    def := replace(
      def,
      'p_type_id in (42,86,52,56,96,78,57,79,84)',
      'p_type_id in (42,86,52,56,96,78,57,79,84,200001)'
    );
    def := replace(
      def,
      'P_TYPE_ID IN (42, 86, 52, 56, 96, 78, 57, 79, 84)',
      'P_TYPE_ID IN (42, 86, 52, 56, 96, 78, 57, 79, 84, 200001)'
    );
    def := replace(
      def,
      'P_TYPE_ID IN (42,86,52,56,96,78,57,79,84)',
      'P_TYPE_ID IN (42,86,52,56,96,78,57,79,84,200001)'
    );
    EXECUTE def;
  END LOOP;
END $$;
