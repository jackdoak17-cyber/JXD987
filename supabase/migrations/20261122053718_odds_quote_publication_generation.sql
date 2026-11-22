-- Private quote-table mutation generation. This does not replace the atomic
-- canonical/release/verification publisher or any quote-freshness rule.
-- The recorded relation OIDs make a table replacement fail closed until a
-- separately reviewed migration preserves generation continuity.
SET LOCAL lock_timeout = '500ms';
SET LOCAL statement_timeout = '15s';

DO $preflight$
DECLARE
  writer_source_hash text;
BEGIN
  IF to_regclass('public.odds_outcomes') IS NULL
     OR to_regclass('public.odds_quote_verifications') IS NULL
     OR to_regclass('operations.quote_verification_maintenance') IS NULL THEN
    RAISE EXCEPTION 'Quote publication generation prerequisites are missing';
  END IF;
  SELECT md5(p.prosrc) INTO writer_source_hash
  FROM pg_catalog.pg_proc p
  WHERE p.oid = to_regprocedure('public.odds_quote_verifications_record(text,jsonb)');
  IF writer_source_hash IS DISTINCT FROM '909133ec6a24a9e209390ac9914d2085' THEN
    RAISE EXCEPTION 'Quote recorder differs from reviewed maintenance-first source';
  END IF;
END
$preflight$;

CREATE TABLE operations.odds_quote_publication_state (
  id smallint PRIMARY KEY CHECK (id = 1),
  generation bigint NOT NULL CHECK (generation >= 0),
  odds_outcomes_relid oid NOT NULL,
  quote_verifications_relid oid NOT NULL
);
ALTER TABLE operations.odds_quote_publication_state ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON operations.odds_quote_publication_state FROM PUBLIC;
DO $privacy$
DECLARE role_name text;
BEGIN
  FOREACH role_name IN ARRAY ARRAY[
    'anon', 'authenticated', 'service_role',
    'statsweb_pool', 'oddssearch_operations_reader'
  ] LOOP
    IF to_regrole(role_name) IS NOT NULL THEN
      EXECUTE format(
        'REVOKE ALL ON operations.odds_quote_publication_state FROM %I',
        role_name
      );
    END IF;
  END LOOP;
END
$privacy$;
INSERT INTO operations.odds_quote_publication_state (
  id, generation, odds_outcomes_relid, quote_verifications_relid
) VALUES (
  1, 0, 'public.odds_outcomes'::regclass,
  'public.odds_quote_verifications'::regclass
);

-- BEFORE STATEMENT obtains the singleton row lock before quote-table row
-- locks. The recorder updates quote_verification_maintenance before its
-- ledger DELETE/UPSERT, so that UPDATE must acquire the same lock first.
CREATE FUNCTION operations.odds_quote_publication_bump()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE affected integer;
BEGIN
  UPDATE operations.odds_quote_publication_state
     SET generation = generation + 1
   WHERE id = 1
     AND odds_outcomes_relid = 'public.odds_outcomes'::regclass
     AND quote_verifications_relid = 'public.odds_quote_verifications'::regclass;
  GET DIAGNOSTICS affected = ROW_COUNT;
  IF affected <> 1 THEN
    RAISE EXCEPTION 'Quote publication generation state unavailable'
      USING ERRCODE = '55000';
  END IF;
  RETURN NULL;
END
$function$;
REVOKE ALL ON FUNCTION operations.odds_quote_publication_bump() FROM PUBLIC;

CREATE TRIGGER odds_quote_generation_dml
BEFORE INSERT OR UPDATE OR DELETE ON public.odds_outcomes
FOR EACH STATEMENT EXECUTE FUNCTION operations.odds_quote_publication_bump();
CREATE TRIGGER odds_quote_generation_truncate
BEFORE TRUNCATE ON public.odds_outcomes
FOR EACH STATEMENT EXECUTE FUNCTION operations.odds_quote_publication_bump();
CREATE TRIGGER odds_quote_verification_generation_dml
BEFORE INSERT OR UPDATE OR DELETE ON public.odds_quote_verifications
FOR EACH STATEMENT EXECUTE FUNCTION operations.odds_quote_publication_bump();
CREATE TRIGGER odds_quote_verification_generation_truncate
BEFORE TRUNCATE ON public.odds_quote_verifications
FOR EACH STATEMENT EXECUTE FUNCTION operations.odds_quote_publication_bump();
CREATE TRIGGER odds_quote_maintenance_generation_update
BEFORE UPDATE ON operations.quote_verification_maintenance
FOR EACH STATEMENT EXECUTE FUNCTION operations.odds_quote_publication_bump();

-- ALWAYS keeps the mutation guard active if a session changes
-- session_replication_role. The protected reader validates all five names,
-- events, relation OIDs and enabled states on every call.
ALTER TABLE public.odds_outcomes ENABLE ALWAYS TRIGGER odds_quote_generation_dml;
ALTER TABLE public.odds_outcomes ENABLE ALWAYS TRIGGER odds_quote_generation_truncate;
ALTER TABLE public.odds_quote_verifications ENABLE ALWAYS TRIGGER odds_quote_verification_generation_dml;
ALTER TABLE public.odds_quote_verifications ENABLE ALWAYS TRIGGER odds_quote_verification_generation_truncate;
ALTER TABLE operations.quote_verification_maintenance ENABLE ALWAYS TRIGGER odds_quote_maintenance_generation_update;

CREATE FUNCTION operations.odds_quote_publication_generation()
RETURNS text
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
  value bigint;
  expected_odds oid;
  expected_verifications oid;
  healthy_triggers integer;
BEGIN
  SELECT s.generation, s.odds_outcomes_relid, s.quote_verifications_relid
    INTO value, expected_odds, expected_verifications
  FROM operations.odds_quote_publication_state s WHERE s.id = 1;
  IF NOT FOUND OR value IS NULL
     OR expected_odds IS DISTINCT FROM to_regclass('public.odds_outcomes')::oid
     OR expected_verifications IS DISTINCT FROM to_regclass('public.odds_quote_verifications')::oid
     OR (SELECT c.relkind FROM pg_catalog.pg_class c WHERE c.oid = expected_odds) <> 'r'
     OR (SELECT c.relkind FROM pg_catalog.pg_class c WHERE c.oid = expected_verifications) <> 'r' THEN
    RAISE EXCEPTION 'Quote publication generation state unavailable'
      USING ERRCODE = '55000';
  END IF;

  SELECT count(*) INTO healthy_triggers
  FROM (
    VALUES
      (expected_odds, 'odds_quote_generation_dml'::name, 30),
      (expected_odds, 'odds_quote_generation_truncate'::name, 34),
      (expected_verifications, 'odds_quote_verification_generation_dml'::name, 30),
      (expected_verifications, 'odds_quote_verification_generation_truncate'::name, 34),
      ('operations.quote_verification_maintenance'::regclass::oid,
       'odds_quote_maintenance_generation_update'::name, 18)
  ) AS e(relid, trigger_name, trigger_type)
  JOIN pg_catalog.pg_trigger t
    ON t.tgrelid = e.relid AND t.tgname = e.trigger_name
   AND t.tgtype::integer = e.trigger_type
   AND t.tgfoid = 'operations.odds_quote_publication_bump()'::regprocedure
   AND t.tgenabled = 'A' AND NOT t.tgisinternal;
  IF healthy_triggers <> 5 THEN
    RAISE EXCEPTION 'Quote publication generation triggers unavailable'
      USING ERRCODE = '55000';
  END IF;
  RETURN value::text;
END
$function$;
REVOKE ALL ON FUNCTION operations.odds_quote_publication_generation() FROM PUBLIC;

COMMENT ON TABLE operations.odds_quote_publication_state IS
  'Private monotonic generation of committed raw and exact-verification quote mutations; never a quote observation time or publication timestamp.';
COMMENT ON FUNCTION operations.odds_quote_publication_generation() IS
  'Protected decimal-text generation for read bracketing; fails closed if quote-table mutation guards are absent.';
