do $$
declare
  owner_role text;
  table_name text;
  public_read_tables text[] := array[
    'betting_picks',
    'betting_picks_runs',
    'fixture_referee_stats',
    'fixture_referees',
    'oddssearcher_opponent_tier_multipliers',
    'referees'
  ];
  private_tables text[] := array[
    'ml_player_prop_rows',
    'post_approvals',
    'telegram_state'
  ];
begin
  foreach owner_role in array array['postgres', 'supabase_admin'] loop
    if exists (select 1 from pg_roles where rolname = owner_role) then
      begin
        execute format(
          'alter default privileges for role %I in schema public revoke all on tables from anon, authenticated',
          owner_role
        );
        execute format(
          'alter default privileges for role %I in schema public revoke all on sequences from anon, authenticated',
          owner_role
        );
      exception when insufficient_privilege then
        raise notice 'Skipping default privilege hardening for role %: insufficient privileges', owner_role;
      end;
    end if;
  end loop;

  foreach table_name in array public_read_tables loop
    if to_regclass(format('public.%I', table_name)) is not null then
      execute format('alter table public.%I enable row level security', table_name);
      execute format('revoke all privileges on table public.%I from anon, authenticated', table_name);
      execute format('grant select on table public.%I to anon, authenticated', table_name);

      execute format('drop policy if exists %I on public.%I', table_name || '_public_read', table_name);
      execute format(
        'create policy %I on public.%I for select to anon, authenticated using (true)',
        table_name || '_public_read',
        table_name
      );
    end if;
  end loop;

  foreach table_name in array private_tables loop
    if to_regclass(format('public.%I', table_name)) is not null then
      execute format('alter table public.%I enable row level security', table_name);
      execute format('revoke all privileges on table public.%I from anon, authenticated', table_name);

      execute format('drop policy if exists %I on public.%I', table_name || '_no_direct_access', table_name);
      execute format(
        'create policy %I on public.%I for all to anon, authenticated using (false) with check (false)',
        table_name || '_no_direct_access',
        table_name
      );
    end if;
  end loop;
end $$;
