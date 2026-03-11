create table if not exists public.referees (
  id bigint primary key,
  name text not null,
  short_name text,
  common_name text,
  image_path text,
  country_id bigint,
  city_id bigint,
  source text not null default 'sportmonks',
  extra jsonb,
  updated_at timestamptz not null default now()
);

create table if not exists public.fixture_referees (
  fixture_id bigint not null references public.fixtures(id) on delete cascade,
  referee_id bigint not null references public.referees(id) on delete cascade,
  role text not null default 'unknown',
  is_primary boolean not null default false,
  source text not null default 'sportmonks',
  extra jsonb,
  updated_at timestamptz not null default now(),
  last_synced_at timestamptz not null default now(),
  primary key (fixture_id, referee_id, role)
);

do $$
begin
  if exists (
    select 1
    from information_schema.tables
    where table_schema = 'public'
      and table_name = 'countries'
  ) and not exists (
    select 1
    from pg_constraint
    where conname = 'referees_country_id_fkey'
  ) then
    alter table public.referees
      add constraint referees_country_id_fkey
      foreign key (country_id)
      references public.countries(id)
      on delete set null;
  end if;
end
$$;

create index if not exists referees_country_id_idx
  on public.referees(country_id);

create index if not exists fixture_referees_fixture_idx
  on public.fixture_referees(fixture_id);

create index if not exists fixture_referees_referee_idx
  on public.fixture_referees(referee_id);

create index if not exists fixture_referees_primary_idx
  on public.fixture_referees(fixture_id, is_primary);
