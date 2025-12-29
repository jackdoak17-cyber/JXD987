-- User saved searches with flexible JSONB criteria storage
-- Allows users to save complex search configurations for re-use

create table if not exists public.user_saved_searches (
  id bigserial primary key,
  user_id uuid not null,
  name text not null,
  description text null,
  search_criteria jsonb not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  last_run_at timestamptz null,
  is_archived boolean not null default false
);

create index if not exists user_saved_searches_user_id_idx
  on public.user_saved_searches (user_id);

create index if not exists user_saved_searches_created_at_idx
  on public.user_saved_searches (created_at desc);

create index if not exists user_saved_searches_criteria_idx
  on public.user_saved_searches using gin (search_criteria);

-- Enable Row Level Security
alter table public.user_saved_searches enable row level security;

-- RLS Policies: Users can only CRUD their own searches
create policy "Users can view own searches"
  on public.user_saved_searches for select
  using (auth.uid() = user_id);

create policy "Users can insert own searches"
  on public.user_saved_searches for insert
  with check (auth.uid() = user_id);

create policy "Users can update own searches"
  on public.user_saved_searches for update
  using (auth.uid() = user_id);

create policy "Users can delete own searches"
  on public.user_saved_searches for delete
  using (auth.uid() = user_id);

-- RPC: Get user's saved searches
create or replace function public.get_user_saved_searches(
  p_user_id uuid,
  p_include_archived boolean default false
)
returns table (
  id bigint,
  name text,
  description text,
  search_criteria jsonb,
  created_at timestamptz,
  updated_at timestamptz,
  last_run_at timestamptz,
  is_archived boolean
)
language sql
stable
security definer
as $$
  select
    id,
    name,
    description,
    search_criteria,
    created_at,
    updated_at,
    last_run_at,
    is_archived
  from public.user_saved_searches
  where user_id = p_user_id
    and (p_include_archived or not is_archived)
  order by created_at desc;
$$;
