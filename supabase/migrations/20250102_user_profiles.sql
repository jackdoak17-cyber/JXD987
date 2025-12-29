-- User profiles table linked to Supabase auth.users
-- Auto-creates profile on signup with trigger

create table if not exists public.user_profiles (
  id bigserial primary key,
  user_id uuid not null unique,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  subscription_tier text not null default 'free',
  subscription_expires_at timestamptz null
);

create index if not exists user_profiles_user_id_idx
  on public.user_profiles (user_id);

-- Enable Row Level Security
alter table public.user_profiles enable row level security;

-- RLS Policies: Users can only view/update their own profile
create policy "Users can view own profile"
  on public.user_profiles for select
  using (auth.uid() = user_id);

create policy "Users can update own profile"
  on public.user_profiles for update
  using (auth.uid() = user_id);

-- Trigger function to auto-create profile on signup
create or replace function public.handle_new_user()
returns trigger
language plpgsql
security definer
as $$
begin
  insert into public.user_profiles (user_id, subscription_tier)
  values (new.id, 'free');
  return new;
end;
$$;

-- Trigger that fires when new user signs up
create trigger on_auth_user_created
  after insert on auth.users
  for each row execute function public.handle_new_user();
