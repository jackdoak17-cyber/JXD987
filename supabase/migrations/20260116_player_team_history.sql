-- Track player team history and prevent team_id flip-flops on squad syncs.
alter table public.players
  add column if not exists team_updated_at timestamptz;

create table if not exists public.player_team_history (
  id bigserial primary key,
  player_id integer not null references public.players(id) on delete cascade,
  team_id integer not null references public.teams(id) on delete cascade,
  source text not null default 'squad_sync',
  effective_from timestamptz not null,
  effective_to timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists player_team_history_player_id_idx
  on public.player_team_history(player_id);

create index if not exists player_team_history_team_id_idx
  on public.player_team_history(team_id);

create index if not exists player_team_history_effective_idx
  on public.player_team_history(player_id, effective_from desc);

create or replace function public.touch_player_team_history_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

drop trigger if exists trg_player_team_history_updated_at on public.player_team_history;

create trigger trg_player_team_history_updated_at
before update on public.player_team_history
for each row
execute function public.touch_player_team_history_updated_at();
