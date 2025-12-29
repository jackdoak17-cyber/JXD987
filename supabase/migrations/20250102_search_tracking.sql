-- Dual-tier result tracking system
-- Simple mode: search_run_summary only (low overhead)
-- Detailed mode: search_run_summary + search_result_details (fixture-by-fixture)

-- Simple hit rate tracking (always created for every run)
create table if not exists public.search_run_summary (
  id bigserial primary key,
  user_id uuid not null,
  saved_search_id bigint null,
  search_criteria jsonb not null,
  run_at timestamptz not null default now(),
  results_count integer not null default 0,
  notes text null
);

create index if not exists search_run_summary_user_id_idx
  on public.search_run_summary (user_id);

create index if not exists search_run_summary_saved_search_id_idx
  on public.search_run_summary (saved_search_id);

create index if not exists search_run_summary_run_at_idx
  on public.search_run_summary (run_at desc);

-- Detailed fixture-by-fixture tracking (opt-in)
create table if not exists public.search_result_details (
  id bigserial primary key,
  search_run_id bigint not null,
  entity_type text not null,
  entity_id bigint not null,
  entity_name text not null,
  fixture_id bigint null,
  fixture_date date null,
  opponent_id bigint null,
  opponent_name text null,
  predicted_outcome text null,
  actual_value numeric null,
  is_hit boolean null,
  notes text null
);

create index if not exists search_result_details_run_id_idx
  on public.search_result_details (search_run_id);

create index if not exists search_result_details_entity_idx
  on public.search_result_details (entity_type, entity_id);

create index if not exists search_result_details_fixture_idx
  on public.search_result_details (fixture_id);

-- Enable Row Level Security
alter table public.search_run_summary enable row level security;
alter table public.search_result_details enable row level security;

-- RLS Policies for search_run_summary
create policy "Users can view own run summaries"
  on public.search_run_summary for select
  using (auth.uid() = user_id);

create policy "Users can insert own run summaries"
  on public.search_run_summary for insert
  with check (auth.uid() = user_id);

-- RLS Policies for search_result_details
create policy "Users can view own result details"
  on public.search_result_details for select
  using (
    exists (
      select 1 from public.search_run_summary srs
      where srs.id = search_result_details.search_run_id
        and srs.user_id = auth.uid()
    )
  );

create policy "Users can insert own result details"
  on public.search_result_details for insert
  with check (
    exists (
      select 1 from public.search_run_summary srs
      where srs.id = search_result_details.search_run_id
        and srs.user_id = auth.uid()
    )
  );

-- RPC: Get search performance analytics
create or replace function public.get_search_performance_analytics(
  p_user_id uuid,
  p_saved_search_id bigint default null,
  p_days_back integer default 30
)
returns table (
  total_runs bigint,
  total_hits bigint,
  total_attempts bigint,
  hit_rate numeric,
  recent_trend jsonb
)
language sql
stable
security definer
as $$
  with runs as (
    select
      srs.id,
      srs.run_at,
      coalesce(sum(case when srd.is_hit then 1 else 0 end), 0) as hits,
      coalesce(count(srd.id), 0) as attempts
    from public.search_run_summary srs
    left join public.search_result_details srd on srd.search_run_id = srs.id
    where srs.user_id = p_user_id
      and (p_saved_search_id is null or srs.saved_search_id = p_saved_search_id)
      and srs.run_at >= (now() - (p_days_back || ' days')::interval)
    group by srs.id, srs.run_at
  )
  select
    count(*)::bigint as total_runs,
    sum(hits)::bigint as total_hits,
    sum(attempts)::bigint as total_attempts,
    case
      when sum(attempts) > 0 then (sum(hits)::numeric / sum(attempts)::numeric * 100)
      else 0
    end as hit_rate,
    jsonb_agg(
      jsonb_build_object(
        'date', run_at::date,
        'hits', hits,
        'attempts', attempts
      ) order by run_at desc
    ) as recent_trend
  from runs;
$$;
