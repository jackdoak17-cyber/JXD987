create index if not exists fixtures_league_finished_start_idx
on public.fixtures (league_id, starting_at desc)
where home_score is not null and away_score is not null;

create index if not exists fixtures_league_season_start_idx
on public.fixtures (league_id, season_id, starting_at desc);

create index if not exists fixture_players_player_fixture_idx
on public.fixture_players (player_id, fixture_id);

create index if not exists fixture_players_fixture_player_idx
on public.fixture_players (fixture_id, player_id);

create index if not exists fixture_players_player_starter_minutes_idx
on public.fixture_players (player_id, is_starter, minutes_played);

create index if not exists fps_player_type_fixture_idx
on public.fixture_player_statistics (player_id, type_id, fixture_id);

create index if not exists fps_fixture_type_idx
on public.fixture_player_statistics (fixture_id, type_id);
