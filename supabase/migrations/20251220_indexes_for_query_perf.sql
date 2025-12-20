create index if not exists fixtures_league_start_idx
on public.fixtures (league_id, starting_at desc);

create index if not exists fixture_players_fixture_player_idx
on public.fixture_players (fixture_id, player_id);

create index if not exists fixture_players_player_fixture_idx
on public.fixture_players (player_id, fixture_id);

create index if not exists fps_type_player_fixture_idx
on public.fixture_player_statistics (type_id, player_id, fixture_id);

create index if not exists fs_type_team_fixture_idx
on public.fixture_statistics (type_id, team_id, fixture_id);
