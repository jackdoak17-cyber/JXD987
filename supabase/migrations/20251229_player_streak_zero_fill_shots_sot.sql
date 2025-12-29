-- Zero-fill shots (42) and shots on target (86) when minutes > 0.
-- Keep strict completeness for other stats.

DROP FUNCTION IF EXISTS public.player_streak_meta(
  integer,
  integer,
  integer,
  numeric,
  integer,
  boolean,
  integer,
  integer,
  integer
);

DROP FUNCTION IF EXISTS public.player_streak_playing_on_date_counts(
  date,
  integer,
  integer,
  integer,
  numeric,
  integer,
  boolean,
  integer,
  integer,
  integer,
  integer
);

DROP FUNCTION IF EXISTS public.player_stat_window_last_n(
  integer,
  integer,
  integer,
  integer,
  boolean,
  integer,
  integer,
  integer,
  integer
);

DROP FUNCTION IF EXISTS public.player_streak_base(
  integer,
  integer,
  integer,
  numeric,
  integer,
  numeric,
  boolean,
  integer,
  integer,
  integer,
  integer
);

DROP FUNCTION IF EXISTS public.player_streak_playing_on_date(
  date,
  integer,
  integer,
  integer,
  numeric,
  integer,
  numeric,
  boolean,
  integer,
  integer,
  integer,
  integer,
  integer,
  integer
);

CREATE OR REPLACE FUNCTION public.player_streak_base(
  p_league_id integer,
  p_type_id integer,
  p_n integer,
  p_threshold numeric,
  p_required integer,
  p_min_avg numeric default null,
  p_started_only boolean default false,
  p_min_minutes integer default null,
  p_season_id integer default null,
  p_recent_days integer default null,
  p_limit integer default 200
)
RETURNS TABLE (
  player_id integer,
  player_name text,
  player_common_name text,
  player_short_name text,
  team_id integer,
  team_name text,
  team_short_code text,
  team_image_path text,
  games integer,
  games_hit integer,
  avg_value numeric,
  last_values numeric[],
  last_fixture_at timestamptz,
  window_start_at timestamptz
)
LANGUAGE sql
STABLE
AS $$
  WITH latest_app AS (
    SELECT
      fp.player_id,
      fp.team_id,
      f.starting_at,
      f.id AS fixture_id,
      row_number() OVER (
        PARTITION BY fp.player_id
        ORDER BY f.starting_at DESC, f.id DESC
      ) AS rn
    FROM fixture_players fp
    JOIN fixtures f ON f.id = fp.fixture_id
    WHERE f.league_id = p_league_id
      AND (p_season_id IS NULL OR f.season_id = p_season_id)
      AND f.home_score IS NOT NULL
      AND f.away_score IS NOT NULL
  ),
  latest AS (
    SELECT
      player_id,
      team_id AS latest_team_id,
      starting_at AS latest_starting_at
    FROM latest_app
    WHERE rn = 1
  ),
  candidates_all AS (
    SELECT fp.player_id
    FROM fixture_players fp
    JOIN fixtures f ON f.id = fp.fixture_id
    WHERE f.league_id = p_league_id
      AND (p_season_id IS NULL OR f.season_id = p_season_id)
      AND f.home_score IS NOT NULL
      AND f.away_score IS NOT NULL
      AND (NOT p_started_only OR fp.is_starter IS TRUE)
      AND (p_min_minutes IS NULL OR fp.minutes_played >= p_min_minutes)
    GROUP BY fp.player_id
    HAVING count(*) >= p_n
  ),
  candidates AS (
    SELECT fp.player_id
    FROM fixture_players fp
    JOIN fixtures f ON f.id = fp.fixture_id
    WHERE f.league_id = p_league_id
      AND (p_season_id IS NULL OR f.season_id = p_season_id)
      AND f.home_score IS NOT NULL
      AND f.away_score IS NOT NULL
      AND (NOT p_started_only OR fp.is_starter IS TRUE)
      AND (p_min_minutes IS NULL OR fp.minutes_played >= p_min_minutes)
      AND coalesce(fp.minutes_played, 0) > 0
    GROUP BY fp.player_id
    HAVING count(*) >= p_n
  ),
  per_player AS (
    SELECT
      c.player_id,
      stats.games_total,
      stats.games_with_stat,
      stats.games_hit,
      stats.avg_value,
      stats.last_values,
      stats.last_fixture_at,
      stats.window_start_at
    FROM candidates c
    JOIN LATERAL (
      WITH last_n AS (
        SELECT
          fp.player_id,
          fp.fixture_id,
          f.starting_at
        FROM fixture_players fp
        JOIN fixtures f ON f.id = fp.fixture_id
        WHERE fp.player_id = c.player_id
          AND f.league_id = p_league_id
          AND (p_season_id IS NULL OR f.season_id = p_season_id)
          AND f.home_score IS NOT NULL
          AND f.away_score IS NOT NULL
          AND (NOT p_started_only OR fp.is_starter IS TRUE)
          AND (p_min_minutes IS NULL OR fp.minutes_played >= p_min_minutes)
          AND coalesce(fp.minutes_played, 0) > 0
        ORDER BY f.starting_at DESC, f.id DESC
        LIMIT p_n
      ),
      stats AS (
        SELECT
          ln.player_id,
          ln.fixture_id,
          max(fps.value) AS raw_value
        FROM last_n ln
        LEFT JOIN fixture_player_statistics fps
          ON fps.fixture_id = ln.fixture_id
         AND fps.player_id = ln.player_id
         AND fps.type_id = p_type_id
        GROUP BY ln.player_id, ln.fixture_id
      ),
      stats_enriched AS (
        SELECT
          stats.player_id,
          stats.fixture_id,
          CASE
            WHEN stats.raw_value IS NULL AND p_type_id IN (42, 86)
              THEN 0::numeric
            ELSE stats.raw_value::numeric
          END AS stat_value
        FROM stats
      )
      SELECT
        ln.player_id,
        count(*) AS games_total,
        count(stats_enriched.stat_value) AS games_with_stat,
        sum(CASE WHEN stats_enriched.stat_value >= p_threshold THEN 1 ELSE 0 END) AS games_hit,
        avg(stats_enriched.stat_value) AS avg_value,
        array_agg(
          stats_enriched.stat_value
          ORDER BY ln.starting_at DESC, ln.fixture_id DESC
        ) AS last_values,
        max(ln.starting_at) AS last_fixture_at,
        min(ln.starting_at) AS window_start_at
      FROM last_n ln
      LEFT JOIN stats_enriched
        ON stats_enriched.player_id = ln.player_id
       AND stats_enriched.fixture_id = ln.fixture_id
      GROUP BY ln.player_id
    ) stats ON true
  )
  SELECT
    per_player.player_id::integer,
    p.name::text AS player_name,
    p.common_name::text AS player_common_name,
    p.short_name::text AS player_short_name,
    latest.latest_team_id::integer AS team_id,
    t.name::text AS team_name,
    t.short_code::text AS team_short_code,
    t.image_path::text AS team_image_path,
    per_player.games_total::integer AS games,
    per_player.games_hit::integer,
    per_player.avg_value::numeric,
    per_player.last_values::numeric[] AS last_values,
    per_player.last_fixture_at AS last_fixture_at,
    per_player.window_start_at AS window_start_at
  FROM per_player
  JOIN players p ON p.id = per_player.player_id
  JOIN latest ON latest.player_id = per_player.player_id
  LEFT JOIN teams t ON t.id = latest.latest_team_id
  WHERE per_player.games_total = p_n
    AND per_player.games_with_stat = p_n
    AND per_player.games_hit >= p_required
    AND (p_min_avg IS NULL OR per_player.avg_value >= p_min_avg)
    AND (
      p_recent_days IS NULL
      OR (per_player.last_fixture_at AT TIME ZONE 'Europe/London')::date >=
        ((now() AT TIME ZONE 'Europe/London')::date - (greatest(p_recent_days, 1) - 1))
    )
  ORDER BY per_player.games_hit DESC, per_player.avg_value DESC
  LIMIT p_limit;
$$;

CREATE OR REPLACE FUNCTION public.player_streak_meta(
  p_league_id integer,
  p_type_id integer,
  p_n integer,
  p_threshold numeric,
  p_required integer,
  p_started_only boolean default false,
  p_min_minutes integer default null,
  p_season_id integer default null,
  p_recent_days integer default null
)
RETURNS TABLE (
  candidate_players_count integer,
  complete_windows_count integer,
  qualified_players_count integer,
  zero_filled_count integer,
  excluded_no_minutes_count integer
)
LANGUAGE sql
STABLE
AS $$
  WITH base_appearances AS (
    SELECT
      fp.player_id,
      fp.minutes_played
    FROM fixture_players fp
    JOIN fixtures f ON f.id = fp.fixture_id
    WHERE f.league_id = p_league_id
      AND (p_season_id IS NULL OR f.season_id = p_season_id)
      AND f.home_score IS NOT NULL
      AND f.away_score IS NOT NULL
      AND (NOT p_started_only OR fp.is_starter IS TRUE)
      AND (p_min_minutes IS NULL OR fp.minutes_played >= p_min_minutes)
  ),
  candidates_all AS (
    SELECT player_id
    FROM base_appearances
    GROUP BY player_id
    HAVING count(*) >= p_n
  ),
  candidates_played AS (
    SELECT player_id
    FROM base_appearances
    WHERE coalesce(minutes_played, 0) > 0
    GROUP BY player_id
    HAVING count(*) >= p_n
  ),
  excluded_no_minutes AS (
    SELECT count(*)::integer AS excluded_count
    FROM candidates_all
    WHERE player_id NOT IN (SELECT player_id FROM candidates_played)
  ),
  per_player AS (
    SELECT
      c.player_id,
      stats.games_total,
      stats.games_with_stat,
      stats.games_hit,
      stats.last_fixture_at,
      stats.zero_filled_count
    FROM candidates_played c
    JOIN LATERAL (
      WITH last_n AS (
        SELECT
          fp.player_id,
          fp.fixture_id,
          f.starting_at
        FROM fixture_players fp
        JOIN fixtures f ON f.id = fp.fixture_id
        WHERE fp.player_id = c.player_id
          AND f.league_id = p_league_id
          AND (p_season_id IS NULL OR f.season_id = p_season_id)
          AND f.home_score IS NOT NULL
          AND f.away_score IS NOT NULL
          AND (NOT p_started_only OR fp.is_starter IS TRUE)
          AND (p_min_minutes IS NULL OR fp.minutes_played >= p_min_minutes)
          AND coalesce(fp.minutes_played, 0) > 0
        ORDER BY f.starting_at DESC, f.id DESC
        LIMIT p_n
      ),
      stats AS (
        SELECT
          ln.player_id,
          ln.fixture_id,
          max(fps.value) AS raw_value
        FROM last_n ln
        LEFT JOIN fixture_player_statistics fps
          ON fps.fixture_id = ln.fixture_id
         AND fps.player_id = ln.player_id
         AND fps.type_id = p_type_id
        GROUP BY ln.player_id, ln.fixture_id
      ),
      stats_enriched AS (
        SELECT
          stats.player_id,
          stats.fixture_id,
          CASE
            WHEN stats.raw_value IS NULL AND p_type_id IN (42, 86)
              THEN 0::numeric
            ELSE stats.raw_value::numeric
          END AS stat_value,
          CASE
            WHEN stats.raw_value IS NULL AND p_type_id IN (42, 86)
              THEN 1
            ELSE 0
          END AS zero_filled
        FROM stats
      )
      SELECT
        ln.player_id,
        count(*) AS games_total,
        count(stats_enriched.stat_value) AS games_with_stat,
        sum(CASE WHEN stats_enriched.stat_value >= p_threshold THEN 1 ELSE 0 END) AS games_hit,
        max(ln.starting_at) AS last_fixture_at,
        sum(stats_enriched.zero_filled) AS zero_filled_count
      FROM last_n ln
      LEFT JOIN stats_enriched
        ON stats_enriched.player_id = ln.player_id
       AND stats_enriched.fixture_id = ln.fixture_id
      GROUP BY ln.player_id
    ) stats ON true
  ),
  complete AS (
    SELECT *
    FROM per_player
    WHERE games_total = p_n
      AND games_with_stat = p_n
      AND (
        p_recent_days IS NULL
        OR (last_fixture_at AT TIME ZONE 'Europe/London')::date >=
          ((now() AT TIME ZONE 'Europe/London')::date - (greatest(p_recent_days, 1) - 1))
      )
  )
  SELECT
    (SELECT count(*) FROM candidates_played)::integer AS candidate_players_count,
    (SELECT count(*) FROM complete)::integer AS complete_windows_count,
    (SELECT count(*) FROM complete WHERE games_hit >= p_required)::integer AS qualified_players_count,
    (SELECT coalesce(sum(zero_filled_count), 0) FROM complete)::integer AS zero_filled_count,
    (SELECT excluded_count FROM excluded_no_minutes)::integer AS excluded_no_minutes_count;
$$;

CREATE OR REPLACE FUNCTION public.player_stat_window_last_n(
  p_league_id integer,
  p_type_id integer,
  p_n integer,
  p_player_id integer default null,
  p_started_only boolean default false,
  p_min_minutes integer default null,
  p_season_id integer default null,
  p_recent_days integer default null,
  p_limit integer default 20
)
RETURNS TABLE (
  player_id integer,
  player_name text,
  player_common_name text,
  player_short_name text,
  team_id integer,
  team_name text,
  team_short_code text,
  team_image_path text,
  games integer,
  total_value numeric,
  avg_value numeric,
  last_values numeric[],
  last_fixture_at timestamptz,
  window_start_at timestamptz,
  last_fixture_dates timestamptz[]
)
LANGUAGE sql
STABLE
AS $$
  WITH latest_app AS (
    SELECT
      fp.player_id,
      fp.team_id,
      f.starting_at,
      f.id AS fixture_id,
      row_number() OVER (
        PARTITION BY fp.player_id
        ORDER BY f.starting_at DESC, f.id DESC
      ) AS rn
    FROM fixture_players fp
    JOIN fixtures f ON f.id = fp.fixture_id
    WHERE f.league_id = p_league_id
      AND (p_season_id IS NULL OR f.season_id = p_season_id)
      AND f.home_score IS NOT NULL
      AND f.away_score IS NOT NULL
      AND (p_player_id IS NULL OR fp.player_id = p_player_id)
  ),
  latest AS (
    SELECT
      player_id,
      team_id AS latest_team_id
    FROM latest_app
    WHERE rn = 1
  ),
  candidates AS (
    SELECT fp.player_id
    FROM fixture_players fp
    JOIN fixtures f ON f.id = fp.fixture_id
    WHERE f.league_id = p_league_id
      AND (p_season_id IS NULL OR f.season_id = p_season_id)
      AND f.home_score IS NOT NULL
      AND f.away_score IS NOT NULL
      AND (p_player_id IS NULL OR fp.player_id = p_player_id)
      AND (NOT p_started_only OR fp.is_starter IS TRUE)
      AND (p_min_minutes IS NULL OR fp.minutes_played >= p_min_minutes)
      AND coalesce(fp.minutes_played, 0) > 0
    GROUP BY fp.player_id
    HAVING count(*) >= p_n
  ),
  per_player AS (
    SELECT
      c.player_id,
      stats.games_total,
      stats.games_with_stat,
      stats.total_value,
      stats.avg_value,
      stats.last_values,
      stats.last_fixture_at,
      stats.window_start_at,
      stats.last_fixture_dates
    FROM candidates c
    JOIN LATERAL (
      WITH last_n AS (
        SELECT
          fp.player_id,
          fp.fixture_id,
          f.starting_at
        FROM fixture_players fp
        JOIN fixtures f ON f.id = fp.fixture_id
        WHERE fp.player_id = c.player_id
          AND f.league_id = p_league_id
          AND (p_season_id IS NULL OR f.season_id = p_season_id)
          AND f.home_score IS NOT NULL
          AND f.away_score IS NOT NULL
          AND (NOT p_started_only OR fp.is_starter IS TRUE)
          AND (p_min_minutes IS NULL OR fp.minutes_played >= p_min_minutes)
          AND coalesce(fp.minutes_played, 0) > 0
        ORDER BY f.starting_at DESC, f.id DESC
        LIMIT p_n
      ),
      stats AS (
        SELECT
          ln.player_id,
          ln.fixture_id,
          max(fps.value) AS raw_value
        FROM last_n ln
        LEFT JOIN fixture_player_statistics fps
          ON fps.fixture_id = ln.fixture_id
         AND fps.player_id = ln.player_id
         AND fps.type_id = p_type_id
        GROUP BY ln.player_id, ln.fixture_id
      ),
      stats_enriched AS (
        SELECT
          stats.player_id,
          stats.fixture_id,
          CASE
            WHEN stats.raw_value IS NULL AND p_type_id IN (42, 86)
              THEN 0::numeric
            ELSE stats.raw_value::numeric
          END AS stat_value
        FROM stats
      )
      SELECT
        ln.player_id,
        count(*) AS games_total,
        count(stats_enriched.stat_value) AS games_with_stat,
        sum(stats_enriched.stat_value) AS total_value,
        avg(stats_enriched.stat_value) AS avg_value,
        array_agg(
          stats_enriched.stat_value
          ORDER BY ln.starting_at DESC, ln.fixture_id DESC
        ) AS last_values,
        max(ln.starting_at) AS last_fixture_at,
        min(ln.starting_at) AS window_start_at,
        array_agg(
          ln.starting_at
          ORDER BY ln.starting_at DESC, ln.fixture_id DESC
        ) AS last_fixture_dates
      FROM last_n ln
      LEFT JOIN stats_enriched
        ON stats_enriched.player_id = ln.player_id
       AND stats_enriched.fixture_id = ln.fixture_id
      GROUP BY ln.player_id
    ) stats ON true
  )
  SELECT
    per_player.player_id::integer,
    p.name::text AS player_name,
    p.common_name::text AS player_common_name,
    p.short_name::text AS player_short_name,
    latest.latest_team_id::integer AS team_id,
    t.name::text AS team_name,
    t.short_code::text AS team_short_code,
    t.image_path::text AS team_image_path,
    per_player.games_total::integer AS games,
    per_player.total_value::numeric AS total_value,
    per_player.avg_value::numeric AS avg_value,
    per_player.last_values::numeric[] AS last_values,
    per_player.last_fixture_at AS last_fixture_at,
    per_player.window_start_at AS window_start_at,
    per_player.last_fixture_dates AS last_fixture_dates
  FROM per_player
  JOIN players p ON p.id = per_player.player_id
  JOIN latest ON latest.player_id = per_player.player_id
  LEFT JOIN teams t ON t.id = latest.latest_team_id
  WHERE per_player.games_total = p_n
    AND per_player.games_with_stat = p_n
    AND (
      p_recent_days IS NULL
      OR (per_player.last_fixture_at AT TIME ZONE 'Europe/London')::date >=
        ((now() AT TIME ZONE 'Europe/London')::date - (greatest(p_recent_days, 1) - 1))
    )
  ORDER BY per_player.avg_value DESC, per_player.total_value DESC
  LIMIT p_limit;
$$;

CREATE OR REPLACE FUNCTION public.player_streak_playing_on_date(
  p_date date,
  p_league_id integer,
  p_type_id integer,
  p_n integer,
  p_threshold numeric,
  p_required integer,
  p_min_avg numeric default null,
  p_started_only boolean default false,
  p_min_minutes integer default null,
  p_season_id integer default null,
  p_team_id integer default null,
  p_result_limit integer default 200,
  p_recent_days integer default null,
  p_limit integer default 20
)
RETURNS TABLE (
  player_id integer,
  player_name text,
  player_common_name text,
  player_short_name text,
  team_id integer,
  team_name text,
  team_short_code text,
  team_image_path text,
  games integer,
  games_hit integer,
  avg_value numeric,
  last_values numeric[],
  last_fixture_at timestamptz,
  window_start_at timestamptz
)
LANGUAGE sql
STABLE
AS $$
  WITH todays_fixtures AS (
    SELECT
      f.id,
      f.home_team_id,
      f.away_team_id
    FROM fixtures f
    WHERE f.league_id = p_league_id
      AND (p_season_id IS NULL OR f.season_id = p_season_id)
      AND (f.starting_at AT TIME ZONE 'Europe/London')::date = p_date
  ),
  todays_teams AS (
    SELECT DISTINCT tf.home_team_id AS team_id
    FROM todays_fixtures tf
    WHERE tf.home_team_id IS NOT NULL
    UNION
    SELECT DISTINCT tf.away_team_id AS team_id
    FROM todays_fixtures tf
    WHERE tf.away_team_id IS NOT NULL
  ),
  candidates AS (
    SELECT DISTINCT tlp.player_id
    FROM team_likely_players tlp
    JOIN todays_teams tt ON tt.team_id = tlp.team_id
    WHERE p_team_id IS NULL OR tlp.team_id = p_team_id
  ),
  appearances AS (
    SELECT
      fp.player_id,
      fp.team_id,
      f.id AS fixture_id,
      f.starting_at,
      max(fps.value) AS raw_value
    FROM fixture_players fp
    JOIN fixtures f ON f.id = fp.fixture_id
    LEFT JOIN fixture_player_statistics fps
      ON fps.fixture_id = f.id
     AND fps.player_id = fp.player_id
     AND fps.type_id = p_type_id
    WHERE fp.player_id IN (SELECT player_id FROM candidates)
      AND f.league_id = p_league_id
      AND (p_season_id IS NULL OR f.season_id = p_season_id)
      AND f.home_score IS NOT NULL
      AND f.away_score IS NOT NULL
      AND (NOT p_started_only OR fp.is_starter IS TRUE)
      AND (p_min_minutes IS NULL OR fp.minutes_played >= p_min_minutes)
      AND coalesce(fp.minutes_played, 0) > 0
    GROUP BY fp.player_id, fp.team_id, f.id, f.starting_at
  ),
  appearances_enriched AS (
    SELECT
      appearances.player_id,
      appearances.team_id,
      appearances.fixture_id,
      appearances.starting_at,
      CASE
        WHEN appearances.raw_value IS NULL AND p_type_id IN (42, 86)
          THEN 0::numeric
        ELSE appearances.raw_value::numeric
      END AS stat_value
    FROM appearances
  ),
  ranked AS (
    SELECT
      appearances_enriched.*,
      row_number() OVER (
        PARTITION BY appearances_enriched.player_id
        ORDER BY appearances_enriched.starting_at DESC, appearances_enriched.fixture_id DESC
      ) AS rn
    FROM appearances_enriched
  ),
  windowed AS (
    SELECT * FROM ranked WHERE rn <= p_n
  ),
  agg AS (
    SELECT
      windowed.player_id,
      count(*) AS games_total,
      count(windowed.stat_value) AS games_with_stat,
      sum(CASE WHEN windowed.stat_value >= p_threshold THEN 1 ELSE 0 END) AS games_hit,
      avg(windowed.stat_value) AS avg_value,
      array_agg(
        windowed.stat_value
        ORDER BY windowed.starting_at DESC, windowed.fixture_id DESC
      ) AS last_values,
      max(windowed.starting_at) AS last_fixture_at,
      min(windowed.starting_at) AS window_start_at
    FROM windowed
    GROUP BY windowed.player_id
  ),
  latest_window_team AS (
    SELECT
      windowed.player_id,
      windowed.team_id,
      windowed.starting_at
    FROM windowed
    WHERE windowed.rn = 1
  ),
  ordered AS (
    SELECT
      agg.player_id::integer,
      p.name::text AS player_name,
      p.common_name::text AS player_common_name,
      p.short_name::text AS player_short_name,
      latest_window_team.team_id::integer AS team_id,
      t.name::text AS team_name,
      t.short_code::text AS team_short_code,
      t.image_path::text AS team_image_path,
      agg.games_total::integer AS games,
      agg.games_hit::integer AS games_hit,
      agg.avg_value::numeric AS avg_value,
      agg.last_values::numeric[] AS last_values,
      agg.last_fixture_at AS last_fixture_at,
      agg.window_start_at AS window_start_at
    FROM agg
    JOIN latest_window_team ON latest_window_team.player_id = agg.player_id
    JOIN players p ON p.id = agg.player_id
    LEFT JOIN teams t ON t.id = latest_window_team.team_id
    WHERE agg.games_total = p_n
      AND agg.games_with_stat = p_n
      AND agg.games_hit >= p_required
      AND (p_min_avg IS NULL OR agg.avg_value >= p_min_avg)
      AND (p_team_id IS NULL OR latest_window_team.team_id = p_team_id)
      AND (
        p_recent_days IS NULL
        OR (agg.last_fixture_at AT TIME ZONE 'Europe/London')::date >=
          ((now() AT TIME ZONE 'Europe/London')::date - (greatest(p_recent_days, 1) - 1))
      )
    ORDER BY agg.games_hit DESC, agg.avg_value DESC
    LIMIT p_result_limit
  )
  SELECT * FROM ordered LIMIT p_limit;
$$;

CREATE OR REPLACE FUNCTION public.player_streak_playing_on_date_counts(
  p_date date,
  p_league_id integer,
  p_type_id integer,
  p_n integer,
  p_threshold numeric,
  p_required integer,
  p_started_only boolean default false,
  p_min_minutes integer default null,
  p_season_id integer default null,
  p_team_id integer default null,
  p_recent_days integer default null
)
RETURNS TABLE (
  today_fixtures_count integer,
  today_teams_count integer,
  candidate_players_count integer,
  complete_windows_count integer,
  qualified_players_count integer,
  zero_filled_count integer,
  excluded_no_minutes_count integer
)
LANGUAGE sql
STABLE
AS $$
  WITH todays_fixtures AS (
    SELECT
      f.id,
      f.home_team_id,
      f.away_team_id
    FROM fixtures f
    WHERE f.league_id = p_league_id
      AND (p_season_id IS NULL OR f.season_id = p_season_id)
      AND (f.starting_at AT TIME ZONE 'Europe/London')::date = p_date
  ),
  todays_teams AS (
    SELECT DISTINCT tf.home_team_id AS team_id
    FROM todays_fixtures tf
    WHERE tf.home_team_id IS NOT NULL
    UNION
    SELECT DISTINCT tf.away_team_id AS team_id
    FROM todays_fixtures tf
    WHERE tf.away_team_id IS NOT NULL
  ),
  candidates AS (
    SELECT DISTINCT tlp.player_id
    FROM team_likely_players tlp
    JOIN todays_teams tt ON tt.team_id = tlp.team_id
    WHERE p_team_id IS NULL OR tlp.team_id = p_team_id
  ),
  base_appearances AS (
    SELECT
      fp.player_id,
      fp.minutes_played
    FROM fixture_players fp
    JOIN fixtures f ON f.id = fp.fixture_id
    WHERE fp.player_id IN (SELECT player_id FROM candidates)
      AND f.league_id = p_league_id
      AND (p_season_id IS NULL OR f.season_id = p_season_id)
      AND f.home_score IS NOT NULL
      AND f.away_score IS NOT NULL
      AND (NOT p_started_only OR fp.is_starter IS TRUE)
      AND (p_min_minutes IS NULL OR fp.minutes_played >= p_min_minutes)
  ),
  candidates_all AS (
    SELECT player_id
    FROM base_appearances
    GROUP BY player_id
    HAVING count(*) >= p_n
  ),
  candidates_played AS (
    SELECT player_id
    FROM base_appearances
    WHERE coalesce(minutes_played, 0) > 0
    GROUP BY player_id
    HAVING count(*) >= p_n
  ),
  excluded_no_minutes AS (
    SELECT count(*)::integer AS excluded_count
    FROM candidates_all
    WHERE player_id NOT IN (SELECT player_id FROM candidates_played)
  ),
  appearances AS (
    SELECT
      fp.player_id,
      f.id AS fixture_id,
      f.starting_at,
      max(fps.value) AS raw_value
    FROM fixture_players fp
    JOIN fixtures f ON f.id = fp.fixture_id
    LEFT JOIN fixture_player_statistics fps
      ON fps.fixture_id = f.id
     AND fps.player_id = fp.player_id
     AND fps.type_id = p_type_id
    WHERE fp.player_id IN (SELECT player_id FROM candidates_played)
      AND f.league_id = p_league_id
      AND (p_season_id IS NULL OR f.season_id = p_season_id)
      AND f.home_score IS NOT NULL
      AND f.away_score IS NOT NULL
      AND (NOT p_started_only OR fp.is_starter IS TRUE)
      AND (p_min_minutes IS NULL OR fp.minutes_played >= p_min_minutes)
      AND coalesce(fp.minutes_played, 0) > 0
    GROUP BY fp.player_id, f.id, f.starting_at
  ),
  appearances_enriched AS (
    SELECT
      appearances.player_id,
      appearances.fixture_id,
      appearances.starting_at,
      CASE
        WHEN appearances.raw_value IS NULL AND p_type_id IN (42, 86)
          THEN 0::numeric
        ELSE appearances.raw_value::numeric
      END AS stat_value,
      CASE
        WHEN appearances.raw_value IS NULL AND p_type_id IN (42, 86)
          THEN 1
        ELSE 0
      END AS zero_filled
    FROM appearances
  ),
  ranked AS (
    SELECT
      appearances_enriched.*,
      row_number() OVER (
        PARTITION BY appearances_enriched.player_id
        ORDER BY appearances_enriched.starting_at DESC, appearances_enriched.fixture_id DESC
      ) AS rn
    FROM appearances_enriched
  ),
  windowed AS (
    SELECT * FROM ranked WHERE rn <= p_n
  ),
  agg AS (
    SELECT
      windowed.player_id,
      count(*) AS games_total,
      count(windowed.stat_value) AS games_with_stat,
      sum(CASE WHEN windowed.stat_value >= p_threshold THEN 1 ELSE 0 END) AS games_hit,
      max(windowed.starting_at) AS last_fixture_at,
      sum(windowed.zero_filled) AS zero_filled_count
    FROM windowed
    GROUP BY windowed.player_id
  ),
  complete AS (
    SELECT *
    FROM agg
    WHERE games_total = p_n
      AND games_with_stat = p_n
      AND (
        p_recent_days IS NULL
        OR (last_fixture_at AT TIME ZONE 'Europe/London')::date >=
          ((now() AT TIME ZONE 'Europe/London')::date - (greatest(p_recent_days, 1) - 1))
      )
  )
  SELECT
    (SELECT count(*) FROM todays_fixtures)::integer AS today_fixtures_count,
    (SELECT count(*) FROM todays_teams)::integer AS today_teams_count,
    (SELECT count(*) FROM candidates)::integer AS candidate_players_count,
    (SELECT count(*) FROM complete)::integer AS complete_windows_count,
    (SELECT count(*) FROM complete WHERE games_hit >= p_required)::integer AS qualified_players_count,
    (SELECT coalesce(sum(zero_filled_count), 0) FROM complete)::integer AS zero_filled_count,
    (SELECT excluded_count FROM excluded_no_minutes)::integer AS excluded_no_minutes_count;
$$;
