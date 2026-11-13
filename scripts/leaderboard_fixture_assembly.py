"""Assemble bounded fixture facts on the worker without changing canonical JSONB."""
from collections import defaultdict
import json
import re

ACCOUNTING_MARKER = "/* leaderboard_shadow_v1.fixture_stage_input */"
NUMERIC = re.compile(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?")

FIXTURE = """
SELECT f.league_id::integer, f.season_id::integer, to_jsonb(f.starting_at),
       f.home_team_id, f.away_team_id,
       f.home_score IS NOT NULL AND f.away_score IS NOT NULL
       AND upper(coalesce(nullif(btrim(f.status_code), ''), f.status, '')) IN
           ('FT','AET','FT_PEN','FT-PEN','FTP','AFTER_PENALTIES','FINISHED','ENDED')
       AND EXISTS (SELECT 1 FROM public.fixture_detail_delivery_status d
                   WHERE d.fixture_id=f.id AND d.provider_finished IS TRUE
                     AND d.status IN ('verified','provider_sparse'))
       AND NOT EXISTS (SELECT 1 FROM public.fixture_stats_quality_exclusions q
                       WHERE q.fixture_id=f.id)
FROM public.fixtures f WHERE f.id=%s
"""
STATISTICS = """
SELECT type_id,player_id, CASE WHEN octet_length(value::text)<=256 THEN value::text END,
       value IS NOT NULL AND octet_length(value::text)>256
FROM public.fixture_player_statistics WHERE fixture_id=%s {after}
ORDER BY type_id,player_id LIMIT 256
"""
LABELS = """
SELECT player_id,team_id,league_id,season_id,
       CASE WHEN greatest(octet_length(player_name),octet_length(team_name),
                          octet_length(player_image_path),octet_length(team_image_path))>2048
            THEN NULL ELSE jsonb_build_array(player_name,team_name,player_image_path,team_image_path) END
FROM leaderboard_shadow_v1.player_appearance_facts
WHERE fixture_id=%s AND valid_to_generation IS NULL
ORDER BY valid_from_generation DESC LIMIT 129
"""
APPEARANCES = """
SELECT p.player_id,p.team_id,p.minutes_played,
       coalesce(p.lineup_detailed_position_id,p.detailed_position_id),
       CASE WHEN greatest(octet_length(p.position_abbr),octet_length(p.position_name),
                          octet_length(p.detailed_position_name),octet_length(p.lineup_detailed_position_name),
                          octet_length(p.detailed_position_code),octet_length(p.lineup_detailed_position_code))>2048
            THEN jsonb_build_array(false)
            ELSE jsonb_build_array(true,leaderboard_shadow_v1.normalize_position_group(
                  p.position_abbr,p.position_name,p.detailed_position_name,p.lineup_detailed_position_name,
                  p.detailed_position_code,p.lineup_detailed_position_code)) END
FROM public.fixture_players p WHERE p.fixture_id=%s ORDER BY p.player_id LIMIT 129
"""
PERSON = """
SELECT CASE WHEN greatest(octet_length(name),octet_length(image_path))>2048 THEN NULL
            ELSE jsonb_build_array(name,image_path) END
FROM (SELECT coalesce(display_name,common_name,short_name,name) AS name,image_path
      FROM public.players WHERE id=%s) person
"""
TEAM = """
SELECT CASE WHEN greatest(octet_length(name),octet_length(image_path))>2048 THEN NULL
            ELSE jsonb_build_array(name,image_path) END
FROM public.teams WHERE id=%s
"""


def one(cur, sql, params):
    cur.execute(sql.replace("SELECT", f"SELECT {ACCOUNTING_MARKER}", 1), params)
    return cur.fetchone()


def rows(cur, sql, params):
    cur.execute(sql.replace("SELECT", f"SELECT {ACCOUNTING_MARKER}", 1), params)
    return cur.fetchall()


def load_metrics(cur, fixture, allowed):
    metrics = defaultdict(dict)
    continuation = ()
    count = 0
    while True:
        after = "AND (type_id,player_id)>(%s,%s)" if continuation else ""
        page = rows(cur, STATISTICS.format(after=after), (fixture, *continuation))
        count += len(page)
        if count > 4096:
            raise ValueError("fixture statistic row limit exceeded")
        for kind, player, numeric, too_long in page:
            if kind not in allowed:
                continue
            if too_long:
                raise ValueError("fixture statistic value limit exceeded")
            if numeric is not None:
                if not NUMERIC.fullmatch(numeric):
                    raise ValueError("fixture statistic is not finite")
                metrics[player][str(kind)] = numeric
        if len(page) < 256:
            return metrics
        continuation = page[-1][:2]


def label_pair(cur, sql, key):
    row = one(cur, sql, (key,))
    if row is None:
        return None, None
    value, = row
    if value is None:
        raise ValueError("fixture source label exceeds envelope")
    return value


def load_facts(cur, fixture, league, season, kickoff, home, away):
    allowed = rows(cur, "SELECT type_id FROM leaderboard_shadow_v1.stat_contracts LIMIT 257", ())
    if len(allowed) > 256:
        raise ValueError("fixture statistic contract limit exceeded")
    metrics = load_metrics(cur, fixture, {kind for kind, in allowed})
    labels = rows(cur, LABELS, (fixture,))
    appearances = rows(cur, APPEARANCES, (fixture,))
    if len(labels) > 128 or len(appearances) > 128:
        raise ValueError("fixture metadata row limit exceeded")

    published = {}
    for player, team, label_league, label_season, label in labels:
        if (label_league, label_season) != (league, season):
            continue
        if label is None or (player, team) in published:
            raise ValueError("fixture published metadata exceeds envelope")
        published[player, team] = label

    teams = {}
    for player, team, minutes, position, group in appearances:
        if team is None or minutes is None or not 1 <= minutes <= 130 or team not in (home, away):
            continue
        if not group[0]:
            raise ValueError("fixture position metadata exceeds envelope")
        if (player, team) in published:
            player_name, team_name, player_image, team_image = published[player, team]
        else:
            player_name, player_image = label_pair(cur, PERSON, player)
            if team not in teams:
                teams[team] = label_pair(cur, TEAM, team)
            team_name, team_image = teams[team]
        fact = dict(player_id=player, kickoff_at=kickoff, team_id=team,
                    player_name=player_name, team_name=team_name,
                    player_image_path=player_image, team_image_path=team_image,
                    venue="H" if team == home else "A", position_id=position, minutes=minutes)
        values = {**metrics.get(player, {}), "__position_group": encode(group[1])}
        metric_json = "{" + ",".join(encode(key) + ":" + value for key, value in values.items()) + "}"
        yield encode(fact)[:-1] + ',"metrics":' + metric_json + "}"


def encode(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def assemble(cur, fixture):
    row = one(cur, FIXTURE, (fixture,))
    if row is None:
        return None
    league, season, kickoff, home, away, eligible = row
    facts = []
    fact_bytes = 2
    if eligible:
        for fact in load_facts(cur, fixture, league, season, kickoff, home, away):
            fact_bytes += len(fact.encode()) + bool(facts)
            if fact_bytes > 524288:
                raise ValueError("fixture facts payload exceeds envelope")
            facts.append(fact)
    header = dict(fixture_id=fixture, league_id=league, season_id=season,
                  change_kind="upsert" if eligible else "exclude")
    return encode(header)[:-1] + ',"facts":[' + ",".join(facts) + "]}"


def read_fixture_stage_input(cur, fixture):
    payload = assemble(cur, fixture)
    if payload is None:
        return None, None
    return one(
        cur,
        "SELECT md5(payload::text), (payload->'facts')::text "
        "FROM (VALUES (%s::jsonb)) input(payload)",
        (payload,),
    )
