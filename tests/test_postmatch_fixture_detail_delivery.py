from __future__ import annotations

import sqlite3

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from jxd.models import Base, Fixture, FixturePlayer, FixturePlayerStatistic, FixtureStatistic
from jxd.sync import SyncService
from scripts.postmatch_fixture_detail_delivery import (
    DetailSnapshot,
    assess_provider_payload,
    candidate_fixture_ids,
    compare_snapshots,
    DERIVED_STAT_TYPE_IDS,
    delivery_reason_code,
    ensure_ledger,
    is_non_competitive_provider_assessment,
    source_engine,
    source_ready,
    stable_provider_sparse_assessment,
    TRACKED_TEAM_STAT_TYPES,
)
from jxd.sync import _extract_stat_value


def provider_payload(*, include_big_chances: bool = True) -> dict:
    stats = []
    for team_id in (101, 202):
        for type_id in (42, 45, 56, 57, 78, 83, 84, 85, 86, 100, 109):
            stats.append({"participant_id": team_id, "type_id": type_id, "data": {"value": 1}})
        if include_big_chances:
            stats.append({"participant_id": team_id, "type_id": 581, "data": {"value": 1}})
    return {
        "id": 9001,
        "state": {"short_name": "FT"},
        "participants": [{"id": 101}, {"id": 202}],
        "statistics": stats,
        "lineups": [
            {"team_id": 101, "player_id": 11, "details": [{"type_id": 119, "data": {"value": 90}}]},
            {"team_id": 202, "player_id": 22, "details": [{"type_id": 119, "data": {"value": 90}}]},
        ],
    }


def test_provider_assessment_distinguishes_ready_sparse_and_pending() -> None:
    assert assess_provider_payload(provider_payload()).status == "ready"
    assert assess_provider_payload(provider_payload(include_big_chances=False)).status == "provider_sparse"
    pending = provider_payload()
    pending["state"] = {"short_name": "NS"}
    assert assess_provider_payload(pending).status == "provider_pending"


def test_delivery_reason_code_is_stable_and_machine_readable() -> None:
    assert delivery_reason_code("provider_pending", "optional provider metrics are absent") == "provider_pending_optional_metrics"
    assert delivery_reason_code("provider_pending", "provider lineup/player identity detail incomplete") == "provider_pending_identity"
    assert delivery_reason_code("provider_pending", "provider detail collection shrank") == "provider_pending_shrink"
    assert delivery_reason_code("export_failed", "insert failed: foreign key violation") == "dependency_missing"
    assert delivery_reason_code("export_failed", "database connection failed") == "export_failed"
    assert delivery_reason_code("excluded", "Cup identity is excluded") == "excluded"
    assert delivery_reason_code("excluded", "SportMonks returned no fixture data") == "provider_unavailable"


def test_derived_stat_types_are_excluded_from_provider_shrink_counts() -> None:
    assert DERIVED_STAT_TYPE_IDS == {200001, 200010, 200011, 200012, 200013}


def test_provider_assessment_accepts_valid_fixture_with_optional_metric_gaps() -> None:
    payload = provider_payload()
    payload["statistics"] = [
        row for row in payload["statistics"]
        if row["type_id"] not in {78, 100, 109, 581}
    ]
    assessment = assess_provider_payload(payload)
    assert assessment.status == "provider_sparse"
    assert assessment.missing_team_stat_type_ids["101"] == [78, 100, 109, 581]
    assert assessment.missing_team_stat_type_ids["202"] == [78, 100, 109, 581]


def test_provider_assessment_accepts_fixture_without_team_stat_rows() -> None:
    payload = provider_payload()
    payload["statistics"] = [
        row for row in payload["statistics"] if row["participant_id"] == 101 and row["type_id"] == 45
    ]
    assert assess_provider_payload(payload).status == "provider_sparse"


def test_provider_assessment_requires_detail_structure() -> None:
    payload = provider_payload()
    payload["lineups"] = []
    assessment = assess_provider_payload(payload)
    assert assessment.status == "provider_pending"
    assert assessment.error == "provider lineup/player identity detail incomplete for team ids 101,202"


def test_provider_assessment_rejects_lineups_without_player_identity() -> None:
    payload = provider_payload()
    payload["lineups"][0]["player_id"] = None
    assessment = assess_provider_payload(payload)
    assert assessment.status == "provider_pending"
    assert assessment.error == "provider lineup/player identity detail incomplete for team ids 101"


def test_stable_incomplete_player_identity_becomes_provider_sparse() -> None:
    payload = provider_payload()
    payload["lineups"][0]["player_id"] = None
    assessment = assess_provider_payload(payload)
    assert stable_provider_sparse_assessment(assessment, 1) is None
    terminal = stable_provider_sparse_assessment(assessment, 2)
    assert terminal is not None
    assert terminal.status == "provider_sparse"
    assert "identical finished payloads" in (terminal.error or "")


def test_only_explicit_non_competitive_provider_status_is_excluded() -> None:
    abandoned = provider_payload()
    abandoned["state"] = {"short_name": "ABAN"}
    assert is_non_competitive_provider_assessment(assess_provider_payload(abandoned))

    in_progress = provider_payload()
    in_progress["state"] = {"short_name": "1ST"}
    assert not is_non_competitive_provider_assessment(assess_provider_payload(in_progress))


def test_provider_revision_hash_is_stable_for_collection_order() -> None:
    from scripts.postmatch_fixture_detail_delivery import normalized_provider_hash, provider_payload_hash

    first = provider_payload()
    second = provider_payload()
    second["statistics"] = list(reversed(second["statistics"]))
    second["lineups"] = list(reversed(second["lineups"]))
    assert provider_payload_hash(first) != provider_payload_hash(second)
    assert normalized_provider_hash(first) == normalized_provider_hash(second)


def test_player_stat_parser_preserves_decimal_ratings() -> None:
    assert str(_extract_stat_value({"value": "7.85"})) == "7.85"
    assert _extract_stat_value({"value": "7"}) == 7


def test_source_ready_requires_player_detail_for_each_team() -> None:
    assessment = assess_provider_payload(provider_payload())
    team_types = {str(team_id): sorted(TRACKED_TEAM_STAT_TYPES) for team_id in (101, 202)}
    team_values = {f"{team_id}:{type_id}": 1 for team_id in (101, 202) for type_id in TRACKED_TEAM_STAT_TYPES}
    incomplete = DetailSnapshot(
        fixture_id=9001,
        team_stat_count=len(team_values),
        player_stat_count=1,
        lineup_count=2,
        team_stat_types=team_types,
        team_stat_values=team_values,
        player_stat_values={"11:101:119": 90},
        lineup_values={"11:101": (True, 90), "22:202": (True, 90)},
    )
    assert not source_ready(incomplete, assessment)

    complete = DetailSnapshot(
        **{**incomplete.__dict__, "player_stat_count": 2, "player_stat_values": {"11:101:119": 90, "22:202:119": 90}}
    )
    assert source_ready(complete, assessment)


def test_candidate_selection_is_due_and_does_not_require_local_scores() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute("create table fixtures (id integer, league_id integer, starting_at text)")
    conn.execute("insert into fixtures values (1, 8, datetime('now', '-4 hours'))")
    conn.execute("insert into fixtures values (2, 8, datetime('now', '-3 hours'))")
    ensure_ledger(conn)
    conn.execute(
        "insert into fixture_detail_deliveries(fixture_id,league_id,status,first_seen_at,next_attempt_at,updated_at) "
        "values (2,8,'verified','2020-01-01T00:00:00Z',null,'2020-01-01T00:00:00Z')"
    )
    conn.commit()
    # A verified fixture is revalidated when it has no prior schedule.
    assert candidate_fixture_ids(conn, [8], 72, 10) == [1, 2]


def test_candidate_selection_prioritizes_recent_fixtures_over_old_revalidation() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute("create table fixtures (id integer, league_id integer, starting_at text)")
    conn.execute("insert into fixtures values (1, 8, datetime('now', '-20 days'))")
    conn.execute("insert into fixtures values (2, 8, datetime('now', '-3 hours'))")
    ensure_ledger(conn)
    conn.execute(
        "insert into fixture_detail_deliveries(fixture_id,league_id,status,first_seen_at,next_revalidation_at,updated_at) "
        "values (1,8,'verified','2020-01-01T00:00:00Z','2020-01-02T00:00:00Z','2020-01-01T00:00:00Z')"
    )
    conn.commit()

    assert candidate_fixture_ids(conn, [8], 72, 1) == [2]


def test_source_engine_uses_configured_busy_timeout(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("JXD_SQLITE_BUSY_TIMEOUT_SECONDS", "12.5")
    engine = source_engine(str(tmp_path / "source.sqlite"))
    try:
        with engine.connect() as connection:
            timeout_ms = connection.exec_driver_sql("pragma busy_timeout").scalar_one()
        assert timeout_ms == 12_500
    finally:
        engine.dispose()


def test_snapshot_comparison_reports_value_and_row_differences() -> None:
    source = DetailSnapshot(
        fixture_id=1,
        team_stat_count=1,
        player_stat_count=1,
        lineup_count=1,
        team_stat_types={"1": [42]},
        team_stat_values={"1:42": 5},
        player_stat_values={"10:1:119": 90},
        lineup_values={"10:1": (True, 90)},
    )
    target = DetailSnapshot(
        fixture_id=1,
        team_stat_count=1,
        player_stat_count=0,
        lineup_count=0,
        team_stat_types={"1": [42]},
        team_stat_values={"1:42": 4},
        player_stat_values={},
        lineup_values={},
    )
    problems = compare_snapshots(source, target)
    assert any("team-stat values differ" in problem for problem in problems)
    assert any("player-stat parity differs" in problem for problem in problems)
    assert any("lineup parity differs" in problem for problem in problems)


def test_lineup_only_storage_preserves_player_statistics(tmp_path) -> None:
    engine = create_engine(f"sqlite:///{tmp_path / 'source.sqlite'}", future=True)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, future=True)
    session = Session()
    session.add(Fixture(id=9001, home_team_id=101, away_team_id=202))
    session.add(FixturePlayerStatistic(fixture_id=9001, player_id=11, team_id=101, type_id=119, code="minutes", value=90))
    session.add(FixturePlayer(fixture_id=9001, player_id=11, team_id=101, minutes_played=90))
    session.add(FixtureStatistic(fixture_id=9001, team_id=101, type_id=42, code="shots", value=5, location="home"))
    session.commit()

    service = SyncService(client=object(), session=session)
    lightweight = provider_payload()
    lightweight.pop("statistics")
    service._store_fixture_raw(lightweight, full_detail=False)
    session.commit()

    assert session.query(FixturePlayerStatistic).filter_by(fixture_id=9001, player_id=11, type_id=119).count() >= 1
    assert session.query(FixtureStatistic).filter_by(fixture_id=9001).count() == 1
    session.close()
