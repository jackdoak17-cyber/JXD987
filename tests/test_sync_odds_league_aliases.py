from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json

from scripts import sync_odds


class FakeOddsClient:
    responses: dict[str, object] = {}
    calls: list[tuple[str, dict[str, object]]] = []

    def __init__(self, *args, **kwargs) -> None:
        self.stats = type(
            "Stats",
            (),
            {
                "total_calls": 0,
                "api_time_seconds": 0.0,
                "rate_limit_hits": 0,
                "rate_limit_sleeps": 0,
                "calls_by_endpoint": {},
                "last_rate_limit": None,
            },
        )()

    def request(self, path: str, params=None):
        params = dict(params or {})
        self.__class__.calls.append((path, params))
        self.stats.total_calls += 1
        self.stats.calls_by_endpoint[path] = self.stats.calls_by_endpoint.get(path, 0) + 1
        key = params.get("league") if path == "events" else path
        value = self.__class__.responses[key]
        if isinstance(value, Exception):
            raise value
        return value


def fixture() -> dict[str, object]:
    kickoff = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=4)
    return {
        "fixture_id": 19904944,
        "league_id": 779,
        "starting_at": kickoff,
        "home_alias": sync_odds.team_aliases("Charlotte", "CLT"),
        "away_alias": sync_odds.team_aliases("Chicago Fire", "CHI"),
        "has_complete_moneyline_odds": False,
    }


def event(fixture_row: dict[str, object]) -> dict[str, object]:
    kickoff = fixture_row["starting_at"]
    assert isinstance(kickoff, datetime)
    return {
        "id": 76609142,
        "home": "Charlotte FC",
        "away": "Chicago Fire",
        "date": kickoff.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z"),
        "status": "pending",
    }


def run_fetch(monkeypatch, responses: dict[str, object]):
    FakeOddsClient.responses = responses
    FakeOddsClient.calls = []
    monkeypatch.setattr(sync_odds, "OddsApiClient", FakeOddsClient)
    row = fixture()
    result = sync_odds.fetch_league_odds_payload(
        779,
        ["usa-mls", "usa-mls-playoffs"],
        [row],
        "football",
        0,
        14,
        ["Bet365"],
        0,
    )
    return row, result


def test_load_league_map_accepts_legacy_strings_and_alias_lists(tmp_path) -> None:
    path = tmp_path / "leagues.json"
    path.write_text(
        json.dumps({"8": "england-premier-league", "779": ["usa-mls", "usa-mls-playoffs"]}),
        encoding="utf-8",
    )
    assert sync_odds.load_league_map(path) == {
        8: ["england-premier-league"],
        779: ["usa-mls", "usa-mls-playoffs"],
    }


def test_fetch_combines_aliases_and_deduplicates_events(monkeypatch) -> None:
    row = fixture()
    shared = event(row)
    _, result = run_fetch(
        monkeypatch,
        {
            "usa-mls": [shared],
            "usa-mls-playoffs": [shared],
            "odds/multi": [{"id": shared["id"], "bookmakers": {"Bet365": []}}],
        },
    )
    assert result.error is None
    assert result.events_returned == 1
    assert result.events_matched == 1
    assert result.fixtures_with_odds_payload == 1
    assert len(result.odds_records) == 1
    assert result.alias_stats["usa-mls"]["events_matched"] == 1
    assert result.alias_stats["usa-mls-playoffs"]["events_matched"] == 1
    assert [path for path, _ in FakeOddsClient.calls].count("odds/multi") == 1


def test_partial_alias_failure_keeps_successful_alias(monkeypatch) -> None:
    row = fixture()
    playoff = event(row)
    _, result = run_fetch(
        monkeypatch,
        {
            "usa-mls": sync_odds.OddsApiError("regular feed unavailable"),
            "usa-mls-playoffs": [playoff],
            "odds/multi": [{"id": playoff["id"], "bookmakers": {"Bet365": []}}],
        },
    )
    assert result.error is None
    assert result.alias_errors == {"usa-mls": "regular feed unavailable"}
    assert result.events_matched == 1
    assert result.fixtures_with_odds_payload == 1


def test_all_alias_failures_fail_the_league(monkeypatch) -> None:
    _, result = run_fetch(
        monkeypatch,
        {
            "usa-mls": sync_odds.OddsApiError("regular failed"),
            "usa-mls-playoffs": sync_odds.OddsApiError("playoffs failed"),
        },
    )
    assert result.error is not None
    assert set(result.alias_errors) == {"usa-mls", "usa-mls-playoffs"}


def test_coverage_gap_distinguishes_no_events_from_no_match() -> None:
    base = sync_odds.LeagueResult(
        league_id=779,
        league_name="usa-mls,usa-mls-playoffs",
        fixtures_fetched=6,
        odds_records=[],
        api_calls_made=2,
        fetch_duration_seconds=0,
        error=None,
        alias_stats={"usa-mls": {"events_returned": 0}},
    )
    assert sync_odds.league_coverage_gap(base)["reason"] == "scheduled_fixtures_but_no_provider_events"
    base.events_returned = 4
    assert sync_odds.league_coverage_gap(base)["reason"] == "provider_events_but_no_fixture_matches"
    base.events_matched = 4
    assert sync_odds.league_coverage_gap(base) is None
