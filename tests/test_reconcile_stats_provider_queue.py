from __future__ import annotations

from scripts import reconcile_stats_provider_queue as queue


class FakeSportMonksClient:
    calls: list[str] = []
    responses: dict[str, list[int]] = {}

    def __init__(self) -> None:
        pass

    def request(self, method: str, endpoint: str, params: dict[str, object]) -> dict[str, object]:
        assert method == "GET"
        assert params["include"] == queue.DETAIL_INCLUDE
        self.calls.append(endpoint)
        ids = [int(value) for value in endpoint.rsplit("/", 1)[1].split(",")]
        returned = self.responses.get(endpoint, ids)
        return {"data": [{"id": fixture_id, "statistics": [], "lineups": []} for fixture_id in returned]}


def test_bulk_fetch_preserves_each_fixture_and_reports_one_http_call(monkeypatch) -> None:
    FakeSportMonksClient.calls = []
    FakeSportMonksClient.responses = {}
    monkeypatch.setattr(queue, "SportMonksClient", FakeSportMonksClient)

    fetched, errors, calls = queue.fetch_provider_fixture_batch([101, 202])

    assert sorted(fetched) == [101, 202]
    assert errors == {}
    assert calls == 1
    assert FakeSportMonksClient.calls == ["fixtures/multi/101,202"]


def test_bulk_fetch_keeps_provider_omissions_per_fixture(monkeypatch) -> None:
    FakeSportMonksClient.calls = []
    FakeSportMonksClient.responses = {"fixtures/multi/101,202": [101]}
    monkeypatch.setattr(queue, "SportMonksClient", FakeSportMonksClient)

    fetched, errors, calls = queue.fetch_provider_fixture_batch([101, 202])

    assert sorted(fetched) == [101]
    assert list(errors) == [202]
    assert "omitted requested fixture" in str(errors[202])
    assert calls == 1


def test_bulk_fetch_splits_batches_and_counts_http_requests(monkeypatch) -> None:
    FakeSportMonksClient.calls = []
    FakeSportMonksClient.responses = {}
    monkeypatch.setattr(queue, "SportMonksClient", FakeSportMonksClient)

    fetched, errors, calls = queue.fetch_provider_fixtures([1, 2, 3, 4, 5], 2, 2)

    assert sorted(fetched) == [1, 2, 3, 4, 5]
    assert errors == {}
    assert calls == 3
    assert sorted(FakeSportMonksClient.calls) == [
        "fixtures/multi/1,2",
        "fixtures/multi/3,4",
        "fixtures/multi/5",
    ]
