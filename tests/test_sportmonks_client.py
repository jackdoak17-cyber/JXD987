from __future__ import annotations

import unittest

from jxd.sportmonks_client import SportMonksClient


class StubSportMonksClient(SportMonksClient):
    def __init__(self, responses: list[dict]) -> None:
        super().__init__(api_token="test-token")
        self.responses = responses
        self.calls: list[tuple[str, dict[str, object]]] = []

    def request(self, method: str, endpoint: str, params: dict[str, object] | None = None) -> dict:
        self.calls.append((endpoint, dict(params or {})))
        if not self.responses:
            raise AssertionError("unexpected extra request")
        return self.responses.pop(0)


class SportMonksPaginationTests(unittest.TestCase):
    def test_follows_cursor_url_after_page_limit(self) -> None:
        client = StubSportMonksClient(
            [
                {
                    "data": [{"id": 1}],
                    "pagination": {
                        "current_page": 800,
                        "has_more": True,
                        "next_cursor": "https://api.sportmonks.com/v3/football/fixtures?per_page=50&cursor=abc%2B123&api_token=redacted",
                    },
                },
                {"data": [{"id": 2}], "pagination": {"has_more": False}},
            ]
        )

        rows = list(client.fetch_collection("fixtures", params={"filters": "fixtureLeagues:24"}, per_page=50))

        self.assertEqual([row["id"] for row in rows], [1, 2])
        self.assertEqual(client.calls[0][1]["page"], 1)
        self.assertEqual(client.calls[1][0], "fixtures")
        self.assertEqual(client.calls[1][1]["cursor"], "abc+123")
        self.assertNotIn("api_token", client.calls[1][1])
        self.assertNotIn("page", client.calls[1][1])

    def test_follows_numeric_next_page_without_dropping_filters(self) -> None:
        client = StubSportMonksClient(
            [
                {
                    "data": [{"id": 1}],
                    "pagination": {"current_page": 1, "has_more": True, "next_page": 2},
                },
                {"data": [{"id": 2}], "pagination": {"current_page": 2, "has_more": False},
                },
            ]
        )

        rows = list(client.fetch_collection("seasons", params={"filters": "fixtureLeagues:24"}, per_page=200))

        self.assertEqual([row["id"] for row in rows], [1, 2])
        self.assertEqual(client.calls[1][1]["page"], 2)
        self.assertEqual(client.calls[1][1]["filters"], "fixtureLeagues:24")

    def test_stops_when_endpoint_returns_complete_oversized_collection_without_pagination(self) -> None:
        client = StubSportMonksClient(
            [
                {
                    "data": [{"id": 1}, {"id": 2}, {"id": 3}],
                },
            ]
        )

        rows = list(client.fetch_collection("teams/seasons/123", per_page=2))

        self.assertEqual([row["id"] for row in rows], [1, 2, 3])
        self.assertEqual(len(client.calls), 1)

    def test_stops_repeated_unpaginated_page_without_duplicate_rows(self) -> None:
        client = StubSportMonksClient(
            [
                {"data": [{"id": 1}, {"id": 2}]},
                {"data": [{"id": 1}, {"id": 2}]},
            ]
        )

        rows = list(client.fetch_collection("teams", per_page=2))

        self.assertEqual([row["id"] for row in rows], [1, 2])
        self.assertEqual(len(client.calls), 2)


if __name__ == "__main__":
    unittest.main()
