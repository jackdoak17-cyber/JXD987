from scripts.sync_fixture_referees import (
    extract_referee_items,
    normalize_assignment,
    payload_hash,
)


def test_fixture_referee_payload_maps_primary_and_supporting_officials():
    payload = {
        "data": {
            "id": 19722197,
            "referees": [
                {"id": 6024882, "fixture_id": 19722197, "referee_id": 14814, "type_id": 6},
                {"id": 6024883, "fixture_id": 19722197, "referee_id": 12090, "type_id": 7},
                {"id": 6024884, "fixture_id": 19722197, "referee_id": 24135, "type_id": 8},
                {"id": 6024885, "fixture_id": 19722197, "referee_id": 13537, "type_id": 9},
            ],
        }
    }

    rows = [normalize_assignment(19722197, item) for item in extract_referee_items(payload)]

    assert len(rows) == 4
    assert rows[0]["referee_id"] == 14814
    assert rows[0]["role"] == "main"
    assert rows[0]["is_primary"] is True
    assert [row["role"] for row in rows[1:]] == ["assistant_1", "assistant_2", "fourth_official"]
    assert all(row["source"] == "sportmonks" for row in rows)


def test_relation_payload_without_referees_is_empty_and_retryable():
    assert extract_referee_items({"data": {"id": 19722197, "referees": []}}) == []
    assert extract_referee_items({"data": {"id": 19722197}}) == []


def test_payload_hash_is_stable_for_key_order():
    assert payload_hash({"b": 2, "a": 1}) == payload_hash({"a": 1, "b": 2})
