from scripts.sync_fixture_referees import (
    ProviderRateLimited,
    extract_referee_items,
    fetch_fixture_ids,
    fetch_json_with_retry,
    normalize_assignment,
    payload_hash,
)


class _FakeResponse:
    def __init__(self, status_code, headers=None, payload=None):
        self.status_code = status_code
        self.headers = headers or {}
        self._payload = payload or {}

    def raise_for_status(self):
        raise AssertionError(f"unexpected response status={self.status_code}")

    def json(self):
        return self._payload


class _FakeSession:
    def __init__(self, response):
        self.response = response

    def get(self, url, timeout):
        return self.response


class _FakeCursor:
    def __init__(self):
        self.sql = ""
        self.params = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, sql, params):
        self.sql = sql
        self.params = params

    def fetchall(self):
        return [(101,), (202,)]


class _FakeConnection:
    def __init__(self):
        self.cursor_value = _FakeCursor()

    def cursor(self):
        return self.cursor_value


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


def test_rate_limit_stops_without_retrying_for_provider_cooldown():
    session = _FakeSession(_FakeResponse(429, headers={"Retry-After": "740"}))

    try:
        fetch_json_with_retry(session=session, url="https://example.test/fixtures/101?api_token=secret", timeout=1)
    except ProviderRateLimited as exc:
        assert exc.retry_after_seconds == 740
    else:
        raise AssertionError("expected ProviderRateLimited")


def test_force_window_bypasses_state_filter_but_normal_run_keeps_it():
    normal_conn = _FakeConnection()
    fetch_fixture_ids(
        normal_conn,
        days_back=30,
        days_forward=31,
        resync_hours=12,
        fixture_id=0,
        limit_fixtures=10,
        force_window=False,
    )
    assert "state.status in ('pending', 'no_assignment', 'error')" in normal_conn.cursor_value.sql
    assert normal_conn.cursor_value.params[-2] == 12
    assert normal_conn.cursor_value.params[-1] == 10

    force_conn = _FakeConnection()
    fetch_fixture_ids(
        force_conn,
        days_back=30,
        days_forward=31,
        resync_hours=12,
        fixture_id=0,
        limit_fixtures=10,
        force_window=True,
    )
    assert "state.status in ('pending', 'no_assignment', 'error')" not in force_conn.cursor_value.sql
    assert force_conn.cursor_value.params[-1] == 10
