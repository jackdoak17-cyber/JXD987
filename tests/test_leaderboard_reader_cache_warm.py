"""Bounded post-publication cache warming for current Last-N readers."""

import ast
import inspect

import pytest

import scripts.leaderboard_shadow_worker as worker


class Cursor:
    def __init__(self, row):
        self.row = row
        self.queries = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, query, params=None):
        self.queries.append((" ".join(query.split()), params))

    def fetchone(self):
        return self.row


class Connection:
    def __init__(self, row):
        self.cursor_instance = Cursor(row)
        self.transactions = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, *_args):
        self.transactions.append("rollback" if exc_type else "commit")
        return False

    def cursor(self):
        return self.cursor_instance


def test_cache_warm_is_bounded_and_uses_the_current_all_league_reader(monkeypatch):
    ticks = iter((10.0, 11.25))
    monkeypatch.setattr(worker.time, "monotonic", lambda: next(ticks))
    conn = Connection((23, 1, "generation-key"))

    result = worker.warm_current_player_last_n_reader(conn)

    assert result == {
        "league_count": 23,
        "row_count": 1,
        "generation_key": "generation-key",
        "elapsed_ms": 1250.0,
    }
    assert conn.transactions == ["commit"]
    queries = conn.cursor_instance.queries
    assert queries[0] == (
        "select set_config('lock_timeout', %s, true)",
        (f"{worker.LOCK_TIMEOUT_MS}ms",),
    )
    assert queries[1] == (
        "select set_config('statement_timeout', %s, true)",
        (f"{worker.READER_CACHE_WARM_TIMEOUT_MS}ms",),
    )
    assert "player_stat_leaderboard_shadow_v4" in queries[2][0]
    assert "581, 5, 'per90'" in queries[2][0]
    assert "array_fill(null::integer" in queries[2][0]


@pytest.mark.parametrize("row", [None, (0, 0, None), (23, 0, "generation-key")])
def test_cache_warm_fails_closed_on_incomplete_evidence(row):
    conn = Connection(row)
    with pytest.raises(RuntimeError, match="incomplete evidence"):
        worker.warm_current_player_last_n_reader(conn)


def test_normal_run_once_warms_after_publication_before_final_resource_check():
    """The dedicated lane invokes main's normal run-once branch, not rolling-once."""

    tree = ast.parse(inspect.getsource(worker.main))
    calls = sorted(
        [
            (node.lineno, node.func.id)
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        ]
    )
    warm_index = next(
        line for line, name in calls if name == "warm_current_player_last_n_reader"
    )
    finish_index = next(
        line for line, name in calls if name == "finish_worker_run_with_resource_check"
    )

    assert warm_index < finish_index
    source = inspect.getsource(worker.main)
    assert 'if generations["published"]:' in source
    assert 'guard_check(conn, require_external)' in source[
        source.index('if generations["published"]:'):source.index(
            "finish_worker_run_with_resource_check"
        )
    ]
    assert '"reader_cache_warm": reader_cache_warm' in source
