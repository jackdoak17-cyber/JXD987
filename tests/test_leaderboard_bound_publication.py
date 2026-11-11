"""Worker wiring for the explicitly requested disposable SQL 92 contract."""

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

import scripts.leaderboard_finite_trial_supervisor as supervisor
import scripts.leaderboard_shadow_worker as worker


GENERATION, FENCE, RUN_ID = 73, 41, 52
LEGACY_SQL = "select leaderboard_shadow_v1.publish_generation(%s)"
BOUND_SQL = "select leaderboard_rolling_v1.publish_generation_for_run(%s,%s,%s)"


class Cursor:
    def __init__(self, connection):
        self.connection = connection
        self.query = ""

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, query, params=None):
        self.query = " ".join(query.split())
        self.connection.queries.append((self.query, params))
        if self.query in {LEGACY_SQL, BOUND_SQL} and self.connection.publication_error:
            raise self.connection.publication_error

    def fetchone(self):
        if "cohort_pending_publications generation" in self.query:
            return ("processing", 0, 0)
        if "to_regprocedure" in self.query:
            return (self.connection.schema_present,)
        if "begin_bound_worker" in self.query:
            return {"decision": {"admitted": True, "fence": FENCE, "run_id": RUN_ID}}
        if "check_bound_worker" in self.query:
            return {"evidence": {"ok": True}}
        if "finish_bound_worker" in self.query:
            return {"outcome": {"run_succeeded": True}}
        if self.query == BOUND_SQL:
            return (self.connection.bound_result,)
        if self.query == LEGACY_SQL:
            return (True,)
        raise AssertionError(f"unexpected fetch: {self.query}")


class Connection:
    def __init__(self, *, transitioned=True, schema_present=True, publication_error=None):
        self.queries = []
        self.transactions = []
        self.schema_present = schema_present
        self.publication_error = publication_error
        self.bound_result = {
            "published": True, "transitioned": transitioned, "generation_id": GENERATION,
        }

    def __enter__(self):
        return self

    def __exit__(self, exc_type, *_args):
        self.transactions.append("rollback" if exc_type else "commit")
        return False

    def cursor(self, **_kwargs):
        return Cursor(self)


def prepare_generation(monkeypatch):
    monkeypatch.setattr(worker, "pending_cohorts", lambda *_a: [(271, 27897)])
    monkeypatch.setattr(worker, "ensure_generation", lambda *_a, **_kw: GENERATION)
    monkeypatch.setattr(worker, "process_generation_job", lambda *_a: True)


@pytest.mark.parametrize("binding,transitioned,published", [
    (None, True, 1), ((FENCE, RUN_ID), True, 1),
    ((FENCE, RUN_ID), False, 0), ((FENCE, RUN_ID), 1, 0),
])
def test_generation_publication_uses_exact_binding_and_counts_only_transitions(
    monkeypatch, binding, transitioned, published,
):
    prepare_generation(monkeypatch)
    conn = Connection(transitioned=transitioned)
    result = worker.process_generations(
        conn, "worker", 1, 1, worker.RunBudget.create(10, 30), False,
        guard_check=lambda *_a: {"ok": True}, publication_binding=binding,
    )
    assert result == {"cohorts": 1, "jobs": 1, "published": published}
    publications = [(query, params) for query, params in conn.queries
                    if query in {LEGACY_SQL, BOUND_SQL}]
    assert publications == [
        (LEGACY_SQL, (GENERATION,)) if binding is None
        else (BOUND_SQL, (GENERATION, FENCE, RUN_ID))
    ]
    assert conn.transactions == ["commit"]


@pytest.mark.parametrize("binding", [None, (FENCE, RUN_ID)])
def test_publication_error_rolls_back_and_never_falls_back(monkeypatch, binding):
    prepare_generation(monkeypatch)
    failure = RuntimeError("publication/link insert denied")
    conn = Connection(publication_error=failure)
    progress = {"cohorts": 0, "jobs": 0, "published": 0}
    with pytest.raises(RuntimeError, match="publication/link insert denied") as error:
        worker.process_generations(
            conn, "worker", 1, 1, worker.RunBudget.create(10, 30), False,
            guard_check=lambda *_a: {"ok": True}, progress=progress,
            publication_binding=binding,
        )
    assert error.value is failure
    assert conn.transactions == ["rollback"]
    assert progress == {"cohorts": 1, "jobs": 1, "published": 0}
    assert [(query, params) for query, params in conn.queries
            if query in {LEGACY_SQL, BOUND_SQL}] == [
        (LEGACY_SQL, (GENERATION,)) if binding is None
        else (BOUND_SQL, (GENERATION, FENCE, RUN_ID))
    ]


def rolling_args(require_bound_publication):
    return SimpleNamespace(
        trial_policy_id="local-publication-policy", fixture_limit=10,
        capture_limit=0, cohort_limit=1, job_limit=1, max_seconds=30,
        league_id=None, season_id=None, production_trial_isolated_test=False,
        require_bound_publication=require_bound_publication,
    )


def prepare_rolling(monkeypatch):
    prepare_generation(monkeypatch)
    monkeypatch.setattr(worker, "validate_production_trial_connection", lambda *_a: None)
    monkeypatch.setattr(worker, "apply_preflight_timeouts", lambda *_a: None)
    monkeypatch.setattr(worker, "load_external_resource_evidence", lambda *_a: None)
    monkeypatch.setattr(worker, "configure_temp_file_limit_if_permitted", lambda *_a: False)
    monkeypatch.setattr(worker, "refresh_eligibility_if_due", lambda *_a: 0)
    monkeypatch.setattr(worker, "apply_recovery_timeouts", lambda *_a: None)


@pytest.mark.parametrize("required,transitioned,published", [
    (False, True, 1), (True, True, 1), (True, False, 0),
])
def test_rolling_opt_in_checks_schema_then_uses_admitted_binding(
    monkeypatch, required, transitioned, published,
):
    prepare_rolling(monkeypatch)
    # Legacy mode works with the original paired SQL, without SQL 92 installed.
    conn = Connection(transitioned=transitioned, schema_present=required)
    result = worker.run_rolling_worker(conn, rolling_args(required), "owner")
    assert result["generations"] == {"cohorts": 1, "jobs": 1, "published": published}
    assert (result["fence"], result["run_id"]) == (FENCE, RUN_ID)
    publications = [(query, params) for query, params in conn.queries
                    if query in {LEGACY_SQL, BOUND_SQL}]
    assert publications == [
        (BOUND_SQL, (GENERATION, FENCE, RUN_ID)) if required
        else (LEGACY_SQL, (GENERATION,))
    ]
    queries = [query for query, _params in conn.queries]
    schema_checks = [i for i, query in enumerate(queries) if "to_regprocedure" in query]
    if required:
        assert len(schema_checks) == 1
        assert "publication_run_links" in queries[schema_checks[0]]
        assert schema_checks[0] < next(i for i, q in enumerate(queries) if "begin_bound_worker" in q)
    else:
        assert not schema_checks


def test_rolling_opt_in_missing_schema_stops_before_admission(monkeypatch):
    prepare_rolling(monkeypatch)
    conn = Connection(schema_present=False)
    monkeypatch.setattr(worker, "load_external_resource_evidence",
                        lambda *_a: pytest.fail("must refuse before evidence writes"))
    with pytest.raises(RuntimeError, match="bound publication requires SQL 92 wrapper schema"):
        worker.run_rolling_worker(conn, rolling_args(True), "owner")
    assert len(conn.queries) == 1
    assert "to_regprocedure" in conn.queries[0][0]
    assert conn.transactions == ["rollback"]


@pytest.mark.parametrize("command", ["run-once", "trial-once", "finite-once", "rolling-recover"])
def test_worker_bound_publication_flag_rejects_other_commands(monkeypatch, capsys, command):
    monkeypatch.setattr(sys, "argv", ["worker", command, "--require-bound-publication"])
    monkeypatch.setattr(worker, "connect", lambda: pytest.fail("must refuse before connecting"))
    with pytest.raises(SystemExit) as error:
        worker.main()
    assert error.value.code == 2
    assert "bound publication requires rolling-once" in capsys.readouterr().err


def supervisor_arguments(tmp_path, worker_path):
    return [
        "supervisor", "--worker-path", str(worker_path),
        "--policy-id", "local-publication-policy",
        "--worker-lock-file", str(tmp_path / "worker.lock"),
        "--database-heavy-lock-file", str(tmp_path / "heavy.lock"),
    ]


@pytest.mark.parametrize("production", [False, True])
def test_supervisor_bound_publication_flag_rejects_nonrolling(
    tmp_path, monkeypatch, capsys, production,
):
    args = supervisor_arguments(tmp_path, Path(__file__)) + ["--require-bound-publication"]
    if production:
        args.append("--production")
    monkeypatch.setattr(sys, "argv", args)
    monkeypatch.setattr(supervisor, "database_url", lambda: pytest.fail("must refuse before connecting"))
    with pytest.raises(SystemExit) as error:
        supervisor.main()
    assert error.value.code == 2
    assert "bound publication requires rolling mode" in capsys.readouterr().err


@pytest.mark.parametrize("required", [False, True])
def test_supervisor_forwards_publication_flag_only_to_main_rolling_worker(
    tmp_path, monkeypatch, required,
):
    probe = tmp_path / "probe_worker.py"
    probe.write_text(
        "import json,sys\n"
        "from pathlib import Path\n"
        "with open(Path(__file__).with_suffix('.jsonl'), 'a') as output:\n"
        "    output.write(json.dumps(sys.argv[1:]) + '\\n')\n"
        "print(json.dumps({'state':'already_settled'}), flush=True)\n"
    )
    monkeypatch.setattr(supervisor, "database_url", lambda: "disposable")
    monkeypatch.setattr(supervisor, "assert_disposable_database", lambda *_a: None)
    monkeypatch.setattr(supervisor, "drain_token_backends", lambda *_a: None)
    monkeypatch.setattr(supervisor, "drain_exact_backend", lambda *_a, **_kw: None)
    args = supervisor_arguments(tmp_path, probe) + [
        "--production", "--rolling", "--production-trial-isolated-test",
    ]
    if required:
        args.append("--require-bound-publication")
    monkeypatch.setattr(sys, "argv", args)
    assert supervisor.main() == 0
    calls = [json.loads(line) for line in probe.with_suffix(".jsonl").read_text().splitlines()]
    assert [call[0] for call in calls] == ["rolling-once", "rolling-recover"]
    assert calls[0].count("--require-bound-publication") == int(required)
    assert "--require-bound-publication" not in calls[1]
