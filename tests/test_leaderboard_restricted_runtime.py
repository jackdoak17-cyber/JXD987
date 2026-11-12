"""Production-restricted connection checks without a hosted database."""

from hashlib import sha256
from types import SimpleNamespace

import psycopg2
from psycopg2.extensions import TRANSACTION_STATUS_IDLE, TRANSACTION_STATUS_INTRANS, make_dsn
import pytest

from scripts import leaderboard_restricted_runtime as runtime


ROLE = "m3_role_worker"
CONTRACT = "1" * 64
PASSWORD = "unlogged-test-password"
PARAMETERS = {
    "host": "localhost", "port": "35432", "dbname": "postgres", "user": ROLE,
    "password": PASSWORD, "sslmode": "verify-full", "sslrootcert": "/reviewed/local-test-ca.pem",
}
IDENTITY = ("postgres", True, ROLE, ROLE, True)


@pytest.fixture(autouse=True)
def clean_connection_environment(monkeypatch):
    for name in runtime._LIBPQ_OVERRIDES | runtime._TLS_TRUST_OVERRIDES:
        monkeypatch.delenv(name, raising=False)


def check_dsn(dsn=None, **overrides):
    dsn = make_dsn(**PARAMETERS) if dsn is None else dsn
    pins = {
        "expected_dsn_sha256": sha256(dsn.encode()).hexdigest(),
        "expected_host": PARAMETERS["host"],
        "expected_port": PARAMETERS["port"],
        "expected_role": ROLE,
        "expected_contract_sha256": CONTRACT,
    }
    return runtime.validate_restricted_dsn(dsn, **(pins | overrides))


@pytest.mark.parametrize("role", ["m3_role_worker", "m3_role_supervisor", "m3_role_operator"])
def test_reviewed_tls_local_fixture_accepts_each_exact_role(role):
    dsn = make_dsn(**(PARAMETERS | {"user": role}))
    target = check_dsn(dsn, expected_role=role, expected_port=35432)
    assert target == runtime.RestrictedRuntime("localhost", "35432", role, PARAMETERS["sslrootcert"], CONTRACT)
    assert PASSWORD not in repr(target)


def test_uri_and_explicit_ip_fixture_require_the_same_contract():
    dsn = (
        "postgresql://m3_role_worker:unlogged-test-password@[::1]:35432/postgres"
        "?sslmode=verify-full&sslrootcert=%2Freviewed%2Flocal-test-ca.pem"
    )
    assert check_dsn(dsn, expected_host="::1").host == "::1"


@pytest.mark.parametrize(("key", "value"), [
    ("expected_dsn_sha256", "0" * 64),
    ("expected_dsn_sha256", "BAD"),
    ("expected_contract_sha256", "A" * 64),
    ("expected_host", "/tmp/oddssearch"),
    ("expected_host", "localhost,localhost"),
    ("expected_port", "0"),
    ("expected_port", "65536"),
    ("expected_port", "5432"),
    ("expected_role", "postgres"),
])
def test_invalid_or_mismatched_pins_fail(key, value):
    with pytest.raises(RuntimeError, match="restricted database"):
        check_dsn(**{key: value})


@pytest.mark.parametrize("change", [
    {"host": "elsewhere.example"}, {"host": "localhost,elsewhere.example"},
    {"host": "/tmp/test"}, {"host": ""}, {"port": ""}, {"port": "35432,5432"},
    {"dbname": "other"}, {"dbname": ""}, {"user": "postgres"}, {"user": "m3_role_supervisor"},
    {"sslmode": "require"}, {"sslmode": "verify-ca"}, {"sslmode": ""},
    {"sslrootcert": ""}, {"sslrootcert": "unreviewed.pem"},
    {"service": "alternate"}, {"hostaddr": "127.0.0.1"},
    {"options": "-c role=postgres"}, {"gssencmode": "prefer"},
])
def test_pinning_a_dsn_does_not_allow_wrong_identity_tls_or_overrides(change):
    with pytest.raises(RuntimeError, match="reviewed direct endpoint, role and TLS"):
        check_dsn(make_dsn(**(PARAMETERS | change)))


def test_parse_failure_does_not_show_credentials():
    with pytest.raises(RuntimeError, match="DSN is invalid") as error:
        check_dsn(f"invalid_{PASSWORD}=not-a-libpq-key")
    assert PASSWORD not in str(error.value)
    assert error.value.__suppress_context__


@pytest.mark.parametrize("name", ["PGHOST", "PGHOSTADDR", "PGOPTIONS", "PGSERVICE", "PGSERVICEFILE",
                                 "PGSSLMODE", "PGSSLROOTCERT", "PGPASSWORD", "PGDATABASE", "PGUSER"])
def test_ambient_libpq_settings_are_rejected_even_if_empty(monkeypatch, name):
    monkeypatch.setenv(name, "")
    with pytest.raises(RuntimeError, match="ambient libpq settings"):
        check_dsn()


@pytest.mark.parametrize("name", ["OPENSSL_CONF", "SSL_CERT_DIR", "SSL_CERT_FILE"])
def test_ambient_tls_trust_settings_are_rejected(monkeypatch, name):
    monkeypatch.setenv(name, "/unreviewed/trust")
    with pytest.raises(RuntimeError, match="ambient TLS trust"):
        check_dsn()


class Connection:
    def __init__(self, *, params=None, tls=True, identity=IDENTITY, contract=(CONTRACT,),
                 status=TRANSACTION_STATUS_IDLE):
        self.params = (PARAMETERS | (params or {})).copy()
        del self.params["password"]
        self.info = SimpleNamespace(ssl_in_use=tls)
        self.status = status
        self.rows = iter((identity, contract))
        self.statements = []
        self.transaction_exit = []

    def get_dsn_parameters(self):
        return self.params

    def get_transaction_status(self):
        return self.status

    def __enter__(self):
        return self

    def __exit__(self, error_type, *_):
        self.transaction_exit.append(error_type)

    def cursor(self):
        return Cursor(self)


class Cursor:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass

    def execute(self, sql):
        self.connection.statements.append(" ".join(sql.split()))

    def fetchone(self):
        row = next(self.connection.rows)
        if isinstance(row, Exception):
            raise row
        return row


def test_actual_connection_checks_session_and_contract_in_read_only_transaction():
    target = check_dsn()
    connection = Connection(params={"options": "-c work_mem=3500kB"})
    runtime.validate_restricted_connection(connection, target)
    assert connection.transaction_exit == [None]
    assert connection.statements == [
        "set transaction read only",
        "select pg_catalog.current_database(), pg_catalog.inet_server_addr() is not null, "
        "session_user, current_user, "
        "exists(select 1 from pg_catalog.pg_stat_ssl where pid=pg_catalog.pg_backend_pid() and ssl)",
        "select leaderboard_role_boundary_v1.restricted_runtime_contract()",
    ]


@pytest.mark.parametrize("options", [
    {"params": {"host": "elsewhere.example"}}, {"params": {"port": "5432"}},
    {"params": {"dbname": "other"}}, {"params": {"user": "m3_role_supervisor"}},
    {"params": {"sslmode": "require"}}, {"params": {"sslrootcert": "/other.pem"}},
    {"params": {"hostaddr": "127.0.0.1"}}, {"params": {"service": "redirect"}},
    {"tls": False}, {"status": TRANSACTION_STATUS_INTRANS},
])
def test_wrong_effective_connection_or_open_transaction_is_refused_before_sql(options):
    connection = Connection(**options)
    with pytest.raises(RuntimeError, match="reviewed endpoint or TLS|idle connection"):
        runtime.validate_restricted_connection(connection, check_dsn())
    assert not connection.statements
    assert not connection.transaction_exit


@pytest.mark.parametrize("identity", [
    ("other", True, ROLE, ROLE, True), ("postgres", False, ROLE, ROLE, True),
    ("postgres", True, "postgres", ROLE, True), ("postgres", True, ROLE, "postgres", True),
    ("postgres", True, ROLE, ROLE, False), None,
])
def test_server_identity_or_tls_mismatch_never_queries_the_contract(identity):
    connection = Connection(identity=identity)
    with pytest.raises(RuntimeError, match="session identity or TLS"):
        runtime.validate_restricted_connection(connection, check_dsn())
    assert len(connection.statements) == 2
    assert connection.transaction_exit == [RuntimeError]


@pytest.mark.parametrize("contract", [("0" * 64,), ("A" * 64,), (None,), (True,), None])
def test_contract_must_be_exact_lowercase_sha256(contract):
    connection = Connection(contract=contract)
    with pytest.raises(RuntimeError, match="installed contract does not match"):
        runtime.validate_restricted_connection(connection, check_dsn())
    assert len(connection.statements) == 3
    assert connection.transaction_exit == [RuntimeError]


def test_missing_contract_entrypoint_fails_with_redacted_error():
    connection = Connection(contract=psycopg2.errors.UndefinedFunction(PASSWORD))
    with pytest.raises(RuntimeError, match="read-only contract check failed") as error:
        runtime.validate_restricted_connection(connection, check_dsn())
    assert PASSWORD not in str(error.value)
    assert error.value.__suppress_context__
    assert connection.transaction_exit == [psycopg2.errors.UndefinedFunction]


@pytest.mark.parametrize(("scope", "limits", "allowed"), [
    ((8, 1), (21, 10, 1, 10, 30), True),
    ((989, 27223), (29, 29, 0, 0, 30), False),
    ((989, 27223), (30, 0, 1, 29, 30), False),
    ((591, 27223), (59, 29, 1, 29, 30), False),
    ((989, 27224), (59, 29, 1, 29, 30), False),
    ((989, 27223), (59, 29, 1, 29, 30), True),
    ((989, 27223), (30, 1, 1, 29, 30), False),
    ((989, 27223), (30, 0, 1, 30, 30), False),
    ((989, 27223), (59, 29, 1, 29, 31), False),
])
def test_hosted_canary_envelope(scope, limits, allowed):
    from scripts.leaderboard_hosted_boundary import reviewed_canary_envelope

    args = SimpleNamespace(
        league_id=scope[0], season_id=scope[1], fixture_limit=limits[0],
        capture_limit=limits[1], cohort_limit=limits[2], job_limit=limits[3], max_seconds=limits[4],
    )
    assert reviewed_canary_envelope(args) is allowed
