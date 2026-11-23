"""Connection checks for the explicitly production-restricted leaderboard runtime."""

from dataclasses import dataclass
import hashlib
import hmac
import ipaddress
import os
import re

import psycopg2
from psycopg2.extensions import TRANSACTION_STATUS_IDLE, parse_dsn


_ROLES = frozenset({"m3_role_worker", "m3_role_supervisor", "m3_role_operator"})
_DSN_KEYS = frozenset({
    "host", "port", "dbname", "user", "password", "sslmode", "sslrootcert",
    "connect_timeout", "gssencmode",
})
_LIBPQ_OVERRIDES = frozenset({
    "PGAPPNAME", "PGCHANNELBINDING", "PGCLIENTENCODING", "PGCONNECT_TIMEOUT", "PGDATABASE",
    "PGGSSDELEGATION", "PGGSSENCMODE", "PGGSSLIB", "PGHOST", "PGHOSTADDR", "PGKRBSRVNAME",
    "PGLOADBALANCEHOSTS", "PGOPTIONS", "PGPASSFILE", "PGPASSWORD", "PGPORT", "PGREQUIREAUTH",
    "PGREQUIREPEER", "PGREQUIRESSL", "PGSERVICE", "PGSERVICEFILE", "PGSSLCERT", "PGSSLCERTMODE",
    "PGSSLCRL", "PGSSLCRLDIR", "PGSSLKEY", "PGSSLMAXPROTOCOLVERSION", "PGSSLMINPROTOCOLVERSION",
    "PGSSLMODE", "PGSSLNEGOTIATION", "PGSSLROOTCERT", "PGSSLSNI", "PGSYSCONFDIR",
    "PGTARGETSESSIONATTRS", "PGTCPUSER_TIMEOUT", "PGUSER",
})
_TLS_TRUST_OVERRIDES = frozenset({"OPENSSL_CONF", "SSL_CERT_DIR", "SSL_CERT_FILE"})


@dataclass(frozen=True)
class RestrictedRuntime:
    """Reviewed connection identity, without the DSN or its credentials."""

    host: str
    port: str
    role: str
    sslrootcert: str
    contract_sha256: str


def _sha256_pin(value: str) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def _single_network_host(host: str) -> bool:
    if not isinstance(host, str) or not host or "%" in host:
        return False
    if ":" in host:
        try:
            return ipaddress.ip_address(host).version == 6
        except ValueError:
            return False
    return all(
        re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?", label)
        for label in host.removesuffix(".").split(".")
    )


def _check_environment() -> None:
    if _LIBPQ_OVERRIDES.intersection(os.environ):
        raise RuntimeError("restricted database connection rejects ambient libpq settings")
    if _TLS_TRUST_OVERRIDES.intersection(os.environ):
        raise RuntimeError("restricted database connection rejects ambient TLS trust settings")


def validate_restricted_dsn(
    dsn: str, *, expected_dsn_sha256: str, expected_host: str, expected_port: str | int,
    expected_role: str, expected_contract_sha256: str,
) -> RestrictedRuntime:
    """Check the exact reviewed DSN before the caller opens any connection."""
    _check_environment()
    port = str(expected_port)
    if (not _sha256_pin(expected_dsn_sha256) or not _sha256_pin(expected_contract_sha256)
            or not _single_network_host(expected_host) or not re.fullmatch(r"[1-9][0-9]{0,4}", port)
            or int(port) > 65535 or not isinstance(expected_role, str) or expected_role not in _ROLES):
        raise RuntimeError("restricted database identity pins are invalid")
    if not isinstance(dsn, str) or not dsn or "\x00" in dsn:
        raise RuntimeError("restricted database DSN is invalid")
    try:
        digest = hashlib.sha256(dsn.encode("utf-8")).hexdigest()
    except UnicodeError:
        raise RuntimeError("restricted database DSN is invalid") from None
    if not hmac.compare_digest(digest, expected_dsn_sha256):
        raise RuntimeError("restricted database DSN does not match its pin")
    try:
        parameters = parse_dsn(dsn)
    except (psycopg2.Error, ValueError):
        raise RuntimeError("restricted database DSN is invalid") from None

    rootcert = parameters.get("sslrootcert", "")
    if (parameters.keys() - _DSN_KEYS
            or parameters.get("host") != expected_host or parameters.get("port") != port
            or parameters.get("dbname") != "postgres" or parameters.get("user") != expected_role
            or parameters.get("sslmode") != "verify-full"
            or not (rootcert == "system" or os.path.isabs(rootcert))
            or parameters.get("gssencmode", "disable") != "disable"):
        raise RuntimeError("restricted database DSN requires the reviewed direct endpoint, role and TLS settings")
    return RestrictedRuntime(expected_host, port, expected_role, rootcert, expected_contract_sha256)


def validate_restricted_connection(connection, target: RestrictedRuntime) -> None:
    """Validate the caller's actual connection in its own read-only transaction."""
    _check_environment()
    if connection.get_transaction_status() != TRANSACTION_STATUS_IDLE:
        raise RuntimeError("restricted database validation requires an idle connection")
    parameters = connection.get_dsn_parameters()
    if (parameters.get("host") != target.host or parameters.get("port") != target.port
            or parameters.get("dbname") != "postgres" or parameters.get("user") != target.role
            or parameters.get("sslmode") != "verify-full" or parameters.get("sslrootcert") != target.sslrootcert
            or parameters.get("service") or parameters.get("hostaddr")
            or not connection.info.ssl_in_use):
        raise RuntimeError("restricted database connection does not match the reviewed endpoint or TLS settings")
    try:
        with connection, connection.cursor() as cursor:
            cursor.execute("set transaction read only")
            cursor.execute("""
                select pg_catalog.current_database(), pg_catalog.inet_server_addr() is not null,
                       session_user, current_user,
                       exists(select 1 from pg_catalog.pg_stat_ssl
                              where pid=pg_catalog.pg_backend_pid() and ssl)
            """)
            if cursor.fetchone() != ("postgres", True, target.role, target.role, True):
                raise RuntimeError("restricted database session identity or TLS is invalid")
            cursor.execute("select leaderboard_role_boundary_v1.restricted_runtime_contract()")
            result = cursor.fetchone()
            digest = result[0] if result else None
            if not _sha256_pin(digest) or not hmac.compare_digest(digest, target.contract_sha256):
                raise RuntimeError("restricted database installed contract does not match its pin")
    except psycopg2.Error:
        raise RuntimeError("restricted database read-only contract check failed") from None
