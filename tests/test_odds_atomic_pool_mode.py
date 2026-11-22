"""The optional atomic pool override changes only its connection port."""

from __future__ import annotations

import os
import unittest
from pathlib import Path
from unittest.mock import patch

import psycopg2

from scripts.odds_atomic_publication import AtomicOddsPublication


POOLER_DSN = (
    "postgresql://postgres.example:synthetic-password@"
    "aws-1-eu-west-1.pooler.supabase.com:5432/postgres"
    "?sslmode=require&application_name=odds-atomic-test"
)


def construct(dsn: str):
    return AtomicOddsPublication("delivery", dsn, Path("unused.sqlite"), [], Path("unused-spool"))


class AtomicPoolModeTests(unittest.TestCase):
    def test_absent_mode_preserves_existing_connection_call(self):
        with patch.dict(os.environ, {}, clear=True), patch(
            "scripts.odds_atomic_publication.psycopg2.connect"
        ) as connect:
            publication = construct(POOLER_DSN)
        self.assertEqual(publication.pool_mode, "session")
        connect.assert_called_once_with(POOLER_DSN, connect_timeout=10)

    def test_explicit_transaction_preserves_dsn_and_overrides_only_port(self):
        with patch.dict(os.environ, {"ODDS_ATOMIC_POOL_MODE": "transaction"}), patch(
            "scripts.odds_atomic_publication.psycopg2.connect"
        ) as connect:
            publication = construct(POOLER_DSN)
        self.assertEqual(publication.pool_mode, "transaction")
        connect.assert_called_once_with(POOLER_DSN, connect_timeout=10, port=6543)
        original = psycopg2.extensions.parse_dsn(POOLER_DSN)
        resolved = psycopg2.extensions.parse_dsn(
            psycopg2.extensions.make_dsn(POOLER_DSN, port=6543)
        )
        self.assertEqual(resolved.pop("port"), "6543")
        self.assertEqual(original.pop("port"), "5432")
        self.assertEqual(resolved, original)

    def test_already_transaction_port_is_allowed_without_dsn_rewrite(self):
        dsn = POOLER_DSN.replace(":5432/", ":6543/")
        with patch.dict(os.environ, {"ODDS_ATOMIC_POOL_MODE": "transaction"}), patch(
            "scripts.odds_atomic_publication.psycopg2.connect"
        ) as connect:
            construct(dsn)
        connect.assert_called_once_with(dsn, connect_timeout=10, port=6543)

    def test_unknown_mode_fails_before_connect(self):
        with patch.dict(os.environ, {"ODDS_ATOMIC_POOL_MODE": "other"}), patch(
            "scripts.odds_atomic_publication.psycopg2.connect"
        ) as connect:
            with self.assertRaisesRegex(ValueError, "ODDS_ATOMIC_POOL_MODE"):
                construct(POOLER_DSN)
        connect.assert_not_called()

    def test_transaction_rejects_nonpooler_and_unexpected_source_port(self):
        with patch.dict(os.environ, {"ODDS_ATOMIC_POOL_MODE": "transaction"}), patch(
            "scripts.odds_atomic_publication.psycopg2.connect"
        ) as connect:
            for dsn in (
                POOLER_DSN.replace("aws-1-eu-west-1.pooler.supabase.com", "db.example.supabase.co"),
                POOLER_DSN.replace(":5432/", ":6432/"),
                POOLER_DSN.replace(":5432/", "/"),
            ):
                with self.subTest(dsn=dsn.split("@")[-1]):
                    with self.assertRaisesRegex(ValueError, "atomic transaction pool requires"):
                        construct(dsn)
        connect.assert_not_called()


if __name__ == "__main__":
    unittest.main()
