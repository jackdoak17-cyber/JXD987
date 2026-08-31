#!/usr/bin/env python3
"""Load and validate the versioned fixture-core freshness contract.

The pipeline and the Operations checker use this contract as the shared
source of truth for the monitored identity horizon.  The provider/source
comparison deliberately remains smaller than the source refresh window: the
extra operational day protects the inclusive provider end date and clock
boundaries without changing the customer-facing contract.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping


CONTRACT_VERSION = 1
DEFAULT_CONTRACT_PATH = Path(__file__).resolve().parents[1] / "config" / "fixture_core_contract.json"
_JOB_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_]*$")


class FixtureCoreContractError(ValueError):
    """Raised when the shared fixture-core contract is missing or invalid."""


@dataclass(frozen=True)
class FixtureCoreContract:
    version: int
    identity_window_days: int
    history_window_days: int
    source_buffer_days: int
    odds_window_days: int
    fixture_delivery_window_days: int
    delivery_window_days: int
    same_day_grace_hours: int
    max_job_age_minutes: int
    job_id: str
    time_basis: str

    @property
    def source_window_days_forward(self) -> int:
        """Number of date-forward days used by the source refresh/export."""

        return self.identity_window_days + self.source_buffer_days

    @property
    def local_read_lookahead_days(self) -> int:
        """Local read horizon needed to cover the inclusive provider end date."""

        return self.source_window_days_forward


def _integer_field(payload: Mapping[str, Any], name: str, minimum: int) -> int:
    value = payload.get(name)
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise FixtureCoreContractError(
            f"{name} must be an integer greater than or equal to {minimum}"
        )
    return value


def _load_payload(path: Path) -> Mapping[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise FixtureCoreContractError(f"contract file not found: {path}") from error
    except (OSError, json.JSONDecodeError) as error:
        raise FixtureCoreContractError(f"unable to read contract file {path}: {error}") from error
    if not isinstance(payload, dict):
        raise FixtureCoreContractError("fixture-core contract must be a JSON object")
    return payload


def parse_contract(payload: Mapping[str, Any]) -> FixtureCoreContract:
    """Validate a decoded contract and return its typed representation."""

    required = {
        "version",
        "identity_window_days",
        "history_window_days",
        "source_buffer_days",
        "odds_window_days",
        "fixture_delivery_window_days",
        "delivery_window_days",
        "same_day_grace_hours",
        "max_job_age_minutes",
        "job_id",
        "time_basis",
    }
    missing = sorted(required.difference(payload))
    if missing:
        raise FixtureCoreContractError(f"missing contract fields: {', '.join(missing)}")
    unknown = sorted(set(payload).difference(required))
    if unknown:
        raise FixtureCoreContractError(f"unknown contract fields: {', '.join(unknown)}")

    version = _integer_field(payload, "version", CONTRACT_VERSION)
    if version != CONTRACT_VERSION:
        raise FixtureCoreContractError(
            f"unsupported fixture-core contract version: {version}; expected {CONTRACT_VERSION}"
        )
    identity_window_days = _integer_field(payload, "identity_window_days", 1)
    history_window_days = _integer_field(payload, "history_window_days", 0)
    source_buffer_days = _integer_field(payload, "source_buffer_days", 1)
    odds_window_days = _integer_field(payload, "odds_window_days", 1)
    fixture_delivery_window_days = _integer_field(payload, "fixture_delivery_window_days", 1)
    delivery_window_days = _integer_field(payload, "delivery_window_days", identity_window_days)
    if delivery_window_days < fixture_delivery_window_days:
        raise FixtureCoreContractError(
            "delivery_window_days must cover fixture_delivery_window_days"
        )
    same_day_grace_hours = _integer_field(payload, "same_day_grace_hours", 0)
    if same_day_grace_hours > 24:
        raise FixtureCoreContractError("same_day_grace_hours must not exceed 24")
    max_job_age_minutes = _integer_field(payload, "max_job_age_minutes", 1)

    job_id = payload["job_id"]
    if not isinstance(job_id, str) or not _JOB_ID_PATTERN.fullmatch(job_id):
        raise FixtureCoreContractError(
            "job_id must contain only lowercase letters, numbers, and underscores"
        )
    time_basis = payload["time_basis"]
    if time_basis != "UTC":
        raise FixtureCoreContractError("time_basis must be UTC")

    return FixtureCoreContract(
        version=version,
        identity_window_days=identity_window_days,
        history_window_days=history_window_days,
        source_buffer_days=source_buffer_days,
        odds_window_days=odds_window_days,
        fixture_delivery_window_days=fixture_delivery_window_days,
        delivery_window_days=delivery_window_days,
        same_day_grace_hours=same_day_grace_hours,
        max_job_age_minutes=max_job_age_minutes,
        job_id=job_id,
        time_basis=time_basis,
    )


def load_contract(path: str | Path | None = None) -> FixtureCoreContract:
    contract_path = Path(
        path
        or os.environ.get("FIXTURE_CORE_CONTRACT_PATH")
        or DEFAULT_CONTRACT_PATH
    ).expanduser()
    return parse_contract(_load_payload(contract_path))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--contract", help="path to the JSON contract")
    parser.add_argument(
        "--field",
        required=True,
        choices=(
            "version",
            "identity_window_days",
            "history_window_days",
            "source_buffer_days",
            "source_window_days_forward",
            "local_read_lookahead_days",
            "odds_window_days",
            "fixture_delivery_window_days",
            "delivery_window_days",
            "same_day_grace_hours",
            "max_job_age_minutes",
            "job_id",
            "time_basis",
        ),
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        contract = load_contract(args.contract)
    except FixtureCoreContractError as error:
        print(f"fixture-core contract error: {error}", file=sys.stderr)
        return 2
    print(getattr(contract, args.field))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
