"""Isolated status evidence review; never enters fixture delivery orchestration."""

from __future__ import annotations

import argparse
import signal

from scripts import postmatch_fixture_detail_delivery as delivery


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--postponement-status-evidence", required=True)
    parser.add_argument("--postponement-observe-status", action="store_true")
    parser.add_argument("--postponement-provider-budget", type=int, default=0)
    parser.add_argument("--execution-seconds", type=int, default=120,
                        help="Hard active-process lifetime, 1-120 seconds; default 120.")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if not args.postponement_status_evidence.strip():
        parser.error("Evidence path must not be empty or whitespace")
    if not 1 <= args.execution_seconds <= 120:
        parser.error("Execution deadline must be 1-120 seconds")
    active = args.postponement_observe_status
    if ((active and not 1 <= args.postponement_provider_budget <= 3)
            or (not active and args.postponement_provider_budget != 0)):
        parser.error("Active observation requires budget 1-3; offline review requires budget zero")
    # SIG_DFL is kernel termination, not a Python callback delayed by blocked C I/O.
    # No child workers are created. Process death closes its own flock descriptor.
    if active:
        try:
            signal.signal(signal.SIGALRM, signal.SIG_DFL)
            signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGALRM})
            if (signal.getsignal(signal.SIGALRM) != signal.SIG_DFL
                    or signal.SIGALRM in signal.pthread_sigmask(signal.SIG_BLOCK, set())):
                raise RuntimeError("SIGALRM termination is not available")
            signal.setitimer(signal.ITIMER_REAL, args.execution_seconds)
            remaining, interval = signal.getitimer(signal.ITIMER_REAL)
            if not 0 < remaining <= args.execution_seconds or interval != 0:
                raise RuntimeError("Hard process timer was not established")
        except (OSError, ValueError, RuntimeError, AttributeError) as exc:
            raise SystemExit(f"Cannot establish hard observation deadline: {exc}") from None
    try:
        args.force = False
        args.fixture_ids = None
        args.batch_projection = False
        args.report_json = None
        return delivery.run_postponement_revalidation(args)
    finally:
        if active:
            signal.setitimer(signal.ITIMER_REAL, 0)


if __name__ == "__main__":
    raise SystemExit(main())
