#!/usr/bin/env python3
"""Recover a committed Observatory tick without reading the origin."""

from __future__ import annotations

import argparse
import sqlite3
import sys

from collect import (
    CollectionError,
    _ledger_pending_path,
    absolute_path,
    drain_tick_outbox,
    recover_pending_jsonl,
)
from verify_ledger import verify_ledger


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=absolute_path)
    parser.add_argument("--signer-state", required=True, type=absolute_path)
    parser.add_argument("--census-state", required=True, type=absolute_path)
    return parser


def ledger_failure(result: dict[str, object]) -> str:
    where = (
        f" at line {result['first_break']}" if result["first_break"] is not None else ""
    )
    return f"ledger verification failed{where}: {result['message']}"


def main() -> int:
    args = build_parser().parse_args()
    if args.output.is_symlink() or (args.output.exists() and not args.output.is_file()):
        print(
            "publication recovery failed: output is not an existing regular tick "
            "ledger",
            file=sys.stderr,
        )
        return 1
    try:
        drained = drain_tick_outbox(
            args.output,
            args.signer_state,
            args.census_state,
        )
        journal_recovered = recover_pending_jsonl(args.output)
        if not args.output.is_file() or args.output.is_symlink():
            print(
                "publication recovery failed: recovery produced no existing regular "
                "tick ledger",
                file=sys.stderr,
            )
            return 1
        pending_path = _ledger_pending_path(args.output)
        if pending_path.exists():
            print(
                "publication recovery failed: unresolved ledger pending journal "
                f"remains: {pending_path}",
                file=sys.stderr,
            )
            return 1

        after = verify_ledger(args.output)
        if not after["ok"]:
            print(
                f"publication recovery failed: {ledger_failure(after)}", file=sys.stderr
            )
            return 1
        if after["ticks"] == 0:
            print(
                "publication recovery failed: ledger contains no ticks",
                file=sys.stderr,
            )
            return 1
    except (CollectionError, OSError, sqlite3.Error) as error:
        print(f"publication recovery failed: {error}", file=sys.stderr)
        return 1

    if drained:
        publication = "pending publication drained"
    elif journal_recovered:
        publication = "pending ledger journal recovered"
    else:
        publication = "no pending publication"
    print(f"OK: {publication}; ledger verified ({after['ticks']} ticks)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
