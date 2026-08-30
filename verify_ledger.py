#!/usr/bin/env python3
"""Verify the Technocore Observatory tick-ledger hash chain.

Canonical JSON is UTF-8 encoded with object keys sorted lexicographically, no
insignificant whitespace, ensure_ascii=False, allow_nan=False, and no trailing
newline. ``previous_sha256`` is SHA-256 of the preceding tick's complete
canonical bytes, including that tick's ``tick_sha256``. ``tick_sha256`` is
SHA-256 of the current tick's canonical bytes with only the
``ledger_chain.tick_sha256`` member omitted; this avoids self-reference while
making a changed current tip detectable.

The first tick whose ``ledger_chain.previous_sha256`` is null is the declared
genesis. Ticks before it are an explicitly unchained prefix. After genesis,
every tick must carry a valid link and self-hash.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
from typing import Any

CHAIN_FIELD = "ledger_chain"
CHAIN_VERSION = 1
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_MISSING = object()


def canonical_tick_bytes(record: dict[str, Any]) -> bytes:
    """Return the complete canonical bytes used by the next tick's link."""
    if not isinstance(record, dict):
        raise ValueError("tick is not an object")
    return json.dumps(
        record,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def canonical_tick_hash_bytes(record: dict[str, Any]) -> bytes:
    """Return canonical bytes with only the tick's own digest omitted."""
    chain = record.get(CHAIN_FIELD)
    if not isinstance(chain, dict):
        raise ValueError("tick has no ledger_chain object")
    projection = dict(record)
    projected_chain = dict(chain)
    projected_chain.pop("tick_sha256", None)
    projection[CHAIN_FIELD] = projected_chain
    return canonical_tick_bytes(projection)


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"duplicate JSON object key {key!r}")
        value[key] = item
    return value


def _reject_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON number {value}")


def _chain_error(value: Any) -> str | None:
    if not isinstance(value, dict) or set(value) != {
        "version",
        "previous_sha256",
        "tick_sha256",
    }:
        return "ledger_chain is not a complete version-1 chain object"
    if isinstance(value["version"], bool) or value["version"] != CHAIN_VERSION:
        return "ledger_chain.version is not 1"
    previous = value["previous_sha256"]
    if previous is not None and (
        not isinstance(previous, str) or SHA256_RE.fullmatch(previous) is None
    ):
        return "ledger_chain.previous_sha256 is not null or lowercase SHA-256"
    tick_hash = value["tick_sha256"]
    if not isinstance(tick_hash, str) or SHA256_RE.fullmatch(tick_hash) is None:
        return "ledger_chain.tick_sha256 is not lowercase SHA-256"
    return None


def _report(
    *,
    ok: bool,
    ticks: int,
    unchained_prefix_ticks: int,
    genesis_line: int | None,
    genesis_ts: str | None,
    tip_sha256: str | None,
    first_break: int | None = None,
    message: str = "",
) -> dict[str, Any]:
    return {
        "ok": ok,
        "ticks": ticks,
        "unchained_prefix_ticks": unchained_prefix_ticks,
        "genesis_line": genesis_line,
        "genesis_ts": genesis_ts,
        "tip_sha256": tip_sha256,
        "first_break": first_break,
        "message": message,
    }


def verify_ledger(path: Path) -> dict[str, Any]:
    """Walk a JSONL ledger and return the first chain break, if any."""
    ticks = 0
    unchained_prefix_ticks = 0
    genesis_line: int | None = None
    genesis_ts: str | None = None
    previous_record: dict[str, Any] | None = None
    tip_sha256: str | None = None

    if not path.exists():
        return _report(
            ok=True,
            ticks=0,
            unchained_prefix_ticks=0,
            genesis_line=None,
            genesis_ts=None,
            tip_sha256=None,
        )

    try:
        source = path.open("rb")
    except OSError as error:
        return _report(
            ok=False,
            ticks=0,
            unchained_prefix_ticks=0,
            genesis_line=None,
            genesis_ts=None,
            tip_sha256=None,
            message=f"cannot read ledger: {error}",
        )

    with source:
        for line_number, raw_line in enumerate(source, start=1):
            if not raw_line.strip():
                continue
            ticks += 1
            try:
                record = json.loads(
                    raw_line.decode("utf-8"),
                    object_pairs_hook=_unique_object,
                    parse_constant=_reject_constant,
                )
                if not isinstance(record, dict):
                    raise ValueError("tick is not an object")
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
                if genesis_line is None:
                    unchained_prefix_ticks += 1
                    continue
                return _report(
                    ok=False,
                    ticks=ticks,
                    unchained_prefix_ticks=unchained_prefix_ticks,
                    genesis_line=genesis_line,
                    genesis_ts=genesis_ts,
                    tip_sha256=tip_sha256,
                    first_break=line_number,
                    message=f"tick does not parse canonically: {error}",
                )

            chain = record.get(CHAIN_FIELD, _MISSING)
            if chain is _MISSING:
                if genesis_line is not None:
                    return _report(
                        ok=False,
                        ticks=ticks,
                        unchained_prefix_ticks=unchained_prefix_ticks,
                        genesis_line=genesis_line,
                        genesis_ts=genesis_ts,
                        tip_sha256=tip_sha256,
                        first_break=line_number,
                        message="tick is missing ledger_chain after genesis",
                    )
                unchained_prefix_ticks += 1
                continue

            error = _chain_error(chain)
            if error is not None:
                return _report(
                    ok=False,
                    ticks=ticks,
                    unchained_prefix_ticks=unchained_prefix_ticks,
                    genesis_line=genesis_line,
                    genesis_ts=genesis_ts,
                    tip_sha256=tip_sha256,
                    first_break=line_number,
                    message=error,
                )

            expected_tick_hash = hashlib.sha256(
                canonical_tick_hash_bytes(record)
            ).hexdigest()
            if chain["tick_sha256"] != expected_tick_hash:
                return _report(
                    ok=False,
                    ticks=ticks,
                    unchained_prefix_ticks=unchained_prefix_ticks,
                    genesis_line=genesis_line,
                    genesis_ts=genesis_ts,
                    tip_sha256=tip_sha256,
                    first_break=line_number,
                    message=(
                        "tick_sha256 mismatch: expected "
                        f"{expected_tick_hash}, found {chain['tick_sha256']}"
                    ),
                )

            if genesis_line is None:
                if chain["previous_sha256"] is not None:
                    return _report(
                        ok=False,
                        ticks=ticks,
                        unchained_prefix_ticks=unchained_prefix_ticks,
                        genesis_line=None,
                        genesis_ts=None,
                        tip_sha256=None,
                        first_break=line_number,
                        message="first chained tick does not declare a null-link genesis",
                    )
                genesis_line = line_number
                genesis_ts = record.get("ts") if isinstance(record.get("ts"), str) else None
            else:
                if chain["previous_sha256"] is None:
                    return _report(
                        ok=False,
                        ticks=ticks,
                        unchained_prefix_ticks=unchained_prefix_ticks,
                        genesis_line=genesis_line,
                        genesis_ts=genesis_ts,
                        tip_sha256=tip_sha256,
                        first_break=line_number,
                        message="unexpected second chain genesis",
                    )
                expected_previous = hashlib.sha256(
                    canonical_tick_bytes(previous_record)
                ).hexdigest()
                if chain["previous_sha256"] != expected_previous:
                    return _report(
                        ok=False,
                        ticks=ticks,
                        unchained_prefix_ticks=unchained_prefix_ticks,
                        genesis_line=genesis_line,
                        genesis_ts=genesis_ts,
                        tip_sha256=tip_sha256,
                        first_break=line_number,
                        message=(
                            "previous_sha256 mismatch: expected "
                            f"{expected_previous}, found {chain['previous_sha256']}"
                        ),
                    )

            previous_record = record
            tip_sha256 = hashlib.sha256(canonical_tick_bytes(record)).hexdigest()

    return _report(
        ok=True,
        ticks=ticks,
        unchained_prefix_ticks=unchained_prefix_ticks,
        genesis_line=genesis_line,
        genesis_ts=genesis_ts,
        tip_sha256=tip_sha256,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("ledger", type=Path)
    args = parser.parse_args()
    result = verify_ledger(args.ledger)
    if not result["ok"]:
        where = (
            f" at line {result['first_break']}"
            if result["first_break"] is not None
            else ""
        )
        print(f"FAIL{where}: {result['message']}")
        return 1
    if result["genesis_line"] is None:
        print(
            f"OK: {result['ticks']} unchained tick(s); "
            "no hash-chain genesis has been declared yet"
        )
    else:
        print(
            f"OK: {result['ticks']} tick(s); {result['unchained_prefix_ticks']} "
            f"unchained prefix tick(s); genesis line {result['genesis_line']} "
            f"at {result['genesis_ts'] or 'timestamp not recorded'}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())