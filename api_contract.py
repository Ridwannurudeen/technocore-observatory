"""Shared response contract for the read-only Observatory API."""

from __future__ import annotations

import json
import math
import unicodedata
from datetime import datetime, timezone
from typing import Any

CONTRACT_VERSION = "1.1.0"
MAX_RESPONSE_BYTES = 64 * 1024
COMMON_FIELDS = (
    "contract_version",
    "generated_at",
    "source_observed_at",
    "valid_until",
    "freshness",
    "collector_version",
    "methodology_version",
    "schema_version",
    "window",
    "coverage",
    "limitations",
    "ledger_chain_head",
)
FRESHNESS_STATES = {"fresh", "stale", "not_observed", "not_applicable"}


def utc_now() -> str:
    return (
        datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    )


def common_metadata(
    *,
    source_observed_at: str | None,
    valid_until: str | None,
    freshness: str,
    collector_version: str | None,
    methodology_version: str | None,
    schema_version: int | None,
    window: dict[str, Any] | None,
    coverage: dict[str, Any] | None,
    limitations: list[str],
    ledger_chain_head: str | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    if freshness not in FRESHNESS_STATES:
        raise ValueError(f"unsupported freshness state: {freshness}")
    if not all(isinstance(item, str) for item in limitations):
        raise TypeError("limitations must contain only strings")
    metadata = {
        "contract_version": CONTRACT_VERSION,
        "generated_at": generated_at or utc_now(),
        "source_observed_at": source_observed_at,
        "valid_until": valid_until,
        "freshness": freshness,
        "collector_version": collector_version,
        "methodology_version": methodology_version,
        "schema_version": schema_version,
        "window": window,
        "coverage": coverage,
        "limitations": limitations,
    }
    if ledger_chain_head is not None:
        metadata["ledger_chain_head"] = ledger_chain_head
    return metadata


def escape_plain_text(value: Any) -> str:
    if value is None:
        return "not_recorded"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        if isinstance(value, float) and not math.isfinite(value):
            raise ValueError("plain-text output cannot contain non-finite numbers")
        return str(value)
    text = str(value)
    escaped: list[str] = []
    for character in text:
        if character == "\r":
            escaped.append("\\r")
        elif character == "\n":
            escaped.append("\\n")
        elif character == "\t":
            escaped.append("\\t")
        elif unicodedata.category(character) in {"Cc", "Cf", "Zl", "Zp"}:
            codepoint = ord(character)
            if codepoint <= 0xFFFF:
                escaped.append(f"\\u{codepoint:04x}")
            else:
                escaped.append(f"\\U{codepoint:08x}")
        else:
            escaped.append(character)
    return "".join(escaped)


def json_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")


def text_bytes(payload: dict[str, Any]) -> bytes:
    ordered = [key for key in COMMON_FIELDS if key in payload]
    ordered.extend(key for key in payload if key not in COMMON_FIELDS)
    lines: list[str] = []
    for key in ordered:
        value = payload[key]
        if isinstance(value, list):
            if not value:
                lines.append(f"{key}: []")
                continue
            for index, item in enumerate(value, start=1):
                serialized = (
                    json.dumps(
                        item,
                        ensure_ascii=False,
                        allow_nan=False,
                        separators=(",", ":"),
                    )
                    if isinstance(item, (dict, list))
                    else escape_plain_text(item)
                )
                lines.append(f"{key}.{index}: {escape_plain_text(serialized)}")
        elif isinstance(value, dict):
            serialized = json.dumps(
                value,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
            )
            lines.append(f"{key}: {escape_plain_text(serialized)}")
        else:
            lines.append(f"{key}: {escape_plain_text(value)}")
    return ("\n".join(lines) + "\n").encode("utf-8")
