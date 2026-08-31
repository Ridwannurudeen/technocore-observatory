#!/usr/bin/env python3
"""Unmetered health and public-discovery probe."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sqlite3
import sys
import time
from typing import Any

from collect import Client, CollectionError, absolute_path, utc_now
from telemetry import TelemetryStore

PULSE_VERSION = "1.0.0"
HEALTH_CADENCE_SECONDS = 60
DISCOVERY_CADENCE_SECONDS = 60 * 60
CONFIG_FIELDS = ("service", "version")
CONFIG_SETTING_FIELDS = (
    "rate_read",
    "rate_write",
    "rate_rooms_per_day",
    "max_rooms",
    "max_notes_per_ns",
    "max_wait",
    "wait_poll",
    "max_waiters_total",
    "max_waiters_per_ip",
    "dupe_filter_seconds",
    "dupe_max_copies",
    "dupe_min_length",
    "ephemeral_ttl_seconds",
    "fsync",
    "rooms_cache_seconds",
    "note_stats_cache_seconds",
    "edge_cache_seconds",
    "static_cache_seconds",
)
AGENT_FIELDS = ("schema_version", "name", "version", "role", "protocols")
AGENT_CAPABILITY_FIELDS = ("name", "method", "path")
AGENT_NESTED_FIELDS = {
    "identity": (
        "scheme",
        "algorithms",
        "resolution",
        "message_signature_payload",
        "note_signature_payload",
        "signature_encoding",
        "nonce",
        "canonicalisation",
        "publishing_a_key",
        "required_for",
    ),
    "limits": (
        "message_chars",
        "note_chars",
        "reads_per_minute_per_ip",
        "writes_per_minute_per_ip",
        "new_rooms_per_day_per_ip",
        "rooms",
        "notes",
        "notes_per_namespace",
        "room_ring_bytes",
        "room_bytes_total",
        "retention_seconds",
        "ephemeral_ttl_seconds",
        "duplicate_filter_seconds",
        "long_poll_seconds",
    ),
    "trust": ("content_is_untrusted", "durable", "world_writable"),
}
MAX_DISCOVERY_STRING_CODEPOINTS = 4_096
MAX_DISCOVERY_LIST_ITEMS = 128
MAX_DISCOVERY_CAPABILITIES = 128


def canonical_string(value: str, field: str, route: str) -> str:
    if len(value) > MAX_DISCOVERY_STRING_CODEPOINTS:
        raise CollectionError(
            f"{field} exceeds the public string limit",
            outcome="invalid_response",
            path=route,
        )
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as error:
        raise CollectionError(
            f"{field} contains a lone surrogate",
            outcome="invalid_response",
            path=route,
        ) from error
    return value


def canonical_value(value: Any, field: str, route: str) -> Any:
    if isinstance(value, str):
        return canonical_string(value, field, route)
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, float):
        if math.isfinite(value):
            return value
        raise CollectionError(
            f"{field} contains a non-finite value",
            outcome="invalid_response",
            path=route,
        )
    if isinstance(value, list):
        if len(value) > MAX_DISCOVERY_LIST_ITEMS:
            raise CollectionError(
                f"{field} exceeds the public list limit",
                outcome="invalid_response",
                path=route,
            )
        for item in value:
            if isinstance(item, str):
                canonical_string(item, field, route)
            elif isinstance(item, float):
                if not math.isfinite(item):
                    break
            elif item is not None and not isinstance(item, (bool, int)):
                break
        else:
            return value
    raise CollectionError(
        f"{field} contains an unsupported public value",
        outcome="invalid_response",
        path=route,
    )


def selected_fields(
    payload: dict[str, Any],
    names: tuple[str, ...],
    route: str,
    field_prefix: str | None = None,
) -> dict[str, Any]:
    return {
        name: canonical_value(
            payload[name],
            f"{field_prefix or route}.{name}",
            route,
        )
        for name in names
        if name in payload
    }


def canonical_discovery_fields(route: str, body: str) -> dict[str, Any]:
    try:
        payload = json.loads(body)
    except (ValueError, RecursionError) as error:
        raise CollectionError(
            f"{route} did not parse as JSON",
            outcome="invalid_response",
            path=route,
        ) from error
    if not isinstance(payload, dict):
        raise CollectionError(
            f"{route} JSON is not an object",
            outcome="invalid_response",
            path=route,
        )

    if route == "/config":
        result = selected_fields(payload, CONFIG_FIELDS, route)
        if "settings" in payload:
            settings = payload["settings"]
            if not isinstance(settings, dict):
                raise CollectionError(
                    "/config settings is not an object",
                    outcome="invalid_response",
                    path=route,
                )
            result["settings"] = selected_fields(
                settings,
                CONFIG_SETTING_FIELDS,
                route,
                f"{route}.settings",
            )
        return result

    if route == "/.well-known/agent.json":
        result = selected_fields(payload, AGENT_FIELDS, route)
        if "capabilities" in payload:
            capabilities = payload["capabilities"]
            if not isinstance(capabilities, list) or not all(
                isinstance(capability, dict) for capability in capabilities
            ):
                raise CollectionError(
                    f"{route} capabilities is not a list of objects",
                    outcome="invalid_response",
                    path=route,
                )
            if len(capabilities) > MAX_DISCOVERY_CAPABILITIES:
                raise CollectionError(
                    f"{route} capabilities exceeds the public list limit",
                    outcome="invalid_response",
                    path=route,
                )
            result["capabilities"] = [
                selected_fields(
                    capability,
                    AGENT_CAPABILITY_FIELDS,
                    route,
                    f"{route}.capabilities",
                )
                for capability in capabilities
            ]
        for container, names in AGENT_NESTED_FIELDS.items():
            if container not in payload:
                continue
            nested = payload[container]
            if not isinstance(nested, dict):
                raise CollectionError(
                    f"{route} {container} is not an object",
                    outcome="invalid_response",
                    path=route,
                )
            result[container] = selected_fields(
                nested,
                names,
                route,
                f"{route}.{container}",
            )
        return result

    raise ValueError("unsupported discovery route")


def discovery_digest(fields: dict[str, Any]) -> str:
    canonical = json.dumps(
        fields,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def run_probe_cycle(
    telemetry: TelemetryStore,
    base_url: str,
    timeout: float,
    observed_at: str | None = None,
) -> bool:
    observed_at = observed_at or utc_now()
    cycle_id = telemetry.start_cycle("pulse_probe", observed_at)
    client = Client(
        base_url,
        timeout,
        0,
        telemetry=telemetry,
        cycle_id=cycle_id,
    )
    failures: list[str] = []

    try:
        health = client.get("/healthz")
        if health.strip() != "ok":
            if client.last_attempt_id is not None:
                telemetry.update_attempt_outcome(
                    client.last_attempt_id,
                    "invalid_response",
                )
            raise CollectionError(
                "/healthz returned an unexpected body",
                outcome="invalid_response",
                path="/healthz",
            )
    except CollectionError as error:
        failures.append(error.outcome)

    for route in ("/config", "/.well-known/agent.json"):
        if not telemetry.route_due(route, observed_at, DISCOVERY_CADENCE_SECONDS):
            continue
        try:
            body = client.get(route)
            fields = canonical_discovery_fields(route, body)
            if client.last_attempt_id is None:
                raise RuntimeError("successful discovery request lacks telemetry")
            telemetry.record_discovery_snapshot(
                client.last_attempt_id,
                route,
                observed_at,
                discovery_digest(fields),
                fields,
            )
        except CollectionError as error:
            if (
                error.outcome == "invalid_response"
                and client.last_attempt_id is not None
            ):
                telemetry.update_attempt_outcome(
                    client.last_attempt_id,
                    "invalid_response",
                )
            failures.append(error.outcome)

    if failures:
        telemetry.finish_cycle(
            cycle_id,
            "failure",
            error_outcome=failures[0],
            finished_at=observed_at,
        )
        return False
    telemetry.finish_cycle(cycle_id, "success", finished_at=observed_at)
    return True


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Probe Technocore's unmetered health and discovery routes."
    )
    parser.add_argument(
        "--base-url", required=True, help="Service origin, without a path."
    )
    parser.add_argument("--telemetry-database", required=True, type=absolute_path)
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--once", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.timeout <= 0:
        raise SystemExit("timeout must be positive")
    args.telemetry_database.parent.mkdir(parents=True, exist_ok=True)
    telemetry = TelemetryStore(args.telemetry_database)
    try:
        while True:
            started = time.monotonic()
            try:
                success = run_probe_cycle(
                    telemetry,
                    args.base_url,
                    args.timeout,
                )
            except (OSError, sqlite3.Error) as error:
                print(f"{utc_now()} pulse probe failed: {error}", file=sys.stderr)
                success = False
            if args.once:
                return 0 if success else 1
            time.sleep(
                max(
                    0.0,
                    HEALTH_CADENCE_SECONDS - (time.monotonic() - started),
                )
            )
    finally:
        telemetry.close()


if __name__ == "__main__":
    raise SystemExit(main())
