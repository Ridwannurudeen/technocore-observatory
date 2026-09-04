"""Build bounded read-only snapshots from collector ticks and probe telemetry."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sqlite3
from collections import defaultdict, deque
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

import derive
from api_contract import (
    MAX_RESPONSE_BYTES,
    common_metadata,
    json_bytes,
    text_bytes,
    utc_now,
)

INCIDENT_RULES_VERSION = "1.0.0"
VALID_FOR = timedelta(minutes=15)
STATUS_WINDOW = timedelta(hours=1)
MAX_ATTEMPTS = 50_000
MAX_DISCOVERY_SNAPSHOTS = 5_000
MAX_INCIDENTS = 100
MAX_CHANGES = 100
FIELD_ABSENT = {"state": "field_absent"}
MISSING = object()


def parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamps must include a UTC offset")
    try:
        return parsed.astimezone(timezone.utc)
    except OverflowError as error:
        raise ValueError(
            "timestamps must normalize within the supported range"
        ) from error


def format_utc(value: datetime) -> str:
    return (
        value.astimezone(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def canonical_time(value: str | None) -> str:
    return format_utc(parse_utc(value or utc_now()))


def valid_until(source_observed_at: str | None) -> str | None:
    if source_observed_at is None:
        return None
    try:
        return format_utc(parse_utc(source_observed_at) + VALID_FOR)
    except OverflowError as error:
        raise ValueError(
            "snapshot validity window exceeds the supported datetime range"
        ) from error


def freshness(
    source_observed_at: str | None, published_at: str, validity: str | None
) -> str:
    if source_observed_at is None or validity is None:
        return "not_observed"
    return "fresh" if parse_utc(published_at) <= parse_utc(validity) else "stale"


def latest_time(values: Iterable[str | None]) -> str | None:
    present = [value for value in values if value is not None]
    return max(present, key=parse_utc) if present else None


def earliest_time(values: Iterable[str | None]) -> str | None:
    present = [value for value in values if value is not None]
    return min(present, key=parse_utc) if present else None


def load_ticks(path: Path) -> tuple[list[dict[str, Any]], int]:
    with path.open(encoding="utf-8") as source:
        return derive.read_jsonl(source)


def load_telemetry(path: Path) -> dict[str, Any]:
    uri = f"file:{path.resolve().as_posix()}?mode=ro"
    with closing(sqlite3.connect(uri, uri=True)) as connection:
        connection.row_factory = sqlite3.Row
        connection.execute("BEGIN")
        schema_version = connection.execute("PRAGMA user_version").fetchone()[0]
        if schema_version != 1:
            raise ValueError(f"unsupported telemetry schema version: {schema_version}")
        cycles = [
            dict(row)
            for row in connection.execute(
                """
                SELECT id, source, started_at, finished_at, outcome, error_outcome
                FROM cycles
                ORDER BY started_at DESC, id DESC
                LIMIT 5000
                """
            )
        ]
        attempts = [
            dict(row)
            for row in connection.execute(
                """
                SELECT id, cycle_id, route, metered, attempt, observed_at,
                       latency_ms, outcome, http_status
                FROM request_attempts
                ORDER BY observed_at DESC, id DESC
                LIMIT ?
                """,
                (MAX_ATTEMPTS,),
            )
        ]
        for attempt in attempts:
            latency_ms = attempt["latency_ms"]
            if (
                isinstance(latency_ms, bool)
                or not isinstance(latency_ms, (int, float))
                or not math.isfinite(latency_ms)
                or latency_ms < 0
            ):
                raise ValueError(
                    "telemetry latency_ms must be a finite non-negative number"
                )
            http_status = attempt["http_status"]
            if http_status is not None and (
                isinstance(http_status, bool)
                or not isinstance(http_status, int)
                or not 100 <= http_status <= 599
            ):
                raise ValueError(
                    "telemetry http_status must be an integer from 100 to 599"
                )
        discoveries = [
            dict(row)
            for row in connection.execute(
                """
                SELECT id, attempt_id, route, observed_at, digest_sha256,
                       fields_json
                FROM discovery_snapshots
                WHERE route IN ('/config', '/.well-known/agent.json')
                ORDER BY observed_at DESC, id DESC
                LIMIT ?
                """,
                (MAX_DISCOVERY_SNAPSHOTS,),
            )
        ]
        attempts.reverse()
        discoveries.reverse()

        attempt_total = int(
            connection.execute("SELECT COUNT(*) FROM request_attempts").fetchone()[0]
        )
        attempt_truncated = attempt_total > len(attempts)
        attempt_carry: dict[str, Any] = {
            "health_probe_failed": None,
            "endpoint_5xx": {},
        }
        if attempt_truncated and attempts:
            cutoff = attempts[0]
            cutoff_parameters = (
                cutoff["observed_at"],
                cutoff["observed_at"],
                cutoff["id"],
            )
            last_health_success = connection.execute(
                """
                SELECT observed_at, id
                FROM request_attempts
                WHERE route = '/healthz'
                  AND outcome = 'success'
                  AND (
                      observed_at < ?
                      OR (observed_at = ? AND id < ?)
                  )
                ORDER BY observed_at DESC, id DESC
                LIMIT 1
                """,
                cutoff_parameters,
            ).fetchone()
            health_after_success = ""
            health_parameters: list[Any] = list(cutoff_parameters)
            if last_health_success is not None:
                health_after_success = """
                  AND (
                      observed_at > ?
                      OR (observed_at = ? AND id > ?)
                  )
                """
                health_parameters.extend(
                    (
                        last_health_success["observed_at"],
                        last_health_success["observed_at"],
                        last_health_success["id"],
                    )
                )
            health_carry = connection.execute(
                f"""
                SELECT COUNT(*) AS attempts,
                       MIN(observed_at) AS opened_at,
                       MAX(observed_at) AS last_observed_at
                FROM request_attempts
                WHERE route = '/healthz'
                  AND outcome <> 'success'
                  AND (
                      observed_at < ?
                      OR (observed_at = ? AND id < ?)
                  )
                  {health_after_success}
                """,
                health_parameters,
            ).fetchone()
            if health_carry["attempts"]:
                attempt_carry["health_probe_failed"] = {
                    "opened_at": health_carry["opened_at"],
                    "last_observed_at": health_carry["last_observed_at"],
                    "attempts": int(health_carry["attempts"]),
                    "observed_failures": int(health_carry["attempts"]),
                }

            carry_routes = connection.execute(
                """
                SELECT DISTINCT route
                FROM request_attempts
                WHERE http_status BETWEEN 500 AND 599
                  AND (
                      observed_at < ?
                      OR (observed_at = ? AND id < ?)
                  )
                """,
                cutoff_parameters,
            )
            for route_row in carry_routes:
                route = route_row["route"]
                last_success = connection.execute(
                    """
                    SELECT observed_at, id
                    FROM request_attempts
                    WHERE route = ?
                      AND outcome = 'success'
                      AND (
                          observed_at < ?
                          OR (observed_at = ? AND id < ?)
                      )
                    ORDER BY observed_at DESC, id DESC
                    LIMIT 1
                    """,
                    (route, *cutoff_parameters),
                ).fetchone()
                after_success = ""
                start_parameters: list[Any] = [route, *cutoff_parameters]
                if last_success is not None:
                    after_success = """
                      AND (
                          observed_at > ?
                          OR (observed_at = ? AND id > ?)
                      )
                    """
                    start_parameters.extend(
                        (
                            last_success["observed_at"],
                            last_success["observed_at"],
                            last_success["id"],
                        )
                    )
                active_start = connection.execute(
                    f"""
                    SELECT observed_at, id
                    FROM request_attempts
                    WHERE route = ?
                      AND http_status BETWEEN 500 AND 599
                      AND (
                          observed_at < ?
                          OR (observed_at = ? AND id < ?)
                      )
                      {after_success}
                    ORDER BY observed_at, id
                    LIMIT 1
                    """,
                    start_parameters,
                ).fetchone()
                if active_start is None:
                    continue
                route_carry = connection.execute(
                    """
                    SELECT COUNT(*) AS attempts,
                           SUM(CASE WHEN outcome <> 'success' THEN 1 ELSE 0 END)
                               AS observed_failures,
                           MAX(observed_at) AS last_observed_at
                    FROM request_attempts
                    WHERE route = ?
                      AND (
                          observed_at < ?
                          OR (observed_at = ? AND id < ?)
                      )
                      AND (
                          observed_at > ?
                          OR (observed_at = ? AND id >= ?)
                      )
                    """,
                    (
                        route,
                        *cutoff_parameters,
                        active_start["observed_at"],
                        active_start["observed_at"],
                        active_start["id"],
                    ),
                ).fetchone()
                attempt_carry["endpoint_5xx"][route] = {
                    "opened_at": active_start["observed_at"],
                    "last_observed_at": route_carry["last_observed_at"],
                    "attempts": int(route_carry["attempts"]),
                    "observed_failures": int(route_carry["observed_failures"]),
                }

        discovery_total = int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM discovery_snapshots
                WHERE route IN ('/config', '/.well-known/agent.json')
                """
            ).fetchone()[0]
        )
        discovery_truncated = discovery_total > len(discoveries)
        discovery_predecessors: dict[str, dict[str, Any]] = {}
        if discovery_truncated:
            first_by_route: dict[str, dict[str, Any]] = {}
            for discovery in discoveries:
                first_by_route.setdefault(discovery["route"], discovery)
            for route, first in first_by_route.items():
                predecessor = connection.execute(
                    """
                    SELECT id, attempt_id, route, observed_at, digest_sha256,
                           fields_json
                    FROM discovery_snapshots
                    WHERE route = ?
                      AND (
                          observed_at < ?
                          OR (observed_at = ? AND id < ?)
                      )
                    ORDER BY observed_at DESC, id DESC
                    LIMIT 1
                    """,
                    (route, first["observed_at"], first["observed_at"], first["id"]),
                ).fetchone()
                if predecessor is not None:
                    discovery_predecessors[route] = dict(predecessor)

    attempt_input = {
        "records_total": attempt_total,
        "records_loaded": len(attempts),
        "records_omitted_before_cutoff": attempt_total - len(attempts),
        "cutoff_observed_at": (
            attempts[0]["observed_at"] if attempt_truncated and attempts else None
        ),
        "truncated": attempt_truncated,
        "continuity": (
            "active_incident_state_carried" if attempt_truncated else "complete"
        ),
    }
    discovery_input = {
        "records_total": discovery_total,
        "records_loaded": len(discoveries),
        "records_omitted_before_cutoff": discovery_total - len(discoveries),
        "cutoff_observed_at": (
            discoveries[0]["observed_at"]
            if discovery_truncated and discoveries
            else None
        ),
        "truncated": discovery_truncated,
        "continuity": (
            "predecessor_snapshots_carried"
            if discovery_truncated and discovery_predecessors
            else "bounded_without_predecessor"
            if discovery_truncated
            else "complete"
        ),
        "predecessor_records_loaded": len(discovery_predecessors),
    }
    return {
        "cycles": cycles,
        "attempts": attempts,
        "discoveries": discoveries,
        "attempt_carry": attempt_carry,
        "discovery_predecessors": discovery_predecessors,
        "attempt_input": attempt_input,
        "discovery_input": discovery_input,
    }


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, math.ceil(len(ordered) * fraction) - 1)
    return round(ordered[index], 3)


def is_success(attempt: dict[str, Any]) -> bool:
    return attempt["outcome"] == "success"


def endpoint_summaries(
    attempts: list[dict[str, Any]], source_observed_at: str | None
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if source_observed_at is None:
        return [], {"attempts": 0, "routes": 0, "from": None, "to": None}
    try:
        window_start = parse_utc(source_observed_at) - STATUS_WINDOW
    except OverflowError as error:
        raise ValueError(
            "status window exceeds the supported datetime range"
        ) from error
    bounded = [
        attempt
        for attempt in attempts
        if parse_utc(attempt["observed_at"]) >= window_start
    ]
    by_route: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for attempt in bounded:
        by_route[attempt["route"]].append(attempt)

    summaries: list[dict[str, Any]] = []
    for route in sorted(by_route):
        route_attempts = by_route[route]
        latencies = [float(attempt["latency_ms"]) for attempt in route_attempts]
        summaries.append(
            {
                "route": route,
                "attempts": len(route_attempts),
                "successes": sum(is_success(attempt) for attempt in route_attempts),
                "rate_limited": sum(
                    attempt["http_status"] == 429 for attempt in route_attempts
                ),
                "server_errors": sum(
                    attempt["http_status"] is not None
                    and 500 <= attempt["http_status"] <= 599
                    for attempt in route_attempts
                ),
                "observed_failures": sum(
                    not is_success(attempt) for attempt in route_attempts
                ),
                "latency_ms": {
                    "minimum": round(min(latencies), 3),
                    "median": percentile(latencies, 0.5),
                    "p95": percentile(latencies, 0.95),
                    "maximum": round(max(latencies), 3),
                },
                "first_observed_at": route_attempts[0]["observed_at"],
                "last_observed_at": route_attempts[-1]["observed_at"],
            }
        )
    return summaries, {
        "attempts": len(bounded),
        "routes": len(summaries),
        "from": format_utc(window_start),
        "to": source_observed_at,
    }


def component_freshness(source: str | None, published_at: str) -> dict[str, Any]:
    validity = valid_until(source)
    return {
        "source_observed_at": source,
        "valid_until": validity,
        "freshness": freshness(source, published_at, validity),
    }


def status_resource(
    ticks: Sequence[dict[str, Any]],
    derived: dict[str, Any],
    telemetry: dict[str, Any],
    published_at: str,
) -> tuple[dict[str, Any], str | None, dict[str, Any]]:
    attempts = telemetry["attempts"]
    latest_attempt_at = attempts[-1]["observed_at"] if attempts else None
    latest_tick_at = ticks[-1]["ts"] if ticks else None
    source_observed_at = earliest_time((latest_tick_at, latest_attempt_at))
    endpoints, endpoint_coverage = endpoint_summaries(attempts, latest_attempt_at)

    health_attempts = [
        attempt for attempt in attempts if attempt["route"] == "/healthz"
    ]
    latest_health = health_attempts[-1] if health_attempts else None
    if latest_health is None:
        origin_state = "not_observed"
    elif is_success(latest_health):
        origin_state = "reachable"
    else:
        origin_state = "observed_failure"
    origin_source = latest_health["observed_at"] if latest_health else None
    origin = {
        "state": origin_state,
        "latest_outcome": latest_health["outcome"] if latest_health else None,
        "latest_http_status": latest_health["http_status"] if latest_health else None,
        "latest_latency_ms": latest_health["latency_ms"] if latest_health else None,
        **component_freshness(origin_source, published_at),
    }

    collector_meta = component_freshness(latest_tick_at, published_at)
    collector = {
        "state": (
            collector_meta["freshness"]
            if latest_tick_at is not None
            else "not_observed"
        ),
        "last_tick_at": latest_tick_at,
        "accepted_ticks": derived["accepted_ticks"],
        "rejected_ticks": derived["rejected_ticks"],
        "expected_tick_seconds": derived["history"]["expected_tick_seconds"],
        "observed_gap_count": derived["gap_count"],
        **collector_meta,
    }

    pulse_cycles = [
        cycle for cycle in telemetry["cycles"] if cycle["source"] == "pulse_probe"
    ]
    latest_cycle = pulse_cycles[0] if pulse_cycles else None
    if latest_cycle is None or latest_cycle["outcome"] == "running":
        cycle_state = "unknown"
    else:
        cycle_state = latest_cycle["outcome"]
    pulse_cycle = {
        "state": cycle_state,
        "started_at": latest_cycle["started_at"] if latest_cycle else None,
        "finished_at": latest_cycle["finished_at"] if latest_cycle else None,
        "error_outcome": latest_cycle["error_outcome"] if latest_cycle else None,
    }

    latest_point = derived["points"][-1] if derived["points"] else None
    status = {
        "origin": origin,
        "collector": collector,
        "pulse_cycle": pulse_cycle,
        "endpoints": endpoints,
        "capacity": latest_point["capacity"] if latest_point else None,
        "rates": latest_point["rates"] if latest_point else None,
    }
    coverage = {
        "collector": {
            "from": derived["collection_started"],
            "to": derived["collection_ended"],
            "accepted_ticks": derived["accepted_ticks"],
            "rejected_ticks": derived["rejected_ticks"],
        },
        "telemetry": endpoint_coverage,
        "attempt_input": telemetry["attempt_input"],
    }
    return status, source_observed_at, coverage


def incident_id(rule: str, route: str | None, opened_at: str) -> str:
    material = f"{rule}|{route or ''}|{opened_at}".encode()
    return hashlib.sha256(material).hexdigest()[:16]


def health_incidents(
    attempts: list[dict[str, Any]],
    carry: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    incidents: list[dict[str, Any]] = []
    active: dict[str, Any] | None = None
    if carry is not None:
        active = {
            "id": incident_id("health_probe_failed", "/healthz", carry["opened_at"]),
            "rule": "health_probe_failed",
            "title": "Observed health probe failures",
            "description": (
                "One or more /healthz observations were unsuccessful. "
                "This record is limited to those attempts and their timestamps."
            ),
            "route": "/healthz",
            "state": "open",
            "opened_at": carry["opened_at"],
            "last_observed_at": carry["last_observed_at"],
            "resolved_at": None,
            "attempts": carry["attempts"],
            "observed_failures": carry["observed_failures"],
            "methodology_version": INCIDENT_RULES_VERSION,
        }
    for attempt in attempts:
        if attempt["route"] != "/healthz":
            continue
        if not is_success(attempt):
            if active is None:
                active = {
                    "id": incident_id(
                        "health_probe_failed", "/healthz", attempt["observed_at"]
                    ),
                    "rule": "health_probe_failed",
                    "title": "Observed health probe failures",
                    "description": (
                        "One or more /healthz observations were unsuccessful. "
                        "This record is limited to those attempts and their timestamps."
                    ),
                    "route": "/healthz",
                    "state": "open",
                    "opened_at": attempt["observed_at"],
                    "last_observed_at": attempt["observed_at"],
                    "resolved_at": None,
                    "attempts": 0,
                    "observed_failures": 0,
                    "methodology_version": INCIDENT_RULES_VERSION,
                }
            active["attempts"] += 1
            active["observed_failures"] += 1
            active["last_observed_at"] = attempt["observed_at"]
        elif active is not None:
            active["state"] = "resolved"
            active["resolved_at"] = attempt["observed_at"]
            incidents.append(active)
            active = None
    if active is not None:
        incidents.append(active)
    return incidents


def endpoint_5xx_incidents(
    attempts: list[dict[str, Any]],
    carry_by_route: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    incidents: list[dict[str, Any]] = []
    active_by_route: dict[str, dict[str, Any]] = {
        route: {
            "id": incident_id("endpoint_5xx", route, carry["opened_at"]),
            "rule": "endpoint_5xx",
            "title": f"Observed HTTP 5xx responses on {route}",
            "description": (
                "The normalized route returned observed HTTP 5xx responses. "
                "This record is limited to counted attempts within its stated window."
            ),
            "route": route,
            "state": "open",
            "opened_at": carry["opened_at"],
            "last_observed_at": carry["last_observed_at"],
            "resolved_at": None,
            "attempts": carry["attempts"],
            "observed_failures": carry["observed_failures"],
            "methodology_version": INCIDENT_RULES_VERSION,
        }
        for route, carry in (carry_by_route or {}).items()
    }
    for attempt in attempts:
        route = attempt["route"]
        status = attempt["http_status"]
        active = active_by_route.get(route)
        if active is None:
            if status is not None and 500 <= status <= 599:
                active = {
                    "id": incident_id("endpoint_5xx", route, attempt["observed_at"]),
                    "rule": "endpoint_5xx",
                    "title": f"Observed HTTP 5xx responses on {route}",
                    "description": (
                        "The normalized route returned observed HTTP 5xx responses. "
                        "This record is limited to counted attempts within its stated window."
                    ),
                    "route": route,
                    "state": "open",
                    "opened_at": attempt["observed_at"],
                    "last_observed_at": attempt["observed_at"],
                    "resolved_at": None,
                    "attempts": 0,
                    "observed_failures": 0,
                    "methodology_version": INCIDENT_RULES_VERSION,
                }
                active_by_route[route] = active
            else:
                continue
        active["attempts"] += 1
        if is_success(attempt):
            active = active_by_route.pop(route)
            active["state"] = "resolved"
            active["resolved_at"] = attempt["observed_at"]
            incidents.append(active)
        else:
            active["observed_failures"] += 1
            active["last_observed_at"] = attempt["observed_at"]
    incidents.extend(active_by_route.values())
    return incidents


def collector_gap_incidents(
    ticks: Sequence[dict[str, Any]], gap_seconds: float, as_of: str
) -> tuple[list[dict[str, Any]], int]:
    incidents: deque[dict[str, Any]] = deque(maxlen=MAX_INCIDENTS)
    records_derived = 0
    for previous, current in zip(ticks, ticks[1:]):
        elapsed = (current["_datetime"] - previous["_datetime"]).total_seconds()
        if elapsed <= gap_seconds:
            continue
        opened_at = format_utc(previous["_datetime"] + timedelta(seconds=gap_seconds))
        records_derived += 1
        incidents.append(
            {
                "id": incident_id("collector_gap", None, opened_at),
                "rule": "collector_gap",
                "title": "Observed collector cadence gap",
                "description": (
                    "No accepted collector tick was observed within the declared "
                    "cadence; the next tick resolves this observation record."
                ),
                "route": None,
                "state": "resolved",
                "opened_at": opened_at,
                "last_observed_at": current["ts"],
                "resolved_at": current["ts"],
                "attempts": 0,
                "observed_failures": 0,
                "window_seconds": round(elapsed, 3),
                "methodology_version": INCIDENT_RULES_VERSION,
            }
        )
    if ticks:
        latest = ticks[-1]
        elapsed = (parse_utc(as_of) - latest["_datetime"]).total_seconds()
        if elapsed > gap_seconds:
            opened_at = format_utc(latest["_datetime"] + timedelta(seconds=gap_seconds))
            records_derived += 1
            incidents.append(
                {
                    "id": incident_id("collector_gap", None, opened_at),
                    "rule": "collector_gap",
                    "title": "Observed collector cadence gap",
                    "description": (
                        "No accepted collector tick was observed within the declared "
                        "cadence; the next tick resolves this observation record."
                    ),
                    "route": None,
                    "state": "open",
                    "opened_at": opened_at,
                    "last_observed_at": as_of,
                    "resolved_at": None,
                    "attempts": 0,
                    "observed_failures": 0,
                    "window_seconds": round(elapsed, 3),
                    "methodology_version": INCIDENT_RULES_VERSION,
                }
            )
    return list(incidents), records_derived


def incident_resource(
    ticks: Sequence[dict[str, Any]],
    attempts: list[dict[str, Any]],
    gap_seconds: float,
    as_of: str,
    *,
    attempt_carry: dict[str, Any] | None = None,
    attempt_input: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], str | None, dict[str, Any]]:
    carry = attempt_carry or {
        "health_probe_failed": None,
        "endpoint_5xx": {},
    }
    incidents = health_incidents(attempts, carry["health_probe_failed"])
    incidents.extend(endpoint_5xx_incidents(attempts, carry["endpoint_5xx"]))
    attempt_incident_count = len(incidents)
    gap_incidents, gap_incident_count = collector_gap_incidents(
        ticks, gap_seconds, as_of
    )
    incidents.extend(gap_incidents)
    incidents.sort(
        key=lambda item: (parse_utc(item["opened_at"]), item["id"]), reverse=True
    )
    bounded = incidents[:MAX_INCIDENTS]
    source_observed_at = latest_time(
        (
            ticks[-1]["ts"] if ticks else None,
            attempts[-1]["observed_at"] if attempts else None,
        )
    )
    coverage = {
        "incident_rules_version": INCIDENT_RULES_VERSION,
        "records_returned": len(bounded),
        "records_derived": attempt_incident_count + gap_incident_count,
        "truncated": attempt_incident_count + gap_incident_count > len(bounded),
        "tick_input": {
            "records_total": len(ticks),
            "records_loaded": len(ticks),
            "records_omitted_before_cutoff": 0,
            "cutoff_observed_at": None,
            "truncated": False,
            "continuity": "complete",
        },
    }
    if attempt_input is not None:
        coverage["attempt_input"] = attempt_input
    return bounded, source_observed_at, coverage


def flatten_fields(
    value: dict[str, Any], prefix: str = ""
) -> dict[str, str | int | float | bool | None | list[Any]]:
    flattened: dict[str, str | int | float | bool | None | list[Any]] = {}
    for key in sorted(value):
        item = value[key]
        dotted = f"{prefix}.{key}" if prefix else key
        if isinstance(item, dict):
            flattened.update(flatten_fields(item, dotted))
        elif item is None or isinstance(item, (str, int, float, bool, list)):
            flattened[dotted] = item
    return flattened


def affects_interpretation(route: str, field: str) -> bool:
    if route == "/config":
        return field.startswith(
            (
                "settings.dupe_",
                "settings.ephemeral_ttl_",
                "settings.max_",
                "settings.rate_",
                "settings.static_cache_",
                "settings.wait_",
                "service",
                "version",
            )
        )
    return field.startswith(
        (
            "capabilities",
            "endpoints.",
            "identity.",
            "limits.",
            "protocol",
            "protocols",
            "schema_version",
            "trust.",
            "version",
        )
    )


def parse_discovery_fields(value: Any, label: str) -> dict[str, Any]:
    try:
        fields = json.loads(value)
    except (TypeError, ValueError, RecursionError) as error:
        raise ValueError(f"{label} fields_json must contain valid JSON") from error
    if not isinstance(fields, dict):
        raise ValueError(f"{label} fields_json must contain an object")
    try:
        json_bytes(fields)
    except (TypeError, ValueError, RecursionError) as error:
        raise ValueError(f"{label} fields_json must contain valid JSON") from error
    return fields


def change_resource(
    discoveries: list[dict[str, Any]],
    *,
    attempts: list[dict[str, Any]] | None = None,
    predecessors: dict[str, dict[str, Any]] | None = None,
    discovery_input: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], str | None, dict[str, Any]]:
    previous_by_route: dict[
        str, dict[str, str | int | float | bool | None | list[Any]]
    ] = {}
    for route, predecessor in (predecessors or {}).items():
        fields = parse_discovery_fields(
            predecessor["fields_json"], "discovery predecessor"
        )
        previous_by_route[route] = flatten_fields(fields)
    changes: list[dict[str, Any]] = []
    accepted_snapshots = 0
    for snapshot in discoveries:
        fields = parse_discovery_fields(snapshot["fields_json"], "discovery")
        current = flatten_fields(fields)
        route = snapshot["route"]
        previous = previous_by_route.get(route)
        if previous is not None:
            for field in sorted(set(previous) | set(current)):
                old = previous.get(field, MISSING)
                new = current.get(field, MISSING)
                if old is new or (
                    old is not MISSING
                    and new is not MISSING
                    and json_bytes({"v": old}) == json_bytes({"v": new})
                ):
                    continue
                changes.append(
                    {
                        "field": field,
                        "old": FIELD_ABSENT.copy() if old is MISSING else old,
                        "new": FIELD_ABSENT.copy() if new is MISSING else new,
                        "first_observed_at": snapshot["observed_at"],
                        "source_route": route,
                        "interpretation_affected": affects_interpretation(route, field),
                        "methodology_version": INCIDENT_RULES_VERSION,
                    }
                )
        previous_by_route[route] = current
        accepted_snapshots += 1
    changes.sort(
        key=lambda item: (
            parse_utc(item["first_observed_at"]),
            item["source_route"],
            item["field"],
        ),
        reverse=True,
    )
    bounded = changes[:MAX_CHANGES]
    last_change_observed_at = discoveries[-1]["observed_at"] if discoveries else None
    source_observed_at = (
        latest_time(
            attempt["observed_at"]
            for attempt in attempts or ()
            if attempt["route"] in ("/config", "/.well-known/agent.json")
            and is_success(attempt)
        )
        or last_change_observed_at
    )
    coverage = {
        "discovery_snapshots": accepted_snapshots,
        "last_change_observed_at": last_change_observed_at,
        "records_returned": len(bounded),
        "records_derived": len(changes),
        "truncated": len(changes) > len(bounded),
    }
    if discovery_input is not None:
        coverage["discovery_input"] = discovery_input
    return bounded, source_observed_at, coverage


def methodology_resource() -> dict[str, Any]:
    return {
        "rules_version": INCIDENT_RULES_VERSION,
        "history_boundary": (
            "Repository-backed methodology history begins at 1.3.0; absent version "
            "numbers and earlier revision details are not inferred."
        ),
        "change_history": [
            {
                "version": "1.15.0",
                "published_on": "2026-09-01",
                "changes": [
                    "Separated checks attempted after their eligibility window "
                    "closed into a distinct attempted_late state, and began "
                    "finalizing never-attempted aged-out checks as terminal "
                    "records in bounded per-tick batches, publishing the count "
                    "finalized each tick and the backlog still remaining."
                ],
                "limitations": [
                    "Ticks recorded before collector 2.13.0 include late "
                    "attempts in their aged_out_unselected counts and publish "
                    "attempted_late as not recorded; their historical meaning "
                    "is not rewritten.",
                    "Finalization is terminal bookkeeping that writes no "
                    "attempt evidence; a check finalized as aged out stays "
                    "aged out even if supersession evidence for its window is "
                    "observed later.",
                    "A recorded origin read now outranks supersession "
                    "evidence, so from collector 2.13.0 a late-read check "
                    "whose name was recreated before the check fell due is "
                    "counted as an eligible late attempt rather than as "
                    "ineligible and superseded before due. This shifts the "
                    "split between those two counts at the version boundary; "
                    "earlier ticks keep their original split.",
                ],
            },
            {
                "version": "1.14.0",
                "published_on": "2026-09-01",
                "changes": [
                    "Published how scheduled room checks are sampled: the "
                    "deterministic selection descriptor and its read budget, "
                    "per-stage coverage as completed checks over eligible "
                    "rooms, and the count of eligible checks that aged out "
                    "without a timely attempt."
                ],
                "limitations": [
                    "Per-stage coverage counts every scheduled check whose "
                    "window has opened since the ledger began, while the "
                    "selection and read-budget figures beside it describe only "
                    "the tick being reported; the two are never summed or "
                    "compared.",
                    "The aged-out count includes checks that were read after "
                    "their eligibility window closed, so it is not a count of "
                    "checks that were never attempted.",
                    "The second-message denominator counts only checks that "
                    "found the room present; a check that found it absent "
                    "observed no room that could carry a second message.",
                ],
            },
            {
                "version": "1.13.0",
                "published_on": "2026-08-31",
                "changes": [
                    "Separated recreated room names into creation generations and "
                    "marked older scheduled checks as superseded without reading "
                    "the origin."
                ],
                "limitations": [
                    "Creation generations begin only where the bounded events "
                    "window was locally observed; missing earlier generations are "
                    "not reconstructed.",
                    "Superseded means a newer creation event was observed for the "
                    "same name, not that either generation was deleted or inactive.",
                ],
            },
            {
                "version": "1.12.0",
                "published_on": "2026-08-30",
                "changes": [
                    "Adopted query-gated room-name disclosure: substring search "
                    "requires at least three characters; shorter queries use exact, "
                    "case-sensitive name matching; responses contain at most 20 "
                    "results with no default listing or pagination."
                ],
                "limitations": [
                    "Query gating reduces amplification of attacker-chosen public "
                    "names; it does not make those names confidential.",
                    "The result cap bounds one response, but repeated permitted "
                    "queries can reveal additional matching public names.",
                ],
            },
            {
                "version": "1.11.0",
                "published_on": "2026-08-30",
                "changes": ["Defined identity-census partial-failure semantics."],
                "limitations": [],
            },
            {
                "version": "1.10.0",
                "published_on": "2026-08-30",
                "changes": ["Added bounded forward room-lifecycle revisits."],
                "limitations": [],
            },
            {
                "version": "1.8.0",
                "published_on": "2026-08-30",
                "changes": [
                    "Added bounded publication history and the tick hash chain."
                ],
                "limitations": [],
            },
            {
                "version": "1.7.0",
                "published_on": "2026-08-30",
                "changes": ["Defined census-walk and alternation semantics."],
                "limitations": [],
            },
            {
                "version": "1.6.0",
                "published_on": "2026-08-30",
                "changes": ["Defined fail-closed cap-release behavior."],
                "limitations": [],
            },
            {
                "version": "1.5.0",
                "published_on": "2026-08-30",
                "changes": ["Added SQLite DID evidence and cap release."],
                "limitations": [],
            },
            {
                "version": "1.4.0",
                "published_on": "2026-08-30",
                "changes": [
                    "Corrected the published methodology to match the implementation."
                ],
                "limitations": [],
            },
            {
                "version": "1.3.0",
                "published_on": "2026-08-30",
                "changes": ["Established the initial Observatory methodology."],
                "limitations": [],
            },
        ],
        "incident_rules": [
            {
                "id": "health_probe_failed",
                "definition": (
                    "A contiguous interval of unsuccessful /healthz observations, "
                    "resolved by the next successful observation."
                ),
            },
            {
                "id": "endpoint_5xx",
                "definition": (
                    "A contiguous interval of observed HTTP 5xx attempts on one "
                    "normalized route, resolved by the next successful attempt."
                ),
            },
            {
                "id": "collector_gap",
                "definition": (
                    "An interval beyond the declared collector cadence, resolved "
                    "when the next accepted tick is observed."
                ),
            },
        ],
        "evidence_classes": [
            "accepted collector tick",
            "normalized request attempt",
            "allowlisted discovery snapshot",
            "versioned derivation",
        ],
        "boundaries": [
            "Incident records report only observed failure intervals.",
            "Unknown and not observed are distinct from zero.",
            "Discovery diffs contain only stored, allowlisted public fields.",
            "Rebuilding frozen input never extends its validity window.",
        ],
    }


def envelope(
    resource_name: str,
    resource: Any,
    *,
    source_observed_at: str | None,
    derived_at: str,
    published_at: str,
    collector_version: str | None,
    methodology_version: str | None,
    schema_version: int | None,
    window: dict[str, Any] | None,
    coverage: dict[str, Any] | None,
    limitations: list[str],
    ledger_chain_head: str | None,
    freshness_state: str | None = None,
) -> dict[str, Any]:
    if parse_utc(derived_at) > parse_utc(published_at):
        raise ValueError("snapshot publication predates derivation")
    if source_observed_at is not None and parse_utc(source_observed_at) > parse_utc(
        derived_at
    ):
        raise ValueError("snapshot source observation postdates derivation")
    validity = valid_until(source_observed_at)
    metadata = common_metadata(
        source_observed_at=source_observed_at,
        valid_until=validity,
        freshness=(
            freshness_state
            if freshness_state is not None
            else freshness(source_observed_at, published_at, validity)
        ),
        collector_version=collector_version,
        methodology_version=methodology_version,
        schema_version=schema_version,
        window=window,
        coverage=coverage,
        limitations=limitations,
        ledger_chain_head=ledger_chain_head,
        generated_at=published_at,
    )
    return {
        **metadata,
        "derived_at": derived_at,
        "published_at": published_at,
        resource_name: resource,
    }


def bounded_representation(
    resource_name: str, payload: dict[str, Any]
) -> dict[str, Any]:
    if (
        len(json_bytes(payload)) <= MAX_RESPONSE_BYTES
        and len(text_bytes(payload)) <= MAX_RESPONSE_BYTES
    ):
        return payload

    if resource_name == "status":
        records = payload["status"]["endpoints"]
    else:
        records = payload[resource_name]
    if not isinstance(records, list):
        raise ValueError(
            f"{resource_name} snapshot exceeds {MAX_RESPONSE_BYTES} bytes and "
            "has no bounded record list"
        )

    bounded = {**payload, "coverage": dict(payload["coverage"])}
    if resource_name == "status":
        bounded["status"] = dict(payload["status"])
        bounded_records = list(records)
        bounded["status"]["endpoints"] = bounded_records
    else:
        bounded_records = list(records)
        bounded[resource_name] = bounded_records

    original_count = len(bounded_records)
    bounded["coverage"].update(
        {
            "response_capped": True,
            "response_byte_limit": MAX_RESPONSE_BYTES,
            "records_omitted_by_response_limit": 0,
            "truncated": True,
        }
    )
    while (
        len(json_bytes(bounded)) > MAX_RESPONSE_BYTES
        or len(text_bytes(bounded)) > MAX_RESPONSE_BYTES
    ):
        if not bounded_records:
            raise ValueError(
                f"{resource_name} snapshot metadata exceeds {MAX_RESPONSE_BYTES} bytes"
            )
        bounded_records.pop()
        bounded["coverage"]["records_omitted_by_response_limit"] = original_count - len(
            bounded_records
        )
    bounded["coverage"]["records_returned"] = len(bounded_records)
    return bounded


def build_snapshots_from_records(
    ticks: Sequence[dict[str, Any]],
    rejected_ticks: int,
    telemetry_path: str | Path,
    *,
    derived_at: str | None = None,
    published_at: str | None = None,
    gap_seconds: float = 300.0,
) -> dict[str, dict[str, Any]]:
    derived = derive.derive_records(ticks, rejected_ticks, gap_seconds)
    telemetry = load_telemetry(Path(telemetry_path))
    derived_time = canonical_time(derived_at)
    published_time = canonical_time(published_at or derived_time)

    collector_version = derived["collector_version"]
    methodology_version = derived["methodology_version"]
    schema_version = derived["schema"]
    ledger_chain_head = derived["ledger_chain"]["tip_tick_sha256"]

    status, status_source, status_coverage = status_resource(
        ticks, derived, telemetry, published_time
    )
    incidents, incident_source, incident_coverage = incident_resource(
        ticks,
        telemetry["attempts"],
        gap_seconds,
        published_time,
        attempt_carry=telemetry["attempt_carry"],
        attempt_input=telemetry["attempt_input"],
    )
    changes, changes_source, changes_coverage = change_resource(
        telemetry["discoveries"],
        attempts=telemetry["attempts"],
        predecessors=telemetry["discovery_predecessors"],
        discovery_input=telemetry["discovery_input"],
    )

    common = {
        "derived_at": derived_time,
        "published_at": published_time,
        "collector_version": collector_version,
        "methodology_version": methodology_version,
        "schema_version": schema_version,
    }
    snapshots = {
        "status": envelope(
            "status",
            status,
            source_observed_at=status_source,
            window={
                "kind": "trailing",
                "seconds": int(STATUS_WINDOW.total_seconds()),
            },
            coverage=status_coverage,
            limitations=[
                "Reachability is an observed probe result, not an availability promise.",
                "Endpoint counts include only locally stored normalized attempts.",
                "No composite health score is produced.",
            ],
            ledger_chain_head=ledger_chain_head,
            **common,
        ),
        "incidents": envelope(
            "incidents",
            incidents,
            source_observed_at=incident_source,
            window={"kind": "bounded_history", "maximum_records": MAX_INCIDENTS},
            coverage=incident_coverage,
            limitations=[
                "Incidents are versioned records of observed attempts under declared rules.",
                "History begins when telemetry collection begins; it is not backfilled.",
            ],
            ledger_chain_head=ledger_chain_head,
            **common,
        ),
        "changes": envelope(
            "changes",
            changes,
            source_observed_at=changes_source,
            window={"kind": "bounded_history", "maximum_records": MAX_CHANGES},
            coverage=changes_coverage,
            limitations=[
                "Only stored, allowlisted public discovery fields are compared.",
                "An observed change does not explain why the upstream value changed.",
            ],
            ledger_chain_head=None,
            **common,
        ),
        "methodology": envelope(
            "methodology",
            methodology_resource(),
            source_observed_at=None,
            window=None,
            coverage={"incident_rules_version": INCIDENT_RULES_VERSION},
            limitations=[
                "Definitions describe the published derivation contract, not origin behavior."
            ],
            ledger_chain_head=None,
            freshness_state="not_applicable",
            **common,
        ),
    }
    return {
        name: bounded_representation(name, payload)
        for name, payload in snapshots.items()
    }


def build_snapshots(
    ticks_path: str | Path,
    telemetry_path: str | Path,
    *,
    derived_at: str | None = None,
    published_at: str | None = None,
    gap_seconds: float = 300.0,
) -> dict[str, dict[str, Any]]:
    ticks, rejected_ticks = load_ticks(Path(ticks_path))
    return build_snapshots_from_records(
        tuple(ticks),
        rejected_ticks,
        telemetry_path,
        derived_at=derived_at,
        published_at=published_at,
        gap_seconds=gap_seconds,
    )


def write_snapshot_artifacts(
    snapshots: dict[str, dict[str, Any]], root: str | Path
) -> None:
    api_root = Path(root) / "api" / "v1"
    api_root.mkdir(parents=True, exist_ok=True)
    for name in ("status", "incidents", "changes", "methodology"):
        payload = snapshots[name]
        json_payload = json_bytes(payload)
        text_payload = text_bytes(payload)
        if max(len(json_payload), len(text_payload)) > MAX_RESPONSE_BYTES:
            raise ValueError(f"{name} snapshot exceeds {MAX_RESPONSE_BYTES} bytes")
        (api_root / f"{name}.json").write_bytes(json_payload)
        (api_root / f"{name}.txt").write_bytes(text_payload)
        (api_root / name).write_bytes(text_payload)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build bounded static Observatory snapshots"
    )
    parser.add_argument("ticks", type=Path)
    parser.add_argument("telemetry", type=Path)
    parser.add_argument("output", type=Path)
    arguments = parser.parse_args()
    snapshots = build_snapshots(arguments.ticks, arguments.telemetry)
    write_snapshot_artifacts(snapshots, arguments.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
