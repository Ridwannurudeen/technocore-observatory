#!/usr/bin/env python3
"""Independent request-attempt telemetry for the observatory collectors."""

from __future__ import annotations

import json
import sqlite3
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

TELEMETRY_SCHEMA_VERSION = 1
RAW_RETENTION_SECONDS = 90 * 24 * 60 * 60
PRUNE_BATCH_SIZE = 1_000
MAX_DATABASE_BYTES = 128 * 1024 * 1024
MAX_JOURNAL_BYTES = 8 * 1024 * 1024
ROUTES = (
    "/healthz",
    "/config",
    "/.well-known/agent.json",
    "/rooms",
    "/r/{room}",
    "/r/events",
    "/kv/{namespace}",
    "/other",
)
UNMETERED_ROUTES = frozenset(("/healthz", "/config", "/.well-known/agent.json"))
ATTEMPT_OUTCOMES = frozenset(
    (
        "success",
        "http_error",
        "timeout",
        "transport_error",
        "decode_error",
        "empty_response",
        "invalid_response",
    )
)


def utc_now() -> str:
    return (
        datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    )


def parse_timestamp(value: str) -> datetime:
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        raise ValueError("timestamp has no timezone")
    return parsed.astimezone(timezone.utc)


def normalize_route(path: str) -> tuple[str, bool]:
    parsed_path = urllib.parse.urlsplit(path).path
    if parsed_path in UNMETERED_ROUTES:
        return parsed_path, False
    if parsed_path == "/rooms":
        return "/rooms", True
    if parsed_path == "/r/events":
        return "/r/events", True
    if parsed_path.startswith("/r/"):
        return "/r/{room}", True
    if parsed_path.startswith("/kv/"):
        return "/kv/{namespace}", True
    return "/other", True


class TelemetryStore:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.connection = sqlite3.connect(self.path, timeout=5.0)
        try:
            if self.connection.execute("PRAGMA user_version").fetchone()[0] == 0:
                self.connection.execute("PRAGMA auto_vacuum = INCREMENTAL")
            self.connection.execute("PRAGMA journal_mode = DELETE")
            self.connection.execute("PRAGMA synchronous = FULL")
            self.connection.execute("PRAGMA foreign_keys = ON")
            page_size = self.connection.execute("PRAGMA page_size").fetchone()[0]
            maximum_pages = max(1, MAX_DATABASE_BYTES // page_size)
            configured_pages = self.connection.execute(
                f"PRAGMA max_page_count = {maximum_pages}"
            ).fetchone()[0]
            if configured_pages * page_size > MAX_DATABASE_BYTES:
                raise sqlite3.DatabaseError(
                    "telemetry database already exceeds its disk budget"
                )
            self.connection.execute(f"PRAGMA journal_size_limit = {MAX_JOURNAL_BYTES}")
            self._initialize()
        except Exception:
            self.connection.close()
            raise

    def _initialize(self) -> None:
        version = self.connection.execute("PRAGMA user_version").fetchone()[0]
        if version not in (0, TELEMETRY_SCHEMA_VERSION):
            raise sqlite3.DatabaseError(
                f"telemetry database has unsupported schema version {version}"
            )
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS cycles (
                id INTEGER PRIMARY KEY,
                source TEXT NOT NULL CHECK (source IN ('collector', 'pulse_probe')),
                started_at TEXT NOT NULL CHECK (
                    length(started_at) >= 20
                    AND substr(started_at, 11, 1) = 'T'
                    AND substr(started_at, -1, 1) = 'Z'
                ),
                finished_at TEXT CHECK (
                    finished_at IS NULL
                    OR (
                        length(finished_at) >= 20
                        AND substr(finished_at, 11, 1) = 'T'
                        AND substr(finished_at, -1, 1) = 'Z'
                    )
                ),
                outcome TEXT NOT NULL CHECK (
                    outcome IN ('running', 'success', 'failure')
                ),
                error_outcome TEXT CHECK (
                    error_outcome IS NULL
                    OR error_outcome IN (
                        'http_error',
                        'timeout',
                        'transport_error',
                        'decode_error',
                        'empty_response',
                        'invalid_response',
                        'deadline',
                        'collection_error',
                        'storage_error',
                        'os_error'
                    )
                ),
                CHECK (
                    (outcome = 'running' AND finished_at IS NULL AND error_outcome IS NULL)
                    OR (outcome = 'success' AND finished_at IS NOT NULL AND error_outcome IS NULL)
                    OR (outcome = 'failure' AND finished_at IS NOT NULL AND error_outcome IS NOT NULL)
                )
            ) STRICT;

            CREATE TABLE IF NOT EXISTS request_attempts (
                id INTEGER PRIMARY KEY,
                cycle_id INTEGER NOT NULL REFERENCES cycles(id),
                route TEXT NOT NULL CHECK (
                    route IN (
                        '/healthz',
                        '/config',
                        '/.well-known/agent.json',
                        '/rooms',
                        '/r/{room}',
                        '/r/events',
                        '/kv/{namespace}',
                        '/other'
                    )
                ),
                metered INTEGER NOT NULL CHECK (metered IN (0, 1)),
                attempt INTEGER NOT NULL CHECK (attempt >= 1),
                observed_at TEXT NOT NULL CHECK (
                    length(observed_at) >= 20
                    AND substr(observed_at, 11, 1) = 'T'
                    AND substr(observed_at, -1, 1) = 'Z'
                ),
                latency_ms REAL NOT NULL CHECK (latency_ms >= 0),
                outcome TEXT NOT NULL CHECK (
                    outcome IN (
                        'success',
                        'http_error',
                        'timeout',
                        'transport_error',
                        'decode_error',
                        'empty_response',
                        'invalid_response'
                    )
                ),
                http_status INTEGER CHECK (
                    http_status IS NULL OR http_status BETWEEN 100 AND 599
                ),
                CHECK (
                    (route IN ('/healthz', '/config', '/.well-known/agent.json') AND metered = 0)
                    OR (route NOT IN ('/healthz', '/config', '/.well-known/agent.json') AND metered = 1)
                ),
                CHECK (
                    (outcome = 'success' AND http_status BETWEEN 200 AND 399)
                    OR (outcome = 'http_error' AND http_status BETWEEN 400 AND 599)
                    OR (outcome IN ('timeout', 'transport_error') AND http_status IS NULL)
                    OR (
                        outcome IN ('decode_error', 'empty_response', 'invalid_response')
                        AND http_status BETWEEN 200 AND 399
                    )
                )
            ) STRICT;

            CREATE INDEX IF NOT EXISTS request_attempts_cycle
            ON request_attempts (cycle_id);

            CREATE INDEX IF NOT EXISTS request_attempts_route_observed
            ON request_attempts (route, observed_at);

            CREATE INDEX IF NOT EXISTS request_attempts_observed
            ON request_attempts (observed_at);

            CREATE TABLE IF NOT EXISTS discovery_snapshots (
                id INTEGER PRIMARY KEY,
                attempt_id INTEGER NOT NULL UNIQUE REFERENCES request_attempts(id),
                route TEXT NOT NULL CHECK (
                    route IN ('/config', '/.well-known/agent.json')
                ),
                observed_at TEXT NOT NULL CHECK (
                    length(observed_at) >= 20
                    AND substr(observed_at, 11, 1) = 'T'
                    AND substr(observed_at, -1, 1) = 'Z'
                ),
                digest_sha256 TEXT NOT NULL CHECK (
                    length(digest_sha256) = 64
                    AND digest_sha256 NOT GLOB '*[^0-9a-f]*'
                ),
                fields_json TEXT NOT NULL CHECK (
                    json_valid(fields_json)
                    AND json_type(fields_json) = 'object'
                )
            ) STRICT;

            CREATE INDEX IF NOT EXISTS discovery_snapshots_route_observed
            ON discovery_snapshots (route, observed_at);

            CREATE INDEX IF NOT EXISTS cycles_finished
            ON cycles (finished_at);
            """
        )
        self.connection.execute(f"PRAGMA user_version = {TELEMETRY_SCHEMA_VERSION}")
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> TelemetryStore:
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def start_cycle(self, source: str, started_at: str | None = None) -> int:
        started_at = started_at or utc_now()
        self.prune(started_at)
        cursor = self.connection.execute(
            """
            INSERT INTO cycles (source, started_at, outcome)
            VALUES (?, ?, 'running')
            """,
            (source, started_at),
        )
        self.connection.commit()
        return int(cursor.lastrowid)

    def finish_cycle(
        self,
        cycle_id: int,
        outcome: str,
        error_outcome: str | None = None,
        finished_at: str | None = None,
    ) -> None:
        cursor = self.connection.execute(
            """
            UPDATE cycles
            SET finished_at = ?, outcome = ?, error_outcome = ?
            WHERE id = ? AND outcome = 'running'
            """,
            (finished_at or utc_now(), outcome, error_outcome, cycle_id),
        )
        if cursor.rowcount != 1:
            self.connection.rollback()
            raise ValueError("telemetry cycle is missing or already finished")
        self.connection.commit()

    def record_attempt(
        self,
        cycle_id: int,
        path: str,
        metered: bool,
        attempt: int,
        observed_at: str,
        latency_ms: float,
        outcome: str,
        http_status: int | None,
    ) -> int:
        route, expected_metered = normalize_route(path)
        if metered is not expected_metered:
            raise ValueError("telemetry route metering classification is inconsistent")
        cursor = self.connection.execute(
            """
            INSERT INTO request_attempts (
                cycle_id,
                route,
                metered,
                attempt,
                observed_at,
                latency_ms,
                outcome,
                http_status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                cycle_id,
                route,
                int(metered),
                attempt,
                observed_at,
                latency_ms,
                outcome,
                http_status,
            ),
        )
        self.connection.commit()
        return int(cursor.lastrowid)

    def update_attempt_outcome(self, attempt_id: int, outcome: str) -> None:
        if outcome not in ATTEMPT_OUTCOMES:
            raise ValueError("unsupported telemetry attempt outcome")
        cursor = self.connection.execute(
            "UPDATE request_attempts SET outcome = ? WHERE id = ?",
            (outcome, attempt_id),
        )
        if cursor.rowcount != 1:
            self.connection.rollback()
            raise ValueError("telemetry attempt does not exist")
        self.connection.commit()

    def record_discovery_snapshot(
        self,
        attempt_id: int,
        route: str,
        observed_at: str,
        digest_sha256: str,
        fields: dict[str, Any],
    ) -> int | None:
        normalized, metered = normalize_route(route)
        if metered or normalized not in ("/config", "/.well-known/agent.json"):
            raise ValueError("discovery snapshot route is not allowed")
        attempt = self.connection.execute(
            "SELECT route, outcome FROM request_attempts WHERE id = ?",
            (attempt_id,),
        ).fetchone()
        if attempt != (normalized, "success"):
            raise ValueError("discovery snapshot does not match a successful attempt")
        latest = self.connection.execute(
            """
            SELECT digest_sha256
            FROM discovery_snapshots
            WHERE route = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (normalized,),
        ).fetchone()
        if latest is not None and latest[0] == digest_sha256:
            return None
        cursor = self.connection.execute(
            """
            INSERT INTO discovery_snapshots (
                attempt_id, route, observed_at, digest_sha256, fields_json
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                attempt_id,
                normalized,
                observed_at,
                digest_sha256,
                json.dumps(
                    fields,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                ),
            ),
        )
        self.connection.commit()
        return int(cursor.lastrowid)

    def prune(
        self,
        observed_at: str | None = None,
        *,
        batch_size: int = PRUNE_BATCH_SIZE,
    ) -> dict[str, int]:
        if (
            isinstance(batch_size, bool)
            or not isinstance(batch_size, int)
            or not 1 <= batch_size <= PRUNE_BATCH_SIZE
        ):
            raise ValueError(f"batch_size must be between 1 and {PRUNE_BATCH_SIZE}")
        now = parse_timestamp(observed_at or utc_now())
        cutoff = now.timestamp() - RAW_RETENTION_SECONDS
        cutoff_text = (
            datetime.fromtimestamp(cutoff, timezone.utc)
            .isoformat(timespec="seconds")
            .replace("+00:00", "Z")
        )

        duplicate_rows = self.connection.execute(
            """
            WITH ordered AS (
                SELECT
                    id,
                    digest_sha256,
                    LAG(digest_sha256) OVER (
                        PARTITION BY route ORDER BY id
                    ) AS previous_digest
                FROM discovery_snapshots
            )
            SELECT id
            FROM ordered
            WHERE digest_sha256 = previous_digest
            ORDER BY id
            LIMIT ?
            """,
            (batch_size,),
        ).fetchall()
        duplicate_ids = [row[0] for row in duplicate_rows]
        if duplicate_ids:
            placeholders = ",".join("?" for _ in duplicate_ids)
            self.connection.execute(
                f"DELETE FROM discovery_snapshots WHERE id IN ({placeholders})",
                duplicate_ids,
            )

        page_count = self.connection.execute("PRAGMA page_count").fetchone()[0]
        free_pages = self.connection.execute("PRAGMA freelist_count").fetchone()[0]
        max_pages = self.connection.execute("PRAGMA max_page_count").fetchone()[0]
        under_pressure = page_count - free_pages >= int(max_pages * 0.9)
        attempt_rows = self.connection.execute(
            """
            SELECT request_attempts.id
            FROM request_attempts
            LEFT JOIN discovery_snapshots
                ON discovery_snapshots.attempt_id = request_attempts.id
            WHERE
                discovery_snapshots.id IS NULL
                AND (request_attempts.observed_at < ? OR ?)
            ORDER BY request_attempts.observed_at, request_attempts.id
            LIMIT ?
            """,
            (cutoff_text, int(under_pressure), batch_size),
        ).fetchall()
        attempt_ids = [row[0] for row in attempt_rows]
        if attempt_ids:
            placeholders = ",".join("?" for _ in attempt_ids)
            self.connection.execute(
                f"DELETE FROM request_attempts WHERE id IN ({placeholders})",
                attempt_ids,
            )

        cycle_rows = self.connection.execute(
            """
            SELECT cycles.id
            FROM cycles
            LEFT JOIN request_attempts ON request_attempts.cycle_id = cycles.id
            WHERE
                request_attempts.id IS NULL
                AND COALESCE(cycles.finished_at, cycles.started_at) < ?
            ORDER BY cycles.started_at, cycles.id
            LIMIT ?
            """,
            (cutoff_text, batch_size),
        ).fetchall()
        cycle_ids = [row[0] for row in cycle_rows]
        if cycle_ids:
            placeholders = ",".join("?" for _ in cycle_ids)
            self.connection.execute(
                f"DELETE FROM cycles WHERE id IN ({placeholders})",
                cycle_ids,
            )

        self.connection.commit()
        self.connection.execute(f"PRAGMA incremental_vacuum({batch_size})")
        return {
            "discovery_snapshots": len(duplicate_ids),
            "request_attempts": len(attempt_ids),
            "cycles": len(cycle_ids),
        }

    def route_due(self, route: str, observed_at: str, cadence_seconds: int) -> bool:
        normalized, _ = normalize_route(route)
        if normalized != route or cadence_seconds <= 0:
            raise ValueError("invalid telemetry cadence query")
        row = self.connection.execute(
            """
            SELECT MAX(cycles.started_at)
            FROM request_attempts
            JOIN cycles ON cycles.id = request_attempts.cycle_id
            WHERE request_attempts.route = ?
            """,
            (route,),
        ).fetchone()
        if row[0] is None:
            return True
        return (
            parse_timestamp(observed_at) - parse_timestamp(row[0])
        ).total_seconds() >= cadence_seconds
