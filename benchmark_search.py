#!/usr/bin/env python3
"""Run the opt-in one-year COMPASS search performance gate."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sqlite3
import tempfile
import time
from pathlib import Path

from query_service import (
    ROOM_ID_LOOKUP_SQL,
    SUBSTRING_SEARCH_SQL,
    QueryApplication,
    ServiceConfig,
    fts_phrase,
    open_readonly_database,
)

ONE_YEAR_ROOM_COUNT = 5_110_000
COLD_LIMIT_SECONDS = 0.500
P95_LIMIT_SECONDS = 0.250
MARKER_COUNT = 100
BATCH_SIZE = 25_000
TIMESTAMP = "2026-08-30T08:00:00Z"
OLD_GENERATION_TIMESTAMP = "2026-08-29T08:00:00Z"
NEWEST_GENERATION_START = ONE_YEAR_ROOM_COUNT - MARKER_COUNT


def synthetic_name(index: int) -> str:
    if index < MARKER_COUNT:
        return f"benchmark-Needle-{index:03d}-target"
    if index >= NEWEST_GENERATION_START:
        marker_index = index - NEWEST_GENERATION_START
        return f"benchmark-Needle-{marker_index:03d}-target"
    return f"r{index:07x}"


def create_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        PRAGMA journal_mode = OFF;
        PRAGMA synchronous = OFF;
        PRAGMA temp_store = MEMORY;
        PRAGMA locking_mode = EXCLUSIVE;
        PRAGMA cache_size = -262144;
        PRAGMA foreign_keys = ON;

        CREATE TABLE room_ledger (
            created_seq INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            room_id TEXT NOT NULL,
            room_sha256 TEXT NOT NULL,
            created_at TEXT NOT NULL,
            first_observed_at TEXT NOT NULL,
            last_listed_at TEXT
        );

        CREATE TABLE signer_metadata (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            state_json TEXT NOT NULL
        );

        INSERT INTO signer_metadata (singleton, state_json)
        VALUES (
            1,
            '{"version":6,"latest_room_listing_observed_at":"2026-08-30T08:00:00Z"}'
        );

        CREATE TABLE room_revisits (
            room_created_seq INTEGER NOT NULL,
            stage_seconds INTEGER NOT NULL,
            due_at TEXT NOT NULL,
            attempted_at TEXT,
            success INTEGER,
            message_count INTEGER,
            has_second_message INTEGER,
            second_sender_class TEXT,
            outcome TEXT,
            PRIMARY KEY (room_created_seq, stage_seconds),
            FOREIGN KEY (room_created_seq) REFERENCES room_ledger(created_seq)
        );
        """
    )


def room_rows(start: int, stop: int):
    for index in range(start, stop):
        name = synthetic_name(index)
        digest = hashlib.sha256(name.encode("utf-8")).hexdigest()
        observed_at = OLD_GENERATION_TIMESTAMP if index < MARKER_COUNT else TIMESTAMP
        yield (
            index,
            name,
            digest[:16],
            digest,
            observed_at,
            observed_at,
            None,
        )


def validate_marker_generations(connection: sqlite3.Connection) -> None:
    for marker_index in range(MARKER_COUNT):
        name = synthetic_name(marker_index)
        rows = connection.execute(
            """
            SELECT created_seq, created_at
            FROM room_ledger
            WHERE name = ? COLLATE BINARY
            ORDER BY created_seq
            """,
            (name,),
        ).fetchall()
        expected = [
            (marker_index, OLD_GENERATION_TIMESTAMP),
            (NEWEST_GENERATION_START + marker_index, TIMESTAMP),
        ]
        if rows != expected:
            raise AssertionError(
                f"benchmark marker {name!r} has unexpected generations: {rows}"
            )


def build_corpus(path: Path) -> float:
    started = time.perf_counter()
    connection = sqlite3.connect(path)
    try:
        create_schema(connection)
        for start in range(0, ONE_YEAR_ROOM_COUNT, BATCH_SIZE):
            stop = min(ONE_YEAR_ROOM_COUNT, start + BATCH_SIZE)
            connection.executemany(
                """
                INSERT INTO room_ledger (
                    created_seq,
                    name,
                    room_id,
                    room_sha256,
                    created_at,
                    first_observed_at,
                    last_listed_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                room_rows(start, stop),
            )
            connection.commit()
        connection.executescript(
            """
            CREATE INDEX room_ledger_room_id
            ON room_ledger (room_id, room_sha256, created_seq DESC);

            CREATE INDEX room_ledger_name
            ON room_ledger (name, created_seq DESC);

            CREATE VIRTUAL TABLE room_search USING fts5(
                name,
                content='room_ledger',
                content_rowid='rowid',
                tokenize='trigram case_sensitive 1'
            );

            INSERT INTO room_search(room_search) VALUES ('rebuild');

            CREATE TRIGGER room_ledger_ai AFTER INSERT ON room_ledger BEGIN
                INSERT INTO room_search(rowid, name) VALUES (new.rowid, new.name);
            END;

            CREATE TRIGGER room_ledger_ad AFTER DELETE ON room_ledger BEGIN
                INSERT INTO room_search(room_search, rowid, name)
                VALUES ('delete', old.rowid, old.name);
            END;

            CREATE TRIGGER room_ledger_au AFTER UPDATE OF name ON room_ledger BEGIN
                INSERT INTO room_search(room_search, rowid, name)
                VALUES ('delete', old.rowid, old.name);
                INSERT INTO room_search(rowid, name) VALUES (new.rowid, new.name);
            END;

            PRAGMA user_version = 6;
            ANALYZE;
            """
        )
        count = connection.execute("SELECT COUNT(*) FROM room_ledger").fetchone()[0]
        if count != ONE_YEAR_ROOM_COUNT:
            raise AssertionError(
                f"benchmark corpus has {count} rows, expected {ONE_YEAR_ROOM_COUNT}"
            )
        validate_marker_generations(connection)
    finally:
        connection.close()
    return time.perf_counter() - started


def timed_search(
    application: QueryApplication,
    query: str,
    *,
    expected_count: int = 1,
    expected_capped: bool = False,
    expected_created_at: str | None = None,
) -> float:
    started = time.perf_counter()
    response = application.room_search_api(
        {"q": query, "limit": "10", "format": "json"}
    )
    elapsed = time.perf_counter() - started
    payload = json.loads(response.body)
    if (
        response.status != 200
        or len(payload["results"]) != expected_count
        or payload["capped"] is not expected_capped
        or payload["match_mode"] != "case_sensitive_substring"
        or (
            expected_created_at is not None
            and (
                len(payload["results"]) != 1
                or payload["results"][0]["created_at"] != expected_created_at
            )
        )
    ):
        raise AssertionError(f"benchmark query {query!r} returned an unexpected result")
    return elapsed


def assert_query_plan(connection: sqlite3.Connection, query: str) -> list[str]:
    plan = connection.execute(
        "EXPLAIN QUERY PLAN " + SUBSTRING_SEARCH_SQL,
        (fts_phrase(query), query, 11),
    ).fetchall()
    details = [str(row[3]) for row in plan]
    if any("SCAN room_ledger" in detail for detail in details):
        raise AssertionError(f"search plan scans room_ledger: {details}")
    if not any("room_ledger_name" in detail for detail in details):
        raise AssertionError(f"search plan skips the generation index: {details}")
    return details


def assert_room_id_query_plan(
    connection: sqlite3.Connection, identifier: str
) -> list[str]:
    plan = connection.execute(
        "EXPLAIN QUERY PLAN " + ROOM_ID_LOOKUP_SQL,
        (identifier,),
    ).fetchall()
    details = [str(row[3]) for row in plan]
    if any("SCAN room_ledger" in detail for detail in details):
        raise AssertionError(f"room ID plan scans room_ledger: {details}")
    if not any("room_ledger_room_id" in detail for detail in details):
        raise AssertionError(f"room ID plan skips the identity index: {details}")
    return details


def percentile_95(values: list[float]) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(0.95 * len(ordered)) - 1)]


def run_benchmark(iterations: int) -> dict[str, object]:
    if iterations < 20:
        raise ValueError("iterations must be at least 20")
    with tempfile.TemporaryDirectory(
        prefix="technocore-search-benchmark-"
    ) as temporary:
        database_path = Path(temporary) / "one-year.sqlite3"
        build_seconds = build_corpus(database_path)
        application = QueryApplication(
            ServiceConfig(
                database_path=database_path,
                snapshot_root=Path(temporary),
                query_timeout_seconds=COLD_LIMIT_SECONDS,
            )
        )

        cold_connection = open_readonly_database(
            database_path, query_timeout_seconds=COLD_LIMIT_SECONDS
        )
        try:
            cold_seconds = timed_search(
                application,
                "r00",
                expected_count=10,
                expected_capped=True,
            )
            substring_plan = assert_query_plan(cold_connection, "r00")
            first_room_id = hashlib.sha256(
                synthetic_name(0).encode("utf-8")
            ).hexdigest()[:16]
            room_id_plan = assert_room_id_query_plan(cold_connection, first_room_id)
        finally:
            cold_connection.close()
        if cold_seconds > COLD_LIMIT_SECONDS:
            raise AssertionError(
                f"cold search took {cold_seconds * 1000:.2f}ms, limit is 500ms"
            )

        adversarial_seconds = timed_search(
            application,
            "R00",
            expected_count=0,
        )
        if adversarial_seconds > COLD_LIMIT_SECONDS:
            raise AssertionError(
                "high-fanout zero-result search took "
                f"{adversarial_seconds * 1000:.2f}ms, limit is 500ms"
            )

        timed_search(
            application,
            "r00",
            expected_count=10,
            expected_capped=True,
        )
        samples = [
            (
                timed_search(
                    application,
                    "r00",
                    expected_count=10,
                    expected_capped=True,
                )
                if index % 3 == 0
                else (
                    timed_search(
                        application,
                        f"Needle-{index % MARKER_COUNT:03d}",
                        expected_created_at=TIMESTAMP,
                    )
                    if index % 3 == 1
                    else timed_search(application, "R00", expected_count=0)
                )
            )
            for index in range(iterations)
        ]
        p95_seconds = percentile_95(samples)
        if p95_seconds > P95_LIMIT_SECONDS:
            raise AssertionError(
                f"warm p95 search took {p95_seconds * 1000:.2f}ms, limit is 250ms"
            )
        return {
            "rows": ONE_YEAR_ROOM_COUNT,
            "generation_history_names": MARKER_COUNT,
            "build_seconds": round(build_seconds, 3),
            "cold_ms": round(cold_seconds * 1000, 3),
            "high_fanout_zero_result_ms": round(adversarial_seconds * 1000, 3),
            "warm_p95_ms": round(p95_seconds * 1000, 3),
            "iterations": iterations,
            "query_plans": {
                "substring": substring_plan,
                "room_id": room_id_plan,
            },
        }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--iterations", type=int, default=100)
    args = parser.parse_args()
    print(json.dumps(run_benchmark(args.iterations), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
