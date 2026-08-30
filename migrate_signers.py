#!/usr/bin/env python3
"""Migrate the v2 signer JSON store to the v3 SQLite store.

The collector daemon must be stopped and the ``23 */6`` census cron must be
fenced before this program runs. Both processes write signer metadata, so
allowing either writer to run during migration can invalidate the snapshot.

The source JSON is never changed. Move or copy the live v2 file to a rollback
snapshot first, then provide that snapshot and the intended live metadata path:

    python migrate_signers.py /snapshot/signers.json /live/signers.json \
        --cap-saturated-at 2026-08-29T18:00:15Z

The SQLite output is derived from the metadata path in exactly the same way as
the collector: ``signers.json`` produces ``signers.sqlite3``. Existing output
files are refused, making a repeated invocation non-destructive.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path
from typing import Any, TextIO

from collect import (
    SIGNER_STATE_VERSION,
    CollectionError,
    connect_signer_database,
    parse_timestamp,
    save_atomic_json,
    signer_database_path,
    signer_funnel_counts,
    utc_now,
    validate_signer_metadata,
    write_signer_metadata,
)

FUNNEL_NAMES = (
    "observed",
    "two_ticks",
    "two_collection_dates",
    "two_rooms",
    "counterparties",
)
INSERT_SQL = """
    INSERT INTO signer_dids (
        did,
        first_observed_ts,
        last_observed_ts,
        tick_count,
        collection_first_utc_date,
        collection_last_utc_date,
        collection_utc_dates_count,
        rooms_json,
        room_count,
        has_counterparty
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


class StreamingJsonReader:
    def __init__(self, stream: TextIO, chunk_size: int = 64 * 1024) -> None:
        self.stream = stream
        self.chunk_size = chunk_size
        self.buffer = ""
        self.position = 0
        self.eof = False
        self.decoder = json.JSONDecoder()

    def _read_more(self) -> None:
        if self.position:
            self.buffer = self.buffer[self.position :]
            self.position = 0
        chunk = self.stream.read(self.chunk_size)
        if chunk:
            self.buffer += chunk
        else:
            self.eof = True

    def _ensure_character(self) -> bool:
        while self.position >= len(self.buffer) and not self.eof:
            self._read_more()
        return self.position < len(self.buffer)

    def skip_whitespace(self) -> None:
        while True:
            while self.position < len(self.buffer) and self.buffer[self.position].isspace():
                self.position += 1
            if self.position < len(self.buffer) or self.eof:
                return
            self._read_more()

    def peek(self) -> str:
        self.skip_whitespace()
        if not self._ensure_character():
            raise CollectionError("unexpected end of signer JSON")
        return self.buffer[self.position]

    def consume(self, expected: str) -> None:
        actual = self.peek()
        if actual != expected:
            raise CollectionError(
                f"expected {expected!r} in signer JSON, found {actual!r}"
            )
        self.position += 1

    def value(self) -> Any:
        while True:
            self.skip_whitespace()
            try:
                value, end = self.decoder.raw_decode(self.buffer, self.position)
            except json.JSONDecodeError as error:
                if self.eof:
                    raise CollectionError("signer JSON contains an invalid value") from error
                self._read_more()
                continue

            if end == len(self.buffer) and not self.eof:
                self._read_more()
                continue
            self.position = end
            return value

    def finish(self) -> None:
        self.skip_whitespace()
        if self._ensure_character():
            raise CollectionError("signer JSON has trailing content")


def validate_source_record(
    did: Any,
    record: Any,
) -> tuple[Any, ...]:
    if not isinstance(did, str) or not isinstance(record, dict):
        raise CollectionError("signer JSON contains an invalid DID record")

    tick_count = record.get("tick_count")
    collection_dates = record.get("collection_utc_dates_count")
    rooms = record.get("rooms")
    counterparties = record.get("counterparties_count")
    if (
        isinstance(tick_count, bool)
        or not isinstance(tick_count, int)
        or tick_count < 0
        or isinstance(collection_dates, bool)
        or not isinstance(collection_dates, int)
        or collection_dates < 0
        or not isinstance(rooms, list)
        or len(rooms) > 8
        or not all(isinstance(room, str) for room in rooms)
        or rooms != sorted(set(rooms))
        or isinstance(counterparties, bool)
        or counterparties not in (0, 1)
    ):
        raise CollectionError(f"signer JSON contains an invalid record for DID {did}")

    nullable_fields = (
        "first_observed_ts",
        "last_observed_ts",
        "collection_first_utc_date",
        "collection_last_utc_date",
    )
    if any(
        record.get(field) is not None and not isinstance(record.get(field), str)
        for field in nullable_fields
    ):
        raise CollectionError(f"signer JSON contains invalid timestamps for DID {did}")

    return (
        did,
        record.get("first_observed_ts"),
        record.get("last_observed_ts"),
        tick_count,
        record.get("collection_first_utc_date"),
        record.get("collection_last_utc_date"),
        collection_dates,
        json.dumps(rooms, ensure_ascii=False, separators=(",", ":")),
        len(rooms),
        counterparties,
    )


def add_source_count(counts: dict[str, int], row: tuple[Any, ...]) -> None:
    tick_count = row[3]
    collection_dates = row[6]
    room_count = row[8]
    has_counterparty = row[9]

    counts["observed"] += 1
    if tick_count < 2:
        return
    counts["two_ticks"] += 1
    if collection_dates < 2:
        return
    counts["two_collection_dates"] += 1
    if room_count < 2:
        return
    counts["two_rooms"] += 1
    if has_counterparty < 1:
        return
    counts["counterparties"] += 1


def stream_dids(
    reader: StreamingJsonReader,
    connection: sqlite3.Connection,
    source_counts: dict[str, int],
) -> None:
    reader.consume("{")
    if reader.peek() == "}":
        reader.consume("}")
        return

    batch: list[tuple[Any, ...]] = []
    seen: set[str] = set()
    while True:
        did = reader.value()
        if not isinstance(did, str):
            raise CollectionError("signer JSON contains a non-string DID key")
        if did in seen:
            raise CollectionError(f"signer JSON contains duplicate DID {did}")
        seen.add(did)

        reader.consume(":")
        row = validate_source_record(did, reader.value())
        add_source_count(source_counts, row)
        batch.append(row)
        if len(batch) == 1_000:
            connection.executemany(INSERT_SQL, batch)
            batch.clear()

        separator = reader.peek()
        if separator == "}":
            reader.consume("}")
            break
        reader.consume(",")

    if batch:
        connection.executemany(INSERT_SQL, batch)


def stream_source(
    source_path: Path,
    connection: sqlite3.Connection,
) -> tuple[dict[str, Any], dict[str, int]]:
    metadata: dict[str, Any] = {}
    source_counts = dict.fromkeys(FUNNEL_NAMES, 0)
    saw_dids = False
    seen_keys: set[str] = set()

    with source_path.open("r", encoding="utf-8") as stream:
        reader = StreamingJsonReader(stream)
        reader.consume("{")
        if reader.peek() == "}":
            raise CollectionError("signer JSON is empty")

        while True:
            key = reader.value()
            if not isinstance(key, str):
                raise CollectionError("signer JSON contains a non-string top-level key")
            if key in seen_keys:
                raise CollectionError(f"signer JSON contains duplicate top-level key {key}")
            seen_keys.add(key)
            reader.consume(":")

            if key == "dids":
                stream_dids(reader, connection, source_counts)
                saw_dids = True
            else:
                metadata[key] = reader.value()

            separator = reader.peek()
            if separator == "}":
                reader.consume("}")
                break
            reader.consume(",")

        reader.finish()

    if metadata.get("version") != 2 or not saw_dids:
        raise CollectionError("source signer state is not a complete v2 store")
    return metadata, source_counts


def remove_database_files(path: Path) -> None:
    for candidate in (
        path,
        path.with_name(path.name + "-wal"),
        path.with_name(path.name + "-shm"),
    ):
        candidate.unlink(missing_ok=True)


def migrate_signers(
    source_path: Path,
    metadata_path: Path,
    cap_saturated_at: str | None = None,
    released_at: str | None = None,
) -> tuple[dict[str, int], dict[str, int]]:
    source_path = source_path.resolve()
    metadata_path = metadata_path.resolve()
    database_path = signer_database_path(metadata_path)

    if source_path == metadata_path or source_path == database_path:
        raise CollectionError("source and output paths must be different")
    if not source_path.is_file():
        raise CollectionError(f"source signer state does not exist: {source_path}")
    if metadata_path.exists() or database_path.exists():
        raise CollectionError("refusing to replace an existing migration output")
    if metadata_path.with_name(metadata_path.name + ".tmp").exists():
        raise CollectionError("refusing to replace an existing metadata temporary file")

    connection: sqlite3.Connection | None = None
    try:
        connection = connect_signer_database(database_path)
        connection.execute("BEGIN IMMEDIATE")
        metadata, source_counts = stream_source(source_path, connection)

        cap_hit = metadata.get("cap_hit")
        if not isinstance(cap_hit, bool):
            raise CollectionError("source signer state has an invalid cap_hit value")
        if cap_hit and cap_saturated_at is None:
            raise CollectionError(
                "--cap-saturated-at is required because the source records cap_hit"
            )
        if not cap_hit and cap_saturated_at is not None:
            raise CollectionError(
                "--cap-saturated-at was provided but the source never recorded cap_hit"
            )

        metadata["version"] = SIGNER_STATE_VERSION
        if cap_hit:
            release = released_at or utc_now()
            try:
                saturation_start = parse_timestamp(cap_saturated_at)
                saturation_release = parse_timestamp(release)
            except ValueError as error:
                raise CollectionError("cap-saturation timestamp is invalid") from error
            if saturation_release < saturation_start:
                raise CollectionError("cap release precedes cap saturation")
            metadata["tracking_cap_saturation"] = {
                "started_at": cap_saturated_at,
                "released_at": release,
                "permanent_undercount": True,
            }
        else:
            metadata["tracking_cap_saturation"] = None

        validate_signer_metadata(metadata)
        sqlite_counts = signer_funnel_counts(connection)
        for name in FUNNEL_NAMES:
            print(
                f"{name}: source={source_counts[name]} sqlite={sqlite_counts[name]}"
            )
        if source_counts != sqlite_counts:
            raise CollectionError("source and SQLite funnel counts do not match")

        write_signer_metadata(connection, metadata)
        connection.commit()
        connection.close()
        connection = None
        save_atomic_json(metadata_path, metadata)
        print(f"metadata: {metadata_path}")
        print(f"sqlite: {database_path}")
        return source_counts, sqlite_counts
    except Exception:
        if connection is not None:
            connection.rollback()
            connection.close()
        remove_database_files(database_path)
        metadata_path.with_name(metadata_path.name + ".tmp").unlink(missing_ok=True)
        raise


def absolute_path(value: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        raise argparse.ArgumentTypeError("path must be absolute")
    return path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Migrate the v2 signer JSON store to SQLite without changing the source."
    )
    parser.add_argument("source", type=absolute_path)
    parser.add_argument("metadata_output", type=absolute_path)
    parser.add_argument(
        "--cap-saturated-at",
        help="Recorded UTC timestamp when the legacy tracked-DID cap first saturated.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        migrate_signers(
            args.source,
            args.metadata_output,
            cap_saturated_at=args.cap_saturated_at,
        )
    except (CollectionError, OSError, sqlite3.Error) as error:
        print(f"migration failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())