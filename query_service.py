#!/usr/bin/env python3
"""Bounded, read-only HTTP service for Observatory room and DID evidence."""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import re
import sqlite3
import threading
import time
import urllib.parse
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from api_contract import (
    CONTRACT_VERSION,
    FRESHNESS_STATES,
    MAX_RESPONSE_BYTES,
    common_metadata,
    escape_plain_text,
    json_bytes,
    text_bytes,
)

DATABASE_SCHEMA_VERSION = 6
MAX_REQUEST_TARGET_BYTES = 2 * 1024
MAX_QUERY_FIELDS = 8
MAX_QUERY_CHARACTERS = 80
MAX_QUERY_BYTES = 320
DEFAULT_SEARCH_LIMIT = 10
MAX_SEARCH_LIMIT = 20
MAX_LIST_LIMIT = 100
DEFAULT_QUERY_TIMEOUT_SECONDS = 0.5
DEFAULT_MAX_CONCURRENT_REQUESTS = 8
QUERY_VALIDITY = timedelta(minutes=15)
ROOM_ID_RE = re.compile(r"^[0-9a-f]{16}$")
DID_RE = re.compile(r"^did:key:z6Mk[1-9A-HJ-NP-Za-km-z]{20,100}$")
INVALID_PERCENT_RE = re.compile(r"%(?![0-9A-Fa-f]{2})")
CONTROL_RE = re.compile(r"[\x00-\x1f\x7f-\x9f]")
STATE_VOCABULARY = (
    "unknown",
    "not_yet_checked",
    "present_at_last_check",
    "absent_at_last_check",
    "check_failed",
    "superseded_before_check",
    "deferred",
    "aged_out_unselected",
)
SNAPSHOT_RESOURCES = {
    "status": None,
    "incidents": "opened_at",
    "changes": "first_observed_at",
    "methodology": None,
}
COMMON_SNAPSHOT_FIELDS = {
    "contract_version",
    "generated_at",
    "source_observed_at",
    "derived_at",
    "published_at",
    "valid_until",
    "freshness",
    "collector_version",
    "methodology_version",
    "schema_version",
    "window",
    "coverage",
    "limitations",
}
LEDGER_SNAPSHOT_RESOURCES = {"status", "incidents"}
LEDGER_CHAIN_HEAD_RE = re.compile(r"^[0-9a-f]{64}$")
_SOURCE_OBSERVATION_UNSET = object()

SEARCH_COLUMNS = """
    room_ledger.name,
    room_ledger.room_id,
    room_ledger.room_sha256,
    room_ledger.created_seq,
    room_ledger.created_at,
    room_ledger.first_observed_at,
    room_ledger.last_listed_at,
    (
        SELECT room_revisits.outcome
        FROM room_revisits
        WHERE
            room_revisits.room_created_seq = room_ledger.created_seq
            AND room_revisits.attempted_at IS NOT NULL
        ORDER BY room_revisits.attempted_at DESC, room_revisits.stage_seconds DESC
        LIMIT 1
    ) AS latest_outcome,
    (
        SELECT room_revisits.success
        FROM room_revisits
        WHERE
            room_revisits.room_created_seq = room_ledger.created_seq
            AND room_revisits.attempted_at IS NOT NULL
        ORDER BY room_revisits.attempted_at DESC, room_revisits.stage_seconds DESC
        LIMIT 1
    ) AS latest_success,
    (
        SELECT room_revisits.attempted_at
        FROM room_revisits
        WHERE
            room_revisits.room_created_seq = room_ledger.created_seq
            AND room_revisits.attempted_at IS NOT NULL
        ORDER BY room_revisits.attempted_at DESC, room_revisits.stage_seconds DESC
        LIMIT 1
    ) AS latest_check_at,
    EXISTS (
        SELECT 1
        FROM room_revisits
        WHERE room_revisits.room_created_seq = room_ledger.created_seq
    ) AS has_scheduled_checks
"""
EXACT_SEARCH_SQL = f"""
    SELECT {SEARCH_COLUMNS}
    FROM room_ledger
    WHERE
        room_ledger.name = ? COLLATE BINARY
        AND NOT EXISTS (
            SELECT 1
            FROM room_ledger AS newer
            WHERE
                newer.name = room_ledger.name COLLATE BINARY
                AND newer.created_seq > room_ledger.created_seq
        )
    ORDER BY room_ledger.created_seq DESC
    LIMIT ?
"""
SUBSTRING_SEARCH_SQL = f"""
    SELECT {SEARCH_COLUMNS}
    FROM room_search
    JOIN room_ledger ON room_ledger.rowid = room_search.rowid
    WHERE
        room_search.name MATCH ?
        AND instr(room_ledger.name, ?) > 0
        AND NOT EXISTS (
            SELECT 1
            FROM room_ledger AS newer
            WHERE
                newer.name = room_ledger.name COLLATE BINARY
                AND newer.created_seq > room_ledger.created_seq
        )
    LIMIT ?
"""
ROOM_ID_LOOKUP_SQL = """
    WITH newest_generations AS (
        SELECT room_sha256, MAX(created_seq) AS created_seq
        FROM room_ledger
        WHERE room_id = ?
        GROUP BY room_sha256
        ORDER BY room_sha256
        LIMIT 2
    )
    SELECT
        room_ledger.name,
        room_ledger.room_id,
        room_ledger.room_sha256,
        room_ledger.created_seq,
        room_ledger.created_at,
        room_ledger.first_observed_at,
        room_ledger.last_listed_at
    FROM newest_generations
    JOIN room_ledger
        ON room_ledger.created_seq = newest_generations.created_seq
    ORDER BY room_ledger.room_sha256
"""


class SchemaError(RuntimeError):
    pass


class ApiError(RuntimeError):
    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message


def utc_datetime() -> datetime:
    return datetime.now(timezone.utc)


def format_utc(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("clock must return a timezone-aware datetime")
    return (
        value.astimezone(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


@dataclass(frozen=True)
class ServiceConfig:
    database_path: Path
    snapshot_root: Path
    collector_version: str | None = None
    methodology_version: str | None = None
    query_timeout_seconds: float = DEFAULT_QUERY_TIMEOUT_SECONDS
    clock: Callable[[], datetime] = utc_datetime


@dataclass
class Response:
    status: int
    content_type: str
    body: bytes
    cache_control: str
    no_index: bool = False
    headers: dict[str, str] = field(default_factory=dict)


def arm_query_deadline(connection: sqlite3.Connection, seconds: float) -> None:
    if seconds <= 0:
        raise ValueError("query timeout must be positive")
    deadline = time.monotonic() + seconds
    connection.set_progress_handler(
        lambda: int(time.monotonic() >= deadline),
        1_000,
    )


def validate_room_generation_schema(connection: sqlite3.Connection) -> None:
    ledger_columns = {
        row["name"]: row
        for row in connection.execute("PRAGMA table_info('room_ledger')")
    }
    revisit_columns = {
        row["name"]: row
        for row in connection.execute("PRAGMA table_info('room_revisits')")
    }
    unique_name = False
    for index in connection.execute(
        "SELECT name FROM pragma_index_list(?) WHERE [unique] = 1",
        ("room_ledger",),
    ):
        indexed_columns = [
            row["name"]
            for row in connection.execute(
                "SELECT name FROM pragma_index_info(?) ORDER BY seqno",
                (index["name"],),
            )
        ]
        if indexed_columns == ["name"]:
            unique_name = True
            break
    foreign_keys = {
        (row["from"], row["table"], row["to"])
        for row in connection.execute("PRAGMA foreign_key_list('room_revisits')")
    }
    created_seq = ledger_columns.get("created_seq")
    name = ledger_columns.get("name")
    room_created_seq = revisit_columns.get("room_created_seq")
    stage_seconds = revisit_columns.get("stage_seconds")
    if (
        created_seq is None
        or str(created_seq["type"]).upper() != "INTEGER"
        or created_seq["pk"] != 1
        or name is None
        or name["pk"] != 0
        or unique_name
        or room_created_seq is None
        or str(room_created_seq["type"]).upper() != "INTEGER"
        or room_created_seq["pk"] != 1
        or stage_seconds is None
        or stage_seconds["pk"] != 2
        or "room_name" in revisit_columns
        or (
            "room_created_seq",
            "room_ledger",
            "created_seq",
        )
        not in foreign_keys
    ):
        raise SchemaError(
            "query database requires the generation-aware v6 room schema "
            "(room_ledger.created_seq primary key and "
            "room_revisits.room_created_seq foreign key)"
        )


def open_readonly_database(
    path: Path,
    *,
    query_timeout_seconds: float = DEFAULT_QUERY_TIMEOUT_SECONDS,
) -> sqlite3.Connection:
    resolved = path.resolve()
    if not resolved.is_file():
        raise SchemaError(f"query database does not exist: {resolved}")
    if query_timeout_seconds <= 0:
        raise ValueError("query timeout must be positive")
    separator = "&" if "?" in resolved.as_uri() else "?"
    connection = sqlite3.connect(
        resolved.as_uri() + separator + "mode=ro",
        uri=True,
        timeout=query_timeout_seconds,
    )
    try:
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only = ON")
        connection.execute("PRAGMA foreign_keys = ON")
        version = connection.execute("PRAGMA user_version").fetchone()[0]
        if version != DATABASE_SCHEMA_VERSION:
            raise SchemaError(
                f"query database must have schema version {DATABASE_SCHEMA_VERSION}; "
                f"found {version}"
            )
        validate_room_generation_schema(connection)
        search_schema = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'room_search'"
        ).fetchone()
        if (
            search_schema is None
            or not isinstance(search_schema[0], str)
            or "case_sensitive 1" not in search_schema[0]
        ):
            raise SchemaError(
                "query database requires a case-sensitive trigram search index"
            )
        arm_query_deadline(connection, query_timeout_seconds)
    except Exception:
        connection.close()
        raise
    return connection


def parse_utc(value: str, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(
            value[:-1] + "+00:00" if value.endswith("Z") else value
        )
    except (TypeError, ValueError) as error:
        raise ApiError(
            400, f"invalid_{field}", f"{field} must be an ISO-8601 timestamp"
        ) from error
    if parsed.tzinfo is None:
        raise ApiError(400, f"invalid_{field}", f"{field} must include a timezone")
    try:
        return parsed.astimezone(timezone.utc)
    except OverflowError as error:
        raise ApiError(
            400,
            f"invalid_{field}",
            f"{field} must normalize within the supported UTC range",
        ) from error


def validate_query_text(value: str) -> str:
    if (
        not value
        or len(value) > MAX_QUERY_CHARACTERS
        or len(value.encode("utf-8")) > MAX_QUERY_BYTES
        or CONTROL_RE.search(value) is not None
    ):
        raise ApiError(
            400,
            "invalid_query",
            "q must contain 1 to 80 characters, at most 320 UTF-8 bytes, and no controls",
        )
    return value


def parse_limit(value: str | None, *, maximum: int, default: int | None) -> int | None:
    if value is None:
        return default
    if re.fullmatch(r"[1-9][0-9]*", value) is None:
        raise ApiError(400, "invalid_limit", f"limit must be between 1 and {maximum}")
    parsed = int(value)
    if parsed > maximum:
        raise ApiError(400, "invalid_limit", f"limit must be between 1 and {maximum}")
    return parsed


def decode_query(target: str) -> tuple[str, dict[str, str]]:
    if len(target.encode("utf-8")) > MAX_REQUEST_TARGET_BYTES:
        raise ApiError(
            414, "request_target_too_long", "request target exceeds 2048 bytes"
        )
    if INVALID_PERCENT_RE.search(target) is not None:
        raise ApiError(
            400, "invalid_encoding", "request target contains invalid percent encoding"
        )
    parsed = urllib.parse.urlsplit(target)
    raw_fields = parsed.query.split("&") if parsed.query else []
    if len(raw_fields) > MAX_QUERY_FIELDS:
        raise ApiError(
            400, "too_many_parameters", "at most 8 query parameters are allowed"
        )
    try:
        pairs = urllib.parse.parse_qsl(
            parsed.query,
            keep_blank_values=True,
            strict_parsing=True,
            encoding="utf-8",
            errors="strict",
            max_num_fields=MAX_QUERY_FIELDS,
        )
    except (UnicodeDecodeError, ValueError) as error:
        raise ApiError(
            400, "invalid_query_string", "query string is malformed"
        ) from error
    params: dict[str, str] = {}
    for key, value in pairs:
        if key in params:
            raise ApiError(
                400, "duplicate_parameter", f"parameter {key} appears more than once"
            )
        params[key] = value
    return parsed.path, params


def require_parameters(params: dict[str, str], allowed: set[str]) -> None:
    unexpected = sorted(set(params) - allowed)
    if unexpected:
        raise ApiError(
            400,
            "unexpected_parameter",
            f"unexpected parameter: {unexpected[0]}",
        )


def response_format(params: dict[str, str]) -> str:
    value = params.pop("format", None)
    if value is None:
        return "text"
    if value != "json":
        raise ApiError(400, "invalid_format", "format must be json when supplied")
    return "json"


def requested_representation(target: str) -> str:
    literal_json = re.search(r"(?:[?&])format=json(?:&|$)", target) is not None
    try:
        _, params = decode_query(target)
    except ApiError:
        return "json" if literal_json else "text"
    return "json" if params.get("format") == "json" else "text"


def pending_windows_by_room(
    connection: sqlite3.Connection,
    created_seqs: list[int],
) -> dict[int, list[tuple[str, int]]]:
    if not created_seqs:
        return {}
    placeholders = ",".join("?" * len(created_seqs))
    rows = connection.execute(
        "SELECT room_created_seq, due_at, stage_seconds "
        "FROM room_revisits "
        f"WHERE room_created_seq IN ({placeholders}) "
        "AND attempted_at IS NULL",
        created_seqs,
    ).fetchall()
    windows: dict[int, list[tuple[str, int]]] = {}
    for row in rows:
        windows.setdefault(row["room_created_seq"], []).append(
            (row["due_at"], row["stage_seconds"])
        )
    return windows


def state_from_outcome(
    outcome: str | None,
    success: int | None,
    attempted_at: str | None,
    *,
    has_scheduled_checks: bool = True,
    pending_windows: list[tuple[str, int]] | None = None,
    now: datetime | None = None,
) -> str:
    if attempted_at is None:
        if not has_scheduled_checks:
            return "unknown"
        # Without the windows we know a check is scheduled but not whether its
        # eligibility has closed, so the caller keeps the older, weaker claim.
        if not pending_windows:
            return "not_yet_checked"
        instant = now if now is not None else utc_datetime()
        open_window = False
        future_window = False
        for due_at, stage_seconds in pending_windows:
            try:
                due = datetime.fromisoformat(
                    due_at[:-1] + "+00:00" if due_at.endswith("Z") else due_at
                )
            except (TypeError, ValueError):
                return "unknown"
            if due.tzinfo is None:
                return "unknown"
            if instant < due:
                future_window = True
            elif instant < due + timedelta(seconds=stage_seconds):
                open_window = True
        if open_window:
            return "deferred"
        if future_window:
            return "not_yet_checked"
        # Every scheduled window closed with no attempt: this room was never
        # checked at any stage and never will be. It is not pending.
        return "aged_out_unselected"
    states = {
        "present_at_last_check": "present_at_last_check",
        "absent_at_last_check": "absent_at_last_check",
        "check_failed": "check_failed",
        "superseded_before_check": "superseded_before_check",
    }
    if outcome in states:
        return states[outcome]
    if success == 1:
        return "present_at_last_check"
    if success == 0:
        return "check_failed"
    return "unknown"


def fts_phrase(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def search_rooms(
    connection: sqlite3.Connection,
    query: str,
    limit: int,
) -> tuple[list[sqlite3.Row], bool, str]:
    probe_limit = min(MAX_SEARCH_LIMIT + 1, limit + 1)
    if len(query) < 3:
        rows = connection.execute(
            EXACT_SEARCH_SQL,
            (query, probe_limit),
        ).fetchall()
        mode = "exact_name"
    else:
        rows = connection.execute(
            SUBSTRING_SEARCH_SQL,
            (fts_phrase(query), query, probe_limit),
        ).fetchall()
        mode = "case_sensitive_substring"
    return list(rows[:limit]), len(rows) > limit, mode


class QueryApplication:
    def __init__(
        self,
        config: ServiceConfig,
        *,
        max_concurrent_requests: int = DEFAULT_MAX_CONCURRENT_REQUESTS,
    ) -> None:
        if max_concurrent_requests <= 0:
            raise ValueError("maximum concurrent requests must be positive")
        self.config = config
        self.capacity = threading.BoundedSemaphore(max_concurrent_requests)

    def handle(self, target: str) -> Response:
        representation = requested_representation(target)
        if not self.capacity.acquire(blocking=False):
            return self.error_response(
                ApiError(
                    503,
                    "query_capacity_exhausted",
                    "local query capacity is temporarily exhausted",
                ),
                representation,
            )
        try:
            try:
                path, params = decode_query(target)
                if path == "/rooms/":
                    return self.room_search_html(params)
                room_page = re.fullmatch(r"/rooms/([0-9a-f]{16})/", path)
                if room_page:
                    return self.room_evidence_html(room_page.group(1), params)
                if path == "/api/v1/rooms/search":
                    return self.room_search_api(params)
                room_api = re.fullmatch(r"/api/v1/rooms/([0-9a-f]{16})", path)
                if room_api:
                    return self.room_evidence_api(room_api.group(1), params)
                did_api = re.fullmatch(r"/api/v1/dids/(.+)", path)
                if did_api:
                    return self.did_api(
                        self.decode_path_segment(did_api.group(1)), params
                    )
                did_page = re.fullmatch(r"/keys/(.+)/", path)
                if did_page:
                    return self.did_html(
                        self.decode_path_segment(did_page.group(1)), params
                    )
                snapshot = re.fullmatch(
                    r"/api/v1/(status|incidents|changes|methodology)", path
                )
                if snapshot:
                    return self.snapshot(snapshot.group(1), params)
                raise ApiError(404, "not_found", "route not found")
            except ApiError as error:
                return self.error_response(error, representation)
            except sqlite3.OperationalError as error:
                if "interrupted" not in str(error).lower():
                    raise
                timeout = ApiError(
                    503, "query_timeout", "local query exceeded its time limit"
                )
                return self.error_response(timeout, representation)
        finally:
            self.capacity.release()

    def database(self) -> sqlite3.Connection:
        return open_readonly_database(
            self.config.database_path,
            query_timeout_seconds=self.config.query_timeout_seconds,
        )

    @staticmethod
    def newest_observation(*values: Any) -> str | None:
        newest: tuple[datetime, str] | None = None
        for value in values:
            if value is None:
                continue
            if not isinstance(value, str):
                raise ApiError(
                    503,
                    "local_data_unavailable",
                    "local evidence contains an invalid observation timestamp",
                )
            try:
                parsed = parse_utc(value, "source_observed_at")
            except ApiError as error:
                raise ApiError(
                    503,
                    "local_data_unavailable",
                    "local evidence contains an invalid observation timestamp",
                ) from error
            if newest is None or parsed > newest[0]:
                newest = (parsed, value)
        return None if newest is None else newest[1]

    def database_context(
        self,
        connection: sqlite3.Connection,
        *,
        limitations: list[str],
        source_observed_at: str | None | object = _SOURCE_OBSERVATION_UNSET,
        include_listing_observation: bool = False,
    ) -> dict[str, Any]:
        metadata_row = connection.execute(
            "SELECT state_json FROM signer_metadata WHERE singleton = 1"
        ).fetchone()
        metadata: dict[str, Any] = {}
        if metadata_row is not None:
            try:
                value = json.loads(metadata_row[0])
            except (ValueError, RecursionError):
                value = None
            if isinstance(value, dict):
                metadata = value
        listing_at = metadata.get("latest_room_listing_observed_at")
        if not isinstance(listing_at, str):
            listing_at = None
        first_row = connection.execute(
            """
            SELECT first_observed_at
            FROM room_ledger
            ORDER BY created_seq
            LIMIT 1
            """
        ).fetchone()
        last_row = connection.execute(
            """
            SELECT first_observed_at
            FROM room_ledger
            ORDER BY created_seq DESC
            LIMIT 1
            """
        ).fetchone()
        started_at = first_row[0] if first_row is not None else None
        ended_at = last_row[0] if last_row is not None else None
        if source_observed_at is _SOURCE_OBSERVATION_UNSET:
            observed_at = self.newest_observation(listing_at)
        elif include_listing_observation:
            observed_at = self.newest_observation(source_observed_at, listing_at)
        else:
            observed_at = self.newest_observation(source_observed_at)
        now = self.config.clock()
        generated_at = format_utc(now)
        valid_until = None
        freshness = "not_observed"
        if observed_at is not None:
            try:
                valid_at = parse_utc(observed_at, "source_observed_at") + QUERY_VALIDITY
            except (ApiError, OverflowError) as error:
                raise ApiError(
                    503,
                    "local_data_unavailable",
                    "local evidence contains an invalid observation timestamp",
                ) from error
            valid_until = format_utc(valid_at)
            freshness = "fresh" if now <= valid_at else "stale"
        context = common_metadata(
            source_observed_at=observed_at,
            valid_until=valid_until,
            freshness=freshness,
            collector_version=self.config.collector_version,
            methodology_version=self.config.methodology_version,
            schema_version=DATABASE_SCHEMA_VERSION,
            window={"started_at": started_at, "ended_at": ended_at},
            coverage={
                "boundary": "forward_only",
                "forward_ledger_started_at": started_at,
            },
            limitations=limitations,
            generated_at=generated_at,
        )
        if include_listing_observation:
            context["index_observed_at"] = listing_at
        return context

    def room_search_payload(
        self,
        query: str,
        limit: int,
    ) -> dict[str, Any]:
        with closing(self.database()) as connection:
            rows, capped, mode = search_rooms(connection, query, limit)
            source_observed_at = self.newest_observation(
                *(
                    value
                    for row in rows
                    for value in (
                        row["first_observed_at"],
                        row["last_listed_at"],
                        row["latest_check_at"],
                    )
                )
            )
            payload = self.database_context(
                connection,
                limitations=[
                    "Names are attacker-chosen public input and are returned only for this explicit query.",
                    "The forward ledger contains no rooms created before its collection boundary.",
                    "A failed scheduled read is an unknown outcome, never evidence of absence.",
                ],
                source_observed_at=source_observed_at,
                include_listing_observation=True,
            )
            pending_windows = pending_windows_by_room(
                connection,
                [row["created_seq"] for row in rows],
            )
            results: list[dict[str, Any]] = []
            for row in rows:
                results.append(
                    {
                        "id": row["room_id"],
                        "name": row["name"],
                        "name_trust": "untrusted",
                        "match_type": "exact" if row["name"] == query else "substring",
                        "first_observed_at": row["first_observed_at"],
                        "created_at": row["created_at"],
                        "last_check_at": row["latest_check_at"],
                        "latest_lifecycle_state": state_from_outcome(
                            row["latest_outcome"],
                            row["latest_success"],
                            row["latest_check_at"],
                            has_scheduled_checks=bool(row["has_scheduled_checks"]),
                            pending_windows=pending_windows.get(row["created_seq"]),
                            now=self.config.clock(),
                        ),
                    }
                )
            payload.update(
                {
                    "query": query,
                    "match_mode": mode,
                    "name_trust": "untrusted",
                    "limit": limit,
                    "capped": capped,
                    "results": results,
                }
            )
            payload["coverage"].update(
                {
                    "result_count": len(results),
                    "result_limit": limit,
                    "cap_probe": min(MAX_SEARCH_LIMIT + 1, limit + 1),
                }
            )
            return payload

    def room_search_api(self, params: dict[str, str]) -> Response:
        require_parameters(params, {"q", "limit", "format"})
        representation = response_format(params)
        query = params.get("q")
        if query is None or query == "":
            raise ApiError(
                400, "missing_query", "q is required; no default listing exists"
            )
        validate_query_text(query)
        limit = parse_limit(
            params.get("limit"), maximum=MAX_SEARCH_LIMIT, default=DEFAULT_SEARCH_LIMIT
        )
        payload = self.room_search_payload(query, limit)
        return self.payload_response(payload, representation, no_index=True)

    def room_search_html(self, params: dict[str, str]) -> Response:
        require_parameters(params, {"q", "limit"})
        query = params.get("q")
        if query is None and params:
            raise ApiError(
                400, "missing_query", "q is required; no default listing exists"
            )
        results = (
            '<p class="empty-row state-missing">'
            '<span class="missing-marker" aria-hidden="true"></span>'
            "Submit a query. No default room listing is published.</p>"
        )
        result_heading = "No query submitted"
        rail = (
            "<div><dt>OBSERVED</dt><dd>"
            '<span class="missing-marker" aria-hidden="true"></span>'
            "— · NOT RECORDED</dd></div>"
            "<div><dt>WINDOW</dt><dd>FORWARD-ONLY LEDGER</dd></div>"
            "<div><dt>COVERAGE</dt><dd>NO DEFAULT LISTING</dd></div>"
            "<div><dt>METHOD</dt><dd>LOCAL NAME SEARCH</dd></div>"
        )
        if query is not None and query != "":
            validate_query_text(query)
            limit = parse_limit(
                params.get("limit"),
                maximum=MAX_SEARCH_LIMIT,
                default=DEFAULT_SEARCH_LIMIT,
            )
            payload = self.room_search_payload(query, limit)
            observed_at = payload["source_observed_at"]
            observed = (
                html.escape(str(observed_at))
                if observed_at is not None
                else (
                    '<span class="missing-marker" aria-hidden="true"></span>'
                    "— · NOT RECORDED"
                )
            )
            method = html.escape(str(payload["methodology_version"] or "not recorded"))
            match_mode = html.escape(str(payload["match_mode"]).replace("_", " "))
            more = " · MORE MATCHES RECORDED" if payload["capped"] else ""
            rail = (
                f"<div><dt>OBSERVED</dt><dd>{observed}</dd></div>"
                "<div><dt>WINDOW</dt><dd>FORWARD-ONLY LEDGER</dd></div>"
                f"<div><dt>COVERAGE</dt><dd>{len(payload['results'])} RETURNED "
                f"· LIMIT {limit}{more}</dd></div>"
                f"<div><dt>METHOD</dt><dd>{match_mode}@{method}</dd></div>"
            )
            entries = []
            for index, result in enumerate(payload["results"], start=1):
                safe_name = html.escape(escape_plain_text(result["name"]))
                safe_identifier = html.escape(result["id"], quote=True)
                lifecycle_state = html.escape(
                    str(result["latest_lifecycle_state"]).replace("_", " ")
                )
                created_at = html.escape(str(result["created_at"]))
                first_observed_at = html.escape(str(result["first_observed_at"]))
                entries.append(
                    '<article class="result-record">'
                    f'<span class="result-index" aria-hidden="true">{index:02d}</span>'
                    '<div class="result-primary">'
                    '<p class="figure-label">Untrusted room name</p>'
                    f"<h3>{safe_name}</h3></div>"
                    '<dl class="result-fields">'
                    f"<div><dt>LIFECYCLE</dt><dd>{lifecycle_state}</dd></div>"
                    f"<div><dt>CREATED</dt><dd>{created_at}</dd></div>"
                    f"<div><dt>FIRST OBSERVED</dt><dd>{first_observed_at}</dd></div>"
                    "</dl>"
                    f'<a class="result-link" href="/rooms/{safe_identifier}/">'
                    "OPEN EVIDENCE</a>"
                    "</article>"
                )
            results = "".join(entries) or (
                '<p class="empty-row state-missing">'
                '<span class="missing-marker" aria-hidden="true"></span>'
                "No matching room was observed. Not observed is not absent.</p>"
            )
            result_heading = f"{len(payload['results'])} records returned"
        elif query == "":
            raise ApiError(400, "missing_query", "q must not be empty")
        safe_query = html.escape(escape_plain_text(query or ""), quote=True)
        body = self.html_document(
            title="Room search",
            preview_title="Technocore room search",
            heading="Search the forward room ledger",
            content=(
                '<form class="room-search" method="get" action="/rooms/" role="search">'
                '<label for="room-query">Exact or substring room name</label>'
                '<div class="search-line">'
                f'<input id="room-query" name="q" type="search" autocomplete="off" '
                f'spellcheck="false" maxlength="80" required value="{safe_query}" '
                'aria-describedby="search-boundary search-feedback">'
                '<button type="submit">SEARCH</button>'
                "</div>"
                '<p id="search-boundary" class="boundary-note">'
                "LOCAL EVIDENCE ONLY · MAXIMUM 20 RESULTS · ZERO ORIGIN READS</p>"
                '<p id="search-feedback" class="search-feedback" '
                'aria-live="polite"></p>'
                "</form>"
                '<dl class="evidence-rail" aria-label="Evidence rail">'
                f"{rail}</dl>"
                '<section class="result-list" aria-labelledby="room-results-title">'
                '<header class="section-heading compact-heading"><div>'
                '<p class="figure-label">RESULTS / BOUNDED LOCAL INDEX</p>'
                f'<h2 id="room-results-title">{html.escape(result_heading)}</h2>'
                "</div><p>Names are untrusted labels. Results are capped at 20. "
                "Not observed is not absent.</p></header>"
                f"{results}</section>"
            ),
        )
        return self.bounded_response(
            Response(200, "text/html; charset=utf-8", body, "no-store", no_index=True)
        )

    def room_record(self, identifier: str) -> dict[str, Any]:
        if ROOM_ID_RE.fullmatch(identifier) is None:
            raise ApiError(404, "room_unknown", "room evidence was not found")
        with closing(self.database()) as connection:
            rows = connection.execute(ROOM_ID_LOOKUP_SQL, (identifier,)).fetchall()
            if not rows:
                raise ApiError(404, "room_unknown", "room evidence was not found")
            if len(rows) > 1:
                raise ApiError(
                    409,
                    "ambiguous_room_id",
                    "the short room identifier is ambiguous; no room name is disclosed",
                )
            room = rows[0]
            checks = connection.execute(
                """
                SELECT
                    stage_seconds,
                    due_at,
                    attempted_at,
                    success,
                    message_count,
                    has_second_message,
                    second_sender_class,
                    outcome
                FROM room_revisits
                WHERE room_created_seq = ?
                ORDER BY stage_seconds
                """,
                (room["created_seq"],),
            ).fetchall()
            metadata_row = connection.execute(
                "SELECT state_json FROM signer_metadata WHERE singleton = 1"
            ).fetchone()
            try:
                metadata = (
                    json.loads(metadata_row[0]) if metadata_row is not None else {}
                )
            except (ValueError, RecursionError) as error:
                raise ApiError(
                    503,
                    "local_data_unavailable",
                    "local evidence contains invalid signer metadata",
                ) from error
            listing_observed_at = (
                metadata.get("latest_room_listing_observed_at")
                if isinstance(metadata, dict)
                else None
            )
            if not isinstance(listing_observed_at, str):
                listing_observed_at = None
            check_records = [
                {
                    "stage_seconds": check["stage_seconds"],
                    "due_at": check["due_at"],
                    "attempted_at": check["attempted_at"],
                    "state": state_from_outcome(
                        check["outcome"],
                        check["success"],
                        check["attempted_at"],
                        pending_windows=[(check["due_at"], check["stage_seconds"])],
                        now=self.config.clock(),
                    ),
                    "message_count": check["message_count"],
                    "has_second_message": (
                        None
                        if check["has_second_message"] is None
                        else bool(check["has_second_message"])
                    ),
                    "second_sender_class": check["second_sender_class"],
                }
                for check in checks
            ]
            attempted = [
                check for check in check_records if check["attempted_at"] is not None
            ]
            if attempted:
                latest_state = max(
                    attempted,
                    key=lambda check: (check["attempted_at"], check["stage_seconds"]),
                )["state"]
            elif check_records:
                latest_state = "not_yet_checked"
            else:
                latest_state = "unknown"
            last_listed_at = room["last_listed_at"]
            if listing_observed_at is None:
                listing_state = "unknown"
            elif last_listed_at == listing_observed_at:
                listing_state = "present"
            elif last_listed_at is None or last_listed_at < listing_observed_at:
                listing_state = "absent"
            else:
                listing_state = "unknown"
            observation_times = [
                value
                for value in (
                    room["first_observed_at"],
                    last_listed_at,
                    listing_observed_at,
                    *(check["attempted_at"] for check in attempted),
                )
                if value is not None
            ]
            context = self.database_context(
                connection,
                limitations=[
                    "The record begins when the room entered the forward ledger.",
                    "Scheduled-check failures remain unknown outcomes.",
                    "Latest listing presence describes only the newest local listing snapshot.",
                ],
                source_observed_at=self.newest_observation(*observation_times),
            )
            context.update(
                {
                    "state_vocabulary": list(STATE_VOCABULARY),
                    "name_trust": "untrusted",
                    "room": {
                        "id": room["room_id"],
                        "sha256": room["room_sha256"],
                        "name": room["name"],
                        "name_trust": "untrusted",
                        "creation": {
                            "created_seq": room["created_seq"],
                            "created_at": room["created_at"],
                            "first_observed_at": room["first_observed_at"],
                        },
                        "scheduled_checks": check_records,
                        "latest_lifecycle_state": latest_state,
                        "latest_local_listing_presence": {
                            "state": listing_state,
                            "listing_observed_at": listing_observed_at,
                            "last_listed_at": last_listed_at,
                        },
                    },
                }
            )
            return context

    def room_evidence_api(self, identifier: str, params: dict[str, str]) -> Response:
        require_parameters(params, {"format"})
        representation = response_format(params)
        return self.payload_response(
            self.room_record(identifier), representation, no_index=True
        )

    def room_evidence_html(self, identifier: str, params: dict[str, str]) -> Response:
        require_parameters(params, set())
        payload = self.room_record(identifier)
        room = payload["room"]
        check_rows = []
        for index, check in enumerate(room["scheduled_checks"], start=1):
            stage_seconds = html.escape(str(check["stage_seconds"]))
            due_at = html.escape(str(check["due_at"]))
            lifecycle_state = html.escape(str(check["state"]).replace("_", " "))
            attempted_at = (
                html.escape(str(check["attempted_at"]))
                if check["attempted_at"] is not None
                else (
                    '<span class="missing-marker" aria-hidden="true"></span>'
                    "— · NOT RECORDED"
                )
            )
            check_rows.append(
                '<div class="ledger-row result-record">'
                f'<span class="result-index" aria-hidden="true">{index:02d}</span>'
                '<div class="result-primary">'
                '<p class="figure-label">Checkpoint</p>'
                f"<h3>{stage_seconds} seconds</h3></div>"
                f'<strong class="state">{lifecycle_state}</strong>'
                '<span class="evidence">'
                f"DUE {due_at}<br>ATTEMPTED {attempted_at}</span>"
                "</div>"
            )
        checks = "".join(check_rows)
        if not checks:
            checks = (
                '<p class="empty-row state-missing">'
                '<span class="missing-marker" aria-hidden="true"></span>'
                "No scheduled checkpoint is recorded.</p>"
            )
        safe_name = html.escape(escape_plain_text(room["name"]))
        first_observed_at = html.escape(str(room["creation"]["first_observed_at"]))
        source_observed_at = payload["source_observed_at"]
        observed = (
            html.escape(str(source_observed_at))
            if source_observed_at is not None
            else (
                '<span class="missing-marker" aria-hidden="true"></span>'
                "— · NOT RECORDED"
            )
        )
        methodology = html.escape(str(payload["methodology_version"] or "not recorded"))
        latest_state = html.escape(
            str(room["latest_lifecycle_state"]).replace("_", " ")
        )
        body = self.html_document(
            title="Room evidence",
            preview_title="Technocore room evidence",
            heading="Forward room evidence",
            content=(
                '<header class="result-detail-header">'
                '<p class="figure-label">Untrusted room name</p>'
                f"<h2>{safe_name}</h2></header>"
                '<dl class="evidence-rail" aria-label="Evidence rail">'
                f"<div><dt>OBSERVED</dt><dd>{observed}</dd></div>"
                f"<div><dt>WINDOW</dt><dd>FORWARD LEDGER SINCE "
                f"{first_observed_at}</dd></div>"
                f"<div><dt>COVERAGE</dt><dd>{len(room['scheduled_checks'])} "
                "SCHEDULED CHECKPOINTS</dd></div>"
                f"<div><dt>METHOD</dt><dd>ROOM LIFECYCLE@{methodology}</dd></div>"
                "</dl>"
                '<div class="status-figure">'
                '<p class="figure-label">Latest lifecycle state</p>'
                f'<p class="figure-value">{latest_state}</p>'
                '<p class="figure-context">A failed scheduled read remains an '
                "unknown outcome.</p></div>"
                '<section class="record-section" '
                'aria-labelledby="scheduled-checks-title">'
                '<header class="section-heading compact-heading"><div>'
                '<p class="figure-label">Scheduled evidence</p>'
                '<h2 id="scheduled-checks-title">Scheduled checks</h2>'
                "</div><p>Each row records one declared checkpoint. Missing reads "
                "remain unknown.</p></header>"
                f'<div class="ruled-list">{checks}</div></section>'
            ),
        )
        return self.bounded_response(
            Response(200, "text/html; charset=utf-8", body, "no-store", no_index=True)
        )

    def decode_path_segment(self, value: str) -> str:
        if "/" in value or INVALID_PERCENT_RE.search(value) is not None:
            raise ApiError(400, "invalid_path", "path segment is malformed")
        try:
            return urllib.parse.unquote(value, encoding="utf-8", errors="strict")
        except UnicodeDecodeError as error:
            raise ApiError(400, "invalid_path", "path segment is not UTF-8") from error

    @staticmethod
    def recorded(value: Any) -> dict[str, Any]:
        return {
            "state": "recorded" if value is not None else "not_recorded",
            "value": value,
        }

    def did_record(self, did: str) -> dict[str, Any]:
        if DID_RE.fullmatch(did) is None:
            raise ApiError(
                400, "invalid_did", "DID must be a complete did:key:z6Mk value"
            )
        suffix = did.removeprefix("did:key:z6Mk")
        with closing(self.database()) as connection:
            row = connection.execute(
                """
                SELECT
                    first_observed_ts,
                    last_observed_ts,
                    tick_count,
                    collection_first_utc_date,
                    collection_last_utc_date,
                    collection_utc_dates_count,
                    rooms_json,
                    room_count,
                    has_counterparty
                FROM signer_dids
                WHERE did = ?
                """,
                (suffix,),
            ).fetchone()
            if row is None:
                raise ApiError(
                    404, "did_unknown", "DID is outside the stored exact-key record"
                )
            for field in ("tick_count", "collection_utc_dates_count", "room_count"):
                value = row[field]
                if value is not None and (
                    isinstance(value, bool) or not isinstance(value, int) or value < 0
                ):
                    raise ApiError(
                        503,
                        "local_data_unavailable",
                        "local evidence contains invalid DID counters",
                    )
            if row["has_counterparty"] is not None and row["has_counterparty"] not in (
                0,
                1,
            ):
                raise ApiError(
                    503,
                    "local_data_unavailable",
                    "local evidence contains an invalid DID alternation value",
                )
            context = self.database_context(
                connection,
                limitations=[
                    "This exact record contains only facts retained from sampled room reads.",
                    "A false alternation fact means no qualifying alternation was observed in stored sampled evidence; it is not evidence of absence.",
                    "Retained room identifiers are bounded to eight and expose no room names.",
                    "Enumerated tick and date histories are not retained.",
                    "Sampling-opportunity denominators are not retained.",
                    "Counterparties are not identified or retained.",
                    "Per-fact historical collector versions are not retained.",
                ],
                source_observed_at=row["last_observed_ts"],
            )
            room_names: list[str] | None = None
            if isinstance(row["rooms_json"], str):
                try:
                    raw_rooms = json.loads(row["rooms_json"])
                except (ValueError, RecursionError):
                    raw_rooms = None
                if isinstance(raw_rooms, list) and all(
                    isinstance(room, str) for room in raw_rooms
                ):
                    room_names = raw_rooms
            if room_names is None or row["room_count"] is None:
                retained_rooms = {
                    "state": "not_recorded",
                    "ids": [],
                    "stored_count": None,
                    "reported_count": None,
                    "count_relation": "not_recorded",
                    "truncated": None,
                }
            else:
                reported_count = row["room_count"]
                truncated = reported_count >= 8
                retained_rooms = {
                    "state": "recorded",
                    "ids": sorted(
                        {
                            hashlib.sha256(room.encode("utf-8")).hexdigest()[:16]
                            for room in room_names
                        }
                    ),
                    "stored_count": len(room_names),
                    "reported_count": reported_count,
                    "count_relation": "at_least" if truncated else "exact",
                    "truncated": truncated,
                }
            if row["collection_utc_dates_count"] is None:
                dates = {
                    "state": "not_recorded",
                    "first": None,
                    "last": None,
                    "count": None,
                }
            else:
                dates = {
                    "state": "recorded",
                    "first": row["collection_first_utc_date"],
                    "last": row["collection_last_utc_date"],
                    "count": row["collection_utc_dates_count"],
                }
            counterparty = row["has_counterparty"]
            alternation = {
                "state": "recorded" if counterparty is not None else "not_recorded",
                "observed": None if counterparty is None else bool(counterparty),
            }
            context["did"] = {
                "id": did,
                "first_observed_at": self.recorded(row["first_observed_ts"]),
                "last_observed_at": self.recorded(row["last_observed_ts"]),
                "covered_ticks": self.recorded(row["tick_count"]),
                "covered_collection_dates": dates,
                "retained_rooms": retained_rooms,
                "signed_reciprocal_alternation": alternation,
            }
            context["coverage"].update(
                {
                    "observed_tick_count": row["tick_count"],
                    "observed_collection_date_count": row["collection_utc_dates_count"],
                }
            )
            return context

    def did_api(self, did: str, params: dict[str, str]) -> Response:
        require_parameters(params, {"format"})
        representation = response_format(params)
        return self.payload_response(
            self.did_record(did), representation, no_index=True
        )

    def did_html(self, did: str, params: dict[str, str]) -> Response:
        require_parameters(params, set())
        payload = self.did_record(did)
        record = payload["did"]
        room_ids = "".join(
            f"<li><code>{html.escape(identifier)}</code></li>"
            for identifier in record["retained_rooms"]["ids"]
        )
        if not room_ids:
            room_ids = "<li>Not recorded</li>"
        body = self.html_document(
            title="Key observation",
            preview_title="Technocore key observation",
            heading="Exact key observation record",
            content=(
                '<p class="figure-label">Supplied did:key</p>'
                f"<p><code>{html.escape(did)}</code></p>"
                '<dl class="evidence-rail" aria-label="Evidence rail">'
                f"<div><dt>First observed</dt><dd>{html.escape(str(record['first_observed_at']['value'] or 'not recorded'))}</dd></div>"
                f"<div><dt>Last observed</dt><dd>{html.escape(str(record['last_observed_at']['value'] or 'not recorded'))}</dd></div>"
                f"<div><dt>Covered ticks</dt><dd>{html.escape(str(record['covered_ticks']['value'] if record['covered_ticks']['value'] is not None else 'not recorded'))}</dd></div>"
                "</dl>"
                f'<div class="ledger-heading"><h2>Retained hashed rooms</h2></div><ul>{room_ids}</ul>'
            ),
        )
        return self.bounded_response(
            Response(200, "text/html; charset=utf-8", body, "no-store", no_index=True)
        )

    @staticmethod
    def validate_snapshot_payload(payload: Any, resource: str) -> None:
        invalid = ApiError(503, "snapshot_invalid", f"{resource} snapshot is invalid")
        if (
            not isinstance(payload, dict)
            or resource not in payload
            or not COMMON_SNAPSHOT_FIELDS.issubset(payload)
            or payload["contract_version"] != CONTRACT_VERSION
        ):
            raise invalid

        timestamps: dict[str, datetime] = {}
        for timestamp_name in ("generated_at", "derived_at", "published_at"):
            value = payload[timestamp_name]
            if not isinstance(value, str):
                raise invalid
            try:
                timestamps[timestamp_name] = parse_utc(value, timestamp_name)
            except ApiError as error:
                raise invalid from error
        if not (
            timestamps["derived_at"]
            <= timestamps["published_at"]
            <= timestamps["generated_at"]
        ):
            raise invalid

        source_observed_at = payload["source_observed_at"]
        valid_until = payload["valid_until"]
        freshness = payload["freshness"]
        if freshness not in FRESHNESS_STATES:
            raise invalid
        if source_observed_at is None:
            if valid_until is not None:
                raise invalid
            expected_freshness = (
                "not_applicable" if resource == "methodology" else "not_observed"
            )
        else:
            if not isinstance(source_observed_at, str) or not isinstance(
                valid_until, str
            ):
                raise invalid
            try:
                source_time = parse_utc(source_observed_at, "source_observed_at")
                valid_time = parse_utc(valid_until, "valid_until")
                expected_valid_time = source_time + QUERY_VALIDITY
            except (ApiError, OverflowError) as error:
                raise invalid from error
            if (
                source_time > timestamps["derived_at"]
                or valid_time != expected_valid_time
            ):
                raise invalid
            expected_freshness = (
                "fresh" if timestamps["generated_at"] <= valid_time else "stale"
            )
        if freshness != expected_freshness:
            raise invalid
        if resource == "methodology" and source_observed_at is not None:
            raise invalid

        for version_name in ("collector_version", "methodology_version"):
            if payload[version_name] is not None and not isinstance(
                payload[version_name], str
            ):
                raise invalid
        schema_version = payload["schema_version"]
        if schema_version is not None and (
            isinstance(schema_version, bool) or not isinstance(schema_version, int)
        ):
            raise invalid
        if payload["window"] is not None and not isinstance(payload["window"], dict):
            raise invalid
        if not isinstance(payload["coverage"], dict):
            raise invalid
        limitations = payload["limitations"]
        if not isinstance(limitations, list) or not all(
            isinstance(item, str) for item in limitations
        ):
            raise invalid

        ledger_chain_head = payload.get("ledger_chain_head")
        if "ledger_chain_head" in payload and (
            resource not in LEDGER_SNAPSHOT_RESOURCES
            or not isinstance(ledger_chain_head, str)
            or LEDGER_CHAIN_HEAD_RE.fullmatch(ledger_chain_head) is None
        ):
            raise invalid

        values = payload[resource]
        timestamp_field = SNAPSHOT_RESOURCES[resource]
        if timestamp_field is None:
            if not isinstance(values, dict):
                raise invalid
        else:
            if not isinstance(values, list):
                raise invalid
            for item in values:
                if not isinstance(item, dict):
                    raise invalid
                timestamp = item.get(timestamp_field)
                if not isinstance(timestamp, str):
                    raise invalid
                try:
                    parse_utc(timestamp, "snapshot_timestamp")
                except ApiError as error:
                    raise invalid from error
        try:
            json_bytes(payload)
            text_bytes(payload)
        except (TypeError, ValueError, RecursionError) as error:
            raise invalid from error

    def snapshot(self, resource: str, params: dict[str, str]) -> Response:
        allowed = {"format"}
        timestamp_field = SNAPSHOT_RESOURCES[resource]
        if timestamp_field is not None:
            allowed.update({"since", "limit"})
        require_parameters(params, allowed)
        representation = response_format(params)
        since_text = params.get("since")
        since = parse_utc(since_text, "since") if since_text is not None else None
        limit = parse_limit(params.get("limit"), maximum=MAX_LIST_LIMIT, default=None)
        filtered = since is not None or limit is not None
        json_path = self.config.snapshot_root / "api" / "v1" / f"{resource}.json"
        if not json_path.is_file():
            raise ApiError(
                503, "snapshot_unavailable", f"{resource} snapshot is unavailable"
            )
        try:
            payload = json.loads(self.read_bounded(json_path))
        except (ValueError, RecursionError) as error:
            raise ApiError(
                503, "snapshot_invalid", f"{resource} snapshot is invalid"
            ) from error
        self.validate_snapshot_payload(payload, resource)
        if filtered:
            values = payload[resource]
            selected = []
            for item in values:
                if since is not None:
                    timestamp = item.get(timestamp_field)
                    item_timestamp = parse_utc(timestamp, "snapshot_timestamp")
                    if item_timestamp < since:
                        continue
                selected.append(item)
            if limit is not None:
                selected = selected[:limit]
            payload = dict(payload)
            payload[resource] = selected
            payload["coverage"] = {
                **payload["coverage"],
                "returned_records": len(selected),
                "filter_since": since_text,
                "filter_limit": limit,
            }
            now = self.config.clock()
            payload["generated_at"] = format_utc(now)
            source_observed_at = payload["source_observed_at"]
            if source_observed_at is None:
                payload["freshness"] = "not_observed"
            else:
                valid_until = parse_utc(payload["valid_until"], "valid_until")
                payload["freshness"] = "fresh" if now <= valid_until else "stale"
            self.validate_snapshot_payload(payload, resource)
        return self.payload_response(
            payload,
            representation,
            cache_control="public, max-age=60, stale-if-error=300",
        )

    def read_bounded(self, path: Path) -> bytes:
        if path.stat().st_size > MAX_RESPONSE_BYTES:
            raise ApiError(413, "response_too_large", "response exceeds 65536 bytes")
        body = path.read_bytes()
        if len(body) > MAX_RESPONSE_BYTES:
            raise ApiError(413, "response_too_large", "response exceeds 65536 bytes")
        return body

    def payload_response(
        self,
        payload: dict[str, Any],
        representation: str,
        *,
        no_index: bool = False,
        cache_control: str = "no-store",
    ) -> Response:
        body = json_bytes(payload) if representation == "json" else text_bytes(payload)
        content_type = (
            "application/json; charset=utf-8"
            if representation == "json"
            else "text/plain; charset=utf-8"
        )
        return self.bounded_response(
            Response(200, content_type, body, cache_control, no_index=no_index),
            representation=representation,
        )

    def error_response(self, error: ApiError, representation: str) -> Response:
        payload = common_metadata(
            source_observed_at=None,
            valid_until=None,
            freshness="not_observed",
            collector_version=self.config.collector_version,
            methodology_version=self.config.methodology_version,
            schema_version=DATABASE_SCHEMA_VERSION,
            window=None,
            coverage=None,
            limitations=[],
        )
        payload.update({"error": error.code, "message": error.message})
        body = json_bytes(payload) if representation == "json" else text_bytes(payload)
        return Response(
            error.status,
            (
                "application/json; charset=utf-8"
                if representation == "json"
                else "text/plain; charset=utf-8"
            ),
            body,
            "no-store",
            no_index=True,
        )

    def bounded_response(
        self,
        response: Response,
        *,
        representation: str = "text",
    ) -> Response:
        if len(response.body) <= MAX_RESPONSE_BYTES:
            return response
        return self.error_response(
            ApiError(413, "response_too_large", "response exceeds 65536 bytes"),
            representation,
        )

    @staticmethod
    def html_document(
        *,
        title: str,
        preview_title: str,
        heading: str,
        content: str,
    ) -> bytes:
        return (
            '<!doctype html><html lang="en"><head><meta charset="utf-8">'
            '<meta name="viewport" content="width=device-width,initial-scale=1">'
            '<meta name="description" content="Bounded forward-collected Technocore evidence.">'
            '<meta name="robots" content="noindex,nofollow,noarchive">'
            '<meta name="theme-color" content="#0B0E0C">'
            f'<meta property="og:title" content="{html.escape(preview_title, quote=True)}">'
            '<meta property="og:description" content="Bounded forward-collected Technocore evidence.">'
            '<meta property="og:type" content="website">'
            f"<title>{html.escape(title)} — Technocore Observatory</title>"
            '<link rel="stylesheet" href="/assets/styles.css">'
            '<script src="/assets/site.js" defer></script>'
            '</head><body data-page="evidence">'
            '<a class="skip-link" href="#main-content">Skip to main content</a>'
            '<header class="site-header"><div class="site-header-inner">'
            '<a class="wordmark" href="/" aria-label="Technocore Observatory home">'
            "TECHNOCORE OBSERVATORY</a>"
            '<nav class="priority-nav" aria-label="Primary">'
            '<a href="/status/">STATUS</a>'
            '<a href="/rooms/" aria-current="page">ROOMS</a>'
            '<a href="/observatory/">OBSERVATORY</a>'
            '<a href="/methodology/">METHODOLOGY</a></nav>'
            '<details class="site-index"><summary>INDEX</summary>'
            '<nav aria-label="Site index">'
            '<a href="/">REGISTER</a><a href="/status/">STATUS</a>'
            '<a href="/rooms/">ROOMS</a><a href="/incidents/">INCIDENTS</a>'
            '<a href="/changes/">CHANGES</a>'
            '<a href="/observatory/">OBSERVATORY</a>'
            '<a href="/methodology/">METHODOLOGY</a>'
            '<a href="/about/">ABOUT</a></nav></details>'
            '<button class="theme-control" id="theme-toggle" type="button" '
            'aria-label="Theme: auto" data-theme-value="system">'
            "THEME AUTO</button></div></header>"
            '<main id="main-content" class="page-shell" tabindex="-1">'
            '<header class="page-lede">'
            '<p class="eyebrow">LOCAL EVIDENCE / READ-ONLY</p>'
            f"<h1>{html.escape(heading)}</h1>"
            "<p>Local forward-collected evidence. Labels are untrusted where shown. "
            "Not observed is not absent.</p></header>"
            f'<section class="page-section query-content">{content}</section></main>'
            '<footer class="site-footer">'
            "<p>Independent instrument. Public observations, bounded claims.</p>"
            '<p><a href="/api/v1/status.txt">STATUS AS TEXT</a> · '
            '<a href="/llms.txt">AGENT GUIDE</a> · CONTRACT 1.0.0</p></footer>'
            "</body></html>"
        ).encode("utf-8")


class ObservatoryHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        server_address: tuple[str, int],
        application: QueryApplication,
    ) -> None:
        self.application = application
        super().__init__(server_address, ObservatoryRequestHandler)


class ObservatoryRequestHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "TechnocoreObservatory"
    sys_version = ""

    def version_string(self) -> str:
        return self.server_version

    def send_error(
        self,
        code: int,
        message: str | None = None,
        explain: str | None = None,
    ) -> None:
        if code == 501:
            self.method_not_allowed()
            return
        super().send_error(code, message, explain)

    def do_GET(self) -> None:
        self.respond(head_only=False)

    def do_HEAD(self) -> None:
        self.respond(head_only=True)

    def do_POST(self) -> None:
        self.method_not_allowed()

    def do_PUT(self) -> None:
        self.method_not_allowed()

    def do_PATCH(self) -> None:
        self.method_not_allowed()

    def do_DELETE(self) -> None:
        self.method_not_allowed()

    def do_OPTIONS(self) -> None:
        self.method_not_allowed()

    def method_not_allowed(self) -> None:
        error = ApiError(405, "method_not_allowed", "only GET and HEAD are allowed")
        response = self.server.application.error_response(
            error, requested_representation(self.path)
        )
        response.headers["Allow"] = "GET, HEAD"
        response.headers["Connection"] = "close"
        self.close_connection = True
        self.write_response(response, head_only=self.command == "HEAD")

    def respond(self, *, head_only: bool) -> None:
        transfer_encodings = self.headers.get_all("Transfer-Encoding", [])
        content_lengths = self.headers.get_all("Content-Length", [])
        expect = self.headers.get("Expect")
        valid_empty_length = (
            len(content_lengths) == 1
            and re.fullmatch(r"0+", content_lengths[0].strip()) is not None
        )
        if (
            transfer_encodings
            or expect is not None
            or len(content_lengths) > 1
            or (content_lengths and not valid_empty_length)
        ):
            error = ApiError(
                400,
                "request_body_not_allowed",
                "GET and HEAD requests must not carry a request body",
            )
            response = self.server.application.error_response(
                error, requested_representation(self.path)
            )
            response.headers["Connection"] = "close"
            self.close_connection = True
            self.write_response(response, head_only=head_only)
            return
        try:
            response = self.server.application.handle(self.path)
        except (OSError, SchemaError, sqlite3.Error, ValueError, TypeError):
            error = ApiError(
                503, "local_data_unavailable", "local evidence is unavailable"
            )
            response = self.server.application.error_response(
                error, requested_representation(self.path)
            )
        self.write_response(response, head_only=head_only)

    def handle_expect_100(self) -> bool:
        return True

    def write_response(self, response: Response, *, head_only: bool) -> None:
        self.send_response(response.status)
        self.send_header("Content-Type", response.content_type)
        self.send_header("Content-Length", str(len(response.body)))
        self.send_header("Cache-Control", response.cache_control)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        if response.no_index:
            self.send_header("X-Robots-Tag", "noindex, nofollow, noarchive")
        for name, value in response.headers.items():
            self.send_header(name, value)
        self.end_headers()
        if not head_only:
            self.wfile.write(response.body)

    def log_message(self, format: str, *args: Any) -> None:
        return


def create_server(
    *,
    database_path: Path,
    snapshot_root: Path,
    host: str = "127.0.0.1",
    port: int = 8765,
    collector_version: str | None = None,
    methodology_version: str | None = None,
    query_timeout_seconds: float = DEFAULT_QUERY_TIMEOUT_SECONDS,
    max_concurrent_requests: int = DEFAULT_MAX_CONCURRENT_REQUESTS,
    clock: Callable[[], datetime] = utc_datetime,
) -> ObservatoryHTTPServer:
    if host != "127.0.0.1":
        raise ValueError("query service must bind to 127.0.0.1")
    config = ServiceConfig(
        database_path=database_path,
        snapshot_root=snapshot_root,
        collector_version=collector_version,
        methodology_version=methodology_version,
        query_timeout_seconds=query_timeout_seconds,
        clock=clock,
    )
    connection = open_readonly_database(
        database_path,
        query_timeout_seconds=query_timeout_seconds,
    )
    connection.close()
    return ObservatoryHTTPServer(
        (host, port),
        QueryApplication(
            config,
            max_concurrent_requests=max_concurrent_requests,
        ),
    )


def absolute_path(value: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        raise argparse.ArgumentTypeError("path must be absolute")
    return path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=absolute_path, required=True)
    parser.add_argument("--snapshot-root", type=absolute_path, required=True)
    parser.add_argument("--host", default="127.0.0.1", choices=("127.0.0.1",))
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--collector-version")
    parser.add_argument("--methodology-version")
    parser.add_argument(
        "--query-timeout-seconds", type=float, default=DEFAULT_QUERY_TIMEOUT_SECONDS
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if not 0 <= args.port <= 65535:
        raise SystemExit("port must be between 0 and 65535")
    server = create_server(
        database_path=args.database,
        snapshot_root=args.snapshot_root,
        host=args.host,
        port=args.port,
        collector_version=args.collector_version,
        methodology_version=args.methodology_version,
        query_timeout_seconds=args.query_timeout_seconds,
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        return 0
    finally:
        server.server_close()


if __name__ == "__main__":
    raise SystemExit(main())
