#!/usr/bin/env python3
"""Forward-only collector for the Technocore Observatory."""

from __future__ import annotations

import argparse
import hashlib
import http.client
import json
import math
import os
import re
import secrets
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, DecimalException, localcontext
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Callable, Iterator

from telemetry import TelemetryStore, normalize_route
from verify_ledger import (
    CHAIN_FIELD,
    CHAIN_VERSION,
    canonical_tick_bytes,
    canonical_tick_hash_bytes,
    verify_ledger,
)

ROOMS_HEADER_RE = re.compile(
    r"^#\s*(?P<shown>\d+)\s+of\s+(?P<total>\d+)\s+rooms\s+"
    r"\(cap\s+(?P<cap>\d+),\s+(?P<stored>[\d.]+[KMGT]?)\s+of\s+"
    r"(?P<storage_cap>[\d.]+[KMGT]?)\s+stored\),\s+newest first\s*$",
    re.MULTILINE | re.IGNORECASE,
)
NOTES_HEADER_RE = re.compile(
    r"^#\s*notes\s+(?P<total>\d+)\s+of\s+(?P<cap>\d+)\s+"
    r"\((?P<stored>[\d.]+[KMGT]?)\s+total,\s+"
    r"(?P<per_namespace>\d+)\s+per namespace(?:,[^)]*)?\)\s*$",
    re.MULTILINE | re.IGNORECASE,
)
LAST_SEQ_HEADER_RE = re.compile(r"^#\s*last[_ ]seq\s*[:=]\s*(\d+)\s*$", re.IGNORECASE)
RETRY_BODY_RE = re.compile(
    r"(?:retry|wait|after)[^0-9]{0,24}(\d+(?:\.\d+)?)\s*(?:s|sec|seconds?)?",
    re.IGNORECASE,
)
IDLE_RE = re.compile(r"^(\d+(?:\.\d+)?)([smhd]?)$", re.IGNORECASE)
KV_KEY_RE = re.compile(r"^/kv/[a-z0-9][a-z0-9_-]{0,47}/[0-9a-f]{14}\s*$", re.IGNORECASE)
SHARD_COUNT_HEADER_RE = re.compile(r"^#\s*(?P<count>\d+)\s+entries\s*$", re.IGNORECASE)
READ_BUDGET_FOOTER_RE = re.compile(
    r"^# budget: \d+ of \d+ reads left this minute "
    r"\(refills (?:\d+\.\d tokens/s|one token every \d+s); "
    r"a 429 states the wait, and the full limits are in "
    r"/\.well-known/agent\.json\)$"
)
HEX_NAME_RE = re.compile(r"^[0-9a-f]{16}$", re.IGNORECASE)
SIGNED_DID_RE = re.compile(r"^did:key:z6Mk[1-9A-HJ-NP-Za-km-z]{20,100}$")
NONCE_RE = re.compile(r"^[0-9]{1,19}$")
ROOM_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,47}$")

PREFIX_CLASSES = (
    ("mb-", "mailbox"),
    ("p-", "unlisted"),
    ("d-", "ownable"),
    ("e-", "ephemeral"),
)
ROOM_READ_BUDGET = 80
PUBLISHED_READS_PER_MINUTE = 600
MAX_READ_SHARE = 0.20
RATE_BUDGET_WINDOW_SECONDS = 60
BASE_READ_BUDGET = ROOM_READ_BUDGET + 2
TOTAL_READ_BUDGET = int(
    PUBLISHED_READS_PER_MINUTE * MAX_READ_SHARE * RATE_BUDGET_WINDOW_SECONDS / 60
)
ROOM_REVISIT_READ_BUDGET = TOTAL_READ_BUDGET - BASE_READ_BUDGET
ROOM_REVISIT_PUBLICATION_BATCH_LIMIT = 305
ROOM_REVISIT_AGED_OUT_FINALIZATION_LIMIT = 2000
TICK_REVISIT_DEADLINE_SECONDS = 300
ROOM_REVISIT_STAGES_SECONDS = (5 * 60, 60 * 60, 24 * 60 * 60)
CENSUS_MAX_PASSES = 5
CENSUS_DEADLINE_SECONDS = 30 * 60
CENSUS_STATE_VERSION = 2
COLLECTOR_VERSION = "2.13.0"
SELECTOR_VERSION = 1
ROOM_ID_HEX_LENGTH = 16
SIGNER_STATE_VERSION = 6
LEDGER_LOCK_TIMEOUT = 15.0
LEDGER_CHECKPOINT_VERSION = 2
LEDGER_PENDING_VERSION = 2
MAX_LEDGER_CHECKPOINT_BYTES = 4 * 1024
MAX_LEDGER_TICK_BYTES = 16 * 1024 * 1024
MAX_LEDGER_PENDING_BYTES = MAX_LEDGER_TICK_BYTES + MAX_LEDGER_CHECKPOINT_BYTES
LEDGER_TAIL_ANCHOR_BYTES = 4 * 1024
TICK_OUTBOX_OPERATION_DOMAIN = b"technocore-observatory:tick-outbox:operation:v1\x00"
_UNCHECKED_LEDGER_LINK = object()
# The daemon fails closed and skips its tick if the signer state is locked; a
# census invocation waits longer because losing its slot discards a completed
# 256-shard walk (the next census run would reset and start over).
SIGNER_LOCK_TIMEOUT = 15.0
CENSUS_SIGNER_LOCK_TIMEOUT = 240.0
CENSUS_STATE_LOCK_TIMEOUT = 15.0
MAX_ORIGIN_RESPONSE_BYTES = 8 * 1024 * 1024
MAX_ERROR_RESPONSE_BYTES = 64 * 1024
RESPONSE_READ_CHUNK_BYTES = 64 * 1024
SQLITE_INTEGER_MAX = (1 << 63) - 1
MAX_RETRIES = 10


class CollectionError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        outcome: str = "collection_error",
        status: int | None = None,
        path: str | None = None,
    ) -> None:
        super().__init__(message)
        self.outcome = outcome
        self.status = status
        self.path = normalize_route(path)[0] if path is not None else None


def utc_now() -> str:
    return (
        datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    )


def parse_timestamp(value: str) -> datetime:
    try:
        normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            raise ValueError("timestamp has no timezone")
        return parsed.astimezone(timezone.utc)
    except OverflowError as error:
        raise ValueError("timestamp is outside the supported range") from error


def parse_collection_date(value: str) -> date:
    if not isinstance(value, str) or re.fullmatch(r"\d{4}-\d{2}-\d{2}", value) is None:
        raise ValueError("collection date is not an ISO date")
    return date.fromisoformat(value)


def parse_bounded_origin_integer(value: str, field: str, path: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise CollectionError(
            f"{path} {field} exceeded the numeric parser limit",
            outcome="invalid_response",
            path=path,
        ) from error
    if parsed > SQLITE_INTEGER_MAX:
        raise CollectionError(
            f"{path} {field} is outside SQLite integer storage",
            outcome="invalid_response",
            path=path,
        )
    return parsed


def parse_size(value: str) -> int:
    match = re.fullmatch(r"(\d+(?:\.\d+)?)([KMGT]?)", value, re.IGNORECASE)
    if not match:
        raise CollectionError(
            "unrecognized byte size",
            outcome="invalid_response",
            path="/rooms",
        )
    multipliers = {
        "": 1,
        "K": 1_000,
        "M": 1_000_000,
        "G": 1_000_000_000,
        "T": 1_000_000_000_000,
    }
    number = match.group(1)
    try:
        with localcontext() as context:
            context.prec = max(1, len(number.replace(".", "")))
            size = Decimal(number) * multipliers[match.group(2).upper()]
    except DecimalException as error:
        raise CollectionError(
            "byte size is outside the supported numeric range",
            outcome="invalid_response",
            path="/rooms",
        ) from error
    if size > SQLITE_INTEGER_MAX:
        raise CollectionError(
            "byte size is outside SQLite integer storage",
            outcome="invalid_response",
            path="/rooms",
        )
    return round(size)


def retry_after_header_delay(error: urllib.error.HTTPError) -> float | None:
    if error.code != 429 or not error.headers:
        return None
    header = error.headers.get("Retry-After")
    if not header:
        return None
    try:
        seconds = float(header)
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(header)
            now = datetime.now(retry_at.tzinfo or timezone.utc)
            seconds = (retry_at - now).total_seconds()
        except (TypeError, ValueError, OverflowError):
            return None
    if not math.isfinite(seconds):
        return None
    return min(300.0, max(0.0, seconds))


def retry_delay(error: urllib.error.HTTPError, body: str, attempt: int) -> float:
    header_delay = retry_after_header_delay(error)
    if header_delay is not None:
        return header_delay
    if error.code == 429:
        body_match = RETRY_BODY_RE.search(body)
        if body_match:
            return min(300.0, max(0.0, float(body_match.group(1))))
    return min(30.0, 2.0**attempt)


class NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, message, headers, new_url):
        return None


ORIGIN_OPENER = urllib.request.build_opener(NoRedirectHandler())


def open_origin(request: urllib.request.Request, timeout: float):
    return ORIGIN_OPENER.open(request, timeout=timeout)


def normalize_base_url(base_url: str) -> str:
    if not isinstance(base_url, str) or not base_url or len(base_url) > 2_048:
        raise ValueError("base URL must be a bounded HTTP(S) origin")
    if any(character.isspace() or ord(character) < 32 for character in base_url):
        raise ValueError("base URL must not contain whitespace or control characters")
    parsed = urllib.parse.urlsplit(base_url)
    if (
        parsed.scheme.lower() not in ("http", "https")
        or parsed.hostname is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in ("", "/")
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("base URL must be an HTTP(S) origin without credentials")
    try:
        port = parsed.port
    except ValueError as error:
        raise ValueError("base URL has an invalid port") from error
    hostname = parsed.hostname.lower()
    if ":" in hostname:
        hostname = f"[{hostname}]"
    authority = hostname if port is None else f"{hostname}:{port}"
    return f"{parsed.scheme.lower()}://{authority}"


def set_response_timeout(response: Any, timeout: float) -> None:
    fp = getattr(response, "fp", None)
    raw = getattr(fp, "raw", None)
    sock = getattr(raw, "_sock", None)
    settimeout = getattr(sock, "settimeout", None)
    if callable(settimeout):
        settimeout(timeout)


def read_response_body(
    response: Any,
    maximum_bytes: int,
    deadline: float,
) -> tuple[bytes, bool]:
    chunks: list[bytes] = []
    total = 0
    read = getattr(response, "read1", None)
    if not callable(read):
        read = response.read

    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("response deadline exceeded")
        set_response_timeout(response, remaining)
        chunk = read(min(RESPONSE_READ_CHUNK_BYTES, maximum_bytes + 1 - total))
        if time.monotonic() > deadline:
            raise TimeoutError("response deadline exceeded")
        if not chunk:
            return b"".join(chunks), False
        if not isinstance(chunk, bytes):
            raise CollectionError("origin response did not contain bytes")
        chunks.append(chunk)
        total += len(chunk)
        if total > maximum_bytes:
            return b"".join(chunks), True


class Client:
    def __init__(
        self,
        base_url: str,
        timeout: float,
        retries: int,
        *,
        telemetry: TelemetryStore | None = None,
        cycle_id: int | None = None,
    ) -> None:
        if (telemetry is None) != (cycle_id is None):
            raise ValueError("telemetry and cycle_id must be provided together")
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not math.isfinite(timeout)
            or timeout <= 0
        ):
            raise ValueError("timeout must be a finite positive number")
        if (
            isinstance(retries, bool)
            or not isinstance(retries, int)
            or retries < 0
            or retries > MAX_RETRIES
        ):
            raise ValueError(f"retries must be an integer from 0 to {MAX_RETRIES}")
        self.base_url = normalize_base_url(base_url)
        self.timeout = timeout
        self.retries = retries
        self.telemetry = telemetry
        self.cycle_id = cycle_id
        self.last_attempt_id: int | None = None
        self.last_attempt_route: str | None = None
        self.telemetry_degraded = False

    def _record_attempt(
        self,
        path: str,
        attempt: int,
        observed_at: str,
        started_at: float,
        outcome: str,
        status: int | None,
    ) -> None:
        self.last_attempt_id = None
        self.last_attempt_route = normalize_route(path)[0]
        if self.telemetry is None or self.cycle_id is None:
            return
        route, metered = normalize_route(path)
        try:
            self.last_attempt_id = self.telemetry.record_attempt(
                self.cycle_id,
                route,
                metered,
                attempt,
                observed_at,
                max(0.0, (time.monotonic() - started_at) * 1000),
                outcome,
                status,
            )
        except (OSError, sqlite3.Error):
            self.telemetry_degraded = True
            self.telemetry = None
            self.cycle_id = None

    def mark_response_invalid(self, path: str) -> None:
        route = normalize_route(path)[0]
        if self.last_attempt_route != route:
            raise ValueError("parser route does not match the latest request attempt")
        if self.telemetry is None or self.last_attempt_id is None:
            return
        try:
            self.telemetry.update_attempt_outcome(
                self.last_attempt_id,
                "invalid_response",
            )
        except (OSError, sqlite3.Error):
            self.telemetry_degraded = True
            self.telemetry = None
            self.cycle_id = None

    def get(self, path: str, deadline: float | None = None) -> str:
        self.last_attempt_id = None
        self.last_attempt_route = None
        parsed_path = urllib.parse.urlsplit(path)
        if (
            not path.startswith("/")
            or len(path.encode("utf-8")) > 2_048
            or parsed_path.scheme
            or parsed_path.netloc
            or parsed_path.fragment
        ):
            raise ValueError("path must be a bounded origin-relative request target")
        url = self.base_url + path
        for attempt in range(self.retries + 1):
            remaining = None if deadline is None else deadline - time.monotonic()
            if remaining is not None and remaining <= 0:
                raise CollectionError(
                    f"GET {path} exceeded its deadline",
                    outcome="deadline",
                    path=path,
                )

            request = urllib.request.Request(
                url,
                headers={
                    "Accept": "text/plain, application/json",
                    "User-Agent": f"technocore-observatory/{COLLECTOR_VERSION}",
                },
                method="GET",
            )
            observed_at = utc_now()
            started_at = time.monotonic()
            attempt_deadline = started_at + self.timeout
            if deadline is not None:
                attempt_deadline = min(attempt_deadline, deadline)
            status: int | None = None
            try:
                timeout = attempt_deadline - time.monotonic()
                if timeout <= 0:
                    raise TimeoutError("request deadline exceeded")
                with open_origin(request, timeout=timeout) as response:
                    status = getattr(response, "status", None)
                    if status is None:
                        status = response.getcode()
                    body, truncated = read_response_body(
                        response,
                        MAX_ORIGIN_RESPONSE_BYTES,
                        attempt_deadline,
                    )
                    if truncated:
                        self._record_attempt(
                            path,
                            attempt + 1,
                            observed_at,
                            started_at,
                            "invalid_response",
                            status,
                        )
                        raise CollectionError(
                            f"GET {path} exceeded the response byte limit",
                            outcome="invalid_response",
                            status=status,
                            path=path,
                        )
                    if not body:
                        self._record_attempt(
                            path,
                            attempt + 1,
                            observed_at,
                            started_at,
                            "empty_response",
                            status,
                        )
                        raise CollectionError(
                            f"empty response from {path}",
                            outcome="empty_response",
                            status=status,
                            path=path,
                        )
                    try:
                        decoded = body.decode("utf-8")
                    except UnicodeDecodeError as error:
                        self._record_attempt(
                            path,
                            attempt + 1,
                            observed_at,
                            started_at,
                            "decode_error",
                            status,
                        )
                        if attempt >= self.retries:
                            raise CollectionError(
                                f"GET {path} failed: {error}",
                                outcome="decode_error",
                                status=status,
                                path=path,
                            ) from error
                        delay = min(30.0, 2.0**attempt)
                    else:
                        self._record_attempt(
                            path,
                            attempt + 1,
                            observed_at,
                            started_at,
                            "success",
                            status,
                        )
                        return decoded
            except urllib.error.HTTPError as error:
                body = ""
                header_delay = retry_after_header_delay(error)
                try:
                    if error.code == 429 and header_delay is None:
                        try:
                            error_body, _ = read_response_body(
                                error,
                                MAX_ERROR_RESPONSE_BYTES,
                                attempt_deadline,
                            )
                        except (OSError, http.client.HTTPException, CollectionError):
                            body = ""
                        else:
                            body = error_body.decode("utf-8", errors="replace")
                finally:
                    error.close()

                if 300 <= error.code <= 399:
                    self._record_attempt(
                        path,
                        attempt + 1,
                        observed_at,
                        started_at,
                        "invalid_response",
                        error.code,
                    )
                    raise CollectionError(
                        f"GET {path} refused HTTP redirect {error.code}",
                        outcome="invalid_response",
                        status=error.code,
                        path=path,
                    ) from error
                self._record_attempt(
                    path,
                    attempt + 1,
                    observed_at,
                    started_at,
                    "http_error",
                    error.code,
                )
                retryable = error.code == 429 or 500 <= error.code <= 599
                if not retryable or attempt >= self.retries:
                    raise CollectionError(
                        f"GET {path} failed with HTTP {error.code}",
                        outcome="http_error",
                        status=error.code,
                        path=path,
                    ) from error
                delay = (
                    header_delay
                    if header_delay is not None
                    else retry_delay(error, body, attempt)
                )
            except TimeoutError as error:
                self._record_attempt(
                    path,
                    attempt + 1,
                    observed_at,
                    started_at,
                    "timeout",
                    None,
                )
                if attempt >= self.retries:
                    outcome = (
                        "deadline"
                        if deadline is not None and time.monotonic() >= deadline
                        else "timeout"
                    )
                    raise CollectionError(
                        f"GET {path} failed: {error}",
                        outcome=outcome,
                        path=path,
                    ) from error
                delay = min(30.0, 2.0**attempt)
            except urllib.error.URLError as error:
                outcome = (
                    "timeout"
                    if isinstance(error.reason, TimeoutError)
                    else "transport_error"
                )
                self._record_attempt(
                    path,
                    attempt + 1,
                    observed_at,
                    started_at,
                    outcome,
                    None,
                )
                if attempt >= self.retries:
                    raise CollectionError(
                        f"GET {path} failed: {error}",
                        outcome=outcome,
                        path=path,
                    ) from error
                delay = min(30.0, 2.0**attempt)
            except (OSError, http.client.HTTPException) as error:
                self._record_attempt(
                    path,
                    attempt + 1,
                    observed_at,
                    started_at,
                    "transport_error",
                    None,
                )
                if attempt >= self.retries:
                    raise CollectionError(
                        f"GET {path} failed: {error}",
                        outcome="transport_error",
                        status=status,
                        path=path,
                    ) from error
                delay = min(30.0, 2.0**attempt)

            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0 or delay >= remaining:
                    raise CollectionError(
                        f"GET {path} exceeded its deadline",
                        outcome="deadline",
                        path=path,
                    )
            time.sleep(delay)
        raise AssertionError("retry loop exhausted unexpectedly")


def parse_collected_response(
    client: Any,
    path: str,
    parser: Callable[..., Any],
    *args: Any,
) -> Any:
    try:
        return parser(*args)
    except CollectionError as error:
        mark_response_invalid = getattr(client, "mark_response_invalid", None)
        if callable(mark_response_invalid):
            mark_response_invalid(path)
        error.outcome = "invalid_response"
        error.status = None
        error.path = normalize_route(path)[0]
        raise


def parse_idle(value: Any) -> float:
    if isinstance(value, bool):
        raise CollectionError("boolean idle value", outcome="invalid_response")
    if isinstance(value, (int, float)) and value >= 0:
        try:
            idle_seconds = float(value)
        except OverflowError as error:
            raise CollectionError(
                "idle value is not finite",
                outcome="invalid_response",
            ) from error
        if math.isfinite(idle_seconds):
            return idle_seconds
        raise CollectionError("idle value is not finite", outcome="invalid_response")
    if not isinstance(value, str):
        raise CollectionError("idle value is not numeric", outcome="invalid_response")
    match = IDLE_RE.fullmatch(value.strip())
    if not match:
        raise CollectionError(
            "unrecognized idle value",
            outcome="invalid_response",
        )
    multipliers = {"": 1, "s": 1, "m": 60, "h": 3600, "d": 86400}
    idle_seconds = float(match.group(1)) * multipliers[match.group(2).lower()]
    if not math.isfinite(idle_seconds):
        raise CollectionError("idle value is not finite", outcome="invalid_response")
    return idle_seconds


def room_from_object(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    name = value.get("name", value.get("room"))
    seq = value.get("seq", value.get("last_seq"))
    idle = value.get("idle", value.get("idle_seconds"))
    if (
        not isinstance(name, str)
        or ROOM_NAME_RE.fullmatch(name) is None
        or isinstance(seq, bool)
        or not isinstance(seq, int)
    ):
        return None
    if seq < 0 or seq > SQLITE_INTEGER_MAX:
        return None
    return {"name": name, "seq": seq, "idle_seconds": parse_idle(idle)}


def parse_room_row(line: str) -> dict[str, Any] | None:
    try:
        parsed = json.loads(line)
    except json.JSONDecodeError:
        parsed = None
    except (ValueError, RecursionError) as error:
        raise CollectionError(
            "/rooms row exceeded the JSON parser limits",
            outcome="invalid_response",
            path="/rooms",
        ) from error

    room = room_from_object(parsed)
    if room is not None:
        return room

    name_match = re.match(r"^(\S+)", line)
    seq_match = re.search(r"\b(?:last_)?seq\b\s*[:=]?\s*(\d+)", line, re.IGNORECASE)
    idle_match = re.search(
        r"\bidle(?:_seconds)?\b\s*[:=]?\s*(\d+(?:\.\d+)?[smhd]?)",
        line,
        re.IGNORECASE,
    )
    if (
        not name_match
        or ROOM_NAME_RE.fullmatch(name_match.group(1)) is None
        or not seq_match
        or not idle_match
    ):
        return None
    seq = parse_bounded_origin_integer(seq_match.group(1), "row sequence", "/rooms")
    return {
        "name": name_match.group(1),
        "seq": seq,
        "idle_seconds": parse_idle(idle_match.group(1)),
    }


def parse_rooms_json(body: str) -> dict[str, Any]:
    try:
        payload = json.loads(body)
    except (ValueError, RecursionError) as error:
        raise CollectionError(
            f"/rooms JSON did not parse: {error}",
            outcome="invalid_response",
            path="/rooms",
        ) from error
    if not isinstance(payload, dict):
        raise CollectionError("/rooms JSON is not an object")
    notes = payload.get("notes")
    if not isinstance(notes, dict):
        raise CollectionError("/rooms JSON is missing the notes counters")

    room_counters: dict[str, int] = {}
    for key in ("total", "capacity", "bytes"):
        value = payload.get(key)
        if (
            isinstance(value, bool)
            or not isinstance(value, int)
            or value < 0
            or value > SQLITE_INTEGER_MAX
        ):
            raise CollectionError(
                f"/rooms JSON has an invalid room counter: {key}",
                outcome="invalid_response",
                path="/rooms",
            )
        room_counters[key] = value

    note_counters: dict[str, int] = {}
    for key in ("total", "capacity", "bytes"):
        value = notes.get(key)
        if (
            isinstance(value, bool)
            or not isinstance(value, int)
            or value < 0
            or value > SQLITE_INTEGER_MAX
        ):
            raise CollectionError(
                f"/rooms JSON has an invalid note counter: {key}",
                outcome="invalid_response",
                path="/rooms",
            )
        note_counters[key] = value
    if (
        room_counters["total"] == 0
        or room_counters["capacity"] == 0
        or note_counters["capacity"] == 0
    ):
        raise CollectionError(
            "/rooms JSON requires positive room totals and capacities",
            outcome="invalid_response",
            path="/rooms",
        )

    rows = payload.get("rooms")
    if not isinstance(rows, list):
        raise CollectionError("/rooms JSON is missing its rooms array")
    if len(rows) > 200:
        raise CollectionError("/rooms returned more than the documented 200-record cap")
    rooms: list[dict[str, Any]] = []
    room_names: set[str] = set()
    for entry in rows:
        room = room_from_object(entry)
        if room is None:
            raise CollectionError(
                "/rooms JSON contains an invalid room row",
                outcome="invalid_response",
                path="/rooms",
            )
        if room["name"] in room_names:
            raise CollectionError(
                "/rooms JSON contains duplicate room names",
                outcome="invalid_response",
                path="/rooms",
            )
        room_names.add(room["name"])
        rooms.append(room)
    if len(rooms) > room_counters["total"]:
        raise CollectionError(
            "/rooms JSON returned more rows than its declared total",
            outcome="invalid_response",
            path="/rooms",
        )

    engagement = payload.get("engagement")
    if isinstance(engagement, dict):
        try:
            json.dumps(
                engagement,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        except (ValueError, UnicodeEncodeError, RecursionError) as error:
            raise CollectionError(
                "/rooms engagement is not canonical JSON",
                outcome="invalid_response",
                path="/rooms",
            ) from error
    return {
        "rooms_total": room_counters["total"],
        "room_cap": room_counters["capacity"],
        "bytes_stored": room_counters["bytes"],
        "notes_total": note_counters["total"],
        "note_cap": note_counters["capacity"],
        "newest_rooms": rooms,
        "engagement": engagement if isinstance(engagement, dict) else None,
    }


def parse_rooms(body: str) -> dict[str, Any]:
    if body.lstrip().startswith("{"):
        return parse_rooms_json(body)
    rooms_match = ROOMS_HEADER_RE.search(body)
    notes_match = NOTES_HEADER_RE.search(body)
    if not rooms_match or not notes_match:
        raise CollectionError(
            "/rooms response is missing a recognized rooms or notes header"
        )

    rooms: list[dict[str, Any]] = []
    unparsed_rows = 0
    for raw_line in body.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        room = parse_room_row(line)
        if room is None:
            unparsed_rows += 1
        else:
            rooms.append(room)

    shown = parse_bounded_origin_integer(
        rooms_match.group("shown"), "shown count", "/rooms"
    )
    rooms_total = parse_bounded_origin_integer(
        rooms_match.group("total"), "room total", "/rooms"
    )
    if shown > 200:
        raise CollectionError("/rooms returned more than the documented 200-record cap")
    if shown > rooms_total:
        raise CollectionError(
            "/rooms declared more shown rows than total rooms",
            outcome="invalid_response",
            path="/rooms",
        )
    if unparsed_rows or len(rooms) != shown:
        raise CollectionError(
            f"/rooms declared {shown} rows; parsed {len(rooms)} with {unparsed_rows} unrecognized"
        )
    if len({room["name"] for room in rooms}) != len(rooms):
        raise CollectionError(
            "/rooms contains duplicate room names",
            outcome="invalid_response",
            path="/rooms",
        )

    room_cap = parse_bounded_origin_integer(
        rooms_match.group("cap"), "room capacity", "/rooms"
    )
    notes_total = parse_bounded_origin_integer(
        notes_match.group("total"), "note total", "/rooms"
    )
    note_cap = parse_bounded_origin_integer(
        notes_match.group("cap"), "note capacity", "/rooms"
    )
    if rooms_total == 0 or room_cap == 0 or note_cap == 0:
        raise CollectionError(
            "/rooms requires positive room totals and capacities",
            outcome="invalid_response",
            path="/rooms",
        )

    return {
        "rooms_total": rooms_total,
        "room_cap": room_cap,
        "bytes_stored": parse_size(rooms_match.group("stored")),
        "notes_total": notes_total,
        "note_cap": note_cap,
        "newest_rooms": rooms,
    }


def extract_last_seq(body: str, path: str) -> int:
    candidates: list[int] = []
    stripped = body.lstrip()
    if stripped.startswith("{"):
        messages = parse_room_messages(body, path)
        candidates.extend(message["seq"] for message in messages)
        try:
            payload = json.loads(body)
        except (ValueError, RecursionError) as error:
            raise CollectionError(
                f"{path} exceeded the JSON parser limits",
                outcome="invalid_response",
                path=path,
            ) from error
        if "last_seq" in payload:
            last_seq = payload["last_seq"]
            if (
                isinstance(last_seq, bool)
                or not isinstance(last_seq, int)
                or last_seq < 0
                or last_seq > SQLITE_INTEGER_MAX
            ):
                raise CollectionError(
                    f"{path} contains an invalid top-level last_seq",
                    outcome="invalid_response",
                    path=path,
                )
            candidates.append(last_seq)
    else:
        rows: list[Any] = []
        for raw_line in body.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            header_match = LAST_SEQ_HEADER_RE.fullmatch(line)
            if header_match is not None:
                candidates.append(
                    parse_bounded_origin_integer(
                        header_match.group(1), "sequence", path
                    )
                )
                continue
            if line.startswith("#"):
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
            except (ValueError, RecursionError) as error:
                raise CollectionError(
                    f"{path} exceeded the JSON parser limits",
                    outcome="invalid_response",
                    path=path,
                ) from error
        if rows:
            messages = parse_room_messages(
                json.dumps({"messages": rows}, ensure_ascii=False),
                path,
            )
            candidates.extend(message["seq"] for message in messages)

    if not candidates:
        raise CollectionError(f"{path} response contains no recognized sequence number")
    return max(candidates)


def classify_name(name: str) -> tuple[list[str], str, str]:
    remaining = name
    classes: list[str] = []

    while True:
        matched = False
        for prefix, label in PREFIX_CLASSES:
            if remaining.startswith(prefix):
                classes.append(label)
                remaining = remaining[len(prefix) :]
                matched = True
                break
        if not matched:
            break

    if classes:
        primary = classes[0]
    elif HEX_NAME_RE.fullmatch(remaining):
        primary = "bare_hex"
    else:
        primary = "human_or_other"

    return classes, primary, remaining


def parse_events(
    body: str,
) -> tuple[int, list[dict[str, Any]], dict[str, int], dict[str, int]]:
    events: list[dict[str, Any]] = []
    seen: set[int] = set()

    rows: list[Any]
    stripped = body.lstrip()
    if stripped.startswith("{"):
        try:
            envelope = json.loads(stripped)
        except (ValueError, RecursionError) as error:
            raise CollectionError(
                "/r/events did not parse as JSON",
                outcome="invalid_response",
                path="/r/events",
            ) from error
        messages = envelope.get("messages") if isinstance(envelope, dict) else None
        if not isinstance(messages, list):
            raise CollectionError("/r/events JSON is missing its messages array")
        if envelope.get("room") != "events":
            raise CollectionError(
                "/r/events JSON room envelope does not match the request target",
                outcome="invalid_response",
                path="/r/events",
            )
        rows = messages
    else:
        rows = []
        for raw_line in body.splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            try:
                rows.append(json.loads(line))
            except (ValueError, RecursionError) as error:
                raise CollectionError(
                    "/r/events contains a non-JSON event row",
                    outcome="invalid_response",
                    path="/r/events",
                ) from error

    if len(rows) > 200:
        raise CollectionError(
            "/r/events returned more than the documented 200-record cap"
        )

    for value in rows:
        if not isinstance(value, dict):
            raise CollectionError("/r/events contains a non-object event")
        seq = value.get("seq")
        ts = value.get("ts")
        sender = value.get("from")
        text = value.get("text")
        if (
            isinstance(seq, bool)
            or not isinstance(seq, int)
            or seq < 0
            or seq > SQLITE_INTEGER_MAX
            or not isinstance(ts, str)
            or sender != "server"
            or not isinstance(text, str)
            or not text.startswith("created ")
            or not text[8:]
        ):
            raise CollectionError(
                "/r/events contains an event with an unexpected shape",
                outcome="invalid_response",
                path="/r/events",
            )
        try:
            created_at = parse_timestamp(ts)
            created_at + timedelta(seconds=max(ROOM_REVISIT_STAGES_SECONDS))
        except (ValueError, OverflowError) as error:
            raise CollectionError(
                "/r/events contains an invalid timestamp",
                outcome="invalid_response",
                path="/r/events",
            ) from error
        if seq in seen:
            raise CollectionError("/r/events contains duplicate sequence numbers")
        seen.add(seq)

        name = text[8:]
        if ROOM_NAME_RE.fullmatch(name) is None:
            raise CollectionError("/r/events contains an invalid room name")
        classes, primary, base_name = classify_name(name)
        events.append(
            {
                "seq": seq,
                "ts": ts,
                "name": name,
                "classes": classes,
                "primary_class": primary,
                "base_name": base_name,
            }
        )

    if not events:
        raise CollectionError("/r/events contains no event rows")
    events.sort(key=lambda event: event["seq"])
    class_counts = {
        "unlisted": 0,
        "mailbox": 0,
        "ownable": 0,
        "ephemeral": 0,
        "bare_hex": 0,
        "human_or_other": 0,
    }
    primary_counts = dict.fromkeys(class_counts, 0)

    for event in events:
        if event["classes"]:
            for label in set(event["classes"]):
                class_counts[label] += 1
        else:
            class_counts[event["primary_class"]] += 1
        primary_counts[event["primary_class"]] += 1

    return events[-1]["seq"], events, class_counts, primary_counts


def parse_room_messages(body: str, path: str) -> list[dict[str, Any]]:
    try:
        payload = json.loads(body)
    except (ValueError, RecursionError) as error:
        raise CollectionError(
            f"{path} did not parse as JSON",
            outcome="invalid_response",
            path=path,
        ) from error
    messages = payload.get("messages") if isinstance(payload, dict) else None
    if not isinstance(messages, list):
        raise CollectionError(
            f"{path} JSON is missing its messages array",
            outcome="invalid_response",
            path=path,
        )
    if "room" in payload:
        request_path = urllib.parse.urlsplit(path).path
        if not request_path.startswith("/r/"):
            raise CollectionError(
                f"{path} cannot bind its room envelope to the request target",
                outcome="invalid_response",
                path=path,
            )
        try:
            requested_room = urllib.parse.unquote_to_bytes(request_path[3:]).decode(
                "utf-8"
            )
        except UnicodeDecodeError as error:
            raise CollectionError(
                f"{path} contains an invalid encoded room target",
                outcome="invalid_response",
                path=path,
            ) from error
        envelope_room = payload["room"]
        if (
            not isinstance(envelope_room, str)
            or ROOM_NAME_RE.fullmatch(envelope_room) is None
            or ROOM_NAME_RE.fullmatch(requested_room) is None
            or envelope_room != requested_room
        ):
            raise CollectionError(
                f"{path} room envelope does not match the request target",
                outcome="invalid_response",
                path=path,
            )
    if len(messages) > 200:
        raise CollectionError(
            f"{path} returned more than the documented 200-record cap",
            outcome="invalid_response",
            path=path,
        )

    result: list[dict[str, Any]] = []
    seen: set[int] = set()
    for position, value in enumerate(messages):
        if not isinstance(value, dict):
            raise CollectionError(
                f"{path} contains a non-object message",
                outcome="invalid_response",
                path=path,
            )
        seq = value.get("seq")
        ts = value.get("ts")
        sender = value.get("from")
        text = value.get("text")
        if (
            isinstance(seq, bool)
            or not isinstance(seq, int)
            or seq < 0
            or seq > SQLITE_INTEGER_MAX
            or seq in seen
            or not isinstance(ts, str)
            or not isinstance(sender, str)
            or not isinstance(text, str)
        ):
            raise CollectionError(
                f"{path} contains a message with invalid required fields",
                outcome="invalid_response",
                path=path,
            )
        try:
            parsed_ts = parse_timestamp(ts)
        except ValueError as error:
            raise CollectionError(
                f"{path} contains an invalid message timestamp",
                outcome="invalid_response",
                path=path,
            ) from error
        # The nonce is the signature marker: the service verifies `from` only
        # on the signed lane, and a nonce appears only on signed messages. It
        # is documented as 1-19 digits; a numeric echo is normalized, anything
        # else is treated as absent.
        raw_nonce = value.get("nonce")
        if (
            isinstance(raw_nonce, int)
            and not isinstance(raw_nonce, bool)
            and raw_nonce >= 0
        ):
            raw_nonce = str(raw_nonce)
        nonce = (
            raw_nonce
            if isinstance(raw_nonce, str) and NONCE_RE.fullmatch(raw_nonce)
            else None
        )
        seen.add(seq)
        result.append(
            {
                "seq": seq,
                "ts": ts,
                "_datetime": parsed_ts,
                "from": sender,
                "nonce": nonce,
                "position": position,
            }
        )
    result.sort(key=lambda message: message["seq"])
    return result


def parse_shard_count(body: str, shard: str) -> int:
    path = f"/kv/{shard}"

    def normalized_item(value: Any) -> str | None:
        if not isinstance(value, str):
            return None
        value = value.strip()
        if re.fullmatch(r"[0-9a-f]{14}", value, re.IGNORECASE):
            return value.lower()
        if (
            KV_KEY_RE.fullmatch(value) is not None
            and value.split("/", 3)[2].lower() == shard.lower()
        ):
            return value.rsplit("/", 1)[-1].lower()
        return None

    def unique_items(values: list[Any]) -> bool:
        normalized = [normalized_item(value) for value in values]
        return None not in normalized and len(set(normalized)) == len(normalized)

    lowered = body.lower()
    if "truncat" in lowered or "more results" in lowered or "next page" in lowered:
        raise CollectionError(f"{shard} reports a truncated result")

    parsed_json = True
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        parsed_json = False
        parsed = None
    except (ValueError, RecursionError) as error:
        raise CollectionError(
            f"{shard} exceeded the JSON parser limits",
            outcome="invalid_response",
            path=path,
        ) from error

    if isinstance(parsed, list):
        if not unique_items(parsed):
            raise CollectionError(
                f"{shard} contains unexpected JSON values",
                outcome="invalid_response",
                path=path,
            )
        return len(parsed)

    if isinstance(parsed, dict):
        if "keys" in parsed:
            keys = parsed.get("keys")
            if (
                set(parsed) != {"ns", "keys"}
                or parsed.get("ns") != shard
                or not isinstance(keys, list)
                or not all(
                    isinstance(key, str)
                    and re.fullmatch(r"[0-9a-f]{14}", key, re.IGNORECASE)
                    for key in keys
                )
                or len({key.lower() for key in keys}) != len(keys)
            ):
                raise CollectionError(
                    f"{shard} JSON contains an invalid key listing",
                    outcome="invalid_response",
                    path=path,
                )
            return len(keys)

        if "items" in parsed:
            items = parsed.get("items")
            total = parsed.get("total")
            if (
                set(parsed) != {"items", "total"}
                or not isinstance(items, list)
                or not unique_items(items)
                or isinstance(total, bool)
                or not isinstance(total, int)
                or total > SQLITE_INTEGER_MAX
                or total != len(items)
            ):
                raise CollectionError(
                    f"{shard} JSON indicates an incomplete listing",
                    outcome="invalid_response",
                    path=path,
                )
            return total

    if parsed_json:
        raise CollectionError(
            f"{shard} JSON has an unrecognized listing shape",
            outcome="invalid_response",
            path=path,
        )

    if not body.strip():
        return 0

    declared: int | None = None
    rows = 0
    seen: set[str] = set()
    saw_budget_footer = False
    for raw_line in body.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if saw_budget_footer:
            raise CollectionError(
                f"{shard} contains content after the budget footer",
                outcome="invalid_response",
                path=path,
            )
        if line.startswith("#"):
            count_match = SHARD_COUNT_HEADER_RE.fullmatch(line)
            if count_match is not None and declared is None:
                declared = parse_bounded_origin_integer(
                    count_match.group("count"), "declared count", path
                )
                continue
            if READ_BUDGET_FOOTER_RE.fullmatch(line) is not None:
                saw_budget_footer = True
                continue
            raise CollectionError(
                f"{shard} contains an unrecognized plaintext comment",
                outcome="invalid_response",
                path=path,
            )
        if (
            KV_KEY_RE.fullmatch(line) is None
            or line.split("/", 3)[2].lower() != shard.lower()
        ):
            raise CollectionError(
                f"{shard} contains an unrecognized plaintext row",
                outcome="invalid_response",
                path=path,
            )
        key = line.rsplit("/", 1)[-1].lower()
        if key in seen:
            raise CollectionError(
                f"{shard} contains a duplicate plaintext key",
                outcome="invalid_response",
                path=path,
            )
        seen.add(key)
        rows += 1

    if declared is None and rows == 0 and not saw_budget_footer:
        raise CollectionError(
            f"{shard} plaintext has no recognized listing",
            outcome="invalid_response",
            path=path,
        )
    if declared is not None and rows != declared:
        raise CollectionError(
            f"{shard} declared {declared} rows but returned {rows}",
            outcome="invalid_response",
            path=path,
        )
    return rows


def _ledger_checkpoint_path(path: Path) -> Path:
    return path.with_name(path.name + ".ledger-checkpoint.json")


def _ledger_pending_path(path: Path) -> Path:
    return path.with_name(path.name + ".ledger-pending.json")


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"duplicate JSON object key {key!r}")
        value[key] = item
    return value


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON number {value}")


def _validate_ledger_checkpoint(value: Any) -> dict[str, Any]:
    version = value.get("version") if isinstance(value, dict) else None
    expected_fields = (
        {"version", "ledger", "tip"}
        if version == 1
        else {
            "version",
            "ledger",
            "tip",
            "operation_id",
            "user_payload_sha256",
        }
    )
    if (
        not isinstance(value, dict)
        or set(value) != expected_fields
        or isinstance(version, bool)
        or version not in (1, LEDGER_CHECKPOINT_VERSION)
    ):
        raise CollectionError("tick ledger checkpoint has an unexpected shape")

    if version == LEDGER_CHECKPOINT_VERSION:
        operation_id = value["operation_id"]
        if operation_id is not None and (
            not isinstance(operation_id, str)
            or re.fullmatch(r"[0-9a-f]{64}", operation_id) is None
        ):
            raise CollectionError("tick ledger checkpoint has an invalid operation ID")
        user_payload_sha256 = value["user_payload_sha256"]
        if (
            not isinstance(user_payload_sha256, str)
            or re.fullmatch(r"[0-9a-f]{64}", user_payload_sha256) is None
        ):
            raise CollectionError(
                "tick ledger checkpoint has an invalid user payload hash"
            )

    ledger = value["ledger"]
    tip = value["tip"]
    if not isinstance(ledger, dict) or set(ledger) != {
        "device",
        "inode",
        "size",
        "mtime_ns",
        "ctime_ns",
    }:
        raise CollectionError("tick ledger checkpoint has invalid file metadata")
    if not isinstance(tip, dict) or set(tip) != {
        "offset",
        "length",
        "canonical_sha256",
        "tick_sha256",
    }:
        raise CollectionError("tick ledger checkpoint has invalid tip metadata")

    for field in ("device", "inode"):
        item = ledger[field]
        if (
            isinstance(item, bool)
            or not isinstance(item, int)
            or item < 0
            or item > (1 << 64) - 1
        ):
            raise CollectionError("tick ledger checkpoint has invalid file metadata")
    for field in ("size", "mtime_ns", "ctime_ns"):
        item = ledger[field]
        if (
            isinstance(item, bool)
            or not isinstance(item, int)
            or item < 0
            or item > SQLITE_INTEGER_MAX
        ):
            raise CollectionError("tick ledger checkpoint has invalid file metadata")

    offset = tip["offset"]
    length = tip["length"]
    if (
        isinstance(offset, bool)
        or not isinstance(offset, int)
        or offset < 0
        or offset > SQLITE_INTEGER_MAX
        or isinstance(length, bool)
        or not isinstance(length, int)
        or length <= 0
        or length > MAX_LEDGER_TICK_BYTES
        or offset + length + 1 != ledger["size"]
    ):
        raise CollectionError("tick ledger checkpoint has invalid tip metadata")
    for field in ("canonical_sha256", "tick_sha256"):
        item = tip[field]
        if not isinstance(item, str) or re.fullmatch(r"[0-9a-f]{64}", item) is None:
            raise CollectionError("tick ledger checkpoint has invalid tip metadata")
    return value


def _canonical_checkpoint_bytes(value: dict[str, Any]) -> bytes:
    try:
        payload = (
            json.dumps(
                value,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
            + b"\n"
        )
    except (TypeError, ValueError, UnicodeEncodeError, RecursionError) as error:
        raise CollectionError("tick ledger checkpoint is not canonical JSON") from error
    if len(payload) > MAX_LEDGER_CHECKPOINT_BYTES:
        raise CollectionError("tick ledger checkpoint exceeds its byte limit")
    return payload


def _load_ledger_checkpoint(path: Path) -> dict[str, Any]:
    try:
        with path.open("rb") as source:
            payload = source.read(MAX_LEDGER_CHECKPOINT_BYTES + 1)
    except OSError as error:
        raise CollectionError(f"cannot read tick ledger checkpoint {path}") from error
    if not payload or len(payload) > MAX_LEDGER_CHECKPOINT_BYTES:
        raise CollectionError("tick ledger checkpoint exceeds its byte limit")
    try:
        value = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_json_object,
            parse_constant=_reject_json_constant,
        )
    except (UnicodeDecodeError, ValueError, RecursionError) as error:
        raise CollectionError("tick ledger checkpoint is not valid JSON") from error
    checkpoint = _validate_ledger_checkpoint(value)
    if payload != _canonical_checkpoint_bytes(checkpoint):
        raise CollectionError("tick ledger checkpoint is not canonical JSON")
    return checkpoint


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _save_atomic_ledger_sidecar(path: Path, payload: bytes) -> None:
    temporary = path.with_name(path.name + ".tmp")
    try:
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_BINARY", 0),
            0o600,
        )
        try:
            if hasattr(os, "fchmod"):
                os.fchmod(descriptor, 0o600)
            remaining = memoryview(payload)
            while remaining:
                written = os.write(descriptor, remaining)
                if written == 0:
                    raise OSError("zero-byte write while saving tick ledger sidecar")
                remaining = remaining[written:]
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _save_ledger_checkpoint(path: Path, value: dict[str, Any]) -> None:
    payload = _canonical_checkpoint_bytes(_validate_ledger_checkpoint(value))
    _save_atomic_ledger_sidecar(path, payload)


def _parse_chained_tick(
    payload: bytes,
    *,
    expected_previous: str | None | object = _UNCHECKED_LEDGER_LINK,
) -> tuple[str, str, bytes]:
    if not payload or len(payload) > MAX_LEDGER_TICK_BYTES:
        raise CollectionError("tick ledger tip exceeds its byte limit")
    try:
        record = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_json_object,
            parse_constant=_reject_json_constant,
        )
        if not isinstance(record, dict):
            raise ValueError("tick is not an object")
        canonical = canonical_tick_bytes(record)
        tick_hash_bytes = canonical_tick_hash_bytes(record)
    except (
        UnicodeDecodeError,
        ValueError,
        UnicodeEncodeError,
        RecursionError,
    ) as error:
        raise CollectionError("tick ledger tip does not parse canonically") from error
    if canonical != payload:
        raise CollectionError("tick ledger tip is not canonical JSON")

    chain = record.get(CHAIN_FIELD)
    if not isinstance(chain, dict) or set(chain) != {
        "version",
        "previous_sha256",
        "tick_sha256",
    }:
        raise CollectionError("tick ledger tip has an incomplete chain object")
    previous = chain["previous_sha256"]
    tick_hash = chain["tick_sha256"]
    if (
        isinstance(chain["version"], bool)
        or chain["version"] != CHAIN_VERSION
        or (
            previous is not None
            and (
                not isinstance(previous, str)
                or re.fullmatch(r"[0-9a-f]{64}", previous) is None
            )
        )
        or (
            expected_previous is not _UNCHECKED_LEDGER_LINK
            and previous != expected_previous
        )
        or not isinstance(tick_hash, str)
        or re.fullmatch(r"[0-9a-f]{64}", tick_hash) is None
    ):
        raise CollectionError("tick ledger tip has an invalid chain link")
    expected_tick_hash = hashlib.sha256(tick_hash_bytes).hexdigest()
    if tick_hash != expected_tick_hash:
        raise CollectionError("tick ledger tip has an invalid self-hash")
    user_record = dict(record)
    del user_record[CHAIN_FIELD]
    user_payload = canonical_tick_bytes(user_record)
    return hashlib.sha256(canonical).hexdigest(), tick_hash, user_payload


def _validate_ledger_pending(value: Any) -> dict[str, Any]:
    version = value.get("version") if isinstance(value, dict) else None
    expected_fields = (
        {"version", "base", "record"}
        if version == 1
        else {
            "version",
            "base",
            "record",
            "operation_id",
            "user_payload_sha256",
        }
    )
    if (
        not isinstance(value, dict)
        or set(value) != expected_fields
        or isinstance(version, bool)
        or version not in (1, LEDGER_PENDING_VERSION)
    ):
        raise CollectionError("tick ledger pending journal has an unexpected shape")

    if version == LEDGER_PENDING_VERSION:
        operation_id = value["operation_id"]
        if operation_id is not None and (
            not isinstance(operation_id, str)
            or re.fullmatch(r"[0-9a-f]{64}", operation_id) is None
        ):
            raise CollectionError(
                "tick ledger pending journal has an invalid operation ID"
            )
        user_payload_sha256 = value["user_payload_sha256"]
        if (
            not isinstance(user_payload_sha256, str)
            or re.fullmatch(r"[0-9a-f]{64}", user_payload_sha256) is None
        ):
            raise CollectionError(
                "tick ledger pending journal has an invalid user payload hash"
            )
    base = value["base"]
    if not isinstance(base, dict) or set(base) != {
        "device",
        "inode",
        "size",
        "mtime_ns",
        "ctime_ns",
        "tail_length",
        "tail_sha256",
        "previous_sha256",
    }:
        raise CollectionError("tick ledger pending journal has invalid base metadata")

    device = base["device"]
    inode = base["inode"]
    mtime_ns = base["mtime_ns"]
    ctime_ns = base["ctime_ns"]
    if (device is None) != (inode is None) or (mtime_ns is None) != (device is None):
        raise CollectionError("tick ledger pending journal has invalid file identity")
    if (ctime_ns is None) != (device is None):
        raise CollectionError("tick ledger pending journal has invalid file identity")
    if device is not None:
        for item in (device, inode):
            if (
                isinstance(item, bool)
                or not isinstance(item, int)
                or item < 0
                or item > (1 << 64) - 1
            ):
                raise CollectionError(
                    "tick ledger pending journal has invalid file identity"
                )
        for item in (mtime_ns, ctime_ns):
            if (
                isinstance(item, bool)
                or not isinstance(item, int)
                or item < 0
                or item > SQLITE_INTEGER_MAX
            ):
                raise CollectionError(
                    "tick ledger pending journal has invalid file identity"
                )

    size = base["size"]
    tail_length = base["tail_length"]
    if (
        isinstance(size, bool)
        or not isinstance(size, int)
        or size < 0
        or size > SQLITE_INTEGER_MAX
        or isinstance(tail_length, bool)
        or not isinstance(tail_length, int)
        or tail_length != min(size, LEDGER_TAIL_ANCHOR_BYTES)
        or (device is None and size != 0)
    ):
        raise CollectionError("tick ledger pending journal has invalid base size")
    tail_sha256 = base["tail_sha256"]
    if (
        not isinstance(tail_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", tail_sha256) is None
        or (size == 0 and tail_sha256 != hashlib.sha256(b"").hexdigest())
    ):
        raise CollectionError("tick ledger pending journal has invalid tail anchor")
    previous_hash = base["previous_sha256"]
    if previous_hash is not None and (
        not isinstance(previous_hash, str)
        or re.fullmatch(r"[0-9a-f]{64}", previous_hash) is None
    ):
        raise CollectionError("tick ledger pending journal has invalid chain anchor")
    if size == 0 and previous_hash is not None:
        raise CollectionError("tick ledger pending journal has invalid chain anchor")

    record = value["record"]
    if not isinstance(record, dict):
        raise CollectionError("tick ledger pending journal has an invalid record")
    try:
        tip_payload = canonical_tick_bytes(record)
    except (ValueError, UnicodeEncodeError, RecursionError) as error:
        raise CollectionError(
            "tick ledger pending journal record is not canonical JSON"
        ) from error
    _, _, user_payload = _parse_chained_tick(
        tip_payload,
        expected_previous=previous_hash,
    )
    if (
        version == LEDGER_PENDING_VERSION
        and value["user_payload_sha256"] != hashlib.sha256(user_payload).hexdigest()
    ):
        raise CollectionError(
            "tick ledger pending journal user payload differs from its hash"
        )
    return value


def _canonical_pending_bytes(value: dict[str, Any]) -> bytes:
    try:
        payload = (
            json.dumps(
                value,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
            + b"\n"
        )
    except (TypeError, ValueError, UnicodeEncodeError, RecursionError) as error:
        raise CollectionError(
            "tick ledger pending journal is not canonical JSON"
        ) from error
    if len(payload) > MAX_LEDGER_PENDING_BYTES:
        raise CollectionError("tick ledger pending journal exceeds its byte limit")
    return payload


def _load_ledger_pending(path: Path) -> dict[str, Any]:
    try:
        with path.open("rb") as source:
            payload = source.read(MAX_LEDGER_PENDING_BYTES + 1)
    except OSError as error:
        raise CollectionError(
            f"cannot read tick ledger pending journal {path}"
        ) from error
    if not payload or len(payload) > MAX_LEDGER_PENDING_BYTES:
        raise CollectionError("tick ledger pending journal exceeds its byte limit")
    try:
        value = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_json_object,
            parse_constant=_reject_json_constant,
        )
    except (UnicodeDecodeError, ValueError, RecursionError) as error:
        raise CollectionError(
            "tick ledger pending journal is not valid JSON"
        ) from error
    pending = _validate_ledger_pending(value)
    if payload != _canonical_pending_bytes(pending):
        raise CollectionError("tick ledger pending journal is not canonical JSON")
    return pending


def _save_ledger_pending(path: Path, value: dict[str, Any]) -> None:
    payload = _canonical_pending_bytes(_validate_ledger_pending(value))
    _save_atomic_ledger_sidecar(path, payload)


def _checkpoint_from_tip(
    ledger_stat: os.stat_result,
    tip_offset: int,
    tip_payload: bytes,
    tick_hash: str,
    operation_id: str | None,
    user_payload_sha256: str,
) -> dict[str, Any]:
    checkpoint = {
        "version": LEDGER_CHECKPOINT_VERSION,
        "operation_id": operation_id,
        "user_payload_sha256": user_payload_sha256,
        "ledger": {
            "device": int(ledger_stat.st_dev),
            "inode": int(ledger_stat.st_ino),
            "size": int(ledger_stat.st_size),
            "mtime_ns": int(ledger_stat.st_mtime_ns),
            "ctime_ns": int(ledger_stat.st_ctime_ns),
        },
        "tip": {
            "offset": tip_offset,
            "length": len(tip_payload),
            "canonical_sha256": hashlib.sha256(tip_payload).hexdigest(),
            "tick_sha256": tick_hash,
        },
    }
    return _validate_ledger_checkpoint(checkpoint)


def _ledger_tail_anchor(
    path: Path,
    ledger_size: int,
    expected_stat: os.stat_result | None,
) -> bytes:
    if expected_stat is None:
        if ledger_size != 0 or path.exists():
            raise CollectionError("tick ledger appeared before pending journal")
        return b""
    try:
        with path.open("rb") as source:
            current_stat = os.fstat(source.fileno())
            if (
                current_stat.st_dev != expected_stat.st_dev
                or current_stat.st_ino != expected_stat.st_ino
                or current_stat.st_size != ledger_size
                or current_stat.st_mtime_ns != expected_stat.st_mtime_ns
                or current_stat.st_ctime_ns != expected_stat.st_ctime_ns
            ):
                raise CollectionError("tick ledger changed before pending journal")
            tail_length = min(ledger_size, LEDGER_TAIL_ANCHOR_BYTES)
            source.seek(ledger_size - tail_length)
            tail = source.read(tail_length)
    except OSError as error:
        raise CollectionError(f"cannot anchor tick ledger {path}") from error
    if len(tail) != tail_length:
        raise CollectionError("tick ledger changed before pending journal")
    return tail


def _pending_from_record(
    path: Path,
    ledger_size: int,
    expected_stat: os.stat_result | None,
    previous_hash: str | None,
    chained: dict[str, Any],
    operation_id: str | None,
    user_payload_sha256: str,
) -> dict[str, Any]:
    tail = _ledger_tail_anchor(path, ledger_size, expected_stat)
    pending = {
        "version": LEDGER_PENDING_VERSION,
        "operation_id": operation_id,
        "user_payload_sha256": user_payload_sha256,
        "base": {
            "device": (
                int(expected_stat.st_dev) if expected_stat is not None else None
            ),
            "inode": (int(expected_stat.st_ino) if expected_stat is not None else None),
            "size": ledger_size,
            "mtime_ns": (
                int(expected_stat.st_mtime_ns) if expected_stat is not None else None
            ),
            "ctime_ns": (
                int(expected_stat.st_ctime_ns) if expected_stat is not None else None
            ),
            "tail_length": len(tail),
            "tail_sha256": hashlib.sha256(tail).hexdigest(),
            "previous_sha256": previous_hash,
        },
        "record": chained,
    }
    return _validate_ledger_pending(pending)


def _pending_user_identity(
    pending: dict[str, Any],
) -> tuple[bytes, str | None, str]:
    user_record = dict(pending["record"])
    del user_record[CHAIN_FIELD]
    user_payload = canonical_tick_bytes(user_record)
    operation_id = (
        pending["operation_id"]
        if pending["version"] == LEDGER_PENDING_VERSION
        else None
    )
    return user_payload, operation_id, hashlib.sha256(user_payload).hexdigest()


class _LedgerPrefixReader:
    def __init__(self, source: Any, length: int) -> None:
        self.source = source
        self.remaining = length

    def __enter__(self) -> _LedgerPrefixReader:
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.source.close()

    def __iter__(self) -> _LedgerPrefixReader:
        return self

    def __next__(self) -> bytes:
        if self.remaining == 0:
            raise StopIteration
        line = self.source.readline(self.remaining)
        if not line:
            raise StopIteration
        self.remaining -= len(line)
        return line


class _LedgerPrefixPath:
    def __init__(self, path: Path, length: int) -> None:
        self.path = path
        self.length = length
        self.reader: _LedgerPrefixReader | None = None

    def exists(self) -> bool:
        return self.path.exists()

    def open(self, mode: str) -> _LedgerPrefixReader:
        if mode != "rb":
            raise ValueError("tick ledger prefix can only be read as bytes")
        self.reader = _LedgerPrefixReader(self.path.open(mode), self.length)
        return self.reader


def _verify_ledger_prefix(
    path: Path,
    prefix_size: int,
    expected_tip_sha256: str | None,
    expected_stat: os.stat_result,
) -> None:
    try:
        with path.open("rb") as source:
            initial_stat = os.fstat(source.fileno())
            if (
                initial_stat.st_dev != expected_stat.st_dev
                or initial_stat.st_ino != expected_stat.st_ino
                or initial_stat.st_size != expected_stat.st_size
                or initial_stat.st_mtime_ns != expected_stat.st_mtime_ns
                or initial_stat.st_ctime_ns != expected_stat.st_ctime_ns
                or initial_stat.st_size < prefix_size
            ):
                raise CollectionError(
                    "tick ledger changed before base-prefix verification"
                )
            source.seek(prefix_size - 1)
            if source.read(1) != b"\n":
                raise CollectionError("tick ledger base does not end with a newline")
    except OSError as error:
        raise CollectionError(f"cannot verify tick ledger base {path}") from error

    prefix_path = _LedgerPrefixPath(path, prefix_size)
    verification = verify_ledger(prefix_path)
    if not verification["ok"]:
        location = (
            f" at line {verification['first_break']}"
            if verification["first_break"] is not None
            else ""
        )
        raise CollectionError(
            f"tick ledger base hash chain is broken{location}: {verification['message']}"
        )
    if prefix_path.reader is None or prefix_path.reader.remaining != 0:
        raise CollectionError("tick ledger changed during base-prefix verification")
    if verification["tip_sha256"] != expected_tip_sha256:
        raise CollectionError("tick ledger base differs from its pending chain anchor")

    try:
        current_stat = path.stat()
    except OSError as error:
        raise CollectionError(f"cannot verify tick ledger base {path}") from error
    if (
        current_stat.st_dev != expected_stat.st_dev
        or current_stat.st_ino != expected_stat.st_ino
        or current_stat.st_size != expected_stat.st_size
        or current_stat.st_mtime_ns != expected_stat.st_mtime_ns
        or (os.name != "nt" and current_stat.st_ctime_ns != expected_stat.st_ctime_ns)
    ):
        raise CollectionError("tick ledger changed during base-prefix verification")


def _complete_pending_append(
    path: Path,
    checkpoint_path: Path,
    pending_path: Path,
) -> tuple[bytes, str | None, str]:
    pending = _load_ledger_pending(pending_path)
    base = pending["base"]
    tip_payload = canonical_tick_bytes(pending["record"])
    user_payload, operation_id, user_payload_sha256 = _pending_user_identity(pending)
    payload = tip_payload + b"\n"
    base_size = base["size"]

    if base["device"] is None and checkpoint_path.exists():
        checkpoint = _load_ledger_checkpoint(checkpoint_path)
        if checkpoint["version"] == LEDGER_CHECKPOINT_VERSION and (
            checkpoint["operation_id"] != operation_id
            or checkpoint["user_payload_sha256"] != user_payload_sha256
        ):
            raise CollectionError(
                "tick ledger checkpoint conflicts with its fresh pending journal"
            )
        try:
            ledger_size = path.stat().st_size
        except FileNotFoundError as error:
            raise CollectionError(
                "tick ledger checkpoint exists without its ledger"
            ) from error
        if ledger_size != len(payload):
            raise CollectionError(
                "tick ledger checkpoint conflicts with an incomplete fresh pending append"
            )
        checkpoint_tip = _checkpointed_ledger_tip(path, checkpoint_path)
        if (
            checkpoint_tip[1] != len(payload)
            or checkpoint_tip[4] != user_payload
            or (operation_id is not None and checkpoint_tip[3] != operation_id)
        ):
            raise CollectionError(
                "tick ledger checkpoint differs from its completed fresh pending append"
            )

    ledger_stat: os.stat_result | None = None
    suffix = b""
    if path.exists():
        try:
            with path.open("rb") as source:
                ledger_stat = os.fstat(source.fileno())
                if not base_size <= ledger_stat.st_size <= base_size + len(payload):
                    raise CollectionError(
                        "tick ledger has bytes outside its pending append"
                    )

                tail_length = base["tail_length"]
                source.seek(base_size - tail_length)
                tail = source.read(tail_length)
                if (
                    len(tail) != tail_length
                    or hashlib.sha256(tail).hexdigest() != base["tail_sha256"]
                ):
                    raise CollectionError(
                        "tick ledger base differs from its pending journal"
                    )
                source.seek(base_size)
                suffix = source.read(ledger_stat.st_size - base_size)
                stable_stat = os.fstat(source.fileno())
        except OSError as error:
            raise CollectionError(
                f"cannot recover pending tick ledger {path}"
            ) from error
        if (
            stable_stat.st_dev != ledger_stat.st_dev
            or stable_stat.st_ino != ledger_stat.st_ino
            or stable_stat.st_size != ledger_stat.st_size
            or stable_stat.st_mtime_ns != ledger_stat.st_mtime_ns
            or stable_stat.st_ctime_ns != ledger_stat.st_ctime_ns
        ):
            raise CollectionError("tick ledger changed during pending recovery")
        if len(suffix) != ledger_stat.st_size - base_size:
            raise CollectionError("tick ledger changed during pending recovery")
    elif base["device"] is not None:
        raise CollectionError("tick ledger disappeared after pending journal")

    if not payload.startswith(suffix):
        raise CollectionError("tick ledger suffix is not its pending append prefix")
    if base_size and (
        suffix
        or ledger_stat.st_dev != base["device"]
        or ledger_stat.st_ino != base["inode"]
        or ledger_stat.st_mtime_ns != base["mtime_ns"]
        or ledger_stat.st_ctime_ns != base["ctime_ns"]
    ):
        _verify_ledger_prefix(
            path,
            base_size,
            base["previous_sha256"],
            ledger_stat,
        )

    descriptor = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_APPEND | getattr(os, "O_BINARY", 0),
        0o600,
    )
    try:
        before = os.fstat(descriptor)
        if before.st_size != base_size + len(suffix) or (
            ledger_stat is not None
            and (
                before.st_dev != ledger_stat.st_dev
                or before.st_ino != ledger_stat.st_ino
                or before.st_mtime_ns != ledger_stat.st_mtime_ns
                or (os.name != "nt" and before.st_ctime_ns != ledger_stat.st_ctime_ns)
            )
        ):
            raise CollectionError("tick ledger changed before pending append resumed")
        remaining = memoryview(payload)[len(suffix) :]
        while remaining:
            written = os.write(descriptor, remaining)
            if written == 0:
                raise OSError("zero-byte write while appending tick ledger")
            remaining = remaining[written:]
        os.fsync(descriptor)
        appended_stat = os.fstat(descriptor)
        if appended_stat.st_size != base_size + len(payload):
            raise CollectionError("tick ledger changed during pending append")
    finally:
        os.close(descriptor)

    current_stat = path.stat()
    if (
        current_stat.st_dev != appended_stat.st_dev
        or current_stat.st_ino != appended_stat.st_ino
        or current_stat.st_size != appended_stat.st_size
        or current_stat.st_mtime_ns != appended_stat.st_mtime_ns
        or (os.name != "nt" and current_stat.st_ctime_ns != appended_stat.st_ctime_ns)
    ):
        raise CollectionError("tick ledger changed after pending append")
    if base["device"] is None:
        _fsync_directory(path.parent)

    checkpoint = _checkpoint_from_tip(
        appended_stat,
        base_size,
        tip_payload,
        pending["record"][CHAIN_FIELD]["tick_sha256"],
        operation_id,
        user_payload_sha256,
    )
    _save_ledger_checkpoint(checkpoint_path, checkpoint)
    try:
        pending_path.unlink()
    except FileNotFoundError as error:
        raise CollectionError("tick ledger pending journal disappeared") from error
    _fsync_directory(pending_path.parent)

    return user_payload, operation_id, user_payload_sha256


def _checkpointed_ledger_tip(
    path: Path,
    checkpoint_path: Path,
) -> tuple[
    str | None,
    int,
    os.stat_result | None,
    str | None,
    bytes | None,
]:
    checkpoint = _load_ledger_checkpoint(checkpoint_path)
    if not path.exists():
        raise CollectionError("tick ledger checkpoint exists without its ledger")

    ledger = checkpoint["ledger"]
    tip = checkpoint["tip"]
    try:
        with path.open("rb") as source:
            ledger_stat = os.fstat(source.fileno())
            if (
                ledger_stat.st_dev != ledger["device"]
                or ledger_stat.st_ino != ledger["inode"]
            ):
                identity_matches = False
            else:
                identity_matches = True
            if ledger_stat.st_size < ledger["size"]:
                raise CollectionError("tick ledger was truncated after its checkpoint")

            source.seek(tip["offset"])
            checkpointed_tip = source.read(tip["length"])
            if len(checkpointed_tip) != tip["length"] or source.read(1) != b"\n":
                raise CollectionError("tick ledger checkpoint tip is missing")
            canonical_hash, tick_hash, user_payload = _parse_chained_tick(
                checkpointed_tip
            )
            if (
                canonical_hash != tip["canonical_sha256"]
                or tick_hash != tip["tick_sha256"]
            ):
                raise CollectionError("tick ledger tip differs from its checkpoint")

            growth = ledger_stat.st_size - ledger["size"]
            operation_id = None
            if checkpoint["version"] == LEDGER_CHECKPOINT_VERSION:
                if (
                    checkpoint["user_payload_sha256"]
                    != hashlib.sha256(user_payload).hexdigest()
                ):
                    raise CollectionError(
                        "tick ledger checkpoint user payload differs from its hash"
                    )
                operation_id = checkpoint["operation_id"]
    except OSError as error:
        raise CollectionError(f"cannot validate tick ledger {path}") from error

    if growth:
        verified = _bootstrap_ledger_tip(path)
        verified_stat = verified[2]
        if verified_stat is None or (
            verified_stat.st_dev != ledger_stat.st_dev
            or verified_stat.st_ino != ledger_stat.st_ino
            or verified_stat.st_size != ledger_stat.st_size
            or verified_stat.st_mtime_ns != ledger_stat.st_mtime_ns
            or verified_stat.st_ctime_ns != ledger_stat.st_ctime_ns
        ):
            raise CollectionError("tick ledger changed during full verification")
        return verified

    metadata_matches = (
        ledger_stat.st_mtime_ns == ledger["mtime_ns"]
        and ledger_stat.st_ctime_ns == ledger["ctime_ns"]
    )
    if not identity_matches or not metadata_matches:
        verified = _bootstrap_ledger_tip(path)
        verified_hash, verified_size, verified_stat = verified[:3]
        if (
            verified_hash != canonical_hash
            or verified_size != ledger["size"]
            or verified_stat is None
            or verified_stat.st_dev != ledger_stat.st_dev
            or verified_stat.st_ino != ledger_stat.st_ino
            or verified_stat.st_size != ledger_stat.st_size
            or verified_stat.st_mtime_ns != ledger_stat.st_mtime_ns
            or verified_stat.st_ctime_ns != ledger_stat.st_ctime_ns
        ):
            raise CollectionError("restored tick ledger differs from its checkpoint")
        rebound = _checkpoint_from_tip(
            verified_stat,
            tip["offset"],
            checkpointed_tip,
            tick_hash,
            operation_id,
            hashlib.sha256(user_payload).hexdigest(),
        )
        _save_ledger_checkpoint(checkpoint_path, rebound)
        ledger_stat = verified_stat

    return (
        canonical_hash,
        ledger_stat.st_size,
        ledger_stat,
        operation_id,
        user_payload,
    )


def _bootstrap_ledger_tip(
    path: Path,
) -> tuple[str | None, int, os.stat_result | None, None, None]:
    verification = verify_ledger(path)
    if not verification["ok"]:
        location = (
            f" at line {verification['first_break']}"
            if verification["first_break"] is not None
            else ""
        )
        raise CollectionError(
            f"tick ledger hash chain is broken{location}: {verification['message']}"
        )

    if not path.exists():
        return None, 0, None, None, None
    try:
        with path.open("rb") as source:
            ledger_stat = os.fstat(source.fileno())
            if ledger_stat.st_size:
                source.seek(-1, os.SEEK_END)
                if source.read(1) != b"\n":
                    raise CollectionError("tick ledger does not end with a newline")
    except OSError as error:
        raise CollectionError(f"cannot inspect tick ledger {path}") from error
    previous_hash = (
        verification["tip_sha256"] if verification["genesis_line"] is not None else None
    )
    return previous_hash, ledger_stat.st_size, ledger_stat, None, None


def _ledger_tip_for_append(
    path: Path,
    checkpoint_path: Path,
) -> tuple[
    str | None,
    int,
    os.stat_result | None,
    str | None,
    bytes | None,
]:
    if not checkpoint_path.exists():
        return _bootstrap_ledger_tip(path)
    return _checkpointed_ledger_tip(path, checkpoint_path)


def _append_jsonl(
    path: Path,
    record: dict[str, Any],
    operation_id: str | None,
    idempotent: bool,
) -> None:
    if CHAIN_FIELD in record:
        raise CollectionError(f"collector record already contains {CHAIN_FIELD}")
    try:
        user_payload = canonical_tick_bytes(record)
    except (ValueError, UnicodeEncodeError, RecursionError) as error:
        raise CollectionError("collector record is not canonical JSON") from error
    user_payload_sha256 = hashlib.sha256(user_payload).hexdigest()

    # Serialize the chain-tip read and append. Two collector processes that
    # read the same tip must never append sibling records with the same link.
    with exclusive_state_lock(path, LEDGER_LOCK_TIMEOUT):
        checkpoint_path = _ledger_checkpoint_path(path)
        pending_path = _ledger_pending_path(path)
        if pending_path.exists():
            pending = _load_ledger_pending(pending_path)
            pending_payload, pending_operation_id, pending_payload_sha256 = (
                _pending_user_identity(pending)
            )
            if (
                idempotent
                and pending_operation_id == operation_id
                and (
                    pending_payload != user_payload
                    or pending_payload_sha256 != user_payload_sha256
                )
            ):
                raise CollectionError(
                    "operation ID already identifies a different user payload"
                )
            (
                recovered_payload,
                recovered_operation_id,
                recovered_payload_sha256,
            ) = _complete_pending_append(
                path,
                checkpoint_path,
                pending_path,
            )
            if idempotent:
                if recovered_operation_id == operation_id:
                    if (
                        recovered_payload != user_payload
                        or recovered_payload_sha256 != user_payload_sha256
                    ):
                        raise CollectionError(
                            "operation ID already identifies a different user payload"
                        )
                    return
            elif recovered_payload == user_payload:
                return

        (
            previous_hash,
            ledger_size,
            expected_stat,
            checkpoint_operation_id,
            checkpoint_user_payload,
        ) = _ledger_tip_for_append(path, checkpoint_path)
        if idempotent and checkpoint_operation_id == operation_id:
            if checkpoint_user_payload != user_payload:
                raise CollectionError(
                    "operation ID already identifies a different user payload"
                )
            return

        chained = dict(record)
        chained[CHAIN_FIELD] = {
            "version": CHAIN_VERSION,
            "previous_sha256": previous_hash,
        }
        chained[CHAIN_FIELD]["tick_sha256"] = hashlib.sha256(
            canonical_tick_hash_bytes(chained)
        ).hexdigest()
        tip_payload = canonical_tick_bytes(chained)
        if len(tip_payload) > MAX_LEDGER_TICK_BYTES:
            raise CollectionError("tick ledger record exceeds its byte limit")
        pending = _pending_from_record(
            path,
            ledger_size,
            expected_stat,
            previous_hash,
            chained,
            operation_id,
            user_payload_sha256,
        )
        _save_ledger_pending(pending_path, pending)
        (
            completed_payload,
            completed_operation_id,
            completed_payload_sha256,
        ) = _complete_pending_append(
            path,
            checkpoint_path,
            pending_path,
        )
        if (
            completed_payload != user_payload
            or completed_operation_id != operation_id
            or completed_payload_sha256 != user_payload_sha256
        ):
            raise CollectionError("tick ledger pending journal changed during append")


def append_jsonl(path: Path, record: dict[str, Any]) -> None:
    _append_jsonl(path, record, None, False)


def append_jsonl_once(
    path: Path,
    record: dict[str, Any],
    operation_id: str,
) -> None:
    if (
        not isinstance(operation_id, str)
        or re.fullmatch(r"[0-9a-f]{64}", operation_id) is None
    ):
        raise CollectionError(
            "operation ID must be exactly 64 lowercase hex characters"
        )
    _append_jsonl(path, record, operation_id, True)


def recover_pending_jsonl(path: Path) -> bool:
    with exclusive_state_lock(path, LEDGER_LOCK_TIMEOUT):
        checkpoint_path = _ledger_checkpoint_path(path)
        pending_path = _ledger_pending_path(path)
        recovered = pending_path.exists()
        if recovered:
            _complete_pending_append(
                path,
                checkpoint_path,
                pending_path,
            )
        _ledger_tip_for_append(path, checkpoint_path)
        return recovered


@contextmanager
def exclusive_state_lock(state_path: Path, timeout: float) -> Iterator[None]:
    """Hold an exclusive OS lock across a whole load-modify-save state cycle.

    The lock is fcntl.flock on a sibling ``.lock`` file. On the deployment
    platform (Linux) fcntl always exists, so concurrent writers either
    serialize or fail closed: if the lock cannot be taken within ``timeout``
    seconds a CollectionError is raised and the cycle never runs unlocked.
    On platforms without fcntl (Windows, where only the tests run), locking
    degrades to a documented no-op.
    """
    try:
        import fcntl
    except ImportError:
        yield
        return

    lock_path = state_path.with_name(state_path.name + ".lock")
    descriptor = os.open(lock_path, os.O_WRONLY | os.O_CREAT, 0o600)
    try:
        deadline = time.monotonic() + timeout
        while True:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError:
                if time.monotonic() >= deadline:
                    raise CollectionError(
                        f"could not lock {lock_path} within {timeout:.0f}s; "
                        "skipping rather than writing unlocked"
                    ) from None
                time.sleep(0.2)
        try:
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        os.close(descriptor)


def load_census_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "version": CENSUS_STATE_VERSION,
            "started_at": utc_now(),
            "counts": {},
            "ledger_published": False,
        }
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, RecursionError) as error:
        raise CollectionError(f"cannot read census state {path}") from error
    if not isinstance(value, dict):
        raise CollectionError("census state has an unexpected shape")
    if set(value) == {"version", "started_at", "counts"} and value.get("version") == 1:
        value = {
            "version": CENSUS_STATE_VERSION,
            "started_at": value.get("started_at"),
            "counts": value.get("counts"),
            "ledger_published": False,
        }
    if (
        set(value) != {"version", "started_at", "counts", "ledger_published"}
        or value.get("version") != CENSUS_STATE_VERSION
        or not isinstance(value.get("started_at"), str)
        or not isinstance(value.get("counts"), dict)
        or not isinstance(value.get("ledger_published"), bool)
    ):
        raise CollectionError("census state has an unexpected shape")
    try:
        parse_timestamp(value["started_at"])
    except ValueError as error:
        raise CollectionError("census state has an invalid start timestamp") from error
    counts = value["counts"]
    total = 0
    for shard, count in counts.items():
        if (
            not isinstance(shard, str)
            or not re.fullmatch(r"did-[0-9a-f]{2}", shard)
            or isinstance(count, bool)
            or not isinstance(count, int)
            or count < 0
            or count > SQLITE_INTEGER_MAX
        ):
            raise CollectionError("census state contains an invalid shard result")
        total += count
        if total > SQLITE_INTEGER_MAX:
            raise CollectionError("census state contains an invalid aggregate count")
    if value["ledger_published"] and len(counts) != 256:
        raise CollectionError("incomplete census state cannot be marked as published")
    return value


def save_atomic_json(path: Path, value: dict[str, Any]) -> None:
    temporary = path.with_name(path.name + ".tmp")
    payload = (
        json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")
    descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
        0o600,
    )
    try:
        os.chmod(temporary, 0o600)
        remaining = memoryview(payload)
        while remaining:
            written = os.write(descriptor, remaining)
            if written == 0:
                raise OSError("zero-byte write while saving JSON state")
            remaining = remaining[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, path)
    if os.name != "nt":
        parent = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(parent)
        finally:
            os.close(parent)


def save_census_state(path: Path, state: dict[str, Any]) -> None:
    save_atomic_json(path, state)


def census_failure_cause(error: CollectionError) -> str:
    message = str(error)
    status_match = re.search(r"\bHTTP (\d{3})$", message)
    if status_match:
        return f"http_{status_match.group(1)}"
    if "exceeded its deadline" in message:
        return "deadline"
    if message.startswith("GET "):
        return "transport_or_decode"
    return "invalid_shard_response"


def run_census(
    client: Client,
    state_path: Path,
    pace: float,
) -> tuple[int | None, str, dict[str, Any]]:
    if (
        isinstance(pace, bool)
        or not isinstance(pace, (int, float))
        or not math.isfinite(pace)
        or pace < 0
    ):
        raise ValueError("census pace must be a finite non-negative number")
    with exclusive_state_lock(state_path, CENSUS_STATE_LOCK_TIMEOUT):
        state = load_census_state(state_path)
        counts: dict[str, int] = state["counts"]

        if len(counts) == 256 and state["ledger_published"]:
            state = {
                "version": CENSUS_STATE_VERSION,
                "started_at": utc_now(),
                "counts": {},
                "ledger_published": False,
            }
            counts = state["counts"]
            save_census_state(state_path, state)
        aggregate_count = sum(counts.values())

        outstanding_at_start = 256 - len(counts)
        deadline = time.monotonic() + CENSUS_DEADLINE_SECONDS
        passes_attempted = 0
        shard_reads_attempted = 0
        shard_read_failures = 0
        failure_causes: dict[str, int] = {}
        deadline_reached = False

        for _ in range(CENSUS_MAX_PASSES):
            missing = [
                f"did-{index:02x}"
                for index in range(256)
                if f"did-{index:02x}" not in counts
            ]
            if not missing:
                break
            passes_attempted += 1

            for shard in missing:
                if time.monotonic() >= deadline:
                    deadline_reached = True
                    break

                shard_reads_attempted += 1
                try:
                    path = f"/kv/{shard}"
                    body = client.get(path, deadline=deadline)
                    count = parse_collected_response(
                        client,
                        path,
                        parse_shard_count,
                        body,
                        shard,
                    )
                    if count > SQLITE_INTEGER_MAX - aggregate_count:
                        mark_response_invalid = getattr(
                            client, "mark_response_invalid", None
                        )
                        if callable(mark_response_invalid):
                            mark_response_invalid(path)
                        raise CollectionError(
                            "identity census aggregate exceeds SQLite integer storage",
                            outcome="invalid_response",
                            path=path,
                        )
                except CollectionError as error:
                    shard_read_failures += 1
                    cause = census_failure_cause(error)
                    failure_causes[cause] = failure_causes.get(cause, 0) + 1
                else:
                    counts[shard] = count
                    aggregate_count += count
                    save_census_state(state_path, state)

                if time.monotonic() >= deadline:
                    deadline_reached = True
                    break
                if pace:
                    remaining = max(0.0, deadline - time.monotonic())
                    if remaining == 0:
                        deadline_reached = True
                        break
                    time.sleep(min(pace, remaining))

            if deadline_reached:
                break

        completed = len(counts) == 256
        census_run = {
            "walk_started_at": state["started_at"],
            "shards_outstanding_at_start": outstanding_at_start,
            "shards_collected": len(counts),
            "shards_outstanding": 256 - len(counts),
            "passes_attempted": passes_attempted,
            "maximum_passes": CENSUS_MAX_PASSES,
            "deadline_seconds": CENSUS_DEADLINE_SECONDS,
            "shard_reads_attempted": shard_reads_attempted,
            "shard_read_failures": shard_read_failures,
            "failure_causes": dict(sorted(failure_causes.items())),
            "stop_reason": (
                "complete"
                if completed
                else ("deadline" if deadline_reached else "maximum_passes")
            ),
        }
        total = aggregate_count if completed else None
        save_census_state(state_path, state)
        return total, state["started_at"], census_run


def _acknowledge_census_publication_locked(
    state_path: Path,
    started_at: str,
    total: int,
) -> None:
    if (
        not isinstance(started_at, str)
        or isinstance(total, bool)
        or not isinstance(total, int)
        or total < 0
        or total > SQLITE_INTEGER_MAX
    ):
        raise CollectionError("invalid census publication acknowledgement")
    state = load_census_state(state_path)
    if (
        len(state["counts"]) != 256
        or state["started_at"] != started_at
        or sum(state["counts"].values()) != total
    ):
        raise CollectionError(
            "census publication acknowledgement does not match completed state"
        )
    if not state["ledger_published"]:
        state["ledger_published"] = True
        save_census_state(state_path, state)


def acknowledge_census_publication(
    state_path: Path,
    started_at: str,
    total: int,
) -> None:
    with exclusive_state_lock(state_path, CENSUS_STATE_LOCK_TIMEOUT):
        _acknowledge_census_publication_locked(state_path, started_at, total)


def signer_database_path(state_path: Path) -> Path:
    return state_path.with_suffix(".sqlite3")


def _validate_tick_outbox_schema(connection: sqlite3.Connection) -> None:
    table = connection.execute(
        "SELECT type FROM sqlite_master WHERE name = 'tick_outbox'"
    ).fetchone()
    columns = [
        (row[1], row[2].upper(), row[3], row[5])
        for row in connection.execute("PRAGMA table_info('tick_outbox')")
    ]
    expected = [
        ("singleton", "INTEGER", 0, 1),
        ("operation_id", "TEXT", 1, 0),
        ("payload", "BLOB", 1, 0),
        ("payload_sha256", "TEXT", 1, 0),
        ("census_started_at", "TEXT", 0, 0),
        ("census_total", "INTEGER", 0, 0),
    ]
    if table != ("table",) or columns != expected:
        raise CollectionError("signer database has an invalid tick outbox schema")


def load_tick_outbox(connection: sqlite3.Connection) -> dict[str, Any] | None:
    _validate_tick_outbox_schema(connection)
    rows = connection.execute(
        """
        SELECT
            singleton,
            operation_id,
            payload,
            payload_sha256,
            census_started_at,
            census_total
        FROM tick_outbox
        ORDER BY singleton
        """
    ).fetchall()
    if not rows:
        return None
    if len(rows) != 1:
        raise CollectionError("tick outbox contains more than one row")

    (
        singleton,
        operation_id,
        payload,
        payload_sha256,
        census_started_at,
        census_total,
    ) = rows[0]
    if (
        isinstance(singleton, bool)
        or singleton != 1
        or not isinstance(operation_id, str)
        or re.fullmatch(r"[0-9a-f]{64}", operation_id) is None
        or not isinstance(payload, bytes)
        or not 0 < len(payload) <= MAX_LEDGER_TICK_BYTES
        or not isinstance(payload_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", payload_sha256) is None
        or (census_started_at is None) != (census_total is None)
    ):
        raise CollectionError("tick outbox contains an invalid row")
    if census_total is not None:
        if (
            not isinstance(census_started_at, str)
            or isinstance(census_total, bool)
            or not isinstance(census_total, int)
            or census_total < 0
            or census_total > SQLITE_INTEGER_MAX
        ):
            raise CollectionError("tick outbox contains an invalid census tuple")
        try:
            parse_timestamp(census_started_at)
        except ValueError as error:
            raise CollectionError(
                "tick outbox contains an invalid census timestamp"
            ) from error

    if hashlib.sha256(payload).hexdigest() != payload_sha256:
        raise CollectionError("tick outbox payload hash does not match its bytes")
    try:
        tick = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_json_object,
            parse_constant=_reject_json_constant,
        )
        if not isinstance(tick, dict):
            raise ValueError("tick is not an object")
        canonical = canonical_tick_bytes(tick)
    except (
        UnicodeDecodeError,
        ValueError,
        UnicodeEncodeError,
        RecursionError,
    ) as error:
        raise CollectionError("tick outbox payload is not canonical JSON") from error
    if payload != canonical:
        raise CollectionError("tick outbox payload is not canonical JSON")
    if CHAIN_FIELD in tick:
        raise CollectionError("tick outbox payload already contains a ledger chain")
    if "identity_total" not in tick or "identity_census_started" not in tick:
        raise CollectionError("tick outbox payload has an incomplete census binding")

    tick_total = tick["identity_total"]
    tick_census_started = tick["identity_census_started"]
    if tick_total is None:
        if census_started_at is not None or census_total is not None:
            raise CollectionError("tick outbox census tuple does not match its payload")
        if tick_census_started is not None:
            if not isinstance(tick_census_started, str):
                raise CollectionError(
                    "tick outbox payload has an invalid census timestamp"
                )
            try:
                parse_timestamp(tick_census_started)
            except ValueError as error:
                raise CollectionError(
                    "tick outbox payload has an invalid census timestamp"
                ) from error
    elif (
        isinstance(tick_total, bool)
        or not isinstance(tick_total, int)
        or tick_total < 0
        or tick_total > SQLITE_INTEGER_MAX
        or not isinstance(tick_census_started, str)
        or tick_total != census_total
        or tick_census_started != census_started_at
    ):
        raise CollectionError("tick outbox census tuple does not match its payload")

    return {
        "operation_id": operation_id,
        "payload": payload,
        "payload_sha256": payload_sha256,
        "census_started_at": census_started_at,
        "census_total": census_total,
        "tick": tick,
    }


def _insert_tick_outbox(
    connection: sqlite3.Connection,
    tick: dict[str, Any],
) -> dict[str, Any]:
    if load_tick_outbox(connection) is not None:
        raise CollectionError("cannot replace a pending tick outbox")
    if CHAIN_FIELD in tick:
        raise CollectionError("collector tick already contains a ledger chain")
    try:
        payload = canonical_tick_bytes(tick)
    except (ValueError, UnicodeEncodeError, RecursionError) as error:
        raise CollectionError("collector tick is not canonical JSON") from error
    if not 0 < len(payload) <= MAX_LEDGER_TICK_BYTES:
        raise CollectionError("collector tick exceeds its byte limit")

    census_total = tick.get("identity_total")
    census_started_at = tick.get("identity_census_started")
    if census_total is None:
        stored_census_started_at = None
    else:
        if (
            isinstance(census_total, bool)
            or not isinstance(census_total, int)
            or census_total < 0
            or census_total > SQLITE_INTEGER_MAX
            or not isinstance(census_started_at, str)
        ):
            raise CollectionError("collector tick has an invalid census tuple")
        try:
            parse_timestamp(census_started_at)
        except ValueError as error:
            raise CollectionError(
                "collector tick has an invalid census timestamp"
            ) from error
        stored_census_started_at = census_started_at

    payload_digest = hashlib.sha256(payload).digest()
    operation_id = hashlib.sha256(
        TICK_OUTBOX_OPERATION_DOMAIN + secrets.token_bytes(32) + payload_digest
    ).hexdigest()
    connection.execute(
        """
        INSERT INTO tick_outbox (
            singleton,
            operation_id,
            payload,
            payload_sha256,
            census_started_at,
            census_total
        )
        VALUES (1, ?, ?, ?, ?, ?)
        """,
        (
            operation_id,
            payload,
            payload_digest.hex(),
            stored_census_started_at,
            census_total,
        ),
    )
    outbox = load_tick_outbox(connection)
    if outbox is None:
        raise CollectionError("tick outbox insert did not persist a row")
    return outbox


def delete_tick_outbox(
    connection: sqlite3.Connection,
    operation_id: str,
    payload_sha256: str,
) -> bool:
    if (
        not isinstance(operation_id, str)
        or re.fullmatch(r"[0-9a-f]{64}", operation_id) is None
        or not isinstance(payload_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", payload_sha256) is None
    ):
        raise CollectionError("invalid tick outbox delete condition")
    outbox = load_tick_outbox(connection)
    if outbox is None:
        return False
    if (
        outbox["operation_id"] != operation_id
        or outbox["payload_sha256"] != payload_sha256
    ):
        return False
    deleted = connection.execute(
        """
        DELETE FROM tick_outbox
        WHERE singleton = 1 AND operation_id = ? AND payload_sha256 = ?
        """,
        (operation_id, payload_sha256),
    )
    return deleted.rowcount == 1


def drain_tick_outbox(
    output_path: Path,
    signer_state_path: Path,
    census_state_path: Path,
    lock_timeout: float = SIGNER_LOCK_TIMEOUT,
) -> bool:
    database_path = signer_database_path(signer_state_path)
    if not database_path.exists():
        return False

    with exclusive_state_lock(signer_state_path, lock_timeout):
        if not database_path.exists():
            return False
        connection = connect_signer_database(database_path)
        try:
            connection.execute("BEGIN IMMEDIATE")
            outbox = load_tick_outbox(connection)
            if outbox is None:
                connection.rollback()
                return False
            append_jsonl_once(
                output_path,
                outbox["tick"],
                outbox["operation_id"],
            )
            if outbox["census_total"] is not None:
                with exclusive_state_lock(
                    census_state_path,
                    CENSUS_STATE_LOCK_TIMEOUT,
                ):
                    _acknowledge_census_publication_locked(
                        census_state_path,
                        outbox["census_started_at"],
                        outbox["census_total"],
                    )
            if not delete_tick_outbox(
                connection,
                outbox["operation_id"],
                outbox["payload_sha256"],
            ):
                raise CollectionError(
                    "tick outbox changed before publication acknowledgement"
                )
            connection.commit()
            return True
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()


def _migrate_room_generation_schema(connection: sqlite3.Connection) -> None:
    if not connection.in_transaction:
        connection.execute("BEGIN IMMEDIATE")
    try:
        for trigger in ("room_ledger_ai", "room_ledger_ad", "room_ledger_au"):
            connection.execute(f"DROP TRIGGER IF EXISTS {trigger}")
        connection.execute("DROP TABLE IF EXISTS room_search")
        connection.execute(
            """
            CREATE TABLE room_ledger_new (
                created_seq INTEGER PRIMARY KEY CHECK (created_seq >= 0),
                name TEXT NOT NULL,
                room_id TEXT NOT NULL CHECK (
                    length(room_id) = 16
                    AND room_id NOT GLOB '*[^0-9a-f]*'
                ),
                room_sha256 TEXT NOT NULL CHECK (
                    length(room_sha256) = 64
                    AND room_sha256 NOT GLOB '*[^0-9a-f]*'
                ),
                created_at TEXT NOT NULL,
                first_observed_at TEXT NOT NULL,
                last_listed_at TEXT
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE room_revisits_new (
                room_created_seq INTEGER NOT NULL,
                stage_seconds INTEGER NOT NULL,
                due_at TEXT NOT NULL,
                attempted_at TEXT,
                success INTEGER CHECK (success IN (0, 1)),
                outcome TEXT CHECK (
                    outcome IN (
                        'present_at_last_check',
                        'absent_at_last_check',
                        'check_failed',
                        'superseded_before_check'
                    )
                ),
                message_count INTEGER CHECK (message_count >= 0),
                has_second_message INTEGER CHECK (has_second_message IN (0, 1)),
                second_sender_class TEXT CHECK (
                    second_sender_class IN (
                        'signed_did',
                        'unsigned_did',
                        'server',
                        'other',
                        'not_observed'
                    )
                ),
                PRIMARY KEY (room_created_seq, stage_seconds),
                FOREIGN KEY (room_created_seq) REFERENCES room_ledger_new(created_seq)
            )
            """
        )

        ledger_rows = connection.execute(
            """
            SELECT
                name,
                room_id,
                room_sha256,
                created_seq,
                created_at,
                first_observed_at,
                last_listed_at
            FROM room_ledger
            ORDER BY created_seq
            """
        )
        ledger_count = 0
        for (
            name,
            room_id,
            room_sha256,
            created_seq,
            created_at,
            first_observed_at,
            last_listed_at,
        ) in ledger_rows:
            ledger_count += 1
            if (
                not isinstance(name, str)
                or ROOM_NAME_RE.fullmatch(name) is None
                or room_id != room_identifier(name)
                or room_sha256 != room_digest(name)
                or isinstance(created_seq, bool)
                or not isinstance(created_seq, int)
                or created_seq < 0
                or created_seq > SQLITE_INTEGER_MAX
                or not isinstance(created_at, str)
                or not isinstance(first_observed_at, str)
                or (last_listed_at is not None and not isinstance(last_listed_at, str))
            ):
                raise CollectionError("legacy room ledger contains an invalid row")
            try:
                created = parse_timestamp(created_at)
                first_observed = parse_timestamp(first_observed_at)
                last_listed = (
                    parse_timestamp(last_listed_at)
                    if last_listed_at is not None
                    else None
                )
            except ValueError as error:
                raise CollectionError(
                    "legacy room ledger contains an invalid timestamp"
                ) from error
            if last_listed is not None and last_listed < first_observed:
                raise CollectionError(
                    "legacy room ledger has inconsistent observation timestamps"
                )
            connection.execute(
                """
                INSERT INTO room_ledger_new (
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
                (
                    created_seq,
                    name,
                    room_id,
                    room_sha256,
                    timestamp_text(created),
                    timestamp_text(first_observed),
                    timestamp_text(last_listed) if last_listed is not None else None,
                ),
            )

        revisit_rows = connection.execute(
            """
            SELECT
                room_ledger.created_seq,
                room_ledger.created_at,
                room_revisits.stage_seconds,
                room_revisits.due_at,
                room_revisits.attempted_at,
                room_revisits.success,
                room_revisits.outcome,
                room_revisits.message_count,
                room_revisits.has_second_message,
                room_revisits.second_sender_class
            FROM room_revisits
            JOIN room_ledger ON room_ledger.name = room_revisits.room_name
            ORDER BY room_ledger.created_seq, room_revisits.stage_seconds
            """
        )
        revisit_count = 0
        for (
            created_seq,
            created_at,
            stage_seconds,
            due_at,
            attempted_at,
            success,
            outcome,
            message_count,
            has_second_message,
            second_sender_class,
        ) in revisit_rows:
            revisit_count += 1
            if (
                isinstance(stage_seconds, bool)
                or not isinstance(stage_seconds, int)
                or stage_seconds not in ROOM_REVISIT_STAGES_SECONDS
                or not isinstance(due_at, str)
            ):
                raise CollectionError(
                    "legacy room revisit store contains an invalid row"
                )
            try:
                created = parse_timestamp(created_at)
                due = parse_timestamp(due_at)
                expected_due = created + timedelta(seconds=stage_seconds)
            except (ValueError, OverflowError) as error:
                raise CollectionError(
                    "legacy room revisit store contains an invalid timestamp"
                ) from error
            if due not in (expected_due, expected_due.replace(microsecond=0)):
                raise CollectionError(
                    "legacy room revisit store has an invalid due time"
                )

            if attempted_at is None:
                if any(
                    value is not None
                    for value in (
                        success,
                        outcome,
                        message_count,
                        has_second_message,
                        second_sender_class,
                    )
                ):
                    raise CollectionError(
                        "legacy room revisit store has contradictory pending state"
                    )
                attempted_at_text = None
            else:
                if not isinstance(attempted_at, str):
                    raise CollectionError(
                        "legacy room revisit store has an invalid attempt timestamp"
                    )
                try:
                    attempted = parse_timestamp(attempted_at)
                except ValueError as error:
                    raise CollectionError(
                        "legacy room revisit store has an invalid attempt timestamp"
                    ) from error
                if attempted < expected_due:
                    raise CollectionError(
                        "legacy room revisit store has an early attempt"
                    )
                attempted_at_text = timestamp_text(attempted)
                valid_failure = (
                    success == 0
                    and outcome
                    in (
                        "absent_at_last_check",
                        "check_failed",
                        "superseded_before_check",
                    )
                    and message_count is None
                    and has_second_message is None
                    and second_sender_class is None
                )
                valid_success = (
                    success == 1
                    and outcome == "present_at_last_check"
                    and isinstance(message_count, int)
                    and not isinstance(message_count, bool)
                    and 0 <= message_count <= 200
                    and has_second_message in (0, 1)
                    and not isinstance(has_second_message, bool)
                    and (
                        (has_second_message == 0 and second_sender_class is None)
                        or (
                            has_second_message == 1
                            and second_sender_class
                            in (
                                "signed_did",
                                "unsigned_did",
                                "server",
                                "other",
                                "not_observed",
                            )
                        )
                    )
                )
                if not (valid_failure or valid_success):
                    raise CollectionError(
                        "legacy room revisit store has contradictory attempted state"
                    )

            connection.execute(
                """
                INSERT INTO room_revisits_new (
                    room_created_seq,
                    stage_seconds,
                    due_at,
                    attempted_at,
                    success,
                    outcome,
                    message_count,
                    has_second_message,
                    second_sender_class
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    created_seq,
                    stage_seconds,
                    timestamp_text(expected_due),
                    attempted_at_text,
                    success,
                    outcome,
                    message_count,
                    has_second_message,
                    second_sender_class,
                ),
            )

        if (
            revisit_count
            != connection.execute("SELECT COUNT(*) FROM room_revisits").fetchone()[0]
        ):
            raise CollectionError("legacy room revisit store has unmatched rows")
        incomplete_schedule = connection.execute(
            """
            SELECT room_ledger_new.created_seq
            FROM room_ledger_new
            LEFT JOIN room_revisits_new
                ON room_revisits_new.room_created_seq = room_ledger_new.created_seq
            GROUP BY room_ledger_new.created_seq
            HAVING
                COUNT(room_revisits_new.stage_seconds) <> 3
                OR COUNT(DISTINCT room_revisits_new.stage_seconds) <> 3
            LIMIT 1
            """
        ).fetchone()
        if (
            incomplete_schedule is not None
            or ledger_count
            != connection.execute("SELECT COUNT(*) FROM room_ledger_new").fetchone()[0]
        ):
            raise CollectionError(
                "legacy room revisit store has an incomplete schedule"
            )
        if connection.execute(
            "PRAGMA foreign_key_check('room_revisits_new')"
        ).fetchone():
            raise CollectionError("migrated room revisit store violates foreign keys")

        connection.execute("DROP TABLE room_revisits")
        connection.execute("DROP TABLE room_ledger")
        connection.execute("ALTER TABLE room_ledger_new RENAME TO room_ledger")
        connection.execute("ALTER TABLE room_revisits_new RENAME TO room_revisits")
        connection.commit()
    except Exception:
        connection.rollback()
        raise


LIFECYCLE_ROLLUP_TRIGGER_NAMES = (
    "room_revisits_validate_insert",
    "room_revisits_validate_update",
    "room_revisits_rollup_before_insert",
    "room_revisits_rollup_after_insert",
    "room_revisits_rollup_before_update",
    "room_revisits_rollup_after_update",
    "room_revisits_rollup_before_delete",
    "room_revisits_rollup_after_delete",
    "room_ledger_rollup_after_insert",
    "room_ledger_rollup_after_delete",
    "room_ledger_rollup_after_first_observed_update",
)


def _validate_room_lifecycle_store(connection: sqlite3.Connection) -> None:
    invalid = connection.execute(
        """
        SELECT room_revisits.room_created_seq
        FROM room_revisits
        LEFT JOIN room_ledger
            ON room_ledger.created_seq = room_revisits.room_created_seq
        WHERE
            typeof(room_revisits.room_created_seq) <> 'integer'
            OR room_ledger.created_seq IS NULL
            OR typeof(room_ledger.name) <> 'text'
            OR length(room_ledger.name) NOT BETWEEN 1 AND 48
            OR substr(room_ledger.name, 1, 1) NOT GLOB '[a-z0-9]'
            OR room_ledger.name GLOB '*[^a-z0-9_-]*'
            OR typeof(room_revisits.stage_seconds) <> 'integer'
            OR room_revisits.stage_seconds NOT IN (?, ?, ?)
            OR typeof(room_revisits.due_at) <> 'text'
            OR typeof(room_ledger.created_at) <> 'text'
            OR julianday(room_ledger.created_at) IS NULL
            OR julianday(room_revisits.due_at) IS NULL
            OR abs(
                (
                    julianday(room_revisits.due_at)
                    - julianday(room_ledger.created_at)
                ) * 86400.0 - room_revisits.stage_seconds
            ) > 0.001
            OR (
                room_revisits.attempted_at IS NULL
                AND (
                    room_revisits.success IS NOT NULL
                    OR room_revisits.outcome IS NOT NULL
                    OR room_revisits.message_count IS NOT NULL
                    OR room_revisits.has_second_message IS NOT NULL
                    OR room_revisits.second_sender_class IS NOT NULL
                )
            )
            OR (
                room_revisits.attempted_at IS NOT NULL
                AND (
                    typeof(room_revisits.attempted_at) <> 'text'
                    OR julianday(room_revisits.attempted_at) IS NULL
                    OR julianday(room_revisits.attempted_at)
                        < julianday(room_revisits.due_at)
                    OR typeof(room_revisits.success) <> 'integer'
                    OR room_revisits.success NOT IN (0, 1)
                    OR (
                        room_revisits.success = 0
                        AND (
                            room_revisits.outcome NOT IN (
                                'absent_at_last_check',
                                'check_failed',
                                'superseded_before_check'
                            )
                            OR room_revisits.outcome IS NULL
                            OR room_revisits.message_count IS NOT NULL
                            OR room_revisits.has_second_message IS NOT NULL
                            OR room_revisits.second_sender_class IS NOT NULL
                        )
                    )
                    OR (
                        room_revisits.outcome = 'superseded_before_check'
                        AND NOT EXISTS (
                            SELECT 1
                            FROM room_ledger AS newer
                            WHERE
                                newer.name = room_ledger.name
                                AND newer.created_seq > room_ledger.created_seq
                        )
                    )
                    OR (
                        room_revisits.success = 1
                        AND (
                            room_revisits.outcome <> 'present_at_last_check'
                            OR room_revisits.outcome IS NULL
                            OR typeof(room_revisits.message_count) <> 'integer'
                            OR room_revisits.message_count NOT BETWEEN 0 AND 200
                            OR typeof(room_revisits.has_second_message) <> 'integer'
                            OR room_revisits.has_second_message NOT IN (0, 1)
                            OR (
                                room_revisits.has_second_message = 0
                                AND room_revisits.second_sender_class IS NOT NULL
                            )
                            OR (
                                room_revisits.has_second_message = 1
                                AND (
                                    typeof(room_revisits.second_sender_class)
                                        <> 'text'
                                    OR room_revisits.second_sender_class NOT IN (
                                        'signed_did',
                                        'unsigned_did',
                                        'server',
                                        'other',
                                        'not_observed'
                                    )
                                )
                            )
                        )
                    )
                )
            )
            OR (
                room_revisits.aged_out_at IS NOT NULL
                AND (
                    room_revisits.attempted_at IS NOT NULL
                    OR typeof(room_revisits.aged_out_at) <> 'text'
                    OR julianday(room_revisits.aged_out_at) IS NULL
                    OR (
                        julianday(room_revisits.aged_out_at)
                        - julianday(room_revisits.due_at)
                    ) * 86400.0 - room_revisits.stage_seconds < -0.001
                )
            )
        LIMIT 1
        """,
        ROOM_REVISIT_STAGES_SECONDS,
    ).fetchone()
    incomplete_schedule = connection.execute(
        """
        SELECT room_ledger.created_seq
        FROM room_ledger
        LEFT JOIN room_revisits
            ON room_revisits.room_created_seq = room_ledger.created_seq
        GROUP BY room_ledger.created_seq
        HAVING
            COUNT(room_revisits.stage_seconds) <> 3
            OR COUNT(DISTINCT room_revisits.stage_seconds) <> 3
        LIMIT 1
        """
    ).fetchone()
    conflicting_sender_class = connection.execute(
        """
        SELECT room_created_seq
        FROM room_revisits
        WHERE
            success = 1
            AND has_second_message = 1
            AND second_sender_class <> 'not_observed'
        GROUP BY room_created_seq
        HAVING COUNT(DISTINCT second_sender_class) > 1
        LIMIT 1
        """
    ).fetchone()
    if (
        invalid is not None
        or incomplete_schedule is not None
        or conflicting_sender_class is not None
    ):
        raise CollectionError("room lifecycle store contains an invalid row")


def _computed_room_lifecycle_totals(
    connection: sqlite3.Connection,
) -> tuple[Any, ...]:
    counts = connection.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM room_ledger),
            COUNT(DISTINCT CASE
                WHEN
                    attempted_at IS NOT NULL
                    AND outcome <> 'superseded_before_check'
                THEN room_created_seq
            END),
            COUNT(DISTINCT CASE WHEN success = 1 THEN room_created_seq END),
            COUNT(DISTINCT CASE
                WHEN success = 1 AND has_second_message = 1
                THEN room_created_seq
            END),
            COUNT(CASE
                WHEN
                    attempted_at IS NOT NULL
                    AND outcome <> 'superseded_before_check'
                THEN 1
            END),
            COUNT(CASE WHEN outcome = 'check_failed' THEN 1 END),
            COUNT(DISTINCT CASE
                WHEN
                    success = 1
                    AND has_second_message = 1
                    AND second_sender_class = 'signed_did'
                THEN room_created_seq
            END),
            COUNT(DISTINCT CASE
                WHEN
                    success = 1
                    AND has_second_message = 1
                    AND second_sender_class = 'unsigned_did'
                THEN room_created_seq
            END),
            COUNT(DISTINCT CASE
                WHEN
                    success = 1
                    AND has_second_message = 1
                    AND second_sender_class = 'server'
                THEN room_created_seq
            END),
            COUNT(DISTINCT CASE
                WHEN
                    success = 1
                    AND has_second_message = 1
                    AND second_sender_class = 'other'
                THEN room_created_seq
            END),
            COUNT(DISTINCT CASE
                WHEN
                    success = 1
                    AND has_second_message = 1
                    AND second_sender_class = 'not_observed'
                    AND NOT EXISTS (
                        SELECT 1
                        FROM room_revisits AS concrete
                        WHERE
                            concrete.room_created_seq
                                = room_revisits.room_created_seq
                            AND concrete.success = 1
                            AND concrete.has_second_message = 1
                            AND concrete.second_sender_class IN (
                                'signed_did',
                                'unsigned_did',
                                'server',
                                'other'
                            )
                    )
                THEN room_created_seq
            END)
        FROM room_revisits
        """
    ).fetchone()
    ledger_started_at = connection.execute(
        "SELECT MIN(first_observed_at) FROM room_ledger"
    ).fetchone()[0]
    return (*counts, ledger_started_at)


def _stored_room_lifecycle_totals(
    connection: sqlite3.Connection,
) -> tuple[Any, ...] | None:
    return connection.execute(
        """
        SELECT
            rooms_in_ledger,
            rooms_revisited,
            rooms_successfully_revisited,
            rooms_with_second_message,
            reads_attempted,
            reads_failed,
            second_sender_signed_did,
            second_sender_unsigned_did,
            second_sender_server,
            second_sender_other,
            second_sender_not_observed,
            ledger_started_at
        FROM room_lifecycle_totals
        WHERE singleton = 1
        """
    ).fetchone()


def verify_lifecycle_rollup(connection: sqlite3.Connection) -> None:
    _validate_room_lifecycle_store(connection)
    stored = _stored_room_lifecycle_totals(connection)
    if stored is None or stored != _computed_room_lifecycle_totals(connection):
        raise CollectionError("room lifecycle rollup does not match its source rows")


def _room_revisit_validation_expression(reference: str) -> str:
    return f"""
        typeof({reference}.room_created_seq) <> 'integer'
        OR typeof({reference}.stage_seconds) <> 'integer'
        OR {reference}.stage_seconds NOT IN (300, 3600, 86400)
        OR typeof({reference}.due_at) <> 'text'
        OR julianday({reference}.due_at) IS NULL
        OR NOT EXISTS (
            SELECT 1
            FROM room_ledger
            WHERE
                room_ledger.created_seq = {reference}.room_created_seq
                AND abs(
                    (
                        julianday({reference}.due_at)
                        - julianday(room_ledger.created_at)
                    ) * 86400.0 - {reference}.stage_seconds
                ) <= 0.001
        )
        OR (
            {reference}.attempted_at IS NULL
            AND (
                {reference}.success IS NOT NULL
                OR {reference}.outcome IS NOT NULL
                OR {reference}.message_count IS NOT NULL
                OR {reference}.has_second_message IS NOT NULL
                OR {reference}.second_sender_class IS NOT NULL
            )
        )
        OR (
            {reference}.attempted_at IS NOT NULL
            AND (
                typeof({reference}.attempted_at) <> 'text'
                OR julianday({reference}.attempted_at) IS NULL
                OR julianday({reference}.attempted_at)
                    < julianday({reference}.due_at)
                OR typeof({reference}.success) <> 'integer'
                OR {reference}.success NOT IN (0, 1)
                OR (
                    {reference}.success = 0
                    AND (
                        {reference}.outcome NOT IN (
                            'absent_at_last_check',
                            'check_failed',
                            'superseded_before_check'
                        )
                        OR {reference}.outcome IS NULL
                        OR {reference}.message_count IS NOT NULL
                        OR {reference}.has_second_message IS NOT NULL
                        OR {reference}.second_sender_class IS NOT NULL
                    )
                )
                OR (
                    {reference}.outcome = 'superseded_before_check'
                    AND NOT EXISTS (
                        SELECT 1
                        FROM room_ledger AS cohort
                        JOIN room_ledger AS newer
                            ON newer.name = cohort.name
                            AND newer.created_seq > cohort.created_seq
                        WHERE cohort.created_seq = {reference}.room_created_seq
                    )
                )
                OR (
                    {reference}.success = 1
                    AND (
                        {reference}.outcome <> 'present_at_last_check'
                        OR {reference}.outcome IS NULL
                        OR typeof({reference}.message_count) <> 'integer'
                        OR {reference}.message_count NOT BETWEEN 0 AND 200
                        OR typeof({reference}.has_second_message) <> 'integer'
                        OR {reference}.has_second_message NOT IN (0, 1)
                        OR (
                            {reference}.has_second_message = 0
                            AND {reference}.second_sender_class IS NOT NULL
                        )
                        OR (
                            {reference}.has_second_message = 1
                            AND (
                                typeof({reference}.second_sender_class) <> 'text'
                                OR {reference}.second_sender_class NOT IN (
                                    'signed_did',
                                    'unsigned_did',
                                    'server',
                                    'other',
                                    'not_observed'
                                )
                            )
                        )
                    )
                )
                OR (
                    {reference}.success = 1
                    AND {reference}.has_second_message = 1
                    AND {reference}.second_sender_class IN (
                        'signed_did',
                        'unsigned_did',
                        'server',
                        'other'
                    )
                    AND EXISTS (
                        SELECT 1
                        FROM room_revisits AS other_stage
                        WHERE
                            other_stage.room_created_seq
                                = {reference}.room_created_seq
                            AND other_stage.stage_seconds
                                <> {reference}.stage_seconds
                            AND other_stage.success = 1
                            AND other_stage.has_second_message = 1
                            AND other_stage.second_sender_class IN (
                                'signed_did',
                                'unsigned_did',
                                'server',
                                'other'
                            )
                            AND other_stage.second_sender_class
                                <> {reference}.second_sender_class
                    )
                )
            )
        )
        OR (
            {reference}.aged_out_at IS NOT NULL
            AND (
                {reference}.attempted_at IS NOT NULL
                OR typeof({reference}.aged_out_at) <> 'text'
                OR julianday({reference}.aged_out_at) IS NULL
                OR (
                    julianday({reference}.aged_out_at)
                    - julianday({reference}.due_at)
                ) * 86400.0 - {reference}.stage_seconds < -0.001
            )
        )
    """


def _lifecycle_rollup_adjustments(
    room_created_seq: str,
    operator: str,
) -> str:
    return f"""
        rooms_revisited = rooms_revisited {operator} CASE WHEN EXISTS (
            SELECT 1 FROM room_revisits
            WHERE
                room_created_seq = {room_created_seq}
                AND attempted_at IS NOT NULL
                AND outcome <> 'superseded_before_check'
        ) THEN 1 ELSE 0 END,
        rooms_successfully_revisited = rooms_successfully_revisited
            {operator} CASE WHEN EXISTS (
                SELECT 1 FROM room_revisits
                WHERE room_created_seq = {room_created_seq} AND success = 1
            ) THEN 1 ELSE 0 END,
        rooms_with_second_message = rooms_with_second_message
            {operator} CASE WHEN EXISTS (
                SELECT 1 FROM room_revisits
                WHERE
                    room_created_seq = {room_created_seq}
                    AND success = 1
                    AND has_second_message = 1
            ) THEN 1 ELSE 0 END,
        reads_attempted = reads_attempted {operator} (
            SELECT COUNT(*) FROM room_revisits
            WHERE
                room_created_seq = {room_created_seq}
                AND attempted_at IS NOT NULL
                AND outcome <> 'superseded_before_check'
        ),
        reads_failed = reads_failed {operator} (
            SELECT COUNT(*) FROM room_revisits
            WHERE
                room_created_seq = {room_created_seq}
                AND outcome = 'check_failed'
        ),
        second_sender_signed_did = second_sender_signed_did
            {operator} CASE WHEN EXISTS (
                SELECT 1 FROM room_revisits
                WHERE
                    room_created_seq = {room_created_seq}
                    AND success = 1
                    AND has_second_message = 1
                    AND second_sender_class = 'signed_did'
            ) THEN 1 ELSE 0 END,
        second_sender_unsigned_did = second_sender_unsigned_did
            {operator} CASE WHEN EXISTS (
                SELECT 1 FROM room_revisits
                WHERE
                    room_created_seq = {room_created_seq}
                    AND success = 1
                    AND has_second_message = 1
                    AND second_sender_class = 'unsigned_did'
            ) THEN 1 ELSE 0 END,
        second_sender_server = second_sender_server
            {operator} CASE WHEN EXISTS (
                SELECT 1 FROM room_revisits
                WHERE
                    room_created_seq = {room_created_seq}
                    AND success = 1
                    AND has_second_message = 1
                    AND second_sender_class = 'server'
            ) THEN 1 ELSE 0 END,
        second_sender_other = second_sender_other
            {operator} CASE WHEN EXISTS (
                SELECT 1 FROM room_revisits
                WHERE
                    room_created_seq = {room_created_seq}
                    AND success = 1
                    AND has_second_message = 1
                    AND second_sender_class = 'other'
            ) THEN 1 ELSE 0 END,
        second_sender_not_observed = second_sender_not_observed
            {operator} CASE WHEN EXISTS (
                SELECT 1 FROM room_revisits
                WHERE
                    room_created_seq = {room_created_seq}
                    AND success = 1
                    AND has_second_message = 1
                    AND second_sender_class = 'not_observed'
            ) AND NOT EXISTS (
                SELECT 1 FROM room_revisits
                WHERE
                    room_created_seq = {room_created_seq}
                    AND success = 1
                    AND has_second_message = 1
                    AND second_sender_class IN (
                        'signed_did',
                        'unsigned_did',
                        'server',
                        'other'
                    )
            ) THEN 1 ELSE 0 END
    """


def _create_room_lifecycle_triggers(connection: sqlite3.Connection) -> None:
    validation = _room_revisit_validation_expression("NEW")
    subtract_new = _lifecycle_rollup_adjustments("NEW.room_created_seq", "-")
    subtract_old = _lifecycle_rollup_adjustments("OLD.room_created_seq", "-")
    add_new = _lifecycle_rollup_adjustments("NEW.room_created_seq", "+")
    add_old = _lifecycle_rollup_adjustments("OLD.room_created_seq", "+")
    connection.executescript(
        f"""
        CREATE TRIGGER IF NOT EXISTS room_revisits_validate_insert
        BEFORE INSERT ON room_revisits BEGIN
            SELECT CASE WHEN {validation}
                THEN RAISE(ABORT, 'invalid room revisit state') END;
        END;

        CREATE TRIGGER IF NOT EXISTS room_revisits_validate_update
        BEFORE UPDATE ON room_revisits BEGIN
            SELECT CASE
                WHEN NEW.room_created_seq <> OLD.room_created_seq
                THEN RAISE(ABORT, 'room revisit identity is immutable')
            END;
            SELECT CASE WHEN {validation}
                THEN RAISE(ABORT, 'invalid room revisit state') END;
        END;

        CREATE TRIGGER IF NOT EXISTS room_revisits_rollup_before_insert
        BEFORE INSERT ON room_revisits BEGIN
            UPDATE room_lifecycle_totals SET {subtract_new} WHERE singleton = 1;
        END;

        CREATE TRIGGER IF NOT EXISTS room_revisits_rollup_after_insert
        AFTER INSERT ON room_revisits BEGIN
            UPDATE room_lifecycle_totals SET {add_new} WHERE singleton = 1;
        END;

        CREATE TRIGGER IF NOT EXISTS room_revisits_rollup_before_update
        BEFORE UPDATE ON room_revisits BEGIN
            UPDATE room_lifecycle_totals SET {subtract_old} WHERE singleton = 1;
        END;

        CREATE TRIGGER IF NOT EXISTS room_revisits_rollup_after_update
        AFTER UPDATE ON room_revisits BEGIN
            UPDATE room_lifecycle_totals SET {add_new} WHERE singleton = 1;
        END;

        CREATE TRIGGER IF NOT EXISTS room_revisits_rollup_before_delete
        BEFORE DELETE ON room_revisits BEGIN
            UPDATE room_lifecycle_totals SET {subtract_old} WHERE singleton = 1;
        END;

        CREATE TRIGGER IF NOT EXISTS room_revisits_rollup_after_delete
        AFTER DELETE ON room_revisits BEGIN
            UPDATE room_lifecycle_totals SET {add_old} WHERE singleton = 1;
        END;

        CREATE TRIGGER IF NOT EXISTS room_ledger_rollup_after_insert
        AFTER INSERT ON room_ledger BEGIN
            UPDATE room_lifecycle_totals
            SET
                rooms_in_ledger = rooms_in_ledger + 1,
                ledger_started_at = CASE
                    WHEN
                        ledger_started_at IS NULL
                        OR NEW.first_observed_at < ledger_started_at
                    THEN NEW.first_observed_at
                    ELSE ledger_started_at
                END
            WHERE singleton = 1;
        END;

        CREATE TRIGGER IF NOT EXISTS room_ledger_rollup_after_delete
        AFTER DELETE ON room_ledger BEGIN
            UPDATE room_lifecycle_totals
            SET
                rooms_in_ledger = rooms_in_ledger - 1,
                ledger_started_at = (
                    SELECT MIN(first_observed_at) FROM room_ledger
                )
            WHERE singleton = 1;
        END;

        CREATE TRIGGER IF NOT EXISTS room_ledger_rollup_after_first_observed_update
        AFTER UPDATE OF first_observed_at ON room_ledger BEGIN
            UPDATE room_lifecycle_totals
            SET ledger_started_at = (
                SELECT MIN(first_observed_at) FROM room_ledger
            )
            WHERE singleton = 1;
        END;
        """
    )


def _initialize_room_lifecycle_rollup(connection: sqlite3.Connection) -> None:
    table_existed = (
        connection.execute(
            """
        SELECT 1 FROM sqlite_master
        WHERE type = 'table' AND name = 'room_lifecycle_totals'
        """
        ).fetchone()
        is not None
    )
    existing_trigger_sql = {
        row[0]: row[1]
        for row in connection.execute(
            "SELECT name, sql FROM sqlite_master WHERE type = 'trigger'"
        )
        if row[0] in LIFECYCLE_ROLLUP_TRIGGER_NAMES
    }
    # Validation triggers created before terminal aged-out finalization
    # cannot guard the aged_out_at contract; rebuild the whole trigger set
    # and revalidate the store once.
    validation_is_stale = any(
        not isinstance(existing_trigger_sql.get(name), str)
        or "aged_out_at" not in existing_trigger_sql[name]
        for name in ("room_revisits_validate_insert", "room_revisits_validate_update")
        if name in existing_trigger_sql
    )
    if validation_is_stale:
        for trigger in LIFECYCLE_ROLLUP_TRIGGER_NAMES:
            connection.execute(f"DROP TRIGGER IF EXISTS {trigger}")
        existing_trigger_sql = {}
    existing_triggers = set(existing_trigger_sql)
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS room_lifecycle_totals (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            rooms_in_ledger INTEGER NOT NULL CHECK (rooms_in_ledger >= 0),
            rooms_revisited INTEGER NOT NULL CHECK (rooms_revisited >= 0),
            rooms_successfully_revisited INTEGER NOT NULL
                CHECK (rooms_successfully_revisited >= 0),
            rooms_with_second_message INTEGER NOT NULL
                CHECK (rooms_with_second_message >= 0),
            reads_attempted INTEGER NOT NULL CHECK (reads_attempted >= 0),
            reads_failed INTEGER NOT NULL CHECK (reads_failed >= 0),
            second_sender_signed_did INTEGER NOT NULL
                CHECK (second_sender_signed_did >= 0),
            second_sender_unsigned_did INTEGER NOT NULL
                CHECK (second_sender_unsigned_did >= 0),
            second_sender_server INTEGER NOT NULL
                CHECK (second_sender_server >= 0),
            second_sender_other INTEGER NOT NULL
                CHECK (second_sender_other >= 0),
            second_sender_not_observed INTEGER NOT NULL
                CHECK (second_sender_not_observed >= 0),
            ledger_started_at TEXT,
            CHECK (rooms_revisited <= rooms_in_ledger),
            CHECK (rooms_successfully_revisited <= rooms_revisited),
            CHECK (rooms_with_second_message <= rooms_successfully_revisited),
            CHECK (reads_failed <= reads_attempted),
            CHECK (
                second_sender_signed_did
                + second_sender_unsigned_did
                + second_sender_server
                + second_sender_other
                + second_sender_not_observed
                = rooms_with_second_message
            ),
            CHECK (
                (rooms_in_ledger = 0 AND ledger_started_at IS NULL)
                OR (rooms_in_ledger > 0 AND ledger_started_at IS NOT NULL)
            )
        )
        """
    )
    expected_columns = {
        "singleton",
        "rooms_in_ledger",
        "rooms_revisited",
        "rooms_successfully_revisited",
        "rooms_with_second_message",
        "reads_attempted",
        "reads_failed",
        "second_sender_signed_did",
        "second_sender_unsigned_did",
        "second_sender_server",
        "second_sender_other",
        "second_sender_not_observed",
        "ledger_started_at",
    }
    actual_columns = {
        row[1]
        for row in connection.execute("PRAGMA table_info('room_lifecycle_totals')")
    }
    if actual_columns != expected_columns:
        raise CollectionError("signer database has an invalid lifecycle rollup schema")

    stored_rows = connection.execute(
        "SELECT COUNT(*) FROM room_lifecycle_totals"
    ).fetchone()[0]
    needs_backfill = (
        not table_existed
        or stored_rows != 1
        or existing_triggers != set(LIFECYCLE_ROLLUP_TRIGGER_NAMES)
    )
    if needs_backfill:
        _validate_room_lifecycle_store(connection)
        totals = _computed_room_lifecycle_totals(connection)
        connection.execute("DELETE FROM room_lifecycle_totals")
        connection.execute(
            """
            INSERT INTO room_lifecycle_totals VALUES (
                1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            totals,
        )
    _create_room_lifecycle_triggers(connection)
    stored = _stored_room_lifecycle_totals(connection)
    if (
        stored is None
        or stored[2] > stored[1]
        or stored[1] > stored[0]
        or stored[3] > stored[2]
        or stored[5] > stored[4]
        or sum(stored[6:11]) != stored[3]
        or (stored[0] == 0) != (stored[11] is None)
    ):
        raise CollectionError("signer database has an invalid lifecycle rollup")


def initialize_signer_database(connection: sqlite3.Connection) -> None:
    version = connection.execute("PRAGMA user_version").fetchone()[0]
    if version not in (0, 3, 4, 5, SIGNER_STATE_VERSION):
        raise CollectionError(
            f"signer database has unsupported schema version {version}"
        )

    room_search_row = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'room_search'"
    ).fetchone()
    room_search_existed = room_search_row is not None
    room_search_requires_rebuild = room_search_existed and (
        not isinstance(room_search_row[0], str)
        or "case_sensitive 1" not in room_search_row[0]
    )
    if room_search_requires_rebuild:
        connection.executescript(
            """
            DROP TRIGGER IF EXISTS room_ledger_ai;
            DROP TRIGGER IF EXISTS room_ledger_ad;
            DROP TRIGGER IF EXISTS room_ledger_au;
            DROP TABLE room_search;
            """
        )
        room_search_existed = False

    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS signer_metadata (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            state_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS tick_outbox (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            operation_id TEXT NOT NULL CHECK (
                length(operation_id) = 64
                AND operation_id NOT GLOB '*[^0-9a-f]*'
            ),
            payload BLOB NOT NULL CHECK (
                typeof(payload) = 'blob'
                AND length(payload) BETWEEN 1 AND 16777216
            ),
            payload_sha256 TEXT NOT NULL CHECK (
                length(payload_sha256) = 64
                AND payload_sha256 NOT GLOB '*[^0-9a-f]*'
            ),
            census_started_at TEXT CHECK (
                census_started_at IS NULL
                OR typeof(census_started_at) = 'text'
            ),
            census_total INTEGER CHECK (
                census_total IS NULL
                OR (
                    typeof(census_total) = 'integer'
                    AND census_total BETWEEN 0 AND 9223372036854775807
                )
            ),
            CHECK (
                (census_started_at IS NULL AND census_total IS NULL)
                OR (census_started_at IS NOT NULL AND census_total IS NOT NULL)
            )
        );

        CREATE TABLE IF NOT EXISTS signer_dids (
            did TEXT PRIMARY KEY,
            first_observed_ts TEXT,
            last_observed_ts TEXT,
            tick_count INTEGER NOT NULL CHECK (tick_count >= 0),
            collection_first_utc_date TEXT,
            collection_last_utc_date TEXT,
            collection_utc_dates_count INTEGER NOT NULL
                CHECK (collection_utc_dates_count >= 0),
            rooms_json TEXT NOT NULL,
            room_count INTEGER NOT NULL CHECK (room_count BETWEEN 0 AND 8),
            has_counterparty INTEGER NOT NULL CHECK (has_counterparty IN (0, 1))
        );

        CREATE INDEX IF NOT EXISTS signer_dids_funnel
        ON signer_dids (
            tick_count,
            collection_utc_dates_count,
            room_count,
            has_counterparty
        );

        CREATE TABLE IF NOT EXISTS room_ledger (
            created_seq INTEGER PRIMARY KEY CHECK (created_seq >= 0),
            name TEXT NOT NULL,
            room_id TEXT NOT NULL CHECK (
                length(room_id) = 16
                AND room_id NOT GLOB '*[^0-9a-f]*'
            ),
            room_sha256 TEXT NOT NULL CHECK (
                length(room_sha256) = 64
                AND room_sha256 NOT GLOB '*[^0-9a-f]*'
            ),
            created_at TEXT NOT NULL,
            first_observed_at TEXT NOT NULL,
            last_listed_at TEXT
        );

        CREATE TABLE IF NOT EXISTS room_revisits (
            room_created_seq INTEGER NOT NULL,
            stage_seconds INTEGER NOT NULL,
            due_at TEXT NOT NULL,
            attempted_at TEXT,
            success INTEGER CHECK (success IN (0, 1)),
            outcome TEXT CHECK (
                outcome IN (
                    'present_at_last_check',
                    'absent_at_last_check',
                    'check_failed',
                    'superseded_before_check'
                )
            ),
            message_count INTEGER CHECK (message_count >= 0),
            has_second_message INTEGER CHECK (has_second_message IN (0, 1)),
            second_sender_class TEXT CHECK (
                second_sender_class IN (
                    'signed_did',
                    'unsigned_did',
                    'server',
                    'other',
                    'not_observed'
                )
            ),
            aged_out_at TEXT,
            PRIMARY KEY (room_created_seq, stage_seconds),
            FOREIGN KEY (room_created_seq) REFERENCES room_ledger(created_seq)
        );
        """
    )
    _validate_tick_outbox_schema(connection)
    load_tick_outbox(connection)

    room_ledger_columns = {
        row[1] for row in connection.execute("PRAGMA table_info('room_ledger')")
    }
    room_ledger_primary_key = [
        row[1]
        for row in connection.execute("PRAGMA table_info('room_ledger')")
        if row[5]
    ]
    room_sha256_was_missing = "room_sha256" not in room_ledger_columns
    if room_sha256_was_missing:
        connection.execute("ALTER TABLE room_ledger ADD COLUMN room_sha256 TEXT")
    if "last_listed_at" not in room_ledger_columns:
        connection.execute("ALTER TABLE room_ledger ADD COLUMN last_listed_at TEXT")

    revisit_columns = {
        row[1] for row in connection.execute("PRAGMA table_info('room_revisits')")
    }
    legacy_room_generation_schema = (
        room_ledger_primary_key == ["name"]
        and "room_name" in revisit_columns
        and "room_created_seq" not in revisit_columns
    )
    if not legacy_room_generation_schema and (
        room_ledger_primary_key != ["created_seq"]
        or "room_created_seq" not in revisit_columns
        or "room_name" in revisit_columns
    ):
        raise CollectionError(
            "signer database has an inconsistent room-generation schema"
        )
    outcome_was_missing = "outcome" not in revisit_columns
    if outcome_was_missing:
        connection.execute(
            """
            ALTER TABLE room_revisits
            ADD COLUMN outcome TEXT CHECK (
                outcome IN (
                    'present_at_last_check',
                    'absent_at_last_check',
                    'check_failed',
                    'superseded_before_check'
                )
            )
            """
        )

    if room_sha256_was_missing:
        connection.executemany(
            "UPDATE room_ledger SET room_sha256 = ? WHERE name = ?",
            (
                (hashlib.sha256(name.encode("utf-8")).hexdigest(), name)
                for (name,) in connection.execute(
                    "SELECT name FROM room_ledger WHERE room_sha256 IS NULL"
                )
            ),
        )
    if outcome_was_missing:
        connection.execute(
            """
            UPDATE room_revisits
            SET outcome = CASE
                WHEN success = 1 THEN 'present_at_last_check'
                WHEN success = 0 THEN 'check_failed'
                ELSE NULL
            END
            WHERE outcome IS NULL AND success IS NOT NULL
            """
        )
    if legacy_room_generation_schema:
        _migrate_room_generation_schema(connection)
        room_search_existed = False
    migrated_revisit_columns = {
        row[1] for row in connection.execute("PRAGMA table_info('room_revisits')")
    }
    if "aged_out_at" not in migrated_revisit_columns:
        connection.execute("ALTER TABLE room_revisits ADD COLUMN aged_out_at TEXT")
    # The pending partial index replaces the retired (attempted_at, due_at)
    # index: finalized aged-out rows would otherwise stay inside the old
    # index's attempted_at IS NULL range forever and every selection would
    # keep scanning them.
    connection.execute("DROP INDEX IF EXISTS room_revisits_due")
    connection.execute(
        "CREATE INDEX IF NOT EXISTS room_revisits_pending "
        "ON room_revisits (due_at) "
        "WHERE attempted_at IS NULL AND aged_out_at IS NULL"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS room_revisits_aged_out "
        "ON room_revisits (stage_seconds, due_at, aged_out_at) "
        "WHERE aged_out_at IS NOT NULL"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS room_ledger_room_id "
        "ON room_ledger (room_id, room_sha256, created_seq DESC)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS room_ledger_name ON room_ledger (name, created_seq DESC)"
    )
    _initialize_room_lifecycle_rollup(connection)
    connection.executescript(
        """
        CREATE VIRTUAL TABLE IF NOT EXISTS room_search USING fts5(
            name,
            content = 'room_ledger',
            content_rowid = 'rowid',
            tokenize = 'trigram case_sensitive 1'
        );

        CREATE TRIGGER IF NOT EXISTS room_ledger_ai
        AFTER INSERT ON room_ledger BEGIN
            INSERT INTO room_search (rowid, name) VALUES (new.rowid, new.name);
        END;

        CREATE TRIGGER IF NOT EXISTS room_ledger_ad
        AFTER DELETE ON room_ledger BEGIN
            INSERT INTO room_search (room_search, rowid, name)
            VALUES ('delete', old.rowid, old.name);
        END;

        CREATE TRIGGER IF NOT EXISTS room_ledger_au
        AFTER UPDATE OF name ON room_ledger BEGIN
            INSERT INTO room_search (room_search, rowid, name)
            VALUES ('delete', old.rowid, old.name);
            INSERT INTO room_search (rowid, name) VALUES (new.rowid, new.name);
        END;
        """
    )
    if version < SIGNER_STATE_VERSION or not room_search_existed:
        connection.execute("INSERT INTO room_search(room_search) VALUES ('rebuild')")

    if version in (3, 4, 5):
        row = connection.execute(
            "SELECT state_json FROM signer_metadata WHERE singleton = 1"
        ).fetchone()
        if row is not None:
            try:
                metadata = json.loads(row[0])
            except (ValueError, RecursionError) as error:
                raise CollectionError(
                    "signer database contains invalid metadata JSON"
                ) from error
            if not isinstance(metadata, dict) or metadata.get("version") != version:
                raise CollectionError(
                    f"signer database has inconsistent v{version} metadata"
                )
            metadata["version"] = SIGNER_STATE_VERSION
            metadata["latest_room_listing_observed_at"] = None
            connection.execute(
                "UPDATE signer_metadata SET state_json = ? WHERE singleton = 1",
                (
                    json.dumps(
                        metadata,
                        ensure_ascii=False,
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                ),
            )

        if version == 3:
            # Version 3 credited mere signed co-occurrence. Those flags cannot
            # be reinterpreted as A → B → A alternation, so the stage restarts
            # empty.
            connection.execute("UPDATE signer_dids SET has_counterparty = 0")

    connection.execute(f"PRAGMA user_version = {SIGNER_STATE_VERSION}")
    connection.commit()


def connect_signer_database(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path, timeout=5.0)
    try:
        connection.execute("PRAGMA journal_mode = DELETE")
        connection.execute("PRAGMA synchronous = FULL")
        connection.execute("PRAGMA foreign_keys = ON")
        initialize_signer_database(connection)
    except Exception:
        connection.close()
        raise
    return connection


def new_signer_state(tracked_cap: int) -> dict[str, Any]:
    started_at = utc_now()
    return {
        "version": SIGNER_STATE_VERSION,
        "collection_started": started_at,
        "persistence_started_at": started_at,
        "persistence_reset_at": None,
        "persistence_first_utc_date": None,
        "persistence_last_utc_date": None,
        "persistence_collection_utc_dates_count": 0,
        "tracked_cap": tracked_cap,
        "cap_hit": False,
        "tracking_cap_saturation": None,
        "census": None,
        "latest_room_listing_observed_at": None,
        "selector_version": SELECTOR_VERSION,
        "selector_seed": secrets.token_hex(16),
        "selector_epoch": -1,
        "selector_frame": [],
        "selector_position": 0,
        "revisit_allocation_rotation": 0,
    }


def validate_signer_metadata(value: Any) -> dict[str, Any]:
    persistence_fields = {
        "collection_started",
        "persistence_started_at",
        "persistence_reset_at",
        "persistence_first_utc_date",
        "persistence_last_utc_date",
        "persistence_collection_utc_dates_count",
    }
    if (
        not isinstance(value, dict)
        or value.get("version") != SIGNER_STATE_VERSION
        or "dids" in value
        or not persistence_fields.issubset(value)
        or isinstance(value.get("tracked_cap"), bool)
        or not isinstance(value.get("tracked_cap"), int)
        or value["tracked_cap"] <= 0
        or value["tracked_cap"] > SQLITE_INTEGER_MAX
        or not isinstance(value.get("cap_hit"), bool)
        or (
            value.get("latest_room_listing_observed_at") is not None
            and not isinstance(value.get("latest_room_listing_observed_at"), str)
        )
    ):
        raise CollectionError("signer metadata has an unexpected shape")

    timestamp_fields = ("collection_started", "persistence_started_at")
    if not all(isinstance(value[field], str) for field in timestamp_fields):
        raise CollectionError("signer metadata has an invalid persistence timestamp")
    if value["persistence_reset_at"] is not None and not isinstance(
        value["persistence_reset_at"], str
    ):
        raise CollectionError("signer metadata has an invalid persistence timestamp")
    try:
        collection_started = parse_timestamp(value["collection_started"])
        persistence_started = parse_timestamp(value["persistence_started_at"])
        persistence_reset = (
            parse_timestamp(value["persistence_reset_at"])
            if value["persistence_reset_at"] is not None
            else None
        )
    except ValueError as error:
        raise CollectionError(
            "signer metadata has an invalid persistence timestamp"
        ) from error
    if persistence_started < collection_started:
        raise CollectionError("signer metadata persistence predates collection")
    if persistence_reset is not None and persistence_reset != persistence_started:
        raise CollectionError("signer metadata has an inconsistent persistence reset")

    collection_dates_count = value["persistence_collection_utc_dates_count"]
    if (
        isinstance(collection_dates_count, bool)
        or not isinstance(collection_dates_count, int)
        or collection_dates_count < 0
        or collection_dates_count > SQLITE_INTEGER_MAX
    ):
        raise CollectionError("signer metadata has an invalid collection-date count")
    first_date_value = value["persistence_first_utc_date"]
    last_date_value = value["persistence_last_utc_date"]
    if (first_date_value is None) != (last_date_value is None):
        raise CollectionError("signer metadata has inconsistent collection dates")
    if first_date_value is None:
        if collection_dates_count != 0:
            raise CollectionError("signer metadata has inconsistent collection dates")
    else:
        try:
            first_date = parse_collection_date(first_date_value)
            last_date = parse_collection_date(last_date_value)
        except (TypeError, ValueError) as error:
            raise CollectionError(
                "signer metadata has an invalid collection date"
            ) from error
        maximum_distinct_dates = (last_date - first_date).days + 1
        if (
            collection_dates_count == 0
            or last_date < first_date
            or collection_dates_count > maximum_distinct_dates
        ):
            raise CollectionError("signer metadata has inconsistent collection dates")

    latest_room_listing_observed_at = value.get("latest_room_listing_observed_at")
    if latest_room_listing_observed_at is not None:
        try:
            parse_timestamp(latest_room_listing_observed_at)
        except ValueError as error:
            raise CollectionError(
                "signer metadata has an invalid latest room-listing timestamp"
            ) from error

    if "last_updated" in value:
        if not isinstance(value["last_updated"], str):
            raise CollectionError("signer metadata has an invalid update timestamp")
        try:
            parse_timestamp(value["last_updated"])
        except ValueError as error:
            raise CollectionError(
                "signer metadata has an invalid update timestamp"
            ) from error

    selector_fields = (
        "selector_version",
        "selector_seed",
        "selector_epoch",
        "selector_frame",
        "selector_position",
        "revisit_allocation_rotation",
    )
    if not all(field in value for field in selector_fields):
        raise CollectionError("signer metadata has an incomplete room selector")

    if (
        value["selector_version"] != SELECTOR_VERSION
        or not isinstance(value["selector_seed"], str)
        or re.fullmatch(r"[0-9a-f]{32}", value["selector_seed"]) is None
        or isinstance(value["selector_epoch"], bool)
        or not isinstance(value["selector_epoch"], int)
        or value["selector_epoch"] < -1
        or value["selector_epoch"] > SQLITE_INTEGER_MAX
        or not isinstance(value["selector_frame"], list)
        or len(value["selector_frame"]) > 200
        or not all(
            isinstance(name, str)
            and name != "lobby"
            and ROOM_NAME_RE.fullmatch(name) is not None
            for name in value["selector_frame"]
        )
        or len(value["selector_frame"]) != len(set(value["selector_frame"]))
        or isinstance(value["selector_position"], bool)
        or not isinstance(value["selector_position"], int)
        or value["selector_position"] < 0
        or value["selector_position"] > SQLITE_INTEGER_MAX
        or value["selector_position"] > len(value["selector_frame"])
        or isinstance(value["revisit_allocation_rotation"], bool)
        or not isinstance(value["revisit_allocation_rotation"], int)
        or value["revisit_allocation_rotation"]
        not in range(len(ROOM_REVISIT_STAGES_SECONDS))
    ):
        raise CollectionError("signer metadata has an invalid room selector")

    saturation = value.get("tracking_cap_saturation")
    if saturation is not None:
        if (
            not isinstance(saturation, dict)
            or set(saturation) != {"started_at", "released_at", "permanent_undercount"}
            or not isinstance(saturation.get("started_at"), str)
            or not isinstance(saturation.get("released_at"), str)
            or saturation.get("permanent_undercount") is not True
        ):
            raise CollectionError(
                "signer metadata has an invalid cap-saturation window"
            )
        try:
            started_at = parse_timestamp(saturation["started_at"])
            released_at = parse_timestamp(saturation["released_at"])
        except ValueError as error:
            raise CollectionError(
                "signer metadata has an invalid cap-saturation timestamp"
            ) from error
        if released_at < started_at:
            raise CollectionError("signer metadata cap release precedes saturation")

    census = value.get("census")
    if census is not None:
        if (
            not isinstance(census, dict)
            or set(census) != {"total", "started_at", "completed_at"}
            or isinstance(census.get("total"), bool)
            or not isinstance(census.get("total"), int)
            or census["total"] < 0
            or census["total"] > SQLITE_INTEGER_MAX
            or not isinstance(census.get("started_at"), str)
            or not isinstance(census.get("completed_at"), str)
        ):
            raise CollectionError("signer metadata has an invalid census")
        try:
            census_started = parse_timestamp(census["started_at"])
            census_completed = parse_timestamp(census["completed_at"])
        except ValueError as error:
            raise CollectionError(
                "signer metadata has an invalid census timestamp"
            ) from error
        if census_completed < census_started:
            raise CollectionError(
                "signer metadata census completion precedes its start"
            )

    try:
        json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeEncodeError, RecursionError) as error:
        raise CollectionError("signer metadata is not canonical JSON") from error

    return value


def write_signer_metadata(
    connection: sqlite3.Connection,
    state: dict[str, Any],
) -> None:
    validate_signer_metadata(state)
    payload = json.dumps(
        state, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    )
    connection.execute(
        """
        INSERT INTO signer_metadata (singleton, state_json)
        VALUES (1, ?)
        ON CONFLICT(singleton) DO UPDATE SET state_json = excluded.state_json
        """,
        (payload,),
    )


def load_signer_state(
    path: Path,
    tracked_cap: int,
    connection: sqlite3.Connection,
) -> dict[str, Any]:
    row = connection.execute(
        "SELECT state_json FROM signer_metadata WHERE singleton = 1"
    ).fetchone()
    if row is None:
        if path.exists():
            raise CollectionError(
                "signer metadata exists without a migrated SQLite store; "
                "run migrate_signers.py while both writers are stopped"
            )
        value = new_signer_state(tracked_cap)
        write_signer_metadata(connection, value)
        return value

    try:
        value = json.loads(row[0])
    except (ValueError, RecursionError) as error:
        raise CollectionError(
            "signer database contains invalid metadata JSON"
        ) from error
    if (
        isinstance(value, dict)
        and value.get("version") == SIGNER_STATE_VERSION
        and "revisit_allocation_rotation" not in value
    ):
        value["revisit_allocation_rotation"] = 0
    validate_signer_metadata(value)

    disk_value: Any = None
    if path.exists():
        try:
            disk_value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError, RecursionError):
            disk_value = None
    if disk_value != value:
        save_atomic_json(path, value)
    return value


def shortened_did(value: str) -> str:
    return value[len("did:key:z6Mk") :]


def room_identifier(name: str) -> str:
    return hashlib.sha256(name.encode("utf-8")).hexdigest()[:ROOM_ID_HEX_LENGTH]


def room_digest(name: str) -> str:
    return hashlib.sha256(name.encode("utf-8")).hexdigest()


def timestamp_text(value: datetime) -> str:
    return (
        value.astimezone(timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def record_created_rooms(
    connection: sqlite3.Connection,
    events: list[dict[str, Any]],
    observed_at: str,
) -> int:
    try:
        observation = parse_timestamp(observed_at)
    except (TypeError, ValueError) as error:
        raise CollectionError(
            "room-creation observation has an invalid timestamp"
        ) from error
    observed_at_text = timestamp_text(observation)
    inserted = 0
    for event in events:
        try:
            created_at = parse_timestamp(event["ts"])
            if created_at > observation:
                raise CollectionError(
                    "room-creation event occurs after its observation"
                )
            created_at_text = timestamp_text(created_at)
            schedule = [
                (
                    event["seq"],
                    stage_seconds,
                    timestamp_text(created_at + timedelta(seconds=stage_seconds)),
                )
                for stage_seconds in ROOM_REVISIT_STAGES_SECONDS
            ]
        except (TypeError, ValueError, OverflowError) as error:
            raise CollectionError(
                "room-creation event has an invalid timestamp"
            ) from error
        try:
            connection.execute(
                """
                INSERT INTO room_ledger (
                    created_seq,
                    name,
                    room_id,
                    room_sha256,
                    created_at,
                    first_observed_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    event["seq"],
                    event["name"],
                    room_identifier(event["name"]),
                    room_digest(event["name"]),
                    created_at_text,
                    observed_at_text,
                ),
            )
        except sqlite3.IntegrityError as error:
            existing = connection.execute(
                """
                SELECT created_seq, name, room_id, room_sha256, created_at
                FROM room_ledger
                WHERE created_seq = ?
                """,
                (event["seq"],),
            ).fetchall()
            expected_ledger_row = (
                event["seq"],
                event["name"],
                room_identifier(event["name"]),
                room_digest(event["name"]),
                created_at_text,
            )
            persisted_schedule = connection.execute(
                """
                SELECT stage_seconds, due_at
                FROM room_revisits
                WHERE room_created_seq = ?
                ORDER BY stage_seconds
                """,
                (event["seq"],),
            ).fetchall()
            expected_schedule = sorted(
                (stage_seconds, due_at) for _, stage_seconds, due_at in schedule
            )
            if (
                existing == [expected_ledger_row]
                and persisted_schedule == expected_schedule
            ):
                continue
            raise CollectionError("conflicting room-creation event") from error
        inserted += 1
        try:
            connection.executemany(
                """
                INSERT INTO room_revisits (
                    room_created_seq,
                    stage_seconds,
                    due_at
                )
                VALUES (?, ?, ?)
                """,
                schedule,
            )
        except sqlite3.IntegrityError as error:
            raise CollectionError("conflicting room-revisit schedule") from error
    return inserted


def record_listed_rooms(
    connection: sqlite3.Connection,
    newest_rooms: list[dict[str, Any]],
    observed_at: str,
    state: dict[str, Any],
) -> None:
    try:
        observation = parse_timestamp(observed_at)
    except (TypeError, ValueError) as error:
        raise CollectionError(
            "room listing observation has an invalid timestamp"
        ) from error
    normalized_observation = timestamp_text(observation)
    latest = state.get("latest_room_listing_observed_at")
    if latest is not None:
        try:
            latest_observation = parse_timestamp(latest)
        except (TypeError, ValueError) as error:
            raise CollectionError(
                "room listing observation has an invalid persisted checkpoint"
            ) from error
        if observation < latest_observation:
            raise CollectionError("room listing observation predates its checkpoint")

    names = [room["name"] for room in newest_rooms]
    if names:
        placeholders = ",".join("?" for _ in names)
        for name, last_listed_at in connection.execute(
            "SELECT room_ledger.name, room_ledger.last_listed_at "
            "FROM room_ledger "
            "JOIN ("
            "SELECT name, MAX(created_seq) AS created_seq "
            f"FROM room_ledger WHERE name IN ({placeholders}) "
            "AND created_at <= ? GROUP BY name"
            ") AS newest ON newest.created_seq = room_ledger.created_seq",
            [*names, normalized_observation],
        ):
            if last_listed_at is None:
                continue
            try:
                last_listing = parse_timestamp(last_listed_at)
            except (TypeError, ValueError) as error:
                raise CollectionError(
                    f"room listing observation found an invalid checkpoint for {name}"
                ) from error
            if observation < last_listing:
                raise CollectionError(
                    f"room listing observation would rewind checkpoint for {name}"
                )
    connection.executemany(
        "UPDATE room_ledger SET last_listed_at = ? "
        "WHERE created_seq = ("
        "SELECT MAX(created_seq) FROM room_ledger "
        "WHERE name = ? AND created_at <= ?"
        ")",
        ((observed_at, room["name"], normalized_observation) for room in newest_rooms),
    )
    state["latest_room_listing_observed_at"] = observed_at


def room_revisit_rank(
    selector_version: int,
    selector_seed: str,
    tick_timestamp: str,
    stage_seconds: int,
    created_seq: int,
) -> bytes:
    material = json.dumps(
        {
            "created_seq": created_seq,
            "selector_seed": selector_seed,
            "selector_version": selector_version,
            "stage_seconds": stage_seconds,
            "tick_timestamp": tick_timestamp,
        },
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    return hashlib.sha256(material).digest()


def select_due_room_revisits(
    connection: sqlite3.Connection,
    now: str,
    *,
    selector_version: int,
    selector_seed: str,
    allocation_rotation: int,
    limit: int = ROOM_REVISIT_READ_BUDGET,
) -> dict[str, Any]:
    if (
        isinstance(limit, bool)
        or not isinstance(limit, int)
        or limit < 0
        or limit > ROOM_REVISIT_READ_BUDGET
    ):
        raise CollectionError("room-revisit selection exceeds its enforced read budget")
    if (
        isinstance(selector_version, bool)
        or selector_version != SELECTOR_VERSION
        or not isinstance(selector_seed, str)
        or re.fullmatch(r"[0-9a-f]{32}", selector_seed) is None
        or isinstance(allocation_rotation, bool)
        or not isinstance(allocation_rotation, int)
        or allocation_rotation not in range(len(ROOM_REVISIT_STAGES_SECONDS))
    ):
        raise CollectionError("room-revisit selection has an invalid selector")
    try:
        now_timestamp = parse_timestamp(now)
    except (TypeError, ValueError) as error:
        raise CollectionError(
            "room-revisit selection has an invalid timestamp"
        ) from error
    normalized_now = timestamp_text(now_timestamp)

    invalid = connection.execute(
        """
        SELECT
            room_revisits.room_created_seq,
            room_ledger.name,
            room_revisits.stage_seconds,
            room_revisits.due_at,
            room_ledger.created_at
        FROM room_revisits
        LEFT JOIN room_ledger
            ON room_ledger.created_seq = room_revisits.room_created_seq
        WHERE
            room_revisits.attempted_at IS NULL
            AND room_revisits.aged_out_at IS NULL
            AND (
                typeof(room_revisits.room_created_seq) <> 'integer'
                OR room_ledger.created_seq IS NULL
                OR typeof(room_ledger.name) <> 'text'
                OR length(room_ledger.name) NOT BETWEEN 1 AND 48
                OR substr(room_ledger.name, 1, 1) NOT GLOB '[a-z0-9]'
                OR room_ledger.name GLOB '*[^a-z0-9_-]*'
                OR typeof(room_revisits.stage_seconds) <> 'integer'
                OR room_revisits.stage_seconds NOT IN (?, ?, ?)
                OR typeof(room_revisits.due_at) <> 'text'
                OR typeof(room_ledger.created_at) <> 'text'
                OR julianday(room_ledger.created_at) IS NULL
                OR julianday(room_revisits.due_at) IS NULL
                OR abs(
                    (
                        julianday(room_revisits.due_at)
                        - julianday(room_ledger.created_at)
                    ) * 86400.0 - room_revisits.stage_seconds
                ) > 0.001
                OR room_revisits.success IS NOT NULL
                OR room_revisits.outcome IS NOT NULL
                OR room_revisits.message_count IS NOT NULL
                OR room_revisits.has_second_message IS NOT NULL
                OR room_revisits.second_sender_class IS NOT NULL
            )
        LIMIT 1
        """,
        ROOM_REVISIT_STAGES_SECONDS,
    ).fetchone()
    if invalid is not None:
        raise CollectionError("room revisit store contains an invalid row")

    rows = connection.execute(
        """
        SELECT
            room_revisits.room_created_seq,
            room_ledger.name,
            room_revisits.stage_seconds,
            room_revisits.due_at,
            room_ledger.created_at,
            (
                SELECT MIN(newer.created_at)
                FROM room_ledger AS newer
                WHERE
                    newer.name = room_ledger.name
                    AND newer.created_seq > room_ledger.created_seq
                    AND newer.created_at <= ?
            )
        FROM room_revisits
        JOIN room_ledger
            ON room_ledger.created_seq = room_revisits.room_created_seq
        WHERE
            room_revisits.attempted_at IS NULL
            AND room_revisits.aged_out_at IS NULL
            AND room_revisits.due_at <= ?
        """,
        (normalized_now, normalized_now),
    ).fetchall()

    active: dict[int, list[tuple[bytes, int, dict[str, Any]]]] = {
        stage_seconds: [] for stage_seconds in ROOM_REVISIT_STAGES_SECONDS
    }
    superseded: list[tuple[datetime, int, int, dict[str, Any]]] = []
    aged_out_unselected = 0
    aged_out_finalizable: list[tuple[int, int]] = []

    for (
        created_seq,
        name,
        stage_seconds,
        due_at,
        created_at,
        superseded_at,
    ) in rows:
        if (
            isinstance(created_seq, bool)
            or not isinstance(created_seq, int)
            or created_seq < 0
            or created_seq > SQLITE_INTEGER_MAX
            or not isinstance(name, str)
            or ROOM_NAME_RE.fullmatch(name) is None
            or isinstance(stage_seconds, bool)
            or not isinstance(stage_seconds, int)
            or stage_seconds not in ROOM_REVISIT_STAGES_SECONDS
            or not isinstance(due_at, str)
            or not isinstance(created_at, str)
            or (superseded_at is not None and not isinstance(superseded_at, str))
        ):
            raise CollectionError("room revisit store contains an invalid row")
        try:
            created = parse_timestamp(created_at)
            due_timestamp = parse_timestamp(due_at)
            eligible_until = due_timestamp + timedelta(seconds=stage_seconds)
            newer_created = (
                parse_timestamp(superseded_at) if superseded_at is not None else None
            )
            expected_due = created + timedelta(seconds=stage_seconds)
        except (ValueError, OverflowError) as error:
            raise CollectionError(
                "room revisit store contains an invalid timestamp"
            ) from error
        if due_timestamp != expected_due:
            raise CollectionError(
                "room revisit store contains an inconsistent due time"
            )

        entry = {
            "created_seq": created_seq,
            "name": name,
            "stage_seconds": stage_seconds,
            "created_at": created_at,
        }
        if newer_created is not None and newer_created < eligible_until:
            entry["superseded"] = True
            superseded.append((due_timestamp, created_seq, stage_seconds, entry))
            continue
        if now_timestamp >= eligible_until:
            aged_out_unselected += 1
            if len(aged_out_finalizable) < ROOM_REVISIT_AGED_OUT_FINALIZATION_LIMIT:
                aged_out_finalizable.append((created_seq, stage_seconds))
            continue

        rank = room_revisit_rank(
            selector_version,
            selector_seed,
            normalized_now,
            stage_seconds,
            created_seq,
        )
        active[stage_seconds].append((rank, created_seq, entry))

    stage_count = len(ROOM_REVISIT_STAGES_SECONDS)
    base_allocation, extra_slots = divmod(limit, stage_count)
    initial_allocation = dict.fromkeys(
        ROOM_REVISIT_STAGES_SECONDS,
        base_allocation,
    )
    for offset in range(extra_slots):
        stage_index = (allocation_rotation + 1 + offset) % stage_count
        stage_seconds = ROOM_REVISIT_STAGES_SECONDS[stage_index]
        initial_allocation[stage_seconds] += 1

    initially_selected: list[tuple[bytes, int, int, dict[str, Any]]] = []
    remaining: list[tuple[bytes, int, int, dict[str, Any]]] = []
    for stage_seconds in ROOM_REVISIT_STAGES_SECONDS:
        ranked = sorted(
            active[stage_seconds],
            key=lambda candidate: (candidate[0], candidate[1]),
        )
        allocation = initial_allocation[stage_seconds]
        initially_selected.extend(
            (rank, stage_seconds, created_seq, entry)
            for rank, created_seq, entry in ranked[:allocation]
        )
        remaining.extend(
            (rank, stage_seconds, created_seq, entry)
            for rank, created_seq, entry in ranked[allocation:]
        )

    unused_slots = limit - len(initially_selected)
    remaining.sort(key=lambda candidate: candidate[:3])
    redistributed = remaining[:unused_slots]
    ranked_selected = sorted(
        [*initially_selected, *redistributed],
        key=lambda candidate: candidate[:3],
    )
    selected = [candidate[3] for candidate in ranked_selected]
    selected_by_stage = {
        str(stage_seconds): sum(
            entry["stage_seconds"] == stage_seconds for entry in selected
        )
        for stage_seconds in ROOM_REVISIT_STAGES_SECONDS
    }

    superseded.sort(key=lambda candidate: candidate[:3])
    publication_capacity = ROOM_REVISIT_PUBLICATION_BATCH_LIMIT - len(selected)
    selected_superseded = [
        candidate[3] for candidate in superseded[:publication_capacity]
    ]

    selection = {
        "selector_version": selector_version,
        "selector_seed": selector_seed,
        "tick_timestamp": normalized_now,
        "eligibility": {
            "lower_bound": "due_at <= tick_timestamp",
            "upper_bound": "tick_timestamp < due_at + stage_seconds",
        },
        "rank": {
            "algorithm": "sha256",
            "canonicalization": (
                "UTF-8 JSON; ensure_ascii=true; separators=(',',':'); sort_keys=true"
            ),
            "fields": [
                "created_seq",
                "selector_seed",
                "selector_version",
                "stage_seconds",
                "tick_timestamp",
            ],
            "order": (
                "digest ascending, then stage_seconds ascending, then created_seq ascending"
            ),
        },
        "allocation_rotation": allocation_rotation,
        "short_stage_seconds": ROOM_REVISIT_STAGES_SECONDS[allocation_rotation],
        "initial_allocation_by_stage": {
            str(stage_seconds): initial_allocation[stage_seconds]
            for stage_seconds in ROOM_REVISIT_STAGES_SECONDS
        },
        "selected_by_stage": selected_by_stage,
        "redistributed_reads": len(redistributed),
        "read_budget": limit,
    }

    return {
        "due_this_tick": sum(len(candidates) for candidates in active.values()),
        "superseded_due_this_tick": len(superseded),
        "aged_out_unselected": aged_out_unselected,
        "aged_out_finalizable": aged_out_finalizable,
        "selected": selected,
        "superseded": selected_superseded,
        "selection": selection,
    }


def room_lifecycle_coverage_by_stage(
    connection: sqlite3.Connection,
    selection_time: str,
) -> dict[str, dict[str, Any]]:
    try:
        selected_at = parse_timestamp(selection_time)
    except (TypeError, ValueError) as error:
        raise CollectionError(
            "room lifecycle coverage has an invalid timestamp"
        ) from error
    normalized_selection_time = timestamp_text(selected_at)
    rows = connection.execute(
        """
        SELECT
            room_revisits.stage_seconds,
            room_revisits.due_at,
            room_revisits.attempted_at,
            room_revisits.success,
            room_revisits.outcome,
            room_revisits.has_second_message,
            (
                SELECT MIN(newer.created_at)
                FROM room_ledger AS newer
                WHERE
                    newer.name = room_ledger.name
                    AND newer.created_seq > room_ledger.created_seq
                    AND newer.created_at <= ?
            )
        FROM room_revisits
        JOIN room_ledger
            ON room_ledger.created_seq = room_revisits.room_created_seq
        WHERE
            room_revisits.due_at <= ?
            AND room_revisits.aged_out_at IS NULL
        """,
        (normalized_selection_time, normalized_selection_time),
    ).fetchall()

    coverage = {
        str(stage_seconds): {
            "scheduled_due_rooms": 0,
            "ineligible_superseded_before_due": 0,
            "eligible_rooms": 0,
            "attempted_checks": 0,
            "completed_checks": 0,
            "failed_checks": 0,
            "attempted_late": 0,
            "deferred_checks": 0,
            "aged_out_unselected": 0,
            "superseded_after_eligibility": 0,
        }
        for stage_seconds in ROOM_REVISIT_STAGES_SECONDS
    }

    # Rows finalized as terminally aged out were validated at write time:
    # never attempted, window closed, no supersession observed inside the
    # window. They keep their cumulative place in coverage without another
    # per-row scan.
    finalized_rows = connection.execute(
        """
        SELECT stage_seconds, COUNT(*)
        FROM room_revisits
        WHERE aged_out_at IS NOT NULL AND due_at <= ?
        GROUP BY stage_seconds
        """,
        (normalized_selection_time,),
    ).fetchall()
    for stage_seconds, finalized_count in finalized_rows:
        if (
            isinstance(stage_seconds, bool)
            or stage_seconds not in ROOM_REVISIT_STAGES_SECONDS
        ):
            raise CollectionError("room lifecycle coverage found an invalid row")
        counters = coverage[str(stage_seconds)]
        counters["scheduled_due_rooms"] += finalized_count
        counters["eligible_rooms"] += finalized_count
        counters["aged_out_unselected"] += finalized_count
    successful_present_checks = dict.fromkeys(ROOM_REVISIT_STAGES_SECONDS, 0)
    second_message_checks = dict.fromkeys(ROOM_REVISIT_STAGES_SECONDS, 0)

    for (
        stage_seconds,
        due_at,
        attempted_at,
        success,
        outcome,
        has_second_message,
        superseded_at,
    ) in rows:
        if (
            isinstance(stage_seconds, bool)
            or stage_seconds not in ROOM_REVISIT_STAGES_SECONDS
            or not isinstance(due_at, str)
            or (attempted_at is not None and not isinstance(attempted_at, str))
            or (superseded_at is not None and not isinstance(superseded_at, str))
        ):
            raise CollectionError("room lifecycle coverage found an invalid row")
        try:
            due_timestamp = parse_timestamp(due_at)
            eligible_until = due_timestamp + timedelta(seconds=stage_seconds)
            attempt_timestamp = (
                parse_timestamp(attempted_at) if attempted_at is not None else None
            )
            newer_created = (
                parse_timestamp(superseded_at) if superseded_at is not None else None
            )
        except (ValueError, OverflowError) as error:
            raise CollectionError(
                "room lifecycle coverage found an invalid timestamp"
            ) from error

        counters = coverage[str(stage_seconds)]
        counters["scheduled_due_rooms"] += 1
        actual_attempt = (
            attempt_timestamp is not None and outcome != "superseded_before_check"
        )
        timely_attempt = actual_attempt and attempt_timestamp < eligible_until

        if actual_attempt:
            if outcome not in (
                "present_at_last_check",
                "absent_at_last_check",
                "check_failed",
            ):
                raise CollectionError(
                    "room lifecycle coverage found an invalid attempt outcome"
                )
            counters["eligible_rooms"] += 1
            if timely_attempt:
                counters["attempted_checks"] += 1
                if outcome == "check_failed":
                    counters["failed_checks"] += 1
                else:
                    counters["completed_checks"] += 1
                if success == 1:
                    successful_present_checks[stage_seconds] += 1
                    if has_second_message == 1:
                        second_message_checks[stage_seconds] += 1
            else:
                # An origin read happened, only after the eligibility window
                # closed. That is neither a timely check nor a check that
                # never happened, so it counts in its own state and stays
                # out of the timely coverage and second-message
                # denominators.
                counters["attempted_late"] += 1
            continue

        if newer_created is not None and newer_created <= due_timestamp:
            counters["ineligible_superseded_before_due"] += 1
            continue

        counters["eligible_rooms"] += 1
        if newer_created is not None and newer_created < eligible_until:
            counters["superseded_after_eligibility"] += 1
        elif selected_at >= eligible_until:
            counters["aged_out_unselected"] += 1
        else:
            counters["deferred_checks"] += 1

    for stage_seconds in ROOM_REVISIT_STAGES_SECONDS:
        counters = coverage[str(stage_seconds)]
        if (
            counters["scheduled_due_rooms"]
            != counters["ineligible_superseded_before_due"] + counters["eligible_rooms"]
            or counters["eligible_rooms"]
            != counters["attempted_checks"]
            + counters["attempted_late"]
            + counters["deferred_checks"]
            + counters["aged_out_unselected"]
            + counters["superseded_after_eligibility"]
            or counters["attempted_checks"]
            != counters["completed_checks"] + counters["failed_checks"]
        ):
            raise CollectionError("room lifecycle stage coverage is inconsistent")
        counters["coverage_fraction"] = {
            "numerator": counters["completed_checks"],
            "denominator": counters["eligible_rooms"],
        }
        counters["second_message_fraction"] = {
            "numerator": second_message_checks[stage_seconds],
            "denominator": successful_present_checks[stage_seconds],
        }

    return coverage


def classify_second_sender(message: dict[str, Any]) -> str:
    sender = message["from"]
    if sender == "server":
        return "server"
    if SIGNED_DID_RE.fullmatch(sender):
        return "signed_did" if message.get("nonce") is not None else "unsigned_did"
    return "other"


def read_budget_summary(
    sampled_room_reads: int,
    revisit_reads: int,
) -> dict[str, Any]:
    base_reads = 2 + sampled_room_reads
    total_reads = base_reads + revisit_reads
    if revisit_reads > ROOM_REVISIT_READ_BUDGET:
        raise CollectionError(
            "collector room revisits exceed the enforced per-tick budget"
        )
    if total_reads > TOTAL_READ_BUDGET:
        raise CollectionError(
            "collector logical reads exceed the enforced per-tick budget"
        )
    reads_per_minute = total_reads * 60 / RATE_BUDGET_WINDOW_SECONDS
    share = reads_per_minute / PUBLISHED_READS_PER_MINUTE
    if share > MAX_READ_SHARE:
        raise CollectionError(
            "collector logical read rate exceeds the configured share ceiling"
        )
    return {
        "base_reads": base_reads,
        "revisit_reads": revisit_reads,
        "total_reads": total_reads,
        "total_read_budget": TOTAL_READ_BUDGET,
        "revisit_read_budget": ROOM_REVISIT_READ_BUDGET,
        "rate_window_seconds": RATE_BUDGET_WINDOW_SECONDS,
        "tick_revisit_deadline_seconds": TICK_REVISIT_DEADLINE_SECONDS,
        "reads_per_minute": reads_per_minute,
        "published_reads_per_minute": PUBLISHED_READS_PER_MINUTE,
        "share": share,
        "maximum_share": MAX_READ_SHARE,
    }


def room_lifecycle_summary(
    connection: sqlite3.Connection,
    *,
    created_this_tick: int,
    due_this_tick: int,
    selected_this_tick: int,
    revisits: list[dict[str, Any]],
    superseded_due_this_tick: int = 0,
) -> dict[str, Any]:
    counts = _stored_room_lifecycle_totals(connection)
    if counts is None:
        raise CollectionError("room lifecycle rollup is missing")
    superseded_this_tick = sum(
        revisit.get("outcome") == "superseded_before_check" for revisit in revisits
    )
    attempted_this_tick = len(revisits) - superseded_this_tick
    deferred_superseded_due_to_batch_limit = (
        superseded_due_this_tick - superseded_this_tick
    )
    deferred_due_to_read_budget = due_this_tick - selected_this_tick
    deferred_due_to_deadline = selected_this_tick - attempted_this_tick
    if (
        deferred_superseded_due_to_batch_limit < 0
        or deferred_due_to_read_budget < 0
        or deferred_due_to_deadline < 0
    ):
        raise CollectionError("room lifecycle tick accounting is inconsistent")
    sender_counts = {
        "signed_did": int(counts[6]),
        "unsigned_did": int(counts[7]),
        "server": int(counts[8]),
        "other": int(counts[9]),
        "not_observed": int(counts[10]),
    }

    return {
        "ledger_started_at": counts[11],
        "rooms_in_ledger": int(counts[0]),
        "rooms_revisited": int(counts[1]),
        "rooms_successfully_revisited": int(counts[2]),
        "rooms_with_second_message": int(counts[3]),
        "reads_attempted": int(counts[4]),
        "reads_failed": int(counts[5]),
        "second_sender_classes": sender_counts,
        "created_rooms_observed_this_tick": created_this_tick,
        "due_this_tick": due_this_tick,
        "attempted_this_tick": attempted_this_tick,
        "superseded_this_tick": superseded_this_tick,
        "deferred_due_to_read_budget": deferred_due_to_read_budget,
        "deferred_due_to_deadline": deferred_due_to_deadline,
        "deferred_superseded_due_to_batch_limit": (
            deferred_superseded_due_to_batch_limit
        ),
        "deferred_due_to_budget": (
            deferred_due_to_read_budget + deferred_due_to_deadline
        ),
        "revisits": revisits,
    }


def collect_room_revisits(
    client: Client,
    connection: sqlite3.Connection,
    tick_ts: str,
    *,
    sampled_room_reads: int,
    selector_version: int,
    selector_seed: str,
    allocation_rotation: int,
    deadline: float,
) -> dict[str, Any]:
    selected = select_due_room_revisits(
        connection,
        tick_ts,
        selector_version=selector_version,
        selector_seed=selector_seed,
        allocation_rotation=allocation_rotation,
    )
    selected_reads = selected["selected"]

    # Enforce the maximum selected work before the first revisit read. The
    # final summary is recomputed from the reads that were actually issued.
    read_budget_summary(sampled_room_reads, len(selected_reads))

    published: list[dict[str, Any]] = []
    for revisit in selected_reads:
        if time.monotonic() >= deadline:
            continue

        name = revisit["name"]
        created_seq = revisit["created_seq"]
        stage_seconds = revisit["stage_seconds"]
        attempted_at = utc_now()
        elapsed_since_creation_seconds = int(
            (
                parse_timestamp(attempted_at) - parse_timestamp(revisit["created_at"])
            ).total_seconds()
        )
        if elapsed_since_creation_seconds < stage_seconds:
            raise CollectionError("room revisit attempt precedes its nominal due stage")

        path = f"/r/{urllib.parse.quote(name, safe='')}?format=json&limit=200"
        public_result = {
            "id": room_identifier(name),
            "created_seq": created_seq,
            "stage_seconds": stage_seconds,
            "elapsed_since_creation_seconds": elapsed_since_creation_seconds,
            "success": False,
            "outcome": "check_failed",
            "message_count": None,
            "has_second_message": None,
            "second_sender_class": None,
        }
        try:
            body = client.get(path, deadline=deadline)
            messages = parse_collected_response(
                client,
                path,
                parse_room_messages,
                body,
                path,
            )
        except CollectionError as error:
            outcome = "absent_at_last_check" if error.status == 404 else "check_failed"
            connection.execute(
                """
                UPDATE room_revisits
                SET
                    attempted_at = ?,
                    success = 0,
                    outcome = ?,
                    message_count = NULL,
                    has_second_message = NULL,
                    second_sender_class = NULL
                WHERE room_created_seq = ? AND stage_seconds = ?
                """,
                (attempted_at, outcome, created_seq, stage_seconds),
            )
            public_result["outcome"] = outcome
            published.append(public_result)
            continue

        has_second_message = any(message["seq"] >= 2 for message in messages)
        exact_second = next(
            (message for message in messages if message["seq"] == 2),
            None,
        )
        second_sender_class = (
            classify_second_sender(exact_second)
            if exact_second is not None
            else ("not_observed" if has_second_message else None)
        )
        connection.execute(
            """
            UPDATE room_revisits
            SET
                attempted_at = ?,
                success = 1,
                outcome = 'present_at_last_check',
                message_count = ?,
                has_second_message = ?,
                second_sender_class = ?
            WHERE room_created_seq = ? AND stage_seconds = ?
            """,
            (
                attempted_at,
                len(messages),
                int(has_second_message),
                second_sender_class,
                created_seq,
                stage_seconds,
            ),
        )
        public_result.update(
            {
                "success": True,
                "outcome": "present_at_last_check",
                "message_count": len(messages),
                "has_second_message": has_second_message,
                "second_sender_class": second_sender_class,
            }
        )
        published.append(public_result)

    # No-read supersession finalization follows the ranked read phase, so
    # administrative publication cannot delay an eligible origin read.
    for revisit in selected["superseded"]:
        name = revisit["name"]
        created_seq = revisit["created_seq"]
        stage_seconds = revisit["stage_seconds"]
        attempted_at = utc_now()
        connection.execute(
            """
            UPDATE room_revisits
            SET
                attempted_at = ?,
                success = 0,
                outcome = 'superseded_before_check',
                message_count = NULL,
                has_second_message = NULL,
                second_sender_class = NULL
            WHERE room_created_seq = ? AND stage_seconds = ?
            """,
            (attempted_at, created_seq, stage_seconds),
        )
        published.append(
            {
                "id": room_identifier(name),
                "created_seq": created_seq,
                "stage_seconds": stage_seconds,
                "elapsed_since_creation_seconds": None,
                "success": False,
                "outcome": "superseded_before_check",
                "message_count": None,
                "has_second_message": None,
                "second_sender_class": None,
            }
        )

    # Terminal finalization of aged-out checks: every window closed with no
    # attempt and no supersession observed inside the window, so nothing can
    # ever change these rows again. The batch is bounded per tick so a large
    # backlog can never stall the tick; the remainder is published, not
    # hidden. No attempt evidence is fabricated: only aged_out_at is set.
    finalizable = selected["aged_out_finalizable"]
    if finalizable:
        finalized_at = utc_now()
        cursor = connection.executemany(
            """
            UPDATE room_revisits
            SET aged_out_at = ?
            WHERE
                room_created_seq = ?
                AND stage_seconds = ?
                AND attempted_at IS NULL
                AND aged_out_at IS NULL
            """,
            [
                (finalized_at, created_seq, stage_seconds)
                for created_seq, stage_seconds in finalizable
            ],
        )
        if cursor.rowcount != len(finalizable):
            raise CollectionError(
                "aged-out finalization did not update its selected rows"
            )

    summary = room_lifecycle_summary(
        connection,
        created_this_tick=0,
        due_this_tick=selected["due_this_tick"],
        selected_this_tick=len(selected_reads),
        revisits=published,
        superseded_due_this_tick=selected["superseded_due_this_tick"],
    )
    coverage_by_stage = room_lifecycle_coverage_by_stage(connection, tick_ts)
    summary["sampling"] = {
        "aged_out_unselected": sum(
            stage["aged_out_unselected"] for stage in coverage_by_stage.values()
        ),
        "aged_out_finalization": {
            "finalized_this_tick": len(finalizable),
            "backlog_remaining": selected["aged_out_unselected"] - len(finalizable),
            "batch_limit": ROOM_REVISIT_AGED_OUT_FINALIZATION_LIMIT,
        },
        "selection": selected["selection"],
        "coverage_by_stage": coverage_by_stage,
    }
    return summary


def selector_frame_id(state: dict[str, Any]) -> str:
    identifiers = sorted(
        [room_identifier("lobby")]
        + [room_identifier(name) for name in state["selector_frame"]]
    )
    material = json.dumps(
        {
            "selector_version": state["selector_version"],
            "seed": state["selector_seed"],
            "epoch": state["selector_epoch"],
            "rooms": identifiers,
        },
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:ROOM_ID_HEX_LENGTH]


def room_sample_names(
    newest_rooms: list[dict[str, Any]],
    state: dict[str, Any],
) -> tuple[list[str], dict[str, Any]]:
    if state["selector_position"] >= len(state["selector_frame"]):
        if state["selector_epoch"] >= SQLITE_INTEGER_MAX:
            raise CollectionError("room selector epoch cannot be incremented")
        candidates: list[str] = []
        seen = {"lobby"}
        for room in newest_rooms:
            name = room["name"]
            normalized = name[3:] if name.startswith("/r/") else name
            if normalized in seen:
                continue
            seen.add(normalized)
            candidates.append(normalized)

        state["selector_epoch"] += 1
        seed = state["selector_seed"]
        epoch = state["selector_epoch"]
        state["selector_frame"] = sorted(
            candidates,
            key=lambda name: (
                hashlib.sha256(
                    f"{seed}:{epoch}:".encode("ascii") + name.encode("utf-8")
                ).digest(),
                name,
            ),
        )
        state["selector_position"] = 0

    start = state["selector_position"]
    stop = min(start + ROOM_READ_BUDGET - 1, len(state["selector_frame"]))
    selected = state["selector_frame"][start:stop]
    state["selector_position"] = stop

    return ["lobby", *selected], {
        "selector_version": state["selector_version"],
        "seed": state["selector_seed"],
        "epoch": state["selector_epoch"],
        "frame_id": selector_frame_id(state),
        "frame_size": len(state["selector_frame"]) + 1,
        "read_budget": ROOM_READ_BUDGET,
        "sampled": [],
    }


def update_signer_state(
    connection: sqlite3.Connection,
    state: dict[str, Any],
    sampled_rooms: list[tuple[str, list[dict[str, Any]]]],
    tick_ts: str,
) -> None:
    validate_signer_metadata(state)
    observed_this_tick: dict[str, dict[str, Any]] = {}
    try:
        tick_timestamp = parse_timestamp(tick_ts)
    except (TypeError, ValueError) as error:
        raise CollectionError("signer update has an invalid tick timestamp") from error
    if "last_updated" in state:
        try:
            last_updated = parse_timestamp(state["last_updated"])
        except (TypeError, ValueError) as error:
            raise CollectionError(
                "signer update has an invalid persisted checkpoint"
            ) from error
        if tick_timestamp < last_updated:
            raise CollectionError("signer update predates its persisted checkpoint")
    collection_date = tick_timestamp.date().isoformat()

    if state["persistence_collection_utc_dates_count"] == 0:
        state["persistence_first_utc_date"] = collection_date
        state["persistence_last_utc_date"] = collection_date
        state["persistence_collection_utc_dates_count"] = 1
    elif state["persistence_last_utc_date"] != collection_date:
        if state["persistence_last_utc_date"] > collection_date:
            raise CollectionError(
                "signer update predates its persisted collection date"
            )
        if state["persistence_collection_utc_dates_count"] >= SQLITE_INTEGER_MAX:
            raise CollectionError("signer collection-date count cannot be incremented")
        state["persistence_last_utc_date"] = collection_date
        state["persistence_collection_utc_dates_count"] += 1

    for room_name, messages in sampled_rooms:
        signed: list[tuple[int, dict[str, Any], str]] = []
        for index, message in enumerate(messages):
            sender = message["from"]
            # An unsigned write may assert any did:key-shaped `from`; only a
            # message that also carries the signature nonce is a signed
            # observation. Anything without one is never counted as a signer.
            if SIGNED_DID_RE.fullmatch(sender) and message.get("nonce") is not None:
                signed.append((index, message, shortened_did(sender)))

        counterparties: set[str] = set()
        for first in range(len(signed)):
            first_position, first_message, first_did = signed[first]
            for middle in range(first + 1, len(signed)):
                middle_position, _, middle_did = signed[middle]
                if middle_position - first_position > 10:
                    break
                if middle_did == first_did:
                    continue
                for last in range(middle + 1, len(signed)):
                    last_position, last_message, last_did = signed[last]
                    if last_position - first_position > 10:
                        break
                    if last_did != first_did:
                        continue
                    seconds = abs(
                        (
                            last_message["_datetime"] - first_message["_datetime"]
                        ).total_seconds()
                    )
                    if seconds <= 900:
                        counterparties.add(first_did)
                        counterparties.add(middle_did)
                        break

        for _, _, did in signed:
            observation = observed_this_tick.setdefault(
                did,
                {
                    "rooms": set(),
                    "counterparty": False,
                },
            )
            observation["rooms"].add(room_name)
            observation["counterparty"] = (
                observation["counterparty"] or did in counterparties
            )

    for did, observation in observed_this_tick.items():
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
                has_counterparty
            FROM signer_dids
            WHERE did = ?
            """,
            (did,),
        ).fetchone()

        if row is None:
            rooms = sorted(observation["rooms"])[:8]
            connection.execute(
                """
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
                VALUES (?, ?, ?, 1, ?, ?, 1, ?, ?, ?)
                """,
                (
                    did,
                    tick_ts,
                    tick_ts,
                    collection_date,
                    collection_date,
                    json.dumps(rooms, ensure_ascii=False, separators=(",", ":")),
                    len(rooms),
                    int(observation["counterparty"]),
                ),
            )
            continue

        (
            first_observed_ts,
            last_observed_ts,
            tick_count,
            collection_first_utc_date,
            collection_last_utc_date,
            collection_utc_dates_count,
            rooms_json,
            has_counterparty,
        ) = row
        if (
            (first_observed_ts is not None and not isinstance(first_observed_ts, str))
            or (last_observed_ts is not None and not isinstance(last_observed_ts, str))
            or isinstance(tick_count, bool)
            or not isinstance(tick_count, int)
            or tick_count < 0
            or tick_count >= SQLITE_INTEGER_MAX
            or isinstance(collection_utc_dates_count, bool)
            or not isinstance(collection_utc_dates_count, int)
            or collection_utc_dates_count < 0
            or collection_utc_dates_count > SQLITE_INTEGER_MAX
            or not isinstance(rooms_json, str)
            or isinstance(has_counterparty, bool)
            or not isinstance(has_counterparty, int)
            or has_counterparty not in (0, 1)
        ):
            raise CollectionError(f"signer database has an invalid row for DID {did}")
        try:
            first_observed = (
                parse_timestamp(first_observed_ts)
                if first_observed_ts is not None
                else None
            )
            last_observed = (
                parse_timestamp(last_observed_ts)
                if last_observed_ts is not None
                else None
            )
        except ValueError as error:
            raise CollectionError(
                f"signer database has an invalid timestamp for DID {did}"
            ) from error
        if (
            (
                tick_count == 0
                and (first_observed is not None or last_observed is not None)
            )
            or ((first_observed is None) != (last_observed is None))
            or (
                first_observed is not None
                and last_observed is not None
                and first_observed > last_observed
            )
            or (last_observed is not None and last_observed > tick_timestamp)
        ):
            raise CollectionError(
                f"signer database has inconsistent observation timestamps for DID {did}"
            )

        if (collection_first_utc_date is None) != (collection_last_utc_date is None):
            raise CollectionError(
                f"signer database has inconsistent collection dates for DID {did}"
            )
        if collection_first_utc_date is None:
            if collection_utc_dates_count != 0:
                raise CollectionError(
                    f"signer database has inconsistent collection dates for DID {did}"
                )
        else:
            try:
                first_collection_date = parse_collection_date(collection_first_utc_date)
                last_collection_date = parse_collection_date(collection_last_utc_date)
            except (TypeError, ValueError) as error:
                raise CollectionError(
                    f"signer database has an invalid collection date for DID {did}"
                ) from error
            maximum_distinct_dates = (
                last_collection_date - first_collection_date
            ).days + 1
            if (
                collection_utc_dates_count == 0
                or last_collection_date < first_collection_date
                or collection_utc_dates_count > maximum_distinct_dates
                or last_collection_date > tick_timestamp.date()
            ):
                raise CollectionError(
                    f"signer database has inconsistent collection dates for DID {did}"
                )
        try:
            rooms = json.loads(rooms_json)
        except (ValueError, RecursionError) as error:
            raise CollectionError(
                f"signer database has invalid rooms for DID {did}"
            ) from error
        if (
            not isinstance(rooms, list)
            or len(rooms) > 8
            or not all(
                isinstance(room, str) and ROOM_NAME_RE.fullmatch(room) is not None
                for room in rooms
            )
            or len(rooms) != len(set(rooms))
        ):
            raise CollectionError(f"signer database has invalid rooms for DID {did}")

        if collection_utc_dates_count == 0:
            collection_first_utc_date = collection_date
            collection_last_utc_date = collection_date
            collection_utc_dates_count = 1
        elif collection_last_utc_date != collection_date:
            if collection_utc_dates_count >= SQLITE_INTEGER_MAX:
                raise CollectionError(
                    f"signer database collection-date count cannot be incremented for DID {did}"
                )
            collection_last_utc_date = collection_date
            collection_utc_dates_count += 1

        rooms = sorted(set(rooms).union(observation["rooms"]))[:8]
        connection.execute(
            """
            UPDATE signer_dids
            SET
                first_observed_ts = ?,
                last_observed_ts = ?,
                tick_count = ?,
                collection_first_utc_date = ?,
                collection_last_utc_date = ?,
                collection_utc_dates_count = ?,
                rooms_json = ?,
                room_count = ?,
                has_counterparty = ?
            WHERE did = ?
            """,
            (
                first_observed_ts or tick_ts,
                tick_ts,
                tick_count + 1,
                collection_first_utc_date,
                collection_last_utc_date,
                collection_utc_dates_count,
                json.dumps(rooms, ensure_ascii=False, separators=(",", ":")),
                len(rooms),
                int(bool(has_counterparty) or observation["counterparty"]),
                did,
            ),
        )

    state["last_updated"] = tick_ts


def signer_funnel_counts(connection: sqlite3.Connection) -> dict[str, int]:
    row = connection.execute(
        """
        SELECT
            COUNT(*),
            COALESCE(SUM(tick_count >= 2), 0),
            COALESCE(SUM(
                tick_count >= 2
                AND collection_utc_dates_count >= 2
            ), 0),
            COALESCE(SUM(
                tick_count >= 2
                AND collection_utc_dates_count >= 2
                AND room_count >= 2
            ), 0),
            COALESCE(SUM(
                tick_count >= 2
                AND collection_utc_dates_count >= 2
                AND room_count >= 2
                AND has_counterparty = 1
            ), 0)
        FROM signer_dids
        """
    ).fetchone()
    return {
        "observed": int(row[0]),
        "two_ticks": int(row[1]),
        "two_collection_dates": int(row[2]),
        "two_rooms": int(row[3]),
        "counterparties": int(row[4]),
    }


def tracking_disclosure(state: dict[str, Any]) -> dict[str, str] | None:
    saturation = state.get("tracking_cap_saturation")
    if saturation is None:
        return None

    started_at = saturation["started_at"]
    released_at = saturation["released_at"]
    return {
        "warning": (
            f"The tracked-DID cap was saturated from {started_at} to {released_at}. "
            "DIDs first appearing during that interval and never re-observed were lost "
            "entirely. DIDs first re-observed after release re-enter the observed count "
            "with restarted persistence counters, so the two-tick and "
            "two-collection-UTC-date stages, and every stage downstream of them, "
            "understate that cohort until those counters rebuild; a DID active then but "
            "first re-observed afterward has a first-observed timestamp no earlier than "
            "that later observation."
        ),
        "methodology": (
            "Observed DIDs are now stored in SQLite without an insertion cap. tracked_cap and "
            "cap_hit report the retired JSON-store limit and its historical saturation; they "
            "no longer gate DID insertion."
        ),
    }


def aggregate_funnel(
    connection: sqlite3.Connection,
    state: dict[str, Any],
    sampled_rooms: int,
    known_rooms: int,
) -> dict[str, Any]:
    counts = signer_funnel_counts(connection)
    census = state.get("census")
    funnel = {
        "well_formed_did_notes": census["total"] if census else None,
        "census_started_at": census["started_at"] if census else None,
        "census_completed_at": census["completed_at"] if census else None,
        "dids_observed_signing": counts["observed"],
        "seen_two_ticks": counts["two_ticks"],
        "two_collection_utc_dates": counts["two_collection_dates"],
        "two_rooms": counts["two_rooms"],
        "signed_reciprocal_alternation": counts["counterparties"],
        "coverage": {
            "sampled_rooms": sampled_rooms,
            "known_rooms": known_rooms,
        },
        "tracked_dids": counts["observed"],
        "tracked_cap": state["tracked_cap"],
        "cap_hit": bool(state["cap_hit"]),
        "signer_state_version": state["version"],
        "collection_started": state["collection_started"],
        "persistence_started_at": state["persistence_started_at"],
        "persistence_reset_at": state["persistence_reset_at"],
        "persistence_collection_utc_dates_count": state[
            "persistence_collection_utc_dates_count"
        ],
    }
    disclosure = tracking_disclosure(state)
    if disclosure is not None:
        funnel["tracking_disclosure"] = disclosure
    return funnel


def collect_tick(
    client: Client,
    signer_state_path: Path,
    signer_cap: int,
    identity_total: int | None = None,
    census_started: str | None = None,
    census_run: dict[str, Any] | None = None,
    lock_timeout: float = SIGNER_LOCK_TIMEOUT,
) -> dict[str, Any]:
    tick_started = time.monotonic()
    rooms = parse_collected_response(
        client,
        "/rooms?format=json&limit=200",
        parse_rooms,
        client.get("/rooms?format=json&limit=200"),
    )
    rooms_observed_at = utc_now()
    # The whole load-modify-save cycle on the signer state runs under one
    # exclusive lock: an unlocked cycle here let the daemon clobber a census
    # result the cron invocation had just recorded.
    with exclusive_state_lock(signer_state_path, lock_timeout):
        database_path = signer_database_path(signer_state_path)
        if signer_state_path.exists() and not database_path.exists():
            raise CollectionError(
                "signer metadata exists without its SQLite store; run migrate_signers.py"
            )

        connection = connect_signer_database(database_path)
        try:
            connection.execute("BEGIN IMMEDIATE")
            if load_tick_outbox(connection) is not None:
                raise CollectionError(
                    "cannot collect while a pending tick outbox awaits publication"
                )
            state = load_signer_state(signer_state_path, signer_cap, connection)
            write_signer_metadata(connection, state)
            connection.commit()
            save_atomic_json(signer_state_path, state)

            connection.execute("BEGIN IMMEDIATE")
            names, room_sampling = room_sample_names(rooms["newest_rooms"], state)

            events_path = "/r/events?format=json&limit=200"
            events_last_seq, events, class_counts, primary_counts = (
                parse_collected_response(
                    client,
                    events_path,
                    parse_events,
                    client.get(events_path),
                )
            )
            tick_ts = utc_now()
            created_this_tick = record_created_rooms(
                connection,
                events,
                tick_ts,
            )
            record_listed_rooms(
                connection,
                rooms["newest_rooms"],
                rooms_observed_at,
                state,
            )
            room_lifecycle = collect_room_revisits(
                client,
                connection,
                tick_ts,
                sampled_room_reads=len(names),
                selector_version=state["selector_version"],
                selector_seed=state["selector_seed"],
                allocation_rotation=state["revisit_allocation_rotation"],
                deadline=tick_started + TICK_REVISIT_DEADLINE_SECONDS,
            )
            state["revisit_allocation_rotation"] = (
                state["revisit_allocation_rotation"] + 1
            ) % len(ROOM_REVISIT_STAGES_SECONDS)
            room_lifecycle_sampling = room_lifecycle.pop("sampling")
            room_lifecycle["created_rooms_observed_this_tick"] = created_this_tick
            room_lifecycle["read_budget"] = read_budget_summary(
                len(names),
                room_lifecycle["attempted_this_tick"],
            )

            sampled: list[tuple[str, list[dict[str, Any]]]] = []
            lobby_body: str | None = None
            lobby_last_seq: int | None = None
            for name in names:
                sample_result = {"id": room_identifier(name), "success": False}
                room_sampling["sampled"].append(sample_result)
                path = f"/r/{urllib.parse.quote(name, safe='')}?format=json&limit=200"
                try:
                    body = client.get(path)
                    messages = parse_collected_response(
                        client,
                        path,
                        parse_room_messages,
                        body,
                        path,
                    )
                    if name == "lobby":
                        lobby_last_seq = parse_collected_response(
                            client,
                            path,
                            extract_last_seq,
                            body,
                            path,
                        )
                except CollectionError:
                    if name == "lobby":
                        raise
                    continue
                sample_result["success"] = True
                sampled.append((name, messages))
                if name == "lobby":
                    lobby_body = body

            if lobby_body is None or lobby_last_seq is None:
                raise CollectionError("lobby was not sampled")

            if identity_total is not None:
                state["census"] = {
                    "total": identity_total,
                    "completed_at": tick_ts,
                    "started_at": census_started,
                }

            update_signer_state(connection, state, sampled, tick_ts)
            funnel = aggregate_funnel(
                connection,
                state,
                len(sampled),
                rooms["rooms_total"],
            )
            tick = {
                "collector_version": COLLECTOR_VERSION,
                "ts": tick_ts,
                **rooms,
                "room_sampling": room_sampling,
                "lobby_last_seq": lobby_last_seq,
                "events_last_seq": events_last_seq,
                "events_window": events,
                "event_class_counts": class_counts,
                "event_primary_class_counts": primary_counts,
                "identity_total": identity_total,
                "identity_census_started": census_started,
                "identity_census_run": census_run,
                "signer_funnel": funnel,
                "room_lifecycle": room_lifecycle,
                "room_lifecycle_sampling": room_lifecycle_sampling,
            }
            outbox = _insert_tick_outbox(connection, tick)
            write_signer_metadata(connection, state)
            connection.commit()
            save_atomic_json(signer_state_path, state)
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    return outbox["tick"]


def absolute_path(value: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        raise argparse.ArgumentTypeError("path must be absolute")
    return path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Append forward-collected Technocore observations to JSONL."
    )
    parser.add_argument(
        "--base-url", required=True, help="Service origin, without a path."
    )
    parser.add_argument("--output", required=True, type=absolute_path)
    parser.add_argument("--telemetry-database", type=absolute_path)
    parser.add_argument("--interval", type=float, default=60.0)
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--retries", type=int, default=4)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--census", action="store_true")
    parser.add_argument("--census-state", type=absolute_path)
    parser.add_argument("--census-pace", type=float, default=0.25)
    parser.add_argument("--signer-state", type=absolute_path)
    parser.add_argument("--signer-cap", type=int, default=200_000)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if (
        not math.isfinite(args.interval)
        or not math.isfinite(args.timeout)
        or not math.isfinite(args.census_pace)
        or args.interval < RATE_BUDGET_WINDOW_SECONDS
        or args.timeout <= 0
        or args.retries < 0
        or args.retries > MAX_RETRIES
        or args.census_pace < 0
        or args.signer_cap <= 0
        or args.signer_cap > SQLITE_INTEGER_MAX
    ):
        raise SystemExit(
            "interval, timeout, and pace must be finite; interval must be at least "
            f"60 seconds; timeout and signer cap must be positive; retries must be "
            f"between 0 and {MAX_RETRIES}; signer cap must fit SQLite; pace cannot "
            "be negative"
        )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    census_state_path = args.census_state or args.output.with_name(
        "identity-census-state.json"
    )
    signer_state_path = args.signer_state or args.output.with_name("signers.json")
    telemetry_path = args.telemetry_database or args.output.with_name(
        "telemetry.sqlite3"
    )
    telemetry_path.parent.mkdir(parents=True, exist_ok=True)
    telemetry: TelemetryStore | None = None

    try:
        while True:
            started = time.monotonic()
            tick_written = False
            cycle_id: int | None = None
            client: Client | None = None
            if telemetry is None:
                try:
                    telemetry = TelemetryStore(telemetry_path)
                except (OSError, sqlite3.Error):
                    print(
                        f"{utc_now()} telemetry degraded; local store is unavailable",
                        file=sys.stderr,
                    )
            if telemetry is not None:
                try:
                    cycle_id = telemetry.start_cycle("collector")
                except (OSError, sqlite3.Error):
                    print(
                        f"{utc_now()} telemetry degraded; cycle could not start",
                        file=sys.stderr,
                    )
                    try:
                        telemetry.close()
                    except (OSError, sqlite3.Error):
                        print(
                            f"{utc_now()} telemetry degraded; local store did not close cleanly",
                            file=sys.stderr,
                        )
                    telemetry = None
            try:
                signer_lock_timeout = (
                    CENSUS_SIGNER_LOCK_TIMEOUT if args.census else SIGNER_LOCK_TIMEOUT
                )
                startup_drained = drain_tick_outbox(
                    args.output,
                    signer_state_path,
                    census_state_path,
                    lock_timeout=signer_lock_timeout,
                )
                if startup_drained:
                    tick_written = True
                else:
                    client = Client(
                        args.base_url,
                        args.timeout,
                        args.retries,
                        telemetry=telemetry if cycle_id is not None else None,
                        cycle_id=cycle_id,
                    )
                    identity_total = None
                    census_started = None
                    census_run = None
                    if args.census:
                        identity_total, census_started, census_run = run_census(
                            client,
                            census_state_path,
                            args.census_pace,
                        )
                        if census_run["shard_read_failures"] or identity_total is None:
                            publication = (
                                f"published total {identity_total}"
                                if identity_total is not None
                                else "no count published"
                            )
                            print(
                                f"{utc_now()} identity census "
                                f"{census_run['stop_reason']}; {publication}; "
                                f"{census_run['shards_outstanding']} shards outstanding; "
                                f"{census_run['shard_read_failures']} shard reads failed; "
                                "causes="
                                f"{json.dumps(census_run['failure_causes'], sort_keys=True)}",
                                file=sys.stderr,
                            )
                    collect_tick(
                        client,
                        signer_state_path,
                        args.signer_cap,
                        identity_total,
                        census_started,
                        census_run,
                        lock_timeout=signer_lock_timeout,
                    )
                    if not drain_tick_outbox(
                        args.output,
                        signer_state_path,
                        census_state_path,
                        lock_timeout=signer_lock_timeout,
                    ):
                        raise CollectionError(
                            "collector transaction committed without a tick outbox"
                        )
                    tick_written = True
            except (CollectionError, OSError, sqlite3.Error) as error:
                if isinstance(error, CollectionError):
                    error_outcome = error.outcome
                    if error_outcome not in {
                        "http_error",
                        "timeout",
                        "transport_error",
                        "decode_error",
                        "empty_response",
                        "invalid_response",
                        "deadline",
                        "collection_error",
                    }:
                        error_outcome = "collection_error"
                elif isinstance(error, sqlite3.Error):
                    error_outcome = "storage_error"
                else:
                    error_outcome = "os_error"
                if telemetry is not None and cycle_id is not None:
                    try:
                        telemetry.finish_cycle(
                            cycle_id,
                            "failure",
                            error_outcome=error_outcome,
                        )
                    except (OSError, sqlite3.Error):
                        print(
                            f"{utc_now()} telemetry degraded; failed cycle was not finalized",
                            file=sys.stderr,
                        )
                        try:
                            telemetry.close()
                        except (OSError, sqlite3.Error):
                            print(
                                f"{utc_now()} telemetry degraded; local store did not close cleanly",
                                file=sys.stderr,
                            )
                        telemetry = None
                failure = (
                    "collection failed after tick write; census publication will replay"
                    if tick_written
                    else "collection failed; no tick written"
                )
                print(f"{utc_now()} {failure}: {error}", file=sys.stderr)
                if args.once or args.census:
                    return 1
            else:
                if telemetry is not None and cycle_id is not None:
                    try:
                        if client is not None and client.telemetry_degraded:
                            telemetry.finish_cycle(
                                cycle_id,
                                "failure",
                                error_outcome="storage_error",
                            )
                            print(
                                f"{utc_now()} telemetry degraded; primary tick was written",
                                file=sys.stderr,
                            )
                        else:
                            telemetry.finish_cycle(cycle_id, "success")
                    except (OSError, sqlite3.Error):
                        print(
                            f"{utc_now()} telemetry degraded; completed cycle was not finalized",
                            file=sys.stderr,
                        )
                        try:
                            telemetry.close()
                        except (OSError, sqlite3.Error):
                            print(
                                f"{utc_now()} telemetry degraded; local store did not close cleanly",
                                file=sys.stderr,
                            )
                        telemetry = None

            if args.once or args.census:
                return 0
            time.sleep(max(0.0, args.interval - (time.monotonic() - started)))
    finally:
        if telemetry is not None:
            try:
                telemetry.close()
            except (OSError, sqlite3.Error):
                print(
                    f"{utc_now()} telemetry degraded; local store did not close cleanly",
                    file=sys.stderr,
                )


if __name__ == "__main__":
    raise SystemExit(main())
