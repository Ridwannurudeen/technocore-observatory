#!/usr/bin/env python3
"""Forward-only collector for the Technocore Observatory."""

from __future__ import annotations

import argparse
import hashlib
import json
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
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Iterator

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
LAST_SEQ_RE = re.compile(r"\blast[_ ]seq\b\s*[:=]\s*(\d+)", re.IGNORECASE)
SEQ_RE = re.compile(r'"seq"\s*:\s*(\d+)')
RETRY_BODY_RE = re.compile(
    r"(?:retry|wait|after)[^0-9]{0,24}(\d+(?:\.\d+)?)\s*(?:s|sec|seconds?)?",
    re.IGNORECASE,
)
IDLE_RE = re.compile(r"^(\d+(?:\.\d+)?)([smhd]?)$", re.IGNORECASE)
KV_KEY_RE = re.compile(r"^/kv/[a-z0-9][a-z0-9_-]{0,47}/[0-9a-f]{14}\s*$", re.IGNORECASE)
HEX_NAME_RE = re.compile(r"^[0-9a-f]{16}$", re.IGNORECASE)
SIGNED_DID_RE = re.compile(r"^did:key:z6Mk[1-9A-HJ-NP-Za-km-z]{20,100}$")
NONCE_RE = re.compile(r"^[0-9]{1,19}$")

PREFIX_CLASSES = (
    ("mb-", "mailbox"),
    ("p-", "unlisted"),
    ("d-", "ownable"),
    ("e-", "ephemeral"),
)
ROOM_READ_BUDGET = 80
PUBLISHED_READS_PER_MINUTE = 600
MAX_READ_SHARE = 0.15
RATE_BUDGET_WINDOW_SECONDS = 60
BASE_READ_BUDGET = ROOM_READ_BUDGET + 2
TOTAL_READ_BUDGET = int(
    PUBLISHED_READS_PER_MINUTE
    * MAX_READ_SHARE
    * RATE_BUDGET_WINDOW_SECONDS
    / 60
)
ROOM_REVISIT_READ_BUDGET = TOTAL_READ_BUDGET - BASE_READ_BUDGET
TICK_REVISIT_DEADLINE_SECONDS = 300
ROOM_REVISIT_STAGES_SECONDS = (5 * 60, 60 * 60, 24 * 60 * 60)
CENSUS_MAX_PASSES = 5
CENSUS_DEADLINE_SECONDS = 30 * 60
COLLECTOR_VERSION = "2.10.0"
SELECTOR_VERSION = 1
ROOM_ID_HEX_LENGTH = 16
SIGNER_STATE_VERSION = 5
LEDGER_LOCK_TIMEOUT = 15.0
# The daemon fails closed and skips its tick if the signer state is locked; a
# census invocation waits longer because losing its slot discards a completed
# 256-shard walk (the next census run would reset and start over).
SIGNER_LOCK_TIMEOUT = 15.0
CENSUS_SIGNER_LOCK_TIMEOUT = 240.0
CENSUS_STATE_LOCK_TIMEOUT = 15.0


class CollectionError(RuntimeError):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_timestamp(value: str) -> datetime:
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        raise ValueError("timestamp has no timezone")
    return parsed.astimezone(timezone.utc)


def parse_size(value: str) -> int:
    match = re.fullmatch(r"(\d+(?:\.\d+)?)([KMGT]?)", value, re.IGNORECASE)
    if not match:
        raise CollectionError(f"unrecognized byte size: {value!r}")
    multipliers = {
        "": 1,
        "K": 1_000,
        "M": 1_000_000,
        "G": 1_000_000_000,
        "T": 1_000_000_000_000,
    }
    return round(float(match.group(1)) * multipliers[match.group(2).upper()])


def retry_delay(error: urllib.error.HTTPError, body: str, attempt: int) -> float:
    if error.code == 429:
        header = error.headers.get("Retry-After")
        if header:
            try:
                return min(300.0, max(0.0, float(header)))
            except ValueError:
                try:
                    retry_at = parsedate_to_datetime(header)
                    now = datetime.now(retry_at.tzinfo or timezone.utc)
                    return min(300.0, max(0.0, (retry_at - now).total_seconds()))
                except (TypeError, ValueError, OverflowError):
                    pass
        body_match = RETRY_BODY_RE.search(body)
        if body_match:
            return min(300.0, max(0.0, float(body_match.group(1))))
    return min(30.0, 2.0**attempt)


class Client:
    def __init__(self, base_url: str, timeout: float, retries: int) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.retries = retries

    def get(self, path: str, deadline: float | None = None) -> str:
        url = self.base_url + path
        for attempt in range(self.retries + 1):
            remaining = None if deadline is None else deadline - time.monotonic()
            if remaining is not None and remaining <= 0:
                raise CollectionError(f"GET {path} exceeded its deadline")

            request = urllib.request.Request(
                url,
                headers={
                    "Accept": "text/plain, application/json",
                    "User-Agent": f"technocore-observatory/{COLLECTOR_VERSION}",
                },
                method="GET",
            )
            try:
                timeout = self.timeout if remaining is None else min(self.timeout, remaining)
                with urllib.request.urlopen(request, timeout=timeout) as response:
                    body = response.read()
                    if not body:
                        raise CollectionError(f"empty response from {path}")
                    return body.decode("utf-8")
            except urllib.error.HTTPError as error:
                body = error.read().decode("utf-8", errors="replace")
                retryable = error.code == 429 or 500 <= error.code <= 599
                if not retryable or attempt >= self.retries:
                    raise CollectionError(f"GET {path} failed with HTTP {error.code}") from error
                delay = retry_delay(error, body, attempt)
            except (urllib.error.URLError, TimeoutError, UnicodeDecodeError) as error:
                if attempt >= self.retries:
                    raise CollectionError(f"GET {path} failed: {error}") from error
                delay = min(30.0, 2.0**attempt)

            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0 or delay >= remaining:
                    raise CollectionError(f"GET {path} exceeded its deadline")
            time.sleep(delay)
        raise AssertionError("retry loop exhausted unexpectedly")


def parse_idle(value: Any) -> float:
    if isinstance(value, bool):
        raise CollectionError("boolean idle value")
    if isinstance(value, (int, float)) and value >= 0:
        return float(value)
    if not isinstance(value, str):
        raise CollectionError("idle value is not numeric")
    match = IDLE_RE.fullmatch(value.strip())
    if not match:
        raise CollectionError(f"unrecognized idle value: {value!r}")
    multipliers = {"": 1, "s": 1, "m": 60, "h": 3600, "d": 86400}
    return float(match.group(1)) * multipliers[match.group(2).lower()]


def room_from_object(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    name = value.get("name", value.get("room"))
    seq = value.get("seq", value.get("last_seq"))
    idle = value.get("idle", value.get("idle_seconds"))
    if not isinstance(name, str) or isinstance(seq, bool) or not isinstance(seq, int):
        return None
    if seq < 0:
        return None
    return {"name": name, "seq": seq, "idle_seconds": parse_idle(idle)}


def parse_room_row(line: str) -> dict[str, Any] | None:
    try:
        parsed = json.loads(line)
    except json.JSONDecodeError:
        parsed = None

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
    if not name_match or not seq_match or not idle_match:
        return None
    return {
        "name": name_match.group(1),
        "seq": int(seq_match.group(1)),
        "idle_seconds": parse_idle(idle_match.group(1)),
    }


def parse_rooms_json(body: str) -> dict[str, Any]:
    try:
        payload = json.loads(body)
    except ValueError as exc:
        raise CollectionError(f"/rooms JSON did not parse: {exc}") from exc
    if not isinstance(payload, dict):
        raise CollectionError("/rooms JSON is not an object")
    notes = payload.get("notes")
    if not isinstance(notes, dict):
        raise CollectionError("/rooms JSON is missing the notes counters")
    if not isinstance(payload.get("total"), int):
        raise CollectionError("/rooms JSON has a non-numeric room total")
    for key in ("total", "capacity", "bytes"):
        if not isinstance(notes.get(key), int):
            raise CollectionError(f"/rooms JSON has a non-numeric note counter: {key}")

    rows = payload.get("rooms")
    rooms: list[dict[str, Any]] = []
    if isinstance(rows, list):
        for entry in rows:
            room = room_from_object(entry)
            if room is not None:
                rooms.append(room)

    engagement = payload.get("engagement")
    return {
        "rooms_total": int(payload["total"]),
        "room_cap": int(payload.get("capacity", 0)),
        "bytes_stored": int(payload.get("bytes", 0)),
        "notes_total": int(notes["total"]),
        "note_cap": int(notes["capacity"]),
        "newest_rooms": rooms,
        "engagement": engagement if isinstance(engagement, dict) else None,
    }


def parse_rooms(body: str) -> dict[str, Any]:
    if body.lstrip().startswith("{"):
        return parse_rooms_json(body)
    rooms_match = ROOMS_HEADER_RE.search(body)
    notes_match = NOTES_HEADER_RE.search(body)
    if not rooms_match or not notes_match:
        raise CollectionError("/rooms response is missing a recognized rooms or notes header")

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

    shown = int(rooms_match.group("shown"))
    if shown and (unparsed_rows or len(rooms) != shown):
        raise CollectionError(
            f"/rooms declared {shown} rows; parsed {len(rooms)} with {unparsed_rows} unrecognized"
        )

    return {
        "rooms_total": int(rooms_match.group("total")),
        "room_cap": int(rooms_match.group("cap")),
        "bytes_stored": parse_size(rooms_match.group("stored")),
        "notes_total": int(notes_match.group("total")),
        "note_cap": int(notes_match.group("cap")),
        "newest_rooms": rooms,
    }


def extract_last_seq(body: str, path: str) -> int:
    candidates = [int(value) for value in LAST_SEQ_RE.findall(body)]
    candidates.extend(int(value) for value in SEQ_RE.findall(body))

    for raw_line in body.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            seq = value.get("seq")
            if isinstance(seq, int) and not isinstance(seq, bool) and seq >= 0:
                candidates.append(seq)

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


def parse_events(body: str) -> tuple[int, list[dict[str, Any]], dict[str, int], dict[str, int]]:
    events: list[dict[str, Any]] = []
    seen: set[int] = set()

    rows: list[Any]
    stripped = body.lstrip()
    if stripped.startswith("{"):
        try:
            envelope = json.loads(stripped)
        except json.JSONDecodeError as error:
            raise CollectionError("/r/events did not parse as JSON") from error
        messages = envelope.get("messages") if isinstance(envelope, dict) else None
        if not isinstance(messages, list):
            raise CollectionError("/r/events JSON is missing its messages array")
        rows = messages
    else:
        rows = []
        for raw_line in body.splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as error:
                raise CollectionError("/r/events contains a non-JSON event row") from error

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
            or not isinstance(ts, str)
            or sender != "server"
            or not isinstance(text, str)
            or not text.startswith("created ")
            or not text[8:]
        ):
            raise CollectionError("/r/events contains an event with an unexpected shape")
        try:
            parse_timestamp(ts)
        except ValueError as error:
            raise CollectionError("/r/events contains an invalid timestamp") from error
        if seq in seen:
            raise CollectionError("/r/events contains duplicate sequence numbers")
        seen.add(seq)

        name = text[8:]
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
    if len(events) > 200:
        raise CollectionError("/r/events returned more than the documented 200-record cap")

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
    except json.JSONDecodeError as error:
        raise CollectionError(f"{path} did not parse as JSON") from error
    messages = payload.get("messages") if isinstance(payload, dict) else None
    if not isinstance(messages, list):
        raise CollectionError(f"{path} JSON is missing its messages array")

    result: list[dict[str, Any]] = []
    seen: set[int] = set()
    for position, value in enumerate(messages):
        if not isinstance(value, dict):
            continue
        seq = value.get("seq")
        ts = value.get("ts")
        sender = value.get("from")
        if (
            isinstance(seq, bool)
            or not isinstance(seq, int)
            or seq < 0
            or seq in seen
            or not isinstance(ts, str)
            or not isinstance(sender, str)
        ):
            continue
        try:
            parsed_ts = parse_timestamp(ts)
        except ValueError:
            continue
        # The nonce is the signature marker: the service verifies `from` only
        # on the signed lane, and a nonce appears only on signed messages. It
        # is documented as 1-19 digits; a numeric echo is normalized, anything
        # else is treated as absent.
        raw_nonce = value.get("nonce")
        if isinstance(raw_nonce, int) and not isinstance(raw_nonce, bool) and raw_nonce >= 0:
            raw_nonce = str(raw_nonce)
        nonce = raw_nonce if isinstance(raw_nonce, str) and NONCE_RE.fullmatch(raw_nonce) else None
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
    lowered = body.lower()
    if "truncat" in lowered or "more results" in lowered or "next page" in lowered:
        raise CollectionError(f"{shard} reports a truncated result")

    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        parsed = None

    if isinstance(parsed, list):
        if not all(isinstance(item, (dict, list, str)) for item in parsed):
            raise CollectionError(f"{shard} contains unexpected JSON values")
        return len(parsed)

    if isinstance(parsed, dict):
        items = parsed.get("items")
        if isinstance(items, list):
            total = parsed.get("total", len(items))
            if isinstance(total, bool) or not isinstance(total, int) or total != len(items):
                raise CollectionError(f"{shard} JSON indicates an incomplete listing")
            return total

    declared: int | None = None
    rows = 0
    for raw_line in body.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("#"):
            count_match = re.search(r"\b(\d+)\b", line)
            if count_match and declared is None:
                declared = int(count_match.group(1))
            continue
        if not KV_KEY_RE.match(line):
            continue
        rows += 1

    if declared is not None and rows != declared:
        raise CollectionError(f"{shard} declared {declared} rows but returned {rows}")
    return rows


def append_jsonl(path: Path, record: dict[str, Any]) -> None:
    if CHAIN_FIELD in record:
        raise CollectionError(f"collector record already contains {CHAIN_FIELD}")

    # Serialize the chain-tip read and append. Two collector processes that
    # read the same tip must never append sibling records with the same link.
    with exclusive_state_lock(path, LEDGER_LOCK_TIMEOUT):
        verification = verify_ledger(path)
        if not verification["ok"]:
            location = (
                f" at line {verification['first_break']}"
                if verification["first_break"] is not None
                else ""
            )
            raise CollectionError(
                f"tick ledger hash chain is broken{location}: "
                f"{verification['message']}"
            )

        if path.exists() and path.stat().st_size:
            with path.open("rb") as source:
                source.seek(-1, os.SEEK_END)
                if source.read(1) != b"\n":
                    raise CollectionError("tick ledger does not end with a newline")

        previous_hash = (
            verification["tip_sha256"]
            if verification["genesis_line"] is not None
            else None
        )
        chained = dict(record)
        chained[CHAIN_FIELD] = {
            "version": CHAIN_VERSION,
            "previous_sha256": previous_hash,
        }
        chained[CHAIN_FIELD]["tick_sha256"] = hashlib.sha256(
            canonical_tick_hash_bytes(chained)
        ).hexdigest()
        payload = canonical_tick_bytes(chained) + b"\n"

        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            remaining = memoryview(payload)
            while remaining:
                written = os.write(descriptor, remaining)
                if written == 0:
                    raise OSError("zero-byte write while appending tick ledger")
                remaining = remaining[written:]
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


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
        return {"version": 1, "started_at": utc_now(), "counts": {}}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise CollectionError(f"cannot read census state {path}") from error
    counts = value.get("counts")
    if value.get("version") != 1 or not isinstance(counts, dict):
        raise CollectionError("census state has an unexpected shape")
    for shard, count in counts.items():
        if (
            not re.fullmatch(r"did-[0-9a-f]{2}", shard)
            or isinstance(count, bool)
            or not isinstance(count, int)
            or count < 0
        ):
            raise CollectionError("census state contains an invalid shard result")
    return value


def save_atomic_json(path: Path, value: dict[str, Any]) -> None:
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


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
    with exclusive_state_lock(state_path, CENSUS_STATE_LOCK_TIMEOUT):
        state = load_census_state(state_path)
        counts: dict[str, int] = state["counts"]

        if len(counts) == 256:
            state = {"version": 1, "started_at": utc_now(), "counts": {}}
            counts = state["counts"]
            save_census_state(state_path, state)

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
                    body = client.get(f"/kv/{shard}", deadline=deadline)
                    count = parse_shard_count(body, shard)
                except CollectionError as error:
                    shard_read_failures += 1
                    cause = census_failure_cause(error)
                    failure_causes[cause] = failure_causes.get(cause, 0) + 1
                else:
                    counts[shard] = count
                    save_census_state(state_path, state)

                if time.monotonic() >= deadline:
                    deadline_reached = True
                    break
                if pace:
                    time.sleep(min(pace, deadline - time.monotonic()))

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
        total = sum(counts.values()) if completed else None
        return total, state["started_at"], census_run


def signer_database_path(state_path: Path) -> Path:
    return state_path.with_suffix(".sqlite3")


def initialize_signer_database(connection: sqlite3.Connection) -> None:
    version = connection.execute("PRAGMA user_version").fetchone()[0]
    if version not in (0, 3, 4, SIGNER_STATE_VERSION):
        raise CollectionError(f"signer database has unsupported schema version {version}")

    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS signer_metadata (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            state_json TEXT NOT NULL
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
            name TEXT PRIMARY KEY,
            room_id TEXT NOT NULL,
            created_seq INTEGER NOT NULL UNIQUE CHECK (created_seq >= 0),
            created_at TEXT NOT NULL,
            first_observed_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS room_revisits (
            room_name TEXT NOT NULL,
            stage_seconds INTEGER NOT NULL,
            due_at TEXT NOT NULL,
            attempted_at TEXT,
            success INTEGER CHECK (success IN (0, 1)),
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
            PRIMARY KEY (room_name, stage_seconds),
            FOREIGN KEY (room_name) REFERENCES room_ledger(name)
        );

        CREATE INDEX IF NOT EXISTS room_revisits_due
        ON room_revisits (attempted_at, due_at);
        """
    )

    if version in (3, 4):
        row = connection.execute(
            "SELECT state_json FROM signer_metadata WHERE singleton = 1"
        ).fetchone()
        if row is not None:
            try:
                metadata = json.loads(row[0])
            except json.JSONDecodeError as error:
                raise CollectionError("signer database contains invalid metadata JSON") from error
            if not isinstance(metadata, dict) or metadata.get("version") != version:
                raise CollectionError(
                    f"signer database has inconsistent v{version} metadata"
                )
            metadata["version"] = SIGNER_STATE_VERSION
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
        connection.execute("PRAGMA journal_mode = WAL")
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
        "selector_version": SELECTOR_VERSION,
        "selector_seed": secrets.token_hex(16),
        "selector_epoch": -1,
        "selector_frame": [],
        "selector_position": 0,
    }


def validate_signer_metadata(value: Any) -> dict[str, Any]:
    if (
        not isinstance(value, dict)
        or value.get("version") != SIGNER_STATE_VERSION
        or "dids" in value
        or isinstance(value.get("tracked_cap"), bool)
        or not isinstance(value.get("tracked_cap"), int)
        or value["tracked_cap"] <= 0
        or not isinstance(value.get("cap_hit"), bool)
    ):
        raise CollectionError("signer metadata has an unexpected shape")

    selector_fields = (
        "selector_version",
        "selector_seed",
        "selector_epoch",
        "selector_frame",
        "selector_position",
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
        or not isinstance(value["selector_frame"], list)
        or not all(isinstance(name, str) and name != "lobby" for name in value["selector_frame"])
        or len(value["selector_frame"]) != len(set(value["selector_frame"]))
        or isinstance(value["selector_position"], bool)
        or not isinstance(value["selector_position"], int)
        or value["selector_position"] < 0
        or value["selector_position"] > len(value["selector_frame"])
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
            raise CollectionError("signer metadata has an invalid cap-saturation window")
        try:
            started_at = parse_timestamp(saturation["started_at"])
            released_at = parse_timestamp(saturation["released_at"])
        except ValueError as error:
            raise CollectionError("signer metadata has an invalid cap-saturation timestamp") from error
        if released_at < started_at:
            raise CollectionError("signer metadata cap release precedes saturation")

    return value


def write_signer_metadata(
    connection: sqlite3.Connection,
    state: dict[str, Any],
) -> None:
    validate_signer_metadata(state)
    payload = json.dumps(state, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
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
    except json.JSONDecodeError as error:
        raise CollectionError("signer database contains invalid metadata JSON") from error
    validate_signer_metadata(value)

    disk_value: Any = None
    if path.exists():
        try:
            disk_value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            disk_value = None
    if disk_value != value:
        save_atomic_json(path, value)
    return value


def shortened_did(value: str) -> str:
    return value[len("did:key:z6Mk") :]


def room_identifier(name: str) -> str:
    return hashlib.sha256(name.encode("utf-8")).hexdigest()[:ROOM_ID_HEX_LENGTH]


def timestamp_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


def record_created_rooms(
    connection: sqlite3.Connection,
    events: list[dict[str, Any]],
    observed_at: str,
) -> int:
    inserted = 0
    for event in events:
        cursor = connection.execute(
            """
            INSERT OR IGNORE INTO room_ledger (
                name,
                room_id,
                created_seq,
                created_at,
                first_observed_at
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                event["name"],
                room_identifier(event["name"]),
                event["seq"],
                event["ts"],
                observed_at,
            ),
        )
        if cursor.rowcount != 1:
            continue
        inserted += 1
        created_at = parse_timestamp(event["ts"])
        connection.executemany(
            """
            INSERT INTO room_revisits (
                room_name,
                stage_seconds,
                due_at
            )
            VALUES (?, ?, ?)
            """,
            (
                (
                    event["name"],
                    stage_seconds,
                    timestamp_text(created_at + timedelta(seconds=stage_seconds)),
                )
                for stage_seconds in ROOM_REVISIT_STAGES_SECONDS
            ),
        )
    return inserted


def select_due_room_revisits(
    connection: sqlite3.Connection,
    now: str,
    limit: int = ROOM_REVISIT_READ_BUDGET,
) -> tuple[int, list[dict[str, Any]]]:
    if (
        isinstance(limit, bool)
        or not isinstance(limit, int)
        or limit < 0
        or limit > ROOM_REVISIT_READ_BUDGET
    ):
        raise CollectionError("room-revisit selection exceeds its enforced read budget")

    due = connection.execute(
        """
        SELECT COUNT(*)
        FROM room_revisits
        WHERE attempted_at IS NULL AND due_at <= ?
        """,
        (now,),
    ).fetchone()[0]
    rows = connection.execute(
        """
        SELECT
            room_revisits.room_name,
            room_revisits.stage_seconds,
            room_ledger.created_at
        FROM room_revisits
        JOIN room_ledger ON room_ledger.name = room_revisits.room_name
        WHERE room_revisits.attempted_at IS NULL AND room_revisits.due_at <= ?
        ORDER BY
            room_revisits.due_at,
            room_revisits.room_name,
            room_revisits.stage_seconds
        LIMIT ?
        """,
        (now, limit),
    ).fetchall()
    return int(due), [
        {
            "name": name,
            "stage_seconds": int(stage_seconds),
            "created_at": created_at,
        }
        for name, stage_seconds, created_at in rows
    ]


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
        raise CollectionError("collector room revisits exceed the enforced per-tick budget")
    if total_reads > TOTAL_READ_BUDGET:
        raise CollectionError("collector logical reads exceed the enforced per-tick budget")
    reads_per_minute = total_reads * 60 / RATE_BUDGET_WINDOW_SECONDS
    share = reads_per_minute / PUBLISHED_READS_PER_MINUTE
    if share > MAX_READ_SHARE:
        raise CollectionError("collector logical read rate exceeds the 15% ceiling")
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
) -> dict[str, Any]:
    counts = connection.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM room_ledger),
            COUNT(DISTINCT CASE WHEN attempted_at IS NOT NULL THEN room_name END),
            COUNT(DISTINCT CASE WHEN success = 1 THEN room_name END),
            COUNT(DISTINCT CASE
                WHEN success = 1 AND has_second_message = 1 THEN room_name
            END),
            COUNT(CASE WHEN attempted_at IS NOT NULL THEN 1 END),
            COUNT(CASE WHEN success = 0 THEN 1 END)
        FROM room_revisits
        """
    ).fetchone()
    ledger_started_at = connection.execute(
        "SELECT MIN(first_observed_at) FROM room_ledger"
    ).fetchone()[0]
    sender_counts = {
        "signed_did": 0,
        "unsigned_did": 0,
        "server": 0,
        "other": 0,
        "not_observed": 0,
    }
    for sender_class, count in connection.execute(
        """
        SELECT second_sender_class, COUNT(DISTINCT room_name)
        FROM room_revisits
        WHERE
            success = 1
            AND has_second_message = 1
            AND second_sender_class IS NOT NULL
        GROUP BY second_sender_class
        """
    ):
        sender_counts[sender_class] = int(count)

    deferred_due_to_read_budget = due_this_tick - selected_this_tick
    deferred_due_to_deadline = selected_this_tick - len(revisits)

    return {
        "ledger_started_at": ledger_started_at,
        "rooms_in_ledger": int(counts[0]),
        "rooms_revisited": int(counts[1]),
        "rooms_successfully_revisited": int(counts[2]),
        "rooms_with_second_message": int(counts[3]),
        "reads_attempted": int(counts[4]),
        "reads_failed": int(counts[5]),
        "second_sender_classes": sender_counts,
        "created_rooms_observed_this_tick": created_this_tick,
        "due_this_tick": due_this_tick,
        "attempted_this_tick": len(revisits),
        "deferred_due_to_read_budget": deferred_due_to_read_budget,
        "deferred_due_to_deadline": deferred_due_to_deadline,
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
    deadline: float,
) -> dict[str, Any]:
    due, selected = select_due_room_revisits(connection, tick_ts)

    # Enforce the maximum selected work before the first revisit read. The
    # final summary is recomputed from the reads that were actually issued.
    read_budget_summary(sampled_room_reads, len(selected))

    published: list[dict[str, Any]] = []
    for revisit in selected:
        if time.monotonic() >= deadline:
            break

        name = revisit["name"]
        stage_seconds = revisit["stage_seconds"]
        attempted_at = utc_now()
        elapsed_since_creation_seconds = int(
            (
                parse_timestamp(attempted_at)
                - parse_timestamp(revisit["created_at"])
            ).total_seconds()
        )
        if elapsed_since_creation_seconds < stage_seconds:
            raise CollectionError("room revisit attempt precedes its nominal due stage")

        path = f"/r/{urllib.parse.quote(name, safe='')}?format=json&limit=200"
        public_result = {
            "id": room_identifier(name),
            "stage_seconds": stage_seconds,
            "elapsed_since_creation_seconds": elapsed_since_creation_seconds,
            "success": False,
            "message_count": None,
            "has_second_message": None,
            "second_sender_class": None,
        }
        try:
            messages = parse_room_messages(
                client.get(path, deadline=deadline),
                path,
            )
        except CollectionError:
            connection.execute(
                """
                UPDATE room_revisits
                SET attempted_at = ?, success = 0
                WHERE room_name = ? AND stage_seconds = ?
                """,
                (attempted_at, name, stage_seconds),
            )
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
                message_count = ?,
                has_second_message = ?,
                second_sender_class = ?
            WHERE room_name = ? AND stage_seconds = ?
            """,
            (
                attempted_at,
                len(messages),
                int(has_second_message),
                second_sender_class,
                name,
                stage_seconds,
            ),
        )
        public_result.update(
            {
                "success": True,
                "message_count": len(messages),
                "has_second_message": has_second_message,
                "second_sender_class": second_sender_class,
            }
        )
        published.append(public_result)

    return room_lifecycle_summary(
        connection,
        created_this_tick=0,
        due_this_tick=due,
        selected_this_tick=len(selected),
        revisits=published,
    )


def selector_frame_id(state: dict[str, Any]) -> str:
    identifiers = sorted(
        [room_identifier("lobby")] + [room_identifier(name) for name in state["selector_frame"]]
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
                hashlib.sha256(f"{seed}:{epoch}:".encode("ascii") + name.encode("utf-8")).digest(),
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
    observed_this_tick: dict[str, dict[str, Any]] = {}
    collection_date = parse_timestamp(tick_ts).date().isoformat()

    if state["persistence_collection_utc_dates_count"] == 0:
        state["persistence_first_utc_date"] = collection_date
        state["persistence_last_utc_date"] = collection_date
        state["persistence_collection_utc_dates_count"] = 1
    elif state["persistence_last_utc_date"] != collection_date:
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
            observation["counterparty"] = observation["counterparty"] or did in counterparties

    for did, observation in observed_this_tick.items():
        row = connection.execute(
            """
            SELECT
                first_observed_ts,
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
            tick_count,
            collection_first_utc_date,
            collection_last_utc_date,
            collection_utc_dates_count,
            rooms_json,
            has_counterparty,
        ) = row
        try:
            rooms = json.loads(rooms_json)
        except json.JSONDecodeError as error:
            raise CollectionError(f"signer database has invalid rooms for DID {did}") from error
        if not isinstance(rooms, list) or not all(isinstance(room, str) for room in rooms):
            raise CollectionError(f"signer database has invalid rooms for DID {did}")

        if collection_utc_dates_count == 0:
            collection_first_utc_date = collection_date
            collection_last_utc_date = collection_date
            collection_utc_dates_count = 1
        elif collection_last_utc_date != collection_date:
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
        "persistence_collection_utc_dates_count": state["persistence_collection_utc_dates_count"],
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
    rooms = parse_rooms(client.get("/rooms?format=json&limit=200"))
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
            state = load_signer_state(signer_state_path, signer_cap, connection)
            names, room_sampling = room_sample_names(rooms["newest_rooms"], state)

            sampled: list[tuple[str, list[dict[str, Any]]]] = []
            lobby_body: str | None = None
            for name in names:
                sample_result = {"id": room_identifier(name), "success": False}
                room_sampling["sampled"].append(sample_result)
                path = f"/r/{urllib.parse.quote(name, safe='')}?format=json&limit=200"
                try:
                    body = client.get(path)
                    messages = parse_room_messages(body, path)
                except CollectionError:
                    if name == "lobby":
                        raise
                    continue
                sample_result["success"] = True
                sampled.append((name, messages))
                if name == "lobby":
                    lobby_body = body

            if lobby_body is None:
                raise CollectionError("lobby was not sampled")

            events_last_seq, events, class_counts, primary_counts = parse_events(
                client.get("/r/events?format=json&limit=200")
            )
            tick_ts = utc_now()
            created_this_tick = record_created_rooms(
                connection,
                events,
                tick_ts,
            )
            room_lifecycle = collect_room_revisits(
                client,
                connection,
                tick_ts,
                sampled_room_reads=len(names),
                deadline=tick_started + TICK_REVISIT_DEADLINE_SECONDS,
            )
            room_lifecycle["created_rooms_observed_this_tick"] = created_this_tick
            room_lifecycle["read_budget"] = read_budget_summary(
                len(names),
                room_lifecycle["attempted_this_tick"],
            )
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
            write_signer_metadata(connection, state)
            connection.commit()
            save_atomic_json(signer_state_path, state)
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    return {
        "collector_version": COLLECTOR_VERSION,
        "ts": tick_ts,
        **rooms,
        "room_sampling": room_sampling,
        "lobby_last_seq": extract_last_seq(lobby_body, "/r/lobby"),
        "events_last_seq": events_last_seq,
        "events_window": events,
        "event_class_counts": class_counts,
        "event_primary_class_counts": primary_counts,
        "identity_total": identity_total,
        "identity_census_started": census_started,
        "identity_census_run": census_run,
        "signer_funnel": funnel,
        "room_lifecycle": room_lifecycle,
    }


def absolute_path(value: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        raise argparse.ArgumentTypeError("path must be absolute")
    return path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Append forward-collected Technocore observations to JSONL."
    )
    parser.add_argument("--base-url", required=True, help="Service origin, without a path.")
    parser.add_argument("--output", required=True, type=absolute_path)
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
        args.interval < RATE_BUDGET_WINDOW_SECONDS
        or args.timeout <= 0
        or args.retries < 0
        or args.census_pace < 0
        or args.signer_cap <= 0
    ):
        raise SystemExit(
            "interval must be at least 60 seconds; timeout and signer cap must be "
            "positive; retries and pace cannot be negative"
        )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    census_state_path = args.census_state or args.output.with_name("identity-census-state.json")
    signer_state_path = args.signer_state or args.output.with_name("signers.json")
    client = Client(args.base_url, args.timeout, args.retries)

    while True:
        started = time.monotonic()
        try:
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
                        f"{utc_now()} identity census {census_run['stop_reason']}; "
                        f"{publication}; "
                        f"{census_run['shards_outstanding']} shards outstanding; "
                        f"{census_run['shard_read_failures']} shard reads failed; "
                        f"causes={json.dumps(census_run['failure_causes'], sort_keys=True)}",
                        file=sys.stderr,
                    )
            append_jsonl(
                args.output,
                collect_tick(
                    client,
                    signer_state_path,
                    args.signer_cap,
                    identity_total,
                    census_started,
                    census_run,
                    lock_timeout=(
                        CENSUS_SIGNER_LOCK_TIMEOUT if args.census else SIGNER_LOCK_TIMEOUT
                    ),
                ),
            )
        except (CollectionError, OSError, sqlite3.Error) as error:
            print(f"{utc_now()} collection failed; no tick written: {error}", file=sys.stderr)
            if args.once or args.census:
                return 1

        if args.once or args.census:
            return 0
        time.sleep(max(0.0, args.interval - (time.monotonic() - started)))


if __name__ == "__main__":
    raise SystemExit(main())
