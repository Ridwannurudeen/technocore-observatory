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
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Iterator

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
COLLECTOR_VERSION = "2.6.0"
SELECTOR_VERSION = 1
ROOM_ID_HEX_LENGTH = 16
SIGNER_STATE_VERSION = 4
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

    def get(self, path: str) -> str:
        url = self.base_url + path
        for attempt in range(self.retries + 1):
            request = urllib.request.Request(
                url,
                headers={
                    "Accept": "text/plain, application/json",
                    "User-Agent": f"technocore-observatory/{COLLECTOR_VERSION}",
                },
                method="GET",
            )
            try:
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    body = response.read()
                    if not body:
                        raise CollectionError(f"empty response from {path}")
                    return body.decode("utf-8")
            except urllib.error.HTTPError as error:
                body = error.read().decode("utf-8", errors="replace")
                retryable = error.code == 429 or 500 <= error.code <= 599
                if not retryable or attempt >= self.retries:
                    raise CollectionError(f"GET {path} failed with HTTP {error.code}") from error
                time.sleep(retry_delay(error, body, attempt))
            except (urllib.error.URLError, TimeoutError, UnicodeDecodeError) as error:
                if attempt >= self.retries:
                    raise CollectionError(f"GET {path} failed: {error}") from error
                time.sleep(min(30.0, 2.0**attempt))
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
    payload = (
        json.dumps(record, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n"
    ).encode("utf-8")
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        os.write(descriptor, payload)
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


def run_census(client: Client, state_path: Path, pace: float) -> tuple[int, str]:
    with exclusive_state_lock(state_path, CENSUS_STATE_LOCK_TIMEOUT):
        state = load_census_state(state_path)
        counts: dict[str, int] = state["counts"]

        if len(counts) == 256:
            state = {"version": 1, "started_at": utc_now(), "counts": {}}
            counts = state["counts"]
            save_census_state(state_path, state)

        for index in range(256):
            shard = f"did-{index:02x}"
            if shard in counts:
                continue
            body = client.get(f"/kv/{shard}")
            counts[shard] = parse_shard_count(body, shard)
            save_census_state(state_path, state)
            if pace:
                time.sleep(pace)

        if len(counts) != 256:
            raise CollectionError("identity census ended without all 256 shards")
        return sum(counts.values()), state["started_at"]


def signer_database_path(state_path: Path) -> Path:
    return state_path.with_suffix(".sqlite3")


def initialize_signer_database(connection: sqlite3.Connection) -> None:
    version = connection.execute("PRAGMA user_version").fetchone()[0]
    if version not in (0, 3, SIGNER_STATE_VERSION):
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
        """
    )

    if version == 3:
        row = connection.execute(
            "SELECT state_json FROM signer_metadata WHERE singleton = 1"
        ).fetchone()
        if row is not None:
            try:
                metadata = json.loads(row[0])
            except json.JSONDecodeError as error:
                raise CollectionError("signer database contains invalid metadata JSON") from error
            if not isinstance(metadata, dict) or metadata.get("version") != 3:
                raise CollectionError("signer database has inconsistent v3 metadata")
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

        # Version 3 credited mere signed co-occurrence. Those flags cannot be
        # reinterpreted as A → B → A alternation, so the stage restarts empty.
        connection.execute("UPDATE signer_dids SET has_counterparty = 0")

    connection.execute(f"PRAGMA user_version = {SIGNER_STATE_VERSION}")
    connection.commit()


def connect_signer_database(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path, timeout=5.0)
    try:
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
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
    lock_timeout: float = SIGNER_LOCK_TIMEOUT,
) -> dict[str, Any]:
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
        "signer_funnel": funnel,
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
        args.interval <= 0
        or args.timeout <= 0
        or args.retries < 0
        or args.census_pace < 0
        or args.signer_cap <= 0
    ):
        raise SystemExit(
            "interval, timeout, and signer cap must be positive; retries and pace cannot be negative"
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
            if args.census:
                identity_total, census_started = run_census(
                    client,
                    census_state_path,
                    args.census_pace,
                )
            append_jsonl(
                args.output,
                collect_tick(
                    client,
                    signer_state_path,
                    args.signer_cap,
                    identity_total,
                    census_started,
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
