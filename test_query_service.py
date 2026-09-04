import ast
import hashlib
import http.client
import json
import re
import socket
import sqlite3
import threading
import urllib.parse
from contextlib import nullcontext
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

import pytest

import benchmark_search
import query_service
from api_contract import (
    CONTRACT_VERSION,
    MAX_RESPONSE_BYTES,
    common_metadata,
    text_bytes,
)


DID = "did:key:z6Mk" + "a" * 40
LEGACY_DID = "did:key:z6Mk" + "b" * 40
FALSE_DID = "did:key:z6Mk" + "d" * 40
HUGE_JSON_INTEGER = "1" * 4_301
DEEPLY_NESTED_JSON = "[" * 5_000 + "0" + "]" * 5_000


def room_id(name):
    return hashlib.sha256(name.encode("utf-8")).hexdigest()[:16]


def insert_room(
    connection,
    name,
    *,
    created_seq,
    short_id=None,
    created_at="2026-08-30T08:00:00Z",
    observed_at="2026-08-30T08:00:05Z",
    listed_at=None,
):
    connection.execute(
        """
        INSERT INTO room_ledger (
            name,
            room_id,
            room_sha256,
            created_seq,
            created_at,
            first_observed_at,
            last_listed_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            name,
            short_id or room_id(name),
            hashlib.sha256(name.encode("utf-8")).hexdigest(),
            created_seq,
            created_at,
            observed_at,
            listed_at,
        ),
    )


@pytest.fixture
def query_database(tmp_path):
    path = tmp_path / "signers.sqlite3"
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE signer_metadata (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            state_json TEXT NOT NULL
        );

        CREATE TABLE signer_dids (
            did TEXT PRIMARY KEY,
            first_observed_ts TEXT,
            last_observed_ts TEXT,
            tick_count INTEGER,
            collection_first_utc_date TEXT,
            collection_last_utc_date TEXT,
            collection_utc_dates_count INTEGER,
            rooms_json TEXT,
            room_count INTEGER,
            has_counterparty INTEGER
        );

        CREATE TABLE room_ledger (
            created_seq INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            room_id TEXT NOT NULL,
            room_sha256 TEXT NOT NULL,
            created_at TEXT NOT NULL,
            first_observed_at TEXT NOT NULL,
            last_listed_at TEXT
        );

        CREATE INDEX room_ledger_room_id ON room_ledger (room_id);
        CREATE INDEX room_ledger_name
        ON room_ledger (name, created_seq DESC);

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
            aged_out_at TEXT,
            PRIMARY KEY (room_created_seq, stage_seconds),
            FOREIGN KEY (room_created_seq) REFERENCES room_ledger(created_seq)
        );

        CREATE VIRTUAL TABLE room_search USING fts5(
            name,
            content='room_ledger',
            content_rowid='rowid',
            tokenize='trigram case_sensitive 1'
        );

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
        """
    )
    metadata = {
        "version": 6,
        "latest_room_listing_observed_at": "2026-08-30T09:00:00Z",
        "last_updated": "2026-08-30T09:00:00Z",
    }
    connection.execute(
        "INSERT INTO signer_metadata (singleton, state_json) VALUES (1, ?)",
        (json.dumps(metadata),),
    )

    insert_room(connection, "Ab", created_seq=1)
    insert_room(connection, "ab", created_seq=2)
    insert_room(connection, "prefix-CaseNeedle-suffix", created_seq=3)
    insert_room(connection, "prefix-caseneedle-suffix", created_seq=4)
    hostile = "evil-forged\r\ncontract_version: forged<script>alert(1)</script>"
    insert_room(connection, hostile, created_seq=5)
    for index in range(22):
        insert_room(connection, f"needle-room-{index:02d}", created_seq=100 + index)

    evidence_name = "evidence-room"
    insert_room(
        connection,
        evidence_name,
        created_seq=200,
        listed_at="2026-08-30T09:00:00Z",
    )
    connection.executemany(
        """
        INSERT INTO room_revisits (
            room_created_seq,
            stage_seconds,
            due_at,
            attempted_at,
            success,
            message_count,
            has_second_message,
            second_sender_class,
            outcome
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            (
                200,
                300,
                "2026-08-30T08:05:00Z",
                "2026-08-30T08:05:02Z",
                1,
                2,
                1,
                "signed_did",
                "present_at_last_check",
            ),
            (
                200,
                3600,
                "2026-08-30T09:00:00Z",
                "2026-08-30T09:00:04Z",
                0,
                None,
                None,
                None,
                "absent_at_last_check",
            ),
            (
                200,
                86400,
                "2026-08-31T08:00:00Z",
                None,
                None,
                None,
                None,
                None,
                None,
            ),
        ),
    )

    insert_room(
        connection,
        "collision-one",
        created_seq=201,
        short_id="deadbeefdeadbeef",
    )
    insert_room(
        connection,
        "collision-two",
        created_seq=202,
        short_id="deadbeefdeadbeef",
    )

    oversized_name = "oversize-marker-" + "x" * 70_000
    insert_room(connection, oversized_name, created_seq=203)

    insert_room(connection, "unchecked-room", created_seq=210)
    connection.execute(
        """
        INSERT INTO room_revisits (
            room_created_seq, stage_seconds, due_at, attempted_at
        )
        VALUES (?, ?, ?, ?)
        """,
        (210, 86400, "2026-08-31T08:00:00Z", None),
    )

    # Terminally finalized aged-out room: never attempted, every window
    # closed, aged_out_at stamped by the collector's bounded finalizer.
    insert_room(connection, "finalized-room", created_seq=211)
    connection.executemany(
        """
        INSERT INTO room_revisits (
            room_created_seq, stage_seconds, due_at, attempted_at, aged_out_at
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            (211, 300, "2026-08-30T08:05:00Z", None, "2026-09-01T00:00:00Z"),
            (211, 3600, "2026-08-30T09:00:00Z", None, "2026-09-01T00:00:00Z"),
            (211, 86400, "2026-08-31T08:00:00Z", None, "2026-09-01T00:00:00Z"),
        ),
    )

    insert_room(connection, "legacy-failed-room", created_seq=204)
    connection.execute(
        """
        INSERT INTO room_revisits (
            room_created_seq,
            stage_seconds,
            due_at,
            attempted_at,
            success,
            outcome
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            204,
            300,
            "2026-08-30T08:05:00Z",
            "2026-08-30T08:05:02Z",
            0,
            None,
        ),
    )
    insert_room(connection, "no-check-room", created_seq=205)

    insert_room(
        connection,
        "generation-room",
        created_seq=206,
        created_at="2026-08-29T08:00:00Z",
        observed_at="2026-08-29T08:00:05Z",
    )
    insert_room(
        connection,
        "generation-room",
        created_seq=207,
        created_at="2026-08-30T08:00:00Z",
        observed_at="2026-08-30T08:00:05Z",
        listed_at="2026-08-30T09:00:00Z",
    )
    connection.executemany(
        """
        INSERT INTO room_revisits (
            room_created_seq,
            stage_seconds,
            due_at,
            attempted_at,
            success,
            message_count,
            has_second_message,
            second_sender_class,
            outcome
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            (
                206,
                86400,
                "2026-08-30T08:00:00Z",
                "2026-08-30T09:00:10Z",
                0,
                None,
                None,
                None,
                "absent_at_last_check",
            ),
            (
                207,
                300,
                "2026-08-30T08:05:00Z",
                "2026-08-30T08:05:02Z",
                1,
                2,
                1,
                "signed_did",
                "present_at_last_check",
            ),
        ),
    )
    insert_room(
        connection,
        "G2",
        created_seq=208,
        created_at="2026-08-29T08:00:00Z",
        observed_at="2026-08-29T08:00:05Z",
    )
    insert_room(
        connection,
        "G2",
        created_seq=209,
        created_at="2026-08-30T08:00:00Z",
        observed_at="2026-08-30T08:00:05Z",
    )

    retained_rooms = [f"retained-{index}" for index in range(8)]
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
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            DID.removeprefix("did:key:z6Mk"),
            "2026-08-28T08:00:00Z",
            "2026-08-30T08:00:00Z",
            19,
            "2026-08-28",
            "2026-08-30",
            3,
            json.dumps(retained_rooms),
            8,
            1,
        ),
    )
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
        VALUES (?, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
        """,
        (LEGACY_DID.removeprefix("did:key:z6Mk"),),
    )
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
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            FALSE_DID.removeprefix("did:key:z6Mk"),
            "2026-08-30T08:00:00Z",
            "2026-08-30T08:00:00Z",
            1,
            "2026-08-30",
            "2026-08-30",
            1,
            "[]",
            0,
            0,
        ),
    )
    connection.commit()
    connection.close()
    return path


@pytest.fixture
def snapshot_root(tmp_path):
    root = tmp_path / "snapshots"
    api = root / "api" / "v1"
    api.mkdir(parents=True)
    common = {
        "contract_version": CONTRACT_VERSION,
        "generated_at": "2026-08-30T09:01:00Z",
        "source_observed_at": "2026-08-30T09:00:00Z",
        "derived_at": "2026-08-30T09:00:30Z",
        "published_at": "2026-08-30T09:01:00Z",
        "valid_until": "2026-08-30T09:15:00Z",
        "freshness": "fresh",
        "collector_version": "2.10.0",
        "methodology_version": "1.12.0",
        "schema_version": 6,
        "window": {"started_at": "2026-08-30T08:00:00Z"},
        "coverage": {"attempts": 10},
        "limitations": ["forward observations only"],
    }
    payloads = {
        "status": {
            **common,
            "ledger_chain_head": "a" * 64,
            "status": {"origin": "reachable"},
        },
        "incidents": {
            **common,
            "ledger_chain_head": "a" * 64,
            "incidents": [
                {"id": "old", "opened_at": "2026-08-29T08:00:00Z"},
                {"id": "new", "opened_at": "2026-08-30T08:30:00Z"},
            ],
        },
        "changes": {
            **common,
            "changes": [
                {"id": "old", "first_observed_at": "2026-08-29T08:00:00Z"},
                {"id": "new", "first_observed_at": "2026-08-30T08:30:00Z"},
            ],
        },
        "methodology": {
            **common,
            "source_observed_at": None,
            "valid_until": None,
            "freshness": "not_applicable",
            "methodology": {"version": "1.12.0"},
        },
    }
    for name, payload in payloads.items():
        (api / f"{name}.json").write_text(
            json.dumps(payload, separators=(",", ":")), encoding="utf-8"
        )
        (api / f"{name}.txt").write_bytes(text_bytes(payload))
    (api / "changes.txt").unlink()
    return root


@pytest.fixture
def running_server(query_database, snapshot_root):
    server = query_service.create_server(
        database_path=query_database,
        snapshot_root=snapshot_root,
        port=0,
        collector_version="2.10.0",
        methodology_version="1.12.0",
        clock=lambda: datetime(2026, 8, 30, 9, 1, tzinfo=timezone.utc),
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def request(server, method, target, *, connection=None):
    owned = connection is None
    client = connection or http.client.HTTPConnection(
        "127.0.0.1", server.server_address[1], timeout=5
    )
    try:
        client.request(method, target)
        response = client.getresponse()
        return response.status, dict(response.getheaders()), response.read()
    finally:
        if owned:
            client.close()


def json_request(server, target):
    status, headers, body = request(server, "GET", target)
    return status, headers, json.loads(body)


def test_server_defaults_to_loopback(query_database, snapshot_root):
    server = query_service.create_server(
        database_path=query_database,
        snapshot_root=snapshot_root,
        port=0,
    )
    try:
        assert server.server_address[0] == "127.0.0.1"
    finally:
        server.server_close()


def test_search_requires_a_query_and_plain_html_has_no_default_listing(running_server):
    status, _, payload = json_request(
        running_server, "/api/v1/rooms/search?format=json"
    )
    assert status == 400
    assert payload["error"] == "missing_query"

    status, headers, body = request(running_server, "GET", "/rooms/")
    assert status == 200
    assert headers["Content-Type"].startswith("text/html")
    form = body.partition(b'<form class="room-search"')[2].partition(b"</form>")[0]
    assert form
    assert b'action="/rooms/"' in form
    assert b'method="get"' in form
    assert b'id="room-query"' in form
    assert b'name="q"' in form
    assert b'id="search-feedback"' in form
    assert b'aria-live="polite"' in form
    assert b"needle-room" not in body

    status, _, body = request(running_server, "GET", "/rooms/?limit=1")
    assert status == 400
    assert b"error: missing_query" in body


@pytest.mark.parametrize(
    "target",
    (
        "/api/v1/rooms/search?format=%6a%73%6f%6e",
        "/api/v1/rooms/search?form%61t=json",
    ),
)
def test_early_errors_honor_percent_decoded_json_format(running_server, target):
    status, headers, body = request(running_server, "GET", target)

    assert status == 400
    assert headers["Content-Type"].startswith("application/json")
    assert json.loads(body)["error"] == "missing_query"


def test_malformed_query_with_literal_json_format_preserves_json_error(
    running_server,
):
    status, headers, body = request(
        running_server,
        "GET",
        "/api/v1/rooms/search?q=%&format=json",
    )

    assert status == 400
    assert headers["Content-Type"].startswith("application/json")
    assert json.loads(body)["error"] == "invalid_encoding"


def test_get_with_a_framed_body_is_rejected_and_connection_closed(running_server):
    second_request = (
        b"GET /api/v1/status?format=json HTTP/1.1\r\nHost: localhost\r\n\r\n"
    )
    first_request = (
        b"GET /api/v1/status?format=json HTTP/1.1\r\n"
        b"Host: localhost\r\n"
        + f"Content-Length: {len(second_request)}\r\n".encode("ascii")
        + b"\r\n"
        + second_request
    )
    client = socket.create_connection(
        ("127.0.0.1", running_server.server_address[1]), timeout=5
    )
    client.settimeout(5)
    try:
        client.sendall(first_request)
        response = bytearray()
        while True:
            chunk = client.recv(65_536)
            if not chunk:
                break
            response.extend(chunk)
    finally:
        client.close()

    assert response.count(b"HTTP/1.1") == 1
    assert response.startswith(b"HTTP/1.1 400")
    assert b"Connection: close\r\n" in response
    assert (
        json.loads(response.partition(b"\r\n\r\n")[2])["error"]
        == "request_body_not_allowed"
    )


def test_oversized_numeric_content_length_is_rejected_without_integer_parsing(
    running_server,
):
    request_bytes = (
        b"GET /api/v1/status?format=json HTTP/1.1\r\n"
        b"Host: localhost\r\n"
        b"Content-Length: " + b"1" * 4_301 + b"\r\n\r\n"
    )
    client = socket.create_connection(
        ("127.0.0.1", running_server.server_address[1]), timeout=5
    )
    client.settimeout(5)
    try:
        client.sendall(request_bytes)
        response = bytearray()
        while True:
            chunk = client.recv(65_536)
            if not chunk:
                break
            response.extend(chunk)
    finally:
        client.close()

    assert response.startswith(b"HTTP/1.1 400")
    assert response.count(b"HTTP/1.1") == 1
    assert b"Connection: close\r\n" in response
    assert (
        json.loads(response.partition(b"\r\n\r\n")[2])["error"]
        == "request_body_not_allowed"
    )


def test_short_query_is_case_sensitive_exact_name_search(running_server):
    status, _, payload = json_request(
        running_server, "/api/v1/rooms/search?q=Ab&format=json"
    )
    assert status == 200
    assert payload["match_mode"] == "exact_name"
    assert [result["name"] for result in payload["results"]] == ["Ab"]


def test_substring_search_is_case_sensitive_and_capped_with_a_probe(running_server):
    status, _, payload = json_request(
        running_server,
        "/api/v1/rooms/search?q=CaseNeedle&limit=20&format=json",
    )
    assert status == 200
    assert [result["name"] for result in payload["results"]] == [
        "prefix-CaseNeedle-suffix"
    ]

    status, _, payload = json_request(
        running_server, "/api/v1/rooms/search?q=needle&limit=20&format=json"
    )
    assert status == 200
    assert payload["match_mode"] == "case_sensitive_substring"
    assert payload["capped"] is True
    assert len(payload["results"]) == 20

    status, _, payload = json_request(
        running_server, "/api/v1/rooms/search?q=needle&format=json"
    )
    assert status == 200
    assert payload["limit"] == 10
    assert len(payload["results"]) == 10
    assert payload["capped"] is True


def test_search_collapses_same_name_generations_to_the_newest_room(running_server):
    status, _, exact = json_request(
        running_server,
        "/api/v1/rooms/search?q=G2&format=json",
    )
    assert status == 200
    assert exact["match_mode"] == "exact_name"
    assert [result["created_at"] for result in exact["results"]] == [
        "2026-08-30T08:00:00Z"
    ]

    status, _, substring = json_request(
        running_server,
        "/api/v1/rooms/search?q=generation-room&format=json",
    )
    assert status == 200
    assert substring["match_mode"] == "case_sensitive_substring"
    assert len(substring["results"]) == 1
    assert substring["results"][0]["created_at"] == "2026-08-30T08:00:00Z"
    assert substring["results"][0]["latest_lifecycle_state"] == "present_at_last_check"
    assert substring["results"][0]["last_check_at"] == "2026-08-30T08:05:02Z"


def test_search_accepts_the_exact_utf8_boundary_and_quotes_fts_syntax(
    running_server,
):
    boundary = urllib.parse.quote("\U0001f642" * 80, safe="")
    status, _, payload = json_request(
        running_server, f"/api/v1/rooms/search?q={boundary}&format=json"
    )
    assert status == 200
    assert payload["results"] == []

    syntax = urllib.parse.quote('a OR b:"*', safe="")
    status, _, payload = json_request(
        running_server, f"/api/v1/rooms/search?q={syntax}&format=json"
    )
    assert status == 200
    assert payload["query"] == 'a OR b:"*'


@pytest.mark.parametrize(
    "target,error",
    (
        ("/api/v1/rooms/search?q=&format=json", "missing_query"),
        ("/api/v1/rooms/search?q=x&limit=0&format=json", "invalid_limit"),
        ("/api/v1/rooms/search?q=x&limit=21&format=json", "invalid_limit"),
        ("/api/v1/rooms/search?q=x&limit=1&limit=2&format=json", "duplicate_parameter"),
        ("/api/v1/rooms/search?q=x&page=2&format=json", "unexpected_parameter"),
        ("/api/v1/rooms/search?q=%0A&format=json", "invalid_query"),
        ("/api/v1/rooms/search?q=" + "x" * 81 + "&format=json", "invalid_query"),
        (
            "/api/v1/rooms/search?q=x&a=1&b=2&c=3&d=4&e=5&f=6&g=7&format=json",
            "too_many_parameters",
        ),
    ),
)
def test_query_bounds_are_enforced(running_server, target, error):
    status, _, payload = json_request(running_server, target)
    assert status == 400
    assert payload["error"] == error


def test_invalid_format_fails_in_the_default_plain_text_representation(running_server):
    status, headers, body = request(
        running_server, "GET", "/api/v1/rooms/search?q=x&format=xml"
    )
    assert status == 400
    assert headers["Content-Type"].startswith("text/plain")
    assert b"error: invalid_format\n" in body


def test_request_target_and_response_byte_caps_are_enforced(running_server):
    status, _, body = request(
        running_server,
        "GET",
        "/api/v1/rooms/search?q=" + "x" * 2_100,
    )
    assert status == 414
    assert len(body) <= MAX_RESPONSE_BYTES

    status, _, body = request(
        running_server,
        "GET",
        "/api/v1/rooms/search?q=oversize-marker&format=json",
    )
    assert status == 413
    assert len(body) <= MAX_RESPONSE_BYTES


def test_hostile_names_are_valid_json_and_cannot_forge_plain_text_fields(
    running_server,
):
    target = "/api/v1/rooms/search?q=forged&format=json"
    status, _, body = request(running_server, "GET", target)
    assert status == 200
    payload = json.loads(body)
    assert payload["results"][0]["name_trust"] == "untrusted"
    assert "\r\n" in payload["results"][0]["name"]
    assert b"\\r\\n" in body
    assert b"\r\ncontract_version: forged" not in body

    status, _, body = request(running_server, "GET", "/api/v1/rooms/search?q=forged")
    assert status == 200
    assert b"name_trust: untrusted" in body
    assert b"\\r\\n" in body
    assert b"\r\ncontract_version: forged" not in body

    for hostile in ("ab\u2028contract_version: forged", "ab\u202espoof"):
        encoded = urllib.parse.quote(hostile, safe="")
        for suffix in ("&format=json", ""):
            status, _, body = request(
                running_server,
                "GET",
                f"/api/v1/rooms/search?q={encoded}{suffix}",
            )
            assert status == 400
            assert b"invalid_query" in body
            assert hostile.encode("utf-8") not in body


def test_common_metadata_and_security_headers_are_present(running_server):
    status, headers, payload = json_request(
        running_server, "/api/v1/rooms/search?q=needle&limit=1&format=json"
    )
    assert status == 200
    assert payload["contract_version"] == CONTRACT_VERSION
    assert payload["generated_at"].endswith("Z")
    assert payload["source_observed_at"] == "2026-08-30T09:00:00Z"
    assert payload["valid_until"] == "2026-08-30T09:15:00Z"
    assert payload["freshness"] == "fresh"
    assert payload["collector_version"] == "2.10.0"
    assert payload["methodology_version"] == "1.12.0"
    assert payload["schema_version"] == 6
    assert isinstance(payload["window"], dict)
    assert isinstance(payload["coverage"], dict)
    assert isinstance(payload["limitations"], list)
    assert "ledger_chain_head" not in payload
    assert headers["Access-Control-Allow-Origin"] == "*"
    assert headers["X-Content-Type-Options"] == "nosniff"
    assert headers["Referrer-Policy"] == "no-referrer"
    assert headers["X-Robots-Tag"] == "noindex, nofollow, noarchive"
    assert headers["Cache-Control"] == "no-store"


@pytest.mark.parametrize(
    ("moment", "generated_at", "freshness"),
    (
        (
            datetime(2026, 8, 30, 9, 15, 0, 500_000, tzinfo=timezone.utc),
            "2026-08-30T09:15:00Z",
            "fresh",
        ),
        (
            datetime(2026, 8, 30, 9, 15, 1, 500_000, tzinfo=timezone.utc),
            "2026-08-30T09:15:01Z",
            "stale",
        ),
        (
            datetime(2026, 8, 30, 9, 16, tzinfo=timezone.utc),
            "2026-08-30T09:16:00Z",
            "stale",
        ),
    ),
)
def test_database_metadata_expires_against_the_injected_clock(
    query_database,
    snapshot_root,
    moment,
    generated_at,
    freshness,
):
    application = query_service.QueryApplication(
        query_service.ServiceConfig(
            database_path=query_database,
            snapshot_root=snapshot_root,
            clock=lambda: moment,
        )
    )

    response = application.room_search_api(
        {"q": "needle", "limit": "1", "format": "json"}
    )
    payload = json.loads(response.body)

    assert payload["generated_at"] == generated_at
    assert payload["source_observed_at"] == "2026-08-30T09:00:00Z"
    assert payload["index_observed_at"] == "2026-08-30T09:00:00Z"
    assert payload["valid_until"] == "2026-08-30T09:15:00Z"
    assert payload["freshness"] == freshness


def test_database_metadata_without_an_observation_uses_injected_clock(
    query_database,
    snapshot_root,
):
    connection = sqlite3.connect(query_database)
    connection.execute(
        "UPDATE signer_metadata SET state_json = ? WHERE singleton = 1",
        (json.dumps({"version": 6, "latest_room_listing_observed_at": None}),),
    )
    connection.commit()
    connection.close()
    application = query_service.QueryApplication(
        query_service.ServiceConfig(
            database_path=query_database,
            snapshot_root=snapshot_root,
            clock=lambda: datetime(2026, 8, 30, 9, 16, tzinfo=timezone.utc),
        )
    )

    response = application.room_search_api({"q": "zzzz", "format": "json"})
    payload = json.loads(response.body)

    assert payload["generated_at"] == "2026-08-30T09:16:00Z"
    assert payload["source_observed_at"] is None
    assert payload["index_observed_at"] is None
    assert payload["valid_until"] is None
    assert payload["freshness"] == "not_observed"


def test_database_metadata_validity_overflow_is_local_data_unavailable(
    query_database,
    snapshot_root,
):
    connection = sqlite3.connect(query_database)
    connection.execute(
        "UPDATE signer_metadata SET state_json = ? WHERE singleton = 1",
        (
            json.dumps(
                {
                    "version": 6,
                    "latest_room_listing_observed_at": "9999-12-31T23:59:59Z",
                }
            ),
        ),
    )
    connection.commit()
    connection.close()
    application = query_service.QueryApplication(
        query_service.ServiceConfig(
            database_path=query_database,
            snapshot_root=snapshot_root,
        )
    )

    response = application.handle("/api/v1/rooms/search?q=zzzz&format=json")
    payload = json.loads(response.body)

    assert response.status == 503
    assert payload["error"] == "local_data_unavailable"


@pytest.mark.parametrize(
    "state_json",
    (
        pytest.param(f'{{"version":{HUGE_JSON_INTEGER}}}', id="integer-limit"),
        pytest.param(DEEPLY_NESTED_JSON, id="recursion-limit"),
    ),
)
def test_search_treats_unparseable_optional_metadata_as_not_observed(
    query_database,
    snapshot_root,
    state_json,
):
    connection = sqlite3.connect(query_database)
    connection.execute(
        "UPDATE signer_metadata SET state_json = ? WHERE singleton = 1",
        (state_json,),
    )
    connection.commit()
    connection.close()
    application = query_service.QueryApplication(
        query_service.ServiceConfig(
            database_path=query_database,
            snapshot_root=snapshot_root,
            clock=lambda: datetime(2026, 8, 30, 9, 16, tzinfo=timezone.utc),
        )
    )

    response = application.handle("/api/v1/rooms/search?q=zzzz&format=json")
    payload = json.loads(response.body)

    assert response.status == 200
    assert payload["source_observed_at"] is None
    assert payload["index_observed_at"] is None
    assert payload["freshness"] == "not_observed"


@pytest.mark.parametrize(
    ("state_json", "decoder_recursion"),
    (
        pytest.param(f'{{"version":{HUGE_JSON_INTEGER}}}', False, id="integer-limit"),
        pytest.param(DEEPLY_NESTED_JSON, True, id="recursion-limit"),
    ),
)
def test_room_evidence_normalizes_unparseable_required_metadata(
    query_database,
    snapshot_root,
    state_json,
    decoder_recursion,
):
    connection = sqlite3.connect(query_database)
    connection.execute(
        "UPDATE signer_metadata SET state_json = ? WHERE singleton = 1",
        (state_json,),
    )
    connection.commit()
    connection.close()
    application = query_service.QueryApplication(
        query_service.ServiceConfig(
            database_path=query_database,
            snapshot_root=snapshot_root,
        )
    )

    decoder = (
        mock.patch.object(json, "loads", side_effect=RecursionError)
        if decoder_recursion
        else nullcontext()
    )
    with decoder:
        response = application.handle(
            f"/api/v1/rooms/{room_id('evidence-room')}?format=json"
        )
    payload = json.loads(response.body)

    assert response.status == 503
    assert payload["error"] == "local_data_unavailable"


@pytest.mark.parametrize(
    "rooms_json",
    (
        pytest.param(f"[{HUGE_JSON_INTEGER}]", id="integer-limit"),
        pytest.param(DEEPLY_NESTED_JSON, id="recursion-limit"),
    ),
)
def test_trace_treats_unparseable_optional_rooms_as_not_recorded(
    query_database,
    snapshot_root,
    rooms_json,
):
    connection = sqlite3.connect(query_database)
    connection.execute(
        "UPDATE signer_dids SET rooms_json = ? WHERE did = ?",
        (rooms_json, DID.removeprefix("did:key:z6Mk")),
    )
    connection.commit()
    connection.close()
    application = query_service.QueryApplication(
        query_service.ServiceConfig(
            database_path=query_database,
            snapshot_root=snapshot_root,
        )
    )

    response = application.handle(
        "/api/v1/dids/" + urllib.parse.quote(DID, safe="") + "?format=json"
    )
    payload = json.loads(response.body)

    assert response.status == 200
    assert payload["did"]["retained_rooms"] == {
        "state": "not_recorded",
        "ids": [],
        "stored_count": None,
        "reported_count": None,
        "count_relation": "not_recorded",
        "truncated": None,
    }


def test_trace_rejects_non_finite_stored_room_count_as_local_data_unavailable(
    query_database,
    snapshot_root,
):
    connection = sqlite3.connect(query_database)
    connection.execute(
        "UPDATE signer_dids SET room_count = CAST('1e999' AS REAL) WHERE did = ?",
        (DID.removeprefix("did:key:z6Mk"),),
    )
    connection.commit()
    assert (
        connection.execute(
            "SELECT typeof(room_count) FROM signer_dids WHERE did = ?",
            (DID.removeprefix("did:key:z6Mk"),),
        ).fetchone()[0]
        == "real"
    )
    connection.close()
    application = query_service.QueryApplication(
        query_service.ServiceConfig(
            database_path=query_database,
            snapshot_root=snapshot_root,
        )
    )

    response = application.handle(
        "/api/v1/dids/" + urllib.parse.quote(DID, safe="") + "?format=json"
    )
    payload = json.loads(response.body)

    assert response.status == 503
    assert payload["error"] == "local_data_unavailable"


def test_search_uses_the_newest_returned_evidence_observation(running_server):
    status, _, payload = json_request(
        running_server,
        "/api/v1/rooms/search?q=evidence-room&format=json",
    )

    assert status == 200
    assert payload["results"][0]["last_check_at"] == "2026-08-30T09:00:04Z"
    assert payload["source_observed_at"] == "2026-08-30T09:00:04Z"
    assert payload["index_observed_at"] == "2026-08-30T09:00:00Z"
    assert payload["valid_until"] == "2026-08-30T09:15:04Z"


def test_common_metadata_uses_the_frozen_freshness_vocabulary():
    for freshness in ("fresh", "stale", "not_observed", "not_applicable"):
        payload = common_metadata(
            source_observed_at=None,
            valid_until=None,
            freshness=freshness,
            collector_version=None,
            methodology_version=None,
            schema_version=None,
            window=None,
            coverage=None,
            limitations=[],
        )
        assert payload["freshness"] == freshness
        assert "ledger_chain_head" not in payload
    payload = common_metadata(
        source_observed_at="2026-08-30T09:00:00Z",
        valid_until="2026-08-30T09:15:00Z",
        freshness="fresh",
        collector_version=None,
        methodology_version=None,
        schema_version=None,
        window=None,
        coverage=None,
        limitations=[],
        ledger_chain_head="a" * 64,
    )
    assert payload["ledger_chain_head"] == "a" * 64
    with pytest.raises(ValueError, match="unsupported freshness state"):
        common_metadata(
            source_observed_at=None,
            valid_until=None,
            freshness="current",
            collector_version=None,
            methodology_version=None,
            schema_version=None,
            window=None,
            coverage=None,
            limitations=[],
        )


@pytest.mark.parametrize(
    "method",
    ("POST", "PUT", "PATCH", "DELETE", "OPTIONS", "TRACE", "CONNECT", "PROPFIND"),
)
def test_head_and_disallowed_methods_have_exact_method_semantics(
    running_server, method
):
    target = "/api/v1/rooms/search?q=needle&limit=1&format=json"
    get_status, get_headers, get_body = request(running_server, "GET", target)
    head_status, head_headers, head_body = request(running_server, "HEAD", target)
    assert head_status == get_status == 200
    assert head_body == b""
    assert head_headers["Content-Length"] == str(len(get_body))
    assert head_headers["Content-Type"] == get_headers["Content-Type"]

    status, headers, body = request(running_server, method, target)
    assert status == 405
    assert headers["Allow"] == "GET, HEAD"
    assert headers["Access-Control-Allow-Origin"] == "*"
    assert "Python" not in headers["Server"]
    assert "BaseHTTP" not in headers["Server"]
    assert body


def test_idle_connections_are_bounded_by_a_socket_timeout():
    assert query_service.ObservatoryRequestHandler.timeout == 10


def test_query_concurrency_limit_rejects_excess_work_and_releases_slot(
    query_database,
    snapshot_root,
    monkeypatch,
):
    application = query_service.QueryApplication(
        query_service.ServiceConfig(
            database_path=query_database,
            snapshot_root=snapshot_root,
            clock=lambda: datetime(2026, 8, 30, 9, 1, tzinfo=timezone.utc),
        ),
        max_concurrent_requests=1,
    )
    entered = threading.Event()
    release = threading.Event()
    original = application.room_search_payload

    def blocked_search(query, limit):
        entered.set()
        assert release.wait(timeout=5)
        return original(query, limit)

    monkeypatch.setattr(application, "room_search_payload", blocked_search)
    responses = []
    worker = threading.Thread(
        target=lambda: responses.append(
            application.handle("/api/v1/rooms/search?q=needle&limit=1&format=json")
        )
    )
    worker.start()
    assert entered.wait(timeout=5)

    busy = application.handle("/api/v1/rooms/search?q=needle&limit=1&format=json")
    assert busy.status == 503
    assert json.loads(busy.body)["error"] == "query_capacity_exhausted"

    release.set()
    worker.join(timeout=5)
    assert not worker.is_alive()
    assert responses[0].status == 200

    recovered = application.handle("/api/v1/rooms/search?q=needle&limit=1&format=json")
    assert recovered.status == 200


def test_room_evidence_includes_every_check_listing_presence_and_state_vocabulary(
    running_server,
):
    identifier = room_id("evidence-room")
    status, _, payload = json_request(
        running_server, f"/api/v1/rooms/{identifier}?format=json"
    )
    assert status == 200
    assert payload["room"]["name"] == "evidence-room"
    assert payload["room"]["name_trust"] == "untrusted"
    assert payload["source_observed_at"] == "2026-08-30T09:00:04Z"
    assert payload["room"]["creation"]["created_seq"] == 200
    assert [check["state"] for check in payload["room"]["scheduled_checks"]] == [
        "present_at_last_check",
        "absent_at_last_check",
        "not_yet_checked",
    ]
    assert payload["room"]["latest_lifecycle_state"] == "absent_at_last_check"
    assert payload["room"]["latest_local_listing_presence"] == {
        "state": "present",
        "listing_observed_at": "2026-08-30T09:00:00Z",
        "last_listed_at": "2026-08-30T09:00:00Z",
    }

    status, _, body = request(
        running_server,
        "GET",
        f"/api/v1/rooms/{identifier}",
    )
    assert status == 200
    assert b"name_trust: untrusted" in body

    assert set(payload["state_vocabulary"]) == {
        "unknown",
        "not_yet_checked",
        "present_at_last_check",
        "absent_at_last_check",
        "check_failed",
        "superseded_before_check",
        "deferred",
        "aged_out_unselected",
    }
    assert (
        query_service.state_from_outcome(
            "superseded_before_check",
            0,
            "2026-08-30T09:00:00Z",
            now=datetime(2026, 8, 30, 9, 1, tzinfo=timezone.utc),
        )
        == "superseded_before_check"
    )

    failed_id = room_id("legacy-failed-room")
    status, _, failed = json_request(
        running_server, f"/api/v1/rooms/{failed_id}?format=json"
    )
    assert status == 200
    assert failed["room"]["latest_lifecycle_state"] == "check_failed"

    unchecked_id = room_id("no-check-room")
    status, _, unchecked = json_request(
        running_server, f"/api/v1/rooms/{unchecked_id}?format=json"
    )
    assert status == 200
    assert unchecked["room"]["latest_lifecycle_state"] == "unknown"


def test_short_hash_collision_fails_closed_without_names(running_server):
    status, _, body = request(
        running_server,
        "GET",
        "/api/v1/rooms/deadbeefdeadbeef?format=json",
    )
    assert status == 409
    payload = json.loads(body)
    assert payload["error"] == "ambiguous_room_id"
    assert b"collision-one" not in body
    assert b"collision-two" not in body


def test_room_id_selects_the_newest_generation_and_only_its_revisits(
    running_server,
):
    status, _, payload = json_request(
        running_server,
        f"/api/v1/rooms/{room_id('generation-room')}?format=json",
    )

    assert status == 200
    assert payload["room"]["creation"]["created_seq"] == 207
    assert payload["room"]["scheduled_checks"] == [
        {
            "stage_seconds": 300,
            "due_at": "2026-08-30T08:05:00Z",
            "attempted_at": "2026-08-30T08:05:02Z",
            "state": "present_at_last_check",
            "message_count": 2,
            "has_second_message": True,
            "second_sender_class": "signed_did",
        }
    ]
    assert payload["room"]["latest_lifecycle_state"] == "present_at_last_check"


def test_progressive_room_html_escapes_names_and_uses_generic_previews(running_server):
    shared_shell = (
        '<link rel="stylesheet" href="/assets/styles.css">',
        '<script src="/assets/site.js" defer></script>',
        '<a class="skip-link" href="#main-content">',
        '<header class="site-header">',
        "TECHNOCORE OBSERVATORY",
        '<details class="site-index">',
        "<summary>INDEX</summary>",
        '<nav aria-label="Site index">',
        '<button class="theme-control" id="theme-toggle" type="button" data-theme-value="system" hidden>THEME AUTO</button>',
        '<main id="main-content" class="page-shell" tabindex="-1">',
        '<footer class="site-footer">',
    )

    status, headers, body = request(running_server, "GET", "/rooms/?q=forged")
    source = body.decode("utf-8")
    assert status == 200
    assert headers["X-Robots-Tag"] == "noindex, nofollow, noarchive".replace(
        ",nofollow", ", nofollow"
    )
    for marker in shared_shell:
        assert marker in source
    assert 'dataset.themeStorage="unavailable"' in source
    assert '<nav class="priority-nav" aria-label="Primary">' in source
    form = source.partition('<form class="room-search"')[2].partition("</form>")[0]
    assert form
    assert 'action="/rooms/"' in form
    assert 'method="get"' in form
    assert 'id="room-query"' in form
    assert 'name="q"' in form
    assert 'id="search-feedback"' in form
    assert 'aria-live="polite"' in form
    assert "maximum 20 results" in form.lower()
    assert "Untrusted room name" in source
    assert '<article class="result-record">' in source
    for label in ("OBSERVED", "WINDOW", "COVERAGE", "METHOD"):
        assert f"<dt>{label}</dt>" in source
    assert "Not observed is not absent" in source
    assert "<script>alert(1)</script>" not in source
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in source
    assert '<meta property="og:title" content="Technocore room search">' in source
    assert "forged<script>" not in source.split("<head>", 1)[1].split("</head>", 1)[0]

    identifier = room_id("evidence-room")
    status, _, body = request(running_server, "GET", f"/rooms/{identifier}/")
    source = body.decode("utf-8")
    assert status == 200
    for marker in shared_shell:
        assert marker in source
    assert '<nav class="priority-nav" aria-label="Primary">' in source
    assert "Evidence rail" in source
    assert "Untrusted room name" in source
    assert '<meta property="og:title" content="Technocore room evidence">' in source
    assert "evidence-room" not in source.split("<head>", 1)[1].split("</head>", 1)[0]


def test_query_shell_classes_are_grid_children_with_stylesheet_rules(
    running_server,
):
    styles = Path("site/assets/styles.css").read_text(encoding="utf-8")
    emitted = set()
    for target in (
        "/rooms/",
        "/rooms/?q=forged",
        f"/rooms/{room_id('evidence-room')}/",
        "/keys/" + urllib.parse.quote(DID, safe="") + "/",
    ):
        status, _, body = request(running_server, "GET", target)
        assert status == 200
        source = body.decode("utf-8")
        assert "site-header-inner" not in source
        head = source.split("</head>", 1)[0]
        assert head.index('localStorage.getItem("observatory-theme")') < head.index(
            '<link rel="stylesheet"'
        )
        for attribute in re.findall(r'class="([^"]*)"', source):
            emitted.update(attribute.split())

    assert "site-header" in emitted
    unstyled = sorted(
        name
        for name in emitted
        if re.search(rf"\.{re.escape(name)}(?![-\w])", styles) is None
    )
    assert unstyled == []


def test_trace_returns_only_direct_bounded_facts_and_hashes_rooms(running_server):
    target = "/api/v1/dids/" + urllib.parse.quote(DID, safe="") + "?format=json"
    status, _, payload = json_request(running_server, target)
    assert status == 200
    trace = payload["did"]
    assert trace["id"] == DID
    assert trace["first_observed_at"] == {
        "state": "recorded",
        "value": "2026-08-28T08:00:00Z",
    }
    assert trace["covered_ticks"] == {"state": "recorded", "value": 19}
    assert trace["covered_collection_dates"]["count"] == 3
    assert trace["retained_rooms"]["count_relation"] == "at_least"
    assert trace["retained_rooms"]["truncated"] is True
    assert len(trace["retained_rooms"]["ids"]) == 8
    assert all(len(identifier) == 16 for identifier in trace["retained_rooms"]["ids"])
    encoded = json.dumps(payload)
    assert "retained-0" not in encoded
    assert trace["signed_reciprocal_alternation"] == {
        "state": "recorded",
        "observed": True,
    }
    for prohibited in ("liveness", "quality", "verification"):
        assert prohibited not in encoded.lower()
    limitations = " ".join(payload["limitations"]).lower()
    for required in (
        "enumerated tick and date histories",
        "sampling-opportunity denominators",
        "counterparties",
        "per-fact historical collector versions",
    ):
        assert required in limitations


def test_trace_distinguishes_unknown_did_from_legacy_not_recorded(running_server):
    legacy_target = (
        "/api/v1/dids/" + urllib.parse.quote(LEGACY_DID, safe="") + "?format=json"
    )
    status, _, payload = json_request(running_server, legacy_target)
    assert status == 200
    assert payload["did"]["first_observed_at"] == {
        "state": "not_recorded",
        "value": None,
    }
    assert payload["did"]["covered_ticks"] == {
        "state": "not_recorded",
        "value": None,
    }
    assert payload["did"]["signed_reciprocal_alternation"] == {
        "state": "not_recorded",
        "observed": None,
    }
    assert payload["source_observed_at"] is None
    assert payload["valid_until"] is None
    assert payload["freshness"] == "not_observed"

    unknown = "did:key:z6Mk" + "c" * 40
    status, _, payload = json_request(
        running_server,
        "/api/v1/dids/" + urllib.parse.quote(unknown, safe="") + "?format=json",
    )
    assert status == 404
    assert payload["error"] == "did_unknown"


def test_trace_false_alternation_remains_a_recorded_observation(running_server):
    target = "/api/v1/dids/" + urllib.parse.quote(FALSE_DID, safe="") + "?format=json"
    status, _, payload = json_request(running_server, target)
    assert status == 200
    assert payload["did"]["signed_reciprocal_alternation"] == {
        "state": "recorded",
        "observed": False,
    }
    assert any(
        "not evidence of absence" in limitation for limitation in payload["limitations"]
    )


def test_trace_is_exact_only_and_html_page_uses_generic_metadata(running_server):
    for target in ("/api/v1/dids", "/api/v1/dids/", "/keys/"):
        status, _, _ = request(running_server, "GET", target)
        assert status == 404

    target = "/keys/" + urllib.parse.quote(DID, safe="") + "/"
    status, headers, body = request(running_server, "GET", target)
    source = body.decode("utf-8")
    assert status == 200
    assert headers["X-Robots-Tag"] == "noindex, nofollow, noarchive"
    assert '<meta property="og:title" content="Technocore key observation">' in source
    assert DID not in source.split("<head>", 1)[1].split("</head>", 1)[0]
    assert '<link rel="stylesheet" href="/assets/styles.css">' in source
    assert '<script src="/assets/site.js" defer></script>' in source
    assert '<header class="site-header">' in source
    assert '<details class="site-index">' in source
    assert '<nav aria-label="Site index">' in source
    assert (
        '<button class="theme-control" id="theme-toggle" type="button" data-theme-value="system" hidden>THEME AUTO</button>'
        in source
    )
    assert '<nav class="priority-nav" aria-label="Primary">' in source
    assert '<main id="main-content" class="page-shell" tabindex="-1">' in source
    for prohibited in ("liveness", "quality", "verification"):
        assert prohibited not in source.lower()


def test_static_snapshots_pass_through_and_lists_filter_in_process(
    running_server, snapshot_root, query_database
):
    def without_generation(text):
        return [
            line
            for line in text.decode("utf-8").splitlines()
            if not line.startswith("generated_at: ")
        ]

    published = (snapshot_root / "api" / "v1" / "status.txt").read_bytes()
    status, headers, body = request(running_server, "GET", "/api/v1/status")
    assert status == 200
    assert without_generation(body) == without_generation(published)
    assert b"generated_at: 2026-08-30T09:01:00Z" in body
    assert headers["Cache-Control"].startswith("public")

    status, _, payload = json_request(running_server, "/api/v1/status?format=json")
    assert status == 200
    assert payload["status"] == {"origin": "reachable"}

    status, _, payload = json_request(
        running_server,
        "/api/v1/incidents?since=2026-08-30T00%3A00%3A00Z&limit=1&format=json",
    )
    assert status == 200
    assert [incident["id"] for incident in payload["incidents"]] == ["new"]

    status, _, body = request(
        running_server,
        "GET",
        "/api/v1/changes?since=2026-08-30T00%3A00%3A00Z&limit=1",
    )
    assert status == 200
    assert b"changes.1:" in body
    assert b'"id":"new"' in body

    status, _, body = request(running_server, "GET", "/api/v1/changes")
    assert status == 200
    assert b"changes.1:" in body
    assert b'"id":"old"' in body

    application = query_service.QueryApplication(
        query_service.ServiceConfig(
            database_path=query_database,
            snapshot_root=snapshot_root,
            clock=lambda: datetime(2026, 8, 30, 9, 16, tzinfo=timezone.utc),
        )
    )

    payload = json.loads(application.snapshot("status", {"format": "json"}).body)
    assert payload["generated_at"] == "2026-08-30T09:16:00Z"
    assert payload["valid_until"] == "2026-08-30T09:15:00Z"
    assert payload["freshness"] == "stale"

    methodology = json.loads(
        application.snapshot("methodology", {"format": "json"}).body
    )
    assert methodology["freshness"] == "not_applicable"


@pytest.mark.parametrize(
    ("moment", "generated_at", "freshness"),
    (
        (
            datetime(2026, 8, 30, 9, 15, 0, 500_000, tzinfo=timezone.utc),
            "2026-08-30T09:15:00Z",
            "fresh",
        ),
        (
            datetime(2026, 8, 30, 9, 15, 1, 500_000, tzinfo=timezone.utc),
            "2026-08-30T09:15:01Z",
            "stale",
        ),
        (
            datetime(2026, 8, 30, 9, 16, tzinfo=timezone.utc),
            "2026-08-30T09:16:00Z",
            "stale",
        ),
    ),
)
def test_filtered_snapshot_uses_request_time_without_moving_validity(
    query_database,
    snapshot_root,
    moment,
    generated_at,
    freshness,
):
    application = query_service.QueryApplication(
        query_service.ServiceConfig(
            database_path=query_database,
            snapshot_root=snapshot_root,
            clock=lambda: moment,
        )
    )

    response = application.snapshot(
        "incidents",
        {
            "since": "2026-08-30T00:00:00Z",
            "limit": "1",
            "format": "json",
        },
    )
    payload = json.loads(response.body)

    assert response.status == 200
    assert payload["generated_at"] == generated_at
    assert payload["published_at"] == "2026-08-30T09:01:00Z"
    assert payload["source_observed_at"] == "2026-08-30T09:00:00Z"
    assert payload["valid_until"] == "2026-08-30T09:15:00Z"
    assert payload["freshness"] == freshness


def test_filtered_snapshot_rejects_clock_rollback_before_stored_publication(
    query_database,
    snapshot_root,
):
    application = query_service.QueryApplication(
        query_service.ServiceConfig(
            database_path=query_database,
            snapshot_root=snapshot_root,
            clock=lambda: datetime(2026, 8, 30, 9, 0, 45, tzinfo=timezone.utc),
        )
    )

    response = application.handle("/api/v1/incidents?limit=1&format=json")
    payload = json.loads(response.body)

    assert response.status == 503
    assert payload["error"] == "snapshot_invalid"


def test_static_snapshot_filter_validation_and_missing_fallback(running_server):
    status, _, payload = json_request(
        running_server, "/api/v1/incidents?limit=101&format=json"
    )
    assert status == 400
    assert payload["error"] == "invalid_limit"
    status, _, payload = json_request(
        running_server, "/api/v1/incidents?since=yesterday&format=json"
    )
    assert status == 400
    assert payload["error"] == "invalid_since"
    status, _, payload = json_request(
        running_server, "/api/v1/status?since=2026-08-30T00%3A00%3A00Z&format=json"
    )
    assert status == 400
    assert payload["error"] == "unexpected_parameter"
    status, _, _ = request(running_server, "GET", "/api/v1/not-a-snapshot")
    assert status == 404


@pytest.mark.parametrize(
    "since",
    (
        pytest.param("0001-01-01T00:00:00+14:00", id="utc-underflow"),
        pytest.param("9999-12-31T23:59:59-23:59", id="utc-overflow"),
    ),
)
def test_snapshot_since_normalization_overflow_is_a_client_error(
    running_server,
    since,
):
    encoded = urllib.parse.quote(since, safe="")
    status, _, payload = json_request(
        running_server,
        f"/api/v1/incidents?since={encoded}&format=json",
    )

    assert status == 400
    assert payload["error"] == "invalid_since"


def test_known_missing_and_corrupt_snapshots_are_server_failures(
    running_server,
    snapshot_root,
):
    status_path = snapshot_root / "api" / "v1" / "status.json"
    status_path.unlink()
    status, _, payload = json_request(running_server, "/api/v1/status?format=json")
    assert status == 503
    assert payload["error"] == "snapshot_unavailable"

    status_path.write_text("{", encoding="utf-8")
    status, _, payload = json_request(running_server, "/api/v1/status?format=json")
    assert status == 503
    assert payload["error"] == "snapshot_invalid"


@pytest.mark.parametrize(
    "body",
    (
        pytest.param(f'{{"status":{HUGE_JSON_INTEGER}}}', id="integer-limit"),
        pytest.param(
            f'{{"status":{DEEPLY_NESTED_JSON}}}',
            id="recursion-limit",
        ),
    ),
)
def test_snapshot_json_parser_limits_are_snapshot_failures(
    running_server,
    snapshot_root,
    body,
):
    status_path = snapshot_root / "api" / "v1" / "status.json"
    status_path.write_text(body, encoding="utf-8")

    status, _, payload = json_request(
        running_server,
        "/api/v1/status?format=json",
    )

    assert status == 503
    assert payload["error"] == "snapshot_invalid"


def test_snapshot_validity_overflow_is_a_snapshot_failure(
    running_server,
    snapshot_root,
):
    status_path = snapshot_root / "api" / "v1" / "status.json"
    payload = json.loads(status_path.read_text(encoding="utf-8"))
    payload["source_observed_at"] = "9999-12-31T23:59:59Z"
    payload["valid_until"] = "9999-12-31T23:59:59Z"
    status_path.write_text(json.dumps(payload), encoding="utf-8")

    response = running_server.application.handle("/api/v1/status?format=json")
    error = json.loads(response.body)

    assert response.status == 503
    assert error["error"] == "snapshot_invalid"


@pytest.mark.parametrize(
    ("nested_value", "target"),
    (
        pytest.param(float("nan"), "/api/v1/status?format=json", id="json-nan"),
        pytest.param("\ud800", "/api/v1/status?format=json", id="json-surrogate"),
        pytest.param(float("nan"), "/api/v1/status", id="text-nan"),
        pytest.param("\ud800", "/api/v1/status", id="text-surrogate"),
    ),
)
def test_snapshot_non_serializable_values_are_snapshot_failures(
    running_server,
    snapshot_root,
    nested_value,
    target,
):
    status_path = snapshot_root / "api" / "v1" / "status.json"
    payload = json.loads(status_path.read_text(encoding="utf-8"))
    payload["status"]["nested"] = nested_value
    status_path.write_text(json.dumps(payload), encoding="utf-8")

    response = running_server.application.handle(target)

    assert response.status == 503
    if target.endswith("format=json"):
        assert json.loads(response.body)["error"] == "snapshot_invalid"
    else:
        assert b"error: snapshot_invalid\n" in response.body


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("contract_version", "0.9.0"),
        ("freshness", "current"),
        ("coverage", []),
        ("limitations", "forward observations only"),
    ),
)
def test_semantically_invalid_snapshot_envelopes_are_server_failures(
    running_server,
    snapshot_root,
    field,
    value,
):
    status_path = snapshot_root / "api" / "v1" / "status.json"
    payload = json.loads(status_path.read_text(encoding="utf-8"))
    payload[field] = value
    status_path.write_text(json.dumps(payload), encoding="utf-8")

    status, _, error = json_request(
        running_server,
        "/api/v1/status?format=json",
    )

    assert status == 503
    assert error["error"] == "snapshot_invalid"


def test_snapshot_source_observation_cannot_postdate_derivation(
    running_server,
    snapshot_root,
):
    status_path = snapshot_root / "api" / "v1" / "status.json"
    payload = json.loads(status_path.read_text(encoding="utf-8"))
    payload["source_observed_at"] = "2026-08-30T09:00:31Z"
    payload["valid_until"] = "2026-08-30T09:15:31Z"
    status_path.write_text(json.dumps(payload), encoding="utf-8")

    status, _, error = json_request(
        running_server,
        "/api/v1/status?format=json",
    )

    assert status == 503
    assert error["error"] == "snapshot_invalid"


def test_snapshot_resource_shape_and_ledger_applicability_are_validated(
    running_server,
    snapshot_root,
):
    status_path = snapshot_root / "api" / "v1" / "status.json"
    status_payload = json.loads(status_path.read_text(encoding="utf-8"))
    status_payload["status"] = []
    status_path.write_text(json.dumps(status_payload), encoding="utf-8")
    status, _, error = json_request(
        running_server,
        "/api/v1/status?format=json",
    )
    assert status == 503
    assert error["error"] == "snapshot_invalid"

    methodology_path = snapshot_root / "api" / "v1" / "methodology.json"
    methodology = json.loads(methodology_path.read_text(encoding="utf-8"))
    methodology["ledger_chain_head"] = "a" * 64
    methodology_path.write_text(json.dumps(methodology), encoding="utf-8")
    status, _, error = json_request(
        running_server,
        "/api/v1/methodology?format=json",
    )
    assert status == 503
    assert error["error"] == "snapshot_invalid"

    incidents_path = snapshot_root / "api" / "v1" / "incidents.json"
    incidents = json.loads(incidents_path.read_text(encoding="utf-8"))
    incidents["incidents"][0]["opened_at"] = "not-a-timestamp"
    incidents_path.write_text(json.dumps(incidents), encoding="utf-8")
    status, _, payload = json_request(
        running_server,
        "/api/v1/incidents?since=2026-08-01T00%3A00%3A00Z&format=json",
    )
    assert status == 503
    assert payload["error"] == "snapshot_invalid"


def test_database_connection_is_read_only_query_only_and_requires_v6(
    query_database, tmp_path
):
    connection = query_service.open_readonly_database(query_database)
    try:
        assert connection.execute("PRAGMA query_only").fetchone()[0] == 1
        with pytest.raises(sqlite3.OperationalError):
            connection.execute(
                "INSERT INTO room_ledger (name) VALUES (?)", ("must-fail",)
            )
    finally:
        connection.close()

    old = tmp_path / "old.sqlite3"
    connection = sqlite3.connect(old)
    connection.execute("PRAGMA user_version = 5")
    connection.close()
    with pytest.raises(query_service.SchemaError, match="schema version 6"):
        query_service.open_readonly_database(old)


def test_query_service_rejects_legacy_case_insensitive_v6_index(query_database):
    connection = sqlite3.connect(query_database)
    connection.executescript(
        """
        DROP TRIGGER room_ledger_ai;
        DROP TRIGGER room_ledger_ad;
        DROP TRIGGER room_ledger_au;
        DROP TABLE room_search;
        CREATE VIRTUAL TABLE room_search USING fts5(
            name,
            content='room_ledger',
            content_rowid='rowid',
            tokenize='trigram'
        );
        INSERT INTO room_search(room_search) VALUES ('rebuild');
        """
    )
    connection.close()

    with pytest.raises(query_service.SchemaError, match="case-sensitive trigram"):
        query_service.open_readonly_database(query_database)


def test_query_service_rejects_old_v6_name_keyed_room_generations(tmp_path):
    old = tmp_path / "old-v6.sqlite3"
    connection = sqlite3.connect(old)
    connection.executescript(
        """
        CREATE TABLE room_ledger (
            name TEXT PRIMARY KEY,
            room_id TEXT NOT NULL,
            room_sha256 TEXT NOT NULL,
            created_seq INTEGER NOT NULL UNIQUE,
            created_at TEXT NOT NULL,
            first_observed_at TEXT NOT NULL,
            last_listed_at TEXT
        );
        CREATE TABLE room_revisits (
            room_name TEXT NOT NULL,
            stage_seconds INTEGER NOT NULL,
            due_at TEXT NOT NULL,
            attempted_at TEXT,
            success INTEGER,
            message_count INTEGER,
            has_second_message INTEGER,
            second_sender_class TEXT,
            outcome TEXT,
            PRIMARY KEY (room_name, stage_seconds)
        );
        CREATE VIRTUAL TABLE room_search USING fts5(
            name,
            content='room_ledger',
            content_rowid='rowid',
            tokenize='trigram case_sensitive 1'
        );
        PRAGMA user_version = 6;
        """
    )
    connection.close()

    with pytest.raises(query_service.SchemaError, match="generation-aware"):
        query_service.open_readonly_database(old)


def test_query_progress_deadline_interrupts_unbounded_local_work(query_database):
    connection = query_service.open_readonly_database(
        query_database, query_timeout_seconds=0.001
    )
    try:
        with pytest.raises(sqlite3.OperationalError, match="interrupted"):
            connection.execute(
                """
                WITH RECURSIVE counter(value) AS (
                    VALUES (0)
                    UNION ALL
                    SELECT value + 1 FROM counter WHERE value < 100000000
                )
                SELECT SUM(value) FROM counter
                """
            ).fetchone()
    finally:
        connection.close()


def test_small_corpus_fts_plan_never_scans_room_ledger(query_database):
    connection = query_service.open_readonly_database(query_database)
    try:
        plan = benchmark_search.assert_query_plan(connection, "needle")
        room_plan = benchmark_search.assert_room_id_query_plan(
            connection,
            room_id("evidence-room"),
        )
    finally:
        connection.close()
    assert plan
    assert all("SCAN room_ledger" not in detail for detail in plan)
    assert room_plan
    assert all("SCAN room_ledger" not in detail for detail in room_plan)

    connection = sqlite3.connect(query_database)
    try:
        schema = connection.execute(
            "SELECT sql FROM sqlite_master WHERE name = 'room_search'"
        ).fetchone()[0]
    finally:
        connection.close()
    assert "case_sensitive 1" in schema


def test_benchmark_uses_one_warmup_and_measures_a_representative_mix(
    monkeypatch,
):
    calls = []
    plans = []

    class FakeConnection:
        def close(self):
            return None

    monkeypatch.setattr(benchmark_search, "build_corpus", lambda path: 0.001)
    monkeypatch.setattr(
        benchmark_search,
        "QueryApplication",
        lambda config: object(),
    )
    monkeypatch.setattr(
        benchmark_search,
        "open_readonly_database",
        lambda *args, **kwargs: FakeConnection(),
    )

    def timed_search(application, query, **kwargs):
        calls.append(query)
        return 0.001

    def substring_plan(connection, query):
        plans.append("substring")
        return ["SEARCH room_search"]

    def room_id_plan(connection, identifier):
        plans.append("room_id")
        return ["SEARCH room_ledger USING INDEX room_ledger_room_id"]

    monkeypatch.setattr(benchmark_search, "timed_search", timed_search)
    monkeypatch.setattr(benchmark_search, "assert_query_plan", substring_plan)
    monkeypatch.setattr(
        benchmark_search,
        "assert_room_id_query_plan",
        room_id_plan,
        raising=False,
    )

    result = benchmark_search.run_benchmark(20)

    assert plans == ["substring", "room_id"]
    assert calls[:3] == ["r00", "R00", "r00"]
    assert calls[3:] == [
        "r00",
        "Needle-001",
        "R00",
        "r00",
        "Needle-004",
        "R00",
        "r00",
        "Needle-007",
        "R00",
        "r00",
        "Needle-010",
        "R00",
        "r00",
        "Needle-013",
        "R00",
        "r00",
        "Needle-016",
        "R00",
        "r00",
        "Needle-019",
    ]
    assert result["iterations"] == 20


def test_query_path_has_no_outbound_client_import_and_origin_can_be_down(
    running_server, monkeypatch
):
    source = Path(query_service.__file__).read_text(encoding="utf-8")
    imports = {
        alias.name
        for node in ast.walk(ast.parse(source))
        if isinstance(node, (ast.Import, ast.ImportFrom))
        for alias in node.names
    }
    assert "urllib.request" not in imports
    assert "http.client" not in imports
    assert "requests" not in imports

    client = http.client.HTTPConnection(
        "127.0.0.1", running_server.server_address[1], timeout=5
    )
    client.connect()

    def reject_outbound(*args, **kwargs):
        raise AssertionError("query service attempted an outbound connection")

    monkeypatch.setattr(socket, "create_connection", reject_outbound)
    try:
        status, _, body = request(
            running_server,
            "GET",
            "/api/v1/rooms/search?q=needle&limit=1&format=json",
            connection=client,
        )
    finally:
        client.close()
    payload = json.loads(body)
    assert status == 200
    assert payload["results"]


def test_local_data_failure_preserves_requested_json_representation(
    running_server, monkeypatch
):
    def unavailable_database():
        raise query_service.SchemaError("database unavailable")

    monkeypatch.setattr(running_server.application, "database", unavailable_database)
    status, headers, payload = json_request(
        running_server, "/api/v1/rooms/search?q=needle&format=json"
    )
    assert status == 503
    assert headers["Content-Type"].startswith("application/json")
    assert payload["error"] == "local_data_unavailable"
    assert payload["freshness"] == "not_observed"


def test_raw_queries_are_not_logged(running_server, capsys):
    secret_query = "forged-secret-query"
    status, _, _ = request(
        running_server,
        "GET",
        "/api/v1/rooms/search?q=" + secret_query,
    )
    assert status == 200
    captured = capsys.readouterr()
    assert secret_query not in captured.out
    assert secret_query not in captured.err


def test_plain_text_escapes_unicode_line_and_format_controls():
    body = text_bytes(
        {
            "contract_version": "1.0.0",
            "value": "safe\u2028contract_version: forged\u2029end\u202espoof",
        }
    ).decode("utf-8")

    assert body.splitlines() == [
        "contract_version: 1.0.0",
        "value: safe\\u2028contract_version: forged\\u2029end\\u202espoof",
    ]


def test_search_reports_lifecycle_state_for_an_unattempted_room(running_server):
    status, _, payload = json_request(
        running_server,
        "/api/v1/rooms/search?q=evidence-room&format=json",
    )
    assert status == 200
    result = next(
        entry for entry in payload["results"] if entry["name"] == "evidence-room"
    )
    assert result["latest_lifecycle_state"] in query_service.STATE_VOCABULARY


def test_state_from_outcome_separates_deferred_from_aged_out():
    now = datetime(2026, 8, 30, 9, 1, tzinfo=timezone.utc)

    def state(windows):
        return query_service.state_from_outcome(
            None, None, None, pending_windows=windows, now=now
        )

    # The window is open and the read budget has not reached this room yet.
    assert state([("2026-08-30T09:00:00Z", 3600)]) == "deferred"
    # Every window closed with no attempt: never checked, and never will be.
    assert state([("2026-08-30T08:00:00Z", 300)]) == "aged_out_unselected"
    # A checkpoint still ahead of its due time is genuinely pending.
    assert state([("2026-08-31T08:00:00Z", 86400)]) == "not_yet_checked"
    # One closed stage does not age out a room that still has a later stage due.
    assert (
        state([("2026-08-30T08:00:00Z", 300), ("2026-08-31T08:00:00Z", 86400)])
        == "not_yet_checked"
    )
    # An open window outranks a closed one.
    assert (
        state([("2026-08-30T08:00:00Z", 300), ("2026-08-30T09:00:00Z", 3600)])
        == "deferred"
    )


def test_state_from_outcome_never_guesses_without_windows():
    now = datetime(2026, 8, 30, 9, 1, tzinfo=timezone.utc)
    # Without windows we know a check is scheduled but not whether it can still
    # happen, so the older, weaker claim stands rather than a fabricated one.
    assert (
        query_service.state_from_outcome(None, None, None, now=now) == "not_yet_checked"
    )
    assert (
        query_service.state_from_outcome(
            None, None, None, has_scheduled_checks=False, now=now
        )
        == "unknown"
    )
    assert (
        query_service.state_from_outcome(
            None, None, None, pending_windows=[("not-a-timestamp", 300)], now=now
        )
        == "unknown"
    )


@pytest.mark.parametrize(
    ("moment", "expected"),
    [
        # The 86400 stage falls due at 2026-08-31T08:00 and has no attempt.
        (datetime(2026, 8, 30, 9, 1, tzinfo=timezone.utc), "not_yet_checked"),
        (datetime(2026, 8, 31, 8, 30, tzinfo=timezone.utc), "deferred"),
        (datetime(2026, 9, 2, 0, 0, tzinfo=timezone.utc), "aged_out_unselected"),
    ],
)
def test_both_endpoints_agree_on_an_unattempted_rooms_state(
    query_database,
    snapshot_root,
    moment,
    expected,
):
    # The evidence page used to hardcode not_yet_checked while search consulted
    # the windows, so one room could report two different states, and the
    # evidence payload could contradict its own scheduled_checks.
    application = query_service.QueryApplication(
        query_service.ServiceConfig(
            database_path=query_database,
            snapshot_root=snapshot_root,
            clock=lambda: moment,
        )
    )

    record = application.room_record(room_id("unchecked-room"))
    assert record["room"]["latest_lifecycle_state"] == expected
    assert [check["state"] for check in record["room"]["scheduled_checks"]] == [
        expected
    ]

    response = application.room_search_api(
        {"q": "unchecked-room", "limit": "20", "format": "json"}
    )
    results = json.loads(response.body)["results"]
    entry = next(r for r in results if r["name"] == "unchecked-room")
    assert entry["latest_lifecycle_state"] == expected


def test_finalized_aged_out_room_still_reports_aged_out_everywhere(
    query_database,
    snapshot_root,
):
    # Terminal finalization stamps aged_out_at while attempted_at stays NULL.
    # The finalized rows must keep contributing their closed windows: the
    # room may never drift to not_yet_checked or unknown, and it must not
    # drop out of has_scheduled_checks.
    application = query_service.QueryApplication(
        query_service.ServiceConfig(
            database_path=query_database,
            snapshot_root=snapshot_root,
            clock=lambda: datetime(2026, 9, 2, 0, 0, tzinfo=timezone.utc),
        )
    )

    record = application.room_record(room_id("finalized-room"))
    assert record["room"]["latest_lifecycle_state"] == "aged_out_unselected"
    assert [check["state"] for check in record["room"]["scheduled_checks"]] == [
        "aged_out_unselected",
        "aged_out_unselected",
        "aged_out_unselected",
    ]

    response = application.room_search_api(
        {"q": "finalized-room", "limit": "20", "format": "json"}
    )
    results = json.loads(response.body)["results"]
    entry = next(r for r in results if r["name"] == "finalized-room")
    assert entry["latest_lifecycle_state"] == "aged_out_unselected"
