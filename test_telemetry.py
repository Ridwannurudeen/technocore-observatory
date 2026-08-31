import io
import hashlib
import json
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.parse
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

import collect
import pulse_probe
from collect import Client, CollectionError
from telemetry import (
    MAX_DATABASE_BYTES,
    RAW_RETENTION_SECONDS,
    TelemetryStore,
    normalize_route,
    parse_timestamp,
)


class Response:
    def __init__(self, body, status=200):
        self.body = io.BytesIO(body)
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def read(self, size=-1):
        return self.body.read(size)


def http_error(url, status, body=b""):
    return urllib.error.HTTPError(url, status, "failed", {}, io.BytesIO(body))


@pytest.fixture
def telemetry_store(tmp_path):
    store = TelemetryStore(tmp_path / "telemetry.sqlite3")
    yield store
    store.close()


def test_telemetry_schema_is_delete_journal_strict_and_rejects_raw_routes(
    telemetry_store,
):
    assert telemetry_store.connection.execute("PRAGMA user_version").fetchone() == (1,)
    assert (
        telemetry_store.connection.execute("PRAGMA journal_mode").fetchone()[0]
        == "delete"
    )
    cycle_id = telemetry_store.start_cycle("collector", "2026-08-30T10:00:00Z")

    with pytest.raises(sqlite3.IntegrityError):
        telemetry_store.connection.execute(
            """
            INSERT INTO request_attempts (
                cycle_id, route, metered, attempt, observed_at,
                latency_ms, outcome, http_status
            )
            VALUES (?, ?, 1, 1, ?, 1, 'success', 200)
            """,
            (cycle_id, "/r/raw-room-name", "2026-08-30T10:00:00Z"),
        )

    assert normalize_route("/r/raw-room-name?format=json") == ("/r/{room}", True)
    assert normalize_route("/kv/did-ff") == ("/kv/{namespace}", True)
    assert normalize_route("/healthz") == ("/healthz", False)


def test_closed_telemetry_store_has_no_wal_sidecar(tmp_path):
    path = tmp_path / "telemetry.sqlite3"
    store = TelemetryStore(path)
    store.close()

    assert not path.with_name(path.name + "-wal").exists()
    assert not path.with_name(path.name + "-shm").exists()


@pytest.mark.parametrize(
    "base_url",
    (
        "ftp://example.com",
        "https://user:password@example.com",
        "https://example.com/origin-path",
        "https://example.com?query=1",
        "https://example.com/#fragment",
    ),
)
def test_client_rejects_non_origin_base_urls(base_url):
    with pytest.raises(ValueError):
        Client(base_url, 1, 0)


def test_client_does_not_follow_cross_origin_redirects():
    reached_destination = threading.Event()

    class DestinationHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            reached_destination.set()
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"private")

        def log_message(self, format, *args):
            return

    destination = ThreadingHTTPServer(("127.0.0.1", 0), DestinationHandler)

    class RedirectHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(302)
            self.send_header(
                "Location",
                f"http://127.0.0.1:{destination.server_address[1]}/internal",
            )
            self.end_headers()

        def log_message(self, format, *args):
            return

    origin = ThreadingHTTPServer(("127.0.0.1", 0), RedirectHandler)
    destination_thread = threading.Thread(target=destination.serve_forever, daemon=True)
    origin_thread = threading.Thread(target=origin.serve_forever, daemon=True)
    destination_thread.start()
    origin_thread.start()
    try:
        client = Client(f"http://127.0.0.1:{origin.server_address[1]}", 1, 0)
        with pytest.raises(CollectionError) as caught:
            client.get("/redirect")
    finally:
        origin.shutdown()
        destination.shutdown()
        origin.server_close()
        destination.server_close()
        origin_thread.join(timeout=5)
        destination_thread.join(timeout=5)

    assert caught.value.outcome == "invalid_response"
    assert caught.value.status == 302
    assert not reached_destination.is_set()


def test_client_rejects_a_response_over_the_byte_limit(monkeypatch):
    monkeypatch.setattr(collect, "MAX_ORIGIN_RESPONSE_BYTES", 4, raising=False)
    monkeypatch.setattr(
        collect,
        "open_origin",
        lambda request, timeout: Response(b"12345"),
    )

    with pytest.raises(CollectionError) as caught:
        Client("https://example.invalid", 1, 0).get("/rooms?format=json")

    assert caught.value.outcome == "invalid_response"


def test_client_enforces_a_wall_clock_response_deadline(monkeypatch):
    class SlowDripResponse:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            return False

        def read(self):
            return b"ok"

        def read1(self, size):
            time.sleep(0.02)
            return b"x"

    monkeypatch.setattr(
        collect,
        "open_origin",
        lambda request, timeout: SlowDripResponse(),
    )

    started = time.monotonic()
    with pytest.raises(CollectionError) as caught:
        Client("https://example.invalid", 0.03, 0).get("/healthz")

    assert caught.value.outcome == "timeout"
    assert time.monotonic() - started < 0.2


@pytest.mark.parametrize("status", [404, 429, 503])
def test_client_preserves_known_http_status_when_error_body_is_slow(
    telemetry_store,
    monkeypatch,
    status,
):
    class SlowErrorBody(io.BytesIO):
        def read1(self, size=-1):
            time.sleep(0.02)
            return super().read(1)

    def fail(request, timeout):
        raise urllib.error.HTTPError(
            request.full_url,
            status,
            "failed",
            {},
            SlowErrorBody(b"slow error body"),
        )

    monkeypatch.setattr(collect, "open_origin", fail)
    cycle_id = telemetry_store.start_cycle("collector", "2026-08-30T10:00:00Z")
    client = Client(
        "https://example.invalid",
        0.03,
        0,
        telemetry=telemetry_store,
        cycle_id=cycle_id,
    )

    with pytest.raises(CollectionError) as caught:
        client.get("/healthz")

    assert caught.value.outcome == "http_error"
    assert caught.value.status == status
    assert caught.value.path == "/healthz"
    assert telemetry_store.connection.execute(
        "SELECT route, attempt, outcome, http_status FROM request_attempts"
    ).fetchone() == ("/healthz", 1, "http_error", status)


def test_client_uses_429_error_body_retry_hint(monkeypatch):
    responses = iter(
        [
            http_error(
                "https://example.invalid/healthz",
                429,
                b"retry after 7 seconds",
            ),
            Response(b"ok"),
        ]
    )

    def respond(request, timeout):
        result = next(responses)
        if isinstance(result, Exception):
            raise result
        return result

    delays = []
    monkeypatch.setattr(collect, "open_origin", respond)
    monkeypatch.setattr(collect.time, "sleep", delays.append)

    assert Client("https://example.invalid", 1, 1).get("/healthz") == "ok"
    assert delays == [7.0]


def test_client_uses_retry_after_header_without_reading_the_429_body(monkeypatch):
    class UnreadableBody(io.BytesIO):
        def read1(self, size=-1):
            raise AssertionError("Retry-After header made the error body unnecessary")

    responses = iter(
        [
            urllib.error.HTTPError(
                "https://example.invalid/healthz",
                429,
                "failed",
                {"Retry-After": "0"},
                UnreadableBody(b"unused"),
            ),
            Response(b"ok"),
        ]
    )

    def respond(request, timeout):
        result = next(responses)
        if isinstance(result, Exception):
            raise result
        return result

    delays = []
    monkeypatch.setattr(collect, "open_origin", respond)
    monkeypatch.setattr(collect.time, "sleep", delays.append)

    assert Client("https://example.invalid", 1, 1).get("/healthz") == "ok"
    assert delays == [0.0]


def test_client_records_success_status_latency_and_redacted_route(
    telemetry_store,
    monkeypatch,
):
    monkeypatch.setattr(
        collect,
        "open_origin",
        lambda request, timeout: Response(b'{"messages":[]}'),
    )
    cycle_id = telemetry_store.start_cycle("collector", "2026-08-30T10:00:00Z")
    client = Client(
        "https://example.invalid",
        1,
        0,
        telemetry=telemetry_store,
        cycle_id=cycle_id,
    )

    assert client.get("/r/private-room-name?format=json") == '{"messages":[]}'
    assert telemetry_store.connection.execute(
        """
        SELECT route, metered, attempt, outcome, http_status,
               latency_ms >= 0
        FROM request_attempts
        """
    ).fetchone() == ("/r/{room}", 1, 1, "success", 200, 1)
    stored_text = " ".join(
        str(value)
        for row in telemetry_store.connection.execute(
            "SELECT route, outcome FROM request_attempts"
        )
        for value in row
    )
    assert "private-room-name" not in stored_text


@pytest.mark.parametrize("status", [404, 429, 503])
def test_client_records_typed_terminal_http_errors(
    telemetry_store,
    monkeypatch,
    status,
):
    def fail(request, timeout):
        raise http_error(request.full_url, status)

    monkeypatch.setattr(collect, "open_origin", fail)
    cycle_id = telemetry_store.start_cycle("collector", "2026-08-30T10:00:00Z")
    client = Client(
        "https://example.invalid",
        1,
        0,
        telemetry=telemetry_store,
        cycle_id=cycle_id,
    )

    with pytest.raises(CollectionError) as caught:
        client.get("/r/never-store-this-name?format=json")

    assert caught.value.outcome == "http_error"
    assert caught.value.status == status
    assert caught.value.path == "/r/{room}"
    assert telemetry_store.connection.execute(
        "SELECT route, attempt, outcome, http_status FROM request_attempts"
    ).fetchone() == ("/r/{room}", 1, "http_error", status)


@pytest.mark.parametrize(
    ("raised", "expected"),
    [
        (TimeoutError("timed out"), "timeout"),
        (urllib.error.URLError("connection refused"), "transport_error"),
    ],
)
def test_client_records_timeout_and_transport_errors(
    telemetry_store,
    monkeypatch,
    raised,
    expected,
):
    def fail(request, timeout):
        raise raised

    monkeypatch.setattr(collect, "open_origin", fail)
    cycle_id = telemetry_store.start_cycle("collector", "2026-08-30T10:00:00Z")
    client = Client(
        "https://example.invalid",
        1,
        0,
        telemetry=telemetry_store,
        cycle_id=cycle_id,
    )

    with pytest.raises(CollectionError) as caught:
        client.get("/rooms?format=json")

    assert caught.value.outcome == expected
    assert caught.value.status is None
    assert caught.value.path == "/rooms"
    assert telemetry_store.connection.execute(
        "SELECT outcome, http_status FROM request_attempts"
    ).fetchone() == (expected, None)


def test_client_records_each_retry_without_changing_retry_behavior(
    telemetry_store,
    monkeypatch,
):
    responses = iter(
        [
            http_error("https://example.invalid/rooms", 503),
            Response(b"ok"),
        ]
    )

    def respond(request, timeout):
        result = next(responses)
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(collect, "open_origin", respond)
    monkeypatch.setattr(collect.time, "sleep", lambda delay: None)
    cycle_id = telemetry_store.start_cycle("collector", "2026-08-30T10:00:00Z")
    client = Client(
        "https://example.invalid",
        1,
        1,
        telemetry=telemetry_store,
        cycle_id=cycle_id,
    )

    assert client.get("/rooms?format=json") == "ok"
    assert telemetry_store.connection.execute(
        """
        SELECT attempt, outcome, http_status
        FROM request_attempts
        ORDER BY id
        """
    ).fetchall() == [
        (1, "http_error", 503),
        (2, "success", 200),
    ]


def test_rooms_parser_failure_reclassifies_the_exact_successful_attempt(
    telemetry_store,
    monkeypatch,
    tmp_path,
):
    monkeypatch.setattr(
        collect,
        "open_origin",
        lambda request, timeout: Response(b'{"total":"not-an-integer"}'),
    )
    cycle_id = telemetry_store.start_cycle("collector", "2026-08-30T10:00:00Z")
    client = Client(
        "https://example.invalid",
        1,
        0,
        telemetry=telemetry_store,
        cycle_id=cycle_id,
    )

    with pytest.raises(CollectionError) as caught:
        collect.collect_tick(client, tmp_path / "signers.json", 10)

    assert caught.value.outcome == "invalid_response"
    assert caught.value.path == "/rooms"
    assert telemetry_store.connection.execute(
        "SELECT route, outcome, http_status FROM request_attempts"
    ).fetchone() == ("/rooms", "invalid_response", 200)


@pytest.mark.parametrize(
    ("path", "body", "parser"),
    (
        (
            "/r/sample-room?format=json&limit=200",
            "not-json",
            lambda value, route: collect.parse_room_messages(value, route),
        ),
        (
            "/r/events?format=json&limit=200",
            '{"messages":"not-a-list"}',
            lambda value, route: collect.parse_events(value),
        ),
        (
            "/kv/did-ff",
            '{"items":[],"total":1}',
            lambda value, route: collect.parse_shard_count(value, "did-ff"),
        ),
    ),
)
def test_collected_response_parser_failures_replace_success_truth(
    telemetry_store,
    monkeypatch,
    path,
    body,
    parser,
):
    monkeypatch.setattr(
        collect,
        "open_origin",
        lambda request, timeout: Response(body.encode("utf-8")),
    )
    cycle_id = telemetry_store.start_cycle("collector", "2026-08-30T10:00:00Z")
    client = Client(
        "https://example.invalid",
        1,
        0,
        telemetry=telemetry_store,
        cycle_id=cycle_id,
    )

    response = client.get(path)
    with pytest.raises(CollectionError) as caught:
        collect.parse_collected_response(client, path, parser, response, path)

    assert caught.value.outcome == "invalid_response"
    expected_route = normalize_route(path)[0]
    assert telemetry_store.connection.execute(
        "SELECT route, outcome, http_status FROM request_attempts"
    ).fetchone() == (expected_route, "invalid_response", 200)


def test_revisit_parser_failure_is_check_failed_and_attempt_is_invalid_response(
    telemetry_store,
    monkeypatch,
    tmp_path,
):
    signer_path = tmp_path / "signers.sqlite3"
    connection = collect.connect_signer_database(signer_path)
    connection.execute(
        """
        INSERT INTO room_ledger (
            name, room_id, room_sha256, created_seq, created_at, first_observed_at
        ) VALUES (?, ?, ?, 1, ?, ?)
        """,
        (
            "invalid-revisit",
            collect.room_identifier("invalid-revisit"),
            hashlib.sha256(b"invalid-revisit").hexdigest(),
            "2026-08-30T08:00:00Z",
            "2026-08-30T08:00:01Z",
        ),
    )
    connection.execute(
        """
        INSERT INTO room_revisits (room_created_seq, stage_seconds, due_at)
        VALUES (1, 300, '2026-08-30T08:05:00Z')
        """
    )
    connection.commit()
    monkeypatch.setattr(collect, "utc_now", lambda: "2026-08-30T08:05:01Z")
    monkeypatch.setattr(
        collect,
        "open_origin",
        lambda request, timeout: Response(b"not-json"),
    )
    cycle_id = telemetry_store.start_cycle("collector", "2026-08-30T08:05:01Z")
    client = Client(
        "https://example.invalid",
        1,
        0,
        telemetry=telemetry_store,
        cycle_id=cycle_id,
    )

    lifecycle = collect.collect_room_revisits(
        client,
        connection,
        "2026-08-30T08:05:01Z",
        sampled_room_reads=1,
        deadline=collect.time.monotonic() + 1,
    )
    connection.close()

    assert lifecycle["revisits"][0]["outcome"] == "check_failed"
    assert telemetry_store.connection.execute(
        "SELECT route, outcome, http_status FROM request_attempts"
    ).fetchone() == ("/r/{room}", "invalid_response", 200)


def test_telemetry_write_failure_does_not_interrupt_primary_response(monkeypatch):
    class FailedTelemetry:
        def record_attempt(self, *args):
            raise sqlite3.OperationalError("disk full")

    monkeypatch.setattr(
        collect,
        "open_origin",
        lambda request, timeout: Response(b"primary response"),
    )
    client = Client(
        "https://example.invalid",
        1,
        0,
        telemetry=FailedTelemetry(),
        cycle_id=1,
    )

    assert client.get("/healthz") == "primary response"
    assert client.telemetry_degraded is True
    assert client.last_attempt_id is None


@pytest.mark.parametrize("failure_stage", ("start", "finish"))
def test_collector_primary_tick_survives_telemetry_cycle_failure(
    tmp_path,
    monkeypatch,
    capsys,
    failure_stage,
):
    output = tmp_path / "ticks.jsonl"

    class FailedStore:
        def __init__(self, path):
            self.path = path

        def start_cycle(self, source):
            if failure_stage == "start":
                raise sqlite3.OperationalError("telemetry unavailable")
            return 7

        def finish_cycle(self, *args, **kwargs):
            raise sqlite3.OperationalError("telemetry unavailable")

        def close(self):
            return None

    monkeypatch.setattr(collect, "TelemetryStore", FailedStore)
    monkeypatch.setattr(collect, "collect_tick", lambda *args, **kwargs: {"ts": "ok"})
    drain_calls = 0

    def drain(path, *args, **kwargs):
        nonlocal drain_calls
        drain_calls += 1
        if drain_calls == 1:
            return False
        path.write_text('{"ts":"ok"}\n', encoding="utf-8")
        return True

    monkeypatch.setattr(collect, "drain_tick_outbox", drain)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "collect.py",
            "--base-url",
            "https://example.invalid",
            "--output",
            str(output),
            "--once",
        ],
    )

    assert collect.main() == 0
    assert output.is_file()
    assert "telemetry degraded" in capsys.readouterr().err


def test_client_telemetry_degradation_marks_cycle_failed_without_losing_tick(
    tmp_path,
    monkeypatch,
):
    output = tmp_path / "ticks.jsonl"
    finished = []

    class RecoverableStore:
        def __init__(self, path):
            self.path = path

        def start_cycle(self, source):
            return 9

        def finish_cycle(self, *args, **kwargs):
            finished.append((args, kwargs))

        def close(self):
            return None

    def degraded_tick(client, *args, **kwargs):
        client.telemetry_degraded = True
        return {"ts": "ok"}

    monkeypatch.setattr(collect, "TelemetryStore", RecoverableStore)
    monkeypatch.setattr(collect, "collect_tick", degraded_tick)
    drain_calls = 0

    def drain(path, *args, **kwargs):
        nonlocal drain_calls
        drain_calls += 1
        if drain_calls == 1:
            return False
        path.write_text('{"ts":"ok"}\n', encoding="utf-8")
        return True

    monkeypatch.setattr(collect, "drain_tick_outbox", drain)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "collect.py",
            "--base-url",
            "https://example.invalid",
            "--output",
            str(output),
            "--once",
        ],
    )

    assert collect.main() == 0
    assert output.is_file()
    assert finished == [((9, "failure"), {"error_outcome": "storage_error"})]


def test_retention_is_bounded_and_preserves_canonical_discovery_changes(tmp_path):
    store = TelemetryStore(tmp_path / "telemetry.sqlite3")
    try:
        cutoff = "2026-08-30T10:00:00Z"
        old_at = (
            (parse_timestamp(cutoff) - timedelta(seconds=RAW_RETENTION_SECONDS + 1))
            .isoformat(timespec="seconds")
            .replace("+00:00", "Z")
        )
        cycle_id = store.start_cycle("pulse_probe", old_at)
        first = store.record_attempt(
            cycle_id, "/config", False, 1, old_at, 1, "success", 200
        )
        store.record_discovery_snapshot(
            first,
            "/config",
            old_at,
            "a" * 64,
            {"version": "one"},
        )
        duplicate = store.record_attempt(
            cycle_id, "/config", False, 2, old_at, 1, "success", 200
        )
        assert (
            store.record_discovery_snapshot(
                duplicate,
                "/config",
                old_at,
                "a" * 64,
                {"version": "one"},
            )
            is None
        )
        changed = store.record_attempt(
            cycle_id, "/config", False, 3, old_at, 1, "success", 200
        )
        store.record_discovery_snapshot(
            changed,
            "/config",
            old_at,
            "b" * 64,
            {"version": "two"},
        )
        for attempt in range(4, 9):
            store.record_attempt(
                cycle_id, "/healthz", False, attempt, old_at, 1, "success", 200
            )
        store.finish_cycle(cycle_id, "success", finished_at=old_at)

        result = store.prune(cutoff, batch_size=2)

        assert result["request_attempts"] == 2
        assert store.connection.execute(
            "SELECT digest_sha256 FROM discovery_snapshots ORDER BY id"
        ).fetchall() == [("a" * 64,), ("b" * 64,)]
        retained_attempts = {
            row[0]
            for row in store.connection.execute(
                "SELECT attempt_id FROM discovery_snapshots"
            )
        }
        assert retained_attempts.issubset(
            {
                row[0]
                for row in store.connection.execute("SELECT id FROM request_attempts")
            }
        )
        page_size = store.connection.execute("PRAGMA page_size").fetchone()[0]
        max_pages = store.connection.execute("PRAGMA max_page_count").fetchone()[0]
        assert page_size * max_pages <= MAX_DATABASE_BYTES
    finally:
        store.close()


def test_collector_failed_cycle_persists_without_writing_a_tick(
    tmp_path,
    monkeypatch,
):
    output = tmp_path / "ticks.jsonl"

    def fail(request, timeout):
        raise http_error(request.full_url, 503)

    monkeypatch.setattr(collect, "open_origin", fail)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "collect.py",
            "--base-url",
            "https://example.invalid",
            "--output",
            str(output),
            "--retries",
            "0",
            "--once",
        ],
    )

    assert collect.main() == 1
    assert not output.exists()
    telemetry_path = tmp_path / "telemetry.sqlite3"
    assert telemetry_path.exists()
    connection = sqlite3.connect(telemetry_path)
    try:
        assert connection.execute(
            "SELECT source, outcome, error_outcome FROM cycles"
        ).fetchone() == ("collector", "failure", "http_error")
        assert connection.execute(
            "SELECT route, outcome, http_status FROM request_attempts"
        ).fetchone() == ("/rooms", "http_error", 503)
    finally:
        connection.close()


def test_collector_parser_requires_absolute_explicit_telemetry_path(tmp_path):
    parser = collect.build_parser()
    base = [
        "--base-url",
        "https://example.invalid",
        "--output",
        str(tmp_path / "ticks.jsonl"),
    ]
    with pytest.raises(SystemExit):
        parser.parse_args([*base, "--telemetry-database", "relative.sqlite3"])
    parsed = parser.parse_args(
        [*base, "--telemetry-database", str(tmp_path / "attempts.sqlite3")]
    )
    assert parsed.telemetry_database == tmp_path / "attempts.sqlite3"


def test_pulse_cadence_allowlist_and_zero_metered_reads(
    telemetry_store,
    monkeypatch,
):
    calls = []
    bodies = {
        "/healthz": b"ok",
        "/config": json.dumps(
            {
                "service": "technocore",
                "version": "0.10.0",
                "env_prefix": "must-not-be-stored",
                "settings": {
                    "rate_read": 600,
                    "max_rooms": 81920,
                    "unknown_future_value": "must-not-be-stored",
                },
            }
        ).encode(),
        "/.well-known/agent.json": json.dumps(
            {
                "schema_version": "1.0",
                "version": "0.10.0",
                "description": "must-not-be-stored",
                "protocols": ["http"],
                "capabilities": [
                    {
                        "name": "read_room",
                        "description": "must-not-be-stored",
                        "method": "GET",
                        "path": "/r/{room}",
                        "unknown_future_value": "must-not-be-stored",
                    }
                ],
                "limits": {
                    "reads_per_minute_per_ip": 600,
                    "duplicate_filter_seconds": 60,
                    "retention_seconds": None,
                    "unknown_future_value": "must-not-be-stored",
                },
                "trust": {"content_is_untrusted": True, "note": "excluded"},
            }
        ).encode(),
    }

    def respond(request, timeout):
        path = urllib.parse.urlsplit(request.full_url).path
        calls.append(path)
        return Response(bodies[path])

    monkeypatch.setattr(collect, "open_origin", respond)

    assert pulse_probe.run_probe_cycle(
        telemetry_store,
        "https://example.invalid",
        1,
        "2026-08-30T10:00:00Z",
    )
    assert pulse_probe.run_probe_cycle(
        telemetry_store,
        "https://example.invalid",
        1,
        "2026-08-30T10:01:00Z",
    )

    assert calls == [
        "/healthz",
        "/config",
        "/.well-known/agent.json",
        "/healthz",
    ]
    assert telemetry_store.connection.execute(
        "SELECT COUNT(*), COALESCE(SUM(metered), 0) FROM request_attempts"
    ).fetchone() == (4, 0)
    snapshots = telemetry_store.connection.execute(
        "SELECT route, digest_sha256, fields_json FROM discovery_snapshots ORDER BY route"
    ).fetchall()
    assert [row[0] for row in snapshots] == [
        "/.well-known/agent.json",
        "/config",
    ]
    assert all(len(row[1]) == 64 for row in snapshots)
    fields = {route: json.loads(raw) for route, _, raw in snapshots}
    assert fields["/config"] == {
        "service": "technocore",
        "settings": {"max_rooms": 81920, "rate_read": 600},
        "version": "0.10.0",
    }
    assert fields["/.well-known/agent.json"] == {
        "capabilities": [{"method": "GET", "name": "read_room", "path": "/r/{room}"}],
        "limits": {
            "duplicate_filter_seconds": 60,
            "reads_per_minute_per_ip": 600,
            "retention_seconds": None,
        },
        "protocols": ["http"],
        "schema_version": "1.0",
        "trust": {"content_is_untrusted": True},
        "version": "0.10.0",
    }


def test_pulse_routes_continue_independently_without_retries(
    telemetry_store,
    monkeypatch,
):
    calls = []

    def respond(request, timeout):
        path = urllib.parse.urlsplit(request.full_url).path
        calls.append(path)
        if path == "/healthz":
            raise TimeoutError("timed out")
        if path == "/config":
            raise http_error(request.full_url, 503)
        return Response(b'{"schema_version":"1.0","version":"0.10.0"}')

    monkeypatch.setattr(collect, "open_origin", respond)

    assert not pulse_probe.run_probe_cycle(
        telemetry_store,
        "https://example.invalid",
        1,
        "2026-08-30T10:00:00Z",
    )
    assert calls == ["/healthz", "/config", "/.well-known/agent.json"]
    assert telemetry_store.connection.execute(
        "SELECT route, attempt, outcome FROM request_attempts ORDER BY id"
    ).fetchall() == [
        ("/healthz", 1, "timeout"),
        ("/config", 1, "http_error"),
        ("/.well-known/agent.json", 1, "success"),
    ]
    assert telemetry_store.connection.execute(
        "SELECT outcome, error_outcome FROM cycles"
    ).fetchone() == ("failure", "timeout")


def test_invalid_agent_allowlisted_value_retains_its_normalized_route():
    with pytest.raises(CollectionError) as caught:
        pulse_probe.canonical_discovery_fields(
            "/.well-known/agent.json",
            '{"limits":{"rooms":{"unexpected":"nested"}}}',
        )

    assert caught.value.outcome == "invalid_response"
    assert caught.value.path == "/.well-known/agent.json"


@pytest.mark.parametrize(
    ("route", "payload"),
    (
        ("/config", {"service": "x" * 4_097}),
        ("/.well-known/agent.json", {"protocols": ["http"] * 129}),
        (
            "/.well-known/agent.json",
            {"capabilities": [{"name": "read"}] * 129},
        ),
    ),
)
def test_discovery_fields_are_bounded_before_storage(route, payload):
    with pytest.raises(CollectionError) as caught:
        pulse_probe.canonical_discovery_fields(route, json.dumps(payload))

    assert caught.value.outcome == "invalid_response"
    assert caught.value.path == route


@pytest.mark.parametrize(
    ("route", "body"),
    (
        pytest.param(
            "/config",
            r'{"service":"\ud800"}',
            id="allowlisted-string",
        ),
        pytest.param(
            "/.well-known/agent.json",
            r'{"protocols":["\ud800"]}',
            id="allowlisted-list-string",
        ),
    ),
)
def test_discovery_fields_reject_lone_surrogates_before_digest(route, body):
    with pytest.raises(CollectionError) as caught:
        pulse_probe.canonical_discovery_fields(route, body)

    assert caught.value.outcome == "invalid_response"
    assert caught.value.path == route
