import json
import sqlite3
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

import collect
from query_service import DEFAULT_QUERY_TIMEOUT_SECONDS, open_readonly_database


def test_signer_database_remains_readable_during_apply_transaction(tmp_path):
    signer_state_path = tmp_path / "signers.json"
    database_path = collect.signer_database_path(signer_state_path)

    connection = collect.connect_signer_database(database_path)
    # Make SQLite's real cache-spill lock escalation deterministic with the
    # maximum-size event window; production can reach the same state as its
    # signer and room indexes grow.
    connection.execute("PRAGMA default_cache_size = 1")
    connection.commit()

    slow_response_started = threading.Event()
    apply_transaction_started = threading.Event()
    reader_finished = threading.Event()
    reader_result = []
    trace_failures = []

    def pause_apply_for_reader(statement):
        if (
            "INSERT INTO tick_outbox" not in statement
            or apply_transaction_started.is_set()
        ):
            return
        apply_transaction_started.set()
        if not reader_finished.wait(timeout=10):
            trace_failures.append(TimeoutError("read did not finish during apply"))

    connection.set_trace_callback(pause_apply_for_reader)

    rooms = {
        "total": 1,
        "capacity": 1_000,
        "bytes": 0,
        "notes": {"total": 0, "capacity": 1_000, "bytes": 0},
        "rooms": [{"name": "lobby", "seq": 1, "idle": 0}],
    }
    events = {
        "room": "events",
        "messages": [
            {
                "seq": index + 1,
                "ts": "2020-01-01T00:00:00Z",
                "from": "server",
                "text": f"created lock-room-{index:03d}",
            }
            for index in range(200)
        ],
    }

    class SlowOriginHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/rooms?format=json&limit=200":
                payload = rooms
            elif self.path == "/r/events?format=json&limit=200":
                payload = events
            elif self.path == "/r/lobby?format=json&limit=200":
                slow_response_started.set()
                time.sleep(0.25)
                payload = {
                    "room": "lobby",
                    "messages": [
                        {
                            "seq": 1,
                            "ts": "2020-01-01T00:00:00Z",
                            "from": "server",
                            "text": "ready",
                        }
                    ],
                }
            else:
                raise AssertionError(f"unexpected origin read: {self.path}")

            body = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), SlowOriginHandler)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()

    def read_during_apply():
        if not apply_transaction_started.wait(timeout=10):
            reader_result.append(TimeoutError("apply transaction did not start"))
            reader_finished.set()
            return
        try:
            readonly = open_readonly_database(
                database_path,
                query_timeout_seconds=DEFAULT_QUERY_TIMEOUT_SECONDS,
            )
            try:
                reader_result.append(
                    (
                        readonly.execute("PRAGMA journal_mode").fetchone()[0],
                        readonly.execute("SELECT COUNT(*) FROM room_ledger").fetchone()[
                            0
                        ],
                        readonly.execute(
                            "SELECT COUNT(*) FROM room_search WHERE room_search MATCH ?",
                            ('"lobby"',),
                        ).fetchone()[0],
                    )
                )
            finally:
                readonly.close()
        except sqlite3.Error as error:
            reader_result.append(error)
        finally:
            reader_finished.set()

    reader_thread = threading.Thread(target=read_during_apply)
    reader_thread.start()
    try:
        client = collect.Client(
            f"http://127.0.0.1:{server.server_port}",
            timeout=5,
            retries=0,
        )
        collect.collect_tick(
            client,
            signer_state_path,
            100,
            signer_connection=connection,
        )
    finally:
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=5)
        reader_thread.join(timeout=5)
        connection.set_trace_callback(None)
        connection.close()

    assert slow_response_started.is_set()
    assert apply_transaction_started.is_set()
    assert reader_finished.is_set()
    assert trace_failures == []
    assert reader_result == [("wal", 0, 0)]


def test_collector_holds_signer_connection_across_two_ticks(tmp_path, monkeypatch):
    output_path = tmp_path / "ticks.jsonl"
    signer_state_path = tmp_path / "signers.json"
    database_path = collect.signer_database_path(signer_state_path)
    wal_path = database_path.with_name(database_path.name + "-wal")
    shm_path = database_path.with_name(database_path.name + "-shm")
    connections = []
    between_tick_sidecars = []
    drain_results = iter((False, True, False, True))

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "collect.py",
            "--base-url",
            "https://example.invalid",
            "--output",
            str(output_path),
            "--signer-state",
            str(signer_state_path),
            "--interval",
            "60",
        ],
    )

    def collect_without_origin_reads(*args, signer_connection, **kwargs):
        connections.append(signer_connection)
        assert wal_path.is_file()
        assert shm_path.is_file()
        return {"tick": len(connections)}

    def drain_without_publication(*args, signer_connection, **kwargs):
        assert signer_connection.in_transaction is False
        return next(drain_results)

    class StopDaemon(Exception):
        pass

    def observe_between_ticks(delay):
        between_tick_sidecars.append((wal_path.is_file(), shm_path.is_file()))
        if len(between_tick_sidecars) == 2:
            raise StopDaemon

    monkeypatch.setattr(collect, "collect_tick", collect_without_origin_reads)
    monkeypatch.setattr(collect, "drain_tick_outbox", drain_without_publication)
    monkeypatch.setattr(collect.time, "sleep", observe_between_ticks)

    with pytest.raises(StopDaemon):
        collect.main()

    assert len(connections) == 2
    assert connections[0] is connections[1]
    assert between_tick_sidecars == [(True, True), (True, True)]
    assert not wal_path.exists()
    assert not shm_path.exists()
