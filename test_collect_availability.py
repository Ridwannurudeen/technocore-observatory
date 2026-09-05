import json
import sqlite3
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import collect
from query_service import DEFAULT_QUERY_TIMEOUT_SECONDS


def test_signer_database_remains_readable_during_slow_origin_fetch(tmp_path):
    signer_state_path = tmp_path / "signers.json"
    database_path = collect.signer_database_path(signer_state_path)

    connection = collect.connect_signer_database(database_path)
    try:
        # Make SQLite's real cache-spill lock escalation deterministic with the
        # maximum-size event window; production can reach the same state as its
        # signer and room indexes grow. This setting persists across connections.
        connection.execute("PRAGMA default_cache_size = 1")
        connection.commit()
    finally:
        connection.close()

    slow_response_started = threading.Event()
    reader_finished = threading.Event()
    reader_result = []

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
                time.sleep(2)
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

    def read_during_fetch():
        if not slow_response_started.wait(timeout=10):
            reader_result.append(TimeoutError("slow origin response did not start"))
            reader_finished.set()
            return
        try:
            readonly = sqlite3.connect(
                database_path.resolve().as_uri() + "?mode=ro",
                uri=True,
                timeout=DEFAULT_QUERY_TIMEOUT_SECONDS,
            )
            try:
                reader_result.append(
                    readonly.execute("SELECT COUNT(*) FROM room_ledger").fetchone()
                )
            finally:
                readonly.close()
        except sqlite3.Error as error:
            reader_result.append(error)
        finally:
            reader_finished.set()

    reader_thread = threading.Thread(target=read_during_fetch)
    reader_thread.start()
    try:
        client = collect.Client(
            f"http://127.0.0.1:{server.server_port}",
            timeout=5,
            retries=0,
        )
        collect.collect_tick(client, signer_state_path, 100)
    finally:
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=5)
        reader_thread.join(timeout=5)

    assert reader_finished.is_set()
    assert reader_result == [(0,)]
