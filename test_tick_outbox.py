import hashlib
import json
import sqlite3
import sys

import pytest

import collect
from verify_ledger import CHAIN_FIELD, canonical_tick_bytes
from verify_ledger import verify_ledger


TICK_TS = "2026-08-31T12:00:00Z"
CENSUS_STARTED_AT = "2026-08-31T11:30:00Z"


class TickClient:
    def __init__(self, event_seq=1, room_name="outbox-room"):
        self.event_seq = event_seq
        self.room_name = room_name
        self.paths = []

    def get(self, path, deadline=None):
        self.paths.append(path)
        if path == "/rooms?format=json&limit=200":
            return json.dumps(
                {
                    "total": 1,
                    "capacity": 100,
                    "bytes": 0,
                    "notes": {"total": 0, "capacity": 100, "bytes": 0},
                    "rooms": [
                        {"name": "lobby", "seq": 1, "idle": 0},
                    ],
                }
            )
        if path == "/r/lobby?format=json&limit=200":
            return json.dumps(
                {
                    "room": "lobby",
                    "messages": [
                        {
                            "seq": 1,
                            "ts": TICK_TS,
                            "from": "server",
                            "text": "ready",
                        }
                    ],
                }
            )
        if path == "/r/events?format=json&limit=200":
            return json.dumps(
                {
                    "room": "events",
                    "messages": [
                        {
                            "seq": self.event_seq,
                            "ts": TICK_TS,
                            "from": "server",
                            "text": f"created {self.room_name}",
                        }
                    ],
                }
            )
        raise AssertionError(f"unexpected read: {path}")


def test_collect_tick_persists_exact_canonical_outbox_payload_and_census_tuple(
    tmp_path,
    monkeypatch,
):
    signer_state_path = tmp_path / "signers.json"
    random_bytes = bytes(range(32))
    monkeypatch.setattr(collect, "utc_now", lambda: TICK_TS)
    monkeypatch.setattr(
        collect.secrets, "token_bytes", lambda size: random_bytes[:size]
    )

    tick = collect.collect_tick(
        TickClient(),
        signer_state_path,
        100,
        identity_total=42,
        census_started=CENSUS_STARTED_AT,
    )

    connection = collect.connect_signer_database(
        collect.signer_database_path(signer_state_path)
    )
    try:
        outbox = collect.load_tick_outbox(connection)
    finally:
        connection.close()

    payload = canonical_tick_bytes(tick)
    payload_digest = hashlib.sha256(payload).digest()
    assert outbox == {
        "operation_id": hashlib.sha256(
            collect.TICK_OUTBOX_OPERATION_DOMAIN + random_bytes + payload_digest
        ).hexdigest(),
        "payload": payload,
        "payload_sha256": payload_digest.hex(),
        "census_started_at": CENSUS_STARTED_AT,
        "census_total": 42,
        "tick": tick,
    }
    assert not payload.endswith(b"\n")
    assert CHAIN_FIELD not in tick


def test_collect_tick_rollback_removes_state_and_outbox_together(
    tmp_path,
    monkeypatch,
):
    signer_state_path = tmp_path / "signers.json"
    monkeypatch.setattr(collect, "utc_now", lambda: TICK_TS)
    original_insert = collect._insert_tick_outbox

    def fail_after_insert(*args, **kwargs):
        original_insert(*args, **kwargs)
        raise RuntimeError("injected failure after outbox insert")

    monkeypatch.setattr(collect, "_insert_tick_outbox", fail_after_insert)

    with pytest.raises(RuntimeError, match="after outbox insert"):
        collect.collect_tick(TickClient(), signer_state_path, 100)

    connection = collect.connect_signer_database(
        collect.signer_database_path(signer_state_path)
    )
    try:
        metadata = json.loads(
            connection.execute(
                "SELECT state_json FROM signer_metadata WHERE singleton = 1"
            ).fetchone()[0]
        )
        assert connection.execute("SELECT COUNT(*) FROM room_ledger").fetchone() == (0,)
        assert connection.execute("SELECT COUNT(*) FROM room_revisits").fetchone() == (
            0,
        )
        assert connection.execute("SELECT COUNT(*) FROM signer_dids").fetchone() == (0,)
        assert collect.load_tick_outbox(connection) is None
        assert metadata["selector_position"] == 0
        assert "last_updated" not in metadata
    finally:
        connection.close()


def test_pending_outbox_blocks_a_second_collection_before_state_mutation(
    tmp_path,
    monkeypatch,
):
    signer_state_path = tmp_path / "signers.json"
    database_path = collect.signer_database_path(signer_state_path)
    monkeypatch.setattr(collect, "utc_now", lambda: TICK_TS)
    collect.collect_tick(TickClient(), signer_state_path, 100)

    connection = collect.connect_signer_database(database_path)
    try:
        before = {
            "metadata": connection.execute(
                "SELECT state_json FROM signer_metadata WHERE singleton = 1"
            ).fetchone(),
            "rooms": connection.execute(
                "SELECT * FROM room_ledger ORDER BY created_seq"
            ).fetchall(),
            "revisits": connection.execute(
                "SELECT * FROM room_revisits ORDER BY room_created_seq, stage_seconds"
            ).fetchall(),
            "signers": connection.execute(
                "SELECT * FROM signer_dids ORDER BY did"
            ).fetchall(),
            "outbox": connection.execute("SELECT * FROM tick_outbox").fetchall(),
        }
    finally:
        connection.close()
    state_file_before = signer_state_path.read_bytes()

    second_client = TickClient(event_seq=2, room_name="overtaking-room")
    with pytest.raises(collect.CollectionError, match="pending tick outbox"):
        collect.collect_tick(second_client, signer_state_path, 100)

    connection = collect.connect_signer_database(database_path)
    try:
        after = {
            "metadata": connection.execute(
                "SELECT state_json FROM signer_metadata WHERE singleton = 1"
            ).fetchone(),
            "rooms": connection.execute(
                "SELECT * FROM room_ledger ORDER BY created_seq"
            ).fetchall(),
            "revisits": connection.execute(
                "SELECT * FROM room_revisits ORDER BY room_created_seq, stage_seconds"
            ).fetchall(),
            "signers": connection.execute(
                "SELECT * FROM signer_dids ORDER BY did"
            ).fetchall(),
            "outbox": connection.execute("SELECT * FROM tick_outbox").fetchall(),
        }
    finally:
        connection.close()

    assert after == before
    assert signer_state_path.read_bytes() == state_file_before
    assert second_client.paths == ["/rooms?format=json&limit=200"]


def test_tick_outbox_delete_is_conditioned_on_operation_and_payload_hash(
    tmp_path,
    monkeypatch,
):
    signer_state_path = tmp_path / "signers.json"
    monkeypatch.setattr(collect, "utc_now", lambda: TICK_TS)
    collect.collect_tick(TickClient(), signer_state_path, 100)
    connection = collect.connect_signer_database(
        collect.signer_database_path(signer_state_path)
    )
    try:
        outbox = collect.load_tick_outbox(connection)
        assert outbox is not None
        assert (
            collect.delete_tick_outbox(
                connection,
                "0" * 64,
                outbox["payload_sha256"],
            )
            is False
        )
        assert collect.load_tick_outbox(connection) == outbox
        assert (
            collect.delete_tick_outbox(
                connection,
                outbox["operation_id"],
                outbox["payload_sha256"],
            )
            is True
        )
        assert collect.load_tick_outbox(connection) is None
    finally:
        connection.close()


@pytest.mark.parametrize(
    "tamper",
    (
        pytest.param("payload-hash", id="payload-hash"),
        pytest.param("noncanonical-payload", id="noncanonical-payload"),
        pytest.param("chained-payload", id="chained-payload"),
        pytest.param("census-binding", id="census-binding"),
    ),
)
def test_load_tick_outbox_rejects_malformed_or_tampered_rows(
    tmp_path,
    monkeypatch,
    tamper,
):
    signer_state_path = tmp_path / "signers.json"
    monkeypatch.setattr(collect, "utc_now", lambda: TICK_TS)
    collect.collect_tick(
        TickClient(),
        signer_state_path,
        100,
        identity_total=42,
        census_started=CENSUS_STARTED_AT,
    )
    connection = collect.connect_signer_database(
        collect.signer_database_path(signer_state_path)
    )
    try:
        payload = connection.execute(
            "SELECT payload FROM tick_outbox WHERE singleton = 1"
        ).fetchone()[0]
        if tamper == "payload-hash":
            connection.execute(
                "UPDATE tick_outbox SET payload_sha256 = ? WHERE singleton = 1",
                ("0" * 64,),
            )
        else:
            tick = json.loads(payload)
            if tamper == "noncanonical-payload":
                changed_payload = json.dumps(tick, sort_keys=True).encode("utf-8")
            elif tamper == "chained-payload":
                tick[CHAIN_FIELD] = {
                    "version": 1,
                    "previous_sha256": None,
                    "tick_sha256": "0" * 64,
                }
                changed_payload = canonical_tick_bytes(tick)
            else:
                connection.execute(
                    "UPDATE tick_outbox SET census_total = 43 WHERE singleton = 1"
                )
                changed_payload = None
            if changed_payload is not None:
                connection.execute(
                    """
                    UPDATE tick_outbox
                    SET payload = ?, payload_sha256 = ?
                    WHERE singleton = 1
                    """,
                    (
                        changed_payload,
                        hashlib.sha256(changed_payload).hexdigest(),
                    ),
                )

        with pytest.raises(collect.CollectionError, match="tick outbox"):
            collect.load_tick_outbox(connection)
    finally:
        connection.close()


@pytest.mark.parametrize(
    "values",
    (
        pytest.param(
            (1, "A" * 64, b"{}", "0" * 64, None, None),
            id="uppercase-operation-id",
        ),
        pytest.param(
            (1, "0" * 64, "{}", "0" * 64, None, None),
            id="text-payload",
        ),
        pytest.param(
            (1, "0" * 64, b"{}", "A" * 64, None, None),
            id="uppercase-payload-hash",
        ),
        pytest.param(
            (1, "0" * 64, b"{}", "0" * 64, CENSUS_STARTED_AT, None),
            id="unpaired-census",
        ),
        pytest.param(
            (1, "0" * 64, b"{}", "0" * 64, CENSUS_STARTED_AT, -1),
            id="negative-census-total",
        ),
        pytest.param(
            (2, "0" * 64, b"{}", "0" * 64, None, None),
            id="non-singleton",
        ),
    ),
)
def test_tick_outbox_schema_rejects_invalid_storage_types_and_bounds(
    tmp_path,
    values,
):
    connection = collect.connect_signer_database(tmp_path / "signers.sqlite3")
    try:
        assert collect.load_tick_outbox(connection) is None
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO tick_outbox VALUES (?, ?, ?, ?, ?, ?)",
                values,
            )
    finally:
        connection.close()


def test_drain_tick_outbox_publishes_once_and_conditionally_deletes(
    tmp_path,
    monkeypatch,
):
    signer_state_path = tmp_path / "signers.json"
    output = tmp_path / "ticks.jsonl"
    census_state_path = tmp_path / "census.json"
    monkeypatch.setattr(collect, "utc_now", lambda: TICK_TS)
    tick = collect.collect_tick(TickClient(), signer_state_path, 100)

    assert (
        collect.drain_tick_outbox(
            output,
            signer_state_path,
            census_state_path,
        )
        is True
    )

    assert verify_ledger(output)["ticks"] == 1
    connection = collect.connect_signer_database(
        collect.signer_database_path(signer_state_path)
    )
    try:
        assert collect.load_tick_outbox(connection) is None
    finally:
        connection.close()
    stored = json.loads(output.read_text(encoding="utf-8"))
    stored.pop(CHAIN_FIELD)
    assert stored == tick
    assert (
        collect.drain_tick_outbox(
            output,
            signer_state_path,
            census_state_path,
        )
        is False
    )


def test_drain_retry_after_ledger_append_does_not_duplicate_tick(
    tmp_path,
    monkeypatch,
):
    signer_state_path = tmp_path / "signers.json"
    output = tmp_path / "ticks.jsonl"
    census_state_path = tmp_path / "census.json"
    monkeypatch.setattr(collect, "utc_now", lambda: TICK_TS)
    collect.collect_tick(TickClient(), signer_state_path, 100)
    real_delete = collect.delete_tick_outbox

    def fail_before_delete(*args, **kwargs):
        raise OSError("injected failure before outbox delete")

    monkeypatch.setattr(collect, "delete_tick_outbox", fail_before_delete)
    with pytest.raises(OSError, match="before outbox delete"):
        collect.drain_tick_outbox(output, signer_state_path, census_state_path)
    assert verify_ledger(output)["ticks"] == 1

    monkeypatch.setattr(collect, "delete_tick_outbox", real_delete)
    assert (
        collect.drain_tick_outbox(
            output,
            signer_state_path,
            census_state_path,
        )
        is True
    )
    assert verify_ledger(output)["ticks"] == 1


def test_drain_retry_preserves_census_acknowledgement_and_deduplicates(
    tmp_path,
    monkeypatch,
):
    signer_state_path = tmp_path / "signers.json"
    output = tmp_path / "ticks.jsonl"
    census_state_path = tmp_path / "census.json"
    counts = {f"did-{index:02x}": 1 for index in range(256)}
    collect.save_census_state(
        census_state_path,
        {
            "version": collect.CENSUS_STATE_VERSION,
            "started_at": CENSUS_STARTED_AT,
            "counts": counts,
            "ledger_published": False,
        },
    )
    monkeypatch.setattr(collect, "utc_now", lambda: TICK_TS)
    collect.collect_tick(
        TickClient(),
        signer_state_path,
        100,
        identity_total=256,
        census_started=CENSUS_STARTED_AT,
    )
    real_delete = collect.delete_tick_outbox

    def fail_before_delete(*args, **kwargs):
        raise OSError("injected failure before outbox delete")

    monkeypatch.setattr(collect, "delete_tick_outbox", fail_before_delete)
    with pytest.raises(OSError, match="before outbox delete"):
        collect.drain_tick_outbox(output, signer_state_path, census_state_path)
    assert collect.load_census_state(census_state_path)["ledger_published"] is True
    assert verify_ledger(output)["ticks"] == 1

    monkeypatch.setattr(collect, "delete_tick_outbox", real_delete)
    assert (
        collect.drain_tick_outbox(
            output,
            signer_state_path,
            census_state_path,
        )
        is True
    )
    assert collect.load_census_state(census_state_path)["ledger_published"] is True
    assert verify_ledger(output)["ticks"] == 1


def test_main_drains_old_outbox_without_spending_a_new_read_budget(
    tmp_path,
    monkeypatch,
):
    output = tmp_path / "ticks.jsonl"
    signer_state_path = tmp_path / "signers.json"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "collect.py",
            "--base-url",
            "https://example.invalid",
            "--output",
            str(output),
            "--signer-state",
            str(signer_state_path),
            "--once",
        ],
    )
    events = []

    def drain(*args, **kwargs):
        events.append("drain")
        return True

    def must_not_collect(*args, **kwargs):
        raise AssertionError("startup recovery spent a new origin read budget")

    monkeypatch.setattr(collect, "drain_tick_outbox", drain)
    monkeypatch.setattr(collect, "collect_tick", must_not_collect)
    monkeypatch.setattr(
        collect,
        "append_jsonl",
        lambda *args, **kwargs: pytest.fail("main used the non-idempotent append path"),
    )

    assert collect.main() == 0
    assert events == ["drain"]


def test_main_collects_then_drains_when_startup_outbox_is_empty(
    tmp_path,
    monkeypatch,
):
    output = tmp_path / "ticks.jsonl"
    signer_state_path = tmp_path / "signers.json"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "collect.py",
            "--base-url",
            "https://example.invalid",
            "--output",
            str(output),
            "--signer-state",
            str(signer_state_path),
            "--once",
        ],
    )
    events = []
    drain_results = iter((False, True))

    def drain(*args, **kwargs):
        events.append("drain")
        return next(drain_results)

    def collect_once(*args, **kwargs):
        events.append("collect")
        return {"ignored": "the outbox is authoritative"}

    monkeypatch.setattr(collect, "drain_tick_outbox", drain)
    monkeypatch.setattr(collect, "collect_tick", collect_once)

    assert collect.main() == 0
    assert events == ["drain", "collect", "drain"]
