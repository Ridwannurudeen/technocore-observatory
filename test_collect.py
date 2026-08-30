import json
import sys
import time
import types

import pytest

from collect import (
    COLLECTOR_VERSION,
    ROOM_READ_BUDGET,
    CollectionError,
    exclusive_state_lock,
    new_signer_state,
    parse_room_messages,
    room_sample_names,
    update_signer_state,
)

DID = "did:key:z6Mk" + "a" * 40
OTHER_DID = "did:key:z6Mk" + "b" * 40


def room_body(*messages):
    return json.dumps({"messages": list(messages)})


def message(seq, sender, **extra):
    return {"seq": seq, "ts": "2026-08-28T08:00:00Z", "from": sender, "text": "x", **extra}


def test_collector_version_is_bumped_for_budgeted_200_room_sampling():
    assert COLLECTOR_VERSION == "2.3.0"


def test_room_sampling_manifest_records_configured_read_budget():
    state = new_signer_state(100)
    newest_rooms = [{"name": f"room-{index}"} for index in range(100)]
    names, manifest = room_sample_names(newest_rooms, state)

    assert len(names) == ROOM_READ_BUDGET
    assert manifest["read_budget"] == ROOM_READ_BUDGET
    assert manifest["frame_size"] == 101
    assert manifest["sampled"] == []


def test_did_shaped_sender_without_nonce_is_not_counted_as_a_signer():
    messages = parse_room_messages(room_body(message(1, DID)), "/r/test")
    state = new_signer_state(100)
    update_signer_state(state, [("test", messages)], "2026-08-28T08:00:01Z")
    assert state["dids"] == {}


def test_did_shaped_sender_with_nonce_is_counted_as_a_signer():
    messages = parse_room_messages(room_body(message(1, DID, nonce="12345")), "/r/test")
    state = new_signer_state(100)
    update_signer_state(state, [("test", messages)], "2026-08-28T08:00:01Z")
    assert list(state["dids"]) == [DID[len("did:key:z6Mk") :]]


def test_nonce_bearing_sender_without_did_shape_is_not_counted():
    messages = parse_room_messages(room_body(message(1, "server", nonce="12345")), "/r/test")
    state = new_signer_state(100)
    update_signer_state(state, [("test", messages)], "2026-08-28T08:00:01Z")
    assert state["dids"] == {}


def test_parse_room_messages_retains_and_normalizes_the_nonce():
    body = room_body(
        message(1, DID, nonce="42"),
        message(2, DID, nonce=42),
        message(3, DID),
        message(4, DID, nonce="not-digits"),
        message(5, DID, nonce=""),
    )
    nonces = [entry["nonce"] for entry in parse_room_messages(body, "/r/test")]
    assert nonces == ["42", "42", None, None, None]


def test_unsigned_messages_never_join_counterparty_detection():
    base = "2026-08-28T08:00:0{}Z"
    body = room_body(
        {"seq": 1, "ts": base.format(0), "from": DID, "text": "x", "nonce": "1"},
        {"seq": 2, "ts": base.format(1), "from": OTHER_DID, "text": "x"},
    )
    messages = parse_room_messages(body, "/r/test")
    state = new_signer_state(100)
    update_signer_state(state, [("test", messages)], "2026-08-28T08:00:02Z")
    record = state["dids"][DID[len("did:key:z6Mk") :]]
    assert record["counterparties_count"] == 0
    assert OTHER_DID[len("did:key:z6Mk") :] not in state["dids"]


def test_state_lock_degrades_to_a_noop_where_fcntl_is_unavailable(tmp_path, monkeypatch):
    monkeypatch.setitem(sys.modules, "fcntl", None)
    entered = False
    with exclusive_state_lock(tmp_path / "signers.json", timeout=0.1):
        entered = True
    assert entered
    assert not (tmp_path / "signers.json.lock").exists()


def test_state_lock_fails_closed_when_the_lock_is_held(tmp_path, monkeypatch):
    fake = types.ModuleType("fcntl")
    fake.LOCK_EX = 2
    fake.LOCK_NB = 4
    fake.LOCK_UN = 8

    def flock(descriptor, operation):
        if operation & fake.LOCK_NB:
            raise BlockingIOError

    fake.flock = flock
    monkeypatch.setitem(sys.modules, "fcntl", fake)
    started = time.monotonic()
    with pytest.raises(CollectionError, match="lock"):
        with exclusive_state_lock(tmp_path / "signers.json", timeout=0.3):
            raise AssertionError("the lock must never be granted here")
    assert time.monotonic() - started >= 0.3


def test_state_lock_serializes_and_releases(tmp_path, monkeypatch):
    fake = types.ModuleType("fcntl")
    fake.LOCK_EX = 2
    fake.LOCK_NB = 4
    fake.LOCK_UN = 8
    calls = []

    def flock(descriptor, operation):
        calls.append(operation)

    fake.flock = flock
    monkeypatch.setitem(sys.modules, "fcntl", fake)
    with exclusive_state_lock(tmp_path / "signers.json", timeout=0.1):
        pass
    assert calls == [fake.LOCK_EX | fake.LOCK_NB, fake.LOCK_UN]
    assert (tmp_path / "signers.json.lock").exists()
