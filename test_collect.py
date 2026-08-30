import json
import sys
import time
import types

import pytest

from collect import (
    COLLECTOR_VERSION,
    ROOM_READ_BUDGET,
    CollectionError,
    aggregate_funnel,
    connect_signer_database,
    exclusive_state_lock,
    new_signer_state,
    parse_room_messages,
    room_sample_names,
    signer_funnel_counts,
    tracking_disclosure,
    update_signer_state,
)
from migrate_signers import migrate_signers

DID = "did:key:z6Mk" + "a" * 40
OTHER_DID = "did:key:z6Mk" + "b" * 40


@pytest.fixture
def signer_store(tmp_path):
    connection = connect_signer_database(tmp_path / "signers.sqlite3")
    yield connection
    connection.close()


def room_body(*messages):
    return json.dumps({"messages": list(messages)})


def message(seq, sender, **extra):
    return {"seq": seq, "ts": "2026-08-28T08:00:00Z", "from": sender, "text": "x", **extra}


def insert_record(
    connection,
    did,
    tick_count,
    collection_dates,
    rooms,
    has_counterparty,
):
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
            did,
            "2026-08-28T08:00:00Z",
            "2026-08-29T08:00:00Z",
            tick_count,
            "2026-08-28",
            "2026-08-29" if collection_dates >= 2 else "2026-08-28",
            collection_dates,
            json.dumps(rooms),
            len(rooms),
            has_counterparty,
        ),
    )


def test_collector_version_is_bumped_for_sqlite_signer_storage():
    assert COLLECTOR_VERSION == "2.4.0"


def test_room_sampling_manifest_records_configured_read_budget():
    state = new_signer_state(100)
    newest_rooms = [{"name": f"room-{index}"} for index in range(100)]
    names, manifest = room_sample_names(newest_rooms, state)

    assert len(names) == ROOM_READ_BUDGET
    assert manifest["read_budget"] == ROOM_READ_BUDGET
    assert manifest["frame_size"] == 101
    assert manifest["sampled"] == []


def test_did_shaped_sender_without_nonce_is_not_counted_as_a_signer(signer_store):
    messages = parse_room_messages(room_body(message(1, DID)), "/r/test")
    state = new_signer_state(100)
    update_signer_state(
        signer_store,
        state,
        [("test", messages)],
        "2026-08-28T08:00:01Z",
    )
    assert signer_funnel_counts(signer_store)["observed"] == 0


def test_did_shaped_sender_with_nonce_is_counted_as_a_signer(signer_store):
    messages = parse_room_messages(room_body(message(1, DID, nonce="12345")), "/r/test")
    state = new_signer_state(100)
    update_signer_state(
        signer_store,
        state,
        [("test", messages)],
        "2026-08-28T08:00:01Z",
    )
    rows = signer_store.execute("SELECT did FROM signer_dids").fetchall()
    assert rows == [(DID[len("did:key:z6Mk") :],)]


def test_nonce_bearing_sender_without_did_shape_is_not_counted(signer_store):
    messages = parse_room_messages(room_body(message(1, "server", nonce="12345")), "/r/test")
    state = new_signer_state(100)
    update_signer_state(
        signer_store,
        state,
        [("test", messages)],
        "2026-08-28T08:00:01Z",
    )
    assert signer_funnel_counts(signer_store)["observed"] == 0


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


def test_unsigned_messages_never_join_counterparty_detection(signer_store):
    base = "2026-08-28T08:00:0{}Z"
    body = room_body(
        {"seq": 1, "ts": base.format(0), "from": DID, "text": "x", "nonce": "1"},
        {"seq": 2, "ts": base.format(1), "from": OTHER_DID, "text": "x"},
    )
    messages = parse_room_messages(body, "/r/test")
    state = new_signer_state(100)
    update_signer_state(
        signer_store,
        state,
        [("test", messages)],
        "2026-08-28T08:00:02Z",
    )
    row = signer_store.execute(
        "SELECT has_counterparty FROM signer_dids WHERE did = ?",
        (DID[len("did:key:z6Mk") :],),
    ).fetchone()
    assert row == (0,)
    assert (
        signer_store.execute(
            "SELECT 1 FROM signer_dids WHERE did = ?",
            (OTHER_DID[len("did:key:z6Mk") :],),
        ).fetchone()
        is None
    )


def test_five_sql_funnel_aggregates_match_hand_computed_fixture(signer_store):
    insert_record(signer_store, "one", 1, 1, ["a"], 0)
    insert_record(signer_store, "two", 2, 1, ["a"], 0)
    insert_record(signer_store, "three", 2, 2, ["a"], 0)
    insert_record(signer_store, "four", 2, 2, ["a", "b"], 0)
    insert_record(signer_store, "five", 2, 2, ["a", "b"], 1)

    assert signer_funnel_counts(signer_store) == {
        "observed": 5,
        "two_ticks": 4,
        "two_collection_dates": 3,
        "two_rooms": 2,
        "counterparties": 1,
    }


def test_observing_a_did_twice_updates_one_row(signer_store):
    messages = parse_room_messages(room_body(message(1, DID, nonce="1")), "/r/test")
    state = new_signer_state(1)
    update_signer_state(
        signer_store,
        state,
        [("test", messages)],
        "2026-08-28T08:00:01Z",
    )
    update_signer_state(
        signer_store,
        state,
        [("test", messages)],
        "2026-08-28T08:04:01Z",
    )

    assert signer_store.execute(
        "SELECT COUNT(*), tick_count FROM signer_dids"
    ).fetchone() == (1, 2)


@pytest.mark.parametrize("room_total", [1, 2, 8, 9])
def test_room_count_matches_capped_room_list_predicate(signer_store, room_total):
    suffix = "abcdefghj"[room_total - 1] * 40
    sender = "did:key:z6Mk" + suffix
    messages = parse_room_messages(room_body(message(1, sender, nonce="1")), "/r/test")
    sampled = [(f"room-{index:02d}", messages) for index in range(room_total)]
    state = new_signer_state(1)
    update_signer_state(
        signer_store,
        state,
        sampled,
        "2026-08-28T08:00:01Z",
    )

    rooms_json, room_count = signer_store.execute(
        "SELECT rooms_json, room_count FROM signer_dids WHERE did = ?",
        (suffix,),
    ).fetchone()
    stored_rooms = json.loads(rooms_json)
    original_rooms = {name for name, _ in sampled}
    assert stored_rooms == sorted(original_rooms)[:8]
    assert room_count == min(len(original_rooms), 8)
    assert (room_count >= 2) == (len(sorted(original_rooms)[:8]) >= 2)


def test_funnel_is_monotonically_non_increasing(signer_store):
    insert_record(signer_store, "one", 1, 1, ["a"], 0)
    insert_record(signer_store, "two", 2, 1, ["a"], 0)
    insert_record(signer_store, "three", 2, 2, ["a"], 0)
    insert_record(signer_store, "four", 2, 2, ["a", "b"], 0)
    insert_record(signer_store, "five", 2, 2, ["a", "b"], 1)

    counts = signer_funnel_counts(signer_store)
    stages = [
        counts["observed"],
        counts["two_ticks"],
        counts["two_collection_dates"],
        counts["two_rooms"],
        counts["counterparties"],
    ]
    assert stages == sorted(stages, reverse=True)


def test_insertion_beyond_legacy_200000_cap_succeeds(signer_store):
    signer_store.executemany(
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
        VALUES (?, NULL, NULL, 0, NULL, NULL, 0, '[]', 0, 0)
        """,
        ((f"legacy-{index}",) for index in range(200_000)),
    )
    state = new_signer_state(200_000)
    messages = parse_room_messages(room_body(message(1, DID, nonce="1")), "/r/test")
    update_signer_state(
        signer_store,
        state,
        [("test", messages)],
        "2026-08-30T08:00:00Z",
    )

    assert signer_funnel_counts(signer_store)["observed"] == 200_001


def test_saturation_disclosure_uses_recorded_timestamps():
    state = new_signer_state(200_000)
    assert tracking_disclosure(state) is None

    state["cap_hit"] = True
    state["tracking_cap_saturation"] = {
        "started_at": "2026-08-29T18:00:15Z",
        "released_at": "2026-08-30T09:10:11Z",
        "permanent_undercount": True,
    }
    disclosure = tracking_disclosure(state)

    assert "2026-08-29T18:00:15Z" in disclosure["warning"]
    assert "2026-08-30T09:10:11Z" in disclosure["warning"]
    assert "not recorded" in disclosure["warning"]
    assert "undercount is permanent" in disclosure["warning"]
    assert "without an insertion cap" in disclosure["methodology"]


def test_aggregate_omits_disclosure_when_cap_never_saturated(signer_store):
    state = new_signer_state(200_000)
    funnel = aggregate_funnel(signer_store, state, 1, 1)
    assert "tracking_disclosure" not in funnel


def test_v2_migration_preserves_all_funnel_counts(tmp_path):
    state = new_signer_state(10)
    state["version"] = 2
    state.pop("tracking_cap_saturation")
    state["dids"] = {
        "one": {
            "first_observed_ts": "2026-08-28T08:00:00Z",
            "last_observed_ts": "2026-08-28T08:00:00Z",
            "tick_count": 1,
            "collection_first_utc_date": "2026-08-28",
            "collection_last_utc_date": "2026-08-28",
            "collection_utc_dates_count": 1,
            "rooms": ["a"],
            "counterparties_count": 0,
        },
        "two": {
            "first_observed_ts": "2026-08-28T08:00:00Z",
            "last_observed_ts": "2026-08-29T08:00:00Z",
            "tick_count": 2,
            "collection_first_utc_date": "2026-08-28",
            "collection_last_utc_date": "2026-08-28",
            "collection_utc_dates_count": 1,
            "rooms": ["a"],
            "counterparties_count": 0,
        },
        "three": {
            "first_observed_ts": "2026-08-28T08:00:00Z",
            "last_observed_ts": "2026-08-29T08:00:00Z",
            "tick_count": 2,
            "collection_first_utc_date": "2026-08-28",
            "collection_last_utc_date": "2026-08-29",
            "collection_utc_dates_count": 2,
            "rooms": ["a"],
            "counterparties_count": 0,
        },
        "four": {
            "first_observed_ts": "2026-08-28T08:00:00Z",
            "last_observed_ts": "2026-08-29T08:00:00Z",
            "tick_count": 2,
            "collection_first_utc_date": "2026-08-28",
            "collection_last_utc_date": "2026-08-29",
            "collection_utc_dates_count": 2,
            "rooms": ["a", "b"],
            "counterparties_count": 0,
        },
        "five": {
            "first_observed_ts": "2026-08-28T08:00:00Z",
            "last_observed_ts": "2026-08-29T08:00:00Z",
            "tick_count": 2,
            "collection_first_utc_date": "2026-08-28",
            "collection_last_utc_date": "2026-08-29",
            "collection_utc_dates_count": 2,
            "rooms": ["a", "b"],
            "counterparties_count": 1,
        },
    }
    source = tmp_path / "signers-v2.json"
    metadata = tmp_path / "signers.json"
    source.write_text(json.dumps(state), encoding="utf-8")

    source_counts, sqlite_counts = migrate_signers(source, metadata)

    assert source_counts == sqlite_counts == {
        "observed": 5,
        "two_ticks": 4,
        "two_collection_dates": 3,
        "two_rooms": 2,
        "counterparties": 1,
    }
    migrated_metadata = json.loads(metadata.read_text(encoding="utf-8"))
    assert migrated_metadata["version"] == 3
    assert "dids" not in migrated_metadata
    assert migrated_metadata["selector_seed"] == state["selector_seed"]
    assert migrated_metadata["selector_epoch"] == state["selector_epoch"]


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
