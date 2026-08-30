import hashlib
import json
import sqlite3
import sys
import time
import types

import pytest

import collect
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
from verify_ledger import (
    canonical_tick_bytes,
    canonical_tick_hash_bytes,
    verify_ledger,
)

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


def test_collector_version_is_bumped_for_hash_chained_ticks():
    assert tuple(map(int, COLLECTOR_VERSION.split("."))) > (2, 6, 0)


def test_hash_chain_declares_genesis_after_unchained_prefix_and_verifies(tmp_path):
    path = tmp_path / "ticks.jsonl"
    path.write_text(
        '{"legacy":"first"}\n{malformed legacy line}\n{"legacy":"second"}\n',
        encoding="utf-8",
    )

    first = {"ts": "2026-08-30T10:00:00Z", "value": "alpha"}
    second = {"ts": "2026-08-30T10:04:18Z", "value": "bravo"}
    collect.append_jsonl(path, first)
    collect.append_jsonl(path, second)

    lines = path.read_text(encoding="utf-8").splitlines()
    genesis = json.loads(lines[3])
    successor = json.loads(lines[4])
    assert genesis["ledger_chain"]["previous_sha256"] is None
    assert genesis["ledger_chain"]["tick_sha256"] == hashlib.sha256(
        canonical_tick_hash_bytes(genesis)
    ).hexdigest()
    assert successor["ledger_chain"]["previous_sha256"] == hashlib.sha256(
        canonical_tick_bytes(genesis)
    ).hexdigest()

    result = verify_ledger(path)
    assert result["ok"] is True
    assert result["ticks"] == 5
    assert result["unchained_prefix_ticks"] == 3
    assert result["genesis_line"] == 4
    assert result["genesis_ts"] == "2026-08-30T10:00:00Z"
    assert result["first_break"] is None


@pytest.mark.parametrize(
    ("target", "before", "after"),
    [
        (0, '"value":"alpha"', '"value":"Alpha"'),
        (1, '"value":"bravo"', '"value":"Bravo"'),
        (2, '"value":"delta"', '"value":"Delta"'),
    ],
)
def test_hash_chain_detects_single_byte_edit_anywhere_in_chained_suffix(
    tmp_path,
    target,
    before,
    after,
):
    path = tmp_path / "ticks.jsonl"
    for index, value in enumerate(("alpha", "bravo", "delta")):
        collect.append_jsonl(
            path,
            {
                "ts": f"2026-08-30T10:{index:02d}:00Z",
                "value": value,
            },
        )

    lines = path.read_text(encoding="utf-8").splitlines()
    changed = lines[target].replace(before, after, 1)
    assert changed != lines[target]
    assert len(changed.encode("utf-8")) == len(lines[target].encode("utf-8"))
    lines[target] = changed
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    result = verify_ledger(path)
    assert result["ok"] is False
    assert result["first_break"] == target + 1
    assert "tick_sha256 mismatch" in result["message"]


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


def test_signed_reciprocal_alternation_marks_both_participants(signer_store):
    body = room_body(
        {
            "seq": 1,
            "ts": "2026-08-28T08:00:00Z",
            "from": DID,
            "text": "x",
            "nonce": "1",
        },
        {
            "seq": 2,
            "ts": "2026-08-28T08:00:01Z",
            "from": OTHER_DID,
            "text": "x",
            "nonce": "2",
        },
        {
            "seq": 3,
            "ts": "2026-08-28T08:00:02Z",
            "from": DID,
            "text": "x",
            "nonce": "3",
        },
    )
    state = new_signer_state(100)
    update_signer_state(
        signer_store,
        state,
        [("test", parse_room_messages(body, "/r/test"))],
        "2026-08-28T08:00:03Z",
    )

    rows = signer_store.execute(
        "SELECT did, has_counterparty FROM signer_dids ORDER BY did"
    ).fetchall()
    assert rows == [
        (DID[len("did:key:z6Mk") :], 1),
        (OTHER_DID[len("did:key:z6Mk") :], 1),
    ]


def test_signed_adjacency_without_return_does_not_count_as_reciprocity(signer_store):
    body = room_body(
        {
            "seq": 1,
            "ts": "2026-08-28T08:00:00Z",
            "from": DID,
            "text": "x",
            "nonce": "1",
        },
        {
            "seq": 2,
            "ts": "2026-08-28T08:00:01Z",
            "from": OTHER_DID,
            "text": "x",
            "nonce": "2",
        },
    )
    state = new_signer_state(100)
    update_signer_state(
        signer_store,
        state,
        [("test", parse_room_messages(body, "/r/test"))],
        "2026-08-28T08:00:02Z",
    )

    assert signer_store.execute(
        "SELECT did, has_counterparty FROM signer_dids ORDER BY did"
    ).fetchall() == [
        (DID[len("did:key:z6Mk") :], 0),
        (OTHER_DID[len("did:key:z6Mk") :], 0),
    ]


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


def test_v3_counterparty_flags_are_reset_during_v4_migration(tmp_path):
    path = tmp_path / "signers.sqlite3"
    connection = connect_signer_database(path)
    state = new_signer_state(100)
    state["version"] = 3
    insert_record(connection, "legacy-cooccurrence", 2, 2, ["a", "b"], 1)
    connection.execute(
        "INSERT INTO signer_metadata (singleton, state_json) VALUES (1, ?)",
        (
            json.dumps(
                state,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ),
        ),
    )
    connection.execute("PRAGMA user_version = 3")
    connection.commit()
    connection.close()

    migrated = connect_signer_database(path)
    try:
        assert migrated.execute(
            "SELECT has_counterparty FROM signer_dids WHERE did = ?",
            ("legacy-cooccurrence",),
        ).fetchone() == (0,)
        metadata = json.loads(
            migrated.execute(
                "SELECT state_json FROM signer_metadata WHERE singleton = 1"
            ).fetchone()[0]
        )
        assert metadata["version"] == collect.SIGNER_STATE_VERSION
    finally:
        migrated.close()


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
    assert (
        "DIDs first appearing during that interval and never re-observed were lost entirely."
        in disclosure["warning"]
    )
    assert "understate that cohort until those counters rebuild" in disclosure["warning"]
    assert (
        "has a first-observed timestamp no earlier than that later observation"
        in disclosure["warning"]
    )
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
    assert migrated_metadata["version"] == collect.SIGNER_STATE_VERSION
    assert "dids" not in migrated_metadata
    assert migrated_metadata["selector_seed"] == state["selector_seed"]
    assert migrated_metadata["selector_epoch"] == state["selector_epoch"]


def test_sqlite_error_skips_tick_and_daemon_loop_continues(
    tmp_path, monkeypatch, capsys
):
    output = tmp_path / "ticks.jsonl"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "collect.py",
            "--base-url",
            "https://example.invalid",
            "--output",
            str(output),
            "--interval",
            "1",
        ],
    )

    attempts = []

    def collect_once_then_succeed(*args, **kwargs):
        attempts.append((args, kwargs))
        if len(attempts) == 1:
            raise sqlite3.OperationalError("database is locked")
        return {"tick": len(attempts)}

    written = []
    monkeypatch.setattr(collect, "collect_tick", collect_once_then_succeed)
    monkeypatch.setattr(
        collect,
        "append_jsonl",
        lambda path, record: written.append((path, record)),
    )

    class StopDaemon(Exception):
        pass

    sleeps = []

    def stop_after_second_iteration(delay):
        sleeps.append(delay)
        if len(sleeps) == 2:
            raise StopDaemon

    monkeypatch.setattr(collect.time, "sleep", stop_after_second_iteration)

    with pytest.raises(StopDaemon):
        collect.main()

    assert len(attempts) == 2
    assert written == [(output, {"tick": 2})]
    assert "collection failed; no tick written: database is locked" in (
        capsys.readouterr().err
    )


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
