import hashlib
import io
import json
import sqlite3
import sys
import time
import types
from contextlib import nullcontext
from unittest import mock

import pytest

import collect
import pulse_probe
from collect import (
    COLLECTOR_VERSION,
    ROOM_READ_BUDGET,
    CollectionError,
    aggregate_funnel,
    connect_signer_database,
    exclusive_state_lock,
    new_signer_state,
    parse_room_messages,
    read_budget_summary,
    record_created_rooms,
    room_sample_names,
    select_due_room_revisits,
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
REVISIT_SELECTOR_SEED = "0123456789abcdef0123456789abcdef"


@pytest.fixture
def signer_store(tmp_path):
    connection = connect_signer_database(tmp_path / "signers.sqlite3")
    yield connection
    connection.close()


def select_revisits(
    connection,
    now,
    *,
    allocation_rotation=0,
    limit=collect.ROOM_REVISIT_READ_BUDGET,
):
    return select_due_room_revisits(
        connection,
        now,
        selector_version=collect.SELECTOR_VERSION,
        selector_seed=REVISIT_SELECTOR_SEED,
        allocation_rotation=allocation_rotation,
        limit=limit,
    )


def collect_revisits(
    client,
    connection,
    tick_ts,
    *,
    sampled_room_reads,
    deadline,
    allocation_rotation=0,
):
    return collect.collect_room_revisits(
        client,
        connection,
        tick_ts,
        sampled_room_reads=sampled_room_reads,
        selector_version=collect.SELECTOR_VERSION,
        selector_seed=REVISIT_SELECTOR_SEED,
        allocation_rotation=allocation_rotation,
        deadline=deadline,
    )


def test_signer_database_uses_delete_journal_without_sidecars(tmp_path):
    path = tmp_path / "signers.sqlite3"
    connection = connect_signer_database(path)
    assert connection.execute("PRAGMA journal_mode").fetchone() == ("delete",)
    connection.close()

    assert not path.with_name(path.name + "-wal").exists()
    assert not path.with_name(path.name + "-shm").exists()


def test_origin_collection_parsers_enforce_documented_record_caps():
    rooms = {
        "total": 201,
        "capacity": 1_000,
        "bytes": 0,
        "notes": {"total": 0, "capacity": 1_000, "bytes": 0},
        "rooms": [
            {"room": f"room-{index}", "last_seq": index, "idle_seconds": 0}
            for index in range(201)
        ],
    }
    messages = {"messages": [message(index, "server") for index in range(201)]}

    with pytest.raises(CollectionError, match="200-record cap"):
        collect.parse_rooms(json.dumps(rooms))
    with pytest.raises(CollectionError, match="200-record cap"):
        parse_room_messages(json.dumps(messages), "/r/test")


@pytest.mark.parametrize("name", ("UPPERCASE", "space room", "x" * 49))
def test_room_listing_and_event_parsers_reject_invalid_room_names(name):
    rooms = {
        "total": 1,
        "capacity": 1_000,
        "bytes": 0,
        "notes": {"total": 0, "capacity": 1_000, "bytes": 0},
        "rooms": [{"room": name, "last_seq": 1, "idle_seconds": 0}],
    }
    event = {
        "room": "events",
        "messages": [
            {
                "seq": 1,
                "ts": "2026-08-28T08:00:00Z",
                "from": "server",
                "text": f"created {name}",
            }
        ],
    }

    with pytest.raises(CollectionError):
        collect.parse_rooms(json.dumps(rooms))
    with pytest.raises(CollectionError):
        collect.parse_events(json.dumps(event))


HUGE_JSON_INTEGER = "1" * 4_301
DEEPLY_NESTED_JSON = "[" * 5_000 + "0" + "]" * 5_000


@pytest.mark.parametrize(
    ("parser", "body"),
    (
        pytest.param(
            collect.parse_rooms,
            f'{{"total":{HUGE_JSON_INTEGER}}}',
            id="rooms",
        ),
        pytest.param(
            collect.parse_room_row,
            f'{{"name":"lobby","seq":{HUGE_JSON_INTEGER},"idle":0}}',
            id="room-row",
        ),
        pytest.param(
            lambda body: collect.extract_last_seq(body, "/r/lobby"),
            f'{{"seq":{HUGE_JSON_INTEGER}}}',
            id="last-sequence",
        ),
        pytest.param(
            collect.parse_events,
            f'{{"messages":[{{"seq":{HUGE_JSON_INTEGER}}}]}}',
            id="events",
        ),
        pytest.param(
            lambda body: collect.parse_room_messages(body, "/r/lobby"),
            f'{{"messages":[{{"seq":{HUGE_JSON_INTEGER}}}]}}',
            id="room-messages",
        ),
        pytest.param(
            lambda body: collect.parse_shard_count(body, "did-ff"),
            f'{{"items":[],"total":{HUGE_JSON_INTEGER}}}',
            id="identity-shard",
        ),
        pytest.param(
            lambda body: pulse_probe.canonical_discovery_fields("/config", body),
            f'{{"service":{HUGE_JSON_INTEGER}}}',
            id="discovery",
        ),
    ),
)
def test_origin_json_integer_limit_failures_are_invalid_responses(parser, body):
    with pytest.raises(CollectionError) as caught:
        parser(body)

    assert caught.value.outcome == "invalid_response"


@pytest.mark.parametrize(
    ("parser", "body"),
    (
        pytest.param(
            collect.parse_rooms,
            f'{{"nested":{DEEPLY_NESTED_JSON}}}',
            id="rooms",
        ),
        pytest.param(
            collect.parse_room_row,
            DEEPLY_NESTED_JSON,
            id="room-row",
        ),
        pytest.param(
            lambda body: collect.extract_last_seq(body, "/r/lobby"),
            DEEPLY_NESTED_JSON,
            id="last-sequence",
        ),
        pytest.param(
            collect.parse_events,
            DEEPLY_NESTED_JSON,
            id="events",
        ),
        pytest.param(
            lambda body: collect.parse_room_messages(body, "/r/lobby"),
            DEEPLY_NESTED_JSON,
            id="room-messages",
        ),
        pytest.param(
            lambda body: collect.parse_shard_count(body, "did-ff"),
            DEEPLY_NESTED_JSON,
            id="identity-shard",
        ),
        pytest.param(
            lambda body: pulse_probe.canonical_discovery_fields("/config", body),
            f'{{"service":{DEEPLY_NESTED_JSON}}}',
            id="discovery",
        ),
    ),
)
def test_origin_json_recursion_failures_are_invalid_responses(parser, body):
    with (
        mock.patch.object(json, "loads", side_effect=RecursionError),
        pytest.raises(CollectionError) as caught,
    ):
        parser(body)

    assert caught.value.outcome == "invalid_response"


def test_plaintext_rooms_reject_names_outside_the_documented_grammar():
    body = """\
# 1 of 1 rooms (cap 100, 0 of 100 stored), newest first
# notes 0 of 100 (0 total, 10 per namespace)
invalid/name last_seq=1 idle=0
"""

    with pytest.raises(CollectionError):
        collect.parse_rooms(body)


def test_rooms_reject_non_finite_idle_values():
    body = """{
        "total": 1,
        "capacity": 100,
        "bytes": 0,
        "notes": {"total": 0, "capacity": 100, "bytes": 0},
        "rooms": [{"name": "lobby", "seq": 1, "idle": 1e400}]
    }"""

    with pytest.raises(CollectionError) as caught:
        collect.parse_rooms(body)

    assert caught.value.outcome == "invalid_response"


@pytest.mark.parametrize(
    "field",
    ("shown", "rooms_total", "room_cap", "notes_total", "note_cap"),
)
def test_plaintext_room_headers_reject_oversized_integer_counters(field):
    counters = {
        "shown": "0",
        "rooms_total": "0",
        "room_cap": "100",
        "notes_total": "0",
        "note_cap": "100",
    }
    counters[field] = HUGE_JSON_INTEGER
    body = (
        f"# {counters['shown']} of {counters['rooms_total']} rooms "
        f"(cap {counters['room_cap']}, 0 of 100 stored), newest first\n"
        f"# notes {counters['notes_total']} of {counters['note_cap']} "
        "(0 total, 10 per namespace)\n"
    )

    with pytest.raises(CollectionError) as caught:
        collect.parse_rooms(body)

    assert caught.value.outcome == "invalid_response"


def test_plaintext_rooms_reject_an_overflowing_stored_size():
    body = (
        "# 0 of 0 rooms (cap 100, " + "9" * 400 + "T of 100 stored), newest first\n"
        "# notes 0 of 100 (0 total, 10 per namespace)\n"
    )

    with pytest.raises(CollectionError) as caught:
        collect.parse_rooms(body)

    assert caught.value.outcome == "invalid_response"


def test_plaintext_size_normalizes_decimal_exponent_overflow():
    with pytest.raises(CollectionError) as caught:
        collect.parse_size("1" + "0" * 1_000_000)

    assert caught.value.outcome == "invalid_response"
    assert caught.value.path == "/rooms"


def test_plaintext_size_accepts_the_exact_sqlite_integer_maximum():
    assert collect.parse_size(str(collect.SQLITE_INTEGER_MAX)) == (
        collect.SQLITE_INTEGER_MAX
    )


def test_plaintext_identity_shard_rejects_an_oversized_declared_count():
    body = f"# {HUGE_JSON_INTEGER} entries\n/kv/did-ff/00000000000000\n"

    with pytest.raises(CollectionError) as caught:
        collect.parse_shard_count(body, "did-ff")

    assert caught.value.outcome == "invalid_response"


def test_events_reject_sequences_outside_sqlite_integer_storage():
    body = json.dumps(
        {
            "room": "events",
            "messages": [
                {
                    "seq": 2**63,
                    "ts": "2026-08-28T08:00:00Z",
                    "from": "server",
                    "text": "created overflow-room",
                }
            ],
        }
    )

    with pytest.raises(CollectionError) as caught:
        collect.parse_events(body)

    assert caught.value.outcome == "invalid_response"


@pytest.mark.parametrize(
    "engagement",
    (
        pytest.param('{"score":NaN}', id="non-standard-number"),
        pytest.param(r'{"label":"\ud800"}', id="lone-surrogate"),
    ),
)
def test_rooms_reject_engagement_that_cannot_be_canonicalized(engagement):
    body = (
        '{"total":0,"capacity":100,"bytes":0,'
        '"notes":{"total":0,"capacity":100,"bytes":0},'
        f'"rooms":[],"engagement":{engagement}}}'
    )

    with pytest.raises(CollectionError) as caught:
        collect.parse_rooms(body)

    assert caught.value.outcome == "invalid_response"
    assert caught.value.path == "/rooms"


@pytest.mark.parametrize(
    "body",
    (
        pytest.param('{"error":"bad"}', id="wrong-object"),
        pytest.param("[42]", id="numeric-item"),
        pytest.param('["not-a-key"]', id="invalid-string-item"),
        pytest.param('[{"error":"bad"}]', id="invalid-object-item"),
        pytest.param('[["00000000000000"]]', id="nested-list-item"),
        pytest.param(
            '{"items":[42],"total":1}',
            id="invalid-items-object-entry",
        ),
        pytest.param('{"items":[]}', id="items-missing-total"),
        pytest.param("upstream failure", id="arbitrary-plaintext"),
        pytest.param("# error 0\n", id="error-comment-with-number"),
    ),
)
def test_identity_shards_reject_unrecognized_response_shapes(body):
    with pytest.raises(CollectionError) as caught:
        collect.parse_shard_count(body, "did-ff")

    assert caught.value.outcome == "invalid_response"
    assert caught.value.path == "/kv/{namespace}"


@pytest.mark.parametrize(
    "body",
    (
        pytest.param(
            '["00000000000000","00000000000000"]',
            id="duplicate-array",
        ),
        pytest.param(
            '["/kv/did-ff/00000000000000","/kv/did-ff/00000000000000 "]',
            id="duplicate-array-path-with-whitespace",
        ),
        pytest.param(
            '{"items":["00000000000000","00000000000000"],"total":2}',
            id="duplicate-items-object",
        ),
        pytest.param(
            '{"items":["/kv/did-ff/00000000000000","/kv/did-ff/00000000000000\\t"],"total":2}',
            id="duplicate-items-path-with-whitespace",
        ),
        pytest.param(
            '{"ns":"did-ff","keys":["00000000000000","00000000000000"]}',
            id="duplicate-keys-object",
        ),
        pytest.param(
            "/kv/did-ff/00000000000000\n/kv/did-ff/00000000000000\n",
            id="duplicate-plaintext",
        ),
        pytest.param(
            '{"items":[],"total":0,"has_more":true}',
            id="items-has-more",
        ),
        pytest.param(
            '{"items":[],"total":0,"next":"cursor"}',
            id="items-next-cursor",
        ),
        pytest.param(
            '{"ns":"did-ff","keys":[],"next":"cursor"}',
            id="keys-next-cursor",
        ),
    ),
)
def test_identity_shards_reject_duplicates_and_pagination(body):
    with pytest.raises(CollectionError) as caught:
        collect.parse_shard_count(body, "did-ff")

    assert caught.value.outcome == "invalid_response"
    assert caught.value.path == "/kv/{namespace}"


@pytest.mark.parametrize(
    "body",
    (
        pytest.param("", id="empty-plaintext"),
        pytest.param("[]", id="empty-array"),
        pytest.param('{"items":[],"total":0}', id="empty-items-object"),
        pytest.param('{"ns":"did-ff","keys":[]}', id="empty-keys-object"),
        pytest.param("# 0 entries\n", id="empty-declared-plaintext"),
        pytest.param(
            "# budget: 599 of 600 reads left this minute "
            "(refills 10.0 tokens/s; a 429 states the wait, and the full limits "
            "are in /.well-known/agent.json)\n",
            id="empty-with-budget-footer",
        ),
    ),
)
def test_identity_shards_preserve_valid_empty_representations(body):
    assert collect.parse_shard_count(body, "did-ff") == 0


def test_identity_shards_accept_the_exact_upstream_budget_footer_after_rows():
    body = (
        "/kv/did-ff/00000000000000\n\n"
        "# budget: 5 of 10 reads left this minute (refills one token every 6s; "
        "a 429 states the wait, and the full limits are in "
        "/.well-known/agent.json)\n"
    )

    assert collect.parse_shard_count(body, "did-ff") == 1


@pytest.mark.parametrize(
    "timestamp",
    (
        pytest.param("9999-12-31T23:59:59Z", id="revisit-overflow"),
        pytest.param("0001-01-01T00:00:00+14:00", id="utc-underflow"),
    ),
)
def test_events_reject_timestamps_outside_the_supported_schedule(timestamp):
    body = json.dumps(
        {
            "room": "events",
            "messages": [
                {
                    "seq": 1,
                    "ts": timestamp,
                    "from": "server",
                    "text": "created boundary-room",
                }
            ],
        }
    )

    with pytest.raises(CollectionError) as caught:
        collect.parse_events(body)

    assert caught.value.outcome == "invalid_response"
    assert caught.value.path == "/r/events"


@pytest.mark.parametrize(
    "envelope",
    (
        pytest.param({"messages": []}, id="missing-room"),
        pytest.param({"room": "lobby", "messages": []}, id="wrong-room"),
        pytest.param({"room": 7, "messages": []}, id="non-string-room"),
    ),
)
def test_events_bind_the_json_envelope_to_the_events_room(envelope):
    with pytest.raises(CollectionError, match="room envelope"):
        collect.parse_events(json.dumps(envelope))


def room_body(*messages):
    return json.dumps({"messages": list(messages)})


def message(seq, sender, **extra):
    return {
        "seq": seq,
        "ts": "2026-08-28T08:00:00Z",
        "from": sender,
        "text": "x",
        **extra,
    }


def test_lobby_last_sequence_ignores_unknown_nested_sequence_fields():
    body = json.dumps(
        {
            "messages": [message(1, "server")],
            "unknown": {"seq": 999},
        }
    )

    assert collect.extract_last_seq(body, "/r/lobby") == 1


@pytest.mark.parametrize(
    ("room", "path"),
    (
        pytest.param("other-room", "/r/target-room?format=json", id="misrouted"),
        pytest.param("bad room", "/r/bad%20room?format=json", id="invalid-name"),
    ),
)
def test_room_messages_bind_a_valid_envelope_to_the_request_target(room, path):
    with pytest.raises(CollectionError, match="room envelope"):
        parse_room_messages(json.dumps({"room": room, "messages": []}), path)


def test_room_messages_keep_legacy_envelopes_without_a_room_field():
    assert parse_room_messages('{"messages":[]}', "/r/target-room?format=json") == []


@pytest.mark.parametrize(
    "messages",
    (
        pytest.param([None], id="non-object"),
        pytest.param([{"seq": 1, "from": "server"}], id="missing-timestamp"),
        pytest.param([message(-1, "server")], id="negative-sequence"),
        pytest.param([message(2**63, "server")], id="oversized-sequence"),
        pytest.param(
            [message(1, "server"), message(1, "server")],
            id="duplicate-sequence",
        ),
        pytest.param([message(1, "server", ts="not-a-time")], id="bad-timestamp"),
        pytest.param(
            [message(1, "server", ts="0001-01-01T00:00:00+14:00")],
            id="utc-underflow",
        ),
        pytest.param([message(1, 7)], id="non-string-sender"),
    ),
)
def test_room_messages_reject_invalid_required_fields(messages):
    with pytest.raises(CollectionError) as caught:
        parse_room_messages(room_body(*messages), "/r/test")

    assert caught.value.outcome == "invalid_response"
    assert caught.value.path == "/r/{room}"


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


def test_collector_version_is_bumped_for_lifecycle_sampling():
    assert tuple(map(int, COLLECTOR_VERSION.split("."))) > (2, 11, 1)
    assert collect.SIGNER_STATE_VERSION == 6
    assert collect.TICK_REVISIT_DEADLINE_SECONDS == 300


@pytest.mark.parametrize("timeout", (float("nan"), float("inf"), -float("inf")))
def test_client_rejects_a_nonfinite_timeout(timeout):
    with pytest.raises(ValueError, match="timeout"):
        collect.Client("https://example.invalid", timeout, 0)


@pytest.mark.parametrize("retries", (-1, collect.MAX_RETRIES + 1, True))
def test_client_rejects_an_invalid_retry_count(retries):
    with pytest.raises(ValueError, match="retries"):
        collect.Client("https://example.invalid", 1, retries)


def test_client_retries_an_http_protocol_error_while_reading_the_body(
    tmp_path,
    monkeypatch,
):
    class Response:
        def __init__(self, status, body=None):
            self.status = status
            self.body = io.BytesIO(body) if body is not None else None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            return False

        def read1(self, size=-1):
            if self.body is None:
                raise collect.http.client.IncompleteRead(b"partial", 10)
            return self.body.read(size)

    responses = iter((Response(206), Response(200, b'{"ok":true}')))
    monkeypatch.setattr(
        collect, "open_origin", lambda request, timeout: next(responses)
    )
    monkeypatch.setattr(collect.time, "sleep", lambda delay: None)

    with collect.TelemetryStore(tmp_path / "telemetry.sqlite3") as telemetry:
        cycle_id = telemetry.start_cycle("collector", "2026-08-30T10:00:00Z")
        client = collect.Client(
            "https://example.invalid",
            1,
            1,
            telemetry=telemetry,
            cycle_id=cycle_id,
        )

        assert client.get("/rooms?format=json") == '{"ok":true}'
        assert telemetry.connection.execute(
            "SELECT attempt, outcome, http_status FROM request_attempts ORDER BY id"
        ).fetchall() == [(1, "transport_error", None), (2, "success", 200)]


@pytest.mark.parametrize("pace", (float("nan"), float("inf"), -float("inf")))
def test_census_rejects_a_nonfinite_pace_before_reading(tmp_path, pace):
    class MustNotReadClient:
        def get(self, path, deadline=None):
            raise AssertionError(f"invalid pace still read {path}")

    with pytest.raises(ValueError, match="pace"):
        collect.run_census(MustNotReadClient(), tmp_path / "census.json", pace)


@pytest.mark.parametrize(
    ("option", "value"),
    (
        pytest.param("--interval", "nan", id="interval-nan"),
        pytest.param("--interval", "inf", id="interval-infinity"),
        pytest.param("--timeout", "nan", id="timeout-nan"),
        pytest.param("--census-pace", "inf", id="pace-infinity"),
    ),
)
def test_main_rejects_nonfinite_numeric_configuration(
    tmp_path,
    monkeypatch,
    option,
    value,
):
    def client_must_not_start(*args, **kwargs):
        raise AssertionError("invalid configuration reached the client")

    monkeypatch.setattr(collect, "Client", client_must_not_start)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "collect.py",
            "--base-url",
            "https://example.invalid",
            "--output",
            str(tmp_path / "ticks.jsonl"),
            option,
            value,
            "--once",
        ],
    )

    with pytest.raises(SystemExit, match="finite"):
        collect.main()


@pytest.mark.parametrize(
    ("option", "value", "message"),
    (
        pytest.param("--retries", "1024", "retries", id="excessive-retries"),
        pytest.param(
            "--signer-cap",
            str(collect.SQLITE_INTEGER_MAX + 1),
            "SQLite",
            id="oversized-signer-cap",
        ),
    ),
)
def test_main_rejects_unbounded_integer_configuration(
    tmp_path,
    monkeypatch,
    option,
    value,
    message,
):
    def client_must_not_start(*args, **kwargs):
        raise AssertionError("invalid configuration reached the client")

    monkeypatch.setattr(collect, "Client", client_must_not_start)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "collect.py",
            "--base-url",
            "https://example.invalid",
            "--output",
            str(tmp_path / "ticks.jsonl"),
            option,
            value,
            "--once",
        ],
    )

    with pytest.raises(SystemExit, match=message):
        collect.main()


@pytest.mark.parametrize("section", ("rooms", "notes"))
@pytest.mark.parametrize("counter", ("total", "capacity", "bytes"))
@pytest.mark.parametrize("invalid", ("1", True, -1))
def test_rooms_json_rejects_invalid_counter_values(section, counter, invalid):
    payload = {
        "total": 1,
        "capacity": 100,
        "bytes": 10,
        "notes": {"total": 2, "capacity": 200, "bytes": 20},
        "rooms": [],
    }
    counters = payload if section == "rooms" else payload["notes"]
    counters[counter] = invalid

    with pytest.raises(CollectionError):
        collect.parse_rooms(json.dumps(payload))


@pytest.mark.parametrize("counter", ("capacity", "bytes"))
def test_rooms_json_rejects_missing_room_counters(counter):
    payload = {
        "total": 1,
        "capacity": 100,
        "bytes": 10,
        "notes": {"total": 2, "capacity": 200, "bytes": 20},
        "rooms": [],
    }
    del payload[counter]

    with pytest.raises(CollectionError):
        collect.parse_rooms(json.dumps(payload))


@pytest.mark.parametrize(
    ("section", "counter"),
    (
        pytest.param("rooms", "total", id="room-total"),
        pytest.param("rooms", "capacity", id="room-capacity"),
        pytest.param("notes", "capacity", id="note-capacity"),
    ),
)
def test_rooms_json_rejects_zero_required_population_counters(section, counter):
    payload = {
        "total": 1,
        "capacity": 100,
        "bytes": 10,
        "notes": {"total": 0, "capacity": 200, "bytes": 20},
        "rooms": [],
    }
    counters = payload if section == "rooms" else payload["notes"]
    counters[counter] = 0

    with pytest.raises(CollectionError, match="positive"):
        collect.parse_rooms(json.dumps(payload))


@pytest.mark.parametrize(
    "body",
    (
        pytest.param(
            "# 0 of 0 rooms (cap 100, 0 of 100 stored), newest first\n"
            "# notes 0 of 100 (0 total, 10 per namespace)\n",
            id="room-total",
        ),
        pytest.param(
            "# 0 of 1 rooms (cap 0, 0 of 100 stored), newest first\n"
            "# notes 0 of 100 (0 total, 10 per namespace)\n",
            id="room-capacity",
        ),
        pytest.param(
            "# 0 of 1 rooms (cap 100, 0 of 100 stored), newest first\n"
            "# notes 0 of 0 (0 total, 10 per namespace)\n",
            id="note-capacity",
        ),
    ),
)
def test_plaintext_rooms_reject_zero_required_population_counters(body):
    with pytest.raises(CollectionError, match="positive"):
        collect.parse_rooms(body)


@pytest.mark.parametrize(
    "body",
    (
        pytest.param(
            json.dumps(
                {
                    "total": 0,
                    "capacity": 100,
                    "bytes": 10,
                    "notes": {"total": 0, "capacity": 200, "bytes": 20},
                    "rooms": [{"name": "room-a", "seq": 1, "idle_seconds": 0}],
                }
            ),
            id="json-rows-exceed-total",
        ),
        pytest.param(
            "# 1 of 0 rooms (cap 100, 10 of 100 stored), newest first\n"
            "# notes 0 of 200 (20 total, 10 per namespace)\n"
            "room-a seq=1 idle=0s\n",
            id="plaintext-shown-exceeds-total",
        ),
        pytest.param(
            json.dumps(
                {
                    "total": 2,
                    "capacity": 100,
                    "bytes": 10,
                    "notes": {"total": 0, "capacity": 200, "bytes": 20},
                    "rooms": [
                        {"name": "room-a", "seq": 1, "idle_seconds": 0},
                        {"name": "room-a", "seq": 1, "idle_seconds": 0},
                    ],
                }
            ),
            id="json-duplicate-room",
        ),
        pytest.param(
            "# 2 of 2 rooms (cap 100, 10 of 100 stored), newest first\n"
            "# notes 0 of 200 (20 total, 10 per namespace)\n"
            "room-a seq=1 idle=0s\n"
            "room-a seq=1 idle=0s\n",
            id="plaintext-duplicate-room",
        ),
    ),
)
def test_rooms_reject_contradictory_totals_and_duplicate_names(body):
    with pytest.raises(CollectionError):
        collect.parse_rooms(body)


def test_invalid_rooms_counter_reclassifies_the_successful_http_attempt(
    tmp_path,
    monkeypatch,
):
    class Response:
        status = 200

        def __init__(self):
            self.body = io.BytesIO(
                json.dumps(
                    {
                        "total": 1,
                        "capacity": "invalid",
                        "bytes": 10,
                        "notes": {"total": 2, "capacity": 200, "bytes": 20},
                        "rooms": [],
                    }
                ).encode()
            )

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            return False

        def read(self, size=-1):
            return self.body.read(size)

    monkeypatch.setattr(
        collect,
        "open_origin",
        lambda request, timeout: Response(),
    )
    with collect.TelemetryStore(tmp_path / "telemetry.sqlite3") as telemetry:
        cycle_id = telemetry.start_cycle("collector", "2026-08-30T10:00:00Z")
        client = collect.Client(
            "https://example.invalid",
            1,
            0,
            telemetry=telemetry,
            cycle_id=cycle_id,
        )
        path = "/rooms?format=json&limit=200"
        body = client.get(path)

        with pytest.raises(CollectionError) as caught:
            collect.parse_collected_response(
                client,
                path,
                collect.parse_rooms,
                body,
            )

        assert caught.value.outcome == "invalid_response"
        assert caught.value.path == "/rooms"
        assert telemetry.connection.execute(
            "SELECT route, outcome, http_status FROM request_attempts"
        ).fetchone() == ("/rooms", "invalid_response", 200)


def test_census_continues_after_failures_and_retries_only_missing_shards(tmp_path):
    state_path = tmp_path / "identity-census-state.json"
    attempts = {}

    class IntermittentClient:
        def get(self, path, deadline=None):
            shard = path.rsplit("/", 1)[-1]
            attempts[shard] = attempts.get(shard, 0) + 1
            if shard == "did-01" or (shard == "did-00" and attempts[shard] == 1):
                raise CollectionError(f"GET {path} failed with HTTP 503")
            return '["00000000000000"]'

    total, started_at, census_run = collect.run_census(
        IntermittentClient(),
        state_path,
        0,
    )

    saved = json.loads(state_path.read_text(encoding="utf-8"))
    assert total is None
    assert started_at == saved["started_at"]
    assert len(saved["counts"]) == 255
    assert "did-00" in saved["counts"]
    assert "did-01" not in saved["counts"]
    assert "did-ff" in saved["counts"]
    assert attempts["did-00"] == 2
    assert attempts["did-01"] == collect.CENSUS_MAX_PASSES
    assert attempts["did-ff"] == 1
    assert census_run == {
        "walk_started_at": started_at,
        "shards_outstanding_at_start": 256,
        "shards_collected": 255,
        "shards_outstanding": 1,
        "passes_attempted": collect.CENSUS_MAX_PASSES,
        "maximum_passes": collect.CENSUS_MAX_PASSES,
        "deadline_seconds": collect.CENSUS_DEADLINE_SECONDS,
        "shard_reads_attempted": 261,
        "shard_read_failures": 6,
        "failure_causes": {"http_503": 6},
        "stop_reason": "maximum_passes",
    }


def test_census_pacing_handles_deadline_crossed_between_clock_reads(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(collect, "exclusive_state_lock", lambda *_args: nullcontext())
    clock = iter((0.0, 1.0, 1799.9, 1800.1))
    monkeypatch.setattr(collect.time, "monotonic", lambda: next(clock))
    sleeps = []

    def reject_negative_sleep(delay):
        sleeps.append(delay)
        if delay < 0:
            raise ValueError("sleep length must be non-negative")

    monkeypatch.setattr(collect.time, "sleep", reject_negative_sleep)

    class OneShardClient:
        def get(self, path, deadline=None):
            assert path == "/kv/did-00"
            return "[]"

    total, _, census_run = collect.run_census(
        OneShardClient(),
        tmp_path / "identity-census-state.json",
        0.25,
    )

    assert total is None
    assert sleeps == []
    assert census_run["shards_collected"] == 1
    assert census_run["stop_reason"] == "deadline"


def test_census_rejects_a_final_shard_that_overflows_the_aggregate(tmp_path):
    state_path = tmp_path / "identity-census-state.json"
    counts = {f"did-{index:02x}": 1 for index in range(255)}
    counts["did-00"] = collect.SQLITE_INTEGER_MAX - 255
    state_path.write_text(
        json.dumps(
            {
                "version": 1,
                "started_at": "2026-08-30T00:00:00Z",
                "counts": counts,
            }
        ),
        encoding="utf-8",
    )

    class FinalShardClient:
        def get(self, path, deadline=None):
            assert path == "/kv/did-ff"
            return '["00000000000000","00000000000001"]'

    total, _, census_run = collect.run_census(FinalShardClient(), state_path, 0)

    saved = json.loads(state_path.read_text(encoding="utf-8"))
    assert total is None
    assert "did-ff" not in saved["counts"]
    assert census_run["stop_reason"] == "maximum_passes"
    assert census_run["shard_read_failures"] == collect.CENSUS_MAX_PASSES
    assert census_run["failure_causes"] == {
        "invalid_shard_response": collect.CENSUS_MAX_PASSES
    }


@pytest.mark.parametrize(
    ("payload", "decoder_recursion"),
    (
        pytest.param(
            f'{{"version":1,"counts":{{"did-00":{HUGE_JSON_INTEGER}}}}}',
            False,
            id="integer-limit",
        ),
        pytest.param(DEEPLY_NESTED_JSON, True, id="recursion-limit"),
    ),
)
def test_census_state_json_parser_failures_are_collection_errors(
    tmp_path,
    payload,
    decoder_recursion,
):
    path = tmp_path / "census.json"
    path.write_text(payload, encoding="utf-8")
    decoder = (
        mock.patch.object(json, "loads", side_effect=RecursionError)
        if decoder_recursion
        else nullcontext()
    )

    with decoder, pytest.raises(CollectionError, match="cannot read census state"):
        collect.load_census_state(path)


@pytest.mark.parametrize(
    "payload",
    (
        pytest.param("null", id="null"),
        pytest.param("[]", id="array"),
        pytest.param('{"version":1,"counts":{}}', id="missing-started-at"),
        pytest.param(
            '{"version":1,"started_at":"not-a-timestamp","counts":{}}',
            id="invalid-started-at",
        ),
        pytest.param(
            '{"version":1,"started_at":"2026-08-30T00:00:00Z","counts":{},"unexpected":true}',
            id="unexpected-field",
        ),
    ),
)
def test_census_state_rejects_an_invalid_top_level_shape(tmp_path, payload):
    path = tmp_path / "census.json"
    path.write_text(payload, encoding="utf-8")

    with pytest.raises(CollectionError, match="census state"):
        collect.load_census_state(path)


@pytest.mark.parametrize(
    "counts_json",
    (
        pytest.param(f'{{"did-00":{"1" * 4_300}}}', id="oversized-shard"),
        pytest.param(
            f'{{"did-00":{collect.SQLITE_INTEGER_MAX},"did-01":1}}',
            id="oversized-aggregate",
        ),
    ),
)
def test_census_state_rejects_counts_outside_integer_storage(
    tmp_path,
    counts_json,
):
    path = tmp_path / "census.json"
    path.write_text(
        f'{{"version":1,"started_at":"2026-08-30T00:00:00Z","counts":{counts_json}}}',
        encoding="utf-8",
    )

    with pytest.raises(CollectionError, match="census state contains"):
        collect.load_census_state(path)


@pytest.mark.parametrize(
    ("payload", "decoder_recursion"),
    (
        pytest.param(
            f'{{"version":5,"tracked_cap":{HUGE_JSON_INTEGER}}}',
            False,
            id="integer-limit",
        ),
        pytest.param(DEEPLY_NESTED_JSON, True, id="recursion-limit"),
    ),
)
def test_legacy_signer_metadata_parser_failures_are_collection_errors(
    tmp_path,
    payload,
    decoder_recursion,
):
    path = tmp_path / "signers.sqlite3"
    connection = sqlite3.connect(path)
    connection.execute(
        "CREATE TABLE signer_metadata (singleton INTEGER PRIMARY KEY, state_json TEXT NOT NULL)"
    )
    connection.execute(
        "INSERT INTO signer_metadata (singleton, state_json) VALUES (1, ?)",
        (payload,),
    )
    connection.execute("PRAGMA user_version = 5")
    connection.commit()
    connection.close()

    decoder = (
        mock.patch.object(json, "loads", side_effect=RecursionError)
        if decoder_recursion
        else nullcontext()
    )
    with decoder, pytest.raises(CollectionError, match="invalid metadata JSON"):
        connect_signer_database(path)


@pytest.mark.parametrize(
    ("payload", "decoder_recursion"),
    (
        pytest.param(
            f'{{"version":6,"tracked_cap":{HUGE_JSON_INTEGER}}}',
            False,
            id="integer-limit",
        ),
        pytest.param(DEEPLY_NESTED_JSON, True, id="recursion-limit"),
    ),
)
def test_current_signer_metadata_parser_failures_are_collection_errors(
    signer_store,
    tmp_path,
    payload,
    decoder_recursion,
):
    state_path = tmp_path / "signers.json"
    state = new_signer_state(100)
    collect.write_signer_metadata(signer_store, state)
    signer_store.execute(
        "UPDATE signer_metadata SET state_json = ? WHERE singleton = 1",
        (payload,),
    )
    signer_store.commit()

    decoder = (
        mock.patch.object(json, "loads", side_effect=RecursionError)
        if decoder_recursion
        else nullcontext()
    )
    with decoder, pytest.raises(CollectionError, match="invalid metadata JSON"):
        collect.load_signer_state(state_path, 100, signer_store)


def test_signer_metadata_rejects_an_invalid_last_updated_timestamp(
    signer_store,
    tmp_path,
):
    state_path = tmp_path / "signers.json"
    state = new_signer_state(100)
    collect.write_signer_metadata(signer_store, state)
    state["last_updated"] = "\ud800"
    signer_store.execute(
        "UPDATE signer_metadata SET state_json = ? WHERE singleton = 1",
        (json.dumps(state),),
    )
    signer_store.commit()

    with pytest.raises(CollectionError, match="signer metadata"):
        collect.load_signer_state(state_path, 100, signer_store)


@pytest.mark.parametrize(
    "field",
    (
        "collection_started",
        "persistence_started_at",
        "persistence_reset_at",
        "persistence_first_utc_date",
        "persistence_last_utc_date",
        "persistence_collection_utc_dates_count",
    ),
)
def test_signer_metadata_requires_every_persistence_field(field):
    state = new_signer_state(100)
    del state[field]

    with pytest.raises(CollectionError, match="signer metadata"):
        collect.validate_signer_metadata(state)


@pytest.mark.parametrize(
    ("field", "value"),
    (
        pytest.param("collection_started", None, id="collection-started-type"),
        pytest.param(
            "persistence_started_at", "not-a-timestamp", id="persistence-started"
        ),
        pytest.param("persistence_reset_at", 1, id="persistence-reset-type"),
        pytest.param("persistence_first_utc_date", "2026-02-30", id="first-date"),
        pytest.param("persistence_last_utc_date", 1, id="last-date-type"),
        pytest.param(
            "persistence_collection_utc_dates_count", True, id="date-count-bool"
        ),
        pytest.param(
            "persistence_collection_utc_dates_count", -1, id="date-count-negative"
        ),
        pytest.param(
            "persistence_collection_utc_dates_count",
            collect.SQLITE_INTEGER_MAX + 1,
            id="date-count-overflow",
        ),
    ),
)
def test_signer_metadata_rejects_invalid_persistence_fields(field, value):
    state = new_signer_state(100)
    state[field] = value

    with pytest.raises(CollectionError, match="signer metadata"):
        collect.validate_signer_metadata(state)


def test_signer_metadata_rejects_inconsistent_persistence_dates():
    state = new_signer_state(100)
    state["persistence_collection_utc_dates_count"] = 1

    with pytest.raises(CollectionError, match="signer metadata"):
        collect.validate_signer_metadata(state)


@pytest.mark.parametrize(
    "census",
    (
        pytest.param("malformed", id="not-an-object"),
        pytest.param({}, id="missing-fields"),
        pytest.param(
            {
                "total": collect.SQLITE_INTEGER_MAX + 1,
                "started_at": "2026-08-30T00:00:00Z",
                "completed_at": "2026-08-30T00:01:00Z",
            },
            id="total-overflow",
        ),
        pytest.param(
            {
                "total": 1,
                "started_at": "2026-08-30T00:01:00Z",
                "completed_at": "2026-08-30T00:00:00Z",
            },
            id="reversed-timestamps",
        ),
    ),
)
def test_signer_metadata_rejects_invalid_census(census):
    state = new_signer_state(100)
    state["census"] = census

    with pytest.raises(CollectionError, match="signer metadata"):
        collect.validate_signer_metadata(state)


def test_signer_metadata_rejects_invalid_selector_frame_names():
    state = new_signer_state(100)
    state["selector_frame"] = ["\ud800"]

    with pytest.raises(CollectionError, match="signer metadata"):
        collect.validate_signer_metadata(state)


def test_room_selector_refuses_to_increment_past_the_integer_contract():
    state = new_signer_state(100)
    state["selector_epoch"] = collect.SQLITE_INTEGER_MAX

    with pytest.raises(CollectionError, match="selector epoch"):
        collect.room_sample_names([], state)


@pytest.mark.parametrize(
    "payload",
    (
        pytest.param(
            f'{{"version":6,"tracked_cap":{HUGE_JSON_INTEGER}}}',
            id="integer-limit",
        ),
        pytest.param(DEEPLY_NESTED_JSON, id="recursion-limit"),
    ),
)
def test_unparseable_signer_metadata_mirror_is_repaired(
    signer_store,
    tmp_path,
    payload,
):
    state_path = tmp_path / "signers.json"
    state = new_signer_state(100)
    collect.write_signer_metadata(signer_store, state)
    signer_store.commit()
    state_path.write_text(payload, encoding="utf-8")

    loaded = collect.load_signer_state(state_path, 100, signer_store)

    assert loaded == state
    assert json.loads(state_path.read_text(encoding="utf-8")) == state


@pytest.mark.parametrize(
    "payload",
    (
        pytest.param(f"[{HUGE_JSON_INTEGER}]", id="integer-limit"),
        pytest.param(DEEPLY_NESTED_JSON, id="recursion-limit"),
    ),
)
def test_stored_signer_rooms_parser_failures_are_collection_errors(
    signer_store,
    payload,
):
    suffix = DID.removeprefix("did:key:z6Mk")
    insert_record(signer_store, suffix, 1, 1, ["room-a"], 0)
    signer_store.execute(
        "UPDATE signer_dids SET rooms_json = ? WHERE did = ?",
        (payload, suffix),
    )
    messages = parse_room_messages(
        room_body(message(1, DID, nonce="1")),
        "/r/test",
    )

    with pytest.raises(CollectionError, match="invalid rooms"):
        update_signer_state(
            signer_store,
            new_signer_state(100),
            [("room-a", messages)],
            "2026-08-30T08:00:00Z",
        )


def test_census_completes_missing_shards_then_resets_the_next_walk(
    tmp_path,
    monkeypatch,
):
    state_path = tmp_path / "identity-census-state.json"
    previous_started_at = "2026-08-29T10:23:00Z"
    collect.save_census_state(
        state_path,
        {
            "version": 1,
            "started_at": previous_started_at,
            "counts": {f"did-{index:02x}": 1 for index in range(256) if index != 7},
        },
    )

    class SuccessfulClient:
        def __init__(self, body):
            self.body = body
            self.calls = []

        def get(self, path, deadline=None):
            self.calls.append(path)
            return self.body

    finishing_client = SuccessfulClient('["00000000000000"]')
    total, started_at, census_run = collect.run_census(
        finishing_client,
        state_path,
        0,
    )

    assert total == 256
    assert started_at == previous_started_at
    assert finishing_client.calls == ["/kv/did-07"]
    assert census_run["shards_outstanding_at_start"] == 1
    assert census_run["shards_collected"] == 256
    assert census_run["shards_outstanding"] == 0
    assert census_run["shard_reads_attempted"] == 1
    assert census_run["shard_read_failures"] == 0
    assert census_run["failure_causes"] == {}
    assert census_run["stop_reason"] == "complete"

    completed_state = json.loads(state_path.read_text(encoding="utf-8"))
    assert completed_state["started_at"] == previous_started_at
    assert len(completed_state["counts"]) == 256
    assert completed_state["ledger_published"] is False

    collect.acknowledge_census_publication(
        state_path,
        previous_started_at,
        256,
    )
    acknowledged_state = json.loads(state_path.read_text(encoding="utf-8"))
    assert acknowledged_state["ledger_published"] is True

    next_started_at = "2026-08-30T16:23:00Z"
    monkeypatch.setattr(collect, "utc_now", lambda: next_started_at)
    next_client = SuccessfulClient('["00000000000000","00000000000001"]')
    next_total, reset_started_at, next_run = collect.run_census(
        next_client,
        state_path,
        0,
    )

    reset_state = json.loads(state_path.read_text(encoding="utf-8"))
    assert next_total == 512
    assert reset_started_at == next_started_at
    assert len(next_client.calls) == 256
    assert reset_state["started_at"] == next_started_at
    assert len(reset_state["counts"]) == 256
    assert next_run["shards_outstanding_at_start"] == 256
    assert next_run["shards_outstanding"] == 0
    assert next_run["stop_reason"] == "complete"


def test_completed_census_is_replayed_until_its_ledger_append_is_acknowledged(
    tmp_path,
):
    state_path = tmp_path / "identity-census-state.json"
    started_at = "2026-08-30T00:00:00Z"
    collect.save_census_state(
        state_path,
        {
            "version": 1,
            "started_at": started_at,
            "counts": {f"did-{index:02x}": 1 for index in range(255)},
        },
    )

    class FinishingClient:
        def get(self, path, deadline=None):
            assert path == "/kv/did-ff"
            return '["00000000000000"]'

    first_total, first_started, _ = collect.run_census(FinishingClient(), state_path, 0)

    class MustNotReadClient:
        def get(self, path, deadline=None):
            raise AssertionError(f"completed unpublished census reread {path}")

    replay_total, replay_started, replay_run = collect.run_census(
        MustNotReadClient(), state_path, 0
    )

    assert (first_total, first_started) == (256, started_at)
    assert (replay_total, replay_started) == (256, started_at)
    assert replay_run["shard_reads_attempted"] == 0
    assert replay_run["stop_reason"] == "complete"
    assert (
        json.loads(state_path.read_text(encoding="utf-8"))["ledger_published"] is False
    )


def test_atomic_json_is_fsynced_before_publication(tmp_path, monkeypatch):
    path = tmp_path / "state.json"
    events = []
    real_replace = collect.os.replace

    monkeypatch.setattr(collect.os, "fsync", lambda descriptor: events.append("fsync"))

    def checked_replace(source, destination):
        assert events == ["fsync"]
        events.append("replace")
        real_replace(source, destination)

    monkeypatch.setattr(collect.os, "replace", checked_replace)

    collect.save_atomic_json(path, {"value": 1})

    assert events[:2] == ["fsync", "replace"]
    assert json.loads(path.read_text(encoding="utf-8")) == {"value": 1}
    assert not path.with_name(path.name + ".tmp").exists()


def test_incomplete_census_tick_publishes_progress_but_no_count(
    tmp_path,
    monkeypatch,
):
    tick_ts = "2026-08-30T16:44:00Z"
    monkeypatch.setattr(collect, "utc_now", lambda: tick_ts)

    class TickClient:
        def get(self, path, deadline=None):
            if path == "/rooms?format=json&limit=200":
                return json.dumps(
                    {
                        "total": 1,
                        "capacity": 100,
                        "bytes": 0,
                        "notes": {
                            "total": 0,
                            "capacity": 100,
                            "bytes": 0,
                        },
                        "rooms": [
                            {"name": "lobby", "seq": 1, "idle": 0},
                        ],
                    }
                )
            if path == "/r/lobby?format=json&limit=200":
                return room_body(message(1, "server"))
            if path == "/r/events?format=json&limit=200":
                return json.dumps(
                    {
                        "room": "events",
                        "messages": [
                            {
                                "seq": 1,
                                "ts": tick_ts,
                                "from": "server",
                                "text": "created census-test-room",
                            }
                        ],
                    }
                )
            raise AssertionError(f"unexpected read: {path}")

    census_run = {
        "walk_started_at": "2026-08-30T10:35:00Z",
        "shards_outstanding_at_start": 256,
        "shards_collected": 255,
        "shards_outstanding": 1,
        "passes_attempted": collect.CENSUS_MAX_PASSES,
        "maximum_passes": collect.CENSUS_MAX_PASSES,
        "deadline_seconds": collect.CENSUS_DEADLINE_SECONDS,
        "shard_reads_attempted": 260,
        "shard_read_failures": 5,
        "failure_causes": {"http_503": 5},
        "stop_reason": "maximum_passes",
    }
    tick = collect.collect_tick(
        TickClient(),
        tmp_path / "signers.json",
        100,
        identity_total=None,
        census_started=census_run["walk_started_at"],
        census_run=census_run,
    )

    assert tick["identity_total"] is None
    assert tick["identity_census_started"] == census_run["walk_started_at"]
    assert tick["identity_census_run"] == census_run
    assert tick["signer_funnel"]["well_formed_did_notes"] is None


def test_hash_chain_declares_genesis_after_unchained_prefix_and_verifies(tmp_path):
    path = tmp_path / "ticks.jsonl"
    path.write_text(
        '{"legacy":"first"}\n{"legacy":"middle"}\n{"legacy":"second"}\n',
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
    assert (
        genesis["ledger_chain"]["tick_sha256"]
        == hashlib.sha256(canonical_tick_hash_bytes(genesis)).hexdigest()
    )
    assert (
        successor["ledger_chain"]["previous_sha256"]
        == hashlib.sha256(canonical_tick_bytes(genesis)).hexdigest()
    )

    result = verify_ledger(path)
    assert result["ok"] is True
    assert result["ticks"] == 5
    assert result["unchained_prefix_ticks"] == 3
    assert result["genesis_line"] == 4
    assert result["genesis_ts"] == "2026-08-30T10:00:00Z"
    assert result["first_break"] is None


@pytest.mark.parametrize(
    "line",
    (
        pytest.param("{malformed legacy line}", id="malformed-json"),
        pytest.param('{"duplicate":1,"duplicate":2}', id="duplicate-key"),
        pytest.param(DEEPLY_NESTED_JSON, id="recursive-json"),
    ),
)
def test_hash_chain_rejects_invalid_json_before_genesis(tmp_path, line):
    path = tmp_path / "ticks.jsonl"
    path.write_text(line + "\n", encoding="utf-8")

    result = verify_ledger(path)

    assert result["ok"] is False
    assert result["ticks"] == 1
    assert result["unchained_prefix_ticks"] == 0
    assert result["genesis_line"] is None
    assert result["first_break"] == 1
    assert "parse" in result["message"]


def test_hash_chain_reports_recursive_json_after_genesis_as_first_break(tmp_path):
    path = tmp_path / "ticks.jsonl"
    genesis = {
        "ts": "2026-08-30T10:00:00Z",
        "ledger_chain": {
            "version": 1,
            "previous_sha256": None,
            "tick_sha256": "",
        },
    }
    genesis["ledger_chain"]["tick_sha256"] = hashlib.sha256(
        canonical_tick_hash_bytes(genesis)
    ).hexdigest()
    path.write_bytes(
        canonical_tick_bytes(genesis) + b"\n" + DEEPLY_NESTED_JSON.encode() + b"\n"
    )

    result = verify_ledger(path)

    assert result["ok"] is False
    assert result["ticks"] == 2
    assert result["unchained_prefix_ticks"] == 0
    assert result["genesis_line"] == 1
    assert result["first_break"] == 2
    assert "parse canonically" in result["message"]


def test_hash_chain_reports_lone_surrogate_as_canonicalization_failure(tmp_path):
    path = tmp_path / "ticks.jsonl"
    line = (
        r'{"ledger_chain":{"version":1,"previous_sha256":null,"tick_sha256":"'
        + "0" * 64
        + r'"},"value":"\ud800"}'
    )
    path.write_bytes((line + "\n").encode("ascii"))

    result = verify_ledger(path)

    assert result["ok"] is False
    assert result["ticks"] == 1
    assert result["unchained_prefix_ticks"] == 0
    assert result["genesis_line"] is None
    assert result["first_break"] == 1
    assert "canonical" in result["message"]


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


def created_event(seq, ts, name):
    return {
        "seq": seq,
        "ts": ts,
        "name": name,
        "classes": [],
        "primary_class": "human_or_other",
        "base_name": name,
    }


def test_created_room_enters_the_sqlite_ledger_exactly_once(signer_store):
    event = created_event(10, "2026-08-30T08:00:00Z", "attacker-controlled-room")

    assert (
        record_created_rooms(
            signer_store,
            [event],
            "2026-08-30T08:00:01Z",
        )
        == 1
    )
    assert (
        record_created_rooms(
            signer_store,
            [event],
            "2026-08-30T08:04:00Z",
        )
        == 0
    )
    assert signer_store.execute("SELECT COUNT(*) FROM room_ledger").fetchone() == (1,)
    assert signer_store.execute("SELECT COUNT(*) FROM room_revisits").fetchone() == (3,)


def test_created_room_preserves_fractional_revisit_schedule_precision(signer_store):
    event = created_event(
        10,
        "2026-08-30T08:00:00.123456Z",
        "fractional-room",
    )

    record_created_rooms(signer_store, [event], "2026-08-30T08:00:01Z")

    assert signer_store.execute(
        "SELECT created_at FROM room_ledger WHERE name = 'fractional-room'"
    ).fetchone() == ("2026-08-30T08:00:00.123456Z",)
    assert signer_store.execute(
        "SELECT due_at FROM room_revisits WHERE room_created_seq = 10 AND stage_seconds = 300"
    ).fetchone() == ("2026-08-30T08:05:00.123456Z",)
    before_due = select_revisits(
        signer_store,
        "2026-08-30T08:05:00.123455Z",
    )
    assert before_due["due_this_tick"] == 0
    assert before_due["superseded_due_this_tick"] == 0
    assert before_due["selected"] == []

    at_due = select_revisits(
        signer_store,
        "2026-08-30T08:05:00.123456Z",
    )
    assert at_due["due_this_tick"] == 1
    assert at_due["superseded_due_this_tick"] == 0
    assert [row["created_seq"] for row in at_due["selected"]] == [10]


def test_created_room_rejects_an_event_after_its_observation(signer_store):
    event = created_event(
        10,
        "2026-08-30T08:00:00.000001Z",
        "future-room",
    )

    with pytest.raises(CollectionError, match="after its observation"):
        record_created_rooms(
            signer_store,
            [event],
            "2026-08-30T08:00:00Z",
        )


def test_recreated_room_gets_an_independent_generation_and_schedule(signer_store):
    first = created_event(
        10,
        "2026-08-30T08:00:00.000001Z",
        "reused-room",
    )
    second = created_event(
        999,
        "2026-08-30T09:00:00.000002Z",
        "reused-room",
    )

    assert record_created_rooms(signer_store, [first], "2026-08-30T08:00:01Z") == 1
    assert record_created_rooms(signer_store, [second], "2026-08-30T09:00:01Z") == 1

    assert signer_store.execute(
        "SELECT created_seq, created_at FROM room_ledger "
        "WHERE name = 'reused-room' ORDER BY created_seq"
    ).fetchall() == [
        (10, "2026-08-30T08:00:00.000001Z"),
        (999, "2026-08-30T09:00:00.000002Z"),
    ]
    assert signer_store.execute(
        "SELECT room_created_seq, COUNT(*) FROM room_revisits "
        "GROUP BY room_created_seq ORDER BY room_created_seq"
    ).fetchall() == [(10, 3), (999, 3)]

    selection = select_revisits(
        signer_store,
        "2026-08-30T08:05:00.000001Z",
    )
    assert selection["due_this_tick"] == 1
    assert selection["superseded_due_this_tick"] == 0
    assert selection["selected"] == [
        {
            "created_seq": 10,
            "name": "reused-room",
            "stage_seconds": 300,
            "created_at": "2026-08-30T08:00:00.000001Z",
        }
    ]


@pytest.mark.parametrize(
    "conflict",
    (
        pytest.param(
            created_event(10, "2026-08-30T08:00:00Z", "different-room"),
            id="sequence-reused-by-another-room",
        ),
        pytest.param(
            created_event(
                10,
                "2026-08-30T08:00:01Z",
                "attacker-controlled-room",
            ),
            id="room-replayed-with-another-timestamp",
        ),
    ),
)
def test_created_room_rejects_conflicting_origin_history(signer_store, conflict):
    original = created_event(
        10,
        "2026-08-30T08:00:00Z",
        "attacker-controlled-room",
    )
    record_created_rooms(signer_store, [original], "2026-08-30T08:00:01Z")

    with pytest.raises(CollectionError, match="conflicting room-creation event"):
        record_created_rooms(signer_store, [conflict], "2026-08-30T08:00:02Z")


def test_created_room_rejects_an_incomplete_persisted_revisit_schedule(signer_store):
    event = created_event(
        10,
        "2026-08-30T08:00:00Z",
        "attacker-controlled-room",
    )
    record_created_rooms(signer_store, [event], "2026-08-30T08:00:01Z")
    signer_store.execute(
        "DELETE FROM room_revisits WHERE room_created_seq = 10 AND stage_seconds = 86400"
    )

    with pytest.raises(CollectionError, match="conflicting room-creation event"):
        record_created_rooms(signer_store, [event], "2026-08-30T08:04:00Z")


def test_room_listing_observation_cannot_rewind_its_global_checkpoint(signer_store):
    state = new_signer_state(100)
    state["latest_room_listing_observed_at"] = "2026-08-30T09:00:00Z"

    with pytest.raises(CollectionError, match="room listing observation"):
        collect.record_listed_rooms(
            signer_store,
            [],
            "2026-08-30T08:30:00Z",
            state,
        )

    assert state["latest_room_listing_observed_at"] == "2026-08-30T09:00:00Z"


def test_room_listing_observation_cannot_rewind_a_room_checkpoint(signer_store):
    record_created_rooms(
        signer_store,
        [created_event(10, "2026-08-30T08:00:00Z", "listed-room")],
        "2026-08-30T08:00:01Z",
    )
    signer_store.execute(
        "UPDATE room_ledger SET last_listed_at = ? WHERE name = ?",
        ("2026-08-30T10:00:00Z", "listed-room"),
    )
    state = new_signer_state(100)
    state["latest_room_listing_observed_at"] = "2026-08-30T09:00:00Z"

    with pytest.raises(CollectionError, match="room listing observation"):
        collect.record_listed_rooms(
            signer_store,
            [{"name": "listed-room"}],
            "2026-08-30T09:30:00Z",
            state,
        )

    assert signer_store.execute(
        "SELECT last_listed_at FROM room_ledger WHERE name = 'listed-room'"
    ).fetchone() == ("2026-08-30T10:00:00Z",)


def test_room_listing_updates_only_the_newest_generation(signer_store):
    record_created_rooms(
        signer_store,
        [
            created_event(10, "2026-08-30T08:00:00Z", "reused-room"),
            created_event(20, "2026-08-30T09:00:00Z", "reused-room"),
        ],
        "2026-08-30T09:00:01Z",
    )
    state = new_signer_state(100)

    collect.record_listed_rooms(
        signer_store,
        [{"name": "reused-room"}],
        "2026-08-30T10:00:00Z",
        state,
    )

    assert signer_store.execute(
        "SELECT created_seq, last_listed_at FROM room_ledger ORDER BY created_seq"
    ).fetchall() == [(10, None), (20, "2026-08-30T10:00:00Z")]


def test_room_listing_updates_only_a_generation_visible_at_observation(signer_store):
    record_created_rooms(
        signer_store,
        [
            created_event(10, "2026-08-30T08:00:00Z", "reused-room"),
            created_event(20, "2026-08-30T08:04:00.500000Z", "reused-room"),
        ],
        "2026-08-30T08:04:01Z",
    )
    state = new_signer_state(100)

    collect.record_listed_rooms(
        signer_store,
        [{"name": "reused-room"}],
        "2026-08-30T08:04:00Z",
        state,
    )

    assert signer_store.execute(
        "SELECT created_seq, last_listed_at FROM room_ledger ORDER BY created_seq"
    ).fetchall() == [(10, "2026-08-30T08:04:00Z"), (20, None)]


def test_room_revisit_schedule_selects_only_active_eligible_stages(signer_store):
    record_created_rooms(
        signer_store,
        [created_event(10, "2026-08-30T08:00:00Z", "scheduled-room")],
        "2026-08-30T08:00:01Z",
    )

    before_due = select_revisits(
        signer_store,
        "2026-08-30T08:04:59Z",
    )
    assert before_due["due_this_tick"] == 0
    assert before_due["superseded_due_this_tick"] == 0
    assert before_due["selected"] == []

    at_due = select_revisits(
        signer_store,
        "2026-08-30T08:05:00Z",
    )
    assert at_due["due_this_tick"] == 1
    assert at_due["superseded_due_this_tick"] == 0
    assert at_due["selected"] == [
        {
            "created_seq": 10,
            "name": "scheduled-room",
            "stage_seconds": 300,
            "created_at": "2026-08-30T08:00:00.000000Z",
        }
    ]


@pytest.mark.parametrize(
    ("table", "column", "value"),
    (
        pytest.param("room_revisits", "stage_seconds", "abc", id="stage-text"),
        pytest.param("room_revisits", "stage_seconds", 301, id="unknown-stage"),
        pytest.param("room_ledger", "created_at", "not-a-timestamp", id="created-at"),
    ),
)
def test_room_revisit_selection_rejects_malformed_persisted_rows(
    signer_store,
    table,
    column,
    value,
):
    record_created_rooms(
        signer_store,
        [created_event(10, "2026-08-30T08:00:00Z", "corrupt-room")],
        "2026-08-30T08:00:01Z",
    )
    statement = (
        f"UPDATE {table} SET {column} = ? WHERE "
        f"{'room_created_seq' if table == 'room_revisits' else 'name'} = ?"
        f"{' AND stage_seconds = 300' if table == 'room_revisits' else ''}"
    )
    parameters = (value, 10 if table == "room_revisits" else "corrupt-room")
    if table == "room_revisits":
        with pytest.raises(sqlite3.IntegrityError, match="invalid room revisit state"):
            signer_store.execute(statement, parameters)
        return
    signer_store.execute(statement, parameters)

    with pytest.raises(CollectionError, match="room revisit"):
        select_revisits(signer_store, "2026-08-30T08:05:00Z")


def test_room_revisit_selection_rejects_a_malformed_future_due_time(signer_store):
    record_created_rooms(
        signer_store,
        [created_event(10, "2026-08-30T08:00:00Z", "corrupt-room")],
        "2026-08-30T08:00:01Z",
    )
    with pytest.raises(sqlite3.IntegrityError, match="invalid room revisit state"):
        signer_store.execute(
            "UPDATE room_revisits SET due_at = 'zzzz' "
            "WHERE room_created_seq = 10 AND stage_seconds = 300"
        )


def test_room_lifecycle_rejects_contradictory_attempted_state(signer_store):
    record_created_rooms(
        signer_store,
        [created_event(10, "2026-08-30T08:00:00Z", "corrupt-room")],
        "2026-08-30T08:00:01Z",
    )
    with pytest.raises(sqlite3.IntegrityError, match="invalid room revisit state"):
        signer_store.execute(
            """
            UPDATE room_revisits
            SET
                attempted_at = '2026-08-30T08:05:00Z',
                success = 1,
                outcome = 'check_failed',
                message_count = 0,
                has_second_message = 0
            WHERE room_created_seq = 10 AND stage_seconds = 300
            """
        )


def test_room_lifecycle_summary_uses_constant_time_rollup(signer_store):
    record_created_rooms(
        signer_store,
        [
            created_event(10, "2026-08-30T08:00:00Z", "active-room"),
            created_event(11, "2026-08-30T08:00:00Z", "failed-room"),
        ],
        "2026-08-30T08:00:01Z",
    )
    signer_store.execute(
        """
        UPDATE room_revisits
        SET attempted_at = due_at,
            success = 1,
            outcome = 'present_at_last_check',
            message_count = 2,
            has_second_message = 1,
            second_sender_class = 'signed_did'
        WHERE room_created_seq = 10 AND stage_seconds = 300
        """
    )
    signer_store.execute(
        """
        UPDATE room_revisits
        SET attempted_at = due_at,
            success = 0,
            outcome = 'check_failed'
        WHERE room_created_seq = 11 AND stage_seconds = 300
        """
    )

    statements = []
    signer_store.set_trace_callback(statements.append)
    summary = collect.room_lifecycle_summary(
        signer_store,
        created_this_tick=0,
        due_this_tick=0,
        selected_this_tick=0,
        revisits=[],
    )
    signer_store.set_trace_callback(None)

    assert summary["rooms_in_ledger"] == 2
    assert summary["rooms_revisited"] == 2
    assert summary["rooms_successfully_revisited"] == 1
    assert summary["rooms_with_second_message"] == 1
    assert summary["reads_attempted"] == 2
    assert summary["reads_failed"] == 1
    assert summary["second_sender_classes"]["signed_did"] == 1
    summary_selects = [
        statement.lower()
        for statement in statements
        if statement.lstrip().lower().startswith("select")
    ]
    assert summary_selects
    assert all("room_lifecycle_totals" in statement for statement in summary_selects)
    assert all("room_revisits" not in statement for statement in summary_selects)
    assert all("room_ledger" not in statement for statement in summary_selects)


def test_lifecycle_rollup_verifier_detects_tampered_totals(signer_store):
    record_created_rooms(
        signer_store,
        [created_event(10, "2026-08-30T08:00:00Z", "active-room")],
        "2026-08-30T08:00:01Z",
    )
    signer_store.execute(
        "UPDATE room_lifecycle_totals SET rooms_in_ledger = 2 WHERE singleton = 1"
    )

    with pytest.raises(CollectionError, match="lifecycle rollup"):
        collect.verify_lifecycle_rollup(signer_store)


def test_lifecycle_rollup_schema_rejects_inconsistent_sender_totals(signer_store):
    with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint failed"):
        signer_store.execute(
            "UPDATE room_lifecycle_totals SET second_sender_signed_did = 1 WHERE singleton = 1"
        )


def test_lifecycle_rollup_assigns_one_sender_class_per_generation(signer_store):
    record_created_rooms(
        signer_store,
        [created_event(10, "2026-08-30T08:00:00Z", "active-room")],
        "2026-08-30T08:00:01Z",
    )
    signer_store.execute(
        """
        UPDATE room_revisits
        SET attempted_at = due_at,
            success = 1,
            outcome = 'present_at_last_check',
            message_count = 2,
            has_second_message = 1,
            second_sender_class = 'signed_did'
        WHERE room_created_seq = 10 AND stage_seconds = 300
        """
    )
    signer_store.execute(
        """
        UPDATE room_revisits
        SET attempted_at = due_at,
            success = 1,
            outcome = 'present_at_last_check',
            message_count = 200,
            has_second_message = 1,
            second_sender_class = 'not_observed'
        WHERE room_created_seq = 10 AND stage_seconds = 3600
        """
    )

    summary = collect.room_lifecycle_summary(
        signer_store,
        created_this_tick=0,
        due_this_tick=0,
        selected_this_tick=0,
        revisits=[],
    )
    assert summary["rooms_with_second_message"] == 1
    assert summary["second_sender_classes"] == {
        "signed_did": 1,
        "unsigned_did": 0,
        "server": 0,
        "other": 0,
        "not_observed": 0,
    }

    with pytest.raises(sqlite3.IntegrityError, match="invalid room revisit state"):
        signer_store.execute(
            """
            UPDATE room_revisits
            SET second_sender_class = 'unsigned_did'
            WHERE room_created_seq = 10 AND stage_seconds = 3600
            """
        )


def test_superseded_generations_are_classified_by_eligibility(
    signer_store,
    monkeypatch,
):
    record_created_rooms(
        signer_store,
        [
            created_event(10, "2026-08-30T08:00:00Z", "before-due"),
            created_event(20, "2026-08-30T08:04:00Z", "before-due"),
            created_event(30, "2026-08-30T08:00:00Z", "after-eligibility"),
            created_event(40, "2026-08-30T08:06:00Z", "after-eligibility"),
        ],
        "2026-08-30T08:06:00Z",
    )
    tick_ts = "2026-08-30T08:06:00Z"
    monkeypatch.setattr(collect, "utc_now", lambda: tick_ts)
    calls = []

    class RecordingClient:
        def get(self, path, deadline=None):
            calls.append(path)
            raise AssertionError("a superseded cohort must not be read")

    lifecycle = collect_revisits(
        RecordingClient(),
        signer_store,
        tick_ts,
        sampled_room_reads=1,
        deadline=time.monotonic() + 1,
    )
    coverage = lifecycle["sampling"]["coverage_by_stage"]["300"]

    assert calls == []
    assert lifecycle["due_this_tick"] == 0
    assert lifecycle["attempted_this_tick"] == 0
    assert lifecycle["superseded_this_tick"] == 2
    assert lifecycle["deferred_due_to_budget"] == 0
    assert {revisit["created_seq"] for revisit in lifecycle["revisits"]} == {10, 30}
    assert coverage["scheduled_due_rooms"] == 2
    assert coverage["ineligible_superseded_before_due"] == 1
    assert coverage["eligible_rooms"] == 1
    assert coverage["superseded_after_eligibility"] == 1
    assert lifecycle["rooms_revisited"] == 0
    assert lifecycle["reads_attempted"] == 0


def test_stage_coverage_distinguishes_sampling_outcomes(
    signer_store,
    monkeypatch,
):
    record_created_rooms(
        signer_store,
        [
            created_event(1, "2026-08-30T08:00:00Z", "completed-room"),
            created_event(2, "2026-08-30T08:00:00Z", "absent-room"),
            created_event(3, "2026-08-30T08:00:00Z", "failed-room"),
            created_event(4, "2026-08-30T08:00:00Z", "deferred-room"),
            created_event(5, "2026-08-30T07:55:00Z", "aged-room"),
            created_event(6, "2026-08-30T08:00:00Z", "before-due"),
            created_event(106, "2026-08-30T08:04:00Z", "before-due"),
            created_event(7, "2026-08-30T08:00:00Z", "after-eligibility"),
            created_event(
                107,
                "2026-08-30T08:05:30Z",
                "after-eligibility",
            ),
        ],
        "2026-08-30T08:06:00Z",
    )
    signer_store.execute(
        """
        UPDATE room_revisits
        SET
            attempted_at = due_at,
            success = 1,
            outcome = 'present_at_last_check',
            message_count = 2,
            has_second_message = 1,
            second_sender_class = 'signed_did'
        WHERE room_created_seq = 1 AND stage_seconds = 300
        """
    )
    signer_store.execute(
        """
        UPDATE room_revisits
        SET
            attempted_at = due_at,
            success = 0,
            outcome = 'absent_at_last_check'
        WHERE room_created_seq = 2 AND stage_seconds = 300
        """
    )
    signer_store.execute(
        """
        UPDATE room_revisits
        SET
            attempted_at = due_at,
            success = 0,
            outcome = 'check_failed'
        WHERE room_created_seq = 3 AND stage_seconds = 300
        """
    )
    tick_ts = "2026-08-30T08:06:00Z"
    monkeypatch.setattr(collect, "utc_now", lambda: tick_ts)
    monkeypatch.setattr(collect.time, "monotonic", lambda: 0.0)

    class MustNotReadClient:
        def get(self, path, deadline=None):
            raise AssertionError(f"deadline-deferred read was issued: {path}")

    lifecycle = collect_revisits(
        MustNotReadClient(),
        signer_store,
        tick_ts,
        sampled_room_reads=1,
        deadline=0.0,
    )
    sampling = lifecycle["sampling"]
    coverage = sampling["coverage_by_stage"]["300"]

    assert lifecycle["due_this_tick"] == 1
    assert lifecycle["attempted_this_tick"] == 0
    assert lifecycle["deferred_due_to_read_budget"] == 0
    assert lifecycle["deferred_due_to_deadline"] == 1
    assert lifecycle["deferred_due_to_budget"] == 1
    assert sampling["aged_out_unselected"] == 1
    assert coverage == {
        "scheduled_due_rooms": 7,
        "ineligible_superseded_before_due": 1,
        "eligible_rooms": 6,
        "attempted_checks": 3,
        "completed_checks": 2,
        "failed_checks": 1,
        "attempted_late": 0,
        "deferred_checks": 1,
        "aged_out_unselected": 1,
        "superseded_after_eligibility": 1,
        "coverage_fraction": {
            "numerator": 2,
            "denominator": 6,
        },
        "second_message_fraction": {
            "numerator": 1,
            "denominator": 1,
        },
    }


def test_stage_coverage_counts_late_attempts_separately_from_aged_out(
    signer_store,
    monkeypatch,
):
    record_created_rooms(
        signer_store,
        [created_event(10, "2026-08-30T08:00:00Z", "late-room")],
        "2026-08-30T08:00:01Z",
    )
    # Selected inside the 5-minute eligibility window, but the read itself
    # lands after the window closes at 08:10:00.
    monkeypatch.setattr(collect, "utc_now", lambda: "2026-08-30T08:11:00Z")

    class SlowClient:
        def get(self, path, deadline=None):
            return room_body(message(1, "server"), message(2, "server"))

    lifecycle = collect_revisits(
        SlowClient(),
        signer_store,
        "2026-08-30T08:06:00Z",
        sampled_room_reads=1,
        deadline=time.monotonic() + 1,
    )
    assert lifecycle["revisits"][0]["outcome"] == "present_at_last_check"

    coverage = collect.room_lifecycle_coverage_by_stage(
        signer_store,
        "2026-08-30T08:20:00Z",
    )["300"]

    assert coverage["attempted_late"] == 1
    assert coverage["aged_out_unselected"] == 0
    assert coverage["attempted_checks"] == 0
    assert coverage["completed_checks"] == 0
    assert coverage["deferred_checks"] == 0
    assert coverage["eligible_rooms"] == 1
    # A late present check never enters the timely coverage or
    # second-message denominators.
    assert coverage["coverage_fraction"] == {"numerator": 0, "denominator": 1}
    assert coverage["second_message_fraction"] == {
        "numerator": 0,
        "denominator": 0,
    }


def test_aged_out_checks_are_finalized_terminally_and_published(
    signer_store,
    monkeypatch,
):
    record_created_rooms(
        signer_store,
        [
            created_event(10, "2026-08-30T07:00:00Z", "aged-room-a"),
            created_event(11, "2026-08-30T07:01:00Z", "aged-room-b"),
        ],
        "2026-08-30T07:01:01Z",
    )
    tick_ts = "2026-09-01T08:00:00Z"
    monkeypatch.setattr(collect, "utc_now", lambda: tick_ts)

    class MustNotReadClient:
        def get(self, path, deadline=None):
            raise AssertionError(f"aged-out stage was read: {path}")

    lifecycle = collect_revisits(
        MustNotReadClient(),
        signer_store,
        tick_ts,
        sampled_room_reads=1,
        deadline=time.monotonic() + 1,
    )
    sampling = lifecycle["sampling"]

    assert sampling["aged_out_unselected"] == 6
    assert sampling["aged_out_finalization"] == {
        "finalized_this_tick": 6,
        "backlog_remaining": 0,
        "batch_limit": collect.ROOM_REVISIT_AGED_OUT_FINALIZATION_LIMIT,
    }
    # Finalization is terminal bookkeeping, never fabricated attempt
    # evidence: every finalized row keeps its null attempt fields.
    assert signer_store.execute(
        """
        SELECT COUNT(*)
        FROM room_revisits
        WHERE
            aged_out_at IS NOT NULL
            AND attempted_at IS NULL
            AND success IS NULL
            AND outcome IS NULL
        """
    ).fetchone() == (6,)

    later = select_revisits(signer_store, "2026-09-01T08:05:00Z")
    assert later["due_this_tick"] == 0
    assert later["aged_out_unselected"] == 0
    assert later["aged_out_finalizable"] == []

    coverage = collect.room_lifecycle_coverage_by_stage(
        signer_store,
        "2026-09-01T08:10:00Z",
    )
    assert {
        stage: counters["aged_out_unselected"] for stage, counters in coverage.items()
    } == {
        "300": 2,
        "3600": 2,
        "86400": 2,
    }
    collect.verify_lifecycle_rollup(signer_store)


def test_aged_out_finalization_is_bounded_and_publishes_its_backlog(
    signer_store,
    monkeypatch,
):
    record_created_rooms(
        signer_store,
        [
            created_event(index, "2026-08-30T07:00:00Z", f"aged-room-{index}")
            for index in range(3)
        ],
        "2026-08-30T07:00:01Z",
    )
    monkeypatch.setattr(collect, "ROOM_REVISIT_AGED_OUT_FINALIZATION_LIMIT", 4)

    class MustNotReadClient:
        def get(self, path, deadline=None):
            raise AssertionError(f"aged-out stage was read: {path}")

    def run_tick(tick_ts):
        monkeypatch.setattr(collect, "utc_now", lambda: tick_ts)
        return collect_revisits(
            MustNotReadClient(),
            signer_store,
            tick_ts,
            sampled_room_reads=1,
            deadline=time.monotonic() + 1,
        )

    first = run_tick("2026-09-01T08:00:00Z")
    assert first["sampling"]["aged_out_unselected"] == 9
    assert first["sampling"]["aged_out_finalization"] == {
        "finalized_this_tick": 4,
        "backlog_remaining": 5,
        "batch_limit": 4,
    }
    assert signer_store.execute(
        "SELECT COUNT(*) FROM room_revisits WHERE aged_out_at IS NOT NULL"
    ).fetchone() == (4,)

    second = run_tick("2026-09-01T08:05:00Z")
    assert second["sampling"]["aged_out_unselected"] == 9
    assert second["sampling"]["aged_out_finalization"] == {
        "finalized_this_tick": 4,
        "backlog_remaining": 1,
        "batch_limit": 4,
    }

    third = run_tick("2026-09-01T08:10:00Z")
    assert third["sampling"]["aged_out_finalization"] == {
        "finalized_this_tick": 1,
        "backlog_remaining": 0,
        "batch_limit": 4,
    }
    assert signer_store.execute(
        "SELECT COUNT(*) FROM room_revisits WHERE aged_out_at IS NOT NULL"
    ).fetchone() == (9,)


def test_revisit_selection_uses_the_pending_partial_index(signer_store):
    record_created_rooms(
        signer_store,
        [created_event(10, "2026-08-30T08:00:00Z", "indexed-room")],
        "2026-08-30T08:00:01Z",
    )
    statements = []
    signer_store.set_trace_callback(statements.append)
    select_revisits(signer_store, "2026-08-30T08:05:00Z")
    signer_store.set_trace_callback(None)

    fetch = next(
        statement
        for statement in statements
        if "MIN(newer.created_at)" in statement and "attempted_at IS NULL" in statement
    )
    plan = " ".join(
        str(row[3])
        for row in signer_store.execute("EXPLAIN QUERY PLAN " + fetch).fetchall()
    )
    assert "room_revisits_pending" in plan
    assert "SCAN room_revisits" not in plan


def test_finalization_state_is_validated_at_write_time(signer_store):
    record_created_rooms(
        signer_store,
        [created_event(10, "2026-08-30T08:00:00Z", "guarded-room")],
        "2026-08-30T08:00:01Z",
    )
    # Finalizing before the eligibility window closes is refused.
    with pytest.raises(sqlite3.IntegrityError, match="invalid room revisit state"):
        signer_store.execute(
            "UPDATE room_revisits SET aged_out_at = '2026-08-30T08:07:00Z' "
            "WHERE room_created_seq = 10 AND stage_seconds = 300"
        )
    # An attempted row can never be finalized as aged out.
    signer_store.execute(
        """
        UPDATE room_revisits
        SET attempted_at = due_at, success = 0, outcome = 'check_failed'
        WHERE room_created_seq = 10 AND stage_seconds = 300
        """
    )
    with pytest.raises(sqlite3.IntegrityError, match="invalid room revisit state"):
        signer_store.execute(
            "UPDATE room_revisits SET aged_out_at = '2026-08-30T09:00:00Z' "
            "WHERE room_created_seq = 10 AND stage_seconds = 300"
        )
    # A closed, never-attempted window accepts terminal finalization.
    signer_store.execute(
        "UPDATE room_revisits SET aged_out_at = '2026-08-30T10:00:00Z' "
        "WHERE room_created_seq = 10 AND stage_seconds = 3600"
    )
    collect.verify_lifecycle_rollup(signer_store)


def test_signer_database_migrates_pre_finalization_revisit_schema(tmp_path):
    path = tmp_path / "signers.sqlite3"
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE room_ledger (
            created_seq INTEGER PRIMARY KEY CHECK (created_seq >= 0),
            name TEXT NOT NULL,
            room_id TEXT NOT NULL CHECK (
                length(room_id) = 16
                AND room_id NOT GLOB '*[^0-9a-f]*'
            ),
            room_sha256 TEXT NOT NULL CHECK (
                length(room_sha256) = 64
                AND room_sha256 NOT GLOB '*[^0-9a-f]*'
            ),
            created_at TEXT NOT NULL,
            first_observed_at TEXT NOT NULL,
            last_listed_at TEXT
        );

        CREATE TABLE room_revisits (
            room_created_seq INTEGER NOT NULL,
            stage_seconds INTEGER NOT NULL,
            due_at TEXT NOT NULL,
            attempted_at TEXT,
            success INTEGER CHECK (success IN (0, 1)),
            outcome TEXT CHECK (
                outcome IN (
                    'present_at_last_check',
                    'absent_at_last_check',
                    'check_failed',
                    'superseded_before_check'
                )
            ),
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
            PRIMARY KEY (room_created_seq, stage_seconds),
            FOREIGN KEY (room_created_seq) REFERENCES room_ledger(created_seq)
        );

        CREATE INDEX room_revisits_due
        ON room_revisits (attempted_at, due_at);
        """
    )
    connection.commit()
    connection.close()

    migrated = connect_signer_database(path)
    try:
        columns = {
            row[1] for row in migrated.execute("PRAGMA table_info('room_revisits')")
        }
        assert "aged_out_at" in columns
        indexes = {
            row[1] for row in migrated.execute("PRAGMA index_list('room_revisits')")
        }
        assert "room_revisits_pending" in indexes
        assert "room_revisits_aged_out" in indexes
        assert "room_revisits_due" not in indexes
    finally:
        migrated.close()


def test_stale_lifecycle_triggers_are_rebuilt_with_finalization_rules(tmp_path):
    path = tmp_path / "signers.sqlite3"
    connection = connect_signer_database(path)
    connection.execute("DROP TRIGGER room_revisits_validate_update")
    connection.execute(
        "CREATE TRIGGER room_revisits_validate_update "
        "BEFORE UPDATE ON room_revisits BEGIN SELECT 1; END"
    )
    connection.commit()
    connection.close()

    reopened = connect_signer_database(path)
    try:
        sql = reopened.execute(
            "SELECT sql FROM sqlite_master "
            "WHERE type = 'trigger' AND name = 'room_revisits_validate_update'"
        ).fetchone()[0]
        assert "aged_out_at" in sql
    finally:
        reopened.close()


def test_read_budget_ceiling_is_enforced_in_selection_and_arithmetic(signer_store):
    events = [
        created_event(
            index,
            "2026-08-30T08:00:00Z",
            f"due-room-{index}",
        )
        for index in range(collect.ROOM_REVISIT_READ_BUDGET + 1)
    ]
    record_created_rooms(
        signer_store,
        events,
        "2026-08-30T08:00:01Z",
    )
    selection = select_revisits(
        signer_store,
        "2026-08-30T08:05:00Z",
    )

    assert collect.ROOM_REVISIT_READ_BUDGET == 38
    assert selection["due_this_tick"] == 39
    assert selection["superseded_due_this_tick"] == 0
    assert len(selection["selected"]) == 38
    budget = read_budget_summary(
        ROOM_READ_BUDGET,
        len(selection["selected"]),
    )
    assert budget["total_reads"] == collect.TOTAL_READ_BUDGET == 120
    assert budget["rate_window_seconds"] == 60
    assert budget["tick_revisit_deadline_seconds"] == 300
    assert budget["reads_per_minute"] == pytest.approx(120.0)
    assert budget["share"] == pytest.approx(0.20)
    assert budget["maximum_share"] == pytest.approx(0.20)
    with pytest.raises(CollectionError, match="exceeds its enforced read budget"):
        select_revisits(
            signer_store,
            "2026-08-30T08:05:00Z",
            limit=collect.ROOM_REVISIT_READ_BUDGET + 1,
        )


def test_revisit_selection_is_deterministic_and_independent_of_room_age(tmp_path):
    tick_ts = "2026-08-30T08:05:39Z"

    def selected_sequences(path, created_seconds):
        connection = connect_signer_database(path)
        try:
            record_created_rooms(
                connection,
                [
                    created_event(
                        index,
                        f"2026-08-30T08:00:{second:02d}Z",
                        f"ranked-room-{index:02d}",
                    )
                    for index, second in enumerate(created_seconds)
                ],
                "2026-08-30T08:00:40Z",
            )
            first = select_revisits(connection, tick_ts, limit=10)
            second = select_revisits(connection, tick_ts, limit=10)
            return (
                [
                    (row["created_seq"], row["stage_seconds"])
                    for row in first["selected"]
                ],
                [
                    (row["created_seq"], row["stage_seconds"])
                    for row in second["selected"]
                ],
                first["selection"],
                second["selection"],
            )
        finally:
            connection.close()

    chronological = selected_sequences(
        tmp_path / "chronological.sqlite3",
        range(40),
    )
    reversed_ages = selected_sequences(
        tmp_path / "reversed.sqlite3",
        reversed(range(40)),
    )

    assert chronological[0] == chronological[1]
    assert chronological[2] == chronological[3]
    assert reversed_ages[0] == reversed_ages[1]
    assert reversed_ages[2] == reversed_ages[3]
    assert chronological[0] == reversed_ages[0]


def test_stage_outside_its_eligibility_window_is_never_selected(
    signer_store,
    monkeypatch,
):
    record_created_rooms(
        signer_store,
        [created_event(10, "2026-08-30T08:00:00Z", "expired-room")],
        "2026-08-30T08:00:01Z",
    )

    still_eligible = select_revisits(
        signer_store,
        "2026-08-30T08:09:59.999999Z",
    )
    assert still_eligible["due_this_tick"] == 1
    assert [row["created_seq"] for row in still_eligible["selected"]] == [10]

    selection_time = "2026-08-30T08:10:00Z"
    monkeypatch.setattr(collect, "utc_now", lambda: selection_time)

    class MustNotReadClient:
        def get(self, path, deadline=None):
            raise AssertionError(f"expired stage was read: {path}")

    lifecycle = collect_revisits(
        MustNotReadClient(),
        signer_store,
        selection_time,
        sampled_room_reads=1,
        deadline=time.monotonic() + 1,
    )
    sampling = lifecycle["sampling"]

    assert lifecycle["due_this_tick"] == 0
    assert lifecycle["attempted_this_tick"] == 0
    assert lifecycle["deferred_due_to_budget"] == 0
    assert lifecycle["revisits"] == []
    assert sampling["aged_out_unselected"] == 1
    assert sampling["coverage_by_stage"]["300"]["aged_out_unselected"] == 1
    assert signer_store.execute(
        """
        SELECT
            attempted_at,
            success,
            outcome,
            message_count,
            has_second_message
        FROM room_revisits
        WHERE room_created_seq = 10 AND stage_seconds = 300
        """
    ).fetchone() == (None, None, None, None, None)


def test_stage_allocation_rotates_and_redistributes_unused_slots(signer_store):
    events = []
    for start, count, created_at, prefix in (
        (0, 20, "2026-08-30T11:55:00Z", "five-minute"),
        (100, 20, "2026-08-30T11:00:00Z", "one-hour"),
        (200, 20, "2026-08-29T12:00:00Z", "one-day"),
    ):
        events.extend(
            created_event(
                start + offset,
                created_at,
                f"{prefix}-{offset:02d}",
            )
            for offset in range(count)
        )
    record_created_rooms(
        signer_store,
        events,
        "2026-08-30T12:00:00Z",
    )

    allocations = []
    for rotation in range(3):
        result = select_revisits(
            signer_store,
            "2026-08-30T12:00:00Z",
            allocation_rotation=rotation,
        )
        counts = {
            stage: sum(row["stage_seconds"] == stage for row in result["selected"])
            for stage in collect.ROOM_REVISIT_STAGES_SECONDS
        }
        assert len(result["selected"]) == 38
        assert sorted(counts.values()) == [12, 13, 13]
        allocations.append(counts)

    for stage in collect.ROOM_REVISIT_STAGES_SECONDS:
        assert sum(allocation[stage] == 12 for allocation in allocations) == 1

    signer_store.execute("DELETE FROM room_revisits")
    signer_store.execute("DELETE FROM room_ledger")
    sparse_events = [
        *[
            created_event(
                offset,
                "2026-08-30T11:55:00Z",
                f"sparse-five-minute-{offset:02d}",
            )
            for offset in range(2)
        ],
        *[
            created_event(
                100 + offset,
                "2026-08-30T11:00:00Z",
                f"sparse-one-hour-{offset:02d}",
            )
            for offset in range(30)
        ],
        *[
            created_event(
                200 + offset,
                "2026-08-29T12:00:00Z",
                f"sparse-one-day-{offset:02d}",
            )
            for offset in range(30)
        ],
    ]
    record_created_rooms(
        signer_store,
        sparse_events,
        "2026-08-30T12:00:00Z",
    )

    first = select_revisits(
        signer_store,
        "2026-08-30T12:00:00Z",
    )
    second = select_revisits(
        signer_store,
        "2026-08-30T12:00:00Z",
    )
    redistributed = {
        stage: sum(row["stage_seconds"] == stage for row in first["selected"])
        for stage in collect.ROOM_REVISIT_STAGES_SECONDS
    }

    assert first["selected"] == second["selected"]
    assert len(first["selected"]) == 38
    assert first["selection"]["redistributed_reads"] == 10
    assert redistributed[300] == 2
    assert redistributed[3600] + redistributed[86400] == 36


def test_failed_revisit_is_failure_not_absence_of_activity(
    signer_store,
    monkeypatch,
):
    record_created_rooms(
        signer_store,
        [created_event(10, "2026-08-30T08:00:00Z", "failed-room")],
        "2026-08-30T08:00:01Z",
    )
    monkeypatch.setattr(collect, "utc_now", lambda: "2026-08-30T08:05:00Z")

    class FailingClient:
        def get(self, path, deadline=None):
            raise CollectionError(f"failed {path}")

    lifecycle = collect_revisits(
        FailingClient(),
        signer_store,
        "2026-08-30T08:05:00Z",
        sampled_room_reads=1,
        deadline=time.monotonic() + 1,
    )

    assert lifecycle["reads_failed"] == 1
    assert lifecycle["rooms_successfully_revisited"] == 0
    assert lifecycle["rooms_with_second_message"] == 0
    assert lifecycle["deferred_due_to_read_budget"] == 0
    assert lifecycle["deferred_due_to_deadline"] == 0
    assert lifecycle["revisits"] == [
        {
            "id": collect.room_identifier("failed-room"),
            "created_seq": 10,
            "stage_seconds": 300,
            "elapsed_since_creation_seconds": 300,
            "success": False,
            "outcome": "check_failed",
            "message_count": None,
            "has_second_message": None,
            "second_sender_class": None,
        }
    ]
    assert signer_store.execute(
        """
        SELECT success, outcome, message_count, has_second_message
        FROM room_revisits
        WHERE room_created_seq = ? AND stage_seconds = ?
        """,
        (10, 300),
    ).fetchone() == (0, "check_failed", None, None)


def test_malformed_revisit_message_is_check_failed_not_no_second_message(
    signer_store,
    monkeypatch,
):
    record_created_rooms(
        signer_store,
        [created_event(10, "2026-08-30T08:00:00Z", "malformed-room")],
        "2026-08-30T08:00:01Z",
    )
    monkeypatch.setattr(collect, "utc_now", lambda: "2026-08-30T08:05:00Z")

    class MalformedClient:
        def get(self, path, deadline=None):
            return room_body(message(1, "server"), {"seq": 2, "from": "visitor"})

    lifecycle = collect_revisits(
        MalformedClient(),
        signer_store,
        "2026-08-30T08:05:00Z",
        sampled_room_reads=1,
        deadline=time.monotonic() + 1,
    )

    assert lifecycle["reads_failed"] == 1
    assert lifecycle["revisits"][0]["outcome"] == "check_failed"
    assert lifecycle["revisits"][0]["has_second_message"] is None
    assert signer_store.execute(
        """
            SELECT success, outcome, message_count, has_second_message
            FROM room_revisits
            WHERE room_created_seq = ? AND stage_seconds = ?
            """,
        (10, 300),
    ).fetchone() == (0, "check_failed", None, None)


def test_http_404_revisit_is_absent_but_other_http_failures_are_failed(
    signer_store,
    monkeypatch,
):
    record_created_rooms(
        signer_store,
        [
            created_event(10, "2026-08-30T08:00:00Z", "missing-room"),
            created_event(11, "2026-08-30T08:00:00Z", "unavailable-room"),
        ],
        "2026-08-30T08:00:01Z",
    )
    monkeypatch.setattr(collect, "utc_now", lambda: "2026-08-30T08:05:00Z")

    class StatusClient:
        def get(self, path, deadline=None):
            status = 404 if "missing-room" in path else 503
            raise CollectionError(
                "room read failed",
                outcome="http_error",
                status=status,
                path=path,
            )

    lifecycle = collect_revisits(
        StatusClient(),
        signer_store,
        "2026-08-30T08:05:00Z",
        sampled_room_reads=1,
        deadline=time.monotonic() + 1,
    )

    assert [entry["outcome"] for entry in lifecycle["revisits"]] == [
        "absent_at_last_check",
        "check_failed",
    ]
    assert lifecycle["reads_failed"] == 1
    assert signer_store.execute(
        """
            SELECT room_ledger.name, room_revisits.success, room_revisits.outcome
            FROM room_revisits
            JOIN room_ledger
                ON room_ledger.created_seq = room_revisits.room_created_seq
            WHERE attempted_at IS NOT NULL
            ORDER BY room_ledger.name
        """
    ).fetchall() == [
        ("missing-room", 0, "absent_at_last_check"),
        ("unavailable-room", 0, "check_failed"),
    ]


def test_deadline_defers_remainder_without_conflating_outcomes(
    signer_store,
    monkeypatch,
):
    events = [
        created_event(10, "2026-08-30T08:00:00Z", "a-failed"),
        created_event(11, "2026-08-30T08:00:00Z", "b-no-second"),
        created_event(12, "2026-08-30T08:00:00Z", "c-deferred"),
    ]
    record_created_rooms(
        signer_store,
        events,
        "2026-08-30T08:00:01Z",
    )
    monkeypatch.setattr(collect, "utc_now", lambda: "2026-08-30T08:07:30Z")
    clock = iter((0.0, 1.0, 2.0))
    monkeypatch.setattr(collect.time, "monotonic", lambda: next(clock))

    calls = []

    class MixedClient:
        def get(self, path, deadline=None):
            calls.append(path)
            if len(calls) == 1:
                raise CollectionError("failed read")
            return room_body(message(1, "server"))

    lifecycle = collect_revisits(
        MixedClient(),
        signer_store,
        "2026-08-30T08:07:30Z",
        sampled_room_reads=1,
        deadline=2.0,
    )

    assert lifecycle["due_this_tick"] == 3
    assert lifecycle["attempted_this_tick"] == 2
    assert lifecycle["deferred_due_to_read_budget"] == 0
    assert lifecycle["deferred_due_to_deadline"] == 1
    assert lifecycle["deferred_due_to_budget"] == 1
    assert len(calls) == 2

    failed, no_second = lifecycle["revisits"]
    assert failed["success"] is False
    assert failed["outcome"] == "check_failed"
    assert failed["message_count"] is None
    assert failed["has_second_message"] is None
    assert no_second["success"] is True
    assert no_second["outcome"] == "present_at_last_check"
    assert no_second["message_count"] == 1
    assert no_second["has_second_message"] is False
    assert no_second["second_sender_class"] is None
    assert failed["elapsed_since_creation_seconds"] == 450
    assert no_second["elapsed_since_creation_seconds"] == 450

    assert signer_store.execute(
        """
        SELECT COUNT(*)
        FROM room_revisits
        WHERE stage_seconds = 300 AND attempted_at IS NULL
        """
    ).fetchone() == (1,)


def test_slow_base_phase_issues_no_room_revisits(signer_store, monkeypatch):
    record_created_rooms(
        signer_store,
        [created_event(10, "2026-08-30T08:00:00Z", "deferred-room")],
        "2026-08-30T08:00:01Z",
    )
    monkeypatch.setattr(collect.time, "monotonic", lambda: 300.0)

    calls = []

    class RecordingClient:
        def get(self, path, deadline=None):
            calls.append(path)
            return room_body(message(1, "server"))

    lifecycle = collect_revisits(
        RecordingClient(),
        signer_store,
        "2026-08-30T08:05:00Z",
        sampled_room_reads=1,
        deadline=300.0,
    )

    assert calls == []
    assert lifecycle["attempted_this_tick"] == 0
    assert lifecycle["deferred_due_to_read_budget"] == 0
    assert lifecycle["deferred_due_to_deadline"] == 1
    assert lifecycle["deferred_due_to_budget"] == 1


def test_revisit_read_cap_deferral_count_is_exact(signer_store, monkeypatch):
    events = [
        created_event(
            index,
            "2026-08-30T08:00:00Z",
            f"due-room-{index:02d}",
        )
        for index in range(collect.ROOM_REVISIT_READ_BUDGET + 2)
    ]
    record_created_rooms(
        signer_store,
        events,
        "2026-08-30T08:00:01Z",
    )
    monkeypatch.setattr(collect, "utc_now", lambda: "2026-08-30T08:05:00Z")
    monkeypatch.setattr(collect.time, "monotonic", lambda: 0.0)

    calls = []

    class SuccessfulClient:
        def get(self, path, deadline=None):
            calls.append(path)
            return room_body(message(1, "server"))

    lifecycle = collect_revisits(
        SuccessfulClient(),
        signer_store,
        "2026-08-30T08:05:00Z",
        sampled_room_reads=ROOM_READ_BUDGET,
        deadline=1.0,
    )

    assert lifecycle["due_this_tick"] == 40
    assert lifecycle["attempted_this_tick"] == 38
    assert lifecycle["deferred_due_to_read_budget"] == 2
    assert lifecycle["deferred_due_to_deadline"] == 0
    assert lifecycle["deferred_due_to_budget"] == 2
    assert len(calls) == 38


def test_read_budget_is_enforced_before_first_revisit_read(
    signer_store,
    monkeypatch,
):
    record_created_rooms(
        signer_store,
        [created_event(10, "2026-08-30T08:00:00Z", "due-room")],
        "2026-08-30T08:00:01Z",
    )
    calls = []

    class RecordingClient:
        def get(self, path, deadline=None):
            calls.append(path)
            return room_body(message(1, "server"))

    def reject_budget(sampled_room_reads, revisit_reads):
        raise CollectionError("preflight budget rejected")

    monkeypatch.setattr(collect, "read_budget_summary", reject_budget)

    with pytest.raises(CollectionError, match="preflight budget rejected"):
        collect_revisits(
            RecordingClient(),
            signer_store,
            "2026-08-30T08:05:00Z",
            sampled_room_reads=1,
            deadline=time.monotonic() + 1,
        )

    assert calls == []


def test_collect_tick_prioritizes_revisits_before_sampled_room_reads(
    tmp_path,
    monkeypatch,
):
    tick_ts = "2026-08-30T08:05:00Z"
    clock = [0.0]
    monkeypatch.setattr(collect, "utc_now", lambda: tick_ts)
    monkeypatch.setattr(collect.time, "monotonic", lambda: clock[0])

    signer_state_path = tmp_path / "signers.json"
    connection = connect_signer_database(
        collect.signer_database_path(signer_state_path)
    )
    try:
        record_created_rooms(
            connection,
            [created_event(10, "2026-08-30T08:00:00Z", "priority-room")],
            "2026-08-30T08:00:01Z",
        )
        state = new_signer_state(100)
        state["selector_seed"] = REVISIT_SELECTOR_SEED
        collect.write_signer_metadata(connection, state)
        connection.commit()
    finally:
        connection.close()

    calls = []

    class SlowSampleClient:
        def get(self, path, deadline=None):
            calls.append(path)
            if path == "/rooms?format=json&limit=200":
                return json.dumps(
                    {
                        "total": 1,
                        "capacity": 100,
                        "bytes": 0,
                        "notes": {
                            "total": 0,
                            "capacity": 100,
                            "bytes": 0,
                        },
                        "rooms": [{"name": "lobby", "seq": 1, "idle": 0}],
                    }
                )
            if path == "/r/events?format=json&limit=200":
                return json.dumps(
                    {
                        "room": "events",
                        "messages": [
                            {
                                "seq": 20,
                                "ts": tick_ts,
                                "from": "server",
                                "text": "created event-room",
                            }
                        ],
                    }
                )
            if path == "/r/priority-room?format=json&limit=200":
                return room_body(message(1, "server"))
            if path == "/r/lobby?format=json&limit=200":
                clock[0] = collect.TICK_REVISIT_DEADLINE_SECONDS + 1
                return room_body(message(1, "server"))
            raise AssertionError(f"unexpected read: {path}")

    tick = collect.collect_tick(
        SlowSampleClient(),
        signer_state_path,
        100,
    )

    assert calls[:4] == [
        "/rooms?format=json&limit=200",
        "/r/events?format=json&limit=200",
        "/r/priority-room?format=json&limit=200",
        "/r/lobby?format=json&limit=200",
    ]
    assert tick["room_lifecycle"]["attempted_this_tick"] == 1
    assert "sampling" not in tick["room_lifecycle"]
    assert tick["room_lifecycle_sampling"]["selection"]
    assert tick["room_lifecycle_sampling"]["coverage_by_stage"]


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


@pytest.mark.parametrize(
    ("column", "value", "tick_ts"),
    (
        pytest.param("tick_count", "abc", "2026-08-30T08:00:00Z", id="tick-text"),
        pytest.param(
            "tick_count",
            collect.SQLITE_INTEGER_MAX,
            "2026-08-30T08:00:00Z",
            id="tick-overflow",
        ),
        pytest.param(
            "collection_utc_dates_count",
            collect.SQLITE_INTEGER_MAX,
            "2026-08-30T08:00:00Z",
            id="date-count-overflow",
        ),
    ),
)
def test_signer_update_rejects_invalid_persisted_counters(
    signer_store,
    column,
    value,
    tick_ts,
):
    suffix = DID.removeprefix("did:key:z6Mk")
    insert_record(signer_store, suffix, 1, 1, ["room-a"], 0)
    signer_store.execute(
        f"UPDATE signer_dids SET {column} = ? WHERE did = ?",
        (value, suffix),
    )
    messages = parse_room_messages(
        room_body(message(1, DID, nonce="1")),
        "/r/test",
    )

    with pytest.raises(CollectionError, match="signer database"):
        update_signer_state(
            signer_store,
            new_signer_state(100),
            [("room-a", messages)],
            tick_ts,
        )


def test_nonce_bearing_sender_without_did_shape_is_not_counted(signer_store):
    messages = parse_room_messages(
        room_body(message(1, "server", nonce="12345")), "/r/test"
    )
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


def test_v5_to_v6_migration_preserves_cross_clock_timestamps_and_backfills_search(
    tmp_path,
):
    path = tmp_path / "signers-v5.sqlite3"
    connection = sqlite3.connect(path)
    state = new_signer_state(100)
    state["version"] = 5
    state.pop("latest_room_listing_observed_at", None)
    connection.executescript(
        """
        CREATE TABLE signer_metadata (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            state_json TEXT NOT NULL
        );
        CREATE TABLE room_ledger (
            name TEXT PRIMARY KEY,
            room_id TEXT NOT NULL,
            created_seq INTEGER NOT NULL UNIQUE CHECK (created_seq >= 0),
            created_at TEXT NOT NULL,
            first_observed_at TEXT NOT NULL
        );
        CREATE TABLE room_revisits (
            room_name TEXT NOT NULL,
            stage_seconds INTEGER NOT NULL,
            due_at TEXT NOT NULL,
            attempted_at TEXT,
            success INTEGER CHECK (success IN (0, 1)),
            message_count INTEGER CHECK (message_count >= 0),
            has_second_message INTEGER CHECK (has_second_message IN (0, 1)),
            second_sender_class TEXT,
            PRIMARY KEY (room_name, stage_seconds),
            FOREIGN KEY (room_name) REFERENCES room_ledger(name)
        );
        """
    )
    connection.execute(
        "INSERT INTO signer_metadata (singleton, state_json) VALUES (1, ?)",
        (json.dumps(state),),
    )
    connection.execute(
        "INSERT INTO room_ledger VALUES (?, ?, ?, ?, ?)",
        (
            "persistent-alpha-room",
            collect.room_identifier("persistent-alpha-room"),
            42,
            "2026-08-29T08:00:00Z",
            "2026-08-29T07:59:59.441996Z",
        ),
    )
    connection.executemany(
        "INSERT INTO room_revisits VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "persistent-alpha-room",
                300,
                "2026-08-29T08:05:00Z",
                "2026-08-29T08:05:01Z",
                1,
                2,
                1,
                "signed_did",
            ),
            (
                "persistent-alpha-room",
                3600,
                "2026-08-29T09:00:00Z",
                "2026-08-29T09:00:01Z",
                0,
                None,
                None,
                None,
            ),
            (
                "persistent-alpha-room",
                86400,
                "2026-08-30T08:00:00Z",
                None,
                None,
                None,
                None,
                None,
            ),
        ],
    )
    connection.execute("PRAGMA user_version = 5")
    connection.commit()
    connection.close()

    migrated = connect_signer_database(path)
    try:
        assert migrated.execute("PRAGMA user_version").fetchone() == (6,)
        assert migrated.execute(
            """
            SELECT name, room_id, room_sha256, created_seq, created_at,
                   first_observed_at, last_listed_at
            FROM room_ledger
            """
        ).fetchone() == (
            "persistent-alpha-room",
            collect.room_identifier("persistent-alpha-room"),
            hashlib.sha256(b"persistent-alpha-room").hexdigest(),
            42,
            "2026-08-29T08:00:00.000000Z",
            "2026-08-29T07:59:59.441996Z",
            None,
        )
        assert migrated.execute(
            "SELECT stage_seconds, success, outcome FROM room_revisits ORDER BY stage_seconds"
        ).fetchall() == [
            (300, 1, "present_at_last_check"),
            (3600, 0, "check_failed"),
            (86400, None, None),
        ]
        metadata = json.loads(
            migrated.execute(
                "SELECT state_json FROM signer_metadata WHERE singleton = 1"
            ).fetchone()[0]
        )
        assert metadata["version"] == 6
        assert metadata["latest_room_listing_observed_at"] is None
        assert migrated.execute(
            """
            SELECT room_ledger.name
            FROM room_search
            JOIN room_ledger ON room_ledger.rowid = room_search.rowid
            WHERE room_search MATCH ?
            """,
            ("alpha",),
        ).fetchall() == [("persistent-alpha-room",)]
    finally:
        migrated.close()


def test_v6_rebuilds_legacy_case_insensitive_room_search(tmp_path):
    path = tmp_path / "signers-v6.sqlite3"
    connection = connect_signer_database(path)
    record_created_rooms(
        connection,
        [
            created_event(
                10,
                "2026-08-30T08:00:00Z",
                "searchable-CaseNeedle-room",
            )
        ],
        "2026-08-30T08:00:01Z",
    )
    connection.executescript(
        """
        DROP TRIGGER room_ledger_ai;
        DROP TRIGGER room_ledger_ad;
        DROP TRIGGER room_ledger_au;
        DROP TABLE room_search;

        CREATE VIRTUAL TABLE room_search USING fts5(
            name,
            content = 'room_ledger',
            content_rowid = 'rowid',
            tokenize = 'trigram'
        );
        INSERT INTO room_search(room_search) VALUES ('rebuild');
        """
    )
    connection.commit()
    connection.close()

    migrated = connect_signer_database(path)
    try:
        schema = migrated.execute(
            "SELECT sql FROM sqlite_master WHERE name = 'room_search'"
        ).fetchone()[0]
        assert "case_sensitive 1" in schema
        assert migrated.execute(
            "SELECT name FROM room_search WHERE room_search MATCH ?",
            ("CaseNeedle",),
        ).fetchall() == [("searchable-CaseNeedle-room",)]
        assert (
            migrated.execute(
                "SELECT name FROM room_search WHERE room_search MATCH ?",
                ("caseneedle",),
            ).fetchall()
            == []
        )
    finally:
        migrated.close()


def test_room_search_uses_fts_and_room_ids_remain_non_unique(signer_store):
    first = created_event(10, "2026-08-30T08:00:00Z", "searchable-alpha-room")
    second = created_event(11, "2026-08-30T08:00:01Z", "searchable-beta-room")
    record_created_rooms(signer_store, [first, second], "2026-08-30T08:00:02Z")
    shared_id = collect.room_identifier(first["name"])
    signer_store.execute(
        "UPDATE room_ledger SET room_id = ? WHERE name = ?",
        (shared_id, second["name"]),
    )

    assert signer_store.execute(
        "SELECT COUNT(*) FROM room_ledger WHERE room_id = ?", (shared_id,)
    ).fetchone() == (2,)
    index_rows = signer_store.execute("PRAGMA index_list('room_ledger')").fetchall()
    room_id_index = next(row for row in index_rows if row[1] == "room_ledger_room_id")
    assert room_id_index[2] == 0
    plan = signer_store.execute(
        """
        EXPLAIN QUERY PLAN
        SELECT room_ledger.name
        FROM room_search
        JOIN room_ledger ON room_ledger.rowid = room_search.rowid
        WHERE room_search MATCH ?
        """,
        ("alpha",),
    ).fetchall()
    details = [row[3] for row in plan]
    assert any("VIRTUAL TABLE INDEX" in detail for detail in details)
    assert not any("SCAN room_ledger" in detail for detail in details)
    assert signer_store.execute(
        "SELECT name FROM room_search WHERE room_search MATCH ?",
        ("alpha",),
    ).fetchall() == [("searchable-alpha-room",)]
    triggers = {
        row[0]
        for row in signer_store.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type = 'trigger' AND tbl_name = 'room_ledger'
            """
        )
    }
    assert triggers == {
        "room_ledger_ai",
        "room_ledger_ad",
        "room_ledger_au",
        "room_ledger_rollup_after_insert",
        "room_ledger_rollup_after_delete",
        "room_ledger_rollup_after_first_observed_update",
    }

    signer_store.execute(
        "DELETE FROM room_revisits WHERE room_created_seq = ?", (second["seq"],)
    )
    signer_store.execute("DELETE FROM room_ledger WHERE name = ?", (second["name"],))
    assert (
        signer_store.execute(
            "SELECT name FROM room_search WHERE room_search MATCH ?",
            ("beta",),
        ).fetchall()
        == []
    )

    renamed = "searchable-gamma-room"
    signer_store.execute(
        "DELETE FROM room_revisits WHERE room_created_seq = ?", (first["seq"],)
    )
    signer_store.execute(
        """
        UPDATE room_ledger
        SET name = ?, room_id = ?, room_sha256 = ?
        WHERE name = ?
        """,
        (
            renamed,
            collect.room_identifier(renamed),
            hashlib.sha256(renamed.encode()).hexdigest(),
            first["name"],
        ),
    )
    assert (
        signer_store.execute(
            "SELECT name FROM room_search WHERE room_search MATCH ?",
            ("alpha",),
        ).fetchall()
        == []
    )
    assert signer_store.execute(
        "SELECT name FROM room_search WHERE room_search MATCH ?",
        ("gamma",),
    ).fetchall() == [(renamed,)]


def test_successful_rooms_listing_updates_latest_listing_state(tmp_path, monkeypatch):
    signer_state_path = tmp_path / "signers.json"
    connection = connect_signer_database(
        collect.signer_database_path(signer_state_path)
    )
    record_created_rooms(
        connection,
        [created_event(1, "2026-08-30T08:00:00Z", "listed-room")],
        "2026-08-30T08:00:01Z",
    )
    connection.commit()
    connection.close()
    tick_ts = "2026-08-30T08:01:00Z"
    monkeypatch.setattr(collect, "utc_now", lambda: tick_ts)

    class ListingClient:
        def get(self, path, deadline=None):
            if path == "/rooms?format=json&limit=200":
                return json.dumps(
                    {
                        "total": 2,
                        "capacity": 100,
                        "bytes": 10,
                        "notes": {"total": 1, "capacity": 100, "bytes": 1},
                        "rooms": [
                            {"name": "lobby", "seq": 1, "idle": 0},
                            {"name": "listed-room", "seq": 1, "idle": 1},
                        ],
                    }
                )
            if path == "/r/events?format=json&limit=200":
                return json.dumps(
                    {
                        "room": "events",
                        "messages": [
                            {
                                "seq": 2,
                                "ts": tick_ts,
                                "from": "server",
                                "text": "created event-only-room",
                            }
                        ],
                    }
                )
            if path.startswith("/r/"):
                return room_body(message(1, "server"))
            raise AssertionError(f"unexpected read: {path}")

    tick = collect.collect_tick(ListingClient(), signer_state_path, 100)

    stored = connect_signer_database(collect.signer_database_path(signer_state_path))
    try:
        assert stored.execute(
            "SELECT name, last_listed_at FROM room_ledger ORDER BY name"
        ).fetchall() == [
            ("event-only-room", None),
            ("listed-room", tick_ts),
        ]
        metadata = json.loads(
            stored.execute(
                "SELECT state_json FROM signer_metadata WHERE singleton = 1"
            ).fetchone()[0]
        )
        assert metadata["latest_room_listing_observed_at"] == tick_ts
    finally:
        stored.close()

    from derive import validate_tick

    assert validate_tick(tick)["signer_funnel"]["signer_state_version"] == 6


def test_collect_tick_does_not_backdate_a_recreated_generation_listing(
    tmp_path,
    monkeypatch,
):
    signer_state_path = tmp_path / "signers.json"
    database_path = collect.signer_database_path(signer_state_path)
    connection = connect_signer_database(database_path)
    record_created_rooms(
        connection,
        [created_event(10, "2026-08-30T08:00:00Z", "reused-room")],
        "2026-08-30T08:00:01Z",
    )
    state = new_signer_state(100)
    state["collection_started"] = "2026-08-30T07:00:00Z"
    state["persistence_started_at"] = "2026-08-30T07:00:00Z"
    collect.write_signer_metadata(connection, state)
    connection.commit()
    connection.close()
    observed_times = iter(("2026-08-30T08:04:00Z", "2026-08-30T08:04:01Z"))
    monkeypatch.setattr(collect, "utc_now", lambda: next(observed_times))

    class RecreationClient:
        def get(self, path, deadline=None):
            if path == "/rooms?format=json&limit=200":
                return json.dumps(
                    {
                        "total": 2,
                        "capacity": 100,
                        "bytes": 10,
                        "notes": {"total": 1, "capacity": 100, "bytes": 1},
                        "rooms": [
                            {"name": "lobby", "seq": 1, "idle": 0},
                            {"name": "reused-room", "seq": 1, "idle": 1},
                        ],
                    }
                )
            if path == "/r/events?format=json&limit=200":
                return json.dumps(
                    {
                        "room": "events",
                        "messages": [
                            {
                                "seq": 20,
                                "ts": "2026-08-30T08:04:00.500000Z",
                                "from": "server",
                                "text": "created reused-room",
                            }
                        ],
                    }
                )
            if path.startswith("/r/"):
                return room_body(message(1, "server"))
            raise AssertionError(f"unexpected read: {path}")

    collect.collect_tick(RecreationClient(), signer_state_path, 100)

    stored = connect_signer_database(database_path)
    try:
        assert stored.execute(
            "SELECT created_seq, last_listed_at FROM room_ledger ORDER BY created_seq"
        ).fetchall() == [(10, "2026-08-30T08:04:00Z"), (20, None)]
    finally:
        stored.close()


def test_failed_cycle_does_not_publish_a_partial_listing_checkpoint(
    tmp_path,
    monkeypatch,
):
    signer_state_path = tmp_path / "signers.json"
    database_path = collect.signer_database_path(signer_state_path)
    connection = connect_signer_database(database_path)
    record_created_rooms(
        connection,
        [created_event(1, "2026-08-30T08:00:00Z", "listed-room")],
        "2026-08-30T08:00:01Z",
    )
    connection.commit()
    connection.close()
    listing_ts = "2026-08-30T08:01:00Z"
    monkeypatch.setattr(collect, "utc_now", lambda: listing_ts)

    class FailedCycleClient:
        def get(self, path, deadline=None):
            if path == "/rooms?format=json&limit=200":
                return json.dumps(
                    {
                        "total": 2,
                        "capacity": 100,
                        "bytes": 10,
                        "notes": {"total": 1, "capacity": 100, "bytes": 1},
                        "rooms": [
                            {"name": "lobby", "seq": 1, "idle": 0},
                            {"name": "listed-room", "seq": 1, "idle": 1},
                        ],
                    }
                )
            raise CollectionError(
                "downstream read failed",
                outcome="http_error",
                status=503,
                path=path,
            )

    with pytest.raises(CollectionError):
        collect.collect_tick(FailedCycleClient(), signer_state_path, 100)

    stored = connect_signer_database(database_path)
    try:
        assert stored.execute(
            "SELECT last_listed_at FROM room_ledger WHERE name = 'listed-room'"
        ).fetchone() == (None,)
        metadata = json.loads(
            stored.execute(
                "SELECT state_json FROM signer_metadata WHERE singleton = 1"
            ).fetchone()[0]
        )
        assert metadata["latest_room_listing_observed_at"] is None
    finally:
        stored.close()


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


def test_signer_update_backfills_missing_legacy_observation_timestamps(signer_store):
    suffix = DID.removeprefix("did:key:z6Mk")
    insert_record(signer_store, suffix, 7, 2, ["room-a"], 1)
    signer_store.execute(
        "UPDATE signer_dids SET "
        "first_observed_ts = NULL, last_observed_ts = NULL, "
        "collection_first_utc_date = NULL, collection_last_utc_date = NULL, "
        "collection_utc_dates_count = 0 "
        "WHERE did = ?",
        (suffix,),
    )
    messages = parse_room_messages(
        room_body(message(1, DID, nonce="1")),
        "/r/test",
    )

    update_signer_state(
        signer_store,
        new_signer_state(1),
        [("room-a", messages)],
        "2026-08-30T08:00:00Z",
    )

    assert signer_store.execute(
        "SELECT first_observed_ts, last_observed_ts, tick_count, "
        "collection_first_utc_date, collection_last_utc_date, "
        "collection_utc_dates_count, rooms_json, has_counterparty "
        "FROM signer_dids WHERE did = ?",
        (suffix,),
    ).fetchone() == (
        "2026-08-30T08:00:00Z",
        "2026-08-30T08:00:00Z",
        8,
        "2026-08-30",
        "2026-08-30",
        1,
        '["room-a"]',
        1,
    )


@pytest.mark.parametrize(
    ("first_observed_ts", "last_observed_ts"),
    (
        (None, "2026-08-29T08:00:00Z"),
        ("2026-08-28T08:00:00Z", None),
    ),
)
def test_signer_update_rejects_a_partial_observation_timestamp_pair(
    signer_store,
    first_observed_ts,
    last_observed_ts,
):
    suffix = DID.removeprefix("did:key:z6Mk")
    insert_record(signer_store, suffix, 3, 2, ["room-a"], 0)
    signer_store.execute(
        "UPDATE signer_dids SET first_observed_ts = ?, last_observed_ts = ? WHERE did = ?",
        (first_observed_ts, last_observed_ts, suffix),
    )
    messages = parse_room_messages(
        room_body(message(1, DID, nonce="1")),
        "/r/test",
    )

    with pytest.raises(CollectionError, match="inconsistent observation timestamps"):
        update_signer_state(
            signer_store,
            new_signer_state(1),
            [("room-a", messages)],
            "2026-08-30T08:00:00Z",
        )


def test_signer_update_rejects_a_tick_before_the_last_observation(signer_store):
    messages = parse_room_messages(room_body(message(1, DID, nonce="1")), "/r/test")
    state = new_signer_state(1)
    update_signer_state(
        signer_store,
        state,
        [("test", messages)],
        "2026-08-30T08:00:00Z",
    )
    update_signer_state(
        signer_store,
        state,
        [("test", messages)],
        "2026-08-30T09:00:00Z",
    )
    before = signer_store.execute(
        "SELECT first_observed_ts, last_observed_ts, tick_count FROM signer_dids"
    ).fetchone()
    state["last_updated"] = "2026-08-30T08:00:00Z"

    with pytest.raises(CollectionError, match="signer database"):
        update_signer_state(
            signer_store,
            state,
            [("test", messages)],
            "2026-08-30T08:30:00Z",
        )

    assert (
        signer_store.execute(
            "SELECT first_observed_ts, last_observed_ts, tick_count FROM signer_dids"
        ).fetchone()
        == before
    )


def test_signer_update_rejects_a_tick_before_the_global_checkpoint(signer_store):
    state = new_signer_state(1)
    update_signer_state(signer_store, state, [], "2026-08-30T09:00:00Z")

    with pytest.raises(CollectionError, match="signer update"):
        update_signer_state(signer_store, state, [], "2026-08-30T08:30:00Z")

    assert state["last_updated"] == "2026-08-30T09:00:00Z"


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
    assert (
        "understate that cohort until those counters rebuild" in disclosure["warning"]
    )
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
    state.pop("latest_room_listing_observed_at")
    state["dids"] = {
        "one": {
            "first_observed_ts": None,
            "last_observed_ts": None,
            "tick_count": 1,
            "collection_first_utc_date": None,
            "collection_last_utc_date": None,
            "collection_utc_dates_count": 0,
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

    assert (
        source_counts
        == sqlite_counts
        == {
            "observed": 5,
            "two_ticks": 4,
            "two_collection_dates": 3,
            "two_rooms": 2,
            "counterparties": 1,
        }
    )
    migrated_metadata = json.loads(metadata.read_text(encoding="utf-8"))
    assert migrated_metadata["version"] == collect.SIGNER_STATE_VERSION
    assert "dids" not in migrated_metadata
    assert migrated_metadata["selector_seed"] == state["selector_seed"]
    assert migrated_metadata["selector_epoch"] == state["selector_epoch"]
    assert migrated_metadata["latest_room_listing_observed_at"] is None
    migrated = sqlite3.connect(collect.signer_database_path(metadata))
    try:
        assert migrated.execute(
            "SELECT first_observed_ts, last_observed_ts, tick_count, "
            "collection_first_utc_date, collection_last_utc_date, "
            "collection_utc_dates_count FROM signer_dids WHERE did = 'one'"
        ).fetchone() == (None, None, 1, None, None, 0)
    finally:
        migrated.close()


@pytest.mark.parametrize(
    "invalid_value",
    (
        pytest.param(HUGE_JSON_INTEGER, id="integer-parser-limit"),
        pytest.param(str(collect.SQLITE_INTEGER_MAX + 1), id="sqlite-integer-limit"),
    ),
)
def test_v2_migration_rejects_unbounded_tick_counts(tmp_path, invalid_value):
    state = new_signer_state(10)
    state["version"] = 2
    state.pop("latest_room_listing_observed_at")
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
        }
    }
    payload = json.dumps(state).replace(
        '"tick_count": 1', f'"tick_count": {invalid_value}'
    )
    source = tmp_path / "signers-v2.json"
    metadata = tmp_path / "signers.json"
    source.write_text(payload, encoding="utf-8")

    with pytest.raises(CollectionError, match="signer JSON"):
        migrate_signers(source, metadata)

    assert not metadata.exists()
    assert not collect.signer_database_path(metadata).exists()


def test_v2_migration_normalizes_recursive_json_failure(tmp_path):
    state = new_signer_state(10)
    state["version"] = 2
    state.pop("latest_room_listing_observed_at")
    state["dids"] = {}
    payload = json.dumps(state)[:-1] + ',"nested":' + DEEPLY_NESTED_JSON + "}"
    source = tmp_path / "signers-v2.json"
    metadata = tmp_path / "signers.json"
    source.write_text(payload, encoding="utf-8")

    with (
        mock.patch.object(json.JSONDecoder, "raw_decode", side_effect=RecursionError),
        pytest.raises(CollectionError, match="signer JSON"),
    ):
        migrate_signers(source, metadata)

    assert not metadata.exists()
    assert not collect.signer_database_path(metadata).exists()


def test_migration_post_publish_broken_pipe_keeps_both_outputs(
    tmp_path,
    monkeypatch,
):
    state = new_signer_state(10)
    state["version"] = 2
    state.pop("latest_room_listing_observed_at")
    state["dids"] = {}
    source = tmp_path / "signers-v2.json"
    metadata = tmp_path / "signers.json"
    source.write_text(json.dumps(state), encoding="utf-8")

    def fail_on_published_path(*values, **kwargs):
        if values and str(values[0]).startswith("metadata:"):
            raise BrokenPipeError("stdout closed after publication")

    monkeypatch.setattr("builtins.print", fail_on_published_path)

    with pytest.raises(BrokenPipeError, match="after publication"):
        migrate_signers(source, metadata)

    database = collect.signer_database_path(metadata)
    assert metadata.is_file()
    assert database.is_file()
    connection = sqlite3.connect(database)
    try:
        assert connection.execute("PRAGMA user_version").fetchone() == (6,)
    finally:
        connection.close()


def test_census_main_publishes_the_committed_outbox_after_collection(
    tmp_path,
    monkeypatch,
):
    output = tmp_path / "ticks.jsonl"
    state_path = tmp_path / "census.json"
    started_at = "2026-08-30T00:00:00Z"
    census_run = {
        "shard_read_failures": 0,
        "stop_reason": "complete",
        "shards_outstanding": 0,
        "failure_causes": {},
    }
    events = []
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "collect.py",
            "--base-url",
            "https://example.invalid",
            "--output",
            str(output),
            "--census-state",
            str(state_path),
            "--census",
        ],
    )
    monkeypatch.setattr(
        collect,
        "run_census",
        lambda client, path, pace: (7, started_at, census_run),
    )

    def collect_tick(*args, **kwargs):
        events.append("collect")
        return {"tick": 1}

    def drain(path, signer_state, census_state, lock_timeout):
        assert path == output
        assert census_state == state_path
        assert lock_timeout == collect.CENSUS_SIGNER_LOCK_TIMEOUT
        if not events:
            events.append("drain-empty")
            return False
        assert events == ["drain-empty", "collect"]
        events.append("drain-publish")
        return True

    monkeypatch.setattr(collect, "collect_tick", collect_tick)
    monkeypatch.setattr(collect, "drain_tick_outbox", drain)

    assert collect.main() == 0
    assert events == ["drain-empty", "collect", "drain-publish"]


def test_census_main_fails_when_the_committed_outbox_cannot_be_published(
    tmp_path,
    monkeypatch,
):
    output = tmp_path / "ticks.jsonl"
    started_at = "2026-08-30T00:00:00Z"
    census_run = {
        "shard_read_failures": 0,
        "stop_reason": "complete",
        "shards_outstanding": 0,
        "failure_causes": {},
    }
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "collect.py",
            "--base-url",
            "https://example.invalid",
            "--output",
            str(output),
            "--census",
        ],
    )
    monkeypatch.setattr(
        collect,
        "run_census",
        lambda client, path, pace: (7, started_at, census_run),
    )
    monkeypatch.setattr(collect, "collect_tick", lambda *args, **kwargs: {"tick": 1})

    drain_calls = 0

    def fail_publication(*args, **kwargs):
        nonlocal drain_calls
        drain_calls += 1
        if drain_calls == 1:
            return False
        raise OSError("outbox publication failed")

    monkeypatch.setattr(collect, "drain_tick_outbox", fail_publication)

    assert collect.main() == 1
    assert drain_calls == 2


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
            "60",
        ],
    )

    attempts = []

    def collect_once_then_succeed(*args, **kwargs):
        attempts.append((args, kwargs))
        if len(attempts) == 1:
            raise sqlite3.OperationalError("database is locked")
        return {"tick": len(attempts)}

    drain_results = iter((False, False, True))
    drain_calls = []
    monkeypatch.setattr(collect, "collect_tick", collect_once_then_succeed)

    def drain(path, signer_state, census_state, lock_timeout):
        drain_calls.append((path, signer_state, census_state, lock_timeout))
        return next(drain_results)

    monkeypatch.setattr(collect, "drain_tick_outbox", drain)

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
    assert len(drain_calls) == 3
    assert all(call[0] == output for call in drain_calls)
    assert "collection failed; no tick written: database is locked" in (
        capsys.readouterr().err
    )


def test_state_lock_degrades_to_a_noop_where_fcntl_is_unavailable(
    tmp_path, monkeypatch
):
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
