import json
import sqlite3
from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import pytest

import derive
import snapshots
from api_contract import MAX_RESPONSE_BYTES, json_bytes, text_bytes
from snapshots import build_snapshots, change_resource, load_telemetry


def tick(ts, *, event_seq=30_000, rooms=100, notes=1_000, lobby=5_000):
    return {
        "collector_version": "2.10.0",
        "ts": ts,
        "rooms_total": rooms,
        "room_cap": 40_960,
        "bytes_stored": 241_200_000,
        "notes_total": notes,
        "note_cap": 1_310_720,
        "lobby_last_seq": lobby,
        "events_last_seq": event_seq,
        "identity_total": None,
        "identity_census_started": None,
        "identity_census_run": None,
        "events_window": [
            {
                "seq": event_seq,
                "ts": ts,
                "name": "baseline",
                "primary_class": "human_or_other",
                "base_name": "baseline",
            }
        ],
        "newest_rooms": [],
        "room_sampling": None,
        "signer_funnel": None,
        "room_lifecycle": None,
    }


@pytest.mark.parametrize(
    "timestamp",
    (
        pytest.param("0001-01-01T00:00:00+14:00", id="utc-underflow"),
        pytest.param("9999-12-31T23:59:59-23:59", id="utc-overflow"),
    ),
)
def test_parse_utc_normalizes_datetime_overflow(timestamp):
    with pytest.raises(ValueError, match="supported range"):
        snapshots.parse_utc(timestamp)


def test_snapshot_time_arithmetic_normalizes_datetime_overflow():
    with pytest.raises(ValueError, match="validity window exceeds"):
        snapshots.valid_until("9999-12-31T23:59:59Z")

    with pytest.raises(ValueError, match="status window exceeds"):
        snapshots.endpoint_summaries([], "0001-01-01T00:00:00Z")


def write_ticks(path: Path, *records: dict) -> None:
    path.write_text(
        "".join(json.dumps(record) + "\n" for record in records),
        encoding="utf-8",
    )


def telemetry_database(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        PRAGMA foreign_keys = ON;
        CREATE TABLE cycles (
            id INTEGER PRIMARY KEY,
            source TEXT NOT NULL,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            outcome TEXT NOT NULL,
            error_outcome TEXT
        );
        CREATE TABLE request_attempts (
            id INTEGER PRIMARY KEY,
            cycle_id INTEGER NOT NULL REFERENCES cycles(id),
            route TEXT NOT NULL,
            metered INTEGER NOT NULL,
            attempt INTEGER NOT NULL,
            observed_at TEXT NOT NULL,
            latency_ms REAL NOT NULL,
            outcome TEXT NOT NULL,
            http_status INTEGER
        );
        CREATE TABLE discovery_snapshots (
            id INTEGER PRIMARY KEY,
            attempt_id INTEGER NOT NULL UNIQUE REFERENCES request_attempts(id),
            route TEXT NOT NULL,
            observed_at TEXT NOT NULL,
            digest_sha256 TEXT NOT NULL,
            fields_json TEXT NOT NULL
        );
        PRAGMA user_version = 1;
        """
    )
    return connection


def add_attempt(
    connection,
    *,
    attempt_id,
    route,
    observed_at,
    outcome,
    status=None,
    source="pulse_probe",
    cycle_outcome="success",
    latency_ms=12.5,
):
    connection.execute(
        "INSERT INTO cycles VALUES (?, ?, ?, ?, ?, ?)",
        (
            attempt_id,
            source,
            observed_at,
            observed_at,
            cycle_outcome,
            None if cycle_outcome == "success" else outcome,
        ),
    )
    connection.execute(
        "INSERT INTO request_attempts VALUES (?, ?, ?, 0, 1, ?, ?, ?, ?)",
        (attempt_id, attempt_id, route, observed_at, latency_ms, outcome, status),
    )


def test_status_counts_503_attempts_instead_of_reporting_zero_activity(tmp_path):
    ticks = tmp_path / "ticks.jsonl"
    telemetry = tmp_path / "telemetry.sqlite3"
    write_ticks(
        ticks,
        tick("2026-08-30T00:00:00Z"),
        tick("2026-08-30T00:02:00Z", event_seq=30_004, rooms=104),
    )
    with telemetry_database(telemetry) as connection:
        add_attempt(
            connection,
            attempt_id=1,
            route="/healthz",
            observed_at="2026-08-30T00:01:00Z",
            outcome="success",
            status=200,
        )
        add_attempt(
            connection,
            attempt_id=2,
            route="/rooms",
            observed_at="2026-08-30T00:02:00Z",
            outcome="http_error",
            status=503,
        )

    result = build_snapshots(
        ticks,
        telemetry,
        derived_at="2026-08-30T00:02:30Z",
        published_at="2026-08-30T00:03:00Z",
    )["status"]
    endpoints = {entry["route"]: entry for entry in result["status"]["endpoints"]}

    assert "overall" not in result["status"]
    assert result["status"]["origin"]["state"] == "reachable"
    assert result["status"]["collector"]["state"] == "fresh"
    assert endpoints["/rooms"]["attempts"] == 1
    assert endpoints["/rooms"]["successes"] == 0
    assert endpoints["/rooms"]["server_errors"] == 1
    assert endpoints["/rooms"]["observed_failures"] == 1


def test_snapshot_rejects_a_source_observation_after_derivation(tmp_path):
    ticks = tmp_path / "ticks.jsonl"
    telemetry = tmp_path / "telemetry.sqlite3"
    write_ticks(ticks, tick("2026-08-30T00:00:00Z"))
    with telemetry_database(telemetry) as connection:
        add_attempt(
            connection,
            attempt_id=1,
            route="/healthz",
            observed_at="2026-08-30T00:00:02Z",
            outcome="success",
            status=200,
        )

    with pytest.raises(ValueError, match="source observation postdates derivation"):
        build_snapshots(
            ticks,
            telemetry,
            derived_at="2026-08-30T00:00:01Z",
            published_at="2026-08-30T00:00:03Z",
        )


def test_snapshot_rejects_publication_before_derivation(tmp_path):
    ticks = tmp_path / "ticks.jsonl"
    telemetry = tmp_path / "telemetry.sqlite3"
    write_ticks(ticks, tick("2026-08-30T00:00:00Z"))
    telemetry_database(telemetry).close()

    with pytest.raises(ValueError, match="publication predates derivation"):
        build_snapshots(
            ticks,
            telemetry,
            derived_at="2026-08-30T00:00:02Z",
            published_at="2026-08-30T00:00:01Z",
        )


def test_incidents_resolve_health_5xx_and_collector_gap_intervals(tmp_path):
    ticks = tmp_path / "ticks.jsonl"
    telemetry = tmp_path / "telemetry.sqlite3"
    write_ticks(
        ticks,
        tick("2026-08-30T00:00:00Z"),
        tick("2026-08-30T00:10:00Z", event_seq=30_010, rooms=110),
    )
    with telemetry_database(telemetry) as connection:
        add_attempt(
            connection,
            attempt_id=1,
            route="/healthz",
            observed_at="2026-08-30T00:01:00Z",
            outcome="timeout",
            cycle_outcome="failure",
        )
        add_attempt(
            connection,
            attempt_id=2,
            route="/healthz",
            observed_at="2026-08-30T00:02:00Z",
            outcome="success",
            status=200,
        )
        add_attempt(
            connection,
            attempt_id=3,
            route="/rooms",
            observed_at="2026-08-30T00:03:00Z",
            outcome="http_error",
            status=503,
        )
        add_attempt(
            connection,
            attempt_id=4,
            route="/rooms",
            observed_at="2026-08-30T00:04:00Z",
            outcome="timeout",
        )
        add_attempt(
            connection,
            attempt_id=5,
            route="/rooms",
            observed_at="2026-08-30T00:05:00Z",
            outcome="http_error",
            status=404,
        )
        add_attempt(
            connection,
            attempt_id=6,
            route="/rooms",
            observed_at="2026-08-30T00:06:00Z",
            outcome="success",
            status=200,
        )

    incidents = build_snapshots(
        ticks,
        telemetry,
        derived_at="2026-08-30T00:10:10Z",
        published_at="2026-08-30T00:11:00Z",
    )["incidents"]["incidents"]
    by_rule = {incident["rule"]: incident for incident in incidents}

    assert by_rule["health_probe_failed"]["state"] == "resolved"
    assert by_rule["health_probe_failed"]["resolved_at"] == "2026-08-30T00:02:00Z"
    assert by_rule["endpoint_5xx"]["route"] == "/rooms"
    assert by_rule["endpoint_5xx"]["attempts"] == 4
    assert by_rule["endpoint_5xx"]["observed_failures"] == 3
    assert "cause" not in by_rule["endpoint_5xx"]
    assert by_rule["collector_gap"]["state"] == "resolved"
    assert by_rule["collector_gap"]["window_seconds"] == 600


def test_collector_gap_opens_after_cadence_and_resolves_only_on_next_tick(tmp_path):
    ticks = tmp_path / "ticks.jsonl"
    telemetry = tmp_path / "telemetry.sqlite3"
    write_ticks(ticks, tick("2026-08-30T00:00:00Z"))
    connection = telemetry_database(telemetry)
    connection.close()

    open_result = build_snapshots(
        ticks,
        telemetry,
        derived_at="2026-08-30T00:06:00Z",
        published_at="2026-08-30T00:06:00Z",
        gap_seconds=300,
    )["incidents"]["incidents"]
    open_gap = next(item for item in open_result if item["rule"] == "collector_gap")

    assert open_gap["state"] == "open"
    assert open_gap["opened_at"] == "2026-08-30T00:05:00Z"
    assert open_gap["last_observed_at"] == "2026-08-30T00:06:00Z"
    assert open_gap["resolved_at"] is None

    write_ticks(
        ticks,
        tick("2026-08-30T00:00:00Z"),
        tick("2026-08-30T00:07:00Z", event_seq=30_007, rooms=107),
    )
    resolved_result = build_snapshots(
        ticks,
        telemetry,
        derived_at="2026-08-30T00:07:10Z",
        published_at="2026-08-30T00:07:10Z",
        gap_seconds=300,
    )["incidents"]["incidents"]
    resolved_gap = next(
        item for item in resolved_result if item["rule"] == "collector_gap"
    )

    assert resolved_gap["id"] == open_gap["id"]
    assert resolved_gap["state"] == "resolved"
    assert resolved_gap["resolved_at"] == "2026-08-30T00:07:00Z"


def test_incident_history_does_not_silently_drop_ticks_before_50000():
    started = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ticks = []
    current = started
    for index in range(50_002):
        if index == 1:
            current += timedelta(minutes=10)
        elif index:
            current += timedelta(seconds=1)
        ticks.append(
            {
                "ts": current.isoformat(timespec="seconds").replace("+00:00", "Z"),
                "_datetime": current,
            }
        )

    incidents, _, coverage = snapshots.incident_resource(
        ticks,
        [],
        300,
        ticks[-1]["ts"],
    )

    assert any(item["rule"] == "collector_gap" for item in incidents)
    assert coverage["tick_input"] == {
        "records_total": 50_002,
        "records_loaded": 50_002,
        "records_omitted_before_cutoff": 0,
        "cutoff_observed_at": None,
        "truncated": False,
        "continuity": "complete",
    }


def test_load_telemetry_reads_cycles_attempts_and_discoveries_in_one_snapshot(
    tmp_path, monkeypatch
):
    telemetry = tmp_path / "telemetry.sqlite3"
    setup = telemetry_database(telemetry)
    setup.execute("PRAGMA journal_mode = WAL")
    setup.close()
    reader = sqlite3.connect(telemetry)
    writer = sqlite3.connect(telemetry)

    class InjectingConnection:
        def __init__(self):
            self.injected = False

        @property
        def row_factory(self):
            return reader.row_factory

        @row_factory.setter
        def row_factory(self, value):
            reader.row_factory = value

        def execute(self, sql, parameters=()):
            cursor = reader.execute(sql, parameters)
            if "FROM cycles" in sql and not self.injected:
                self.injected = True
                writer.execute(
                    "INSERT INTO cycles VALUES (1, 'pulse_probe', ?, ?, 'success', NULL)",
                    ("2026-08-30T00:01:00Z", "2026-08-30T00:01:00Z"),
                )
                writer.execute(
                    "INSERT INTO request_attempts VALUES (1, 1, '/healthz', 0, 1, ?, 8.5, 'success', 200)",
                    ("2026-08-30T00:01:00Z",),
                )
                writer.commit()
            return cursor

        def close(self):
            reader.close()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            self.close()

    proxy = InjectingConnection()
    monkeypatch.setattr(snapshots.sqlite3, "connect", lambda *args, **kwargs: proxy)
    try:
        result = load_telemetry(telemetry)
    finally:
        writer.close()

    assert result["cycles"] == []
    assert result["attempts"] == []
    assert result["discoveries"] == []
    assert result["attempt_carry"] == {
        "health_probe_failed": None,
        "endpoint_5xx": {},
    }
    assert result["discovery_predecessors"] == {}
    assert result["attempt_input"] == {
        "records_total": 0,
        "records_loaded": 0,
        "records_omitted_before_cutoff": 0,
        "cutoff_observed_at": None,
        "truncated": False,
        "continuity": "complete",
    }
    assert result["discovery_input"] == {
        "records_total": 0,
        "records_loaded": 0,
        "records_omitted_before_cutoff": 0,
        "cutoff_observed_at": None,
        "truncated": False,
        "continuity": "complete",
        "predecessor_records_loaded": 0,
    }


@pytest.mark.parametrize(
    ("latency_ms", "status", "message"),
    (
        pytest.param(float("inf"), 200, "latency_ms", id="infinite-latency"),
        pytest.param(12.5, float("inf"), "http_status", id="infinite-status"),
    ),
)
def test_load_telemetry_rejects_non_finite_dynamic_values(
    tmp_path,
    latency_ms,
    status,
    message,
):
    telemetry = tmp_path / "telemetry.sqlite3"
    with telemetry_database(telemetry) as connection:
        add_attempt(
            connection,
            attempt_id=1,
            route="/healthz",
            observed_at="2026-08-30T00:01:00Z",
            outcome="success",
            status=status,
            latency_ms=latency_ms,
        )

    with pytest.raises(ValueError, match=message):
        load_telemetry(telemetry)


def test_attempt_cap_carries_active_incident_identity_and_discloses_cutoff(
    tmp_path,
    monkeypatch,
):
    ticks = tmp_path / "ticks.jsonl"
    telemetry = tmp_path / "telemetry.sqlite3"
    write_ticks(ticks, tick("2026-08-30T00:05:00Z"))
    monkeypatch.setattr(snapshots, "MAX_ATTEMPTS", 2)
    with telemetry_database(telemetry) as connection:
        for attempt_id, route, observed_at, outcome, status in (
            (1, "/healthz", "2026-08-30T00:01:00Z", "timeout", None),
            (2, "/rooms", "2026-08-30T00:02:00Z", "http_error", 503),
            (3, "/config", "2026-08-30T00:03:00Z", "success", 200),
            (4, "/healthz", "2026-08-30T00:04:00Z", "timeout", None),
            (5, "/rooms", "2026-08-30T00:05:00Z", "timeout", None),
        ):
            add_attempt(
                connection,
                attempt_id=attempt_id,
                route=route,
                observed_at=observed_at,
                outcome=outcome,
                status=status,
                cycle_outcome="success" if outcome == "success" else "failure",
            )

    result = build_snapshots(
        ticks,
        telemetry,
        derived_at="2026-08-30T00:05:10Z",
        published_at="2026-08-30T00:05:30Z",
    )
    incidents = {item["rule"]: item for item in result["incidents"]["incidents"]}

    assert incidents["health_probe_failed"]["opened_at"] == "2026-08-30T00:01:00Z"
    assert incidents["health_probe_failed"]["attempts"] == 2
    assert incidents["endpoint_5xx"]["opened_at"] == "2026-08-30T00:02:00Z"
    assert incidents["endpoint_5xx"]["attempts"] == 2
    assert incidents["endpoint_5xx"]["observed_failures"] == 2
    expected_input = {
        "records_total": 5,
        "records_loaded": 2,
        "records_omitted_before_cutoff": 3,
        "cutoff_observed_at": "2026-08-30T00:04:00Z",
        "truncated": True,
        "continuity": "active_incident_state_carried",
    }
    assert result["incidents"]["coverage"]["attempt_input"] == expected_input
    assert result["status"]["coverage"]["attempt_input"] == expected_input


def test_discovery_changes_publish_allowlisted_old_and_new_values(tmp_path):
    ticks = tmp_path / "ticks.jsonl"
    telemetry = tmp_path / "telemetry.sqlite3"
    write_ticks(ticks, tick("2026-08-30T00:00:00Z"))
    with telemetry_database(telemetry) as connection:
        for attempt_id, observed_at, rooms in (
            (1, "2026-08-30T00:01:00Z", 40_960),
            (2, "2026-08-30T01:01:00Z", 50_000),
        ):
            add_attempt(
                connection,
                attempt_id=attempt_id,
                route="/config",
                observed_at=observed_at,
                outcome="success",
                status=200,
            )
            fields = {"settings": {"max_rooms": rooms, "static_cache_seconds": 30}}
            connection.execute(
                "INSERT INTO discovery_snapshots VALUES (?, ?, ?, ?, ?, ?)",
                (
                    attempt_id,
                    attempt_id,
                    "/config",
                    observed_at,
                    f"{attempt_id:064x}",
                    json.dumps(fields),
                ),
            )

    changes = build_snapshots(
        ticks,
        telemetry,
        derived_at="2026-08-30T01:01:10Z",
        published_at="2026-08-30T01:02:00Z",
    )["changes"]["changes"]

    assert changes == [
        {
            "field": "settings.max_rooms",
            "old": 40_960,
            "new": 50_000,
            "first_observed_at": "2026-08-30T01:01:00Z",
            "source_route": "/config",
            "interpretation_affected": True,
            "methodology_version": "1.0.0",
        }
    ]


def test_discovery_cap_loads_route_predecessor_and_discloses_cutoff(
    tmp_path,
    monkeypatch,
):
    ticks = tmp_path / "ticks.jsonl"
    telemetry = tmp_path / "telemetry.sqlite3"
    write_ticks(ticks, tick("2026-08-30T00:03:00Z"))
    monkeypatch.setattr(snapshots, "MAX_DISCOVERY_SNAPSHOTS", 2)
    with telemetry_database(telemetry) as connection:
        for attempt_id, observed_at, rooms in (
            (1, "2026-08-30T00:01:00Z", 40_000),
            (2, "2026-08-30T00:02:00Z", 50_000),
            (3, "2026-08-30T00:03:00Z", 60_000),
        ):
            add_attempt(
                connection,
                attempt_id=attempt_id,
                route="/config",
                observed_at=observed_at,
                outcome="success",
                status=200,
            )
            connection.execute(
                "INSERT INTO discovery_snapshots VALUES (?, ?, ?, ?, ?, ?)",
                (
                    attempt_id,
                    attempt_id,
                    "/config",
                    observed_at,
                    f"{attempt_id:064x}",
                    json.dumps({"settings": {"max_rooms": rooms}}),
                ),
            )

    result = build_snapshots(
        ticks,
        telemetry,
        derived_at="2026-08-30T00:03:10Z",
        published_at="2026-08-30T00:03:30Z",
    )["changes"]

    assert [
        (item["old"], item["new"], item["first_observed_at"])
        for item in reversed(result["changes"])
    ] == [
        (40_000, 50_000, "2026-08-30T00:02:00Z"),
        (50_000, 60_000, "2026-08-30T00:03:00Z"),
    ]
    assert result["coverage"]["discovery_input"] == {
        "records_total": 3,
        "records_loaded": 2,
        "records_omitted_before_cutoff": 1,
        "cutoff_observed_at": "2026-08-30T00:02:00Z",
        "truncated": True,
        "continuity": "predecessor_snapshots_carried",
        "predecessor_records_loaded": 1,
    }


def test_discovery_changes_distinguish_explicit_null_from_field_absence():
    changes, _, _ = change_resource(
        [
            {
                "route": "/config",
                "observed_at": "2026-08-30T00:00:00Z",
                "fields_json": json.dumps({"service": None}),
            },
            {
                "route": "/config",
                "observed_at": "2026-08-30T01:00:00Z",
                "fields_json": json.dumps({}),
            },
            {
                "route": "/config",
                "observed_at": "2026-08-30T02:00:00Z",
                "fields_json": json.dumps({"service": None}),
            },
        ]
    )

    assert [(item["old"], item["new"]) for item in reversed(changes)] == [
        (None, {"state": "field_absent"}),
        ({"state": "field_absent"}, None),
    ]


@pytest.mark.parametrize(
    ("location", "fields_json", "message", "decoder_recursion"),
    (
        pytest.param(
            "predecessor",
            '{"service":' + "1" * 4_301 + "}",
            "discovery predecessor fields_json must contain valid JSON",
            False,
            id="predecessor-integer-limit",
        ),
        pytest.param(
            "predecessor",
            "[" * 5_000 + "0" + "]" * 5_000,
            "discovery predecessor fields_json must contain valid JSON",
            True,
            id="predecessor-recursion-limit",
        ),
        pytest.param(
            "snapshot",
            '{"service":' + "1" * 4_301 + "}",
            "discovery fields_json must contain valid JSON",
            False,
            id="snapshot-integer-limit",
        ),
        pytest.param(
            "snapshot",
            "[" * 5_000 + "0" + "]" * 5_000,
            "discovery fields_json must contain valid JSON",
            True,
            id="snapshot-recursion-limit",
        ),
    ),
)
def test_discovery_json_parser_failures_are_normalized(
    location,
    fields_json,
    message,
    decoder_recursion,
):
    discovery = {
        "route": "/config",
        "observed_at": "2026-08-30T00:00:00Z",
        "fields_json": fields_json,
    }
    discoveries = [discovery] if location == "snapshot" else []
    predecessors = {"/config": discovery} if location == "predecessor" else None

    decoder = (
        mock.patch.object(json, "loads", side_effect=RecursionError)
        if decoder_recursion
        else nullcontext()
    )
    with decoder, pytest.raises(ValueError, match=message):
        change_resource(discoveries, predecessors=predecessors)


@pytest.mark.parametrize("location", ("predecessor", "snapshot"))
@pytest.mark.parametrize(
    "fields_json",
    (
        pytest.param('{"service":NaN}', id="non-standard-nan"),
        pytest.param('{"service":"\\ud800"}', id="lone-surrogate"),
    ),
)
def test_discovery_non_serializable_json_is_normalized(location, fields_json):
    discovery = {
        "route": "/config",
        "observed_at": "2026-08-30T00:00:00Z",
        "fields_json": fields_json,
    }
    discoveries = [discovery] if location == "snapshot" else []
    predecessors = {"/config": discovery} if location == "predecessor" else None

    with pytest.raises(ValueError, match="fields_json must contain valid JSON"):
        change_resource(discoveries, predecessors=predecessors)


@pytest.mark.parametrize("field", ("rooms_total", "identity_total"))
def test_unrepresentable_persisted_counter_is_rejected_before_rate_math(
    tmp_path,
    field,
):
    ticks = tmp_path / "ticks.jsonl"
    telemetry = tmp_path / "telemetry.sqlite3"
    first = tick("2026-08-30T00:00:00Z", rooms=0)
    second = tick("2026-08-30T00:01:00Z", rooms=0)
    first[field] = 0
    second[field] = 10**4000
    write_ticks(ticks, first, second)
    with telemetry_database(telemetry):
        pass

    status = build_snapshots(
        ticks,
        telemetry,
        derived_at="2026-08-30T00:01:10Z",
        published_at="2026-08-30T00:01:30Z",
    )["status"]["status"]["collector"]

    assert status["accepted_ticks"] == 1
    assert status["rejected_ticks"] == 1


def test_deeply_nested_persisted_json_is_counted_as_a_rejected_tick(tmp_path):
    ticks_path = tmp_path / "ticks.jsonl"
    ticks_path.write_text("[" * 5_000 + "0" + "]" * 5_000 + "\n", encoding="utf-8")

    ticks, rejected = snapshots.load_ticks(ticks_path)

    assert ticks == []
    assert rejected == 1


@pytest.mark.parametrize(
    "timestamp",
    (
        pytest.param("0001-01-01T00:00:00+14:00", id="utc-underflow"),
        pytest.param("9999-12-31T23:59:59-23:59", id="utc-overflow"),
    ),
)
def test_persisted_tick_timestamp_normalization_overflow_is_rejected(
    tmp_path,
    timestamp,
):
    ticks_path = tmp_path / "ticks.jsonl"
    write_ticks(ticks_path, tick(timestamp))

    ticks, rejected = snapshots.load_ticks(ticks_path)

    assert ticks == []
    assert rejected == 1


@pytest.mark.parametrize(
    "timestamp",
    (
        pytest.param("0001-01-01T00:00:00Z", id="retention-underflow"),
        pytest.param("9999-12-31T23:59:59Z", id="rollup-overflow"),
    ),
)
def test_persisted_tick_timestamp_must_support_declared_history_windows(
    tmp_path,
    timestamp,
):
    ticks_path = tmp_path / "ticks.jsonl"
    write_ticks(ticks_path, tick(timestamp))

    ticks, rejected = snapshots.load_ticks(ticks_path)

    assert ticks == []
    assert rejected == 1


def test_huge_engagement_integers_remain_not_recorded_without_raising():
    engagement = snapshots.derive.validate_engagement(
        {
            "windowed_note_to_message_ratio": 10**4000,
            "zero_response_share": 10**4000,
            "nick_diversity": 10**4000,
            "window_cap": 10**4000,
            "windowed_messages": 10**4000,
        }
    )

    assert engagement == {
        "windowed_note_to_message_ratio": None,
        "zero_response_share": None,
        "nick_diversity": None,
        "window_cap": None,
        "windowed_messages": None,
    }


def test_methodology_discloses_current_room_name_policy_and_history_boundary():
    methodology = snapshots.methodology_resource()

    assert methodology["history_boundary"] == (
        "Repository-backed methodology history begins at 1.3.0; absent version "
        "numbers and earlier revision details are not inferred."
    )
    history = methodology["change_history"]
    assert [entry["version"] for entry in history] == [
        "1.14.0",
        "1.13.0",
        "1.12.0",
        "1.11.0",
        "1.10.0",
        "1.8.0",
        "1.7.0",
        "1.6.0",
        "1.5.0",
        "1.4.0",
        "1.3.0",
    ]
    assert all(
        set(entry) == {"version", "published_on", "changes", "limitations"}
        for entry in history
    )
    # The newest entry must describe the version the page is stamping, or a
    # reader cannot see what the live methodology changed.
    assert history[0]["version"] == derive.METHODOLOGY_VERSION
    assert history[0] == {
        "version": "1.14.0",
        "published_on": "2026-09-01",
        "changes": [
            "Published how scheduled room checks are sampled: the deterministic "
            "selection descriptor and its read budget, per-stage coverage as "
            "completed checks over eligible rooms, and the count of eligible "
            "checks that aged out without a timely attempt."
        ],
        "limitations": [
            "Per-stage coverage counts every scheduled check whose window has "
            "opened since the ledger began, while the selection and read-budget "
            "figures beside it describe only the tick being reported; the two are "
            "never summed or compared.",
            "The aged-out count includes checks that were read after their "
            "eligibility window closed, so it is not a count of checks that were "
            "never attempted.",
            "The second-message denominator counts only checks that found the "
            "room present; a check that found it absent observed no room that "
            "could carry a second message.",
        ],
    }


def test_snapshot_representations_are_safely_capped_by_serialized_bytes(tmp_path):
    ticks = tmp_path / "ticks.jsonl"
    telemetry = tmp_path / "telemetry.sqlite3"
    write_ticks(ticks, tick("2026-08-30T00:00:00Z"))
    with telemetry_database(telemetry) as connection:
        for attempt_id, observed_at, service in (
            (1, "2026-08-30T00:01:00Z", "a" * 40_000),
            (2, "2026-08-30T01:01:00Z", "b" * 40_000),
        ):
            add_attempt(
                connection,
                attempt_id=attempt_id,
                route="/config",
                observed_at=observed_at,
                outcome="success",
                status=200,
            )
            connection.execute(
                "INSERT INTO discovery_snapshots VALUES (?, ?, ?, ?, ?, ?)",
                (
                    attempt_id,
                    attempt_id,
                    "/config",
                    observed_at,
                    f"{attempt_id:064x}",
                    json.dumps({"service": service}),
                ),
            )

    changes = build_snapshots(
        ticks,
        telemetry,
        derived_at="2026-08-30T01:01:10Z",
        published_at="2026-08-30T01:02:00Z",
    )["changes"]

    assert len(json_bytes(changes)) <= MAX_RESPONSE_BYTES
    assert len(text_bytes(changes)) <= MAX_RESPONSE_BYTES
    assert changes["coverage"]["response_capped"] is True
    assert changes["coverage"]["records_omitted_by_response_limit"] == 1
    assert changes["coverage"]["truncated"] is True


def test_frozen_input_valid_until_never_slides_forward_on_rebuild(tmp_path):
    ticks = tmp_path / "ticks.jsonl"
    telemetry = tmp_path / "telemetry.sqlite3"
    write_ticks(ticks, tick("2026-08-30T00:00:00Z"))
    with telemetry_database(telemetry):
        pass

    first = build_snapshots(
        ticks,
        telemetry,
        derived_at="2026-08-30T01:00:00Z",
        published_at="2026-08-30T01:01:00Z",
    )["status"]
    second = build_snapshots(
        ticks,
        telemetry,
        derived_at="2026-08-30T02:00:00Z",
        published_at="2026-08-30T02:01:00Z",
    )["status"]

    assert first["valid_until"] == "2026-08-30T00:15:00Z"
    assert second["valid_until"] == first["valid_until"]
    assert first["freshness"] == second["freshness"] == "stale"
    assert first["derived_at"] != second["derived_at"]
    assert first["published_at"] != second["published_at"]


def test_unfinished_cycle_is_unknown_and_never_an_incident(tmp_path):
    ticks = tmp_path / "ticks.jsonl"
    telemetry = tmp_path / "telemetry.sqlite3"
    write_ticks(ticks, tick("2026-08-30T00:00:00Z"))
    with telemetry_database(telemetry) as connection:
        connection.execute(
            "INSERT INTO cycles VALUES (1, 'pulse_probe', ?, NULL, 'running', NULL)",
            ("2026-08-30T00:01:00Z",),
        )

    result = build_snapshots(
        ticks,
        telemetry,
        derived_at="2026-08-30T00:01:10Z",
        published_at="2026-08-30T00:02:00Z",
    )

    assert result["status"]["status"]["pulse_cycle"]["state"] == "unknown"
    assert result["status"]["freshness"] == "fresh"
    assert result["changes"]["freshness"] == "not_observed"
    assert result["methodology"]["freshness"] == "not_applicable"
    assert result["incidents"]["incidents"] == []


def test_ledger_chain_head_is_only_published_for_tick_derived_resources(tmp_path):
    ticks = tmp_path / "ticks.jsonl"
    telemetry = tmp_path / "telemetry.sqlite3"
    chained_tick = tick("2026-08-30T00:00:00Z")
    chained_tick["ledger_chain"] = {
        "version": 1,
        "previous_sha256": None,
        "tick_sha256": "a" * 64,
    }
    write_ticks(ticks, chained_tick)
    with telemetry_database(telemetry):
        pass

    result = build_snapshots(
        ticks,
        telemetry,
        derived_at="2026-08-30T00:00:10Z",
        published_at="2026-08-30T00:00:30Z",
    )

    assert result["status"]["ledger_chain_head"] == "a" * 64
    assert result["incidents"]["ledger_chain_head"] == "a" * 64
    assert "ledger_chain_head" not in result["changes"]
    assert "ledger_chain_head" not in result["methodology"]
