import ast
import inspect
import json
import re
import textwrap
from copy import deepcopy

import pytest

import collect
import derive

from derive import derive_records, inject_html, read_jsonl, ssr_values, validate_tick


def funnel(
    *,
    census=435_006,
    observed=100,
    two_ticks=80,
    two_collection_dates=60,
    two_rooms=40,
    counterparties=20,
    sampled=20,
    known=28_940,
    persistence_dates_count=2,
    persistence_started_at="2026-08-28T08:00:00Z",
    persistence_reset_at=None,
    cap_hit=False,
    census_completed_at="2026-08-28T07:59:00Z",
):
    return {
        "well_formed_did_notes": census,
        "census_completed_at": (census_completed_at if census is not None else None),
        "dids_observed_signing": observed,
        "seen_two_ticks": two_ticks,
        "two_collection_utc_dates": two_collection_dates,
        "two_rooms": two_rooms,
        "signed_counterparty": counterparties,
        "persistence_started_at": persistence_started_at,
        "persistence_reset_at": persistence_reset_at,
        "persistence_collection_utc_dates_count": persistence_dates_count,
        "coverage": {"sampled_rooms": sampled, "known_rooms": known},
        "tracked_dids": observed,
        "tracked_cap": 200_000,
        "cap_hit": cap_hit,
        "collection_started": "2026-08-28T08:00:00Z",
    }


def legacy_funnel(**overrides):
    value = funnel(**overrides)
    value["two_utc_dates"] = value.pop("two_collection_utc_dates")
    value.pop("persistence_started_at")
    value.pop("persistence_reset_at")
    value.pop("persistence_collection_utc_dates_count")
    return value


def room(name):
    return {"name": name, "seq": 1, "idle_seconds": 0}


def manifest(
    *ids,
    success=None,
    epoch=0,
    frame_id="0123456789abcdef",
    frame_size=4,
    read_budget=None,
):
    outcomes = success if success is not None else [True] * len(ids)
    value = {
        "selector_version": 1,
        "seed": "0123456789abcdef0123456789abcdef",
        "epoch": epoch,
        "frame_id": frame_id,
        "frame_size": frame_size,
        "sampled": [
            {"id": room_id, "success": outcome}
            for room_id, outcome in zip(ids, outcomes, strict=True)
        ],
    }
    if read_budget is not None:
        value["read_budget"] = read_budget
    return value


def tick(
    ts,
    *,
    rooms=100,
    room_cap=40_960,
    notes=1000,
    note_cap=1_310_720,
    lobby=5000,
    event_seq=30000,
    identity=None,
    events=None,
    newest_rooms=None,
    signer_funnel=None,
    room_sampling=None,
    collector_version="2.0.0",
):
    return {
        "collector_version": collector_version,
        "ts": ts,
        "rooms_total": rooms,
        "room_cap": room_cap,
        "bytes_stored": 241_200_000,
        "notes_total": notes,
        "note_cap": note_cap,
        "lobby_last_seq": lobby,
        "events_last_seq": event_seq,
        "identity_total": identity,
        "events_window": (
            events
            if events is not None
            else [
                {
                    "seq": event_seq,
                    "ts": ts,
                    "name": "baseline",
                    "primary_class": "human_or_other",
                    "base_name": "baseline",
                }
            ]
        ),
        "newest_rooms": newest_rooms if newest_rooms is not None else [],
        "room_sampling": room_sampling,
        "signer_funnel": signer_funnel,
    }


def html_template():
    keys = (
        "status",
        "hero-value",
        "timestamp",
        "room-rate",
        "room-rate-samples",
        "lobby-rate",
        "lobby-rate-samples",
        "identity-total",
        "identity-rate",
        "stillborn",
        "stillborn-samples",
        "engagement-ratio",
        "engagement-ratio-context",
        "engagement-zero",
        "engagement-zero-context",
        "engagement-nick",
        "engagement-nick-context",
        "notes-cap-count",
        "notes-headroom",
        "notes-rate",
        "rooms-cap-count",
        "rooms-headroom",
        "rooms-rate",
        "funnel-census",
        "funnel-census-context",
        "funnel-observed",
        "funnel-observed-context",
        "funnel-two-ticks",
        "funnel-two-ticks-context",
        "funnel-two-dates",
        "funnel-two-dates-context",
        "funnel-sustained",
        "funnel-sustained-context",
        "funnel-warning",
        "funnel-coverage",
        "coverage-frame",
        "coverage-sampled",
        "coverage-unique",
        "coverage-repeats",
        "coverage-failures",
        "coverage-selector",
        "collector-version",
        "methodology-version",
        "computed-at",
        "collection-start",
        "collection-phase",
        "stall-alert",
        "tracked-dids",
        "quality",
        "empty-state",
    )
    width_keys = (
        "funnel-observed",
        "funnel-two-ticks",
        "funnel-two-dates",
        "funnel-sustained",
        "notes-fill",
        "rooms-fill",
    )
    elements = "".join(f'<span data-ssr="{key}">placeholder</span>' for key in keys)
    bars = "".join(f'<span data-ssr-width="{key}" style="width:0%"></span>' for key in width_keys)
    return (
        '<meta name="description" content="placeholder">'
        '<meta property="og:description" content="placeholder">'
        f"{elements}{bars}"
        '<script id="observatory-data" type="application/json">{}</script>'
    )


def rendered_ssr(source, key):
    match = re.search(
        rf'<span data-ssr="{re.escape(key)}">(.*?)</span>',
        source,
        re.DOTALL,
    )
    assert match
    return match.group(1)


def embedded_data(source):
    match = re.search(
        r'<script id="observatory-data" type="application/json">(.*?)</script>',
        source,
        re.DOTALL,
    )
    assert match
    return json.loads(match.group(1))


def test_methodology_version_is_bumped_for_room_sampling_change():
    result = derive_records([])
    assert derive.METHODOLOGY_VERSION == "1.4.0"
    assert result["methodology_version"] == "1.4.0"
    assert (
        "since collector 2.2.0 a sender without a nonce is never counted"
        in result["methodology"]["signer_funnel"]
    )


def test_room_sampling_grouping_sentence_matches_accumulator_key():
    source = inspect.getsource(derive.derived_room_sampling)
    tree = ast.parse(textwrap.dedent(source))
    assignments = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "key"
            for target in node.targets
        )
    ]
    assert len(assignments) == 1
    key_node = assignments[0].value
    assert isinstance(key_node, ast.Tuple)
    key_fields = [
        element.slice.value
        for element in key_node.elts
        if (
            isinstance(element, ast.Subscript)
            and isinstance(element.value, ast.Name)
            and element.value.id == "manifest"
            and isinstance(element.slice, ast.Constant)
            and isinstance(element.slice.value, str)
        )
    ]
    assert len(key_fields) == len(key_node.elts)
    assert key_fields == [
        "selector_version",
        "seed",
        "epoch",
        "frame_id",
        "frame_size",
    ]

    methodology = derive.methodology_definitions()["room_sampling"]
    grouping = re.search(
        r"Cumulative unique rooms, repeats and failed reads are counted "
        r".*?denominator\.",
        methodology,
    )
    assert grouping
    field_phrases = {
        "selector_version": "selector version",
        "seed": "seed",
        "epoch": "epoch",
        "frame_id": "frame identifier",
        "frame_size": "frame-size denominator",
        "read_budget": "read budget",
    }
    mentioned_fields = {
        field
        for field, phrase in field_phrases.items()
        if phrase in grouping.group(0)
    }
    assert mentioned_fields == set(key_fields)


def test_published_room_listing_endpoints_match_collector_request():
    collector_source = inspect.getsource(collect.collect_tick)
    collector_endpoints = re.findall(
        r'client\.get\("(/rooms\?format=json&limit=\d+)"\)',
        collector_source,
    )
    assert collector_endpoints == ["/rooms?format=json&limit=200"]
    room_endpoint = collector_endpoints[0]

    methodology = derive.methodology_definitions()
    listing_entries = (
        "first_message_only",
        "capacity",
        "room_sampling",
        "service_engagement",
    )
    for name in listing_entries:
        assert room_endpoint in methodology[name]

    bare_room_endpoint = re.compile(r"/rooms\?format=json(?!&limit=200)")
    for name, text in methodology.items():
        assert bare_room_endpoint.search(text) is None, name


def test_rate_math_and_sample_counts():
    records = [
        tick(
            "2026-08-28T08:00:00Z",
            rooms=100,
            notes=1000,
            lobby=5000,
            event_seq=30000,
        ),
        tick(
            "2026-08-28T08:01:00Z",
            rooms=106,
            notes=1030,
            lobby=5120,
            event_seq=30012,
            events=[
                {
                    "seq": seq,
                    "ts": "2026-08-28T08:01:00Z",
                    "name": f"d-room-{seq}",
                    "primary_class": "ownable",
                    "base_name": f"room-{seq}",
                }
                for seq in range(30001, 30013)
            ],
        ),
    ]
    point = derive_records(records)["points"][1]
    assert point["rates"]["public_rooms_per_second"] == {
        "value": pytest.approx(0.2),
        "samples": 2,
        "seconds": 60.0,
    }
    assert point["rates"]["lobby_messages_per_second"]["value"] == pytest.approx(2.0)
    assert point["rates"]["rooms_total_per_second"]["value"] == pytest.approx(0.1)
    assert point["rates"]["notes_per_second"]["value"] == pytest.approx(0.5)
    assert point["composition"]["samples"] == 12
    assert point["composition"]["counts"]["ownable"] == 12
    assert point["composition"]["complete"] is True


def test_headroom_and_trailing_rate_math():
    records = [
        tick(
            "2026-08-28T08:00:00Z",
            rooms=100,
            room_cap=200,
            notes=1000,
            note_cap=2000,
        ),
        tick(
            "2026-08-28T09:00:00Z",
            rooms=110,
            room_cap=200,
            notes=1360,
            note_cap=2000,
            lobby=5010,
            event_seq=30010,
        ),
        tick(
            "2026-08-28T10:00:00Z",
            rooms=120,
            room_cap=200,
            notes=1720,
            note_cap=2000,
            lobby=5020,
            event_seq=30020,
        ),
    ]
    point = derive_records(records, gap_seconds=4000)["points"][-1]
    notes = point["capacity"]["notes"]
    rooms = point["capacity"]["rooms"]
    assert notes["headroom"] == 280
    assert notes["headroom_fraction"] == pytest.approx(0.14)
    assert notes["fill_fraction"] == pytest.approx(0.86)
    assert notes["trailing_rate"] == pytest.approx(0.1)
    assert notes["trailing_window"] == {"seconds": 7200.0, "samples": 3}
    assert rooms["headroom"] == 80
    assert rooms["headroom_fraction"] == pytest.approx(0.4)
    assert rooms["trailing_rate"] == pytest.approx(20 / 7200)
    assert {"projection_seconds", "rate_range", "status"}.isdisjoint(notes)
    assert {"projection_seconds", "rate_range", "status"}.isdisjoint(rooms)


def test_cap_change_is_marked_and_resets_trailing_window():
    records = [
        tick("2026-08-28T08:00:00Z", notes=1000, note_cap=2000),
        tick(
            "2026-08-28T08:02:00Z",
            notes=1100,
            note_cap=4000,
            lobby=5010,
            event_seq=30010,
        ),
    ]
    capacity = derive_records(records)["points"][1]["capacity"]["notes"]
    assert capacity["cap_change"] == {
        "ts": "2026-08-28T08:02:00Z",
        "previous": 2000,
        "new": 4000,
    }
    assert capacity["trailing_rate"] is None
    assert capacity["trailing_window"]["samples"] == 0


@pytest.mark.parametrize("end_notes", [1000, 900])
def test_zero_or_negative_growth_rate_is_measured(end_notes):
    records = [
        tick("2026-08-28T08:00:00Z", notes=1000, note_cap=2000),
        tick(
            "2026-08-28T08:02:00Z",
            notes=end_notes,
            note_cap=2000,
            lobby=5010,
            event_seq=30010,
        ),
    ]
    capacity = derive_records(records)["points"][1]["capacity"]["notes"]
    assert capacity["trailing_rate"] == pytest.approx((end_notes - 1000) / 120)
    assert capacity["trailing_window"] == {"seconds": 120.0, "samples": 2}
    assert "projection_seconds" not in capacity
    assert "rate_range" not in capacity


def test_sampling_manifest_survives_validation_and_derive_round_trip():
    value = manifest(
        "aaaaaaaaaaaaaaaa",
        "bbbbbbbbbbbbbbbb",
        success=[True, False],
    )
    record = tick("2026-08-28T08:00:00Z", room_sampling=value)
    validated = validate_tick(record)
    assert validated["room_sampling"] == {**value, "read_budget": None}
    accepted, rejected = read_jsonl([json.dumps(record)])
    assert rejected == 0
    derived = derive_records(accepted)["points"][0]["room_sampling"]
    assert derived["seed"] == value["seed"]
    assert derived["read_budget"] is None
    assert derived["read_budget"] != 0
    assert derived["read_budget"] != 20
    assert derived["sampled"] == value["sampled"]
    assert derived["sampled_rooms"] == 2
    assert derived["cumulative_unique_rooms"] == 2
    assert derived["repeat_count"] == 0
    assert derived["failed_reads"] == 1
    assert derived["failed_reads_this_tick"] == 1


def test_recorded_read_budget_validates_and_reaches_funnel_coverage():
    value = manifest(
        "aaaaaaaaaaaaaaaa",
        "bbbbbbbbbbbbbbbb",
        frame_size=200,
        read_budget=80,
    )
    record = tick(
        "2026-08-28T08:00:00Z",
        room_sampling=value,
        signer_funnel=funnel(),
    )
    validated = validate_tick(record)
    assert validated["room_sampling"]["read_budget"] == 80

    point = derive_records([validated])["points"][0]
    assert point["room_sampling"]["read_budget"] == 80
    assert point["signer_funnel"]["coverage"]["read_budget"] == 80


def test_sampling_manifest_rejects_more_entries_than_recorded_budget():
    value = manifest(
        "aaaaaaaaaaaaaaaa",
        "bbbbbbbbbbbbbbbb",
        read_budget=1,
    )
    with pytest.raises(
        ValueError,
        match="room_sampling.sampled is not a non-empty list within its read limit",
    ):
        validate_tick(tick("2026-08-28T08:00:00Z", room_sampling=value))


def test_sampling_manifest_rejects_budget_above_structural_ceiling():
    value = manifest(
        "aaaaaaaaaaaaaaaa",
        read_budget=derive.ROOM_SAMPLING_STRUCTURAL_CEILING + 1,
    )
    with pytest.raises(
        ValueError,
        match="room_sampling.read_budget exceeds the structural ceiling",
    ):
        validate_tick(tick("2026-08-28T08:00:00Z", room_sampling=value))


def test_legacy_sampling_manifest_is_not_bounded_to_twenty_entries():
    room_ids = [f"{index:016x}" for index in range(21)]
    value = manifest(*room_ids, frame_size=200)
    validated = validate_tick(tick("2026-08-28T08:00:00Z", room_sampling=value))
    assert validated["room_sampling"]["read_budget"] is None
    assert len(validated["room_sampling"]["sampled"]) == 21


def test_legacy_sampling_manifest_remains_structurally_bounded():
    room_ids = [
        f"{index:016x}"
        for index in range(derive.ROOM_SAMPLING_STRUCTURAL_CEILING + 1)
    ]
    value = manifest(*room_ids, frame_size=len(room_ids))
    with pytest.raises(
        ValueError,
        match="room_sampling.sampled is not a non-empty list within its read limit",
    ):
        validate_tick(tick("2026-08-28T08:00:00Z", room_sampling=value))


def test_sampling_manifest_cannot_sample_more_rooms_than_its_frame():
    value = manifest(
        "aaaaaaaaaaaaaaaa",
        "bbbbbbbbbbbbbbbb",
        frame_size=1,
    )
    with pytest.raises(
        ValueError,
        match="room_sampling samples more rooms than its frame",
    ):
        validate_tick(tick("2026-08-28T08:00:00Z", room_sampling=value))


def test_room_sampling_prose_distinguishes_recorded_and_legacy_budgets():
    recorded = derive_records(
        [
            tick(
                "2026-08-28T08:00:00Z",
                room_sampling=manifest(
                    "aaaaaaaaaaaaaaaa",
                    frame_size=200,
                    read_budget=80,
                ),
                signer_funnel=funnel(),
            )
        ]
    )
    legacy = derive_records(
        [
            tick(
                "2026-08-28T08:00:00Z",
                room_sampling=manifest(
                    "aaaaaaaaaaaaaaaa",
                    frame_size=200,
                ),
                signer_funnel=funnel(),
            )
        ]
    )

    recorded_coverage = recorded["points"][0]["signer_funnel"]["display"]["coverage_text"]
    legacy_coverage = legacy["points"][0]["signer_funnel"]["display"]["coverage_text"]
    methodology = recorded["methodology"]["room_sampling"]

    assert "The recorded room-read budget for this tick is 80." in recorded_coverage
    assert (
        "The room-read budget for this tick was not recorded; no value is inferred."
        in legacy_coverage
    )
    assert "when absent on legacy ticks it is reported as not recorded" in methodology
    for coverage_text in (recorded_coverage, legacy_coverage):
        assert "1 / 200 distinct room hashes selected" in coverage_text
        assert "distinct room hashes observed" not in coverage_text
    for prose in (recorded_coverage, legacy_coverage, methodology):
        assert "20 room reads" not in prose


def test_cumulative_unique_room_coverage_and_repeats_are_frame_scoped():
    first = manifest("aaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbb")
    second = manifest("bbbbbbbbbbbbbbbb", "cccccccccccccccc")
    records = [
        tick("2026-08-28T08:00:00Z", room_sampling=first),
        tick(
            "2026-08-28T08:02:00Z",
            event_seq=30001,
            lobby=5001,
            room_sampling=second,
        ),
    ]
    sampling = derive_records(records)["points"][-1]["room_sampling"]
    assert sampling["frame_size"] == 4
    assert sampling["sampled_rooms"] == 2
    assert sampling["cumulative_unique_rooms"] == 3
    assert sampling["repeat_count"] == 1


def test_read_budget_change_does_not_fork_frame_coverage():
    first = manifest(
        "aaaaaaaaaaaaaaaa",
        "bbbbbbbbbbbbbbbb",
        success=[True, False],
        read_budget=20,
    )
    second = manifest(
        "bbbbbbbbbbbbbbbb",
        "cccccccccccccccc",
        success=[True, True],
        read_budget=80,
    )
    records = [
        tick("2026-08-28T08:00:00Z", room_sampling=first),
        tick(
            "2026-08-28T08:02:00Z",
            event_seq=30001,
            lobby=5001,
            room_sampling=second,
        ),
    ]
    sampling = derive_records(records)["points"][-1]["room_sampling"]
    assert sampling["read_budget"] == 80
    assert sampling["cumulative_unique_rooms"] == 3
    assert sampling["repeat_count"] == 1
    assert sampling["failed_reads"] == 1
    assert sampling["failed_reads_this_tick"] == 0


def test_failed_room_read_is_counted_without_becoming_zero():
    records = [
        tick(
            "2026-08-28T08:00:00Z",
            room_sampling=manifest("aaaaaaaaaaaaaaaa", success=[False]),
        ),
        tick(
            "2026-08-28T08:02:00Z",
            event_seq=30001,
            lobby=5001,
            room_sampling=manifest("bbbbbbbbbbbbbbbb", success=[True]),
        ),
    ]
    points = derive_records(records)["points"]
    assert points[0]["room_sampling"]["failed_reads"] == 1
    assert points[1]["room_sampling"]["failed_reads"] == 1
    assert points[1]["room_sampling"]["failed_reads_this_tick"] == 0
    assert points[0]["room_sampling"]["sampled"][0]["success"] is False


def test_new_frame_resets_cumulative_coverage_denominator():
    records = [
        tick(
            "2026-08-28T08:00:00Z",
            room_sampling=manifest("aaaaaaaaaaaaaaaa", frame_size=4),
        ),
        tick(
            "2026-08-28T08:02:00Z",
            event_seq=30001,
            lobby=5001,
            room_sampling=manifest(
                "bbbbbbbbbbbbbbbb",
                epoch=1,
                frame_id="fedcba9876543210",
                frame_size=2,
            ),
        ),
    ]
    sampling = derive_records(records)["points"][-1]["room_sampling"]
    assert sampling["frame_size"] == 2
    assert sampling["cumulative_unique_rooms"] == 1
    assert sampling["repeat_count"] == 0


def test_no_projection_fields_remain_after_phase_four():
    result = derive_records(
        [
            tick("2026-08-28T08:00:00Z", notes=1000, note_cap=2000),
            tick(
                "2026-08-28T09:00:00Z",
                notes=1360,
                note_cap=2000,
                lobby=5010,
                event_seq=30010,
            ),
        ],
        gap_seconds=4000,
    )
    encoded = json.dumps(result, sort_keys=True)
    assert "projection_seconds" not in encoded
    assert "rate_range" not in encoded
    capacity = result["points"][-1]["capacity"]["notes"]
    assert capacity["trailing_rate"] == pytest.approx(0.1)
    assert capacity["trailing_window"] == {"seconds": 3600.0, "samples": 2}


def test_one_collection_date_yields_zero_at_persistence_stage():
    records = [
        tick(
            "2026-08-28T08:00:00Z",
            signer_funnel=funnel(
                two_collection_dates=0,
                two_rooms=0,
                counterparties=0,
                persistence_dates_count=1,
            ),
        ),
        tick(
            "2026-08-28T20:00:00Z",
            event_seq=30001,
            lobby=5001,
            signer_funnel=funnel(
                two_collection_dates=0,
                two_rooms=0,
                counterparties=0,
                persistence_dates_count=1,
            ),
        ),
    ]
    derived = derive_records(records, gap_seconds=50_000)["points"][-1]["signer_funnel"]
    assert derived["two_collection_utc_dates"] == 0
    assert derived["persistence_collection_utc_dates_count"] == 1
    assert derived["sustained_reciprocal_footprint"] == 0


def test_two_distinct_collection_dates_yield_expected_count():
    records = [
        tick(
            "2026-08-28T23:59:00Z",
            signer_funnel=funnel(
                two_collection_dates=0,
                two_rooms=0,
                counterparties=0,
                persistence_dates_count=1,
            ),
        ),
        tick(
            "2026-08-29T00:01:00Z",
            event_seq=30001,
            lobby=5001,
            signer_funnel=funnel(
                two_collection_dates=37,
                two_rooms=31,
                counterparties=19,
                persistence_dates_count=2,
            ),
        ),
    ]
    derived = derive_records(records)["points"][-1]["signer_funnel"]
    assert derived["two_collection_utc_dates"] == 37
    assert derived["persistence_collection_utc_dates_count"] == 2
    assert derived["sustained_reciprocal_footprint"] == 19


def test_legacy_message_derived_dates_do_not_inflate_persistence_stage():
    record = tick(
        "2026-08-28T08:00:00Z",
        signer_funnel=legacy_funnel(
            two_collection_dates=60,
            two_rooms=40,
            counterparties=20,
        ),
    )
    derived = derive_records([record])["points"][0]["signer_funnel"]
    assert derived["two_collection_utc_dates"] == 0
    assert derived["two_rooms"] == 0
    assert derived["signed_counterparty"] == 0
    assert derived["sustained_reciprocal_footprint"] == 0
    assert derived["persistence_collection_utc_dates_count"] == 0
    assert derived["legacy_persistence_reset"] is True
    assert "two_utc_dates" not in derived


def test_funnel_stages_are_monotonic_after_collapse():
    value = funnel()
    point = derive_records(
        [
            tick(
                "2026-08-28T08:00:00Z",
                rooms=28_940,
                newest_rooms=[room(f"room-{index}") for index in range(200)],
                signer_funnel=value,
            )
        ]
    )["points"][0]
    derived = point["signer_funnel"]
    stages = [
        derived["well_formed_did_notes"],
        derived["dids_observed_signing"],
        derived["seen_two_ticks"],
        derived["two_collection_utc_dates"],
        derived["two_rooms"],
        derived["signed_counterparty"],
        derived["sustained_reciprocal_footprint"],
    ]
    assert all(right <= left for left, right in zip(stages, stages[1:]))
    assert derived["coverage"]["sampled_rooms"] == 20
    assert derived["coverage"]["newest_listing_rooms"] == 200
    assert derived["coverage"]["rooms_total"] == 28_940
    assert derived["coverage"]["frame_size"] is None


def test_second_funnel_stage_reports_two_separate_populations():
    result = derive_records(
        [
            tick(
                "2026-08-28T08:00:00Z",
                rooms=28_940,
                newest_rooms=[room(f"room-{index}") for index in range(200)],
                signer_funnel=funnel(census=556_973, observed=21_104),
            )
        ]
    )
    context = ssr_values(result)["funnel-observed-context"]
    assert context == (
        "21,104 distinct did:key signers observed in sampled rooms · a lower "
        "bound on signers, since sampling covers a fraction of rooms · a "
        "separate population from the census: neither contains the other"
    )
    assert "of 556,973" not in context
    assert "DID notes observed signing" not in context


def test_funnel_census_is_never_older_than_newest_census_in_payload():
    records = [
        tick(
            "2026-08-28T08:00:00Z",
            signer_funnel=funnel(census=556_973),
        ),
        tick(
            "2026-08-28T08:02:00Z",
            event_seq=30_001,
            lobby=5_001,
            identity=608_600,
            signer_funnel=funnel(census=556_973),
        ),
    ]
    result = derive_records(records)
    derived = result["points"][-1]["signer_funnel"]
    assert derived["well_formed_did_notes"] == 608_600
    assert derived["census_completed_at"] == "2026-08-28T08:02:00Z"
    assert derived["census_superseded"] is True
    values = ssr_values(result)
    assert values["funnel-census"] == "608,600"
    assert values["funnel-census-context"] == ("last completed census · 2026-08-28T08:02:00Z")
    # The first point predates the newer census and keeps its own.
    earlier = result["points"][0]["signer_funnel"]
    assert earlier["well_formed_did_notes"] == 556_973
    assert earlier["census_superseded"] is False


def test_ssr_reads_the_shared_display_contract_verbatim():
    result = derive_records(
        [
            tick(
                "2026-08-28T08:00:00Z",
                signer_funnel=funnel(
                    observed=100,
                    two_ticks=80,
                    two_collection_dates=30,
                    two_rooms=25,
                    counterparties=20,
                ),
            )
        ]
    )
    display = result["points"][-1]["signer_funnel"]["display"]
    values = ssr_values(result)
    stages = {stage["key"]: stage for stage in display["stages"]}
    assert values["funnel-sustained-context"] == stages["sustained"]["context"]
    assert values["funnel-sustained-context"] == (
        "66.7% of 30 observed on ≥2 distinct collection UTC dates"
    )
    assert values["funnel-warning"] == display["warning"]
    assert values["funnel-coverage"] == display["coverage_text"]
    assert values["funnel-two-dates-context"] == stages["two_dates"]["context"]
    # A measured non-zero stage-4 value must carry the percent context, never
    # the "0 qualify" explanation.
    assert not values["funnel-two-dates-context"].startswith("0 qualify")
    assert values["funnel-two-dates-context"] == (
        "37.5% of 80 observed on at least two distinct collection UTC dates"
    )


def test_measured_zero_at_stage_four_renders_zero_not_dash():
    result = derive_records(
        [
            tick(
                "2026-08-28T08:00:00Z",
                signer_funnel=funnel(
                    two_collection_dates=0,
                    two_rooms=0,
                    counterparties=0,
                    persistence_dates_count=1,
                ),
            )
        ]
    )
    values = ssr_values(result)
    assert values["funnel-two-dates"] == "0"
    assert values["funnel-two-dates-context"].startswith("0 qualify")


def test_cap_hit_and_persistence_reset_surface_in_ssr_warning():
    result = derive_records(
        [
            tick(
                "2026-08-28T08:00:00Z",
                signer_funnel=funnel(
                    cap_hit=True,
                    persistence_reset_at="2026-08-28T07:00:00Z",
                ),
            )
        ]
    )
    warning = ssr_values(result)["funnel-warning"]
    assert "state cap has been reached" in warning
    assert "Persistence restarted at 2026-08-28T07:00:00Z" in warning


def test_observed_signers_may_exceed_the_census():
    value = funnel(census=50, observed=100)
    validated = validate_tick(tick("2026-08-28T08:00:00Z", signer_funnel=value))
    assert validated["signer_funnel"]["dids_observed_signing"] == 100


def test_service_derived_strings_survive_injection_escaped(tmp_path):
    data = derive_records([tick("2026-08-28T08:00:00Z")])
    data["computed_at"] = "</script><img onerror=alert(1) src=x>"
    path = tmp_path / "index.html"
    path.write_text(html_template(), encoding="utf-8")
    inject_html(path, data)
    source = path.read_text(encoding="utf-8")
    assert source.count("</script>") == 1
    assert "<img" not in source
    assert "&lt;/script&gt;&lt;img" in source


def test_funnel_rejects_non_monotonic_stage():
    value = funnel(two_collection_dates=81)
    with pytest.raises(ValueError, match="monotonic"):
        validate_tick(tick("2026-08-28T08:00:00Z", signer_funnel=value))


def test_funnel_rejects_missing_sample_count():
    value = funnel()
    del value["coverage"]["sampled_rooms"]
    with pytest.raises(ValueError, match="coverage.sampled_rooms"):
        validate_tick(tick("2026-08-28T08:00:00Z", signer_funnel=value))


def test_server_rendered_values_match_embedded_newest_observation(tmp_path):
    records = [
        tick(
            "2026-08-28T08:00:00Z",
            rooms=28_900,
            notes=556_000,
            event_seq=30_000,
            newest_rooms=[room(f"old-{index}") for index in range(200)],
        ),
        tick(
            "2026-08-28T08:02:00Z",
            rooms=28_940,
            notes=556_973,
            lobby=5_120,
            event_seq=30_042,
            identity=556_973,
            newest_rooms=[room(f"new-{index}") for index in range(200)],
            room_sampling=manifest(
                "aaaaaaaaaaaaaaaa",
                "bbbbbbbbbbbbbbbb",
                frame_size=200,
            ),
            signer_funnel=funnel(
                census=556_973,
                observed=21_104,
                two_ticks=7_425,
                two_collection_dates=1_287,
                two_rooms=1_270,
                counterparties=1_270,
            ),
        ),
    ]
    data = derive_records(records)
    path = tmp_path / "index.html"
    path.write_text(html_template(), encoding="utf-8")
    inject_html(path, data)
    source = path.read_text(encoding="utf-8")
    embedded = embedded_data(source)
    newest = embedded["points"][-1]
    assert rendered_ssr(source, "status") == "2 collected observations"
    assert rendered_ssr(source, "hero-value") == (f"{newest['observed_public_rooms']:,} observed")
    assert rendered_ssr(source, "identity-total") == f"{newest['identity_total']:,}"
    assert rendered_ssr(source, "notes-cap-count") == (
        f"{newest['capacity']['notes']['total']:,} / {newest['capacity']['notes']['cap']:,}"
    )
    assert rendered_ssr(source, "rooms-cap-count") == (
        f"{newest['capacity']['rooms']['total']:,} / {newest['capacity']['rooms']['cap']:,}"
    )
    assert rendered_ssr(source, "funnel-observed") == "21,104"
    assert rendered_ssr(source, "funnel-sustained") == "1,270"
    assert rendered_ssr(source, "coverage-unique") == (
        "2 / 200 unique room hashes in this frame epoch"
    )
    assert "2 forward-collected observations" in source
    assert "No observations have been collected" not in source


def test_zero_observations_render_honest_empty_state(tmp_path):
    data = derive_records([])
    assert data["collector_version"] is None
    path = tmp_path / "index.html"
    path.write_text(html_template(), encoding="utf-8")
    inject_html(path, data)
    source = path.read_text(encoding="utf-8")
    assert embedded_data(source)["points"] == []
    assert rendered_ssr(source, "collector-version") == "—"
    assert rendered_ssr(source, "status") == "0 collected observations"
    assert rendered_ssr(source, "hero-value") == "—"
    assert rendered_ssr(source, "empty-state") == (
        "No observations have been collected. The chart remains empty by design."
    )
    assert "0 forward-collected observations" in source


def test_legacy_tick_without_manifest_or_collection_dates_is_accepted():
    older_tick = tick(
        "2026-08-28T08:00:00Z",
        newest_rooms=[room("legacy-room")],
        signer_funnel=legacy_funnel(),
    )
    older_tick.pop("room_sampling")
    older_tick.pop("collector_version")

    validated = validate_tick(older_tick)
    assert validated["room_sampling"] is None
    assert validated["collector_version"] == "legacy"
    assert validated["signer_funnel"]["two_collection_utc_dates"] == 0
    assert validated["signer_funnel"]["persistence_collection_utc_dates_count"] == 0
    assert validated["signer_funnel"]["legacy_persistence_reset"] is True

    accepted, rejected = read_jsonl([json.dumps(older_tick)])
    assert len(accepted) == 1
    assert rejected == 0

    point = derive_records(accepted)["points"][0]
    assert point["room_sampling"] is None
    assert point["signer_funnel"]["two_collection_utc_dates"] == 0
    assert point["signer_funnel"]["sustained_reciprocal_footprint"] == 0


def test_gap_suppresses_interval_rates_and_is_explicit():
    records = [
        tick("2026-08-28T08:00:00Z", event_seq=30000),
        tick(
            "2026-08-28T08:20:00Z",
            rooms=110,
            notes=1100,
            lobby=5200,
            event_seq=30020,
            events=[
                {
                    "seq": 30020,
                    "ts": "2026-08-28T08:20:00Z",
                    "name": "later",
                    "primary_class": "human_or_other",
                    "base_name": "later",
                }
            ],
        ),
    ]
    result = derive_records(records, gap_seconds=300)
    point = result["points"][1]
    assert point["rates"]["public_rooms_per_second"]["value"] is None
    assert point["rates"]["public_rooms_per_second"]["samples"] == 0
    assert any(gap["reason"] == "polling_gap" for gap in result["gaps"])
    assert point["composition"]["expected"] == 20
    assert point["composition"]["samples"] == 1
    assert point["composition"]["complete"] is False
    assert any(gap["reason"] == "events_window_incomplete" for gap in result["gaps"])
    assert point["capacity"]["notes"]["trailing_rate"] is None
    assert point["capacity"]["notes"]["trailing_window"]["samples"] == 0


def test_short_series_stays_short():
    result = derive_records([tick("2026-08-28T08:00:00Z")])
    assert result["collection_started"] == "2026-08-28T08:00:00Z"
    assert result["accepted_ticks"] == 1
    assert len(result["points"]) == 1
    assert result["points"][0]["observed_public_rooms"] == 0
    assert result["points"][0]["rates"]["public_rooms_per_second"]["value"] is None
    assert result["points"][0]["composition"]["samples"] == 0
    assert result["points"][0]["capacity"]["notes"]["trailing_rate"] is None
    assert result["points"][0]["capacity"]["notes"]["trailing_window"]["samples"] == 0


def test_malformed_tick_is_rejected():
    valid = tick("2026-08-28T08:00:00Z")
    malformed = deepcopy(valid)
    malformed["rooms_total"] = "25752"
    accepted, rejected = read_jsonl([json.dumps(valid), json.dumps(malformed), "{not json}"])
    result = derive_records(accepted, rejected_ticks=rejected)
    assert result["accepted_ticks"] == 1
    assert result["rejected_ticks"] == 2
    assert len(result["points"]) == 1


def test_no_tick_is_fabricated():
    source = [
        tick("2026-08-28T08:00:00Z", event_seq=30000),
        tick("2026-08-28T08:05:00Z", event_seq=30100),
        tick("2026-08-28T08:10:00Z", event_seq=30200),
    ]
    result = derive_records(source)
    assert len(result["points"]) == len(source)
    assert [point["ts"] for point in result["points"]] == [record["ts"] for record in source]
    assert result["points"][0]["observed_public_rooms"] == 0
    assert result["points"][-1]["observed_public_rooms"] == 200


def test_identity_rate_uses_complete_census_samples_only():
    source = [
        tick("2026-08-28T08:00:00Z", identity=422000),
        tick("2026-08-28T08:10:00Z", event_seq=30010),
        tick(
            "2026-08-28T09:00:00Z",
            event_seq=30020,
            identity=422600,
        ),
    ]
    result = derive_records(source)
    identity_rate = result["points"][2]["rates"]["identities_per_second"]
    assert identity_rate["samples"] == 2
    assert identity_rate["seconds"] == 3600.0
    assert identity_rate["value"] == pytest.approx(1 / 6)


def test_counter_decrease_is_not_rendered_as_a_negative_interval_rate():
    source = [
        tick("2026-08-28T08:00:00Z", event_seq=30000),
        tick(
            "2026-08-28T08:01:00Z",
            rooms=90,
            notes=900,
            lobby=4900,
            event_seq=29900,
        ),
    ]
    result = derive_records(source)
    assert result["points"][1]["rates"]["public_rooms_per_second"]["value"] is None
    assert any(gap["reason"] == "counter_decreased" for gap in result["gaps"])


def test_validate_tick_rejects_event_window_mismatch():
    value = tick("2026-08-28T08:00:00Z")
    value["events_last_seq"] = 30001
    with pytest.raises(ValueError, match="events_last_seq"):
        validate_tick(value)


def test_stalled_collection_is_detected_with_exact_banner_wording(monkeypatch):
    monkeypatch.setattr(derive, "utc_now", lambda: "2026-08-28T15:12:00Z")
    result = derive_records([tick("2026-08-28T08:00:00Z")])
    assert result["collection_age_seconds"] == pytest.approx(25_920.0)
    assert result["collection_stalled"] is True
    assert result["collection_stall_threshold_seconds"] == 600
    assert result["collection_stall_banner"] == (
        "COLLECTION STALLED — no observation for 7h 12m"
    )
    assert result["collection_phase"] == "Collection began"
    values = ssr_values(result)
    assert values["stall-alert"] == result["collection_stall_banner"]
    assert values["collection-phase"] == "Collection began"


def test_fresh_collection_is_not_stalled_and_threshold_is_strict(monkeypatch):
    monkeypatch.setattr(derive, "utc_now", lambda: "2026-08-28T08:01:00Z")
    fresh = derive_records([tick("2026-08-28T08:00:00Z")])
    assert fresh["collection_age_seconds"] == pytest.approx(60.0)
    assert fresh["collection_stalled"] is False
    assert fresh["collection_stall_banner"] == ""
    assert fresh["collection_phase"] == "Collecting since"
    assert ssr_values(fresh)["stall-alert"] == ""
    assert ssr_values(fresh)["collection-phase"] == "Collecting since"

    monkeypatch.setattr(derive, "utc_now", lambda: "2026-08-28T08:10:00Z")
    boundary = derive_records([tick("2026-08-28T08:00:00Z")])
    assert boundary["collection_age_seconds"] == pytest.approx(600.0)
    assert boundary["collection_stalled"] is False


def test_clock_skew_clamps_collection_age_to_zero(monkeypatch):
    monkeypatch.setattr(derive, "utc_now", lambda: "2026-08-28T07:00:00Z")
    result = derive_records([tick("2026-08-28T08:00:00Z")])
    assert result["collection_age_seconds"] == 0.0
    assert result["collection_stalled"] is False
    assert result["collection_stall_banner"] == ""


def test_empty_payload_reports_no_stall():
    result = derive_records([])
    assert result["collection_age_seconds"] is None
    assert result["collection_stalled"] is False
    assert result["collection_stall_threshold_seconds"] == 600
    assert result["collection_stall_banner"] == ""
    assert result["collection_phase"] == "Collecting since"
    values = ssr_values(result)
    assert values["stall-alert"] == ""
    assert values["collection-phase"] == "Collecting since"


def test_stall_banner_is_server_rendered_and_empty_when_healthy(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(derive, "utc_now", lambda: "2026-08-28T15:12:00Z")
    stalled = derive_records([tick("2026-08-28T08:00:00Z")])
    path = tmp_path / "stalled.html"
    path.write_text(html_template(), encoding="utf-8")
    inject_html(path, stalled)
    source = path.read_text(encoding="utf-8")
    assert rendered_ssr(source, "stall-alert") == (
        "COLLECTION STALLED — no observation for 7h 12m"
    )
    assert rendered_ssr(source, "collection-phase") == "Collection began"

    monkeypatch.setattr(derive, "utc_now", lambda: "2026-08-28T08:01:00Z")
    healthy = derive_records([tick("2026-08-28T08:00:00Z")])
    path = tmp_path / "healthy.html"
    path.write_text(html_template(), encoding="utf-8")
    inject_html(path, healthy)
    source = path.read_text(encoding="utf-8")
    assert rendered_ssr(source, "stall-alert") == ""
    assert rendered_ssr(source, "collection-phase") == "Collecting since"


def test_stall_duration_wording_across_magnitudes():
    assert derive.stall_duration_text(720) == "12m"
    assert derive.stall_duration_text(25_920) == "7h 12m"
    assert derive.stall_duration_text(96 * 3600 + 120) == "4d 0h"


def test_tracked_did_headroom_is_surfaced_with_its_denominator():
    result = derive_records(
        [tick("2026-08-28T08:00:00Z", signer_funnel=funnel(observed=174_307))]
    )
    display = result["points"][-1]["signer_funnel"]["display"]
    values = ssr_values(result)
    assert values["tracked-dids"] == display["tracked_text"]
    assert values["tracked-dids"] == (
        "174,307 / 200,000 tracked DIDs (87.2% of the state cap used)"
    )


def test_tracked_did_state_reads_not_recorded_without_a_funnel():
    result = derive_records([tick("2026-08-28T08:00:00Z")])
    assert ssr_values(result)["tracked-dids"] == "No signer state recorded"


def test_computed_interval_rate_caption_is_shared_between_ssr_and_display():
    result = derive_records(
        [
            tick("2026-08-28T08:00:00Z"),
            tick("2026-08-28T08:01:00Z", event_seq=30_012, lobby=5_120, rooms=106, notes=1_030),
        ]
    )
    point = result["points"][-1]
    tile = point["rate_display"]["public_rooms_per_second"]
    assert tile["value_text"] == "12.00/min"
    assert tile["context"] == "2 samples over 60s"
    values = ssr_values(result)
    assert values["room-rate"] == tile["value_text"]
    assert values["room-rate-samples"] == tile["context"]
    lobby_tile = point["rate_display"]["lobby_messages_per_second"]
    assert values["lobby-rate"] == lobby_tile["value_text"]
    assert values["lobby-rate-samples"] == lobby_tile["context"]


def test_counter_decrease_carries_last_rate_forward_with_reason():
    result = derive_records(
        [
            tick("2026-08-28T08:00:00Z"),
            tick("2026-08-28T08:01:00Z", event_seq=30_012, lobby=5_120, rooms=106, notes=1_030),
            tick("2026-08-28T08:02:00Z", event_seq=30_024, lobby=5_240, rooms=90, notes=1_060),
        ]
    )
    point = result["points"][-1]
    assert point["rates"]["public_rooms_per_second"]["value"] is None
    tile = point["rate_display"]["public_rooms_per_second"]
    assert tile["value_text"] == "12.00/min"
    assert tile["context"] == (
        "last computed rate · 2 samples over 60s · measured 2026-08-28T08:01:00Z "
        "· the room total fell between ticks (idle rooms are reaped faster than "
        "new rooms appear), so the interval is recorded as a gap and no rate is "
        "computed across it"
    )
    values = ssr_values(result)
    assert values["room-rate"] == tile["value_text"]
    assert values["room-rate-samples"] == tile["context"]
    lobby_tile = point["rate_display"]["lobby_messages_per_second"]
    assert lobby_tile["value_text"] == "2.00/s"
    assert lobby_tile["context"].startswith(
        "last computed rate · 2 samples over 60s · measured 2026-08-28T08:01:00Z"
    )


def test_polling_gap_with_no_prior_rate_keeps_an_honest_empty_state():
    result = derive_records(
        [
            tick("2026-08-28T08:00:00Z"),
            tick("2026-08-28T08:20:00Z", event_seq=30_020, lobby=5_200, rooms=110, notes=1_100),
        ],
        gap_seconds=300,
    )
    tile = result["points"][-1]["rate_display"]["public_rooms_per_second"]
    assert tile["value_text"] == "—"
    assert tile["context"] == (
        "no rate computed yet · 1200s elapsed between ticks, beyond the gap "
        "threshold, so the interval is recorded as a gap and no rate is computed "
        "across it"
    )
    values = ssr_values(result)
    assert values["room-rate"] == "—"
    assert values["room-rate-samples"] == tile["context"]


def test_first_point_rate_tiles_keep_the_original_empty_state():
    result = derive_records([tick("2026-08-28T08:00:00Z")])
    tile = result["points"][0]["rate_display"]["public_rooms_per_second"]
    assert tile == {"value_text": "—", "context": "Needs two gap-free observations"}
    values = ssr_values(result)
    assert values["room-rate"] == "—"
    assert values["room-rate-samples"] == "Needs two gap-free observations"


def test_cap_hit_stage_two_caption_discloses_the_pinned_cap():
    result = derive_records(
        [
            tick(
                "2026-08-28T08:00:00Z",
                signer_funnel=funnel(
                    census=608_600,
                    observed=200_000,
                    two_ticks=39_629,
                    two_collection_dates=0,
                    two_rooms=0,
                    counterparties=0,
                    persistence_dates_count=1,
                    cap_hit=True,
                ),
            )
        ]
    )
    display = result["points"][-1]["signer_funnel"]["display"]
    values = ssr_values(result)
    stages = {stage["key"]: stage for stage in display["stages"]}
    assert values["funnel-observed-context"] == stages["observed"]["context"]
    assert values["funnel-observed-context"] == (
        "200,000 distinct did:key signers observed in sampled rooms · pinned at "
        "the 200,000-DID tracking-state cap — admissions stopped when the cap was "
        "reached, so signers observed since are not counted · a lower bound on "
        "signers, since sampling covers a fraction of rooms · a separate "
        "population from the census: neither contains the other"
    )
    assert values["funnel-two-ticks-context"] == (
        "19.8% of 200,000 in the captured observed-signing cohort "
        "(pinned at the tracking cap)"
    )
    # The warning paragraph keeps its own cap sentence; the caption is additive.
    assert "state cap has been reached" in values["funnel-warning"]


def test_uncapped_stage_two_caption_is_unchanged_by_the_cap_clause():
    result = derive_records(
        [tick("2026-08-28T08:00:00Z", signer_funnel=funnel(observed=100, two_ticks=80))]
    )
    values = ssr_values(result)
    assert "pinned" not in values["funnel-observed-context"]
    assert "cap" not in values["funnel-observed-context"]
    assert values["funnel-two-ticks-context"] == (
        "80.0% of 100 in the captured observed-signing cohort"
    )


def engagement(
    *,
    ratio=120.2167,
    zero_share=0.2289,
    nick=0.3323,
    window_cap=200,
    windowed_messages=7013,
):
    return {
        "windowed_note_to_message_ratio": ratio,
        "zero_response_share": zero_share,
        "nick_diversity": nick,
        "window_cap": window_cap,
        "windowed_messages": windowed_messages,
    }


def test_engagement_round_trips_with_window_and_provenance():
    record = tick("2026-08-28T08:00:00Z")
    record["engagement"] = engagement()
    result = derive_records([record])
    point = result["points"][0]
    assert point["engagement"] == engagement()

    display = point["engagement_display"]
    assert display["ratio"]["value_text"] == "120.22"
    assert display["zero_response"]["value_text"] == "22.9%"
    assert display["nick_diversity"]["value_text"] == "0.3323"
    for entry in display.values():
        # Every caption carries the service's declared window, the provenance,
        # and the not-network-wide disclosure.
        assert "window cap 200 messages" in entry["context"]
        assert "7,013 windowed messages" in entry["context"]
        assert "republished unverified" in entry["context"]
        assert "not the network" in entry["context"]

    # SSR reads the shared display contract verbatim.
    values = ssr_values(result)
    assert values["engagement-ratio"] == display["ratio"]["value_text"]
    assert values["engagement-ratio-context"] == display["ratio"]["context"]
    assert values["engagement-zero"] == display["zero_response"]["value_text"]
    assert values["engagement-zero-context"] == display["zero_response"]["context"]
    assert values["engagement-nick"] == display["nick_diversity"]["value_text"]
    assert values["engagement-nick-context"] == display["nick_diversity"]["context"]


def test_tick_without_engagement_reads_not_recorded_never_zero():
    record = tick("2026-08-28T08:00:00Z")
    assert "engagement" not in record

    accepted, rejected = read_jsonl([json.dumps(record)])
    assert (len(accepted), rejected) == (1, 0)

    result = derive_records(accepted)
    point = result["points"][0]
    assert point["engagement"] is None
    values = ssr_values(result)
    for value_key, context_key in (
        ("engagement-ratio", "engagement-ratio-context"),
        ("engagement-zero", "engagement-zero-context"),
        ("engagement-nick", "engagement-nick-context"),
    ):
        assert values[value_key] == "—"
        assert "not recorded" in values[context_key]
        assert "absence never means zero" in values[context_key]
        assert "0" not in values[value_key]


def test_malformed_engagement_field_is_not_recorded_while_siblings_publish():
    record = tick("2026-08-28T08:00:00Z")
    record["engagement"] = engagement(ratio="<img src=x>", zero_share=1.7)
    accepted, rejected = read_jsonl([json.dumps(record)])
    assert (len(accepted), rejected) == (1, 0)

    point = derive_records(accepted)["points"][0]
    # The service-controlled string never reaches the payload.
    assert point["engagement"]["windowed_note_to_message_ratio"] is None
    # A share above 1 is nonsensical and is not published as a percentage.
    assert point["engagement"]["zero_response_share"] is None
    assert point["engagement"]["nick_diversity"] == 0.3323

    display = point["engagement_display"]
    assert display["ratio"]["value_text"] == "—"
    assert "not recorded" in display["ratio"]["context"]
    assert display["zero_response"]["value_text"] == "—"
    assert display["nick_diversity"]["value_text"] == "0.3323"
    assert "<img" not in json.dumps(derive_records(accepted))


def test_engagement_shape_never_rejects_a_tick():
    # The collector republishes the service's /rooms engagement object
    # verbatim, so its shape is service-controlled input: a hostile shape must
    # not invalidate a forward-collected tick.
    for hostile in ([1, 2], "text", 7, True, {}, {"unrelated": "x"}):
        record = tick("2026-08-28T08:00:00Z")
        record["engagement"] = hostile
        accepted, rejected = read_jsonl([json.dumps(record)])
        assert (len(accepted), rejected) == (1, 0), hostile


def test_engagement_window_absence_is_disclosed_in_caption():
    record = tick("2026-08-28T08:00:00Z")
    record["engagement"] = engagement(window_cap=None, windowed_messages=None)
    display = derive_records([record])["points"][0]["engagement_display"]
    assert "window figures not published this tick" in display["ratio"]["context"]


def test_engagement_ssr_is_baked_into_the_page(tmp_path):
    record = tick("2026-08-28T08:00:00Z")
    record["engagement"] = engagement()
    data = derive_records([record])
    path = tmp_path / "index.html"
    path.write_text(html_template(), encoding="utf-8")
    inject_html(path, data)
    source = path.read_text(encoding="utf-8")
    assert rendered_ssr(source, "engagement-ratio") == "120.22"
    assert rendered_ssr(source, "engagement-zero") == "22.9%"
    assert rendered_ssr(source, "engagement-nick") == "0.3323"
    assert "window cap 200 messages" in rendered_ssr(source, "engagement-ratio-context")


def test_funnel_bars_scale_to_the_observed_cohort_not_the_census():
    result = derive_records(
        [
            tick(
                "2026-08-28T08:00:00Z",
                signer_funnel=funnel(
                    census=608_600,
                    observed=100,
                    two_ticks=50,
                    two_collection_dates=25,
                    two_rooms=10,
                    counterparties=10,
                ),
            )
        ]
    )
    display = result["points"][0]["signer_funnel"]["display"]

    # The census is not a stage: it is published beside the funnel instead.
    assert [stage["key"] for stage in display["stages"]] == [
        "observed",
        "two_ticks",
        "two_dates",
        "sustained",
    ]
    assert display["census"]["value"] == 608_600
    assert display["census"]["value_text"] == "608,600"

    # Bars are proportions of the observed-signing cohort, so the visual no
    # longer asserts the census contains the observed population.
    widths = {stage["key"]: stage["width_percent"] for stage in display["stages"]}
    assert widths == {"observed": 100.0, "two_ticks": 50.0, "two_dates": 25.0, "sustained": 10.0}

    ssr = derive.ssr_widths(result)
    assert "funnel-census" not in ssr
    assert ssr["funnel-observed"] == 100.0
    assert ssr["funnel-two-ticks"] == 50.0


def test_tiny_funnel_stage_keeps_its_visibility_floor():
    result = derive_records(
        [
            tick(
                "2026-08-28T08:00:00Z",
                signer_funnel=funnel(
                    observed=100_000,
                    two_ticks=3,
                    two_collection_dates=2,
                    two_rooms=1,
                    counterparties=1,
                ),
            )
        ]
    )
    display = result["points"][0]["signer_funnel"]["display"]
    widths = {stage["key"]: stage["width_percent"] for stage in display["stages"]}
    assert widths["two_ticks"] == 1.0
    assert widths["sustained"] == 1.0


def test_funnel_census_block_reads_the_shared_display_contract_verbatim():
    result = derive_records([tick("2026-08-28T08:00:00Z", signer_funnel=funnel())])
    display = result["points"][0]["signer_funnel"]["display"]
    values = ssr_values(result)
    assert values["funnel-census"] == display["census"]["value_text"]
    assert values["funnel-census-context"] == display["census"]["context"]
