#!/usr/bin/env python3
"""Derive compact, forward-only Observatory data from collector JSONL."""

from __future__ import annotations

import argparse
import html
import json
import math
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

CLASSES = (
    "unlisted",
    "mailbox",
    "ownable",
    "ephemeral",
    "bare_hex",
    "human_or_other",
)
HEX_NAME_RE = re.compile(r"^[0-9a-f]{16}$", re.IGNORECASE)
HASH16_RE = re.compile(r"^[0-9a-f]{16}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
UUID_LIKE_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
    re.IGNORECASE,
)
MIXED_TOKEN_RE = re.compile(r"^[a-z0-9]{12,}$", re.IGNORECASE)
TRAILING_WINDOW_SECONDS = 24 * 60 * 60
RAW_RETENTION_SECONDS = 24 * 60 * 60
HOURLY_RETENTION_SECONDS = 30 * 24 * 60 * 60
DAILY_RETENTION_SECONDS = 365 * 24 * 60 * 60
HOURLY_ROLLUP_SECONDS = 60 * 60
DAILY_ROLLUP_SECONDS = 24 * 60 * 60
METHODOLOGY_VERSION = "1.16.0"
CENSUS_SHARD_COUNT = 256
CENSUS_RUN_FIELDS = {
    "walk_started_at",
    "shards_outstanding_at_start",
    "shards_collected",
    "shards_outstanding",
    "passes_attempted",
    "maximum_passes",
    "deadline_seconds",
    "shard_reads_attempted",
    "shard_read_failures",
    "failure_causes",
    "stop_reason",
}
CENSUS_FAILURE_CAUSES = {
    "deadline",
    "transport_or_decode",
    "invalid_shard_response",
}
CENSUS_STOP_REASONS = {"complete", "deadline", "maximum_passes"}
CENSUS_RUN_NOT_RECORDED = (
    "Census sweep cost not recorded · this tick does not carry "
    "identity_census_run; absence never means zero"
)
ROOM_SAMPLING_STRUCTURAL_CEILING = 200
ROOM_REVISIT_STAGES_SECONDS = (5 * 60, 60 * 60, 24 * 60 * 60)
ROOM_REVISIT_STRUCTURAL_CEILING = 305
ROOM_GENERATION_COLLECTOR_VERSION = (2, 11, 0)
ROOM_LIFECYCLE_SAMPLING_COLLECTOR_VERSION = (2, 12, 0)
ROOM_AGED_OUT_FINALIZATION_COLLECTOR_VERSION = (2, 13, 0)
ROOM_REVISIT_OUTCOMES = {
    "present_at_last_check",
    "absent_at_last_check",
    "check_failed",
    "superseded_before_check",
}
CENSUS_LONG_WALK_SECONDS = 60 * 60
LEDGER_ANCHOR_NOT_RECORDED = (
    "No external anchor is recorded. The hash chain shows internal consistency "
    "only; it cannot prove when a tick was collected."
)
# Room-name classes and their published labels. The page prints these labels,
# so they live beside the class tuple rather than in the page's JavaScript.
CLASS_LABELS = {
    "unlisted": "unlisted p-",
    "mailbox": "mailbox mb-",
    "ownable": "ownable d-",
    "ephemeral": "ephemeral e-",
    "bare_hex": "bare 16-hex",
    "human_or_other": "human / other",
}
SERIES_SUFFIXES = {
    "rooms": " observed",
    "lobby": " messages",
    "notes": " notes",
}
SAMPLING_NOT_RECORDED = "Not recorded before collector v2.0.0"
# A fixed ten-minute honesty threshold. At the measured post-deploy cadence of
# about 258 seconds per tick, this is about 2.3 collector intervals. The rebuild
# cron runs independently of the collector, so a payload whose newest tick is
# older than this at rebuild time means collection has stalled; age exactly at
# the threshold is not yet a stall (strictly greater).
STALL_THRESHOLD_SECONDS = 600
FUNNEL_BASE_FIELDS = (
    "well_formed_did_notes",
    "dids_observed_signing",
    "seen_two_ticks",
)
MAX_INTEGER = (1 << 63) - 1


def utc_now() -> str:
    return (
        datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    )


def parse_ts(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("timestamp is not a string")
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        raise ValueError("timestamp has no timezone")
    try:
        return parsed.astimezone(timezone.utc)
    except OverflowError as error:
        raise ValueError(
            "timestamp must normalize within the supported UTC range"
        ) from error


def optional_timestamp(value: Any, field: str) -> str | None:
    if value is None:
        return None
    try:
        parse_ts(value)
    except ValueError as error:
        raise ValueError(f"{field} is not a valid timestamp") from error
    return value


def integer(value: Any, field: str, minimum: int = 0) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < minimum
        or value > MAX_INTEGER
    ):
        raise ValueError(f"{field} is not a valid integer")
    return value


def optional_integer(value: Any, field: str) -> int | None:
    if value is None:
        return None
    return integer(value, field)


def version_string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > 64:
        raise ValueError(f"{field} is not a valid version")
    return value


def collector_version_at_least(value: str, minimum: tuple[int, int, int]) -> bool:
    match = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)", value)
    if match is None:
        return False
    return tuple(int(part) for part in match.groups()) >= minimum


def validate_tracking_disclosure(value: Any) -> dict[str, str] | None:
    if value is None:
        return None
    if not isinstance(value, dict) or set(value) != {"warning", "methodology"}:
        raise ValueError("tracking_disclosure is not a complete disclosure object")

    result: dict[str, str] = {}
    for field in ("warning", "methodology"):
        text = value[field]
        if not isinstance(text, str) or not text.strip():
            raise ValueError(f"tracking_disclosure.{field} is not valid text")
        result[field] = text
    return result


def validate_ledger_chain(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != {
        "version",
        "previous_sha256",
        "tick_sha256",
    }:
        raise ValueError("ledger_chain is not a complete version-1 chain object")
    version = integer(value["version"], "ledger_chain.version", 1)
    if version != 1:
        raise ValueError("ledger_chain.version is not 1")
    previous_hash = value["previous_sha256"]
    if previous_hash is not None and (
        not isinstance(previous_hash, str) or SHA256_RE.fullmatch(previous_hash) is None
    ):
        raise ValueError(
            "ledger_chain.previous_sha256 is not null or lowercase SHA-256"
        )
    tick_hash = value["tick_sha256"]
    if not isinstance(tick_hash, str) or SHA256_RE.fullmatch(tick_hash) is None:
        raise ValueError("ledger_chain.tick_sha256 is not lowercase SHA-256")
    return {
        "version": version,
        "previous_sha256": previous_hash,
        "tick_sha256": tick_hash,
    }


def validate_room_sampling(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError("room_sampling is not an object")

    selector_version = integer(
        value.get("selector_version"),
        "room_sampling.selector_version",
        1,
    )
    seed = value.get("seed")
    epoch = integer(value.get("epoch"), "room_sampling.epoch")
    frame_id = value.get("frame_id")
    frame_size = integer(value.get("frame_size"), "room_sampling.frame_size", 1)
    if "read_budget" in value:
        read_budget = integer(
            value["read_budget"],
            "room_sampling.read_budget",
            1,
        )
        if read_budget > ROOM_SAMPLING_STRUCTURAL_CEILING:
            raise ValueError("room_sampling.read_budget exceeds the structural ceiling")
    else:
        read_budget = None
    sampled = value.get("sampled")

    if not isinstance(seed, str) or re.fullmatch(r"[0-9a-f]{32}", seed) is None:
        raise ValueError("room_sampling.seed is not a 32-hex seed")
    if not isinstance(frame_id, str) or HASH16_RE.fullmatch(frame_id) is None:
        raise ValueError("room_sampling.frame_id is not a 16-hex identifier")
    sampled_ceiling = (
        read_budget if read_budget is not None else ROOM_SAMPLING_STRUCTURAL_CEILING
    )
    if not isinstance(sampled, list) or not 1 <= len(sampled) <= sampled_ceiling:
        raise ValueError(
            "room_sampling.sampled is not a non-empty list within its read limit"
        )

    rooms: list[dict[str, Any]] = []
    seen: set[str] = set()
    for entry in sampled:
        if not isinstance(entry, dict):
            raise ValueError("room_sampling.sampled contains a non-object")
        room_id = entry.get("id")
        success = entry.get("success")
        if not isinstance(room_id, str) or HASH16_RE.fullmatch(room_id) is None:
            raise ValueError("room_sampling sampled id is not a 16-hex identifier")
        if room_id in seen:
            raise ValueError("room_sampling contains a duplicate sampled id")
        if not isinstance(success, bool):
            raise ValueError("room_sampling sampled success is not boolean")
        seen.add(room_id)
        rooms.append({"id": room_id, "success": success})

    if len(rooms) > frame_size:
        raise ValueError("room_sampling samples more rooms than its frame")

    return {
        "selector_version": selector_version,
        "seed": seed,
        "epoch": epoch,
        "frame_id": frame_id,
        "frame_size": frame_size,
        "read_budget": read_budget,
        "sampled": rooms,
    }


def validate_engagement(value: Any) -> dict[str, Any] | None:
    """Validate the service-published engagement object, field by field.

    The collector projects the /rooms `engagement` object down to the fields
    read here, keeping only bool, int, float and null values, so those
    values are the service's choice, not the collector's. Rejecting the
    whole tick over a malformed field would hand the service a lever to
    invalidate forward-collected history, so this validator never raises:
    a field that is absent or ill-typed becomes None and is published as
    "not recorded", never as zero. An absent or non-object `engagement`
    (every tick written before the collector carried it) is None as a whole.
    """
    if not isinstance(value, dict):
        return None

    def finite_number(raw: Any, maximum: float | None = None) -> float | None:
        if isinstance(raw, bool) or not isinstance(raw, (int, float)):
            return None
        if raw < 0 or isinstance(raw, int) and raw > MAX_INTEGER:
            return None
        number = float(raw)
        if not math.isfinite(number):
            return None
        if maximum is not None and number > maximum:
            return None
        return number

    def non_negative_int(raw: Any) -> int | None:
        if (
            isinstance(raw, bool)
            or not isinstance(raw, int)
            or raw < 0
            or raw > MAX_INTEGER
        ):
            return None
        return raw

    return {
        "windowed_note_to_message_ratio": finite_number(
            value.get("windowed_note_to_message_ratio")
        ),
        "zero_response_share": finite_number(
            value.get("zero_response_share"), maximum=1.0
        ),
        "nick_diversity": finite_number(value.get("nick_diversity")),
        "window_cap": non_negative_int(value.get("window_cap")),
        "windowed_messages": non_negative_int(value.get("windowed_messages")),
    }


def validate_identity_census_run(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, dict) or set(value) != CENSUS_RUN_FIELDS:
        raise ValueError("identity_census_run is not a complete census-run object")

    walk_started_at = value["walk_started_at"]
    try:
        parse_ts(walk_started_at)
    except ValueError as error:
        raise ValueError(
            "identity_census_run.walk_started_at is not a valid timestamp"
        ) from error

    outstanding_at_start = integer(
        value["shards_outstanding_at_start"],
        "identity_census_run.shards_outstanding_at_start",
    )
    shards_collected = integer(
        value["shards_collected"],
        "identity_census_run.shards_collected",
    )
    shards_outstanding = integer(
        value["shards_outstanding"],
        "identity_census_run.shards_outstanding",
    )
    passes_attempted = integer(
        value["passes_attempted"],
        "identity_census_run.passes_attempted",
    )
    maximum_passes = integer(
        value["maximum_passes"],
        "identity_census_run.maximum_passes",
        1,
    )
    deadline_seconds = integer(
        value["deadline_seconds"],
        "identity_census_run.deadline_seconds",
        1,
    )
    reads_attempted = integer(
        value["shard_reads_attempted"],
        "identity_census_run.shard_reads_attempted",
    )
    read_failures = integer(
        value["shard_read_failures"],
        "identity_census_run.shard_read_failures",
    )
    stop_reason = value["stop_reason"]

    failure_causes = value["failure_causes"]
    if not isinstance(failure_causes, dict):
        raise ValueError("identity_census_run.failure_causes is not an object")
    validated_causes: dict[str, int] = {}
    for cause, count in failure_causes.items():
        if not isinstance(cause, str) or (
            cause not in CENSUS_FAILURE_CAUSES
            and re.fullmatch(r"http_\d{3}", cause) is None
        ):
            raise ValueError("identity_census_run contains an invalid failure cause")
        validated_causes[cause] = integer(
            count,
            f"identity_census_run.failure_causes.{cause}",
        )

    if (
        outstanding_at_start > CENSUS_SHARD_COUNT
        or shards_collected > CENSUS_SHARD_COUNT
        or shards_outstanding > CENSUS_SHARD_COUNT
        or shards_collected + shards_outstanding != CENSUS_SHARD_COUNT
        or shards_outstanding > outstanding_at_start
    ):
        raise ValueError("identity_census_run shard accounting is inconsistent")
    if (
        passes_attempted > maximum_passes
        or read_failures > reads_attempted
        or sum(validated_causes.values()) != read_failures
        or outstanding_at_start - shards_outstanding > reads_attempted - read_failures
    ):
        raise ValueError("identity_census_run read accounting is inconsistent")
    if stop_reason not in CENSUS_STOP_REASONS:
        raise ValueError("identity_census_run.stop_reason is invalid")
    if (stop_reason == "complete") != (shards_outstanding == 0):
        raise ValueError("identity_census_run stop reason contradicts shard state")
    if stop_reason == "maximum_passes" and passes_attempted != maximum_passes:
        raise ValueError("identity_census_run stopped before its maximum pass")

    return {
        "walk_started_at": walk_started_at,
        "shards_outstanding_at_start": outstanding_at_start,
        "shards_collected": shards_collected,
        "shards_outstanding": shards_outstanding,
        "passes_attempted": passes_attempted,
        "maximum_passes": maximum_passes,
        "deadline_seconds": deadline_seconds,
        "shard_reads_attempted": reads_attempted,
        "shard_read_failures": read_failures,
        "failure_causes": dict(sorted(validated_causes.items())),
        "stop_reason": stop_reason,
    }


def validate_event(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("event is not an object")
    seq = integer(value.get("seq"), "event.seq")
    ts = value.get("ts")
    parse_ts(ts)
    name = value.get("name")
    primary = value.get("primary_class")
    base_name = value.get("base_name")
    if (
        not isinstance(name, str)
        or not isinstance(base_name, str)
        or primary not in CLASSES
    ):
        raise ValueError("event contains an invalid name or class")
    return {
        "seq": seq,
        "ts": ts,
        "name": name,
        "primary_class": primary,
        "base_name": base_name,
    }


def validate_room_lifecycle_sampling(
    value: Any,
    tick_datetime: datetime,
    *,
    require_finalization: bool = False,
) -> dict[str, Any] | None:
    if value is None:
        return None
    base_fields = {
        "aged_out_unselected",
        "selection",
        "coverage_by_stage",
    }
    if not isinstance(value, dict) or set(value) not in (
        base_fields,
        base_fields | {"aged_out_finalization"},
    ):
        raise ValueError(
            "room_lifecycle_sampling is not a complete sampling-evidence object"
        )
    # From collector 2.13.0 the attempted-late split and the bounded aged-out
    # finalization record ship together; older ticks carry neither and keep
    # their historical accounting, published as not recorded.
    has_finalization = "aged_out_finalization" in value
    if require_finalization and not has_finalization:
        raise ValueError(
            "room_lifecycle_sampling is missing its aged-out finalization evidence"
        )

    selection = value["selection"]
    expected_selection_fields = {
        "allocation_rotation",
        "eligibility",
        "initial_allocation_by_stage",
        "rank",
        "read_budget",
        "redistributed_reads",
        "selected_by_stage",
        "selector_seed",
        "selector_version",
        "short_stage_seconds",
        "tick_timestamp",
    }
    if not isinstance(selection, dict) or set(selection) != expected_selection_fields:
        raise ValueError("room_lifecycle_sampling.selection is incomplete")

    def descriptor_value(raw: Any, field: str, depth: int = 0) -> Any:
        if isinstance(raw, str):
            if not raw.strip() or len(raw) > 512:
                raise ValueError(f"{field} is not valid text")
            return raw
        if isinstance(raw, bool):
            return raw
        if isinstance(raw, int):
            return integer(raw, field)
        if depth >= 3:
            raise ValueError(f"{field} exceeds the descriptor depth limit")
        if isinstance(raw, list) and 1 <= len(raw) <= 32:
            return [
                descriptor_value(item, f"{field}.{index}", depth + 1)
                for index, item in enumerate(raw)
            ]
        if isinstance(raw, dict) and 1 <= len(raw) <= 32:
            result: dict[str, Any] = {}
            for key, item in raw.items():
                if not isinstance(key, str) or not key.strip() or len(key) > 64:
                    raise ValueError(f"{field} contains an invalid field name")
                result[key] = descriptor_value(
                    item,
                    f"{field}.{key}",
                    depth + 1,
                )
            return result
        raise ValueError(f"{field} is not valid descriptor data")

    rank_value = selection["rank"]
    if not isinstance(rank_value, dict) or not {
        "algorithm",
        "canonicalization",
    }.issubset(rank_value):
        raise ValueError(
            "room_lifecycle_sampling.selection.rank is not a complete descriptor"
        )
    rank = descriptor_value(
        rank_value,
        "room_lifecycle_sampling.selection.rank",
    )
    if (
        not isinstance(rank["algorithm"], str)
        or rank["algorithm"] != "sha256"
        or not isinstance(rank["canonicalization"], str)
        or not rank["canonicalization"].startswith("UTF-8 JSON")
    ):
        raise ValueError(
            "room_lifecycle_sampling.selection.rank is not a supported descriptor"
        )

    eligibility_value = selection["eligibility"]
    if not isinstance(eligibility_value, dict) or set(eligibility_value) != {
        "lower_bound",
        "upper_bound",
    }:
        raise ValueError(
            "room_lifecycle_sampling.selection.eligibility is not a complete descriptor"
        )
    eligibility: dict[str, str] = {}
    for field in ("lower_bound", "upper_bound"):
        descriptor = eligibility_value[field]
        if (
            not isinstance(descriptor, str)
            or not descriptor.strip()
            or len(descriptor) > 256
        ):
            raise ValueError(
                f"room_lifecycle_sampling.selection.eligibility.{field} is not valid text"
            )
        eligibility[field] = descriptor
    if eligibility != {
        "lower_bound": "due_at <= tick_timestamp",
        "upper_bound": "tick_timestamp < due_at + stage_seconds",
    }:
        raise ValueError(
            "room_lifecycle_sampling.selection.eligibility does not describe "
            "the recorded eligibility window"
        )

    selector_seed = selection["selector_seed"]
    if (
        not isinstance(selector_seed, str)
        or len(selector_seed) not in {16, 32, 64}
        or re.fullmatch(r"[0-9a-f]+", selector_seed) is None
    ):
        raise ValueError(
            "room_lifecycle_sampling.selection.selector_seed is not lowercase hexadecimal"
        )

    tick_timestamp = selection["tick_timestamp"]
    try:
        selection_tick_datetime = parse_ts(tick_timestamp)
    except ValueError as error:
        raise ValueError(
            "room_lifecycle_sampling.selection.tick_timestamp is not valid"
        ) from error
    if selection_tick_datetime != tick_datetime:
        raise ValueError(
            "room_lifecycle_sampling selection tick timestamp does not match the tick timestamp"
        )

    stage_keys = tuple(str(stage) for stage in ROOM_REVISIT_STAGES_SECONDS)
    selected_by_stage = selection["selected_by_stage"]
    if not isinstance(selected_by_stage, dict) or set(selected_by_stage) != set(
        stage_keys
    ):
        raise ValueError("room_lifecycle_sampling.selection stages are incomplete")
    validated_selected = {
        stage: integer(
            selected_by_stage[stage],
            f"room_lifecycle_sampling.selection.selected_by_stage.{stage}",
        )
        for stage in stage_keys
    }

    initial_allocation = selection["initial_allocation_by_stage"]
    if not isinstance(initial_allocation, dict) or set(initial_allocation) != set(
        stage_keys
    ):
        raise ValueError(
            "room_lifecycle_sampling selection initial stages are incomplete"
        )
    validated_initial = {
        stage: integer(
            initial_allocation[stage],
            (f"room_lifecycle_sampling.selection.initial_allocation_by_stage.{stage}"),
        )
        for stage in stage_keys
    }

    read_budget = integer(
        selection["read_budget"],
        "room_lifecycle_sampling.selection.read_budget",
    )
    allocation_rotation = integer(
        selection["allocation_rotation"],
        "room_lifecycle_sampling.selection.allocation_rotation",
    )
    if allocation_rotation >= len(stage_keys):
        raise ValueError(
            "room_lifecycle_sampling selection allocation rotation is invalid"
        )

    short_stage_seconds = integer(
        selection["short_stage_seconds"],
        "room_lifecycle_sampling.selection.short_stage_seconds",
        1,
    )
    expected_short_stage_seconds = ROOM_REVISIT_STAGES_SECONDS[allocation_rotation]
    if short_stage_seconds != expected_short_stage_seconds:
        raise ValueError(
            "room_lifecycle_sampling selection short stage is inconsistent"
        )

    # Validate the allocation contract without copying the collector's cyclic
    # surplus-placement formula: the split is balanced, exhausts the budget,
    # and the rotating stage receives the short allocation.
    base_allocation = read_budget // len(stage_keys)
    allowed_allocations = {base_allocation, base_allocation + 1}
    if (
        sum(validated_initial.values()) != read_budget
        or any(
            allocation not in allowed_allocations
            for allocation in validated_initial.values()
        )
        or validated_initial[str(short_stage_seconds)] != base_allocation
    ):
        raise ValueError(
            "room_lifecycle_sampling selection initial allocation is inconsistent"
        )

    redistributed_reads = integer(
        selection["redistributed_reads"],
        "room_lifecycle_sampling.selection.redistributed_reads",
    )
    selected_total = sum(validated_selected.values())
    redistributed_total = sum(
        max(0, validated_selected[stage] - validated_initial[stage])
        for stage in stage_keys
    )
    if selected_total > read_budget or redistributed_reads != redistributed_total:
        raise ValueError("room_lifecycle_sampling selection accounting is inconsistent")

    coverage_by_stage = value["coverage_by_stage"]
    if not isinstance(coverage_by_stage, dict) or set(coverage_by_stage) != set(
        stage_keys
    ):
        raise ValueError("room_lifecycle_sampling coverage stages are incomplete")

    expected_stage_fields = {
        "scheduled_due_rooms",
        "ineligible_superseded_before_due",
        "eligible_rooms",
        "attempted_checks",
        "completed_checks",
        "failed_checks",
        "deferred_checks",
        "aged_out_unselected",
        "superseded_after_eligibility",
        "coverage_fraction",
        "second_message_fraction",
    }
    validated_coverage: dict[str, dict[str, Any]] = {}
    total_aged_out = 0
    for stage in stage_keys:
        coverage = coverage_by_stage[stage]
        stage_fields = expected_stage_fields
        if has_finalization:
            stage_fields = expected_stage_fields | {"attempted_late"}
        if not isinstance(coverage, dict) or set(coverage) != stage_fields:
            if (
                isinstance(coverage, dict)
                and set(coverage) - {"attempted_late"} == expected_stage_fields
            ):
                raise ValueError(
                    "room_lifecycle_sampling attempted-late evidence is incomplete"
                )
            raise ValueError(
                f"room_lifecycle_sampling coverage for stage {stage} is incomplete"
            )

        validated = {
            field: integer(
                coverage[field],
                f"room_lifecycle_sampling.coverage_by_stage.{stage}.{field}",
            )
            for field in stage_fields
            if field not in {"coverage_fraction", "second_message_fraction"}
        }
        if not has_finalization:
            # Ticks recorded before collector 2.13.0 did not separate late
            # attempts from aged-out checks; the split is not recorded, and
            # absence never becomes zero.
            validated["attempted_late"] = None
        if (
            validated["scheduled_due_rooms"]
            != validated["ineligible_superseded_before_due"]
            + validated["eligible_rooms"]
        ):
            raise ValueError(
                "room_lifecycle_sampling scheduled-due accounting is inconsistent"
            )
        if validated["eligible_rooms"] != (
            validated["attempted_checks"]
            + (validated["attempted_late"] or 0)
            + validated["deferred_checks"]
            + validated["aged_out_unselected"]
            + validated["superseded_after_eligibility"]
        ):
            raise ValueError(
                "room_lifecycle_sampling eligible-room accounting is inconsistent"
            )
        if (
            validated["attempted_checks"]
            != validated["completed_checks"] + validated["failed_checks"]
        ):
            raise ValueError(
                "room_lifecycle_sampling attempted-check accounting is inconsistent"
            )

        coverage_fraction = coverage["coverage_fraction"]
        if not isinstance(coverage_fraction, dict) or set(coverage_fraction) != {
            "numerator",
            "denominator",
        }:
            raise ValueError("room_lifecycle_sampling coverage_fraction is incomplete")
        coverage_numerator = integer(
            coverage_fraction["numerator"],
            (
                f"room_lifecycle_sampling.coverage_by_stage.{stage}.coverage_fraction.numerator"
            ),
        )
        coverage_denominator = integer(
            coverage_fraction["denominator"],
            (
                f"room_lifecycle_sampling.coverage_by_stage.{stage}.coverage_fraction.denominator"
            ),
        )
        if (
            coverage_numerator != validated["completed_checks"]
            or coverage_denominator != validated["eligible_rooms"]
            or coverage_numerator > coverage_denominator
        ):
            raise ValueError(
                "room_lifecycle_sampling coverage-fraction accounting is inconsistent"
            )
        validated["coverage_fraction"] = {
            "numerator": coverage_numerator,
            "denominator": coverage_denominator,
        }

        fraction = coverage["second_message_fraction"]
        if not isinstance(fraction, dict) or set(fraction) != {
            "numerator",
            "denominator",
        }:
            raise ValueError(
                "room_lifecycle_sampling second_message_fraction is incomplete"
            )
        numerator = integer(
            fraction["numerator"],
            (
                "room_lifecycle_sampling.coverage_by_stage."
                f"{stage}.second_message_fraction.numerator"
            ),
        )
        denominator = integer(
            fraction["denominator"],
            (
                "room_lifecycle_sampling.coverage_by_stage."
                f"{stage}.second_message_fraction.denominator"
            ),
        )
        if numerator > denominator or denominator > validated["completed_checks"]:
            raise ValueError(
                "room_lifecycle_sampling second-message accounting is inconsistent"
            )
        # The collector counts second messages only for checks that found the room
        # present, so the denominator is those present checks, not every completed
        # check. A completed check that returned 404 observed no room to be quiet.
        validated["checked_and_quiet"] = denominator - numerator
        validated["second_message_fraction"] = {
            "numerator": numerator,
            "denominator": denominator,
        }
        validated_coverage[stage] = validated
        total_aged_out += validated["aged_out_unselected"]

    aged_out_unselected = integer(
        value["aged_out_unselected"],
        "room_lifecycle_sampling.aged_out_unselected",
    )
    if aged_out_unselected != total_aged_out:
        raise ValueError("room_lifecycle_sampling aged-out accounting is inconsistent")

    finalization: dict[str, int] | None = None
    if has_finalization:
        raw_finalization = value["aged_out_finalization"]
        if not isinstance(raw_finalization, dict) or set(raw_finalization) != {
            "finalized_this_tick",
            "backlog_remaining",
            "batch_limit",
        }:
            raise ValueError(
                "room_lifecycle_sampling.aged_out_finalization is not a "
                "complete finalization object"
            )
        finalization = {
            field: integer(
                raw_finalization[field],
                f"room_lifecycle_sampling.aged_out_finalization.{field}",
            )
            for field in ("finalized_this_tick", "backlog_remaining", "batch_limit")
        }
        if finalization["batch_limit"] < 1:
            raise ValueError(
                "room_lifecycle_sampling.aged_out_finalization.batch_limit is invalid"
            )
        if finalization["finalized_this_tick"] > finalization["batch_limit"]:
            raise ValueError(
                "room_lifecycle_sampling finalization exceeds its batch limit"
            )
        # finalized_this_tick and backlog_remaining describe only the tick
        # being reported; the coverage figures beside them run since the
        # ledger began. No equality between the two scopes is enforced.

    return {
        "aged_out_unselected": aged_out_unselected,
        "aged_out_finalization": finalization,
        "selection": {
            "allocation_rotation": allocation_rotation,
            "eligibility": eligibility,
            "initial_allocation_by_stage": validated_initial,
            "rank": rank,
            "read_budget": read_budget,
            "redistributed_reads": redistributed_reads,
            "selected_by_stage": validated_selected,
            "selector_seed": selector_seed,
            "selector_version": integer(
                selection["selector_version"],
                "room_lifecycle_sampling.selection.selector_version",
                1,
            ),
            "short_stage_seconds": short_stage_seconds,
            "tick_timestamp": tick_timestamp,
        },
        "coverage_by_stage": validated_coverage,
    }


def validate_room_lifecycle(
    value: Any,
    *,
    require_generation_contract: bool = False,
    sampling_contract: bool = False,
) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError("room_lifecycle is not an object")

    ledger_started_at = optional_timestamp(
        value.get("ledger_started_at"),
        "room_lifecycle.ledger_started_at",
    )
    result = {
        "ledger_started_at": ledger_started_at,
        "rooms_in_ledger": integer(
            value.get("rooms_in_ledger"),
            "room_lifecycle.rooms_in_ledger",
        ),
        "rooms_revisited": integer(
            value.get("rooms_revisited"),
            "room_lifecycle.rooms_revisited",
        ),
        "rooms_successfully_revisited": integer(
            value.get("rooms_successfully_revisited"),
            "room_lifecycle.rooms_successfully_revisited",
        ),
        "rooms_with_second_message": integer(
            value.get("rooms_with_second_message"),
            "room_lifecycle.rooms_with_second_message",
        ),
        "reads_attempted": integer(
            value.get("reads_attempted"),
            "room_lifecycle.reads_attempted",
        ),
        "reads_failed": integer(
            value.get("reads_failed"),
            "room_lifecycle.reads_failed",
        ),
        "created_rooms_observed_this_tick": integer(
            value.get("created_rooms_observed_this_tick"),
            "room_lifecycle.created_rooms_observed_this_tick",
        ),
        "due_this_tick": integer(
            value.get("due_this_tick"),
            "room_lifecycle.due_this_tick",
        ),
        "attempted_this_tick": integer(
            value.get("attempted_this_tick"),
            "room_lifecycle.attempted_this_tick",
        ),
        "deferred_due_to_budget": integer(
            value.get("deferred_due_to_budget"),
            "room_lifecycle.deferred_due_to_budget",
        ),
    }

    has_generation_contract = (
        require_generation_contract or "superseded_this_tick" in value
    )
    result["superseded_this_tick"] = (
        integer(
            value.get("superseded_this_tick"),
            "room_lifecycle.superseded_this_tick",
        )
        if has_generation_contract
        else None
    )
    has_superseded_batch_contract = (
        require_generation_contract or "deferred_superseded_due_to_batch_limit" in value
    )
    result["deferred_superseded_due_to_batch_limit"] = (
        integer(
            value.get("deferred_superseded_due_to_batch_limit"),
            "room_lifecycle.deferred_superseded_due_to_batch_limit",
        )
        if has_superseded_batch_contract
        else None
    )

    deferred_due_to_read_budget = value.get("deferred_due_to_read_budget")
    deferred_due_to_deadline = value.get("deferred_due_to_deadline")
    if deferred_due_to_read_budget is None and deferred_due_to_deadline is None:
        result["deferred_due_to_read_budget"] = None
        result["deferred_due_to_deadline"] = None
        has_wall_clock_contract = False
    elif deferred_due_to_read_budget is None or deferred_due_to_deadline is None:
        raise ValueError("room_lifecycle deferral reasons are incomplete")
    else:
        result["deferred_due_to_read_budget"] = integer(
            deferred_due_to_read_budget,
            "room_lifecycle.deferred_due_to_read_budget",
        )
        result["deferred_due_to_deadline"] = integer(
            deferred_due_to_deadline,
            "room_lifecycle.deferred_due_to_deadline",
        )
        has_wall_clock_contract = True

    if not (
        result["rooms_with_second_message"]
        <= result["rooms_successfully_revisited"]
        <= result["rooms_revisited"]
        <= result["rooms_in_ledger"]
    ):
        raise ValueError("room_lifecycle room denominators are inconsistent")
    if result["reads_failed"] > result["reads_attempted"]:
        raise ValueError("room_lifecycle failed reads exceed attempted reads")
    finalized_this_tick = result["attempted_this_tick"] + (
        result["superseded_this_tick"] or 0
    )
    deferred_superseded = result["deferred_superseded_due_to_batch_limit"] or 0
    if sampling_contract:
        due_accounting_is_inconsistent = (
            result["attempted_this_tick"] > result["due_this_tick"]
            or result["deferred_due_to_budget"]
            != result["due_this_tick"] - result["attempted_this_tick"]
        )
    else:
        due_accounting_is_inconsistent = (
            finalized_this_tick > result["due_this_tick"]
            or result["deferred_due_to_budget"]
            != result["due_this_tick"] - finalized_this_tick
        )
    if due_accounting_is_inconsistent:
        raise ValueError("room_lifecycle due-read accounting is inconsistent")

    if has_wall_clock_contract:
        expected_deferred = (
            result["deferred_due_to_read_budget"] + result["deferred_due_to_deadline"]
        )
        if not sampling_contract:
            expected_deferred += deferred_superseded
        if expected_deferred != result["deferred_due_to_budget"]:
            raise ValueError(
                "room_lifecycle deferral reasons do not partition deferred work"
            )
    if result["rooms_in_ledger"] == 0 and ledger_started_at is not None:
        raise ValueError("empty room ledger has a start timestamp")
    if result["rooms_in_ledger"] > 0 and ledger_started_at is None:
        raise ValueError("non-empty room ledger has no start timestamp")

    sender_classes = value.get("second_sender_classes")
    expected_sender_classes = {
        "signed_did",
        "unsigned_did",
        "server",
        "other",
        "not_observed",
    }
    if (
        not isinstance(sender_classes, dict)
        or set(sender_classes) != expected_sender_classes
    ):
        raise ValueError("room_lifecycle second-sender classes are incomplete")
    result["second_sender_classes"] = {
        key: integer(
            sender_classes[key],
            f"room_lifecycle.second_sender_classes.{key}",
        )
        for key in sorted(expected_sender_classes)
    }
    sender_class_total = sum(result["second_sender_classes"].values())
    if (
        has_generation_contract
        and sender_class_total != result["rooms_with_second_message"]
    ):
        raise ValueError(
            "room_lifecycle sender classes do not partition their room denominator"
        )
    if sender_class_total > result["rooms_with_second_message"]:
        raise ValueError("room_lifecycle sender classes exceed their room denominator")

    revisits = value.get("revisits")
    if (
        not isinstance(revisits, list)
        or len(revisits) > ROOM_REVISIT_STRUCTURAL_CEILING
        or len(revisits) != finalized_this_tick
    ):
        raise ValueError("room_lifecycle revisits exceed their recorded bounds")
    validated_revisits: list[dict[str, Any]] = []
    superseded_revisits = 0
    seen_revisit_keys: set[tuple[int, int]] = set()
    room_ids_by_creation: dict[int, str] = {}
    for revisit in revisits:
        if not isinstance(revisit, dict):
            raise ValueError("room_lifecycle revisit is not an object")
        room_id = revisit.get("id")
        created_seq = revisit.get("created_seq")
        stage_seconds = revisit.get("stage_seconds")
        success = revisit.get("success")
        if (
            not isinstance(room_id, str)
            or HASH16_RE.fullmatch(room_id) is None
            or stage_seconds not in ROOM_REVISIT_STAGES_SECONDS
            or not isinstance(success, bool)
        ):
            raise ValueError("room_lifecycle revisit has an invalid identity or stage")

        if created_seq is None:
            if has_generation_contract:
                raise ValueError(
                    "current room_lifecycle revisit lacks its creation sequence"
                )
        else:
            created_seq = integer(
                created_seq,
                "room_lifecycle.revisit.created_seq",
            )
            revisit_key = (created_seq, stage_seconds)
            if revisit_key in seen_revisit_keys:
                raise ValueError("room_lifecycle contains a duplicate generation stage")
            prior_room_id = room_ids_by_creation.setdefault(created_seq, room_id)
            if prior_room_id != room_id:
                raise ValueError(
                    "room_lifecycle maps one creation sequence to multiple room ids"
                )
            seen_revisit_keys.add(revisit_key)

        outcome = revisit.get("outcome")
        if has_generation_contract:
            if outcome not in ROOM_REVISIT_OUTCOMES:
                raise ValueError(
                    "current room_lifecycle revisit has an invalid outcome"
                )
        elif outcome is not None:
            if outcome not in ROOM_REVISIT_OUTCOMES - {"superseded_before_check"}:
                raise ValueError("legacy room_lifecycle revisit has an invalid outcome")

        elapsed_since_creation_seconds = revisit.get("elapsed_since_creation_seconds")
        if outcome == "superseded_before_check":
            superseded_revisits += 1
            if success:
                raise ValueError("superseded room revisit has a contradictory outcome")
            if elapsed_since_creation_seconds is not None:
                raise ValueError(
                    "superseded room revisit records elapsed time without an origin read"
                )
        elif elapsed_since_creation_seconds is None:
            if has_wall_clock_contract:
                raise ValueError(
                    "new-format room revisit lacks its actual elapsed time"
                )
        else:
            elapsed_since_creation_seconds = integer(
                elapsed_since_creation_seconds,
                "room_lifecycle.revisit.elapsed_since_creation_seconds",
            )
            if elapsed_since_creation_seconds < stage_seconds:
                raise ValueError("room revisit occurred before its nominal due stage")

        message_count = revisit.get("message_count")
        has_second_message = revisit.get("has_second_message")
        second_sender_class = revisit.get("second_sender_class")
        if not success:
            if outcome is not None and outcome not in {
                "absent_at_last_check",
                "check_failed",
                "superseded_before_check",
            }:
                raise ValueError("failed room revisit has a contradictory outcome")
            if (
                message_count is not None
                or has_second_message is not None
                or second_sender_class is not None
            ):
                raise ValueError("failed room revisit contains an activity result")
        else:
            if outcome is not None and outcome != "present_at_last_check":
                raise ValueError("successful room revisit has a contradictory outcome")
            message_count = integer(
                message_count,
                "room_lifecycle.revisit.message_count",
            )
            if not isinstance(has_second_message, bool):
                raise ValueError(
                    "successful room revisit lacks its second-message result"
                )
            if has_second_message:
                if second_sender_class not in expected_sender_classes:
                    raise ValueError("room revisit has an invalid second-sender class")
            elif second_sender_class is not None:
                raise ValueError("room without a second message has a sender class")
        validated_revisits.append(
            {
                "id": room_id,
                "created_seq": created_seq,
                "stage_seconds": stage_seconds,
                "elapsed_since_creation_seconds": elapsed_since_creation_seconds,
                "success": success,
                "outcome": outcome,
                "message_count": message_count,
                "has_second_message": has_second_message,
                "second_sender_class": second_sender_class,
            }
        )
    if has_generation_contract and (
        superseded_revisits != result["superseded_this_tick"]
    ):
        raise ValueError("room_lifecycle superseded accounting is inconsistent")
    result["revisits"] = validated_revisits

    budget = value.get("read_budget")
    if not isinstance(budget, dict):
        raise ValueError("room_lifecycle has no read-budget accounting")
    assumed_tick_seconds = budget.get("assumed_tick_seconds")
    rate_window_seconds = budget.get("rate_window_seconds")
    tick_revisit_deadline_seconds = budget.get("tick_revisit_deadline_seconds")
    if rate_window_seconds is None and tick_revisit_deadline_seconds is None:
        assumed_tick_seconds = integer(
            assumed_tick_seconds,
            "read_budget.assumed_tick_seconds",
            1,
        )
        normalization_seconds = assumed_tick_seconds
    elif (
        rate_window_seconds is None
        or tick_revisit_deadline_seconds is None
        or assumed_tick_seconds is not None
    ):
        raise ValueError("read_budget wall-clock accounting is incomplete")
    else:
        rate_window_seconds = integer(
            rate_window_seconds,
            "read_budget.rate_window_seconds",
            1,
        )
        tick_revisit_deadline_seconds = integer(
            tick_revisit_deadline_seconds,
            "read_budget.tick_revisit_deadline_seconds",
            1,
        )
        normalization_seconds = rate_window_seconds

    validated_budget = {
        "base_reads": integer(budget.get("base_reads"), "read_budget.base_reads"),
        "revisit_reads": integer(
            budget.get("revisit_reads"),
            "read_budget.revisit_reads",
        ),
        "total_reads": integer(budget.get("total_reads"), "read_budget.total_reads"),
        "total_read_budget": integer(
            budget.get("total_read_budget"),
            "read_budget.total_read_budget",
            1,
        ),
        "revisit_read_budget": integer(
            budget.get("revisit_read_budget"),
            "read_budget.revisit_read_budget",
        ),
        "assumed_tick_seconds": assumed_tick_seconds,
        "rate_window_seconds": rate_window_seconds,
        "tick_revisit_deadline_seconds": tick_revisit_deadline_seconds,
        "published_reads_per_minute": integer(
            budget.get("published_reads_per_minute"),
            "read_budget.published_reads_per_minute",
            1,
        ),
    }
    for field in ("reads_per_minute", "share", "maximum_share"):
        raw = budget.get(field)
        if (
            isinstance(raw, bool)
            or not isinstance(raw, (int, float))
            or raw < 0
            or isinstance(raw, int)
            and raw > MAX_INTEGER
            or not math.isfinite(raw)
        ):
            raise ValueError(f"read_budget.{field} is not a valid number")
        validated_budget[field] = float(raw)
    expected_rate = validated_budget["total_reads"] * 60 / normalization_seconds
    expected_share = expected_rate / validated_budget["published_reads_per_minute"]
    if (
        validated_budget["total_reads"]
        != validated_budget["base_reads"] + validated_budget["revisit_reads"]
        or validated_budget["revisit_reads"] != result["attempted_this_tick"]
        or validated_budget["total_reads"] > validated_budget["total_read_budget"]
        or validated_budget["revisit_reads"] > validated_budget["revisit_read_budget"]
        or not math.isclose(
            validated_budget["reads_per_minute"],
            expected_rate,
        )
        or not math.isclose(validated_budget["share"], expected_share)
        or validated_budget["share"] > validated_budget["maximum_share"]
    ):
        raise ValueError("room_lifecycle read-budget accounting is inconsistent")
    if has_wall_clock_contract != (rate_window_seconds is not None):
        raise ValueError("room_lifecycle wall-clock fields are incomplete")
    if (
        has_wall_clock_contract
        and tick_revisit_deadline_seconds >= STALL_THRESHOLD_SECONDS
    ):
        raise ValueError("room_lifecycle wall-clock budget is inconsistent")
    if (
        has_wall_clock_contract
        and not sampling_contract
        and result["deferred_due_to_read_budget"]
        != max(
            0,
            result["due_this_tick"]
            - (result["superseded_this_tick"] or 0)
            - deferred_superseded
            - validated_budget["revisit_read_budget"],
        )
    ):
        raise ValueError("room_lifecycle wall-clock budget is inconsistent")
    result["read_budget"] = validated_budget
    return result


def validate_funnel(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError("signer_funnel is not an object")

    result: dict[str, Any] = {}
    for field in FUNNEL_BASE_FIELDS:
        result[field] = optional_integer(value.get(field), f"signer_funnel.{field}")

    census_started_at = value.get("census_started_at")
    census_completed_at = value.get("census_completed_at")
    if result["well_formed_did_notes"] is None:
        if census_started_at is not None or census_completed_at is not None:
            raise ValueError("funnel has a census timestamp without a census")
    else:
        census_started_at = optional_timestamp(
            census_started_at,
            "signer_funnel.census_started_at",
        )
        parse_ts(census_completed_at)
        if census_started_at is not None and parse_ts(census_started_at) > parse_ts(
            census_completed_at
        ):
            raise ValueError("funnel census starts after it completes")
    result["census_started_at"] = census_started_at
    result["census_completed_at"] = census_completed_at

    coverage = value.get("coverage")
    if not isinstance(coverage, dict):
        raise ValueError("signer funnel is missing its coverage data")
    sampled = integer(coverage.get("sampled_rooms"), "coverage.sampled_rooms")
    known = coverage.get("known_rooms")
    if known is not None:
        known = integer(known, "coverage.known_rooms", 1)
    result["coverage"] = {"sampled_rooms": sampled, "known_rooms": known}

    legacy_persistence = "two_collection_utc_dates" not in value
    persistence_field = (
        "two_utc_dates" if legacy_persistence else "two_collection_utc_dates"
    )
    raw_two_dates = optional_integer(
        value.get(persistence_field),
        f"signer_funnel.{persistence_field}",
    )
    raw_two_rooms = optional_integer(value.get("two_rooms"), "signer_funnel.two_rooms")
    legacy_reciprocity = "signed_reciprocal_alternation" not in value
    reciprocity_field = (
        "signed_counterparty" if legacy_reciprocity else "signed_reciprocal_alternation"
    )
    raw_reciprocity = optional_integer(
        value.get(reciprocity_field),
        f"signer_funnel.{reciprocity_field}",
    )

    raw_observed_stages = [
        result["dids_observed_signing"],
        result["seen_two_ticks"],
        raw_two_dates,
        raw_two_rooms,
        raw_reciprocity,
    ]
    if any(stage is None for stage in raw_observed_stages):
        raise ValueError("signer funnel has a missing observed stage")
    if any(
        right > left
        for left, right in zip(raw_observed_stages, raw_observed_stages[1:])
    ):
        raise ValueError("signer funnel stages are not monotonic")
    # The census and the observed-signing count measure different populations
    # (a signer need not have published a DID note), so observed > census is a
    # possible honest measurement and is not rejected.

    if legacy_persistence:
        result["two_collection_utc_dates"] = 0
        result["two_rooms"] = 0
        result["persistence_started_at"] = None
        result["persistence_reset_at"] = None
        result["persistence_collection_utc_dates_count"] = 0
    else:
        result["two_collection_utc_dates"] = raw_two_dates
        result["two_rooms"] = raw_two_rooms
        result["persistence_started_at"] = optional_timestamp(
            value.get("persistence_started_at"),
            "signer_funnel.persistence_started_at",
        )
        result["persistence_reset_at"] = optional_timestamp(
            value.get("persistence_reset_at"),
            "signer_funnel.persistence_reset_at",
        )
        result["persistence_collection_utc_dates_count"] = integer(
            value.get("persistence_collection_utc_dates_count"),
            "signer_funnel.persistence_collection_utc_dates_count",
        )

    result["signed_reciprocal_alternation"] = (
        None if legacy_reciprocity else raw_reciprocity
    )
    corrected_stages = [
        result["dids_observed_signing"],
        result["seen_two_ticks"],
        result["two_collection_utc_dates"],
        result["two_rooms"],
    ]
    if result["signed_reciprocal_alternation"] is not None:
        corrected_stages.append(result["signed_reciprocal_alternation"])
    if any(right > left for left, right in zip(corrected_stages, corrected_stages[1:])):
        raise ValueError("corrected signer funnel stages are not monotonic")

    result["legacy_persistence_reset"] = legacy_persistence
    result["legacy_reciprocity"] = legacy_reciprocity
    result["sustained_reciprocal_footprint"] = result["signed_reciprocal_alternation"]
    result["tracking_disclosure"] = validate_tracking_disclosure(
        value.get("tracking_disclosure")
    )
    raw_signer_state_version = value.get("signer_state_version")
    result["signer_state_version"] = (
        None
        if raw_signer_state_version is None
        else integer(
            raw_signer_state_version,
            "signer_funnel.signer_state_version",
            1,
        )
    )
    result["tracked_dids"] = integer(value.get("tracked_dids"), "tracked_dids")
    result["tracked_cap"] = integer(value.get("tracked_cap"), "tracked_cap", 1)
    cap_gates = not (
        result["signer_state_version"] is not None
        and result["signer_state_version"] >= 3
    ) and not (
        result["signer_state_version"] is None
        and result["tracking_disclosure"] is not None
    )
    if cap_gates and result["tracked_dids"] > result["tracked_cap"]:
        raise ValueError("tracked DID count exceeds its cap")
    if not isinstance(value.get("cap_hit"), bool):
        raise ValueError("signer funnel cap_hit is not boolean")
    result["cap_hit"] = value["cap_hit"]
    collection_started = value.get("collection_started")
    parse_ts(collection_started)
    result["collection_started"] = collection_started
    return result


def validate_tick(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("tick is not an object")
    ts = value.get("ts")
    parsed_ts = parse_ts(ts)
    try:
        parsed_ts - timedelta(seconds=DAILY_RETENTION_SECONDS)
        parsed_ts + timedelta(seconds=DAILY_ROLLUP_SECONDS)
    except OverflowError as error:
        raise ValueError(
            "tick timestamp cannot support the declared history windows"
        ) from error
    events = value.get("events_window")
    rooms = value.get("newest_rooms")
    if not isinstance(events, list) or not isinstance(rooms, list):
        raise ValueError("tick is missing event or room windows")

    validated_events = [validate_event(event) for event in events]
    event_sequences = [event["seq"] for event in validated_events]
    if event_sequences != sorted(set(event_sequences)):
        raise ValueError("event window is not strictly sequence ordered")

    validated_rooms: list[dict[str, Any]] = []
    for room in rooms:
        if not isinstance(room, dict):
            raise ValueError("room is not an object")
        name = room.get("name")
        seq = room.get("seq")
        idle = room.get("idle_seconds")
        if (
            not isinstance(name, str)
            or isinstance(seq, bool)
            or not isinstance(seq, int)
            or seq < 0
            or seq > MAX_INTEGER
            or isinstance(idle, bool)
            or not isinstance(idle, (int, float))
            or idle < 0
            or isinstance(idle, int)
            and idle > MAX_INTEGER
            or not math.isfinite(idle)
        ):
            raise ValueError("room has an invalid name, seq, or idle value")
        validated_rooms.append({"name": name, "seq": seq, "idle_seconds": float(idle)})

    identity_total = optional_integer(value.get("identity_total"), "identity_total")
    identity_census_started = optional_timestamp(
        value.get("identity_census_started"),
        "identity_census_started",
    )
    identity_census_run = validate_identity_census_run(value.get("identity_census_run"))
    if (
        identity_total is None
        and identity_census_started is not None
        and identity_census_run is None
    ):
        raise ValueError("tick has a census start without a census or census run")
    if (
        identity_census_started is not None
        and parse_ts(identity_census_started) > parsed_ts
    ):
        raise ValueError("tick census starts after the tick timestamp")
    if identity_census_run is not None:
        if identity_census_run["walk_started_at"] != identity_census_started:
            raise ValueError("tick census start disagrees with its census run")
        if parse_ts(identity_census_run["walk_started_at"]) > parsed_ts:
            raise ValueError("tick census run starts after the tick timestamp")
        if identity_census_run["shards_outstanding"] > 0 and identity_total is not None:
            raise ValueError("incomplete census run publishes an identity total")

    raw_collector_version = value.get("collector_version")
    collector_version = (
        "legacy"
        if raw_collector_version is None
        else version_string(raw_collector_version, "collector_version")
    )
    require_lifecycle_sampling = collector_version_at_least(
        collector_version,
        ROOM_LIFECYCLE_SAMPLING_COLLECTOR_VERSION,
    )
    lifecycle_sampling = validate_room_lifecycle_sampling(
        value.get("room_lifecycle_sampling"),
        parsed_ts,
        require_finalization=collector_version_at_least(
            collector_version,
            ROOM_AGED_OUT_FINALIZATION_COLLECTOR_VERSION,
        ),
    )
    if require_lifecycle_sampling and lifecycle_sampling is None:
        raise ValueError("current tick is missing room_lifecycle_sampling evidence")

    lifecycle = validate_room_lifecycle(
        value.get("room_lifecycle"),
        require_generation_contract=(
            collector_version_at_least(
                collector_version,
                ROOM_GENERATION_COLLECTOR_VERSION,
            )
            or lifecycle_sampling is not None
        ),
        sampling_contract=lifecycle_sampling is not None,
    )
    if lifecycle_sampling is not None:
        if lifecycle is None:
            raise ValueError(
                "room_lifecycle_sampling has no room_lifecycle measurement"
            )
        # coverage_by_stage is cumulative: the collector counts every revisit row whose
        # window has opened (`WHERE due_at <= selection_time`, with no lower bound),
        # while room_lifecycle counts only this tick. A running total and a snapshot
        # have no equality to enforce between them. The read budget is the one figure
        # both sides take from this tick, so it is the only one worth cross-checking.
        if (
            lifecycle["read_budget"]["revisit_read_budget"]
            != lifecycle_sampling["selection"]["read_budget"]
        ):
            raise ValueError(
                "room_lifecycle sampling and lifecycle accounting disagree"
            )

    result = {
        "collector_version": collector_version,
        "ledger_chain": (
            validate_ledger_chain(value["ledger_chain"])
            if "ledger_chain" in value
            else None
        ),
        "ts": ts,
        "_datetime": parsed_ts,
        "rooms_total": integer(value.get("rooms_total"), "rooms_total"),
        "room_cap": integer(value.get("room_cap"), "room_cap", 1),
        "bytes_stored": integer(value.get("bytes_stored"), "bytes_stored"),
        "notes_total": integer(value.get("notes_total"), "notes_total"),
        "note_cap": integer(value.get("note_cap"), "note_cap", 1),
        "lobby_last_seq": integer(value.get("lobby_last_seq"), "lobby_last_seq"),
        "events_last_seq": integer(value.get("events_last_seq"), "events_last_seq"),
        "identity_total": identity_total,
        "identity_census_started": identity_census_started,
        "identity_census_run": identity_census_run,
        "events_window": validated_events,
        "newest_rooms": validated_rooms,
        "room_sampling": validate_room_sampling(value.get("room_sampling")),
        "signer_funnel": validate_funnel(value.get("signer_funnel")),
        "room_lifecycle": lifecycle,
        "room_lifecycle_sampling": lifecycle_sampling,
        "engagement": validate_engagement(value.get("engagement")),
    }
    if validated_events and validated_events[-1]["seq"] != result["events_last_seq"]:
        raise ValueError("events_last_seq does not match the event window")
    return result


def auto_generated_looking(name: str) -> bool:
    if HEX_NAME_RE.fullmatch(name) or UUID_LIKE_RE.fullmatch(name):
        return True
    return bool(
        MIXED_TOKEN_RE.fullmatch(name)
        and any(character.isalpha() for character in name)
        and any(character.isdigit() for character in name)
    )


def rate(delta: int, seconds: float) -> dict[str, Any]:
    return {"value": delta / seconds, "samples": 2, "seconds": seconds}


def empty_rate() -> dict[str, Any]:
    return {"value": None, "samples": 0, "seconds": None}


ROLLUP_VALUE_FIELDS = (
    "observed_public_rooms",
    "lobby_last_seq",
    "notes_total",
)
ROLLUP_RATE_FIELDS = (
    "public_rooms_per_second",
    "lobby_messages_per_second",
    "rooms_total_per_second",
    "notes_per_second",
    "identities_per_second",
)


def utc_text(value: datetime) -> str:
    return (
        value.astimezone(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def aligned_floor(value: datetime, seconds: int) -> datetime:
    epoch = int(value.timestamp())
    return datetime.fromtimestamp(epoch - epoch % seconds, timezone.utc)


def aligned_end(value: datetime, seconds: int) -> datetime:
    return aligned_floor(value, seconds) + timedelta(seconds=seconds)


def rollup_snapshot(point: dict[str, Any]) -> dict[str, Any]:
    return {
        "ts": point["ts"],
        **{field: point[field] for field in ROLLUP_VALUE_FIELDS},
    }


def is_cadence_gap(gap: dict[str, Any], gap_seconds: float) -> bool:
    """Whether a gap record means the collector missed an observation.

    A polling gap always does. A counter decrease does only where the interval
    also ran past the polling threshold; within the threshold every tick
    arrived and only the service's counter moved backwards. An identity counter
    decrease spans census to census, not collector ticks.
    """
    if gap["reason"] == "polling_gap":
        return True
    return gap["reason"] == "counter_decreased" and gap["seconds"] > gap_seconds


def aggregate_rollup_bucket(
    points: list[dict[str, Any]],
    start: datetime,
    end: datetime,
    cadence_gaps: list[dict[str, Any]],
    expected_tick_seconds: float,
    window_start: datetime,
    window_end: datetime,
) -> dict[str, Any] | None:
    if not points:
        return None

    values = {
        field: [point[field] for point in points if point[field] is not None]
        for field in ROLLUP_VALUE_FIELDS
    }
    ratios: dict[str, dict[str, Any]] = {}
    observed_seconds = 0.0
    for point in points:
        interval = point["rates"]["public_rooms_per_second"]
        if interval["samples"] and interval["seconds"] is not None:
            observed_seconds += interval["seconds"]

    for field in ROLLUP_RATE_FIELDS:
        numerator = 0.0
        denominator = 0.0
        observations = 0
        for point in points:
            rate_value = point["rates"][field]
            if (
                rate_value["samples"]
                and rate_value["value"] is not None
                and rate_value["seconds"] is not None
            ):
                numerator += rate_value["value"] * rate_value["seconds"]
                denominator += rate_value["seconds"]
                observations += 1
        ratios[field] = {
            "numerator": numerator,
            "denominator": denominator,
            "value": numerator / denominator if denominator else None,
            "observation_count": observations,
        }

    # A boundary bucket only holds the span its own retention level covers, and
    # nothing exists before collection started, so expected counts that span and
    # never the ticks published by the neighbouring level.
    covered_seconds = max(
        0.0,
        (min(end, window_end) - max(start, window_start)).total_seconds(),
    )
    # The tick at a level's upper cutoff belongs to the next level, so a
    # boundary bucket holds only the whole cadence intervals its span can carry.
    expected = max(
        len(points),
        int(covered_seconds // expected_tick_seconds),
    )
    missing = max(0, expected - len(points))
    # Only a collector cadence gap breaks the line. An incomplete event window
    # is published in composition.complete, and a counter decrease inside the
    # polling threshold already suppresses that interval's rates.
    has_gap = any(
        parse_ts(gap["from"]) < end and parse_ts(gap["to"]) > start
        for gap in cadence_gaps
    )
    return {
        "start": utc_text(start),
        "end": utc_text(end),
        "first": rollup_snapshot(points[0]),
        "last": rollup_snapshot(points[-1]),
        "min": {
            field: min(field_values) if field_values else None
            for field, field_values in values.items()
        },
        "max": {
            field: max(field_values) if field_values else None
            for field, field_values in values.items()
        },
        "ratios": ratios,
        "observation_count": len(points),
        "expected_tick_count": expected,
        "missing_count": missing,
        "observed_seconds": observed_seconds,
        "complete": missing == 0 and not has_gap,
        "has_gap": has_gap,
    }


def rollup_level(
    points: list[dict[str, Any]],
    cadence_gaps: list[dict[str, Any]],
    resolution_seconds: int,
    resolution_label: str,
    expected_tick_seconds: float,
    window_start: datetime,
    window_end: datetime,
) -> dict[str, Any] | None:
    if not points:
        return None

    first_time = parse_ts(points[0]["ts"])
    last_time = parse_ts(points[-1]["ts"])
    start = aligned_floor(first_time, resolution_seconds)
    end = aligned_end(last_time, resolution_seconds)
    buckets: list[dict[str, Any] | None] = []
    point_index = 0
    bucket_start = start
    while bucket_start < end:
        bucket_end = bucket_start + timedelta(seconds=resolution_seconds)
        bucket_points: list[dict[str, Any]] = []
        while point_index < len(points):
            point_time = parse_ts(points[point_index]["ts"])
            if point_time >= bucket_end:
                break
            if point_time >= bucket_start:
                bucket_points.append(points[point_index])
            point_index += 1
        buckets.append(
            aggregate_rollup_bucket(
                bucket_points,
                bucket_start,
                bucket_end,
                cadence_gaps,
                expected_tick_seconds,
                window_start,
                window_end,
            )
        )
        bucket_start = bucket_end

    return {
        "resolution_seconds": resolution_seconds,
        "resolution_label": resolution_label,
        "start": utc_text(start),
        "end": utc_text(end),
        "buckets": buckets,
    }


def expected_tick_interval(
    points: list[dict[str, Any]],
    gap_seconds: float,
) -> float:
    intervals = [
        (parse_ts(right["ts"]) - parse_ts(left["ts"])).total_seconds()
        for left, right in zip(points, points[1:])
    ]
    usable = sorted(interval for interval in intervals if 0 < interval <= gap_seconds)
    if not usable:
        return gap_seconds
    middle = len(usable) // 2
    if len(usable) % 2:
        return usable[middle]
    return (usable[middle - 1] + usable[middle]) / 2


def reduce_payload_history(
    points: list[dict[str, Any]],
    gaps: list[dict[str, Any]],
    newest: datetime,
    gap_seconds: float,
) -> tuple[list[dict[str, Any]], dict[str, Any], list[dict[str, Any]]]:
    raw_cutoff = newest - timedelta(seconds=RAW_RETENTION_SECONDS)
    hourly_cutoff = newest - timedelta(seconds=HOURLY_RETENTION_SECONDS)
    daily_cutoff = newest - timedelta(seconds=DAILY_RETENTION_SECONDS)

    raw_points = [point for point in points if parse_ts(point["ts"]) >= raw_cutoff]
    hourly_points = [
        point for point in points if hourly_cutoff <= parse_ts(point["ts"]) < raw_cutoff
    ]
    daily_points = [
        point
        for point in points
        if daily_cutoff <= parse_ts(point["ts"]) < hourly_cutoff
    ]
    archive_points = [point for point in points if parse_ts(point["ts"]) < daily_cutoff]

    tick_seconds = expected_tick_interval(points, gap_seconds)
    collection_start = parse_ts(points[0]["ts"])
    cadence_gaps = [gap for gap in gaps if is_cadence_gap(gap, gap_seconds)]
    levels: list[dict[str, Any]] = []
    if archive_points:
        archive_start = aligned_floor(
            parse_ts(archive_points[0]["ts"]),
            DAILY_ROLLUP_SECONDS,
        )
        archive_end = aligned_end(
            parse_ts(archive_points[-1]["ts"]),
            DAILY_ROLLUP_SECONDS,
        )
        archive_bucket = aggregate_rollup_bucket(
            archive_points,
            archive_start,
            archive_end,
            cadence_gaps,
            tick_seconds,
            collection_start,
            daily_cutoff,
        )
        levels.append(
            {
                "resolution_seconds": None,
                "resolution_label": "lifetime archive rollup",
                "start": utc_text(archive_start),
                "end": utc_text(archive_end),
                "buckets": [archive_bucket],
            }
        )

    for level in (
        rollup_level(
            daily_points,
            cadence_gaps,
            DAILY_ROLLUP_SECONDS,
            "1-day rollup",
            tick_seconds,
            max(daily_cutoff, collection_start),
            hourly_cutoff,
        ),
        rollup_level(
            hourly_points,
            cadence_gaps,
            HOURLY_ROLLUP_SECONDS,
            "1-hour rollup",
            tick_seconds,
            max(hourly_cutoff, collection_start),
            raw_cutoff,
        ),
    ):
        if level is not None:
            levels.append(level)

    # Every recorded gap is still published; cadence_gap says which of them the
    # chart may break on, so the page cannot re-derive the rule and disagree.
    recent_gaps = [
        {**gap, "cadence_gap": is_cadence_gap(gap, gap_seconds)}
        for gap in gaps
        if parse_ts(gap["to"]) >= raw_cutoff
    ]
    history = {
        "raw_retention_seconds": RAW_RETENTION_SECONDS,
        "raw_started_at": raw_points[0]["ts"] if raw_points else None,
        "raw_resolution_label": "collector-tick raw · 24-hour retention",
        "chart_resolution_label": (
            "Chart: lifetime / 1-day / 1-hour rollups; scrubber: collector-tick raw (24 hours)"
        ),
        "expected_tick_seconds": tick_seconds,
        "rollup_levels": levels,
    }
    return raw_points, history, recent_gaps


def ledger_chain_summary(ticks: list[dict[str, Any]]) -> dict[str, Any]:
    genesis_index = next(
        (
            index
            for index, tick in enumerate(ticks)
            if tick["ledger_chain"] is not None
            and tick["ledger_chain"]["previous_sha256"] is None
        ),
        None,
    )
    if genesis_index is None:
        return {
            "version": 1,
            "algorithm": "sha256",
            "genesis_ts": None,
            "unchained_prefix_ticks": len(ticks),
            "chained_ticks": 0,
            "tip_tick_sha256": None,
            "externally_anchored": False,
            "display": (
                f"Hash chain not started · {format_int(len(ticks))} ticks remain "
                "unchained and rest on operator trust"
            ),
            "anchor_display": LEDGER_ANCHOR_NOT_RECORDED,
        }

    chained = [
        tick["ledger_chain"]
        for tick in ticks[genesis_index:]
        if tick["ledger_chain"] is not None
    ]
    return {
        "version": 1,
        "algorithm": "sha256",
        "genesis_ts": ticks[genesis_index]["ts"],
        "unchained_prefix_ticks": genesis_index,
        "chained_ticks": len(chained),
        "tip_tick_sha256": chained[-1]["tick_sha256"],
        "externally_anchored": False,
        "display": (
            f"SHA-256 chain from {ticks[genesis_index]['ts']} · "
            f"{format_int(len(chained))} chained ticks · "
            f"{format_int(genesis_index)} pre-genesis ticks remain unchained · "
            "internal consistency only; no proof of collection time without "
            "an external anchor"
        ),
        "anchor_display": LEDGER_ANCHOR_NOT_RECORDED,
    }


def read_jsonl(lines: Iterable[str]) -> tuple[list[dict[str, Any]], int]:
    ticks: list[dict[str, Any]] = []
    rejected = 0
    previous_time: datetime | None = None

    for line in lines:
        if not line.strip():
            continue
        try:
            tick = validate_tick(json.loads(line))
            if previous_time is not None and tick["_datetime"] <= previous_time:
                raise ValueError("tick timestamps are not strictly increasing")
        except (json.JSONDecodeError, ValueError, RecursionError):
            rejected += 1
            continue
        ticks.append(tick)
        previous_time = tick["_datetime"]

    return ticks, rejected


def trailing_segment(
    ticks: list[dict[str, Any]],
    index: int,
    cap_field: str,
    gap_seconds: float,
) -> list[dict[str, Any]]:
    current = ticks[index]
    segment = [current]
    for candidate_index in range(index - 1, -1, -1):
        candidate = ticks[candidate_index]
        later = ticks[candidate_index + 1]
        elapsed = (later["_datetime"] - candidate["_datetime"]).total_seconds()
        window = (current["_datetime"] - candidate["_datetime"]).total_seconds()
        if (
            elapsed > gap_seconds
            or window > TRAILING_WINDOW_SECONDS
            or candidate[cap_field] != current[cap_field]
        ):
            break
        segment.append(candidate)
    segment.reverse()
    return segment


def measured_capacity(
    ticks: list[dict[str, Any]],
    index: int,
    total_field: str,
    cap_field: str,
    gap_seconds: float,
) -> dict[str, Any]:
    tick = ticks[index]
    total = tick[total_field]
    cap = tick[cap_field]
    headroom = max(0, cap - total)
    result: dict[str, Any] = {
        "total": total,
        "cap": cap,
        "headroom": headroom,
        "headroom_fraction": headroom / cap,
        "fill_fraction": min(1.0, total / cap),
        "trailing_rate": None,
        "trailing_window": {"seconds": None, "samples": 0},
        "cap_change": None,
    }

    if index:
        previous = ticks[index - 1]
        if previous[cap_field] != cap:
            result["cap_change"] = {
                "ts": tick["ts"],
                "previous": previous[cap_field],
                "new": cap,
            }

    segment = trailing_segment(ticks, index, cap_field, gap_seconds)
    if len(segment) >= 2:
        elapsed = (segment[-1]["_datetime"] - segment[0]["_datetime"]).total_seconds()
        result["trailing_rate"] = (
            segment[-1][total_field] - segment[0][total_field]
        ) / elapsed
        result["trailing_window"] = {"seconds": elapsed, "samples": len(segment)}
    return result


def derived_room_sampling(
    manifest: dict[str, Any] | None,
    coverage_by_frame: dict[tuple[Any, ...], dict[str, Any]],
) -> dict[str, Any] | None:
    if manifest is None:
        return None

    key = (
        manifest["selector_version"],
        manifest["seed"],
        manifest["epoch"],
        manifest["frame_id"],
        manifest["frame_size"],
    )
    state = coverage_by_frame.setdefault(
        key,
        {"ids": set(), "attempts": 0, "failures": 0},
    )
    sampled = manifest["sampled"]
    failed_this_tick = sum(not entry["success"] for entry in sampled)
    state["attempts"] += len(sampled)
    state["failures"] += failed_this_tick
    state["ids"].update(entry["id"] for entry in sampled)

    return {
        **manifest,
        "sampled_rooms": len(sampled),
        "cumulative_unique_rooms": len(state["ids"]),
        "repeat_count": state["attempts"] - len(state["ids"]),
        "failed_reads": state["failures"],
        "failed_reads_this_tick": failed_this_tick,
    }


def room_lifecycle_display(
    lifecycle: dict[str, Any] | None,
    sampling: dict[str, Any] | None,
) -> dict[str, Any]:
    if lifecycle is None:
        missing = {
            "value_text": "—",
            "context": (
                "not recorded · this tick predates the forward room-name ledger; "
                "absence never means zero"
            ),
        }
        return {
            "ledger": dict(missing),
            "revisited": dict(missing),
            "conversion": dict(missing),
            "failures": dict(missing),
            "budget": dict(missing),
            "senders": dict(missing),
            "coverage_text": (
                "Room lifecycle and room-lifecycle sampling evidence were not "
                "recorded for this tick. No value is inferred from later ledger state."
            ),
        }

    rooms = lifecycle["rooms_in_ledger"]
    revisited = lifecycle["rooms_revisited"]
    successful = lifecycle["rooms_successfully_revisited"]
    second = lifecycle["rooms_with_second_message"]
    attempts = lifecycle["reads_attempted"]
    failed = lifecycle["reads_failed"]
    budget = lifecycle["read_budget"]
    senders = lifecycle["second_sender_classes"]

    revisit_context = (
        f"{format_percent(revisited / rooms)} of {format_int(rooms)} rooms created "
        "since the ledger began have reached at least one scheduled revisit"
        if rooms
        else "No created-room denominator has entered the ledger yet"
    )
    conversion = (
        {
            "value_text": format_percent(second / successful),
            "context": (
                f"{format_int(second)} / {format_int(successful)} rooms with at least "
                "one successful scheduled revisit had reached sequence 2 or later"
            ),
        }
        if successful
        else {
            "value_text": "—",
            "context": (
                "No successful scheduled room-read denominator yet; failed reads "
                "are unknown outcomes, never absence of a second message"
            ),
        }
    )
    failure_display = (
        {
            "value_text": format_int(failed),
            "context": (
                f"{format_percent(failed / attempts)} of {format_int(attempts)} "
                "attempted scheduled reads failed · failures remain unknown outcomes"
            ),
        }
        if attempts
        else {
            "value_text": "—",
            "context": "No scheduled room reads have been attempted yet",
        }
    )
    sender_denominator = second
    sender_context = (
        (
            f"Of {format_int(sender_denominator)} rooms known to have a second "
            f"message: {format_int(senders['signed_did'])} signed did:key, "
            f"{format_int(senders['unsigned_did'])} unsigned did:key, "
            f"{format_int(senders['server'])} server, "
            f"{format_int(senders['other'])} other, and "
            f"{format_int(senders['not_observed'])} whose exact sequence-2 sender "
            "was no longer in the returned window"
        )
        if sender_denominator
        else "No room with a second message has been observed yet"
    )

    if sampling is None:
        sampling_evidence_text = (
            " Room-lifecycle sampling evidence was not recorded for this legacy "
            "tick; no selector, stage allocation or aged-out count is inferred."
        )
    else:
        selected_total = sum(sampling["selection"]["selected_by_stage"].values())
        sampling_evidence_text = (
            f" Sampling evidence: {format_int(selected_total)} / "
            f"{format_int(sampling['selection']['read_budget'])} lifecycle reads "
            f"selected this tick; {format_int(sampling['aged_out_unselected'])} "
            "eligible checks have aged out across the three stages since the ledger "
            "began."
        )
        finalization = sampling["aged_out_finalization"]
        if finalization is not None:
            sampling_evidence_text += (
                " This tick finalized "
                f"{format_int(finalization['finalized_this_tick'])} aged-out "
                "checks as terminal ledger records under its "
                f"{format_int(finalization['batch_limit'])}-check per-tick bound; "
                f"{format_int(finalization['backlog_remaining'])} aged-out checks "
                "remained unfinalized when the tick was written."
            )

    if sampling is not None:
        deferral_text = (
            f"{format_int(lifecycle['deferred_due_to_budget'])} active-eligible "
            f"checks deferred: "
            f"{format_int(lifecycle['deferred_due_to_read_budget'])} by the "
            f"recorded read budget and "
            f"{format_int(lifecycle['deferred_due_to_deadline'])} because the "
            "wall-clock deadline was reached"
        )
        deferred_superseded = lifecycle["deferred_superseded_due_to_batch_limit"]
        superseded_batch_text = (
            ""
            if deferred_superseded is None or deferred_superseded == 0
            else (
                f" {format_int(deferred_superseded)} superseded "
                f"{'cohort remains' if deferred_superseded == 1 else 'cohorts remain'} "
                f"pending behind the "
                f"{format_int(ROOM_REVISIT_STRUCTURAL_CEILING)}-record bounded "
                "local-finalization batch."
            )
        )
    elif lifecycle["deferred_due_to_deadline"] is None:
        deferral_text = (
            f"{format_int(lifecycle['deferred_due_to_budget'])} deferred by the "
            "recorded aggregate budget; the reason split was not recorded"
        )
        superseded_batch_text = ""
    elif lifecycle["deferred_superseded_due_to_batch_limit"] is None:
        deferral_text = (
            f"{format_int(lifecycle['deferred_due_to_budget'])} deferred: "
            f"{format_int(lifecycle['deferred_due_to_read_budget'])} by the "
            f"per-tick read cap and "
            f"{format_int(lifecycle['deferred_due_to_deadline'])} because the "
            "wall-clock deadline was reached"
        )
        superseded_batch_text = ""
    else:
        deferral_text = (
            f"{format_int(lifecycle['deferred_due_to_budget'])} deferred: "
            f"{format_int(lifecycle['deferred_due_to_read_budget'])} by the "
            "per-tick read cap, "
            f"{format_int(lifecycle['deferred_due_to_deadline'])} because the "
            "wall-clock deadline was reached, and "
            f"{format_int(lifecycle['deferred_superseded_due_to_batch_limit'])} "
            f"by the {format_int(ROOM_REVISIT_STRUCTURAL_CEILING)}-record bounded "
            "local-finalization batch"
        )
        superseded_batch_text = ""

    superseded = lifecycle["superseded_this_tick"]
    superseded_text = (
        ""
        if superseded is None
        else (
            f"{format_int(superseded)} older creation "
            f"{'cohort' if superseded == 1 else 'cohorts'} finalized as "
            "superseded without an origin read · "
        )
    )
    generation_evidence_text = (
        "only 16-hex SHA-256 prefixes leave the legacy collector"
        if superseded is None
        else (
            "published evidence carries a stable 16-hex SHA-256 room-name prefix "
            "plus its nonnegative creation-event sequence"
        )
    )

    actual_delays = [
        revisit["elapsed_since_creation_seconds"]
        for revisit in lifecycle["revisits"]
        if revisit["elapsed_since_creation_seconds"] is not None
    ]
    if actual_delays:
        delay_text = (
            f"actual creation-to-attempt delay this tick {format_int(min(actual_delays))}s"
            if len(actual_delays) == 1
            else (
                f"actual creation-to-attempt delays this tick "
                f"{format_int(min(actual_delays))}s to "
                f"{format_int(max(actual_delays))}s"
            )
        )
    elif lifecycle["attempted_this_tick"]:
        delay_text = (
            "actual creation-to-attempt delay was not recorded for this legacy tick"
        )
    else:
        delay_text = "no revisit was attempted this tick"

    if sampling is None:
        coverage_text = (
            f"{format_int(lifecycle['created_rooms_observed_this_tick'])} new ledger "
            f"entries this tick · {format_int(lifecycle['due_this_tick'])} scheduled "
            f"revisits due · {format_int(lifecycle['attempted_this_tick'])} origin "
            f"reads attempted · {superseded_text}"
            f"{deferral_text}. Read-cap and deadline deferrals were not attempted and "
            "are neither failed reads nor evidence that a room had no second message. "
            "A superseded batch deferral is bounded local bookkeeping and not an "
            "origin-read or deadline failure. Nominal due "
            "targets are 5 minutes, 1 hour and 24 hours after creation; they are "
            f"scheduling targets, not measured delays · {delay_text}. Coverage begins "
            "when this ledger begins and says nothing about older rooms."
        )
    else:
        coverage_text = (
            f"{format_int(lifecycle['created_rooms_observed_this_tick'])} new ledger "
            f"entries this tick · {format_int(lifecycle['due_this_tick'])} "
            "active-eligible scheduled revisits · "
            f"{format_int(lifecycle['attempted_this_tick'])} origin reads attempted · "
            f"{superseded_text}{deferral_text}.{superseded_batch_text} "
            "Read-budget and deadline deferrals were not attempted and are neither "
            "failed reads nor evidence that a room had no second message. Superseded "
            "cohorts and bounded local-finalization backlog are not active-eligible "
            "origin reads. Nominal due targets are 5 minutes, 1 hour and 24 hours "
            "after creation; they are scheduling targets, not measured delays · "
            f"{delay_text}. Coverage begins when this ledger begins and says nothing "
            "about older rooms."
        )

    coverage_text += sampling_evidence_text

    return {
        "ledger": {
            "value_text": format_int(rooms),
            "context": (
                f"exact created-room names retained privately in SQLite since "
                f"{lifecycle['ledger_started_at']} · {generation_evidence_text}"
                if lifecycle["ledger_started_at"] is not None
                else (
                    "exact created-room names retained privately in SQLite since "
                    "the ledger began; no start timestamp is recorded · "
                    f"{generation_evidence_text}"
                )
            ),
        },
        "revisited": {
            "value_text": format_int(revisited),
            "context": revisit_context,
        },
        "conversion": conversion,
        "failures": failure_display,
        "budget": {
            "value_text": f"{budget['reads_per_minute']:.2f}/min",
            "context": (
                (
                    f"{format_int(budget['total_reads'])} logical reads this tick · "
                    f"normalized to the enforced "
                    f"{format_int(budget['rate_window_seconds'])}s minimum scheduling "
                    f"window · {format_percent(budget['share'])} of the published "
                    f"{format_int(budget['published_reads_per_minute'])}/min budget · "
                    f"enforced ceiling {format_percent(budget['maximum_share'])} · "
                    f"revisit issue deadline "
                    f"{format_int(budget['tick_revisit_deadline_seconds'])}s from "
                    "tick start"
                )
                if budget["rate_window_seconds"] is not None
                else (
                    f"{format_int(budget['total_reads'])} logical reads this tick over "
                    f"the legacy assumed "
                    f"{format_int(budget['assumed_tick_seconds'])}s cadence · "
                    f"{format_percent(budget['share'])} of the published "
                    f"{format_int(budget['published_reads_per_minute'])}/min budget · "
                    f"enforced ceiling {format_percent(budget['maximum_share'])} · "
                    "wall-clock deadline not recorded"
                )
            ),
        },
        "senders": {
            "value_text": format_int(sender_denominator),
            "context": sender_context,
        },
        "coverage_text": coverage_text,
    }


def room_lifecycle_sampling_display(
    sampling: dict[str, Any] | None,
) -> dict[str, Any]:
    if sampling is None:
        missing = {
            "value_text": "Not recorded",
            "context": (
                "not recorded · this tick predates room-lifecycle sampling evidence; "
                "absence never means zero"
            ),
        }
        return {
            "selection": dict(missing),
            "aged_out": dict(missing),
            "stages": {
                str(stage): dict(missing) for stage in ROOM_REVISIT_STAGES_SECONDS
            },
        }

    selection = sampling["selection"]
    selected_by_stage = selection["selected_by_stage"]
    selected_total = sum(selected_by_stage.values())
    initial_by_stage = selection["initial_allocation_by_stage"]
    rank_text = json.dumps(selection["rank"], ensure_ascii=False, sort_keys=True)
    eligibility_text = json.dumps(
        selection["eligibility"],
        ensure_ascii=False,
        sort_keys=True,
    )
    rule_text = (
        f"deterministic rank {rank_text} · eligibility {eligibility_text} · "
        "age gates eligibility but does not order the draw · "
        f"selector seed {selection['selector_seed']} · tick timestamp "
        f"{selection['tick_timestamp']} · allocation rotation "
        f"{format_int(selection['allocation_rotation'])} · initial allocation "
        f"by stage: 5-minute {format_int(initial_by_stage['300'])}, "
        f"1-hour {format_int(initial_by_stage['3600'])}, "
        f"24-hour {format_int(initial_by_stage['86400'])} · short stage "
        f"{format_int(selection['short_stage_seconds'])}s"
    )
    stage_labels = {
        "300": "5-minute",
        "3600": "1-hour",
        "86400": "24-hour",
    }
    stages: dict[str, dict[str, str]] = {}
    for stage, coverage in sampling["coverage_by_stage"].items():
        coverage_fraction = coverage["coverage_fraction"]
        fraction = coverage["second_message_fraction"]
        if fraction["denominator"] == 0:
            fraction_text = "second-message fraction not recorded because the present-check denominator is 0"
        else:
            fraction_text = (
                f"{format_int(fraction['numerator'])} / "
                f"{format_int(fraction['denominator'])} recorded a second message "
                f"({format_percent(fraction['numerator'] / fraction['denominator'])})"
            )
        if coverage["attempted_late"] is None:
            # This tick predates the attempted-late split: its aged-out
            # count also contains checks read after their window closed,
            # and that historical meaning is preserved, not rewritten.
            late_text = "attempted late not recorded · "
            aged_out_text = (
                f"{format_int(coverage['aged_out_unselected'])} aged out "
                "without a timely attempt · "
            )
        else:
            late_text = f"{format_int(coverage['attempted_late'])} attempted late · "
            aged_out_text = f"{format_int(coverage['aged_out_unselected'])} aged out with no attempt · "
        stages[stage] = {
            "value_text": (
                f"{format_int(coverage_fraction['numerator'])} / "
                f"{format_int(coverage_fraction['denominator'])} completed"
            ),
            "context": (
                f"{stage_labels[stage]} stage, since the ledger began · "
                f"{format_int(coverage['deferred_checks'])} deferred · "
                f"{format_int(coverage['failed_checks'])} failed · "
                f"{late_text}"
                f"{aged_out_text}"
                f"{format_int(coverage['checked_and_quiet'])} checked and quiet · "
                f"{fraction_text}"
            ),
        }

    return {
        "selection": {
            "value_text": (
                f"{format_int(selected_total)} / "
                f"{format_int(selection['read_budget'])} reads selected"
            ),
            "context": (
                f"{rule_text} · selector version "
                f"{format_int(selection['selector_version'])} · selected by stage: "
                f"5-minute {format_int(selected_by_stage['300'])}, "
                f"1-hour {format_int(selected_by_stage['3600'])}, "
                f"24-hour {format_int(selected_by_stage['86400'])} · "
                f"{format_int(selection['redistributed_reads'])} redistributed reads"
            ),
        },
        "aged_out": {
            "value_text": format_int(sampling["aged_out_unselected"]),
            "context": (
                (
                    "eligible scheduled checks, since the ledger began, never "
                    "attempted before their window closed and terminal once it "
                    "did; a check read after its window closed is counted as "
                    "attempted late, not here · not a quiet check, failure, "
                    "or deferral"
                )
                if sampling["aged_out_finalization"] is not None
                else (
                    "eligible scheduled checks, since the ledger began, with no "
                    "timely in-window attempt; a check read after its window "
                    "closed is counted here and excluded from coverage · not a "
                    "quiet check, failure, or deferral"
                )
            ),
        },
        "stages": stages,
    }


def engagement_display(engagement: dict[str, Any] | None) -> dict[str, dict[str, str]]:
    """One source of truth for every engagement string the page shows.

    Both the server-rendered pass and the page JavaScript read these values
    verbatim, so the no-JS view and the scripted view cannot diverge. These
    are the service's own published figures over its own message window,
    republished unverified — never observatory measurements and never
    network-wide claims. A missing figure is "not recorded", never zero.
    """
    if engagement is None:
        absent = {
            "value_text": "—",
            "context": (
                "not recorded · this tick does not carry the service's "
                "engagement object, and absence never means zero"
            ),
        }
        return {
            "ratio": dict(absent),
            "zero_response": dict(absent),
            "nick_diversity": dict(absent),
        }

    window_parts = []
    if engagement["window_cap"] is not None:
        window_parts.append(
            f"window cap {format_int(engagement['window_cap'])} messages"
        )
    if engagement["windowed_messages"] is not None:
        window_parts.append(
            f"{format_int(engagement['windowed_messages'])} windowed messages"
        )
    window_text = (
        " · ".join(window_parts)
        if window_parts
        else "window figures not published this tick"
    )
    provenance = (
        f"the service's own figure over its declared message window ({window_text}) · "
        "republished unverified from the /rooms engagement object · "
        "describes that window, not the network"
    )

    def field_missing(field: str) -> dict[str, str]:
        return {
            "value_text": "—",
            "context": (
                f"not recorded · the service's engagement object did not carry a "
                f"well-formed {field} this tick, and absence never means zero"
            ),
        }

    ratio = engagement["windowed_note_to_message_ratio"]
    zero_share = engagement["zero_response_share"]
    nick = engagement["nick_diversity"]
    return {
        "ratio": (
            {
                "value_text": f"{ratio:,.2f}",
                "context": (
                    "durable published notes per observed chat message · "
                    f"service field windowed_note_to_message_ratio · {provenance}"
                ),
            }
            if ratio is not None
            else field_missing("windowed_note_to_message_ratio")
        ),
        "zero_response": (
            {
                "value_text": format_percent(zero_share),
                "context": (
                    "share of the service's windowed messages · "
                    f"service field zero_response_share · {provenance}"
                ),
            }
            if zero_share is not None
            else field_missing("zero_response_share")
        ),
        "nick_diversity": (
            {
                "value_text": f"{nick:.4f}",
                "context": (
                    "distinct-nick diversity of the service's windowed messages · "
                    f"service field nick_diversity · {provenance}"
                ),
            }
            if nick is not None
            else field_missing("nick_diversity")
        ),
    }


def funnel_display(funnel: dict[str, Any]) -> dict[str, Any]:
    """One source of truth for every funnel string the page shows.

    Both the server-rendered pass and the page JavaScript read these values
    verbatim, so the no-JS view and the scripted view cannot diverge.
    """
    census = funnel["well_formed_did_notes"]
    observed = funnel["dids_observed_signing"]
    two_ticks = funnel["seen_two_ticks"]
    two_dates = funnel["two_collection_utc_dates"]
    two_rooms = funnel["two_rooms"]
    sustained = funnel["sustained_reciprocal_footprint"]
    date_count = funnel["persistence_collection_utc_dates_count"]
    tracking_disclosure = funnel["tracking_disclosure"]
    cap_gates = not (
        funnel["signer_state_version"] is not None
        and funnel["signer_state_version"] >= 3
    ) and not (
        funnel["signer_state_version"] is None and tracking_disclosure is not None
    )

    # Bars are proportions of the observed-signing cohort, the population the
    # later stages actually filter. The census is a separate population that
    # neither contains nor is contained by the observed cohort, so it is never
    # a bar denominator: drawn against the census width, every bar visually
    # asserted "N of the census are real", a containment claim the data does
    # not support.
    bar_denominator = max(1, observed) if isinstance(observed, int) else 1

    def width(value: Any) -> float:
        # A measured zero draws nothing. The old 1% visibility floor gave an
        # exact zero the same bar as a small positive count, which read as
        # "almost none" where the measurement says "none".
        if isinstance(value, bool) or not isinstance(value, int):
            return 0.0
        return value / bar_denominator * 100

    census_measurement = census_display(
        (
            census,
            funnel["census_started_at"],
            funnel["census_completed_at"],
        )
        if census is not None
        else None
    )
    observed_context = (
        f"{format_int(observed)} distinct did:key signers observed in sampled rooms · "
    )
    if funnel["cap_hit"] and cap_gates:
        # Adjacent disclosure: while the tracking cap gates insertion the
        # figure is the cap, not a live count, and must not imply it is rising.
        observed_context += (
            f"pinned at the {format_int(funnel['tracked_cap'])}-DID tracking-state cap — "
            "admissions stopped when the cap was reached, so signers observed since "
            "are not counted · "
        )
    observed_context += (
        "a lower bound on signers, since sampling covers a fraction of rooms · "
        "a separate population from the census: neither contains the other"
    )
    cohort_suffix = (
        " (pinned at the tracking cap)" if funnel["cap_hit"] and cap_gates else ""
    )
    two_ticks_context = (
        f"{format_percent(two_ticks / observed)} of {format_int(observed)} "
        f"in the captured observed-signing cohort{cohort_suffix}"
        if observed > 0
        else "Captured cohort denominator unavailable"
    )
    if funnel["legacy_persistence_reset"]:
        two_dates_context = (
            "not recorded · this tick predates collection-UTC-date persistence "
            "measurement; legacy message-derived dates are not reinterpreted"
        )
    elif two_dates == 0 and date_count < 2:
        two_dates_context = (
            "0 qualify · collection has observed 1 distinct collection UTC date; "
            "collection has not yet crossed a UTC day boundary, so no key can yet qualify"
            if date_count == 1
            else (
                "0 qualify · collection has observed 0 distinct collection UTC dates, "
                "so no key can yet qualify"
            )
        )
    elif two_ticks > 0:
        two_dates_context = (
            f"{format_percent(two_dates / two_ticks)} of {format_int(two_ticks)} "
            "observed on at least two distinct collection UTC dates"
        )
    else:
        two_dates_context = "Collection-date observation unavailable"
    if funnel["legacy_reciprocity"]:
        sustained_context = (
            "not recorded · this tick predates signed A → B → A alternation "
            "measurement; legacy co-occurrence is not reinterpreted"
        )
    elif two_rooms > 0:
        sustained_context = (
            f"{format_percent(sustained / two_rooms)} of {format_int(two_rooms)} "
            "observed in ≥2 sampled rooms also observed in a signed "
            "A → B → A sequence"
        )
    else:
        sustained_context = "Two-room-qualified cohort denominator unavailable"

    warning = (
        "The sampling frame is the newest rooms, so this funnel is biased "
        "toward new and short-lived rooms."
    )
    if funnel["legacy_persistence_reset"] or funnel["persistence_reset_at"]:
        warning += (
            " Earlier message-derived date values were discarded rather than "
            "reinterpreted because the persistence chain could not be recovered honestly."
        )
        if funnel["persistence_reset_at"]:
            warning += f" Persistence restarted at {funnel['persistence_reset_at']}."
    if funnel["cap_hit"] and cap_gates:
        warning += (
            f" The {format_int(funnel['tracked_cap'])}-DID state cap has been "
            "reached; new observed DIDs are no longer added."
        )
    if tracking_disclosure is not None:
        warning += f" {tracking_disclosure['warning']}"

    coverage = funnel["coverage"]
    if coverage["frame_size"] is not None:
        frame_size = coverage["frame_size"]
        read_budget_text = (
            "The room-read budget for this tick was not recorded; no value is inferred. "
            if coverage["read_budget"] is None
            else (
                f"The recorded room-read budget for this tick is "
                f"{format_int(coverage['read_budget'])}. "
            )
        )
        coverage_text = (
            f"The current selector frame contains {format_int(frame_size)} rooms: "
            f"{format_int(max(0, frame_size - 1))} newest-listing rooms plus lobby. "
            f"{read_budget_text}"
            f"{format_int(coverage['cumulative_unique_rooms'])} / "
            f"{format_int(frame_size)} distinct room hashes selected; "
            f"{format_int(coverage['selected_rooms'])} selected this tick; "
            f"{format_int(coverage['failed_reads'])} cumulative failed reads and "
            f"{format_int(coverage['repeat_count'])} repeated selections in this frame epoch. "
            f"Selector version {format_int(coverage['selector_version'])}. "
            f"{format_int(coverage['rooms_total'])} rooms exist, but that total is not "
            "the sampling denominator. Room identifiers are hashes of "
            "attacker-controlled names."
        )
    else:
        coverage_text = (
            "Legacy tick: room reads were counted, but no hashed sampling manifest was "
            "recorded, so cumulative frame coverage is unavailable."
        )

    stages = [
        ("observed", observed, observed_context),
        ("two_ticks", two_ticks, two_ticks_context),
        (
            "two_dates",
            None if funnel["legacy_persistence_reset"] else two_dates,
            two_dates_context,
        ),
        ("sustained", sustained, sustained_context),
    ]
    return {
        "census": census_measurement,
        "stages": [
            {
                "key": key,
                "value": value,
                "value_text": format_int(value),
                "context": context,
                "width_percent": width(value),
            }
            for key, value, context in stages
        ],
        "warning": warning,
        "coverage_text": coverage_text,
        "tracked_text": (
            (
                f"{format_int(funnel['tracked_dids'])} tracked DIDs · retired JSON-store "
                f"cap {format_int(funnel['tracked_cap'])} (no longer gates insertion)"
            )
            if not cap_gates
            else (
                f"{format_int(funnel['tracked_dids'])} / "
                f"{format_int(funnel['tracked_cap'])} tracked DIDs "
                f"({format_percent(funnel['tracked_dids'] / funnel['tracked_cap'])} "
                "of the state cap used)"
            )
        ),
    }


def derived_identity_census_run(
    run: dict[str, Any] | None,
    runs_by_walk: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    if run is None:
        return None

    aggregate = runs_by_walk.setdefault(
        run["walk_started_at"],
        {
            "invocations": 0,
            "shard_reads_attempted": 0,
            "shard_read_failures": 0,
            "failure_causes": {},
        },
    )
    aggregate["invocations"] += 1
    aggregate["shard_reads_attempted"] += run["shard_reads_attempted"]
    aggregate["shard_read_failures"] += run["shard_read_failures"]
    for cause, count in run["failure_causes"].items():
        aggregate["failure_causes"][cause] = (
            aggregate["failure_causes"].get(cause, 0) + count
        )

    return {
        **run,
        "invocations": aggregate["invocations"],
        "shard_reads_attempted": aggregate["shard_reads_attempted"],
        "shard_read_failures": aggregate["shard_read_failures"],
        "failure_causes": dict(sorted(aggregate["failure_causes"].items())),
    }


def identity_census_run_display(run: dict[str, Any] | None) -> str:
    if run is None:
        return CENSUS_RUN_NOT_RECORDED

    stop_text = {
        "complete": "all shards collected",
        "deadline": "collector deadline reached",
        "maximum_passes": "collector maximum-pass bound reached",
    }[run["stop_reason"]]
    cause_labels = {
        "deadline": "collector deadline expirations",
        "transport_or_decode": "transport or decode errors while reading the service",
        "invalid_shard_response": "invalid shard responses",
    }
    causes = []
    for cause, count in run["failure_causes"].items():
        label = (
            f"service HTTP {cause.removeprefix('http_')} responses"
            if cause.startswith("http_")
            else cause_labels[cause]
        )
        causes.append(f"{format_int(count)} {label}")
    failure_text = (
        "no shard-read failures recorded"
        if not causes
        else (
            f"{format_int(run['shard_read_failures'])} shard-read failures ({'; '.join(causes)})"
        )
    )
    invocation_label = "invocation" if run["invocations"] == 1 else "invocations"
    return (
        f"census sweep {run['walk_started_at']} · "
        f"{format_int(run['shards_collected'])} / "
        f"{format_int(CENSUS_SHARD_COUNT)} shards collected · "
        f"{format_int(run['shards_outstanding'])} outstanding · "
        f"stop reason {run['stop_reason']}: {stop_text} · "
        f"{format_int(run['shard_reads_attempted'])} shard reads attempted across "
        f"{format_int(run['invocations'])} {invocation_label} · {failure_text} · "
        f"latest invocation began with "
        f"{format_int(run['shards_outstanding_at_start'])} outstanding and attempted "
        f"{format_int(run['passes_attempted'])} / "
        f"{format_int(run['maximum_passes'])} passes under a "
        f"{format_int(run['deadline_seconds'])}s collector deadline"
    )


def census_display(
    latest_census: tuple[int, str | None, str] | None,
) -> dict[str, Any]:
    if latest_census is None:
        return {
            "value": None,
            "value_text": "—",
            "context": "No completed census yet",
        }

    total, started_at, completed_at = latest_census
    if started_at is None:
        context = (
            f"census walk start not recorded · completed {completed_at} · "
            "the assembly window is unknown, not a point-in-time count; "
            "no start is inferred"
        )
    else:
        duration = (parse_ts(completed_at) - parse_ts(started_at)).total_seconds()
        context = (
            f"census walk {started_at} → {completed_at} · assembled while "
            "the namespace changed; not a point-in-time count"
        )
        if duration >= CENSUS_LONG_WALK_SECONDS:
            context += f" · LONG CENSUS WALK ({stall_duration_text(duration)})"

    return {
        "value": total,
        "value_text": format_int(total),
        "context": context,
    }


def display_funnel(
    funnel: dict[str, Any] | None,
    newest_listing_rooms: int,
    rooms_total: int,
    room_sampling: dict[str, Any] | None,
    latest_census: tuple[int, str | None, str] | None = None,
) -> dict[str, Any] | None:
    if funnel is None:
        return None
    result = dict(funnel)
    coverage = {
        "sampled_rooms": funnel["coverage"]["sampled_rooms"],
        "selected_rooms": None,
        "newest_listing_rooms": newest_listing_rooms,
        "rooms_total": rooms_total,
        "frame_size": None,
        "read_budget": None,
        "cumulative_unique_rooms": None,
        "repeat_count": None,
        "failed_reads": None,
        "failed_reads_this_tick": None,
        "selector_version": None,
        "epoch": None,
        "frame_id": None,
    }
    if room_sampling is not None:
        for key in (
            "frame_size",
            "read_budget",
            "cumulative_unique_rooms",
            "repeat_count",
            "failed_reads",
            "failed_reads_this_tick",
            "selector_version",
            "epoch",
            "frame_id",
        ):
            coverage[key] = room_sampling[key]
        # The manifest counts rooms selected for a read; the funnel's own
        # sampled_rooms counts the reads that succeeded. They are different
        # numbers and neither may stand in for the other.
        coverage["selected_rooms"] = room_sampling["sampled_rooms"]
    result["coverage"] = coverage

    # Honesty invariant: never present a census under a "latest" promise when
    # a newer completed census exists in the same payload. The unlocked
    # read-modify-write in the collector let the signer state carry a census
    # hours staler than a measured identity_total in the very same ledger.
    superseded = False
    if latest_census is not None:
        latest_total, latest_started_at, latest_completed_at = latest_census
        current_completed_at = result["census_completed_at"]
        replace_census = (
            current_completed_at is None
            or parse_ts(latest_completed_at) > parse_ts(current_completed_at)
            or (
                latest_completed_at == current_completed_at
                and result["census_started_at"] is None
                and latest_started_at is not None
            )
        )
        if replace_census:
            superseded = (
                result["well_formed_did_notes"] is not None
                and current_completed_at != latest_completed_at
            )
            result["well_formed_did_notes"] = latest_total
            result["census_started_at"] = latest_started_at
            result["census_completed_at"] = latest_completed_at
    result["census_superseded"] = superseded

    result["display"] = funnel_display(result)
    return result


def methodology_definitions() -> dict[str, str]:
    return {
        "history": (
            "Forward-collected only. No observation exists before collection_started; "
            "missing intervals are recorded as gaps and are never interpolated. The "
            "embedded page and /data.json retain collector-tick raw points for the "
            "newest 24 hours. Older points are numeric-only UTC-aligned rollups: one "
            "hour through day 30, one day through day 365, then one lifetime archive "
            "aggregate. Rollup ratios are recomputed from summed numerators and "
            "denominators, never from averaged percentages or rates. Empty buckets are "
            "null, partial buckets publish expected and missing counts, and chart lines "
            "do not cross a bucket containing a recorded collector cadence gap. "
            "Expected tick counts use "
            "the median gap-free accepted interval in the complete input. The scrubber "
            "addresses only the 24-hour raw window; older history is a derived chart "
            "series, not raw observations."
        ),
        "ledger_integrity": (
            "ticks.jsonl is explicitly unchained before the first tick whose "
            "ledger_chain.previous_sha256 is null. That tick is the declared genesis. "
            "From genesis onward, previous_sha256 is SHA-256 of the preceding tick's "
            "complete canonical bytes, and tick_sha256 is SHA-256 of the current "
            "tick's canonical bytes with only its own tick_sha256 member omitted. "
            "Canonical bytes are UTF-8 JSON produced by Python 3.12 json.dumps with "
            "ensure_ascii=False, allow_nan=False, separators=(',', ':') and "
            "sort_keys=True, encoded without a trailing newline. The chain and "
            "per-tick hashes establish internal consistency and make changed history "
            "detectable to anyone holding an earlier copy. They do not prove when a "
            "tick was collected: without an external anchor, the operator can rewrite "
            "a suffix and recompute its hashes. The unchained prefix remains "
            "unverifiable and rests on operator trust. verify_ledger.py reports the "
            "first break."
        ),
        "room_growth": (
            "Numerator: change in /r/events latest sequence. Denominator: elapsed UTC "
            "seconds between two consecutive accepted, gap-free ticks. A decrease in "
            "any of the four counted totals suppresses all four interval rates for "
            "that interval, and the last computed rate is carried forward with the "
            "time it was measured. Endpoint: "
            "/r/events?format=json&limit=200. Deduplication key: event sequence. This is "
            "a forward sample and lower bound when the 200-event window is incomplete; "
            "missing is null, never zero."
        ),
        "lobby_velocity": (
            "Numerator: change in /r/lobby latest sequence. Denominator: elapsed UTC "
            "seconds between two consecutive accepted, gap-free ticks. A decrease in "
            "any of the four counted totals suppresses all four interval rates for "
            "that interval, and the last computed rate is carried forward with the "
            "time it was measured. Endpoint: "
            "/r/lobby?format=json&limit=200. Deduplication key: message sequence. This "
            "is a forward sample; missing is null, never zero."
        ),
        "identity_census": (
            "Numerator: well-formed published DID-note keys assembled by walking all "
            "256 did-00 through did-ff shards. Denominator: none for the count; "
            "census-to-census rates divide count change by elapsed UTC seconds. "
            "Endpoint: /kv/did-XX. Deduplication key: published key. The published "
            "start and completion timestamps are the census-walk window: shards are "
            "read at different times while the namespace changes underneath, so the "
            "result is a walk, not a point-in-time count. Walks lasting at least one "
            "hour are labelled LONG CENSUS WALK. A legacy census without a recorded "
            "start says so explicitly and no start is inferred. A count publishes "
            "only when all shards succeed; incomplete censuses publish no count. "
            "Census-run invocations are grouped by walk_started_at. Only shard-read "
            "attempts, shard-read failures and their corresponding failure-cause "
            "counts are summed across invocations; collected and outstanding shard "
            "counts remain the latest point-in-time state and are never added. HTTP "
            "causes describe service responses, while deadline causes and stop reasons "
            "describe collector bounds. A tick without identity_census_run reports "
            "the sweep cost as not recorded, never zero. It counts neither people, "
            "agents nor users."
        ),
        "first_message_only": (
            "Numerator: captured new-room events whose same-tick newest-room record has "
            "sequence <= 1. Denominator: captured new-room events matched by exact room "
            "name to that listing. Window: one collector interval. Endpoints: "
            "/r/events?format=json&limit=200 and /rooms?format=json&limit=200. "
            "Deduplication key: event sequence, then exact room name for the join. "
            "It is a newest-listing "
            "sample and a live lower-bound signal, not a final outcome; unmatched and "
            "missing rooms are excluded, never counted as zero."
        ),
        "capacity": (
            "Numerator and denominator for utilisation: current total and current "
            "operator cap from /rooms?format=json&limit=200; headroom is "
            "max(0, cap - total), and fill fraction is min(1, total / cap). "
            "Net-change rate is "
            "(last total - first total) / elapsed UTC seconds over at most 24 hours of "
            "consecutive samples within the polling threshold under one unchanged cap; "
            "a counter decrease does not end the window. Its sample count and window "
            "are published. These are point observations and a trailing measurement, "
            "not a forecast. A cap change starts a new rate window."
        ),
        "room_sampling": (
            "Frame: the deduplicated newest-room listing plus lobby. The per-tick "
            "room-read budget is recorded in each new sampling manifest; when absent on "
            "legacy ticks it is reported as not recorded and no value is inferred. A "
            "seeded permutation is walked without replacement within each epoch. "
            "Published room identifiers are the first 16 hexadecimal "
            "characters of SHA-256(room name), so attacker-controlled names are not "
            "republished. Cumulative unique rooms, repeats and failed reads are counted "
            "only within the exact selector version, seed, epoch, frame identifier and "
            "frame-size denominator. Endpoint: /rooms?format=json&limit=200 then "
            "/r/<name>?format=json&limit=200. Deduplication key: hashed normalized room "
            "name. This is a sample; a failed read remains a recorded failure and "
            "contributes no message body."
        ),
        "signer_funnel": (
            "Census count: well-formed published DID notes, shown from the latest "
            "completed census in this payload. Observed-signing count: distinct "
            "did:key senders whose messages carry a signature nonce, in successful "
            "sampled room windows; since collector 2.2.0 a sender without a nonce is "
            "never counted as a signer, while earlier collector versions checked only "
            "the did:key shape of the sender. The census and the observed-signing "
            "count are separate measurements of populations that do not contain each "
            "other: publishing a DID note is not required in order to sign, and a "
            "published note need not be observed signing. The observed count is a "
            "lower bound on signers because sampling covers a fraction of rooms. "
            "Later stages filter the captured observed-signing cohort in order: at least "
            "two collector ticks, observed on at least two distinct UTC dates determined "
            "from collector tick timestamps, observed in at least two sampled rooms, "
            "then observed in at least one signed A → B → A sequence between two "
            "distinct DIDs in the same sampled room, contained within 10 message "
            "positions and 900 seconds. Mere adjacency or co-occurrence does not "
            "qualify. Pre-field ticks retain their historical co-occurrence value only "
            "for input validation; the reciprocal-alternation stage is published as "
            "not recorded and that value is never reinterpreted. Deduplication key: "
            "shortened did:key value. "
            "Window: persistence_started_at through the displayed tick. Legacy "
            "message-timestamp date fields are not reinterpreted as collection dates; "
            "legacy persistence and all downstream stages are reset to zero in the "
            "validated record and published as not recorded. Endpoints: "
            "successful sampled /r/<name>?format=json&limit=200 reads and /kv/did-XX "
            "census shards. Missing room reads add no signers and never mean inactivity. "
            "The newest-room sampling frame biases the result toward new and short-lived "
            "rooms. Funnel bars are drawn as proportions of the observed-signing cohort; "
            "the census count is published beside the funnel and is never a bar "
            "denominator, because the two populations do not contain each other. "
            "Signer-state versions 3 and later store observed DIDs in SQLite without "
            "an insertion cap; version 4 resets the old co-occurrence flags and "
            "rebuilds them only from signed reciprocal alternation, while version 5 "
            "adds the separate room-lifecycle ledger. tracked_cap and cap_hit "
            "retain the retired JSON-store metadata and do not gate insertion. New "
            "ticks declare the signer-state version explicitly. "
            "For unrecoverable pre-field ticks, an existing historical cap-release "
            "disclosure retains the same uncapped interpretation; other missing-version "
            "ticks remain subject to the recorded cap invariant."
        ),
        "room_lifecycle": (
            "Frame: exact room names observed in server-authored created events from "
            "/r/events?format=json&limit=200, beginning only when the SQLite ledger "
            "starts. Rooms older than that boundary are outside the measurement and "
            "nothing is inferred about them. Each creation cohort is keyed by its "
            "nonnegative creation-event sequence; the published 16-hex room-name hash "
            "remains stable across same-name recreations, while created_seq "
            "distinguishes those cohorts. SQL values are parameter-bound. Each newly "
            "observed room is scheduled for one read at approximately 5 minutes, "
            "1 hour and 24 hours after its creation timestamp. Endpoint: "
            "/r/<exact-name>?format=json&limit=200. A successful read records whether "
            "the returned sequence reached 2 or later. The exact sequence-2 sender is "
            "classified as signed did:key, unsigned did:key, server or other; if "
            "sequence 2 has rotated out of the returned window, the room still counts "
            "as having a second message but its exact second sender is reported as not "
            "observed. A failed read is recorded as a failure with null activity "
            "fields and never becomes evidence of no response. When a newer cohort "
            "with the same exact name exists before an older cohort's scheduled check, "
            "the older cohort is finalized as superseded_before_check with no origin "
            "read, null activity fields, and no contribution to attempted reads, failed "
            "reads, presence, absence, or revisit denominators. Published denominators "
            "include rooms in the ledger, distinct rooms revisited, distinct rooms "
            "successfully revisited and failed reads. Raw room names remain only in "
            "SQLite; tick manifests publish the first 16 hexadecimal characters of "
            "SHA-256(room name). The three stages are nominal scheduling targets, not "
            "claims about actual revisit age. New-format attempted revisits publish "
            "actual elapsed seconds from creation to the read attempt; legacy ticks "
            "without that field report it as not recorded and no delay is inferred. "
            "Ticks carrying room_lifecycle_sampling publish the lifecycle scheduler's "
            "selection and per-stage coverage evidence. For each nominal stage, "
            "scheduled_due_rooms is exactly ineligible_superseded_before_due plus "
            "eligible_rooms; eligible_rooms is exactly attempted_checks plus "
            "attempted_late plus deferred_checks plus aged_out_unselected plus "
            "superseded_after_eligibility; and attempted_checks is exactly "
            "completed_checks plus failed_checks. From collector 2.13.0 a check "
            "whose origin read happened only after its eligibility window closed "
            "is counted as attempted_late — a real read, outside the timely "
            "coverage and second-message denominators — and aged_out_unselected "
            "counts only checks that were never attempted at all; ticks recorded "
            "before 2.13.0 keep their historical meaning, in which "
            "aged_out_unselected also contained late attempts, and publish "
            "attempted_late as not recorded. From the same version the collector "
            "finalizes each aged-out check as a terminal record, in a bounded "
            "per-tick batch, publishing the count finalized this tick and the "
            "backlog still unfinalized; finalization writes no attempt evidence "
            "and never reclassifies a check, and a check finalized as aged out "
            "stays aged out even if supersession evidence arrives later. "
            "The top-level aged_out_unselected "
            "is the sum across stages. Per-stage coverage is printed as completed "
            "checks / eligible rooms, never as a bare percentage. Every coverage_by_stage "
            "figure, and the aged-out total drawn from it, counts every scheduled "
            "check whose window has opened since the ledger began; the selection, "
            "read-budget, due-count and finalization figures beside them describe "
            "only the tick "
            "being reported. A running total and a single tick are never summed or "
            "compared. Deferred checks, "
            "failed checks, late-attempted checks, aged-out checks and present "
            "checks that were "
            "checked and quiet remain separate published states. A second-message "
            "fraction is printed only with its numerator and present-check "
            "denominator; a zero denominator is reported as not recorded, never as "
            "0%. Selection evidence publishes the rank and eligibility descriptors, "
            "selector seed, tick timestamp, allocation rotation, initial per-stage "
            "allocation and short-stage duration that define the deterministic "
            "selection. The deriver validates the tick timestamp, seed shape, rotating "
            "initial allocation and redistribution accounting, then prints the "
            "collector's descriptors without fabricating a rule field. For these "
            "ticks, room_lifecycle.due_this_tick counts only active eligibility: "
            "attempted "
            "checks plus read-budget and deadline deferrals. "
            "deferred_due_to_budget is exactly deferred_due_to_read_budget plus "
            "deferred_due_to_deadline. Superseded cohorts and aged-out unselected "
            "checks are outside that active-eligibility count. Earlier ticks retain "
            "their historical accounting contract and publish lifecycle-sampling "
            "fields as not recorded. Revisit reads remain bounded by the recorded "
            "per-tick read budget and may be issued only before the recorded deadline; "
            "the rate is normalized to the recorded scheduling window rather than "
            "tick duration. Superseded cohorts held behind the 305-record bounded "
            "local-finalization batch remain separate local bookkeeping, not a read "
            "deferral, failed read or claim of absent room activity."
        ),
        "service_engagement": (
            "The service publishes an engagement object on /rooms — nick_diversity, "
            "windowed_note_to_message_ratio and zero_response_share — computed by the "
            "service over its own message window, whose declared figures (window_cap "
            "and windowed_messages) are republished beside every value. Endpoint: "
            "/rooms?format=json&limit=200. These are the service's own figures, "
            "republished unverified under the service's own field names: the observatory cannot "
            "recompute them, and they describe the service's declared window, never "
            "the whole network. A tick without a well-formed engagement object or "
            "field publishes 'not recorded', never zero."
        ),
        "room_composition": (
            "Numerator: captured new events in each stated room-name class. Denominator: "
            "captured new events in that collector interval. Endpoint: "
            "/r/events?format=json&limit=200. Deduplication key: event sequence. This is "
            "a capped forward sample and a lower bound when captured is below expected; "
            "incomplete windows are marked, not filled."
        ),
        "name_entropy": (
            "Numerator: captured new events whose base room name is bare 16-hex, "
            "UUID-like, or a 12+-character unseparated alphanumeric token containing "
            "letters and digits. Denominator: captured new events in the interval. "
            "Endpoint and deduplication key: /r/events?format=json&limit=200 and event "
            "sequence. It is a naming sample, not evidence of identity or intent; "
            "missing events are excluded."
        ),
        "data_download": (
            "/data.json is a bounded derived series. It carries collector-tick raw "
            "observations only for the declared newest 24-hour retention window and "
            "numeric UTC-aligned rollups for older history. It is not the unreduced "
            "tick ledger. The complete local input remains ticks.jsonl; whether that "
            "file is publicly served is a deployment property, not asserted here. "
            "The series documents accepted measurements, gap completeness, hashed "
            "sampling manifests and derived values, not raw message content. The "
            "source rotates message bodies away, so bodies available now could not "
            "reproduce earlier metrics."
        ),
    }


RATE_NOUNS = {
    "public_rooms_per_second": "the public-room event counter",
    "lobby_messages_per_second": "the lobby message counter",
    "rooms_total_per_second": "the room total",
    "notes_per_second": "the note total",
}


def interval_reason_text(negative: list[str], elapsed: float) -> str:
    """Plain-language reason an interval carries no rate. One shared source
    for the tile captions in both the server-rendered and scripted views."""
    if negative:
        nouns = " and ".join(RATE_NOUNS[name] for name in negative)
        text = f"{nouns} fell between ticks"
        if "rooms_total_per_second" in negative:
            text += " (idle rooms are reaped faster than new rooms appear)"
        return f"{text}, so the interval is recorded as a gap and no rate is computed across it"
    return (
        f"{round(elapsed)}s elapsed between ticks, beyond the gap threshold, "
        "so the interval is recorded as a gap and no rate is computed across it"
    )


def rate_tile_display(
    rate_value: dict[str, Any],
    carried: dict[str, Any] | None,
    reason: str | None,
) -> dict[str, str]:
    if rate_value["samples"]:
        return {
            "value_text": format_rate(rate_value["value"]),
            "context": (
                f"{format_int(rate_value['samples'])} samples over {round(rate_value['seconds'])}s"
            ),
        }
    if carried is not None:
        context = (
            f"last computed rate · {format_int(carried['samples'])} samples over "
            f"{round(carried['seconds'])}s · measured {carried['ts']}"
        )
        if reason:
            context += f" · {reason}"
        return {"value_text": format_rate(carried["value"]), "context": context}
    if reason:
        return {"value_text": "—", "context": f"no rate computed yet · {reason}"}
    return {"value_text": "—", "context": "Needs two gap-free observations"}


def stall_duration_text(seconds: float) -> str:
    minutes = int(seconds // 60)
    if minutes < 60:
        return f"{minutes}m"
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}h {minutes}m"
    days, hours = divmod(hours, 24)
    return f"{days}d {hours}h"


def capacity_metric_display(metric: Any) -> dict[str, Any]:
    """Every string the capacity ledger row prints.

    The server rendered the trailing window as raw seconds while the browser
    rendered the same window as minutes, hours or days, so the two views
    disagreed about one measurement. Both now read these strings.
    """
    if not isinstance(metric, dict):
        return {
            "count_text": "—",
            "headroom_text": "No capacity observation recorded",
            "rate_text": (
                "Observed net-change rate unavailable because no capacity observation was recorded"
            ),
            "cap_change_text": "",
            "fill_percent": 0.0,
        }

    fraction = metric.get("fill_fraction")
    fill_percent = (
        max(0.0, min(1.0, float(fraction))) * 100
        if isinstance(fraction, (int, float))
        and not isinstance(fraction, bool)
        and math.isfinite(fraction)
        else 0.0
    )
    cap_change = metric.get("cap_change")
    return {
        "count_text": (
            f"{format_int(metric.get('total'))} / {format_int(metric.get('cap'))}"
        ),
        "headroom_text": (
            f"{format_int(metric.get('headroom'))} headroom "
            f"({format_percent(metric.get('headroom_fraction'))} of "
            f"{format_int(metric.get('cap'))})"
        ),
        "rate_text": capacity_rate_context(metric),
        "cap_change_text": (
            f"Cap change · {format_int(cap_change['previous'])} → "
            f"{format_int(cap_change['new'])} at {cap_change['ts']} · "
            "a new observed-rate window began"
            if isinstance(cap_change, dict)
            else ""
        ),
        "fill_percent": fill_percent,
    }


def stillborn_display(signal: Any) -> dict[str, str]:
    """First-message-only figure and its denominator. The server wrote `x/y`
    and the browser wrote `x / y` for the same ratio."""
    matched = signal.get("matched_new_rooms") if isinstance(signal, dict) else None
    if not matched:
        return {"value_text": "—", "context": "No matched new rooms"}
    return {
        "value_text": format_percent(signal.get("fraction")),
        "context": (
            f"{format_int(signal.get('first_message_only'))} / "
            f"{format_int(matched)} matched new rooms"
        ),
    }


def sampling_display(coverage: Any) -> dict[str, str]:
    """The sampling register, from the funnel's merged coverage where one
    exists. The server said "Unavailable for this legacy tick" where the
    browser said the collector version that started recording manifests."""
    frame_size = coverage.get("frame_size") if isinstance(coverage, dict) else None
    if isinstance(frame_size, bool) or not isinstance(frame_size, int):
        values = dict.fromkeys(
            ("frame", "sampled", "unique", "repeats", "failures", "selector"),
            SAMPLING_NOT_RECORDED,
        )
    else:
        # The merged funnel coverage names the manifest count selected_rooms and
        # keeps sampled_rooms for successful reads; a bare manifest carries the
        # same selection count under its own sampled_rooms.
        selected = coverage.get("selected_rooms", coverage.get("sampled_rooms"))
        values = {
            "frame": f"{format_int(frame_size)} rooms in the recorded frame",
            "sampled": f"{format_int(selected)} selected this tick",
            "unique": (
                f"{format_int(coverage.get('cumulative_unique_rooms'))} / "
                f"{format_int(frame_size)} unique room hashes in this frame epoch"
            ),
            "repeats": (
                f"{format_int(coverage.get('repeat_count'))} repeated selections "
                "in this frame epoch"
            ),
            "failures": (
                f"{format_int(coverage.get('failed_reads'))} cumulative failures "
                f"in this frame epoch · "
                f"{format_int(coverage.get('failed_reads_this_tick'))} / "
                f"{format_int(selected)} selected reads "
                "failed this tick"
            ),
            "selector": (
                f"version {format_int(coverage.get('selector_version'))} · "
                f"epoch {format_int(coverage.get('epoch'))}"
            ),
        }

    newest = (
        coverage.get("newest_listing_rooms") if isinstance(coverage, dict) else None
    )
    total = coverage.get("rooms_total") if isinstance(coverage, dict) else None
    values["newest"] = (
        f"{format_int(newest)} returned in the newest listing"
        if isinstance(newest, int) and not isinstance(newest, bool)
        else "Not recorded"
    )
    values["total"] = (
        f"{format_int(total)} exist · not reachable for sampling"
        if isinstance(total, int) and not isinstance(total, bool)
        else "Not recorded"
    )
    return values


def series_snapshot_value(
    point: dict[str, Any],
    key: str,
    baseline: dict[str, Any],
) -> int | None:
    # A counter below the chart baseline means the service's sequence reset,
    # so the cumulative since collection began is unmeasurable, not zero.
    if key == "lobby":
        delta = point["lobby_last_seq"] - baseline["lobby_last_seq"]
        return delta if delta >= 0 else None
    if key == "notes":
        delta = point["notes_total"] - baseline["notes_total"]
        return delta if delta >= 0 else None
    return point["observed_public_rooms"]


def attach_series_display(
    raw_points: list[dict[str, Any]],
    baseline: dict[str, Any],
) -> None:
    """Every string the growth chart prints for a selected raw observation.

    The scrubber addresses these points only, so the selected timestamp is a
    separate published value from the newest-tick status timestamp.
    """
    total = len(raw_points)
    for index, point in enumerate(raw_points):
        ordinal_text = f"raw observation {format_int(index + 1)} of {format_int(total)}"
        point["selected_display"] = {
            "timestamp_text": point["ts"],
            "ordinal_text": ordinal_text,
        }
        series: dict[str, dict[str, str]] = {}
        for key, suffix in SERIES_SUFFIXES.items():
            value_text = (
                f"{format_int(series_snapshot_value(point, key, baseline))}{suffix}"
            )
            series[key] = {
                "value_text": value_text,
                "summary": (
                    f"At {point['ts']} the selected series measured {value_text}. "
                    f"This is {ordinal_text} in the retained raw window."
                ),
                "scrubber_text": f"{point['ts']} — {value_text}",
            }
        point["series_display"] = series


def composition_display(raw_points: list[dict[str, Any]]) -> dict[str, Any]:
    """Class-composition totals over the raw retention window, as published
    strings. The first retained point carries no interval, so it contributes
    no classified event."""
    source = raw_points[1:]
    totals = dict.fromkeys(CLASSES, 0)
    partial_totals = dict.fromkeys(CLASSES, 0)
    samples = 0
    incomplete_intervals = 0
    for point in source:
        composition = point["composition"]
        complete = composition["complete"]
        samples += composition["samples"]
        if not complete:
            incomplete_intervals += 1
        for name in CLASSES:
            count = composition["counts"][name]
            totals[name] += count
            if not complete:
                partial_totals[name] += count

    classes = [
        {
            "key": name,
            "label": CLASS_LABELS[name],
            "count_text": format_int(totals[name]),
            "share_text": format_percent(totals[name] / samples) if samples else "—",
            "window_text": (
                f"{format_int(partial_totals[name])} counted in incomplete event windows"
                if partial_totals[name]
                else "all counted in complete event windows"
            ),
        }
        for name in CLASSES
    ]

    if not source:
        summary_text = "No class-composition interval has been observed yet."
    else:
        summary_text = (
            f"{format_int(samples)} captured new-room events were classified "
            f"across {format_int(len(source))} observed intervals"
        )
        summary_text += (
            "; every event window was complete."
            if not incomplete_intervals
            else (
                f"; {format_int(incomplete_intervals)} of those intervals have "
                "an incomplete event window."
            )
        )
    return {
        "resolution_text": (
            "collector-tick raw · 24-hour retention · one column per observed interval"
        ),
        "summary_text": summary_text,
        "classes_text": (
            " · ".join(
                f"{entry['label']} {entry['count_text']} ({entry['share_text']})"
                for entry in classes
            )
            if samples
            else "No classified new-room event has been captured yet"
        ),
        "classes": classes,
        "interval_count": len(source),
        "incomplete_count": incomplete_intervals,
    }


def derive_records(
    records: Iterable[dict[str, Any]],
    rejected_ticks: int = 0,
    gap_seconds: float = 300.0,
) -> dict[str, Any]:
    if gap_seconds <= 0:
        raise ValueError("gap_seconds must be positive")

    computed_at = utc_now()
    ticks: list[dict[str, Any]] = []
    previous_time: datetime | None = None
    for record in records:
        try:
            tick = record if "_datetime" in record else validate_tick(record)
            if previous_time is not None and tick["_datetime"] <= previous_time:
                raise ValueError("tick timestamps are not strictly increasing")
        except ValueError:
            rejected_ticks += 1
            continue
        ticks.append(tick)
        previous_time = tick["_datetime"]

    methodology = methodology_definitions()
    if not ticks:
        return {
            "schema": 6,
            "collector_version": None,
            "methodology_version": METHODOLOGY_VERSION,
            "computed_at": computed_at,
            "collection_started": None,
            "collection_ended": None,
            "collection_age_seconds": None,
            "collection_stalled": False,
            "collection_stall_threshold_seconds": STALL_THRESHOLD_SECONDS,
            "collection_stall_banner": "",
            "collection_phase": "Collecting since",
            "status_display": {
                "state_text": "COLLECTION NOT STARTED",
                "age_text": "no observation recorded",
                "schema_text": "schema 6",
                "raw_window_text": "collector-tick raw · 24-hour retention",
            },
            "points": [],
            "gaps": [],
            "gap_count": 0,
            "composition_display": composition_display([]),
            "history": {
                "raw_retention_seconds": RAW_RETENTION_SECONDS,
                "raw_started_at": None,
                "raw_resolution_label": "collector-tick raw · 24-hour retention",
                "chart_resolution_label": (
                    "Chart: lifetime / 1-day / 1-hour rollups; "
                    "scrubber: collector-tick raw (24 hours)"
                ),
                "expected_tick_seconds": None,
                "rollup_levels": [],
            },
            "ledger_chain": ledger_chain_summary([]),
            "accepted_ticks": 0,
            "rejected_ticks": rejected_ticks,
            "methodology": methodology,
        }

    for tick in reversed(ticks):
        funnel = tick["signer_funnel"]
        disclosure = funnel["tracking_disclosure"] if funnel is not None else None
        if disclosure is not None:
            methodology["signer_funnel"] += f" {disclosure['methodology']}"
            break

    points: list[dict[str, Any]] = []
    gaps: list[dict[str, Any]] = []
    first_event_seq = ticks[0]["events_last_seq"]
    previous_identity: dict[str, Any] | None = None
    coverage_by_frame: dict[tuple[Any, ...], dict[str, Any]] = {}
    census_runs_by_walk: dict[str, dict[str, Any]] = {}
    # The newest completed census known at each point, from measured
    # identity_total ticks and from funnel-recorded census results alike.
    latest_census: tuple[int, str | None, str] | None = None
    # The most recent computed interval rate per headline tile, carried
    # forward with its sample count, window and measurement time so a
    # gap-suppressed interval still shows an honestly stamped value.
    carried_rates: dict[str, dict[str, Any]] = {}

    for index, tick in enumerate(ticks):
        census_candidates: list[tuple[int, str | None, str]] = []
        if tick["identity_total"] is not None:
            census_candidates.append(
                (
                    tick["identity_total"],
                    tick["identity_census_started"],
                    tick["ts"],
                )
            )
        raw_funnel = tick["signer_funnel"]
        if raw_funnel is not None and raw_funnel["well_formed_did_notes"] is not None:
            census_candidates.append(
                (
                    raw_funnel["well_formed_did_notes"],
                    raw_funnel["census_started_at"],
                    raw_funnel["census_completed_at"],
                )
            )
        for candidate in census_candidates:
            if (
                latest_census is None
                or parse_ts(candidate[2]) > parse_ts(latest_census[2])
                or (
                    candidate[2] == latest_census[2]
                    and latest_census[1] is None
                    and candidate[1] is not None
                )
            ):
                latest_census = candidate

        room_sampling = derived_room_sampling(tick["room_sampling"], coverage_by_frame)
        census_run = derived_identity_census_run(
            tick.get("identity_census_run"),
            census_runs_by_walk,
        )
        point = {
            "ts": tick["ts"],
            "rooms_total": tick["rooms_total"],
            "room_cap": tick["room_cap"],
            "bytes_stored": tick["bytes_stored"],
            "notes_total": tick["notes_total"],
            "note_cap": tick["note_cap"],
            "lobby_last_seq": tick["lobby_last_seq"],
            "events_last_seq": tick["events_last_seq"],
            "identity_total": tick["identity_total"],
            "identity_census_started": tick["identity_census_started"],
            "identity_census_run": census_run,
            "identity_census_run_display": identity_census_run_display(census_run),
            "census_display": census_display(latest_census),
            "observed_public_rooms": (
                tick["events_last_seq"] - first_event_seq
                if tick["events_last_seq"] >= first_event_seq
                else None
            ),
            "room_sampling": room_sampling,
            "rates": {
                "public_rooms_per_second": empty_rate(),
                "lobby_messages_per_second": empty_rate(),
                "rooms_total_per_second": empty_rate(),
                "notes_per_second": empty_rate(),
                "identities_per_second": empty_rate(),
            },
            "capacity": {
                "notes": measured_capacity(
                    ticks, index, "notes_total", "note_cap", gap_seconds
                ),
                "rooms": measured_capacity(
                    ticks, index, "rooms_total", "room_cap", gap_seconds
                ),
            },
            "signer_funnel": display_funnel(
                tick["signer_funnel"],
                len(tick["newest_rooms"]),
                tick["rooms_total"],
                room_sampling,
                latest_census,
            ),
            "room_lifecycle": tick["room_lifecycle"],
            "room_lifecycle_display": room_lifecycle_display(
                tick["room_lifecycle"],
                tick["room_lifecycle_sampling"],
            ),
            "room_lifecycle_sampling": tick["room_lifecycle_sampling"],
            "room_lifecycle_sampling_display": room_lifecycle_sampling_display(
                tick["room_lifecycle_sampling"]
            ),
            "engagement": tick["engagement"],
            "engagement_display": engagement_display(tick["engagement"]),
            "composition": {
                "counts": dict.fromkeys(CLASSES, 0),
                "samples": 0,
                "expected": 0,
                "complete": False,
            },
            "name_signal": {
                "auto_generated_looking": 0,
                "samples": 0,
                "fraction": None,
            },
            "stillborn_signal": {
                "first_message_only": 0,
                "matched_new_rooms": 0,
                "fraction": None,
                "definition": "Newest listed rooms matching captured new events with seq <= 1.",
            },
        }

        interval_reason: str | None = None
        if index:
            previous = ticks[index - 1]
            elapsed = (tick["_datetime"] - previous["_datetime"]).total_seconds()
            is_gap = elapsed > gap_seconds
            deltas = {
                "public_rooms_per_second": tick["events_last_seq"]
                - previous["events_last_seq"],
                "lobby_messages_per_second": tick["lobby_last_seq"]
                - previous["lobby_last_seq"],
                "rooms_total_per_second": tick["rooms_total"] - previous["rooms_total"],
                "notes_per_second": tick["notes_total"] - previous["notes_total"],
            }
            negative = [name for name, delta in deltas.items() if delta < 0]
            if is_gap or negative:
                gaps.append(
                    {
                        "from": previous["ts"],
                        "to": tick["ts"],
                        "seconds": elapsed,
                        "reason": "counter_decreased" if negative else "polling_gap",
                        "metrics": negative,
                    }
                )
                interval_reason = interval_reason_text(negative, elapsed)
            else:
                for name, delta in deltas.items():
                    point["rates"][name] = rate(delta, elapsed)

            new_events = [
                event
                for event in tick["events_window"]
                if event["seq"] > previous["events_last_seq"]
            ]
            expected = max(0, tick["events_last_seq"] - previous["events_last_seq"])
            counts = dict.fromkeys(CLASSES, 0)
            for event in new_events:
                counts[event["primary_class"]] += 1
            point["composition"] = {
                "counts": counts,
                "samples": len(new_events),
                "expected": expected,
                "complete": len(new_events) == expected,
            }

            auto_count = sum(
                auto_generated_looking(event["base_name"]) for event in new_events
            )
            point["name_signal"] = {
                "auto_generated_looking": auto_count,
                "samples": len(new_events),
                "fraction": auto_count / len(new_events) if new_events else None,
            }

            rooms_by_name = {room["name"]: room for room in tick["newest_rooms"]}
            matched = [
                rooms_by_name[event["name"]]
                for event in new_events
                if event["name"] in rooms_by_name
            ]
            first_message_only = sum(room["seq"] <= 1 for room in matched)
            point["stillborn_signal"] = {
                "first_message_only": first_message_only,
                "matched_new_rooms": len(matched),
                "fraction": first_message_only / len(matched) if matched else None,
                "definition": "Newest listed rooms matching captured new events with seq <= 1.",
            }

            if expected > len(new_events):
                gaps.append(
                    {
                        "from": previous["ts"],
                        "to": tick["ts"],
                        "seconds": elapsed,
                        "reason": "events_window_incomplete",
                        "expected": expected,
                        "captured": len(new_events),
                    }
                )

        point["rate_display"] = {}
        for metric in ("public_rooms_per_second", "lobby_messages_per_second"):
            rate_value = point["rates"][metric]
            point["rate_display"][metric] = rate_tile_display(
                rate_value, carried_rates.get(metric), interval_reason
            )
            if rate_value["samples"]:
                carried_rates[metric] = {
                    "value": rate_value["value"],
                    "samples": rate_value["samples"],
                    "seconds": rate_value["seconds"],
                    "ts": tick["ts"],
                }

        point["capacity_display"] = {
            name: capacity_metric_display(point["capacity"][name])
            for name in ("notes", "rooms")
        }
        point["stillborn_display"] = stillborn_display(point["stillborn_signal"])
        # The funnel merges the manifest with the listing and room totals, so
        # its coverage is the fuller record where a funnel exists.
        point["sampling_display"] = sampling_display(
            point["signer_funnel"]["coverage"]
            if point["signer_funnel"] is not None
            else room_sampling
        )

        if tick["identity_total"] is not None:
            if previous_identity is not None:
                identity_elapsed = (
                    tick["_datetime"] - previous_identity["_datetime"]
                ).total_seconds()
                identity_delta = (
                    tick["identity_total"] - previous_identity["identity_total"]
                )
                if identity_elapsed > 0 and identity_delta >= 0:
                    point["rates"]["identities_per_second"] = rate(
                        identity_delta, identity_elapsed
                    )
                elif identity_delta < 0:
                    gaps.append(
                        {
                            "from": previous_identity["ts"],
                            "to": tick["ts"],
                            "seconds": identity_elapsed,
                            "reason": "identity_counter_decreased",
                            "metrics": ["identities_per_second"],
                        }
                    )
            previous_identity = tick

        points.append(point)

    raw_points, history, recent_gaps = reduce_payload_history(
        points,
        gaps,
        ticks[-1]["_datetime"],
        gap_seconds,
    )

    # Stall detection: the rebuild cron runs independently of the collector,
    # so the age of the newest accepted tick at rebuild time is the collector
    # liveness signal. Clamped at zero in case computed_at precedes the tick.
    age_seconds = max(
        0.0, (parse_ts(computed_at) - ticks[-1]["_datetime"]).total_seconds()
    )
    stalled = age_seconds > STALL_THRESHOLD_SECONDS

    # The chart baseline is the oldest accepted observation, which is also the
    # first record of the oldest rollup level, so the scripted view and these
    # strings measure the same distance from the collection boundary.
    attach_series_display(raw_points, points[0])

    return {
        "schema": 6,
        "collector_version": ticks[-1]["collector_version"],
        "methodology_version": METHODOLOGY_VERSION,
        "computed_at": computed_at,
        "collection_started": ticks[0]["ts"],
        "collection_ended": ticks[-1]["ts"],
        "collection_age_seconds": age_seconds,
        "collection_stalled": stalled,
        "collection_stall_threshold_seconds": STALL_THRESHOLD_SECONDS,
        "collection_stall_banner": (
            f"COLLECTION STALLED — no observation for {stall_duration_text(age_seconds)}"
            if stalled
            else ""
        ),
        "collection_phase": "Collection began" if stalled else "Collecting since",
        "status_display": {
            "state_text": "STALLED" if stalled else "ACTIVE",
            "age_text": f"newest tick {stall_duration_text(age_seconds)} old",
            "schema_text": "schema 6",
            "raw_window_text": history["raw_resolution_label"],
        },
        "points": raw_points,
        "gaps": recent_gaps,
        # "Recorded gaps" counts collector cadence gaps as intervals, not
        # records: one interval can carry a polling gap and an incomplete
        # event window, and a counter decrease inside the polling threshold is
        # not a missed observation.
        "gap_count": len(
            {
                (gap["from"], gap["to"])
                for gap in gaps
                if is_cadence_gap(gap, gap_seconds)
            }
        ),
        "history": history,
        "composition_display": composition_display(raw_points),
        "ledger_chain": ledger_chain_summary(ticks),
        "accepted_ticks": len(points),
        "rejected_ticks": rejected_ticks,
        "methodology": methodology,
    }


def format_int(value: Any) -> str:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
    ):
        return "—"
    return f"{value:,.0f}"


def format_rate(value: Any) -> str:
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(value)
    ):
        return "—"
    if abs(value) >= 1:
        return f"{value:.2f}/s"
    return f"{value * 60:.2f}/min"


def format_percent(value: Any) -> str:
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(value)
    ):
        return "—"
    return f"{value * 100:.1f}%"


def capacity_rate_context(metric: Any) -> str:
    if not isinstance(metric, dict):
        return "No capacity observation"
    window = metric.get("trailing_window")
    if not isinstance(window, dict) or not window.get("samples"):
        return "Observed net-change rate needs at least two gap-free samples under the current cap"
    return (
        f"{format_rate(metric.get('trailing_rate'))} observed net change · "
        f"{format_int(window['samples'])} samples over {round(window['seconds'])}s"
    )


def sampling_ssr(display: Any) -> dict[str, str]:
    """The sampling register, read verbatim from the per-point display contract
    so the server-rendered and scripted views cannot use different fallbacks."""
    values = display if isinstance(display, dict) else sampling_display(None)
    return {
        "coverage-frame": values["frame"],
        "coverage-sampled": values["sampled"],
        "coverage-unique": values["unique"],
        "coverage-repeats": values["repeats"],
        "coverage-failures": values["failures"],
        "coverage-selector": values["selector"],
        "coverage-newest": values["newest"],
        "coverage-total": values["total"],
    }


def room_lifecycle_ssr(display: Any) -> dict[str, str]:
    entries = {
        "ledger": ("lifecycle-ledger", "lifecycle-ledger-context"),
        "revisited": ("lifecycle-revisited", "lifecycle-revisited-context"),
        "conversion": ("lifecycle-conversion", "lifecycle-conversion-context"),
        "failures": ("lifecycle-failures", "lifecycle-failures-context"),
        "budget": ("lifecycle-budget", "lifecycle-budget-context"),
        "senders": ("lifecycle-senders", "lifecycle-senders-context"),
    }
    values: dict[str, str] = {}
    for key, (value_key, context_key) in entries.items():
        entry = display.get(key) if isinstance(display, dict) else None
        if isinstance(entry, dict):
            values[value_key] = str(entry.get("value_text", "—"))
            values[context_key] = str(
                entry.get("context", "No room-lifecycle display recorded")
            )
        else:
            values[value_key] = "—"
            values[context_key] = "No room-lifecycle display recorded"
    values["lifecycle-coverage"] = (
        str(display.get("coverage_text"))
        if isinstance(display, dict) and isinstance(display.get("coverage_text"), str)
        else "Room lifecycle has not been recorded."
    )
    return values


def room_lifecycle_sampling_ssr(display: Any) -> dict[str, str]:
    fallback = room_lifecycle_sampling_display(None)
    source = display if isinstance(display, dict) else fallback
    selection = source.get("selection")
    if not isinstance(selection, dict):
        selection = fallback["selection"]
    aged_out = source.get("aged_out")
    if not isinstance(aged_out, dict):
        aged_out = fallback["aged_out"]
    stages = source.get("stages")
    if not isinstance(stages, dict):
        stages = fallback["stages"]

    values = {
        "lifecycle-sampling-selection": str(selection["value_text"]),
        "lifecycle-sampling-selection-context": str(selection["context"]),
        "lifecycle-sampling-aged-out": str(aged_out["value_text"]),
        "lifecycle-sampling-aged-out-context": str(aged_out["context"]),
    }
    for stage in ("300", "3600", "86400"):
        entry = stages.get(stage)
        if not isinstance(entry, dict):
            entry = fallback["stages"][stage]
        values[f"lifecycle-stage-{stage}"] = str(entry["value_text"])
        values[f"lifecycle-stage-{stage}-context"] = str(entry["context"])
    return values


def engagement_ssr(display: Any) -> dict[str, str]:
    """Server-rendered engagement tiles, read verbatim from the shared display
    contract. The fallback strings match the page JavaScript's fallbacks
    exactly, so the two views cannot disagree even on a defective payload."""
    tiles = {
        "ratio": ("engagement-ratio", "engagement-ratio-context"),
        "zero_response": ("engagement-zero", "engagement-zero-context"),
        "nick_diversity": ("engagement-nick", "engagement-nick-context"),
    }
    values: dict[str, str] = {}
    for key, (value_key, context_key) in tiles.items():
        entry = display.get(key) if isinstance(display, dict) else None
        if isinstance(entry, dict):
            values[value_key] = str(entry.get("value_text", "—"))
            values[context_key] = str(
                entry.get("context", "No engagement display recorded")
            )
        else:
            values[value_key] = "—"
            values[context_key] = "No engagement display recorded"
    return values


# Every measured block names the versioned definition it was computed under.
# The stamp is assembled here so a rail can never quote a version the payload
# does not carry.
METHOD_STAMPS = {
    "method-instrument": "signer_funnel",
    "method-sampling": "room_sampling",
    "method-census": "identity_census",
    "method-integrity-census": "identity_census",
    "method-growth": "room_growth",
    "method-growth-rate": "room_growth",
    "method-lobby": "lobby_velocity",
    "method-capacity": "capacity",
    "method-lifecycle": "room_lifecycle",
    "method-first-message": "first_message_only",
    "method-engagement": "service_engagement",
    "method-composition": "room_composition",
    "method-integrity": "ledger_integrity",
}


def ssr_values(data: dict[str, Any]) -> dict[str, str]:
    history = data.get("history")
    resolution_label = (
        history.get("chart_resolution_label") if isinstance(history, dict) else None
    )
    chain = data.get("ledger_chain")
    chain_display = chain.get("display") if isinstance(chain, dict) else None
    chain_anchor = chain.get("anchor_display") if isinstance(chain, dict) else None
    status = data.get("status_display")
    status = status if isinstance(status, dict) else {}
    composition = data.get("composition_display")
    if not isinstance(composition, dict):
        composition = composition_display([])
    methodology_version = str(data.get("methodology_version") or "—")
    versions = {
        "collector-version": str(data.get("collector_version") or "—"),
        "methodology-version": f"methodology {methodology_version}",
        "computed-at": str(data.get("computed_at") or "—"),
        "schema-version": str(status.get("schema_text") or "schema not recorded"),
        "raw-window": str(
            status.get("raw_window_text") or "collector-tick raw · 24-hour retention"
        ),
        "resolution-label": str(
            resolution_label
            or "Chart resolution unavailable; scrubber contains no raw observations"
        ),
        "ledger-integrity": str(
            chain_display or "Hash-chain state unavailable; no integrity claim is made"
        ),
        "ledger-anchor": str(chain_anchor or LEDGER_ANCHOR_NOT_RECORDED),
        "composition-summary": str(composition["summary_text"]),
        "composition-classes": str(composition["classes_text"]),
        **{
            key: f"{definition}@{methodology_version}"
            for key, definition in METHOD_STAMPS.items()
        },
    }
    missing_capacity = capacity_metric_display(None)
    points = data.get("points")
    if not isinstance(points, list) or not points:
        return {
            "status": "0 collected observations",
            "collection-state": str(
                status.get("state_text") or "COLLECTION NOT STARTED"
            ),
            "newest-age": str(status.get("age_text") or "no observation recorded"),
            "hero-value": "—",
            "chart-summary": (
                "No observation has been collected, so no series can be summarised."
            ),
            "selected-observation": "No collection start",
            "selected-ordinal": "No raw observation retained",
            "timestamp": "No collection start",
            "room-rate": "—",
            "room-rate-samples": "Needs two gap-free observations",
            "lobby-rate": "—",
            "lobby-rate-samples": "Needs two gap-free observations",
            "identity-total": "—",
            "identity-rate": "No complete census yet",
            "identity-census-run": CENSUS_RUN_NOT_RECORDED,
            "stillborn": "—",
            "stillborn-samples": "No matched new rooms",
            "engagement-ratio": "—",
            "engagement-ratio-context": "No engagement observation yet",
            "engagement-zero": "—",
            "engagement-zero-context": "No engagement observation yet",
            "engagement-nick": "—",
            "engagement-nick-context": "No engagement observation yet",
            **room_lifecycle_ssr(None),
            **room_lifecycle_sampling_ssr(None),
            "notes-cap-count": missing_capacity["count_text"],
            "notes-headroom": missing_capacity["headroom_text"],
            "notes-rate": missing_capacity["rate_text"],
            "notes-cap-change": missing_capacity["cap_change_text"],
            "rooms-cap-count": missing_capacity["count_text"],
            "rooms-headroom": missing_capacity["headroom_text"],
            "rooms-rate": missing_capacity["rate_text"],
            "rooms-cap-change": missing_capacity["cap_change_text"],
            "funnel-census": "—",
            "funnel-census-context": "No completed census yet",
            "funnel-observed": "—",
            "funnel-observed-context": "No observed-signing measurement yet",
            "funnel-two-ticks": "—",
            "funnel-two-ticks-context": "Captured cohort unavailable",
            "funnel-two-dates": "—",
            "funnel-two-dates-context": "Captured cohort unavailable",
            "funnel-sustained": "—",
            "funnel-sustained-context": "Captured cohort unavailable",
            "funnel-warning": (
                "Signer sampling has not started. No historical activity is inferred."
            ),
            "funnel-coverage": "No manifest-backed room coverage is available.",
            "tracked-dids": "No signer state recorded",
            "stall-alert": "",
            "collection-phase": "Collecting since",
            "collection-start": "collection start",
            "quality": (
                f"0 accepted · {format_int(data.get('rejected_ticks', 0))} rejected "
                "· 0 recorded gaps"
            ),
            "empty-state": (
                "No observations have been collected. The chart remains empty by design."
            ),
            **sampling_ssr(None),
            **versions,
        }

    point = points[-1]
    rate_display = point.get("rate_display") or {}
    unavailable_rate = {"value_text": "—", "context": "Needs two gap-free observations"}
    room_tile = rate_display.get("public_rooms_per_second") or unavailable_rate
    lobby_tile = rate_display.get("lobby_messages_per_second") or unavailable_rate
    census_tile = point.get("census_display")
    if not isinstance(census_tile, dict):
        census_tile = census_display(None)
    stillborn = point.get("stillborn_display")
    if not isinstance(stillborn, dict):
        stillborn = stillborn_display(point.get("stillborn_signal"))
    capacity_tiles = point.get("capacity_display")
    if not isinstance(capacity_tiles, dict):
        capacity_tiles = {}
    notes = capacity_tiles.get("notes") or missing_capacity
    rooms = capacity_tiles.get("rooms") or missing_capacity
    series = point.get("series_display")
    rooms_series = series.get("rooms") if isinstance(series, dict) else None
    if not isinstance(rooms_series, dict):
        rooms_series = {
            "value_text": f"{format_int(point.get('observed_public_rooms'))} observed",
            "summary": "No series summary was recorded for this observation.",
        }
    selected = point.get("selected_display")
    selected = selected if isinstance(selected, dict) else {}
    funnel = point.get("signer_funnel")
    accepted = data.get("accepted_ticks", len(points))
    rejected = data.get("rejected_ticks", 0)
    gaps = data.get("gaps") if isinstance(data.get("gaps"), list) else []
    gap_count = data.get("gap_count", len(gaps))

    values = {
        "status": f"{format_int(accepted)} collected observations",
        "collection-state": str(status.get("state_text") or "ACTIVE"),
        "newest-age": str(status.get("age_text") or "newest tick age not recorded"),
        "hero-value": str(rooms_series["value_text"]),
        "chart-summary": str(rooms_series["summary"]),
        "selected-observation": str(
            selected.get("timestamp_text") or point.get("ts", "invalid timestamp")
        ),
        "selected-ordinal": str(
            selected.get("ordinal_text") or "No raw observation retained"
        ),
        # The status timestamp is the newest accepted tick and never follows the
        # scrubber; the selected observation above is the one that moves.
        "timestamp": str(point.get("ts", "invalid timestamp")),
        "room-rate": room_tile["value_text"],
        "room-rate-samples": room_tile["context"],
        "lobby-rate": lobby_tile["value_text"],
        "lobby-rate-samples": lobby_tile["context"],
        "identity-total": str(census_tile.get("value_text", "—")),
        "identity-rate": str(census_tile.get("context", "No completed census yet")),
        "identity-census-run": str(
            point.get("identity_census_run_display") or CENSUS_RUN_NOT_RECORDED
        ),
        "stillborn": stillborn["value_text"],
        "stillborn-samples": stillborn["context"],
        **engagement_ssr(point.get("engagement_display")),
        **room_lifecycle_ssr(point.get("room_lifecycle_display")),
        **room_lifecycle_sampling_ssr(point.get("room_lifecycle_sampling_display")),
        "notes-cap-count": notes["count_text"],
        "notes-headroom": notes["headroom_text"],
        "notes-rate": notes["rate_text"],
        "notes-cap-change": notes["cap_change_text"],
        "rooms-cap-count": rooms["count_text"],
        "rooms-headroom": rooms["headroom_text"],
        "rooms-rate": rooms["rate_text"],
        "rooms-cap-change": rooms["cap_change_text"],
        "stall-alert": str(data.get("collection_stall_banner") or ""),
        "collection-phase": str(data.get("collection_phase") or "Collecting since"),
        "collection-start": str(data.get("collection_started")),
        "quality": (
            f"{format_int(accepted)} accepted · {format_int(rejected)} rejected "
            f"· {format_int(gap_count)} recorded gaps"
        ),
        "empty-state": "",
        **sampling_ssr(point.get("sampling_display")),
        **versions,
    }

    display = funnel.get("display") if isinstance(funnel, dict) else None
    if not isinstance(display, dict):
        values.update(
            {
                "funnel-census": "—",
                "funnel-census-context": "No completed census yet",
                "funnel-observed": "—",
                "funnel-observed-context": "No observed-signing measurement yet",
                "funnel-two-ticks": "—",
                "funnel-two-ticks-context": "Captured cohort unavailable",
                "funnel-two-dates": "—",
                "funnel-two-dates-context": "Captured cohort unavailable",
                "funnel-sustained": "—",
                "funnel-sustained-context": "Captured cohort unavailable",
                "funnel-warning": (
                    "Signer sampling has not started. No historical activity is inferred."
                ),
                "funnel-coverage": "No manifest-backed room coverage is available.",
                "tracked-dids": "No signer state recorded",
            }
        )
        return values

    stages = {stage["key"]: stage for stage in display["stages"]}
    values.update(
        {
            "tracked-dids": display["tracked_text"],
            "funnel-census": display["census"]["value_text"],
            "funnel-census-context": display["census"]["context"],
            "funnel-observed": stages["observed"]["value_text"],
            "funnel-observed-context": stages["observed"]["context"],
            "funnel-two-ticks": stages["two_ticks"]["value_text"],
            "funnel-two-ticks-context": stages["two_ticks"]["context"],
            "funnel-two-dates": stages["two_dates"]["value_text"],
            "funnel-two-dates-context": stages["two_dates"]["context"],
            "funnel-sustained": stages["sustained"]["value_text"],
            "funnel-sustained-context": stages["sustained"]["context"],
            "funnel-warning": display["warning"],
            "funnel-coverage": display["coverage_text"],
        }
    )
    return values


def ssr_widths(data: dict[str, Any]) -> dict[str, float]:
    """Server-rendered bar widths, so the no-JS view never shows empty bars."""
    # The census carries no bar: it is a separate population from the observed
    # cohort the funnel filters, so drawing it on the same track would assert
    # containment the data does not support.
    widths = {
        "funnel-observed": 0.0,
        "funnel-two-ticks": 0.0,
        "funnel-two-dates": 0.0,
        "funnel-sustained": 0.0,
        "notes-fill": 0.0,
        "rooms-fill": 0.0,
    }
    points = data.get("points")
    if not isinstance(points, list) or not points:
        return widths

    point = points[-1]
    capacity = point.get("capacity") or {}
    for key, name in (("notes-fill", "notes"), ("rooms-fill", "rooms")):
        metric = capacity.get(name) or {}
        fraction = metric.get("fill_fraction")
        if (
            isinstance(fraction, (int, float))
            and not isinstance(fraction, bool)
            and math.isfinite(fraction)
        ):
            widths[key] = max(0.0, min(1.0, float(fraction))) * 100

    funnel = point.get("signer_funnel")
    display = funnel.get("display") if isinstance(funnel, dict) else None
    if isinstance(display, dict):
        stage_keys = {
            "observed": "funnel-observed",
            "two_ticks": "funnel-two-ticks",
            "two_dates": "funnel-two-dates",
            "sustained": "funnel-sustained",
        }
        for stage in display["stages"]:
            widths[stage_keys[stage["key"]]] = stage["width_percent"]
    return widths


def description_text(data: dict[str, Any]) -> str:
    points = data.get("points")
    raw_count = len(points) if isinstance(points, list) else 0
    count = data.get("accepted_ticks", raw_count)
    if not count:
        return (
            "Technocore Observatory: 0 forward-collected observations; "
            "the collection window has not started."
        )
    return (
        f"Technocore Observatory: {format_int(count)} forward-collected observations "
        f"from {data.get('collection_started')} to {data.get('collection_ended')}."
    )


def replace_ssr_text(source: str, key: str, value: str) -> str:
    pattern = re.compile(
        rf'(<(?P<tag>[a-z][\w:-]*)\b(?=[^>]*\bdata-ssr="{re.escape(key)}")[^>]*>)'
        rf".*?(</(?P=tag)>)",
        re.DOTALL | re.IGNORECASE,
    )
    updated, replacements = pattern.subn(
        lambda match: match.group(1) + html.escape(value, quote=False) + match.group(3),
        source,
        count=1,
    )
    if replacements != 1:
        raise ValueError(f"HTML contains no unique data-ssr element for {key}")
    return updated


def replace_ssr_width(source: str, key: str, percent: float) -> str:
    pattern = re.compile(rf'(data-ssr-width="{re.escape(key)}" style=")[^"]*(")')
    updated, replacements = pattern.subn(
        lambda match: match.group(1) + f"width:{percent:.4f}%" + match.group(2),
        source,
        count=1,
    )
    if replacements != 1:
        raise ValueError(f"HTML contains no unique data-ssr-width element for {key}")
    return updated


def replace_meta(source: str, attribute: str, name: str, value: str) -> str:
    pattern = re.compile(
        rf'(<meta\s+{re.escape(attribute)}="{re.escape(name)}"\s+content=")[^"]*(">)',
        re.IGNORECASE,
    )
    updated, replacements = pattern.subn(
        lambda match: match.group(1) + html.escape(value, quote=True) + match.group(2),
        source,
        count=1,
    )
    if replacements != 1:
        raise ValueError(f"HTML contains no unique {attribute}={name} meta element")
    return updated


def inject_html(path: Path, data: dict[str, Any]) -> None:
    source = path.read_text(encoding="utf-8")
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":")).replace(
        "<", "\\u003c"
    )
    pattern = re.compile(
        r'(<script id="observatory-data" type="application/json">).*?(</script>)',
        re.DOTALL,
    )
    updated, replacements = pattern.subn(
        lambda match: match.group(1) + payload + match.group(2),
        source,
        count=1,
    )
    if replacements != 1:
        raise ValueError("HTML contains no unique observatory-data script")

    for key, value in ssr_values(data).items():
        updated = replace_ssr_text(updated, key, value)

    for key, percent in ssr_widths(data).items():
        updated = replace_ssr_width(updated, key, percent)

    # The search-snippet description carries the observation count and window.
    # og:description must NOT: a share card is cached indefinitely by the
    # consuming platform, so an observed figure there goes stale with no
    # evidence rail to qualify it. The card keeps the template's generic line.
    description = description_text(data)
    updated = replace_meta(updated, "name", "description", description)
    path.write_text(updated, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--html", type=Path)
    parser.add_argument("--gap-seconds", type=float, default=300.0)
    args = parser.parse_args()

    with args.input.open(encoding="utf-8") as source:
        ticks, rejected = read_jsonl(source)
    data = derive_records(ticks, rejected, args.gap_seconds)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(data, ensure_ascii=False, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    if args.html:
        inject_html(args.html, data)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
