#!/usr/bin/env python3
"""Derive compact, forward-only Observatory data from collector JSONL."""

from __future__ import annotations

import argparse
import html
import json
import math
import re
from datetime import datetime, timezone
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
UUID_LIKE_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
    re.IGNORECASE,
)
MIXED_TOKEN_RE = re.compile(r"^[a-z0-9]{12,}$", re.IGNORECASE)
TRAILING_WINDOW_SECONDS = 24 * 60 * 60
METHODOLOGY_VERSION = "1.5.0"
ROOM_SAMPLING_STRUCTURAL_CEILING = 200
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


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_ts(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("timestamp is not a string")
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        raise ValueError("timestamp has no timezone")
    return parsed.astimezone(timezone.utc)


def optional_timestamp(value: Any, field: str) -> str | None:
    if value is None:
        return None
    try:
        parse_ts(value)
    except ValueError as error:
        raise ValueError(f"{field} is not a valid timestamp") from error
    return value


def integer(value: Any, field: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
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
        raise ValueError("room_sampling.sampled is not a non-empty list within its read limit")

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

    The collector stores the /rooms `engagement` object verbatim, so its
    contents are the service's choice, not the collector's. Rejecting the
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
        number = float(raw)
        if not math.isfinite(number) or number < 0:
            return None
        if maximum is not None and number > maximum:
            return None
        return number

    def non_negative_int(raw: Any) -> int | None:
        if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
            return None
        return raw

    return {
        "windowed_note_to_message_ratio": finite_number(
            value.get("windowed_note_to_message_ratio")
        ),
        "zero_response_share": finite_number(value.get("zero_response_share"), maximum=1.0),
        "nick_diversity": finite_number(value.get("nick_diversity")),
        "window_cap": non_negative_int(value.get("window_cap")),
        "windowed_messages": non_negative_int(value.get("windowed_messages")),
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
    if not isinstance(name, str) or not isinstance(base_name, str) or primary not in CLASSES:
        raise ValueError("event contains an invalid name or class")
    return {
        "seq": seq,
        "ts": ts,
        "name": name,
        "primary_class": primary,
        "base_name": base_name,
    }


def validate_funnel(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError("signer_funnel is not an object")

    result: dict[str, Any] = {}
    for field in FUNNEL_BASE_FIELDS:
        result[field] = optional_integer(value.get(field), f"signer_funnel.{field}")

    if result["well_formed_did_notes"] is None:
        if value.get("census_completed_at") is not None:
            raise ValueError("funnel has a census timestamp without a census")
    else:
        parse_ts(value.get("census_completed_at"))
    result["census_completed_at"] = value.get("census_completed_at")

    coverage = value.get("coverage")
    if not isinstance(coverage, dict):
        raise ValueError("signer funnel is missing its coverage data")
    sampled = integer(coverage.get("sampled_rooms"), "coverage.sampled_rooms")
    known = coverage.get("known_rooms")
    if known is not None:
        known = integer(known, "coverage.known_rooms", 1)
    result["coverage"] = {"sampled_rooms": sampled, "known_rooms": known}

    legacy_persistence = "two_collection_utc_dates" not in value
    persistence_field = "two_utc_dates" if legacy_persistence else "two_collection_utc_dates"
    raw_two_dates = optional_integer(
        value.get(persistence_field),
        f"signer_funnel.{persistence_field}",
    )
    raw_two_rooms = optional_integer(value.get("two_rooms"), "signer_funnel.two_rooms")
    raw_counterparty = optional_integer(
        value.get("signed_counterparty"),
        "signer_funnel.signed_counterparty",
    )

    raw_observed_stages = [
        result["dids_observed_signing"],
        result["seen_two_ticks"],
        raw_two_dates,
        raw_two_rooms,
        raw_counterparty,
    ]
    if any(stage is None for stage in raw_observed_stages):
        raise ValueError("signer funnel has a missing observed stage")
    if any(right > left for left, right in zip(raw_observed_stages, raw_observed_stages[1:])):
        raise ValueError("signer funnel stages are not monotonic")
    # The census and the observed-signing count measure different populations
    # (a signer need not have published a DID note), so observed > census is a
    # possible honest measurement and is not rejected.

    if legacy_persistence:
        result["two_collection_utc_dates"] = 0
        result["two_rooms"] = 0
        result["signed_counterparty"] = 0
        result["persistence_started_at"] = None
        result["persistence_reset_at"] = None
        result["persistence_collection_utc_dates_count"] = 0
    else:
        result["two_collection_utc_dates"] = raw_two_dates
        result["two_rooms"] = raw_two_rooms
        result["signed_counterparty"] = raw_counterparty
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

    corrected_stages = [
        result["dids_observed_signing"],
        result["seen_two_ticks"],
        result["two_collection_utc_dates"],
        result["two_rooms"],
        result["signed_counterparty"],
    ]
    if any(right > left for left, right in zip(corrected_stages, corrected_stages[1:])):
        raise ValueError("corrected signer funnel stages are not monotonic")

    result["legacy_persistence_reset"] = legacy_persistence
    result["sustained_reciprocal_footprint"] = result["signed_counterparty"]
    result["tracking_disclosure"] = validate_tracking_disclosure(
        value.get("tracking_disclosure")
    )
    result["tracked_dids"] = integer(value.get("tracked_dids"), "tracked_dids")
    result["tracked_cap"] = integer(value.get("tracked_cap"), "tracked_cap", 1)
    if (
        result["tracking_disclosure"] is None
        and result["tracked_dids"] > result["tracked_cap"]
    ):
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
            or isinstance(idle, bool)
            or not isinstance(idle, (int, float))
            or not math.isfinite(idle)
            or idle < 0
        ):
            raise ValueError("room has an invalid name, seq, or idle value")
        validated_rooms.append({"name": name, "seq": seq, "idle_seconds": float(idle)})

    raw_collector_version = value.get("collector_version")
    result = {
        "collector_version": (
            "legacy"
            if raw_collector_version is None
            else version_string(raw_collector_version, "collector_version")
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
        "identity_total": optional_integer(value.get("identity_total"), "identity_total"),
        "events_window": validated_events,
        "newest_rooms": validated_rooms,
        "room_sampling": validate_room_sampling(value.get("room_sampling")),
        "signer_funnel": validate_funnel(value.get("signer_funnel")),
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
        except (json.JSONDecodeError, ValueError):
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
        result["trailing_rate"] = (segment[-1][total_field] - segment[0][total_field]) / elapsed
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
        window_parts.append(f"window cap {format_int(engagement['window_cap'])} messages")
    if engagement["windowed_messages"] is not None:
        window_parts.append(f"{format_int(engagement['windowed_messages'])} windowed messages")
    window_text = (
        " · ".join(window_parts) if window_parts else "window figures not published this tick"
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
    sustained = funnel["sustained_reciprocal_footprint"]
    date_count = funnel["persistence_collection_utc_dates_count"]
    tracking_disclosure = funnel["tracking_disclosure"]
    cap_gates = tracking_disclosure is None

    # Bars are proportions of the observed-signing cohort, the population the
    # later stages actually filter. The census is a separate population that
    # neither contains nor is contained by the observed cohort, so it is never
    # a bar denominator: drawn against the census width, every bar visually
    # asserted "N of the census are real", a containment claim the data does
    # not support.
    bar_denominator = max(1, observed) if isinstance(observed, int) else 1

    def width(value: Any) -> float:
        if isinstance(value, bool) or not isinstance(value, int):
            return 0.0
        return max(1.0, value / bar_denominator * 100)

    census_context = (
        f"last completed census · {funnel['census_completed_at']}"
        if census is not None
        else "No completed census yet"
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
    if two_dates == 0 and date_count < 2:
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
    sustained_context = (
        f"{format_percent(sustained / two_dates)} of {format_int(two_dates)} "
        "observed on ≥2 distinct collection UTC dates"
        if two_dates > 0
        else "Collection-day-qualified cohort denominator unavailable"
    )

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
            f"{format_int(coverage['sampled_rooms'])} selected this tick; "
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
        ("two_dates", two_dates, two_dates_context),
        ("sustained", sustained, sustained_context),
    ]
    return {
        "census": {
            "value": census,
            "value_text": format_int(census),
            "context": census_context,
        },
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
            if tracking_disclosure is not None
            else (
                f"{format_int(funnel['tracked_dids'])} / "
                f"{format_int(funnel['tracked_cap'])} tracked DIDs "
                f"({format_percent(funnel['tracked_dids'] / funnel['tracked_cap'])} "
                "of the state cap used)"
            )
        ),
    }


def display_funnel(
    funnel: dict[str, Any] | None,
    newest_listing_rooms: int,
    rooms_total: int,
    room_sampling: dict[str, Any] | None,
    latest_census: tuple[int, str] | None = None,
) -> dict[str, Any] | None:
    if funnel is None:
        return None
    result = dict(funnel)
    coverage = {
        "sampled_rooms": funnel["coverage"]["sampled_rooms"],
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
            "sampled_rooms",
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
    result["coverage"] = coverage

    # Honesty invariant: never present a census under a "latest" promise when
    # a newer completed census exists in the same payload. The unlocked
    # read-modify-write in the collector let the signer state carry a census
    # hours staler than a measured identity_total in the very same ledger.
    superseded = False
    if latest_census is not None:
        latest_total, latest_at = latest_census
        if result["census_completed_at"] is None or parse_ts(latest_at) > parse_ts(
            result["census_completed_at"]
        ):
            superseded = result["well_formed_did_notes"] is not None
            result["well_formed_did_notes"] = latest_total
            result["census_completed_at"] = latest_at
    result["census_superseded"] = superseded

    result["display"] = funnel_display(result)
    return result


def methodology_definitions() -> dict[str, str]:
    return {
        "history": (
            "Forward-collected only. No observation exists before collection_started; "
            "missing intervals are recorded as gaps and are never interpolated."
        ),
        "room_growth": (
            "Numerator: change in /r/events latest sequence. Denominator: elapsed UTC "
            "seconds between two consecutive accepted, gap-free ticks. Endpoint: "
            "/r/events?format=json&limit=200. Deduplication key: event sequence. This is "
            "a forward sample and lower bound when the 200-event window is incomplete; "
            "missing is null, never zero."
        ),
        "lobby_velocity": (
            "Numerator: change in /r/lobby latest sequence. Denominator: elapsed UTC "
            "seconds between two consecutive accepted, gap-free ticks. Endpoint: "
            "/r/lobby?format=json&limit=200. Deduplication key: message sequence. This "
            "is a forward sample; missing is null, never zero."
        ),
        "identity_census": (
            "Numerator: well-formed published DID-note keys across all 256 did-00 "
            "through did-ff shards. Denominator: none for the count; census-to-census "
            "rates divide count change by elapsed UTC seconds. Endpoint: /kv/did-XX. "
            "Deduplication key: published key. This is a complete shard census only "
            "when all shards succeed; incomplete censuses publish no count. It counts "
            "neither people, agents nor users."
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
            "gap-free samples under one unchanged cap; its sample count and window are "
            "published. These are point observations and a trailing measurement, not a "
            "forecast. A cap change starts a new rate window."
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
            "then observed with at least one distinct signed counterparty within 10 "
            "messages and 900 seconds. Deduplication key: shortened did:key value. "
            "Window: persistence_started_at through the displayed tick. Legacy "
            "message-timestamp date fields are not reinterpreted as collection dates; "
            "legacy persistence and all downstream stages are reset to zero. Endpoints: "
            "successful sampled /r/<name>?format=json&limit=200 reads and /kv/did-XX "
            "census shards. Missing room reads add no signers and never mean inactivity. "
            "The newest-room sampling frame biases the result toward new and short-lived "
            "rooms. Funnel bars are drawn as proportions of the observed-signing cohort; "
            "the census count is published beside the funnel and is never a bar "
            "denominator, because the two populations do not contain each other."
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
            "/data.json is the downloadable derived observation ledger. It documents "
            "accepted tick measurements, gaps, hashed sampling manifests and derived "
            "values, not raw message content. The source rotates history away, so raw "
            "bodies available now could not reproduce earlier metrics."
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
            "schema": 4,
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
            "points": [],
            "gaps": [],
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
    # The newest completed census known at each point, from measured
    # identity_total ticks and from funnel-recorded census results alike.
    latest_census: tuple[int, str] | None = None
    # The most recent computed interval rate per headline tile, carried
    # forward with its sample count, window and measurement time so a
    # gap-suppressed interval still shows an honestly stamped value.
    carried_rates: dict[str, dict[str, Any]] = {}

    for index, tick in enumerate(ticks):
        census_candidates: list[tuple[int, str]] = []
        if tick["identity_total"] is not None:
            census_candidates.append((tick["identity_total"], tick["ts"]))
        raw_funnel = tick["signer_funnel"]
        if raw_funnel is not None and raw_funnel["well_formed_did_notes"] is not None:
            census_candidates.append(
                (raw_funnel["well_formed_did_notes"], raw_funnel["census_completed_at"])
            )
        for candidate in census_candidates:
            if latest_census is None or parse_ts(candidate[1]) > parse_ts(latest_census[1]):
                latest_census = candidate

        room_sampling = derived_room_sampling(tick["room_sampling"], coverage_by_frame)
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
            "observed_public_rooms": max(0, tick["events_last_seq"] - first_event_seq),
            "room_sampling": room_sampling,
            "rates": {
                "public_rooms_per_second": empty_rate(),
                "lobby_messages_per_second": empty_rate(),
                "rooms_total_per_second": empty_rate(),
                "notes_per_second": empty_rate(),
                "identities_per_second": empty_rate(),
            },
            "capacity": {
                "notes": measured_capacity(ticks, index, "notes_total", "note_cap", gap_seconds),
                "rooms": measured_capacity(ticks, index, "rooms_total", "room_cap", gap_seconds),
            },
            "signer_funnel": display_funnel(
                tick["signer_funnel"],
                len(tick["newest_rooms"]),
                tick["rooms_total"],
                room_sampling,
                latest_census,
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
                "public_rooms_per_second": tick["events_last_seq"] - previous["events_last_seq"],
                "lobby_messages_per_second": tick["lobby_last_seq"] - previous["lobby_last_seq"],
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

            auto_count = sum(auto_generated_looking(event["base_name"]) for event in new_events)
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

        if tick["identity_total"] is not None:
            if previous_identity is not None:
                identity_elapsed = (
                    tick["_datetime"] - previous_identity["_datetime"]
                ).total_seconds()
                identity_delta = tick["identity_total"] - previous_identity["identity_total"]
                if identity_elapsed > 0 and identity_delta >= 0:
                    point["rates"]["identities_per_second"] = rate(identity_delta, identity_elapsed)
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

    # Stall detection: the rebuild cron runs independently of the collector,
    # so the age of the newest accepted tick at rebuild time is the collector
    # liveness signal. Clamped at zero in case computed_at precedes the tick.
    age_seconds = max(0.0, (parse_ts(computed_at) - ticks[-1]["_datetime"]).total_seconds())
    stalled = age_seconds > STALL_THRESHOLD_SECONDS

    return {
        "schema": 4,
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
        "points": points,
        "gaps": gaps,
        "accepted_ticks": len(points),
        "rejected_ticks": rejected_ticks,
        "methodology": methodology,
    }


def format_int(value: Any) -> str:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        return "—"
    return f"{value:,.0f}"


def format_rate(value: Any) -> str:
    if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(value):
        return "—"
    if abs(value) >= 1:
        return f"{value:.2f}/s"
    return f"{value * 60:.2f}/min"


def format_percent(value: Any) -> str:
    if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(value):
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


def latest_identity(points: list[dict[str, Any]]) -> tuple[int | None, str | None]:
    for point in reversed(points):
        if point.get("identity_total") is not None:
            return point["identity_total"], point["ts"]
    return None, None


def sampling_ssr(sample: Any) -> dict[str, str]:
    if not isinstance(sample, dict):
        unavailable = "Unavailable for this legacy tick"
        return {
            "coverage-frame": unavailable,
            "coverage-sampled": unavailable,
            "coverage-unique": unavailable,
            "coverage-repeats": unavailable,
            "coverage-failures": unavailable,
            "coverage-selector": "No recorded selector manifest",
        }
    return {
        "coverage-frame": f"{format_int(sample.get('frame_size'))} rooms in the recorded frame",
        "coverage-sampled": (f"{format_int(sample.get('sampled_rooms'))} selected this tick"),
        "coverage-unique": (
            f"{format_int(sample.get('cumulative_unique_rooms'))} / "
            f"{format_int(sample.get('frame_size'))} unique room hashes in this frame epoch"
        ),
        "coverage-repeats": (
            f"{format_int(sample.get('repeat_count'))} repeated selections in this frame epoch"
        ),
        "coverage-failures": (
            f"{format_int(sample.get('failed_reads'))} cumulative failures in this frame epoch · "
            f"{format_int(sample.get('failed_reads_this_tick'))} / "
            f"{format_int(sample.get('sampled_rooms'))} selected reads failed this tick"
        ),
        "coverage-selector": (
            f"version {format_int(sample.get('selector_version'))} · "
            f"epoch {format_int(sample.get('epoch'))}"
        ),
    }


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
            values[context_key] = str(entry.get("context", "No engagement display recorded"))
        else:
            values[value_key] = "—"
            values[context_key] = "No engagement display recorded"
    return values


def ssr_values(data: dict[str, Any]) -> dict[str, str]:
    versions = {
        "collector-version": str(data.get("collector_version") or "—"),
        "methodology-version": f"methodology {data.get('methodology_version') or '—'}",
        "computed-at": str(data.get("computed_at") or "—"),
    }
    points = data.get("points")
    if not isinstance(points, list) or not points:
        return {
            "status": "0 collected observations",
            "hero-value": "—",
            "timestamp": "No collection start",
            "room-rate": "—",
            "room-rate-samples": "Needs two gap-free observations",
            "lobby-rate": "—",
            "lobby-rate-samples": "Needs two gap-free observations",
            "identity-total": "—",
            "identity-rate": "No complete census yet",
            "stillborn": "—",
            "stillborn-samples": "No matched new rooms",
            "engagement-ratio": "—",
            "engagement-ratio-context": "No engagement observation yet",
            "engagement-zero": "—",
            "engagement-zero-context": "No engagement observation yet",
            "engagement-nick": "—",
            "engagement-nick-context": "No engagement observation yet",
            "notes-cap-count": "—",
            "notes-headroom": "—",
            "notes-rate": "Observed net-change rate needs at least two gap-free samples under the current cap",
            "rooms-cap-count": "—",
            "rooms-headroom": "—",
            "rooms-rate": "Observed net-change rate needs at least two gap-free samples under the current cap",
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
            "funnel-warning": "Signer sampling has not started. No historical activity is inferred.",
            "funnel-coverage": "No manifest-backed room coverage is available.",
            "tracked-dids": "No signer state recorded",
            "stall-alert": "",
            "collection-phase": "Collecting since",
            "collection-start": "collection start",
            "quality": (
                f" · 0 accepted · {format_int(data.get('rejected_ticks', 0))} rejected "
                "· 0 recorded gaps"
            ),
            "empty-state": (
                "No observations have been collected. The chart remains empty by design."
            ),
            **sampling_ssr(None),
            **versions,
        }

    point = points[-1]
    rates = point.get("rates") or {}
    rate_display = point.get("rate_display") or {}
    unavailable_rate = {"value_text": "—", "context": "Needs two gap-free observations"}
    room_tile = rate_display.get("public_rooms_per_second") or unavailable_rate
    lobby_tile = rate_display.get("lobby_messages_per_second") or unavailable_rate
    census, census_at = latest_identity(points)
    identity_rate = rates.get("identities_per_second") or {}
    stillborn = point.get("stillborn_signal") or {}
    capacity = point.get("capacity") or {}
    notes = capacity.get("notes") or {}
    rooms = capacity.get("rooms") or {}
    funnel = point.get("signer_funnel")
    accepted = data.get("accepted_ticks", len(points))
    rejected = data.get("rejected_ticks", 0)
    gaps = data.get("gaps") if isinstance(data.get("gaps"), list) else []

    if census is None:
        identity_context = "No complete census yet"
    elif identity_rate.get("samples"):
        identity_context = (
            f"{format_rate(identity_rate.get('value'))} · "
            f"{format_int(identity_rate['samples'])} censuses"
        )
    else:
        identity_context = f"well-formed published DID notes · measured {census_at}"

    values = {
        "status": f"{format_int(len(points))} collected observations",
        "hero-value": f"{format_int(point.get('observed_public_rooms'))} observed",
        "timestamp": str(point.get("ts", "invalid timestamp")),
        "room-rate": room_tile["value_text"],
        "room-rate-samples": room_tile["context"],
        "lobby-rate": lobby_tile["value_text"],
        "lobby-rate-samples": lobby_tile["context"],
        "identity-total": format_int(census),
        "identity-rate": identity_context,
        "stillborn": format_percent(stillborn.get("fraction")),
        "stillborn-samples": (
            f"{format_int(stillborn.get('first_message_only'))}/"
            f"{format_int(stillborn.get('matched_new_rooms'))} matched new rooms"
            if stillborn.get("matched_new_rooms")
            else "No matched new rooms"
        ),
        **engagement_ssr(point.get("engagement_display")),
        "notes-cap-count": f"{format_int(notes.get('total'))} / {format_int(notes.get('cap'))}",
        "notes-headroom": (
            f"{format_int(notes.get('headroom'))} headroom "
            f"({format_percent(notes.get('headroom_fraction'))} of "
            f"{format_int(notes.get('cap'))})"
        ),
        "notes-rate": capacity_rate_context(notes),
        "rooms-cap-count": f"{format_int(rooms.get('total'))} / {format_int(rooms.get('cap'))}",
        "rooms-headroom": (
            f"{format_int(rooms.get('headroom'))} headroom "
            f"({format_percent(rooms.get('headroom_fraction'))} of "
            f"{format_int(rooms.get('cap'))})"
        ),
        "rooms-rate": capacity_rate_context(rooms),
        "stall-alert": str(data.get("collection_stall_banner") or ""),
        "collection-phase": str(data.get("collection_phase") or "Collecting since"),
        "collection-start": str(data.get("collection_started")),
        "quality": (
            f" · {format_int(accepted)} accepted · {format_int(rejected)} rejected "
            f"· {format_int(len(gaps))} recorded gaps"
        ),
        "empty-state": "",
        **sampling_ssr(point.get("room_sampling")),
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
    count = len(points) if isinstance(points, list) else 0
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
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":")).replace("<", "\\u003c")
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

    description = description_text(data)
    updated = replace_meta(updated, "name", "description", description)
    updated = replace_meta(updated, "property", "og:description", description)
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
