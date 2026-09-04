import hashlib
import json
from pathlib import Path

import pytest

import collect
from verify_ledger import (
    CHAIN_FIELD,
    CHAIN_VERSION,
    canonical_tick_bytes,
    canonical_tick_hash_bytes,
    verify_ledger,
)

OPERATION_A = "a" * 64
OPERATION_B = "b" * 64


def sidecar_path(path, suffix):
    return path.with_name(path.name + suffix)


def read_sidecar(path):
    return json.loads(path.read_text(encoding="utf-8"))


def write_sidecar(path, value):
    path.write_bytes(
        (
            json.dumps(
                value,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n"
        ).encode("utf-8")
    )


def ledger_records(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def append_unjournaled_tick(path, record):
    previous_payload = path.read_bytes().splitlines()[-1]
    chained = dict(record)
    chained[CHAIN_FIELD] = {
        "version": CHAIN_VERSION,
        "previous_sha256": hashlib.sha256(previous_payload).hexdigest(),
    }
    chained[CHAIN_FIELD]["tick_sha256"] = hashlib.sha256(
        canonical_tick_hash_bytes(chained)
    ).hexdigest()
    with path.open("ab") as ledger:
        ledger.write(canonical_tick_bytes(chained) + b"\n")


def copy_file(source, destination):
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(source.read_bytes())


def test_valid_unjournaled_growth_gets_one_full_verification_before_append(
    tmp_path,
    monkeypatch,
):
    path = tmp_path / "ticks.jsonl"
    collect.append_jsonl(path, {"ts": "2026-08-30T10:00:00Z", "value": "alpha"})
    collect.append_jsonl(path, {"ts": "2026-08-30T10:01:00Z", "value": "bravo"})
    append_unjournaled_tick(
        path,
        {"ts": "2026-08-30T10:02:00Z", "value": "charlie"},
    )
    assert verify_ledger(path)["ok"] is True

    full_verifications = []
    real_verify_ledger = collect.verify_ledger

    def track_full_verification(ledger_path):
        full_verifications.append(ledger_path)
        return real_verify_ledger(ledger_path)

    monkeypatch.setattr(collect, "verify_ledger", track_full_verification)
    collect.append_jsonl(
        path,
        {"ts": "2026-08-30T10:03:00Z", "value": "delta"},
    )

    assert full_verifications == [path]
    result = verify_ledger(path)
    assert result["ok"] is True
    assert result["ticks"] == 4
    checkpoint_path = path.with_name(path.name + ".ledger-checkpoint.json")
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    assert checkpoint["ledger"]["size"] == path.stat().st_size
    assert not path.with_name(path.name + ".ledger-pending.json").exists()


def test_unjournaled_growth_cannot_hide_equal_length_prefix_corruption(tmp_path):
    path = tmp_path / "ticks.jsonl"
    collect.append_jsonl(path, {"ts": "2026-08-30T10:00:00Z", "value": "alpha"})
    collect.append_jsonl(path, {"ts": "2026-08-30T10:01:00Z", "value": "bravo"})
    checkpoint_path = path.with_name(path.name + ".ledger-checkpoint.json")
    checkpoint_before = checkpoint_path.read_bytes()
    assert verify_ledger(path)["ok"] is True
    assert not path.with_name(path.name + ".ledger-pending.json").exists()

    lines = path.read_bytes().splitlines()
    corrupted = lines[0].replace(b'"value":"alpha"', b'"value":"Alpha"', 1)
    assert corrupted != lines[0]
    assert len(corrupted) == len(lines[0])
    lines[0] = corrupted
    path.write_bytes(b"\n".join(lines) + b"\n")
    append_unjournaled_tick(
        path,
        {"ts": "2026-08-30T10:02:00Z", "value": "charlie"},
    )
    ledger_before = path.read_bytes()

    with pytest.raises(collect.CollectionError, match="hash chain is broken at line 1"):
        collect.append_jsonl(
            path,
            {"ts": "2026-08-30T10:03:00Z", "value": "delta"},
        )

    assert path.read_bytes() == ledger_before
    assert checkpoint_path.read_bytes() == checkpoint_before
    assert not path.with_name(path.name + ".ledger-pending.json").exists()
    result = verify_ledger(path)
    assert result["ok"] is False
    assert result["first_break"] == 1


@pytest.mark.parametrize(
    "operation_id",
    (
        None,
        True,
        1,
        "",
        "a" * 63,
        "a" * 65,
        "A" * 64,
        "g" * 64,
        "0x" + "a" * 64,
    ),
)
def test_append_once_rejects_noncanonical_operation_ids(tmp_path, operation_id):
    path = tmp_path / "ticks.jsonl"

    with pytest.raises(collect.CollectionError, match="operation ID"):
        collect.append_jsonl_once(path, {"value": "alpha"}, operation_id)

    assert not path.exists()
    assert not sidecar_path(path, ".ledger-pending.json").exists()
    assert not sidecar_path(path, ".ledger-checkpoint.json").exists()


def test_append_once_retries_from_the_verified_checkpoint_without_duplication(tmp_path):
    path = tmp_path / "ticks.jsonl"
    record = {"ts": "2026-08-30T10:00:00Z", "value": "alpha"}
    expected_digest = hashlib.sha256(canonical_tick_bytes(record)).hexdigest()

    collect.append_jsonl_once(path, record, OPERATION_A)
    ledger_before = path.read_bytes()
    checkpoint_before = sidecar_path(path, ".ledger-checkpoint.json").read_bytes()
    collect.append_jsonl_once(path, record, OPERATION_A)

    assert path.read_bytes() == ledger_before
    assert sidecar_path(path, ".ledger-checkpoint.json").read_bytes() == (
        checkpoint_before
    )
    assert len(ledger_records(path)) == 1
    checkpoint = read_sidecar(sidecar_path(path, ".ledger-checkpoint.json"))
    assert checkpoint["version"] == 2
    assert checkpoint["operation_id"] == OPERATION_A
    assert checkpoint["user_payload_sha256"] == expected_digest


def test_copied_checkpoint_rebinds_before_restored_outbox_exact_operation_retry(
    tmp_path,
):
    source = tmp_path / "source"
    restored = tmp_path / "restored"
    source.mkdir()
    restored.mkdir()
    source_ledger = source / "ticks.jsonl"
    restored_ledger = restored / "ticks.jsonl"
    record = {
        "identity_total": None,
        "identity_census_started": None,
        "value": "alpha",
    }
    payload = canonical_tick_bytes(record)
    payload_sha256 = hashlib.sha256(payload).hexdigest()
    collect.append_jsonl_once(source_ledger, record, OPERATION_A)

    source_state = source / "signers.json"
    source_database = collect.signer_database_path(source_state)
    connection = collect.connect_signer_database(source_database)
    try:
        connection.execute(
            """
            INSERT INTO tick_outbox (
                singleton,
                operation_id,
                payload,
                payload_sha256,
                census_started_at,
                census_total
            )
            VALUES (1, ?, ?, ?, NULL, NULL)
            """,
            (OPERATION_A, payload, payload_sha256),
        )
        connection.commit()
    finally:
        connection.close()

    copy_file(source_ledger, restored_ledger)
    restored_checkpoint = sidecar_path(
        restored_ledger,
        ".ledger-checkpoint.json",
    )
    copy_file(
        sidecar_path(source_ledger, ".ledger-checkpoint.json"),
        restored_checkpoint,
    )
    restored_state = restored / "signers.json"
    restored_database = collect.signer_database_path(restored_state)
    copy_file(source_database, restored_database)
    checkpoint_before = read_sidecar(restored_checkpoint)
    restored_stat = restored_ledger.stat()
    assert (
        checkpoint_before["ledger"]["device"],
        checkpoint_before["ledger"]["inode"],
    ) != (restored_stat.st_dev, restored_stat.st_ino)
    ledger_before = restored_ledger.read_bytes()

    assert (
        collect.drain_tick_outbox(
            restored_ledger,
            restored_state,
            restored / "identity-census-state.json",
        )
        is True
    )

    assert restored_ledger.read_bytes() == ledger_before
    assert len(ledger_records(restored_ledger)) == 1
    checkpoint = read_sidecar(restored_checkpoint)
    rebound_stat = restored_ledger.stat()
    assert checkpoint["ledger"]["device"] == rebound_stat.st_dev
    assert checkpoint["ledger"]["inode"] == rebound_stat.st_ino
    assert checkpoint["operation_id"] == OPERATION_A
    connection = collect.connect_signer_database(restored_database)
    try:
        assert collect.load_tick_outbox(connection) is None
    finally:
        connection.close()


def test_copied_checkpoint_rebind_refuses_a_corrupted_pre_tip_record(tmp_path):
    source = tmp_path / "source" / "ticks.jsonl"
    restored = tmp_path / "restored" / "ticks.jsonl"
    source.parent.mkdir()
    first_record = {"value": "alpha"}
    tip_record = {"value": "bravo"}
    collect.append_jsonl_once(source, first_record, OPERATION_A)
    collect.append_jsonl_once(source, tip_record, OPERATION_B)
    copy_file(source, restored)
    restored_checkpoint = sidecar_path(restored, ".ledger-checkpoint.json")
    copy_file(
        sidecar_path(source, ".ledger-checkpoint.json"),
        restored_checkpoint,
    )
    lines = restored.read_bytes().splitlines()
    lines[0] = lines[0].replace(b'"value":"alpha"', b'"value":"Alpha"', 1)
    restored.write_bytes(b"\n".join(lines) + b"\n")
    ledger_before = restored.read_bytes()
    checkpoint_before = restored_checkpoint.read_bytes()

    with pytest.raises(collect.CollectionError, match="hash chain is broken at line 1"):
        collect.append_jsonl_once(restored, tip_record, OPERATION_B)

    assert restored.read_bytes() == ledger_before
    assert restored_checkpoint.read_bytes() == checkpoint_before
    assert not sidecar_path(restored, ".ledger-pending.json").exists()


@pytest.mark.parametrize(
    "prefix_kind",
    ("none", "first-byte", "middle", "last-byte-missing", "complete"),
)
def test_copied_pending_family_rebinds_after_full_base_verification(
    tmp_path,
    monkeypatch,
    prefix_kind,
):
    source = tmp_path / "source" / "ticks.jsonl"
    restored = tmp_path / "restored" / "ticks.jsonl"
    source.parent.mkdir()
    collect.append_jsonl_once(source, {"value": "base"}, OPERATION_A)
    pending_record = {"value": "pending"}
    real_complete = collect._complete_pending_append

    def crash_before_ledger(*args):
        raise OSError("simulated crash before ledger append")

    monkeypatch.setattr(collect, "_complete_pending_append", crash_before_ledger)
    with pytest.raises(OSError, match="simulated crash"):
        collect.append_jsonl_once(source, pending_record, OPERATION_B)
    monkeypatch.setattr(collect, "_complete_pending_append", real_complete)

    copy_file(source, restored)
    for suffix in (".ledger-checkpoint.json", ".ledger-pending.json"):
        copy_file(sidecar_path(source, suffix), sidecar_path(restored, suffix))
    pending = read_sidecar(sidecar_path(restored, ".ledger-pending.json"))
    pending_payload = canonical_tick_bytes(pending["record"]) + b"\n"
    prefix_lengths = {
        "none": 0,
        "first-byte": 1,
        "middle": len(pending_payload) // 2,
        "last-byte-missing": len(pending_payload) - 1,
        "complete": len(pending_payload),
    }
    with restored.open("ab") as ledger:
        ledger.write(pending_payload[: prefix_lengths[prefix_kind]])

    collect.append_jsonl_once(restored, pending_record, OPERATION_B)

    assert [item["value"] for item in ledger_records(restored)] == [
        "base",
        "pending",
    ]
    assert verify_ledger(restored)["ok"] is True
    assert not sidecar_path(restored, ".ledger-pending.json").exists()
    checkpoint = read_sidecar(sidecar_path(restored, ".ledger-checkpoint.json"))
    restored_stat = restored.stat()
    assert checkpoint["ledger"]["device"] == restored_stat.st_dev
    assert checkpoint["ledger"]["inode"] == restored_stat.st_ino
    assert checkpoint["operation_id"] == OPERATION_B


def test_copied_pending_recovery_refuses_a_corrupted_base_prefix(
    tmp_path,
    monkeypatch,
):
    source = tmp_path / "source" / "ticks.jsonl"
    restored = tmp_path / "restored" / "ticks.jsonl"
    source.parent.mkdir()
    collect.append_jsonl_once(source, {"value": "x" * 7_000}, OPERATION_A)
    real_complete = collect._complete_pending_append

    def crash_before_ledger(*args):
        raise OSError("simulated crash before ledger append")

    monkeypatch.setattr(collect, "_complete_pending_append", crash_before_ledger)
    with pytest.raises(OSError, match="simulated crash"):
        collect.append_jsonl_once(source, {"value": "pending"}, OPERATION_B)
    monkeypatch.setattr(collect, "_complete_pending_append", real_complete)

    copy_file(source, restored)
    for suffix in (".ledger-checkpoint.json", ".ledger-pending.json"):
        copy_file(sidecar_path(source, suffix), sidecar_path(restored, suffix))
    ledger = bytearray(restored.read_bytes())
    assert len(ledger) - 143 > collect.LEDGER_TAIL_ANCHOR_BYTES
    assert ledger[143] == ord("x")
    ledger[143] = ord("y")
    restored.write_bytes(ledger)
    checkpoint_path = sidecar_path(restored, ".ledger-checkpoint.json")
    pending_path = sidecar_path(restored, ".ledger-pending.json")
    ledger_before = restored.read_bytes()
    checkpoint_before = checkpoint_path.read_bytes()
    pending_before = pending_path.read_bytes()

    with pytest.raises(collect.CollectionError, match="hash chain is broken at line 1"):
        collect.recover_pending_jsonl(restored)

    assert restored.read_bytes() == ledger_before
    assert checkpoint_path.read_bytes() == checkpoint_before
    assert pending_path.read_bytes() == pending_before


def test_append_once_refuses_to_reuse_an_operation_id_for_another_payload(tmp_path):
    path = tmp_path / "ticks.jsonl"
    collect.append_jsonl_once(path, {"value": "alpha"}, OPERATION_A)
    ledger_before = path.read_bytes()
    checkpoint_path = sidecar_path(path, ".ledger-checkpoint.json")
    checkpoint_before = checkpoint_path.read_bytes()

    with pytest.raises(collect.CollectionError, match="different user payload"):
        collect.append_jsonl_once(path, {"value": "bravo"}, OPERATION_A)

    assert path.read_bytes() == ledger_before
    assert checkpoint_path.read_bytes() == checkpoint_before
    assert not sidecar_path(path, ".ledger-pending.json").exists()


def test_mismatched_copied_checkpoint_fails_closed(tmp_path):
    first = tmp_path / "first.jsonl"
    second = tmp_path / "second.jsonl"
    collect.append_jsonl_once(first, {"value": "alpha"}, OPERATION_A)
    collect.append_jsonl_once(second, {"value": "bravo"}, OPERATION_B)
    second_checkpoint = sidecar_path(second, ".ledger-checkpoint.json")
    copy_file(sidecar_path(first, ".ledger-checkpoint.json"), second_checkpoint)
    ledger_before = second.read_bytes()
    checkpoint_before = second_checkpoint.read_bytes()

    with pytest.raises(collect.CollectionError):
        collect.append_jsonl_once(second, {"value": "alpha"}, OPERATION_A)

    assert second.read_bytes() == ledger_before
    assert second_checkpoint.read_bytes() == checkpoint_before
    assert not sidecar_path(second, ".ledger-pending.json").exists()
    assert verify_ledger(second)["ok"] is True


def test_checkpoint_without_ledger_fails_closed(tmp_path):
    path = tmp_path / "ticks.jsonl"
    checkpoint_path = sidecar_path(path, ".ledger-checkpoint.json")
    collect.append_jsonl_once(path, {"value": "old"}, OPERATION_A)
    checkpoint_before = checkpoint_path.read_bytes()
    path.unlink()

    with pytest.raises(collect.CollectionError, match="without its ledger"):
        collect.append_jsonl_once(path, {"value": "new"}, OPERATION_B)

    assert not path.exists()
    assert checkpoint_path.read_bytes() == checkpoint_before
    assert not sidecar_path(path, ".ledger-pending.json").exists()


@pytest.mark.parametrize("ledger_prefix", ("absent", "empty", "partial"))
def test_existing_checkpoint_cannot_be_replaced_by_a_fresh_pending_genesis(
    tmp_path,
    monkeypatch,
    ledger_prefix,
):
    path = tmp_path / "ticks.jsonl"
    checkpoint_path = sidecar_path(path, ".ledger-checkpoint.json")
    pending_path = sidecar_path(path, ".ledger-pending.json")
    collect.append_jsonl(path, {"value": "anchored-history"})

    fresh_path = tmp_path / "fresh.jsonl"
    fresh_pending_path = sidecar_path(fresh_path, ".ledger-pending.json")
    real_complete = collect._complete_pending_append

    def crash_before_ledger(*args):
        raise OSError("simulated crash before fresh genesis")

    monkeypatch.setattr(collect, "_complete_pending_append", crash_before_ledger)
    with pytest.raises(OSError, match="fresh genesis"):
        collect.append_jsonl(fresh_path, {"value": "replacement"})
    monkeypatch.setattr(collect, "_complete_pending_append", real_complete)

    copy_file(fresh_pending_path, pending_path)
    path.unlink()
    if ledger_prefix == "empty":
        path.write_bytes(b"")
    elif ledger_prefix == "partial":
        pending = read_sidecar(pending_path)
        payload = canonical_tick_bytes(pending["record"]) + b"\n"
        path.write_bytes(payload[:1])
    ledger_before = path.read_bytes() if path.exists() else None
    checkpoint_before = checkpoint_path.read_bytes()
    pending_before = pending_path.read_bytes()

    with pytest.raises(collect.CollectionError, match="checkpoint"):
        collect.recover_pending_jsonl(path)

    if ledger_before is None:
        assert not path.exists()
    else:
        assert path.read_bytes() == ledger_before
    assert checkpoint_path.read_bytes() == checkpoint_before
    assert pending_path.read_bytes() == pending_before


def test_checkpoint_rejects_valid_prefix_rollback(tmp_path):
    path = tmp_path / "ticks.jsonl"
    checkpoint_path = sidecar_path(path, ".ledger-checkpoint.json")
    collect.append_jsonl_once(path, {"value": "one"}, OPERATION_A)
    first_record = path.read_bytes()
    collect.append_jsonl_once(path, {"value": "two"}, OPERATION_B)
    checkpoint_before = checkpoint_path.read_bytes()
    path.write_bytes(first_record)
    ledger_before = path.read_bytes()
    assert verify_ledger(path)["ok"] is True

    with pytest.raises(collect.CollectionError, match="truncated"):
        collect.append_jsonl_once(path, {"value": "three"}, "c" * 64)

    assert path.read_bytes() == ledger_before
    assert checkpoint_path.read_bytes() == checkpoint_before
    assert not sidecar_path(path, ".ledger-pending.json").exists()


def test_partial_pending_recovery_fully_verifies_the_base_prefix(
    tmp_path,
    monkeypatch,
):
    path = tmp_path / "ticks.jsonl"
    checkpoint_path = sidecar_path(path, ".ledger-checkpoint.json")
    pending_path = sidecar_path(path, ".ledger-pending.json")
    collect.append_jsonl_once(path, {"value": "x" * 7_000}, OPERATION_A)
    pending_record = {"value": "pending"}
    real_complete = collect._complete_pending_append

    def crash_before_ledger(*args):
        raise OSError("simulated crash before ledger append")

    monkeypatch.setattr(collect, "_complete_pending_append", crash_before_ledger)
    with pytest.raises(OSError, match="simulated crash"):
        collect.append_jsonl_once(path, pending_record, OPERATION_B)
    monkeypatch.setattr(collect, "_complete_pending_append", real_complete)

    ledger = bytearray(path.read_bytes())
    assert len(ledger) - 143 > collect.LEDGER_TAIL_ANCHOR_BYTES
    assert ledger[143] == ord("x")
    ledger[143] = ord("y")
    pending = read_sidecar(pending_path)
    pending_payload = canonical_tick_bytes(pending["record"]) + b"\n"
    path.write_bytes(bytes(ledger) + pending_payload[:1])
    ledger_before = path.read_bytes()
    checkpoint_before = checkpoint_path.read_bytes()
    pending_before = pending_path.read_bytes()

    with pytest.raises(collect.CollectionError, match="hash chain is broken"):
        collect.append_jsonl_once(path, pending_record, OPERATION_B)

    assert path.read_bytes() == ledger_before
    assert checkpoint_path.read_bytes() == checkpoint_before
    assert pending_path.read_bytes() == pending_before
    assert verify_ledger(path)["ok"] is False


def test_different_operation_ids_append_even_when_the_payload_is_identical(tmp_path):
    path = tmp_path / "ticks.jsonl"
    record = {"value": "alpha"}

    collect.append_jsonl_once(path, record, OPERATION_A)
    collect.append_jsonl_once(path, record, OPERATION_B)

    assert [item["value"] for item in ledger_records(path)] == ["alpha", "alpha"]
    assert verify_ledger(path)["ok"] is True
    checkpoint = read_sidecar(sidecar_path(path, ".ledger-checkpoint.json"))
    assert checkpoint["operation_id"] == OPERATION_B


def test_ordinary_append_keeps_non_idempotent_checkpoint_behavior(tmp_path):
    path = tmp_path / "ticks.jsonl"
    record = {"value": "alpha"}

    collect.append_jsonl(path, record)
    collect.append_jsonl(path, record)

    assert [item["value"] for item in ledger_records(path)] == ["alpha", "alpha"]
    checkpoint = read_sidecar(sidecar_path(path, ".ledger-checkpoint.json"))
    assert checkpoint["version"] == 2
    assert checkpoint["operation_id"] is None
    assert (
        checkpoint["user_payload_sha256"]
        == hashlib.sha256(canonical_tick_bytes(record)).hexdigest()
    )


def test_same_operation_with_a_different_pending_payload_fails_without_recovery(
    tmp_path,
    monkeypatch,
):
    path = tmp_path / "ticks.jsonl"
    pending_path = sidecar_path(path, ".ledger-pending.json")
    real_complete = collect._complete_pending_append

    def crash_before_ledger(*args):
        raise OSError("simulated crash before ledger append")

    monkeypatch.setattr(collect, "_complete_pending_append", crash_before_ledger)
    with pytest.raises(OSError, match="simulated crash"):
        collect.append_jsonl_once(path, {"value": "alpha"}, OPERATION_A)
    monkeypatch.setattr(collect, "_complete_pending_append", real_complete)
    pending_before = pending_path.read_bytes()

    with pytest.raises(collect.CollectionError, match="different user payload"):
        collect.append_jsonl_once(path, {"value": "bravo"}, OPERATION_A)

    assert not path.exists()
    assert pending_path.read_bytes() == pending_before
    assert not sidecar_path(path, ".ledger-checkpoint.json").exists()


@pytest.mark.parametrize(
    "prefix_kind",
    ("none", "first-byte", "middle", "last-byte-missing", "complete"),
)
def test_append_once_recovers_every_pending_ledger_prefix(
    tmp_path,
    monkeypatch,
    prefix_kind,
):
    path = tmp_path / "ticks.jsonl"
    pending_path = sidecar_path(path, ".ledger-pending.json")
    record = {"ts": "2026-08-30T10:00:00Z", "value": "alpha"}
    real_complete = collect._complete_pending_append

    def crash_before_ledger(*args):
        raise OSError("simulated crash before ledger append")

    monkeypatch.setattr(collect, "_complete_pending_append", crash_before_ledger)
    with pytest.raises(OSError, match="simulated crash"):
        collect.append_jsonl_once(path, record, OPERATION_A)
    monkeypatch.setattr(collect, "_complete_pending_append", real_complete)

    pending = read_sidecar(pending_path)
    assert pending["version"] == 2
    assert pending["operation_id"] == OPERATION_A
    assert (
        pending["user_payload_sha256"]
        == hashlib.sha256(canonical_tick_bytes(record)).hexdigest()
    )
    pending_payload = canonical_tick_bytes(pending["record"]) + b"\n"
    prefix_lengths = {
        "none": 0,
        "first-byte": 1,
        "middle": len(pending_payload) // 2,
        "last-byte-missing": len(pending_payload) - 1,
        "complete": len(pending_payload),
    }
    prefix_length = prefix_lengths[prefix_kind]
    if prefix_length:
        path.write_bytes(pending_payload[:prefix_length])

    collect.append_jsonl_once(path, record, OPERATION_A)

    assert len(ledger_records(path)) == 1
    assert verify_ledger(path)["ok"] is True
    assert not pending_path.exists()
    checkpoint = read_sidecar(sidecar_path(path, ".ledger-checkpoint.json"))
    assert checkpoint["operation_id"] == OPERATION_A


@pytest.mark.parametrize("pending_version", ("current", "legacy"))
@pytest.mark.parametrize(
    "prefix_kind",
    ("none", "first-byte", "middle", "last-byte-missing", "complete"),
)
def test_recover_pending_jsonl_without_an_outbox_recovers_every_prefix(
    tmp_path,
    monkeypatch,
    pending_version,
    prefix_kind,
):
    path = tmp_path / "ticks.jsonl"
    pending_path = sidecar_path(path, ".ledger-pending.json")
    checkpoint_path = sidecar_path(path, ".ledger-checkpoint.json")
    record = {"value": "pending-without-outbox"}
    real_complete = collect._complete_pending_append

    def crash_before_ledger(*args):
        raise OSError("simulated crash before ledger append")

    monkeypatch.setattr(collect, "_complete_pending_append", crash_before_ledger)
    with pytest.raises(OSError, match="simulated crash"):
        collect.append_jsonl(path, record)
    monkeypatch.setattr(collect, "_complete_pending_append", real_complete)

    pending = read_sidecar(pending_path)
    if pending_version == "legacy":
        pending = {
            "version": 1,
            "base": pending["base"],
            "record": pending["record"],
        }
        write_sidecar(pending_path, pending)
    pending_payload = canonical_tick_bytes(pending["record"]) + b"\n"
    prefix_lengths = {
        "none": 0,
        "first-byte": 1,
        "middle": len(pending_payload) // 2,
        "last-byte-missing": len(pending_payload) - 1,
        "complete": len(pending_payload),
    }
    prefix_length = prefix_lengths[prefix_kind]
    if prefix_length:
        path.write_bytes(pending_payload[:prefix_length])

    assert collect.recover_pending_jsonl(path) is True
    assert collect.recover_pending_jsonl(path) is False

    assert [item["value"] for item in ledger_records(path)] == [record["value"]]
    assert verify_ledger(path)["ok"] is True
    assert not pending_path.exists()
    checkpoint = read_sidecar(checkpoint_path)
    assert checkpoint["version"] == 2
    assert checkpoint["operation_id"] is None


def test_unjournaled_growth_rebinds_the_checkpoint_to_the_verified_tip(
    tmp_path,
    monkeypatch,
):
    path = tmp_path / "ticks.jsonl"
    checkpoint_path = sidecar_path(path, ".ledger-checkpoint.json")
    collect.append_jsonl(path, {"ts": "2026-08-30T10:00:00Z", "value": "alpha"})
    append_unjournaled_tick(
        path,
        {"ts": "2026-08-30T10:01:00Z", "value": "bravo"},
    )

    full_verifications = []
    real_verify_ledger = collect.verify_ledger

    def track_full_verification(ledger_path):
        full_verifications.append(ledger_path)
        return real_verify_ledger(ledger_path)

    monkeypatch.setattr(collect, "verify_ledger", track_full_verification)
    verified = collect._ledger_tip_for_append(path, checkpoint_path)
    checkpoint = read_sidecar(checkpoint_path)
    revisited = collect._ledger_tip_for_append(path, checkpoint_path)

    assert full_verifications == [path]
    assert checkpoint["ledger"]["size"] == path.stat().st_size
    assert checkpoint["tip"]["canonical_sha256"] == verified[0]
    assert checkpoint["operation_id"] is None
    assert (revisited[0], revisited[1]) == (verified[0], verified[1])


def test_quiet_recovery_rejects_a_malformed_checkpoint(tmp_path):
    path = tmp_path / "ticks.jsonl"
    checkpoint_path = sidecar_path(path, ".ledger-checkpoint.json")
    collect.append_jsonl(path, {"value": "anchored"})
    ledger_before = path.read_bytes()
    checkpoint_path.write_text("{}\n", encoding="utf-8")
    checkpoint_before = checkpoint_path.read_bytes()

    with pytest.raises(collect.CollectionError, match="checkpoint"):
        collect.recover_pending_jsonl(path)

    assert path.read_bytes() == ledger_before
    assert checkpoint_path.read_bytes() == checkpoint_before
    assert not sidecar_path(path, ".ledger-pending.json").exists()


def test_quiet_recovery_rebinds_an_exact_copied_checkpoint(tmp_path):
    source = tmp_path / "source" / "ticks.jsonl"
    restored = tmp_path / "restored" / "ticks.jsonl"
    source.parent.mkdir()
    collect.append_jsonl_once(source, {"value": "anchored"}, OPERATION_A)
    copy_file(source, restored)
    restored_checkpoint = sidecar_path(restored, ".ledger-checkpoint.json")
    copy_file(
        sidecar_path(source, ".ledger-checkpoint.json"),
        restored_checkpoint,
    )

    assert collect.recover_pending_jsonl(restored) is False

    checkpoint = read_sidecar(restored_checkpoint)
    restored_stat = restored.stat()
    assert checkpoint["ledger"]["device"] == restored_stat.st_dev
    assert checkpoint["ledger"]["inode"] == restored_stat.st_ino
    assert checkpoint["operation_id"] == OPERATION_A


def test_quiet_recovery_accepts_a_verified_legacy_ledger_without_checkpoint(
    tmp_path,
):
    path = tmp_path / "ticks.jsonl"
    checkpoint_path = sidecar_path(path, ".ledger-checkpoint.json")
    collect.append_jsonl(path, {"value": "legacy"})
    checkpoint_path.unlink()

    assert collect.recover_pending_jsonl(path) is False
    assert verify_ledger(path)["ok"] is True
    assert not checkpoint_path.exists()


@pytest.mark.parametrize(
    "crash_point",
    ("after-ledger", "after-checkpoint", "after-pending-unlink"),
)
def test_append_once_retry_is_idempotent_across_commit_crash_points(
    tmp_path,
    monkeypatch,
    crash_point,
):
    path = tmp_path / "ticks.jsonl"
    pending_path = sidecar_path(path, ".ledger-pending.json")
    checkpoint_path = sidecar_path(path, ".ledger-checkpoint.json")
    record = {"ts": "2026-08-30T10:00:00Z", "value": "alpha"}

    if crash_point == "after-ledger":
        real_save_checkpoint = collect._save_ledger_checkpoint

        def crash_after_ledger(*args):
            raise OSError("simulated crash after ledger append")

        monkeypatch.setattr(collect, "_save_ledger_checkpoint", crash_after_ledger)
    elif crash_point == "after-checkpoint":
        real_unlink = Path.unlink

        def crash_after_checkpoint(target, *args, **kwargs):
            if target == pending_path:
                raise OSError("simulated crash after checkpoint")
            return real_unlink(target, *args, **kwargs)

        monkeypatch.setattr(Path, "unlink", crash_after_checkpoint)
    else:
        real_fsync_directory = collect._fsync_directory

        def crash_after_pending_unlink(directory):
            if checkpoint_path.exists() and not pending_path.exists():
                raise OSError("simulated crash after pending unlink")
            return real_fsync_directory(directory)

        monkeypatch.setattr(collect, "_fsync_directory", crash_after_pending_unlink)

    with pytest.raises(OSError, match="simulated crash"):
        collect.append_jsonl_once(path, record, OPERATION_A)

    if crash_point == "after-ledger":
        monkeypatch.setattr(
            collect,
            "_save_ledger_checkpoint",
            real_save_checkpoint,
        )
    elif crash_point == "after-checkpoint":
        monkeypatch.setattr(Path, "unlink", real_unlink)
    else:
        monkeypatch.setattr(collect, "_fsync_directory", real_fsync_directory)

    assert len(ledger_records(path)) == 1
    ledger_before_retry = path.read_bytes()
    collect.append_jsonl_once(path, record, OPERATION_A)

    assert path.read_bytes() == ledger_before_retry
    assert len(ledger_records(path)) == 1
    assert verify_ledger(path)["ok"] is True
    assert not pending_path.exists()
    checkpoint = read_sidecar(checkpoint_path)
    assert checkpoint["operation_id"] == OPERATION_A


def test_legacy_checkpoint_is_accepted_and_upgraded_on_append(tmp_path):
    path = tmp_path / "ticks.jsonl"
    checkpoint_path = sidecar_path(path, ".ledger-checkpoint.json")
    collect.append_jsonl(path, {"value": "legacy-tip"})
    current = read_sidecar(checkpoint_path)
    legacy = {
        "version": 1,
        "ledger": current["ledger"],
        "tip": current["tip"],
    }
    write_sidecar(checkpoint_path, legacy)

    collect.append_jsonl(path, {"value": "current-tip"})

    assert [item["value"] for item in ledger_records(path)] == [
        "legacy-tip",
        "current-tip",
    ]
    upgraded = read_sidecar(checkpoint_path)
    assert upgraded["version"] == 2
    assert upgraded["operation_id"] is None
    assert (
        upgraded["user_payload_sha256"]
        == hashlib.sha256(canonical_tick_bytes({"value": "current-tip"})).hexdigest()
    )


def test_append_once_does_not_infer_an_operation_from_a_legacy_checkpoint(tmp_path):
    path = tmp_path / "ticks.jsonl"
    checkpoint_path = sidecar_path(path, ".ledger-checkpoint.json")
    record = {"value": "same-payload"}
    collect.append_jsonl(path, record)
    current = read_sidecar(checkpoint_path)
    write_sidecar(
        checkpoint_path,
        {
            "version": 1,
            "ledger": current["ledger"],
            "tip": current["tip"],
        },
    )

    collect.append_jsonl_once(path, record, OPERATION_A)
    collect.append_jsonl_once(path, record, OPERATION_A)

    assert len(ledger_records(path)) == 2
    assert verify_ledger(path)["ok"] is True
    upgraded = read_sidecar(checkpoint_path)
    assert upgraded["version"] == 2
    assert upgraded["operation_id"] == OPERATION_A


def test_legacy_pending_journal_is_recovered_and_upgraded(tmp_path, monkeypatch):
    path = tmp_path / "ticks.jsonl"
    pending_path = sidecar_path(path, ".ledger-pending.json")
    checkpoint_path = sidecar_path(path, ".ledger-checkpoint.json")
    record = {"value": "legacy-pending"}
    real_complete = collect._complete_pending_append

    def crash_before_ledger(*args):
        raise OSError("simulated crash before ledger append")

    monkeypatch.setattr(collect, "_complete_pending_append", crash_before_ledger)
    with pytest.raises(OSError, match="simulated crash"):
        collect.append_jsonl(path, record)
    monkeypatch.setattr(collect, "_complete_pending_append", real_complete)
    current = read_sidecar(pending_path)
    legacy = {
        "version": 1,
        "base": current["base"],
        "record": current["record"],
    }
    write_sidecar(pending_path, legacy)

    collect.append_jsonl(path, record)

    assert len(ledger_records(path)) == 1
    assert not pending_path.exists()
    upgraded = read_sidecar(checkpoint_path)
    assert upgraded["version"] == 2
    assert upgraded["operation_id"] is None
    assert (
        upgraded["user_payload_sha256"]
        == hashlib.sha256(canonical_tick_bytes(record)).hexdigest()
    )


def test_append_once_preserves_an_unknown_legacy_pending_operation(
    tmp_path,
    monkeypatch,
):
    path = tmp_path / "ticks.jsonl"
    pending_path = sidecar_path(path, ".ledger-pending.json")
    checkpoint_path = sidecar_path(path, ".ledger-checkpoint.json")
    record = {"value": "same-payload"}
    real_complete = collect._complete_pending_append

    def crash_before_ledger(*args):
        raise OSError("simulated crash before ledger append")

    monkeypatch.setattr(collect, "_complete_pending_append", crash_before_ledger)
    with pytest.raises(OSError, match="simulated crash"):
        collect.append_jsonl(path, record)
    monkeypatch.setattr(collect, "_complete_pending_append", real_complete)
    current = read_sidecar(pending_path)
    write_sidecar(
        pending_path,
        {
            "version": 1,
            "base": current["base"],
            "record": current["record"],
        },
    )

    collect.append_jsonl_once(path, record, OPERATION_A)
    collect.append_jsonl_once(path, record, OPERATION_A)

    assert len(ledger_records(path)) == 2
    assert verify_ledger(path)["ok"] is True
    assert not pending_path.exists()
    upgraded = read_sidecar(checkpoint_path)
    assert upgraded["version"] == 2
    assert upgraded["operation_id"] == OPERATION_A
