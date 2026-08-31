import sys

import pytest

import recover_publication


def command_args(output, signer_state, census_state):
    return [
        "recover_publication.py",
        "--output",
        str(output),
        "--signer-state",
        str(signer_state),
        "--census-state",
        str(census_state),
    ]


@pytest.mark.parametrize(
    ("drained", "message"),
    (
        (True, "pending publication drained"),
        (False, "no pending publication"),
    ),
)
def test_main_drains_authoritative_outbox_and_verifies_ledger(
    tmp_path,
    monkeypatch,
    capsys,
    drained,
    message,
):
    output = tmp_path / "ticks.jsonl"
    signer_state = tmp_path / "signers.json"
    census_state = tmp_path / "identity-census-state.json"
    output.write_bytes(b"")
    events = []

    def verify(path):
        events.append(("verify", path))
        return {
            "ok": True,
            "ticks": 3,
            "first_break": None,
            "message": "",
        }

    def drain(output_path, signer_state_path, census_state_path):
        events.append(("drain", output_path, signer_state_path, census_state_path))
        return drained

    def recover(path):
        events.append(("recover", path))
        return False

    monkeypatch.setattr(sys, "argv", command_args(output, signer_state, census_state))
    monkeypatch.setattr(recover_publication, "verify_ledger", verify)
    monkeypatch.setattr(recover_publication, "drain_tick_outbox", drain)
    monkeypatch.setattr(recover_publication, "recover_pending_jsonl", recover)

    assert recover_publication.main() == 0
    assert events == [
        ("drain", output, signer_state, census_state),
        ("recover", output),
        ("verify", output),
    ]
    captured = capsys.readouterr()
    assert message in captured.out
    assert "ledger verified (3 ticks)" in captured.out
    assert captured.err == ""


def test_main_fails_closed_when_outbox_drain_fails(tmp_path, monkeypatch, capsys):
    output = tmp_path / "ticks.jsonl"
    signer_state = tmp_path / "signers.json"
    census_state = tmp_path / "identity-census-state.json"
    verifications = []

    def verify(path):
        verifications.append(path)
        return {
            "ok": True,
            "ticks": 0,
            "first_break": None,
            "message": "",
        }

    def fail_drain(*args):
        raise recover_publication.CollectionError("injected drain failure")

    monkeypatch.setattr(sys, "argv", command_args(output, signer_state, census_state))
    monkeypatch.setattr(recover_publication, "verify_ledger", verify)
    monkeypatch.setattr(recover_publication, "drain_tick_outbox", fail_drain)

    assert recover_publication.main() == 1
    assert verifications == []
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "publication recovery failed: injected drain failure" in captured.err


@pytest.mark.parametrize("ledger_kind", ("missing", "directory"))
def test_main_requires_an_existing_regular_tick_ledger(
    tmp_path,
    monkeypatch,
    capsys,
    ledger_kind,
):
    output = tmp_path / "ticks.jsonl"
    signer_state = tmp_path / "signers.json"
    census_state = tmp_path / "identity-census-state.json"
    if ledger_kind == "directory":
        output.mkdir()

    monkeypatch.setattr(sys, "argv", command_args(output, signer_state, census_state))
    monkeypatch.setattr(
        recover_publication,
        "drain_tick_outbox",
        lambda *args: False,
    )
    monkeypatch.setattr(
        recover_publication,
        "recover_pending_jsonl",
        lambda *args: False,
    )
    monkeypatch.setattr(
        recover_publication,
        "verify_ledger",
        lambda *args: pytest.fail("invalid ledger reached verification"),
    )

    assert recover_publication.main() == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "existing regular tick ledger" in captured.err
    assert not signer_state.exists()
    assert not census_state.exists()


@pytest.mark.parametrize("content", (b"", b"\n \n"), ids=("empty", "blank-only"))
def test_main_rejects_a_zero_tick_ledger_without_recoverable_state(
    tmp_path,
    monkeypatch,
    capsys,
    content,
):
    output = tmp_path / "ticks.jsonl"
    signer_state = tmp_path / "signers.json"
    census_state = tmp_path / "identity-census-state.json"
    output.write_bytes(content)

    monkeypatch.setattr(sys, "argv", command_args(output, signer_state, census_state))
    monkeypatch.setattr(recover_publication, "drain_tick_outbox", lambda *args: False)

    assert recover_publication.main() == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "ledger contains no ticks" in captured.err


@pytest.mark.parametrize("recovery_kind", ("outbox", "pending"))
def test_main_allows_first_ledger_to_be_created_by_recovery(
    tmp_path,
    monkeypatch,
    capsys,
    recovery_kind,
):
    output = tmp_path / "ticks.jsonl"
    signer_state = tmp_path / "signers.json"
    census_state = tmp_path / "identity-census-state.json"

    def drain(*args):
        if recovery_kind == "outbox":
            output.write_text("genesis\n", encoding="utf-8")
            return True
        return False

    def recover(*args):
        if recovery_kind == "pending":
            output.write_text("genesis\n", encoding="utf-8")
            return True
        return False

    monkeypatch.setattr(sys, "argv", command_args(output, signer_state, census_state))
    monkeypatch.setattr(recover_publication, "drain_tick_outbox", drain)
    monkeypatch.setattr(recover_publication, "recover_pending_jsonl", recover)
    monkeypatch.setattr(
        recover_publication,
        "verify_ledger",
        lambda path: {
            "ok": True,
            "ticks": 1,
            "first_break": None,
            "message": "",
        },
    )

    assert recover_publication.main() == 0
    assert output.is_file()
    captured = capsys.readouterr()
    assert "ledger verified (1 ticks)" in captured.out
    assert captured.err == ""


def test_main_reports_an_invalid_ledger_when_no_outbox_is_pending(
    tmp_path,
    monkeypatch,
    capsys,
):
    output = tmp_path / "ticks.jsonl"
    signer_state = tmp_path / "signers.json"
    census_state = tmp_path / "identity-census-state.json"
    output.write_bytes(b"")

    monkeypatch.setattr(sys, "argv", command_args(output, signer_state, census_state))
    monkeypatch.setattr(
        recover_publication,
        "verify_ledger",
        lambda path: {
            "ok": False,
            "ticks": 2,
            "first_break": 2,
            "message": "previous_sha256 mismatch",
        },
    )
    monkeypatch.setattr(recover_publication, "drain_tick_outbox", lambda *args: False)

    assert recover_publication.main() == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert (
        "publication recovery failed: ledger verification failed at line 2: "
        "previous_sha256 mismatch"
    ) in captured.err


def test_main_fails_when_resulting_ledger_is_invalid(tmp_path, monkeypatch, capsys):
    output = tmp_path / "ticks.jsonl"
    signer_state = tmp_path / "signers.json"
    census_state = tmp_path / "identity-census-state.json"
    output.write_bytes(b"")
    monkeypatch.setattr(sys, "argv", command_args(output, signer_state, census_state))
    monkeypatch.setattr(
        recover_publication,
        "verify_ledger",
        lambda path: {
            "ok": False,
            "ticks": 2,
            "first_break": 2,
            "message": "tick_sha256 mismatch",
        },
    )
    monkeypatch.setattr(
        recover_publication,
        "drain_tick_outbox",
        lambda *args: True,
    )

    assert recover_publication.main() == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert (
        "publication recovery failed: ledger verification failed at line 2: "
        "tick_sha256 mismatch"
    ) in captured.err


@pytest.mark.parametrize("drained", (True, False), ids=("drained", "no-outbox"))
def test_main_fails_closed_when_pending_ledger_journal_survives_drain(
    tmp_path,
    monkeypatch,
    capsys,
    drained,
):
    output = tmp_path / "ticks.jsonl"
    signer_state = tmp_path / "signers.json"
    census_state = tmp_path / "identity-census-state.json"
    pending_path = output.with_name(output.name + ".ledger-pending.json")
    output.write_bytes(b"")
    pending_path.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(sys, "argv", command_args(output, signer_state, census_state))
    monkeypatch.setattr(
        recover_publication,
        "drain_tick_outbox",
        lambda *args: drained,
    )
    monkeypatch.setattr(
        recover_publication,
        "verify_ledger",
        lambda path: {
            "ok": True,
            "ticks": 1,
            "first_break": None,
            "message": "",
        },
    )
    monkeypatch.setattr(
        recover_publication,
        "recover_pending_jsonl",
        lambda path: False,
    )

    assert recover_publication.main() == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert (
        f"publication recovery failed: unresolved ledger pending journal remains: "
        f"{pending_path}"
    ) in captured.err


def test_main_recovers_a_legacy_pending_journal_without_an_outbox(
    tmp_path,
    monkeypatch,
    capsys,
):
    output = tmp_path / "ticks.jsonl"
    signer_state = tmp_path / "signers.json"
    census_state = tmp_path / "identity-census-state.json"
    pending_path = output.with_name(output.name + ".ledger-pending.json")
    pending_path.write_text("journal", encoding="utf-8")

    def recover(path):
        assert path == output
        output.write_text("genesis\n", encoding="utf-8")
        pending_path.unlink()
        return True

    monkeypatch.setattr(sys, "argv", command_args(output, signer_state, census_state))
    monkeypatch.setattr(recover_publication, "drain_tick_outbox", lambda *args: False)
    monkeypatch.setattr(recover_publication, "recover_pending_jsonl", recover)
    monkeypatch.setattr(
        recover_publication,
        "verify_ledger",
        lambda path: {
            "ok": True,
            "ticks": 1,
            "first_break": None,
            "message": "",
        },
    )

    assert recover_publication.main() == 0
    captured = capsys.readouterr()
    assert "pending ledger journal recovered" in captured.out
    assert captured.err == ""


@pytest.mark.parametrize("argument", ("--output", "--signer-state", "--census-state"))
def test_parser_rejects_relative_state_paths(argument):
    arguments = [
        "--output",
        "C:/observatory/ticks.jsonl",
        "--signer-state",
        "C:/observatory/signers.json",
        "--census-state",
        "C:/observatory/identity-census-state.json",
    ]
    arguments[arguments.index(argument) + 1] = "relative/path"

    with pytest.raises(SystemExit) as error:
        recover_publication.build_parser().parse_args(arguments)

    assert error.value.code == 2
