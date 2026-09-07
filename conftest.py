"""Test-suite fixtures for deployment-only runtime requirements."""

import sqlite3

import pytest

import collect
import query_service


@pytest.fixture(autouse=True)
def allow_test_runner_sqlite(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep unit tests portable while dedicated tests exercise the runtime guard."""
    monkeypatch.setattr(collect, "PINNED_SQLITE_VERSION", sqlite3.sqlite_version_info)
    monkeypatch.setattr(
        query_service,
        "PINNED_SQLITE_VERSION",
        sqlite3.sqlite_version_info,
    )
