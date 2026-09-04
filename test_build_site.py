import json
import os
import re
import stat
import struct
import sys
import types
from pathlib import Path

import pytest

import build_site
import guards
import snapshots
from api_contract import CONTRACT_VERSION, MAX_RESPONSE_BYTES
from build_site import build_release
from test_snapshots import add_attempt, telemetry_database, tick, write_ticks


EXPECTED_FILES = {
    "index.html",
    "status/index.html",
    "rooms/index.html",
    "incidents/index.html",
    "changes/index.html",
    "observatory/index.html",
    "methodology/index.html",
    "about/index.html",
    "api/v1/status",
    "api/v1/status.txt",
    "api/v1/status.json",
    "api/v1/incidents",
    "api/v1/incidents.txt",
    "api/v1/incidents.json",
    "api/v1/changes",
    "api/v1/changes.txt",
    "api/v1/changes.json",
    "api/v1/methodology",
    "api/v1/methodology.txt",
    "api/v1/methodology.json",
    "llms.txt",
    "openapi.json",
    ".well-known/agent.json",
    "robots.txt",
    "favicon.ico",
    "og.png",
    "assets/favicon.svg",
    "assets/styles.css",
    "assets/site.js",
    "data.json",
}


@pytest.fixture
def built_release(tmp_path):
    ticks = tmp_path / "ticks.jsonl"
    telemetry = tmp_path / "telemetry.sqlite3"
    hostile_name = "private-hostile-room-name"
    record = tick("2026-08-30T00:00:00Z")
    record["events_window"][0]["name"] = hostile_name
    record["events_window"][0]["base_name"] = hostile_name
    write_ticks(ticks, record)
    with telemetry_database(telemetry) as connection:
        add_attempt(
            connection,
            attempt_id=1,
            route="/healthz",
            observed_at="2026-08-30T00:01:00Z",
            outcome="success",
            status=200,
        )
        for attempt_id, observed_at, service in (
            (2, "2026-08-30T00:01:20Z", "alpha"),
            (
                3,
                "2026-08-30T00:01:40Z",
                "%%VALID_UNTIL%%<script>alert(1)</script>",
            ),
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
    release = build_release(
        ticks,
        telemetry,
        tmp_path / "public",
        Path("index.html"),
        derived_at="2026-08-30T00:01:50Z",
        published_at="2026-08-30T00:02:00Z",
    )
    return release, hostile_name


def test_static_build_is_complete_versioned_and_contains_no_room_names(built_release):
    release, hostile_name = built_release
    files = {
        path.relative_to(release).as_posix()
        for path in release.rglob("*")
        if path.is_file()
    }

    assert release.parent.name == "releases"
    assert EXPECTED_FILES <= files
    for resource in ("status", "incidents", "changes", "methodology"):
        plain = (release / "api/v1" / resource).read_bytes()
        assert plain == (release / "api/v1" / f"{resource}.txt").read_bytes()
        assert len(plain) <= MAX_RESPONSE_BYTES
        encoded = (release / "api/v1" / f"{resource}.json").read_bytes()
        assert len(encoded) <= MAX_RESPONSE_BYTES
        json.loads(encoded)
    for path in release.rglob("*"):
        if path.is_file():
            assert hostile_name.encode() not in path.read_bytes()


def test_build_release_records_an_external_unpublished_sidecar(built_release):
    release, _ = built_release
    sidecar = release.parent / f"{build_site.UNPUBLISHED_PREFIX}{release.name}"

    assert sidecar.is_file()
    assert not sidecar.is_symlink()
    assert not any(path.name == sidecar.name for path in release.rglob("*"))


def test_finalize_public_tree_applies_exact_directory_and_file_modes(
    tmp_path, monkeypatch
):
    root = tmp_path / "release"
    nested = root / "assets"
    nested.mkdir(parents=True)
    first = root / "index.html"
    second = nested / "site.js"
    first.write_text("index", encoding="utf-8")
    second.write_text("script", encoding="utf-8")
    applied = {}

    def record_mode(path, mode):
        applied[path] = mode

    monkeypatch.setattr(Path, "chmod", record_mode)
    build_site.finalize_public_tree(root)

    assert applied == {
        root: 0o755,
        nested: 0o755,
        first: 0o644,
        second: 0o644,
    }


@pytest.mark.skipif(os.name == "nt", reason="POSIX release modes require Linux")
def test_built_release_is_world_readable_and_traversable(built_release):
    release, _ = built_release
    sidecar = release.parent / f"{build_site.UNPUBLISHED_PREFIX}{release.name}"

    for path in (release, *(item for item in release.rglob("*") if item.is_dir())):
        assert stat.S_IMODE(path.stat().st_mode) == 0o755, path
    for path in (item for item in release.rglob("*") if item.is_file()):
        assert stat.S_IMODE(path.stat().st_mode) == 0o644, path
    assert stat.S_IMODE(sidecar.stat().st_mode) == 0o600


def test_finalize_public_tree_rejects_symlinks_without_touching_the_target(tmp_path):
    root = tmp_path / "release"
    root.mkdir()
    outside = tmp_path / "outside.txt"
    outside.write_text("preserve", encoding="utf-8")
    link = root / "escape"
    try:
        link.symlink_to(outside)
    except OSError as error:
        pytest.skip(f"symlinks are unavailable: {error}")

    with pytest.raises(ValueError, match="symlink"):
        build_site.finalize_public_tree(root)

    assert link.is_symlink()
    assert outside.read_text(encoding="utf-8") == "preserve"


def test_pages_use_local_assets_progressive_search_and_evidence_rails(built_release):
    release, _ = built_release
    home = (release / "index.html").read_text(encoding="utf-8")
    rooms = (release / "rooms/index.html").read_text(encoding="utf-8")
    status = (release / "status/index.html").read_text(encoding="utf-8")
    script = (release / "assets/site.js").read_text(encoding="utf-8")

    shell_markers = (
        '<a class="skip-link" href="#main-content">',
        '<header class="site-header">',
        "TECHNOCORE OBSERVATORY",
        '<details class="site-index">',
        "<summary>INDEX</summary>",
        '<nav aria-label="Site index">',
        '<button class="theme-control" id="theme-toggle" type="button" data-theme-value="system" hidden>THEME AUTO</button>',
        '<main id="main-content" class="page-shell" tabindex="-1">',
        '<footer class="site-footer">',
    )
    for source in (home, rooms):
        for marker in shell_markers:
            assert marker in source
        assert '<nav class="priority-nav" aria-label="Primary">' in source
        form_match = re.search(
            r'<form class="room-search"[^>]*>.*?</form>',
            source,
            re.S,
        )
        assert form_match is not None
        form = form_match.group(0)
        assert 'action="/rooms/"' in form
        assert 'method="get"' in form
        assert 'role="search"' in form
        assert 'id="room-query"' in form
        assert 'name="q"' in form
        assert 'maxlength="80"' in form
        assert 'id="search-feedback"' in form
        assert 'aria-live="polite"' in form

    assert "Room name or 16-character record ID" not in home
    assert 'href="/changes/"' in home
    assert 'name="robots" content="noindex,nofollow,noarchive"' in rooms
    assert "exact 16-character ID" not in rooms
    assert "/rooms/{16-hex}/" in rooms
    assert "maximum 20 results" in (home + rooms).lower()
    assert "zero origin reads" in rooms.lower()
    assert "untrusted labels" in rooms.lower()
    assert "not observed is not absent" in (home + rooms).lower()
    assert 'class="evidence-rail"' in home
    assert "<dt>Observed</dt>" in home
    assert "Collector tick observed" not in home
    assert "Index observed" not in home
    assert "2026-08-30T00:00:00Z" in home
    assert "2026-08-30T00:15:00Z" in home
    assert ">Fresh<" in home
    assert not re.search(r'<script\b[^>]*src="https?://', home)
    assert not re.search(
        r'<link\b[^>]*rel="(?:stylesheet|icon)"[^>]*href="https?://',
        home,
    )

    head = home.split("</head>", 1)[0]
    assert head.index('localStorage.getItem("observatory-theme")') < head.index(
        '<link rel="stylesheet"'
    )
    assert re.search(
        r'<time id="status-valid-until" datetime="20\d\d-\d\d-\d\dT[^"]+">',
        status,
    )
    assert 'data-freshness-for="status-valid-until"' in status
    assert 'data-freshness-for="collector-valid-until"' in home
    assert 'document.querySelectorAll("[data-freshness-for]")' in script
    assert (
        'word.textContent = printed === printed.toLowerCase() ? "stale" : "Stale";'
        in script
    )

    assert 'const themes = ["system", "light", "dark"];' in script
    assert "themeToggle.textContent = `THEME ${label.toUpperCase()}`;" in script
    assert "themeToggle.dataset.themeValue = theme;" in script
    assert 'localStorage.getItem("observatory-theme")' in script
    assert 'localStorage.setItem("observatory-theme", theme)' in script
    assert ".innerHTML" not in script
    assert ".textContent" in script
    assert "\u00e2" not in home + rooms + script
    assert "\u00c2" not in home + rooms + script


def test_pages_publish_canonical_icon_and_generic_social_cards(built_release):
    release, hostile_name = built_release
    routes = {
        "index.html": "/",
        "status/index.html": "/status/",
        "rooms/index.html": "/rooms/",
        "incidents/index.html": "/incidents/",
        "changes/index.html": "/changes/",
        "observatory/index.html": "/observatory/",
        "methodology/index.html": "/methodology/",
        "about/index.html": "/about/",
    }
    icon_markers = (
        '<link rel="icon" href="/favicon.ico" sizes="16x16 32x32 48x48 64x64">',
        '<link rel="icon" href="/assets/favicon.svg" type="image/svg+xml" sizes="any">',
    )
    image_url = f"{build_site.PUBLIC_BASE_URL}/og.png"

    for relative, route in routes.items():
        source = (release / relative).read_text(encoding="utf-8")
        head = source.split("</head>", 1)[0]

        for marker in icon_markers:
            assert head.count(marker) == 1, relative
        assert (
            head.count(
                f'<link rel="canonical" href="{build_site.PUBLIC_BASE_URL}{route}">'
            )
            == 1
        )
        assert head.count(f'<meta property="og:image" content="{image_url}">') == 1
        # X falls back to og:image, so twitter:image would only duplicate the
        # single %%OG_IMAGE_URL%% marker that replace_marker requires to be unique.
        assert '<meta name="twitter:image"' not in head
        assert (
            head.count('<meta name="twitter:card" content="summary_large_image">') == 1
        )

        social = "\n".join(
            line
            for line in head.splitlines()
            if 'property="og:' in line or 'name="twitter:' in line
        )
        assert hostile_name not in social
        assert not re.search(r"\b20\d{2}-\d{2}-\d{2}T", social)

    rooms = (release / "rooms/index.html").read_text(encoding="utf-8")
    assert '<meta name="robots" content="noindex,nofollow,noarchive">' in rooms

    shared_head = (
        (release / "status/index.html")
        .read_text(encoding="utf-8")
        .split("</head>", 1)[0]
    )
    observatory_head = (
        (release / "observatory/index.html")
        .read_text(encoding="utf-8")
        .split("</head>", 1)[0]
    )
    for marker in (
        *icon_markers,
        f'<meta property="og:image" content="{image_url}">',
        '<meta name="twitter:card" content="summary_large_image">',
    ):
        assert marker in shared_head
        assert marker in observatory_head


def test_brand_assets_are_static_valid_and_generic(built_release):
    release, hostile_name = built_release
    favicon_svg = Path("site/assets/favicon.svg").read_bytes()
    favicon_ico = Path("site/assets/favicon.ico").read_bytes()
    share_png = Path("site/assets/og.png").read_bytes()
    share_svg = Path("site/assets/og.svg").read_text(encoding="utf-8")

    assert (release / "assets/favicon.svg").read_bytes() == favicon_svg
    assert (release / "favicon.ico").read_bytes() == favicon_ico
    assert (release / "og.png").read_bytes() == share_png
    assert favicon_ico[:6] == b"\x00\x00\x01\x00\x04\x00"
    assert share_png[:8] == b"\x89PNG\r\n\x1a\n"
    assert struct.unpack(">II", share_png[16:24]) == (1200, 630)
    assert hostile_name.encode() not in favicon_ico + share_png
    assert hostile_name not in share_svg
    assert "%%" not in share_svg
    assert "<text" not in share_svg
    assert set(re.findall(r'fill="([^"]+)"', share_svg)) == {
        "#0B0E0C",
        "#F4F1E8",
        "#63D286",
    }


def test_site_index_links_have_the_shared_explicit_focus_treatment():
    expected = """.site-index nav a:focus-visible {
  outline: 2px solid var(--accent);
  outline-offset: 2px;
}"""

    for path in (Path("site/assets/styles.css"), Path("index.html")):
        source = path.read_text(encoding="utf-8")
        assert expected in source
        assert ':root[data-theme="light"]' in source
        assert ':root[data-theme="dark"]' in source
        assert "--accent: #176B3A;" in source
        assert "--accent: #63D286;" in source


def test_query_unavailable_fallback_uses_the_shared_shell():
    source = Path("deploy/fallback/query-unavailable.html").read_text(encoding="utf-8")

    for marker in (
        '<link rel="stylesheet" href="/assets/styles.css">',
        '<a class="skip-link" href="#main-content">',
        '<header class="site-header">',
        "TECHNOCORE OBSERVATORY",
        '<details class="site-index">',
        "<summary>INDEX</summary>",
        '<nav aria-label="Site index">',
        '<main id="main-content" class="page-shell" tabindex="-1">',
        '<footer class="site-footer">',
    ):
        assert marker in source
    assert '<nav class="priority-nav" aria-label="Primary">' in source
    assert "<script" not in source.lower()
    assert 'id="theme-toggle"' not in source
    assert "<h1>SEARCH IS UNAVAILABLE</h1>" in source
    assert (
        "The local query service is unavailable. Static evidence still serves."
        in source
    )
    assert not re.search(r'<(?:script|link)\b[^>]*(?:src|href)="https?://', source)


def test_discovery_documents_and_static_contracts_are_valid(built_release):
    release, _ = built_release
    openapi = json.loads((release / "openapi.json").read_text(encoding="utf-8"))
    agent = json.loads((release / ".well-known/agent.json").read_text(encoding="utf-8"))
    status = json.loads((release / "api/v1/status.json").read_text(encoding="utf-8"))

    assert openapi["openapi"] == "3.1.0"
    for route in (
        "/api/v1/status",
        "/api/v1/incidents",
        "/api/v1/changes",
        "/api/v1/methodology",
        "/api/v1/rooms/search",
        "/api/v1/rooms/{room_id}",
        "/api/v1/dids/{did}",
    ):
        assert {"get", "head"} <= openapi["paths"][route].keys()
    search = openapi["paths"]["/api/v1/rooms/search"]["get"]
    query = next(item for item in search["parameters"] if item["name"] == "q")
    search_format = next(
        item for item in search["parameters"] if item["name"] == "format"
    )
    assert query["schema"]["maxLength"] == 80
    assert search_format["schema"] == {
        "type": "string",
        "enum": ["json"],
    }
    assert "omit" in search_format["description"].lower()
    assert "text" in search_format["description"].lower()
    assert set(search["responses"]["200"]["content"]) == {
        "text/plain",
        "application/json",
    }
    search_schema = search["responses"]["200"]["content"]["application/json"]["schema"]
    assert search_schema["required"] == ["index_observed_at"]
    assert search_schema["properties"]["index_observed_at"] == {
        "type": ["string", "null"],
        "format": "date-time",
    }
    room_parameter = openapi["paths"]["/api/v1/rooms/{room_id}"]["get"]["parameters"][0]
    assert room_parameter["name"] == "room_id"
    assert room_parameter["schema"]["pattern"] == "^[0-9a-f]{16}$"
    assert agent["url"] == "https://technocore.gudman.xyz/"
    assert agent["auth"] == {"type": "none"}
    assert "local_room_evidence" in agent["capabilities"]
    assert "exact_did_trace" in agent["capabilities"]
    assert agent["methods"] == ["GET", "HEAD"]
    assert agent["representations"] == {
        "default": "text/plain",
        "json": "?format=json",
    }
    assert agent["interfaces"]["room_evidence"].endswith("/api/v1/rooms/{room_id}")
    assert agent["interfaces"]["trace"].endswith("/api/v1/dids/{did}")
    assert CONTRACT_VERSION == "1.1.0"
    assert status["contract_version"] == "1.1.0"
    assert status["schema_version"] == 6
    assert status["status"]["origin"]["state"] == "reachable"

    llms = (release / "llms.txt").read_text(encoding="utf-8")
    assert "/api/v1/rooms/{room_id}" in llms
    assert "/api/v1/dids/{did}" in llms
    assert "GET and HEAD" in llms
    assert "plain text by default" in llms


def test_changes_page_is_escaped_and_preserves_evidence_fields(built_release):
    release, _ = built_release
    changes = (release / "changes/index.html").read_text(encoding="utf-8")

    assert "Configuration change register" in changes
    assert "service" in changes
    assert "alpha" in changes
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in changes
    assert "<script>alert(1)</script>" not in changes
    assert "%%VALID_UNTIL%%" in changes
    assert "/config" in changes
    assert "2026-08-30T00:01:40Z" in changes
    assert "Methodology 1.0.0" in changes
    assert 'href="/api/v1/changes"' in changes


def test_incident_stamp_escapes_its_timestamps_and_never_prints_none_counts():
    rows = build_site.incident_rows(
        [
            {
                "title": "Observed health probe failures",
                "rule": "health_probe_failed",
                "state": "open",
                "description": "Bounded observation record.",
                "opened_at": "<script>alert(1)</script>",
                "last_observed_at": "2026-08-30T00:01:00Z",
                "resolved_at": None,
                "attempts": None,
                "observed_failures": None,
                "methodology_version": "1.0.0",
            }
        ]
    )

    assert "<script>alert(1)</script>" not in rows
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in rows
    assert "Opened &lt;script&gt;" in rows
    assert "Resolved Still open" in rows
    assert "None attempts" not in rows
    assert "Not observed attempts · Not observed observed failures" in rows


def test_endpoint_measure_never_prints_none_as_a_count():
    rows = build_site.endpoint_rows(
        [
            {
                "route": "/healthz",
                "attempts": None,
                "successes": None,
                "server_errors": None,
                "latency_ms": {"p95": None},
                "first_observed_at": "2026-08-30T00:00:00Z",
                "last_observed_at": "2026-08-30T00:01:00Z",
            }
        ]
    )

    assert "None attempts" not in rows
    assert (
        "Not observed attempts · Not observed successes · "
        "Not observed observed 5xx" in rows
    )


def test_incident_and_change_machine_links_use_public_api_routes(built_release):
    release, _ = built_release
    incidents = (release / "incidents/index.html").read_text(encoding="utf-8")
    changes = (release / "changes/index.html").read_text(encoding="utf-8")

    assert 'href="/api/v1/incidents"' in incidents
    assert 'href="/api/v1/incidents?format=json"' in incidents
    assert 'href="/api/v1/incidents.txt"' not in incidents
    assert 'href="/api/v1/incidents.json"' not in incidents
    assert 'href="/api/v1/changes"' in changes
    assert 'href="/api/v1/changes?format=json"' in changes
    assert 'href="/api/v1/changes.txt"' not in changes
    assert 'href="/api/v1/changes.json"' not in changes


def test_methodology_page_publishes_revision_history_and_residual_limit(
    built_release,
):
    release, _ = built_release
    methodology = (release / "methodology/index.html").read_text(encoding="utf-8")

    assert 'href="#history"' in methodology
    assert "Revision 1.13.0" in methodology
    assert "Revision 1.12.0" in methodology
    assert "older scheduled checks as superseded" in methodology
    assert "Revision 1.11.0" in methodology
    assert "does not make those names confidential" in methodology
    assert "Repository-backed methodology history begins at 1.3.0" in methodology


def test_build_release_reads_the_tick_ledger_once(tmp_path, monkeypatch):
    ticks = tmp_path / "ticks.jsonl"
    telemetry = tmp_path / "telemetry.sqlite3"
    write_ticks(ticks, tick("2026-08-30T00:00:00Z"))
    telemetry_database(telemetry).close()
    original = snapshots.load_ticks
    calls = []

    def counting_load(path):
        calls.append(Path(path))
        return original(Path(path))

    monkeypatch.setattr(snapshots, "load_ticks", counting_load)
    monkeypatch.setattr(build_site, "load_ticks", counting_load)
    build_release(
        ticks,
        telemetry,
        tmp_path / "public",
        Path("index.html"),
        derived_at="2026-08-30T00:01:00Z",
        published_at="2026-08-30T00:01:00Z",
    )

    assert calls == [ticks]


def test_build_release_samples_clock_after_the_telemetry_snapshot(
    tmp_path,
    monkeypatch,
):
    ticks = tmp_path / "ticks.jsonl"
    telemetry = tmp_path / "telemetry.sqlite3"
    write_ticks(ticks, tick("2026-08-30T00:00:00Z"))
    telemetry_connection = telemetry_database(telemetry)
    original_load = snapshots.load_telemetry
    telemetry_loaded = False

    def racing_load(path):
        nonlocal telemetry_loaded
        add_attempt(
            telemetry_connection,
            attempt_id=1,
            route="/healthz",
            observed_at="2026-08-30T00:00:02Z",
            outcome="success",
            status=200,
        )
        telemetry_connection.commit()
        telemetry_connection.close()
        result = original_load(path)
        telemetry_loaded = True
        return result

    def ordered_clock():
        return "2026-08-30T00:00:03Z" if telemetry_loaded else "2026-08-30T00:00:01Z"

    monkeypatch.setattr(snapshots, "load_telemetry", racing_load)
    monkeypatch.setattr(snapshots, "utc_now", ordered_clock)

    release = build_release(
        ticks,
        telemetry,
        tmp_path / "public",
        Path("index.html"),
    )
    status = json.loads((release / "api/v1/status.json").read_bytes())

    assert release.name.startswith("20260830000003-")
    assert status["status"]["origin"]["source_observed_at"] == "2026-08-30T00:00:02Z"
    assert status["derived_at"] == "2026-08-30T00:00:03Z"
    assert status["published_at"] == "2026-08-30T00:00:03Z"
    assert status["generated_at"] == "2026-08-30T00:00:03Z"


def test_unpublished_sidecar_exists_before_the_release_rename(tmp_path, monkeypatch):
    ticks = tmp_path / "ticks.jsonl"
    telemetry = tmp_path / "telemetry.sqlite3"
    write_ticks(ticks, tick("2026-08-30T00:00:00Z"))
    telemetry_database(telemetry).close()
    observed = []
    replace = build_site.os.replace

    def assert_journaled_before_rename(source, target):
        source = Path(source)
        target = Path(target)
        if source.name.startswith(".building-") and target.parent.name == "releases":
            sidecar = target.parent / f"{build_site.UNPUBLISHED_PREFIX}{target.name}"
            assert sidecar.is_file()
            assert not sidecar.is_symlink()
            observed.append((source.name, target.name))
        return replace(source, target)

    monkeypatch.setattr(build_site.os, "replace", assert_journaled_before_rename)
    release = build_release(
        ticks,
        telemetry,
        tmp_path / "public",
        Path("index.html"),
        derived_at="2026-08-30T00:01:00Z",
        published_at="2026-08-30T00:01:00Z",
    )

    assert len(observed) == 1
    assert observed[0][0].startswith(".building-")
    assert observed[0][1] == release.name


def test_observatory_source_has_phase_three_accessibility_contracts():
    source = Path("index.html").read_text(encoding="utf-8")

    assert 'href="#main-content"' in source
    assert '<main id="main-content"' in source
    assert 'id="chart-summary"' in source
    assert 'id="chart-data-table"' in source
    assert 'id="composition-summary"' in source
    assert 'id="composition-data-table"' in source
    assert "aria-valuetext" in source
    assert "const MAX_BAND_COLUMNS = 96;" in source
    assert "const SERIES_RATE_METRICS = {" in source
    assert "SERIES_RATE_METRICS[selectedSeries]" in source
    assert 'event.key === "ArrowRight"' in source

    # Playback is gone: the record is not a demo, so there is no control that
    # animates it and no timer that could start one.
    assert 'id="play"' not in source
    assert "setInterval" not in source

    # Every section of the report is deep-linkable, and the report column is
    # one column of ruled rows, not cards.
    for anchor in (
        "status",
        "about",
        "instrument",
        "growth",
        "lifecycle",
        "engagement",
        "integrity",
        "methodology",
        "colophon",
    ):
        assert f'id="{anchor}"' in source

    # The shell contract is the site's own: one INDEX control, one theme
    # button with the shared storage key and value attribute.
    assert '<details class="site-index">' in source
    assert 'id="theme-toggle"' in source
    assert 'data-theme-value="system"' in source
    assert '"observatory-theme"' in source

    # The theme button does nothing without scripting, so it is hidden until
    # the script that drives it runs, and its visible text is its whole
    # accessible name.
    assert 'data-theme-value="system" hidden>THEME AUTO</button>' in source
    assert "themeToggle.hidden = false;" in source
    assert 'setAttribute("aria-label"' not in source

    # The stored theme is applied before the stylesheet, so a stored dark
    # preference never paints light first.
    head = source.split("</head>", 1)[0]
    assert head.index('localStorage.getItem("observatory-theme")') < head.index(
        "<style>"
    )

    # The scrubber thumb is a real pointer target on both engines.
    assert "  width: 24px;\n  height: 44px;\n  margin-top: -21px;\n" in source
    assert source.count("  width: 24px;\n  height: 44px;\n") == 2

    # Nothing that needs scripting may render as a bare em dash or paint at the
    # chart origin before the script places it.
    assert '<circle id="point" class="point" r="4" cx="-10" cy="-10">' in source
    assert source.count("With scripting disabled they are not rendered here.") == 2

    # A tick without sampling evidence writes the not-recorded pair rather than
    # leaving the previous tick's numbers under an older observation.
    assert "const LIFECYCLE_SAMPLING_FALLBACK = {" in source
    assert "if (!display) return;" not in source

    # The newest-tick timestamp and the scrubbed observation are separate
    # published values; scrubbing must never rewrite the status timestamp.
    assert 'data-ssr="timestamp"' in source
    assert 'data-ssr="selected-observation"' in source
    assert 'byId("timestamp").textContent' in source
    update_body = source.split("function update(")[1].split("\nfunction ")[0]
    assert 'byId("timestamp")' not in update_body
    assert 'byId("selected-observation").textContent' in update_body

    # Locale-dependent display would break the byte-identical server-rendered
    # and scripted views.
    assert "new Intl.NumberFormat" not in source
    assert "toLocaleString" not in source

    # Composition shares come from the deriver, not from a browser-side
    # division that the server-rendered pass cannot reproduce.
    assert "share.textContent = entry.share_text;" in source
    assert "\u00e2" not in source


def test_theme_tokens_meet_text_chart_and_non_text_contrast_thresholds():
    def tokens(source, selector):
        match = re.search(rf"{re.escape(selector)}\s*\{{(.*?)\}}", source, re.S)
        assert match is not None
        return dict(re.findall(r"--([\w-]+):\s*([^;\n]+)", match.group(1)))

    def channels(value, background):
        if re.fullmatch(r"#[0-9a-fA-F]{3}", value):
            return tuple(int(character * 2, 16) / 255 for character in value[1:])
        if re.fullmatch(r"#[0-9a-fA-F]{6}", value):
            return tuple(int(value[index : index + 2], 16) / 255 for index in (1, 3, 5))
        match = re.fullmatch(r"rgba\((\d+),(\d+),(\d+),([01]|0?\.\d+)\)", value)
        assert match is not None, value
        foreground = tuple(int(match.group(index)) / 255 for index in range(1, 4))
        alpha = float(match.group(4))
        backdrop = channels(background, background)
        return tuple(
            alpha * front + (1 - alpha) * back
            for front, back in zip(foreground, backdrop)
        )

    def luminance(value, background):
        rgb = channels(value, background)
        adjusted = [
            channel / 12.92
            if channel <= 0.04045
            else ((channel + 0.055) / 1.055) ** 2.4
            for channel in rgb
        ]
        return 0.2126 * adjusted[0] + 0.7152 * adjusted[1] + 0.0722 * adjusted[2]

    def contrast(first, second, background=None):
        background = background or second
        lighter, darker = sorted(
            (luminance(first, background), luminance(second, second)), reverse=True
        )
        return (lighter + 0.05) / (darker + 0.05)

    site = Path("site/assets/styles.css").read_text(encoding="utf-8")
    light = {
        "bg": "#F4F1E8",
        "surface": "#FAF8F1",
        "surface-muted": "#EEEADF",
        "text": "#171B18",
        "text-muted": "#535B55",
        "line": "#747D76",
        "line-subtle": "#8C8880",
        "data-ink": "#27302A",
        "accent": "#176B3A",
        "accent-ink": "#FAF8F1",
        "warning": "#875B0D",
        "warning-bg": "#F4E8CE",
        "danger": "#A63A34",
        "danger-bg": "#F6DEDA",
        "missing": "#70665A",
    }
    dark = {
        "bg": "#0B0E0C",
        "surface": "#111512",
        "surface-muted": "#171C18",
        "text": "#E7ECE8",
        "text-muted": "#A8B0A9",
        "line": "#606C63",
        "line-subtle": "#5F6960",
        "data-ink": "#C7D0C9",
        "accent": "#63D286",
        "accent-ink": "#07100A",
        "warning": "#E6B35A",
        "warning-bg": "#211A0D",
        "danger": "#F27D73",
        "danger-bg": "#241110",
        "missing": "#B7AA99",
    }
    assert "color-scheme: light dark;" in site
    assert "@media (prefers-color-scheme: dark)" in site
    assert ":root:not([data-theme])" in site
    for selector, expected in (
        (":root", light),
        (':root[data-theme="light"]', light),
        (':root[data-theme="dark"]', dark),
    ):
        palette = tokens(site, selector)
        assert {name: palette[name] for name in expected} == expected
        for name in ("text", "text-muted", "data-ink", "accent", "missing"):
            assert contrast(palette[name], palette["bg"]) >= 4.5, (selector, name)
        assert contrast(palette["accent-ink"], palette["accent"]) >= 4.5, selector
        assert contrast(palette["warning"], palette["warning-bg"]) >= 4.5, selector
        assert contrast(palette["danger"], palette["danger-bg"]) >= 4.5, selector
        for name in ("line", "line-subtle"):
            for ground in ("bg", "surface"):
                assert contrast(palette[name], palette[ground]) >= 3, (
                    selector,
                    name,
                    ground,
                )

    # The observatory page is self-contained, so it carries its own copy of the
    # tokens. The copy has to be the same system, value for value, or the two
    # public surfaces drift apart.
    observatory = Path("index.html").read_text(encoding="utf-8")
    assert "color-scheme: light dark;" in observatory
    assert "@media (prefers-color-scheme: dark)" in observatory
    assert ":root:not([data-theme])" in observatory
    for selector, expected in (
        (":root", light),
        (':root[data-theme="light"]', light),
        (':root[data-theme="dark"]', dark),
    ):
        palette = tokens(observatory, selector)
        assert {name: palette[name] for name in expected} == expected, selector
        for name in ("text", "text-muted", "data-ink", "accent", "missing"):
            assert contrast(palette[name], palette["bg"]) >= 4.5, (selector, name)
        assert contrast(palette["accent-ink"], palette["accent"]) >= 4.5, selector
        assert contrast(palette["warning"], palette["warning-bg"]) >= 4.5, selector
        assert contrast(palette["danger"], palette["danger-bg"]) >= 4.5, selector
        for name in ("line", "line-subtle"):
            for ground in ("bg", "surface"):
                assert contrast(palette[name], palette[ground]) >= 3, (
                    selector,
                    name,
                    ground,
                )


def test_observatory_ssr_and_scripted_views_agree_value_for_value(built_release):
    """Every published value must read the same with and without scripting.

    The page used to reformat timestamps, ratios and durations in the browser,
    so a crawler and a reader saw different text for the same measurement.
    """
    sync_api = pytest.importorskip("playwright.sync_api")
    release, _ = built_release
    page_uri = (release / "observatory/index.html").resolve().as_uri()
    read = (
        "() => { const out = {};"
        " document.querySelectorAll('[data-ssr]').forEach(el => {"
        " out[el.getAttribute('data-ssr')] = el.textContent; });"
        " return out; }"
    )
    with sync_api.sync_playwright() as playwright:
        browser = None
        for channel in ("chrome", "msedge", None):
            try:
                browser = (
                    playwright.chromium.launch(channel=channel)
                    if channel
                    else playwright.chromium.launch()
                )
                break
            except Exception:
                continue
        if browser is None:
            pytest.skip("no Chromium browser is installed")
        try:
            context = browser.new_context(
                java_script_enabled=False, viewport={"width": 1280, "height": 900}
            )
            page = context.new_page()
            page.goto(page_uri)
            without_script = page.evaluate(read)
            page.close()

            page = browser.new_page(viewport={"width": 1280, "height": 900})
            page.goto(page_uri)
            page.wait_for_timeout(400)
            with_script = page.evaluate(read)
            page.close()
        finally:
            browser.close()

    assert without_script
    assert set(without_script) == set(with_script)
    assert without_script == with_script


def test_chart_breaks_at_an_unmeasurable_value_and_not_at_an_on_cadence_decrease(
    tmp_path,
):
    """The polyline must say what the readout says.

    A cumulative below its baseline is unmeasurable, so the line breaks there
    and draws no vertex; drawing it at zero put an affirmative zero on the axis
    beside a hero readout of "—". A counter that fell between two ticks that
    both arrived on cadence is not a missed observation and must not break the
    line either, or the raw segment breaks where the rollup segment does not.
    """
    sync_api = pytest.importorskip("playwright.sync_api")
    ticks = tmp_path / "ticks.jsonl"
    telemetry = tmp_path / "telemetry.sqlite3"
    write_ticks(
        ticks,
        tick("2026-08-30T08:00:00Z", event_seq=30_000),
        # Rooms fall inside the polling threshold: a recorded gap, not a break.
        tick(
            "2026-08-30T08:05:00Z",
            event_seq=30_010,
            rooms=90,
            notes=1_010,
            lobby=5_010,
        ),
        tick(
            "2026-08-30T08:10:00Z",
            event_seq=30_020,
            rooms=95,
            notes=1_020,
            lobby=5_020,
        ),
        # The event sequence resets below the chart baseline: unmeasurable.
        tick(
            "2026-08-30T08:15:00Z",
            event_seq=29_900,
            rooms=96,
            notes=900,
            lobby=4_900,
        ),
    )
    with telemetry_database(telemetry) as connection:
        add_attempt(
            connection,
            attempt_id=1,
            route="/healthz",
            observed_at="2026-08-30T08:16:00Z",
            outcome="success",
            status=200,
        )
    release = build_release(
        ticks,
        telemetry,
        tmp_path / "public",
        Path("index.html"),
        derived_at="2026-08-30T08:16:30Z",
        published_at="2026-08-30T08:17:00Z",
    )
    page_uri = (release / "observatory/index.html").resolve().as_uri()
    read = (
        "() => ({"
        " path: document.getElementById('series-path').getAttribute('d'),"
        " marker: document.getElementById('point').style.display,"
        " cursor: document.getElementById('cursor').style.display,"
        " hero: document.getElementById('hero-value').textContent })"
    )
    with sync_api.sync_playwright() as playwright:
        browser = None
        for channel in ("chrome", "msedge", None):
            try:
                browser = (
                    playwright.chromium.launch(channel=channel)
                    if channel
                    else playwright.chromium.launch()
                )
                break
            except Exception:
                continue
        if browser is None:
            pytest.skip("no Chromium browser is installed")
        try:
            page = browser.new_page(viewport={"width": 1280, "height": 900})
            page.goto(page_uri)
            page.wait_for_timeout(400)
            drawn = page.evaluate(read)
            page.close()
        finally:
            browser.close()

    # Three measurable observations, one unbroken segment, no fourth vertex.
    assert drawn["path"].count("M") == 1
    assert drawn["path"].count("L") == 2
    assert drawn["marker"] == "none"
    assert drawn["cursor"] == ""
    assert drawn["hero"] == "— observed"


def test_published_freshness_restates_itself_against_the_reader_clock(
    built_release,
):
    """The freshness word is baked at build time; a dead rebuild must not lie.

    The server-rendered word stays for readers without scripting. With
    scripting, every page that prints a validity boundary re-reads it against
    the reader's own clock.
    """
    sync_api = pytest.importorskip("playwright.sync_api")
    release, _ = built_release
    pages = {
        "status/index.html": ("status-valid-until", "fresh", "stale"),
        "index.html": ("collector-valid-until", "Fresh", "Stale"),
    }

    with sync_api.sync_playwright() as playwright:
        browser = None
        for channel in ("chrome", "msedge", None):
            try:
                browser = (
                    playwright.chromium.launch(channel=channel)
                    if channel
                    else playwright.chromium.launch()
                )
                break
            except Exception:
                continue
        if browser is None:
            pytest.skip("no Chromium browser is installed")
        try:
            for relative, (anchor, fresh, stale) in pages.items():
                built = release / relative
                source = built.read_text(encoding="utf-8")
                validity = re.search(
                    rf'<time id="{anchor}" datetime="([^"]+)">', source
                )
                assert validity is not None, relative
                assert f">{fresh}</span>" in source
                for offset, expected in ((-1000, fresh), (1000, stale)):
                    page = browser.new_page(viewport={"width": 1280, "height": 900})
                    page.add_init_script(
                        f"Date.now = () => Date.parse('{validity.group(1)}') "
                        f"+ ({offset});"
                    )
                    page.goto(built.resolve().as_uri())
                    page.wait_for_timeout(50)
                    word = page.locator(f'[data-freshness-for="{anchor}"]')
                    assert word.text_content() == expected, (relative, offset)
                    page.close()
        finally:
            browser.close()


def test_lifecycle_sampling_scrubs_back_to_the_not_recorded_pair(tmp_path):
    """Scrubbing back to a tick without sampling evidence must clear the DOM.

    The renderer used to return early, so the newer tick's numbers stayed on
    screen under the older observation timestamp.
    """
    sync_api = pytest.importorskip("playwright.sync_api")
    ticks = tmp_path / "ticks.jsonl"
    telemetry = tmp_path / "telemetry.sqlite3"
    write_ticks(
        ticks,
        tick("2026-08-30T00:00:00Z"),
        tick(
            "2026-08-30T00:01:00Z",
            event_seq=30_001,
            rooms=101,
            notes=1_001,
            lobby=5_001,
        ),
    )
    telemetry_database(telemetry).close()
    release = build_release(
        ticks,
        telemetry,
        tmp_path / "public",
        Path("index.html"),
        derived_at="2026-08-30T00:02:00Z",
        published_at="2026-08-30T00:02:00Z",
    )

    built = release / "observatory/index.html"
    source = built.read_text(encoding="utf-8")
    assert source.count("With scripting disabled they are not rendered here.") == 2
    opening = '<script id="observatory-data" type="application/json">'
    head, _, rest = source.partition(opening)
    encoded, _, tail = rest.partition("</script>")
    payload = json.loads(encoded)
    assert len(payload["points"]) == 2

    # An older published payload predates the sampling-evidence contract.
    del payload["points"][0]["room_lifecycle_sampling_display"]
    payload["points"][1]["room_lifecycle_sampling_display"] = {
        "selection": {"value_text": "42 selected", "context": "recorded selection"},
        "aged_out": {"value_text": "7 aged out", "context": "recorded aged out"},
        "stages": {
            stage: {"value_text": f"{stage} completed", "context": f"{stage} evidence"}
            for stage in ("300", "3600", "86400")
        },
    }
    rewritten = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).replace(
        "<", "\\u003c"
    )
    built.write_text(head + opening + rewritten + "</script>" + tail, encoding="utf-8")

    with sync_api.sync_playwright() as playwright:
        browser = None
        for channel in ("chrome", "msedge", None):
            try:
                browser = (
                    playwright.chromium.launch(channel=channel)
                    if channel
                    else playwright.chromium.launch()
                )
                break
            except Exception:
                continue
        if browser is None:
            pytest.skip("no Chromium browser is installed")
        try:
            page = browser.new_page(viewport={"width": 1280, "height": 900})
            page.goto(built.resolve().as_uri())
            page.wait_for_timeout(200)
            assert (
                page.locator("#lifecycle-sampling-selection").text_content()
                == "42 selected"
            )
            page.evaluate("() => update(0)")
            for identifier in (
                "lifecycle-sampling-selection",
                "lifecycle-sampling-aged-out",
                "lifecycle-stage-300",
                "lifecycle-stage-3600",
                "lifecycle-stage-86400",
            ):
                assert (
                    page.locator(f"#{identifier}").text_content() == "Not recorded"
                ), identifier
                context = page.locator(f"#{identifier}-context").text_content()
                assert "predates room-lifecycle sampling evidence" in context
            page.close()
        finally:
            browser.close()


def test_theme_storage_failure_is_disclosed_without_disabling_the_control(
    built_release,
):
    sync_api = pytest.importorskip("playwright.sync_api")
    release, _ = built_release

    with sync_api.sync_playwright() as playwright:
        browser = None
        for channel in ("chrome", "msedge", None):
            try:
                browser = (
                    playwright.chromium.launch(channel=channel)
                    if channel
                    else playwright.chromium.launch()
                )
                break
            except Exception:
                continue
        if browser is None:
            pytest.skip("no Chromium browser is installed")
        try:
            for relative in ("status/index.html", "observatory/index.html"):
                page = browser.new_page(viewport={"width": 1280, "height": 900})
                page.add_init_script(
                    "Storage.prototype.getItem = function () { throw new Error('blocked'); };"
                    "Storage.prototype.setItem = function () { throw new Error('blocked'); };"
                )
                page.goto((release / relative).resolve().as_uri())
                page.wait_for_timeout(100)
                root = page.locator("html")
                toggle = page.locator("#theme-toggle")
                assert root.get_attribute("data-theme-storage") == "unavailable"
                assert toggle.is_visible()
                toggle.click()
                assert toggle.get_attribute("data-theme-value") == "light"
                assert root.get_attribute("data-theme") == "light"
                page.close()
        finally:
            browser.close()


def test_reflow_themes_and_no_autoplay_in_real_browser(built_release):
    sync_api = pytest.importorskip("playwright.sync_api")
    release, _ = built_release

    def assert_index_focus(page):
        summary = page.locator(".site-index summary")
        summary.click()
        page.keyboard.press("Tab")
        focused = page.evaluate(
            "() => {"
            " const link = document.activeElement;"
            " const panel = link.closest('.site-index nav');"
            " const style = getComputedStyle(link);"
            " return {"
            "   target: link.matches('.site-index nav a'),"
            "   outlineStyle: style.outlineStyle,"
            "   outlineWidth: style.outlineWidth,"
            "   outlineColor: style.outlineColor,"
            "   panelColor: getComputedStyle(panel).backgroundColor"
            " };"
            "}"
        )
        assert focused["target"]
        assert focused["outlineStyle"] == "solid"
        assert focused["outlineWidth"] == "2px"
        assert focused["outlineColor"] != focused["panelColor"]
        summary.click()

    with sync_api.sync_playwright() as playwright:
        browser = None
        for channel in ("chrome", "msedge", None):
            try:
                browser = (
                    playwright.chromium.launch(channel=channel)
                    if channel
                    else playwright.chromium.launch()
                )
                break
            except Exception:
                continue
        if browser is None:
            pytest.skip("no Chromium browser is installed")
        try:
            static_pages = (
                "index.html",
                "status/index.html",
                "rooms/index.html",
                "incidents/index.html",
                "changes/index.html",
                "methodology/index.html",
                "about/index.html",
            )
            for width in (320, 1440):
                page = browser.new_page(viewport={"width": width, "height": 900})
                for relative in static_pages:
                    page.goto((release / relative).resolve().as_uri())
                    page.evaluate("localStorage.removeItem('observatory-theme')")
                    page.reload()
                    page.wait_for_timeout(50)
                    assert page.evaluate(
                        "document.documentElement.scrollWidth <= document.documentElement.clientWidth"
                    ), relative

                    toggle = page.locator("#theme-toggle")
                    assert (
                        page.evaluate(
                            "() => document.querySelector('#theme-toggle').hidden"
                        )
                        is False
                    ), relative
                    assert toggle.text_content() == "THEME AUTO"
                    assert toggle.get_attribute("aria-label") is None
                    assert toggle.get_attribute("data-theme-value") == "system"
                    assert page.locator("html").get_attribute("data-theme") is None
                    toggle.click()
                    assert toggle.text_content() == "THEME LIGHT"
                    assert toggle.get_attribute("aria-label") is None
                    assert toggle.get_attribute("data-theme-value") == "light"
                    assert page.locator("html").get_attribute("data-theme") == "light"
                    assert_index_focus(page)
                    toggle.click()
                    assert toggle.text_content() == "THEME DARK"
                    assert toggle.get_attribute("aria-label") is None
                    assert toggle.get_attribute("data-theme-value") == "dark"
                    assert page.locator("html").get_attribute("data-theme") == "dark"
                    assert_index_focus(page)
                    toggle.click()
                    assert toggle.text_content() == "THEME AUTO"
                    assert toggle.get_attribute("aria-label") is None
                    assert toggle.get_attribute("data-theme-value") == "system"
                    assert page.locator("html").get_attribute("data-theme") is None

                    short_controls = page.locator(
                        "button, select, input, summary, .section-link a"
                    ).evaluate_all(
                        "els => els.filter(el => { const r=el.getBoundingClientRect(); return r.width && r.height && (r.width < 44 || r.height < 44); }).map(el => el.id || el.textContent.trim())"
                    )
                    assert short_controls == [], relative

                observatory = "observatory/index.html"
                page.goto((release / observatory).resolve().as_uri())
                page.evaluate("localStorage.removeItem('observatory-theme')")
                page.reload()
                page.wait_for_timeout(50)
                assert page.evaluate(
                    "document.documentElement.scrollWidth <= document.documentElement.clientWidth"
                ), observatory

                # The self-contained page drives the same theme contract as the
                # templated shell, from its own inline script.
                toggle = page.locator("#theme-toggle")
                assert (
                    page.evaluate(
                        "() => document.querySelector('#theme-toggle').hidden"
                    )
                    is False
                )
                assert toggle.text_content() == "THEME AUTO"
                assert toggle.get_attribute("aria-label") is None
                assert page.locator("html").get_attribute("data-theme") is None
                toggle.click()
                assert toggle.get_attribute("data-theme-value") == "light"
                assert page.locator("html").get_attribute("data-theme") == "light"
                assert_index_focus(page)
                toggle.click()
                assert toggle.get_attribute("data-theme-value") == "dark"
                assert page.locator("html").get_attribute("data-theme") == "dark"
                assert_index_focus(page)

                # No playback control: the record does not animate itself.
                assert page.locator("#play").count() == 0
                # The scrubber addresses raw retained observations only.
                assert page.locator("#scrubber").get_attribute("max") == str(
                    len(json.loads((release / "data.json").read_text())["points"]) - 1
                )
                page.close()
        finally:
            browser.close()


def test_no_js_guard_fails_when_every_bar_anchor_disappears(monkeypatch):
    class Page:
        def goto(self, uri):
            return None

        def evaluate(self, script):
            if "data-ssr-width" in script:
                return []
            return {"hero-value": "100", "status": "1"}

    class Context:
        def new_page(self):
            return Page()

    class Browser:
        def new_context(self, **options):
            return Context()

        def close(self):
            return None

    class Chromium:
        def launch(self, **options):
            return Browser()

    class Playwright:
        chromium = Chromium()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            return False

    sync_api = types.ModuleType("playwright.sync_api")
    sync_api.sync_playwright = Playwright
    package = types.ModuleType("playwright")
    package.sync_api = sync_api
    monkeypatch.setitem(sys.modules, "playwright", package)
    monkeypatch.setitem(sys.modules, "playwright.sync_api", sync_api)

    findings = guards.guard_no_js_state(
        Path("index.html"), {"points": [{"observed_public_rooms": 100}]}
    )

    assert findings == [
        "no data-ssr-width anchors remain, so no bar width can be verified"
    ]


def test_render_guards_hold_on_a_built_release(built_release):
    """Run the two browser guards that never run anywhere else.

    `rebuild.sh` invokes them on the collector host, which has no browser, so
    they SKIP on every deploy; nothing else calls them. They are the only
    checks that look at the rendered page rather than the payload, and the
    no-JS one exists because a crawler-facing divergence shipped past the
    scripted check. CI installs Chromium, so this is where they can bite.
    """
    pytest.importorskip("playwright.sync_api")
    release, _ = built_release
    built = release / "observatory/index.html"
    payload = json.loads((release / "data.json").read_text(encoding="utf-8"))

    findings = guards.guard_zero_width_render(built) + guards.guard_no_js_state(
        built, payload
    )
    skipped = [f for f in findings if f.startswith("SKIPPED:")]
    if skipped:
        pytest.skip(skipped[0])
    assert findings == []
