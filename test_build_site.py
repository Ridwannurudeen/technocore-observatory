import json
import os
import re
import stat
from pathlib import Path

import pytest

import build_site
import snapshots
from api_contract import MAX_RESPONSE_BYTES
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
        derived_at="2026-08-30T00:01:10Z",
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
            assert hostile_name not in path.read_text(encoding="utf-8")


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
    script = (release / "assets/site.js").read_text(encoding="utf-8")

    assert 'href="#main-content"' in home
    assert 'action="/rooms/"' in home
    assert 'method="get"' in home
    assert 'name="q"' in home
    assert 'maxlength="80"' in home
    assert "Room name or 16-character record ID" not in home
    assert 'href="/changes/"' in home
    assert 'name="robots" content="noindex,nofollow,noarchive"' in rooms
    assert 'maxlength="80"' in rooms
    assert "exact 16-character ID" not in rooms
    assert "/rooms/{16-hex}/" in rooms
    assert 'class="evidence-rail"' in home
    assert "Collector tick observed" in home
    assert "Index observed" not in home
    assert "2026-08-30T00:00:00Z" in home
    assert "2026-08-30T00:15:00Z" in home
    assert ">Fresh<" in home
    assert not re.search(r'<(?:script|link)\b[^>]*(?:src|href)="https?://', home)
    assert ".innerHTML" not in script
    assert ".textContent" in script
    assert "\u00e2" not in home + rooms + script
    assert "\u00c2" not in home + rooms + script


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
    assert status["contract_version"] == "1.0.0"
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
    assert "const MAX_BAND_COLUMNS=" in source
    assert "const SERIES_RATE_METRICS=" in source
    assert "SERIES_RATE_METRICS[selectedSeries]" in source
    assert "if(!reduceMotion&&points.length>1)play()" not in source
    assert 'event.key==="ArrowRight"' in source
    assert (
        'textContent=totalSamples?formatPercent(totals[name]/totalSamples):"--"'
        in source
    )
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
    for palette in (
        tokens(site, ":root"),
        tokens(site, ':root[data-theme="dark"]'),
    ):
        for name in ("ink", "ink-soft", "green", "green-bright", "amber", "red"):
            assert contrast(palette[name], palette["paper"]) >= 4.5, name
        for name in ("line", "line-strong", "focus"):
            assert contrast(palette[name], palette["paper"]) >= 3, name

    observatory = Path("index.html").read_text(encoding="utf-8")
    for palette in (
        tokens(observatory, ":root"),
        tokens(observatory, ":root[data-theme=dark]"),
    ):
        for name in ("text", "muted", "accent", "warning"):
            assert contrast(palette[name], palette["bg"]) >= 4.5, name
        for name in (
            "terracotta",
            "sage",
            "warm-grey",
            "lavender",
            "dusty-blue",
            "ochre",
        ):
            assert contrast(palette[name], palette["bg"]) >= 3, name
        for name in ("line", "line-light", "line-strong"):
            assert contrast(palette[name], palette["bg"]) >= 3, name
            assert (
                contrast(
                    palette[name],
                    palette["surface-subtle"],
                    palette["surface-subtle"],
                )
                >= 3
            ), name


def test_reflow_themes_and_no_autoplay_in_real_browser(built_release):
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
            pages = (
                "index.html",
                "status/index.html",
                "rooms/index.html",
                "incidents/index.html",
                "changes/index.html",
                "methodology/index.html",
                "about/index.html",
                "observatory/index.html",
            )
            for width in (320, 1440):
                page = browser.new_page(viewport={"width": width, "height": 900})
                for relative in pages:
                    page.goto((release / relative).resolve().as_uri())
                    page.wait_for_timeout(50)
                    assert page.evaluate(
                        "document.documentElement.scrollWidth <= document.documentElement.clientWidth"
                    ), relative
                    page.locator("#theme-select").select_option("dark")
                    assert page.locator("html").get_attribute("data-theme") == "dark"
                    page.locator("#theme-select").select_option("light")
                    assert page.locator("html").get_attribute("data-theme") == "light"
                    short_controls = page.locator("button, select, input").evaluate_all(
                        "els => els.filter(el => { const r=el.getBoundingClientRect(); return r.width && r.height && (r.width < 44 || r.height < 44); }).map(el => el.id || el.textContent.trim())"
                    )
                    assert short_controls == [], relative
                page.goto((release / "observatory/index.html").resolve().as_uri())
                page.wait_for_timeout(50)
                assert page.locator("#play").text_content() == "Play"
                page.close()
        finally:
            browser.close()
