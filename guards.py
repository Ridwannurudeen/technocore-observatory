#!/usr/bin/env python3
"""Pre-deploy guards for the Observatory.

Two classes of defect shipped past a careful review and a clean self-audit, because
neither is visible by reading the code:

  1. A CSS rule set width/height on a class applied to a <span>. Inline elements ignore
     both, so every funnel bar rendered at zero pixels while the JavaScript setting those
     widths was entirely correct. The capacity panel's rule was byte-identical CSS and
     worked only because it was applied to a <div>.

  2. `projection_seconds` was removed from the deriver while the page still read it.
     Each file was self-consistent; the pair was broken.

The first is a LAYOUT fact, not a code fact. A static CSS reading cannot settle it —
`.funnel li{display:grid}` blockifies a <span> child that no class selector matches — so
that guard renders the page in a real browser and asks the layout engine. The second is a
pure data-contract question and is answered statically, by running the deriver and checking
every field the page reads against what it actually emits.

Exit status is non-zero if any guard fails, so a deploy script can gate on it.

    python guards.py --html index.html --derive derive.py --ticks ticks.jsonl \
        --site-root /opt/technocore-observatory/current
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from api_contract import text_bytes
from verify_ledger import verify_ledger

# Elements that asked for a size and got nothing are reported with this much context.
PROBE = """
() => {
  const bad = [];
  document.querySelectorAll('*').forEach(el => {
    const cs = getComputedStyle(el);
    if (cs.display === 'none' || cs.visibility === 'hidden') return;
    // Did anything ask this element to have a width?
    const asked = (el.style.width && el.style.width !== '0px' && el.style.width !== '0%')
                || (cs.width !== 'auto' && cs.width !== '0px');
    if (!asked) return;
    if (el.clientWidth === 0 && el.getBoundingClientRect().width === 0) {
      bad.push({
        tag: el.tagName.toLowerCase(),
        cls: (el.className || '').toString().slice(0, 48),
        id: el.id || '',
        asked: el.style.width || cs.width,
        display: cs.display
      });
    }
  });
  return bad;
}
"""

FALLBACK_ERROR_CONTRACTS = {
    "errors/api-bad-request": "bad_request",
    "errors/api-method-not-allowed": "method_not_allowed",
    "errors/api-rate-limited": "rate_limited",
    "errors/query-unavailable": "local_query_unavailable",
}
STATIC_RELEASE_FILES = frozenset(
    {
        ".well-known/agent.json",
        "about/index.html",
        "api/v1/changes",
        "api/v1/changes.json",
        "api/v1/changes.txt",
        "api/v1/incidents",
        "api/v1/incidents.json",
        "api/v1/incidents.txt",
        "api/v1/methodology",
        "api/v1/methodology.json",
        "api/v1/methodology.txt",
        "api/v1/status",
        "api/v1/status.json",
        "api/v1/status.txt",
        "assets/site.js",
        "assets/styles.css",
        "changes/index.html",
        "data.json",
        "errors/query-unavailable.html",
        "incidents/index.html",
        "index.html",
        "llms.txt",
        "methodology/index.html",
        "observatory/index.html",
        "openapi.json",
        "robots.txt",
        "rooms/index.html",
        "status/index.html",
    }
    | {
        f"{stem}.{suffix}"
        for stem in FALLBACK_ERROR_CONTRACTS
        for suffix in ("json", "txt")
    }
)
DISCOVERY_PATHS = frozenset(
    {
        "/api/v1/changes",
        "/api/v1/dids/{did}",
        "/api/v1/incidents",
        "/api/v1/methodology",
        "/api/v1/rooms/search",
        "/api/v1/rooms/{room_id}",
        "/api/v1/status",
    }
)
RESOURCE_ATTRIBUTE = re.compile(
    r'<(?:script|link|img|source|iframe)\b[^>]*\b(?:src|href)=["\']([^"\']+)',
    re.I,
)
FORM = re.compile(r"<form\b([^>]*)>", re.I)
SCRIPT = re.compile(r"<script\b([^>]*)>(.*?)</script>", re.I | re.S)
ATTRIBUTE = re.compile(r"\b([A-Za-z_:][-A-Za-z0-9_:.]*)\s*=\s*([\"\'])(.*?)\2", re.S)
EXTERNAL_STYLE = re.compile(r"(?:@import\s+|url\(\s*)[\"\']?(?:https?:)?//", re.I)
EXTERNAL_SCRIPT = re.compile(
    r"\b(?:fetch|importScripts|WebSocket|EventSource)\s*\(\s*[\"\'](?:https?:)?//",
    re.I,
)
UNSAFE_DOM_SINKS = (
    ".innerHTML",
    ".outerHTML",
    "insertAdjacentHTML(",
    "document.write(",
)


def launch_browser(pw):
    for channel in ("chrome", "msedge", None):
        try:
            return (
                pw.chromium.launch(channel=channel) if channel else pw.chromium.launch()
            )
        except Exception:
            continue
    return None


def guard_zero_width_render(html_path: Path) -> list[str]:
    """Render the built page and fail any element that asked for a width and got zero."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return ["SKIPPED: playwright is not installed, so layout could not be verified"]

    failures: list[str] = []
    with sync_playwright() as pw:
        browser = launch_browser(pw)
        if browser is None:
            return [
                "SKIPPED: no Chromium/Chrome/Edge available, so layout could not be verified"
            ]
        try:
            page = browser.new_page(viewport={"width": 1280, "height": 900})
            page.goto(html_path.resolve().as_uri())
            # Settle on the newest observation so every panel is populated, and stop the
            # animation so nothing is measured mid-transition.
            page.evaluate(
                "() => { try { update(points.length - 1); if (typeof stop === 'function') stop(); }"
                " catch (e) { window.__guardError = e.message; } }"
            )
            page.wait_for_timeout(250)
            script_error = page.evaluate("() => window.__guardError || null")
            if script_error:
                failures.append(f"the page threw while rendering: {script_error}")
            for element in page.evaluate(PROBE):
                where = element["id"] or element["cls"] or "(no class or id)"
                failures.append(
                    f"<{element['tag']}> {where} asked for width {element['asked']} "
                    f"but rendered 0px (display:{element['display']})"
                )
        finally:
            browser.close()
    return failures


def read_paths(html: str) -> set[str]:
    """Payload paths the page's JavaScript reads, as `data.x` / `point.x.y`."""
    scripts = re.findall(
        r"<script(?![^>]*application/json)[^>]*>(.*?)</script>", html, re.S | re.I
    )
    paths: set[str] = set()
    for script in scripts:
        for root in ("data", "point"):
            for match in re.finditer(
                rf"\b{root}((?:\.[A-Za-z_][A-Za-z0-9_]*)+)", script
            ):
                paths.add(root + match.group(1))
    return paths


def payload_has(payload: dict, points: list, path: str) -> bool:
    """True if the path resolves on the payload, or on any single point."""
    root, *rest = path.split(".")
    for candidate in [payload] if root == "data" else points:
        node = candidate
        for part in rest:
            if isinstance(node, dict) and part in node:
                node = node[part]
            else:
                break
        else:
            return True
    return False


def payload_declares_optional_prefix(payload: dict, points: list, path: str) -> bool:
    """True when a parent field is present but null in this observation set."""
    root, *rest = path.split(".")
    for candidate in [payload] if root == "data" else points:
        node = candidate
        for part in rest:
            if node is None:
                return True
            if isinstance(node, dict) and part in node:
                node = node[part]
            else:
                break
    return False


def guard_ledger_chain(ticks: Path) -> list[str]:
    """The chained suffix must verify before its observations can be deployed."""
    result = verify_ledger(ticks)
    if result["ok"]:
        return []
    location = (
        f" at line {result['first_break']}" if result["first_break"] is not None else ""
    )
    return [f"tick ledger hash chain breaks{location}: {result['message']}"]


def guard_payload_contract(html: str, derive: Path, ticks: Path) -> list[str]:
    """Every field the page reads must exist in what the deriver actually emits."""
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "data.json"
        result = subprocess.run(
            [sys.executable, str(derive.resolve()), str(ticks.resolve()), str(out)],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            return [
                "derive.py failed, so the contract could not be checked: "
                + result.stderr.strip()[:300]
            ]
        payload = json.loads(out.read_text(encoding="utf-8"))

    points = payload.get("points", [])
    if not points:
        return ["the deriver produced no points; run this guard against real ticks"]

    # Paths the page synthesises itself rather than reading from the payload.
    # `coordinates()` builds {x, y, breakBefore} objects and iterates them as
    # `point`, so those three names never come from a tick.
    local = {"data.json", "point.x", "point.y", "point.breakBefore"}

    # A payload array is legitimately used as an array. Calling one of these on
    # it does not mean the deriver owes us a field by that name.
    builtins = {
        "map",
        "filter",
        "forEach",
        "reduce",
        "slice",
        "join",
        "at",
        "push",
        "length",
        "includes",
        "indexOf",
        "some",
        "every",
        "find",
        "sort",
        "concat",
        "keys",
        "values",
        "entries",
        "toFixed",
        "valueOf",
        "toString",
    }

    def satisfied(path: str) -> bool:
        if payload_has(payload, points, path) or payload_declares_optional_prefix(
            payload, points, path
        ):
            return True
        root, _, member = path.rpartition(".")
        return bool(root) and member in builtins and payload_has(payload, points, root)

    return [
        f"the page reads `{path}` but the deriver never emits it (producer/consumer drift)"
        for path in sorted(read_paths(html) - local)
        if not satisfied(path)
    ]


def guard_no_js_state(html_path: Path, payload: dict) -> list[str]:
    """Load the built page with JavaScript disabled; the SSR state must stand alone.

    The zero-width guard runs `update()` first, so it never sees the no-JS
    view — which is exactly how a crawler-facing divergence shipped past it.
    With scripts off: no element that should carry a measured value may render
    an em dash while the payload holds a number, and no bar with a payload
    width may render at zero pixels.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return [
            "SKIPPED: playwright is not installed, so the no-JS state could not be verified"
        ]

    points = payload.get("points") or []
    if not points:
        return ["the payload has no points; run this guard against real ticks"]
    point = points[-1]

    expectations: list[tuple[str, object]] = [
        ("hero-value", point.get("observed_public_rooms")),
        ("status", len(points)),
    ]
    census_display = point.get("census_display")
    identity = census_display.get("value") if isinstance(census_display, dict) else None
    expectations.append(("identity-total", identity))
    funnel = point.get("signer_funnel") or {}
    display = funnel.get("display") if isinstance(funnel, dict) else None
    stage_keys = {
        "census": "funnel-census",
        "observed": "funnel-observed",
        "two_ticks": "funnel-two-ticks",
        "two_dates": "funnel-two-dates",
        "sustained": "funnel-sustained",
    }
    if isinstance(display, dict):
        for stage in display.get("stages", []):
            ssr_key = stage_keys.get(stage.get("key"))
            if ssr_key:
                expectations.append((ssr_key, stage.get("value")))
        census = display.get("census")
        if isinstance(census, dict):
            expectations.append(("funnel-census", census.get("value")))
    engagement = point.get("engagement") or {}
    expectations.append(
        ("engagement-ratio", engagement.get("windowed_note_to_message_ratio"))
    )
    expectations.append(("engagement-zero", engagement.get("zero_response_share")))
    expectations.append(("engagement-nick", engagement.get("nick_diversity")))

    failures: list[str] = []
    with sync_playwright() as pw:
        browser = launch_browser(pw)
        if browser is None:
            return [
                "SKIPPED: no Chromium/Chrome/Edge available, so the no-JS state could not be verified"
            ]
        try:
            context = browser.new_context(
                java_script_enabled=False, viewport={"width": 1280, "height": 900}
            )
            page = context.new_page()
            page.goto(html_path.resolve().as_uri())
            texts = page.evaluate(
                "() => { const out = {};"
                " document.querySelectorAll('[data-ssr]').forEach(el => {"
                " out[el.getAttribute('data-ssr')] = el.textContent.trim(); });"
                " return out; }"
            )
            bars = page.evaluate(
                "() => Array.from(document.querySelectorAll('[data-ssr-width]')).map(el => ({"
                " key: el.getAttribute('data-ssr-width'),"
                " inline: el.style.width || '',"
                " px: el.getBoundingClientRect().width }))"
            )
        finally:
            browser.close()

    for key, value in expectations:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            continue
        text = texts.get(key)
        if text is None:
            failures.append(
                f"no data-ssr anchor for `{key}`, so the no-JS view cannot show its measured value"
            )
        elif text in ("—", ""):
            failures.append(
                f"`{key}` renders {text!r} without JavaScript while the payload holds {value}"
            )

    for bar in bars:
        inline = bar["inline"]
        if not inline:
            failures.append(f"bar `{bar['key']}` has no server-rendered width")
            continue
        try:
            asked = float(inline.rstrip("%"))
        except ValueError:
            failures.append(f"bar `{bar['key']}` has an unparseable width {inline!r}")
            continue
        if asked > 0 and bar["px"] == 0:
            failures.append(
                f"bar `{bar['key']}` asked for {inline} but rendered 0px without JavaScript"
            )
    return failures


def guard_static_release(root: Path) -> list[str]:
    """Validate the complete, local-only static publication contract."""
    root = root.resolve()
    if not root.is_dir():
        return [f"static release root does not exist: {root}"]

    failures = [
        f"static release is missing `{relative}`"
        for relative in sorted(STATIC_RELEASE_FILES)
        if not (root / relative).is_file()
    ]
    if failures:
        return failures

    documents: dict[str, dict] = {}
    for relative in (
        ".well-known/agent.json",
        "data.json",
        "openapi.json",
        "api/v1/status.json",
        "api/v1/incidents.json",
        "api/v1/changes.json",
        "api/v1/methodology.json",
        *(f"{stem}.json" for stem in FALLBACK_ERROR_CONTRACTS),
    ):
        try:
            value = json.loads((root / relative).read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            failures.append(f"`{relative}` is not valid UTF-8 JSON: {error}")
            continue
        if not isinstance(value, dict):
            failures.append(f"`{relative}` must contain a JSON object")
            continue
        documents[relative] = value

    openapi = documents.get("openapi.json")
    if openapi is not None:
        if openapi.get("openapi") != "3.1.0":
            failures.append("`openapi.json` must declare OpenAPI 3.1.0")
        servers = openapi.get("servers")
        if servers != [{"url": "https://technocore.gudman.xyz"}]:
            failures.append(
                "`openapi.json` must advertise only the public Observatory origin"
            )
        paths = openapi.get("paths")
        missing_paths = (
            sorted(DISCOVERY_PATHS - set(paths))
            if isinstance(paths, dict)
            else sorted(DISCOVERY_PATHS)
        )
        for path in missing_paths:
            failures.append(f"`openapi.json` does not discover `{path}`")
        if isinstance(paths, dict):
            for path in sorted(DISCOVERY_PATHS & set(paths)):
                operations = paths[path]
                if not isinstance(operations, dict) or set(operations) != {
                    "get",
                    "head",
                }:
                    failures.append(
                        f"`openapi.json` must expose only GET and HEAD for `{path}`"
                    )

    agent = documents.get(".well-known/agent.json")
    if agent is not None:
        if agent.get("url") != "https://technocore.gudman.xyz/":
            failures.append("agent discovery has the wrong public URL")
        if agent.get("auth") != {"type": "none"}:
            failures.append("agent discovery must declare credential-free access")
        if agent.get("methods") != ["GET", "HEAD"]:
            failures.append("agent discovery must declare GET and HEAD only")
        if agent.get("representations") != {
            "default": "text/plain",
            "json": "?format=json",
        }:
            failures.append("agent discovery has the wrong representation contract")
        interfaces = agent.get("interfaces")
        expected_interfaces = {
            "openapi": "https://technocore.gudman.xyz/openapi.json",
            "plain_text": "https://technocore.gudman.xyz/llms.txt",
            "room_search": (
                "https://technocore.gudman.xyz/api/v1/rooms/search?q={query}"
            ),
            "room_evidence": ("https://technocore.gudman.xyz/api/v1/rooms/{room_id}"),
            "trace": "https://technocore.gudman.xyz/api/v1/dids/{did}",
        }
        if not isinstance(interfaces, dict) or interfaces != expected_interfaces:
            failures.append("agent discovery has incomplete or non-local interfaces")

    for resource in ("status", "incidents", "changes", "methodology"):
        payload = documents.get(f"api/v1/{resource}.json")
        if payload is not None and resource not in payload:
            failures.append(f"`api/v1/{resource}.json` lacks its `{resource}` resource")
        if payload is not None:
            expected = text_bytes(payload)
            for relative in (f"api/v1/{resource}.txt", f"api/v1/{resource}"):
                if (root / relative).read_bytes() != expected:
                    failures.append(
                        f"`{relative}` does not match text_bytes of its JSON artifact"
                    )

    for stem, error_name in FALLBACK_ERROR_CONTRACTS.items():
        payload = documents.get(f"{stem}.json")
        if payload is None:
            continue
        if set(payload) != {"contract_version", "error", "message", "freshness"}:
            failures.append(f"`{stem}.json` has an unbounded error contract")
        if payload.get("contract_version") != "1.0.0":
            failures.append(f"`{stem}.json` has the wrong contract version")
        if payload.get("error") != error_name:
            failures.append(f"`{stem}.json` has the wrong error identifier")
        if payload.get("freshness") != "not_observed":
            failures.append(f"`{stem}.json` has the wrong freshness state")
        plain = (root / f"{stem}.txt").read_bytes()
        if plain != text_bytes(payload):
            failures.append(
                f"`{stem}.txt` does not match text_bytes of its JSON artifact"
            )
        if len(plain) > 1024 or len((root / f"{stem}.json").read_bytes()) > 1024:
            failures.append(f"`{stem}` exceeds the bounded failure-artifact size")

    html_files = sorted(root.rglob("*.html"))
    forms = 0
    inline_scripts: list[tuple[str, str]] = []
    for path in html_files:
        relative = path.relative_to(root).as_posix()
        source = path.read_text(encoding="utf-8")
        for target in RESOURCE_ATTRIBUTE.findall(source):
            if target.lower().startswith(("http://", "https://", "//")):
                failures.append(f"`{relative}` loads an external resource: {target}")
        for attributes in FORM.findall(source):
            forms += 1
            parsed = {
                name.lower(): value for name, _, value in ATTRIBUTE.findall(attributes)
            }
            if parsed.get("method", "get").lower() != "get":
                failures.append(f"`{relative}` contains a non-GET form")
            action = parsed.get("action")
            if action is None or not action.startswith("/") or action.startswith("//"):
                failures.append(f"`{relative}` contains a non-local form action")
        for attributes, body in SCRIPT.findall(source):
            parsed = {
                name.lower(): value for name, _, value in ATTRIBUTE.findall(attributes)
            }
            script_type = parsed.get("type", "").lower()
            if "src" not in parsed and script_type not in {
                "application/json",
                "application/ld+json",
            }:
                inline_scripts.append((relative, body))
    if forms == 0:
        failures.append("static release contains no progressive-enhancement GET form")

    rooms = (root / "rooms/index.html").read_text(encoding="utf-8")
    if '<meta name="robots" content="noindex,nofollow,noarchive">' not in rooms:
        failures.append("room search shell is missing its noindex metadata")
    unavailable = (root / "errors/query-unavailable.html").read_text(encoding="utf-8")
    if '<meta name="robots" content="noindex,nofollow,noarchive">' not in unavailable:
        failures.append("query failure page is missing its noindex metadata")

    robots = (root / "robots.txt").read_text(encoding="utf-8")
    for path in ("/rooms/", "/api/v1/rooms/", "/keys/", "/api/v1/dids/"):
        if f"Disallow: {path}" not in robots:
            failures.append(f"`robots.txt` does not disallow `{path}`")

    llms = (root / "llms.txt").read_text(encoding="utf-8")
    for path in DISCOVERY_PATHS:
        if path not in llms:
            failures.append(f"`llms.txt` does not discover `{path}`")

    styles = (root / "assets/styles.css").read_text(encoding="utf-8")
    if EXTERNAL_STYLE.search(styles):
        failures.append("`assets/styles.css` loads an external resource")
    script_sources = [
        ("assets/site.js", (root / "assets/site.js").read_text(encoding="utf-8")),
        *inline_scripts,
    ]
    for relative, source in script_sources:
        if EXTERNAL_SCRIPT.search(source):
            failures.append(f"`{relative}` calls an external origin")
        for sink in UNSAFE_DOM_SINKS:
            if sink in source:
                failures.append(f"`{relative}` uses the unsafe `{sink}` DOM sink")
    return failures


def build_page(html: Path, derive: Path, ticks: Path, into: Path) -> Path:
    """Render a throwaway copy of the page with real data, for the layout guard."""
    page = into / "index.html"
    shutil.copyfile(html, page)
    subprocess.run(
        [
            sys.executable,
            str(derive.resolve()),
            str(ticks.resolve()),
            str(into / "data.json"),
            "--html",
            str(page),
        ],
        check=True,
        capture_output=True,
    )
    return page


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--html", type=Path, required=True)
    parser.add_argument("--derive", type=Path, required=True)
    parser.add_argument("--ticks", type=Path, required=True)
    parser.add_argument("--site-root", type=Path, required=True)
    args = parser.parse_args()

    html = args.html.read_text(encoding="utf-8")
    with tempfile.TemporaryDirectory() as tmp:
        built = build_page(args.html, args.derive, args.ticks, Path(tmp))
        payload = json.loads((Path(tmp) / "data.json").read_text(encoding="utf-8"))
        checks = (
            ("tick ledger hash chain", guard_ledger_chain(args.ticks)),
            ("payload contract", guard_payload_contract(html, args.derive, args.ticks)),
            ("zero-width render", guard_zero_width_render(built)),
            ("no-JS honesty", guard_no_js_state(built, payload)),
            ("static release", guard_static_release(args.site_root)),
        )

        failed = 0
        for name, findings in checks:
            skipped = [f for f in findings if f.startswith("SKIPPED")]
            real = [f for f in findings if not f.startswith("SKIPPED")]
            if real:
                failed += len(real)
                print(f"FAIL  {name}")
                for finding in real:
                    print(f"        {finding}")
            elif skipped:
                print(f"SKIP  {name}: {skipped[0].split(': ', 1)[1]}")
            else:
                print(f"ok    {name}")

    if failed:
        print(f"\n{failed} guard failure(s); not safe to deploy.")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
