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

    python guards.py --html index.html --derive derive.py --ticks ticks.jsonl
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


def launch_browser(pw):
    for channel in ("chrome", "msedge", None):
        try:
            return pw.chromium.launch(channel=channel) if channel else pw.chromium.launch()
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
            return ["SKIPPED: no Chromium/Chrome/Edge available, so layout could not be verified"]
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
    scripts = re.findall(r"<script(?![^>]*application/json)[^>]*>(.*?)</script>", html, re.S | re.I)
    paths: set[str] = set()
    for script in scripts:
        for root in ("data", "point"):
            for match in re.finditer(rf"\b{root}((?:\.[A-Za-z_][A-Za-z0-9_]*)+)", script):
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
    local = {"data.json", "point.x", "point.y"}
    return [
        f"the page reads `{path}` but the deriver never emits it (producer/consumer drift)"
        for path in sorted(read_paths(html) - local)
        if not payload_has(payload, points, path)
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
        return ["SKIPPED: playwright is not installed, so the no-JS state could not be verified"]

    points = payload.get("points") or []
    if not points:
        return ["the payload has no points; run this guard against real ticks"]
    point = points[-1]

    expectations: list[tuple[str, object]] = [
        ("hero-value", point.get("observed_public_rooms")),
        ("status", len(points)),
    ]
    identity = None
    for candidate in points:
        if candidate.get("identity_total") is not None:
            identity = candidate["identity_total"]
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
    expectations.append(("engagement-ratio", engagement.get("windowed_note_to_message_ratio")))
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
    args = parser.parse_args()

    html = args.html.read_text(encoding="utf-8")
    with tempfile.TemporaryDirectory() as tmp:
        built = build_page(args.html, args.derive, args.ticks, Path(tmp))
        payload = json.loads((Path(tmp) / "data.json").read_text(encoding="utf-8"))
        checks = (
            ("payload contract", guard_payload_contract(html, args.derive, args.ticks)),
            ("zero-width render", guard_zero_width_render(built)),
            ("no-JS honesty", guard_no_js_state(built, payload)),
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
