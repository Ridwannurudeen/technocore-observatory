"""Build an atomic, versioned static Observatory release."""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import os
import re
import shutil
import tempfile
from pathlib import Path
from typing import Any

import derive
from api_contract import CONTRACT_VERSION, json_bytes
from snapshots import (
    MAX_INCIDENTS,
    build_snapshots_from_derived,
    parse_utc,
    write_snapshot_artifacts,
)

SITE_ROOT = Path(__file__).with_name("site")
RELEASE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
UNPUBLISHED_PREFIX = ".unpublished-"
TOKEN = re.compile(r"%%([A-Z][A-Z0-9_]*)%%")
PUBLIC_BASE_URL = "https://technocore.gudman.xyz"
SHARE_IMAGE_URL = f"{PUBLIC_BASE_URL}/og.png"
PAGE_PATHS = {
    "about": "/about/",
    "changes": "/changes/",
    "home": "/",
    "incidents": "/incidents/",
    "methodology": "/methodology/",
    "observatory": "/observatory/",
    "rooms": "/rooms/",
    "status": "/status/",
}


def text(value: Any) -> str:
    if value is None:
        return "Not observed"
    if isinstance(value, bool):
        return "Yes" if value else "No"
    return str(value)


def escaped(value: Any) -> str:
    return html.escape(text(value), quote=True)


def label(value: Any) -> str:
    return text(value).replace("_", " ")


def substitute(template: str, values: dict[str, Any]) -> str:
    missing = sorted(set(TOKEN.findall(template)) - set(values))
    if missing:
        raise ValueError(f"template values missing: {', '.join(missing)}")
    return TOKEN.sub(lambda match: str(values[match.group(1)]), template)


def replace_marker(source: str, marker: str, value: str) -> str:
    if source.count(marker) != 1:
        raise ValueError(f"template marker must occur exactly once: {marker}")
    return source.replace(marker, value)


def read_template(name: str) -> str:
    return (SITE_ROOT / "templates" / name).read_text(encoding="utf-8")


def safe_tone(value: Any) -> str:
    state = str(value)
    allowed = {
        "current",
        "fresh",
        "stale",
        "reachable",
        "observed_failure",
        "not_observed",
        "success",
        "failure",
        "unknown",
        "open",
        "resolved",
    }
    return state if state in allowed else "unknown"


def component_rows(status: dict[str, Any]) -> str:
    components = (
        (
            "Origin probe",
            status["origin"]["state"],
            status["origin"]["source_observed_at"],
            status["origin"]["valid_until"],
        ),
        (
            "Collector",
            status["collector"]["state"],
            status["collector"]["source_observed_at"],
            status["collector"]["valid_until"],
        ),
        (
            "Pulse cycle",
            status["pulse_cycle"]["state"],
            status["pulse_cycle"]["finished_at"],
            None,
        ),
    )
    rows = []
    for index, (name, state, observed, validity) in enumerate(components, start=1):
        evidence = f"Observed {text(observed)}"
        if validity is not None:
            evidence += f" · valid until {validity}"
        rows.append(
            '<div class="ledger-row">'
            f'<span class="index">{index:02d}</span>'
            f'<span class="label">{escaped(name)}</span>'
            f'<strong class="state state-{safe_tone(state)}">{escaped(label(state))}</strong>'
            f'<span class="evidence">{escaped(evidence)}</span>'
            "</div>"
        )
    return "\n".join(rows)


def endpoint_rows(endpoints: list[dict[str, Any]]) -> str:
    if not endpoints:
        return (
            '<p class="empty-row">No normalized endpoint attempts have been observed '
            "in the declared trailing window. Unknown is not zero.</p>"
        )
    rows = []
    for index, endpoint in enumerate(endpoints, start=1):
        measure = (
            f"{text(endpoint['attempts'])} attempts · "
            f"{text(endpoint['successes'])} successes · "
            f"{text(endpoint['server_errors'])} observed 5xx"
        )
        evidence = (
            f"{endpoint['first_observed_at']} to {endpoint['last_observed_at']} · "
            f"p95 {text(endpoint['latency_ms']['p95'])} ms"
        )
        rows.append(
            '<div class="ledger-row">'
            f'<span class="index">{index:02d}</span>'
            f'<code class="label">{escaped(endpoint["route"])}</code>'
            f'<span class="measure">{escaped(measure)}</span>'
            f'<span class="evidence">{escaped(evidence)}</span>'
            "</div>"
        )
    return "\n".join(rows)


def incident_rows(incidents: list[dict[str, Any]]) -> str:
    if not incidents:
        return (
            '<p class="empty-row">No interval currently meets a declared incident '
            "rule in the retained evidence window.</p>"
        )
    rows = []
    for index, incident in enumerate(incidents, start=1):
        resolved = incident["resolved_at"] or "Still open"
        stamp = (
            f"Opened {escaped(incident['opened_at'])}<br>"
            f"Last observed {escaped(incident['last_observed_at'])}<br>"
            f"Resolved {escaped(resolved)}"
        )
        counts = (
            f"{text(incident['attempts'])} attempts · "
            f"{text(incident['observed_failures'])} observed failures · "
            f"rule {text(incident['methodology_version'])}"
        )
        rows.append(
            '<article class="incident-row">'
            f'<span class="index">{index:02d}</span>'
            f"<div><h3>{escaped(incident['title'])}</h3>"
            f"<p><code>{escaped(incident['rule'])}</code> · "
            f"{escaped(label(incident['state']))}</p></div>"
            f"<div><p>{escaped(incident['description'])}</p>"
            f"<p>{escaped(counts)}</p></div>"
            f'<p class="incident-stamp">{stamp}</p>'
            "</article>"
        )
    return "\n".join(rows)


def change_rows(changes: list[dict[str, Any]]) -> str:
    if not changes:
        return (
            '<p class="empty-row">No allowlisted configuration changes have been '
            "observed in the retained discovery window.</p>"
        )
    rows = []
    for index, change in enumerate(changes, start=1):
        old_value = json.dumps(
            change["old"], ensure_ascii=True, sort_keys=True, separators=(",", ":")
        )
        new_value = json.dumps(
            change["new"], ensure_ascii=True, sort_keys=True, separators=(",", ":")
        )
        rows.append(
            '<article class="change-row">'
            f'<span class="index">{index:02d}</span>'
            f"<div><h3>{escaped(change['field'])}</h3>"
            f"<p>Source <code>{escaped(change['source_route'])}</code></p></div>"
            '<div class="change-values">'
            f"<p><span>Old</span><code>{escaped(old_value)}</code></p>"
            f"<p><span>New</span><code>{escaped(new_value)}</code></p>"
            "</div>"
            '<p class="change-stamp">'
            f"First observed {escaped(change['first_observed_at'])}<br>"
            f"Methodology {escaped(change['methodology_version'])}<br>"
            f"Interpretation affected: {escaped(change['interpretation_affected'])}"
            "</p></article>"
        )
    return "\n".join(rows)


def methodology_history_rows(revisions: list[dict[str, Any]]) -> str:
    rows = []
    for revision in revisions:
        changes = "".join(
            f"<li>{escaped(change)}</li>" for change in revision["changes"]
        )
        limitations = "".join(
            f"<li>{escaped(limitation)}</li>" for limitation in revision["limitations"]
        )
        limitation_block = (
            f"<p><strong>Limits carried by this revision</strong></p><ul>{limitations}</ul>"
            if limitations
            else ""
        )
        rows.append(
            "<div>"
            f"<dt>Revision {escaped(revision['version'])}<br>"
            f"{escaped(revision['published_on'])}</dt>"
            f"<dd><ul>{changes}</ul>{limitation_block}</dd>"
            "</div>"
        )
    return "\n".join(rows)


def page(
    name: str,
    *,
    title: str,
    description: str,
    robots: str,
    values: dict[str, Any],
) -> str:
    body = substitute(read_template(f"{name}.html"), values)
    asset_prefix = "" if name == "home" else "../"
    return substitute(
        read_template("base.html"),
        {
            "TITLE": html.escape(title, quote=True),
            "DESCRIPTION": html.escape(description, quote=True),
            "ROBOTS": robots,
            "CANONICAL_URL": f"{PUBLIC_BASE_URL}{PAGE_PATHS[name]}",
            "OG_IMAGE_URL": SHARE_IMAGE_URL,
            "ASSET_PREFIX": asset_prefix,
            "PAGE": name,
            "CONTENT": body,
        },
    )


def write_text(root: Path, relative: str, value: str) -> None:
    destination = root / relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(value, encoding="utf-8", newline="\n")


def finalize_public_tree(root: Path) -> None:
    root = Path(root)
    if root.is_symlink() or not root.is_dir():
        raise ValueError(f"public release root must be a directory: {root}")

    directories = [root]
    files = []
    for directory, names, filenames in os.walk(root, followlinks=False):
        directory = Path(directory)
        if directory.is_symlink():
            raise ValueError(f"public release tree contains a symlink: {directory}")
        for name in names:
            path = directory / name
            if path.is_symlink():
                raise ValueError(f"public release tree contains a symlink: {path}")
            if not path.is_dir():
                raise ValueError(
                    f"public release tree contains a non-directory: {path}"
                )
            directories.append(path)
        for name in filenames:
            path = directory / name
            if path.is_symlink():
                raise ValueError(f"public release tree contains a symlink: {path}")
            if not path.is_file():
                raise ValueError(f"public release tree contains a non-file: {path}")
            files.append(path)

    for directory in directories:
        directory.chmod(0o755)
    for path in files:
        path.chmod(0o644)


def write_pages(root: Path, snapshots: dict[str, dict[str, Any]]) -> None:
    status_envelope = snapshots["status"]
    status = status_envelope["status"]
    collector_coverage = status_envelope["coverage"]["collector"]
    published_date = status_envelope["published_at"].split("T", 1)[0]
    window_label = f"{status_envelope['window']['seconds'] // 60} minutes"

    write_text(
        root,
        "index.html",
        page(
            "home",
            title="Public evidence register",
            description=(
                "Search locally observed Technocore room records and read bounded "
                "status evidence with explicit source and validity times."
            ),
            robots="index,follow,max-image-preview:large",
            values={
                "PUBLISHED_DATE": escaped(published_date),
                "SOURCE_OBSERVED_AT": escaped(
                    status["collector"]["source_observed_at"]
                ),
                "COLLECTOR_VALID_UNTIL": escaped(status["collector"]["valid_until"]),
                "COLLECTOR_FRESHNESS": escaped(
                    label(status["collector"]["freshness"]).capitalize()
                ),
                "TICK_COVERAGE": escaped(collector_coverage["accepted_ticks"]),
                "ORIGIN_TONE": safe_tone(status["origin"]["state"]),
                "ORIGIN_STATE": escaped(label(status["origin"]["state"])),
                "ORIGIN_OBSERVED_AT": escaped(status["origin"]["source_observed_at"]),
                "COLLECTOR_TONE": safe_tone(status["collector"]["state"]),
                "COLLECTOR_STATE": escaped(label(status["collector"]["state"])),
                "COLLECTOR_OBSERVED_AT": escaped(
                    status["collector"]["source_observed_at"]
                ),
                "ATTEMPT_COUNT": escaped(
                    status_envelope["coverage"]["telemetry"]["attempts"]
                ),
                "WINDOW_LABEL": escaped(window_label),
            },
        ),
    )
    write_text(
        root,
        "status/index.html",
        page(
            "status",
            title="Status ledger",
            description=(
                "Separate origin, collector and endpoint observations without a "
                "composite health score."
            ),
            robots="index,follow,max-image-preview:large",
            values={
                "COMPONENT_ROWS": component_rows(status),
                "ENDPOINT_ROWS": endpoint_rows(status["endpoints"]),
                "SOURCE_OBSERVED_AT": escaped(status_envelope["source_observed_at"]),
                "VALID_UNTIL": escaped(status_envelope["valid_until"]),
                "PUBLISHED_AT": escaped(status_envelope["published_at"]),
                "FRESHNESS": escaped(label(status_envelope["freshness"])),
            },
        ),
    )

    incident_envelope = snapshots["incidents"]
    incidents = incident_envelope["incidents"]
    write_text(
        root,
        "incidents/index.html",
        page(
            "incidents",
            title="Incident register",
            description=(
                "Versioned intervals derived from observed health, HTTP 5xx and "
                "collector cadence evidence."
            ),
            robots="index,follow,max-image-preview:large",
            values={
                "INCIDENT_COUNT": escaped(len(incidents)),
                "INCIDENT_ROWS": incident_rows(incidents),
                "SOURCE_OBSERVED_AT": escaped(incident_envelope["source_observed_at"]),
                "VALID_UNTIL": escaped(incident_envelope["valid_until"]),
            },
        ),
    )
    change_envelope = snapshots["changes"]
    changes = change_envelope["changes"]
    write_text(
        root,
        "changes/index.html",
        page(
            "changes",
            title="Configuration change register",
            description=(
                "A chronological register of allowlisted public discovery-field "
                "changes with versioned evidence."
            ),
            robots="index,follow,max-image-preview:large",
            values={
                "CHANGE_COUNT": escaped(len(changes)),
                "CHANGE_ROWS": change_rows(changes),
                "SOURCE_OBSERVED_AT": escaped(change_envelope["source_observed_at"]),
                "VALID_UNTIL": escaped(change_envelope["valid_until"]),
            },
        ),
    )
    write_text(
        root,
        "rooms/index.html",
        page(
            "rooms",
            title="Room evidence search",
            description=(
                "Search the local Technocore room observation register. No room "
                "names are included in this generic preview."
            ),
            robots="noindex,nofollow,noarchive",
            values={},
        ),
    )
    methodology_envelope = snapshots["methodology"]
    write_text(
        root,
        "methodology/index.html",
        page(
            "methodology",
            title="Methodology",
            description=(
                "Versioned definitions, evidence classes and claim boundaries for "
                "the Technocore Observatory."
            ),
            robots="index,follow,max-image-preview:large",
            values={
                "METHODOLOGY_VERSION": escaped(
                    methodology_envelope["methodology_version"]
                ),
                "SCHEMA_VERSION": escaped(methodology_envelope["schema_version"]),
                "HISTORY_BOUNDARY": escaped(
                    methodology_envelope["methodology"]["history_boundary"]
                ),
                "CHANGE_HISTORY": methodology_history_rows(
                    methodology_envelope["methodology"]["change_history"]
                ),
            },
        ),
    )
    write_text(
        root,
        "about/index.html",
        page(
            "about",
            title="About",
            description=(
                "A read-only public measurement publication for bounded Technocore observations."
            ),
            robots="index,follow,max-image-preview:large",
            values={},
        ),
    )


def discovery_documents() -> tuple[dict[str, Any], dict[str, Any], str, str]:
    paths = {}
    for resource in ("status", "incidents", "changes", "methodology"):
        parameters = [
            {
                "name": "format",
                "in": "query",
                "description": (
                    "Omit for the default text response; use json for JSON."
                ),
                "schema": {"type": "string", "enum": ["json"]},
            }
        ]
        if resource in {"incidents", "changes"}:
            parameters.extend(
                [
                    {
                        "name": "since",
                        "in": "query",
                        "schema": {"type": "string", "format": "date-time"},
                    },
                    {
                        "name": "limit",
                        "in": "query",
                        "schema": {"type": "integer", "minimum": 1, "maximum": 100},
                    },
                ]
            )
        paths[f"/api/v1/{resource}"] = {
            "get": {
                "summary": f"Read the bounded {resource} snapshot",
                "parameters": parameters,
                "responses": {
                    "200": {
                        "description": "Versioned read-only snapshot",
                        "content": {
                            "text/plain": {"schema": {"type": "string"}},
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "additionalProperties": True,
                                }
                            },
                        },
                    }
                },
            }
        }
    paths["/api/v1/rooms/search"] = {
        "get": {
            "summary": "Search the local room evidence index",
            "parameters": [
                {
                    "name": "q",
                    "in": "query",
                    "required": True,
                    "schema": {"type": "string", "minLength": 1, "maxLength": 80},
                },
                {
                    "name": "limit",
                    "in": "query",
                    "schema": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 20,
                        "default": 10,
                    },
                },
                {
                    "name": "format",
                    "in": "query",
                    "description": (
                        "Omit for the default text response; use json for JSON."
                    ),
                    "schema": {"type": "string", "enum": ["json"]},
                },
            ],
            "responses": {
                "200": {
                    "description": "Bounded local evidence results",
                    "content": {
                        "text/plain": {"schema": {"type": "string"}},
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "required": ["index_observed_at"],
                                "properties": {
                                    "index_observed_at": {
                                        "type": ["string", "null"],
                                        "format": "date-time",
                                    }
                                },
                                "additionalProperties": True,
                            }
                        },
                    },
                }
            },
        }
    }
    format_parameter = {
        "name": "format",
        "in": "query",
        "description": "Omit for the default text response; use json for JSON.",
        "schema": {"type": "string", "enum": ["json"]},
    }
    evidence_response = {
        "200": {
            "description": "Bounded exact local evidence",
            "content": {
                "text/plain": {"schema": {"type": "string"}},
                "application/json": {
                    "schema": {"type": "object", "additionalProperties": True}
                },
            },
        }
    }
    paths["/api/v1/rooms/{room_id}"] = {
        "get": {
            "summary": "Read one exact local room evidence record",
            "parameters": [
                {
                    "name": "room_id",
                    "in": "path",
                    "required": True,
                    "schema": {"type": "string", "pattern": "^[0-9a-f]{16}$"},
                },
                format_parameter,
            ],
            "responses": evidence_response,
        }
    }
    paths["/api/v1/dids/{did}"] = {
        "get": {
            "summary": "TRACE one exact stored did:key observation",
            "parameters": [
                {
                    "name": "did",
                    "in": "path",
                    "required": True,
                    "schema": {
                        "type": "string",
                        "pattern": "^did:key:z6Mk[1-9A-HJ-NP-Za-km-z]{20,100}$",
                    },
                },
                format_parameter,
            ],
            "responses": evidence_response,
        }
    }
    for operations in paths.values():
        get = operations["get"]
        operations["head"] = {
            **get,
            "summary": f"Read headers for: {get['summary'].lower()}",
            "description": "Returns the same status and headers as GET without a body.",
        }
    openapi = {
        "openapi": "3.1.0",
        "info": {
            "title": "Technocore Observatory read-only API",
            "version": CONTRACT_VERSION,
            "description": (
                "Bounded local observations. Unknown is not zero; observed failure "
                "is not an outage-cause claim."
            ),
        },
        "servers": [{"url": "https://technocore.gudman.xyz"}],
        "paths": paths,
    }
    agent = {
        "schema_version": "1.0",
        "name": "Technocore Observatory",
        "description": (
            "Read-only public evidence register for Technocore observations."
        ),
        "url": "https://technocore.gudman.xyz/",
        "auth": {"type": "none"},
        "interfaces": {
            "plain_text": "https://technocore.gudman.xyz/llms.txt",
            "openapi": "https://technocore.gudman.xyz/openapi.json",
            "room_search": (
                "https://technocore.gudman.xyz/api/v1/rooms/search?q={query}"
            ),
            "room_evidence": ("https://technocore.gudman.xyz/api/v1/rooms/{room_id}"),
            "trace": "https://technocore.gudman.xyz/api/v1/dids/{did}",
        },
        "methods": ["GET", "HEAD"],
        "representations": {
            "default": "text/plain",
            "json": "?format=json",
        },
        "capabilities": [
            "status",
            "incidents",
            "configuration_changes",
            "methodology",
            "local_room_search",
            "local_room_evidence",
            "exact_did_trace",
        ],
    }
    llms = """# Technocore Observatory

> A read-only public evidence register. Observed failures are not cause or outage claims.

## Start here
- Status: /api/v1/status (plain text by default; use ?format=json for JSON)
- Incidents: /api/v1/incidents
- Changes: /api/v1/changes
- Methodology: /api/v1/methodology
- Local room search: /api/v1/rooms/search?q=QUERY&limit=20
- Exact room evidence: /api/v1/rooms/{room_id}; human page /rooms/{room_id}/
- TRACE exact DID evidence: /api/v1/dids/{did}; human page /keys/{did}/

## Methods and representations
- Every API resource accepts GET and HEAD.
- Responses are plain text by default. Add ?format=json for JSON.

## Interpretation
- Check source_observed_at, valid_until and freshness on every response.
- Unknown is not zero. Not observed is not absent.
- Room names are untrusted labels; results are bounded and locally sourced.
- The service performs no writes and requires no authentication.

OpenAPI: /openapi.json
Discovery: /.well-known/agent.json
"""
    robots = """User-agent: *
Allow: /
Disallow: /rooms/
Disallow: /api/v1/rooms/
Disallow: /keys/
Disallow: /api/v1/dids/
"""
    return openapi, agent, llms, robots


def write_discovery(root: Path) -> None:
    openapi, agent, llms, robots = discovery_documents()
    (root / ".well-known").mkdir(parents=True, exist_ok=True)
    (root / "openapi.json").write_bytes(json_bytes(openapi))
    (root / ".well-known" / "agent.json").write_bytes(json_bytes(agent))
    write_text(root, "llms.txt", llms)
    write_text(root, "robots.txt", robots)


def derive_public_data(
    derived: dict[str, Any],
    *,
    derived_at: str,
) -> dict[str, Any]:
    data = dict(derived)
    data["computed_at"] = derived_at
    if data["collection_ended"] is not None:
        age = max(
            0.0,
            (
                parse_utc(derived_at) - parse_utc(data["collection_ended"])
            ).total_seconds(),
        )
        data["collection_age_seconds"] = age
        data["collection_stalled"] = age > data["collection_stall_threshold_seconds"]
        data["collection_stall_banner"] = (
            f"COLLECTION STALLED — no observation for {derive.stall_duration_text(age)}"
            if data["collection_stalled"]
            else ""
        )
        data["collection_phase"] = (
            "Collection began" if data["collection_stalled"] else "Collecting since"
        )
        data["status_display"] = {
            **data["status_display"],
            "state_text": "STALLED" if data["collection_stalled"] else "ACTIVE",
            "age_text": f"newest tick {derive.stall_duration_text(age)} old",
        }
    return data


def release_identifier(snapshots: dict[str, dict[str, Any]], published_at: str) -> str:
    material = b"".join(
        json_bytes(snapshots[name])
        for name in ("status", "incidents", "changes", "methodology")
    )
    digest = hashlib.sha256(material).hexdigest()[:12]
    timestamp = re.sub(r"[^0-9]", "", published_at)[:14]
    return f"{timestamp}-{digest}"


def build_release(
    ticks_path: str | Path,
    telemetry_path: str | Path,
    output_root: str | Path,
    observatory_template: str | Path,
    *,
    derived_at: str | None = None,
    published_at: str | None = None,
    release_id: str | None = None,
    gap_seconds: float = 300.0,
) -> Path:
    derived, tick_summary = derive.derive_jsonl(
        ticks_path,
        gap_seconds,
        MAX_INCIDENTS,
    )
    snapshots = build_snapshots_from_derived(
        derived,
        tick_summary,
        telemetry_path,
        derived_at=derived_at,
        published_at=published_at,
        gap_seconds=gap_seconds,
    )
    derived_time = snapshots["status"]["derived_at"]
    published_time = snapshots["status"]["published_at"]
    identifier = release_id or release_identifier(snapshots, published_time)
    if RELEASE_NAME.fullmatch(identifier) is None:
        raise ValueError(
            "release_id must contain only letters, digits, dot, dash or underscore"
        )

    output = Path(output_root).resolve()
    releases = output / "releases"
    releases.mkdir(parents=True, exist_ok=True)
    target = releases / identifier
    if target.exists() or target.is_symlink():
        raise FileExistsError(f"release already exists: {target}")
    temporary = Path(tempfile.mkdtemp(prefix=".building-", dir=releases))
    sidecar = releases / f"{UNPUBLISHED_PREFIX}{identifier}"
    sidecar_created = False
    renamed = False
    try:
        write_pages(temporary, snapshots)
        write_snapshot_artifacts(snapshots, temporary)
        write_discovery(temporary)

        assets = temporary / "assets"
        assets.mkdir()
        shutil.copy2(SITE_ROOT / "assets" / "styles.css", assets / "styles.css")
        shutil.copy2(SITE_ROOT / "assets" / "site.js", assets / "site.js")
        shutil.copy2(SITE_ROOT / "assets" / "home-motion.js", assets / "home-motion.js")
        shutil.copy2(SITE_ROOT / "assets" / "favicon.svg", assets / "favicon.svg")
        vendor = assets / "vendor"
        vendor.mkdir()
        shutil.copy2(
            SITE_ROOT / "assets" / "vendor" / "motion-13.1.1.min.js",
            vendor / "motion-13.1.1.min.js",
        )
        shutil.copy2(
            SITE_ROOT / "assets" / "vendor" / "MOTION-LICENSE.txt",
            vendor / "MOTION-LICENSE.txt",
        )
        shutil.copy2(SITE_ROOT / "assets" / "favicon.ico", temporary / "favicon.ico")
        shutil.copy2(SITE_ROOT / "assets" / "og.png", temporary / "og.png")

        public_data = derive_public_data(
            derived,
            derived_at=derived_time,
        )
        (temporary / "data.json").write_bytes(json_bytes(public_data))
        observatory_source = Path(observatory_template).read_text(encoding="utf-8")
        observatory_source = replace_marker(
            observatory_source,
            "%%CANONICAL_URL%%",
            f"{PUBLIC_BASE_URL}{PAGE_PATHS['observatory']}",
        )
        observatory_source = replace_marker(
            observatory_source,
            "%%OG_IMAGE_URL%%",
            SHARE_IMAGE_URL,
        )
        write_text(temporary, "observatory/index.html", observatory_source)
        observatory = temporary / "observatory" / "index.html"
        derive.inject_html(observatory, public_data)

        finalize_public_tree(temporary)
        descriptor = os.open(sidecar, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        sidecar_created = True
        os.close(descriptor)
        os.replace(temporary, target)
        renamed = True
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)
        if (
            sidecar_created
            and not renamed
            and not target.exists()
            and not target.is_symlink()
        ):
            sidecar.unlink()
    return target


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build an atomic versioned Observatory static release"
    )
    parser.add_argument("ticks", type=Path)
    parser.add_argument("telemetry", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--template", type=Path, default=Path("index.html"))
    parser.add_argument("--release-id")
    arguments = parser.parse_args()
    release = build_release(
        arguments.ticks,
        arguments.telemetry,
        arguments.output,
        arguments.template,
        release_id=arguments.release_id,
    )
    print(release)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
