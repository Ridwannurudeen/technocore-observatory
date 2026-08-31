import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

import guards
from api_contract import text_bytes
from build_site import build_release
from test_snapshots import telemetry_database, tick, write_ticks


ROOT = Path(__file__).parent
NGINX_HTTP = ROOT / "deploy/nginx/http-context.conf"
NGINX_VHOST = ROOT / "deploy/nginx/technocore.gudman.xyz.conf"
SYSTEMD = ROOT / "deploy/systemd"
FALLBACK = ROOT / "deploy/fallback"
API_FALLBACKS = {
    "api-bad-request": "bad_request",
    "api-method-not-allowed": "method_not_allowed",
    "api-rate-limited": "rate_limited",
    "query-unavailable": "local_query_unavailable",
}

DEPLOY_FILES = {
    NGINX_HTTP,
    NGINX_VHOST,
    SYSTEMD / "technocore-observatory.service",
    SYSTEMD / "technocore-observatory-query.service",
    SYSTEMD / "technocore-observatory-pulse.service",
    SYSTEMD / "technocore-observatory-pulse.timer",
    SYSTEMD / "technocore-observatory-rebuild.service",
    SYSTEMD / "technocore-observatory-rebuild.timer",
    *(
        FALLBACK / f"{stem}.{suffix}"
        for stem in API_FALLBACKS
        for suffix in ("json", "txt")
    ),
    FALLBACK / "query-unavailable.html",
    ROOT / "recover_publication.py",
    ROOT / "rebuild.sh",
    ROOT / "DEMO.md",
}


def read(path):
    return path.read_text(encoding="utf-8")


def test_all_deployment_assets_are_tracked():
    assert {
        path.relative_to(ROOT).as_posix() for path in DEPLOY_FILES if not path.is_file()
    } == set()


def test_nginx_http_context_is_query_private_and_supplies_shared_maps():
    source = read(NGINX_HTTP)

    assert "log_format observatory_privacy" in source
    log_format = source.split("log_format observatory_privacy", 1)[1].split(";", 1)[0]
    assert "$request_method" in log_format
    assert "$uri" in log_format
    assert "$request_uri" not in log_format
    assert "$args" not in log_format
    assert "$query_string" not in log_format
    assert not re.search(r"\$request(?:\s|['\"])", log_format)
    assert "limit_req_zone" in source
    assert "$binary_remote_addr" in source
    assert "map $arg_format $observatory_format_suffix" in source
    assert '"" ".txt"' in source
    assert '~^json$ ".json"' in source
    assert "map $arg_format $observatory_error_suffix" in source
    error_map = source.split("map $arg_format $observatory_error_suffix", 1)[1].split(
        "}", 1
    )[0]
    assert '~^json$ ".json"' in error_map
    assert 'default ".txt"' in error_map
    assert "map $args $observatory_static_request_valid" in source
    assert "map $status $observatory_retry_after" in source
    assert '429 "60"' in source
    assert "map $request_uri $observatory_robots" in source
    assert "$observatory_cache_control" in source
    assert "api/v1/rooms" in source
    assert "api/v1/dids" in source
    assert source.count("{") == source.count("}")


def test_nginx_vhost_mirrors_tls_and_keeps_headers_out_of_locations():
    source = read(NGINX_VHOST)

    assert "listen 80;" in source
    assert "listen [::]:80;" in source
    assert "include /etc/nginx/snippets/acme-challenge.conf;" in source
    assert "return 301 https://$host$request_uri;" in source
    assert "listen 443 ssl http2;" in source
    assert "listen [::]:443 ssl http2;" in source
    assert (
        "ssl_certificate /etc/letsencrypt/live/technocore.gudman.xyz/fullchain.pem;"
        in source
    )
    assert (
        "ssl_certificate_key /etc/letsencrypt/live/technocore.gudman.xyz/privkey.pem;"
        in source
    )
    assert "include /etc/letsencrypt/options-ssl-nginx.conf;" in source
    assert "ssl_dhparam /etc/letsencrypt/ssl-dhparams.pem;" in source
    assert "root /opt/technocore-observatory/current;" in source
    assert (
        source.count(
            "access_log /var/log/nginx/technocore.gudman.xyz.access.log "
            "observatory_privacy;"
        )
        == 2
    )
    assert not re.search(r"(?m)^\s{8,}add_header\b", source)

    required_headers = (
        "Content-Security-Policy",
        "Strict-Transport-Security",
        "X-Content-Type-Options",
        "Referrer-Policy",
        "Permissions-Policy",
        "X-Frame-Options",
        "Access-Control-Allow-Origin",
        "X-Robots-Tag",
        "Retry-After",
    )
    for header in required_headers:
        assert re.search(rf"add_header {re.escape(header)} .* always;", source)

    csp = next(
        line for line in source.splitlines() if "Content-Security-Policy" in line
    )
    assert "style-src 'self' 'unsafe-inline'" in csp
    assert "script-src 'self' 'unsafe-inline'" in csp
    assert "form-action 'self'" in csp
    assert "frame-ancestors 'none'" in csp
    assert 'add_header Access-Control-Allow-Origin "*" always;' in source
    assert "add_header Cache-Control $observatory_cache_control always;" in source
    assert "Access-Control-Allow-Credentials" not in source
    assert "limit_req_status 429;" in source
    assert "proxy_pass http://127.0.0.1:8765;" in source
    assert "proxy_intercept_errors on;" in source
    assert "@snapshot_fallback" not in source
    assert "@api_unavailable" not in source
    assert "@html_unavailable" in source
    assert "location = /api/v1/status" in source
    assert "location = /api/v1/incidents" in source
    assert "location = /api/v1/changes" in source
    assert 'location ~ "^/(?:rooms/[0-9a-f]{16}|keys/[^/]+)/$" {' in source
    assert "try_files /api/v1/status$observatory_format_suffix" in source
    assert source.count("if ($request_method !~ ^(GET|HEAD)$)") == 2
    assert source.count("if ($observatory_static_request_valid = 0)") == 2
    assert source.count("proxy_hide_header Cache-Control;") == 5
    assert source.count("if ($observatory_static_request_valid = 1)") == 2
    assert "rewrite ^ /api/v1/incidents$observatory_format_suffix? last;" in source
    assert "rewrite ^ /api/v1/changes$observatory_format_suffix? last;" in source
    assert re.search(
        r"location ~ \^/api/v1/\(\?:incidents\|changes\)"
        r"\\\.\(\?:txt\|json\)\$ \{\s+internal;",
        source,
    )
    for status, stem in (
        (400, "api-bad-request"),
        (405, "api-method-not-allowed"),
        (429, "api-rate-limited"),
        (503, "query-unavailable"),
    ):
        assert (
            f"error_page {status} ={status} /errors/{stem}$observatory_error_suffix;"
        ) in source
    assert (
        source.count(
            "error_page 429 =429 /errors/api-rate-limited$observatory_error_suffix;"
        )
        >= 4
    )
    assert (
        source.count(
            "error_page 400 =400 /errors/api-bad-request$observatory_error_suffix;"
        )
        >= 5
    )
    assert (
        source.count(
            "error_page 405 =405 /errors/api-method-not-allowed$observatory_error_suffix;"
        )
        >= 5
    )
    assert (
        source.count(
            "error_page 502 503 504 =503 "
            "/errors/query-unavailable$observatory_error_suffix;"
        )
        == 3
    )
    assert source.count("{") == source.count("}")


def test_every_nginx_proxy_location_discards_get_and_head_bodies():
    source = read(NGINX_VHOST)
    proxy_locations = []

    for match in re.finditer(r"(?m)^\s*location\b[^\n]*\{\s*$", source):
        opening = source.rfind("{", match.start(), match.end())
        depth = 1
        closing = opening + 1
        while depth and closing < len(source):
            if source[closing] == "{":
                depth += 1
            elif source[closing] == "}":
                depth -= 1
            closing += 1
        assert depth == 0
        body = source[opening + 1 : closing - 1]
        if re.search(r"(?m)^\s*proxy_pass\s+", body):
            proxy_locations.append(body)

    assert len(proxy_locations) == 5
    for body in proxy_locations:
        assert re.search(r"(?m)^\s*proxy_pass_request_body off;\s*$", body)
        assert re.search(r'(?m)^\s*proxy_set_header Content-Length "";\s*$', body)


def test_nginx_errors_are_no_store_while_static_api_successes_are_cacheable():
    context = read(NGINX_HTTP)
    vhost = read(NGINX_VHOST)

    assert (
        'map "$status:$observatory_static_request_valid:$request_uri" '
        "$observatory_cache_control {" in context
    )
    cache_map = context.split("$observatory_cache_control {", 1)[1].split("}", 1)[0]
    assert '~^(?:400|405|429|503): "no-store";' in cache_map
    assert (
        "~^(?:200|206|304):1:/api/v1/"
        "(?:status|incidents|changes|methodology)(?:\\?|$) "
        '"public, max-age=60, stale-if-error=300";' in cache_map
    )
    assert (
        vhost.count("add_header Cache-Control $observatory_cache_control always;") == 2
    )


def test_rooms_index_is_static_without_args_and_search_requests_are_proxied():
    context = read(NGINX_HTTP)
    vhost = read(NGINX_VHOST)

    assert "map $args $observatory_rooms_static_request_valid" in context
    rooms_map = context.split("map $args $observatory_rooms_static_request_valid", 1)[
        1
    ].split("}", 1)[0]
    assert '"" 1;' in rooms_map
    assert "default 0;" in rooms_map

    match = re.search(r"(?ms)^\s*location = /rooms/ \{(.*?)^\s{4}\}", vhost)
    assert match is not None
    location = match.group(1)
    assert "if ($observatory_rooms_static_request_valid = 1)" in location
    assert "rewrite ^ /rooms/index.html? last;" in location
    assert location.index("rewrite ^ /rooms/index.html? last;") < location.index(
        "proxy_pass http://127.0.0.1:8765;"
    )
    assert "rooms/index.html" in guards.STATIC_RELEASE_FILES


def test_systemd_units_use_the_verified_cli_contracts_and_permissions():
    collector = read(SYSTEMD / "technocore-observatory.service")
    pulse = read(SYSTEMD / "technocore-observatory-pulse.service")
    query = read(SYSTEMD / "technocore-observatory-query.service")
    rebuild = read(SYSTEMD / "technocore-observatory-rebuild.service")
    pulse_timer = read(SYSTEMD / "technocore-observatory-pulse.timer")
    rebuild_timer = read(SYSTEMD / "technocore-observatory-rebuild.timer")

    assert "User=technocore" in collector
    assert "Group=technocore" in collector
    assert "WorkingDirectory=/home/technocore/observatory" in collector
    assert "--base-url https://technocore.chat" in collector
    assert "--output /home/technocore/observatory/ticks.jsonl" in collector
    assert (
        "--telemetry-database /home/technocore/observatory/telemetry.sqlite3"
        in collector
    )
    assert "--interval 120" in collector
    assert "--signer-state /home/technocore/observatory/signers.json" in collector
    assert "UMask=0027" in collector

    assert "User=technocore" in pulse
    assert "pulse_probe.py" in pulse
    assert "--base-url https://technocore.chat" in pulse
    assert (
        "--telemetry-database /home/technocore/observatory/telemetry.sqlite3" in pulse
    )
    assert "--once" in pulse
    assert "OnUnitActiveSec=60s" in pulse_timer

    assert "User=technocore-query" in query
    assert "Group=technocore-query" in query
    assert "SupplementaryGroups=technocore" in query
    assert "query_service.py" in query
    assert "--database /home/technocore/observatory/signers.sqlite3" in query
    assert "--snapshot-root /opt/technocore-observatory/current" in query
    assert "--host 127.0.0.1" in query
    assert "--collector-version 2.11.0" in query
    assert "--methodology-version 1.13.0" in query
    assert "ProtectSystem=strict" in query
    assert "ProtectHome=read-only" in query
    assert (
        "ReadOnlyPaths=/home/technocore/observatory /opt/technocore-observatory"
        in query
    )
    assert "ReadWritePaths=" not in query

    assert "User=technocore" in rebuild
    assert "ExecStart=/home/technocore/observatory/rebuild.sh" in rebuild
    assert "ReadWritePaths=/opt/technocore-observatory" in rebuild
    assert "OnUnitActiveSec=10min" in rebuild_timer


def test_rebuild_orders_failure_prone_steps_before_the_atomic_flip():
    source = read(ROOT / "rebuild.sh")

    assert source.startswith("#!/bin/sh\nset -eu\n")
    lock_open = source.index('exec 9<"$public_root"')
    lock_acquire = source.index("flock -n 9")
    recovery = source.index("release_maintenance recover")
    build = source.index("build_site.py")
    unpublished_validation = source.index("release_maintenance validate-unpublished")
    fallbacks = source.index("deploy/fallback")
    guards_call = source.index("guards.py")
    link = source.index("ln -s")
    flip = source.index("mv -Tf")
    assert lock_open < lock_acquire < recovery < build
    assert build < unpublished_validation < fallbacks < guards_call < link < flip
    assert "command -v flock" in source
    assert "--template" in source
    assert "--site-root" in source
    assert "releases/$(basename" in source
    assert "trap" in source
    assert not re.search(
        r"(?:>|install|cp|mv)[^\n]*(?:ticks|telemetry|signers)\.sqlite3", source
    )
    assert not re.search(r"(?:>|install|cp|mv)[^\n]*ticks\.jsonl", source)
    assert "release_maintenance discard-unpublished" in source
    assert source.index("mv -Tf") < source.index("release_maintenance publish")
    assert 'mkdir -m 0755 "$release_path/errors"' in source
    assert 'chmod 0644 "$release_path"/errors/*' in source
    assert "release_maintenance prune" in source
    assert source.index("mv -Tf") < source.index("release_maintenance prune")
    assert "max_release_count=1008" in source
    assert "max_release_bytes=2147483648" in source


def release_maintenance_source():
    source = read(ROOT / "rebuild.sh")
    match = re.search(
        r"<<'PY_RELEASE_MAINTENANCE'\n(.*?)\nPY_RELEASE_MAINTENANCE",
        source,
        re.S,
    )
    assert match is not None
    return match.group(1)


def run_release_maintenance(*arguments):
    return subprocess.run(
        [sys.executable, "-c", release_maintenance_source(), *map(str, arguments)],
        capture_output=True,
        text=True,
        check=False,
    )


def test_release_maintenance_validates_deletion_and_bounds_retention(tmp_path):
    releases = tmp_path / "public/releases"
    releases.mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.mkdir()

    rejected = run_release_maintenance("delete", releases, outside)
    assert rejected.returncode != 0
    assert outside.is_dir()

    names = [f"20260830{i:06d}-release" for i in range(8)]
    for index, name in enumerate(names):
        release = releases / name
        release.mkdir()
        (release / "payload").write_bytes(b"x" * (index + 1))
        os.utime(release, (index + 1, index + 1))

    active = releases / names[-1]
    previous = releases / names[-2]
    pruned = run_release_maintenance("prune", releases, active, previous, "4", "1024")
    assert pruned.returncode == 0, pruned.stderr
    assert {path.name for path in releases.iterdir()} == set(names[-4:])

    for path in list(releases.iterdir()):
        shutil.rmtree(path)
    for index, name in enumerate(names[-4:]):
        release = releases / name
        release.mkdir()
        (release / "payload").write_bytes(b"x" * 6)
        os.utime(release, (index + 1, index + 1))

    active = releases / names[-1]
    previous = releases / names[-2]
    capped = run_release_maintenance("prune", releases, active, previous, "1008", "10")
    assert capped.returncode == 0, capped.stderr
    assert {path.name for path in releases.iterdir()} == {active.name, previous.name}


def test_release_maintenance_cleans_only_direct_valid_staging_directories(tmp_path):
    releases = tmp_path / "public/releases"
    releases.mkdir(parents=True)
    stranded = [
        releases / ".building-abc12345",
        releases / ".building-_abc1234",
    ]
    for path in stranded:
        path.mkdir()
        (path / "payload").write_bytes(b"x" * 32)

    invalid = [
        releases / ".building-",
        releases / ".building-bad+name",
    ]
    for path in invalid:
        path.mkdir()

    release = releases / "20260830000000-release"
    nested = release / ".building-nested"
    nested.mkdir(parents=True)
    staging_file = releases / ".building-file"
    staging_file.write_text("not a directory", encoding="utf-8")
    pre_rename_sidecar = releases / ".unpublished-20260830000000-pre-rename"
    pre_rename_sidecar.write_bytes(b"")

    outside = tmp_path / "outside"
    outside.mkdir()
    staging_symlink = releases / ".building-symlink"
    symlink_created = False
    try:
        staging_symlink.symlink_to(outside, target_is_directory=True)
        symlink_created = True
    except OSError:
        symlink_created = False

    cleaned = run_release_maintenance("recover", releases, releases.parent / "current")

    assert cleaned.returncode == 0, cleaned.stderr
    assert all(not path.exists() for path in stranded)
    assert all(path.is_dir() for path in invalid)
    assert nested.is_dir()
    assert staging_file.is_file()
    assert not pre_rename_sidecar.exists()
    assert outside.is_dir()
    if symlink_created:
        assert staging_symlink.is_symlink()


def test_release_recovery_reclaims_preflip_artifacts(tmp_path):
    public = tmp_path / "public"
    releases = public / "releases"
    releases.mkdir(parents=True)
    orphan = releases / "20260830000000-orphan"
    orphan.mkdir()
    orphan_sidecar = releases / f".unpublished-{orphan.name}"
    orphan_sidecar.write_bytes(b"")

    recovered = run_release_maintenance("recover", releases, public / "current")
    assert recovered.returncode == 0, recovered.stderr
    assert not orphan.exists()
    assert not orphan_sidecar.exists()


def test_release_recovery_preserves_an_active_postflip_artifact(tmp_path):
    public = tmp_path / "public"
    releases = public / "releases"
    releases.mkdir(parents=True)
    active = releases / "20260830000001-active"
    active.mkdir()
    (active / "index.html").write_text("active", encoding="utf-8")
    active_sidecar = releases / f".unpublished-{active.name}"
    active_sidecar.write_bytes(b"")
    try:
        (public / "current").symlink_to(active, target_is_directory=True)
    except OSError as error:
        pytest.skip(f"directory symlinks are unavailable: {error}")

    resumed = run_release_maintenance("recover", releases, public / "current")
    assert resumed.returncode == 0, resumed.stderr
    assert active.is_dir()
    assert (active / "index.html").read_text(encoding="utf-8") == "active"
    assert not active_sidecar.exists()


def test_release_recovery_rejects_unsafe_sidecars_before_deleting_anything(tmp_path):
    public = tmp_path / "public"
    releases = public / "releases"
    releases.mkdir(parents=True)
    safe = releases / "20260830000000-safe"
    safe.mkdir()
    safe_sidecar = releases / f".unpublished-{safe.name}"
    safe_sidecar.write_bytes(b"")

    outside = tmp_path / "outside"
    outside.mkdir()
    unsafe = releases / ".unpublished-20260830000001-unsafe"
    try:
        unsafe.symlink_to(outside, target_is_directory=True)
    except OSError:
        unsafe.mkdir()

    rejected = run_release_maintenance("recover", releases, public / "current")

    assert rejected.returncode != 0
    assert safe.is_dir()
    assert safe_sidecar.is_file()
    assert unsafe.is_symlink() or unsafe.is_dir()
    assert outside.is_dir()


def test_rebuild_removes_an_unpublished_release_when_guards_fail(tmp_path):
    if os.name == "nt":
        git_bash = Path("C:/Program Files/Git/bin/bash.exe")
        bash = str(git_bash) if git_bash.is_file() else None
    else:
        bash = shutil.which("bash")
    if bash is None:
        pytest.skip(
            "bash is unavailable; candidate cleanup received structural validation only"
        )

    checkout = tmp_path / "checkout"
    checkout.mkdir()
    shutil.copy2(ROOT / "rebuild.sh", checkout / "rebuild.sh")
    shutil.copytree(FALLBACK, checkout / "deploy/fallback")
    (checkout / "index.html").write_text("fixture", encoding="utf-8")
    (checkout / "build_site.py").write_text(
        "import os\n"
        "import tempfile\n"
        "from pathlib import Path\n"
        "release = Path(os.environ['TEST_RELEASE_WINDOWS'])\n"
        "release.mkdir(parents=True)\n"
        "(release / 'observatory').mkdir()\n"
        "(release.parent / f'.unpublished-{release.name}').write_bytes(b'')\n"
        "if os.name == 'nt':\n"
        "    relative = release.relative_to(Path(tempfile.gettempdir()))\n"
        "    print('/tmp/' + relative.as_posix())\n"
        "else:\n"
        "    print(release.resolve())\n",
        encoding="utf-8",
    )
    (checkout / "guards.py").write_text("raise SystemExit(9)\n", encoding="utf-8")
    (checkout / "derive.py").write_text("", encoding="utf-8")
    ticks = tmp_path / "ticks.jsonl"
    telemetry = tmp_path / "telemetry.sqlite3"
    ticks.write_text("", encoding="utf-8")
    telemetry.write_bytes(b"")
    public = tmp_path / "public"
    public.mkdir()
    release = public / "releases/20260830000000-candidate"
    environment = os.environ.copy()
    environment.update(
        {
            "OBSERVATORY_PYTHON": Path(sys.executable).as_posix(),
            "TEST_RELEASE_WINDOWS": str(release),
        }
    )
    if os.name == "nt":
        fake_bin = tmp_path / "bin"
        fake_bin.mkdir()
        fake_flock = fake_bin / "flock"
        fake_flock.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        fake_flock.chmod(0o755)
        environment["PATH"] = str(fake_bin) + os.pathsep + environment["PATH"]
    result = subprocess.run(
        [
            bash,
            (checkout / "rebuild.sh").as_posix(),
            ticks.as_posix(),
            telemetry.as_posix(),
            public.as_posix(),
        ],
        capture_output=True,
        text=True,
        check=False,
        env=environment,
    )
    assert result.returncode == 9, result.stderr
    assert not release.exists()
    assert not (release.parent / f".unpublished-{release.name}").exists()


def test_runbook_snapshots_before_install_and_upgrades_before_query_start():
    source = read(ROOT / "DEPLOY.md")
    evidence = source.index("Only after fencing")
    recovery = source.index("python3 recover_publication.py")
    restorable = source.index("recovery-ready state snapshot")
    install = source.index(
        "/home/technocore/observatory-candidate/deploy/nginx/http-context.conf"
    )
    assert evidence < recovery < restorable < install
    assert re.search(
        r"`signers\.sqlite3` if it exists,.*record the absence",
        source,
        re.S,
    )
    assert "Do not overwrite `/home/technocore/observatory`" in source

    activation = source.index("The corresponding activation commands are:")
    migration = source.index("For a v2 JSON source")
    refreshed_snapshot = source.index(
        "refresh the recovery-ready state snapshot",
        migration,
    )
    collector = source.index(
        "systemctl start technocore-observatory.service", activation
    )
    schema_gate = source.index(
        "# Wait for one accepted tick and verify schema 6 before continuing.",
        activation,
    )
    query = source.index(
        "systemctl start technocore-observatory-query.service", activation
    )
    assert migration < refreshed_snapshot < activation
    assert collector < schema_gate < query


def test_tick_ledger_sidecars_are_private_and_one_recovery_state_family():
    ledger_family = (
        "ticks.jsonl",
        "ticks.jsonl.ledger-checkpoint.json",
        "ticks.jsonl.ledger-pending.json",
    )
    ignored = set(read(ROOT / ".gitignore").splitlines())
    assert set(ledger_family) <= ignored
    assert {
        "ticks.jsonl.ledger-checkpoint.json.tmp",
        "ticks.jsonl.ledger-pending.json.tmp",
    } <= ignored
    assert {
        "ticks.jsonl.lock",
        "signers.json.tmp",
        "signers.json.lock",
        "signers.sqlite3-journal",
        "telemetry.sqlite3-journal",
        "census.json",
        "census.json.tmp",
        "census.json.lock",
        "identity-census-state.json",
        "identity-census-state.json.tmp",
        "identity-census-state.json.lock",
    } <= ignored

    deploy = read(ROOT / "DEPLOY.md")
    step_three = deploy.split(
        "## 3. Fence writers, snapshot the prior release, then install the candidate",
        1,
    )[1].split("## 4.", 1)[0]
    assert "one ledger recovery state family" in step_three
    assert "pending journal may be valid after an interrupted append" in step_three
    assert "python3 recover_publication.py" in step_three
    assert "performs no origin reads" in step_three
    assert "no unresolved pending journal" in step_three
    for filename in ledger_family:
        assert step_three.count(f"`{filename}`") >= 3

    readme = read(ROOT / "README.md")
    assert "ledger recovery state family" in readme
    assert "pending journal" in readme
    assert "recover_publication.py" in readme
    assert "no origin request" in readme
    for filename in ledger_family:
        assert f"`{filename}`" in readme


def test_runbook_rollback_validates_an_exact_release_child_before_linking(tmp_path):
    source = read(ROOT / "DEPLOY.md")
    rollback = source.split("## 6. Rollback without losing forward state", 1)[1]
    match = re.search(
        r"<<'PY_VALIDATE_ROLLBACK'\n(.*?)\nPY_VALIDATE_ROLLBACK",
        source,
        re.S,
    )
    assert match is not None
    validator = match.group(1)
    assert "RELEASE_NAME.fullmatch(name)" in validator
    assert "candidate.is_symlink()" in validator
    assert "resolved.parent != root" in validator
    assert ".unpublished-" in validator
    assert "```bash\n(\nset -eu\npublic_root=" in source
    assert 'ln -s "releases/$validated_release_id" "$rollback_link"' in source
    assert 'ln -s "releases/$previous_release_id" "$rollback_link"' not in source
    lock_open = rollback.index('exec 9<"$public_root"')
    lock_acquire = rollback.index("flock -n 9")
    validation = rollback.index("validated_release_id=")
    link = rollback.index('ln -s "releases/$validated_release_id"')
    assert lock_open < lock_acquire < validation < link
    rebuild = read(ROOT / "rebuild.sh")
    assert 'exec 9<"$public_root"' in rebuild
    assert "flock -n 9" in rebuild

    releases = tmp_path / "releases"
    releases.mkdir()
    valid = releases / "20260830T120000Z-release_1.12.0"
    valid.mkdir()
    accepted = subprocess.run(
        [sys.executable, "-c", validator, str(releases), valid.name],
        capture_output=True,
        text=True,
        check=False,
    )
    assert accepted.returncode == 0, accepted.stderr
    assert accepted.stdout.strip() == valid.name

    sidecar = releases / f".unpublished-{valid.name}"
    sidecar.write_bytes(b"")
    unpublished = subprocess.run(
        [sys.executable, "-c", validator, str(releases), valid.name],
        capture_output=True,
        text=True,
        check=False,
    )
    assert unpublished.returncode != 0
    sidecar.unlink()

    outside = tmp_path / "outside"
    outside.mkdir()
    traversal = subprocess.run(
        [sys.executable, "-c", validator, str(releases), "../outside"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert traversal.returncode != 0

    symlink = releases / "symlink-release"
    try:
        symlink.symlink_to(valid, target_is_directory=True)
    except OSError:
        pass
    else:
        linked = subprocess.run(
            [sys.executable, "-c", validator, str(releases), symlink.name],
            capture_output=True,
            text=True,
            check=False,
        )
        assert linked.returncode != 0


def test_runbook_has_no_sqlite_wal_sidecar_dependency():
    source = read(ROOT / "DEPLOY.md")

    assert "signer and telemetry databases use SQLite DELETE journal mode" in source
    assert "no WAL/SHM sidecar dependency" in source
    assert "sidecar for both SQLite databases" not in source
    assert "signer database and any WAL/SHM sidecars" not in source
    assert "telemetry database remains in WAL mode" not in source


def test_runbook_fences_both_legacy_crons_and_preserves_live_census_state():
    source = read(ROOT / "DEPLOY.md")

    assert "# technocore-observatory-rebuild" in source
    assert "# technocore-observatory-census" in source
    assert "snapshot and fence both" in source.lower()
    assert "do not restore the legacy rebuild cron" in source
    assert "--census-state /home/technocore/observatory/census.json" in source
    assert "restore the exact fenced census cron entry" in source.lower()


def test_runbook_handles_the_first_versioned_release_from_the_flat_publication():
    source = read(ROOT / "DEPLOY.md")

    assert "legacy flat publication" in source
    assert "/opt/technocore-observatory/index.html" in source
    assert "/opt/technocore-observatory/data.json" in source
    assert "record `current` as absent" in source
    assert "old vhost and flat files" in source


def test_query_identity_provisioning_is_idempotent():
    source = read(ROOT / "DEPLOY.md")

    assert re.search(
        r"if ! getent passwd technocore-query.*?useradd --system.*?fi",
        source,
        re.S,
    )


def test_runbook_preserves_legacy_wal_shm_in_raw_evidence_only():
    source = read(ROOT / "DEPLOY.md")
    evidence = source.index("read-only evidence")
    recovery_ready = source.index("recovery-ready state snapshot", evidence)
    raw_section = source[evidence:recovery_ready]
    ready_section = source[recovery_ready:]

    assert "signers.sqlite3-wal" in raw_section
    assert "signers.sqlite3-shm" in raw_section
    assert "legacy WAL/SHM" in raw_section
    assert re.search(
        r"no SQLite\s+`-journal`, `-wal`, or `-shm`",
        ready_section,
    )

    readme = read(ROOT / "README.md")
    assert "legacy WAL/SHM" in readme


def test_runbook_preserves_then_normalizes_sqlite_rollback_journals():
    source = read(ROOT / "DEPLOY.md")

    evidence = source.index("read-only evidence")
    integrity = source.index("PRAGMA integrity_check")
    recovery_ready = source.index("recovery-ready state snapshot", integrity)
    assert evidence < integrity < recovery_ready
    assert "signers.sqlite3-journal" in source
    assert "telemetry.sqlite3-journal" in source
    assert "hot rollback journal" in source
    assert re.search(
        r"recovery-ready snapshot must contain no SQLite\s+`-journal`",
        source,
    )

    readme = read(ROOT / "README.md")
    assert "rollback `-journal`" in readme
    assert "raw evidence snapshot" in readme
    assert re.search(
        r"restore the ledger family, `telemetry\.sqlite3`, `signers\.json`,\s+"
        r"`signers\.sqlite3`, and the\s+census state",
        readme,
    )


def test_runbook_allows_verified_legacy_checkpoint_absence():
    source = read(ROOT / "DEPLOY.md")

    assert "record a missing legacy checkpoint" in source
    assert re.search(
        r"next accepted append\s+creates its canonical checkpoint",
        source,
    )


def test_runbook_checks_invalid_queries_only_after_query_service_restart():
    source = read(ROOT / "DEPLOY.md")
    stopped_match = re.search(
        r"With the\s+query service stopped, every parameterized query route",
        source,
    )
    assert stopped_match is not None
    stopped = stopped_match.start()
    restart = source.index("Restart the query service after the stopped-daemon checks")
    invalid_match = re.search(
        r"After restart, invalid search arguments\s+must stay bounded",
        source,
    )
    assert invalid_match is not None
    invalid = invalid_match.start()

    assert stopped < restart < invalid


def test_docs_state_the_scoped_lint_waiver_and_failure_metadata_boundary():
    for path in (ROOT / "README.md", ROOT / "DEPLOY.md"):
        assert 'ruff check . --per-file-ignores "derive.py:F841"' in read(path)

    demo = read(ROOT / "DEMO.md")
    assert "Every successful evidence response carries source time" in demo
    assert "failure artifacts report only their error contract" in demo
    readme = read(ROOT / "README.md")
    deploy = read(ROOT / "DEPLOY.md")
    for source in (readme, deploy):
        assert "1,008" in source
        assert "2 GiB" in source
        assert "immediate predecessor" in source
        assert "methodology 1.13.0" in source
    assert "silently" in deploy
    assert "filtered" in deploy
    assert "`.unpublished-<id>` sidecar before" in deploy
    assert "every generated directory mode `0755`" in deploy
    assert "every ordinary generated file mode" in deploy
    assert "same exclusive publication-root lock" in deploy
    assert re.search(
        r"TRACE must return the bounded `no-store` 405 method\s+artifact",
        deploy,
    )
    assert "Room and TRACE requests must return" not in deploy


def test_fallback_contracts_are_bounded_credential_free_and_non_indexable():
    html = read(FALLBACK / "query-unavailable.html")

    assert '<meta name="robots" content="noindex,nofollow,noarchive">' in html
    assert "local query service is unavailable" in html.lower()
    assert "<script" not in html.lower()
    for stem, error in API_FALLBACKS.items():
        payload = json.loads(read(FALLBACK / f"{stem}.json"))
        plain = (FALLBACK / f"{stem}.txt").read_bytes()
        assert payload["error"] == error
        assert payload["freshness"] == "not_observed"
        assert set(payload) == {
            "contract_version",
            "error",
            "message",
            "freshness",
        }
        assert plain == text_bytes(payload)
        assert len(plain) <= 1024
        assert len(json.dumps(payload).encode()) <= 1024


def test_static_release_guard_requires_every_generated_route_artifact():
    assert {
        "changes/index.html",
        "api/v1/status",
        "api/v1/incidents",
        "api/v1/changes",
        "api/v1/methodology",
        *(
            f"errors/{stem}.{suffix}"
            for stem in API_FALLBACKS
            for suffix in ("json", "txt")
        ),
    } <= guards.STATIC_RELEASE_FILES


def test_payload_guard_accepts_a_declared_but_unobserved_optional_branch(tmp_path):
    ticks = tmp_path / "ticks.jsonl"
    write_ticks(ticks, tick("2026-08-30T00:00:00Z"))

    assert (
        guards.guard_payload_contract(
            read(ROOT / "index.html"), ROOT / "derive.py", ticks
        )
        == []
    )


def test_payload_guard_still_rejects_an_undeclared_field(tmp_path):
    ticks = tmp_path / "ticks.jsonl"
    write_ticks(ticks, tick("2026-08-30T00:00:00Z"))
    html = "<script>const value = point.never_emitted;</script>"

    assert guards.guard_payload_contract(html, ROOT / "derive.py", ticks) == [
        "the page reads `point.never_emitted` but the deriver never emits it "
        "(producer/consumer drift)"
    ]


def test_complete_built_tree_passes_the_static_release_guard(tmp_path):
    ticks = tmp_path / "ticks.jsonl"
    telemetry = tmp_path / "telemetry.sqlite3"
    write_ticks(ticks, tick("2026-08-30T00:00:00Z"))
    with telemetry_database(telemetry):
        pass
    release = build_release(
        ticks,
        telemetry,
        tmp_path / "public",
        ROOT / "index.html",
        derived_at="2026-08-30T00:01:00Z",
        published_at="2026-08-30T00:01:00Z",
    )
    shutil.copytree(FALLBACK, release / "errors")

    assert guards.guard_static_release(release) == []
    status_text = release / "api/v1/status.txt"
    original_status_text = status_text.read_bytes()
    status_text.write_bytes(b"semantically divergent\n")
    findings = guards.guard_static_release(release)
    assert any(
        "status.txt" in finding and "text_bytes" in finding for finding in findings
    )
    status_text.write_bytes(original_status_text)

    status_extensionless = release / "api/v1/status"
    original_status_extensionless = status_extensionless.read_bytes()
    status_extensionless.write_bytes(b"semantically divergent\n")
    findings = guards.guard_static_release(release)
    assert any(
        "api/v1/status`" in finding and "text_bytes" in finding for finding in findings
    )
    status_extensionless.write_bytes(original_status_extensionless)
    script_path = release / "assets/site.js"
    script = script_path.read_text(encoding="utf-8")
    script_path.write_text(
        script + "\ndocument.body.innerHTML = location.search;\n", encoding="utf-8"
    )
    findings = guards.guard_static_release(release)
    assert any("innerHTML" in finding for finding in findings)
    script_path.write_text(script, encoding="utf-8")

    index_path = release / "index.html"
    index_source = index_path.read_text(encoding="utf-8")
    index_path.write_text(
        index_source.replace(
            "</body>",
            "<script>document.body.innerHTML = location.search;</script></body>",
        ),
        encoding="utf-8",
    )
    findings = guards.guard_static_release(release)
    assert any("innerHTML" in finding for finding in findings)
    index_path.write_text(index_source, encoding="utf-8")

    openapi_path = release / "openapi.json"
    openapi = json.loads(openapi_path.read_text(encoding="utf-8"))
    del openapi["paths"]["/api/v1/status"]["head"]
    openapi_path.write_text(json.dumps(openapi), encoding="utf-8")
    findings = guards.guard_static_release(release)
    assert any("GET and HEAD" in finding for finding in findings)

    agent_path = release / ".well-known/agent.json"
    agent = json.loads(agent_path.read_text(encoding="utf-8"))
    agent["methods"] = ["GET"]
    agent_path.write_text(json.dumps(agent), encoding="utf-8")
    findings = guards.guard_static_release(release)
    assert any("GET and HEAD" in finding for finding in findings)

    llms_path = release / "llms.txt"
    llms = llms_path.read_text(encoding="utf-8")
    llms_path.write_text(
        llms.replace("/api/v1/rooms/{room_id}", "/room-evidence-omitted"),
        encoding="utf-8",
    )
    findings = guards.guard_static_release(release)
    assert any("llms.txt" in finding and "{room_id}" in finding for finding in findings)

    openapi_path.unlink()
    findings = guards.guard_static_release(release)
    assert any("openapi.json" in finding for finding in findings)


def test_shell_syntax_when_bash_is_available():
    script = str(ROOT / "rebuild.sh")
    if os.name == "nt":
        git_bash = Path("C:/Program Files/Git/bin/bash.exe")
        bash = str(git_bash) if git_bash.is_file() else None
        script = script.replace("\\", "/")
    else:
        bash = shutil.which("bash")
    if bash is None:
        pytest.skip(
            "bash is unavailable; rebuild.sh received structural validation only"
        )
    result = subprocess.run(
        [bash, "-n", script],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_systemd_unit_syntax_when_systemd_analyze_is_available(tmp_path):
    analyzer = shutil.which("systemd-analyze")
    if analyzer is None or os.name == "nt":
        pytest.skip(
            "systemd-analyze is unavailable; unit tests are structural, not runtime validation"
        )
    rebuild = tmp_path / "rebuild.sh"
    shutil.copyfile(ROOT / "rebuild.sh", rebuild)
    rebuild.chmod(0o755)
    units = []
    for source in sorted(SYSTEMD.glob("*")):
        content = read(source)
        if source.name == "technocore-observatory-rebuild.service":
            target = "ExecStart=/home/technocore/observatory/rebuild.sh"
            assert target in content
            content = content.replace(target, f"ExecStart={rebuild}")
        staged = tmp_path / source.name
        staged.write_text(content, encoding="utf-8")
        staged.chmod(0o644)
        units.append(str(staged))
    result = subprocess.run(
        [analyzer, "verify", *units],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_nginx_runtime_validation_is_explicitly_deferred_on_windows():
    if shutil.which("nginx") is None or os.name == "nt":
        pytest.skip(
            "nginx is unavailable; tests validate structure only and do not claim nginx -t"
        )
    pytest.skip(
        "the vhost references target-only TLS and ACME files; run nginx -t on the target"
    )
