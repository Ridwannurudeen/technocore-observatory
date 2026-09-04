#!/bin/sh
set -eu

if [ "$#" -gt 3 ]; then
    echo "usage: rebuild.sh [ticks.jsonl [telemetry.sqlite3 [public-root]]]" >&2
    exit 2
fi

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ticks_path=${1:-/home/technocore/observatory/ticks.jsonl}
telemetry_path=${2:-/home/technocore/observatory/telemetry.sqlite3}
public_root=${3:-/opt/technocore-observatory}
python_bin=${OBSERVATORY_PYTHON:-/usr/bin/python3}
max_release_count=1008
max_release_bytes=2147483648

mkdir -p "$public_root/releases"
public_root=$(CDPATH= cd -- "$public_root" && pwd -P)
releases_root=$(CDPATH= cd -- "$public_root/releases" && pwd -P)

if ! command -v flock >/dev/null 2>&1; then
    echo "flock is required to serialize release rebuilds" >&2
    exit 1
fi
exec 9<"$public_root"
if ! flock -n 9; then
    echo "another release rebuild is already running for $public_root" >&2
    exit 75
fi

release_maintenance() {
    "$python_bin" - "$@" <<'PY_RELEASE_MAINTENANCE'
import os
import re
import shutil
import sys
from pathlib import Path


RELEASE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
BUILDING_NAME = re.compile(r"\.building-[A-Za-z0-9_][A-Za-z0-9._-]{0,127}")
UNPUBLISHED_NAME = re.compile(
    r"\.unpublished-([A-Za-z0-9][A-Za-z0-9._-]{0,127})"
)


def fail(message):
    raise SystemExit(message)


def release_root(value):
    root = Path(value).resolve(strict=True)
    if not root.is_dir():
        fail(f"release root is not a directory: {root}")
    return root


def release_directory(value, root):
    candidate = Path(value)
    if candidate.is_symlink():
        fail(f"refusing symlink release target: {candidate}")
    resolved = candidate.resolve(strict=True)
    if (
        not resolved.is_dir()
        or resolved.parent != root
        or RELEASE_NAME.fullmatch(resolved.name) is None
    ):
        fail(f"release target is not an exact child of {root}: {resolved}")
    return resolved


def staging_directory(value, root):
    candidate = Path(value)
    if candidate.is_symlink():
        fail(f"refusing symlink staging target: {candidate}")
    resolved = candidate.resolve(strict=True)
    if (
        not resolved.is_dir()
        or resolved.parent != root
        or BUILDING_NAME.fullmatch(resolved.name) is None
    ):
        fail(f"staging target is not an exact child of {root}: {resolved}")
    return resolved


def current_release(value, root):
    candidate = Path(value)
    if not candidate.is_symlink():
        if candidate.exists():
            fail(f"current path is not a symlink: {candidate}")
        return None
    try:
        resolved = candidate.resolve(strict=True)
    except OSError as error:
        fail(f"current symlink cannot be resolved: {error}")
    return release_directory(resolved, root)


def unpublished_sidecar(value, root):
    candidate = Path(value)
    if candidate.is_symlink():
        fail(f"refusing symlink unpublished sidecar: {candidate}")
    try:
        resolved = candidate.resolve(strict=True)
    except OSError as error:
        fail(f"unpublished sidecar cannot be resolved: {error}")
    match = UNPUBLISHED_NAME.fullmatch(resolved.name)
    if not resolved.is_file() or resolved.parent != root or match is None:
        fail(f"unpublished sidecar is not an exact file child of {root}: {resolved}")
    return resolved, match.group(1)


def sidecar_for_release(release, root):
    sidecar, release_name = unpublished_sidecar(
        root / f".unpublished-{release.name}", root
    )
    if release_name != release.name:
        fail(f"unpublished sidecar does not match release: {release}")
    return sidecar


def recovery_candidates(root, current):
    active = current_release(current, root)
    staging = []
    unpublished = []
    for entry in root.iterdir():
        if (
            not entry.is_symlink()
            and entry.is_dir()
            and BUILDING_NAME.fullmatch(entry.name) is not None
        ):
            staging.append(staging_directory(entry, root))
        if UNPUBLISHED_NAME.fullmatch(entry.name) is None:
            continue
        sidecar, release_name = unpublished_sidecar(entry, root)
        release = root / release_name
        if release.is_symlink():
            fail(f"refusing symlink unpublished release: {release}")
        unpublished.append(
            (sidecar, release_directory(release, root) if release.exists() else None)
        )
    return active, staging, unpublished


def recover(root, current):
    active, staging, unpublished = recovery_candidates(root, current)
    for sidecar, release in unpublished:
        if release is None:
            unpublished_sidecar(sidecar, root)[0].unlink()
        elif release == active:
            sidecar_for_release(release_directory(release, root), root).unlink()
        else:
            shutil.rmtree(release_directory(release, root))
            unpublished_sidecar(sidecar, root)[0].unlink()
    for candidate in staging:
        shutil.rmtree(staging_directory(candidate, root))


def cleanup_staging(root):
    candidates = []
    for entry in root.iterdir():
        if (
            entry.is_symlink()
            or not entry.is_dir()
            or BUILDING_NAME.fullmatch(entry.name) is None
        ):
            continue
        candidates.append(staging_directory(entry, root))
    for candidate in candidates:
        shutil.rmtree(staging_directory(candidate, root))


def apparent_size(root):
    total = 0
    for directory, names, files in os.walk(root, followlinks=False):
        directory = Path(directory)
        for name in list(names):
            path = directory / name
            if path.is_symlink():
                total += path.lstat().st_size
                names.remove(name)
        for name in files:
            total += (directory / name).lstat().st_size
    return total


operation = sys.argv[1]
root = release_root(sys.argv[2])

if operation == "validate" and len(sys.argv) == 4:
    release_directory(sys.argv[3], root)
elif operation == "delete" and len(sys.argv) == 4:
    shutil.rmtree(release_directory(sys.argv[3], root))
elif operation == "recover" and len(sys.argv) == 4:
    recover(root, sys.argv[3])
elif operation == "validate-unpublished" and len(sys.argv) == 4:
    release = release_directory(sys.argv[3], root)
    sidecar_for_release(release, root)
elif operation == "discard-unpublished" and len(sys.argv) == 4:
    release = release_directory(sys.argv[3], root)
    sidecar = sidecar_for_release(release, root)
    shutil.rmtree(release)
    unpublished_sidecar(sidecar, root)[0].unlink()
elif operation == "publish" and len(sys.argv) == 5:
    release = release_directory(sys.argv[3], root)
    if current_release(sys.argv[4], root) != release:
        fail(f"refusing to publish a release that is not current: {release}")
    sidecar_for_release(release, root).unlink()
elif operation == "prune" and len(sys.argv) == 7:
    cleanup_staging(root)
    active = release_directory(sys.argv[3], root)
    previous = release_directory(sys.argv[4], root) if sys.argv[4] else None
    maximum_count = int(sys.argv[5])
    maximum_bytes = int(sys.argv[6])
    if maximum_count < 1 or maximum_bytes < 1:
        fail("retention limits must be positive")

    releases = []
    for entry in root.iterdir():
        if (
            entry.is_symlink()
            or not entry.is_dir()
            or RELEASE_NAME.fullmatch(entry.name) is None
        ):
            continue
        releases.append(release_directory(entry, root))
    releases.sort(key=lambda path: (path.stat().st_mtime_ns, path.name), reverse=True)

    protected = {active}
    if previous is not None:
        protected.add(previous)
    sizes = {path: apparent_size(path) for path in releases}
    retained = set(protected)
    retained_bytes = sum(sizes[path] for path in protected)
    capacity_exhausted = False
    discarded = []
    for candidate in releases:
        if candidate in retained:
            continue
        size = sizes[candidate]
        if (
            capacity_exhausted
            or len(retained) >= maximum_count
            or retained_bytes + size > maximum_bytes
        ):
            capacity_exhausted = True
            discarded.append(candidate)
            continue
        retained.add(candidate)
        retained_bytes += size

    for candidate in discarded:
        shutil.rmtree(release_directory(candidate, root))
else:
    fail("invalid release maintenance operation")
PY_RELEASE_MAINTENANCE
}

release_maintenance recover "$releases_root" "$public_root/current"

ledger_copy=$(mktemp)
trap 'rm -f -- "$ledger_copy"' 0
if [ ! -f "${ticks_path}.lock" ]; then
    echo "ledger lock is unavailable: ${ticks_path}.lock" >&2
    exit 1
fi
exec 8<"${ticks_path}.lock"
if ! flock 8; then
    echo "ledger lock is unavailable: ${ticks_path}.lock" >&2
    exit 1
fi
cp -- "$ticks_path" "$ledger_copy"
exec 8<&-

release_path=$(
    "$python_bin" "$script_dir/build_site.py" \
        "$ledger_copy" \
        "$telemetry_path" \
        "$public_root" \
        --template "$script_dir/index.html"
)

next_link="$public_root/.current.$$"
published=0
is_active_release() {
    current_path=$(CDPATH= cd -- "$public_root/current" 2>/dev/null && pwd -P) || return 1
    [ "$current_path" = "$release_path" ]
}
cleanup() {
    status=$?
    trap - 0 HUP INT TERM
    rm -f -- "$ledger_copy" || :
    rm -f -- "$next_link" || :
    if [ "$published" -eq 0 ] && ! is_active_release; then
        release_maintenance discard-unpublished "$releases_root" "$release_path" || \
            echo "failed to clean unpublished release: $release_path" >&2
    fi
    exit "$status"
}
trap cleanup 0
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

case "$release_path" in
    "$releases_root"/*) ;;
    *)
        echo "build_site.py returned a release outside $releases_root" >&2
        exit 1
        ;;
esac

test -d "$release_path"
release_path=$(CDPATH= cd -- "$release_path" && pwd -P)
release_maintenance validate-unpublished "$releases_root" "$release_path"

mkdir -m 0755 "$release_path/errors"
cp "$script_dir"/deploy/fallback/* "$release_path/errors/"
chmod 0644 "$release_path"/errors/*

"$python_bin" "$script_dir/guards.py" \
    --html "$release_path/observatory/index.html" \
    --derive "$script_dir/derive.py" \
    --ticks "$ledger_copy" \
    --site-root "$release_path"

previous_release=""
if [ -e "$public_root/current" ] || [ -L "$public_root/current" ]; then
    if [ ! -L "$public_root/current" ]; then
        echo "$public_root/current is not a symlink" >&2
        exit 1
    fi
    previous_release=$(CDPATH= cd -- "$public_root/current" && pwd -P)
    release_maintenance validate "$releases_root" "$previous_release"
fi

ln -s "releases/$(basename "$release_path")" "$next_link"
mv -Tf "$next_link" "$public_root/current"
release_maintenance publish "$releases_root" "$release_path" "$public_root/current"
published=1

release_maintenance prune \
    "$releases_root" \
    "$release_path" \
    "$previous_release" \
    "$max_release_count" \
    "$max_release_bytes"

trap - 0 HUP INT TERM
printf '%s\n' "$release_path"
