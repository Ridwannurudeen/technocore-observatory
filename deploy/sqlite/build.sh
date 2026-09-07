#!/usr/bin/env bash
set -euo pipefail

readonly SQLITE_VERSION="3.53.4"
readonly SQLITE_ARCHIVE="sqlite-amalgamation-3530400.zip"
readonly SQLITE_URL="https://sqlite.org/2026/${SQLITE_ARCHIVE}"
readonly SQLITE_SHA3_256="628a44cfe82c66aed1ccbbe85a562d2e33ebe64b3288981ed76285612227934e"
readonly INSTALL_DIR="/home/technocore/observatory/lib"

work_dir="$(mktemp -d)"
trap 'rm -rf -- "$work_dir"' EXIT

archive_path="${work_dir}/${SQLITE_ARCHIVE}"

python3 - "$SQLITE_URL" "$archive_path" "$SQLITE_SHA3_256" <<'PY'
import hashlib
from pathlib import Path
import sys
import urllib.request

url, destination_text, expected_hash = sys.argv[1:]
destination = Path(destination_text)

with urllib.request.urlopen(url) as response, destination.open("wb") as output:
    while chunk := response.read(1024 * 1024):
        output.write(chunk)

actual_hash = hashlib.sha3_256(destination.read_bytes()).hexdigest()
if actual_hash != expected_hash:
    raise SystemExit(
        f"SHA3-256 mismatch for {url}: expected {expected_hash}, got {actual_hash}"
    )
PY

python3 - "$archive_path" "$work_dir" <<'PY'
from pathlib import Path
import sys
import zipfile

archive = Path(sys.argv[1])
destination = Path(sys.argv[2])
with zipfile.ZipFile(archive) as bundle:
    bundle.extractall(destination)
PY

source_dir="${work_dir}/sqlite-amalgamation-3530400"
output_library="${work_dir}/libsqlite3.so.0"

gcc -O2 -fPIC -shared \
    -DSQLITE_ENABLE_FTS5 \
    -DSQLITE_ENABLE_RTREE \
    -DSQLITE_ENABLE_MATH_FUNCTIONS \
    -DSQLITE_ENABLE_DBSTAT_VTAB \
    -DSQLITE_SECURE_DELETE \
    -DSQLITE_THREADSAFE=1 \
    -DSQLITE_ENABLE_COLUMN_METADATA \
    "${source_dir}/sqlite3.c" \
    -o "$output_library" \
    -lm -ldl -lpthread

mkdir -p "$INSTALL_DIR"
install -m 0755 "$output_library" "${INSTALL_DIR}/libsqlite3.so.0"

LD_LIBRARY_PATH="$INSTALL_DIR${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
    python3 - "$SQLITE_VERSION" <<'PY'
import sqlite3
import sys

expected_version = sys.argv[1]
if sqlite3.sqlite_version != expected_version:
    raise SystemExit(
        f"vendored SQLite version mismatch: expected {expected_version}, "
        f"loaded {sqlite3.sqlite_version}"
    )

required_options = {
    "ENABLE_COLUMN_METADATA",
    "ENABLE_DBSTAT_VTAB",
    "ENABLE_FTS5",
    "ENABLE_MATH_FUNCTIONS",
    "ENABLE_RTREE",
    "SECURE_DELETE",
    "THREADSAFE=1",
}
with sqlite3.connect(":memory:") as connection:
    compile_options = {
        row[0] for row in connection.execute("PRAGMA compile_options").fetchall()
    }
    missing_options = sorted(required_options - compile_options)
    if missing_options:
        raise SystemExit(
            "vendored SQLite is missing compile options: " + ", ".join(missing_options)
        )
    connection.execute("CREATE VIRTUAL TABLE fts_probe USING fts5(content)")
    connection.execute("CREATE VIRTUAL TABLE rtree_probe USING rtree(id, min_x, max_x)")
    connection.execute("SELECT json('{}'), sqrt(4)").fetchone()

print(f"installed SQLite {sqlite3.sqlite_version} at /home/technocore/observatory/lib/libsqlite3.so.0")
PY
