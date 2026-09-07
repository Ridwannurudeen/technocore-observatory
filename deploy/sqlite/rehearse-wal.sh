#!/usr/bin/env bash
set -u
set -o pipefail

readonly PINNED_SQLITE_VERSION="3.53.4"
readonly OBSERVATORY_ROOT="/home/technocore/observatory"
readonly SQLITE_LIBRARY_DIRECTORY="${OBSERVATORY_ROOT}/lib"
readonly SQLITE_LIBRARY="${SQLITE_LIBRARY_DIRECTORY}/libsqlite3.so.0"

read_succeeds=0
shm_group_readable=0
version_assertion=0
checkpoint_on_close=0
switch_back_to_delete=0
scratch_directory=""
writer_pid=""
writer_release=""

report_results() {
    if [ "$read_succeeds" -eq 1 ]; then
        echo "PASS: read succeeds during write"
    else
        echo "FAIL: read succeeds during write"
    fi
    if [ "$shm_group_readable" -eq 1 ]; then
        echo "PASS: -shm group-readable"
    else
        echo "FAIL: -shm group-readable"
    fi
    if [ "$version_assertion" -eq 1 ]; then
        echo "PASS: version assertion"
    else
        echo "FAIL: version assertion"
    fi
    if [ "$checkpoint_on_close" -eq 1 ]; then
        echo "PASS: checkpoint on close"
    else
        echo "FAIL: checkpoint on close"
    fi
    if [ "$switch_back_to_delete" -eq 1 ]; then
        echo "PASS: switch back to DELETE succeeds"
    else
        echo "FAIL: switch back to DELETE succeeds"
    fi
}

finish() {
    report_results
    if [ "$read_succeeds" -eq 1 ] \
        && [ "$shm_group_readable" -eq 1 ] \
        && [ "$version_assertion" -eq 1 ] \
        && [ "$checkpoint_on_close" -eq 1 ] \
        && [ "$switch_back_to_delete" -eq 1 ]; then
        exit 0
    fi
    exit 1
}

cleanup() {
    if [ -n "$writer_pid" ] && kill -0 "$writer_pid" 2>/dev/null; then
        if [ -n "$writer_release" ]; then
            touch -- "$writer_release" 2>/dev/null || true
        fi
        kill "$writer_pid" 2>/dev/null || true
        wait "$writer_pid" 2>/dev/null || true
    fi
    if [ -n "$scratch_directory" ]; then
        case "$scratch_directory" in
            "${OBSERVATORY_ROOT}"/.wal-rehearsal.*)
                rm -rf -- "$scratch_directory"
                ;;
            *)
                echo "refusing to remove unexpected scratch path: $scratch_directory" >&2
                ;;
        esac
    fi
}

trap cleanup EXIT
trap 'exit 130' HUP INT TERM

if [ "$#" -ne 1 ]; then
    echo "usage: sudo $0 /absolute/path/to/signers.sqlite3" >&2
    finish
fi
if [ "$(id -u)" -ne 0 ]; then
    echo "the WAL rehearsal must run as root so it can enter both service identities" >&2
    finish
fi
for command in readlink mktemp chown chmod stat sudo systemd-run; do
    if ! command -v "$command" >/dev/null 2>&1; then
        echo "required command is unavailable: $command" >&2
        finish
    fi
done
if ! id technocore >/dev/null 2>&1 || ! id technocore-query >/dev/null 2>&1; then
    echo "both technocore and technocore-query identities must exist" >&2
    finish
fi
if [ ! -x /usr/bin/python3 ] || [ ! -r "$SQLITE_LIBRARY" ]; then
    echo "the Python runtime or vendored SQLite library is unavailable" >&2
    finish
fi

source_database=$(readlink -f -- "$1")
if [ -z "$source_database" ] || [ ! -f "$source_database" ]; then
    echo "signer database source is not a regular file: $1" >&2
    finish
fi

version_assertion=1
for runtime_user in technocore technocore-query; do
    if ! sudo -u "$runtime_user" -- \
        env LD_LIBRARY_PATH="$SQLITE_LIBRARY_DIRECTORY" \
        /usr/bin/python3 -c '
import sqlite3
import sys

expected = sys.argv[1]
if sqlite3.sqlite_version != expected:
    raise SystemExit(
        f"vendored SQLite version mismatch: expected {expected}, "
        f"loaded {sqlite3.sqlite_version}"
    )
options = {row[0] for row in sqlite3.connect(":memory:").execute("PRAGMA compile_options")}
if "ENABLE_FTS5" not in options:
    raise SystemExit("vendored SQLite is missing ENABLE_FTS5")
' "$PINNED_SQLITE_VERSION"; then
        version_assertion=0
    fi
done
if [ "$version_assertion" -ne 1 ]; then
    echo "the vendored SQLite assertion failed; the scratch database was not created" >&2
    finish
fi

scratch_directory=$(mktemp -d "${OBSERVATORY_ROOT}/.wal-rehearsal.XXXXXXXX")
if [ -z "$scratch_directory" ] || [ ! -d "$scratch_directory" ]; then
    echo "could not create the WAL rehearsal directory" >&2
    finish
fi
chown technocore:technocore "$scratch_directory"
chmod 0750 "$scratch_directory"

copy_database="${scratch_directory}/signers.sqlite3"
if ! sudo -u technocore -- sh -c '
    umask 0027
    exec env LD_LIBRARY_PATH="$1" /usr/bin/python3 - "$2" "$3"
' sh "$SQLITE_LIBRARY_DIRECTORY" "$source_database" "$copy_database" <<'PY_COPY'; then
import sqlite3
import sys
from pathlib import Path


source = Path(sys.argv[1]).resolve(strict=True)
destination = Path(sys.argv[2])
source_uri = source.as_uri() + "?mode=ro"
with (
    sqlite3.connect(source_uri, uri=True, timeout=30.0) as source_connection,
    sqlite3.connect(destination) as destination_connection,
):
    source_connection.backup(destination_connection)
PY_COPY
    echo "could not make a consistent read-only backup of the signer database" >&2
    finish
fi
chown technocore:technocore "$copy_database"
chmod 0640 "$copy_database"

writer_ready="${scratch_directory}/writer.ready"
writer_release="${scratch_directory}/writer.release"
writer_log="${scratch_directory}/writer.log"
probe_name="zzzwalrehearsalprobe${$}zzz"

sudo -u technocore -- sh -c '
    umask 0027
    exec env LD_LIBRARY_PATH="$1" /usr/bin/python3 - "$2" "$3" "$4" "$5" "$6"
' sh \
    "$SQLITE_LIBRARY_DIRECTORY" \
    "$copy_database" \
    "$writer_ready" \
    "$writer_release" \
    "$PINNED_SQLITE_VERSION" \
    "$probe_name" \
    >"$writer_log" 2>&1 <<'PY_WRITER' &
import hashlib
import sqlite3
import sys
import time
from pathlib import Path


database = Path(sys.argv[1])
ready = Path(sys.argv[2])
release = Path(sys.argv[3])
expected_version = sys.argv[4]
probe_name = sys.argv[5]

if sqlite3.sqlite_version != expected_version:
    raise SystemExit(
        f"vendored SQLite version mismatch: expected {expected_version}, "
        f"loaded {sqlite3.sqlite_version}"
    )

connection = sqlite3.connect(database, timeout=5.0)
try:
    mode = connection.execute("PRAGMA journal_mode = WAL").fetchone()[0]
    if mode.lower() != "wal":
        raise SystemExit(f"scratch database refused WAL mode: {mode!r}")
    connection.execute("PRAGMA synchronous = NORMAL")
    connection.execute("PRAGMA wal_autocheckpoint = 1000")
    if connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'room_search'"
    ).fetchone() != (1,):
        raise SystemExit("scratch database has no room_search table")
    connection.execute(
        "CREATE TABLE wal_rehearsal_probe (singleton INTEGER PRIMARY KEY, payload BLOB)"
    )
    connection.commit()
    connection.execute("PRAGMA cache_size = 10")
    connection.execute("PRAGMA cache_spill = ON")
    connection.execute("BEGIN IMMEDIATE")
    connection.execute(
        "INSERT INTO wal_rehearsal_probe VALUES (1, zeroblob(1048576))"
    )
    created_seq = connection.execute(
        "SELECT COALESCE(MAX(created_seq), -1) + 1 FROM room_ledger"
    ).fetchone()[0]
    if not isinstance(created_seq, int) or created_seq < 0:
        raise SystemExit("could not allocate a rehearsal room sequence")
    room_sha256 = hashlib.sha256(probe_name.encode("utf-8")).hexdigest()
    connection.execute(
        """
        INSERT INTO room_ledger (
            created_seq,
            name,
            room_id,
            room_sha256,
            created_at,
            first_observed_at,
            last_listed_at
        )
        VALUES (?, ?, ?, ?, ?, ?, NULL)
        """,
        (
            created_seq,
            probe_name,
            room_sha256[:16],
            room_sha256,
            "2026-09-07T00:00:00Z",
            "2026-09-07T00:00:00Z",
        ),
    )
    ready.write_text("writer transaction is open\n", encoding="utf-8")
    deadline = time.monotonic() + 60.0
    while not release.exists():
        if time.monotonic() >= deadline:
            raise SystemExit("reader did not release the writer within 60 seconds")
        time.sleep(0.05)
    connection.commit()
finally:
    connection.close()
PY_WRITER
writer_pid=$!

attempt=0
while [ ! -f "$writer_ready" ] && kill -0 "$writer_pid" 2>/dev/null; do
    if [ "$attempt" -ge 300 ]; then
        break
    fi
    sleep 0.1
    attempt=$((attempt + 1))
done
if [ ! -f "$writer_ready" ]; then
    echo "the scratch writer did not enter its apply-style transaction" >&2
    touch -- "$writer_release" 2>/dev/null || true
    wait "$writer_pid" 2>/dev/null || true
    writer_pid=""
    sed -n '1,20p' "$writer_log" >&2
    finish
fi

shm_path="${copy_database}-shm"
if [ -f "$shm_path" ]; then
    shm_permissions=$(stat -c '%A' "$shm_path")
    shm_group=$(stat -c '%G' "$shm_path")
    if [ "$shm_group" = "technocore" ] \
        && [ "${shm_permissions:4:1}" = "r" ] \
        && sudo -u technocore-query -- test -r "$shm_path" \
        && sudo -u technocore-query -- test ! -w "$scratch_directory"; then
        shm_group_readable=1
    fi
fi

reader_unit="technocore-wal-rehearsal-reader-$$"
if systemd-run \
    --quiet \
    --wait \
    --pipe \
    --collect \
    --unit="$reader_unit" \
    --service-type=exec \
    --working-directory="$OBSERVATORY_ROOT" \
    --property=User=technocore-query \
    --property=Group=technocore-query \
    --property=SupplementaryGroups=technocore \
    --property=UMask=0077 \
    --property=MemoryMax=256M \
    --property=NoNewPrivileges=true \
    --property=PrivateTmp=true \
    --property=PrivateDevices=true \
    --property=ProtectSystem=strict \
    --property=ProtectHome=read-only \
    --property=ProtectKernelTunables=true \
    --property=ProtectKernelModules=true \
    --property=ProtectControlGroups=true \
    --property=RestrictSUIDSGID=true \
    --property=LockPersonality=true \
    --property=CapabilityBoundingSet= \
    --property=AmbientCapabilities= \
    --property="ReadOnlyPaths=${OBSERVATORY_ROOT} /opt/technocore-observatory" \
    --property="RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6" \
    --property=IPAddressDeny=any \
    --property=IPAddressAllow=localhost \
    --setenv=LD_LIBRARY_PATH="$SQLITE_LIBRARY_DIRECTORY" \
    /usr/bin/python3 - \
    "$copy_database" \
    "$PINNED_SQLITE_VERSION" \
    "$probe_name" <<'PY_READER'; then
import os
import sqlite3
import sys
from pathlib import Path


database = Path(sys.argv[1]).resolve(strict=True)
expected_version = sys.argv[2]
probe_name = sys.argv[3]
if sqlite3.sqlite_version != expected_version:
    raise SystemExit(
        f"vendored SQLite version mismatch: expected {expected_version}, "
        f"loaded {sqlite3.sqlite_version}"
    )
if os.access(database.parent, os.W_OK):
    raise SystemExit("confined query identity can write the scratch directory")
connection = sqlite3.connect(
    database.as_uri() + "?mode=ro",
    uri=True,
    timeout=0.5,
)
try:
    connection.execute("PRAGMA query_only = ON")
    mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
    if mode.lower() != "wal":
        raise SystemExit(f"read-only connection did not observe WAL mode: {mode!r}")
    row = connection.execute(
        "SELECT COUNT(*) FROM room_search WHERE room_search MATCH ?",
        (f'"{probe_name}"',),
    ).fetchone()
    if row != (0,):
        raise SystemExit(
            "room_search exposed the writer's uncommitted rehearsal room"
        )
finally:
    connection.close()
PY_READER
    read_succeeds=1
fi

touch -- "$writer_release"
if wait "$writer_pid"; then
    writer_succeeded=1
else
    writer_succeeded=0
    echo "the scratch writer failed" >&2
    sed -n '1,20p' "$writer_log" >&2
fi
writer_pid=""

if [ "$writer_succeeded" -eq 1 ] \
    && [ ! -e "${copy_database}-wal" ] \
    && [ ! -e "${copy_database}-shm" ] \
    && sudo -u technocore -- \
        env LD_LIBRARY_PATH="$SQLITE_LIBRARY_DIRECTORY" \
        /usr/bin/python3 - "$copy_database" "$probe_name" <<'PY_CHECKPOINT'
import sqlite3
import sys


with sqlite3.connect(sys.argv[1]) as connection:
    probe_name = sys.argv[2]
    mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
    marker = connection.execute(
        "SELECT length(payload) FROM wal_rehearsal_probe WHERE singleton = 1"
    ).fetchone()
    indexed_room = connection.execute(
        "SELECT COUNT(*) FROM room_search WHERE room_search MATCH ?",
        (f'"{probe_name}"',),
    ).fetchone()
if mode.lower() != "wal" or marker != (1048576,) or indexed_room != (1,):
    raise SystemExit("the committed rehearsal write was not checkpointed on close")
PY_CHECKPOINT
then
    checkpoint_on_close=1
fi

if sudo -u technocore -- sh -c '
    umask 0027
    exec env LD_LIBRARY_PATH="$1" /usr/bin/python3 - "$2" "$3"
' sh "$SQLITE_LIBRARY_DIRECTORY" "$copy_database" "$PINNED_SQLITE_VERSION" <<'PY_DELETE'; then
import sqlite3
import sys
from pathlib import Path


database = Path(sys.argv[1])
expected_version = sys.argv[2]
if sqlite3.sqlite_version != expected_version:
    raise SystemExit(
        f"vendored SQLite version mismatch: expected {expected_version}, "
        f"loaded {sqlite3.sqlite_version}"
    )
connection = sqlite3.connect(database, timeout=5.0)
try:
    mode = connection.execute("PRAGMA journal_mode = DELETE").fetchone()[0]
    if mode.lower() != "delete":
        raise SystemExit(f"scratch database refused DELETE mode: {mode!r}")
    result = connection.execute("PRAGMA integrity_check").fetchall()
    if result != [("ok",)]:
        raise SystemExit(f"scratch database failed integrity_check: {result!r}")
    connection.execute("DROP TABLE wal_rehearsal_probe")
    connection.commit()
finally:
    connection.close()
for suffix in ("-wal", "-shm"):
    if database.with_name(database.name + suffix).exists():
        raise SystemExit(f"sidecar remains after DELETE switch: {suffix}")
with sqlite3.connect(database.as_uri() + "?mode=ro", uri=True) as readonly:
    if readonly.execute("PRAGMA journal_mode").fetchone()[0].lower() != "delete":
        raise SystemExit("scratch database did not persist DELETE mode")
PY_DELETE
    switch_back_to_delete=1
fi

finish
