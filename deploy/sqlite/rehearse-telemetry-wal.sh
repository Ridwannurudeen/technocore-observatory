#!/usr/bin/env bash
set -u
set -o pipefail

readonly PINNED_SQLITE_VERSION="3.53.4"
readonly OBSERVATORY_ROOT="/home/technocore/observatory"
readonly SQLITE_LIBRARY_DIRECTORY="${OBSERVATORY_ROOT}/lib"
readonly SQLITE_LIBRARY="${SQLITE_LIBRARY_DIRECTORY}/libsqlite3.so.0"

read_during_write=0
write_during_long_read=0
version_assertion=0
checkpoint_on_close=0
switch_back_to_delete=0
scratch_directory=""
writer_pid=""
long_reader_pid=""
writer_write_release=""
long_reader_release=""
writer_release=""

report_results() {
    if [ "$read_during_write" -eq 1 ]; then
        echo "PASS: read succeeds during write under ReadOnlyPaths"
    else
        echo "FAIL: read succeeds during write under ReadOnlyPaths"
    fi
    if [ "$write_during_long_read" -eq 1 ]; then
        echo "PASS: record_attempt commits during long read"
    else
        echo "FAIL: record_attempt commits during long read"
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
    if [ "$read_during_write" -eq 1 ] \
        && [ "$write_during_long_read" -eq 1 ] \
        && [ "$version_assertion" -eq 1 ] \
        && [ "$checkpoint_on_close" -eq 1 ] \
        && [ "$switch_back_to_delete" -eq 1 ]; then
        exit 0
    fi
    exit 1
}

cleanup() {
    for release in "$writer_write_release" "$long_reader_release" "$writer_release"; do
        if [ -n "$release" ]; then
            touch -- "$release" 2>/dev/null || true
        fi
    done
    if [ -n "$long_reader_pid" ] && kill -0 "$long_reader_pid" 2>/dev/null; then
        kill "$long_reader_pid" 2>/dev/null || true
        wait "$long_reader_pid" 2>/dev/null || true
    fi
    if [ -n "$writer_pid" ] && kill -0 "$writer_pid" 2>/dev/null; then
        kill "$writer_pid" 2>/dev/null || true
        wait "$writer_pid" 2>/dev/null || true
    fi
    if [ -n "$scratch_directory" ]; then
        case "$scratch_directory" in
            "${OBSERVATORY_ROOT}"/.telemetry-wal-rehearsal.*)
                rm -rf -- "$scratch_directory"
                ;;
            *)
                echo "refusing to remove unexpected scratch path: $scratch_directory" >&2
                ;;
        esac
    fi
}

wait_for_file() {
    awaited_path=$1
    owner_pid=$2
    description=$3
    attempt=0
    while [ ! -f "$awaited_path" ] && kill -0 "$owner_pid" 2>/dev/null; do
        if [ "$attempt" -ge 300 ]; then
            break
        fi
        sleep 0.1
        attempt=$((attempt + 1))
    done
    if [ ! -f "$awaited_path" ]; then
        echo "timed out waiting for $description" >&2
        return 1
    fi
    return 0
}

wait_for_log() {
    log_path=$1
    owner_pid=$2
    marker=$3
    description=$4
    attempt=0
    while kill -0 "$owner_pid" 2>/dev/null; do
        if [ -f "$log_path" ] && grep -F -q -- "$marker" "$log_path"; then
            return 0
        fi
        if [ "$attempt" -ge 300 ]; then
            break
        fi
        sleep 0.1
        attempt=$((attempt + 1))
    done
    echo "timed out waiting for $description" >&2
    return 1
}

trap cleanup EXIT
trap 'exit 130' HUP INT TERM

if [ "$#" -ne 1 ]; then
    echo "usage: sudo $0 /absolute/path/to/telemetry.sqlite3" >&2
    finish
fi
if [ "$(id -u)" -ne 0 ]; then
    echo "the telemetry WAL rehearsal must run as root for systemd confinement" >&2
    finish
fi
for command in readlink mktemp chown chmod grep sudo systemd-run; do
    if ! command -v "$command" >/dev/null 2>&1; then
        echo "required command is unavailable: $command" >&2
        finish
    fi
done
if ! id technocore >/dev/null 2>&1; then
    echo "the technocore identity must exist" >&2
    finish
fi
if [ ! -x /usr/bin/python3 ] || [ ! -r "$SQLITE_LIBRARY" ]; then
    echo "the Python runtime or vendored SQLite library is unavailable" >&2
    finish
fi

source_database=$(readlink -f -- "$1")
if [ -z "$source_database" ] || [ ! -f "$source_database" ]; then
    echo "telemetry database source is not a regular file: $1" >&2
    finish
fi

if sudo -u technocore -- sh -c '
    cd "$1" || exit 1
    exec env LD_LIBRARY_PATH="$2" /usr/bin/python3 - "$3"
' sh "$OBSERVATORY_ROOT" "$SQLITE_LIBRARY_DIRECTORY" "$PINNED_SQLITE_VERSION" <<'PY_VERSION'; then
import sqlite3
import sys

from telemetry import assert_pinned_sqlite


expected = sys.argv[1]
assert_pinned_sqlite()
if sqlite3.sqlite_version != expected:
    raise SystemExit(
        f"vendored SQLite version mismatch: expected {expected}, "
        f"loaded {sqlite3.sqlite_version}"
    )
PY_VERSION
    version_assertion=1
else
    echo "the vendored SQLite assertion failed; the scratch database was not created" >&2
    finish
fi

scratch_directory=$(mktemp -d "${OBSERVATORY_ROOT}/.telemetry-wal-rehearsal.XXXXXXXX")
if [ -z "$scratch_directory" ] || [ ! -d "$scratch_directory" ]; then
    echo "could not create the telemetry WAL rehearsal directory" >&2
    finish
fi
chown technocore:technocore "$scratch_directory"
chmod 0750 "$scratch_directory"

copy_database="${scratch_directory}/telemetry.sqlite3"
if ! sudo -u technocore -- sh -c '
    umask 0027
    exec env LD_LIBRARY_PATH="$1" /usr/bin/python3 - "$2" "$3"
' sh "$SQLITE_LIBRARY_DIRECTORY" "$source_database" "$copy_database" <<'PY_COPY'; then
import sqlite3
import sys
from pathlib import Path


source = Path(sys.argv[1]).resolve(strict=True)
destination = Path(sys.argv[2])
with (
    sqlite3.connect(source.as_uri() + "?mode=ro", uri=True, timeout=30.0) as source_connection,
    sqlite3.connect(destination) as destination_connection,
):
    source_connection.backup(destination_connection)
PY_COPY
    echo "could not make a consistent read-only backup of the telemetry database" >&2
    finish
fi
chown technocore:technocore "$copy_database"
chmod 0640 "$copy_database"

writer_ready="${scratch_directory}/writer.ready"
writer_write_release="${scratch_directory}/writer-write.release"
writer_committed="${scratch_directory}/writer.committed"
long_reader_ready="${scratch_directory}/long-reader.ready"
long_reader_release="${scratch_directory}/long-reader.release"
write_during_read_succeeded="${scratch_directory}/write-during-read.succeeded"
writer_release="${scratch_directory}/writer.release"
writer_log="${scratch_directory}/writer.log"

sudo -u technocore -- sh -c '
    umask 0027
    cd "$1" || exit 1
    exec env LD_LIBRARY_PATH="$2" /usr/bin/python3 - "$3" "$4" "$5" "$6" "$7" "$8" "$9"
' sh \
    "$OBSERVATORY_ROOT" \
    "$SQLITE_LIBRARY_DIRECTORY" \
    "$copy_database" \
    "$writer_ready" \
    "$writer_write_release" \
    "$writer_committed" \
    "$long_reader_ready" \
    "$write_during_read_succeeded" \
    "$writer_release" \
    >"$writer_log" 2>&1 <<'PY_WRITER' &
import sqlite3
import sys
import time
from pathlib import Path

from telemetry import TELEMETRY_WAL_AUTOCHECKPOINT_PAGES, TelemetryStore, utc_now


database = Path(sys.argv[1])
ready = Path(sys.argv[2])
write_release = Path(sys.argv[3])
committed = Path(sys.argv[4])
long_reader_ready = Path(sys.argv[5])
write_during_read_succeeded = Path(sys.argv[6])
release = Path(sys.argv[7])


def wait_for(path: Path, description: str) -> None:
    deadline = time.monotonic() + 60.0
    while not path.exists():
        if time.monotonic() >= deadline:
            raise SystemExit(f"timed out waiting for {description}")
        time.sleep(0.05)


store = TelemetryStore(database)
cycle_id = None
try:
    if store.connection.execute("PRAGMA journal_mode").fetchone()[0].lower() != "wal":
        raise SystemExit("TelemetryStore did not retain WAL mode")
    if store.connection.execute("PRAGMA synchronous").fetchone()[0] != 1:
        raise SystemExit("TelemetryStore did not configure synchronous=NORMAL")
    if (
        store.connection.execute("PRAGMA wal_autocheckpoint").fetchone()[0]
        != TELEMETRY_WAL_AUTOCHECKPOINT_PAGES
    ):
        raise SystemExit("TelemetryStore did not configure its WAL autocheckpoint")

    observed_at = utc_now()
    cycle_id = store.start_cycle("collector", observed_at)
    store.connection.execute("BEGIN IMMEDIATE")
    cursor = store.connection.execute(
        """
        INSERT INTO request_attempts (
            cycle_id, route, metered, attempt, observed_at,
            latency_ms, outcome, http_status
        )
        VALUES (?, '/healthz', 0, 1, ?, 1, 'success', 200)
        """,
        (cycle_id, observed_at),
    )
    ready.write_text(f"{cursor.lastrowid}\n", encoding="utf-8")
    wait_for(write_release, "read-during-write release")
    store.connection.commit()
    committed.write_text("writer transaction committed\n", encoding="utf-8")

    wait_for(long_reader_ready, "long reader")
    attempt_id = store.record_attempt(
        cycle_id,
        "/healthz",
        False,
        2,
        utc_now(),
        1.0,
        "success",
        200,
    )
    write_during_read_succeeded.write_text(f"{attempt_id}\n", encoding="utf-8")
    wait_for(release, "writer shutdown release")
    store.finish_cycle(cycle_id, "success")
finally:
    if store.connection.in_transaction:
        store.connection.rollback()
    store.close()
PY_WRITER
writer_pid=$!

if ! wait_for_file "$writer_ready" "$writer_pid" "the open telemetry write transaction"; then
    sed -n '1,30p' "$writer_log" >&2
    finish
fi

reader_unit="technocore-telemetry-wal-reader-$$"
if systemd-run \
    --quiet \
    --wait \
    --pipe \
    --collect \
    --unit="$reader_unit" \
    --service-type=exec \
    --working-directory="$OBSERVATORY_ROOT" \
    --property=User=technocore \
    --property=Group=technocore \
    --property=UMask=0022 \
    --property=MemoryMax=2G \
    --property=NoNewPrivileges=true \
    --property=PrivateTmp=true \
    --property=ProtectSystem=strict \
    --property=ProtectHome=read-only \
    --property=ProtectKernelTunables=true \
    --property=ProtectKernelModules=true \
    --property=ProtectControlGroups=true \
    --property=RestrictSUIDSGID=true \
    --property=LockPersonality=true \
    --property="ReadOnlyPaths=${OBSERVATORY_ROOT}" \
    --property="ReadWritePaths=/opt/technocore-observatory" \
    --setenv=LD_LIBRARY_PATH="$SQLITE_LIBRARY_DIRECTORY" \
    /usr/bin/python3 - \
    "$copy_database" \
    "$writer_ready" \
    "$PINNED_SQLITE_VERSION" <<'PY_READER'; then
import errno
import sqlite3
import sys
from pathlib import Path

from snapshots import load_telemetry


database = Path(sys.argv[1]).resolve(strict=True)
uncommitted_id = int(Path(sys.argv[2]).read_text(encoding="utf-8"))
expected_version = sys.argv[3]
if sqlite3.sqlite_version != expected_version:
    raise SystemExit(
        f"vendored SQLite version mismatch: expected {expected_version}, "
        f"loaded {sqlite3.sqlite_version}"
    )
write_probe = database.parent / "confined-write-probe"
try:
    write_probe.write_text("must fail\n", encoding="utf-8")
except OSError as error:
    if error.errno not in (errno.EACCES, errno.EPERM, errno.EROFS):
        raise
else:
    write_probe.unlink()
    raise SystemExit("rebuild-equivalent reader could write the scratch directory")

loaded = load_telemetry(database)
if any(attempt["id"] == uncommitted_id for attempt in loaded["attempts"]):
    raise SystemExit("read-only loader exposed the uncommitted telemetry attempt")
PY_READER
    read_during_write=1
fi

touch -- "$writer_write_release"
if ! wait_for_file "$writer_committed" "$writer_pid" "the first telemetry commit"; then
    sed -n '1,30p' "$writer_log" >&2
    finish
fi

long_reader_unit="technocore-telemetry-wal-long-reader-$$"
long_reader_log="${scratch_directory}/long-reader.log"
systemd-run \
    --quiet \
    --wait \
    --pipe \
    --collect \
    --unit="$long_reader_unit" \
    --service-type=exec \
    --working-directory="$OBSERVATORY_ROOT" \
    --property=User=technocore \
    --property=Group=technocore \
    --property=UMask=0022 \
    --property=MemoryMax=2G \
    --property=NoNewPrivileges=true \
    --property=PrivateTmp=true \
    --property=ProtectSystem=strict \
    --property=ProtectHome=read-only \
    --property=ProtectKernelTunables=true \
    --property=ProtectKernelModules=true \
    --property=ProtectControlGroups=true \
    --property=RestrictSUIDSGID=true \
    --property=LockPersonality=true \
    --property="ReadOnlyPaths=${OBSERVATORY_ROOT}" \
    --property="ReadWritePaths=/opt/technocore-observatory" \
    --setenv=LD_LIBRARY_PATH="$SQLITE_LIBRARY_DIRECTORY" \
    /usr/bin/python3 - \
    "$copy_database" \
    "$long_reader_release" \
    "$PINNED_SQLITE_VERSION" \
    >"$long_reader_log" 2>&1 <<'PY_LONG_READER' &
import sqlite3
import sys
import time
from pathlib import Path


database = Path(sys.argv[1]).resolve(strict=True)
release = Path(sys.argv[2])
expected_version = sys.argv[3]
if sqlite3.sqlite_version != expected_version:
    raise SystemExit(
        f"vendored SQLite version mismatch: expected {expected_version}, "
        f"loaded {sqlite3.sqlite_version}"
    )
connection = sqlite3.connect(database.as_uri() + "?mode=ro", uri=True, timeout=30.0)
try:
    connection.execute("PRAGMA query_only = ON")
    if connection.execute("PRAGMA journal_mode").fetchone()[0].lower() != "wal":
        raise SystemExit("long reader did not observe WAL mode")
    connection.execute("BEGIN")
    cursor = connection.execute(
        """
        WITH RECURSIVE sequence(value) AS (
            VALUES(1)
            UNION ALL
            SELECT value + 1 FROM sequence WHERE value < 50000
        )
        SELECT request_attempts.id, sequence.value
        FROM request_attempts CROSS JOIN sequence
        LIMIT 50000
        """
    )
    first = cursor.fetchone()
    if first is None:
        raise SystemExit("scratch telemetry database has no attempts for the long read")
    print("READY: long read transaction is open", flush=True)
    deadline = time.monotonic() + 60.0
    while not release.exists():
        if time.monotonic() >= deadline:
            raise SystemExit("writer did not release the long reader within 60 seconds")
        time.sleep(0.05)
    rows = 1 + sum(1 for _ in cursor)
    if rows != 50000:
        raise SystemExit(f"long SELECT returned {rows} rows instead of 50000")
finally:
    connection.close()
PY_LONG_READER
long_reader_pid=$!

if wait_for_log \
    "$long_reader_log" \
    "$long_reader_pid" \
    "READY: long read transaction is open" \
    "the long read transaction"; then
    touch -- "$long_reader_ready"
fi
if [ -f "$long_reader_ready" ] \
    && wait_for_file \
        "$write_during_read_succeeded" \
        "$writer_pid" \
        "record_attempt during the long read" \
    && kill -0 "$long_reader_pid" 2>/dev/null; then
    write_during_long_read=1
fi
touch -- "$long_reader_release"
if ! wait "$long_reader_pid"; then
    write_during_long_read=0
    echo "the confined long reader failed" >&2
    sed -n '1,30p' "$long_reader_log" >&2
fi
long_reader_pid=""

touch -- "$writer_release"
if wait "$writer_pid"; then
    writer_succeeded=1
else
    writer_succeeded=0
    echo "the TelemetryStore writer failed" >&2
    sed -n '1,30p' "$writer_log" >&2
fi
writer_pid=""

if [ "$writer_succeeded" -eq 1 ] \
    && [ ! -e "${copy_database}-wal" ] \
    && [ ! -e "${copy_database}-shm" ] \
    && sudo -u technocore -- \
        env LD_LIBRARY_PATH="$SQLITE_LIBRARY_DIRECTORY" \
        /usr/bin/python3 - \
        "$copy_database" \
        "$writer_ready" \
        "$write_during_read_succeeded" <<'PY_CHECKPOINT'
import sqlite3
import sys
from pathlib import Path


database = Path(sys.argv[1])
attempt_ids = tuple(int(Path(value).read_text(encoding="utf-8")) for value in sys.argv[2:])
connection = sqlite3.connect(database)
try:
    mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
    stored = connection.execute(
        "SELECT id FROM request_attempts WHERE id IN (?, ?) ORDER BY id",
        attempt_ids,
    ).fetchall()
finally:
    connection.close()
if mode.lower() != "wal" or stored != [(value,) for value in sorted(attempt_ids)]:
    raise SystemExit("the committed telemetry writes were not checkpointed on close")
for suffix in ("-wal", "-shm"):
    if database.with_name(database.name + suffix).exists():
        raise SystemExit(f"telemetry sidecar remains after clean close: {suffix}")
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
        raise SystemExit(f"scratch telemetry database refused DELETE mode: {mode!r}")
    result = connection.execute("PRAGMA integrity_check").fetchall()
    if result != [("ok",)]:
        raise SystemExit(f"scratch telemetry database failed integrity_check: {result!r}")
finally:
    connection.close()
for suffix in ("-wal", "-shm"):
    if database.with_name(database.name + suffix).exists():
        raise SystemExit(f"telemetry sidecar remains after DELETE switch: {suffix}")
with sqlite3.connect(database.as_uri() + "?mode=ro", uri=True) as readonly:
    if readonly.execute("PRAGMA journal_mode").fetchone()[0].lower() != "delete":
        raise SystemExit("scratch telemetry database did not persist DELETE mode")
PY_DELETE
    switch_back_to_delete=1
fi

finish
