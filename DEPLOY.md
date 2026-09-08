# Deploying the Observatory

> **Release-code status, 2026-08-31:** these assets are prepared and locally verified. Deployment
> state must be established from the live `current` target and public smoke checks; this operator
> runbook is not, by itself, evidence that a particular release is active.

The verified live boundaries are:

- public origin: `https://technocore.gudman.xyz`;
- measured origin: `https://technocore.chat`;
- source and state: `/home/technocore/observatory`;
- static publication root: `/opt/technocore-observatory`;
- nginx vhost: `/etc/nginx/sites-available/technocore.gudman.xyz.conf`;
- existing collector unit: `/etc/systemd/system/technocore-observatory.service`;
- TLS: `/etc/letsencrypt/live/technocore.gudman.xyz/{fullchain.pem,privkey.pem}`;
- HTTP ACME include: `/etc/nginx/snippets/acme-challenge.conf`.

The live pre-migration `technocore` crontab was verified on 2026-08-31 to contain both of these
tagged Observatory jobs:

```cron
*/10 * * * * /home/technocore/observatory/rebuild.sh # technocore-observatory-rebuild
23 */6 * * * LD_LIBRARY_PATH=/home/technocore/observatory/lib /usr/bin/python3 /home/technocore/observatory/collect.py --base-url https://technocore.chat --output /home/technocore/observatory/ticks.jsonl --once --census --census-state /home/technocore/observatory/census.json --signer-state /home/technocore/observatory/signers.json >/dev/null 2>&1 # technocore-observatory-census
```

Snapshot and fence both tagged jobs during migration. The legacy rebuild cron writes the flat
publication root and would race the versioned builder. Restore the exact fenced census cron entry
after activation, but do not restore the legacy rebuild cron; the new rebuild timer replaces it.
The existing `census.json` remains the authoritative census state path for this migration.

This is the first versioned publication on the verified host: the legacy flat publication consists
of `/opt/technocore-observatory/index.html` and `/opt/technocore-observatory/data.json`, while
`/opt/technocore-observatory/current` and `releases/` are absent. Preserve that fact explicitly in
the rollback evidence.

## Deployment assets

- `deploy/nginx/http-context.conf` supplies the `http {}`-scope maps, query-free log format, and
  rate-limit zone.
- `deploy/nginx/technocore.gudman.xyz.conf` mirrors the working dual-stack redirect, ACME include,
  TLS certificate paths, static root, and loopback proxy routes.
- `deploy/systemd/` contains the collector, query, pulse, rebuild, and publication-staleness units
  and the pulse, rebuild, and staleness timers.
- `deploy/sqlite/` pins and builds the SQLite shared library used by every process that opens the
  signer or telemetry database. The collector, query, pulse, and rebuild units set
  `LD_LIBRARY_PATH`; staleness remains on its normal runtime environment because it opens neither
  database.
- `rebuild.sh` takes a non-blocking exclusive lock on the resolved publication root, recovers
  interrupted unpublished builds, copies the tick ledger once while holding the collector's
  `ticks.jsonl.lock` so the build and the guards read one untorn snapshot, creates a new versioned
  release, runs every guard, atomically replaces only the `current` symlink, and then applies
  bounded release retention. The copy lives in the unit's private `/tmp` and is removed on every
  exit path. If the lock file is missing, is not a regular file, cannot be opened, or cannot be
  locked, the rebuild fails closed before copying the ledger or invoking the builder.

All nginx security and CORS headers are declared once at server scope with `always`. Locations do
not add their own headers, so nginx cannot silently drop the inherited set on error responses. The
CSP permits same-origin CSS/JavaScript and the legacy inline observatory code, but limits forms to
`'self'`. Dynamic room/DID paths receive `X-Robots-Tag`; access logs use `$uri`, never raw query
arguments or referrers. nginx-generated API 400, 404, 405, 429, and 503 responses use bounded
text/JSON artifacts selected by `format=json`, with text as the safe default. A 429 also carries
`Retry-After: 60`; that 60 s is a deliberate over-backoff, not the replenishment interval, because
the zone replenishes one request every 2 s with a burst of 10. All five error statuses are
`no-store`, while successful static status, incidents, changes, and methodology representations
remain publicly cacheable. Every loopback proxy suppresses GET/HEAD request bodies and clears the
forwarded `Content-Length`.

The signer and telemetry databases use WAL journal mode with `synchronous=NORMAL` and an explicit
1,000-page automatic-checkpoint threshold. WAL with NORMAL remains durable against a process crash
and cannot corrupt either database on power loss, but the last committed transaction can be lost
after power loss. The signer tick outbox and best-effort telemetry path already tolerate that loss
boundary.

Treat each of these sets as a separate recovery family:

- `signers.sqlite3`, `signers.sqlite3-wal`, and `signers.sqlite3-shm`;
- `telemetry.sqlite3`, `telemetry.sqlite3-wal`, and `telemetry.sqlite3-shm`.

Never copy, restore, or delete either sidecar independently of the other existing members of its
family. When the last connection to a WAL database closes cleanly, SQLite checkpoints the WAL and
normally removes both sidecars. The collector therefore holds one connection to each database for
its whole process lifetime, so the SHM files remain available to the confined read-only query and
rebuild services between ticks. While the collector is stopped, the query service's signer read
and the rebuild's telemetry read can fail closed until the collector starts and recreates the
corresponding sidecars; nginx returns the bounded 503 for a failed query, and a failed rebuild
leaves the prior `current` release untouched.

### Signer WAL deploy order

Apply the signer-WAL change in this order; do not combine or reorder the storage gates:

1. Deploy the tracked tree to `/home/technocore/observatory`.
2. As `technocore`, run `deploy/sqlite/build.sh` and verify that it reports SQLite 3.53.4 installed
   at `/home/technocore/observatory/lib/libsqlite3.so.0`.
3. Install the updated collector and query units, run `systemctl daemon-reload`, and replace the
   fenced census entry with the documented `LD_LIBRARY_PATH` form. Do not restart either service
   yet.
4. Run the WAL rehearsal script as root, passing the real database as its read-only copy source:

   ```bash
   sudo /home/technocore/observatory/deploy/sqlite/rehearse-wal.sh \
     /home/technocore/observatory/signers.sqlite3
   ```

   The script works only on its scratch copy. Continue only if the forward and rollback rehearsal
   prints PASS for every gate.
5. Fence the census cron, stop the query service, then stop the collector. Confirm no process has
   the real signer database open.
6. Under the vendored library, switch the real signer database to WAL:

   ```bash
   sudo -u technocore -- env LD_LIBRARY_PATH=/home/technocore/observatory/lib \
     /usr/bin/python3 - <<'PY_SWITCH_SIGNER_WAL'
   import sqlite3


   if sqlite3.sqlite_version_info < (3, 53, 4):
       raise SystemExit(f"vendored SQLite 3.53.4+ required; loaded {sqlite3.sqlite_version}")
   database = "/home/technocore/observatory/signers.sqlite3"
   with sqlite3.connect(database) as connection:
       mode = connection.execute("PRAGMA journal_mode = WAL").fetchone()[0]
       if mode.lower() != "wal":
           raise SystemExit(f"failed to switch signer database to WAL: {mode!r}")
   PY_SWITCH_SIGNER_WAL
   ```

7. Start the collector. Its held connection applies `synchronous=NORMAL` and
   `wal_autocheckpoint=1000` before any operation.
8. Verify `signers.sqlite3-wal` and `signers.sqlite3-shm` both exist, are owned by
   `technocore:technocore`, and are readable through the query identity's supplementary group.
9. Restart the query service and verify it reports WAL mode while remaining unable to write the
   directory or any signer-family member.
10. Measure an apply transaction while issuing room searches. Readers must complete during the
    write; record latency, 503 rate, WAL size, and checkpoint behavior.

The rehearsal's switch-back gate is also the rollback rehearsal: it proves on a copy that a clean
close checkpoints the WAL and that `PRAGMA journal_mode = DELETE` succeeds under the vendored
library before the production mode is changed.

### Telemetry WAL deploy order

Apply the telemetry-WAL change in this order. Do not combine or reorder the storage gates:

1. Deploy the tracked tree to `/home/technocore/observatory`. The already-running collector still
   has its old code mapped until it is stopped below.
2. Install the updated pulse and rebuild units, then run `systemctl daemon-reload`. Confirm the
   collector unit still carries the same vendored-library environment; do not restart anything
   yet.
3. Run the telemetry rehearsal as root against a scratch backup of the real database:

   ```bash
   sudo /home/technocore/observatory/deploy/sqlite/rehearse-telemetry-wal.sh \
     /home/technocore/observatory/telemetry.sqlite3
   ```

   Continue only if it prints PASS for read-during-write under the rebuild's `ReadOnlyPaths`,
   `record_attempt` during a held 50,000-row read, the exact SQLite version, checkpoint-on-close,
   and switch-back to DELETE. The script never changes the source database.
4. Stop `technocore-observatory-pulse.timer`, then
   `technocore-observatory-rebuild.timer`. Confirm both oneshot services are inactive, stopping
   either service if necessary. Fence the census cron and confirm no census process is running.
   Only then stop `technocore-observatory.service` and confirm no process has the real telemetry
   database open.
5. Switch the real telemetry database to WAL under the vendored library:

   ```bash
   sudo -u technocore -- env LD_LIBRARY_PATH=/home/technocore/observatory/lib \
     /usr/bin/python3 - <<'PY_SWITCH_TELEMETRY_WAL'
   import sqlite3


   if sqlite3.sqlite_version_info < (3, 53, 4):
       raise SystemExit(f"vendored SQLite 3.53.4+ required; loaded {sqlite3.sqlite_version}")
   database = "/home/technocore/observatory/telemetry.sqlite3"
   with sqlite3.connect(database) as connection:
       mode = connection.execute("PRAGMA journal_mode = WAL").fetchone()[0]
       if mode.lower() != "wal":
           raise SystemExit(f"failed to switch telemetry database to WAL: {mode!r}")
   PY_SWITCH_TELEMETRY_WAL
   ```

6. Start `technocore-observatory.service`. Its process-lifetime `TelemetryStore` connection
   verifies WAL, applies `synchronous=NORMAL` and `wal_autocheckpoint=1000`, and keeps the sidecars
   present between ticks.
7. Before starting either reader, verify `telemetry.sqlite3-wal` and `telemetry.sqlite3-shm` both
   exist and are owned by `technocore:technocore`. A missing member means stop; do not create or
   copy a sidecar by hand.
8. Start `technocore-observatory-pulse.timer`, then
   `technocore-observatory-rebuild.timer`, and restore the exact fenced census cron entry.
9. Trigger or await one rebuild and confirm it succeeds while the collector remains active. Confirm
   a new collector tick lands in under three minutes during that rebuild, and record the rebuild
   duration, tick duration, WAL size, and checkpoint behavior.

While the collector remains stopped, do not treat a rebuild telemetry-open failure as permission
to loosen `ReadOnlyPaths`: the missing SHM file is the expected fail-closed state. Start the
collector, verify the complete telemetry recovery family, and retry the rebuild.

## 1. Verify the candidate locally

From the repository root, using a real local tick ledger and telemetry database:

```bash
python -m pytest -q
python -m py_compile ./*.py
ruff check .

python build_site.py \
  /absolute/path/ticks.jsonl \
  /absolute/path/telemetry.sqlite3 \
  /absolute/path/public \
  --template /absolute/path/index.html

mkdir /absolute/path/public/releases/RELEASE_ID/errors
cp deploy/fallback/* \
  /absolute/path/public/releases/RELEASE_ID/errors/

python guards.py \
  --html /absolute/path/public/releases/RELEASE_ID/observatory/index.html \
  --derive /absolute/path/derive.py \
  --ticks /absolute/path/ticks.jsonl \
  --site-root /absolute/path/public/releases/RELEASE_ID
```

Non-zero means stop. A `SKIP` for Playwright or a browser is not a render validation; both render
guards must already have passed on a browser-capable machine before the release is eligible.

The current public contract is methodology 1.16.0. The query unit must pass that exact
version so every dynamic response reports the same methodology as the generated snapshots.

## 2. Stage files without activating them

Place the tracked candidate checkout in `/home/technocore/observatory-candidate`. It must contain
no copied state files. Do not overwrite `/home/technocore/observatory`, `/etc/nginx`,
`/etc/systemd/system`, or `/opt/technocore-observatory/current` yet; the rollback snapshot in step 3
must capture the prior live release.

Verify the staged candidate and its deployment payload without executing either:

```bash
test -f /home/technocore/observatory-candidate/collect.py
test -f /home/technocore/observatory-candidate/recover_publication.py
test -f /home/technocore/observatory-candidate/query_service.py
test -f /home/technocore/observatory-candidate/rebuild.sh
test -f /home/technocore/observatory-candidate/check_staleness.py
test -f /home/technocore/observatory-candidate/deploy/nginx/technocore.gudman.xyz.conf
```

## 3. Fence writers, snapshot the prior release, then install the candidate

The collector, census cron, and legacy rebuild cron all write live state or publication paths.
Before changing the crontab, save its complete output in the mode-0700 rollback directory. Remove
only the two lines tagged `# technocore-observatory-rebuild` and
`# technocore-observatory-census`; preserve the unrelated check-in job. Snapshot and fence both
tagged jobs before stopping services, and verify both tags are absent from the installed crontab.
Do not restore the legacy rebuild cron after the new timer is enabled.

The collector and census both write signer state. Fence every installed service before migration:

```bash
systemctl stop technocore-observatory-rebuild.timer
systemctl stop technocore-observatory-rebuild.service
systemctl stop technocore-observatory-pulse.timer
systemctl stop technocore-observatory-pulse.service
systemctl stop technocore-observatory-query.service
systemctl stop technocore-observatory.service
```

Confirm no collector, census, or legacy rebuild process remains. Do not migrate while a writer can
reach signer state or the flat publication root.

Treat the tick ledger as one ledger recovery state family:

- `ticks.jsonl` is the append-only ledger;
- `ticks.jsonl.ledger-checkpoint.json` anchors its last verified tip to the ledger file;
- `ticks.jsonl.ledger-pending.json` journals the exact append being completed.

The pending journal may be valid after an interrupted append. Its presence alone is not corruption:
with the collector lock held, the next append validates the family and completes that journal
idempotently or fails closed. Do not delete either sidecar independently.

Only after fencing, create a mode-0700 rollback directory and take an exact, read-only evidence
copy before running candidate code. Copy:

- the deployed Python, HTML, shell, nginx, and systemd files;
- the complete ledger recovery state family: `ticks.jsonl`,
  `ticks.jsonl.ledger-checkpoint.json`, and `ticks.jsonl.ledger-pending.json`;
- `signers.json`, `census.json`, and `census.json.lock`;
- `telemetry.sqlite3` if it exists, together with every existing `telemetry.sqlite3-wal` and
  `telemetry.sqlite3-shm` sidecar; if the pre-switch database instead has a legacy
  `telemetry.sqlite3-journal`, preserve that evidence too and record the absence of every missing
  telemetry-family member;
- `signers.sqlite3` if it exists, together with every existing `signers.sqlite3-wal` and
  `signers.sqlite3-shm` sidecar; if the pre-switch database instead has a legacy
  `signers.sqlite3-journal`, preserve that evidence too and record the absence of every missing
  signer-family member;
- the complete legacy flat publication, including `/opt/technocore-observatory/index.html` and
  `/opt/technocore-observatory/data.json`, plus the old nginx vhost;
- the output of `readlink /opt/technocore-observatory/current`, or record `current` as absent for
  the verified first versioned deployment.

This first copy is disaster-recovery evidence, not an ordinary code-rollback input. Snapshot every
ledger-family member that exists and record any absent sidecar; do not synthesize a missing member.
For an already-WAL signer or telemetry database, copy the database, WAL, and SHM together while
every process is fenced. Never infer that a missing sidecar is disposable or delete one to make
the family look normalized. Those files remain legacy WAL/SHM evidence for the pre-transition raw
snapshot; after activation they are live recovery-family members.

Next, use the staged candidate's recovery-only command against the fenced live state:

```bash
(
  cd /home/technocore/observatory-candidate
  sudo -u technocore -- env LD_LIBRARY_PATH=/home/technocore/observatory/lib \
    python3 recover_publication.py \
    --output /home/technocore/observatory/ticks.jsonl \
    --signer-state /home/technocore/observatory/signers.json \
    --census-state /home/technocore/observatory/census.json
)
test ! -e /home/technocore/observatory/ticks.jsonl.ledger-pending.json
```

This command performs no origin reads. It takes the existing collector lock, publishes any exact
tick committed in the SQLite outbox, acknowledges its paired census state, requires no unresolved
pending journal (success leaves no unresolved pending journal), and verifies the resulting ledger.
Non-zero means stop and preserve the evidence
copy; never delete or hand-edit a journal to make recovery pass.

For the pre-WAL recovery-ready snapshot only, normalize and integrity-check every fenced SQLite
database under the vendored library:

```bash
sudo -u technocore -- env LD_LIBRARY_PATH=/home/technocore/observatory/lib \
  python3 - <<'PY_SQLITE_RECOVERY'
import sqlite3
from pathlib import Path

if sqlite3.sqlite_version_info < (3, 53, 4):
    raise SystemExit(f"vendored SQLite 3.53.4+ required; loaded {sqlite3.sqlite_version}")

for database in (
    Path("/home/technocore/observatory/signers.sqlite3"),
    Path("/home/technocore/observatory/telemetry.sqlite3"),
):
    if not database.exists():
        continue
    connection = sqlite3.connect(database)
    try:
        journal_mode = connection.execute("PRAGMA journal_mode = DELETE").fetchone()[0]
        if journal_mode.lower() != "delete":
            raise SystemExit(f"failed to normalize journal mode for {database}")
        connection.execute("BEGIN IMMEDIATE")
        connection.execute("COMMIT")
        result = connection.execute("PRAGMA integrity_check").fetchall()
    finally:
        connection.close()
    if result != [("ok",)]:
        raise SystemExit(f"integrity check failed for {database}: {result!r}")
    journal = database.with_name(database.name + "-journal")
    if journal.exists():
        raise SystemExit(f"rollback journal remains after recovery: {journal}")
PY_SQLITE_RECOVERY
test ! -e /home/technocore/observatory/signers.sqlite3-journal
test ! -e /home/technocore/observatory/telemetry.sqlite3-journal
test ! -e /home/technocore/observatory/signers.sqlite3-wal
test ! -e /home/technocore/observatory/signers.sqlite3-shm
test ! -e /home/technocore/observatory/telemetry.sqlite3-wal
test ! -e /home/technocore/observatory/telemetry.sqlite3-shm
```

This pre-switch normalization lets SQLite finish any hot rollback journal left by legacy DELETE
mode before the integrity check. A non-`ok` result or surviving journal means stop; never delete
the journal to force this gate. If the signer database was already in WAL mode,
`PRAGMA journal_mode = DELETE` performs the checkpoint and mode switch; it must run under the
vendored library, with every other opener of that database fenced.

After it succeeds, create a separate recovery-ready state snapshot containing `ticks.jsonl`, the
recorded absence of `ticks.jsonl.ledger-pending.json`, `telemetry.sqlite3`, `signers.json`, and
`census.json`. Include `ticks.jsonl.ledger-checkpoint.json` when it exists. For a
verified legacy ledger without one, record a missing legacy checkpoint; the next accepted append
creates its canonical checkpoint. Include `signers.sqlite3` when it exists; for a v2 JSON source,
record that it was absent before migration. This snapshot is deliberately pre-transition. The
recovery-ready snapshot must contain no SQLite `-journal`, `-wal`, or `-shm`. After WAL activation,
a new recovery snapshot must instead preserve each SQLite database and every existing `-wal` and
`-shm` member of its signer or telemetry recovery family; only a successful clean checkpoint or an
explicit switch to DELETE may leave that family's sidecars absent. If disaster recovery becomes
necessary, keep every opener fenced, restore every member from one snapshot, then run the same
recovery command before starting a writer. Never mix members from different snapshots.

Do not continue unless the rollback directory contains the prior deployed code/config evidence,
the recovery-ready state snapshot, and the recorded `current` target.

Only now copy the candidate's tracked source files into `/home/technocore/observatory`. The
candidate must have been extracted from the verified commit archive and contain no state files, so
this additive copy cannot replace a state member. Preserve
`ticks.jsonl`, `ticks.jsonl.ledger-checkpoint.json`, and `ticks.jsonl.ledger-pending.json` as the
same family, plus both complete SQLite recovery families, `signers.json`, `census.json`, and all
lock files. Provision the query identity idempotently:

```bash
if ! getent passwd technocore-query >/dev/null; then
  useradd --system --user-group --no-create-home --shell /usr/sbin/nologin technocore-query
fi
usermod --append --groups technocore technocore-query
install -d -o technocore -g technocore -m 0755 /opt/technocore-observatory
install -d -o technocore -g technocore -m 0755 /opt/technocore-observatory/releases
```

Install staged unit/config files only after reviewing their diffs against the snapshotted live
files. The verified `/etc/nginx/nginx.conf` loads `/etc/nginx/conf.d/*.conf` inside `http {}` and
`/etc/nginx/sites-enabled/*` immediately after it. Install the assets from the separate candidate:

```bash
install -o root -g root -m 0644 \
  /home/technocore/observatory-candidate/deploy/nginx/http-context.conf \
  /etc/nginx/conf.d/technocore-observatory-http.conf
install -o root -g root -m 0644 \
  /home/technocore/observatory-candidate/deploy/nginx/technocore.gudman.xyz.conf \
  /etc/nginx/sites-available/technocore.gudman.xyz.conf
test -L /etc/nginx/sites-enabled/technocore.gudman.xyz.conf || \
  ln -s /etc/nginx/sites-available/technocore.gudman.xyz.conf \
    /etc/nginx/sites-enabled/technocore.gudman.xyz.conf
install -o root -g root -m 0644 \
  /home/technocore/observatory-candidate/deploy/systemd/technocore-observatory* \
  /etc/systemd/system/
chmod 0755 /home/technocore/observatory/rebuild.sh
command -v flock
```

Do not place `map`, `log_format`, or `limit_req_zone` inside a server block. Do not reload nginx or
systemd yet. `UMask=0027` makes SQLite databases owner-writable and group-readable at mode `0640`.
Atomically replaced JSON state, the tick ledger, and lock files deliberately use mode `0600`.
Before starting the query service, verify the signer database is owned by
`technocore:technocore` and is not group-writable. The `technocore-query` user receives read access
through its supplementary `technocore` group; its unit has no `ReadWritePaths` and opens SQLite in
`mode=ro` with `PRAGMA query_only=ON`. The directory-level `ReadOnlyPaths` boundary covers
`signers.sqlite3`, `signers.sqlite3-wal`, and `signers.sqlite3-shm`; do not add a writable exception
for any family member. The rehearsal must prove that the confined query identity can mmap the
existing read-only SHM file. If systemd confinement blocks that mmap, stop the deployment and
report the failure instead of loosening permissions.

The rebuild runs as `technocore`, but its unit exposes `/home/technocore/observatory` through
`ReadOnlyPaths` and opens telemetry with `mode=ro`. That boundary covers `telemetry.sqlite3`,
`telemetry.sqlite3-wal`, and `telemetry.sqlite3-shm`; do not add a writable exception for any
member. The telemetry rehearsal must prove both that this confined reader can use the existing SHM
while `TelemetryStore` holds it and that `record_attempt` commits while the reader holds a large
SELECT. A confinement failure is a stop condition, not permission to loosen the unit.

## 4. Run the one-time signer migration only when needed

Every invocation of `migrate_signers.py` or `recover_publication.py` that can open the signer
database must run with `LD_LIBRARY_PATH=/home/technocore/observatory/lib`. Both tools use the
collector's signer-database opener and fail closed when the vendored SQLite version is not loaded.

First inspect the checked-in contract:

```bash
LD_LIBRARY_PATH=/home/technocore/observatory/lib \
  /usr/bin/python3 /home/technocore/observatory/migrate_signers.py --help
```

Skip the JSON-to-SQLite migrator when an authoritative v3-v5 `signers.sqlite3` already exists. The
candidate recovery command opens that database through the same initializer as the collector and
therefore performs the additive upgrade to schema v6 before it inspects the outbox. Verify schema 6
after recovery and keep the query service stopped until that gate succeeds. For a v2 JSON source,
the migrator requires different source and output paths and refuses to replace either output.
Preserve the fenced v2 file under a distinct absolute path, make sure both target paths are absent,
then run:

```bash
LD_LIBRARY_PATH=/home/technocore/observatory/lib \
  /usr/bin/python3 /home/technocore/observatory/migrate_signers.py \
  /absolute/fenced/source/signers-v2.json \
  /home/technocore/observatory/signers.json
```

If the source records `cap_hit: true`, add `--cap-saturated-at` with the separately verified UTC
timestamp. Do not invent it. A successful run prints matching source/SQLite counts and creates both
`signers.json` metadata and `signers.sqlite3`. If migration fails, it removes its partial database;
keep writers fenced and diagnose before retrying.

Restore `technocore:technocore` ownership, mode `0600` on the signer JSON metadata, and mode `0640`
on the signer database before continuing.

For a v2 source, refresh the recovery-ready state snapshot with the newly created `signers.json`
and `signers.sqlite3` before activation. Record that these two files are the paired output of the
verified migration, and preserve the original fenced v2 JSON in the evidence copy. From this point
forward the recovery-ready snapshot must contain the signer database; do not activate from the
pre-migration absence record.

Verify the planned query identity can read the database through its supplementary group but cannot
write it:

```bash
sudo -u technocore-query test -r /home/technocore/observatory/signers.sqlite3
sudo -u technocore-query test ! -w /home/technocore/observatory/signers.sqlite3
```

Both commands must return zero. The unit adds a read-only mount boundary over the source/state tree,
so the running query process remains unable to write even if a future file mode is loosened.

## 5. Validate the target and activate in dependency order

Run parser checks before any reload or enablement:

```bash
systemd-analyze verify /etc/systemd/system/technocore-observatory*.service \
  /etc/systemd/system/technocore-observatory*.timer
systemctl daemon-reload
nginx -t
```

Then establish data before consumers:

1. Start `technocore-observatory.service`, wait for one accepted tick, and verify collector 2.15.0,
   signer-state/SQLite schema 6, and telemetry schema 1 in local state. This upgrades an existing
   v3-v5 SQLite store before any schema-v6-only reader starts.
2. Verify both the signer and telemetry `-wal`/`-shm` pairs exist while the collector is running.
3. Run `systemctl start technocore-observatory-pulse.service` once.
4. Run `systemctl start technocore-observatory-rebuild.service`. It must create a new
   `releases/<id>` and atomically set `current`; a failed build or guard leaves the prior `current`
   untouched and removes the exact unpublished candidate. The builder creates an external
   `.unpublished-<id>` sidecar before the staging-directory rename, and the rebuild clears that
   sidecar only after the atomic flip proves the candidate is current. Only then does it prune
   release history.
5. Confirm `/api/v1/status.txt`, `/api/v1/status.json`, discovery documents, local assets, and the
   static error artifacts exist beneath the resolved `current` release.
6. Start `technocore-observatory-query.service`; confirm it listens only on `127.0.0.1:8765` and
   can read but not mutate the signer database.
7. Restore the exact fenced census cron entry.
8. Enable/start `technocore-observatory-pulse.timer` and
   `technocore-observatory-rebuild.timer`.
9. Reload nginx only after another successful `nginx -t`.

The corresponding activation commands are:

```bash
systemctl start technocore-observatory.service
systemctl enable technocore-observatory.service
# Wait for one accepted tick and verify schema 6 before continuing.
# Verify both WAL/SHM pairs while the collector holds its connections.
systemctl start technocore-observatory-pulse.service
systemctl start technocore-observatory-rebuild.service
systemctl start technocore-observatory-query.service
systemctl enable technocore-observatory-query.service
# Restore the exact fenced 23 */6 census cron entry here.
systemctl enable --now technocore-observatory-pulse.timer
systemctl enable --now technocore-observatory-rebuild.timer
nginx -t && systemctl reload nginx
```

Static status must still serve with the query daemon stopped. Verify both representations. For
incidents and changes, no query string and exactly `format=json` must serve their unfiltered static
snapshots without the daemon. `since`, `limit`, combined filters, and every other argument must go
to the daemon. Stop the query service and confirm those filtered requests return the bounded
representation-aware 503 contract; they must never silently degrade to an unfiltered 200. With the
query service stopped, every parameterized query route must return its bounded
representation-aware 503 artifact, including `/rooms/?q=...`, `/rooms/?limit=...`, filtered
incidents/changes, and exact room/DID lookups. TRACE must return the bounded `no-store` 405 method
artifact. The empty-query `/rooms/` path must instead serve the built `rooms/index.html` without
the daemon.

Restart the query service after the stopped-daemon checks. After restart, invalid search arguments
must stay bounded `no-store` 400 responses and must never fall back to the static default; valid
`/rooms/?q=...` requests must proxy and return the bounded search contract.

Inspect headers on both 200 and error responses: CSP, HSTS, nosniff, referrer policy, permissions
policy, frame denial, credential-free CORS, and the route-specific robot policy. Confirm a rate-limit
response is 429 with `Retry-After`; on `/rooms/?q=...`, `/rooms/{16-hex}/` and `/keys/{did}/` its
body is the styled `errors/query-rate-limited.html`, elsewhere the text/JSON artifact. Confirm the
access log contains no raw `q`, `since`,
`limit`, or `format` values. Confirm every bounded 400, 404, 405, 429, and 503 is `no-store`, while a
successful static status, incidents, changes, or methodology response retains the documented
public cache policy.

The five-minute rebuild retains the active release and its immediate predecessor unconditionally.
It then retains the newest direct-child release directories only while the total retained set is
at or below both 1,008 entries and 2 GiB of apparent payload bytes. Once the next-newest release
would cross either limit, it and all older managed releases are removed. The two protected releases
remain even if they alone exceed a limit. Before every recursive deletion, the script resolves the
candidate and verifies that it is an exact, non-symlink child of
`/opt/technocore-observatory/releases`; unrelated and unsafe entries are not deleted.
The builder makes every generated directory mode `0755` and every ordinary generated file mode
`0644` before renaming the private staging tree. On startup, while holding the same publication-root
lock used through the build and flip, the rebuild deletes strictly validated `.building-*`
directories and sidecar-marked non-current releases. A sidecar-marked release already selected by
`current` is preserved and its sidecar is cleared, covering interruption immediately after the
atomic flip. The sidecars are siblings of release directories and are never beneath nginx's
`current` document root.

## 6. Rollback without losing forward state

On this first versioned deployment there is no retained predecessor and `current` began absent.
Until a second guarded release exists, the static rollback is the snapshotted old vhost and flat
files: fence the new rebuild timer, restore the old vhost and flat files from the same rollback
snapshot, run `nginx -t`, and reload nginx. Do not delete the new releases or forward state while
diagnosing. The old vhost and flat files are the only verified pre-versioned static rollback.

### Roll back signer WAL to DELETE

A static/publication rollback does not require a database-mode change. A code/config rollback that
removes the vendored library or its `LD_LIBRARY_PATH` settings does. Rehearse this path on the copy
first: `deploy/sqlite/rehearse-wal.sh` must print PASS for its checkpoint-on-close and switch-back
to DELETE gates before the real signer database is touched.

Fence the census cron, stop the query service, stop the collector, and confirm no process has the
signer database open. Snapshot the complete current signer recovery family. Then switch the real
database under the vendored library:

```bash
sudo -u technocore -- env LD_LIBRARY_PATH=/home/technocore/observatory/lib \
  /usr/bin/python3 - <<'PY_ROLLBACK_SIGNER_DELETE'
import sqlite3
from pathlib import Path


if sqlite3.sqlite_version_info < (3, 53, 4):
    raise SystemExit(f"vendored SQLite 3.53.4+ required; loaded {sqlite3.sqlite_version}")
database = Path("/home/technocore/observatory/signers.sqlite3")
connection = sqlite3.connect(database)
try:
    mode = connection.execute("PRAGMA journal_mode = DELETE").fetchone()[0]
    if mode.lower() != "delete":
        raise SystemExit(f"failed to switch signer database to DELETE: {mode!r}")
    result = connection.execute("PRAGMA integrity_check").fetchall()
finally:
    connection.close()
if result != [("ok",)]:
    raise SystemExit(f"signer integrity check failed: {result!r}")
for suffix in ("-wal", "-shm"):
    sidecar = database.with_name(database.name + suffix)
    if sidecar.exists():
        raise SystemExit(f"signer sidecar remains after DELETE switch: {sidecar}")
PY_ROLLBACK_SIGNER_DELETE
```

The `PRAGMA journal_mode = DELETE` statement checkpoints the WAL and changes the persistent mode.
Do not remove the vendored library, remove either sidecar, restore an older unit, or restart a
system-library signer opener before that command and integrity check succeed. Once they do, update
the collector unit and census cron together, start the compatible collector, then restart the query
service. This procedure changes only the signer database; use the telemetry procedure below when a
rollback also removes telemetry-WAL support.

### Roll back telemetry WAL to DELETE

Apply the signer rollback procedure to telemetry as its own recovery family. First run
`deploy/sqlite/rehearse-telemetry-wal.sh`; its checkpoint-on-close and switch-back gates must print
PASS. Stop the pulse and rebuild timers, confirm both oneshot services are inactive, fence the
census cron, then stop the collector. Confirm no process has the telemetry database open and
snapshot `telemetry.sqlite3` with both existing sidecars before touching the real family.

Switch the real database under the vendored library and verify its integrity:

```bash
sudo -u technocore -- env LD_LIBRARY_PATH=/home/technocore/observatory/lib \
  /usr/bin/python3 - <<'PY_ROLLBACK_TELEMETRY_DELETE'
import sqlite3
from pathlib import Path


if sqlite3.sqlite_version_info < (3, 53, 4):
    raise SystemExit(f"vendored SQLite 3.53.4+ required; loaded {sqlite3.sqlite_version}")
database = Path("/home/technocore/observatory/telemetry.sqlite3")
connection = sqlite3.connect(database)
try:
    mode = connection.execute("PRAGMA journal_mode = DELETE").fetchone()[0]
    if mode.lower() != "delete":
        raise SystemExit(f"failed to switch telemetry database to DELETE: {mode!r}")
    result = connection.execute("PRAGMA integrity_check").fetchall()
finally:
    connection.close()
if result != [("ok",)]:
    raise SystemExit(f"telemetry integrity check failed: {result!r}")
for suffix in ("-wal", "-shm"):
    sidecar = database.with_name(database.name + suffix)
    if sidecar.exists():
        raise SystemExit(f"telemetry sidecar remains after DELETE switch: {sidecar}")
PY_ROLLBACK_TELEMETRY_DELETE
```

Do not remove the vendored library, change any of the four units' `LD_LIBRARY_PATH`, delete a
sidecar, or start an older opener before the DELETE switch and integrity check succeed. Then deploy
the mutually compatible older code and units, start the collector, restore the census entry, and
start the pulse and rebuild timers. This rollback changes no telemetry schema or record content.

### Deploy order at 2.13.0

Collector and deriver ship from the same archived tree, so they move together. If a 2.13.0 tick is
ever written while an older deriver is still running, that deriver rejects it: the sampling object
gained required fields. Nothing is lost — the deriver re-reads the retained corpus on each run, so
the rejected tick is picked up as soon as the matching deriver is live. Deploy both, then rebuild.

### Reverting the collector below 2.13.0

A static rollback changes no collector state and needs nothing extra. Reverting the **collector**
below 2.13.0 does: the signer state version is unchanged, so an older binary will open a 2.13.0
store, and the 2.13.0 validation triggers survive because they are created `IF NOT EXISTS` under
unchanged names. A pre-2.13.0 collector does not know about `aged_out_at`, so it can reselect a
finalized check and try to write an attempt to it, which the surviving trigger aborts — failing
that tick, and every tick after it, until someone intervenes.

Before starting a pre-2.13.0 collector, drop the eight room-revisit triggers:

```sql
DROP TRIGGER IF EXISTS room_revisits_validate_insert;
DROP TRIGGER IF EXISTS room_revisits_validate_update;
DROP TRIGGER IF EXISTS room_revisits_rollup_before_insert;
DROP TRIGGER IF EXISTS room_revisits_rollup_after_insert;
DROP TRIGGER IF EXISTS room_revisits_rollup_before_update;
DROP TRIGGER IF EXISTS room_revisits_rollup_after_update;
DROP TRIGGER IF EXISTS room_revisits_rollup_before_delete;
DROP TRIGGER IF EXISTS room_revisits_rollup_after_delete;
```

The older collector recreates its own set on start, finds the trigger set incomplete, and
revalidates the store. Finalized rows pass its checks: their attempt fields are still null and the
extra `aged_out_at` column is invisible to its queries. The column and its partial indexes are left
in place; they cost nothing to a collector that never reads them, and they are still there if you
roll forward again.

### Deploy order at 2.14.0

Nothing in the published payload changes: 2.14.0 only moves terminal revisit rows out of the
per-tick coverage scan and into per-stage rollup counters, so the methodology version stays at
1.15.0 and any deriver that accepts a 2.13.0 tick accepts a 2.14.0 tick. On its first start the
2.14.0 collector detects the 2.13.0 trigger set by its missing per-stage tokens, rebuilds all
eleven lifecycle triggers, rebuilds `room_lifecycle_totals` with the per-stage columns, and
revalidates the store — exactly once; that first tick is slower by one full-store validation pass.
Deploy the tree, restart the collector, then rebuild.

That one-time migration is not charged to a tick's revisit deadline. Every invocation drains the
tick outbox before it collects, and that drain opens the signer database, so the trigger rebuild,
the `room_lifecycle_totals` rebuild, and the revalidation all finish before `collect_tick` starts
its clock. Expect no `deferred_due_to_deadline` spike on the first 2.14.0 tick; if one appears,
it is an ordinary slow tick, not the migration.

### Deploy order at 1.16.0

No collector version moves: 1.16.0 is a deriver-only change to what the payload publishes.
Recorded gaps now count collector cadence intervals rather than gap records, a rollup bucket is
marked gapped only by a cadence gap, boundary buckets expect only the ticks their own retention
level can hold, a legacy funnel tick reports its persistence stage as not recorded, and a
cumulative series below its chart baseline publishes null instead of zero. Two published fields
change meaning rather than merely appearing: `signer_funnel.coverage.sampled_rooms` now counts the
sampled room reads that succeeded, with the manifest selection count moved to the new
`coverage.selected_rooms`, and the rollup bucket `sum` key is gone. Anything reading either field
must be updated in the same deploy; earlier payloads keep the old meaning. The query unit's
`--methodology-version` pin must be moved to 1.16.0 in the same deploy, or the API advertises a
methodology the snapshots beside it no longer use. Deploy the tree, restart the query unit, then
rebuild.

### Rebuild memory ceiling

`derive.py` peak RSS scales with the tick ledger. Measured on 2026-09-04 against a 155 MB
`ticks.jsonl`: **719 MB peak, 13.9 s wall clock** when it is given enough memory. Under the
original `MemoryMax=512M` the same work swapped instead, and every rebuild from roughly 03:20Z that
day failed with `result 'timeout'` against `TimeoutStartSec=5min` — the publication root silently
stopped advancing for nine hours while the collector kept accepting ticks normally. The failure
mode is a timeout, not an OOM kill, so nothing in the journal names memory as the cause.

The unit now allows `MemoryMax=2G` and `TimeoutStartSec=15min`; a full rebuild takes about 70 s.
The ceiling is not a reservation. **This will recur**: the ledger grows roughly 22 MB/day, so the
ceiling buys time rather than fixing the shape of the problem — `derive.py` loads the ledger rather
than streaming it. When a rebuild starts timing out again, check peak RSS against the ceiling
before assuming the origin or the collector is at fault.

### Publication staleness alarm

`technocore-observatory-staleness.timer` starts one minute after boot and every five minutes after
that. Its `Type=oneshot` `technocore-observatory-staleness.service` resolves
`/opt/technocore-observatory/current` and applies two independent conditions; either one makes the
service exit non-zero and write the reason to stderr and the journal.

1. **Release age.** The release time comes from the leading `YYYYmmddHHMMSS-<hex>` UTC release
   name, falling back to the resolved release directory's mtime when the name does not parse. A
   release older than 30 minutes fails with `publication is stale: age=<seconds>s
   release=<resolved-path>`. This fires when rebuilds stop landing: the timer is fenced, the
   service fails, the build exceeds its timeout, or the host is down. Look at
   `journalctl -u technocore-observatory-rebuild.service` first.
2. **Published validity.** The release's own `api/v1/status.json` carries the `valid_until` the
   public envelope promises (fifteen minutes after the source observation the build read). When
   that instant is more than 10 minutes in the past the check fails with `publication is past its
   validity: overdue=<seconds>s release=<resolved-path>`; a release without a readable
   `valid_until` fails with `publication validity check failed`. This fires when rebuilds keep
   landing but each one is already past the validity it publishes, which the age condition cannot
   see: builds running longer than the rebuild interval on a loaded host, or a collector whose
   ticks have stopped while the rebuild keeps republishing old observations. Compare the rebuild
   wall time in the journal with the timer interval, and check that fresh ticks are landing in
   `ticks.jsonl`. The ten minutes of grace are deliberate: a validity lapse of a few minutes under
   host load is the site reporting STALE honestly, not an outage.

The check is read-only: the unit exposes the source tree and publication root through read-only
mount boundaries, and the script opens no ledger or database. It does not inspect the rebuild
service or its result, so any cause of a stale publication produces the same failure. After the
unit files are installed and `systemctl daemon-reload` has succeeded, enable the independent
timer:

```bash
systemctl enable --now technocore-observatory-staleness.timer
```

### Deploy order at 2.15.0

No store migration: 2.15.0 adds no table, column, trigger or schema version, so unlike 2.14.0 its
first start does no one-time rebuild and its first tick is not slower. What moves is what a tick
may contain. An origin event whose timestamp sits within `ORIGIN_CLOCK_TOLERANCE` (one second)
after the tick observation is now accepted rather than rejecting the whole tick, so two honest
clocks disagreeing by under a second no longer cost a tick's creation events. The published
engagement object is projected onto the fields `derive.validate_engagement` actually reads, so an
origin that adds a member no longer widens the payload. A response body that ends short of its
declared `Content-Length` now raises rather than being accepted as a complete read, and a shard
listing with no recognised rows and no budget footer is refused. `--census-pace` gains a floor at
the published read budget.

A deriver that has not been revised for 2.15.0 still accepts a 2.15.0 tick — the timing change is
internal to collection and `validate_engagement` normalises a retained value rather than rejecting
the tick that carries it. `test_2_15_collector_tick_passes_the_existing_sampling_gates` pins this;
keep it, because an unrevised validator has now rejected a collector change three times and every
one of those was caught by hand rather than by a test.

Deploy the tree, restart the collector, then rebuild. This release also moves the methodology to
1.16.0, so take the query-unit restart from that section in the same deploy.

### Reverting the collector from 2.15.0 to 2.14.0

A plain binary revert. 2.15.0 creates no trigger, column or table that a 2.14.0 collector would
not understand, so none of the drops the 2.14.0-to-2.13.0 revert needs apply. Ticks already
published under 2.15.0 stay valid and keep their own version stamp; the reverted collector simply
resumes rejecting the sub-second-skew ticks 2.15.0 accepted, and publishing the unprojected
engagement object.

### Reverting the collector from 2.14.0 to 2.13.0

The trigger list is unchanged — the same eight room-revisit triggers under the same names — but
two things now survive a plain binary revert. The 2.14.0 triggers maintain `stage_*` counter
columns that a 2.13.0 collector's table does not have, and `room_lifecycle_totals` itself has the
31-column 2.14.0 schema. A 2.13.0 collector opening that store passes its own trigger staleness
check (its `aged_out_at` token is present in the 2.14.0 trigger text), then fails its rollup
column check against the wider table and refuses every tick with "invalid lifecycle rollup
schema". Dropping only the triggers does not help: the 2.13.0 backfill inserts thirteen values
into the 31-column table and fails.

Before starting a 2.13.0 collector, drop the same eight room-revisit triggers listed above **and
the rollup table**:

```sql
DROP TRIGGER IF EXISTS room_revisits_validate_insert;
DROP TRIGGER IF EXISTS room_revisits_validate_update;
DROP TRIGGER IF EXISTS room_revisits_rollup_before_insert;
DROP TRIGGER IF EXISTS room_revisits_rollup_after_insert;
DROP TRIGGER IF EXISTS room_revisits_rollup_before_update;
DROP TRIGGER IF EXISTS room_revisits_rollup_after_update;
DROP TRIGGER IF EXISTS room_revisits_rollup_before_delete;
DROP TRIGGER IF EXISTS room_revisits_rollup_after_delete;
DROP TABLE IF EXISTS room_lifecycle_totals;
```

The 2.13.0 collector finds the trigger set incomplete and the table absent, recreates both in its
own shape, and revalidates and backfills the store from the source rows. No revisit evidence is
touched: the counters are derived state. The `room_revisits_superseded` partial index is left in
place; SQLite maintains it under any writer and the 2.13.0 collector simply never reads it.
Rolling forward to 2.14.0 again re-runs the one-time rebuild.

For a static/publication rollback, take the same exclusive publication-root lock used by
`rebuild.sh`, then point `current` at a previously verified versioned release with the same atomic
link pattern and run `nginx -t`. This changes no collector, telemetry, or signer state. The
immediate predecessor is always retained for this purpose; older releases are subject to the
documented count/byte bounds. Keep the rejected release for diagnosis. A release built before
`errors/query-rate-limited.html` existed serves nginx's built-in 429 body on the two human page
routes, with the same status and headers, until a newer release is published. A release built
before `errors/query-not-found.html` existed likewise serves nginx's built-in 404 body on the
room/key human detail routes until a newer release is published.

```bash
(
set -eu
public_root=/opt/technocore-observatory
public_root=$(CDPATH= cd -- "$public_root" && pwd -P)
releases_root=$(CDPATH= cd -- "$public_root/releases" && pwd -P)
if ! command -v flock >/dev/null 2>&1; then
    echo "flock is required to serialize release publication" >&2
    exit 1
fi
exec 9<"$public_root"
if ! flock -n 9; then
    echo "another release publication is already running for $public_root" >&2
    exit 75
fi
previous_release_id=REPLACE_WITH_VERIFIED_RELEASE_ID
validated_release_id=$(
  /usr/bin/python3 - "$releases_root" "$previous_release_id" <<'PY_VALIDATE_ROLLBACK'
import re
import sys
from pathlib import Path


RELEASE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")

if len(sys.argv) != 3:
    raise SystemExit("expected release root and release ID")
root = Path(sys.argv[1]).resolve(strict=True)
name = sys.argv[2]
if not root.is_dir() or RELEASE_NAME.fullmatch(name) is None:
    raise SystemExit("invalid release root or release ID")
candidate = root / name
if candidate.is_symlink():
    raise SystemExit("release target must not be a symlink")
resolved = candidate.resolve(strict=True)
if not resolved.is_dir() or resolved.parent != root:
    raise SystemExit("release target must be an exact child of the releases root")
sidecar = root / f".unpublished-{name}"
if sidecar.exists() or sidecar.is_symlink():
    raise SystemExit("release target is still marked unpublished")
print(resolved.name)
PY_VALIDATE_ROLLBACK
)
rollback_link="/opt/technocore-observatory/.current.rollback.$$"
trap 'rm -f "$rollback_link"' EXIT HUP INT TERM
ln -s "releases/$validated_release_id" "$rollback_link"
mv -Tf "$rollback_link" /opt/technocore-observatory/current
trap - EXIT HUP INT TERM
nginx -t && systemctl reload nginx
)
```

For a code rollback, fence collector and census writers again and snapshot the *current* state
before replacing code. If the target code does not load the vendored library, complete the
applicable signer and telemetry WAL-to-DELETE procedures above before removing the library or
changing `LD_LIBRARY_PATH`. Restore only code/config that supports the current on-disk schema, then
build a fresh release from the current `ticks.jsonl` and `telemetry.sqlite3`. Never restore old
`ticks.jsonl`, `telemetry.sqlite3`, `signers.json`, or `signers.sqlite3` as part of an ordinary code
rollback. If old code cannot read the forward schema, leave writers stopped and roll forward with a
compatible fix instead of destroying newer observations.

No step in this runbook authorizes a post, integration message, repository push, or submission.
Those remain separately approval-gated.
