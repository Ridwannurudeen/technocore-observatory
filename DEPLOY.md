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
23 */6 * * * /usr/bin/python3 /home/technocore/observatory/collect.py --base-url https://technocore.chat --output /home/technocore/observatory/ticks.jsonl --once --census --census-state /home/technocore/observatory/census.json --signer-state /home/technocore/observatory/signers.json >/dev/null 2>&1 # technocore-observatory-census
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
- `deploy/systemd/` contains the collector, query, pulse, and rebuild units and the pulse/rebuild
  timers.
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
arguments or referrers. nginx-generated API 400, 405, 429, and 503 responses use bounded text/JSON
artifacts selected by `format=json`, with text as the safe default. A 429 also carries
`Retry-After: 60`; that 60 s is a deliberate over-backoff, not the replenishment interval, because
the zone replenishes one request every 2 s with a burst of 10. All four error statuses are
`no-store`, while successful static status, incidents, changes, and methodology representations
remain publicly cacheable. Every loopback proxy suppresses GET/HEAD request bodies and clears the
forwarded `Content-Length`.

The signer and telemetry databases use SQLite DELETE journal mode. Consequently, the read-only
query and rebuild services have no WAL/SHM sidecar dependency. An interrupted write can still
leave a hot rollback journal; until normalized, each database and its existing `-journal` are one
recovery family.

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
- `telemetry.sqlite3` together with `telemetry.sqlite3-journal` if that journal exists, recording
  its absence otherwise;
- `signers.sqlite3` if it exists, together with `signers.sqlite3-journal` if that journal exists;
  record the absence of either file rather than synthesizing it;
- any legacy WAL/SHM evidence files `signers.sqlite3-wal`, `signers.sqlite3-shm`,
  `telemetry.sqlite3-wal`, or `telemetry.sqlite3-shm` that exist in the raw fenced state; they are
  evidence inputs only, not dependencies of the normalized release;
- the complete legacy flat publication, including `/opt/technocore-observatory/index.html` and
  `/opt/technocore-observatory/data.json`, plus the old nginx vhost;
- the output of `readlink /opt/technocore-observatory/current`, or record `current` as absent for
  the verified first versioned deployment.

This first copy is disaster-recovery evidence, not an ordinary code-rollback input. Snapshot every
ledger-family member that exists and record any absent sidecar; do not synthesize a missing member.

Next, use the staged candidate's recovery-only command against the fenced live state:

```bash
(
  cd /home/technocore/observatory-candidate
  sudo -u technocore -- python3 recover_publication.py \
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

Normalize and integrity-check every fenced SQLite database before the recovery-ready copy:

```bash
sudo -u technocore -- python3 - <<'PY_SQLITE_RECOVERY'
import sqlite3
from pathlib import Path

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

Opening the fenced databases recovers any hot rollback journal before the integrity check. A
non-`ok` result or surviving journal means stop; never delete the journal to force this gate.

After it succeeds, create a separate recovery-ready state snapshot containing `ticks.jsonl`, the
recorded absence of `ticks.jsonl.ledger-pending.json`, `telemetry.sqlite3`, `signers.json`, and
`census.json`. Include `ticks.jsonl.ledger-checkpoint.json` when it exists. For a
verified legacy ledger without one, record a missing legacy checkpoint; the next accepted append
creates its canonical checkpoint. Include `signers.sqlite3` when it exists; for a v2 JSON source,
record that it was absent before migration. The recovery-ready snapshot must contain no SQLite
`-journal`, `-wal`, or `-shm`. If disaster recovery becomes necessary, keep every writer fenced,
restore every member from that one recovery-ready snapshot, then run the same recovery command
before starting a writer. Never mix members from different snapshots.

Do not continue unless the rollback directory contains the prior deployed code/config evidence,
the recovery-ready state snapshot, and the recorded `current` target.

Only now copy the candidate's tracked source files into `/home/technocore/observatory`. The
candidate must have been extracted from the verified commit archive and contain no state files, so
this additive copy cannot replace a state member. Preserve
`ticks.jsonl`, `ticks.jsonl.ledger-checkpoint.json`, and `ticks.jsonl.ledger-pending.json` as the
same family, plus `telemetry.sqlite3`, `signers.json`, `signers.sqlite3`, `census.json`, and all lock
files. Provision the query identity idempotently:

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
`mode=ro` with `PRAGMA query_only=ON`.

## 4. Run the one-time signer migration only when needed

First inspect the checked-in contract:

```bash
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

1. Run `systemctl start technocore-observatory-pulse.service` once so telemetry schema/data exist.
2. Start `technocore-observatory.service`, wait for one accepted tick, and verify collector 2.14.0,
   signer-state/SQLite schema 6, and telemetry schema 1 in local state. This upgrades an existing
   v3-v5 SQLite store before any schema-v6-only reader starts.
3. Run `systemctl start technocore-observatory-rebuild.service`. It must create a new
   `releases/<id>` and atomically set `current`; a failed build or guard leaves the prior `current`
   untouched and removes the exact unpublished candidate. The builder creates an external
   `.unpublished-<id>` sidecar before the staging-directory rename, and the rebuild clears that
   sidecar only after the atomic flip proves the candidate is current. Only then does it prune
   release history.
4. Confirm `/api/v1/status.txt`, `/api/v1/status.json`, discovery documents, local assets, and the
   static error artifacts exist beneath the resolved `current` release.
5. Start `technocore-observatory-query.service`; confirm it listens only on `127.0.0.1:8765` and
   can read but not mutate the signer database.
6. Restore the exact fenced census cron entry.
7. Enable/start `technocore-observatory-pulse.timer` and
   `technocore-observatory-rebuild.timer`.
8. Reload nginx only after another successful `nginx -t`.

The corresponding activation commands are:

```bash
systemctl start technocore-observatory-pulse.service
systemctl start technocore-observatory.service
systemctl enable technocore-observatory.service
# Wait for one accepted tick and verify schema 6 before continuing.
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
`limit`, or `format` values. Confirm every intercepted 400, 405, 429, and 503 is `no-store`, while a
successful static status, incidents, changes, or methodology response retains the documented
public cache policy.

The ten-minute rebuild retains the active release and its immediate predecessor unconditionally.
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
routes, with the same status and headers, until a newer release is published.

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
before replacing code. Restore only code/config that supports the current on-disk schema, then build
a fresh release from the current `ticks.jsonl` and `telemetry.sqlite3`. Never restore old
`ticks.jsonl`, `telemetry.sqlite3`, `signers.json`, or `signers.sqlite3` as part of an ordinary code
rollback. If old code cannot read the forward schema, leave writers stopped and roll forward with a
compatible fix instead of destroying newer observations.

No step in this runbook authorizes a post, integration message, repository push, or submission.
Those remain separately approval-gated.
