# Technocore Observatory

The Technocore Observatory is Technocore's independent read-side utility layer: a forward-only
room evidence index, an externally observed status and change record, and bounded exact-key
observation records. It never writes to Technocore, proxies a public request upstream, ranks a
participant, or converts non-observation into absence.

> **Release status, 2026-08-31:** the read-side release is implemented, merged, and live at
> `technocore.gudman.xyz`. The collector and query service are running, the pulse and rebuild timers
> are active, and a private one-curl/one-screenshot COMPASS capture has been verified. Runtime state
> remains authoritative over source history; see [DEMO.md](DEMO.md) for the bounded proof procedure.

## What the release provides

- **COMPASS:** query-gated search over the local forward room ledger, capped at 20 results, plus
  stable 16-hex room evidence URLs.
- **PULSE + REGIME:** independently stored request-attempt telemetry, an unmetered `/healthz`
  probe, bounded incidents, and observed discovery/configuration changes.
- **THE FACE:** home, status, rooms, incidents, observatory, methodology, and about views built
  from local assets with progressive-enhancement GET forms.
- **TRACE:** exact DID observation records from retained evidence. Unknown and not-recorded remain
  distinct from zero, and retained room references are hashed.
- **Agent-native discovery:** text by default, `?format=json` for JSON, plus `/llms.txt`,
  `/openapi.json`, and `/.well-known/agent.json`.

Names returned by room search are anonymous, attacker-chosen public input. They are disclosed only
after an explicit query, labelled untrusted, escaped in HTML, encoded in JSON, excluded from generic
previews, and never included in a default listing or bulk export.

## Architecture

| Component | Reads | Writes | Runtime boundary |
|---|---|---|---|
| `collect.py` | `https://technocore.chat` | append-only ticks, signer metadata/SQLite, request telemetry | `technocore` user |
| `pulse_probe.py --once` | unmetered `/healthz`, `/config`, discovery | telemetry SQLite | one-minute systemd timer as `technocore` |
| `build_site.py` through `rebuild.sh` | ticks and telemetry | a guarded `releases/<id>` candidate, one atomic `current` symlink flip, then bounded retention | ten-minute systemd timer as `technocore` |
| `query_service.py` | signer SQLite in `mode=ro`, current static snapshots | nothing persistent | `127.0.0.1:8765` as distinct `technocore-query` user |
| nginx | `/opt/technocore-observatory/current` and loopback query responses | query-free access logs | public GET/HEAD boundary |

Static status and methodology responses do not depend on the query daemon. Incidents and changes
are also static for no query string or exactly `format=json`; `since`, `limit`, combined filters,
and every other argument require the local query daemon. If it is unavailable, a filtered request
returns a bounded 503 contract instead of an unfiltered 200. Search, room evidence, and TRACE use
the same bounded failure boundary. A public request never causes an upstream Technocore request.

A failure after the builder returns a validated candidate but before the atomic flip removes that
exact unpublished candidate without changing `current`. After a successful flip, retention always
protects the active release and its immediate predecessor, then keeps the newest releases while the
retained set remains at or below both 1,008 entries (seven days at the ten-minute cadence) and 2 GiB
of apparent payload bytes. If the two protected releases alone exceed a bound, they remain and older
history is removed.

## Evidence and limits

Collection begins at the first locally accepted tick. `/r/events` and `/rooms` expose bounded
newest windows rather than pageable history, so the Observatory does not backfill or reconstruct
anything before that boundary. It retains incomplete intervals, marks collector gaps and counter
decreases, rejects malformed JSONL records instead of repairing them, and records an identity
census only after all 256 shards complete.

The room-name heuristic is only a string-shape signal. First-message-only is only an observation
within the reachable listing window. DID notes are never called people, users, or agents. Every
published rate keeps its window and denominator; missing is never rendered as zero.

The hash chain establishes internal consistency, not collection time. The private tick ledger is
not published because it contains attacker-chosen room names; public artifacts expose aggregates,
explicit-query results, and hashed identifiers instead.

`ticks.jsonl`, `ticks.jsonl.ledger-checkpoint.json`, and
`ticks.jsonl.ledger-pending.json` are one ledger recovery state family. The checkpoint binds the
last verified tip to the ledger file. The pending journal records an exact in-progress append and
may legitimately remain after an interruption so the next locked append can complete it
idempotently. The signer SQLite outbox binds a committed collector transaction to that exact
publication operation. Before making a recovery-ready backup, fence every writer and run
`recover_publication.py` with absolute ledger, signer-state, and census-state paths. It performs
no origin request, drains any committed outbox, resolves its journal, and verifies the full ledger.
Back up and disaster-recovery restore the ledger family, `telemetry.sqlite3`, `signers.json`,
`signers.sqlite3`, and the census state from one snapshot; never discard a pending journal merely
because it exists.

The normalized SQLite databases use DELETE journaling, so the release has no persistent WAL/SHM
dependency. A killed transaction can still leave a required rollback `-journal`, and a legacy
pre-migration database can have WAL/SHM evidence from its prior mode. A raw evidence snapshot must
preserve each database, its existing rollback journal, and any legacy WAL/SHM sidecars together.
Before making the recovery-ready copy, normalize the fenced database to DELETE mode, run
`PRAGMA integrity_check`, close it, and require every `-journal`, `-wal`, and `-shm` sidecar to be
absent; never discard a sidecar merely to make that gate pass.

## Requirements

- Python 3.12
- Python standard library for production code
- pytest for tests
- Playwright plus an installed Chromium-family browser only for the two render guards
- nginx and systemd only on the Linux deployment target

There are no browser analytics, cookies, credentials, CDNs, external fonts, or external client-side
requests. The telemetry database contains the collector's and pulse probe's normalized upstream
attempts; it is not visitor telemetry.

## Exact local commands

Use absolute state paths so the local run matches the deployment boundary. In PowerShell from the
repository root:

```powershell
$repo = (Get-Location).Path

python .\collect.py `
  --base-url https://technocore.chat `
  --output "$repo\ticks.jsonl" `
  --telemetry-database "$repo\telemetry.sqlite3" `
  --signer-state "$repo\signers.json" `
  --once

python .\pulse_probe.py `
  --base-url https://technocore.chat `
  --telemetry-database "$repo\telemetry.sqlite3" `
  --once

python .\build_site.py `
  "$repo\ticks.jsonl" `
  "$repo\telemetry.sqlite3" `
  "$repo\public" `
  --template "$repo\index.html"
```

`build_site.py` prints the exact versioned release directory. Use that path for the local query
service and guards:

```powershell
$release = (Get-ChildItem "$repo\public\releases" -Directory |
  Sort-Object Name -Descending |
  Select-Object -First 1).FullName

New-Item "$release\errors" -ItemType Directory
Copy-Item ".\deploy\fallback\*" "$release\errors\"

python .\query_service.py `
  --database "$repo\signers.sqlite3" `
  --snapshot-root "$release" `
  --host 127.0.0.1 `
  --port 8765

python .\guards.py `
  --html "$release\observatory\index.html" `
  --derive "$repo\derive.py" `
  --ticks "$repo\ticks.jsonl" `
  --site-root "$release"
```

`query_service.py` refuses a relative database or snapshot path and refuses any bind address other
than `127.0.0.1`. The signer database is `signers.sqlite3`, derived by the collector from the
`--signer-state signers.json` metadata path. The current public contract is methodology 1.13.0;
the deployed query unit must advertise that exact version.

For a complete test run:

```powershell
python -m pip install -r requirements-dev.txt
python -m pytest -q
python -m py_compile (Get-ChildItem -Filter *.py | ForEach-Object FullName)
python -m ruff check . --per-file-ignores "derive.py:F841"
python -m ruff format --check .
```

The Ruff command records the one known pre-existing unused local in `derive.py:2808` as an explicit
waiver; the roadmap implementation does not change that dead-code finding.

On Windows, the deployment tests perform structural nginx and systemd validation and say so in
their skip messages. They do not claim that `nginx -t` or `systemd-analyze verify` ran. Those two
runtime checks belong on the Linux target immediately before activation.

GitHub Actions runs the full suite, compilation, Ruff checks, and real-browser render coverage on
Python 3.12 across Ubuntu and Windows, with the deployment shell syntax check on Ubuntu. Direct test
tools are pinned in `requirements-dev.txt`; production remains standard-library-only.

## Public API

- `GET /api/v1/status`
- `GET /api/v1/incidents?since=&limit=`
- `GET /api/v1/changes?since=&limit=`
- `GET /api/v1/methodology`
- `GET /api/v1/rooms/search?q=&limit=1..20`
- `GET /api/v1/rooms/{16-hex}`
- `GET /api/v1/dids/{did}`

Text is the default API representation. Add `?format=json` for JSON. All public routes accept only
GET and HEAD. Responses are credential-free CORS; room and DID surfaces are non-indexable. Query
limits, byte limits, a SQLite progress deadline, loopback binding, read-only/query-only SQLite, and
nginx rate limits bound the dynamic surface. nginx-generated API 400, 405, 429, and 503 responses
use matching bounded text/JSON contracts; `format=json` selects JSON and text is the safe default.

## Tests and guards

The suite covers collection retries and validation, migrations, SQLite persistence, query bounds
and escaping, zero-upstream behavior, snapshot freshness/incidents/changes, static release
completeness, deployment structure, and accessibility contracts.

The pre-publication guard runs the four existing checks unchanged in purpose:

1. tick-ledger hash-chain verification;
2. producer/consumer payload contract;
3. real-browser zero-width layout detection;
4. no-JavaScript honesty.

It also validates the complete built static tree, API discovery files, local-only assets,
non-indexing policy, and progressive GET forms. Any real finding returns non-zero. A missing browser
is reported as a skipped render check rather than a successful runtime validation.

## Source, licence and contact

Source: <https://github.com/Ridwannurudeen/technocore-observatory>

The code is licensed under the Apache License 2.0; see [LICENSE](LICENSE). Copyright 2026 Ridwan
Nurudeen. Questions, corrections, and disputes belong on the repository issue tracker.

The Observatory is an independent instrument. It is not affiliated with FLOP Labs or with the
measured service.
