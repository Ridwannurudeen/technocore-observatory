# Technocore Observatory

The Technocore Observatory is a forward-collected, animated view of measurable Technocore activity. It is deliberately not a status page and not a reconstruction of the earlier surge.

The service cannot page backward through its event or room listings. Collection therefore begins at the first locally recorded tick. The page never draws, infers, back-fills, or synthesises history before that timestamp.

## What it measures

Each successful collector tick records:

- UTC collection time
- total rooms and the room cap
- stored bytes
- total notes and the separate note cap
- latest lobby sequence
- latest public-room event sequence
- the newest event window and validated room rows
- room-class counts for `p-`, `mb-`, `d-`, `e-`, bare 16-hex, and other names
- an optional complete census of all 256 `did-00` through `did-ff` identity-note namespaces

The derived page shows:

- public rooms observed after collection began
- room-creation and lobby-message rates
- room-class composition by collected interval
- identity totals and growth between complete censuses
- a first-message-only signal among newly observed rooms still present in the newest room listing
- an openly defined auto-generated-looking name signal
- the service's own engagement figures — nick diversity, note-to-message ratio, and zero-response share — republished unverified beside the window figures the service declares; a tick without them reads "not recorded", never zero

The name heuristic marks a base name when it is a bare 16-hex token, UUID-like, or an unseparated alphanumeric token of at least 12 characters containing both letters and digits. This is only a string-shape signal. It does not identify farming, ownership, people, or agents.

The first-message-only measure is also a signal rather than a final stillborn verdict. It counts captured new rooms that can still be matched in the newest room listing and have `seq <= 1`. A room may later receive messages, and rooms outside the newest listing cannot be followed through the available interface.

## Limits that shape the design

`/r/events` exposes one server-written event for each new public room but returns at most the newest 200 events. Supplying an older `since` value does not page backward.

`/rooms` likewise exposes only the newest 200 rooms. There is no usable historical offset for older rooms.

Consequently:

- no event or room history before `collection_started` is shown
- the first event window establishes a baseline and contributes no invented interval
- a sequence delta can measure aggregate activity while the event window may still be too short to recover its composition
- incomplete composition intervals are retained, marked incomplete, and rendered faded
- polling gaps and decreasing counters are explicit metadata
- malformed or partial responses produce no collector tick
- malformed JSONL ticks are rejected rather than repaired
- identity totals are recorded only after all 256 shards complete successfully

Room names, topics, and values are anonymous world-writable input. The collector retains names only to calculate interval aggregates and match current room rows. `data.json` contains numeric aggregates rather than those names. The page uses DOM text nodes for dynamic text.

## Requirements

- Python 3.12
- Python standard library for collection and derivation
- pytest only for tests
- a local or remote HTTP service origin supplied explicitly with `--base-url`

There are no writes to the service, credentials, external browser requests, CDNs, frameworks, or telemetry.

## Collect

Use absolute paths for collector output and census state:

    python observatory/collect.py \
      --base-url https://SERVICE-ORIGIN \
      --output C:\absolute\path\to\observatory.jsonl

The default interval is 60 seconds. A single ordinary poll is:

    python observatory/collect.py \
      --base-url https://SERVICE-ORIGIN \
      --output C:\absolute\path\to\observatory.jsonl \
      --once

A complete identity census is:

    python observatory/collect.py \
      --base-url https://SERVICE-ORIGIN \
      --output C:\absolute\path\to\observatory.jsonl \
      --census \
      --census-state C:\absolute\path\to\identity-census-state.json

The census reads the 256 `did-xx` namespace listings with pacing. Its state file is updated after each valid shard, so an interrupted census resumes at the next unfinished shard. After a completed census has been recorded, the next `--census` invocation begins a fresh census.

HTTP 429 responses use `Retry-After` when valid, then a retry duration found in the body. HTTP 5xx responses and transport failures use bounded exponential backoff. Exhausted retries, empty bodies, malformed headers, incomplete room listings, truncated shard listings, and unexpected event shapes cause the attempt to fail without appending a tick.

The JSONL history is append-only. The collector opens it in append mode, writes exactly one JSON object for a successful tick, and never rewrites prior ticks.

## Derive and embed

Generate compact page data:

    python observatory/derive.py \
      C:\absolute\path\to\observatory.jsonl \
      observatory\data.json

Generate `data.json` and replace the embedded payload in the self-contained page:

    python observatory/derive.py \
      C:\absolute\path\to\observatory.jsonl \
      observatory\data.json \
      --html observatory\index.html

The default polling-gap threshold is 300 seconds. Change it only when the intended polling cadence requires a different definition:

    python observatory/derive.py input.jsonl output.json --gap-seconds 900

Open `observatory/index.html` directly or serve that single file from any static web server. It makes no external requests.

Run derivation again whenever new ticks should be published. The HTML injection replaces only the `observatory-data` JSON script; it does not add observations that are absent from the JSONL source.

## Test

    pytest -q observatory/test_derive.py

The tests cover rate calculations, polling gaps, a one-point series, malformed-tick rejection, and the invariant that derivation never fabricates ticks.

## Historical honesty

No history before collection start can be shown.

The earlier surge is unrecoverable through the listed interfaces. The empty region before collection is a result, not missing decoration. Any claim about how much of the observed identity population represents agents, people, or farming remains outside what these endpoints can prove.

## Source, licence and contact

The observatory's source lives at https://github.com/Ridwannurudeen/technocore-observatory.

The code is licensed under the Apache License 2.0; see `LICENSE`. Copyright 2026 Ridwan Nurudeen.

Questions, corrections, and disputes about any published measurement belong on the repository issue tracker: https://github.com/Ridwannurudeen/technocore-observatory/issues.

The observatory is an independent instrument. It is not affiliated with FLOP Labs or with the measured service.
