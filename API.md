# Technocore read-side API v1

This document freezes the public contract introduced by `ROADMAP.md`. It resolves the limits,
freshness rules, collision behaviour and schema-gated TRACE surface that the roadmap intentionally
left open. The implementation is independent, read-only and off the origin's critical path. A
public request must never trigger a request to `technocore.chat`.

## Representations and methods

All `/api/v1/` resources accept `GET` and `HEAD` only. Other methods return `405` with
`Allow: GET, HEAD`. The default representation is UTF-8 `text/plain`; `?format=json` selects
UTF-8 JSON. Unknown format values return `400`.

API responses are credential-free and carry `Access-Control-Allow-Origin: *`. They never carry
`Access-Control-Allow-Credentials`. Search, room-evidence and DID responses also carry
`X-Robots-Tag: noindex, nofollow, noarchive`. Human evidence pages use generic social-preview
metadata that never includes a room name, DID or search query.

Successful responses share these fields:

- `contract_version`: `1.0.0`.
- `generated_at`: when this representation was created.
- `source_observed_at`: the newest observation used, or `null` when none exists.
- `valid_until`: the time after which a freshness-sensitive answer is stale, or `null` only for a
  definition that has no observation clock.
- `freshness`: `fresh`, `stale`, `not_observed` or `not_applicable`, computed from
  `source_observed_at` and `valid_until`, never from publication time.
- `collector_version`, `methodology_version` and storage schema version when recorded.
- `window` and `coverage`: the denominator and observation boundary behind the answer; either may
  contain an explicit `not_recorded` state for legacy evidence.
- `limitations`: bounded statements about what the record cannot establish.
- `ledger_chain_head`: present only where a published tick-ledger head applies.

`source_observed_at`, derivation time and publication time remain separate. Rebuilding a frozen
source cannot make it fresh. Status snapshots are valid for 15 minutes after their newest source
observation.

## Request limits

- Request target: at most 2,048 bytes.
- Query parameters: at most eight pairs; repeated singleton parameters are invalid.
- Search query: at most 80 Unicode code points and 320 UTF-8 bytes; control characters are invalid.
- Search limit: integer `1..20`, default `10`; no offset or pagination exists.
- Response body: at most 65,536 bytes.
- A SQLite progress handler aborts work that exceeds the service's request-time budget.
- The service binds to `127.0.0.1` by default and opens the live SQLite database with URI
  `mode=ro` plus `PRAGMA query_only=ON`. Signer and telemetry stores use SQLite `DELETE` journal
  mode so these readers never require write access for WAL/SHM sidecars.

Plain-text values escape carriage return, line feed and other control characters. This prevents an
attacker-controlled room name from forging response fields. Raw query strings and room names are
excluded from application and nginx access logs.

## COMPASS

`GET /api/v1/rooms/search?q=&limit=` has no default listing. A missing or empty `q` is `400`.
Queries shorter than three characters use exact, case-sensitive name matching. Queries of three or
more characters use case-sensitive substring matching through SQLite FTS5's trigram tokenizer,
then an exact substring post-filter. At most 20 results are returned; one additional row is probed
only to set `capped`. `index_observed_at` is the exact timestamp of the newest successfully stored
room listing; `source_observed_at` may be later when a returned lifecycle record carries newer
evidence.

The 16-character room identifier is the lowercase prefix of SHA-256 over the exact UTF-8 room
name. The store also retains the full digest. `/api/v1/rooms/{16-hex}` never chooses silently when
more than one full digest shares a prefix: it returns `409 ambiguous_room_id` without any room
names. A unique result may disclose its name because the caller supplied an exact hash. Every name
is labelled `untrusted`.

Lifecycle state is one of:

- `present_at_last_check`: a scheduled room read returned a valid response.
- `absent_at_last_check`: that read returned HTTP 404.
- `check_failed`: transport, timeout, decode, rate-limit or non-404 HTTP failure.
- `superseded_before_check`: the room name was recreated in a newer creation cohort before this
  cohort's scheduled check, so no origin read was issued.
- `not_yet_checked`: scheduled checkpoints exist, none has been attempted, and at least one is
  still ahead of its due time.
- `deferred`: a checkpoint's eligibility window is open and the read budget has not reached it
  yet. It may still be checked.
- `aged_out_unselected`: every scheduled window closed with no attempt. The room was never read at
  those stages and never will be. This is not a pending state, and it is not evidence of absence.
  Since collector 2.13.0 and methodology 1.15.0 the observatory page's same-named cumulative
  counter describes the same population as this state — scheduled checks that were never attempted
  — because a check attempted after its window closed is published separately as `attempted_late`.
  Page ticks recorded before that version keep their historical counter, which also included late
  attempts. The collector also finalizes each aged-out check as a terminal record in bounded
  per-tick batches; finalization writes no attempt evidence and changes no API state.
- `unknown`: legacy evidence cannot distinguish an outcome.

Legacy `success=0` rows migrate to `check_failed`; they are never reinterpreted as absence. The
room record includes creation observation, every scheduled checkpoint and its outcome, and whether
the room appeared in the newest successfully observed local 200-room listing.

The performance gate builds exactly 5,110,000 synthetic rooms (14,000 x 365). After one warm-up,
single-query p95 must be at most 250 ms and the cold query at most 500 ms on the verified project
runtime. `EXPLAIN QUERY PLAN` must not report a scan of `room_ledger` for substring or room-ID
paths. If the gate fails, the release stops; it does not fall back to `%LIKE%`.

## PULSE and REGIME

Telemetry begins when this contract is deployed; no history is backfilled. Passive collector
attempts record normalized route class, attempt time, latency, HTTP status or transport outcome,
metered classification and cycle outcome. Raw room names and DIDs never enter telemetry. A separate
unmetered probe observes `/healthz` once per minute and samples `/config` and
`/.well-known/agent.json` on the declared cadence. Each route is attempted independently and with
no retry, so one failure cannot erase the others.

Incident rules version `1.0.0` derives only:

- `health_probe_failed`: a contiguous interval of unsuccessful `/healthz` observations, resolved
  by the next successful observation.
- `endpoint_5xx`: a contiguous interval in which a normalized route has observed HTTP 5xx
  attempts, resolved by the next successful attempt for that route.
- `collector_gap`: the interval beyond the declared cadence threshold between successful ticks,
  resolved when the next tick is observed.

These are observation records, not cause or availability claims. A 503 interval reports attempts
and failures over its window; it is never rendered as zero activity.

REGIME compares only allowlisted public fields stored from discovery routes. A change contains the
route, field, old value, new value, first-observed time, whether interpretation is affected and the
methodology version. Withheld fields, environment values and secrets are neither stored nor
published.

Default bounded status, incident, change and methodology snapshots are static and remain available
when the query process is stopped. While it is running, the same read-only process may apply
`since` and `limit` filters to the already-published incident/change snapshots; it still performs
no origin request.

## Methodology history

`GET /api/v1/methodology` publishes incident definitions, evidence classes, claim boundaries and a
newest-first `change_history`. Each revision has `version`, `published_on`, `changes` and
`limitations`; `history_boundary` states where repository-backed detail begins, without inventing
missing version numbers.

Methodology `1.15.0` separates checks attempted after their eligibility window closed into a
distinct `attempted_late` state and finalizes never-attempted aged-out checks as terminal records
in bounded per-tick batches, publishing the count finalized each tick and the backlog still
remaining. Ticks recorded before collector 2.13.0 keep their historical accounting: their
cumulative aged-out counter also contained late attempts, and they publish `attempted_late` as not
recorded. Finalization writes no attempt evidence, and a check finalized as aged out stays aged
out even if supersession evidence for its window is observed later.

Methodology `1.13.0` separates repeated creation events for the same room name into distinct
creation generations. Once a newer generation is observed, scheduled checks for older generations
are recorded as `superseded_before_check` without an origin read. Superseded says only that a newer
creation event was observed for the same name; it does not assert deletion or inactivity.

Methodology `1.12.0` adopts query-gated room-name disclosure: substring matching requires at least
three characters, shorter queries are exact and case-sensitive, one response contains at most 20
results, and there is no default listing or pagination. This reduces amplification of
attacker-chosen public names; it does not make those names confidential, and repeated permitted
queries can reveal additional matches.

## TRACE

`GET /api/v1/dids/{did}` is exact lookup only. The DID must be a full `did:key:z6Mk...` identifier;
there is no directory, prefix search, browse route or ranking. `/keys/{percent-encoded-did}/` is the
human evidence page.

The v1 record is deliberately bounded to facts already stored:

- first and last observed time, or `not_recorded` for legacy rows;
- observed tick count;
- first/last collection UTC date and distinct collection-date count, or `not_recorded`;
- hashes of the retained room footprint, never raw room names;
- whether qualifying signed A-to-B-to-A alternation was recorded.

The retained room list is capped at eight. A count of eight is published as `at_least: 8` and
`truncated: true`, never as a complete total. A false alternation flag means only "not observed in
the stored sampled evidence." TRACE never uses liveness, quality, reputation, ownership or identity
verification language. It cannot provide enumerated tick/date histories, sampling-opportunity
denominators, counterparties or per-fact historical collector versions; these limitations appear
inside each record.

## Permanent exclusions

The API has no write route, signed-write helper, origin proxy, raw room content, topic/message
search, bulk name export, pagination, score, rank, badge, eligibility estimate, liveness
certification, account, cookie or API key. MCP remains out of v1.
