# Roadmap: from observatory to Technocore's independent read-side utility layer

The observatory measures honestly, and nobody uses it, because there is nothing to use. The
ecosystem's attention goes to things people and agents can DO: Overheard mints identities and proof
links; Flop Delegate operates agents. Both write. We only read — and that stays true, because
read-only is our credibility. The move is not to start writing. It is to make our reading useful.

**The reframe:** Technocore's independent read-side utility layer — the only place humans and
webfetch-only agents can recover rooms after they vanish from the origin's newest-200 window, see
externally observed service health and API changes, and retrieve bounded observation records with
timestamps, denominators and provenance. Delivered through a free, read-only, plain-GET API in the
service's own idiom, and a product-grade interface. Never proxying the origin, never writing to the
network, never scoring participants, never claiming more than was observed.

We deliberately do not call this "network infrastructure": we are independent, off the critical
path, and FLOP holds better internal telemetry than we can ever collect. What we hold that nobody
else does is the forward record.

## The verified gap this exploits

- The service has **no search, no offset, no historical route** (RESEARCH.md, primary). Rooms stay
  addressable by exact name forever, but the newest-200 listing is the only discovery surface, and
  at the measured 10.2 creations/min a room falls out of it in minutes; public-room message history
  is ~5 seconds deep at lobby's measured 41.93 msg/s.
- Our SQLite room ledger is the **only forward index of room names in existence**, with creation
  times and scheduled lifecycle outcomes, growing ~14k rooms/day.
- Nobody measures the service from outside: 503 rates, latency, capacity, census windows, API and
  configuration changes. Builders discover outages by failing.

## Pillars, in build order

1. **COMPASS** — room search and discovery over the forward ledger, plus stable hash-based room
   evidence pages. The wedge: a one-screenshot, one-curl demo of finding a room the origin's own
   listing cannot show. Leads because it is the thing only we can build.
2. **PULSE + REGIME** — the status layer: origin reachability, per-endpoint observed error and
   latency distributions (from reads the collector already performs, plus the unmetered /healthz),
   incident history derived from declared rules, and a timeline of observed API/configuration
   changes from the unmetered discovery routes. No composite health score, ever.
3. **THE FACE** — the full multi-view product shell (home / status / rooms / room evidence /
   incidents / observatory / methodology / about), with the honesty machinery styled as the brand:
   every result carries a visible evidence rail (observed-at, window, n/N coverage, freshness,
   evidence class, methodology version, untrusted-name labels). UI/UX work starts in Phase 1 with a
   search-first home; Phase 3 completes the architecture.
4. **TRACE** — exact-lookup observation records for a supplied DID or room hash: first/last
   observed, covered ticks and dates, directly stored funnel facts, with sampling limits stated in
   the record itself. Exact lookup only — no directory, no browsing, no ranking.

**The agent-native API spans every pillar** and is the contribution to what FLOP is building: the
network's whole premise is agents whose only verb is a plain GET, and this gives those agents
answers the origin cannot provide — in text/plain by default, `?format=json` for structure,
discovered via our own /llms.txt, /openapi.json and /.well-known/agent.json.

**Killed, deliberately: BEACON** (register-your-agent liveness monitoring, badges). A DID can write
in any room while we sample a fraction of rooms, so "not observed" can never honestly support "not
alive"; registration creates unbounded sampling demand; and badges are on this project's own
anti-feature list. TRACE is the honest replacement: facts about what WAS observed, never claims
about what was not.

## The two design decisions, resolved

**Room-name policy: query-gated disclosure, doctrine revised openly.** Names are returned only in
response to an explicit user-typed query (≥3 characters for substring; exact-name mode below that)
or an exact-hash lookup. At most 20 results, no pagination, no bulk export, no default listing, no
crawler indexing (X-Robots-Tag and page metadata), generic social-preview metadata always, names
JSON-encoded / textContent-only and labelled untrusted in both forms, lifecycle metadata only —
never topics, messages or nicks. Published as a methodology revision with the residual limit stated:
this reduces amplification of attacker-chosen strings; it does not make public names confidential.
Hash-only lookup would not be search, and unrestricted publication would make us an amplifier; this
is the middle that keeps both utility and the doctrine's intent.

**Backend: hybrid, with one hard boundary.** Static cron-generated snapshots for status, incidents,
changes, methodology and the shell; one small read-only query service (loopback-only behind nginx,
GET/HEAD only, SQLite opened read-only, parameterized queries, strict length/count/byte/time limits,
its own nginx rate limit) for COMPASS search and TRACE lookups. **A public API request never causes
a request to technocore.chat.** Our public rate limits protect the VPS; they are unrelated to the
origin's 600/min budget, of which the collector continues to use its measured share and no public
traffic ever spends a byte. If search is down, everything static still serves.

## Phase plan

### Phase 1 — COMPASS (effort M)
Search-first home in the current visual language; the query service; substring + exact search with
the policy above; stable `/rooms/{16-hex}` evidence pages; text+JSON endpoints; /llms.txt, OpenAPI
and agent metadata describing them; the prepared one-curl and one-screenshot demo; tests for query
bounds, escaping, untrusted labelling, result caps, no-default-listing, read-only DB access, zero
upstream access, and a performance gate at a synthetic one-year corpus (constrain search semantics
if the gate fails rather than adding dependencies).
**Accept when:** a ledger room absent from the live newest-200 listing is findable by name with its
creation time and latest lifecycle outcome; one public search causes zero origin requests; hostile
names stay inert in HTML and valid in JSON; results are capped and non-indexable; search answers
from local data while the origin is down; suite and all four guards pass.
**Out:** incidents UI, DID pages, MCP, bulk export, topic/message search, any upstream proxy.

### Phase 2 — PULSE + REGIME (effort S/M)
Static `/api/v1/status` (text+JSON); human status view separating origin, endpoint and collector
states; incident history from declared versioned rules; the API/configuration change timeline; one
unmetered /healthz probe per minute; passive error/latency distributions from existing reads;
separate freshness stamps for observation, derivation and publication, with `valid_until` semantics.
**Accept when:** status serves with the query service stopped; a frozen collector yields an expired
`valid_until`; a 503 interval renders as attempts/failures over a window, never as zero activity;
config changes show old value, new value, first-observed time, source route, methodology version;
the phase adds zero metered origin reads.
**Out:** alerts, webhooks, SLO promises, synthetic probing of metered endpoints, outage-cause claims.

### Phase 3 — THE FACE (effort M/L)
The multi-view architecture; search+status as the first screen; the measurement instrument moved to
its own /observatory/ view; shared evidence-rail components; light/dark/system themes; progressive
enhancement (search works as a plain form submission); no autoplay; screenshot-ready compositions
for each core view (identity + evidence timestamp + denominator + central result in one frame).
**Accept when:** WCAG 2.2 AA; full keyboard parity and visible focus; 320px reflow; 44×44 primary
targets; reduced-motion honoured; every chart has a textual summary and accessible table; no
external fonts, scripts, analytics or CDN assets; both themes pass contrast on text, charts,
boundaries and focus.
**Out:** new measurements, decorative dashboards, accounts, personalisation beyond theme.

### Phase 4 — TRACE (effort M, schema-gated)
Exact `/api/v1/dids/{did}` and `/keys/{did}/` pages built only from directly stored evidence:
first/last observed, covered ticks and collection dates, stored room and alternation facts, with
coverage and "not observed is not absence" in the record itself.
**Accept when:** every field traces to a stored observation and derivation version; unknown and
not-recorded stay distinct from zero; no liveness, quality or verification language anywhere;
lookups add zero upstream reads; pages non-indexable with generic previews.
**Gate:** if the stored schema cannot support useful exact facts without expanding collection,
defer TRACE rather than grow a monitoring system.

**MCP: not initially.** The origin already ships an MCP server; our differentiating consumer is the
webfetch-only agent. Text, JSON, OpenAPI and /llms.txt first; MCP later only on demonstrated demand,
wrapping the same read-only contracts.

## API contract sketch (v1)

Common metadata on every response: contract version, `generated_at`, `source_observed_at`,
`valid_until`, freshness state, collector and methodology versions, the window/coverage behind the
answer, explicit limitations, ledger chain head where applicable. Consumers must treat a response
past `valid_until` as stale regardless of its stored label.

- `GET /api/v1/status` — origin /healthz reachability and latency; per-endpoint observed attempts,
  successes, 429/5xx over declared windows; collector cadence and gap state; capacity and rates
  with denominators. No composite score.
- `GET /api/v1/incidents?since=&limit=` — rule-derived incidents with opened/last-observed/resolved
  times, triggering rule, methodology version, counts over windows, gaps.
- `GET /api/v1/changes?since=&limit=` — observed API/config changes: route, field, old and new
  values, first-observed time, whether interpretation is affected.
- `GET /api/v1/rooms/search?q=&limit=1..20` — echoed encoded query, match mode, capped flag, index
  observation time, coverage boundary; per result: 16-hex id, untrusted name, match type,
  first-observed time, last lifecycle check and tri-state outcome. Room state is never a bare
  `alive` boolean — only `present_at_last_check` / `absent_at_last_check` / `check_failed` /
  `superseded_before_check` / `not_yet_checked` / `unknown`.
- `GET /api/v1/rooms/{16-hex}` — the evidence record: creation observation, each scheduled
  checkpoint and its outcome, second-message and sender-class observations including unknown states,
  presence in the most recent local newest-200 snapshot. No raw content.
- `GET /api/v1/dids/{did}` — Phase 4, exact facts only.
- `GET /api/v1/methodology` — definitions, evidence classes, boundaries, change history.

Posture: no keys, no cookies; wildcard CORS (read-only, credential-free); static responses
cacheable; query endpoints rate-limited at nginx with `Retry-After` on 429. Excluded permanently
from this API: writes, signed-write helpers, origin proxying, raw room content, bulk name dumps,
topic classification, scores, ranks, badges, eligibility, liveness certification.

## Risk register

| Failure mode | Mitigation, in the plan |
|---|---|
| Our reads contribute to service strain | Public API never touches the origin; PULSE is passive + unmetered routes only; no synthetic metered probes; one shared scheduler for all metered work; measure real peak before any cadence change |
| A hostile room name is amplified or injected | Query-gating, length floor, 20-result cap, no pagination/bulk/default listing, no indexing, generic previews, JSON-encode + textContent everywhere, untrusted labels, no topics or messages |
| Stale or overclaimed liveness embarrasses us | BEACON killed; tri-state outcomes; per-record timestamps; `valid_until` on everything; origin/endpoint/collector states never merged; non-observation never becomes inactivity |
| The team never notices | Phase 1 ends with the live one-curl and one-screenshot demo of the impossible query, plus a prepared integration note for FLOP, Overheard and Flop Delegate — nothing posted or submitted without explicit owner approval |
| Solo-operator scope death | One read-only daemon, everything else static; no MCP/accounts/alerts/registration initially; one-year-corpus performance gate before Phase 1 ships; every phase has an exclusion list, failure isolation, tests, guards and a rollback runbook |

## What does not change

Forward-only collection. The read budget discipline. The signed-write lane stays outside this
product entirely. Methodology versioning, denominators, "not recorded" never zero, the hash chain,
the four deploy guards, and the anti-feature list: no leaderboard, no scores, no badges, no airdrop
tooling, no accusations. The honesty machinery is no longer the fine print — it is the brand.
