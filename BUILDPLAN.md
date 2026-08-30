# Build plan: Technocore Observatory, correctness pass

Execute in order. Phases 0–1 are **defect fixes** — the live page currently makes two claims
that are not true. Phases 2–3 are additions. Do not start a later phase before the earlier one
is complete and its acceptance criteria are met.

Your sandbox cannot read files or reach the network. You author TEXT; the operator deploys and
verifies against the live service.

**OUTPUT FORMAT** — markers, NO code fences:

    === FILE: relative/path ===
    (entire file contents verbatim)
    === END FILE ===

Emit only files you changed, complete. Emit them in phase order.

---

## Verified current state — do not re-derive

Live at `https://technocore.gudman.xyz`. Collector runs every 120 s as a systemd service,
`rebuild.sh` regenerates and publishes every 10 min, census every 6 h. 102 ticks, 98 accepted,
0 rejected, 0 gaps.

Files: `collect.py`, `derive.py`, `index.html` (the template), `test_derive.py`.

Mechanisms I read this session:

- `derive.py::inject_html(path, data)` replaces **only** the body of
  `<script id="observatory-data" type="application/json">…</script>` via a regex, then writes the
  file back. Nothing else in the HTML is touched at build time.
- `collect.py::room_sample_names(newest_rooms, cursor)` dedupes `newest_rooms`, always includes
  `lobby`, then takes `ROOM_READ_BUDGET - 1` names starting at `cursor` and advances
  `cursor` modulo the candidate count. `signers.json` holds `rotation_cursor` (currently 42).
- Page JS globals: `data` (parsed payload), `points` (`data.points`), module-level `current`,
  render entry `update(index)`, helpers `byId`, `formatInt`, `formatRate`, `formatPercent`,
  `formatTime`, `setRate`. **`DATA` does not exist** — referencing it throws inside `update()`,
  which aborts `initialise()` before autoplay and before listeners attach, silently freezing the
  page. This already happened once.

Current funnel reading: 556,973 well-formed DID notes → 21,104 observed signing → 7,425 on
≥2 ticks → 1,287 on ≥2 UTC dates → 1,270 in ≥2 rooms → 1,270 with ≥1 signed counterparty.

---

## Immovable constraints

- **No historical backfill, ever.** `/r/events?since=<anything>&limit=200` always returns the
  newest 200; `limit` caps at 200; there is no `before` and no offset. `/rooms?limit=1000&offset=200`
  likewise returns only the newest 200. Nothing before collection start may be drawn, inferred,
  interpolated or synthesised.
- **One self-contained HTML file.** No external requests, no CDN, no web fonts, no telemetry.
- **Everything the service returns is attacker-controlled** — room names, topics, nicks, note
  values, DIDs. Never `innerHTML`; use `textContent` and attribute-safe assignment.
- No percentage without its denominator. No rate without its sample count and window. No line
  drawn across a collector gap. Missing is never zero.
- No composite score, no leaderboard, no named farmer/bot/sybil accusation, no airdrop
  estimator, no countdown.
- Never call DID notes "agents" or "users". They are **well-formed published DID notes**.
- Python 3.12, stdlib only (plus `pytest`). No new dependencies. No Claude/Anthropic/Codex
  attribution anywhere.

---

## PHASE 0 — The page is empty without JavaScript (defect)

**The problem.** All numbers render client-side from the embedded JSON. A `curl`, a crawler, or
a link-preview bot sees the literal strings *"Awaiting observations"* and *"No observations have
been collected"* while 98 real observations sit in the payload. There are no `og:` or
`twitter:` meta tags at all. The page exists to be shared by FLOP Labs; shared, it currently
looks broken.

**Build.**

1. In `index.html`, mark every element whose text is a headline figure with a stable
   `data-ssr="<key>"` attribute — at minimum: observation count, collection start, rooms
   observed, room-creation rate, lobby velocity, identity census, funnel stage counts, notes and
   rooms utilisation. Keep the existing element ids; add the attribute alongside.
2. Extend `derive.py` so `inject_html` **also** fills those elements' text content at build
   time, from the same derived data it embeds. Implement it as a small, explicit
   `{key: rendered string}` map — no template engine, no HTML parsing library. Escape with the
   same rules as everywhere else; these values are numeric aggregates, but escape anyway.
3. The page JS must keep working exactly as now: on load it re-renders from the JSON and
   overwrites the server-rendered text. Server rendering is a fallback, not a second source of
   truth. If the JSON is missing or unparseable the JS must leave the server-rendered text alone
   rather than blanking it.
4. Add `<meta name="description">`, `og:title`, `og:description`, `og:type`, `og:url`, and
   `twitter:card` (`summary_large_image` only if an image actually exists; otherwise `summary`).
   `derive.py` must refresh the description text at build time so it always states the current
   observation count and collection window. No `og:image` unless a real image file is produced —
   do not reference one that does not exist.

**Acceptance.** With JavaScript disabled, the page shows the real current figures and never the
words "Awaiting observations" or "No observations have been collected" while observations exist.
`curl` output contains the current observation count. Both `<script>` blocks still parse — the
JSON one as JSON, the page one as JS.

---

## PHASE 1 — The funnel misstates its own sampling frame (defect)

**The problem.** The panel says *"sampled 20 of 29,000 known rooms"*, implying a sample of the
network. It is not. `room_sample_names` draws from `newest_rooms`, which is the newest **200**
rooms — and that window churns almost entirely every ~10 minutes at ~21 new rooms/min. The
~30,000 older rooms are **structurally unreachable**: the API exposes no offset. So the funnel
describes signing behaviour **among recently created rooms**, which is precisely the population
most likely to be stillborn — a strong selection bias currently presented as network coverage.

**Build.**

1. Report the frame honestly. The denominator for sampling is **the newest-200 listing**, not
   the room total. Show the room total separately and clearly labelled as
   "rooms in existence (not reachable for sampling)".
2. Relabel every funnel stage with an explicit `observed` prefix, and render the first
   transition as a **lower bound**: "at least 21,104 of 556,973 DID notes observed signing",
   never "3.8% sign". Later stages are conditional on the captured cohort and must say so —
   they describe activity-biased survivors, not the population.
3. Collapse the final three stages into one terminal **"sustained reciprocal footprint"** stage
   with the three predicates (≥2 UTC dates, ≥2 rooms, ≥1 distinct signed counterparty) listed
   beneath it. 1,287 → 1,270 → 1,270 currently reads as three independent proofs and ends in a
   100% conversion that is a tautology.
4. State the bias in the panel itself, in one plain sentence: the sampling frame is the newest
   rooms, so the funnel is biased toward new and short-lived rooms.

**Acceptance.** No element of the funnel panel presents the room total as a sampling
denominator. The first transition reads as a lower bound. There is no 100% conversion step.

---

## PHASE 2 — Make coverage measurable (currently unknowable)

**The problem.** `ticks.jsonl` records `sampled_rooms: 20` but **not which rooms**. Cumulative
unique-room coverage therefore cannot be computed from our own data in either direction. This is
the missing piece that would make the funnel defensible.

**Build.**

1. Record, per tick, the identifiers of the rooms actually sampled — plus, for each, whether the
   read succeeded. Room names are attacker-controlled: record a **stable hash** (e.g. first 16
   hex of SHA-256 of the name) as the identifier, so the manifest never republishes hostile
   strings, and note in the methodology that hashes are used and why.
2. Make the rotation a reproducible seeded permutation of the current frame, walked without
   replacement before reshuffling. Persist the seed/epoch and a selector version in
   `signers.json`. The current cursor-modulo-a-churning-list does not sweep, and cannot be
   reasoned about.
3. Derive and display: frame size, rooms sampled this tick, **cumulative unique rooms observed**,
   repeat count, failed reads, selector version. Cumulative unique coverage must be expressed
   against the frame it was drawn from, not the room total.
4. Keep the read budget at 20 per tick. Do not increase it until the existing budget is being
   spent without avoidable repetition.

**Acceptance.** From `ticks.jsonl` alone, a third party can compute how many distinct rooms were
ever sampled and how often reads failed. The page shows a measured cumulative coverage number,
not an assumption.

---

## PHASE 3 — Methodology v1

**The problem.** The closest comparable, `https://rnwy.com/methodology`, publishes versioned
formulas with computation timestamps, sample-size gates and per-result breakdowns. We publish a
"What we cannot see" panel and nothing about our arithmetic.

**Build.**

1. A methodology section **inside the same self-contained page** (no second file, no external
   link required to understand a number). For every displayed metric give: exact numerator, exact
   denominator, the time window, the endpoint the inputs came from, the deduplication key,
   missing-data treatment, whether it is a census or a sample, and whether it is a lower bound.
2. Stamp collector version, methodology version and computation timestamp, and show them on the
   page. Versions are meaningless without archived definitions, so the definition text must live
   in the page beside the version it describes.
3. Fold the existing "What we cannot see" content into methodology, keeping every limitation
   beside the metric it affects rather than in a separate panel.
4. Serve the derived dataset for download and link it from the methodology section. `data.json`
   is already published at `/data.json`; document its schema and state plainly that it is an
   observation ledger, not raw message content — the source rotates history away, so raw bodies
   could not reproduce these metrics anyway.

**Acceptance.** Every number on the page has a stated formula, denominator and window reachable
without leaving the page.

---

## PHASE 4 — Remove the false-precision projection

Delete the projected time-to-cap range from the capacity panel. Keep current utilisation, the
observed net-change rate with its window and sample count, and the cap-change markers. A
projected exhaustion date implies a stable generative process; the operator has already raised
the note cap twice this week, and short windows are serially correlated. Utilisation and rate are
facts; the date is a forecast we cannot stand behind.

---

## Tests — extend `test_derive.py`

Cover, without touching the network:

- server-rendered values match the embedded JSON for the newest observation
- a page with zero observations still renders the honest empty state
- funnel stages remain monotonically non-increasing after the collapse
- the first funnel transition is expressed against the census denominator and flagged as a
  lower bound
- sampling frame size, cumulative unique rooms and failed reads survive a derive round-trip
- a tick missing the manifest (older schema) is still accepted — **backward compatibility with
  the existing 100+ ticks is mandatory; losing collected data is unacceptable and unrecoverable**
- the capacity panel exposes no projected exhaustion field after Phase 4

---

## Do not

- Do not add panels beyond those named here.
- Do not add a confidence interval. Selection probabilities are not yet known, and an interval
  over a convenience sample would be worse than none.
- Do not add a second network. That decision is made: not until this instrument is auditable.
- Do not rewrite working code for style. Every changed line should trace to a phase above.
