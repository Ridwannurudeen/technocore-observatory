# Fix list — ranked by leverage, not by ease

All claims below verified live on 2026-08-29.

## F1 — Give the instrument a public identity  [gates the only real wedge]
The live page links **no repository, no licence, no contact** (verified: zero matches for
github.com / licence / mailto). An "independent observatory" that is anonymous and unlicensed
fails its own philosophy exactly where its value lives. Without this, the one time-boxed wedge —
being the pre-snapshot independent record before the Q4 airdrop — reduces to "an anonymous page
said so". Cheapest item on the list; blocks the most.

## F2 — Widen the sampling frame 50 -> 200  [DONE, deployed 2026-08-30]
`/rooms?format=json` was requested with no limit and got the service default of 50. Verified:
`limit=200` returns 200 rooms; `limit=500` errors. This was described as an environment
constraint; it was a build choice. Shipped with `limit=200` and a per-tick room read budget of
80 — ~41 requests/minute against the published 600/min.

Widening the frame alone would have frozen the page. `validate_room_sampling` rejected any
manifest with more than 20 sampled entries, so every new tick would have been dropped while the
collector still looked healthy. The bound now follows the budget the collector records, with a
structural ceiling of 200 for legacy ticks that carry none.

The methodology also hardcoded "Up to 20 room reads are attempted per tick". `read_budget` is now
recorded in the sampling manifest and the figure is rendered from it; ticks predating the field
report it as not recorded rather than inheriting a number nobody measured.

Measured after deploy: cadence slowed from ~110s to ~258s per tick (80 reads instead of 20), which
is still well inside the 600s stall threshold but at ~2.3x margin rather than ~8x. Points grew
from 6,677 to 10,326 bytes, but the slower cadence more than offsets it — daily payload growth
*fell* from ~5.0MB/day to ~3.3MB/day.

## F3 — Ship the engagement metrics already being collected
`engagement` appears twice in `collect.py` and **zero times in `derive.py`** — collected every
tick, discarded on ingest. A real tick carries `windowed_note_to_message_ratio: 120.2167`,
`nick_diversity: 0.3323`, `zero_response_share: 0.2289`.
**120 durable registration notes per observed chat message** says the network is a registration
book, not a conversation. That single number is more citeable than the entire funnel and costs
nothing to publish.

## F4 — Stop the funnel's visual asserting containment
Stage 1 counts DID-note keys; stage 2 counts senders seen in sampled rooms. Neither population
contains the other — the fine print says so — but every density bar is scaled against the census
width, so the picture claims containment the data does not support. Either scale stages 2-5
against stage 2, or separate the census visually from the observed funnel.

## F5a — SQLite DID store, cap released  [DONE, deployed 2026-08-30]
`signers.json` was a 59MB JSON object read, mutated and rewritten in full every tick. It caused an
OOM crash-loop (~7h of dead collection) and the 200,000-DID cap existed only to bound it. That cap
saturated **2026-08-29T18:00:15Z**, after which stage 1 reported the cap rather than an observation
and no new DID could enter the funnel at all.

Observed DIDs now live in SQLite, updated per tick; the five funnel stages are SQL aggregates. The
metadata still in JSON (selector state, persistence, census, retired cap fields) is **4.0KB**, down
from 59MB. Migration of the live store reproduced all five funnel counts exactly, at a peak of 53MB
against the old collector's 445MB. Live RSS is now ~25MB of the 512MB limit.

Cap released **2026-08-30T09:49:03Z**. Stage 1 was pinned at exactly 200,000 for ~16 hours and is
measuring again (202,610 at deploy). The saturation window is recorded in the state and published
as a disclosure derived from those timestamps — DIDs first appearing in the window were never
recorded and the undercount is permanent.

`derive.py` refused the new ticks (`tracked DID count exceeds its cap`) because it asserted the
tracked count never exceeds the cap. True while the cap gates insertion, wrong once retired: it now
applies only to ticks that do not declare the cap retired, and a legacy over-cap tick is still
refused. Rollback and the pre-migration store are at `/root/f5a-deploy-20260830-094808/`.

**A collector change has now been rejected by an unrevised `derive.py` validator three times** —
the 20-entry sampling bound, the read budget, and this. Each was caught by hand, never by the
suite. `test_collector_assembled_current_and_legacy_ticks_validate` now asserts that what the
collector emits is what the deriver accepts.

Not covered: there is no test for the census round-trip through v3 state. The write path is
unchanged and the live metadata carries the census correctly, but the `23 */6` cron is the first
real exercise of it.

## F5b — The death clock (payload)  [OPEN]
The page embeds the entire tick history: **6.5MB raw** across ~1,034 points, ~854KB gzipped on the
wire, growing ~3.3MB/day. gzip is already on, so the binding cost is JSON parse and memory on
mobile, not bandwidth.

Measured composition: **46% of the payload is rendered prose** that only the newest point needs —
`signer_funnel.display` 1.69MB, `engagement_display` 1.11MB, `rate_display` 0.23MB. Interning is
not the answer (only 1.7x; the strings embed each point's own numbers).

Dropping prose for non-newest points is a one-time 46% cut that delays rather than solves; growth
continues. Bounded retention is what actually fixes it: keep raw points for a declared recent
window, aggregate older history into the multiresolution buckets specified in RESEARCH.md §5.

**Design decision to carry into the brief:** make the scrubber's window equal the raw-retention
window, and let older history feed charts only as aggregate buckets. Then no display prose is ever
recomputed in JavaScript, and the derive.py-vs-JS duplication trap — the divergence class that has
already shipped twice here — never opens.

## F6 — Tamper-evidence
The tick ledger has no hash chain and no external timestamp anchor, so the operator is free to
silently rewrite history. Required for the pre-snapshot record to mean anything to a third party.

## F7 — Reciprocity predicate is co-occurrence
`collect.py:838-852`: two distinct signed DIDs within 10 message positions and 900s in the same
room. At lobby's ~10.8 msg/s that is about one second of traffic. RESEARCH.md:160 specified
*alternation*; the implementation dropped it. Either require alternation/directedness or rename
the stage to what it measures.

## F8 — Forward room-name ledger  [largest measurement gain]
`/r/events` names essentially every room created since collection began (~22/min against a
200-event window per tick). Rooms are addressable by name even though the listing cannot reach
them. Revisiting them at 5m/1h/24h converts a 50-room keyhole into a rolling ~1-day panel of
~30k rooms, inside ~11% of the verified read budget. RESEARCH.md MUST #5 already prescribes it.

## Cannot be fixed at all (record, do not attempt)
- Room reads return `seq, ts, from, text, nonce` — **no `sig`**. Signatures cannot be
  re-verified; the instrument trusts FLOP's assertion. Mirror, not auditor.
- The base rate ("what fraction of the 724k ever did anything") is outside the light cone of any
  collector started in August.
- FLOP's `/stats` holds strictly better data than we can ever collect.
