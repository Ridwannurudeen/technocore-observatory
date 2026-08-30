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

## F5 — The death clock
The page embeds the entire tick history: **6.42MB raw** across 1,004 points as of 2026-08-30,
growing ~3.3MB/day. gzip is already enabled, so this is ~804KB over the wire — the binding cost is
JSON parse and memory on mobile, not bandwidth. Needs the multiresolution rollup already specified
in RESEARCH.md §5.

Related, and now the more urgent half: `signers.json` is a 62MB JSON blob re-read and rewritten
every tick, which is *why* the tracking cap exists and what caused the OOM. **That cap saturated
at 200,000 on 2026-08-29T18:00:15Z.** The funnel's top stage now reads exactly 200,000 — the cap,
not an observation — and no newly observed DID can enter the funnel while it holds. The page
discloses the cap in its warning text, so it is not dishonest, but the headline number is a
ceiling artifact and F2's wider frame cannot move it. Fixing the storage resolves the page weight
and the frozen funnel together.

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
