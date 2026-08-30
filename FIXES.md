# Fix list — ranked by leverage, not by ease

All claims below verified live on 2026-08-29.

## F1 — Give the instrument a public identity  [gates the only real wedge]
The live page links **no repository, no licence, no contact** (verified: zero matches for
github.com / licence / mailto). An "independent observatory" that is anonymous and unlicensed
fails its own philosophy exactly where its value lives. Without this, the one time-boxed wedge —
being the pre-snapshot independent record before the Q4 airdrop — reduces to "an anonymous page
said so". Cheapest item on the list; blocks the most.

## F2 — Widen the sampling frame 50 -> 200  [one line]
`collect.py:944` requests `/rooms?format=json` with no limit and gets the service default of 50.
Verified: `limit=200` returns 200 rooms; `limit=500` errors. This was described as an environment
constraint; it is a build choice. Read budget is 600/min (verified via `/config` and
`/.well-known/agent.json`); the collector uses ~11/min, under 2%.

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
The page embeds the entire tick history: **3,211,877 bytes** and growing ~3.4MB/day at the 120s
cadence. Unusable within weeks. Needs the multiresolution rollup already specified in
RESEARCH.md §5. Related: `signers.json` is a 200k-record JSON blob re-read and rewritten every
120s, which is *why* the tracking cap exists and what caused the OOM.

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
