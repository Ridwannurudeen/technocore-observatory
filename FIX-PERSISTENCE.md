# Fix: the funnel's persistence stage measures the wrong thing

A stage on the live page overstates itself. It is the most credibility-sensitive number we
publish, and honest measurement is the only thing distinguishing this project from the
competition, so this takes priority over any new feature.

Your sandbox cannot read files or reach the network. You author TEXT; the operator deploys and
verifies against the live service.

**OUTPUT FORMAT** — markers, NO code fences. Emit complete files only:

    === FILE: relative/path ===
    (entire file contents verbatim)
    === END FILE ===

Do NOT put your self-audit inside any artifact. Report it as prose in your reply.

---

## The defect, measured

Every one of our 178 collected ticks falls on a single UTC date: `2026-08-28`, from 11:15:36Z to
17:14:24Z. Collection has never crossed midnight.

Yet the funnel reports **8,785 DIDs "observed on ≥2 UTC dates"**, and the signer state contains
records like:

    utc_dates: ['2026-08-27', '2026-08-28']   utc_dates_count: 2   tick_count: 3

This is **not fabricated data** — I checked. In `update_signer_state` the date comes from each
*message's own* timestamp:

    observation["utc_dates"].add(message["_datetime"].date().isoformat())

A room ring holds up to 200 messages, and in a low-traffic room those can reach back before our
collection began. So the dates are genuine; `2026-08-27` is simply a message timestamp that
predates our first tick.

**The label is what is wrong.** The page renders this stage as *"Observed on ≥2 UTC dates"*,
inside a funnel explicitly framed as our forward-collected observations. A reader takes that to
mean *we saw this key active on two different days*. What it actually means is *the messages we
captured carry two different dates* — which a single read of one room whose ring crosses
midnight satisfies instantly, with no persistence whatsoever.

That stage exists to demonstrate persistence. As built it can be satisfied without any.

## What to change

**1. Measure persistence by COLLECTION date, not message date.**

Track, per DID, the set of distinct UTC dates *on which we observed it* — derive that from the
tick timestamp, not from `message["ts"]`. The predicate becomes "we saw this key on ≥2 separate
days of collection", which is what the funnel claims and what "sustained" means for a
forward-collected instrument.

Keep the existing storage discipline: the current code caps the stored list with
`sorted(dates)[:2]` and sets the count from `len()`. A bounded list is fine — but if you keep a
cap, the field must not be presented as a total. Prefer storing the first and last observation
date plus a distinct-day count that is not truncated, so the number stays meaningful.

**2. Do not silently discard the message-date spread — either drop it or name it honestly.**

If you keep it, it is a different and weaker property. Give it its own name that says what it
is (message timestamps span more than one date) and do not let it gate the persistence stage.
If it earns no place on the page, remove it rather than leaving a misleading field in the data.

**3. Accept that the honest number is smaller.**

Measured correctly, this stage reads **0** until collection crosses midnight UTC, and the two
stages beneath it inherit that. The funnel gets shorter and less impressive. That is the correct
outcome and it must not be softened, back-filled, seeded or worked around. Render the honest
zero with an explicit explanation that collection has not yet crossed a UTC day boundary — the
same discipline the rest of the page already applies to unmeasured things.

**4. Backward compatibility with existing records.**

`signers.json` currently holds ~61,000 DID records whose `utc_dates` are message-derived and
cannot be re-derived — we cannot recover which of our ticks observed them, and the source
rotates history away. Do not attempt to reinterpret them as collection dates; that would invent
data. Either migrate them to a clearly-marked legacy field or reset the persistence counters and
say so on the page. Whichever you choose, `ticks.jsonl` must keep parsing — **all 178 collected
ticks must still be accepted by `derive.py`, and their loss is unrecoverable.**

## Also fix, same root cause, currently latent

`first_seen_ts` and `last_seen_ts` are likewise taken from message timestamps
(`update_signer_state`, the `observed_this_tick.setdefault(...)` block). Neither reaches
`derive.py` or the page today, so nothing published is wrong — but the same mislabel is waiting
if either is ever surfaced. Either switch them to observation time or rename them to say they
are message-derived.

## Verified sound — do NOT change these

I re-checked the other three stages adversarially:

- `tick_count` is incremented once per tick in which the DID was observed. Genuinely ours.
- `rooms` is populated from the names of rooms **we sampled**, so "≥2 rooms" is a real
  observation. The `[:8]` cap is above the `≥2` predicate and harmless.
- The counterparty rule pairs two *distinct* signed DIDs within 10 messages and 900 seconds in
  the same room, computed from the fetched window. Sound. Note `counterparties_count` is only
  ever set to `1`, so it is a boolean in a counter's clothing — the displayed predicate
  ("≥1 distinct signed counterparty") is still accurate, but rename the field if you touch it.

## Constraints — all still binding

- One self-contained HTML file. Zero external requests. No `innerHTML` on service-derived
  strings. Never call DID notes "agents" or "users".
- Keep every `data-ssr` key and every element id referenced by `byId`/`getElementById`; adding
  is fine, losing one is not. Keep the exact tag
  `<script id="observatory-data" type="application/json">`.
- Globals `data`, `points`, `current`; entry `update(index)`; helpers `byId`, `formatInt`,
  `formatRate`, `formatPercent`, `formatTime`, `setRate`. **`DATA` does not exist** —
  referencing it throws inside `update()` and silently freezes the whole page.
- `.density` and `.density-fill` must keep `display:block`. They were `<span>`s without it and
  every funnel bar rendered at zero width.
- Funnel stages must stay monotonically non-increasing — `derive.py` validates this and rejects
  the tick otherwise. Compute them as cumulative filters over the previous stage's survivors.
- No percentage without a denominator, no rate without its sample count and window, missing is
  never zero.
- Python 3.12, stdlib only (plus pytest). No new dependencies. No Claude/Anthropic/Codex
  attribution anywhere.

## Deliverables

`collect.py`, `derive.py`, `index.html` and `test_derive.py`, complete.

Extend the tests to cover: a single-collection-date corpus yields **0** at the persistence stage;
two collection dates yield the expected count; a legacy record with message-derived dates does
not inflate it; stage monotonicity still holds; and old ticks are still accepted.

## Before you finish

The operator will run `guards.py`, which renders the page in real Chrome and fails any element
that asked for a width and got zero, and separately checks every `data.x` / `point.x.y` path the
page reads against what `derive.py` actually emits. If you remove a field from the deriver,
remove its reader from the page in the same pass — that exact producer/consumer drift has
already shipped once.
