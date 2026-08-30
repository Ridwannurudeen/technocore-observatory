# What keeps this from being exceptional

Fable-5 ceiling analysis, 2026-08-29. Key claims independently verified by the operator; the
verified ones are marked. This is a ceiling analysis, not a bug list.

> **Superseded status, 2026-08-30:** The F5a SQLite/cap release moved signer state to v3 and
> retired the 200,000-DID insertion cap. The frozen-cohort paragraph below records the condition
> at the time of this analysis; it is no longer a present-tense description of collection.

## The hardest finding

**The funnel cannot answer the question its own visual poses.** Stage 1 counts published DID-note
keys; stage 2 counts did:key senders seen in sampled rooms. Neither population contains the
other — the page says so in fine print — but every density bar is scaled against the census
width, so the visual asserts containment the data does not support. Every visitor will read
"31,163 of 724,230 identities are real". That is a different claim than the one the numbers make.

Supporting weaknesses, all real:
- The cohort is FROZEN. The 200,000 tracking cap was hit in roughly the first day, so every
  downstream stage is a panel study of whoever was admitted in the first ~30 hours, drawn in the
  grammar of a live funnel. It drifts further from the network every day it runs.
- Conversions confound behaviour with observation pressure. Lobby is sampled every tick; other
  rooms rotate. "Seen twice" measures capture intensity, not persistence.
- The reciprocal predicate is nearly free: two distinct signed DIDs within 10 message positions
  and 900s. At lobby's ~10.8 msg/s, 10 positions is about one second. It is co-occurrence, not
  reciprocity. RESEARCH.md specified "alternate"; the implementation dropped it.
- "Sustained" currently means two days, the maximum achievable value of the statistic.
- Every rung is purchasable for ~zero, which RESEARCH.md:143 already conceded.

What the funnel DOES support: "at least N distinct keys were observed signing on two calendar
days, in two sampled rooms, near another signer, under a disclosed keyhole." True, defensible,
and not what the page will be read to say.

## VERIFIED: two large self-inflicted limits

1. **The 50-room frame is a BUILD CHOICE, not an API cap.** `collect.py:944` requests
   `/rooms?format=json` with no limit and receives the service default of 50. Verified live:
   `limit=200` returns **200 rooms** (`limit=500` errors). One line quadruples the frame.
   The operator previously described this as an environment constraint. It is not.
2. **The collector uses under 2% of its permitted read budget.** Verified live: `/config` and
   `/.well-known/agent.json` both report `rate_read` / `reads_per_minute_per_ip` = **600**.
   The collector spends ~11 reads/min. This is the single most consequential unused resource.

The forward room-name ledger follows from these: `/r/events` names essentially every room created
since collection began (~22/min against a 200-event window per tick), and rooms are addressable
by name even though the listing cannot reach them. That converts a 50-room keyhole into a rolling
~1-day panel of ~30k rooms, inside ~11% of the verified budget. RESEARCH.md MUST #5 already
prescribes it.

## Collected but discarded

`/rooms.engagement` has been in the tick ledger since collector 2.2.0 and `validate_tick` throws
it away. It contains `windowed_note_to_message_ratio` — measured 83-120 durable registration
notes per observed chat message. **That single number — the network is a registration book, not
a conversation — is more citeable than the entire funnel**, and it is already being collected.
Same for `zero_response_share`, `nick_diversity`, concentration (HHI/top-k) and template
clustering, all ranked MUST in RESEARCH.md and all unshipped.

## Structural — no effort fixes these

1. **The evidence lane is operator-attested, not cryptographic.** Room reads return
   `seq, ts, from, text, nonce` — **no `sig`**. The observatory cannot re-verify a single
   signature; it trusts FLOP's assertion that a message was signed. So it can characterise
   user-side behaviour conditional on FLOP's honesty, but can never detect operator-side
   fabrication — the adversary that actually matters in an airdrop dispute. Mirror, not auditor.
   This defines the project's maximum epistemic status.
2. **The base-rate question is permanently closed.** No backfill plus ring rotation means "what
   fraction of the 724k ever did anything" is outside the light cone of any collector started in
   August. That is the question the airdrop debate will ask.
3. **FLOP holds strictly better data** — `/stats` has lifetime counters and distinct client IPs.
   The subject of the measurement never needs the measurement.
4. **One collector, one VPS, one operator.** Coverage is already ~70% of intervals in week one.
   A forward-only instrument's only asset is continuity.

## The verdict

Research artifact and portfolio piece. Not a product: FLOP has better data, farmers want the
eligibility signal the site correctly refuses to give, researchers need provenance the site does
not offer, and the pure-play comp (Trusta, $3M raise) says this is a feature, not a company.
An honesty moat is not a moat — honesty is copyable, and RNWY already copied it, across 12
networks.

**The one real wedge is time-boxed and currently void:** being the independent, continuous,
pre-snapshot behavioural record of the Technocore registration farm, in place before the Q4 2026
airdrop. If a sybil controversy happens, the only forward ledger predating the snapshot has
genuine citation value — once, briefly. That requires three things it lacks: tamper-evidence
(hash-chained, externally anchored ticks), a public identity (the live page links no repo, no
licence, no contact), and continuity. Without them the claim reduces to "an anonymous page said
so", which for an independence claim is the same as nothing.

The durable asset is not the site. It is the demonstrated ability to reverse a live API to
primary-source standard, and the measurement doctrine. Those transfer to the next network.

---

## External anchoring is not achievable on this network (measured 2026-08-30)

The tick ledger is hash-chained (`0ce0142`), which makes silent edits detectable to anyone holding
an earlier copy. The page states the remaining limit honestly: *internal consistency only; no proof
of collection time without an external anchor.* Closing that gap was attempted and **abandoned on
evidence**. It cannot be done here.

An anchor needs a venue that is (a) durable and (b) not controlled by the operator. This network
offers neither together:

- **A public high-traffic room does not retain.** Room reads return the newest 200 messages. Lobby,
  measured from our own `lobby_last_seq` series, runs at **41.93 msg/s** — 292,014 messages in
  6,965 s. A 200-message window therefore covers **about 5 seconds** of history. An anchor posted to
  lobby is unreadable within seconds. (Note this rate is ~4x the ~10.8 msg/s recorded earlier in
  this document; the figure below is left as measured at the time.)
- **A room that does retain is one we own.** `d-gudman-agent` holds 13 messages spanning six days,
  precisely because it is low-traffic — and `checkin.sh` owns it, guards it, and keeps it alive.
  Anchoring there is the operator attesting to the operator. It would not survive an adversarial
  reading, and claiming otherwise would be worse than claiming nothing.
- **Two head hashes cannot prove ancestry while the ledger is private.** `ticks.jsonl` is not
  published and never will be — it embeds ~2,000 attacker-controlled room names, which this
  project's own doctrine forbids republishing. A stranger comparing an anchored head to a later
  published head can confirm equality, but cannot establish that a *different* later head legitimately
  descends from it. Only someone holding the ledger can.
- **Room reads carry no `sig`.** Re-reading an anchor message shows text and the service's timestamp,
  not a signature a third party can recheck. The service would be both the timestamp authority and
  the subject of measurement.

**Conclusion:** the hash chain stands on its own merits — it binds our own future claims and makes
tampering detectable to anyone we hand the ledger to. It does not, and on this network cannot,
prove when anything was collected. The page must keep saying exactly that.

An independent anchor (an external timestamping service, a public append-only log elsewhere) would
work, but it would require the observatory to write off-network, and the collector is deliberately
read-only polling. That is a product decision, not a technical impossibility.
