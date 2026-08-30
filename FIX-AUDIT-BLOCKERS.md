# Fix: three audit blockers, all live on the public page

An adversarial audit found three claims the site makes that the data does not support. I
verified every one against the deployed source and the live payload before writing this. The
project's only differentiator is measurement honesty, so these outrank every other work item and
the site should not be shown to its subject until they are fixed.

Your sandbox cannot read files, reach the network, or run anything. That is expected and is not
a reason to decline: the operator executes pytest, `guards.py` and the deploy, and reports
failures back. Everything you need is inlined.

**OUTPUT FORMAT** — markers, NO code fences:

    === FILE: relative/path ===
    (entire file contents verbatim)
    === END FILE ===

Do NOT put your self-audit inside any artifact — report it as prose in your reply.

**SCOPE LIMIT: at most two large files per run.** A previous run asked for four and emitted
nothing. See "Run split" at the end; do not exceed it.

---

## B2 — the funnel cites a stale census and calls it "last completed" (fix this first)

**Verified on the live payload.** The funnel renders census `556,973` completed `13:56:55Z`,
while a newer measured census of `608,600` at `16:28:46Z` sits in the same payload. The
methodology promises the funnel uses "the latest completed DID-note census". It does not — it is
2.5 hours and 51,627 notes stale, and stays stale until a later census happens to win a race.

**Cause, confirmed in source.** Both the `--census` cron invocation and the collector daemon do
an unlocked read-modify-write on `signers.json`: `load_signer_state` (`collect.py:609`, called at
`:895`) then `save_atomic_json` (`:527`, called at `:932`), with roughly twenty network reads in
between. `save_atomic_json` is atomic per write; the *cycle* is not. The daemon loaded pre-census
state and clobbered the census result. The same collision is why one tick was rejected — two
ticks carry `2026-08-28T16:28:46Z` and `read_jsonl` requires strictly increasing timestamps.

**Fix — make it single-writer.** Take an exclusive OS lock around the whole load→modify→save
cycle for `signers.json` (and for the census state). On Windows-free deployment this is
`fcntl.flock` on a sibling lock file; fail closed and skip the tick if the lock cannot be taken
within a short timeout, rather than proceeding unlocked. Running the census inside the daemon
loop on a timer is an acceptable alternative; pick one and say which.

**Fix — add a derive-time honesty invariant.** If any accepted point carries an `identity_total`
census newer than the funnel's `census_completed_at`, the page must not print "last completed
census". Either use the newer census or label the shown one explicitly as superseded. Never
present a stale value under a "latest" promise.

---

## B1 — the headline funnel line asserts a subset that is never established

The page renders: *"at least 71,901 **of 556,973 well-formed DID notes** observed signing ·
lower bound"*. Two independent overstatements.

**(a) The two populations are not nested, in either direction.** The numerator counts distinct
did:key-shaped `from` strings observed in sampled room windows. The denominator counts
`/kv/did-XX` note **keys**; the census stores per-shard integer counts and never the key set, so
membership is uncheckable *in principle*, not merely unchecked. And publishing a DID note is not
required in order to sign — technocore's own `auth.md` says publishing a key is "Convention, not
a server feature". So a signer with no published note is in the numerator and absent from the
denominator. "N of M DID notes observed signing" is therefore not a valid lower bound of census
members observed signing; it is a comparison of two different populations.

**(b) "Observed signing" does not check the signature marker.** Verified: `nonce` appears **zero
times** in `collect.py`. `parse_room_messages` (`:407`) keeps only
`{seq, ts, _datetime, from, position}`, and a signer is admitted solely by
`SIGNED_DID_RE.fullmatch(sender)` at `:780`, where `SIGNED_DID_RE` (`:43`) matches the *shape* of
a did:key string. The service never verifies `from` on the unsigned lane, so an unsigned write
asserting `from: did:key:z6Mk…` is counted as a signer. This project's own `RESEARCH.md:141`
requires DID-shaped `from` **plus `nonce`**. The audit found all 1,799 current signers do carry
nonces, so nothing is being spoofed today — the exposure is latent, and the subject of this site
could demonstrate it with a single request.

**Fix — gate on the nonce.** Retain `nonce` in `parse_room_messages` and require its presence in
`update_signer_state` before a sender counts as a signer. Anything without a nonce is not a
signed observation.

**Fix — say what is actually measured.** Stop claiming a subset relationship. State the observed
signer count and the census as two separate measurements, and say plainly that neither population
contains the other: a signer need not have published a note, and a published note need not have
been observed signing. Keep the "lower bound" idea only where it is true — the observed signer
count is a lower bound on *signers*, because sampling covers a fraction of rooms.

**The suite will fight this.** `test_first_funnel_transition_is_a_lower_bound_against_census`
(`test_derive.py:532`) asserts the current wording verbatim, so it enshrines the overstated
claim. Rewrite that test to assert the corrected claim; do not preserve the old string.

---

## B3 — the no-JS view contradicts the JS view on three honesty surfaces

**Verified in the live static HTML and in `ssr_values`.**

- **Stage 4 has no SSR anchor at all.** The emitted funnel SSR keys are `funnel-census`,
  `funnel-census-context`, `funnel-observed`, `funnel-observed-context`, `funnel-two-ticks`,
  `funnel-two-ticks-context`, `funnel-sustained`, `funnel-sustained-context`, `funnel-coverage`,
  `funnel-warning`. There is no `funnel-two-dates`. So a crawler sees row 04 as
  `— Collection-date observation unavailable` while the measured value is **0**. A measured zero
  rendered as a dash is the precise inverse of this project's stated rule, and it will remain a
  dash forever once the value becomes non-zero. Add the anchor and the key.
- **SSR and JS use different denominators for stage 5.** SSR renders
  `0.0% of 40,595 observed on ≥2 ticks` (the stage-3 cohort) while the JS renders against the
  stage-4 cohort. Two different claims about one number depending on whether scripts ran — the
  same producer/consumer drift class already fixed once for `projection_seconds`.
- **SSR omits disclosures the JS adds.** The JS appends the persistence-reset sentence and the
  `cap_hit` warning; `ssr_values` hardcodes only the sampling-bias sentence. A crawler never
  learns the persistence counters were reset, and if the tracked-DID cap is ever hit the no-JS
  view keeps presenting stage 2 as if uncapped.

**Fix — one source of truth.** Build the stage values, the conversion contexts and the warning
text from a single function used by both the SSR pass and the page JS, so they cannot diverge
again. Add the missing stage-4 anchor and key.

---

## Also fix (defects, not honesty claims)

1. **No-JS bars render at zero width.** In the static HTML only funnel row 01 carries an inline
   width; rows 02–05 and the capacity fills have none, and the CSS default is `width:0`. The SSR
   pass fixed text but not bars. Emit the widths server-side too.
2. **`frame_size` counts lobby as a newest-listing room.** It is computed as
   `len(selector_frame) + 1` (`collect.py:742`) because lobby is always sampled, but the page
   says "The current selector frame contains 50 **newest-listing rooms**". It contains 49 plus
   lobby. Either exclude lobby from the figure or say "49 newest-listing rooms plus lobby".
3. **Empty-payload path claims a collector version with zero ticks** (`derive.py:627` emits
   `collector_version: "2.1.0"` when nothing was ever collected). Emit null.
4. **The composition band is invisible to screen readers** — plain divs, no role, label or text
   alternative, unlike the hero SVG which has `role="img"` with title and desc. Give it an
   equivalent.
5. **`aria-valuenow` is an unrounded float** (e.g. `72.34714…`). Round it.

---

## Constraints — all still binding

- One self-contained HTML file. Zero external requests. No `innerHTML` on service-derived
  strings — room names, topics, nicks and DIDs are attacker-controlled.
- Keep every existing `data-ssr` key and every element id referenced by
  `byId`/`getElementById`; adding is fine, losing one is not. Keep the exact tag
  `<script id="observatory-data" type="application/json">` — `derive.py` rewrites its body by
  regex.
- Globals `data`, `points`, `current`; entry `update(index)`; helpers `byId`, `formatInt`,
  `formatRate`, `formatPercent`, `formatTime`, `setRate`. **`DATA` does not exist** —
  referencing it throws inside `update()` and silently freezes the whole page.
- `.density` and `.density-fill` must keep `display:block`. They were `<span>`s without it and
  every funnel bar rendered at zero width while the JS widths were correct.
- Funnel stages must remain monotonically non-increasing — the deriver rejects the tick
  otherwise. Compute them as cumulative filters over the previous stage's survivors.
- **Backward compatibility is mandatory.** 204+ collected ticks span two collector schemas and
  are unrecoverable; `derive.py` must keep accepting the old ones. Fields absent on legacy ticks
  render an explicit "not recorded" state, never 0 and never a dash implying a measurement.
- No percentage without its denominator, no rate without its sample count and window. Never call
  DID notes "agents" or "users".
- Python 3.12, stdlib only plus pytest. No new dependencies. No Claude/Anthropic/Codex
  attribution anywhere.

## Tests

The suite has **zero tests for `collect.py`** — every one of these blockers lives in untested
code. Add them. Required coverage:

- a sender with a did:key-shaped `from` and **no nonce** is NOT counted as a signer
- a sender with both is counted
- the funnel's census denominator is never older than the newest census in the payload
- SSR and JS produce the same stage-5 denominator and the same warning text
- a measured 0 at stage 4 renders as `0` in the SSR output, not `—`
- `cap_hit=True` surfaces the cap warning in the SSR output
- legacy ticks with no manifest and no collection-date fields are still accepted
- a value containing `</script>` or `<img onerror=…>` survives `inject_html` escaped

## Guard extension

`guards.py` renders the page in Chrome **after** running `update()`, so the no-JS state is
unguarded — which is exactly how B3 shipped past the guard written for that class of bug. Add a
third guard that loads the page with JavaScript disabled and asserts: no element that should
carry a measured value renders `—` while the payload holds a number, and no bar with a payload
width renders 0px.

## Run split

- **Run 1:** `collect.py` + `derive.py` (B2 locking, B1a/B1b, the derive-side invariant and SSR
  keys). End your reply with an explicit CONTRACT section listing every field added, removed,
  renamed, or changed in meaning, and exactly what the page must stop and start reading.
- **Run 2:** `index.html` + `test_derive.py`, commissioned with your contract in hand.
- **Run 3:** `guards.py` alone.
