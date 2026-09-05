# Homepage redesign review — 2026-09-05

Review scope: `main..feat/home-motion`, limited to the 12 changed files and the
specific vhost/payload definitions needed to verify the review questions.

## Live defects

Two defects in the reviewed production branch were confirmed. Both are fixed
locally in `f8ec729`, but remain **LIVE** in production until that commit goes
through the normal deployment process:

1. Deferred narrative animations could move content that had already painted.
2. The endpoint-attempt count looked like a publication-relative trailing
   window even when its stored telemetry window was old.

## 1. Supply chain

**Disposition: rejected.** The vendored script is independently checkable; this
is not an unidentified binary blob.

- The file is exactly 8,310 bytes and its SHA-256 is
  `2b9b37fd2b8ebfac996f4d9a94f14360dc7bf9140d7a87cf79ff112af2307932`,
  matching the bundle checksum in `site/assets/vendor/README.md`.
- The README distinguishes that output checksum from the npm-package SHA-512
  integrities and gives a complete entry file plus an exact `esbuild@0.28.1`
  command. All packages named by the recipe are version-pinned. A reader can
  verify the downloaded tarballs, rebuild, and compare the resulting bundle
  SHA-256.
- Under the no-network rule, an end-to-end rebuild could not be run because the
  local npm cache lacked `motion-dom`, `motion-utils`, and `esbuild`. This audit
  therefore does not claim that it personally reproduced the bytes; the
  repository nevertheless contains the exact version pins, package
  integrities, output checksum, entry source, and recipe needed for an
  independent reproduction.
- The cached `motion@13.1.1` tarball matched the README's published SHA-512. Its
  package metadata declares MIT, and its `LICENSE.md` text appears exactly in
  `MOTION-LICENSE.txt`. The separately cached `framer-motion@13.1.1` tarball and
  its MIT licence text also matched the README and the second licence block.

## 2. CSP and script surface

**Disposition: rejected.** The current vhost policy is sufficient for this
change, and the redesign does not enlarge the network or code-generation
surface beyond it.

The vhost declares `script-src 'self' 'unsafe-inline'`. Both new scripts are
same-origin classic scripts. A direct scan of `home-motion.js` and the complete
vendored bundle found no `eval`, `new Function`, dynamic import, `fetch`, XHR,
WebSocket, EventSource, `importScripts`, or remote URL. The two touched templates
contain no inline event-handler attributes. The bundle operates through DOM,
Promise, computed-style, and Web Animations APIs only.

## 3. No-JS and reduced-motion honesty

**Disposition: confirmed — LIVE.** The values and controls are honest without
JavaScript, and reduced motion is respected, but the original entrance motion
could visibly reposition already-rendered prose.

The home page server-renders its evidence values and native GET search form. It
does not emit a numeric em-dash placeholder or a chart mark at an origin; the
decorative frame is transparent until scripted. With JavaScript disabled, the
narrative, form, evidence card, source times, validity, and coverage remain
visible and usable. Initial `prefers-reduced-motion: reduce` suppresses motion,
and a runtime change disconnects the observer and completes active animation.

The defect was the deferred script's explicit starting keyframes:
`opacity: 0.92` and `translateY(12px)`. CSS initially rendered those elements
at opacity 1 and identity transform, so a delayed script could move content
after first paint. A browser regression test reproduced two animations on an
already-painted narrative element. Commit `f8ec729` removes content entrance
animation and confines motion to the decorative frame trace; the late-load test
now proves narrative geometry stays unchanged.

## 4. Evidence boundaries

**Disposition: confirmed — LIVE.** The endpoint preview overstated the temporal
currency of its payload.

`coverage.telemetry.attempts` counts locally stored normalized attempts from
`coverage.telemetry.from` through `coverage.telemetry.to`. That interval is
anchored to the latest stored attempt, not to `published_at`. The original card
discarded both timestamps and rendered only “Trailing window: 60 minutes” under
a publication snapshot heading. If telemetry collection was stale, an old
one-hour interval therefore looked like the hour preceding publication. The
card also said source times define each reading while omitting this reading's
source bounds.

Commit `f8ec729` renders the exact counted `from`/`to` interval and its duration,
or `Not observed` when no telemetry window exists. A regression test compares
the rendered endpoint row with the status payload. No additional claim of
liveness, completeness, availability, cause, or participant verification was
found: the remaining copy explicitly limits search to retained observations,
calls labels untrusted, says “Not observed is not absent,” and disclaims
availability and certification.

## 5. `guards.py`

**Disposition: rejected.** No guard was relaxed or bypassed for the redesign.

The static release allowlist gained exactly the three shipped assets. More
importantly, script inspection was strengthened: instead of checking only
`assets/site.js`, the guard now recursively reads every JavaScript file under
`assets`, including `home-motion.js` and the vendor bundle, before applying the
existing external-origin and unsafe-DOM-sink checks. The corresponding deploy
test now injects an unsafe sink into the vendor file, demonstrating that the
new route is covered.

## 6. Added tests

**Disposition: rejected.** The added coverage is not merely template-shape
grep. Some assertions intentionally pin generated filenames and markup order,
but the important paths exercise browser behaviour.

The tests run a built release through a local HTTP server and Playwright. They
verify native encoded GET submission with JavaScript both off and on, prevent
non-local requests, hit-test the above-fold search action at desktop and mobile
sizes, verify the cache-busted stylesheet response, inspect actual Web
Animations targets/keyframes/durations, test missing-vendor and no-JS fallback,
and exercise both initial and runtime reduced-motion changes. The original set
did miss the late-script first-paint transition; the regression added in
`f8ec729` closes that specific behavioural gap.
