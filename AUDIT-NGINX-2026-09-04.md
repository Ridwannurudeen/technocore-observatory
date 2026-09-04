# nginx/systemd post-hoc review — 2026-09-04

Reviewed `b274f8c..578ca82` against the supplied diff and the deployed nginx/systemd configuration.

## 1. `$uri` re-keying and header coverage

**Disposition: rejected.** No request shape that reaches the query daemon escapes the headers required for that route. nginx location matching and both maps use the same normalized, current `$uri`: percent-decoding, slash merging, and dot-segment removal therefore cannot make a private query location match while leaving the maps keyed on a different spelling. A normalized path that no longer matches a proxy location does not reach the daemon.

The daemon-bound incidents/changes routes are always `no-store` when they proxy; their valid static forms take `rewrite ... last` and do not reach the daemon. The rooms/keys/dids proxy routes match the explicit robot and `no-store` rules for either value of the static-request flag. A successful `try_files` or `rewrite ... last` updates the current URI to the selected static target, so the static/private policy follows that target. API error-page redirects move to `/errors/*`, which has its own robot rule, while the preserved 400/405/429/503 status selects `no-store`. The named human fallbacks are covered both as their original rooms/keys URI and as the selected `/errors/*` file. Incidents and changes intentionally have no `X-Robots-Tag`; that is the existing public route policy, not a bypass of the room/DID policy.

## 2. Five-second proxy timeout and 504 handling

**Disposition: rejected.** Five seconds is a reasonable outer failure ceiling around the daemon's 0.5-second query budget. It leaves headroom for request dispatch, scheduling, response construction, and local I/O while reducing nginx's otherwise much longer wait for a wedged upstream. It is a watchdog, not a second query budget.

Every proxy location enables `proxy_intercept_errors` and maps 502/503/504 to a response with an explicit final status of 503. API routes select the bounded text/JSON `query-unavailable` artifact; the two human route families select the bounded HTML artifact through `@html_unavailable`. The final 503 also selects `Cache-Control: no-store`, so a read timeout does not expose nginx's default 504 body.

## 3. Human-page 429 artifact versus API 429 artifacts

**Disposition: rejected.** Only `/rooms/` queries and the human room/key detail regex use `@html_rate_limited`. The API proxy locations retain `error_page 429 =429 /errors/api-rate-limited$observatory_error_suffix`, so `format=json` selects JSON and every other argument shape selects the bounded text artifact.

The HTML fallback preserves status 429, which selects `Cache-Control: no-store`. It is non-indexable through the `X-Robots-Tag` map (both the originating private route and `/errors/` are covered), and the artifact itself also contains `meta name="robots" content="noindex,nofollow,noarchive"`.

## 4. systemd IP allow/deny policy

**Disposition: rejected.** `IPAddressDeny=any` plus `IPAddressAllow=localhost` is a loopback allow-list. The unit explicitly binds `127.0.0.1:8765`, nginx proxies to that same address, and `AF_INET`/`AF_INET6` remain permitted address families. File and `AF_UNIX` access are unaffected. The policy would intentionally block a future non-loopback network dependency, but none is part of this unit's current query-service contract.

## 5. Removal of the `derive.py:F841` Ruff waiver

**Disposition: rejected.** Removing a per-file ignore makes F841 visible; it cannot conceal an F841 finding. The supplied change set does not alter `derive.py`, and all documented/CI lint commands now run `ruff check .` without the exception. A real unused-local finding would therefore fail the lint command. The removed waiver was dead, not masking a live finding.
