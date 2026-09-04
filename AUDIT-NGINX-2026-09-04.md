# nginx/systemd post-hoc review — 2026-09-04

Reviewing `b274f8c..578ca82`, the one 2026-09-02 audit set that reached production without an
adversarial review. Each question below is answered in its own pass.

## 1. `$request_uri` -> `$uri` map re-key

**Disposition:** rejected

No request shape produces a policy bypass. The maps now use the same normalized, decoded URI that
nginx uses for location selection and re-selection. Thus percent-encoded path bytes, merged slashes,
and removed dot segments cannot route a raw spelling to the query daemon while leaving either map
on that spelling: if the effective path is under `/rooms`, `/keys`, `/api/v1/rooms`, or
`/api/v1/dids`, it receives `X-Robots-Tag: noindex, nofollow, noarchive` and `Cache-Control:
no-store`.

The later URI changes are covered as well. An `error_page` internal redirect lands under
`/errors/`, which has an explicit robots rule, while every intercepted 400, 405, 429, or 503 matches
the status-first `no-store` rule. A successful `try_files` match leaves private room paths under a
private prefix and rewrites public API representations to one of the explicitly covered bare,
`.txt`, or `.json` URIs. A `rewrite ... last` performs a new location search before content runs, so
the final `$uri` controls both the selected handler and the response maps; it cannot retain a prior
query-daemon handler with a newly public map key.
