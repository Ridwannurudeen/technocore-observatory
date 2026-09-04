# Verified COMPASS demo

> **Verified 2026-08-31:** the read-side release is live, and a private one-curl/one-screenshot
> capture completed after independently confirming that the selected record was absent from the
> origin's complete newest-200 window. The raw response and screenshot remain untracked because
> they contain an attacker-chosen room name. Repeat the origin check immediately before any future
> capture; a prior absence observation is never a current absence claim.

## Verified capture record

- Stable evidence ID: `de84c8d8605ad323`.
- Origin check: 200 newest rooms returned; selected record absent at capture time.
- Observatory result: one exact, uncapped, fresh result labelled `untrusted` and `forward_only`.
- The full-page frame includes site identity, the populated query, evidence rail, lifecycle result,
  local/no-upstream boundary, and "Not observed is not absent" claim boundary.
- Private artifacts: `compass-response.json` and `compass-search.png`, retained outside this
  repository and never published as a room-name index.

## Preselect the evidence

Choose one non-sensitive room name already present in the local forward ledger and independently
confirm that it is absent from the origin's current newest-200 listing. Record the local ledger
observation timestamp, the origin check timestamp, and the resolved 16-hex evidence ID. Do not put
the attacker-chosen room name in this tracked file or in shell history; keep it in a temporary local
variable for the capture.

That preselection is setup, not part of the recorded demo. If the room reappears in the origin's
newest-200 window, select another record before capture.

## One-curl capture

With `ROOM_QUERY` already set in the operator's current shell, record exactly this request and its
response:

```bash
curl --fail-with-body --silent --show-error --get \
  --data-urlencode "q=$ROOM_QUERY" \
  --data-urlencode "limit=1" \
  --data-urlencode "format=json" \
  https://technocore.gudman.xyz/api/v1/rooms/search
```

The response must show one untrusted-name result, its local observation time, lifecycle state,
coverage boundary, and 16-hex evidence ID. Abort the demo if the result is absent, capped
unexpectedly, stale beyond its declared boundary, or missing the evidence metadata.

## One-screenshot capture

Open a private browser window at:

```text
https://technocore.gudman.xyz/rooms/?q=<URL-ENCODED-ROOM_QUERY>&limit=1
```

Capture one frame containing the site identity, the query, the `Untrusted room name` label, the
result, its observation/evidence rail, and the local-only/no-upstream boundary. Do not include the
operator shell, server console, private ledger, unrelated tabs, or credentials. Verify the page
source uses a generic preview and returns `X-Robots-Tag: noindex, nofollow, noarchive` before using
the screenshot.

## Integration note

**Owner-approved for delivery on 2026-08-31. DELIVERED on 2026-09-04** as
[flop-labs/technocore-chat#710](https://github.com/flop-labs/technocore-chat/issues/710), a blank
issue. Verify each recipient's official identity and use a contextual project channel; do not turn
implementation issue trackers into announcement feeds.

FLOP Labs is the only one of the three suggested recipients with a contactable channel, and they
document the lane themselves in `.github/ISSUE_TEMPLATE/config.yml`: "Proposals, questions,
measurements and external-tool announcements do not fit the report form - blank issues stay open
for them." The Report form requires a commit hash, which is the discriminator - a note carrying no
commit hash is not a bug report. GitHub Discussions is disabled on that repo, and
`security@flop.finance` is scoped by their README to software vulnerabilities, so neither is an
alternative. Overheard publishes no contact channel and states it is one person's independent
project; flopdelegate.com publishes none either and serves `noindex` on every path. Both were
dropped rather than contacted through an unverified handle.

> We built an independent, read-only Technocore evidence layer for the forward record the newest-200
> origin window rotates away. A plain GET can search a locally observed room, return its bounded
> lifecycle evidence, and link a stable hash-based record without causing any request to Technocore.
> The same contract exposes externally observed status, incidents, configuration changes, and exact
> DID observation facts in text or JSON. Every successful evidence response carries source time,
> validity, coverage, and limitations; bounded failure artifacts report only their error contract
> and freshness state. Filtered incident/change requests never degrade to an unfiltered success
> when the local query service is unavailable. Room names are query-gated and labelled untrusted.
> The one-curl and one-screenshot proof are prepared for review at
> `https://technocore.gudman.xyz` after deployment approval.

Suggested recipients after approval: FLOP Labs, Overheard, and Flop Delegate maintainers. Tailor the
opening sentence to the recipient, but do not change the evidence boundaries or imply affiliation,
endorsement, availability guarantees, historical completeness, participant verification, or
liveness certification.
