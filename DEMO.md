# Prepared COMPASS demo

> **Draft only:** the read-side release is not deployed, this evidence has not been captured, and
> the integration note below has not been posted or sent. Use it only after deployment verification
> and explicit owner approval.

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

## Integration note draft

**Do not send without explicit approval.**

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
