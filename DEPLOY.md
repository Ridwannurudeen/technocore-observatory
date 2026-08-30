# Deploying the Observatory

Run the guards before shipping. They exist because two defects reached production past a
careful review and a clean self-audit.

```bash
python guards.py --html staged/index.html --derive staged/derive.py --ticks ticks-sample.jsonl
```

Non-zero exit means do not deploy.

## What each guard catches, and why reading the code could not

**zero-width render** — renders the built page in real Chrome and fails any element that asked
for a width and got zero. This is a *layout* fact, not a code fact: `.density-fill` set
`width: 6.15936%` correctly from JavaScript, but the class sat on a `<span>`, and inline
elements ignore width. The capacity panel's rule was byte-identical CSS and worked only because
it was on a `<div>`. No amount of reading reveals that; the layout engine has to answer it.

A static CSS check was tried first and abandoned: `.funnel li{display:grid}` blockifies a
`<span>` child that no class selector matches, so the heuristic produced a false positive on
`.funnel-label` immediately. A guard that cries wolf gets muted, which is worse than no guard.

**payload contract** — runs the deriver for real and checks every `data.x` / `point.x.y` path
the page reads against what was actually emitted. `projection_seconds` was removed from the
deriver while the page still read it; each file was self-consistent and the pair was broken.
Static field-name matching on the deriver source would have been guesswork, so the guard uses
the real output as ground truth.

Both are proven against the original defects: re-break `.density-fill` and the first fails with
the element, the requested width and `display:inline`; make the page read a removed field and
the second names the exact path.

## Deploy

1. Guards pass.
2. Snapshot for rollback, including `ticks.jsonl` — forward-collected data is unrecoverable:
   `cp collect.py derive.py index.html template.html ticks.jsonl signers.json /root/observatory-rollback-$(date -u +%Y%m%dT%H%M%SZ)/`
3. `systemctl stop technocore-observatory.service`, copy the four files, strip CRLF, copy
   `index.html` to `template.html`, `chown technocore:technocore`.
4. `systemctl start technocore-observatory.service`; wait for a tick and confirm it carries the
   expected schema before trusting the rebuild.
5. `rebuild.sh`, then check `accepted` / `rejected` in `data.json`. Any rejected tick after a
   schema change means backward compatibility broke — roll back rather than lose collection.
