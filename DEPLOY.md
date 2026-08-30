# Deploying the Observatory

Run the guards before shipping. They exist because two defects reached production past a
careful review and a clean self-audit.

```bash
python guards.py --html index.html --derive derive.py --ticks ticks-sample.jsonl
```

`ticks-sample.jsonl` currently contains 204 collector-2.0.0-era ticks and none carries
`read_budget`. Refresh this gitignored guard corpus after every tick-schema change so the
guards exercise the new manifest shape; a legacy-only corpus cannot validate newly added fields.

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

1. Run the guards and stop if any guard fails.
2. Fence both signer-state writers before taking a snapshot or running a migration:
   - `systemctl stop technocore-observatory.service`.
   - Disable or comment out the `23 */6` census cron entry.
   - Confirm that no daemon or census invocation is still running. Do not migrate while either
     writer can reach the signer state.
3. Take a rollback snapshot only after both writers are stopped. Forward-collected ticks and the
   SQLite DID table are unrecoverable:
   - Set `rollback_dir=/root/observatory-rollback-$(date -u +%Y%m%dT%H%M%SZ)` and run
     `mkdir -m 700 "$rollback_dir"`.
   - Copy `collect.py`, `derive.py`, `migrate_signers.py`, `index.html`, `rebuild.sh`,
     `ticks.jsonl`, and `signers.json` into that directory.
   - Copy `signers.sqlite3` and each existing `signers.sqlite3-wal` and
     `signers.sqlite3-shm` sidecar into the same directory. Writers are stopped, so the database
     and any remaining sidecars form one coherent snapshot.
4. Copy the release versions of `collect.py`, `derive.py` and `migrate_signers.py`, then strip
   CRLF from the copied scripts.

   There is no separate template: `derive.py --html index.html` rewrites the deployed
   `index.html` in place, and `rebuild.sh` publishes to `/opt/technocore-observatory` only after
   that injection. So copy the committed `index.html` **only when the page markup, CSS or
   JavaScript actually changed**, and never as a routine step — it carries baked values from
   whichever run last touched it. When you do copy it, step 7 must run before anything is
   served, which is what replaces those values.
5. For a one-time JSON-to-SQLite migration only:
   - Run `python migrate_signers.py --help` and use the paths required by that checked-in
     migrator version.
   - Confirm every migration output path does not already exist. The migrator refuses
     pre-existing outputs; do not delete or overwrite an existing SQLite store to force it.
   - Run the migration only while both writers remain fenced. Preserve the source JSON and the
     rollback snapshot if migration fails.
   - Skip this step when the authoritative v3 SQLite store already exists.
6. Before starting either writer, make every deployed script and state file owned by
   `technocore:technocore`. This includes `signers.json`, `signers.sqlite3`, and every existing
   `signers.sqlite3-wal` or `signers.sqlite3-shm` sidecar. Files created by a root-run migration
   must not remain root-owned.
7. Run `rebuild.sh` while the service and census cron are still stopped. It injects the payload
   derived from the deployed `ticks.jsonl` into `index.html` and only then publishes to
   `/opt/technocore-observatory`, so the public page never shows another run's baked values. Check `accepted` and `rejected` in `data.json`, then inspect the no-JavaScript SSR
   versions, warning text, and tracked-DID label. Any newly rejected historical tick means
   backward compatibility broke; do not serve the rebuilt page.
8. Start `technocore-observatory.service`, restore the fenced census cron entry, and wait for one
   collector tick. Confirm that the tick carries the expected collector and signer-state schema.
9. Run `rebuild.sh` again and repeat the accepted/rejected and no-JavaScript SSR checks.
10. For a code rollback, fence both writers again and snapshot the current live ledger and SQLite
    store before changing anything. Restore only the previous code and page, then rebuild from
    the current live `ticks.jsonl`. Never overwrite the current `ticks.jsonl`, `signers.json`, or
    SQLite store with the pre-deploy snapshot as part of an ordinary code rollback; those copies
    are disaster-recovery material, not rollback inputs.
