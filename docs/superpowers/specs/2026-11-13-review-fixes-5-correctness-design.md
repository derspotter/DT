# Review fixes, phase 5: correctness, silent failures and hardening — design

Date: 2026-11-13. Branch `review-fixes-5` (worktree from master `2c8b8b7`). Source: Phase 1 of
`.superpowers/sdd/2026-11-13-review-fixes-5-remaining/inventory.md` ("Proposed fix phases"),
33 rows: FE-39 to FE-46, FE-48, FE-50, FE-56 to FE-58, FE-60; BE-32, BE-40, BE-41, BE-43,
BE-44, BE-49, BE-50; PY-36, PY-39, PY-44 to PY-53. PY-02 (the three #78 identifier helpers
that miss their partial indexes) is the 34th Phase 1 row and ships separately as the hotfix
on `hotfix-identifier-index-scans`. Same method as phases 2 to 4: Opus-medium implementers
under subagent-driven development, Codex `gpt-6.1-sol` xhigh as the review gate.

Every row's evidence was re-read in the code on `2c8b8b7`; the line numbers below are from
that commit. Where a fix is a product choice the inventory leaves open, the decision says
**controller call**. Those calls go to the owner as a list.

## Problems (verified against master `2c8b8b7`)

### Frontend

1. **Corpus loads drop user requests (FE-39).** `loadCorpus` returns early on
   `!append && corpusLoading` (`App.svelte:3548`). The filter input (`:2636-2642`) and the sort
   toggle (`:2688-2691`) call `loadCorpus({ quiet: true })`, so a typed filter or a sort click
   during a poll is dropped and the header shows the new value over the old rows until the
   next poll. "Show more" and the scroll trigger are refused or disabled while any load runs
   (`:3555`, `:917`, `Corpus.svelte:379`), and the 2 s poll keeps a load running much of the time.
2. **Append does not dedupe (FE-40).** `corpusItems = append ? [...corpusItems, ...incoming]`
   (`:3642`). A row inserted between polls shifts the offsets, so "Show more" repeats a row
   and Corpus's keyed `{#each}` gets a duplicate key.
3. **Corpus component state survives a corpus switch (FE-41).** `selectedWorkIds`,
   `stageFilter` and `activeCorpusKey` are component-local (`Corpus.svelte:64`, `:114-115`), and
   `<Corpus>` (`App.svelte:5122`) is not keyed. A bulk-remove selection can carry over to
   another corpus that has the same work ids.
4. **The select-all checkbox of a partly promoted search seed never reads checked (FE-44).** In
   all-selected mode the selected count is `page.total` (`:2785`, `:4818`), while
   `estimatedSelectableSeedCount` subtracts in-corpus and downloaded rows (`:2806-2826`). The
   comparison at `:4821` fails, so the box looks unchecked, and a click selects again instead
   of clearing.
5. **`var(--text)` is undefined (FE-45).** It is used at `app.css:2920` (Kantropos assignment
   toolbar buttons) and `:3335` (`.document-run`), but `:root` defines `--ink`, not `--text`, so
   both fall back to the browser's button colour.
6. **"Load older" throws away what it fetched once the view is full (FE-56).**
   `logs = [...older, ...logs].slice(-maxLogHistory)` (`:4015`) cuts the fetched lines when
   5,000 are shown, but the status still says "Loaded N older log lines" (`:4025`), and the
   cursor moves past lines that were never shown.
7. **A log socket reconnect loses the lines emitted while it was down (FE-60).**
   `hydrateLogsFromTail` returns when the view is not empty (`:3963`), and the reconnect path
   (`:4077-4101`) calls it unchanged.
8. **Korpus Management hides "none configured" and reads a failed target load as "none" (FE-42).**
   The status is set at `:2234`, then `resetUpstreamBrowseState()` clears it (`:668`). A failed
   `fetchKantroposCorpora` empties `kantroposTargets` (`:2203-2205`), so the view resets as if no
   targets existed.
9. **A failed quiet browse load is silent and shows false empty states (FE-46).** The catch
   only writes the status when `!quiet` (`:2271-2274`). The tab loader is quiet
   (`:1478`). The pending and current sections render without a loaded target
   (`UpstreamBrowser.svelte:383-490`), so a first load that fails shows "No pending additions
   match the current filters." and "This upstream corpus has not been imported into DT yet."
10. **Boot deletes a valid token on any non-401 `/me` failure (FE-43).** The catch at
    `:1601-1609` clears the token on a timeout or a 502 during a deploy, so a reload signs the user
    out without a message.
11. **Session expiry is a silent sign-out (FE-48).** `endSession` clears `authError` (`:1816`),
    and the socket's 4401 close calls `handleLogout` (`:4133`). The user lands on the login form
    mid-work with no reason given.
12. **Invite/reset: the password is saved, but a failed follow-up `/me` reads as a failed
    activation (FE-50).** The catches at `:1712-1715` and `:1762-1765` show the raw error and leave
    the new token stored. Retrying the form answers "Invite link is invalid or expired"
    because the token is already used.
13. **A quota answer from before a sign-out reaches the next session (FE-57).**
    `loadOpenAlexQuota` (`:344-353`) has no session guard, so a late answer repopulates the pill
    after a sign-out. Its `openalexQuotaLoading` flag also blocks the next session's first load
    until the old request settles.
14. **The admin global overview shows zeros after a failed load (FE-58).** `globalIngestStats`
    starts as zeros (`:168-172`), the global catch leaves it there (`:2486-2493`), and
    `loadPipelineWorkerStatus({ global })` has no failure handling for the global branch
    (`:3702-3705`, `:3734-3737`). `Dashboard.svelte:113-146` renders the zeros as real numbers.

### Backend

15. **Invite and reset mails put `username` into HTML unescaped (BE-32).** See
    `app.js:3035` and `:3063`. The two builders (`:3020-3051`, `:3053-3083`) also repeat the same
    markup.
16. **bcrypt blocks the event loop, and login has no rate limit (BE-40).** `compareSync` runs at
    `:3097`, and `hashSync` at `:3209`, `:3236`, `:3291` and `:3356`. Every failed login costs about
    70 ms of main-thread time, and nothing limits the attempts, so this is a cheap DoS and an
    open password-guessing endpoint.
17. **Every boot rehashes and rewrites the admin password (BE-41).** The unconditional
    `hashSync` and `UPDATE` run at `:972` and `:983`.
18. **Multi-statement writes run without a transaction (BE-43).** These are
    `createUserWithDefaultCorpus` (`:3000-3017`: user, corpus, membership, `last_corpus_id`) and
    `POST /api/corpora` (`:3773-3785`: corpus, membership, `last_corpus_id`). A failure between
    statements leaves a user without a corpus or a corpus without an owner row.
19. **`/api/ingest/import-seed` passes `--corpus-id` twice (BE-44).** The route adds it at
    `:5553`, and `spawnPython` appends it again (`pythonProcess.js:101-103`).
20. **Route code hard-codes the inline-extraction models (BE-49).** `inlineModelOverride:
    'gemini-3.1-pro-preview'` is at `:4252`. The cheap and pro inline models come from
    `process.env` with literals (`:4441`, `:4450`). `APP_SETTING_DEFS` (`:2264-2275`) has no entry
    for them, so the Admin tab cannot change them. Admin settings reach only the children's env,
    and the explicit override beats them.
21. **`/api/corpus` does not clamp `limit` (BE-50).** It uses `coerceInt(req.query?.limit, 200)`
    (`:5065`), so one request can ask for the whole 72k corpus.

### Python

22. **An IntegrityError in `create_pending_work` leaves its writes in place (PY-36).** The
    handler at `db_manager.py:1420-1421` returns an error without undoing anything. On
    `commit=True` paths (`APIscraper_v2.py:879`) the implicit transaction stays open, so the rest
    of the file fails. On `commit=False` paths the caller's savepoint is released with the
    entry's partial rows, because the exception never reaches it.
23. **`ingest_import_seed` aborts without a summary (PY-39).** A failed `BEGIN IMMEDIATE` or
    `COMMIT`, or a lost transaction (re-raised since #78), leaves the chunk loop through the
    bare `try/finally` (`ingest_import_seed.py:109-145`). The JSON print at `:147` is skipped, the
    process exits non-zero, and the UI shows "Python script failed (1): …" although earlier
    chunks were committed.
24. **The rate-limit waits run once per call, not once per attempt (PY-44).**
    `get_bib_pages.py:198-199`, `APIscraper_v2.py:430-431` and `:671-672` wait before calling
    `run_with_timeout`, so timed-out retries are never counted against the rpm and daily budgets.
25. **Abandoned attempts disappear (PY-45).** `llm_common._run_attempt` drops a late result or
    error. Tokens spent by a timed-out call are never charged or logged, and its late error is
    invisible.
26. **`get_bib_pages.main()` returns 0 after per-PDF errors (PY-46).** See `:563-571`. The backend
    (`app.js:4330-4382`) then falls back to inline extraction with the reason "no extracted
    bibliography pages found" instead of "get_bib_pages failed with code 1".
27. **The LibGen error branches never run (PY-47).** `e.response and …` (`new_dl.py:966`, `:976`)
    is always false for a 4xx/5xx, because `Response.__bool__` is `ok`. The 429 and 5xx debug
    captures never run.
28. **Heartbeat threads die on errors `_heartbeat` does not catch (PY-48).** `_heartbeat`
    catches only `OperationalError` (`worker.py:315`). The beat loops at `:337-339` and `:355-357`
    have no try, so one `DatabaseError` ("malformed") ends the thread, and a working job then
    shows "offline".
29. **The job timeout is enforced only at start-up (PY-49).** `_recover_stale_running_jobs`
    runs from `__init__` only (`:258`). A hung job or direct batch stays "running" and blocks its
    queue until someone restarts the worker.
30. **Related-work fetches return partial lists as success (PY-50).**
    `fetch_citing_work_ids` and `fetch_referenced_work_details` (`OpenAlexScraper.py:150-158`,
    `:236-243`) print and return what they have on any error, including
    `OpenAlexRateLimitExceeded`. The callers' own `except OpenAlexRateLimitExceeded` branches
    (`backend/scripts/keyword_search.py:390`, `seed_expand.py:138`, `:151`) are therefore dead,
    and expansion silently truncates.
31. **`fetch_openalex_batch` drops failed batches with only a print (PY-51).** See
    `new_dl.py:2224-2232`. Its caller (`:2334`) cannot tell that works are missing.
32. **`dl_lit add-pending-works` exits 0 after an exception (PY-52).** See `cli.py:474-475`.
33. **Three writers catch only `sqlite3.Error` after `BEGIN` (PY-53).** These are
    `apply_enriched_metadata` (`db_manager.py:1532→1594`), `enqueue_for_download` (`:1602→1620`) and
    `queue_for_enrichment` (`:1628→1646`). Any other exception leaves the worker's shared
    connection mid-transaction, and every later `BEGIN` fails until a restart.

## Goals

- No silent failure in this list: every failure is either shown to the user with what is
  still true, or logged with what was lost.
- Nothing from one session reaches the next: quota, Corpus selection, errors.
- A user request is never dropped because a background poll is running.
- Auth endpoints neither block the event loop nor accept unlimited guesses; mails escape what
  they interpolate.
- Every write that spans several statements is atomic, and every Python exit code tells the
  truth.
- Each row has a test that fails before its fix.

## Non-goals

- **PY-02** ships as its own hotfix (branch `hotfix-identifier-index-scans`, commit `916e711`).
- **BE-19, BE-46, FE-55, PY-14** are DECISION rows for the owner and are not touched.
- No Phase 2 or Phase 3 rows. In particular, FE-14 (server-side stage filter) stays: the
  Corpus stage filter is still client-side over the loaded page after D3. FE-59 (merged
  logs-tail scope) also stays: the reconnect backfill in D7 uses the same `getLogsCorpusId()`
  scope as today's tail. Neither is made worse.
- BE-40 fixes neither username enumeration through response timing (an unknown username
  skips the bcrypt compare) nor rate limits on forgot-password, accept-invite and
  reset-password. Those take high-entropy tokens or send mail and are out of this row.
- PY-47 does not add LibGen retries: a 429 still moves to the next mirror, as the code
  intends. Only the dead branches are fixed.
- PY-49 does not make a hung Python thread stoppable. It fails the job and restarts the
  process (D27).
- PY-50 and PY-51 add no retries; a partial result is reported, not repaired.
- The other `dl_lit` CLI commands that print errors and exit 0 (for example
  `extract-bib-pages`, `cli.py:444-445`) are not in the row and stay unchanged.
- `README.md` is not changed. The behaviours are internal or covered by the UI text below.

## Decisions

### Frontend: workspace tables and logs

**D1. User reloads supersede, polls yield (FE-39).** `loadCorpus` gains the option
`{ supersede = false }`. The filter debounce and `toggleCorpusSort` pass `supersede: true`. A
superseding load starts even while another load is in flight. It bumps `corpusLoadRequestSeq`,
so the older answer is discarded by the existing seq check, and its `finally` (which only
resets the flags when it owns the seq) leaves the flags to the new load. Polls
(`quiet && preserveSelection`) keep returning early while anything is in flight. An append
requested while a non-append load is in flight is queued in `corpusAppendQueued = true` and
runs from that load's `finally` when it still owns the seq and `corpusHasMore` holds. A
superseding load clears the queue, because the new query starts from offset 0. The scroll
handler and the "Show more" button use the queue: the button is disabled only while
`corpusLoadingMore`.

**D2. Append dedupes by id (FE-40).** Rows whose `id` is already loaded are dropped from
`incoming` before concatenation. `corpusHasMore` still compares the loaded count with the
total, so a dropped duplicate can never stop paging early: the next offset is
`corpusItems.length`, as before.

**D3. Corpus is keyed by corpus (FE-41), controller call.** `{#key currentCorpusId}` wraps
`<Corpus …>`. A corpus switch therefore resets the selection, the stage filter (back to
"All") and the expanded row. Column visibility is stored per browser and survives the switch.

**D4. All-selected means every selectable row (FE-44), controller call.** In all-selected mode,
the selected count and the "Selected: N / M selectable" text show the selectable estimate
(`estimatedSelectableSeedCount`) instead of `page.total`, and `fullySelected` is true whenever
`seedAllSelected[sourceId]` is set. `selectedSeedCount`, `isSeedFullySelected` and the
`{@const}` lines at `:4818`/`:4821` use the same rule. Promotion and dismissal are unchanged:
they already skip unselectable rows on the server.

**D5. `--text` → `--ink` (FE-45).** Both uses become `var(--ink)`. A node-side spec scans
`src/app.css` and the `<style>` blocks of `src/**/*.svelte` for `var(--name` and fails on any
name that no `--name:` defines. This covers the whole class of bug, not just the two lines.

**D6. Load older stops at the view's limit (FE-56), controller call.** `loadOlderLogs`
requests `lines: Math.min(maxLogs, maxLogHistory - logs.length)` and prepends every line it
gets (no trim). When `logs.length >= maxLogHistory`, it sends no request and sets
`logsTailStatus = 'The log view holds 5,000 lines. Older lines stay in the log file.'`. The
button stays enabled so that this message can be seen. The cursor advances by the lines
actually prepended. Live lines keep trimming the oldest lines as today.

**D7. Reconnect backfill (FE-60), controller call.** `onclose` (except 4401) stores
`logsGapAnchor`, the texts of the last 3 lines of `[...logs, ...logBuffer]`. A reconnect is
`connectLogs({ reconnect: true })`. After its `onopen`, it requests the tail
(`lines: maxLogs` = 200, `cursor: 0`, same type and corpus scope) and calls the new pure
function `gapLines(anchor, tailTexts, receivedSince)` from `frontend/src/lib/logMerge.js`:
- it finds the last index `i` where `tailTexts[i-k+1..i]` equals `anchor` (`k = anchor.length`,
  1 to 3);
- it takes `missing = tailTexts.slice(i + 1)`;
- it cuts `missing` at the first index from which it starts with `receivedSince[0]` (the lines
  that arrived on the new socket before the answer are already shown);
- it returns `missing`, or `null` when the anchor is not in the tail or the anchor is empty.

On an array result, the missing lines are inserted before the `receivedSince.length` newest
view lines (`flushLogs()` first), the view is trimmed to `maxLogHistory`, and `logsCursor`
grows by `missing.length` (capped). On `null` (a gap over 200 lines, or nothing to anchor on),
the view reloads from the file: `hydrateLogsFromTail({ force: true })`, with the status
`'Reconnected; reloaded the latest log lines.'`. The epoch guard (`logsScopeEpoch`) applies.
The first connect keeps today's behaviour.

### Frontend: session and failure states

**D8. Kantropos target failures are not "none configured" (FE-42), controller call.**
`fetchKantroposTargets` sets `kantroposTargetsError` (the error message, `''` on success).
`loadUpstreamBrowse` behaves as follows:
- When the target list failed and no target id is known, it keeps the last browse and sets
  `upstreamBrowseError = true` with the status `Could not load the Kantropos corpora:
  <message>`.
- When the list loaded empty, it runs `resetUpstreamBrowseState()` first and then sets the status
  `No upstream corpora are configured.`, both quiet and not.

**D9. A failed browse load says so and keeps the last data (FE-46), controller call.**
- The catch sets `upstreamBrowseError = true` and the status `Could not load the upstream
  corpus: <message>.`, quiet or not. When a target was loaded before, the status adds
  ` Showing the last loaded data.`.
- A successful load clears the error.
- `UpstreamBrowser` gets `statusError` (renders the status with class `error`) and shows a
  `Retry` button (`data-testid="upstream-retry"`, calls `refresh`) while it is set.
- The pending and current sections, and with them their empty-state texts, render only when
  `browse.target` is set.

**D10. Boot keeps the token on a non-401 failure (FE-43), controller call.** The catch in
`bootstrapAuth` no longer clears the token on a non-401 error. It sets
`authStatus = 'unauthenticated'`,
`authError = '<transport message> Your session is kept; press Retry when the server is back.'`
and `authBootRetry = true`, and disconnects the socket. The sign-in card shows a `Retry`
button (`data-testid="auth-retry"`) while `authBootRetry` is set; it calls `bootstrapAuth()`.
There is no automatic retry. A manual sign-in still works (`enterSession` replaces the token).
A 401 behaves as today.

**D11. "Session expired" message (FE-48), controller call.** `expireSession` runs when a
token the page holds is rejected: a 401 from any request, or the socket's 4401 close, which
now calls `expireSession()` instead of `handleLogout()`. It sets
`authError = 'Your session has expired. Please sign in again.'` after `endSession`. This
applies both mid-session and at boot (a stored token that the server rejects). An explicit
sign-out (`handleLogout`) shows no message. The sign-in submit clears the message, as it
clears every `authError` today.

**D12. Invite/reset after a saved password (FE-50), controller call.** In
`handleAcceptInvite` and `handleResetPassword`, a failure after the password call succeeded
(`fetchMe` or the loads) does the following:
- it clears the token (`setAuthToken('')`) and disconnects;
- it switches to the sign-in form (`authFlow = 'login'`, hash `#/login`), with `loginUsername`
  prefilled from the password answer's `user.username`;
- it sets `authError = 'Your password was saved, but signing in failed (<message>). Sign in
  with your new password.'`.

A failure of the password call itself keeps today's text.

**D13. Session-scoped quota load (FE-57).** `openalexQuotaLoading` becomes
`openalexQuotaEpoch` (`null` when idle). The guard is
`if (openalexQuotaEpoch === sessionEpoch) return`, the answer is applied only when
`epoch === sessionEpoch`, and the `finally` clears the field only when it still holds this
request's epoch. Sign-out already sets `openalexQuota = null` (`:2025`).

**D14. Global overview shows "unavailable" (FE-58), controller call.**
- `globalIngestStats` and `globalWorkerBacklog` start as `null`.
- The global branches of `loadIngestStats` and `loadPipelineWorkerStatus` set
  `globalStatsError` or `globalDaemonError` on failure and clear them on success.
- `Dashboard` gets `globalUnavailable` (either error) and renders `—` for every `null` value,
  plus `Global totals could not be loaded.` under the heading while `globalUnavailable` is set.
  The last loaded numbers stay on screen next to that note.
- Sign-out resets both values to `null`.

**D15. A stopped seed import is a failure (PY-39 UI half).** `uploadItem` checks
`result.stopped`. When it is set, the upload gets status `failed` and the message
`Import stopped after <processed> of <total> entries: <error> (<added> added, <corpus_linked>
linked before it).`. The summary contract is D22.

### Backend

**D16. Escaped auth mails (BE-32).** These live in the new `backend/src/authMail.js`:
- `escapeHtml(value) -> string`, which escapes `& < > " '`;
- `tokenMailContent({ kind, username, url }) -> { subject, html, text }`, with `kind` either
  `'invite'` or `'reset'`.

It renders one template with the current subjects, headings, texts and expiry lines
(72 hours, 2 hours). `username` and `url` are escaped in the HTML, and the text part is left
as is. `sendInviteEmail` and `sendPasswordResetEmail` keep their token logic and call it.

**D17. Async bcrypt (BE-40).** `bcrypt.compare` / `bcrypt.hash` (bcryptjs, async) replace
every `compareSync`/`hashSync` in request handlers; `/api/auth/login`,
`/api/auth/register`, `/accept-invite`, `/reset-password` become `async`. The boot
compare/hash in `bootstrapDefaultCorpus` stays synchronous (before `listen`).

**D18. Login rate limit (BE-40), controller call.**
- `backend/src/loginLimiter.js` exports `createLoginLimiter({ maxPerUser = 10,
  maxPerIp = 100, windowMs = 15 * 60_000, maxKeys = 10_000, now = Date.now })`. It returns
  `{ check(ip, username) -> { allowed: boolean, retryAfterSeconds: number },
  recordFailure(ip, username), recordSuccess(ip, username) }`.
- Usernames are trimmed and lowercased. Failures are counted in a sliding window: once the
  oldest failure in the window ages out, the key is allowed again.
- A key is the pair `(ip, username)` with at most 10 failures, or the `ip` alone with at most
  100 failures across usernames. Nothing is keyed by username alone, so nobody can lock
  another user out from elsewhere.
- A success clears that pair. Keys over `maxKeys` evict the oldest.
- `/api/auth/login` calls `check` before the user lookup. When the check refuses, the route
  answers `429` with the header `Retry-After: <seconds>` and
  `{ error: 'Too many failed sign-in attempts. Try again in <n> minute(s).' }`, where
  `n = ceil(retryAfterSeconds / 60)`.
- Invalid credentials call `recordFailure`. The "invitation pending" 403 is not a failure.
- `createApp` sets `app.set('trust proxy', 'loopback, linklocal, uniquelocal')`, so `req.ip` is
  the client behind the two Caddy hops on the docker networks and not the proxy. A spoofed
  leftmost `X-Forwarded-For` entry is ignored.
- The frontend already shows the body's `error` on the sign-in form.

**D19. Boot keeps a matching admin hash (BE-41).** `bootstrapDefaultCorpus` first reads the
stored hash. When the row exists and `bcrypt.compareSync(adminPassword, stored)` is true, it
does no hash and no `UPDATE`. Otherwise it behaves as today: the env stays authoritative.

**D20. Transactions (BE-43).** `createUserWithDefaultCorpus` runs its four statements in
`authDb.transaction(...)()`. `POST /api/corpora` runs its three statements in one transaction
inside a `try`, which answers `500 { error }` on failure.

**D21. One `--corpus-id` (BE-44).** The import-seed route stops pushing `--corpus-id` itself.
`spawnPython` appends `--corpus-id <id>` only when `args` does not already contain
`'--corpus-id'`, so a caller that passes both cannot double it either.

**D22. Inline models are Admin settings (BE-49), controller call.**
- `APP_SETTING_DEFS` gains `inline_cheap_model` (env `RAG_FEEDER_API_INLINE_CHEAP_MODEL`) and
  `inline_model` (env `RAG_FEEDER_API_INLINE_MODEL`), both non-secret.
- `export function extractionInlineModels(settingsEnv = appSettingsEnv(), env = process.env)`
  in `app.js` returns `{ cheap, pro }`. Each value is the Admin setting, else the env value,
  else the default: `'gemini-3-flash-preview'` for `cheap` and `'gemini-3.1-pro-preview'`
  for `pro`.
- The route uses `pro` for the fallback override at `:4252` and for the last-resort inline
  call, and `cheap` for the cheap inline step.
- The Admin panel's "Advanced" block gets two text inputs, "Inline model (cheap)" and
  "Inline model (fallback)", bound like `gemini_model`.

**D23. `/api/corpus` limit clamp (BE-50), controller call.** The route clamps the limit with
`Math.min(CORPUS_MAX_LIMIT, Math.max(0, coerceInt(req.query?.limit, 200)))`, where
`CORPUS_MAX_LIMIT = 1000`. That matches the frontend's largest request,
`CORPUS_POLL_PAGE_ROWS = 1000` (`App.svelte:403`). `listCorpus` stays unclamped for the golden
tests.

### Python

**D24. `create_pending_work` undoes its own writes on an IntegrityError (PY-36).** With
`commit=False`, the body runs inside `self.savepoint()`, so the savepoint rolls the entry
back before the `except sqlite3.IntegrityError` returns the error. With `commit=True`, the
handler calls `self.conn.rollback()` when `self.conn.in_transaction` is true. The return
value is unchanged: `(None, "IntegrityError inserting into works: …")`.

**D25. `ingest_import_seed` always prints a summary (PY-39).** Each chunk counts into local
counters that are added to the totals only after its `with db.transaction()` exits. An
exception out of a chunk (BEGIN, COMMIT or a lost transaction) stops the loop. The script
then prints the totals of the committed chunks plus the following and exits 0:

```
stopped: true
processed: <entries in committed chunks>
error: "<ExceptionType>: <message>"
```

`errors` keeps the per-entry messages. A run without a failure prints `"stopped": false` and
`"processed": total`.

**D26. Exit codes (PY-46, PY-52).**
- `get_bib_pages.main()` counts the PDFs that raised, prints
  `Done. Extracted N page(s) in Xs; F of P PDF(s) failed.` and returns `1` when `F > 0`. `2`
  still means a usage error.
- `add-pending-works` echoes the error with `err=True` and raises `SystemExit(1)`.
- The backend already handles a non-zero `get_bib_pages`: it runs the inline fallback with the
  reason `get_bib_pages failed with code 1`.

**D27. LLM attempts (PY-44, PY-45).**
- `run_with_timeout` gains `before_attempt: Callable[[], None] | None = None`, called before
  every attempt including retries, and `token_service: str | None = None`.
- `_run_attempt(call, timeout_seconds, *, on_late=None)` hands a result that arrives after the
  timeout to `on_late(ok, value, late_seconds)` on the attempt's thread. A lock around
  "append outcome" and "mark abandoned" decides whether a result counts as late.
- The default late handler prints one line to stderr: `[WARNING] <label> attempt <n>
  finished <late>s after its timeout and was discarded (tokens=<t> | error=<Type>: <msg>)`.
  When `ok` and `token_service` is set and the response has tokens, it also calls
  `wait_rate_limit(token_service, units=t)`.
- `get_bib_pages` and `APIscraper_v2._run_generate_content` pass
  `before_attempt=lambda: (wait_rate_limit("gemini"), wait_rate_limit("gemini_daily"))` and
  `token_service="gemini_tokens"`, and drop the waits at `get_bib_pages.py:198-199`,
  `APIscraper_v2.py:430-431` and `:671-672`.
- The success path still charges the tokens of the attempt that returned.

**D28. Heartbeats survive any error (PY-48).**
- `_heartbeat` catches `sqlite3.Error`. Busy and other errors are logged as today, at most once
  a minute each.
- The two thread starters become one `_start_heartbeat_thread(payload, *, name,
  deadline_seconds, on_deadline)`. Its loop wraps each beat in `try/except Exception`, logged
  at most once a minute, so the thread never dies of a beat.
- `_start_job_heartbeat` and `_start_direct_batch_heartbeat` keep their names and payloads and
  delegate to it.

**D29. Job and batch watchdog (PY-49), controller call.** The heartbeat thread is also the
watchdog.
- `_start_job_heartbeat` passes `deadline_seconds=self.job_timeout_seconds` (env
  `RAG_FEEDER_PIPELINE_JOB_TIMEOUT`, default 1800).
- When the deadline passes, the thread marks the job failed through `heartbeat_conn` (its own
  connection, under `heartbeat_lock`):

  ```sql
  UPDATE pipeline_jobs
     SET status='failed', finished_at=CURRENT_TIMESTAMP, result_json=?
   WHERE id=? AND status='running' AND worker_id=?
  ```

  `result_json` is `{"error": "timed out after <N>s; worker restarted"}`.
- It then writes an `error` heartbeat with `state: "job_timeout"`, logs, and calls
  `self._exit(75)` (`os._exit` by default), so that docker's `restart: unless-stopped` starts a
  fresh worker. A hung thread cannot be stopped any other way. If the failed-mark itself
  fails (main connection holding the lock), the worker still exits, and the start-up recovery
  requeues the job.
- Direct batches use `deadline_seconds = env_int("RAG_FEEDER_DIRECT_BATCH_TIMEOUT",
  self.job_timeout_seconds, minimum=60)` with `state: "direct_batch_timeout"` and no job row;
  their claimed rows' leases expire as today.
- `run()` also calls `_recover_stale_running_jobs()` at most once every 60 s before
  `fetch_next_job`, for jobs that a worker which never came back left running.

**D30. Partial related-work fetches are reported (PY-50, PY-51), controller call.**
- `dl_lit/utils.py` gains `class FetchResult(list)` with the attributes `complete: bool = True`,
  `quota_exhausted: bool = False`, `errors: list[str]` and `failed_ids: list[str]`.
- `fetch_citing_work_ids`, `fetch_referenced_work_details` and `new_dl.fetch_openalex_batch`
  return one. On an error they set `complete = False` and append the error (redacted as
  today). On `OpenAlexRateLimitExceeded` they also set `quota_exhausted = True`. The batch
  fetchers add the failed batch's ids to `failed_ids`.
- Each prints one closing `[OpenAlex WARN] <what> incomplete: <len(self)> fetched;
  <errors[0]>` line when incomplete.
- Nothing raises, so enrichment callers keep their behaviour.
- `backend/scripts/keyword_search.py` (`:297`, `:384`) and `seed_expand.py` (`:135`, `:145`)
  check `quota_exhausted` and stop early with their existing WARN text, replacing the
  `except` branches that could never run. They print the incomplete line to stderr otherwise.
- `ingest_json_directory` prints `[OpenAlex WARN] <len(failed_ids)> of <requested> referenced
  works could not be fetched.` when `failed_ids` is set.
- There are no retries.

**D31. LibGen branches (PY-47).** Both conditions become `getattr(e, "response", None) is
not None and e.response.status_code …`.

**D32. Writers roll back on any exception (PY-53).** `apply_enriched_metadata`,
`enqueue_for_download` and `queue_for_enrichment` run their body in `with self.transaction():`
(`BEGIN IMMEDIATE`, rollback and re-raise on any exception). An outer
`except sqlite3.Error as e` keeps today's return values. Any other exception propagates
after the rollback. The early `return None, "Row not found in works"` inside the block
commits an empty transaction.

## Order of work

1. Python transactions and exit codes (D24, D25, D26 for PY-52, D32).
2. Python LLM attempts (D26 for PY-46, D27).
3. Python worker liveness and partial fetches (D28 to D31).
4. Backend auth and routes (D16 to D23).
5. Frontend tables and logs (D1 to D7).
6. Frontend session and failure states (D8 to D15).

## Acceptance

- Every row in scope has a test that fails on `2c8b8b7` and passes after its task. Each
  task's commit names the rows it closes.
- Python: `cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests -q` is green, and
  the new test files are in the `ci.yml` pytest list (line 68).
- Backend: `cd backend && npm test` is green. After the change,
  `grep -n "compareSync\|hashSync" backend/src/app.js` shows only `bootstrapDefaultCorpus`.
- Frontend: `npx vite build` succeeds. The CI Playwright list, run from `frontend/` with
  `E2E_FRONTEND_PORT=5190`, is green with the new specs added to it. No live spec is run,
  and the dev stack on `:5175` is not used.
- Never against `dl_lit_project/data/literature.db`.
- After merge, check on the dev stack:
  - a sign-in with a wrong password 11 times gives 429;
  - a backend restart while the Logs tab is open fills the gap;
  - a stopped import shows the counts;
  - an admin overview with the daemon-status route failing shows "—".
