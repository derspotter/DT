# Review fixes, phase 5: correctness, silent failures and hardening — design

Date: 2026-11-13, revised the same day after Codex's pre-flight review
(`.superpowers/sdd/2026-11-13-review-fixes-5-correctness/preflight-codex.md` in this worktree,
findings 1-15) and the controller's rulings on it. Branch `review-fixes-5` (worktree from
master `2c8b8b7`). Source: Phase 1 of
`/home/jay/DT/.superpowers/sdd/2026-11-13-review-fixes-5-remaining/inventory.md` ("Proposed
fix phases"; a copy sits in this worktree's `.superpowers/sdd/2026-11-13-review-fixes-5-correctness/`),
33 rows: FE-39 to FE-46, FE-48, FE-50, FE-56 to FE-58, FE-60; BE-32, BE-40, BE-41, BE-43,
BE-44, BE-49, BE-50; PY-36, PY-39, PY-44 to PY-53. PY-02 (the three #78 identifier helpers
that miss their partial indexes) is the 34th Phase 1 row and ships separately as the hotfix
on `hotfix-identifier-index-scans`. Same method as phases 2 to 4: Opus-medium implementers
under subagent-driven development, Codex `gpt-6.1-sol` xhigh as the review gate.

Two rows are closed only in part:
- **BE-40:** this phase moves hashing off the event loop. The login rate limit is an
  owner/ops follow-up (see non-goals).
- **FE-60:** this phase makes the reconnect gap visible. Filling the gap is a later phase.

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
    open password-guessing endpoint. Switching to bcryptjs's async API would not help: it
    still computes on the main thread, yielding only between chunks of about 100 ms.
16a. **Invite and reset tokens can be used twice once hashing is async (found in pre-flight).**
    Accept-invite and reset-password validate the token (`:3274-3285`, `:3341-3353`), hash, and
    then mark the token used by id without checking `used_at` (`:3301`, `:3361`). Once the hash
    is awaited, two submissions can validate the same token, and both change the password.
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
    chunks were committed. A parse failure (`:101-103`) prints a summary in the same
    `added/skipped/…` shape as a success, so the UI shows "Imported 0 works". A failing
    `DatabaseManager(...)` (`:105`) is outside any handler.
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
- Auth endpoints do not block the event loop, password hashing cannot exhaust the CPU, a
  one-time token works exactly once, and mails escape what they interpolate.
- Every write that spans several statements is atomic, and every Python exit code tells the
  truth.
- Each row has a test that fails before its fix.

## Non-goals

- **PY-02** ships as its own hotfix (branch `hotfix-identifier-index-scans`, commit `916e711`).
- **BE-19, BE-46, FE-55, PY-14** are DECISION rows for the owner and are not touched.
- No Phase 2 or Phase 3 rows. In particular, FE-14 (server-side stage filter) stays: the
  Corpus stage filter is still client-side over the loaded page after D3. FE-59 (merged
  logs-tail scope) also stays: D7 does not touch the tail's scope. Neither is made worse.
- **FE-60 is closed by making the gap visible, not by filling it (D7).** Filling the gap
  reliably needs persisted log-record identities and one replay cursor shared by
  `/api/logs/tail` and the socket. Today the tail returns backward line offsets, the socket
  sends plain strings, and the frontend `seq` values are local. No text matching can
  reconcile them: repeated lines are ambiguous. **Later phase:** record ids and a replay
  cursor (high-water mark) for tail and socket, live traffic buffered during replay and
  deduped by id, and an expired cursor reported as a gap.
- **The login rate limit of BE-40 is deferred; owner/ops follow-up.** A limiter needs the
  real client address. Production is outer Caddy → frontend Caddy (`frontend/Caddyfile:7`, no
  `trusted_proxies`) → backend, so the inner Caddy replaces the forwarded address with its
  peer. In that chain every user would share one bucket, and a per-IP limit would become a
  site-wide lockout. Blanket trust of private addresses would in turn let anything reaching
  the backend directly forge its address: port 4000 is reachable on the docker network, and
  `backend/docker-compose.yml:7` publishes it. The fix needs a production config change:
  `trusted_proxies` on both Caddies, an Express `trust proxy` naming those peers only, and
  closed alternate ingress. It must then be verified on the dev and production proxy paths.
  This phase closes the event-loop half of BE-40 (D17). Its bounded hashing queue (at most 2
  concurrent hashes, 64 queued, `503` beyond) is what stops a login burst from exhausting the
  CPU in the meantime.
- BE-40 does not address username enumeration through response timing: an unknown username
  skips the bcrypt compare.
- PY-47 does not add LibGen retries: a 429 still moves to the next mirror, as the code
  intends. Only the dead branches are fixed.
- PY-49 does not make a hung Python thread stoppable. It fails the job and restarts the whole
  worker process (D29); sibling threads of that worker die with it, and their claims are
  released by the restarted worker's start-up recovery. Selective termination would need
  each job in its own child process, which is a later phase. A job that hangs while holding
  the database write lock cannot be failed by the watchdog: the watchdog keeps retrying and
  logs once a minute, and exits only after it wins.
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

**D5. `--text` → `--ink` (FE-45).** Both uses become `var(--ink)`. A node-side spec collects
the custom properties that are used and the ones that are defined:
- **Used:** every `var(--name)` without a fallback in `src/app.css` and in all of
  `src/**/*.svelte`. A `var(--name, fallback)` counts as safe, because `--border` and
  `--row-checked` rely on their fallbacks on purpose.
- **Defined:** every `--name:` anywhere in those files. That includes inline style
  declarations such as ThreeGraph's `--label-x:` (`ThreeGraph.svelte:1976-1996`).

The spec fails on a used name that nothing defines. On `2c8b8b7` it reports exactly `text`;
after the fix it reports nothing, with no other CSS changed.

**D6. Load older admits what fits when it answers (FE-56), controller call.**
- `loadOlderLogs` sends no request when the view is full. Then it sets
  `logsTailStatus = 'The log view holds 5,000 lines. Older lines stay in the log file.'`.
- Otherwise it requests `lines: Math.min(maxLogs, maxLogHistory - logs.length)`.
- Capacity is decided again when the answer arrives, because live lines may have filled the
  view meanwhile. `flushLogs()` runs first. Then
  `room = Math.max(0, maxLogHistory - logs.length)`, and the newest `room` of the fetched lines
  (`room > 0 ? entries.slice(-room) : []`, the ones adjacent to the view) are prepended.
  Nothing is trimmed. The guard matters, because `entries.slice(-0)` is `slice(0)`, the whole
  page. When `room` is 0, nothing is prepended, the cursor stays as it was, and the status is
  the full-view text.
- The cursor advances only by the accepted count. The lines that were not accepted are
  fetched again by the next request.
- The status says what happened:
  - `Loaded <n> older log lines.` when all were accepted;
  - `Loaded <k> of <n> older log lines; the view holds 5,000 lines.` when only some were;
  - the full-view text when none were.
- The button stays enabled so that the message can be seen.
- Live lines keep trimming the oldest lines as today.

**D7. A reconnect marks the gap (FE-60), controller call.** A reconnect never hydrates and
never replaces the rendered view.
- `onclose` (except 4401) records `logsDisconnectedAt = new Date()` the first time, so a series
  of failed reconnects keeps the first time.
- When a reconnecting socket opens, `onopen` pushes one marker entry into the log buffer and
  then clears `logsDisconnectedAt`:

  ```
  { seq, text: 'Connection lost <from>–<to>. Lines from that period may be missing.', marker: true }
  ```

  `<from>` and `<to>` are local 24-hour `HH:MM:SS` times of the first close and the reopen,
  and the dash is an en dash.
- `Logs.svelte` renders a marker entry with the class `log-gap` and a `Reload logs` button
  (`data-testid="logs-reload"`). The button calls `hydrateLogsFromTail({ force: true })`, the
  existing explicit hydration, so the tail wins.
- Marker entries are not file lines. `flushLogs` advances `logsCursor` only by the non-marker
  entries, and "Load older" keeps working without a gap or a repeat.
- The first connect hydrates as today. A reconnect issues no tail request, so the existing
  reconnect specs keep their invariants: one tail request, and every line already shown or
  buffered is kept.
- A 4401 close adds no marker.

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
and `authBootRetry = true`, and disconnects the socket. The transport message is shown as a
sentence that ends in a period: a text without final punctuation gets one (`Bad gateway` →
`Bad gateway. Your session is kept; …`), and one that already ends a sentence stays as is. The sign-in card shows a `Retry`
button (`data-testid="auth-retry"`) while `authBootRetry` is set; it calls `bootstrapAuth()`.
There is no automatic retry. A manual sign-in still works (`enterSession` replaces the token).
A 401 behaves as today.

**D11. "Session expired" message (FE-48), controller call.** `expireSession` runs when a
token the page holds is rejected: a 401 from any request, or the socket's 4401 close, which
now calls `expireSession()` instead of `handleLogout()`. It shows
`Your session has expired. Please sign in again.` only when no newer outcome owns the form:
- Every sign-in, forgot-password, accept-invite or reset-password submit registers itself
  when it starts (removed in its `finally`) with the `signInEpoch` it answers to (updated
  after its own `enterSession`) and its place in the order of submits. A submit owns the
  form only while it is the newest submit and no sign-out or newer sign-in has moved
  `signInEpoch`. An abandoned submit therefore owns nothing, even before its `finally` runs:
  it neither suppresses a later session's expiry message nor overwrites a newer submit's
  outcome (this also gates the D12 recovery).
- `expireSession` decides before `endSession`:
  `const showExpiry = !authSubmitOwnsForm() && !authError && <a token is held>`. After
  `endSession`, it sets the message only when `showExpiry` is true.
- An error shown by the current submission therefore stays. That includes #77's
  `keepError` case, pinned by `session.spec.ts:706` ("a stale-token 401 at boot leaves the
  failure of … on the form"). The same goes for an error that arrives after a stale boot 401:
  the submission writes its own error last.
- The rule applies both mid-session and at boot (a stored token that the server rejects).
- An explicit sign-out (`handleLogout`) shows no message.
- The next submit clears the message, as it clears every `authError` today.

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
- Both global branches capture `sessionEpoch` at the start. On success and on failure they
  write nothing when it has changed, so an answer or error from before a sign-out cannot undo
  the reset or reach the next session. The corpus guard does not cover the global loads.
- `Dashboard` gets `globalUnavailable` (either error) and renders `—` for every `null` value,
  plus `Global totals could not be loaded.` under the heading while `globalUnavailable` is set.
  The last loaded numbers stay on screen next to that note.
- Sign-out resets both values to `null`.

**D15. A stopped seed import is a failure (PY-39 UI half), controller call.** `uploadItem`
checks `result.stopped`. When it is set, the upload gets status `failed`:
- with `processed === 0` and `added === 0` (a parse or database-setup failure, or the first
  chunk failing), the message is `Import failed: <error>`;
- otherwise it is `Import stopped after <processed> of <total> entries: <error> (<added> added,
  <corpus_linked> linked before it).`.

The summary contract is D25.

### Backend

**D16. Escaped auth mails (BE-32).** These live in the new `backend/src/authMail.js`:
- `escapeHtml(value) -> string`, which escapes `& < > " '`;
- `tokenMailContent({ kind, username, url }) -> { subject, html, text }`, with `kind` either
  `'invite'` or `'reset'`.

It renders one template with the current subjects, headings, texts and expiry lines
(72 hours, 2 hours). `username` and `url` are escaped in the HTML, and the text part is left
as is. `sendInviteEmail` and `sendPasswordResetEmail` keep their token logic and call it.
`createApp({ broadcast, broadcastEvent, mailTransport } = {})` accepts an optional transport
with `sendMail(options)`. When it is given, `sendTransactionalMail` uses it instead of SMTP.
Tests use it to read the HTML that the invitation and forgot-password routes actually send.

**D17. Password hashing off the event loop (BE-40), controller call.**
- `backend/src/passwordHasher.js` holds a small `worker_threads` pool. Its workers run
  `backend/src/passwordWorker.js`, which calls bcryptjs's `hashSync`/`compareSync`, so the
  `$2b$10$` hash format is unchanged. It exports:
  - `createPasswordHasher({ size = 2, maxQueue = 64 } = {}) -> { hash(password, rounds = 10) ->
    Promise<string>, compare(password, hash) -> Promise<boolean>, close() -> Promise<void> }`
  - `getPasswordHasher()`, a process-wide instance created lazily. Its workers are
    `unref()`'d, so neither the server nor Jest is kept alive by them.
- At most `size` hashes run at once; up to `maxQueue` more wait in FIFO order. Beyond that,
  the call rejects with an `Error` whose `code` is `'HASHER_BUSY'`. A worker that dies is
  replaced, and its pending call rejects.
- The handlers of `/api/auth/login`, `/api/auth/register`, `/api/auth/invitations`,
  `/accept-invite` and `/reset-password` await `getPasswordHasher()`. On `HASHER_BUSY` they
  answer `503` with `Retry-After: 5` and
  `{ error: 'The server is busy; try again in a moment.' }`.
- No `compareSync`/`hashSync` is left in a request handler. The boot compare/hash in
  `bootstrapDefaultCorpus` stays synchronous, because it runs before `listen`.
- The bound (2 running, 64 queued) is also what keeps a login burst from exhausting the CPU
  until the rate limit (see non-goals) lands.

**D18. One-time tokens are consumed atomically (found in pre-flight).** Accept-invite and
reset-password keep the early read of the token, which gives the fast "invalid or expired"
answer. After hashing, one `authDb.transaction` does two things:
1. It runs `UPDATE <token table> SET used_at = CURRENT_TIMESTAMP WHERE id = ? AND used_at IS
   NULL AND datetime(expires_at) > datetime('now')`. When that changes anything other than
   exactly one row, the transaction throws, and the route answers the existing
   `400 { error: 'Invite link is invalid or expired' }` or `'Reset link is invalid or
   expired'`.
2. Only after that, it updates the user's password (plus `account_status` and the timestamps,
   as today).

When two submissions race, exactly one succeeds. The stored hash matches the winner's
password.

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
- The route reads `extractionInlineModels()` once per request, when the extraction starts.
  The inline model reaches the APIscraper child as `RAG_FEEDER_API_INLINE_MODEL` through
  `runApiScraper`'s `inlineModelOverride`, as today.
- A route-level Jest test proves that the route uses the value of the Admin setting:
  - It sets distinctive models through `PUT /api/admin/settings`: `inline_cheap_model:
    'adm-cheap-x'` and `inline_model: 'adm-pro-x'`.
  - It starts `/api/extract-bibliography` with `RAG_FEEDER_PYTHON` pointing at a fake
    interpreter. The fake records, for each child it is asked to run, the script name, the
    args, and the env values `RAG_FEEDER_API_INLINE_MODEL` and `RAG_FEEDER_API_EXTRACT_MODEL`.
  - It asserts which models each APIscraper child received.

  The plan specifies the fake and its scenarios.

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

**D25. `ingest_import_seed` always prints one summary shape (PY-39).** Every outcome prints
one JSON line and exits 0:

```
added: int
skipped: int
corpus_linked: int
total: int
processed: int
stopped: bool
error: str | None
errors: list[str]
```

The only consumer chain is `spawnPython` → `runPythonJson` → the import-seed route, which
forwards the JSON unchanged.
- **Chunks.** Each chunk counts into local counters, which are added to the totals only
  after its `with db.transaction()` exits. An exception out of a chunk (`BEGIN`, `COMMIT` or a
  lost transaction) stops the loop. The script then prints the totals of the committed
  chunks, with `stopped: true`, `processed` set to the entries in the committed chunks, and
  `error: "<ExceptionType>: <message>"`.
- **Parse failure.** It prints `stopped: true`, `processed: 0`, `total: 0`,
  `error: "<ExceptionType>: Failed to parse <kind>: <message>"`, and all counts 0.
- **Database setup failure** (`DatabaseManager(...)` raising). It prints `stopped: true`,
  `processed: 0`, `total: <entries parsed>` and `error: "<ExceptionType>: <message>"`.
- **A run without a failure** prints `stopped: false`, `processed: total` and `error: null`.
- `errors` keeps the per-entry messages.

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
watchdog. It exits the worker only after it has won ownership of the timed-out work, and a
callback from finished work can never act. A hung Python thread cannot be stopped any other
way, so the exit is how the work ends: both workers run under docker's
`restart: unless-stopped` in dev and prod.

- **Generation token.** `self._work_lock` (a `threading.Lock`) guards
  `self._work_generation` (an int), `self._work_finished` (a bool) and `self._terminal` (a
  bool, false until a watchdog wins).
  - `_begin_work()` takes the lock. When `_terminal` is set, it returns `None`. Otherwise it
    increments the generation, sets `_work_finished = False` and returns the new generation.
  - The run loop calls `_begin_work()` before it claims anything: before `fetch_next_job` and
    before each direct cycle, both of which claim rows. On `None` the loop claims nothing and
    starts no job; it sleeps in 0.5 s steps until the process exits. When no job was found
    and no direct cycle runs, the loop ends the generation with `_end_work`.
  - The work's `finally` calls `_end_work(generation)`, which sets `_work_finished = True`
    under the lock, before it stops the heartbeat thread.
  - A deadline callback acts only when, under the lock, its generation is still current,
    `_work_finished` is false and `_terminal` is false. The 1-second teardown join can
    therefore leave a late callback running, but that callback cannot touch later work.
- **Ownership holds through exit.** The callback decides and latches inside one `_work_lock`
  section. Either it wins and sets `_terminal = True` before releasing the lock, or it does
  not win and changes nothing. After a win, the finishing work can still call `_end_work`
  (harmless), but `_begin_work` refuses every new generation, so no row is claimed and no job
  starts until the process exits. The exit itself needs no further generation check: nothing
  started after the win.
- **Deadlines.**
  - Jobs: `deadline_seconds = self.job_timeout_seconds` (env
    `RAG_FEEDER_PIPELINE_JOB_TIMEOUT`, default 1800).
  - Direct batches: `env_int("RAG_FEEDER_DIRECT_BATCH_TIMEOUT", self.job_timeout_seconds,
    minimum=60)`.
- **Job ownership.** Under `_work_lock`, the callback runs a conditional `UPDATE` through
  `heartbeat_conn` (its own connection). It takes `heartbeat_lock` with
  `acquire(timeout=1.0)`; a timeout counts as not won, and it retries on the next beat.
  `busy_timeout` is set to 1000 ms for this statement and restored afterwards:

  ```sql
  UPDATE pipeline_jobs
     SET status='failed', finished_at=CURRENT_TIMESTAMP, result_json=?
   WHERE id=? AND status='running' AND worker_id=?
  ```

  `result_json` is `{"error": "timed out after <N>s; worker restarted"}`.
  - When exactly one row changed, the watchdog has won.
  - When no row changed, the job was completed or cancelled first. The watchdog stops without
    exiting.
  - When the statement raises `sqlite3.Error` (for example, the hung job holds the write
    lock), or `heartbeat_lock` cannot be taken within 1 s, the watchdog has not won. It retries on every beat interval and logs
    `job <id> timed out but could not be marked failed: <error>` at most once a minute. It
    never exits without winning.
  - The completion path's `mark_job_completed` is already conditional on `status='running'`.
    When the watchdog has won, completion records nothing, as for a cancelled job.
- **Batch ownership.** A direct batch has no job row. The watchdog wins when the generation
  check passes, and it latches `_terminal` in the same lock section.
- **Terminal reporting is time-bounded.** After the win (and outside `_work_lock`), the
  watchdog makes one attempt at an `error` heartbeat with `state: "job_timeout"` (or
  `"direct_batch_timeout"`). The attempt takes `heartbeat_lock` with `acquire(timeout=1.0)` and
  skips the heartbeat on timeout. It writes with a 1000 ms busy timeout and swallows any
  error. The watchdog then logs and calls `self._exit(75)` (`os._exit` by default). From the
  win to the exit, the waits are at most 1 s for `heartbeat_lock` and 1 s for SQLite's busy
  timeout, about 2 s plus the log write. A test seam `self._after_watchdog_win` (a no-op by
  default) runs right after the latch, so that a test can suspend the callback there.
- **Restart.** The restarted worker's start-up recovery (`_recover_stale_running_jobs`, which
  includes `_release_work_claims`, `worker.py:376-432`) immediately returns every row the dead
  worker had claimed (`in_progress` with its `worker_id`) to its queue state. Leases do not
  have to expire. The whole process exits, so sibling threads of the same worker die with it.
  Their claims are released the same way.
- **Periodic recovery.** `run()` also calls `_recover_stale_running_jobs(periodic=True)` at most
  once every 60 s before `fetch_next_job`, for work that a worker which never came back left
  running. The periodic call reclaims a job, and releases a work claim, only when its owner is
  positively dead:
  - the owner's `worker_id` has no `daemon_heartbeat` row at all;
  - its row has status `'stopped'`;
  - it is a predecessor: our `daemon_name` row now names a different `worker_id`.

  It never reclaims by job age or by heartbeat staleness, and never this worker's own jobs.
  A live owner whose job holds the write lock looks overdue and stale while it still runs: its
  watchdog cannot win, and a reclaim would hand the job to a second worker (duplicate
  execution). Start-up recovery keeps its full rule (age plus staleness) unchanged.

**D30. Partial related-work fetches are reported (PY-50, PY-51), controller call.**
- `dl_lit/utils.py` gains `class FetchResult(list)` with the attributes `complete: bool = True`,
  `quota_exhausted: bool = False`, `errors: list[str]` and `failed_ids: list[str]`.
- `fetch_citing_work_ids`, `fetch_referenced_work_details` and `new_dl.fetch_openalex_batch`
  return one. On an error they set `complete = False` and append the error (redacted as
  today). On `OpenAlexRateLimitExceeded` they also set `quota_exhausted = True`. The batch
  fetchers add the failed batch's ids to `failed_ids`. `FetchResult` compares equal to a
  plain list.
- **Status is reported where it is known.** Each fetcher prints one closing
  `[OpenAlex WARN] <what> incomplete: <len(self)> fetched; <errors[0]>` line when incomplete,
  on stdout like its existing progress lines. No wrapper has to carry the attributes:
  `_fetch_upstream_candidates`, `_collect_candidate_ids` and `_extract_openalex_ids` turn
  results into plain id lists, and deduplication or concatenation drops subclass attributes.
  Only direct callers read the attributes.
- **Quota exhaustion propagates as an exception where a caller handles it.** The two
  OpenAlexScraper fetchers gain `raise_on_quota: bool = False`.
  - With `True`, they re-raise `OpenAlexRateLimitExceeded` after printing.
  - The keyword goes only on the three direct fetcher calls:
    1. `keyword_search.py:297`, `fetch_citing_work_ids(...)` inside
       `_fetch_upstream_candidates`;
    2. `keyword_search.py:384`, `fetch_referenced_work_details(...)` in `_crawl_direction`;
    3. `seed_expand.py:145`, `fetch_referenced_work_details(...)`.
  - No wrapper gains a parameter. The exception travels unchanged through
    `_fetch_upstream_candidates` and `_collect_candidate_ids` (shared by both scripts;
    `seed_expand.py:31-34` imports it), and also through `seed_expand._collect_for_direction`
    (`seed_expand.py:72`, called at `:135`), which takes no such keyword and is not changed.
  - The existing `except OpenAlexRateLimitExceeded` branches (`keyword_search.py:374`,
    `:390`; `seed_expand.py:138`, `:151`) stay. They can now run, and they also still catch
    the helpers that raise today (for example `fetch_work_by_openalex_id`).
  - Enrichment callers keep the default `False`, so their behaviour does not change.
- **`fetch_openalex_batch`** returns a `FetchResult` too. Its only caller,
  `ingest_json_directory` (`new_dl.py:2334`), uses it directly and prints
  `[OpenAlex WARN] <len(failed_ids)> of <requested> referenced works could not be fetched.`
  when `failed_ids` is set.
- **Rate limiter call fixed.** `fetch_openalex_batch` calls `limiter.wait_if_needed()` with
  no service name (`new_dl.py:2204`), but the real `ServiceRateLimiter.wait_if_needed`
  requires one (`utils.py:49`). The call becomes `limiter.wait_if_needed("openalex")`, and the
  tests use the real limiter class.
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
4. Backend auth and routes (D16 to D23; D18 is the token race found in pre-flight).
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
  - 20 sign-ins in parallel stay responsive: other requests answer while the hashes run;
  - a backend restart while the Logs tab is open shows the "Connection lost" marker, and
    "Reload logs" replaces the view with the tail;
  - a stopped import shows the counts;
  - an admin overview with the daemon-status route failing shows "—".

## Controller calls (for the owner)

- **D3 (FE-41):** a corpus switch resets the Corpus selection, the stage filter (back to
  "All") and the expanded row.
- **D4 (FE-44):** in all-selected mode the count shows selectable rows, not the seed total.
- **D6 (FE-56):** "Load older" admits only what fits in the 5,000-line view when the answer
  arrives, and says how many lines it accepted.
- **D7 (FE-60):** a reconnect adds a "Connection lost <from>–<to>. Lines from that period may
  be missing." marker with "Reload logs". Filling the gap automatically waits for record ids
  and a replay cursor in a later phase.
- **D8 (FE-42):** a failed Kantropos target list is shown as an error and keeps the last view.
  An empty list says "No upstream corpora are configured.".
- **D9 (FE-46):** a failed Korpus Management load shows the error with Retry, keeps the last
  data with a note, and hides the empty-state texts until a load succeeds.
- **D10 (FE-43):** a non-401 failure at boot keeps the token and offers Retry. There is no
  automatic retry.
- **D11 (FE-48):** "Your session has expired. Please sign in again." appears only when no
  current submission owns the form.
- **D12 (FE-50):** when the password was saved but signing in failed, the sign-in form opens
  with the username filled in, and the message says the password was saved.
- **D14 (FE-58):** a failed global overview shows "—" plus "Global totals could not be
  loaded.".
- **D15 / D25 (PY-39):** a stopped import exits 0 with `stopped: true`. The UI shows
  "Import failed: …" when nothing was saved, or "Import stopped after … (… added, … linked
  before it)." otherwise.
- **D17 (BE-40):** hashing runs in a worker pool, 2 at a time with 64 queued. Beyond that,
  the answer is 503 "The server is busy; try again in a moment." with Retry-After: 5.
- **D22 (BE-49):** two new Admin settings, "Inline model (cheap)" and "Inline model
  (fallback)". They take precedence over the env, then today's defaults.
- **D23 (BE-50):** `/api/corpus` returns at most 1000 rows per request.
- **D29 (PY-49):** a job or batch past its timeout is marked failed, and the worker exits
  with 75 to be restarted. This happens only after the watchdog has won ownership, and from
  that moment the worker claims no new work. A job hung while holding the DB write lock is
  retried and logged, never killed blind.
- **D30 (PY-50, PY-51):** partial OpenAlex fetches are logged as `[OpenAlex WARN]`, with no
  retries. Quota exhaustion now stops keyword and seed expansion early, as their handlers
  intended.
- **Owner/ops follow-up (BE-40):** the login rate limit needs the outer-Caddy →
  frontend-Caddy → backend forwarding chain configured (`trusted_proxies`, an Express trust
  list naming the peers, closed alternate ingress on port 4000). It also needs verification
  on both proxy paths. Not in this phase.
