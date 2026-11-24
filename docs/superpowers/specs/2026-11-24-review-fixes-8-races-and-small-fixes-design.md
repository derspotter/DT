# Review fixes, phase 8: races and small fixes — design

Date: 2026-11-24, revised the same day after the Codex pre-flight round 1
(`.superpowers/sdd/2026-11-24-review-fixes-8-races-and-small-fixes/preflight.md`, rulings in
`progress.md` next to it, section "Pre-flight round 1"). Branch `review-fixes-8` (worktree from
master `c574aba`).
Source: the reconciliation of the review inventory against `c574aba`
(`.superpowers/sdd/review-fixes-8-reconcile/{fe,be,py}.md`, and the section "Reconciliation against
master `c574aba` (2026-11-24)" at the end of
`/home/jay/DT/.superpowers/sdd/2026-11-13-review-fixes-5-remaining/inventory.md`, which holds the
owner decisions of 2026-11-24). Rows: FE-60b, FE-63, FE-61, FE-64, FE-22, FE-24, BE-55, BE-54,
BE-38, PY-34, PY-54, PY-27 (DRY half). Same method as phases 2 to 7: Opus-medium implementers
under subagent-driven development, Codex as the review gate, plus the Claude review pass while
Copilot is out of quota.

The owner's principle: secure and fast, not over-engineered; the smallest change that removes the
cost or the risk.

Every row was re-read in the code on `c574aba`; the line numbers below are from that commit. Where
a fix is a choice the row leaves open, the decision says **controller call**. Those calls are
listed at the end. Two rows are closed without a code change (FE-22, BE-38); the reasons are in
their decisions.

## Problems (verified against master `c574aba`)

### Frontend

1. **A seed candidate PDF that arrives after a sign-out is still saved (FE-60b).**
   `handleSeedCandidateFile` (`App.svelte:4390-4410`) awaits `downloadSeedCandidateFile`, then
   creates the object URL, clicks the link and writes `seedActionStatus`, with no `sessionEpoch`
   check. The next user of the browser gets the previous user's PDF and a "Downloaded …" line. The
   three sibling download handlers have the same shape: `handleExportDownload` (`:4266-4296`,
   blob, `exportStatus`, and `exportBusy` cleared in `finally`; `exportBusy` is not part of the
   workspace reset, `:506`, `:2194`), `handleDownloadedCorpusFile` (`:4298-4314`, navigates to a
   ticket URL, `corpusLoadStatus`) and `handleDownloadSeedDocument` (`:4369-4388`, blob,
   `ingestRunsStatus`).
2. **A search launched before a sign-out or corpus switch updates the next session (FE-63).**
   `runSearch` checks the session and corpus epochs after its preview (`App.svelte:3810-3834`),
   but `startSearch` (`:3863-3900`) does not. After `await runKeywordSearch(...)` (`:3877`) it
   adds the run id to `startedSearchRunIds` (`:3882`), which makes `anySearchRunning` true
   (`:217`) and puts the next session on the 2 s cadence until two seed-list misses drop it
   (`:186-215`); it reloads and focuses the seed list, and writes `searchStatus` (`:3889`,
   `:3893`, `:3897`). The sign-out and corpus-switch reset (`resetFrontendState`, `:2212-2247`)
   clears these values before the answer lands, so the late writes survive.
3. **A late node-detail failure clears another node's detail (FE-61).** `loadNodeDetail`
   (`components/ThreeGraph.svelte:1112-1133`) checks that the node is still selected before it
   stores a success, but its `catch` sets `selectedNodeDetail = null` and its `finally` sets
   `nodeDetailLoading = false` for whichever node is selected now. Select A (slow), then B: A's
   failure wipes B's metadata, and A's settling (success or failure) hides B's "Loading
   metadata..." while B's own request is still running.
4. **A full or blocked localStorage fails a good upload (FE-64).** `persistStoredUploads`
   (`App.svelte:766-788`) calls `window.localStorage.setItem` unguarded (`:787`). Every
   `updateUpload` (`:739-742`) calls it after replacing `uploads`. In `uploadItem`
   (`:2693-2735`) the first call (`:2694`) sits outside the `try`, so a throwing `setItem`
   rejects `uploadItem` before the upload starts; a throw inside the `try` lands in the `catch`,
   which marks the upload failed (`:2733`) and throws again from the same `setItem`.
   `extractForItem` (`:2737-2759`) has the same shape. `restoreStoredUploads` (`:790-812`) and
   `tableColumns.js:84-86` already guard their storage calls.
5. **`rearmRefreshOnChange` does not list `activeTab` or `isAdmin` (FE-22).** The reactive line
   is `$: rearmRefreshOnChange(authStatus, anySearchRunning, ingestStats, pipelineWorker, downloadWorker)`
   (`App.svelte:220`); `refreshReason` (`:1351-1366`) reads `activeTab` and `diagnosticsEnabled`.
   See D5: no stale cadence follows from it.
6. **The Vite dev server always polls the file system (FE-24).** `vite.config.js:54-56` sets
   `watch.usePolling: true` unconditionally. The dev containers already set
   `CHOKIDAR_USEPOLLING=1` (`docker-compose.yml:169`, `docker-compose.dev.yml:30`). The Playwright
   web server (`playwright.config.ts:12-21`) runs the same `npm run dev` on the host and in CI,
   where inotify works, so every mocked spec run polls every watched file at 100 ms. Vite 7.3.1
   bundles chokidar 3, which reads `CHOKIDAR_USEPOLLING` itself and lets it override the option
   (`node_modules/vite/dist/node/chunks/config.js:13887-13893`).

### Backend

7. **Removing one work from a corpus is two writes without a transaction (BE-55).** `DELETE
   /api/corpus/works/:workId` (`app.js:5507-5530`) runs the `corpus_works` delete and then the
   `seed_candidates_in_corpus` marker delete as two autocommit statements. If the second fails
   (a lock past `busy_timeout`, I/O), the route answers 500 although the work is already unlinked,
   and the stale marker keeps the seed table showing it as promoted. The bulk route
   (`:5535-5569`) runs the same two statements in one `authDb.transaction`.
8. **A graph load can mix files of two snapshot builds (BE-54).** The snapshot key is stable
   across rebuilds (`app.js:421-438`), and a rebuild swaps the whole key dir by rename
   (`buildGraph3dSnapshot`, `:513-555`; the old dir is removed right after the swap). The
   manifest's resource URLs only carry `?v=<generated_at>` (`decorateGraph3dManifest`,
   `:600-635`). The file route (`:5874-5917`) ignores `v` and serves whatever the key dir holds
   now. If a background rebuild (`:5798-5802`, after the 1 h TTL) swaps between the manifest
   answer and the four parallel file requests (`frontend/src/lib/api.js:792-802`), the browser
   pairs `nodes.bin` of one build with `edges.bin` or `nodes_meta.json` of the other: edge
   indices point at the wrong nodes or past the end. The files carry `max-age=600`
   (`app.js:5889`), so the mixed answer is also cached under the old build's URL. The gzip path
   also stats `.gz` (`:5907`) separately from opening it (`:5908`).
9. **Every log line costs a sync open, stat and close (BE-38).** `appendLogLine`
   (`index.js:46-53`) calls `rotateIfNeeded` (`existsSync` and `statSync`, `:27-44`), then
   `appendFileSync`. `backend-pipeline-v2.log` is shared: the enrich and download workers append
   to it too, through an `O_APPEND` handle they reopen when `os.path.samestat` sees a rotation
   (`backend/scripts/daemon/worker.py:115-142`). Only the backend rotates, by rename.

### Python

10. **The topic backfill writes by row id long after it read the row (PY-34).**
    `backfill_openalex_topics.py` reads all candidate rows of a pass up front (pass 1
    `:250-262`, pass 2 `doi_rows` `:86-105`), then fetches them in batches of 50 (`run_pass`,
    `:178-224`). `write_results` (`:154-166`) and `mark_fetched` (`:169-175`) update
    `WHERE id = ?` only. A row whose `openalex_id` or `doi` the enrich worker changed (or that a
    duplicate merge rewrote) in the meantime gets the topics of the identifier it no longer has,
    and `topics_fetched = 1` keeps later runs from correcting it. The window is the whole pass,
    not one request.
11. **A failed enrich claim looks like an empty queue (PY-54).** `claim_enrich_batch`
    (`dl_lit/db_manager.py:1730-1859`) answers `[]` on any `sqlite3.Error` (`:1854-1859`). Its
    one caller, `do_enrich` (`backend/scripts/daemon/worker.py:1247-1268`), then returns zero
    counters, so an `enrich` job (`:1710-1719`) or a `pipeline_tick` (`:1746`) is marked
    `completed` with nothing done and no error, and a targeted enrich (`pending_work_ids`) leaves
    its rows pending without a word. A direct cycle reads it as idle and sleeps
    (`:1673-1678`, `:1906`). With a 30 s `busy_timeout` (`db_manager.py:100-109`), such an
    error is a real fault. Phase 7 (PY-35, #82) gave `claim_download_batch` a `raise_errors`
    flag (`:2857-2870`, `:2979-2985`) and used it in the CLI only (`cli.py:682-690`); the
    worker's download claim (`worker.py:1596`) still gets `[]` on an error, with the same effect
    on `download` jobs.
12. **The scraper reads the OpenAlex key itself (PY-27, DRY half).**
    `OpenAlexCrossrefSearcher.__init__` (`dl_lit/OpenAlexScraper.py:576-583`) reads
    `os.getenv('OPENALEX_API_KEY') or os.getenv('RAG_FEEDER_OPENALEX_API_KEY')` unstripped and
    warns under its own flag (`:58`). `utils.get_openalex_api_key` (`utils.py:260-262`) strips
    and treats a blank value as unset, and `openalex_request_json` warns under a second flag
    (`utils.py:42`, `:357-364`, `:527-529`). A process without a key prints the warning twice; a
    key with a trailing newline is sent as is by the scraper's own requests and stripped by
    `openalex_request_json`.

## Goals

- Nothing requested under one session (a file, a search launch) changes the next session: no
  saved file, no status line, no polling cadence.
- A node's detail panel shows only that node's outcome.
- An upload never fails because the browser cannot store the upload list.
- Mocked Playwright runs and CI do not poll the file system; the dev containers still do, at
  their current cost.
- Removing one work from a corpus is all or nothing.
- A graph load notices a rebuild and reloads once, in the same session and only while the graph
  is open (with D8's accepted residual).
- The topic backfill writes only rows still in the state it selected them in.
- A failed claim in the worker is a failed job or cycle, never an empty one, and a job whose
  failure cannot be recorded is requeued by a restart.
- One place reads the OpenAlex key, and the missing-key warning prints once per process.
- Each change has a test that fails on `c574aba`.

## Non-goals

Everything else that is still open after the reconciliation belongs to later phases:
- **Phase 9 (backend/frontend DRY):** FE-11, FE-16 (one download helper for the four handlers
  D1 touches), FE-23 "built twice" half, BE-24, BE-27, BE-28, BE-29, BE-30, BE-31, BE-33, BE-34,
  PY-32.
- **Phase 10 (performance):** BE-03, BE-11 residual, BE-35, BE-47, PY-07, PY-08, PY-28 (source
  order).
- **Phase 11 (structure):** BE-23 (schema module and `user_version`), FE-10, PY-30, PY-23.
- Still parked: BE-39 (structured refresh events), BE-52 (needs the dev DB, which no phase opens),
  PY-33, FE-17 remainder.
- Closed as won't fix by the owner on 2026-11-24: FE-21, PY-25, FE-20 (perf half; its crash half
  is FE-64, here), PY-31 (phase 6).
- **D1 does not merge the four download handlers;** each gets the same three-line guard. FE-16
  replaces their link code with one helper in phase 9.
- **D8 keeps one build per key on disk and assumes one writer.** No per-build directories, no
  grace period for old builds, no fd streaming; the small gap between the version check and the
  file open is an accepted residual (D8).
- **D11 does not change the watchdog or the direct-cycle error path;** it adds only the restart
  when a job's failure cannot be recorded.

## Decisions

### Frontend

**Late-answer gates for every browser race test (D1 to D4, D8).** No race test waits a fixed
time. Each one:
- holds the late request on a gate (`deferred()`);
- confirms that the new session or corpus has loaded before it releases the gate (the new
  user's name in `auth-user` and the workspace rendered; or the switched corpus in the picker and
  its `/api/auth/me` answered);
- after the release, waits on a browser-side checkpoint before it asserts:
  `const response = page.waitForResponse(<url>)`, release, `await (await response).finished()`,
  then `await page.evaluate(() => new Promise((r) => requestAnimationFrame(() => setTimeout(r, 0))))`.
  The helper is `afterLateAnswer(page, responsePromise)` in `tests/helpers/mockApi.ts`.

**D1. A download that settles after a sign-out is dropped (FE-60b), controller call.**
- `handleSeedCandidateFile`, `handleDownloadSeedDocument`, `handleDownloadedCorpusFile` and
  `handleExportDownload` each take `const session = sessionEpoch` before their request. Right
  after the `await`, `if (session !== sessionEpoch) return`: no object URL, no link, no
  navigation, no status. Their `catch` returns on `error?.status === 401 || session !== sessionEpoch`.
- Only the session counts, not the corpus: a file the user asked for under corpus A is still
  theirs after a switch to B.
- **The export lock is session-wide.** `exportBusy` stays out of `initialWorkspaceState()`, so a
  corpus switch keeps it: one export at a time per session, and no second export of the same
  session can start while the first runs. `resetFrontendState` sets `exportBusy = false` in its
  `signOut` branch only. `handleExportDownload`'s `finally` clears it only when
  `session === sessionEpoch`, so an old session's export never clears a new session's.
- Tests:
  - `seed-selection.spec.ts` (it has the readable-candidate fixture, `:217-225`). Its `boot`
    answers `/api/auth/login` with token `second-token`, and `/api/auth/me` by the bearer token
    (`second-token` → user `second`, else `reader`). Hold `…/candidates/readable/file`; click the
    row's Download; log out; sign in as `second` and wait for `auth-user` to read `second`;
    release with a PDF and a `Content-Disposition`; `afterLateAnswer`. Then: no `download` event
    and no `Downloaded` text.
  - Same file, the failure case: the file answers `500 { error: 'late-candidate-error-7f3' }`;
    after the checkpoint the text `late-candidate-error-7f3` is absent. (A non-JSON body would be
    shown as the error text, so `/Failed to download/` alone would already be absent on
    `c574aba`.)
  - `session.spec.ts`: alice starts the Bundle ZIP export (held); log out; bob signs in; bob's
    export button is enabled; bob starts an export (held too); release alice's export and wait
    for the checkpoint: no download event, no `export downloaded` text, and bob's button is still
    disabled (his export owns the lock); release bob's: one download event, button enabled.
  - `tab-resources.spec.ts`: an export held across a corpus switch keeps the button disabled in
    the new corpus until it settles; then the button is enabled.

**D2. A search launch is dropped once the frontend commits a session or corpus change (FE-63),
controller call.**
- `startSearch`, after its corpus check, takes `const session = sessionEpoch` and
  `const scope = corpusScopeEpoch`, with `const stale = () => session !== sessionEpoch || scope !== corpusScopeEpoch`.
- `if (stale()) return` right after `runKeywordSearch` (before `refreshQuota` and before
  `startedSearchRunIds`), after `loadSeedSources` and after `focusSeedSource`. The `catch`
  returns on `error?.status === 401 || stale()`.
- **The boundary is the frontend's commit.** `handleSelectCorpus` (`App.svelte:2319-2349`)
  saves the selection on the server, then waits for `/me`, and only then bumps
  `corpusScopeEpoch` (through `resetFrontendState`). A launch answered in that gap still applies
  its effects, as today; the commit that follows resets all of them (`startedSearchRunIds`,
  `searchStatus`, the seed list), and a `loadSeedSources` or `focusSeedSource` still running then
  ends on its own epoch check and on `stale()`. Phase 7's submitted-corpus binding (FE-62: the
  body carries `corpusId`, and `startSearch` drops a body whose corpus is no longer current) is
  unchanged.
- The run itself goes on server side, as it does today.
- Tests:
  - `session.spec.ts`: the preview answers `{ count: 5, threshold: 100000 }`; hold
    `POST /api/keyword-search`; once it is seen, log out and sign in as bob (wait for
    `auth-user` = `bob`); release `202 { runId: 7, status: 'running' }`; `afterLateAnswer`. Then
    no `Fetching in the background` text, and `window.__dtRefresh.reason()` is not `'active'`.
  - `tab-resources.spec.ts`, after the frontend commit: the same with a corpus switch (wait for
    the picker to read `2` and for the `/api/auth/me` that follows the select).
  - `tab-resources.spec.ts`, in the gap: hold the launch and hold the `/me` after the select
    (`delays.me` gate); release the launch first and wait for its checkpoint; then release `/me`
    and wait for the commit. Then no `Fetching in the background` text, the reason is not
    `'active'`, and the launch body carried `corpusId: 1`.

**D3. Only the newest detail request writes the panel and the cache (FE-61).**
- `ThreeGraph.svelte` gets `let nodeDetailSeq = 0`. `loadNodeDetail` takes
  `const seq = ++nodeDetailSeq` first, before the cache check, so a cache hit also supersedes an
  older request.
- After the fetch, nothing happens unless `seq === nodeDetailSeq`: the cache write, the panel
  write (still only while the node is selected), the `catch`'s clear and the `finally`'s
  `nodeDetailLoading = false` all check it. A superseded answer is dropped, not cached; a later
  selection fetches again.
- **Deselection ends the request.** `clearSelection` (`:1174`), `showAllNodes` (`:946`) and the
  reset at the start of `load3DGraph` (`:1215-1222`) bump `nodeDetailSeq` and set
  `nodeDetailLoading = false`, so hovering another node never shows the old node's
  "Loading metadata...".
- Tests, `graph-3d.spec.ts`, with gates on `/api/graph/3d/node/1` and `/node/2`:
  - select `Graph seed work` (held), select `Referenced work` (answers), see
    `Identifiers: 10.1000/example`; release node 1 with 500 and wait for the checkpoint: still
    shown, no `Loading metadata...`;
  - A → B → A: node 1's first request held (gate 1), node 2 answers, node 1's second request held
    (gate 2); release gate 2 with `doi: '10.1000/seed-v2'` (shown), then gate 1 with
    `10.1000/seed-v1` and wait for the checkpoint: `10.1000/seed-v2` still shown; select node 2,
    then node 1 again: no third node-1 request, and `10.1000/seed-v2` shows (the cache hit);
  - deselection: select node 2 (answers, cached), select node 1 (held), press Escape, select node
    2 again (cache hit); release node 1 with 500 and wait for the checkpoint:
    `Identifiers: 10.1000/example` still shown and no `Loading metadata...`;
  - older answer: node 1 and node 2 both held, select 1 then 2; release node 1 with 200 and wait
    for the checkpoint: `Loading metadata...` still shown, no `10.1000/seed`; release node 2:
    its identifiers show.

**D4. The upload list is saved best effort (FE-64).**
- `persistStoredUploads` wraps the `window.localStorage.setItem` call (the `window.localStorage`
  access included) in `try`/`catch`. On a failure it logs
  `console.warn('[uploads] could not save the upload list:', error?.message)` once per page load
  (a module flag) and returns. The upload and extraction go on. No other caller changes.
- **What a reload shows:** the last list that was saved. If every save failed, that is nothing.
  If earlier saves worked and later ones fail (the quota filled up mid-session), a reload restores
  that older list, with the older statuses (`normalizeStoredUploadStatus` maps running states to
  `uploaded`). The seed list from the server stays the source of truth for extracted PDFs.
- Test, `seed-upload.spec.ts`: an init script replaces `Storage.prototype.setItem` with one that
  throws a `DOMException` named `QuotaExceededError` for keys starting with `rag-feeder:uploads:`
  and calls the original otherwise (the token key keeps working). The BibTeX upload of the first
  test (`:68`) runs; the test asserts the upload row's final text directly
  (`Imported 2 works (1 skipped, 2 added to corpus).`) and that it is not failed. It does not
  wait on the import request, which never starts on `c574aba`, so the baseline fails at that UI
  assertion. It also expects exactly one `[uploads] could not save the upload list:` warning.

**D5. FE-22 is closed: not a bug, controller call.** Every way the two values change already
re-arms the timer, or the stale cadence ends at the next cycle:
- `activeTab` is only assigned in `setActiveTab` (`App.svelte:4729`), which calls
  `scheduleRefresh()` itself (`:4734`), and in `onMount` before sign-in (`:4743`), when
  `authStatus` is not `authenticated`. The sign-in that follows changes `authStatus`, which is on
  the list.
- `isAdmin`/`diagnosticsEnabled` derive from `authUser` (`:120-121`, declared before `:220`, so
  they update first in the same flush). The frontend learns of a role change only when a `/me`
  answer shows it:
  - at sign-in, which changes `authStatus` too;
  - in the `/me` poll inside a refresh cycle (`:1476`), whose `finally` re-arms
    (`runLiveRefreshCycle`, `:1587-1593`) with the new value;
  - in the corpus actions (`:2332`, `:2363`, `:2412`), which reset `ingestStats`, which is on the
    list.
  `diagnosticsEnabled` only matters next to a busy worker, and loading the worker status changes
  `pipelineWorker`/`downloadWorker`, which are on the list.
- So once `/me` has shown a change, the cadence is stale for at most one cycle (2 s active, 20 s
  idle), which a dependency would not shorten. No code change; the row is closed in the inventory.

**D6. The config no longer forces polling (FE-24), controller call.**
- `vite.config.js` sets `watch: { usePolling: false }`, with a comment: the bundled chokidar 3
  (`node_modules/vite/dist/node/chunks/config.js:13887-13893`) lets `CHOKIDAR_USEPOLLING`
  override the option; the dev containers set it to `1` (`docker-compose.yml:169`,
  `docker-compose.dev.yml:30`) for the bind mount; Playwright and CI leave it unset.
- Effect: the host (Playwright, CI, a local `npm run dev`) stops polling. The containers keep
  polling and keep its CPU cost, unchanged; the genkia.de dev container (`rag_feeder_frontend`,
  `./frontend:/app` bind mount) sees the same watcher after Vite restarts on the config change.
  No compose change.
- Chokidar's own parsing of the variable stays: `0` and `false` turn polling off; any other
  non-empty value, `bogus` and ` 0 ` included, turns it on. The config does not parse the
  variable itself, because chokidar would override the result anyway.
- Test, new `frontend/tests/dev-server-config.spec.ts` (no page). It checks the watcher Vite
  actually builds, not the config object. A node child with `cwd` the frontend dir runs
  `createServer({ configFile: 'vite.config.js', logLevel: 'silent', optimizeDeps: { noDiscovery: true } })`
  from `vite`, prints `JSON.stringify({ usePolling: server.watcher.options.usePolling })` and
  closes the server without `listen()`. The expected values:
  - `false` with the variable removed from the env;
  - `true` with `1` (compose's value);
  - `true` with ` TRUE `.

  The spec is added to the CI Playwright list (`.github/workflows/ci.yml:54`). The same probe in
  a scratch dir against Vite 7.3.1 gave `false` unset, `true` for `1`, `false` for `0`, and
  `true` for ` 0 ` and `bogus`.

### Backend

**D7. The single-work removal is one transaction (BE-55).**
- The route runs both deletes in `authDb.transaction(() => { … })()`: the `corpus_works` delete
  first; when it changed nothing, the transaction returns `0` and the route answers 404 as today;
  otherwise the marker delete runs (same `tableExists` check) and the route answers
  `{ removed: true, work_id }`. Any error rolls both back and answers 500 as today.
- Test, `corpus-sharing.test.js` next to the removal tests (`:338-391`): seed a work in the
  corpus with a `seed_candidates_in_corpus` marker row (the table comes from `seed.js:629`; the
  test creates it with that DDL if the test schema has none), add
  `CREATE TRIGGER fail_marker_delete BEFORE DELETE ON seed_candidates_in_corpus BEGIN SELECT RAISE(ABORT, 'marker delete failed'); END`,
  send the DELETE: 500, and both the membership and the marker are still there. Drop the trigger
  afterwards.

**D8. A versioned file request is checked against the live build once (BE-54), controller
call.**
- **Deployment assumption: one writer.** Only the backend process publishes snapshots, and it
  swaps them with synchronous renames (`app.js:538-548`, `renameSync` at `:540-546`). No other request in that process can
  run between a synchronous check and the next statement.
- A helper `graph3dManifestVersion(key) -> string | null` returns the version
  `decorateGraph3dManifest` puts in `?v=`: `String(manifest.generated_at)` when present, else the
  manifest's `mtimeMs` floored, as today (`app.js:610-619`); `null` when the manifest cannot be
  read. `decorateGraph3dManifest` uses the same rule (it keeps taking `generated_at` from the
  manifest it decorates).
- The file route keeps its middleware order: `requireAuthMiddleware`, then
  `requireGraph3dScopeAccess`, then the handler. An outsider gets 403 before any version check,
  for a matching and for a stale `v` alike.
- In the handler, after the key and file-name checks and before the existence check: when the
  request has a string `v` and `graph3dManifestVersion(key) !== v`, it sets
  `Cache-Control: no-store` and answers
  `409 { error: 'The graph snapshot was rebuilt; reload the graph.', code: 'snapshot_rebuilt' }`.
  Otherwise the route goes on exactly as today: the gzip stream, or `res.sendFile` with its range
  and conditional handling. A request without `v` is unchanged.
- **Accepted residual:** the gzip path's `statSync` and stream open, and `sendFile`'s
  asynchronous open, run after the check. A rebuild that swaps in that gap can still serve one
  file of the new build under the old version. Rebuilds are rare (1 h TTL, or an admin refresh),
  the worst case is one mixed graph until the next load, and closing the gap would need fd
  streaming that drops `sendFile`'s range and conditional responses.
- `generated_at` is an ISO timestamp with microseconds (`graph_3d_export.py:817`), and a build
  takes tens of seconds, so two builds never share a version.
- **Frontend: one load, one owner.** `load3DGraph` (`ThreeGraph.svelte:1215-1260`):
  - takes `const load = ++graphLoadSeq` and captures its options once
    (`{ maxNodes, relationship, groupBy, corpusId: selectedGraphCorpusId }`);
  - `owns() = componentMounted && load === graphLoadSeq`, checked after the manifest, before the
    resources, before a retry, after the retry's manifest, and before it publishes `graphData`
    or `graphStatus`; a load that no longer owns returns without a request or a write;
  - retries once: when the resources reject with `error.status === 409` and it still owns, it
    fetches the manifest again with the captured options and loads that manifest's resources; a
    second failure is shown like any other error;
  - its `catch` writes the status only while it owns, and its `finally` clears `graphLoading`
    only when `load === graphLoadSeq`.

  A sign-out unmounts the graph tab, so a late 409 never starts a request under the next
  session's token (`requestJson` reads the token at call time, `api.js:41`).
- Tests:
  - `graph-read-path.test.js`, a hand-written snapshot dir as in the cluster test (`:327-340`),
    manifest `generated_at: 'A'`, `nodes.bin` and its `.gz`:
    - `nodes.bin?v=A` answers 200 with the bytes (gzip path);
    - with `Accept-Encoding: identity`, `?v=A` answers 200 through `sendFile`: octet-stream
      type, length 4, and a `Range: bytes=0-1` request answers 206;
    - `?v=B` answers 409 with the body above and `Cache-Control: no-store`;
    - no `v` answers 200.
  - `graph-access.test.js`: the lonely user gets 403 for `clusters.json` of `k1` with
    `?v=gen-<k1>` (matching) and with `?v=stale`.
  - The build-streaming test (`:307-325`) still passes.
  - `graph-3d.spec.ts`:
    - manifest 1 lists resource URLs with `?v=A`, manifest 2 with `?v=B`; `nodes.bin?v=A`
      answers 409. The manifest is requested twice, every resource request after the 409 carries
      `v=B`, and the graph status shows the loaded node count.
    - A 409 after unmount: hold `nodes.bin?v=A`; switch to the workspace tab (the graph
      unmounts); release with 409 and wait for the checkpoint: the manifest was requested once
      and no request carries `v=B`.

**D9. BE-38 is closed: sync writes stay, controller call.**
- Measured on this machine (Node 22, 20,000 lines of 150 bytes in a loop): today's `existsSync` +
  `statSync` + `appendFileSync` costs about 26 µs a line; a held fd with `fstatSync` +
  `writeSync` about 8 µs. At a busy 100 lines/s the difference is under 0.2 % of the event loop.
  This is the steady-state cost only: the loop does not measure stalls during a rotation (five
  renames) or under bursts.
- Tracking the size in memory is wrong for `backend-pipeline-v2.log`: the two workers append to
  it, so the backend would rotate long past the limit. A per-line `stat` is what counts their
  bytes.
- A held fd would need its own reopen after rotation and loses lines if the file is removed
  under it (the workers stat the path for that, `worker.py:115-122`). Buffered async writes lose
  the last lines before a crash, including the error that caused it, and change the order of
  backend lines against the workers' appends.
- The gain is a few microseconds a line with no stall ever seen; each variant adds a failure mode
  to the one shared log. No code change; the row is closed in the inventory.

### Python

**D10. The backfill writes only rows still in the state it read (PY-34), controller call.**
- Rows carry the raw identifier they were read with: pass 1 rows become
  `(work_id, token, raw_openalex_id)`, and `doi_rows` returns `(work_id, key, raw_doi)` triples;
  its annotation becomes `tuple[list[tuple[int, str, str]], int]`
  (`test_backfill_helpers_carry_the_specified_annotations`, `test_doi_rule_scripts.py:157-165`,
  changes with it).
- `write_results` takes the pass's guard and runs one `UPDATE` per row inside the batch's
  transaction (at most 50). Each `UPDATE` repeats in one statement what the pass selected the
  row for:
  - pass 1: `WHERE id = ? AND openalex_id = ? AND (topics_fetched IS NULL OR topics_fetched = 0)`;
  - pass 2: `WHERE id = ? AND doi = ? AND (openalex_id IS NULL OR openalex_id = '') AND (topics_fetched IS NULL OR topics_fetched = 0)`.

  A DOI row that gained an OpenAlex id while its request was held is therefore not written, and
  the next pass 1 looks it up by that id. The guard is chosen by the pass (`"openalex"` or
  `"doi"`), never by input. `write_results` returns the ids it changed; `matched` and `fielded`
  count only those.
- **`mark_fetched` goes.** Every result `run_pass` resolves to a row is appended to `updates` as
  well as to `seen_work_ids` (`backfill_openalex_topics.py:204-215`), and `write_results` already
  sets `topics_fetched = 1`. The second write only repeated it, and unguarded it would mark a row
  that the guard skipped as fetched without topics.
- Rows the guard skipped keep `topics_fetched` as it was. The pass logs
  `[<label>] <n> work(s) changed while the batch was fetched; left for the next run` when `n > 0`.
- Tests, `test_doi_rule_scripts.py`, with `_fake_openalex`; the fake request changes the row
  through a second connection before it answers:
  - pass 2, `doi` changed: row 1 keeps `(None, None, None)`, row 2 of the same batch gets its
    topics, and the log line says 1;
  - pass 2, only `openalex_id` set (to `W9`): row 1 keeps `topics_fetched` `None`;
  - pass 1, `openalex_id` changed: the same;
  - the existing pass-2 tests pass unchanged.

**D11. The worker raises on a failed claim, and a job whose failure cannot be recorded is
recovered by a restart (PY-54), controller call.**
- `claim_enrich_batch` gets `raise_errors: bool = False`, as `claim_download_batch` has: on a
  `sqlite3.Error` it rolls back and re-raises when set, else answers `[]`. The docstring says so.
- The worker passes `raise_errors=True` to both claims: `do_enrich` (`worker.py:1249`) and
  `do_download` (`:1596`). The download claim joins because it is the same defect in the same
  loop.
- Effects through existing paths:
  - a job (`enrich`, `download`, `pipeline_tick`) is marked `failed` with the traceback and an
    error heartbeat (`_process_job`'s `except`, `:1785-1803`), instead of `completed` with zero
    counts; the user can run it again;
  - a direct cycle's error goes to `_run_direct_cycles` (`:1817-1857`): the other role's cycle
    still runs, and with nothing processed the main loop logs it, sends its error heartbeat and
    waits 10 s (`:1907-1913`).
- **When the terminal write fails too.** A lock long enough to fail the claim (30 s
  `busy_timeout`) can also fail `mark_job_failed`. Today that exception leaves the `except`, the
  `finally` stops the job's heartbeat and watchdog, and the job stays `running` under this live
  worker, which periodic recovery never reclaims (`_recover_stale_running_jobs(periodic=True)` → `_recover_dead_owners_only`, `:743-763`, `:887`). The
  fix uses the existing restart path:
  - `_process_job` wraps `mark_job_failed` in `try`/`except sqlite3.Error`. On that error it logs
    `Job <id>: the failure could not be recorded (<error>); exiting with code 75 so start-up recovery requeues it.`,
    still sends the error heartbeat (which handles its own errors), and after the `finally` calls
    `self._exit(JOB_TIMEOUT_EXIT_CODE)`.
  - Docker's restart policy brings the worker back, and start-up recovery
    (`_recover_stale_running_jobs()` in `__init__`, `:298`; the predecessor rule, `:778-781`)
    sets the job back to `pending` and releases the dead worker's work claims. The job runs again
    rather than ending `failed`: the same outcome as any other worker restart.
  - The watchdog's own failure path is unchanged.
- Other callers keep `[]`: the default is unchanged, and the "daemon's contract" assertion of
  `test_a_failed_claim_is_not_an_empty_queue` (`test_cli_download_leases.py:1248-1258`) still
  holds for the default.
- Tests:
  - `db_manager`: with `_deny_next_begin` (`test_cli_download_leases.py:1236-1245`),
    `claim_enrich_batch` answers `[]` by default and raises `sqlite3.Error` with
    `raise_errors=True`, leaving no open transaction;
  - worker (`daemon._exit = exits.append`): an `enrich`, a `download` and a `pipeline_tick` job
    whose claim is denied end `failed`, with the SQLite message in the error, and `exits == []`;
  - both failures injected: the claim is denied and `mark_job_failed` raises
    `sqlite3.OperationalError('database is locked')`. The job is still `running`, the log line
    above was written, and `exits == [75]`. A new daemon of the same role on the same DB (the
    restart) finds the job `pending`;
  - `_run_direct_cycles` for role `enrich` raises `RuntimeError("enrich cycle failed: …")`.

**D12. The scraper uses the shared key reader (PY-27, DRY half), controller call.**
- `OpenAlexCrossrefSearcher.__init__` sets `self.api_key = get_openalex_api_key()` and, when it
  is `None`, calls `utils._warn_missing_openalex_api_key()`. `_missing_api_key_warned` and the
  scraper's own warning text are removed. Both are imported from `.utils`; the helper keeps its
  name (it is package-internal).
- Behaviour changes:
  - a key with surrounding whitespace is stripped;
  - a blank or whitespace-only `OPENALEX_API_KEY` counts as unset, so the reader falls back to
    `RAG_FEEDER_OPENALEX_API_KEY` (`utils.py:260-262`); today the scraper sends the blank value;
  - the missing-key warning prints once per process instead of twice.

  The worker's settings refresh (`test_worker_settings.py:98-103`) still sees the new key,
  because the reader runs at construction as before.
- Tests, `test_hygiene.py` next to `:118`:
  - with `OPENALEX_API_KEY="  sekret-key-123\n"` the searcher's `api_key` is `sekret-key-123`;
  - with `OPENALEX_API_KEY="   "` and `RAG_FEEDER_OPENALEX_API_KEY="fallback-key"` it is
    `fallback-key`;
  - with both variables unset and both module flags reset, building a searcher and then calling
    `openalex_request_json` prints the `[WARN] OPENALEX_API_KEY not set` line exactly once
    (twice on `c574aba`). The request uses a stub session whose response has `status_code` 200,
    `headers` `{}`, `json()` and `raise_for_status()`, and `RAG_FEEDER_OPENALEX_QUOTA_PATH`
    points into `tmp_path`;
  - `git grep -n "getenv('OPENALEX_API_KEY')" -- dl_lit_project/dl_lit` finds nothing.

## Order of work

Five tasks, each one reviewable commit or a small group of them.
- **Browser runs are serialized.** Tasks 1, 2 and 3 each run Playwright on the same port in the
  same worktree, and Playwright reuses a server it finds there, so these three tasks run one
  after another (1, 2, 3). Task 3 must follow Task 2 anyway: both edit `backend/src/app.js`
  (different routes).
- Tasks 4 and 5 (Python) touch disjoint files and can run beside them.
- Each task runs only its own specs; the whole CI Playwright list runs once, at the final
  verification.

1. **Frontend session races:** D1, D2, D4 (`App.svelte`; `seed-selection.spec.ts`,
   `session.spec.ts`, `tab-resources.spec.ts`, `seed-upload.spec.ts`, `helpers/mockApi.ts` with
   `logIn` and `afterLateAnswer`).
2. **Graph consistency:** D8 (file route, `decorateGraph3dManifest`) and D3, plus D8's load
   ownership and retry (`ThreeGraph.svelte`; `graph-read-path.test.js`, `graph-access.test.js`,
   `graph-3d.spec.ts`).
3. **Backend transaction and dev config:** D7 (`app.js`, `corpus-sharing.test.js`) and D6
   (`vite.config.js`, new `dev-server-config.spec.ts`, `ci.yml`).
4. **Python claims:** D11 (`db_manager.py`, `worker.py`, worker and claim tests).
5. **Python backfill and key:** D10 and D12 (`backfill_openalex_topics.py`, `OpenAlexScraper.py`,
   `test_doi_rule_scripts.py`, `test_hygiene.py`).

**Setup, once, before Task 1.** The worktree has no `node_modules`: `cd backend && npm ci`,
`cd frontend && npm ci`, then `cd frontend && npx playwright install chromium`.

D5 and D9 have no task: the controller marks FE-22 and BE-38 closed in the inventory's
reconciliation section with a pointer to this spec.

## Acceptance

- Every change has a test that fails on `c574aba` and passes after its task. Each commit names
  the rows it closes.
- Python: `cd dl_lit_project && /home/jay/DT/dl_lit_project/.venv/bin/python -m pytest tests -q`
  is green.
- Backend: `cd backend && npm test` is green. Suites can time out under the full parallel load
  on this machine; compare with a `c574aba` baseline or use `npm test -- --maxWorkers=2` before
  blaming a task.
- Frontend: `cd frontend && npx vite build` succeeds. The whole CI Playwright list (`ci.yml:54`,
  now with `dev-server-config.spec.ts`) passes once at the end with
  `E2E_FRONTEND_PORT=5190 npx playwright test <specs> --workers=1` (check first that 5190 is
  free, e.g. `ss -ltn`; any free port works). Never the default 5175: that is the dev container,
  which serves master's code, not this branch. No live spec is run.
- `git grep -n "usePolling: true" -- frontend/vite.config.js` finds nothing.
- Never against `dl_lit_project/data/literature.db` or any other dev or prod DB; tests use temp
  DBs only.
- After merge: the dev container still polls
  (`docker exec rag_feeder_frontend printenv CHOKIDAR_USEPOLLING` prints `1`), and
  `docker logs rag_feeder_frontend` shows Vite's restart after the config change with no error.

## Controller calls (for the owner)

- **D1 (FE-60b):** the session guard goes into all four download handlers, not only the seed
  candidate one; a corpus switch does not cancel a download; the export lock is session-wide (one
  export at a time per session, kept across corpus switches) and is reset on sign-out only.
- **D2 (FE-63):** a search launch is dropped once the frontend commits a sign-out or corpus
  switch; a launch answered between the server's save and the `/me` answer applies its effects
  until the commit resets them; the run goes on server side.
- **D5 (FE-22):** closed, not a bug: once `/me` shows a role change, the next cycle at the latest
  re-arms with it; a tab change re-arms itself.
- **D6 (FE-24):** the config sets `usePolling: false`; chokidar's own `CHOKIDAR_USEPOLLING`
  override keeps the containers polling (compose sets `1`), so the host stops polling and the
  containers keep their cost.
- **D8 (BE-54):** single-writer assumption; a versioned file request is compared once with the
  live build's version and answers 409 (no-store) on a mismatch; otherwise the route is unchanged
  (`sendFile` keeps range and conditional responses); requests without `v` are unchanged; the
  scope check stays first. Residual: a swap between the check and the file open can still serve
  one new file under the old version (rare, fixed by the next load). The frontend owns each load
  once and retries once, never after unmount. One build per key stays on disk.
- **D9 (BE-38):** closed, won't fix: per-line sync writes cost about 26 µs in steady state, the
  per-line stat is needed for the workers' bytes, and every faster variant adds a failure mode to
  the shared log.
- **D10 (PY-34):** each write repeats the pass's selection (identifier, no OpenAlex id for pass
  2, unfetched) in one `UPDATE`; a changed row is left for the next run; `mark_fetched` goes;
  `doi_rows` returns triples, so a phase 7 annotation test changes.
- **D11 (PY-54):** the worker raises on a failed claim for both enrich and download; such a job
  now ends `failed` instead of `completed` with zero counts. When the failure cannot be recorded
  either, the worker exits with 75, and start-up recovery requeues the job (it runs again). Other
  callers keep `[]`.
- **D12 (PY-27):** the scraper strips the key, treats a blank one as unset (falling back to
  `RAG_FEEDER_OPENALEX_API_KEY`), and the warning prints once per process.
