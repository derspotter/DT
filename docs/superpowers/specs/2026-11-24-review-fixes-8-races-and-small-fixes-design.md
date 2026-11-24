# Review fixes, phase 8: races and small fixes — design

Date: 2026-11-24. Branch `review-fixes-8` (worktree from master `c574aba`).
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
- Mocked Playwright runs and CI do not poll the file system; the dev containers still do.
- Removing one work from a corpus is all or nothing.
- A graph load uses files of one build only, or reloads.
- The topic backfill writes only rows whose identifier is still the one it looked up.
- A failed claim in the worker is a failed job or cycle, never an empty one.
- One place reads the OpenAlex key, and the missing-key warning prints once per process.
- Each change has a test that fails on `c574aba`, except D6's, which pins a config value.

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
- **D8 keeps one build per key on disk.** No per-build directories, no grace period for old
  builds.

## Decisions

### Frontend

**D1. A download that settles after a sign-out is dropped (FE-60b), controller call.**
- `handleSeedCandidateFile`, `handleDownloadSeedDocument`, `handleDownloadedCorpusFile` and
  `handleExportDownload` each take `const session = sessionEpoch` before their request. Right
  after the `await`, `if (session !== sessionEpoch) return`: no object URL, no link, no
  navigation, no status. Their `catch` returns on `error?.status === 401 || session !== sessionEpoch`.
- Only the session counts, not the corpus: a file the user asked for under corpus A is still
  theirs after a switch to B.
- `exportBusy` joins `initialWorkspaceState()` (`false`) and the `resetFrontendState`
  destructuring, so a new session's export button is not held by the old export.
  `handleExportDownload`'s `finally` clears it only when `session === sessionEpoch`.
- Tests:
  - `seed-selection.spec.ts` (it has the readable-candidate fixture, `:217-225`): hold the
    `…/candidates/readable/file` route, click the row's Download, log out, sign in as another
    user (the `logIn` pattern of `session.spec.ts`; move `logIn` to `helpers/mockApi.ts` if both
    specs need it), then answer with a PDF and a `Content-Disposition`. After 300 ms: no
    `download` event on the page and no `Downloaded` text.
  - `session.spec.ts`: the same for the corpus export, plus: the export button is enabled for
    the new user while the old export is still held.

**D2. A search launch is dropped when the session or corpus changed (FE-63), controller call.**
- `startSearch`, after its corpus check, takes `const session = sessionEpoch` and
  `const scope = corpusScopeEpoch`, with `const stale = () => session !== sessionEpoch || scope !== corpusScopeEpoch`.
- `if (stale()) return` right after `runKeywordSearch` (before `refreshQuota` and before
  `startedSearchRunIds`), after `loadSeedSources` and after `focusSeedSource`. The `catch`
  returns on `error?.status === 401 || stale()`.
- The corpus switch counts here (unlike D1): the run belongs to corpus A, and marking it running
  in B only polls B for nothing. The run itself goes on server side, as it does today.
- Tests:
  - `session.spec.ts`: the preview answers `{ count: 5, threshold: 100000 }`; hold
    `POST /api/keyword-search`; once it is seen, log out and sign in as bob; answer `202`
    `{ runId: 7, status: 'running' }` (the shape the FE-55 test there uses). After 300 ms: no
    `Fetching in the background` text, and `window.__dtRefresh.reason()` is not `'active'`.
  - `tab-resources.spec.ts` (next to the FE-62 test, `:625`): the same with a corpus switch
    instead of the sign-out.

**D3. Only the newest detail request writes the panel (FE-61).**
- `ThreeGraph.svelte` gets `let nodeDetailSeq = 0`. `loadNodeDetail` takes
  `const seq = ++nodeDetailSeq` first, before the cache check, so a cache hit also supersedes an
  older request.
- The fetched detail is always cached. It is shown only when `seq === nodeDetailSeq` and the node
  is still selected (today's check). The `catch` clears `selectedNodeDetail` and the `finally`
  clears `nodeDetailLoading` only when `seq === nodeDetailSeq`.
- Test, `graph-3d.spec.ts`: hold the detail of `Graph seed work` (node 1); select it through the
  search; select `Referenced work` (node 2, answers at once) and wait for
  `Identifiers: 10.1000/example`; answer node 1 with 500. After 300 ms the identifiers are still
  visible. A second case holds node 2 instead and answers node 1 first: `Loading metadata...`
  stays visible until node 2 answers.

**D4. The upload list is saved best effort (FE-64).**
- `persistStoredUploads` wraps the `window.localStorage.setItem` call (the `window.localStorage`
  access included) in `try`/`catch`. On a failure it logs
  `console.warn('[uploads] could not save the upload list:', error?.message)` once per page load
  (a module flag) and returns. The upload and extraction go on; the list only does not come back
  after a reload. No other caller changes.
- Test, `seed-upload.spec.ts`: an init script replaces `Storage.prototype.setItem` with one that
  throws a `DOMException` named `QuotaExceededError` for keys starting with `rag-feeder:uploads:`
  and calls the original otherwise (the token key keeps working). The BibTeX import of the first
  test (`:68`) then still ends with its success text, not `failed`.

**D5. FE-22 is closed: not a bug, controller call.** Every way the two values change already
re-arms the timer, or the stale cadence ends at the next tick:
- `activeTab` is only assigned in `setActiveTab` (`App.svelte:4729`), which calls
  `scheduleRefresh()` itself (`:4734`), and in `onMount` before sign-in (`:4743`), when
  `authStatus` is not `authenticated`. The sign-in that follows changes `authStatus`, which is on
  the list.
- `isAdmin`/`diagnosticsEnabled` derive from `authUser` (`:120-121`, declared before `:220`, so
  they update first in the same flush). A sign-in changes `authStatus` too. A role change during
  a session arrives through the `/me` poll inside a refresh cycle (`:1476`), and
  `runLiveRefreshCycle`'s `finally` re-arms (`:1587-1593`) with the new value. The corpus
  actions that also set `authUser` (`:2332`, `:2363`, `:2412`) reset `ingestStats`, which is on
  the list. `diagnosticsEnabled` only matters next to a busy worker, and loading the worker
  status changes `pipelineWorker`/`downloadWorker`, which are on the list.
- So the cadence is never stale for longer than one cycle (2 s active, 20 s idle), which a
  dependency would not shorten. No code change; the row is closed in the inventory.

**D6. Polling is an env flag (FE-24), controller call.**
- `vite.config.js`:
  `const usePolling = ['1', 'true', 'yes'].includes(String(process.env.CHOKIDAR_USEPOLLING || '').trim().toLowerCase())`,
  and `watch: { usePolling }`, with a comment that the dev containers set the variable
  (`docker-compose.yml`, `docker-compose.dev.yml`) for the bind mount, and that Playwright and CI
  leave it unset.
- The variable is the one compose already sets, so the containers keep polling with no compose
  change; the genkia.de dev container (`rag_feeder_frontend`, `./frontend:/app` bind mount) sees
  the same watcher after Vite restarts on the config change. Deleting the line alone would work
  today, because chokidar 3 reads the variable itself; the explicit read keeps that true if
  Vite's watcher stops doing so.
- Test, new `frontend/tests/dev-server-config.spec.ts` (no page): it runs
  `node --input-type=module -e "const m = await import('./vite.config.js'); console.log(JSON.stringify(m.default.server.watch))"`
  with `cwd` the frontend dir, once with `CHOKIDAR_USEPOLLING` removed from the env and once each
  with `1`, `true` and `0`, and expects `usePolling` `false`, `true`, `true`, `false`. The spec
  is added to the CI Playwright list (`.github/workflows/ci.yml:54`).

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

**D8. A versioned file request is served from that build or not at all (BE-54), controller call.**
- A helper `graph3dManifestVersion(key)` returns the version `decorateGraph3dManifest` puts in
  `?v=`: `String(manifest.generated_at)` when present, else the manifest's `mtimeMs` floored, as
  today (`app.js:610-619`); `null` when the manifest cannot be read. `decorateGraph3dManifest`
  uses the same rule (it keeps taking `generated_at` from the manifest it decorates).
- The file route, when the request has a `v` query value:
  1. reads the version (V1);
  2. opens the file it will serve with `fs.openSync` (the `.gz` when the client accepts gzip and
     it exists, else the plain file; a missing file is 404 as today);
  3. reads the version again (V2).

  If `V1 !== v` or `V2 !== v`, it closes the fd and answers
  `409 { error: 'The graph snapshot was rebuilt; reload the graph.', code: 'snapshot_rebuilt' }`
  with `Cache-Control: no-store`. A swap renames the whole dir, so with `V1 === V2` the fd
  belongs to that build. Otherwise it streams from the fd
  (`fs.createReadStream(filePath, { fd })`), with `Content-Length` from `fs.fstatSync(fd)` and
  the same headers as today (`Content-Type` by extension on the plain path too, which today goes
  through `res.sendFile`). The client-abort and stream-error handling stays.
- Without `v` (old manifests, other callers) the route answers as today, through the same fd
  path.
- **Frontend:** `load3DGraph` (`ThreeGraph.svelte:1215-1260`) retries once: when
  `fetchGraph3DSnapshotResources` rejects with `error.status === 409`, it fetches the manifest
  again with the same options and loads its resources; a second 409 is shown like any other
  error.
- `generated_at` is an ISO timestamp with microseconds (`graph_3d_export.py:817`), and a build
  takes tens of seconds, so two builds never share a version.
- Tests:
  - `graph-read-path.test.js`, a hand-written snapshot dir as in the cluster test (`:327-340`):
    `nodes.bin?v=A` answers 200 with the bytes; `?v=B` answers 409 with `no-store`; no `v`
    answers 200.
  - Same file: `jest.spyOn(fs, 'openSync')` calls through and then rewrites `manifest.json` with
    `generated_at: 'B'` when the opened path is that `nodes.bin`; `?v=A` answers 409.
  - The build-streaming test (`:307-325`) still passes: the manifest is the only file read with
    `readFileSync`.
  - `graph-3d.spec.ts`: the first `nodes.bin` request answers 409; the manifest is requested
    twice and the graph status shows the loaded node count.

**D9. BE-38 is closed: sync writes stay, controller call.**
- Measured on this machine (Node 22, 20,000 lines of 150 bytes): today's `existsSync` +
  `statSync` + `appendFileSync` costs about 26 µs a line; a held fd with `fstatSync` +
  `writeSync` about 8 µs. At a busy 100 lines/s the difference is under 0.2 % of the event loop.
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

**D10. The backfill writes only rows whose identifier is unchanged (PY-34), controller call.**
- Rows carry the raw identifier they were read with: pass 1 rows become
  `(work_id, token, raw_openalex_id)`, and `doi_rows` returns `(work_id, key, raw_doi)` triples;
  its annotation becomes `tuple[list[tuple[int, str, str]], int]`
  (`test_backfill_helpers_carry_the_specified_annotations`, `test_doi_rule_scripts.py:157-165`,
  changes with it).
- `run_pass` takes `guard_column` (`"openalex_id"` for pass 1, `"doi"` for pass 2; fixed
  strings, never user input). `write_results` and `mark_fetched` add `AND <guard_column> = ?`
  with the raw value. `write_results` runs one `UPDATE` per row inside the batch's transaction
  (at most 50) and returns the ids it changed; `matched` and `fielded` count only those.
- Rows whose write changed nothing keep `topics_fetched` as it was, so the next run looks them up
  under their new identifier. The pass logs
  `[<label>] <n> work(s) changed while the batch was fetched; left for the next run` when `n > 0`.
- Tests, `test_doi_rule_scripts.py`, with `_fake_openalex`:
  - pass 2: the fake request changes row 1's `doi` through a second connection before it
    answers; row 1 keeps `(None, None, None)`, row 2 of the same batch gets its topics, and the
    log line says 1;
  - pass 1: the same with `openalex_id`;
  - the existing pass-2 tests pass unchanged.

**D11. The worker raises on a failed claim (PY-54), controller call.**
- `claim_enrich_batch` gets `raise_errors: bool = False`, as `claim_download_batch` has: on a
  `sqlite3.Error` it rolls back and re-raises when set, else answers `[]`. The docstring says so.
- The worker passes `raise_errors=True` to both claims: `do_enrich` (`worker.py:1249`) and
  `do_download` (`:1596`). The download claim joins because it is the same defect in the same
  loop.
- Effects, all through existing paths:
  - a job (`enrich`, `download`, `pipeline_tick`) is marked `failed` with the traceback and an
    error heartbeat (`_process_job`'s `except`, `:1785-1803`), instead of `completed` with zero
    counts; the user can run it again;
  - a direct cycle's error goes to `_run_direct_cycles` (`:1817-1857`): the other role's cycle
    still runs, and with nothing processed the main loop logs it, sends its error heartbeat and
    waits 10 s (`:1907-1913`).
- Other callers keep `[]`: the default is unchanged, and the "daemon's contract" assertion of
  `test_a_failed_claim_is_not_an_empty_queue` (`test_cli_download_leases.py:1248-1258`) still
  holds for the default.
- Tests:
  - `db_manager`: with `_deny_next_begin` (`test_cli_download_leases.py:1236-1245`),
    `claim_enrich_batch` answers `[]` by default and raises `sqlite3.Error` with
    `raise_errors=True`, leaving no open transaction;
  - worker, using the `_daemon` fixture of `test_worker_settings.py`: an `enrich` job whose
    claim is denied ends `failed`, with the SQLite message in its error, not `completed`; the
    same for a `download` job; `_run_direct_cycles` for role `enrich` raises
    `RuntimeError("enrich cycle failed: …")`.

**D12. The scraper uses the shared key reader (PY-27, DRY half), controller call.**
- `OpenAlexCrossrefSearcher.__init__` sets `self.api_key = get_openalex_api_key()` and, when it
  is `None`, calls `utils._warn_missing_openalex_api_key()`. `_missing_api_key_warned` and the
  scraper's own warning text are removed. Both are imported from `.utils`; the helper keeps its
  name (it is package-internal).
- Behaviour changes: a key with surrounding whitespace is stripped; a whitespace-only key counts
  as unset; the missing-key warning prints once per process instead of up to twice. The worker's
  settings refresh (`test_worker_settings.py:98-103`) still sees the new key, because the reader
  runs at construction as before.
- Tests, `test_hygiene.py` next to `:118`:
  - with `OPENALEX_API_KEY="  sekret-key-123\n"` the searcher's `api_key` is `sekret-key-123`;
  - with both key variables unset and the two module flags reset, building a searcher and then
    calling `openalex_request_json` (request faked) prints the `[WARN] OPENALEX_API_KEY not set`
    line exactly once;
  - `git grep -n "getenv('OPENALEX_API_KEY')" -- dl_lit_project/dl_lit` finds nothing.

## Order of work

Five tasks, each one reviewable commit or a small group of them. Tasks 1, 4 and 5 touch disjoint
files and can run in parallel with the others; task 3 runs after task 2, because both edit
`backend/src/app.js` (different routes).
1. **Frontend session races:** D1, D2, D4 (`App.svelte`; `seed-selection.spec.ts`,
   `session.spec.ts`, `tab-resources.spec.ts`, `seed-upload.spec.ts`, and `helpers/mockApi.ts`
   if `logIn` moves).
2. **Graph consistency:** D8 (file route, `decorateGraph3dManifest`) and D3, plus D8's retry
   (`ThreeGraph.svelte`; `graph-read-path.test.js`, `graph-3d.spec.ts`).
3. **Backend transaction and dev config:** D7 (`app.js`, `corpus-sharing.test.js`) and D6
   (`vite.config.js`, new `dev-server-config.spec.ts`, `ci.yml`).
4. **Python claims:** D11 (`db_manager.py`, `worker.py`, new or existing worker and claim tests).
5. **Python backfill and key:** D10 and D12 (`backfill_openalex_topics.py`, `OpenAlexScraper.py`,
   `test_doi_rule_scripts.py`, `test_hygiene.py`).

D5 and D9 have no task: the controller marks FE-22 and BE-38 closed in the inventory's
reconciliation section with a pointer to this spec.

## Acceptance

- Every change has a test that fails on `c574aba` and passes after its task, except D6's config
  spec. Each commit names the rows it closes.
- Python: `cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests -q` is green.
- Backend: `cd backend && npm test` is green. Suites can time out under the full parallel load
  on this machine; compare with a `c574aba` baseline or use `npm test -- --maxWorkers=2` before
  blaming a task.
- Frontend: `cd frontend && npx vite build` succeeds. The specs this phase touches and the whole
  CI Playwright list (`ci.yml:54`, now with `dev-server-config.spec.ts`) pass with
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
  candidate one; a corpus switch does not cancel a download; `exportBusy` is reset with the
  workspace, so a new session can export while the old export is still running.
- **D2 (FE-63):** a search launch is also dropped from view on a corpus switch, not only on a
  sign-out; the run goes on server side.
- **D5 (FE-22):** closed, not a bug: every change of the two values already re-arms the timer or
  is corrected at the next cycle.
- **D6 (FE-24):** the flag is the existing `CHOKIDAR_USEPOLLING`, read explicitly, default off;
  compose already sets it for the containers, so nothing changes there.
- **D8 (BE-54):** a file request whose `v` no longer matches the build answers 409 (no-store),
  and the frontend reloads the graph once; requests without `v` behave as before. One build per
  key stays on disk.
- **D9 (BE-38):** closed, won't fix: per-line sync writes cost about 26 µs, the per-line stat is
  needed for the workers' bytes, and every faster variant adds a failure mode to the shared log.
- **D10 (PY-34):** a write is guarded on the identifier the lookup used; a changed row is left
  unfetched for the next run; `doi_rows` returns triples, so a phase 7 annotation test changes.
- **D11 (PY-54):** the worker raises on a failed claim for both enrich and download; such a job
  now ends `failed` instead of `completed` with zero counts. Other callers keep `[]`.
- **D12 (PY-27):** the scraper strips the key and treats a blank one as unset; the warning
  prints once per process.
