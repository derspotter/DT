# Review fixes, phase 9: backend/frontend DRY cleanup plus the rows parked in phase 8 — design

Date: 2026-11-24, revised the same day after the Codex pre-flight round 1
(`.superpowers/sdd/2026-11-24-review-fixes-9-dry-cleanup/preflight.md`) and the controller's
rulings on it. Branch `review-fixes-9` (worktree from master `d4518a9`).
Source: the reconciliation of the review inventory against `c574aba`
(`/home/jay/DT/.worktrees/review-fixes-8/.superpowers/sdd/review-fixes-8-reconcile/{fe,be,py}.md`)
and the section "Reconciliation against master `c574aba` (2026-11-24)" at the end of
`/home/jay/DT/.superpowers/sdd/2026-11-13-review-fixes-5-remaining/inventory.md`, which lists the
rows parked in phase 8 and the owner decisions. Rows: FE-11, FE-16, FE-23 (remainder), BE-24, BE-27,
BE-28, BE-29, BE-30, BE-31, BE-33, BE-34, PY-32, and the parked FE-65b, FE-66, FE-67, PY-55, PY-57,
PY-58, PY-59. Same method as phases 2 to 8: Opus-medium implementers under subagent-driven
development, Codex as the review gate, plus the Claude review pass while Copilot is out of quota.

The owner's principle: secure and fast, not over-engineered; the smallest change that removes the
cost or the risk. Everyone keeps access to their own corpora.

Every row was re-read on `d4518a9`; the line numbers below are from that commit. A DRY row does not
change behaviour, error texts or status codes unless its decision says so. Where the copies differ
today, the decision says which behaviour wins and marks it **controller call**. One row is closed
without a code change (PY-32, controller ruling). Pre-flight 1 added one new row, FE-68 (a staged
upload can be imported or extracted by another user), fixed here with FE-65b.

## Problems (verified against master `d4518a9`)

### Frontend

1. **The sign-in tail is copied three times (FE-11).** `handleLogin` (`App.svelte:1729-1796`),
   `handleAcceptInvite` (`:1818-1891`) and `handleResetPassword` (`:1893-1966`) each run the same
   steps after `fetchMe`: set `authUser`, `corpora`, `currentCorpusId`, `authStatus`, `authFlow`,
   `authFlowToken`, then `loadRecursionConfig`, `refreshAll`, `connectLogs` and
   `setActiveTab('workspace', …)` with a `lostSession()` check between them. The invite and reset
   handlers are identical apart from the API call and three texts.
2. **Six copies of the download link (FE-16).** `document.createElement('a')` … `click()` …
   `remove()` sits in `handleExportDownload` (`App.svelte:4335`), `handleDownloadedCorpusFile`
   (`:4360`), `handleDownloadSeedDocument` (`:4434`), `handleSeedCandidateFile` (`:4459`),
   `ScraperLab.svelte:191` and `UpstreamBrowser.svelte:125`. Four save a blob and revoke its object
   URL right after the click; two navigate to a ticket URL (`UpstreamBrowser` sets
   `rel = 'noopener'`, `handleDownloadedCorpusFile` does not).
3. **The two worker views are built inline (FE-23 remainder).** `loadPipelineWorkerStatus`
   (`App.svelte:4222-4239`) builds `pipelineWorker` and `downloadWorker` from the same six
   heartbeat fields; `downloadWorker` adds two backlog counts. The error-text half was fixed in
   phase 7 (`workerErrorText`, `:4202-4207`).
4. **An import still lands in the corpus the server holds at import time (FE-65b).**
   `POST /api/ingest/import-seed` (`app.js:5955`) runs `requireAuthMiddleware,
   requireCorpusWriteAccess` and imports into `req.corpusId`, the user's saved corpus. `importSeed`
   (`lib/api.js:282-289`) sends only `{ filename, kind }`. Phase 8 (FE-65) drops the import once the
   frontend commits a corpus switch (`uploadItem`, `App.svelte:2721-2777`), but between the server
   saving the selection and the frontend's `/me` commit an upload that reaches `importSeed` imports
   into the new corpus. Phase 7 closed the same gap for keyword search with `bindSubmittedCorpus`
   (`app.js:2961-2973`, routes `:4899` and `:4922`). Extraction has the same gap:
   `POST /api/extract-bibliography/:filename` (`app.js:4470`) uses the saved corpus, and the
   upload rows of the old corpus, with their **Extract** button (`App.svelte:5115-5117`), stay on
   screen until `/me` commits the switch.
4b. **A staged upload is not bound to its uploader (FE-68, new in pre-flight 1).** Multer stores
   every upload in one shared directory as `<Date.now()>-<random 1e9>-<original name>`
   (`app.js:4409-4421`); `/api/process_pdf` (`:4426`) records nothing about who uploaded it. The
   import (`:5955-5967`) and the extraction (`:4470-4502`) accept any name in that directory. Names
   are not secret: they reach the corpus-scoped log lines (`[file=…]`, `:4517`) and the seed labels
   and source keys every member of that corpus reads. A member (or a former member) who knows a
   name can import or extract another user's staged file into a corpus they can write.
5. **A create or delete pending from the last session locks the next session's corpus buttons
   (FE-66).** `creatingCorpus` and `deletingCorpus` (`App.svelte:547-548`) are cleared only in the
   handlers' `finally` (`:2400`, `:2452`). After a sign-out the epoch checks drop the late answer,
   but both buttons (`UpstreamBrowser.svelte:233`, `:240`) stay disabled for the next user until the
   old request settles. `resetFrontendState`'s sign-out branch (`:2296-2299`) clears `exportBusy`
   for the same reason, not these two.
6. **A failed switch in the graph's own Corpus picker leaves the picker on the failed corpus
   (FE-67).** The picker (`ThreeGraph.svelte:1900`) binds `selectedGraphCorpusId` and calls
   `load3DGraph` on change. When that load fails, the `catch` (`:1312-1315`) writes the error, the
   previous corpus's graph stays on screen, and the picker names the corpus that failed. The
   comment at `:77-81` makes this deliberate for the recolour path; it still misleads.

### Backend

7. **`['pdf', 'search'].includes(…)` is copied eight times (BE-24):** `app.js:5061`, `:5135`,
   `:5198`, `:5460`, `:5484`; `seed.js:824`, `:1157`, `:1169`.
8. **Four routes open a second, read-only connection per request (BE-27):** the upstream file
   route (`app.js:3855`), the seed candidate file route (`:5157`), the corpus file route
   (`:5631`) and the graph node detail (`:5843`). The ticket mint routes next to them already read
   through `authDb` (`:3813`, `:5595`). Two of the four close the handle only on success
   (`:3855`, `:5631`).
9. **Stub responses are inline `process.env` checks (BE-28).** Seven handlers test
   `process.env.RAG_FEEDER_STUB === '1'` (`app.js:4904`, `:4925`, `:5496`, `:5703`, `:5733`,
   `:5798`, `:5837`) next to the `isStubMode()` helper (`:190-192`) that 21 other places use. The
   export stub alone is 23 lines inside the handler (`:5733-5756`).
10. **The 500 handler is copied 35 times (BE-29).** 47 `status(500)` in `app.js`; 35 of them are
    `console.error('[<route>] Error:', error); return res.status(500).json({ error: error?.message || '<text>' })`,
    16 with `error.message` and 19 with `error?.message`. The other 12 differ on purpose (below).
    The copied pattern also leaks: `error.message` carries SQLite text and paths, and a failed
    Python child's whole stderr or stdout (`pythonProcess.js:194-195`,
    `Python script failed (<code>): <stderr || stdout>`), which routes such as `/api/exports`
    (`app.js:5792-5794`) return to any corpus member. Only the log tail already answers members
    with a generic text (`:6377-6379`).
11. **Two download-ticket stores repeat mint, prune and expiry (BE-30).** `corpusDownloadTickets`
    and `upstreamDownloadTickets` (`app.js:2824-2825`), two prune functions (`:2864-2871`,
    `:2903-2910`), two mint blocks (`:3828-3835`, `:5607-5614`), both with a literal `60_000`.
12. **Two expansion builders and one dead arg builder (BE-31).** `buildKeywordSearchExpansion`
    (`app.js:2219-2243`) and `buildUploadedDocsExpansion` (`:2388-2420`) differ only in their
    defaults and in the extra `relatedDepth` field of the second. `applyUploadedDocsExpansionArgs`
    (`:2422-2443`) has no caller.
13. **Helpers are defined several times, and the two OpenAlex-id normalizers disagree (BE-33).**
    `tableExists` in `app.js:339`, `seed.js:4`, `pipelineReads.js:1`; column probes
    `listTableColumns` (`app.js:361`), `tableHasColumn` (`seed.js:9`), `hasColumn`
    (`pipelineReads.js:10`); JSON parsers `safeJsonParse` (`app.js:1327`), `parseJson`
    (`seed.js:14`), `parseJson` (`corpusList.js:89`, a `{ ok, value }` contract); DOI normalizers
    `normalizeGraphDoi` (`app.js:1535`) and `normalizeDoi` (`seed.js:46`), which behave the same;
    OpenAlex normalizers `normalizeGraphOpenAlexId` (`app.js:1528-1533`: the first `W\d+`, upper,
    else `null`) and `normalizeOpenAlexId` (`seed.js:53-59`: an `openalex.org` URL is kept whole and
    uppercased, anything else without a `W` id is uppercased as it is).
14. **`tableExists` runs on request paths for tables that exist (BE-34).** About 50 calls in `app.js`,
    `seed.js` and `pipelineReads.js`, each a `prepare` plus a `sqlite_master` lookup; most name
    tables `createApp` creates at boot (`ensureAuthSchema`, `app.js:735-945`; `ensureSeedSchema`,
    `seed.js:607-645`).

### Python

15. **The two backfill scripts repeat a skeleton (PY-32).** `backfill_citation_edges.py:41-75`
    and `backfill_openalex_topics.py:55-60`, `:107-121`, `:236-251`: `log`, the four common
    arguments, the batch clamp, the connection setup and the add-missing-columns loop.
16. **Promotion writes the seed marker after the membership commit (PY-55).** The membership is
    committed first (`db_manager.add_corpus_item`, `db_manager.py:1131-1139`, through
    `_handle_existing`, `add_entry_to_download_queue` or `create_pending_work`), then
    `_mark_candidate_in_corpus` (`seed_promote.py:118-146`) commits the marker on its own. A
    removal (`app.js:5518`, `:5553`, one transaction since phase 8) that commits in between deletes
    the membership and the then-absent marker; the marker written after it shows the candidate as
    in the corpus for a work that is not.
17. **Rows that share a lookup token are not written (PY-57).** `run_pass`
    (`backfill_openalex_topics.py:198`) maps each token to one row
    (`token_to_row = {token: (work_id, raw) …}`). Two rows with the same token (`W1` and
    `https://openalex.org/W1`; two raw DOIs with one DB key) keep only the last; the others are
    never written or marked fetched, so every run selects and requests them again.
18. **A finished scraper run is replayed after a lost job end (PY-58).** When a scraper job's end
    cannot be recorded (`worker.py:1845-1852`), the job stays `running`; start-up recovery requeues
    it (`_recover_stale_running_jobs`, `:747-816`) and `execute_scraper_job`
    (`scraper_lab.py:371-390`) sets the run back to `running` and scrapes again, adding a second
    set of `scraper_artifacts` rows.
19. **The bibliography CLI never fetches citing works (PY-59).** `process_single_file`
    (`OpenAlexScraper.py:975-1004`) reads `enriched_ref['best_match_result']`, which the producer
    never sets; `_build_enrichment_payload` (`:1012-1048`) returns `openalex_id` and
    `cited_by_api_url` at the top level. So `--fetch-citations`, the default of
    `dl_lit enrich-openalex` (`cli.py:859-862`), does nothing.

## Goals

- One copy of each helper named above, with the call sites moved to it, and no change to what a
  user or a client sees unless a decision says so.
- A seed import or an extraction runs in the corpus the user submitted it from, with that
  corpus's write check, and only on a file that user staged (an admin may use any).
- A 500 answers a non-admin with the route's own text; the detail goes to the server log, and to
  admins.
- A sign-out never leaves the next user's corpus buttons disabled.
- The graph's Corpus picker always names the graph on screen.
- A seed marker is written only while its work is in the corpus.
- Every candidate row of a backfill batch is written or left, never silently skipped forever.
- A completed scraper run is never scraped again.
- `dl_lit enrich-openalex --fetch-citations` fetches citing works.
- Each bug fix has a test that fails on `d4518a9`; each DRY change is pinned by tests that pass
  before and after.

## Non-goals

- **Phase 10 (performance):** BE-03, BE-11 residual, BE-35, BE-47, PY-07, PY-08, PY-28.
- **Phase 11 (structure):** BE-23 (schema module and `user_version`), FE-10, PY-30, PY-23.
- Still parked, with the reasons recorded in phase 8 and the reconcile files:
  - BE-39 (structured refresh events): it changes the worker/frontend contract, and the owner has
    not asked for it yet;
  - BE-52 (two test PDFs in dev corpus 182): it can only be checked by opening the dev DB, which no
    phase does, so it waits for the owner;
  - PY-33 (wall-clock rate windows): an earlier spec accepted them because the containers run
    UTC; only a clock step by NTP remains;
  - FE-17 remainder (lazy-load `UpstreamBrowser`): it needs a refactor of its `bind:` props, worth
    it only if the owner wants a smaller main chunk.
- **BE-28 builds no stub router** (D9). The 21 `isStubMode()` checks in the auth and access
  middleware stay.
- **BE-29 keeps the route tags, fallback texts and status codes** (D10); only who sees the detail
  changes.
- **FE-68 does not rename staged files.** Crypto-random names would not bind a file to its
  uploader, because the names reach corpus members through the logs and seed labels (D4).
- **BE-33 leaves `corpusList.js`'s `parseJson`** (its `{ ok, value }` result tells a JSON `null`
  from a parse failure, as the Python port needs) and `ensureColumn` (a writer).
- **PY-58 does not make an interrupted run's replay idempotent.** A run cut off mid-way is still
  marked failed by recovery and scraped again (K-I1); the artifacts it wrote before the cut stay
  next to the new ones.

## Decisions

### Frontend

**D1. One sign-in tail, one set-password handler (FE-11).**
- `async function openSignedInSession(me, lostSession)` in `App.svelte`, next to `enterSession`:
  it applies `me` (`authUser`, `corpora`, `currentCorpusId` with today's fallback chain),
  sets `authStatus`, `authFlow`, `authFlowToken`, then runs `loadRecursionConfig`, `refreshAll`,
  `connectLogs` and the guarded `setActiveTab`, returning early when `lostSession()` is true after
  each await. Errors propagate to the caller's `catch`, as they do today.
- Each caller keeps its own check right after `fetchMe` (login: `epoch !== sessionEpoch`, silent;
  invite and reset: `lostSession()`, which recovers the saved password), so no message changes.
- `async function submitNewPassword(event, { form, request, missingTokenText, failureText })`
  holds the invite/reset body. `handleAcceptInvite` calls it with
  `{ form: 'acceptInvite', request: acceptInvite, missingTokenText: 'Invite token is missing.', failureText: 'Failed to accept invite' }`;
  `handleResetPassword` with `resetPassword`, `'Reset token is missing.'`,
  `'Failed to reset password'`. The two names stay, because the markup binds them.
- Tests: the existing `session.spec.ts` sign-in, invite and reset cases (`:572-700`, `:1124`,
  `:1255`, `:1408`, `:1580`) pass unchanged. New, parametrized over both links in `session.spec.ts`:
  a missing token, a short password and a mismatch show their texts; a `400 { error: 'Link expired' }`
  answer shows `Link expired` and leaves the form up.

**D2. One download helper (FE-16), controller call.**
- New `frontend/src/lib/download.js`:
  - `saveBlob(blob, filename)`: object URL, link with `download = filename`, click, remove, and
    `URL.revokeObjectURL` in a `finally`;
  - `openDownloadUrl(url)`: link with `href = url`, click, remove.
  - Both set `rel = 'noopener'` (today only `UpstreamBrowser` does; on a same-origin link without
    `target` it changes nothing).
- The six call sites use them; each keeps its status texts and its session guard from phase 8.
  `ScraperLab` passes `filename || 'metadata.bib'` as today.
- Behaviour: none visible. The `finally` revokes the URL even when `click()` throws (today only
  `handleDownloadSeedDocument` does).
- Tests: the existing download specs (`seed-selection.spec.ts` candidate file, `session.spec.ts`
  and `tab-resources.spec.ts` export) pass unchanged. New `frontend/tests/download-helper.spec.ts`
  (added to the CI list): with `/api/**` answered 401 so the app boots to the sign-in form,
  `page.evaluate` imports `/src/lib/download.js` from the dev server, calls `saveBlob` and
  `openDownloadUrl` (to a routed URL answering `Content-Disposition: attachment`), and the
  `download` events carry the expected suggested filenames.

**D3. One worker-view builder (FE-23 remainder).**
- `function workerView(previous, entry)` next to `workerErrorText` returns
  `{ ...previous, running, in_flight, started_at, last_tick_at, last_result, last_error }` with
  today's expressions. `downloadWorker` is
  `{ ...workerView(downloadWorker, download), queued_download_items, running_jobs }`.
- Test, `tab-resources.spec.ts` next to the FE-23 test (`:855`): a heartbeat fixture with an
  enrich worker `running` and a download worker in `error`; `window.__dtRefresh.workers()`
  (`App.svelte:4864-4868`) returns both views with exactly today's fields.

**D4. Import and extraction take the submitted corpus and only the caller's own staged file
(FE-65b, FE-68), controller call.**
- **Submitted corpus.** Both routes become
  `requireAuthMiddleware, bindSubmittedCorpus, requireCorpusWriteAccess, requireOwnStagedUpload(…)`.
  `bindSubmittedCorpus` is unchanged: no `corpusId` (or `null`) keeps the saved corpus; an invalid
  one answers `400 { error: 'Invalid corpusId' }`; a valid one sets `req.corpusId` and its role, and
  `requireCorpusWriteAccess` answers 403 for a corpus the caller cannot write (viewer, foreign).
  Owners and editors use any corpus they hold. Its comment names the two routes.
- **Staged-file owner, stored in the DB.** Nothing records an upload today: `ingest_entries` and
  `ingest_source_metadata` are written by the extraction and the import, keyed by corpus, not by
  uploader. So `ensureAuthSchema` gets
  `CREATE TABLE IF NOT EXISTS staged_uploads (filename TEXT PRIMARY KEY, user_id INTEGER NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP)`.
  `/api/process_pdf` inserts `(req.file.filename, req.user.id)` after multer has stored the file;
  if the insert fails it removes the file and answers its 500 as today.
- `requireOwnStagedUpload(getFilename, notFoundError)` in `createApp` (`getFilename` is
  `(req) => req.body?.filename` for the import and `(req) => req.params.filename` for the
  extraction): stub mode and the admin pass; otherwise a row for the name with the caller's `user_id` must exist, else
  `404 { error: notFoundError }`, with no Python call. 404, not 403, so it does not confirm that
  another user's file exists. The texts are the routes' own missing-file texts:
  `'Uploaded file not found. Please re-upload.'` (import) and
  `'Uploaded PDF not found (and no extracted pages exist). Please re-upload the PDF.'`
  (extraction). The lookup is one indexed read.
- **Rejected:** an in-memory map, which a restart or deploy empties, refusing every file staged
  before it (the upload rows come back from `localStorage` with their Extract button);
  crypto-random names, which do not bind anything, because the names reach the members of the
  corpus through the logs and the seed labels.
- **Accepted effects:** files staged before the deploy have no row, so their import or extraction
  answers 404 once and the user uploads again. Rows are never pruned, like the staged files
  themselves.
- Frontend: `importSeed(filename, kind, { corpusId } = {})` and
  `extractBibliography(filename, { corpusId } = {})` send `{ corpusId }` when it is a number.
  `uploadItem` takes `const corpusId = currentCorpusId` next to its `session`/`scope` capture;
  `extractForItem` takes it at the click. Phase 8's drop after a committed switch stays.
- Tests:
  - new `backend/tests/staged-uploads.test.js`, with the corpora, roles and argv-logging fake
    Python of `keyword-search-corpus.test.js` and a temp `RAG_FEEDER_UPLOADS_DIR`. Two editors,
    alice and bob, both editors of corpus `a`; bob also owns `b`:
    - alice uploads through `/api/process_pdf`; bob's import and extraction of her file into `b`
      (writable for bob) answer 404 with no Python call (on `d4518a9`: 200 and 202); alice's own
      import and the admin's import of her file succeed;
    - alice's import with `corpusId: a` while her saved corpus is `b` runs with `--corpus-id a`;
      without `corpusId` it runs in `b`; `viewed` and `foreign` answer 403 and `'x'` answers 400,
      with no Python call;
    - alice's extraction with `corpusId: a` while her saved corpus is `b` starts under `a` (the
      `[extract-status][corpus=a]` line through the `broadcast` the test passes to `createApp`);
  - `seed-upload.spec.ts`, the gap: corpora 1 and 2; hold `/api/process_pdf`; select corpus 2
    with its `/me` held; release the upload: the import body carries `corpusId: 1`. And for a PDF
    row: select corpus 2 with `/me` held, click Extract on the row still on screen: the extraction
    body carries `corpusId: 1`. On `d4518a9` neither body has a `corpusId`.

**D5. The corpus button locks are the session's (FE-66).**
- As D1 of phase 8 did for `exportBusy`: `handleCreateCorpus` and `handleDeleteCorpus` clear
  their flag in `finally` only when `epoch === sessionEpoch`, and `resetFrontendState`'s sign-out
  branch sets `creatingCorpus = false` and `deletingCorpus = false`. A corpus switch keeps them.
- Tests, the parametrized `session.spec.ts` test at `:1934-1993`:
  - after bob's sign-in and before alice's release, `New corpus` is enabled (and `Delete corpus`
    for the deletion case, bob owning his corpus). On `d4518a9` both are disabled;
  - the stale unlock (a reset at sign-out alone would pass the first check): bob reopens Korpus
    Management and starts the same action, held at its own gate (a second request to the same
    path, told apart by its bearer token). Then alice's request is released and passes
    `afterBodyRead`: bob's buttons are still disabled. They are enabled once bob's request
    settles.

**D6. A failed graph corpus switch puts the picker back (FE-67), controller call.**
- `ThreeGraph.svelte` gets `let loadedCorpusId = null`, set together with `loadedScope`
  (`:1309`). In the `catch`, while the load owns, when `loadedCorpusId !== null` and
  `options.corpusId !== loadedCorpusId`, it sets `selectedGraphCorpusId = loadedCorpusId`. This is
  the header picker's rule (`handleSelectCorpus` puts the select back, `App.svelte:2366`).
- **The error survives a recolour.** With the picker back, the controls match `loadedScope`
  again, and `onGroupModeChange` (`:1241-1242`) would bring back the loaded status. So the
  rollback also sets `let keepLoadError = true`; `onGroupModeChange` restores the loaded status
  only while it is `false`; `load3DGraph` sets it `false` when it starts. The error stays until
  the next load.
- **Scope of the guarantee:** manifest, resource and retry failures, which all happen before the
  new data is published. A failure in `renderGraph()` comes after `loadedScope` and
  `loadedCorpusId` already name the new corpus, so nothing is put back there: the picker names
  the corpus whose data was published.
- With no graph loaded yet, nothing is put back: the picker names the corpus that failed and the
  canvas is empty, which is consistent. The comment at `:77-81` is updated.
- Rejected: clearing the old graph on failure (loses a working view for a transient error).
- Tests, `graph-3d.spec.ts`: the existing test "a recolour after a failed corpus load keeps the
  corpus error" (`:794-811`) passes, and also asserts that the picker's value is `1` after the
  failure and after the recolour (on `d4518a9`: `2`). New: the same with the manifest for corpus 2
  answering `500`, not its `nodes.bin`.

### Backend

**D7. Shared DB helpers and one identifier rule (BE-33, BE-34, BE-24), controller call.**
- New `backend/src/dbHelpers.js`:
  - `tableExists(db, name)`: a positive answer is remembered per handle
    (`WeakMap<Database, Set<string>>`); a negative one is asked again each time, because the
    Python side creates `search_runs`, `search_results`, `ingest_entries`, `work_aliases` and
    `citation_edges` after the backend starts.
  - `tableHasColumn(db, table, column)` and `listTableColumns(db, table)` (today's `app.js`
    rule: `[]` for a missing table). Columns are not remembered: scripts add columns at run time.
  - `parseJson(value, fallback = null)` with `seed.js`'s rule. For the string and `NULL` column
    values `safeJsonParse` receives (`app.js:1421`, `:1447`) both give the same result.
- `app.js`, `seed.js`, `pipelineReads.js` and `corpusList.js` import these; their copies go, and
  `hasColumn` becomes `tableHasColumn`. `pipeline-reads.test.js` imports `tableExists` from the new
  module. The redundant second `tableExists` in `loadMatchRows` (`seed.js:137`) goes.
- **Accepted residual (BE-34):** a table dropped under a running backend still counts as present
  until restart, so a later query answers 500 instead of an empty result. No code in
  `backend/src`, `backend/scripts` or `dl_lit` drops a table; the test that drops one
  (`search-run-failure.test.js:240`) expects the failure.
- New `backend/src/identifiers.js` with `normalizeOpenAlexId(value)` and `normalizeDoi(value)`.
  `normalizeDoi` is `seed.js`'s (the graph copy gives the same result for every input).
  `normalizeOpenAlexId` takes the graph rule: the first `W\d+` anywhere, uppercased, else `null`.
  That is the Python `works.openalex_id` rule (`identifiers.normalize_openalex_id`,
  `identifiers.py:84-92`), so the key matches what the workers store.
- **What the normalizer change moves.** The graph keeps its keys. Seed matching changes in two
  cases:
  - an `openalex.org` URL: `https://openalex.org/W12` keys as `W12` instead of
    `HTTPS://OPENALEX.ORG/W12`. A search candidate falls back to the raw OpenAlex `id` (a URL) when
    `search_results.openalex_id` is empty (`seed.js:586`), and stored `works.openalex_id` values are
    bare. Such a candidate now matches its work by OpenAlex id and can show `downloaded`, `queued`
    or `added` instead of `pending`. A match in another corpus the caller can read shows
    `downloaded_elsewhere`, with a `downloaded_work_id`, and its Download action is enabled
    (`readableDownloadedIds`, `seed.js:369-373`; `resolveState`, `:389-395`). A match the caller
    cannot read is filtered out as today and leaves the candidate `pending`. A legacy work stored
    as a URL now matches a bare candidate;
  - a value without a `W` id no longer keys at all; today two equal junk values match each other.
  Nothing is stored: the keys live in per-request sets and in the in-memory downloaded-works cache
  (`seed.js:299-329`), rebuilt at start-up; `candidate_key` comes from the row id (`seed.js:493`,
  `:569`), and the candidate's own `openalex_id` field is passed through unchanged. Promotion
  sends the raw value to Python, which normalizes it as before.
- `seed.js` exports `isSeedSourceType(value)` (`SEED_SOURCE_TYPES = new Set(['pdf', 'search'])`)
  and the eight sites use it. Each site keeps its own trimming and lowercasing.
- Tests:
  - new `backend/tests/db-helpers.test.js`: `tableExists` on an in-memory DB is `false`, then
    `true` after a `CREATE TABLE`; a second positive call does not `prepare` (a counting wrapper
    around the handle); `listTableColumns` of a missing table is `[]`; `parseJson` of `null`,
    `''`, `'{"a":1}'`, `'null'` and `'{bad'`;
  - new `backend/tests/identifiers.test.js`: `normalizeOpenAlexId` of `'W12'`, `'w12'`,
    `'https://openalex.org/W12'`, `'https://api.openalex.org/works/W12'`, `'A123'`, `''` and
    `null` (the Python cases of `test_identifiers.py`); `normalizeDoi` of the graph route's cases;
  - `seed-state.test.js`: a search candidate whose `raw_json.id` is `https://openalex.org/W12` and
    whose `openalex_id` is empty, against a downloaded corpus work `W12` with a different title,
    resolves to `downloaded` (on `d4518a9`: `pending`). The same candidate against a downloaded
    `W12` in another corpus: with a `canReadWork` that allows it, `downloaded_elsewhere` and that
    work's id as `downloaded_work_id` (on `d4518a9`: `pending`); with one that refuses it,
    `pending` and no `downloaded_work_id`, on both commits;
  - the existing seed, graph and pipeline-read tests pass unchanged.

**D8. File routes read through `authDb` (BE-27).**
- The four routes drop `new Database(DB_PATH, { readonly: true })` and its `close()` and run the
  same statements on `authDb`. The SELECT text, the read rule inside it and the answers stay.
  The node route's `listTableColumns(authDb, 'works')` uses D7's helper.
- There is no gain in a second handle: better-sqlite3 is synchronous, so it adds no concurrency,
  and each statement reads one snapshot either way. The leaked handle on a throw (`:3855`,
  `:5631`) goes with it.
- Tests: the existing route tests pass unchanged (`scraper-draft.test.js:174-187` upstream file,
  `seed-read-path.test.js:493-562` candidate file, `access-revalidation.test.js:183-200` corpus
  file, `graph-access.test.js` node detail).

**D9. Stub mode: one check, the bodies next to the stub data (BE-28), controller call.**
- The seven handler checks become `if (isStubMode())`. The export stub body moves to
  `sendStubExport(res, format)`, the corpus page to `stubCorpusPage(stage)` and the node lookup to
  `stubGraphNode(nodeId)`, next to `STUB_RESULTS` and `stubGraph3dManifest` (`app.js:270-660`).
  Each handler keeps one line, at the same place in its order.
- **No stub router.** It would repeat seven paths and their middleware chains and move the stub
  answer ahead of the handlers' validation; the stub tests rely on that order
  (`exports.test.js:69`, `:74`, `keyword-search.test.js:18` expect 400 in stub mode). It would also
  leave the 21 middleware checks, which are most of the stub surface.
- Tests: the stub suites (`corpus`, `downloads`, `exports`, `graph`, `keyword-search`,
  `topic-search`) pass unchanged; `git grep -n "RAG_FEEDER_STUB === '1'" -- backend/src/app.js`
  finds only `isStubMode`.

**D10. One 500 helper; the detail goes to admins only (BE-29), controller ruling.**
- In `createApp` (it needs `authConfig`):
  `const sendServerError = (req, res, route, error, fallback) => …` logs
  `` `[${route}] Error:` `` with the whole error, as today, and answers
  `res.status(500).json({ error: (isAdminUser(req.user, authConfig) && error?.message) || fallback })`.
  The 35 regular sites call it with their current route tag and fallback text.
- **The rule is the log tail's** (`app.js:6377-6379`): members get the route's text; the admin,
  who can read the server log anyway, keeps the detail. The full error always goes to the log.
- **Which copy wins for the admin:** `error?.message`. The two forms differ only when a
  non-object is thrown: today the 16 `error.message` sites then throw inside their `catch`, and
  the request gets Express 5's default error answer (HTML) instead of the JSON 500.
- **User-visible change:** a non-admin who hits a 500 on one of the 35 routes reads the route's
  fallback (for example `Failed to export corpus data`, `Failed to load seed candidates`,
  `Failed to remove work from corpus`) instead of the SQLite message, a path or a Python child's
  output. Stub mode's user is not the admin, so it gets the fallback too. Admins see no change.
  Route tags, fallback texts and status codes stay. Keyword search's mapped errors
  (`keywordSearchErrorResponse`, 429/400/502) are not 500 handlers and are unchanged.
- The 12 irregular sites keep their shape: the two sign-in failures (`:3389`, `:3420`, fixed
  text), the two invitation routes (`:3572`, `:3611`, admin-only, so the detail stays), the
  search registration (`:4960`, `:4985`), the export without output (`:5787`), the manifest
  read (`:5909`), the stream error (`:5944`) and the log tail (`:6378-6379`, already this rule).
  The upload (`:4455`) is the one irregular site that leaks to members: its `details` field is
  sent only to the admin.
- Tests:
  - new `backend/tests/server-errors.test.js`, with a fake Python that writes
    `SECRET-DIAG /srv/private/path` to stderr and exits 3, an admin and a corpus member:
    `GET /api/exports?format=json` answers the member `500 { error: 'Failed to export corpus data' }`
    with no `SECRET-DIAG` anywhere in the body, the admin a body whose `error` contains
    `SECRET-DIAG`, and `console.error` (spied) got `'[/api/exports] Error:'` with an error whose
    message contains `SECRET-DIAG` both times. On `d4518a9` the member's body contains it;
  - phase 8's trigger test in `corpus-sharing.test.js` (`:400`, run as the admin) also asserts
    the body `{ error: 'marker delete failed' }`; an editor of the corpus (not the admin) on the
    same trigger gets `{ error: 'Failed to remove work from corpus' }`;
  - the existing 500 tests pass unchanged; the ones that read a detail (`auth.test.js:251`,
    `:259`, `corpus-sharing.test.js:408`, `access-routes.test.js:617`) run as the admin.

**D11. One ticket store (BE-30).**
- New `backend/src/downloadTickets.js`:
  `createTicketStore({ ttlMs = 60_000, now = Date.now } = {})` returns
  `{ ttlSeconds, mint(fields) → token, get(token) → ticket | null, revoke(token) }`. `mint` and
  `get` prune expired tickets first, as today; tokens stay `crypto.randomBytes(24)` in
  `base64url`; a ticket stays reusable until it expires, as today.
- `createApp` builds `corpusDownloadTickets` and `upstreamDownloadTickets` from it. The two access
  middlewares (`app.js:2872-2902`, `:2911-2941`) keep their own checks, texts and stub handling;
  the mint routes answer `expires_in_seconds: store.ttlSeconds` (60).
- Tests: new `backend/tests/download-tickets.test.js` with an injected clock (valid before 60 s,
  `null` after, pruned on the next `mint`, `revoke`); the existing ticket route tests pass
  unchanged.

**D12. One expansion builder; the dead arg builder goes (BE-31).**
- `function buildExpansion(body, defaults)` holds the seven shared fields of today's builders
  (`relatedDepthDownstream`, `relatedDepthUpstream`, `maxRelated`, `relatedSort`,
  `promotionMode`, `includeDownstream`, `includeUpstream`).
  The keyword path calls `buildExpansion(req.body, RECURSION_DEFAULTS.keyword)` (the pre-coercion
  at `app.js:2357-2366` repeats what `coerceInt` does inside, so passing the body is the same).
  `buildUploadedDocsExpansion(body)` becomes
  `{ relatedDepth: coercePositiveInt(coerceInt(body?.relatedDepth, null), RECURSION_DEFAULTS.uploadedDocs.relatedDepth, 0), ...buildExpansion(body, RECURSION_DEFAULTS.uploadedDocs) }`,
  so the job payload keeps its eight fields.
- `applyUploadedDocsExpansionArgs` is deleted. `applyKeywordSearchExpansionArgs` is unchanged;
  the two argv shapes differ because the scripts' flags differ.
- Tests: `recursion-args.test.js` passes unchanged; new there: the enrich job's
  `parameters_json.expansion` `toEqual`s the full eight-field object for one body with every field
  set and for an empty body (the defaults).

### Python

**D13. PY-32 is closed, won't fix (controller ruling).**
- The common part is about 20 lines (`log`, four arguments, the clamp, the connection, the
  column loop). The scope rules, the columns and the loops differ on purpose. A shared module
  removes those 20 lines and adds a third file of about 35 that two hand-run scripts and the test
  loader (`_script` in `test_doi_rule_scripts.py:14-25`) then depend on. No change; the row is
  closed in the inventory.

**D14. The seed marker is written only while the work is a member (PY-55), controller call.**
- `_mark_candidate_in_corpus(db, corpus_id, candidate, work_id: int)`: the work id becomes
  required (every production caller passes one). It runs its upsert as
  `INSERT INTO seed_candidates_in_corpus (…) SELECT ?, ?, ?, ?, ?, CURRENT_TIMESTAMP WHERE EXISTS (SELECT 1 FROM corpus_works WHERE corpus_id = ? AND work_id = ?) ON CONFLICT(…) DO UPDATE SET work_id = excluded.work_id, marked_at = CURRENT_TIMESTAMP`.
  One statement under SQLite's write lock: a removal committed before it leaves no marker; a
  removal after it deletes the marker in its own transaction. Without the `NULL` branch and the
  `COALESCE`, the call can no longer refresh an old marker whose work left the corpus.
- Every caller already linked the work before marking it (`seed_promote.py:290`, `:303`, `:338`,
  `:369`, `:397`, `:422`). A link that failed silently (`_add_existing_to_corpus` swallows errors,
  `:150-155`) now leaves no marker either, which is the truth.
- Rejected: checking membership when the seed list reads the markers. It would also hide markers
  left by the race before this fix, but it adds a lookup to every list load and leaves the stale
  rows in place.
- Tests, new `dl_lit_project/tests/test_seed_promote_marker.py` (loads the script as
  `test_doi_rule_scripts.py` does): a member work gets its marker; a work removed from the corpus
  before the call gets none (on `d4518a9` it does); an existing marker of a member is refreshed;
  an old marker of a non-member is not refreshed (its `marked_at` is unchanged). Atomicity: with
  `conn.set_trace_callback` recording the SQL, the one `INSERT` the call runs contains the
  `EXISTS (SELECT 1 FROM corpus_works` predicate, and no separate `SELECT` on `corpus_works`
  precedes it (a check-then-write would pass the outcome tests).

**D15. Every row of a shared token is written (PY-57).**
- `run_pass` builds `token_to_rows: dict[str, list[tuple[int, str]]]` and requests each token once
  (`dict.fromkeys` keeps the order). Every row of a returned token gets an update with its own raw
  value, so the D10 guard of phase 8 still checks each row against what it was read with.
- Tests, `test_doi_rule_scripts.py` with `_fake_openalex`: pass 1 rows `W1` and
  `https://openalex.org/W1` both get the topics and `topics_fetched = 1`, the filter lists `W1`
  once, and `matched` counts 2; pass 2 rows `10.1000/abc` and `10.1000/ABC` the same. On
  `d4518a9` one row of each pair stays unfetched.

**D16. A completed scraper run is not scraped again (PY-58), controller call.**
- `execute_scraper_job` gets an outer `try`/`finally` right after `sqlite3.connect`, so the
  connection is closed on every path. Inside it, after the run is loaded: when
  `status == 'completed'` it returns `{"run_id", "source_type", "total", "downloaded", "failed"}`
  from the stored counts, plus `"replayed": True`, and writes nothing. The replayed job then ends
  `completed` with that result. The existing `try`/`except` that marks the run `failed`
  (`scraper_lab.py:392-472`) stays around the scrape only; the unknown-run and unknown-source
  errors keep raising before it, as today.
- Safe because every scrape creates a new run (`app.js:3882-3913`); nothing runs a completed
  run on purpose. One check in the runner covers start-up recovery, periodic recovery and any
  later replay path. Start-up recovery is unchanged; `_fail_scraper_runs_of` touches only
  `running` runs.
- Tests, `test_scraper_lab.py`: a `completed` run with counts 3/2/1 and one artifact; with
  `scrape_eurlex` patched to raise, the call returns the stored counts and `replayed`, the run row
  is unchanged and the artifact count stays 1 (on `d4518a9` the patched scraper raises and the run
  ends `failed`). A `pending` run still scrapes. With `sqlite3.connect` wrapped to record the
  connections it opens, every one is closed after the completed-run return and after an
  unknown run id.

**D17. The CLI reads the producer's fields; `--fetch-citations` stays on (PY-59), controller
ruling.**
- `process_single_file` reads the citation fields only inside the guard, because
  `process_single_reference` answers `None` for an unmatched reference:
  `if enriched_ref and fetch_citations:` then
  `cited_by_url = citing_works_url(enriched_ref.get('cited_by_api_url'), enriched_ref.get('openalex_id'))`,
  and only `if cited_by_url:` it prints `enriched_ref.get('title')` and fetches. An unmatched
  reference is written back as it was, as today. `citing_works_url` already strips the URL form
  of the id (`normalize_openalex_id_strict`). The output key stays `citing_works_ids`.
- **Behaviour change:** with the default `--fetch-citations`, each matched entry that has a stored
  `cited_by_api_url` now requests its citing works, as the option says; a match without one
  (`citing_works_url` answers `None`) fetches nothing. The web pipeline does not use this path.
- **Cost:** one pure-filter list request per such entry (`filter=cites:<id>`,
  `per-page=100`, `OpenAlexScraper.py:171`; `max_citations` 100, so one page). Under the measured
  rule (`dl_lit/keyword_search.py:191-194`: a filter listing costs 1 credit a page, a text search
  10) that is about 1 credit per matched reference, e.g. about 60 credits for a 60-entry
  bibliography.
- Tests, `test_hygiene.py` (next to the PY-27 tests): one input file with a matched reference
  (`process_single_reference` patched to return a payload with
  `openalex_id: 'https://openalex.org/W7'` and an OpenAlex `cited_by_api_url`) and an unmatched
  one (patched to `None`), and `fetch_citing_work_ids` patched to record its URL. The output file
  holds both entries: the first with `citing_works_ids`, the second unchanged; the fetch ran once,
  with the stored URL. With `fetch_citations=False` nothing is fetched. On `d4518a9` the fetch
  never runs.

## Order of work

Six tasks, each one reviewable commit or a small group of them.
- Tasks 1, 2 and 5 edit `App.svelte` and run Playwright on one port in one worktree, so they run
  one after another. Tasks 3, 4 and 5 edit `backend/src/app.js`, so they run in that order too.
- Tasks 1 and 2 (frontend only) can run beside Tasks 3 and 4 (backend only, Jest). Task 5 waits
  for 2 and 4. Task 6 (Python) can run beside any of them.
- Each task runs only its own suites; the whole CI Playwright list runs once, at the final
  verification.

1. **Frontend DRY:** D1, D2, D3 (`App.svelte`, new `lib/download.js`, `ScraperLab.svelte`,
   `UpstreamBrowser.svelte`; `session.spec.ts`, new `download-helper.spec.ts`,
   `tab-resources.spec.ts`, `ci.yml`).
2. **Frontend small fixes:** D5, D6 (`App.svelte`, `ThreeGraph.svelte`; `session.spec.ts`,
   `graph-3d.spec.ts`).
3. **Backend helpers:** D7 (new `dbHelpers.js` and `identifiers.js`, `seed.js`,
   `pipelineReads.js`, `corpusList.js`, `app.js`; `db-helpers.test.js`, `identifiers.test.js`,
   `seed-state.test.js`, `pipeline-reads.test.js`).
4. **Backend routes:** D8, D9, D10, D11, D12 (`app.js`, new `downloadTickets.js`;
   `download-tickets.test.js`, `recursion-args.test.js`, new `server-errors.test.js`,
   `corpus-sharing.test.js`).
5. **Import and extraction binding:** D4 (`app.js` with `staged_uploads` in `ensureAuthSchema`,
   `lib/api.js`, `App.svelte`; new `staged-uploads.test.js`, `seed-upload.spec.ts`).
6. **Python:** D14, D15, D16, D17 (`seed_promote.py`, `backfill_openalex_topics.py`,
   `scraper_lab.py`, `OpenAlexScraper.py`; `test_seed_promote_marker.py`,
   `test_doi_rule_scripts.py`, `test_scraper_lab.py`, `test_hygiene.py`).

**Setup, once, before Task 1.** The worktree has no `node_modules`: `cd backend && npm ci`,
`cd frontend && npm ci`, then `cd frontend && npx playwright install chromium`.

D13 has no task: the controller marks PY-32 closed in the inventory's reconciliation section with
a pointer to this spec, adds FE-68 there, and records the phase 9 outcome for the other rows.

## Acceptance

- Every bug fix (D4, D5, D6, D10's member text, D14, D15, D16, D17, and D7's seed-matching
  change) has a test that
  fails on `d4518a9` and passes after its task. Every DRY change keeps its existing tests green
  unchanged. Each commit names the rows it closes.
- Python: `cd dl_lit_project && /home/jay/DT/dl_lit_project/.venv/bin/python -m pytest tests -q`
  is green.
- Backend: `cd backend && npm test` is green. Suites can time out under the full parallel load
  on this machine; compare with a `d4518a9` baseline or use `npm test -- --maxWorkers=2` before
  blaming a task.
- Frontend: `cd frontend && npx vite build` succeeds. The whole CI Playwright list
  (`.github/workflows/ci.yml:54`, now with `download-helper.spec.ts`) passes once at the end with
  `E2E_FRONTEND_PORT=5190 npx playwright test <specs> --workers=1` (check first that 5190 is free,
  e.g. `ss -ltn`). Never the default 5175: that is the dev container, which serves master's code.
  No live spec is run.
- Greps, each finding only the line named:
  - `git grep -n "createElement('a')" -- frontend/src`: `lib/download.js`;
  - `git grep -n "RAG_FEEDER_STUB === '1'" -- backend/src/app.js`: `isStubMode`;
  - `git grep -n "new Database(DB_PATH, { readonly" -- backend/src`: nothing;
  - `git grep -n "function tableExists" -- backend/src`: `dbHelpers.js`;
  - `git grep -n "\['pdf', 'search'\]" -- backend/src`: `SEED_SOURCE_TYPES` in `seed.js`;
  - `git grep -n "applyUploadedDocsExpansionArgs\|normalizeGraphOpenAlexId\|safeJsonParse" -- backend/src`:
    nothing;
  - `git grep -n "best_match_result" -- dl_lit_project/dl_lit`: nothing.
- Never against `dl_lit_project/data/literature.db` or any other dev or prod DB; tests use temp
  DBs only.

## Controller calls (for the owner)

- **D2 (FE-16):** both download helpers set `rel = 'noopener'` and always revoke the object URL;
  no visible change.
- **D4 (FE-65b, FE-68, extraction race):** import and extraction both take the submitted corpus
  with its write check. A new `staged_uploads` table binds each staged file to its uploader;
  another user's file answers 404 (the admin may use any). Files staged before the deploy need one
  re-upload. No in-memory map (a restart would refuse every staged file) and no renaming (names
  are visible to corpus members anyway).
- **D6 (FE-67):** a failed graph corpus switch puts the picker back on the corpus on screen and
  keeps the error, through recolours, until the next load; the old graph is not cleared. Only
  manifest, resource and retry failures roll back; a render failure does not.
- **D7 (BE-33):** the seed matcher takes the graph's (and the Python workers') OpenAlex rule.
  An `openalex.org` URL now matches its bare id, so some search candidates can move from
  `pending` to their work's state, including `downloaded_elsewhere` with a working Download for a
  readable work in another corpus (an unreadable one stays hidden); a value without a `W` id no
  longer matches. Nothing stored
  changes. Positive `tableExists` answers are remembered per handle (BE-34); a table dropped under
  a running backend counts as present until restart.
- **D9 (BE-28):** no stub router; one `isStubMode()` check per handler, stub bodies moved next to
  the stub data, validation still first.
- **D10 (BE-29), controller ruling 2026-11-24, owner may override:** a 500 answers non-admins
  with the route's fallback text and admins with `error?.message`; the full error always goes to
  the server log. User-visible: members no longer see SQLite messages, paths or Python output in
  500s. The upload's `details` field goes to admins only. Tags, fallback texts and statuses stay.
- **D13 (PY-32), controller ruling 2026-11-24, owner may override:** closed, won't fix: about 20
  shared lines between two hand-run scripts do not pay for a shared module.
- **D14 (PY-55):** the marker insert checks membership in the same statement and needs a work id;
  markers left by the race before this fix stay.
- **D16 (PY-58):** a completed run returns its stored counts (`replayed: true`) instead of
  scraping again, with the connection closed on every path; an interrupted run's replay is
  unchanged.
- **D17 (PY-59), controller ruling 2026-11-24, owner may override:** `--fetch-citations` stays on
  by default. A matched entry with a stored citing URL now costs one filter list request, about
  1 OpenAlex credit; an unmatched entry is written back unchanged.
- **Parked rows (BE-39, BE-52, PY-33, FE-17 remainder), controller ruling 2026-11-24, owner may
  override:** they stay parked for the reasons given under Non-goals.
