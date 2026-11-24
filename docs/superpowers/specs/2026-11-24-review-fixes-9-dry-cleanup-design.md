# Review fixes, phase 9: backend/frontend DRY cleanup plus the rows parked in phase 8 — design

Date: 2026-11-24, revised the same day after pre-flight 1 and pre-flight 2 (Codex,
`.superpowers/sdd/2026-11-24-review-fixes-9-dry-cleanup/preflight.md` and `preflight-2.md`) and
the controller's rulings on them. Branch `review-fixes-9` (worktree from master `d4518a9`).
Source: the reconciliation of the review inventory against `c574aba`
(`/home/jay/DT/.worktrees/review-fixes-8/.superpowers/sdd/review-fixes-8-reconcile/{fe,be,py}.md`)
and the section "Reconciliation against master `c574aba` (2026-11-24)" at the end of
`/home/jay/DT/.superpowers/sdd/2026-11-13-review-fixes-5-remaining/inventory.md`. Rows: FE-11,
FE-16, FE-23 (remainder), BE-24, BE-27, BE-28, BE-29, BE-30, BE-31, BE-33, BE-34, PY-32, the parked
FE-65b, FE-66, FE-67, PY-55, PY-57, PY-58, PY-59, and FE-68 (new in pre-flight 1). Same method as
phases 2 to 8: Opus-medium implementers under subagent-driven development, Codex as the review
gate, plus the Claude review pass while Copilot is out of quota.

The owner's principle: secure and fast, not over-engineered; the smallest change that removes the
cost or the risk. Everyone keeps access to their own corpora.

Every row was re-read on `d4518a9`; the line numbers are from that commit. A DRY row does not
change behaviour, error texts or status codes unless its decision says so. Where copies differ
today, the decision says which behaviour wins. Choices are marked **controller call**, or
**controller ruling** where the controller decided on 2026-11-24 (the owner may override any of
them). PY-32 is closed without a code change.

## Problems (verified against master `d4518a9`)

The evidence for each row is in its decision; this list names the defect.

### Frontend

1. **FE-11:** `handleLogin` (`App.svelte:1729-1796`), `handleAcceptInvite` (`:1818-1891`) and
   `handleResetPassword` (`:1893-1966`) repeat the steps after `fetchMe`; the invite and reset
   handlers are identical apart from the API call and three texts.
2. **FE-16:** six copies of the download link (`App.svelte:4335`, `:4360`, `:4434`, `:4459`,
   `ScraperLab.svelte:191`, `UpstreamBrowser.svelte:125`): four save a blob, two open a ticket URL.
3. **FE-23 remainder:** `loadPipelineWorkerStatus` builds the two worker views inline from the same
   six fields (`App.svelte:4222-4239`).
4. **FE-65b:** `POST /api/ingest/import-seed` (`app.js:5955`) and
   `POST /api/extract-bibliography/:filename` (`:4470`) use the server's saved corpus. Between the
   server saving a corpus switch and the frontend's `/me` commit, an upload's import, or a click on
   **Extract** on an old row still on screen (`App.svelte:5115-5117`), lands in the new corpus.
   Phase 7 closed this gap for keyword search with `bindSubmittedCorpus` (`app.js:2961-2973`).
5. **Late extraction replies (pre-flight 2):** `extractForItem` (`App.svelte:2779-2800`) has no
   epoch check. A reply that arrives after a committed switch starts the old file's progress in
   the new workspace (`ensureExtractionRunActive`, `:992`), or on failure clears the new
   workspace's progress (`finishExtractionProgress`, `:1005`).
6. **FE-68 (new):** staged uploads share one directory (`app.js:4409-4421`) and nothing records the
   uploader. Import and extraction accept any staged name. Names are not secret: corpus members
   see them in the corpus-scoped log lines (`[file=…]`, `:4517`) and in seed labels. Anyone who
   knows a name can import or extract another user's file into a corpus they can write.
7. **FE-66:** `creatingCorpus`/`deletingCorpus` (`App.svelte:547-548`) are cleared only in the
   handlers' `finally` (`:2400`, `:2452`), so a request pending at sign-out locks the next user's
   corpus buttons (`UpstreamBrowser.svelte:233`, `:240`).
8. **FE-67:** after a failed load from the graph's own Corpus picker (`ThreeGraph.svelte:1900`,
   `catch` `:1312-1315`), the picker names the failed corpus while the previous graph stays.

### Backend

9. **BE-24:** `['pdf', 'search'].includes(…)` eight times (`app.js:5061`, `:5135`, `:5198`,
   `:5460`, `:5484`; `seed.js:824`, `:1157`, `:1169`).
10. **BE-27:** four routes open a read-only `new Database(DB_PATH)` per request (`app.js:3855`,
    `:5157`, `:5631`, `:5843`); two leak it on a throw (`:3855`, `:5631`).
11. **BE-28:** seven handlers test `process.env.RAG_FEEDER_STUB === '1'` (`app.js:4904`, `:4925`,
    `:5496`, `:5703`, `:5733`, `:5798`, `:5837`) beside `isStubMode()` (`:190-192`).
12. **BE-29:** 35 copies of `console.error(…); res.status(500).json({ error: error?.message || '…' })`
    (16 with `error.message`). Members receive SQLite messages, paths and a failed Python child's
    whole stderr/stdout (`pythonProcess.js:194-195`). Keyword search does too: its unmapped
    failures answer the raw text (preview `502`, `app.js:4915-4918`; launch `500`, `:4973-4979`).
    Only the log tail hides the detail from members (`:6377-6379`).
13. **BE-30:** two ticket stores with copied mint, prune and expiry (`app.js:2824-2825`,
    `:2864-2871`, `:2903-2910`, `:3828-3835`, `:5607-5614`).
14. **BE-31:** `buildKeywordSearchExpansion` (`app.js:2219-2243`) and `buildUploadedDocsExpansion`
    (`:2388-2420`) differ only in defaults and one field; `applyUploadedDocsExpansionArgs`
    (`:2422-2443`) has no caller.
15. **BE-33:** `tableExists` three times (`app.js:339`, `seed.js:4`, `pipelineReads.js:1`), three
    column probes, two equivalent JSON parsers, two equivalent DOI normalizers, and two OpenAlex-id
    normalizers that disagree (`app.js:1528-1533` vs `seed.js:53-59`).
16. **BE-34:** about 50 `tableExists` calls on request paths, mostly for tables `createApp` creates
    at boot (`app.js:735-945`, `seed.js:607-645`).

### Python

17. **PY-32:** the two backfill scripts repeat about 20 lines of skeleton.
18. **PY-55:** promotion commits the membership (`db_manager.py:1131-1139`), then the seed marker
    separately (`seed_promote.py:118-146`); a removal committed in between leaves a marker for a
    work that is not in the corpus.
19. **PY-57:** `run_pass` maps each token to one row (`backfill_openalex_topics.py:198`); other rows
    with the same token are never written and are fetched again on every run.
20. **PY-58:** a scraper job whose end could not be recorded (`worker.py:1845-1852`) is requeued at
    start-up, and `execute_scraper_job` (`scraper_lab.py:371-390`) scrapes a completed run again,
    duplicating its artifacts.
21. **PY-59:** `process_single_file` (`OpenAlexScraper.py:975-1004`) reads `best_match_result`,
    which nothing sets, so the CLI's default `--fetch-citations` (`cli.py:859-862`) does nothing.

## Goals

- One copy of each helper named above; no change a user or client sees unless a decision says so.
- Import and extraction run in the corpus the user submitted from, with its write check, only on
  a file that user staged (the admin may use any), and a late reply never touches the next
  workspace.
- A 500 answers a non-admin with the route's own text; the detail goes to the log and to admins.
- A sign-out never leaves the next user's corpus buttons disabled.
- The graph's Corpus picker names the graph on screen.
- A seed marker exists only while its work is in the corpus.
- Every row of a backfill batch is written or left; none is skipped forever.
- A completed scraper run is never scraped again.
- `dl_lit enrich-openalex --fetch-citations` fetches citing works.
- Each bug fix has a test that fails on `d4518a9`; each DRY change is pinned by tests that pass
  before and after.

## Non-goals

- **Phase 10 (performance):** BE-03, BE-11 residual, BE-35, BE-47, PY-07, PY-08, PY-28.
- **Phase 11 (structure):** BE-23 (schema module and `user_version`), FE-10, PY-30, PY-23.
- Still parked, for the reasons recorded in phase 8 and the reconcile files:
  - BE-39 (structured refresh events): changes the worker/frontend contract; the owner has not
    asked for it;
  - BE-52 (two test PDFs in dev corpus 182): needs the dev DB, which no phase opens;
  - PY-33 (wall-clock rate windows): accepted earlier because the containers run UTC; only an NTP
    clock step remains;
  - FE-17 remainder (lazy-load `UpstreamBrowser`): needs a `bind:`-props refactor, worth it only
    for a smaller main chunk.
- No stub router (D9); the 21 `isStubMode()` checks in the middleware stay.
- No renaming of staged files (D4).
- `corpusList.js`'s `parseJson` (`{ ok, value }`, which tells a JSON `null` from a parse failure)
  and `ensureColumn` (a writer) stay.
- An interrupted scraper run is still marked failed and scraped again (K-I1); its earlier
  artifacts stay next to the new ones (D16 covers only completed runs).

## Decisions

### Frontend

**D1. One sign-in tail, one set-password handler (FE-11).**
- `async function openSignedInSession(me, lostSession)` next to `enterSession`: applies `me`
  (`authUser`, `corpora`, `currentCorpusId` with today's fallback chain), sets `authStatus`,
  `authFlow`, `authFlowToken`, then runs `loadRecursionConfig`, `refreshAll`, `connectLogs` and the
  guarded `setActiveTab`, returning when `lostSession()` is true after each await. Errors reach
  the caller's `catch`, as today.
- Each caller keeps its own check right after `fetchMe` (login: `epoch !== sessionEpoch`, silent;
  invite and reset: `lostSession()`, which recovers the saved password), so no message changes.
- `async function submitNewPassword(event, { form, request, missingTokenText, failureText })`
  holds the invite/reset body. `handleAcceptInvite` passes
  `{ form: 'acceptInvite', request: acceptInvite, missingTokenText: 'Invite token is missing.', failureText: 'Failed to accept invite' }`;
  `handleResetPassword` passes `resetPassword`, `'Reset token is missing.'`,
  `'Failed to reset password'`. Both names stay; the markup binds them.
- Tests: the existing `session.spec.ts` sign-in, invite and reset cases (`:572-700`, `:1124`,
  `:1255`, `:1408`, `:1580`) pass unchanged. New, parametrized over both links: a missing token, a
  short password and a mismatch show their texts; `400 { error: 'Link expired' }` shows
  `Link expired` and leaves the form up.

**D2. One download helper (FE-16), controller call.**
- New `frontend/src/lib/download.js`: `saveBlob(blob, filename)` (object URL, link with
  `download = filename`, click, remove, `URL.revokeObjectURL` in a `finally`) and
  `openDownloadUrl(url)` (link with `href = url`, click, remove). Both set `rel = 'noopener'` (today
  only `UpstreamBrowser`; on a same-origin link without `target` it changes nothing).
- The six call sites use them and keep their texts and phase 8 session guards. `ScraperLab`
  passes `filename || 'metadata.bib'` as today. The `finally` also revokes when `click()` throws.
- Tests (pre-flight 2: no standalone spec): the blob path is pinned by the existing download specs
  (`seed-selection.spec.ts` candidate file, `session.spec.ts` and `tab-resources.spec.ts:1012`
  export). The ticket path gets one new case in `corpus-load.spec.ts`, through the real caller:
  a downloaded corpus row's stored-file chip (`Corpus.svelte:358-366`) mints a ticket
  (`POST /api/corpus/downloaded/5/download-ticket` answered with
  `/api/corpus/downloaded/5/file?ticket=t`), the routed file answers
  `Content-Disposition: attachment; filename="paper.pdf"`, the download event's suggested name is
  `paper.pdf`, and `Download started.` shows. It passes on `d4518a9` and after.

**D3. One worker-view builder (FE-23 remainder).**
- `function workerView(previous, entry)` next to `workerErrorText` returns
  `{ ...previous, running, in_flight, started_at, last_tick_at, last_result, last_error }` with
  today's expressions; `downloadWorker` is
  `{ ...workerView(downloadWorker, download), queued_download_items, running_jobs }`.
- Test, `tab-resources.spec.ts` next to `:855`: with an enrich worker `running` and a download
  worker in `error`, `window.__dtRefresh.workers()` (`App.svelte:4864-4868`) returns both views
  with exactly today's fields.

**D4. Import and extraction take the submitted corpus and only the caller's own staged file
(FE-65b, FE-68, late replies), controller call.**
- **Corpus first, as middleware.** Both routes become
  `requireAuthMiddleware, bindSubmittedCorpus, requireCorpusWriteAccess, <handler>`.
  `bindSubmittedCorpus` is unchanged: no `corpusId` (or `null`) keeps the saved corpus; an invalid
  one answers `400 { error: 'Invalid corpusId' }`; a valid one sets `req.corpusId` and its role;
  `requireCorpusWriteAccess` answers 403 for a corpus the caller cannot write (viewer, foreign).
  Owners and editors use any corpus they hold. Its comment names the two routes.
- **Then input, then ownership, in the handler** (pre-flight 2: a middleware would check the raw
  name before validation, turn today's 400s into 404s, and miss the import's trimming). Each
  handler runs its validation exactly as today (import: trimmed `filename`, `kind`, separator and
  `..` checks; extraction: separator check and `ensureInsideDirectory`), then
  `if (!ownsStagedUpload(req, filename)) return res.status(404).json({ error: <route's missing-file text> })`
  with the exact `filename` it will use, then its existence checks and the Python call.
  `ownsStagedUpload(req, filename)` in `createApp`: `true` in stub mode and for the admin, else
  `true` only when `staged_uploads` has the name with `req.user.id`. 404, not 403, so it does not
  confirm another user's file. The texts are the routes' own:
  `'Uploaded file not found. Please re-upload.'` (import) and
  `'Uploaded PDF not found (and no extracted pages exist). Please re-upload the PDF.'` (extraction).
- **Storage.** Nothing records an upload today (`ingest_entries` and `ingest_source_metadata` are
  written later, keyed by corpus). `ensureAuthSchema` gets
  `CREATE TABLE IF NOT EXISTS staged_uploads (filename TEXT PRIMARY KEY, user_id INTEGER NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP)`.
  `/api/process_pdf` inserts `(req.file.filename, req.user.id)` after multer stored the file; if
  the insert fails it removes the file and answers its 500. Rejected: an in-memory map (a restart
  or deploy would refuse every staged file, while the upload rows come back from `localStorage`);
  crypto-random names (they bind nothing, since members see the names anyway). Accepted: files
  staged before the deploy answer 404 once and are uploaded again; rows are never pruned, like
  the staged files.
- **Frontend.** `importSeed(filename, kind, { corpusId } = {})` and
  `extractBibliography(filename, { corpusId } = {})` send `{ corpusId }` when it is a number.
  `uploadItem` takes `const corpusId = currentCorpusId` beside its `session`/`scope` capture.
  `extractForItem` takes `corpusId`, `const session = sessionEpoch`, `const scope = corpusScopeEpoch`
  and `stale()` at the click (pre-flight 2). After the reply, `if (stale()) return` before any
  `updateUpload`, `ensureExtractionRunActive` or `apiStatus`; its `catch` returns on `stale()`
  before `finishExtractionProgress`. Phase 8's drop of an import after a committed switch stays.
- Tests:
  - new `backend/tests/staged-uploads.test.js`, with the roles, corpora and argv-logging fake Python
    of `keyword-search-corpus.test.js` and a temp `RAG_FEEDER_UPLOADS_DIR`. Alice is an editor of
    `a` and `b`; the `b` grant is made before `b` is saved as her `last_corpus_id`, because
    `ensureLastCorpus` (`app.js:1261-1271`) replaces a saved corpus she cannot use. Bob is an
    editor of `a` and owns `b`.
    - ownership: alice uploads through `/api/process_pdf`; bob's import and extraction of her file
      into `b` (writable for him) answer 404 with no Python call (on `d4518a9`: 200 and 202);
      alice's own import and the admin's import of her file succeed;
    - input before ownership: an import without `kind`, a name with `/` or `..`, and an extraction
      with a `\` in the name answer 400 as today, also for names nobody staged; a name with
      surrounding spaces is checked as trimmed (alice's own file, sent with spaces, imports);
    - corpus: alice's import with `corpusId: a` while her saved corpus is `b` runs with
      `--corpus-id a`; without `corpusId` it runs in `b`; `viewed` and `foreign` answer 403 and
      `'x'` answers 400, with no Python call;
    - alice's extraction with `corpusId: a` while her saved corpus is `b` starts under `a` (the
      `[extract-status][corpus=a]` line through the `broadcast` the test passes to `createApp`);
  - `seed-upload.spec.ts`, corpora 1 and 2:
    - import gap: hold `/api/process_pdf`; select corpus 2 with its `/me` held; release the upload:
      the import body carries `corpusId: 1` (on `d4518a9`: none);
    - extraction gap: select corpus 2 with `/me` held; click Extract on the old row: the body
      carries `corpusId: 1` (on `d4518a9`: none). The extraction reply is held too;
    - late success: release `/me` (the switch commits), then the held extraction `202 started`, and
      pass `afterBodyRead`: no `.extract-progress` and no `extracting` tag in the new workspace
      (on `d4518a9` both show);
    - late failure: as above, but corpus 2 starts its own extraction (answered at once, its
      `.extract-progress` visible) before the old reply is released with `500`: the progress is
      still visible (on `d4518a9` it is cleared).

**D5. The corpus button locks are the session's (FE-66).**
- As phase 8 did for `exportBusy`: `handleCreateCorpus` and `handleDeleteCorpus` clear their flag
  in `finally` only when `epoch === sessionEpoch`; `resetFrontendState`'s sign-out branch sets
  both to `false`. A corpus switch keeps them.
- Tests, the parametrized `session.spec.ts` test at `:1934-1993`:
  - after bob's sign-in and before alice's release, `New corpus` (and `Delete corpus` for the
    deletion case, bob owning his corpus) is enabled; on `d4518a9` both are disabled;
  - stale unlock: bob reopens Korpus Management and starts the same action, held at its own gate
    (told apart by its bearer token). Alice's request is released and passes `afterBodyRead`:
    bob's buttons are still disabled; they are enabled once his request settles.

**D6. A failed graph corpus switch puts the picker back (FE-67), controller call.**
- `ThreeGraph.svelte` gets `let loadedCorpusId = null`, set together with `loadedScope` (`:1309`).
  In the `catch`, while the load owns, when `loadedCorpusId !== null` and
  `options.corpusId !== loadedCorpusId`, it sets `selectedGraphCorpusId = loadedCorpusId` (the
  header picker's rule, `App.svelte:2366`).
- The error survives a recolour: the rollback sets `keepLoadError = true`; `onGroupModeChange`
  (`:1241-1242`) restores the loaded status only while it is `false`; `load3DGraph` resets it when
  it starts.
- Scope: manifest, resource and retry failures, all before the new data is published. A failure in
  `renderGraph()` comes after `loadedScope` and `loadedCorpusId` name the new corpus, so nothing is
  put back. With no graph loaded yet, nothing is put back either. The comment at `:77-81` is
  updated. Rejected: clearing the old graph on failure.
- Tests, `graph-3d.spec.ts`: the existing "a recolour after a failed corpus load keeps the corpus
  error" (`:794-811`) passes and also asserts the picker is `1` after the failure and after the
  recolour (on `d4518a9`: `2`). New: the same with corpus 2's manifest answering `500`.

### Backend

**D7. Shared DB helpers and one identifier rule (BE-33, BE-34, BE-24), controller call.**
- New `backend/src/dbHelpers.js`:
  - `tableExists(db, name)`: a positive answer is remembered per handle
    (`WeakMap<Database, Set<string>>`); a negative one is asked again, because the Python side
    creates `search_runs`, `search_results`, `ingest_entries`, `work_aliases` and `citation_edges`
    after boot;
  - `tableHasColumn(db, table, column)` and `listTableColumns(db, table)` (`[]` for a missing
    table), not remembered, because scripts add columns at run time;
  - `parseJson(value, fallback = null)` with `seed.js`'s rule; for the strings and `NULL`s
    `safeJsonParse` receives (`app.js:1421`, `:1447`) both agree.
- `app.js`, `seed.js`, `pipelineReads.js` and `corpusList.js` import these; their copies go
  (`hasColumn` becomes `tableHasColumn`; the redundant second check in `loadMatchRows`,
  `seed.js:137`, goes). `pipeline-reads.test.js` imports `tableExists` from the new module.
- Accepted residual (BE-34): a table dropped under a running backend counts as present until
  restart, so a query answers 500 instead of an empty result. Nothing in `backend/src`,
  `backend/scripts` or `dl_lit` drops a table; `search-run-failure.test.js:240` expects the failure.
- New `backend/src/identifiers.js`: `normalizeDoi` is `seed.js`'s (the graph copy agrees for every
  input); `normalizeOpenAlexId` takes the graph rule (the first `W\d+`, uppercased, else `null`),
  which is the Python `works.openalex_id` rule (`identifiers.py:84-92`).
- What the normalizer change moves (seed matching only; the graph keeps its keys):
  - `https://openalex.org/W12` keys as `W12` instead of `HTTPS://OPENALEX.ORG/W12`. A search
    candidate falls back to the raw OpenAlex `id`, a URL, when `search_results.openalex_id` is
    empty (`seed.js:586`), while stored ids are bare. Such a candidate now matches its work and can
    show `downloaded`, `queued` or `added`; a match in another corpus the caller can read shows
    `downloaded_elsewhere` with a `downloaded_work_id` and an enabled Download
    (`seed.js:369-373`, `:389-395`); an unreadable one is filtered out as today and leaves it
    `pending`. A legacy work stored as a URL now matches a bare candidate;
  - a value without a `W` id no longer keys; today two equal junk values match.
  Nothing stored changes: the keys live in per-request sets and the in-memory downloaded-works
  cache (`seed.js:299-329`); `candidate_key` comes from the row id (`:493`, `:569`); the
  candidate's `openalex_id` field and what promotion sends to Python are unchanged.
- `seed.js` exports `isSeedSourceType(value)` (`SEED_SOURCE_TYPES = new Set(['pdf', 'search'])`);
  the eight sites use it and keep their own trimming and lowercasing.
- Tests:
  - new `db-helpers.test.js`: `tableExists` is `false`, then `true` after `CREATE TABLE`; a second
    positive call does not `prepare` (a counting wrapper); `listTableColumns` of a missing table is
    `[]`; `parseJson` of `null`, `''`, `'{"a":1}'`, `'null'`, `'{bad'`;
  - new `identifiers.test.js`: `normalizeOpenAlexId` of `'W12'`, `'w12'`,
    `'https://openalex.org/W12'`, `'https://api.openalex.org/works/W12'`, `'A123'`, `''`, `null`
    (the Python cases); `normalizeDoi` of the graph route's cases;
  - `seed-state.test.js`: a candidate with `raw_json.id` `https://openalex.org/W12` and an empty
    `openalex_id`, against a downloaded corpus work `W12` with another title: `downloaded` (on
    `d4518a9`: `pending`). Against `W12` in another corpus: with a permitting `canReadWork`,
    `downloaded_elsewhere` and that work's id (on `d4518a9`: `pending`); with a refusing one,
    `pending` and no `downloaded_work_id` on both commits;
  - the existing seed, graph and pipeline-read tests pass unchanged.

**D8. File routes read through `authDb` (BE-27).**
- The four routes drop the per-request handle and its `close()` and run the same statements, with
  the same read rules and answers, on `authDb` (the node route through D7's `listTableColumns`).
  better-sqlite3 is synchronous, so a second handle adds no concurrency; each statement reads one
  snapshot either way. The leak on a throw goes with it.
- Tests: the existing route tests pass unchanged (`scraper-draft.test.js:174-187`,
  `seed-read-path.test.js:493-562`, `access-revalidation.test.js:183-200`, `graph-access.test.js`).

**D9. Stub mode: one check, the bodies next to the stub data (BE-28), controller call.**
- The seven checks become `if (isStubMode())`. The export stub body moves to
  `sendStubExport(res, format)`, the corpus page to `stubCorpusPage(stage)` and the node lookup to
  `stubGraphNode(nodeId)`, next to `STUB_RESULTS` (`app.js:270-660`). Each handler keeps one line
  at the same place.
- No stub router: it would repeat seven paths and their middleware chains and put the stub answer
  ahead of the handlers' validation, which the stub tests rely on (`exports.test.js:69`, `:74`,
  `keyword-search.test.js:18` expect 400), and the 21 middleware checks would stay anyway.
- Tests: the stub suites (`corpus`, `downloads`, `exports`, `graph`, `keyword-search`,
  `topic-search`) pass unchanged.

**D10. One 500 rule; the detail goes to admins only (BE-29), controller ruling.**
- In `createApp` (it needs `authConfig`):
  - `const serverErrorText = (req, error, fallback) => (isAdminUser(req.user, authConfig) && error?.message) || fallback`;
  - `const sendServerError = (req, res, route, error, fallback) => …` logs `` `[${route}] Error:` ``
    with the whole error, as today, and answers `500 { error: serverErrorText(req, error, fallback) }`.
  The 35 regular sites call it with their current tag and fallback.
- The rule is the log tail's (`app.js:6377-6379`). For the admin, `error?.message` wins: the 16
  `error.message` copies differ only when a non-object is thrown, where today the `catch` itself
  throws and Express 5 answers its default HTML 500.
- **Keyword search (pre-flight 2).** The mapped answers of `keywordSearchErrorResponse`
  (`app.js:2278-2286`) stay for everyone: 429 with the rate-limit text, 400 with the query-syntax
  text. Only the unmapped case changes: the preview keeps its `502` and the launch its `500`, with
  body `serverErrorText(req, error, 'Keyword search failed')`. Each route logs the full error once:
  the preview's `console.error` already does; the launch's `Search script failed:` line logs the
  error object instead of `error?.message`, also when the response was already sent.
- User-visible: a non-admin hitting a 500 on the 35 routes, or an unmapped keyword-search failure,
  reads the route's fallback (`Failed to export corpus data`, `Keyword search failed`, …) instead of
  SQLite text, paths or Python output. Stub mode's user is not the admin and gets the fallback.
  Admins see no change. Tags, fallbacks and status codes stay.
- The other irregular sites keep their shape: sign-in (`:3389`, `:3420`, fixed text), invitations
  (`:3572`, `:3611`, admin-only), search registration (`:4960`, `:4985`), export without output
  (`:5787`), manifest read (`:5909`), stream error (`:5944`), log tail (`:6378-6379`). The upload
  (`:4455`) sends its `details` field to the admin only.
- Tests:
  - new `backend/tests/server-errors.test.js`, a fake Python and an admin, an editor and a corpus
    member; `console.error` spied:
    - the fake writes `SECRET-DIAG /srv/private/path` to stderr and exits 3:
      `GET /api/exports?format=json` answers the member `500 { error: 'Failed to export corpus data' }`
      with no `SECRET-DIAG` in the body and the admin an `error` containing it; the log got
      `'[/api/exports] Error:'` with that message both times. On `d4518a9` the member's body has it;
    - the same fake on `POST /api/keyword-search/preview` and `POST /api/keyword-search` (before
      any `run_created` line): the editor gets `502` and `500` with `{ error: 'Keyword search failed' }`,
      the admin the detail, and the log the full error for each;
    - mapped cases stay for the editor: stderr `OpenAlex rate limit exceeded. Retry after 5s.`
      answers 429 with that text, `ValueError: bad query` answers 400 `bad query`;
  - phase 8's trigger test in `corpus-sharing.test.js` (`:400`, as the admin) also asserts
    `{ error: 'marker delete failed' }`; an editor on the same trigger gets
    `{ error: 'Failed to remove work from corpus' }`;
  - the existing 500 tests pass unchanged; those that read a detail (`auth.test.js:251`, `:259`,
    `corpus-sharing.test.js:408`, `access-routes.test.js:617`) run as the admin.

**D11. One ticket store (BE-30).**
- New `backend/src/downloadTickets.js`:
  `createTicketStore({ ttlMs = 60_000, now = Date.now } = {})` returns
  `{ ttlSeconds, mint(fields) → token, get(token) → ticket | null, revoke(token) }`. `mint` and `get`
  prune expired tickets first; tokens stay `crypto.randomBytes(24)` in `base64url`; a ticket stays
  reusable until it expires, as today.
- Both stores come from it. The access middlewares (`app.js:2872-2902`, `:2911-2941`) keep their
  checks, texts and stub handling; the mint routes answer `expires_in_seconds: store.ttlSeconds`.
- Tests: new `download-tickets.test.js` with an injected clock (valid before 60 s, `null` after,
  pruned on the next `mint`, `revoke`); the existing ticket route tests pass unchanged.

**D12. One expansion builder; the dead arg builder goes (BE-31).**
- `buildExpansion(body, defaults)` holds the seven shared fields (`relatedDepthDownstream`,
  `relatedDepthUpstream`, `maxRelated`, `relatedSort`, `promotionMode`, `includeDownstream`,
  `includeUpstream`). The keyword path calls `buildExpansion(req.body, RECURSION_DEFAULTS.keyword)`
  (its pre-coercion, `app.js:2357-2366`, repeats what `coerceInt` does inside).
  `buildUploadedDocsExpansion(body)` becomes
  `{ relatedDepth: coercePositiveInt(coerceInt(body?.relatedDepth, null), RECURSION_DEFAULTS.uploadedDocs.relatedDepth, 0), ...buildExpansion(body, RECURSION_DEFAULTS.uploadedDocs) }`,
  so the job payload keeps its eight fields.
- `applyUploadedDocsExpansionArgs` is deleted; `applyKeywordSearchExpansionArgs` is unchanged.
- Tests: `recursion-args.test.js` passes unchanged; new: the enrich job's
  `parameters_json.expansion` `toEqual`s the eight-field object for a body with every field set and
  for an empty body.

### Python

**D13. PY-32 is closed, won't fix (controller ruling).** About 20 shared lines (`log`, four
arguments, the clamp, the connection, the column loop) between two hand-run scripts whose scopes,
columns and loops differ on purpose; a shared module of about 35 lines would add a dependency for
both and for the test loader (`test_doi_rule_scripts.py:14-25`).

**D14. The seed marker is written only while the work is a member (PY-55), controller call.**
- `_mark_candidate_in_corpus(db, corpus_id, candidate, work_id: int)`: the work id is required
  (every caller passes one, `seed_promote.py:290`, `:303`, `:338`, `:369`, `:397`, `:422`). The
  upsert becomes
  `INSERT INTO seed_candidates_in_corpus (…) SELECT ?, ?, ?, ?, ?, CURRENT_TIMESTAMP WHERE EXISTS (SELECT 1 FROM corpus_works WHERE corpus_id = ? AND work_id = ?) ON CONFLICT(…) DO UPDATE SET work_id = excluded.work_id, marked_at = CURRENT_TIMESTAMP`.
  One statement under SQLite's write lock: a removal committed before it leaves no marker; one
  after it deletes the marker. Without the `NULL` branch and the `COALESCE`, an old marker of a
  work that left the corpus is never refreshed. A link that failed silently
  (`_add_existing_to_corpus`, `:150-155`) now leaves no marker either.
- Rejected: checking membership when the list reads the markers (a lookup on every list load, and
  the stale rows stay).
- Tests, new `test_seed_promote_marker.py` (loads the script like `test_doi_rule_scripts.py`): a
  member gets its marker; a work removed before the call gets none (on `d4518a9` it does); a
  member's marker is refreshed; a non-member's old marker keeps its `marked_at`. Atomicity: with
  `conn.set_trace_callback`, the one `INSERT` contains `EXISTS (SELECT 1 FROM corpus_works` and no
  separate `SELECT` on `corpus_works` precedes it.

**D15. Every row of a shared token is written (PY-57).**
- `run_pass` builds `token_to_rows: dict[str, list[tuple[int, str]]]` and requests each token once
  (`dict.fromkeys` keeps the order). Every row of a returned token gets an update with its own raw
  value, so phase 8's write guard still checks each row against what it was read with.
- Tests, `test_doi_rule_scripts.py` with `_fake_openalex`: pass 1 rows `W1` and
  `https://openalex.org/W1` both get topics and `topics_fetched = 1`, the filter lists `W1` once,
  `matched` counts 2; pass 2 rows `10.1000/abc` and `10.1000/ABC` the same. On `d4518a9` one row of
  each pair stays unfetched.

**D16. A completed scraper run is not scraped again (PY-58), controller call.**
- `execute_scraper_job` gets an outer `try`/`finally` right after `sqlite3.connect`, so the
  connection closes on every path. After loading the run, `status == 'completed'` returns
  `{"run_id", "source_type", "total", "downloaded", "failed"}` from the stored counts plus
  `"replayed": True` and writes nothing; the job ends `completed`. The `try`/`except` that marks
  the run failed (`scraper_lab.py:392-472`) stays around the scrape only; unknown run and source
  still raise before it.
- Safe because every scrape creates a new run (`app.js:3882-3913`). One check covers start-up
  recovery, periodic recovery and any later replay; `_fail_scraper_runs_of` touches only `running`
  runs.
- Tests, `test_scraper_lab.py`: a `completed` run with counts 3/2/1 and one artifact, with
  `scrape_eurlex` patched to raise: the stored counts and `replayed` come back, the row is unchanged,
  the artifact count stays 1 (on `d4518a9` the run ends `failed`). A `pending` run still scrapes.
  With `sqlite3.connect` wrapped, every connection is closed after the completed-run return and
  after an unknown run id.

**D17. The CLI reads the producer's fields; `--fetch-citations` stays on (PY-59), controller
ruling.**
- `process_single_file` reads the citation fields only under
  `if enriched_ref and fetch_citations:` (an unmatched reference is `None`):
  `cited_by_url = citing_works_url(enriched_ref.get('cited_by_api_url'), enriched_ref.get('openalex_id'))`;
  only `if cited_by_url:` it prints `enriched_ref.get('title')` and fetches. An unmatched reference
  is written back as today. The output key stays `citing_works_ids`.
- Which matches fetch (pre-flight 2): only an eligible OpenAlex citing lookup. A match without a
  stored URL fetches nothing (`citing_works_url` answers `None`). A Crossref match stores an
  `api.crossref.org` URL and has no usable OpenAlex id, so `citing_works_url` returns it
  unchanged and `fetch_citing_work_ids` rejects it before any request (`OpenAlexScraper.py:113-133`).
- Cost: one pure-filter list request per eligible lookup (`filter=cites:<id>`, `per-page=100`,
  `OpenAlexScraper.py:171`; `max_citations` 100, one page). Under the measured rule
  (`dl_lit/keyword_search.py:191-194`: 1 credit a filter page, 10 a text search) that is about
  1 credit per eligible lookup, at most about 60 for a 60-entry bibliography.
- Tests, `test_hygiene.py`: one file with an OpenAlex match (`openalex_id: 'https://openalex.org/W7'`
  and an OpenAlex `cited_by_api_url`), a Crossref match (an `api.crossref.org` URL, no OpenAlex id)
  and an unmatched reference (`None`); `fetch_citing_work_ids` patched to record its URL. The output
  holds all three; only the first has `citing_works_ids`; one fetch ran, with the stored URL. With
  `fetch_citations=False` nothing is fetched. On `d4518a9` no fetch runs.

## Order of work

Six tasks, each one reviewable commit or a small group of them.
- Tasks 1, 2 and 5 edit `App.svelte` and run Playwright on one port in one worktree: one after
  another. Tasks 3, 4 and 5 edit `backend/src/app.js`: in that order too. Tasks 1–2 can run beside
  Tasks 3–4; Task 5 waits for 2 and 4; Task 6 can run beside any.
- Each task runs only its own suites; the whole CI Playwright list runs once at the end.

1. **Frontend DRY:** D1, D2, D3 (`App.svelte`, new `lib/download.js`, `ScraperLab.svelte`,
   `UpstreamBrowser.svelte`; `session.spec.ts`, `corpus-load.spec.ts`, `tab-resources.spec.ts`).
2. **Frontend small fixes:** D5, D6 (`App.svelte`, `ThreeGraph.svelte`; `session.spec.ts`,
   `graph-3d.spec.ts`).
3. **Backend helpers:** D7 (new `dbHelpers.js`, `identifiers.js`; `seed.js`, `pipelineReads.js`,
   `corpusList.js`, `app.js`; `db-helpers.test.js`, `identifiers.test.js`, `seed-state.test.js`,
   `pipeline-reads.test.js`).
4. **Backend routes:** D8–D12 (`app.js`, new `downloadTickets.js`; `download-tickets.test.js`,
   `recursion-args.test.js`, new `server-errors.test.js`, `corpus-sharing.test.js`).
5. **Import and extraction binding:** D4 (`app.js` with `staged_uploads`, `lib/api.js`,
   `App.svelte`; new `staged-uploads.test.js`, `seed-upload.spec.ts`).
6. **Python:** D14–D17 (`seed_promote.py`, `backfill_openalex_topics.py`, `scraper_lab.py`,
   `OpenAlexScraper.py`; `test_seed_promote_marker.py`, `test_doi_rule_scripts.py`,
   `test_scraper_lab.py`, `test_hygiene.py`).

**Setup, once, before Task 1:** `cd backend && npm ci`, `cd frontend && npm ci`, then
`cd frontend && npx playwright install chromium`.

D13 has no task: the controller marks PY-32 closed in the inventory's reconciliation section, adds
FE-68 there, and records the phase 9 outcome for the other rows.

## Acceptance

- Every bug fix (D4, D5, D6, D7's matching change, D10's member texts, D14–D17) has a test that
  fails on `d4518a9` and passes after its task. Every DRY change keeps its existing tests green.
  Each commit names the rows it closes.
- Python: `cd dl_lit_project && /home/jay/DT/dl_lit_project/.venv/bin/python -m pytest tests -q`.
- Backend: `cd backend && npm test`. Under full parallel load suites can time out; compare with a
  `d4518a9` baseline or use `npm test -- --maxWorkers=2` before blaming a task.
- Frontend: `cd frontend && npx vite build`; the CI Playwright list (`.github/workflows/ci.yml:54`,
  unchanged) passes once at the end with
  `E2E_FRONTEND_PORT=5190 npx playwright test <specs> --workers=1` (check that 5190 is free, e.g.
  `ss -ltn`). Never 5175, the dev container serving master. No live spec.
- Greps, each finding only what is named:
  - `git grep -n "createElement('a')" -- frontend/src`: `lib/download.js`;
  - `git grep -n "RAG_FEEDER_STUB === '1'" -- backend/src/app.js`: `isStubMode`;
  - `git grep -n "new Database(DB_PATH, { readonly" -- backend/src`: nothing;
  - `git grep -n "function tableExists" -- backend/src`: `dbHelpers.js`;
  - `git grep -n "\['pdf', 'search'\]" -- backend/src`: `SEED_SOURCE_TYPES` in `seed.js`;
  - `git grep -n "applyUploadedDocsExpansionArgs\|normalizeGraphOpenAlexId\|safeJsonParse" -- backend/src`:
    nothing;
  - `git grep -n "best_match_result" -- dl_lit_project/dl_lit`: nothing.
- Never against `dl_lit_project/data/literature.db` or any other dev or prod DB; temp DBs only.

## Controller calls and rulings (the owner may override any)

- **D2 (FE-16), call:** both helpers set `noopener` and always revoke; no visible change.
- **D4 (FE-65b, FE-68), call:** corpus check, then input validation, then a DB-recorded uploader
  check (404; admin exempt); late extraction replies dropped; pre-deploy staged files need one
  re-upload.
- **D6 (FE-67), call:** the picker rolls back before publish and keeps the error through recolours.
- **D7 (BE-33/34), call:** seed matching takes the workers' OpenAlex rule (URL ids now match, which
  can show `downloaded_elsewhere` for readable works); positive `tableExists` answers remembered.
- **D9 (BE-28), call:** no stub router; one `isStubMode()` line per handler.
- **D10 (BE-29), ruling:** non-admins get the route's fallback on 500s and unmapped keyword-search
  failures; admins keep the detail; the log always has it; mapped 400/429 unchanged.
- **D13 (PY-32), ruling:** closed, won't fix.
- **D14 (PY-55), call:** the marker insert checks membership in the same statement; races before the
  fix leave their markers.
- **D16 (PY-58), call:** a completed run returns `replayed: true`; interrupted runs unchanged.
- **D17 (PY-59), ruling:** `--fetch-citations` stays on; about 1 credit per eligible OpenAlex lookup.
- **Parked rows (BE-39, BE-52, PY-33, FE-17 remainder), ruling:** stay parked (Non-goals).
