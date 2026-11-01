# Review fixes, phase 2: backend access control, child processes, dead scheduler code — design

Date: 2026-11-01. Source: `docs/reviews/2026-10-27-backend-dry-perf.md` (I5 part, I7, I8, I9,
I11 part). Jay: "let's do that", with the method: Opus-medium implementers under
subagent-driven development, Codex (`gpt-6.1-sol`, reasoning high) as the review gate.
Phase 1 (SQL hot paths, PR #74/#75) is merged and deployed.

## Problems

1. **The log WebSocket at `/api/ws` is unauthenticated** (`index.js`). Anyone who can reach
   the host receives every `broadcast()` line and event for every corpus: file names, corpus
   ids, extraction progress, raw script stdout/stderr. The frontend filters by corpus only
   client-side.
2. **Read routes trust a stale corpus.** `requireAuth` sets `req.corpusId = user.last_corpus_id`
   without checking `user_corpora`. After an owner un-shares a corpus, the former member keeps
   reading it through every read route (`/api/corpus`, seed sources, exports, ingest stats,
   logs) until they select another corpus. Writes are safe (`requireCorpusWriteAccess` looks
   the role up). The two download-ticket middlewares have the same fallback.
3. **Routes that take a corpus from the query string check nothing**: `/api/logs/tail`
   (`corpus_id=<any>`, and `type=app` gives every viewer the whole application log),
   `/api/graph/3d/snapshot` (`corpus_id=<any>`, and `all` merges every corpus),
   `/api/graph/3d/node/:id` (any work), the graph cluster and snapshot-file routes.
4. **Scraper apply has no role check**: any logged-in user can stage `metadata.bib` drafts into
   a Kantropos target directory.
5. **PDF extraction blocks the event loop**: `getPdfPageCountSync`, `llmUploadPdfSync` (uploads
   the PDF to Gemini over the network) and `llmDeleteUploadedSync` are `spawnSync` calls on the
   main thread. While they run, every request and WebSocket frame for every user stalls.
6. **Five hand-built child environments skip `appSettingsEnv()`** (the Admin-tab overrides),
   so `get_bib_pages.py`, `APIscraper_v2.py`, the Gemini upload/delete and the pikepdf probe
   run with the container env. An LLM provider, key or model set in the Admin tab is silently
   ignored for PDF extraction. Six `PYTHONPATH:` literals; stdout/stderr forwarding copied.
7. **About 500 lines of dead in-memory scheduler code** in `app.js` (`downloadWorkers`,
   `pipelineWorkers`, `getOrInit*`, `tick*`, `start*`/`stop*`/`cancel*`, ...) plus three
   unreferenced Python scripts (`downloads_list.py`, `ingest_runs.py`, `ingest_stats.py`). The
   worker routes are stubs whose status JSON is duplicated across six handlers.

## Decisions

### Access model (one rule, applied everywhere)

A user may **read** a corpus when they have any row in `user_corpora` for it, or they are the
global admin (`isAdminUser`). They may **configure** it (share, delete, Kantropos assignment,
promote, ...) with role `owner`/`editor` as today. Admins keep access to everything. Nobody
with a current membership loses anything; only anonymous sockets and revoked memberships change.

- `resolveCorpusRole(db, user, corpusId, authConfig)` in `app.js` (module level, next to
  `getCorpusRoleForUser`): returns `'admin'` for the admin user, else the `user_corpora` role,
  else `null`. Every check below goes through it.
- **`requireAuth` re-validates the current corpus.** After loading the user it sets
  `req.corpusId` to `user.last_corpus_id` only if `resolveCorpusRole` is non-null for it;
  otherwise it falls back to the user's most recently created membership (the same query the
  login handler already uses) and persists that as `last_corpus_id`; with no memberships,
  `req.corpusId = null`. `req.corpusRole` is set alongside. The login / accept-invite /
  reset-password handlers share this through one `ensureLastCorpus(db, user, authConfig)`
  helper (returns the resolved corpus id or null; persists when it changes). A null
  `req.corpusId` is not new: a fresh account has one today, and every `req.corpusId`-scoped
  read already answers it with an empty result or its existing "select a corpus" error, which
  the frontend handles. Only the explicit guards in the table below answer 403.
- **`requireCorpusMember(getCorpusId)` middleware factory** (inside `createApp`, next to
  `requireCorpusWriteAccess`): `getCorpusId(req)` defaults to `() => req.corpusId`; a missing
  id → 403 `Select a corpus first`; a non-null role → sets `req.corpusRole` and continues;
  otherwise 403 `No access to this corpus`. `requireCorpusRole(...roles)` is the same with a
  role whitelist (admin always passes). Stub mode (`RAG_FEEDER_STUB=1`) passes through, as the
  existing guards do.
- **Download tickets** (`requireCorpusDownloadAccess`, `requireUpstreamDownloadAccess`): the
  corpus a ticket was minted for is re-validated with `resolveCorpusRole` at download time;
  the `user.last_corpus_id` fallback goes through the same check.

### Route coverage

| Route | Rule |
|---|---|
| `GET /api/logs/tail` `type=app` | admin only (403 otherwise) |
| `GET /api/logs/tail` `type=pipeline` | `corpus_id` given → member of that corpus; absent → `req.corpusId` (null → 403) |
| `GET /api/graph/3d/snapshot` | `corpus_id=<n>` → member; `all`/absent → admin gets the merged view, a non-admin is scoped to `req.corpusId` (null → 403 `Select a corpus first`). The `corpusId` used for the snapshot key is the resolved one, so caches stay per corpus. |
| `GET /api/graph/3d/node/:nodeId` | the work must belong to a corpus the user may read (admin: any); otherwise 404, not 403, so existence does not leak. `users`, `user_corpora`, `corpus_works` and `works` live in the same database file (`authDb` opens `DB_PATH`), so this is one `EXISTS` over `corpus_works JOIN user_corpora`. A work in no corpus at all is admin-only. |
| `GET /api/graph/3d/cluster/:snapshotKey/:clusterId`, `GET /api/graph/3d/snapshot/:snapshotKey/:fileName` | the snapshot's corpus scope must be readable by the user. The key is an opaque hash, so Node writes a sidecar `scope.json` (`{"corpusId": <n> \| null}`) into the snapshot directory whenever it builds or schedules a snapshot (`graph3dSnapshotPath(key, 'scope.json')`); these routes read it: `corpusId` → member check, `null` (the merged view) → admin only, **no sidecar** (a cache dir from before this change) → admin only until the TTL rebuild writes one. Never trust a client-supplied corpus here. |
| `POST /api/corpora/:id/select` | `requireCorpusMember(req => req.params.id)` (replaces the inline role SELECT) |
| `POST /api/corpora/:id/share`, `DELETE /api/corpora/:id` | `requireCorpusRole('owner')` (today's semantics, via the helper) |
| `GET/PUT /api/corpora/:id/kantropos-assignment` | GET member; PUT `requireCorpusRole('owner','editor')` |
| `POST /api/scraper/apply`, `GET /api/scraper/apply-events/:id/metadata-bib` | admin, or owner/editor of at least one corpus assigned (`corpus_kantropos_assignments`) to the target (`body.targetId`, resp. the event's `target_id`); the `metadata-bib` download is also allowed to the user who applied (`scraper_apply_events.user_id`). A target with no assigned corpus is therefore admin-only. 403 otherwise |
| `POST/GET /api/scraper/runs*` | unchanged: any logged-in user (runs are not corpus data; the Scraper Lab tab is shown to every user) |
| `/api/kantropos/corpora*` (list, browse, download-ticket, file) | **unchanged, deliberately**: Kantropos targets are institution-wide upstream corpora that the Korpus Management tab offers to every user; the browse payload carries no user data. Recorded as a decision, not an oversight. |
| All `req.corpusId`-scoped reads (`/api/corpus`, seed sources, exports, ingest, pipeline summaries, downloads) | covered by the `requireAuth` re-validation; no per-route change |

### WebSocket

- WebSocket setup moves out of `index.js` into `backend/src/logSocket.js`:
  `attachLogSocket(server, { authDb, authConfig, path: '/api/ws', isStub })` returns
  `{ broadcast, broadcastEvent, clients, close }`. `index.js` keeps file logging and calls it;
  `broadcast` still appends to the pipeline log file (pass `appendLogLine` in).
- **Authentication on connect.** The browser cannot set headers on a WebSocket, and a
  `?token=` query string would land in Caddy's access log, so the token travels in
  `Sec-WebSocket-Protocol`: the client opens `new WebSocket(url, ['bearer', token])`; the
  server (`ws` `handleProtocols`) selects `bearer` and reads the other entry as the JWT
  (JWT characters are all valid protocol tokens). The upgrade always completes (101), then the
  server verifies: invalid or missing token → `ws.close(4401, 'unauthorized')`; unknown user →
  same. This keeps the deploy script's unauthenticated `101` probe valid.
- **Per-client scope.** On connect the server stores `{ userId, isAdmin, corpusIds }`, with
  `corpusIds` re-read from `user_corpora` at most every 5 s (memo per client, lazy; a revoked
  member may therefore see up to 5 s of further lines, accepted; no active disconnect). The
  `ws` dependency is 8.19 (`handleProtocols(protocols: Set<string>, request)` must return
  exactly `'bearer'`, or the browser aborts the handshake). Caddy's reverse proxy forwards
  `Sec-WebSocket-Protocol` with the other upgrade headers; the dev-stack manual check below
  confirms it end to end before the production deploy. A text
  line is delivered when its `corpus=<id>` tag names a corpus in the client's set, or the
  client is admin. A JSON event is delivered when its `corpus_id` is in the set, or the client
  is admin. Lines/events with `corpus=none` / `corpus_id: null` or no tag go to admins only
  (all 22 `send()` sites and all three `sendEvent()` sites tag their corpus today; the rule is
  deny-by-default for anything new that forgets).
- **Stub mode** (`RAG_FEEDER_STUB=1`, used by Jest and the mocked Playwright specs): no token
  required, every client receives everything, as today.
- **Frontend** (`App.svelte connectLogs`): pass the protocols; on `close` with code 4401 do
  not reconnect, call the existing 401 handling path (token cleared, login shown). The
  `routeWebSocket('**/api/ws', ...)` mocks in Playwright keep working.

### Child processes

- New module `backend/src/pythonProcess.js`:
  `createPythonRunner({ pythonExec, dlLitProjectDir, repoRoot, dbPath, appSettingsEnv })`
  returns `{ pythonEnv, spawnPython, runPythonJson }`.
  - `pythonEnv(extra = {})` → `{ ...process.env, ...appSettingsEnv(), PYTHONPATH,
    RAG_FEEDER_DL_LIT_PROJECT_DIR, RAG_FEEDER_DB_PATH, ...extra }` (`extra` wins, so the
    extraction task's explicit model overrides still beat the Admin settings, as today).
  - `spawnPython(scriptPath, args, { env, dbPath, corpusId, onStdoutLine, onStderrLine,
    lowPriority, label })` → `{ child, done }`; `done` resolves `{ stdout, stderr, code }` on
    exit 0 and rejects `Error('Python script failed (<code>): ...')` otherwise; line handlers
    receive trimmed complete lines, the partial tail is flushed on close (today's
    `flushLines` semantics). `label` is used in the rejection message.
  - `runPythonJson(scriptPath, args, opts)` keeps its current signature and behaviour
    (parses JSON from stdout with `parseJsonFromOutput`, which moves into the module).
- `app.js` builds one runner at module load (`PYTHON_EXEC`, `DL_LIT_PROJECT_DIR`, `REPO_ROOT`,
  `DB_PATH`, `appSettingsEnv`) and deletes `spawnPythonJson`'s body and every hand-built env.
  The two extraction spawns (`APIscraper_v2.py`, `get_bib_pages.py`) use `spawnPython` with
  `onStdoutLine`/`onStderrLine` that `console.log` and `send(...)` per line with the same
  prefixes as today (`[<label>] [corpus=<tag>] <line>`, `[<label> ERROR] ...`,
  `[get_bib_pages][corpus=<tag>] ...`). Lines instead of raw chunks: the frontend already
  matches per message with regexes, so this is at worst a change in how multi-line chunks are
  split.
- The three inline `-c` snippets become scripts under `backend/scripts/`:
  `pdf_page_count.py` (prints the page count), `llm_upload_pdf.py` (prints
  `{"name": ...}`; keeps the OpenAI-compat local-handle branch and the non-ASCII copy), and
  `llm_delete_upload.py`. `app.js` gets `getPdfPageCount(pdfPath)`, `llmUploadPdf(pdfPath)`,
  `llmDeleteUploaded(name)` as `async` functions over `runPythonJson`/`spawnPython`, awaited
  inside the extraction task (which already runs detached from the HTTP response). Failure
  semantics stay: page count → `null` with a warning; upload → throws; delete → warns.
- `spawnSync` is no longer imported in `app.js`.

### Dead scheduler code (I9)

- Delete `downloadWorkers`, `pipelineWorkers`, `getOrInit*`, `tick*`, `countPipelineTickJobs`,
  `countQueuedDownloads`, `startDownloadWorker` … `stopAllDownloadWorkers`,
  `cancelDownloadJobs`, `cancelAllDownloadJobs`, `countDownloadPipelineJobs`,
  `resolveAutoDownloadBatchSize`, `resolveDownloadWorkerCount`, `aggregateDownloadPayloads`,
  `emitPromotionEvent`, `pauseAllPipelineWorkers`, and the no-op calls to
  `pausePipelineWorker`/`stopDownloadWorker` in corpus delete.
- The six worker stub routes keep their paths and response shapes (the frontend polls them)
  through one `dbDrivenWorkerStatus(kind)` helper; the Jest tests that cover them today must
  pass unchanged.
- Delete `backend/scripts/downloads_list.py`, `ingest_runs.py`, `ingest_stats.py` and the
  comment that references the last one.

### Deferred (recorded, not in this phase)

I5's other items (gzip of graph snapshot files, `warmSeedState` on a worker thread,
`/api/logs/tail` backwards reads); the rest of I11 (route error wrapper, stub router, ticket
store, expansion builders, token mails, `authDb` instead of per-request `new Database`); I3,
I4, I6. Frontend-side consolidation of the 401 handler is phase 3; this phase only adds the
4401 branch to `connectLogs`.

## Verification

- Jest, real-auth fixture (as in `corpus-sharing.test.js`): a revoked member gets 403 on
  `/api/corpus`, `/api/logs/tail`, `/api/graph/3d/snapshot?corpus_id=` and the member-only
  routes, and their `req.corpusId` falls back to a remaining membership; a viewer gets 403 on
  `type=app` logs and on scraper apply; admin passes everywhere; `node/:id` of a foreign work
  is 404; the `all` graph is admin-only and a non-admin is scoped to their corpus.
- Jest, `logSocket.js` over a real `http` server and the `ws` client: no token → close 4401;
  valid token → receives lines for own corpora only, not others, not `corpus=none`; admin
  receives all; membership change takes effect within the memo window; stub mode receives all.
- Jest, `pythonProcess.js`: `pythonEnv` merges `appSettingsEnv()` under `extra`; `spawnPython`
  line forwarding and partial-tail flush with a tiny Python script; rejection on non-zero exit.
  A source assertion that `app.js` no longer imports `spawnSync` and contains no
  `PYTHONPATH:` literal.
- pytest: the three new scripts (page count on a generated PDF; upload/delete with an injected
  fake client).
- Jest: the existing worker-status route tests pass after the deletion; `npm test` whole suite;
  `npx vite build`; the mocked Playwright specs.
- Manual on the dev stack: log in, Logs panel connects (status "connected"); a second user who
  is not a member of the current corpus sees none of its extraction lines; an un-shared user's
  Workspace switches to another corpus on the next poll.
