# Review fixes 2: backend access control, child processes, dead scheduler code — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every corpus-scoped read, the log WebSocket and the graph routes enforce current corpus membership; PDF extraction no longer blocks the event loop and honours the Admin-tab LLM settings; the dead in-memory scheduler is gone.

**Architecture:** One `resolveCorpusRole` rule feeds `requireAuth` (re-validates `last_corpus_id`), two middleware factories (`requireCorpusMember`, `requireCorpusRole`), the download-ticket guards, the graph routes and a new `logSocket.js` that authenticates the WebSocket via `Sec-WebSocket-Protocol` and filters lines per client. A new `pythonProcess.js` owns the child-process env and spawning; the three inline Python snippets become scripts. The dead scheduler block is deleted behind the existing worker-route tests.

**Tech Stack:** Node 20 ESM, Express, better-sqlite3, `ws` 8.19, jsonwebtoken, Jest (`cd backend && npm test`), supertest; Svelte 5 compat frontend; Python 3.11 scripts under `backend/scripts/` with pytest (`cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests -q`).

**Spec:** `docs/superpowers/specs/2026-11-01-review-fixes-2-backend-access-and-child-processes-design.md`

## Global Constraints

- Admin (`isAdminUser(user, authConfig)`) passes every corpus check; role string `'admin'`.
- Stub mode (`process.env.RAG_FEEDER_STUB === '1'`) passes every new guard and the WebSocket auth, as the existing guards do.
- Error bodies: `{ error: 'Select a corpus first' }` (403) when no corpus can be resolved; `{ error: 'No access to this corpus' }` (403) for a non-member; `{ error: 'Admin access required' }` (403, existing text) for admin-only.
- WebSocket: client protocols `['bearer', <jwt>]`; server selects `'bearer'`; auth failure closes with code `4401`, reason `'unauthorized'`. Lines without a readable `corpus=<id>` tag, and events with `corpus_id: null`, go to admins only.
- Log line prefixes from the extraction task are unchanged: `[<label>] [corpus=<tag>] <line>`, `[<label> ERROR] [corpus=<tag>] <line>`, `[get_bib_pages][corpus=<tag>] <line>`, `[get_bib_pages ERROR][corpus=<tag>] <line>`.
- `app.js` must not import `spawnSync` and must contain no `PYTHONPATH:` literal after Task 5.
- Worker stub routes keep their paths and JSON shapes (`backend/tests/downloads.test.js` is the oracle).
- Real-auth Jest fixtures follow `backend/tests/corpus-sharing.test.js` (env `RAG_FEEDER_DB_PATH`, `RAG_FEEDER_JWT_SECRET`, `RAG_ADMIN_USER`, `RAG_ADMIN_PASSWORD`; dynamic `import('../src/app.js')`; users inserted with bcryptjs; login via `/api/auth/login`).
- Commits: one per task minimum, trailers `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>` and `Claude-Session: https://claude.ai/code/session_01U3c4gfg7jQvvmPdpLNBN5T`.
- Never run anything against `dl_lit_project/data/literature.db`; tests use temp files.

## Review Focus

1. A user whose only corpus is un-shared mid-session: every poll must answer with their next membership (or empty), never 500. Pinned in Task 1 (`falls back to the remaining membership`, `no memberships -> null corpusId, /api/corpus answers 200 with an empty list or its existing select-a-corpus error`).
2. A member deleted from `users` while a socket is open: the WebSocket must stop delivering, not throw. Pinned in Task 4 (`deleted user receives nothing after the memo expires`).
3. A JWT that is syntactically a protocol token but expired: upgrade completes, close 4401, the frontend does not loop reconnecting. Pinned in Task 4 (server) and the Playwright mock still passes (Task 4 frontend step).
4. An old graph snapshot directory without `scope.json`: admin still gets it, a member gets 403, and the TTL rebuild writes the sidecar. Pinned in Task 3.
5. A Python child that exits non-zero with a partial last line: the partial line is still forwarded and the promise rejects once. Pinned in Task 5.

---

### Task 1: Membership re-validation in `requireAuth`, `ensureLastCorpus`, ticket guards

**Files:**
- Modify: `backend/src/app.js` (module level: after `getCorpusRoleForUser` ~line 1104; `requireAuth` ~1823; inside `createApp`: `requireCorpusDownloadAccess`/`requireUpstreamDownloadAccess` ~2684-2740; login ~3568-3580; accept-invite ~3781-3790; reset-password ~3847-3856)
- Test: `backend/tests/access-revalidation.test.js`

**Interfaces:**
- Produces: `resolveCorpusRole(db, user, corpusId, authConfig = resolveAuthConfig()) -> 'admin' | 'owner' | 'editor' | 'viewer' | null` (module level, exported). `ensureLastCorpus(db, user, authConfig) -> number | null`: returns `user.last_corpus_id` when `resolveCorpusRole` is non-null for it; otherwise the newest `user_corpora` row (`ORDER BY created_at DESC, corpus_id DESC LIMIT 1`) and persists it with `UPDATE users SET last_corpus_id`; otherwise `null` (and persists `NULL` if the stored value was stale). `requireAuth` sets `req.corpusId` and `req.corpusRole` from it. The `defaultCorpusId` fallback in the three auth handlers is removed: they call `ensureLastCorpus`.

- [ ] **Step 1: Write the failing tests** in `backend/tests/access-revalidation.test.js` (real-auth fixture). Users: admin (bootstrap), `owner1` with personal corpus A, `member2` shared on A as viewer with their own personal corpus B.

```js
test('a revoked member no longer reads the corpus through last_corpus_id', ...)
// member2 selects A (POST /api/corpora/:A/select -> 200), then owner removes the row:
// DELETE FROM user_corpora WHERE user_id = member2 AND corpus_id = A.
// GET /api/corpus as member2 -> 200 and every returned work id belongs to B (not A);
// GET /api/auth/me -> user.last_corpus_id === B (fell back and persisted).
test('with no memberships left, corpusId is null and reads do not 500', ...)
// also delete member2's B row: GET /api/corpus -> status in [200, 403], never 500;
// SELECT last_corpus_id FROM users WHERE id = member2 -> null.
test('admin keeps a last_corpus_id they are not a member of', ...)
// UPDATE users SET last_corpus_id = B WHERE id = admin; GET /api/auth/me -> last_corpus_id === B.
test('login no longer hands a membership-less user the default corpus', ...)
// a fresh user row inserted with no corpora rows: login -> user.last_corpus_id === null.
test('a download ticket minted for A fails after revocation', ...)
// member2 mints POST /api/corpus/downloaded/:id/download-ticket for a downloaded work in A
// (insert a works row with download_status 'downloaded' and a temp file) -> 200 ticket;
// revoke; GET /api/corpus/downloaded/:id/file?ticket=... -> 403.
```

- [ ] **Step 2: Run** `cd backend && npm test -- tests/access-revalidation.test.js` → FAIL (member still reads A; default corpus assigned).
- [ ] **Step 3: Implement** `resolveCorpusRole`, `ensureLastCorpus`; rewrite the body of `requireAuth` after `getUserById` to `req.corpusId = ensureLastCorpus(db, user, authConfig); req.corpusRole = req.corpusId ? resolveCorpusRole(db, user, req.corpusId, authConfig) : null;`; in both ticket middlewares replace `ticket.corpusId || user.last_corpus_id || null` with: resolve `ticket.corpusId ?? ensureLastCorpus(...)`, then 403 `No access to this corpus` when `resolveCorpusRole` is null; login/accept-invite/reset-password: replace the inline fallback block with `const lastCorpusId = ensureLastCorpus(authDb, user, authConfig)`.
- [ ] **Step 4: Run** the new file and `npm test -- tests/corpus-sharing.test.js tests/auth.test.js` → PASS.
- [ ] **Step 5: Commit** `access: requireAuth re-validates last_corpus_id; ensureLastCorpus shared by login paths and ticket guards`.

### Task 2: `requireCorpusMember` / `requireCorpusRole` and the explicit-corpus routes

**Files:**
- Modify: `backend/src/app.js` (next to `requireCorpusWriteAccess` ~2751; routes `/api/logs/tail` ~6333, `/api/corpora/:id/select|share|kantropos-assignment` ~4268-4365, `DELETE /api/corpora/:id` ~4367, `/api/scraper/apply` ~4066, `/api/scraper/apply-events/:id/metadata-bib` ~4228)
- Test: `backend/tests/access-routes.test.js`

**Interfaces:**
- Consumes: `resolveCorpusRole` (Task 1).
- Produces (inside `createApp`): `requireCorpusMember(getCorpusId = (req) => req.corpusId)`, `requireCorpusRole(roles, getCorpusId)` where `roles` is an array of role strings; both set `req.corpusRole`; `canUseKantroposTarget(user, targetId, { roles }) -> boolean` (admin, or a `user_corpora` row with a role in `roles` on a corpus whose `corpus_kantropos_assignments.target_id = targetId`).

- [ ] **Step 1: Write the failing tests** (fixture as Task 1, plus `viewer3` shared on A as viewer, a Kantropos target `target-a` from `RAG_FEEDER_KANTROPOS_CORPORA` assigned to A by owner1 via `PUT /api/corpora/:A/kantropos-assignment`).

```js
test('logs: type=app is admin only', ...)            // viewer3 -> 403 'Admin access required'; admin -> 200
test('logs: pipeline corpus_id of a foreign corpus -> 403', ...)   // viewer3 ?corpus_id=B -> 403; ?corpus_id=A -> 200
test('logs: pipeline without corpus_id uses req.corpusId; none -> 403 Select a corpus first', ...)
test('select: non-member 403, member 200', ...)      // viewer3 select B -> 403; select A -> 200
test('share and delete need owner', ...)             // viewer3 share A -> 403; owner1 share A -> 200 (existing shape)
test('kantropos-assignment: GET member, PUT owner/editor', ...)
test('scraper apply: viewer 403, owner of an assigned corpus 200 path reached, admin reached', ...)
// "reached" = status is not 403 (the handler may 400 on a missing target dir)
test('scraper metadata-bib: applier, assigned owner/editor and admin may download; others 403', ...)
```

- [ ] **Step 2: Run** `npm test -- tests/access-routes.test.js` → FAIL.
- [ ] **Step 3: Implement** the two factories and `canUseKantroposTarget`; wire: `/api/logs/tail` (parse `type` and `corpus_id` first, then `requireAdminMiddleware`-equivalent inline check for `app`, `resolveCorpusRole` for the corpus); `select` → `requireCorpusMember((req) => req.params.id)` replacing the inline `SELECT role`; `share`, `DELETE` → `requireCorpusRole(['owner'], (req) => req.params.id)`; `kantropos-assignment` GET member / PUT `['owner','editor']`; scraper apply → `canUseKantroposTarget(req.user, targetId, { roles: ['owner','editor'] })`; metadata-bib → same on the event's `target_id`, or `event.user_id === req.user.id`.
- [ ] **Step 4: Run** the new file plus `tests/corpus-sharing.test.js tests/corpus-delete-cleanup.test.js` → PASS.
- [ ] **Step 5: Commit** `access: requireCorpusMember/requireCorpusRole on logs, corpus admin routes and scraper apply`.

### Task 3: Graph routes scoped to readable corpora, `scope.json` sidecar

**Files:**
- Modify: `backend/src/app.js` (`parseGraph3dOptions`/the options builder ~359-392, snapshot build/schedule ~440-560, routes ~5867-6029)
- Test: `backend/tests/graph-access.test.js`

**Interfaces:**
- Consumes: `resolveCorpusRole`.
- Produces: `graph3dScopePath(key) -> string` (`graph3dSnapshotPath(key, 'scope.json')`); `writeGraph3dScope(key, corpusId)` called wherever a snapshot build is started; `readGraph3dScope(key) -> { corpusId: number | null } | null`; `canReadGraph3dScope(req, scope) -> boolean` (`scope === null` → admin only; `scope.corpusId === null` → admin only; else `resolveCorpusRole` non-null); `workReadableByUser(db, user, workId, authConfig) -> boolean` (admin → true; else `EXISTS (SELECT 1 FROM corpus_works cw JOIN user_corpora uc ON uc.corpus_id = cw.corpus_id WHERE cw.work_id = ? AND uc.user_id = ?)`).

- [ ] **Step 1: Write the failing tests** (real-auth fixture with works in A and B, `corpus_works` rows; stub the snapshot builder by pre-creating a snapshot dir with `manifest.json` and `clusters.json` for key `k1` with `scope.json` `{"corpusId": A}`, key `k2` with `{"corpusId": null}`, key `k3` without sidecar; use `RAG_FEEDER_GRAPH_3D_CACHE_DIR` or the existing cache-dir env/constant the code reads).

```js
test('snapshot: corpus_id of a foreign corpus -> 403; own -> not 403', ...)
test('snapshot: all is admin only; a non-admin is scoped to req.corpusId; none -> 403 Select a corpus first', ...)
// assert via the snapshot key: the response/manifest for viewer3 with ?corpus_id=all equals the one for ?corpus_id=A
test('node: foreign work -> 404, own -> 200, admin -> 200 for both; a work in no corpus -> 404 for members', ...)
test('cluster and snapshot files: k1 member 200, k2 member 403 admin 200, k3 member 403 admin 200', ...)
test('writeGraph3dScope / readGraph3dScope round-trip, and a missing sidecar reads as null', ...)
// both exported from app.js; the route-level write (every build start and warmGraph3dSnapshot
// call writeGraph3dScope) is checked by reading the code path and by the Task 7 dev-stack run
```

- [ ] **Step 2: Run** `npm test -- tests/graph-access.test.js` → FAIL.
- [ ] **Step 3: Implement** the helpers; in the snapshot route resolve the effective corpus before computing the key (admin + `all` → `null`; non-admin + `all`/absent → `req.corpusId`, 403 if null; explicit id → `resolveCorpusRole` or 403); write `scope.json` next to the manifest whenever a build is kicked off (and in `warmGraph3dSnapshot`); cluster/file routes read the sidecar and call `canReadGraph3dScope`; node route uses `workReadableByUser` → 404.
- [ ] **Step 4: Run** the new file plus `tests/graph.test.js` → PASS.
- [ ] **Step 5: Commit** `graph: snapshots, clusters, files and nodes are scoped to corpora the user may read; scope.json sidecar`.

### Task 4: Authenticated, per-client-filtered log WebSocket

**Files:**
- Create: `backend/src/logSocket.js`
- Modify: `backend/src/index.js` (replace lines ~84-138: `broadcast`, `broadcastEvent`, `WebSocketServer`, `wss.on('connection')`), `frontend/src/App.svelte` (`connectLogs` ~4552-4640)
- Test: `backend/tests/log-socket.test.js`

**Interfaces:**
- Consumes: `isAdminUser`, `resolveAuthConfig`, `getUserById` (export from `app.js` if not already), `jsonwebtoken`.
- Produces: `attachLogSocket(server, { authDb, authConfig, path = '/api/ws', isStub = () => process.env.RAG_FEEDER_STUB === '1', appendLogLine, membershipTtlMs = 5000 }) -> { broadcast(message), broadcastEvent(payload), clients: Set, close() }`. `broadcast` formats `[<ISO timestamp>] <message>`, calls `appendLogLine(formatted)`, delivers per the rule. Client record: `{ ws, userId, isAdmin, corpusIds: Set<number>, loadedAt }`; `corpusIds` reloaded when `Date.now() - loadedAt > membershipTtlMs` at delivery time; a user no longer in `users` gets an empty set and `isAdmin = false`. Corpus tag regex: `/\[corpus=(\d+)\]/` or `corpus=(\d+)` (match the frontend's `/corpus=([a-zA-Z0-9_-]+)/`): deliver when the captured id is in `corpusIds`; `corpus=none` / no match → admins only. Events: `payload.corpus_id` number in set, `null`/missing → admins only.
- Frontend: `new WebSocket(wsUrl, ['bearer', getAuthToken()])` when a token exists; `ws.onclose = (ev) => { if (ev.code === 4401) { logsStatus = 'unauthorized'; handle like a 401: setAuthToken(''), authStatus = 'unauthenticated' via the existing sign-out path; return } scheduleReconnect() }`.

- [ ] **Step 1: Write the failing tests** in `backend/tests/log-socket.test.js`: start `http.createServer()`, call `attachLogSocket` with a temp `authDb` holding `users`/`user_corpora` rows (reuse `ensureAuthSchema` from `app.js` to create tables) and `authConfig = { jwtSecret: 's', adminUsername: 'admin' }`; connect with the `ws` client.

```js
test('no protocol/token -> close 4401 unauthorized', ...)
test('invalid or expired token -> close 4401', ...)
test('member receives own-corpus lines and events only', ...)
// broadcast('[x][corpus=1] a'), broadcast('[x][corpus=2] b'), broadcast('[x][corpus=none] c'),
// broadcastEvent({type:'t', corpus_id: 1}), broadcastEvent({type:'t', corpus_id: null})
// member of 1 receives exactly 'a' line and the corpus_id 1 event
test('admin receives everything', ...)
test('revocation takes effect after the memo window', ...)   // membershipTtlMs: 10 in the test
test('deleted user receives nothing after the memo expires', ...)
test('stub mode: no token required, everything delivered', ...)
test('handleProtocols selects bearer', ...)  // the client ws.protocol === 'bearer'
```

- [ ] **Step 2: Run** `npm test -- tests/log-socket.test.js` → FAIL (module missing).
- [ ] **Step 3: Implement** `logSocket.js` (`new WebSocketServer({ server, path, handleProtocols })`; on `connection` read the token from the request's `sec-websocket-protocol` header entries other than `bearer`, verify, load the record; on failure `ws.close(4401, 'unauthorized')`), rewrite `index.js` to call it and pass `appendLogLine`, remove the old `clients`/`wss` code; update `connectLogs` in `App.svelte`.
- [ ] **Step 4: Run** the new test, `npm test`, `cd frontend && npx vite build`, and the mocked Playwright spec `tests/app.spec.ts` (`set -a && . ../.env && set +a && npx playwright test tests/app.spec.ts`) → PASS; `grep -n "RAG_FEEDER_STUB" backend/src/logSocket.js` shows the stub branch.
- [ ] **Step 5: Commit** `log socket: JWT via Sec-WebSocket-Protocol, per-client corpus filtering, 4401 on auth failure`.

### Task 5: `pythonProcess.js`, scripts for the three inline snippets, async extraction

**Files:**
- Create: `backend/src/pythonProcess.js`, `backend/scripts/pdf_page_count.py`, `backend/scripts/llm_upload_pdf.py`, `backend/scripts/llm_delete_upload.py`
- Modify: `backend/src/app.js` (`appSettingsEnv` stays; delete `runPythonJson`/`spawnPythonJson` bodies ~2265-2340 and `parseJsonFromOutput`; replace `getPdfPageCountSync`/`llmUploadPdfSync`/`llmDeleteUploadedSync` ~2509-2612 with async versions; extraction task ~4655-4860: `await` the three, use `spawnPython` for `APIscraper_v2.py` and `get_bib_pages.py`)
- Test: `backend/tests/python-process.test.js`, `dl_lit_project/tests/test_backend_helper_scripts.py`

**Interfaces:**
- Produces: `createPythonRunner({ pythonExec, dlLitProjectDir, repoRoot, dbPath, appSettingsEnv }) -> { pythonEnv(extra = {}), spawnPython(scriptPath, args, opts), runPythonJson(scriptPath, args, opts), parseJsonFromOutput(text) }`. `spawnPython` opts: `{ env, dbPath, corpusId, onStdoutLine, onStderrLine, lowPriority, label }` → `{ child, done: Promise<{ stdout, stderr, code }> }`; `--corpus-id <id>` appended when `corpusId` is non-null (as today); rejection `Error('Python script failed (<code>): <stderr || stdout>')`. `runPythonJson(scriptPath, args, { dbPath, corpusId, lowPriority })` → parsed JSON (today's behaviour). In `app.js`: `const pythonRunner = createPythonRunner({...})` at module level; `const { pythonEnv, spawnPython, runPythonJson, parseJsonFromOutput } = pythonRunner`; `async function getPdfPageCount(pdfPath) -> number | null`, `async function llmUploadPdf(pdfPath) -> string` (throws), `async function llmDeleteUploaded(name) -> void` (warns).
- Scripts: `pdf_page_count.py <pdf>` prints the count; `llm_upload_pdf.py <pdf>` prints `{"name": ...}`; `llm_delete_upload.py <name>` prints `ok`. Each exposes `main(argv, client=None)` so pytest can inject a fake client.

- [ ] **Step 1: Write the failing Jest tests** in `backend/tests/python-process.test.js` (use `process.execPath`? No: the scripts under test are tiny Python files written to a temp dir; `pythonExec` is `/home/jay/DT/venv/bin/python` if it exists else `python3`).

```js
test('pythonEnv merges appSettingsEnv under extra and sets PYTHONPATH/db vars', ...)
// appSettingsEnv: () => ({ RAG_FEEDER_GEMINI_MODEL: 'from-admin', X: '1' }); extra { RAG_FEEDER_GEMINI_MODEL: 'override' }
// -> env.RAG_FEEDER_GEMINI_MODEL === 'override', env.X === '1', env.PYTHONPATH startsWith(dlLitProjectDir), env.RAG_FEEDER_DB_PATH === dbPath
test('spawnPython forwards complete lines and flushes the partial tail on close', ...)
// script: print("a"); print("b", end="") ; sys.stderr.write("e\n") -> onStdoutLine ['a','b'], onStderrLine ['e']
test('spawnPython rejects once on non-zero exit with the partial line still forwarded', ...)
test('runPythonJson parses the last JSON line', ...)
test('app.js no longer imports spawnSync and has no PYTHONPATH literal', ...)
// const src = fs.readFileSync('../src/app.js'); expect(src).not.toMatch(/\bspawnSync\b/); expect(src).not.toMatch(/PYTHONPATH:/)
```

- [ ] **Step 2: Write the failing pytest** `dl_lit_project/tests/test_backend_helper_scripts.py`: generate a 3-page PDF with pikepdf; `pdf_page_count.main([path])` prints `3`; `llm_upload_pdf.main([path], client=FakeClient())` where `FakeClient.files.upload(file, config)` records `config['display_name']` and returns an object with `.name = 'files/x'` → stdout `{"name": "files/x"}`; a non-ASCII filename is copied to an ASCII name before upload (`display_name` is ASCII); `llm_delete_upload.main(['files/x'], client=FakeClient())` calls `files.delete(name='files/x')`. Load scripts the way `tests/test_keyword_search_script.py` does.
- [ ] **Step 3: Run** both → FAIL.
- [ ] **Step 4: Implement** the module, the scripts (`llm_upload_pdf.py` keeps the `OpenAICompatClient` local-handle branch and the non-ASCII copy from today's snippet), the async helpers, adopt `spawnPython` in the two extraction spawns with line handlers reproducing the Global-Constraints prefixes, drop the `spawnSync` import.
- [ ] **Step 5: Run** `npm test` and `pytest tests -q` → PASS. Manual: on the dev stack upload a small PDF via the Workspace, confirm extraction completes and the Logs panel shows `[get_bib_pages][corpus=...]` lines.
- [ ] **Step 6: Commit** `python children: one runner with Admin settings; page count, LLM upload/delete as async scripts`.

### Task 6: Delete the dead in-memory scheduler

**Files:**
- Modify: `backend/src/app.js` (`downloadWorkers`/`pipelineWorkers` ~2602-2603; `getOrInit*` ~2768-2810; `tickDownloadWorker` ~2930; `startDownloadWorker` … `stopAllDownloadWorkers` ~3030-3460; corpus delete calls to `pausePipelineWorker`/`stopDownloadWorker` ~4400; stub routes ~6398-6530)
- Delete: `backend/scripts/downloads_list.py`, `backend/scripts/ingest_runs.py`, `backend/scripts/ingest_stats.py` (and the comment referencing it)
- Test: `backend/tests/downloads.test.js` (existing oracle), `backend/tests/dead-scheduler.test.js` (source assertions)

**Interfaces:**
- Produces: `dbDrivenWorkerStatus(kind: 'pipeline' | 'download') -> object` returning exactly the JSON the six stub handlers return today (capture the current bodies first with `curl`/supertest in stub mode and freeze them in the new test).

- [ ] **Step 1: Freeze today's stub responses** in `backend/tests/dead-scheduler.test.js`: in stub mode GET/POST each of the six routes and `expect(res.body).toEqual(<captured literal>)`; plus `expect(src).not.toMatch(/\b(startDownloadWorker|tickDownloadWorker|getOrInitWorkerState|downloadWorkers|pipelineWorkers|emitPromotionEvent)\b/)` and `expect(fs.existsSync('../scripts/downloads_list.py')).toBe(false)` for the three scripts.
- [ ] **Step 2: Run** → the source/file assertions FAIL, the response assertions PASS (baseline).
- [ ] **Step 3: Delete** the listed functions and state, add `dbDrivenWorkerStatus`, remove the two no-op calls in corpus delete, `git rm` the three scripts, fix the comment.
- [ ] **Step 4: Run** `npm test` → PASS; `node --check backend/src/app.js`; `wc -l backend/src/app.js` is ≥ 450 lines smaller than before (report the number).
- [ ] **Step 5: Commit** `app.js: delete the dead in-memory scheduler and three unreferenced scripts`.

### Task 7: README note and dev-stack verification

**Files:**
- Modify: `README.md` (a short "Access model" paragraph under the architecture/auth section: membership rule, admin, WebSocket token in `Sec-WebSocket-Protocol`, 4401)

- [ ] **Step 1:** Write the paragraph (≤ 12 lines).
- [ ] **Step 2:** Dev stack (the backend container bind-mounts the checkout and nodemon reloads it; do not run `scripts/deploy.sh` on a branch): verify in the browser at the configured public URL: log in, Logs panel status "connected"; open the app as a second user not shared on the current corpus in another browser profile, run an extraction as the first user, confirm the second user sees none of its lines; un-share the second user from a shared corpus and confirm their Workspace moves to another corpus on the next poll; `curl --http1.1` WS probe without a token still returns 101.
- [ ] **Step 3: Commit** `README: access model and log socket authentication`.
