# Review fixes, phase 8: races and small fixes — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

Revised 2026-11-24 after the Codex pre-flight round 1 (`.superpowers/sdd/2026-11-24-review-fixes-8-races-and-small-fixes/preflight.md`, rulings in `progress.md`, section "Pre-flight round 1").

**Goal:** Close the small open rows of the reconciliation:
- session and corpus races in the frontend (FE-60b, FE-63, FE-61);
- a storage crash in the upload list (FE-64) and forced dev-server polling (FE-24);
- graph builds mixed in one load (BE-54) and a non-atomic corpus removal (BE-55);
- a stale backfill write (PY-34), a failed claim read as an empty queue (PY-54) and the scraper's own key reader (PY-27, DRY half).

FE-22 and BE-38 are closed without code (spec D5, D9).

**Architecture:** Five tasks grouped by surface.
1. Frontend session races (`App.svelte`).
2. Graph consistency (a version check in the backend file route, plus load ownership and node-detail ownership in `ThreeGraph.svelte`).
3. Backend transaction and dev config (`app.js` single-work removal, `vite.config.js`, CI list).
4. Python claims (`db_manager.py`, `worker.py`).
5. Python backfill and key (`backfill_openalex_topics.py`, `OpenAlexScraper.py`).

No new module, no schema change, no new dependency. One new Playwright spec (`dev-server-config.spec.ts`, Task 3) and one new pytest file (`test_worker_claim_errors.py`, Task 4).

**Tech Stack:** Python 3.11 (`/home/jay/DT/dl_lit_project/.venv/bin/python`), pytest, pytest-mock, sqlite3; Node 20, Express 5, better-sqlite3, Jest + supertest; Svelte 5 (legacy syntax in `App.svelte`) + Vite 7.3.1 (bundled chokidar 3), Playwright (mocked).

**Spec:** `docs/superpowers/specs/2026-11-24-review-fixes-8-races-and-small-fixes-design.md`

## Setup (controller, once, before Task 1)

The worktree has no `node_modules`.
- `cd backend && npm ci`
- `cd frontend && npm ci`
- `cd frontend && npx playwright install chromium`
- The Python venv `/home/jay/DT/dl_lit_project/.venv` already has pytest 9.0.2 and pytest-mock.

## Parallelism and shared files

| Task | Surface | Runs |
|---|---|---|
| 1 Session races (FE-60b, FE-63, FE-64) | frontend | first browser task |
| 2 Graph consistency (BE-54, FE-61) | backend + frontend | **after Task 1** |
| 3 Removal transaction + polling (BE-55, FE-24) | backend + config | **after Task 2** |
| 4 Claim errors (PY-54) | Python | parallel to all |
| 5 Backfill guard + key reader (PY-34, PY-27) | Python | parallel to all |

- **Browser runs are serialized.** Tasks 1, 2 and 3 run Playwright on port 5190 in the same worktree, and Playwright reuses a server it finds there (`playwright.config.ts:12-21`, `reuseExistingServer: true`). So they run one after another, and each runs only its own specs. The whole CI list runs once, in the final verification.
- `backend/src/app.js`: Task 2 (graph file route, `decorateGraph3dManifest`, new `graph3dManifestVersion`) and Task 3 (`DELETE /api/corpus/works/:workId`).
- `frontend/src/App.svelte` and `frontend/tests/helpers/mockApi.ts`: Task 1 creates `logIn` and `afterLateAnswer` there; Task 2 uses `afterLateAnswer`.
- `frontend/tests/tab-resources.spec.ts`: Task 1 only. `ThreeGraph.svelte`: Task 2 only. `.github/workflows/ci.yml`: Task 3 only.

## Global Constraints

- Work only in `/home/jay/DT/.worktrees/review-fixes-8`, on branch `review-fixes-8`. Do not push.
- Never open `dl_lit_project/data/literature.db` or any other dev or prod `*.db`; tests use `tmp_path` / `fs.mkdtempSync` DBs. Never let a test write the repo's `dl_lit_project/data/openalex_quota.json` (set `RAG_FEEDER_OPENALEX_QUOTA_PATH` into `tmp_path`).
- Python: `cd dl_lit_project && /home/jay/DT/dl_lit_project/.venv/bin/python -m pytest tests/<file> -q`; whole suite with `tests -q`. CI runs the whole suite, so new test files need no CI edit.
- Backend: `cd backend && npm test -- <file>`; full run `npm test -- --maxWorkers=2` (full parallel runs can time out suites that pass alone; compare with a `c574aba` baseline before blaming a change).
- Frontend: `cd frontend && E2E_FRONTEND_PORT=5190 npx playwright test tests/<spec> --workers=1`, mocked specs only (check `ss -ltn` first that 5190 is free). Never the default 5175 (the dev container serves master). Never `*.real*.spec.ts`, `pipeline-perf.real-pdf.spec.ts` or `upstream-metadata.spec.ts`. Every spec that fakes a token calls `mockLogSocket(page)`. `npx vite build` must pass. A new spec goes into the CI list (`ci.yml:54`) in the task that creates it.
- **Race tests never wait a fixed time** (no `waitForTimeout`). Each one:
  - holds the late request on a `deferred()` gate;
  - confirms that the new session or corpus has loaded before the release;
  - waits with `afterLateAnswer` after the release.
- Exact strings from the spec:
  - D4: `[uploads] could not save the upload list:` (followed by the error message)
  - D8: 409 body `{ error: 'The graph snapshot was rebuilt; reload the graph.', code: 'snapshot_rebuilt' }`, header `Cache-Control: no-store`
  - D10: `[<label>] <n> work(s) changed while the batch was fetched; left for the next run`
  - D11: `Job <id>: the failure could not be recorded (<error>); exiting with code 75 so start-up recovery requeues it.`; the direct-cycle prefix `enrich cycle failed: ` (built by the existing `_run_direct_cycles`)
  - D12: `[WARN] OPENALEX_API_KEY not set. OpenAlex daily credits may be severely limited without an API key.` (the existing utils text)
- Exact values from the spec:
  - D6: `watch: { usePolling: false }`; chokidar's env override decides inside the containers.
  - D8: one version check, no fd streaming; `sendFile` and the gzip stream stay; the middleware order stays.
  - D10: the guard is chosen by the pass, never by input.
  - D11: default `raise_errors=False`; exit code `JOB_TIMEOUT_EXIT_CODE` (75).
- No compose, Caddy or schema change. No change to `index.js` (D9 closed).
- Each commit message names the rows it closes and ends with:
  ```
  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01U3c4gfg7jQvvmPdpLNBN5T
  ```

## Review Focus

1. **A download that fails after a sign-out.** Its distinctive error text must not reach the next user. Test in Task 1.
2. **A client that refuses gzip, or asks for a range.** With a matching `v` it must still get `sendFile`'s plain file and a 206. Test in Task 2.
3. **An outsider with a matching or a stale `v`.** Both get 403 from the scope check, never a 409 that reveals the build. Test in Task 2.
4. **A job whose failure cannot be recorded.** It must not stay `running` under a live worker; the restart requeues it. Test in Task 4.
5. **`CHOKIDAR_USEPOLLING` set to compose's `1`, or to ` TRUE `.** The watcher Vite builds polls. Test in Task 3.

---

### Task 1: Frontend session races (FE-60b, FE-63, FE-64)

**Files:**
- Modify: `frontend/src/App.svelte`:
  - `resetFrontendState` (`:2212-2270`): `exportBusy = false` in the `signOut` branch only (`exportBusy` stays out of `initialWorkspaceState()`);
  - `:766-788` `persistStoredUploads`, plus a module flag `uploadStorageWarned` next to it;
  - `:3863-3900` `startSearch`;
  - `:4266-4296` `handleExportDownload`, `:4298-4314` `handleDownloadedCorpusFile`, `:4369-4388` `handleDownloadSeedDocument`, `:4390-4410` `handleSeedCandidateFile`.
- Modify: `frontend/tests/helpers/mockApi.ts`: add `logIn` (moved from `session.spec.ts:48-54`) and `afterLateAnswer`; `session.spec.ts` imports `logIn`.
- Test: `frontend/tests/seed-selection.spec.ts`, `frontend/tests/session.spec.ts`, `frontend/tests/tab-resources.spec.ts`, `frontend/tests/seed-upload.spec.ts` (all in the CI list).

**Interfaces:**
- Produces, in `helpers/mockApi.ts`:
  - `logIn(page: Page, username: string): Promise<void>`: today's `session.spec.ts` body.
  - `afterLateAnswer(page: Page, response: Promise<Response>): Promise<void>`: `await (await response).finished()`, then `await page.evaluate(() => new Promise((r) => requestAnimationFrame(() => setTimeout(r, 0))))`.
- Consumes: nothing from other tasks.

- [ ] **Step 1: Write the failing tests.** Each release follows `const answered = page.waitForResponse(<url predicate>)`, then `gate.resolve()`, then `await afterLateAnswer(page, answered)`.

```ts
// seed-selection.spec.ts — boot(): State gains `fileGate?: Promise<void>`, `fileStatus?: number`, `fileRequests: number`.
// '/api/auth/login' -> { token: 'second-token', user: { id: 2, username: 'second', last_corpus_id: 1, is_admin: false } }
// '/api/auth/me' by bearer: 'second-token' -> mePayload({ id: 2, username: 'second', admin: false, ... }), else today's 'reader'
// '/api/seed/sources/search/70/candidates/readable/file': count, await fileGate, then
//   fileStatus === 500 ? fulfil(route, { error: 'late-candidate-error-7f3' }, 500)
//   : route.fulfill({ status: 200, contentType: 'application/pdf',
//       headers: { 'Content-Disposition': 'attachment; filename="readable.pdf"' }, body: '%PDF-1.4 x' })
// candidates as in the readable-candidate test (:217-225)
test('a candidate file that arrives after a sign-out is not saved', async ({ page }) => {
  // downloads = []; page.on('download', ...); expandSeed; click 'Readable copy' .seed-download-link; poll fileRequests === 1
  // 'Log out'; logIn(page, 'second'); expect(getByTestId('auth-user')).toHaveText('second'); seed-panel visible
  // release + afterLateAnswer
  // expect(downloads).toHaveLength(0); getByText(/Downloaded/) count 0
})
test('a candidate file failing after a sign-out shows nothing', async ({ page }) => {
  // fileStatus 500; same flow -> getByText('late-candidate-error-7f3') count 0
})
// session.spec.ts — '/api/exports/bundle': each request gets its own gate (exportGates[n]);
// answers application/zip with 'Content-Disposition: attachment; filename="bundle.zip"'
test('an old export neither saves nor unlocks the next user\'s export', async ({ page }) => {
  // logIn alice; goto '#/downloads'; click export-bundle-button; poll export request 1 seen
  // 'Log out'; logIn bob; auth-user 'bob'; goto '#/downloads'
  // expect(export-bundle-button).toBeEnabled(); click it; poll export request 2 seen; button disabled
  // release request 1 + afterLateAnswer: downloads length 0; getByText(/export downloaded/) count 0; button still disabled
  // release request 2 + afterLateAnswer: downloads length 1; button enabled
})
test('a search launched before a sign-out does not reach the next session', async ({ page }) => {
  // preview answers { count: 5, threshold: 100000 }; POST /api/keyword-search on a gate
  // Query 'economics'; Search; poll POST seen; 'Log out'; logIn bob; auth-user 'bob'
  // release fulfil(route, { runId: 7, status: 'running' }, 202) + afterLateAnswer
  // getByText(/Fetching in the background/) count 0
  // expect(await page.evaluate(() => (window as any).__dtRefresh.reason())).not.toBe('active')
})
// tab-resources.spec.ts — boot state gains `searchGate: Promise<void> | null` (awaited before the 202 at :118-121)
// and `exportGate: Promise<void> | null` for '/api/exports/bundle'; the existing `delays.me` covers the held /me
test('a search launched before a corpus switch does not mark the new corpus as searching', async ({ page }) => {
  // previewCount 5; searchGate held; Search; poll requests('/api/keyword-search') === 1
  // select '2' in .header-corpus-picker; wait for the picker to read '2' and for the /api/auth/me response after the select
  // release + afterLateAnswer: no /Fetching in the background/; __dtRefresh.reason() is not 'active'
})
test('a launch answered between the saved switch and its /me is reset by the commit', async ({ page }) => {
  // searchGate held; Search; poll the POST; hold /me (gate) ; select '2' and poll the select request
  // release the launch + afterLateAnswer; then release /me and wait for the picker to read '2' and the /me response
  // no /Fetching in the background/; reason is not 'active'; state.searchBodies[0].corpusId === 1
})
test('an export keeps the session\'s export lock across a corpus switch', async ({ page }) => {
  // goto '#/downloads'; exportGate held; click export-bundle-button; poll the export request
  // switch to corpus 2 and wait for its /me; expect(export-bundle-button).toBeDisabled()
  // release + afterLateAnswer: one download event; button enabled
})
// seed-upload.spec.ts — the first test's flow (:68-96) with an init script:
//   const real = Storage.prototype.setItem
//   Storage.prototype.setItem = function (k, v) { if (String(k).startsWith('rag-feeder:uploads:'))
//     throw new DOMException('full', 'QuotaExceededError'); return real.call(this, k, v) }
test('a full localStorage does not fail an upload', async ({ page }) => {
  // no wait on the import request: assert the upload row directly
  // expect(<upload row>).toContainText('Imported 2 works (1 skipped, 2 added to corpus).'); row has no 'failed' status
  // console warnings starting '[uploads] could not save the upload list:' === 1
})
```

- [ ] **Step 2: Run them and verify they fail.** Run `cd frontend && E2E_FRONTEND_PORT=5190 npx playwright test tests/seed-selection.spec.ts tests/session.spec.ts tests/tab-resources.spec.ts tests/seed-upload.spec.ts --workers=1`. Expected: the new tests FAIL, except the corpus-switch export test, which pins today's behaviour (the lock survives a switch today) and passes:
  - a download event fires and `Downloaded readable.pdf.` shows;
  - `late-candidate-error-7f3` shows;
  - bob's button is disabled while alice's export is held;
  - `Fetching in the background` shows and the reason is `active`;
  - the upload row never reaches `Imported 2 works`.

  All other tests pass.
- [ ] **Step 3: Implement D1, D2 and D4.**
  - D1: each of the four handlers takes `const session = sessionEpoch` before its request. Right after the `await` it returns when `session !== sessionEpoch`. Its `catch` returns on `error?.status === 401 || session !== sessionEpoch`. In `handleExportDownload`, the `finally` sets `exportBusy = false` only when `session === sessionEpoch`. `resetFrontendState` sets `exportBusy = false` when `signOut` is true.
  - D2: in `startSearch`, after the corpus check, `const session = sessionEpoch`, `const scope = corpusScopeEpoch`, `const stale = () => session !== sessionEpoch || scope !== corpusScopeEpoch`. `if (stale()) return` comes after `runKeywordSearch` (before `refreshQuota` and `startedSearchRunIds`), after `loadSeedSources` and after `focusSeedSource`. The `catch` returns on `error?.status === 401 || stale()`. `handleSelectCorpus` is unchanged.
  - D4: `try { window.localStorage.setItem(key, JSON.stringify(persisted)) } catch (error) { if (!uploadStorageWarned) { uploadStorageWarned = true; console.warn('[uploads] could not save the upload list:', error?.message) } }`.
  - Add `logIn` and `afterLateAnswer` to `helpers/mockApi.ts`.
- [ ] **Step 4: Run the four specs again, then `npx vite build`.** Expected: all pass.
- [ ] **Step 5: Commit**: `frontend: downloads and search launches from an ended session are dropped; the export lock is session-wide; a full localStorage no longer fails an upload (FE-60b, FE-63, FE-64)`

---

### Task 2: Graph rebuild check, load ownership and node-detail ownership (BE-54, FE-61)

Runs after Task 1 (browser runs are serialized; it uses `afterLateAnswer`).

**Files:**
- Modify: `backend/src/app.js`:
  - new `graph3dManifestVersion(key)` next to `decorateGraph3dManifest` (`:600-635`), which uses it for its mtime fallback;
  - the file route handler `:5874-5917` (one check before the existence check; the rest unchanged).
- Modify: `frontend/src/components/ThreeGraph.svelte`:
  - `:80-84` (`nodeDetailSeq`, `graphLoadSeq`);
  - `showAllNodes` (`:946`), `loadNodeDetail` (`:1112-1133`), `clearSelection` (`:1174`);
  - `load3DGraph` (`:1215-1262`).
- Test: `backend/tests/graph-read-path.test.js`, `backend/tests/graph-access.test.js`, `frontend/tests/graph-3d.spec.ts` (in the CI list).

**Interfaces:**
- Produces (module-local, not exported): `graph3dManifestVersion(key: string) -> string | null`. It reads `graph3dSnapshotPath(key, 'manifest.json')` once. When `generated_at` is present it returns `String(manifest.generated_at)`, else `String(Math.floor(mtimeMs))`, and `null` when the manifest is unreadable.
- Consumes: `afterLateAnswer` (Task 1).

- [ ] **Step 1: Write the failing tests.**

```js
// graph-read-path.test.js — own snapshot dir, as 'the cluster detail route is gone' (:327-340):
// key 'f6f6f6f6f6f6f6f6f6f6'; manifest { schema_version: 2, generated_at: 'A', files: {} };
// nodes.bin = Buffer.from([1, 2, 3, 4]); nodes.bin.gz = zlib.gzipSync(that)
test('a versioned file request is checked against the live build', async () => {
  // ?v=A -> 200, bytes [1,2,3,4]
  // ?v=B -> 409, body { error: 'The graph snapshot was rebuilt; reload the graph.', code: 'snapshot_rebuilt' },
  //   cache-control 'no-store'
  // no v -> 200
})
test('a matching version keeps sendFile for a client without gzip, ranges included', async () => {
  // .set('Accept-Encoding', 'identity') ?v=A -> 200, content-type /application\/octet-stream/, content-length '4', no content-encoding
  // same plus .set('Range', 'bytes=0-1') -> 206, 2 bytes
})
// graph-access.test.js, next to the lonely-user check (:205-208)
test('an outsider gets 403 for a snapshot file whatever its version', async () => {
  // lonelyToken: `/api/graph/3d/snapshot/${fx.k1}/clusters.json?v=gen-${fx.k1}` -> 403 { error: 'No access to this corpus' }
  // ...?v=stale -> 403, same body
})
// the existing build-streaming test (:307-325) stays as it is and must still pass
```

```ts
// graph-3d.spec.ts — the manifest mock gains a build counter: manifest n lists every resource URL with `?v=${['A', 'B'][n]}`
test('a 409 on a snapshot file reloads the manifest once and loads build B', async ({ page }) => {
  // nodes.bin?v=A -> 409 { error: 'The graph snapshot was rebuilt; reload the graph.', code: 'snapshot_rebuilt' }
  // /Loaded 2 nodes and 1 edges? from snapshot\./ visible; manifest requests === 2
  // every resource request after the 409 has searchParams v === 'B'
})
test('a 409 after the graph unmounts starts no request', async ({ page }) => {
  // nodes.bin?v=A on a gate; open the graph; poll the nodes.bin request; click the workspace tab (graph unmounts)
  // release with 409 + afterLateAnswer: manifest requests === 1; no request with v=B
})
// node detail: '/api/graph/3d/node/1' and '/node/2' each on a per-request gate list
test('a late detail failure for another node leaves the selected node\'s detail', async ({ page }) => {
  // select 'Graph seed work' (held); select 'Referenced work' (answered) -> 'Identifiers: 10.1000/example'
  // release node 1 with 500 + afterLateAnswer: identifiers still visible; 'Loading metadata...' count 0
})
test('an older answer for the same node neither shows nor caches', async ({ page }) => {
  // node 1 request 1 on gate 1; node 2 answers; node 1 request 2 on gate 2 (select 1, 2, 1)
  // release gate 2 with { id: 1, title: 'Graph seed work', doi: '10.1000/seed-v2' } -> 'Identifiers: 10.1000/seed-v2'
  // release gate 1 with doi '10.1000/seed-v1' + afterLateAnswer: still seed-v2
  // select 'Referenced work', then 'Graph seed work': node-1 requests === 2; 'Identifiers: 10.1000/seed-v2'
})
test('deselecting ends the pending detail request', async ({ page }) => {
  // select node 2 (answered, cached); select node 1 (held); press Escape; select node 2 again (cache hit)
  // release node 1 with 500 + afterLateAnswer: 'Identifiers: 10.1000/example' visible; 'Loading metadata...' count 0
})
test('an older detail answer does not end the newer node\'s loading', async ({ page }) => {
  // nodes 1 and 2 held; select 1, then 2; release node 1 with 200 { id: 1, doi: '10.1000/seed' } + afterLateAnswer
  // 'Loading metadata...' visible; '10.1000/seed' count 0; release node 2 -> 'Identifiers: 10.1000/example'
})
```

- [ ] **Step 2: Run them and verify they fail.** Run `cd backend && npm test -- tests/graph-read-path.test.js tests/graph-access.test.js` and `cd frontend && E2E_FRONTEND_PORT=5190 npx playwright test tests/graph-3d.spec.ts --workers=1`. Expected:
  - FAIL: `?v=B` answers 200, the 409 shows as an error, a superseded node-1 answer wins the cache, and node 1's failure clears node 2's identifiers.
  - PASS already: the outsider and sendFile tests (they pin today's behaviour), and the unmount test (nothing retries today).
- [ ] **Step 3: Implement D8 and D3.**
  - File route: after the key and file-name checks, `if (typeof req.query?.v === 'string' && graph3dManifestVersion(key) !== req.query.v)` set `Cache-Control: no-store` and answer the 409. Nothing else in the route changes; `requireGraph3dScopeAccess` stays ahead of the handler.
  - `decorateGraph3dManifest` keeps `generated_at` from its manifest and uses the helper's mtime rule only as the fallback.
  - `load3DGraph`:
    - `const load = ++graphLoadSeq`; `const options = { maxNodes, relationship, groupBy, corpusId: selectedGraphCorpusId }` captured once; `const owns = () => componentMounted && load === graphLoadSeq`.
    - Check `owns()` after the manifest, before the resources, before the retry, after the retry's manifest, and before writing `graphData`/`graphStatus`. A load that no longer owns returns.
    - The one retry runs on `error?.status === 409` with the same `options`.
    - The `catch` writes the status only while `owns()`. The `finally` clears `graphLoading` only when `load === graphLoadSeq`. The reset at its start bumps `nodeDetailSeq` and clears `nodeDetailLoading`.
  - `loadNodeDetail`: `const seq = ++nodeDetailSeq` right after the id check. After the fetch, the cache write, the panel write (still only while that node is selected), the `catch` and the `finally` each act only when `seq === nodeDetailSeq`.
  - `clearSelection` and `showAllNodes`: `nodeDetailSeq += 1; nodeDetailLoading = false`.
- [ ] **Step 4: Run both commands again, then `npx vite build`.** Expected: all pass, the build-streaming test included.
- [ ] **Step 5: Commit**: `graph: a versioned snapshot file request answers 409 after a rebuild and the graph reloads once while it owns the load; node detail shows and caches only the newest request (BE-54, FE-61)`

---

### Task 3: Single-work removal in one transaction, and no forced polling (BE-55, FE-24)

Runs after Task 2 (both edit `backend/src/app.js`; browser runs are serialized).

**Files:**
- Modify: `backend/src/app.js:5507-5530` (`DELETE /api/corpus/works/:workId`).
- Modify: `frontend/vite.config.js:54-56`.
- Create: `frontend/tests/dev-server-config.spec.ts`
- Modify: `.github/workflows/ci.yml:54` (append `tests/dev-server-config.spec.ts` to the Playwright list).
- Test: `backend/tests/corpus-sharing.test.js`

**Interfaces:** none.

- [ ] **Step 1: Write the failing tests.**

```js
// corpus-sharing.test.js, next to the removal tests (:338-391)
test('a failed marker delete keeps the membership and the marker', async () => {
  // admin login; workId = seedCorpusWork(corpusId, 'Marked Work')
  // withDb: CREATE TABLE IF NOT EXISTS seed_candidates_in_corpus (DDL of seed.js:629-637);
  //   INSERT (corpus_id, 'search', '1', 'k1', workId);
  //   CREATE TRIGGER fail_marker_delete BEFORE DELETE ON seed_candidates_in_corpus BEGIN SELECT RAISE(ABORT, 'marker delete failed'); END
  // DELETE /api/corpus/works/${workId} -> 500
  // withDb: membership row still there; marker row still there; DROP TRIGGER fail_marker_delete
})
```

```ts
// dev-server-config.spec.ts (no page)
import { execFileSync } from 'node:child_process'
import { fileURLToPath } from 'node:url'
// const frontendDir = fileURLToPath(new URL('..', import.meta.url))
// const probe = "import { createServer } from 'vite'; const s = await createServer({ configFile: 'vite.config.js', logLevel: 'silent', optimizeDeps: { noDiscovery: true } }); console.log(JSON.stringify({ usePolling: s.watcher.options.usePolling })); await s.close()"
// function effectivePolling(value?: string) { const env = { ...process.env }; delete env.CHOKIDAR_USEPOLLING
//   if (value !== undefined) env.CHOKIDAR_USEPOLLING = value
//   const out = execFileSync(process.execPath, ['--input-type=module', '-e', probe], { cwd: frontendDir, env, encoding: 'utf8' })
//   return JSON.parse(out.trim().split('\n').pop()!).usePolling }
test('the watcher Vite builds polls only when CHOKIDAR_USEPOLLING turns it on', () => {
  // effectivePolling() === false; effectivePolling('1') === true; effectivePolling(' TRUE ') === true
})
```

- [ ] **Step 2: Run them and verify they fail.** Run `cd backend && npm test -- tests/corpus-sharing.test.js` and `cd frontend && E2E_FRONTEND_PORT=5190 npx playwright test tests/dev-server-config.spec.ts --workers=1`. Expected: FAIL. The marker test sees the membership gone; the probe sees `usePolling: true` with the variable unset. The `1` and ` TRUE ` cases already pass.
- [ ] **Step 3: Implement D7 and D6.**
  - D7: `const removed = authDb.transaction(() => { … })()`. Inside, the `corpus_works` delete runs; on `changes === 0` it returns `0`; otherwise the marker delete runs (same `tableExists` check) and it returns `changes`. The route answers 404 on `0`, else `{ removed: true, work_id }`; the `catch` is unchanged.
  - D6: `watch: { usePolling: false }`. The comment names chokidar's `CHOKIDAR_USEPOLLING` override (`node_modules/vite/dist/node/chunks/config.js:13887-13893`) and the compose files that set it to `1` for the bind mount, and says that Playwright and CI leave it unset.
  - Append the new spec to the `ci.yml:54` list.
- [ ] **Step 4: Run both again, then `npx vite build`.** Expected: all pass. `git grep -n "usePolling: true" -- frontend/vite.config.js` prints nothing.
- [ ] **Step 5: Commit**: `backend: removing one work from a corpus is one transaction; vite no longer forces polling outside the containers (BE-55, FE-24)`

---

### Task 4: A failed claim fails the worker's job or cycle, and an unrecordable failure restarts the worker (PY-54)

**Files:**
- Modify: `dl_lit_project/dl_lit/db_manager.py:1730-1859` (`claim_enrich_batch`: parameter, docstring, `except`).
- Modify: `backend/scripts/daemon/worker.py`:
  - `:1249-1255` and `:1596`: pass `raise_errors=True`;
  - `_process_job`'s `except` (`:1785-1803`): guard `mark_job_failed`, then exit after the `finally`.
- Create: `dl_lit_project/tests/test_worker_claim_errors.py`

**Interfaces:**
- Produces: `DatabaseManager.claim_enrich_batch(self, *, limit: int, corpus_id: int | None, claimed_by: str, lease_seconds: int = 900, max_attempts: int | None = None, target_ids: list[int] | None = None, raise_errors: bool = False) -> list[dict]`
- Consumes:
  - `claim_download_batch(..., raise_errors=True)` (exists since #82);
  - `_deny_next_begin(db)` from `tests.test_cli_download_leases`;
  - `load_worker` from `tests._worker_import`;
  - `JOB_TIMEOUT_EXIT_CODE`, `_recover_stale_running_jobs()` (start-up), `daemon._exit` (test seam).

- [ ] **Step 1: Write the failing tests** in `test_worker_claim_errors.py`.
  - The daemon: build it as `test_worker_watchdog._daemon` does (a local copy with `job_timeout=30`, so no watchdog fires; `daemon._exit = exits.append`).
  - Each job: insert it `running` under `daemon.worker_id` (`corpus_id` 1), then call `daemon._process_job(job, generation=daemon._begin_work())`, and read `status, result_json` back.
  - Denying the claim's BEGIN only: `real = daemon.db.claim_enrich_batch; monkeypatch.setattr(daemon.db, "claim_enrich_batch", lambda **kw: (_deny_next_begin(daemon.db), real(**kw))[1])`, and the same for `claim_download_batch`.

```python
def test_claim_enrich_batch_raises_only_when_asked(tmp_path):
    # DatabaseManager(tmp_path / "lit.db"); _deny_next_begin(db)
    # claim_enrich_batch(limit=1, corpus_id=None, claimed_by="w") == []
    # _deny_next_begin(db); with pytest.raises(sqlite3.Error): claim_enrich_batch(..., raise_errors=True)
    # assert not db.conn.in_transaction

@pytest.mark.parametrize("job_type, params, claim", [
    ("enrich", {"limit": 1}, "claim_enrich_batch"),
    ("download", {"batchSize": 1}, "claim_download_batch"),
    ("pipeline_tick", {"config": {"promoteBatchSize": 1}}, "claim_enrich_batch"),
])
def test_a_failed_claim_fails_the_job(monkeypatch, tmp_path, job_type, params, claim):
    # status == "failed"; "not authorized" in json.loads(result_json)["error"]; exits == []

def test_a_failure_that_cannot_be_recorded_restarts_the_worker_and_requeues_the_job(monkeypatch, tmp_path):
    # claim denied as above, and monkeypatch.setattr(daemon, "mark_job_failed",
    #   lambda *a: (_ for _ in ()).throw(sqlite3.OperationalError("database is locked")))
    # logged lines (monkeypatch mod.log) contain
    #   f"Job {job['id']}: the failure could not be recorded (database is locked); exiting with code 75 so start-up recovery requeues it."
    # exits == [75]; the job row is still 'running' under daemon.worker_id
    # restart: mod.PipelineDaemon(str(tmp_path / "lit.db"), role="enrich") -> the job row is 'pending', worker_id NULL

def test_a_failed_claim_in_a_direct_cycle_reaches_the_loop(monkeypatch, tmp_path):
    # role "enrich"; wrap claim_enrich_batch as above
    # with pytest.raises(RuntimeError, match=r"^enrich cycle failed: "): daemon._run_direct_cycles()
```

- [ ] **Step 2: Run them and verify they fail.** Run `cd dl_lit_project && /home/jay/DT/dl_lit_project/.venv/bin/python -m pytest tests/test_worker_claim_errors.py -q`. Expected: FAIL. `raise_errors` is an unexpected keyword, the jobs end `completed`, the unrecordable-failure test never reaches `mark_job_failed`, and the direct cycle returns `None`.
- [ ] **Step 3: Implement D11.**
  - `claim_enrich_batch`: add the parameter. The `except sqlite3.Error` rolls back as today, then `if raise_errors: raise`, else `return []`. The docstring takes `claim_download_batch`'s sentence.
  - The worker passes `raise_errors=True` in `do_enrich` and `do_download`.
  - `_process_job`: `record_failed = False` before the `try`. In the `except`, `mark_job_failed` runs inside `try`/`except sqlite3.Error as write_error`. That handler logs the D11 line, sets `updated = False` and `record_failed = True`, and the existing "not recorded" log lines are skipped when `record_failed` is set. The error heartbeat still runs. After the `finally` (heartbeat thread stopped), `if record_failed: self._exit(JOB_TIMEOUT_EXIT_CODE)`.
  - No other change: `_run_direct_cycles`, the watchdog and start-up recovery stay as they are.
- [ ] **Step 4: Run the new file, then `tests/test_cli_download_leases.py`, `tests/test_worker_watchdog.py`, `tests/test_worker_recovery.py`, `tests/test_hung_items.py`, then the whole suite.** Expected: all passed; `test_a_failed_claim_is_not_an_empty_queue` still passes (default unchanged).
- [ ] **Step 5: Commit**: `worker: a failed enrich or download claim fails the job or cycle; a failure that cannot be recorded restarts the worker so start-up recovery requeues the job (PY-54)`

---

### Task 5: Guarded backfill writes, and one OpenAlex key reader (PY-34, PY-27)

**Files:**
- Modify: `backend/scripts/backfill_openalex_topics.py`:
  - `doi_rows` (`:86-105`), `write_results` (`:154-166`), `run_pass` (`:178-224`);
  - delete `mark_fetched` (`:169-175`) and its call (`:219-221`);
  - pass 1 rows in `main` (`:250-272`), pass 2 call (`:276-286`).
- Modify: `dl_lit_project/dl_lit/OpenAlexScraper.py`: imports (`:17-25`), delete `:58`, `__init__` `:576-583`.
- Test: `dl_lit_project/tests/test_doi_rule_scripts.py` (the annotation test at `:157-165` changes, plus new tests), `dl_lit_project/tests/test_hygiene.py`.

**Interfaces:**
- Produces:
  - `doi_rows(conn, *, scope_clause: str, limit_clause: str) -> tuple[list[tuple[int, str, str]], int]`: `(work_id, key, raw_doi)`, where `raw_doi` is the row's `doi` exactly as read.
  - `WRITE_GUARDS: dict[str, str]`, the SQL fragment per pass:
    - `"openalex"`: `"openalex_id = ? AND (topics_fetched IS NULL OR topics_fetched = 0)"`;
    - `"doi"`: `"doi = ? AND (openalex_id IS NULL OR openalex_id = '') AND (topics_fetched IS NULL OR topics_fetched = 0)"`.
  - `write_results(conn, updates, guard: str) -> list[int]`:
    - updates are `(work_id, raw, topic, subfield, field, domain, topics_json)`;
    - one `UPDATE works SET … topics_fetched = 1 WHERE id = ? AND <WRITE_GUARDS[guard]>` per row, one commit;
    - returns the ids whose `rowcount` was 1.
  - `run_pass(conn, rows, key, build_filter, mailto, batch_size, label, guard) -> tuple[int, int, int]`: rows are `(work_id, token, raw)`. Pass 1 rows are `(id, normalize_openalex_id(openalex_id), openalex_id)`.
- Consumes: `utils.get_openalex_api_key()`, `utils._warn_missing_openalex_api_key()` (both exist).

- [ ] **Step 1: Write the failing tests.** In the backfill tests, the fake request first changes the row through a second `sqlite3` connection (commit), then answers.

```python
# test_doi_rule_scripts.py
# the annotation test now expects "return": tuple[list[tuple[int, str, str]], int]

def test_backfill_pass2_skips_a_row_whose_doi_changed_during_the_request(tmp_path, monkeypatch, capsys, backfill):
    # rows 1 '10.1000/ONE', 2 '10.1000/TWO' (matched); change: UPDATE works SET doi = '10.1000/OTHER' WHERE id = 1
    # answer works for one and two
    # _topic_row(1) == (None, None, None); _topic_row(2) == ("T2", "T2-field", 1)
    # "[doi] 1 work(s) changed while the batch was fetched; left for the next run" in capsys.readouterr().err

def test_backfill_pass2_skips_a_row_that_gained_an_openalex_id(tmp_path, monkeypatch, capsys, backfill):
    # rows 1 '10.1000/ONE', 2 '10.1000/TWO'; change: UPDATE works SET openalex_id = 'W9' WHERE id = 1
    # (the fake answers pass 1's filter for W9 with no results)
    # _topic_row(1) == (None, None, None); row 2 updated; the '[doi] 1 work(s) changed …' line is logged

def test_backfill_pass1_skips_a_row_whose_openalex_id_changed(tmp_path, monkeypatch, capsys, backfill):
    # rows 1 openalex_id 'W1', 2 'W2' (matched); change: row 1 -> 'W9'; answer W1 and W2 ("id": "https://openalex.org/W1" ...)
    # row 1 topics_fetched None; row 2 updated; '[id] 1 work(s) changed …' logged

def test_backfill_unchanged_rows_log_no_changed_line(tmp_path, monkeypatch, capsys, backfill):
    # one matched DOI row, no change -> updated; 'changed while the batch was fetched' not in err

# test_hygiene.py — each test reloads nothing; it resets utils._missing_openalex_api_key_warned via monkeypatch
def test_the_scraper_strips_the_api_key(monkeypatch):
    # setenv OPENALEX_API_KEY "  sekret-key-123\n" -> OpenAlexCrossrefSearcher(rate_limiter=_NoRateLimit()).api_key == "sekret-key-123"

def test_a_blank_primary_key_falls_back_to_the_rag_feeder_key(monkeypatch):
    # setenv OPENALEX_API_KEY "   ", RAG_FEEDER_OPENALEX_API_KEY "fallback-key" -> api_key == "fallback-key"

def test_the_missing_key_warning_prints_once(monkeypatch, capsys, tmp_path):
    # delenv OPENALEX_API_KEY and RAG_FEEDER_OPENALEX_API_KEY; setenv RAG_FEEDER_OPENALEX_QUOTA_PATH tmp_path / "quota.json"
    # monkeypatch utils._missing_openalex_api_key_warned False and utils.time.sleep to a no-op
    # OpenAlexCrossrefSearcher(rate_limiter=_NoRateLimit()) -> api_key is None
    # utils.openalex_request_json(url="https://api.openalex.org/works", params={"filter": "doi:x"},
    #   session=<stub: get() returns status_code 200, headers {}, json() {"results": []}, raise_for_status() no-op>,
    #   rate_limiter=_FreeLimiter(), retries=1)
    # capsys.readouterr().out.count("[WARN] OPENALEX_API_KEY not set.") == 1   (2 on c574aba)

def test_the_scraper_has_no_key_reader_of_its_own():
    # source of dl_lit/OpenAlexScraper.py contains neither "getenv('OPENALEX_API_KEY')" nor "_missing_api_key_warned"
```

- [ ] **Step 2: Run them and verify they fail.** Run `cd dl_lit_project && /home/jay/DT/dl_lit_project/.venv/bin/python -m pytest tests/test_doi_rule_scripts.py tests/test_hygiene.py -q`. Expected: FAIL. The annotation differs, the changed rows get topics, the key keeps its whitespace, the blank key wins over the fallback, and the warning prints twice.
- [ ] **Step 3: Implement D10 and D12.**
  - `run_pass` maps `token -> (work_id, raw)` and builds updates with `raw`. It calls `write_results(conn, updates, guard)` and counts `matched` and `fielded` over the returned ids only. It logs the D10 line when `len(updates) - len(written) > 0`. `seen_work_ids` and `mark_fetched` go.
  - `main` passes `guard="openalex"` and `guard="doi"`.
  - Scraper: `self.api_key = get_openalex_api_key()`; `if not self.api_key: _warn_missing_openalex_api_key()`. Import both from `.utils`; delete `_missing_api_key_warned` and the scraper's own print.
- [ ] **Step 4: Run both files, then `tests/test_worker_settings.py` and the whole suite.** Expected: all passed. `git grep -n "getenv('OPENALEX_API_KEY')" -- dl_lit_project/dl_lit` prints nothing.
- [ ] **Step 5: Commit**: `backfill: topics are written only while the row is still in the state the pass selected it in; the scraper uses the shared OpenAlex key reader (PY-34, PY-27)`

---

## Final verification (controller, after all tasks)

- `cd dl_lit_project && /home/jay/DT/dl_lit_project/.venv/bin/python -m pytest tests -q`: all passed.
- `cd backend && npm test -- --maxWorkers=2`: all passed.
- `cd frontend && npx vite build`, then, once, the whole CI Playwright list from `ci.yml:54` (now with `tests/dev-server-config.spec.ts`): `E2E_FRONTEND_PORT=5190 npx playwright test <list> --workers=1`. All pass.
- `git diff c574aba --stat` touches no compose file, no Caddyfile, no `backend/src/index.js` and no schema DDL.
- The inventory's reconciliation section records FE-22 and BE-38 as closed (spec D5, D9), and the rows of this phase as done.
- After merge: `docker exec rag_feeder_frontend printenv CHOKIDAR_USEPOLLING` prints `1`, and `docker logs rag_feeder_frontend` shows Vite's restart after the config change with no error. Never the dev DB.
