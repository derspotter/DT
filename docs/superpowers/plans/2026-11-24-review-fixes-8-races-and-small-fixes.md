# Review fixes, phase 8: races and small fixes — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

Revised 2026-11-24 after the Codex pre-flight rounds 1 to 4 (`.superpowers/sdd/2026-11-24-review-fixes-8-races-and-small-fixes/preflight.md` to `preflight-4.md`; rulings in `progress.md`).

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
- `frontend/src/App.svelte`: Task 1 (handlers, `startSearch`, `loadSeedSources`, reset, upload list) and Task 2 (the `{#key}` around the graph, `:5984`).
- `frontend/tests/helpers/mockApi.ts`: Task 1 adds `logIn`, `trackFetches`, `fetchStats` and `afterBodyRead`; Task 2 uses the last three.
- `frontend/tests/tab-resources.spec.ts`: Task 1 only. `ThreeGraph.svelte`: Task 2 only. `.github/workflows/ci.yml`: Task 3 only.

## Global Constraints

- Work only in `/home/jay/DT/.worktrees/review-fixes-8`, on branch `review-fixes-8`. Do not push.
- Never open `dl_lit_project/data/literature.db` or any other dev or prod `*.db`; tests use `tmp_path` / `fs.mkdtempSync` DBs. Never let a test write the repo's `dl_lit_project/data/openalex_quota.json` (set `RAG_FEEDER_OPENALEX_QUOTA_PATH` into `tmp_path`).
- Python: `cd dl_lit_project && /home/jay/DT/dl_lit_project/.venv/bin/python -m pytest tests/<file> -q`. Tasks 4 and 5 run only their targeted files; the whole suite (`tests -q`) runs once, in the final verification. CI runs the whole suite, so new test files need no CI edit.
- Backend: `cd backend && npm test -- <file>`; full run `npm test -- --maxWorkers=2` (full parallel runs can time out suites that pass alone; compare with a `c574aba` baseline before blaming a change).
- Frontend: `cd frontend && E2E_FRONTEND_PORT=5190 npx playwright test tests/<spec> --workers=1`, mocked specs only (check `ss -ltn` first that 5190 is free). Never the default 5175 (the dev container serves master). Never `*.real*.spec.ts`, `pipeline-perf.real-pdf.spec.ts` or `upstream-metadata.spec.ts`. Every spec that fakes a token calls `mockLogSocket(page)`. `npx vite build` must pass. A new spec goes into the CI list (`ci.yml:54`) in the task that creates it.
- **Race tests never wait a fixed time** (no `waitForTimeout`; a response reaching the browser is not a checkpoint) and add no hook to the app. Each one uses the checkpoint helpers of Task 1:
  - it calls `trackFetches(page)` before `page.goto`;
  - it confirms the commit by a request the app sends only after it (seen in the route log): after a sign-in, a seed-list request with the new token; after a corpus switch, the first `/api/seed/sources` after the switch's `/me` response;
  - it releases the late answer, then calls `afterBodyRead(page, <path>, <reads before>)` and asserts right there;
  - every follow-up request the late answer could start is route-gated, and the test asserts it with `fetchStats(page, <path>).calls` before releasing it;
  - no DOM observer is used (none of the transient states needs one).
- Exact strings from the spec:
  - D4: `[uploads] could not save the upload list:` (followed by the error message)
  - D8: 409 body `{ error: 'The graph snapshot was rebuilt; reload the graph.', code: 'snapshot_rebuilt' }`, header `Cache-Control: no-store`
  - D10: `[<label>] <n> work(s) changed while the batch was fetched; left for the next run`
  - D11: `Job <id>: the failure could not be recorded (<error>); exiting with code 75 so start-up recovery requeues it.`; the direct-cycle prefix `enrich cycle failed: ` (built by the existing `_run_direct_cycles`)
  - D12: `[WARN] OPENALEX_API_KEY not set. OpenAlex daily credits may be severely limited without an API key.` (the existing utils text)
- Exact values from the spec:
  - D6: `watch: { usePolling: pollingFromEnv(process.env.CHOKIDAR_USEPOLLING) }`, chokidar's own rule applied to the env value (corrected after `59841c8`: a literal `false` lets macOS pick FSEvents from the option).
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
3. **A late 403 on a seed follow-up after a sign-out.** It must not reconcile, so no `/me` is sent with the next user's token. Test in Task 1.
4. **An outsider with a matching or a stale `v`.** Both get 403 from the scope check, never a 409 that reveals the build. Test in Task 2.
5. **A job whose failure cannot be recorded.** After a failed claim it must not stay `running` under a live worker; the restart requeues it. A job that did work is not replayed (final review correction, spec D11). Test in Task 4 and the final fix.
6. **`CHOKIDAR_USEPOLLING` set to compose's `1`, or to ` TRUE `.** The watcher Vite builds polls. Test in Task 3.

---

### Task 1: Frontend session races (FE-60b, FE-63, FE-64)

**Files:**
- Modify: `frontend/src/App.svelte`:
  - `resetFrontendState` (`:2212-2270`): `exportBusy = false` in the `signOut` branch only (`exportBusy` stays out of `initialWorkspaceState()`);
  - `:766-788` `persistStoredUploads`, plus a module flag `uploadStorageWarned` next to it;
  - `loadSeedSources`'s `catch` (`:3283-3286`): the request-token check moves ahead of `handleCorpusPollForbidden`;
  - `:3863-3900` `startSearch`;
  - `:4266-4296` `handleExportDownload`, `:4298-4314` `handleDownloadedCorpusFile`, `:4369-4388` `handleDownloadSeedDocument`, `:4390-4410` `handleSeedCandidateFile`.
- Modify: `frontend/tests/helpers/mockApi.ts`: add `logIn` (moved from `session.spec.ts:48-54`), `trackFetches`, `fetchStats` and `afterBodyRead`; `session.spec.ts` imports `logIn`.
- Test: `frontend/tests/seed-selection.spec.ts`, `frontend/tests/session.spec.ts`, `frontend/tests/tab-resources.spec.ts`, `frontend/tests/seed-upload.spec.ts` (all in the CI list).

**Interfaces:**
- Produces, in `helpers/mockApi.ts` (test code only; nothing is added to the app):
  - `logIn(page: Page, username: string): Promise<void>`: today's `session.spec.ts` body.
  - `trackFetches(page: Page): Promise<void>`: `page.addInitScript` that wraps `window.fetch`.
    - Per call it adds 1 to `window.__dtFetch.calls[key]`, synchronously, where `key` is `pathname + search` of the request URL.
    - It wraps the returned response's `json`, `blob`, `text` and `arrayBuffer`. When the promise the app awaits resolves or rejects, it adds 1 to `window.__dtFetch.reads[key]`, then hands the result on.
  - `fetchStats(page: Page, match: string | RegExp): Promise<{ calls: number; reads: number }>`: sums over the keys whose pathname equals `match` (string) or whose full key matches `match` (RegExp).
  - `afterBodyRead(page: Page, match: string | RegExp, previousReads: number): Promise<void>`: `page.waitForFunction` until the summed reads exceed `previousReads`, then `page.evaluate(() => new Promise((r) => setTimeout(() => setTimeout(r, 0), 0)))`.
- Consumes: nothing from other tasks.

- [ ] **Step 1: Write the failing tests.** Every race test calls `trackFetches(page)` before `goto` and follows the checkpoint rule of Global Constraints. "Checkpoint on X" below means: note `fetchStats(page, X).reads`, release the gate, `afterBodyRead(page, X, noted)`.

```ts
// seed-selection.spec.ts — boot(): per-request gates for '/api/seed/sources/search/70/candidates/readable/file'
// (fileGates[n]); request 0 answers lateFileStatus === 500 ? fulfil(route, { error: 'late-candidate-error-7f3' }, 500)
// : a PDF with 'Content-Disposition: attachment; filename="readable.pdf"'; later requests a PDF named "probe.pdf".
// '/api/auth/login' -> { token: 'second-token', user: { id: 2, username: 'second', last_corpus_id: 1, is_admin: false } }
// '/api/auth/me' by bearer: 'second-token' -> user 'second', else today's 'reader'; a route log of { path, auth }
// candidates as in the readable-candidate test (:217-225)
test('a candidate file that arrives after a sign-out is not saved', async ({ page }) => {
  // names = []; page.on('download', (d) => names.push(d.suggestedFilename()))
  // expandSeed; click 'Readable copy' .seed-download-link (request 0, held)
  // 'Log out'; logIn(page, 'second'); auth-user 'second'; route log has a seed request with 'Bearer second-token' (commit)
  // checkpoint on the file path: getByText('Downloaded readable.pdf.') count 0
  // expandSeed; click the link again (request 1, answered); wait for the download event
  // expect(names).toEqual(['probe.pdf'])
})
test('a candidate file failing after a sign-out shows nothing', async ({ page }) => {
  // lateFileStatus 500; same flow up to the checkpoint (the text() read): getByText('late-candidate-error-7f3') count 0
})
// session.spec.ts — admin users adminA ('alice', is_admin true) and adminB ('bob', is_admin true) for the export test;
// '/api/exports/bundle' request n awaits exportGates[n]; request 0 -> 'alice-bundle.zip', request 1 -> 'bob-bundle.zip'
test("an old export neither saves nor unlocks the next user's export", async ({ page }) => {
  // logIn alice; click getByTestId('tab-downloads'); click export-bundle-button (request 0, held)
  // 'Log out'; logIn bob; auth-user 'bob'; click tab-downloads
  // expect(export-bundle-button).toBeEnabled()            // fails on c574aba
  // click it (request 1, held)
  // checkpoint on '/api/exports/bundle': button still disabled; getByText(/export downloaded/) count 0
  // release request 1; wait for the download event; names toEqual(['bob-bundle.zip']); button enabled
})
// session.spec.ts — keyword search: preview -> { count: 5, threshold: 100000 }; POST request 0 held on searchGate,
// answers fulfil(route, { runId: 7, status: 'running' }, 202); '/api/seed/sources' gated once `seedGateOn` is set
test('a search launched before a sign-out does not reach the next session', async ({ page }) => {
  // alice: Query 'economics'; Search; poll POST 0 seen
  // 'Log out'; logIn bob; auth-user 'bob'; bob's first seed request answered (commit); seedGateOn = true
  // seedCalls = (await fetchStats(page, '/api/seed/sources')).calls
  // checkpoint on '/api/keyword-search':
  //   expect(await page.evaluate(() => (window as any).__dtRefresh.reason())).not.toBe('active')   // c574aba: 'active'
  //   fetchStats('/api/seed/sources').calls === seedCalls                                         // c574aba: + 1
  // seedGateOn = false; release the held seed requests
})
test("a late 403 on the launch's seed follow-up does not reconcile the next session", async ({ page }) => {
  // alice signed in; wait until her initial seed load was read (afterBodyRead on '/api/seed/sources' from 0) before launching
  // seedGateOn = true; Search; POST 0 answers 202 { runId: 7 } at once; poll the launch's follow-up seed request (alice's token): held
  // 'Log out'; logIn bob; auth-user 'bob' (bob's seed requests are held by seedGateOn too); meGateOn = true (gates later /me)
  // note seedReads, seedCalls ('/api/seed/sources') and meCalls ('/api/auth/me')
  // release only alice's held request, with 403; afterBodyRead('/api/seed/sources', seedReads)   // the only body that can be read
  // fetchStats('/api/auth/me').calls === meCalls          // c574aba: + 1 (reconcile, bob's token)
  // fetchStats('/api/seed/sources').calls === seedCalls   // c574aba: + 1 (focusSeedSource's second reload)
  // then meGateOn = false, seedGateOn = false, release everything held; 'Seed One' visible
})
// tab-resources.spec.ts — boot state gains searchGate (POST request 0 held, 202 { runId: 55 }), meGateOn (gates /api/auth/me
// while set), seedGateOn (gates '/api/seed/sources' while set), exportGate; admin boot for the export test.
// New option boot(page, { settle: 'events' }) for these four tests: it calls trackFetches before goto and, instead of the
// 1.5 s sleep (:168), waits with afterBodyRead for one read of each boot load ('/api/auth/me', '/api/seed/sources',
// '/api/corpus', '/api/ingest/stats', '/api/ingest/runs').
// switchTo2(): meGateOn = true; select '2' in .header-corpus-picker; poll until the select-triggered /me is held at its gate
// (the server save is answered by then). commit(): note meReads; release that /me; afterBodyRead('/api/auth/me', meReads)
// (handleSelectCorpus resets the workspace synchronously after reading it).
test('a search launched before a corpus switch does not mark the new corpus as searching', async ({ page }) => {
  // previewCount 5; Search; poll POST 0 (held); seedGateOn = true; switchTo2(); commit()
  // note seedCalls; checkpoint on '/api/keyword-search': reason is not 'active'; seed calls === seedCalls   // c574aba: 'active', + 1
  // seedGateOn = false; release
})
test('a launch answered between the saved switch and its /me is reset by the commit', async ({ page }) => {
  // preservation (passes on c574aba): POST 0 held; seedGateOn = true; switchTo2() (the /me stays held)
  // checkpoint on '/api/keyword-search': reason === 'active'; seed calls + 1 (the held follow-up)
  // seedGateOn = false; release the held seed requests; expect('Fetching in the background') visible
  // commit(); 'Fetching in the background' count 0; reason is not 'active'; state.searchBodies[0].corpusId === 1
})
test("an export keeps the session's export lock across a corpus switch", async ({ page }) => {
  // preservation: admin boot; click tab-downloads; exportGate held; click export-bundle-button
  // switchTo2(); commit(); click tab-downloads; expect(export-bundle-button).toBeDisabled()
  // release; wait for the download event; button enabled
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

- [ ] **Step 2: Run them and verify they fail.** Run `cd frontend && E2E_FRONTEND_PORT=5190 npx playwright test tests/seed-selection.spec.ts tests/session.spec.ts tests/tab-resources.spec.ts tests/seed-upload.spec.ts --workers=1`.
  - Expected FAIL on the regression tests:
    - `Downloaded readable.pdf.` at the checkpoint, and `readable.pdf` among the download names;
    - `late-candidate-error-7f3` at the checkpoint;
    - bob's button disabled while alice's export is held;
    - the reason `'active'` and one more seed call at the checkpoint, after a sign-out and after a corpus commit;
    - one more `/me` call after the late 403;
    - the upload row never reaching `Imported 2 works`.
  - Expected PASS already (preservation): the in-gap reset test and the corpus-switch export-lock test.
  - All existing tests pass.
- [ ] **Step 3: Implement D1, D2 and D4.**
  - D1: each of the four handlers takes `const session = sessionEpoch` before its request. Right after the `await` it returns when `session !== sessionEpoch`. Its `catch` returns on `error?.status === 401 || session !== sessionEpoch`. In `handleExportDownload`, the `finally` sets `exportBusy = false` only when `session === sessionEpoch`. `resetFrontendState` sets `exportBusy = false` when `signOut` is true.
  - D2:
    - In `startSearch`, after the corpus check: `const session = sessionEpoch`, `const scope = corpusScopeEpoch`, `const stale = () => session !== sessionEpoch || scope !== corpusScopeEpoch`.
    - `if (stale()) return` after `runKeywordSearch` (before `refreshQuota` and `startedSearchRunIds`), after `loadSeedSources` and after `focusSeedSource`.
    - The `catch` returns on `error?.status === 401 || stale()`.
    - In `loadSeedSources`' `catch`: `if (error?.status === 401) return; if (token !== seedSourcesRequestToken) return; handleCorpusPollForbidden(error); …`.
    - `handleSelectCorpus` is unchanged.
  - D4: `try { window.localStorage.setItem(key, JSON.stringify(persisted)) } catch (error) { if (!uploadStorageWarned) { uploadStorageWarned = true; console.warn('[uploads] could not save the upload list:', error?.message) } }`.
  - Add `logIn`, `trackFetches`, `fetchStats` and `afterBodyRead` to `helpers/mockApi.ts`.
- [ ] **Step 4: Run the four specs again, then `npx vite build`.** Expected: all pass.
- [ ] **Step 5: Commit**: `frontend: downloads, search launches and their seed follow-ups from an ended session are dropped; the export lock is session-wide; a full localStorage no longer fails an upload (FE-60b, FE-63, FE-64)`

---

### Task 2: Graph rebuild check, load ownership, per-corpus graph instance and node-detail ownership (BE-54, FE-61)

Runs after Task 1 (browser runs are serialized; it uses Task 1's checkpoint helpers and edits `App.svelte` after Task 1).

**Files:**
- Modify: `backend/src/app.js`:
  - new `graph3dManifestVersion(key)` next to `decorateGraph3dManifest` (`:600-635`), which uses it for its mtime fallback;
  - the file route handler `:5874-5917` (one check before the existence check; the rest unchanged).
- Modify: `frontend/src/App.svelte:5984`: wrap the graph's `<svelte:component …>` in `{#key graphCorpusId}`.
- Modify: `frontend/src/components/ThreeGraph.svelte`:
  - `:80-84` (`nodeDetailSeq`, `graphLoadSeq`);
  - `showAllNodes` (`:946`), `loadNodeDetail` (`:1112-1133`), `clearSelection` (`:1174`);
  - `load3DGraph` (`:1215-1262`).
- Test: `backend/tests/graph-read-path.test.js`, `backend/tests/graph-access.test.js`, `frontend/tests/graph-3d.spec.ts` (in the CI list).

**Interfaces:**
- Produces (module-local, not exported): `graph3dManifestVersion(key: string) -> string | null`. It reads `graph3dSnapshotPath(key, 'manifest.json')` once. When `generated_at` is present it returns `String(manifest.generated_at)`, else `String(Math.floor(mtimeMs))`, and `null` when the manifest is unreadable.
- Consumes: `trackFetches`, `fetchStats`, `afterBodyRead` (Task 1).

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
// graph-3d.spec.ts — every test calls trackFetches(page) before goto. The mock gains per-test state:
// manifestVersions (by request order, or by corpus_id), resource gates per URL, per-request gates and a call log for
// '/api/graph/3d/node/<id>', and a mutable lastCorpusId that '/api/corpora/<id>/select' sets and '/api/auth/me' returns
// (mePayload({ corpusId: lastCorpusId, corpora: [corpus 1, corpus 2] })). Each manifest lists every resource URL with `?v=<version>`.
test('a 409 on a snapshot file reloads the manifest once and loads build B', async ({ page }) => {
  // versions ['A', 'B']; nodes.bin?v=A -> 409 { error: 'The graph snapshot was rebuilt; reload the graph.', code: 'snapshot_rebuilt' }
  // /Loaded 2 nodes and 1 edges? from snapshot\./ visible; manifest calls === 2; every resource call after the 409 has v=B
})
test('a 409 after the graph unmounts starts no request', async ({ page }) => {
  // preservation: versions ['A', 'R']; nodes.bin?v=A held; open the graph; poll its call; click tab-workspace (unmount)
  // checkpoint on /nodes\.bin\?v=A/ (release with 409): manifest calls === 1
  // click tab-graph again; loaded status visible; manifest calls === 2; the new resources carry v=R
})
test('a 409 after a header corpus switch neither retries nor publishes the old corpus', async ({ page }) => {
  // versions by corpus_id: '1' -> 'A', '2' -> 'C'; the corpus-2 manifest has stats.edge_count 7 (unique to corpus 2)
  // open the graph under corpus 1; nodes.bin?v=A held; all v=C resources held
  // select '2' in .header-corpus-picker; wait for a manifest call with corpus_id=2
  // checkpoint on /nodes\.bin\?v=A/ (release with 409): fetchStats('/api/graph/3d/snapshot').calls === 2
  //   (one per corpus; a retry under either corpus makes 3)
  // release the v=C resources; 'Loaded 2 nodes and 7 edges from snapshot.' visible   // c574aba: no corpus_id=2 call, the wait fails
})
for (const late of ['fails', 'succeeds']) {
  test(`an older detail answer that ${late} does not end the selected node's loading`, async ({ page }) => {
    // node 1 and node 2 held; select 'Graph seed work', then 'Referenced work'
    // checkpoint on '/api/graph/3d/node/1' (release with 500, or 200 { id: 1, title: 'Graph seed work', doi: '10.1000/seed' }):
    //   'Loading metadata...' visible                       // c574aba: hidden by node 1's finally
    // release node 2; 'Identifiers: 10.1000/example' visible
  })
}
test('an older answer for the same node neither shows nor caches', async ({ page }) => {
  // node-1 requests 0 (A1) and 1 (A2) held; node 2 answers at once; select 1, 2, 1
  // release A2 with doi '10.1000/seed-v2'; 'Identifiers: 10.1000/seed-v2' visible
  // checkpoint on '/api/graph/3d/node/1' (release A1 with '10.1000/seed-v1'): 'Identifiers: 10.1000/seed-v2' still visible
  // select 'Referenced work', then 'Graph seed work': node-1 calls === 2; 'Identifiers: 10.1000/seed-v2'   // c574aba: seed-v1
})
test('Escape ends the pending detail request', async ({ page }) => {
  // node-1 request 0 held; select 'Graph seed work'; press Escape
  // checkpoint on '/api/graph/3d/node/1' (release with 200 { id: 1, doi: '10.1000/seed' }), before any new selection
  // select 'Graph seed work' again: expect.poll(node-1 calls) === 2                // c574aba: cache hit, 1
})
```

- [ ] **Step 2: Run them and verify they fail.** Run `cd backend && npm test -- tests/graph-read-path.test.js tests/graph-access.test.js` and `cd frontend && E2E_FRONTEND_PORT=5190 npx playwright test tests/graph-3d.spec.ts --workers=1`.
  - Expected FAIL:
    - `?v=B` answers 200;
    - the 409 shows as an error instead of loading build B;
    - the header switch loads no corpus-2 graph;
    - node 1's settling hides node 2's loading line (both cases);
    - A1 overwrites panel and cache with `seed-v1`;
    - Escape leaves the answer cached.
  - Expected PASS already (preservation): the outsider and sendFile tests, and the unmount test.
- [ ] **Step 3: Implement D8 and D3.**
  - File route: after the key and file-name checks, `if (typeof req.query?.v === 'string' && graph3dManifestVersion(key) !== req.query.v)` set `Cache-Control: no-store` and answer the 409. Nothing else in the route changes; `requireGraph3dScopeAccess` stays ahead of the handler.
  - `decorateGraph3dManifest` keeps `generated_at` from its manifest and uses the helper's mtime rule only as the fallback.
  - App.svelte: `{#key graphCorpusId}<svelte:component this={module.default} {corpora} selectedGraphCorpusId={graphCorpusId} allowAllCorpora={isAdmin} />{/key}`.
  - `load3DGraph`:
    - `const load = ++graphLoadSeq`; `const options = { maxNodes, relationship, groupBy, corpusId: selectedGraphCorpusId }` captured once; `const owns = () => componentMounted && load === graphLoadSeq`.
    - Check `owns()` after the manifest, before the resources, before the retry, after the retry's manifest, and before writing `graphData`/`graphStatus`. A load that no longer owns returns.
    - The one retry runs on `error?.status === 409` with the same `options`.
    - The `catch` writes the status only while `owns()`. The `finally` clears `graphLoading` only when `load === graphLoadSeq`. The reset at its start bumps `nodeDetailSeq` and clears `nodeDetailLoading`.
  - `loadNodeDetail`: `const seq = ++nodeDetailSeq` right after the id check. After the fetch, the cache write, the panel write (still only while that node is selected), the `catch` and the `finally` each act only when `seq === nodeDetailSeq`.
  - `clearSelection` and `showAllNodes`: `nodeDetailSeq += 1; nodeDetailLoading = false`.
- [ ] **Step 4: Run both commands again, then `npx vite build`.** Expected: all pass, the build-streaming test included.
- [ ] **Step 5: Commit**: `graph: a versioned snapshot file request answers 409 after a rebuild and the graph reloads once while it owns the load; one graph instance per committed workspace corpus; node detail shows and caches only the newest request (BE-54, FE-61)`

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
  - D6: `watch: { usePolling: pollingFromEnv(process.env.CHOKIDAR_USEPOLLING) }` (corrected after `59841c8`; not a literal `false`, because chokidar picks FSEvents on macOS from the option). The comment names chokidar's `CHOKIDAR_USEPOLLING` override (`node_modules/vite/dist/node/chunks/config.js:13887-13893`) and the compose files that set it to `1` for the bind mount, and says that Playwright and CI leave it unset.
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
- [ ] **Step 4: Run the new file, then `tests/test_cli_download_leases.py tests/test_worker_watchdog.py tests/test_worker_recovery.py tests/test_hung_items.py`** (one command, as in Global Constraints). Expected: all passed; `test_a_failed_claim_is_not_an_empty_queue` still passes (default unchanged). The whole suite runs in the final verification.
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
  - `WRITE_GUARDS: dict[str, str]`, the identifier fragment per pass (the scope comes on top):
    - `"openalex"`: `"openalex_id = ? AND (topics_fetched IS NULL OR topics_fetched = 0)"`;
    - `"doi"`: `"doi = ? AND (openalex_id IS NULL OR openalex_id = '') AND (topics_fetched IS NULL OR topics_fetched = 0)"`.
  - `write_results(conn, updates, guard: str, scope_clause: str) -> list[int]`:
    - updates are `(work_id, raw, topic, subfield, field, domain, topics_json)`;
    - one `UPDATE works SET … topics_fetched = 1 WHERE id = ? AND (<scope_clause>) AND <WRITE_GUARDS[guard]>` per row, one commit;
    - `scope_clause` is `main`'s existing value (`:244-246`): `metadata_status = 'matched'` or `1 = 1`, chosen by `--scope`;
    - returns the ids whose `rowcount` was 1.
  - `run_pass(conn, rows, key, build_filter, mailto, batch_size, label, guard, scope_clause) -> tuple[int, int, int]`: rows are `(work_id, token, raw)`. Pass 1 rows are `(id, normalize_openalex_id(openalex_id), openalex_id)`.
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

def test_backfill_skips_a_row_requeued_out_of_the_default_scope(tmp_path, monkeypatch, capsys, backfill):
    # default scope; rows 1 '10.1000/ONE', 2 '10.1000/TWO' (matched); change: UPDATE works SET metadata_status = 'pending' WHERE id = 1
    # (both identifiers kept); answer both -> row 1 topics_fetched None; row 2 updated; '[doi] 1 work(s) changed …' logged

def test_backfill_unchanged_rows_log_no_changed_line(tmp_path, monkeypatch, capsys, backfill):
    # preservation (passes on c574aba): one matched DOI row, no change -> updated; 'changed while the batch was fetched' not in err

# test_hygiene.py — the warning test resets both warning flags (below), so its result does not depend on test order
def test_the_scraper_strips_the_api_key(monkeypatch):
    # setenv OPENALEX_API_KEY "  sekret-key-123\n" -> OpenAlexCrossrefSearcher(rate_limiter=_NoRateLimit()).api_key == "sekret-key-123"

def test_a_blank_primary_key_falls_back_to_the_rag_feeder_key(monkeypatch):
    # setenv OPENALEX_API_KEY "   ", RAG_FEEDER_OPENALEX_API_KEY "fallback-key" -> api_key == "fallback-key"

def test_the_missing_key_warning_prints_once(monkeypatch, capsys, tmp_path):
    # delenv OPENALEX_API_KEY and RAG_FEEDER_OPENALEX_API_KEY; setenv RAG_FEEDER_OPENALEX_QUOTA_PATH tmp_path / "quota.json"
    # monkeypatch.setattr(utils, "_missing_openalex_api_key_warned", False)
    # monkeypatch.setattr(OpenAlexScraper, "_missing_api_key_warned", False, raising=False)   # exists on c574aba only
    # monkeypatch utils.time.sleep to a no-op
    # OpenAlexCrossrefSearcher(rate_limiter=_NoRateLimit()) -> api_key is None
    # utils.openalex_request_json(url="https://api.openalex.org/works", params={"filter": "doi:x"},
    #   session=<stub: get() returns status_code 200, headers {}, json() {"results": []}, raise_for_status() no-op>,
    #   rate_limiter=_FreeLimiter(), retries=1)
    # capsys.readouterr().out.count("[WARN] OPENALEX_API_KEY not set.") == 1   (2 on c574aba)

def test_the_scraper_has_no_key_reader_of_its_own():
    # source of dl_lit/OpenAlexScraper.py contains neither "getenv('OPENALEX_API_KEY')" nor "_missing_api_key_warned"
```

- [ ] **Step 2: Run them and verify they fail.** Run `cd dl_lit_project && /home/jay/DT/dl_lit_project/.venv/bin/python -m pytest tests/test_doi_rule_scripts.py tests/test_hygiene.py -q`. Expected:
  - FAIL: the annotation differs; the changed rows (DOI, OpenAlex id, status) get topics; the key keeps its whitespace; the blank key wins over the fallback; the warning prints twice; the scraper still has its own reader.
  - PASS already (preservation): the unchanged-row test.
- [ ] **Step 3: Implement D10 and D12.**
  - `run_pass` maps `token -> (work_id, raw)` and builds updates with `raw`. It calls `write_results(conn, updates, guard, scope_clause)` and counts `matched` and `fielded` over the returned ids only. It logs the D10 line when `len(updates) - len(written) > 0`. `seen_work_ids` and `mark_fetched` go.
  - `main` passes `guard="openalex"` and `guard="doi"`, and its `scope_clause`, to both passes.
  - Scraper: `self.api_key = get_openalex_api_key()`; `if not self.api_key: _warn_missing_openalex_api_key()`. Import both from `.utils`; delete `_missing_api_key_warned` and the scraper's own print.
- [ ] **Step 4: Run `tests/test_doi_rule_scripts.py tests/test_hygiene.py tests/test_worker_settings.py tests/test_identifiers.py`** (one command). Expected: all passed. `git grep -n "getenv('OPENALEX_API_KEY')" -- dl_lit_project/dl_lit` prints nothing. The whole suite runs in the final verification.
- [ ] **Step 5: Commit**: `backfill: topics are written only while the row is still in the state the pass selected it in; the scraper uses the shared OpenAlex key reader (PY-34, PY-27)`

---

## Final verification (controller, after all tasks)

- `cd dl_lit_project && /home/jay/DT/dl_lit_project/.venv/bin/python -m pytest tests -q`: all passed.
- `cd backend && npm test -- --maxWorkers=2`: all passed.
- `cd frontend && npx vite build`, then, once, the whole CI Playwright list from `ci.yml:54` (now with `tests/dev-server-config.spec.ts`): `E2E_FRONTEND_PORT=5190 npx playwright test <list> --workers=1`. All pass.
- `git diff c574aba --stat` touches no compose file, no Caddyfile, no `backend/src/index.js` and no schema DDL.
- The inventory's reconciliation section records FE-22 and BE-38 as closed (spec D5, D9), and the rows of this phase as done.
- After merge: `docker exec rag_feeder_frontend printenv CHOKIDAR_USEPOLLING` prints `1`, and `docker logs rag_feeder_frontend` shows Vite's restart after the config change with no error. Never the dev DB.
