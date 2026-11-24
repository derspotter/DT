# Review fixes, phase 8: races and small fixes — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the small open rows of the reconciliation:
- session and corpus races in the frontend (FE-60b, FE-63, FE-61);
- a storage crash in the upload list (FE-64) and dev-server polling (FE-24);
- graph builds mixed in one load (BE-54) and a non-atomic corpus removal (BE-55);
- a stale backfill write (PY-34), a failed claim read as an empty queue (PY-54) and the scraper's own key reader (PY-27, DRY half).

FE-22 and BE-38 are closed without code (spec D5, D9).

**Architecture:** Five tasks grouped by surface.
1. Frontend session races (`App.svelte`).
2. Graph consistency (the backend file route plus `ThreeGraph.svelte`).
3. Backend transaction and dev config (`app.js` single-work removal, `vite.config.js`, CI list).
4. Python claims (`db_manager.py`, `worker.py`).
5. Python backfill and key (`backfill_openalex_topics.py`, `OpenAlexScraper.py`).

No new module, no schema change, no new dependency. One new Playwright spec (`dev-server-config.spec.ts`, Task 3) and one new pytest file (`test_worker_claim_errors.py`, Task 4).

**Tech Stack:** Python 3.11 (`/home/jay/DT/dl_lit_project/.venv/bin/python`), pytest, pytest-mock, sqlite3; Node 20, Express 5, better-sqlite3, Jest + supertest; Svelte 5 (legacy syntax in `App.svelte`) + Vite 7.3.1, Playwright (mocked).

**Spec:** `docs/superpowers/specs/2026-11-24-review-fixes-8-races-and-small-fixes-design.md`

## Parallelism and shared files

| Task | Surface | Runs |
|---|---|---|
| 1 Session races (FE-60b, FE-63, FE-64) | frontend | parallel |
| 2 Graph consistency (BE-54, FE-61) | backend + frontend | parallel |
| 3 Removal transaction + polling flag (BE-55, FE-24) | backend + config | **after Task 2** |
| 4 Claim errors (PY-54) | Python | parallel |
| 5 Backfill guard + key reader (PY-34, PY-27) | Python | parallel |

Shared files:
- `backend/src/app.js`: Task 2 (graph file route, `decorateGraph3dManifest`, new `graph3dManifestVersion`) and Task 3 (`DELETE /api/corpus/works/:workId`). Different routes; Task 3 runs after Task 2 so the second diff applies cleanly.
- `frontend/src/App.svelte`: Task 1 only. `ThreeGraph.svelte`: Task 2 only.
- `.github/workflows/ci.yml`: Task 3 only.

## Global Constraints

- Work only in `/home/jay/DT/.worktrees/review-fixes-8`, on branch `review-fixes-8`. Do not push.
- Never open `dl_lit_project/data/literature.db` or any other dev or prod `*.db`; tests use `tmp_path` / `fs.mkdtempSync` DBs.
- Python: `cd dl_lit_project && /home/jay/DT/dl_lit_project/.venv/bin/python -m pytest tests/<file> -q`; whole suite with `tests -q`. CI runs the whole suite, so new test files need no CI edit.
- Backend: `cd backend && npm test -- <file>`; full run `npm test -- --maxWorkers=2` (full parallel runs can time out suites that pass alone; compare with a `c574aba` baseline before blaming a change).
- Frontend: `cd frontend && E2E_FRONTEND_PORT=5190 npx playwright test tests/<spec> --workers=1`, mocked specs only (check `ss -ltn` first that 5190 is free). Never the default 5175 (the dev container serves master). Never `*.real*.spec.ts`, `pipeline-perf.real-pdf.spec.ts` or `upstream-metadata.spec.ts`. Every spec that fakes a token calls `mockLogSocket(page)`. `npx vite build` must pass. A new spec goes into the CI list (`ci.yml:54`) in the task that creates it.
- Exact strings from the spec:
  - D4: `[uploads] could not save the upload list:` (followed by the error message)
  - D8: 409 body `{ error: 'The graph snapshot was rebuilt; reload the graph.', code: 'snapshot_rebuilt' }`, header `Cache-Control: no-store`
  - D10: `[<label>] <n> work(s) changed while the batch was fetched; left for the next run`
  - D11: `enrich cycle failed: ` (prefix built by the existing `_run_direct_cycles`)
  - D12: `[WARN] OPENALEX_API_KEY not set. OpenAlex daily credits may be severely limited without an API key.` (the existing utils text)
- Exact values from the spec: D6 truthy values `1`, `true`, `yes` (trimmed, lowercased), anything else false; D10 `guard_column` is `"openalex_id"` (pass 1) or `"doi"` (pass 2), never user input; D11 default `raise_errors=False`.
- No compose, Caddy or schema change. No change to `index.js` (D9 closed).
- Each commit message names the rows it closes and ends with:
  ```
  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01U3c4gfg7jQvvmPdpLNBN5T
  ```

## Review Focus

1. **A download that fails after a sign-out.** The 500 must show no error text to the next user, like a success shows no file. Test in Task 1 (`a candidate file failing after a sign-out shows nothing`).
2. **A client that refuses gzip.** With a matching `v` it must get the plain file through the fd path with its `Content-Type` and `Content-Length`, as `res.sendFile` gave it. Test in Task 2.
3. **A 409 must not leak the opened fd.** Test in Task 2 (`fs.closeSync` called with the fd `openSync` returned).
4. **A `pipeline_tick` whose enrich claim fails.** It must end `failed`, not `completed` with an empty `enriched`. Test in Task 4.
5. **`CHOKIDAR_USEPOLLING` with case or whitespace (` TRUE `).** It must poll, as the flag rule says. Test in Task 3.

---

### Task 1: Frontend session races (FE-60b, FE-63, FE-64)

**Files:**
- Modify: `frontend/src/App.svelte`:
  - `:506` and `initialWorkspaceState()` (`:2147-2210`), `resetFrontendState` destructuring (`:2219-2270`): add `exportBusy`;
  - `:766-788` `persistStoredUploads`, plus a module flag `uploadStorageWarned` next to it;
  - `:3863-3900` `startSearch`;
  - `:4266-4296` `handleExportDownload`, `:4298-4314` `handleDownloadedCorpusFile`, `:4369-4388` `handleDownloadSeedDocument`, `:4390-4410` `handleSeedCandidateFile`.
- Modify: `frontend/tests/helpers/mockApi.ts`: export `logIn` (moved from `session.spec.ts:48-54`); `session.spec.ts` imports it.
- Test: `frontend/tests/seed-selection.spec.ts`, `frontend/tests/session.spec.ts`, `frontend/tests/tab-resources.spec.ts`, `frontend/tests/seed-upload.spec.ts` (all in the CI list).

**Interfaces:**
- Produces: `logIn(page: Page, username: string): Promise<void>` in `helpers/mockApi.ts` (same body as today's `session.spec.ts` copy).
- Consumes: nothing from other tasks.

- [ ] **Step 1: Write the failing tests.**

```ts
// seed-selection.spec.ts — boot(): State gains `holdFile?: Promise<void>`, `fileStatus?: number`, `fileRequests?: number`;
// the route answers '/api/auth/login' with { token: 'second-token', user: { id: 2, username: 'second', last_corpus_id: 1, is_admin: false } }
// and '/api/seed/sources/search/70/candidates/readable/file' by counting, awaiting holdFile, then
// route.fulfill({ status: fileStatus ?? 200, contentType: 'application/pdf',
//   headers: { 'Content-Disposition': 'attachment; filename="readable.pdf"' }, body: '%PDF-1.4 x' }).
// Candidates as in the readable-candidate test (:217-225).
test('a candidate file that arrives after a sign-out is not saved', async ({ page }) => {
  // const downloads = []; page.on('download', (d) => downloads.push(d))
  // expandSeed; click the 'Readable copy' row's .seed-download-link; poll fileRequests === 1
  // click 'Log out'; logIn(page, 'second'); expect getByTestId('seed-panel') visible again (the /me mock still answers 'reader')
  // release holdFile; waitForTimeout(300)
  // expect(downloads).toHaveLength(0); getByText(/Downloaded/) count 0
})
test('a candidate file failing after a sign-out shows nothing', async ({ page }) => {
  // same with fileStatus 500 -> getByText(/Failed to download/) count 0, and no 'Downloaded'
})
// session.spec.ts — '/api/exports/bundle' held on a deferred(); answers application/zip with a Content-Disposition
test('a corpus export that arrives after a sign-out is not saved and does not block the next user', async ({ page }) => {
  // log in alice; goto '#/downloads'; click getByTestId('export-bundle-button'); poll the export request seen
  // Log out; logIn(page, 'bob'); goto '#/downloads'
  // expect(getByTestId('export-bundle-button')).toBeEnabled()   // before the release
  // release; waitForTimeout(300); no download event; getByText(/export downloaded/) count 0
})
test('a search launched before a sign-out does not reach the next session', async ({ page }) => {
  // preview answers { count: 5, threshold: 100000 }; POST /api/keyword-search held on a deferred()
  // fill Query 'economics'; Search; poll the POST seen; Log out; logIn(page, 'bob')
  // release with fulfil(route, { runId: 7, status: 'running' }, 202); waitForTimeout(300)
  // getByText(/Fetching in the background/) count 0
  // expect(await page.evaluate(() => (window as any).__dtRefresh.reason())).not.toBe('active')
})
// tab-resources.spec.ts — boot state gains `searchDelayMs` (default 0), awaited before the 202 at :118-121
test('a search launched before a corpus switch does not mark the new corpus as searching', async ({ page }) => {
  // state.previewCount = 5; state.searchDelayMs = 800; Query 'economics'; Search
  // poll requests('/api/keyword-search') === 1; select corpus '2' in .header-corpus-picker; waitForTimeout(1200)
  // getByText(/Fetching in the background/) count 0; __dtRefresh.reason() is not 'active'
})
// seed-upload.spec.ts — the first test's flow (:68-96) with an init script:
//   const real = Storage.prototype.setItem
//   Storage.prototype.setItem = function (k, v) { if (String(k).startsWith('rag-feeder:uploads:'))
//     throw new DOMException('full', 'QuotaExceededError'); return real.call(this, k, v) }
test('a full localStorage does not fail an upload', async ({ page }) => {
  // the same assertion as :96: /Imported 2 works \(1 skipped, 2 added to corpus\)/ visible; no 'failed' text in the upload row
  // collect console warnings: exactly one starts with '[uploads] could not save the upload list:'
})
```

- [ ] **Step 2: Run them and verify they fail.** Run `cd frontend && E2E_FRONTEND_PORT=5190 npx playwright test tests/seed-selection.spec.ts tests/session.spec.ts tests/tab-resources.spec.ts tests/seed-upload.spec.ts --workers=1`. Expected: the six new tests FAIL. Today a download event fires and `Downloaded readable.pdf.` shows; the 500 shows its error; the export button stays disabled; `Fetching in the background` shows and the reason is `active`; the BibTeX upload ends `failed`. The other tests pass.
- [ ] **Step 3: Implement D1, D2 and D4.**
  - D1: each of the four handlers takes `const session = sessionEpoch` before its request. Right after the `await` it returns when `session !== sessionEpoch`. Its `catch` returns on `error?.status === 401 || session !== sessionEpoch`. `handleExportDownload`'s `finally` sets `exportBusy = false` only when `session === sessionEpoch`. `exportBusy: false` goes into `initialWorkspaceState()` and the destructuring.
  - D2: in `startSearch`, after the corpus check, `const session = sessionEpoch`, `const scope = corpusScopeEpoch`, `const stale = () => session !== sessionEpoch || scope !== corpusScopeEpoch`. `if (stale()) return` comes after `runKeywordSearch` (before `refreshQuota` and `startedSearchRunIds`), after `loadSeedSources` and after `focusSeedSource`. The `catch` returns on `error?.status === 401 || stale()`.
  - D4: `try { window.localStorage.setItem(key, JSON.stringify(persisted)) } catch (error) { if (!uploadStorageWarned) { uploadStorageWarned = true; console.warn('[uploads] could not save the upload list:', error?.message) } }`.
  - Move `logIn` to `helpers/mockApi.ts`.
- [ ] **Step 4: Run the four specs, then the whole CI list (`ci.yml:54`) with `--workers=1`, then `npx vite build`.** Expected: all pass.
- [ ] **Step 5: Commit**: `frontend: downloads and search launches from an ended session are dropped; a full localStorage no longer fails an upload (FE-60b, FE-63, FE-64)`

---

### Task 2: Graph files from one build, and node detail by the newest request (BE-54, FE-61)

**Files:**
- Modify: `backend/src/app.js`:
  - new `graph3dManifestVersion(key)` next to `decorateGraph3dManifest` (`:600-635`), which uses it for the version;
  - the file route `:5874-5917`.
- Modify: `frontend/src/components/ThreeGraph.svelte`: `:80-84` (`nodeDetailSeq`), `:1112-1133` (`loadNodeDetail`), `:1215-1260` (`load3DGraph` retry).
- Test: `backend/tests/graph-read-path.test.js`, `frontend/tests/graph-3d.spec.ts` (in the CI list).

**Interfaces:**
- Produces (module-local, not exported): `graph3dManifestVersion(key: string) -> string | null`. It reads `graph3dSnapshotPath(key, 'manifest.json')`. When `generated_at` is present it returns `String(manifest.generated_at)`, else `String(Math.floor(mtimeMs))`, and `null` when the manifest is unreadable. `decorateGraph3dManifest` keeps `generated_at` from the manifest it decorates and falls back to this helper's mtime branch.
- Consumes: nothing.

- [ ] **Step 1: Write the failing tests.**

```js
// graph-read-path.test.js — own snapshot dir per test, as 'the cluster detail route is gone' (:327-340):
// key 'f6f6f6f6f6f6f6f6f6f6'; manifest { schema_version: 2, generated_at: 'A', files: {} }; nodes.bin = Buffer.from([1,2,3,4]);
// nodes.bin.gz = zlib.gzipSync(that)
test('a versioned file request is served only from its build', async () => {
  // get(`/api/graph/3d/snapshot/${key}/nodes.bin?v=A`) -> 200, body bytes [1,2,3,4] (supertest decodes gzip)
  // ?v=B -> 409, body { error: 'The graph snapshot was rebuilt; reload the graph.', code: 'snapshot_rebuilt' },
  //   header cache-control 'no-store'
  // no v -> 200
})
test('a rebuild between the version check and the open answers 409 and closes the fd', async () => {
  // const realOpen = fs.openSync; let opened = null
  // jest.spyOn(fs, 'openSync').mockImplementation((p, ...rest) => { const fd = realOpen(p, ...rest);
  //   if (path.basename(String(p)).startsWith('nodes.bin')) { opened = fd;
  //     fs.writeFileSync(path.join(dir, 'manifest.json'), JSON.stringify({ schema_version: 2, generated_at: 'B', files: {} })) }
  //   return fd })
  // const close = jest.spyOn(fs, 'closeSync')
  // ?v=A -> 409; expect(close).toHaveBeenCalledWith(opened); jest.restoreAllMocks()
})
test('a client refusing gzip gets the plain file with its type and length', async () => {
  // .set('Accept-Encoding', 'identity') ?v=A -> 200, content-type /application\/octet-stream/, content-length '4',
  //   no content-encoding header; manifest.json?v=A -> 200, content-type /application\/json/
})
// the existing build-streaming test (:307-325) stays as it is and must still pass
```

```ts
// graph-3d.spec.ts
test('a 409 on a snapshot file reloads the manifest once and loads the graph', async ({ page }) => {
  // count '/api/graph/3d/snapshot' requests; the first nodes.bin request answers 409
  //   { error: 'The graph snapshot was rebuilt; reload the graph.', code: 'snapshot_rebuilt' }, later ones as mockApi
  // open the graph panel -> /Loaded 2 nodes and 1 edges? from snapshot\./ visible; manifest requests === 2
})
test('a late detail failure for another node leaves the selected node\'s detail', async ({ page }) => {
  // '/api/graph/3d/node/1' held on a deferred(), then answers 500; '/api/graph/3d/node/2' as mockApi
  // search 'Graph seed' -> select 'Graph seed work'; search 'Referenced' -> select 'Referenced work'
  // expect 'Identifiers: 10.1000/example' visible; release node 1; waitForTimeout(300); still visible;
  //   'Loading metadata...' count 0
})
test('an older detail answer does not end the newer node\'s loading', async ({ page }) => {
  // '/api/graph/3d/node/1' held on gate1, '/api/graph/3d/node/2' held on gate2
  // select 'Graph seed work', then 'Referenced work'; release gate1 with 200 { id: 1, title: 'Graph seed work', doi: '10.1000/seed' }
  // waitForTimeout(300): 'Loading metadata...' still visible, 'Identifiers: 10.1000/seed' count 0
  // release gate2 (mockApi body) -> 'Identifiers: 10.1000/example' visible, 'Loading metadata...' count 0
})
```

- [ ] **Step 2: Run them and verify they fail.** Run `cd backend && npm test -- tests/graph-read-path.test.js` and `cd frontend && E2E_FRONTEND_PORT=5190 npx playwright test tests/graph-3d.spec.ts --workers=1`. Expected: FAIL. `?v=B` answers 200, `openSync` is never called, the 409 test shows the error, and node 1's failure clears node 2's identifiers.
- [ ] **Step 3: Implement D8 and D3.**
  - File route: read `v = typeof req.query?.v === 'string' ? req.query.v : null`. Pick the path first (the `.gz` when the client accepts gzip and it exists, else the plain file; missing → today's 404). With `v`: V1 = `graph3dManifestVersion(key)`, `fd = fs.openSync(chosen, 'r')`, V2 = `graph3dManifestVersion(key)`. On `V1 !== v || V2 !== v`: `fs.closeSync(fd)`, set `Cache-Control: no-store`, answer the 409. Without `v`: open the same way, no checks.
  - Then both paths stream `fs.createReadStream(chosen, { fd })`. `Content-Length` comes from `fs.fstatSync(fd).size`; `Content-Type` is by extension (`.json` → `application/json; charset=utf-8`, else `application/octet-stream`); `Content-Encoding: gzip` and `Vary` stay on the gzip path; `Cache-Control: private, max-age=600` stays on a 200. Today's error and `close` handlers stay. `res.sendFile` goes.
  - `load3DGraph`: wrap the manifest and resource fetch in a local `async function fetchSnapshot()` returning `{ manifest, snapshot }`. On a rejection with `error?.status === 409`, call it once more; a second failure falls through to the existing `catch`.
  - `loadNodeDetail`: `const seq = ++nodeDetailSeq` as the first line after the id check. Cache always; show only when `seq === nodeDetailSeq` and the node is still selected; the `catch` and `finally` act only when `seq === nodeDetailSeq`.
- [ ] **Step 4: Run both commands again, then `npx vite build`.** Expected: all pass, the build-streaming test included.
- [ ] **Step 5: Commit**: `graph: a snapshot file request is served only from the build its manifest named, else 409 and one reload; node detail shows only the newest request (BE-54, FE-61)`

---

### Task 3: Single-work removal in one transaction, and the polling flag (BE-55, FE-24)

Runs after Task 2 (both edit `backend/src/app.js`).

**Files:**
- Modify: `backend/src/app.js:5507-5530` (`DELETE /api/corpus/works/:workId`).
- Modify: `frontend/vite.config.js:5-13` (new `usePolling` const) and `:54-56`.
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
// function watch(value: string | undefined) { env = { ...process.env }; delete env.CHOKIDAR_USEPOLLING; if (value !== undefined) env.CHOKIDAR_USEPOLLING = value
//   return JSON.parse(execFileSync(process.execPath, ['--input-type=module', '-e',
//     "const m = await import('./vite.config.js'); console.log(JSON.stringify(m.default.server.watch))"],
//     { cwd: frontendDir, env, encoding: 'utf8' }).trim().split('\n').pop()) }
test('the dev server polls only when CHOKIDAR_USEPOLLING says so', () => {
  // watch(undefined).usePolling === false; '1' -> true; 'true' -> true; ' TRUE ' -> true; 'yes' -> true; '0' -> false
})
```

- [ ] **Step 2: Run them and verify they fail.** Run `cd backend && npm test -- tests/corpus-sharing.test.js` and `cd frontend && E2E_FRONTEND_PORT=5190 npx playwright test tests/dev-server-config.spec.ts --workers=1`. Expected: FAIL. The marker test sees the membership gone, and the config test sees `usePolling: true` without the variable.
- [ ] **Step 3: Implement D7 and D6.**
  - D7: `const removed = authDb.transaction(() => { … })()`. Inside, the `corpus_works` delete runs; on `changes === 0` it returns `0`; otherwise the marker delete runs (same `tableExists` check) and it returns `changes`. The route answers 404 on `0`, else `{ removed: true, work_id }`; the `catch` is unchanged.
  - D6: `const usePolling = ['1', 'true', 'yes'].includes(String(process.env.CHOKIDAR_USEPOLLING || '').trim().toLowerCase())` and `watch: { usePolling }`. The comment says the dev containers set the variable for the bind mount (`docker-compose.yml`, `docker-compose.dev.yml`) and that Playwright and CI leave it unset.
  - Append the new spec to the `ci.yml:54` list.
- [ ] **Step 4: Run both again; then the whole CI Playwright list with `--workers=1` and `npx vite build`.** Expected: all pass. `git grep -n "usePolling: true" -- frontend/vite.config.js` prints nothing.
- [ ] **Step 5: Commit**: `backend: removing one work from a corpus is one transaction; vite polls only when CHOKIDAR_USEPOLLING is set (BE-55, FE-24)`

---

### Task 4: A failed claim fails the worker's job or cycle (PY-54)

**Files:**
- Modify: `dl_lit_project/dl_lit/db_manager.py:1730-1859` (`claim_enrich_batch`: parameter, docstring, `except`).
- Modify: `backend/scripts/daemon/worker.py:1249-1255` and `:1596` (pass `raise_errors=True`).
- Create: `dl_lit_project/tests/test_worker_claim_errors.py`

**Interfaces:**
- Produces: `DatabaseManager.claim_enrich_batch(self, *, limit: int, corpus_id: int | None, claimed_by: str, lease_seconds: int = 900, max_attempts: int | None = None, target_ids: list[int] | None = None, raise_errors: bool = False) -> list[dict]`
- Consumes: `claim_download_batch(..., raise_errors=True)` (exists since #82); `_deny_next_begin(db)` from `tests.test_cli_download_leases`; `load_worker` from `tests._worker_import`.

- [ ] **Step 1: Write the failing tests** in `test_worker_claim_errors.py`. Build the daemon as `test_worker_watchdog._daemon` does (copy it locally with `job_timeout=30`, so no watchdog fires). Insert a `running` job owned by `daemon.worker_id`, call `daemon._process_job(job, generation=daemon._begin_work())`, and read `status, result_json` back. To deny exactly the claim's BEGIN, wrap the claim: `real = daemon.db.claim_enrich_batch; monkeypatch.setattr(daemon.db, "claim_enrich_batch", lambda **kw: (_deny_next_begin(daemon.db), real(**kw))[1])`, and the same for `claim_download_batch`.

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
    # job_type pipeline_tick: corpus_id 1 (skips the interactive-backlog branch, as corpus_id is not None)
    # status == "failed"; json.loads(result_json)["error"] mentions "not authorized" (the authorizer's sqlite3 message)

def test_a_failed_claim_in_a_direct_cycle_reaches_the_loop(monkeypatch, tmp_path):
    # role "enrich"; wrap claim_enrich_batch as above
    # with pytest.raises(RuntimeError, match=r"^enrich cycle failed: "): daemon._run_direct_cycles()
```

- [ ] **Step 2: Run them and verify they fail.** Run `cd dl_lit_project && /home/jay/DT/dl_lit_project/.venv/bin/python -m pytest tests/test_worker_claim_errors.py -q`. Expected: FAIL. `raise_errors` is an unexpected keyword, the jobs end `completed`, and the direct cycle returns `None`.
- [ ] **Step 3: Implement D11.** Add the `raise_errors` parameter. The `except sqlite3.Error` rolls back as today, then `if raise_errors: raise`, else `return []`. The docstring takes `claim_download_batch`'s sentence. The worker passes `raise_errors=True` in `do_enrich` and `do_download`. No other change: `_process_job`'s `except` and `_run_direct_cycles` already handle the exception.
- [ ] **Step 4: Run the new file, then `tests/test_cli_download_leases.py`, `tests/test_worker_watchdog.py`, `tests/test_hung_items.py`, then the whole suite.** Expected: all passed; `test_a_failed_claim_is_not_an_empty_queue` still passes (default unchanged).
- [ ] **Step 5: Commit**: `worker: a failed enrich or download claim fails the job or cycle instead of finishing it empty (PY-54)`

---

### Task 5: Guarded backfill writes, and one OpenAlex key reader (PY-34, PY-27)

**Files:**
- Modify: `backend/scripts/backfill_openalex_topics.py`: `doi_rows` (`:86-105`), `write_results` (`:154-166`), `mark_fetched` (`:169-175`), `run_pass` (`:178-224`), pass 1 rows in `main` (`:250-272`), pass 2 call (`:276-286`).
- Modify: `dl_lit_project/dl_lit/OpenAlexScraper.py`: imports (`:17-25`), delete `:58`, `__init__` `:576-583`.
- Test: `dl_lit_project/tests/test_doi_rule_scripts.py` (the annotation test at `:157-165` changes, plus new tests), `dl_lit_project/tests/test_hygiene.py`.

**Interfaces:**
- Produces:
  - `doi_rows(conn, *, scope_clause: str, limit_clause: str) -> tuple[list[tuple[int, str, str]], int]`: `(work_id, key, raw_doi)`, where `raw_doi` is the row's `doi` exactly as read.
  - `write_results(conn, updates, guard_column: str) -> list[int]`: updates are `(work_id, raw, topic, subfield, field, domain, topics_json)`. It runs one `UPDATE … WHERE id = ? AND <guard_column> = ?` per row, commits once and returns the ids whose `rowcount` was 1.
  - `mark_fetched(conn, rows, guard_column: str) -> None`: rows are `(work_id, raw)`; `… WHERE id = ? AND topics_fetched IS NULL AND <guard_column> = ?`.
  - `run_pass(conn, rows, key, build_filter, mailto, batch_size, label, guard_column) -> tuple[int, int, int]`: rows are `(work_id, token, raw)`.
  - Pass 1 rows are `(id, normalize_openalex_id(openalex_id), openalex_id)`.
- Consumes: `utils.get_openalex_api_key()`, `utils._warn_missing_openalex_api_key()` (both exist).

- [ ] **Step 1: Write the failing tests.**

```python
# test_doi_rule_scripts.py
# the annotation test now expects "return": tuple[list[tuple[int, str, str]], int]

def test_backfill_pass2_skips_a_row_whose_doi_changed_during_the_request(tmp_path, monkeypatch, capsys, backfill):
    # rows 1 '10.1000/ONE', 2 '10.1000/TWO' (matched); fake request: first, through a second sqlite3 connection,
    #   UPDATE works SET doi = '10.1000/OTHER' WHERE id = 1 (commit); then answer works for one and two
    # _topic_row(1) == (None, None, None); _topic_row(2) == ("T2", "T2-field", 1)
    # "[doi] 1 work(s) changed while the batch was fetched; left for the next run" in capsys.readouterr().err

def test_backfill_pass1_skips_a_row_whose_openalex_id_changed_during_the_request(tmp_path, monkeypatch, capsys, backfill):
    # rows 1 openalex_id 'W1', 2 'W2' (matched); fake request changes row 1 to 'W9' first; answers W1 and W2
    #   ("id": "https://openalex.org/W1" ...)
    # row 1 untouched (topics_fetched None), row 2 updated; the '[id] 1 work(s) changed …' line is logged

def test_backfill_unchanged_rows_log_no_changed_line(tmp_path, monkeypatch, capsys, backfill):
    # one matched DOI row, no change -> updated; 'changed while the batch was fetched' not in err

# test_hygiene.py
def test_the_scraper_strips_the_api_key(monkeypatch):
    # setenv OPENALEX_API_KEY "  sekret-key-123\n" -> OpenAlexCrossrefSearcher(rate_limiter=_NoRateLimit()).api_key == "sekret-key-123"

def test_a_blank_api_key_is_unset_and_the_warning_prints_once(monkeypatch, capsys, tmp_path):
    # setenv OPENALEX_API_KEY "   "; delenv RAG_FEEDER_OPENALEX_API_KEY
    # monkeypatch.setattr(utils, "_missing_openalex_api_key_warned", False)
    # searcher = OpenAlexCrossrefSearcher(rate_limiter=_NoRateLimit()); assert searcher.api_key is None
    # setenv RAG_FEEDER_OPENALEX_QUOTA_PATH to tmp_path / "quota.json" (never the repo's data dir); utils.time.sleep -> no-op
    # utils.openalex_request_json(url="https://api.openalex.org/works", params={"filter": "doi:x"},
    #   session=<stub whose get() returns status_code 200, headers {}, json() {"results": []}>, rate_limiter=_FreeLimiter(), retries=1)
    #   (_FreeLimiter and the session-stub style of :555-600)
    # capsys.readouterr().out.count("[WARN] OPENALEX_API_KEY not set.") == 1

def test_the_scraper_has_no_key_reader_of_its_own():
    # source of dl_lit/OpenAlexScraper.py contains neither "getenv('OPENALEX_API_KEY')" nor "_missing_api_key_warned"
```

- [ ] **Step 2: Run them and verify they fail.** Run `cd dl_lit_project && /home/jay/DT/dl_lit_project/.venv/bin/python -m pytest tests/test_doi_rule_scripts.py tests/test_hygiene.py -q`. Expected: FAIL. The annotation differs, the changed rows get topics, the key keeps its whitespace, and the warning prints twice.
- [ ] **Step 3: Implement D10 and D12.**
  - `run_pass` maps `token -> (work_id, raw)`, builds updates and seen rows with `raw`, and calls `write_results(conn, updates, guard_column)`. It counts `matched` and `fielded` over the returned ids only, logs the D10 line when `len(updates) - len(written) > 0`, and calls `mark_fetched(conn, seen, guard_column)`.
  - `main` passes `guard_column="openalex_id"` and `"doi"`.
  - Scraper: `self.api_key = get_openalex_api_key()`; `if not self.api_key: _warn_missing_openalex_api_key()`. Import both from `.utils`; delete `_missing_api_key_warned` and the scraper's own print.
- [ ] **Step 4: Run both files, then `tests/test_worker_settings.py` and the whole suite.** Expected: all passed. `git grep -n "getenv('OPENALEX_API_KEY')" -- dl_lit_project/dl_lit` prints nothing.
- [ ] **Step 5: Commit**: `backfill: topics are written only while the row keeps the identifier they were fetched for; the scraper uses the shared OpenAlex key reader (PY-34, PY-27)`

---

## Final verification (controller, after all tasks)

- `cd dl_lit_project && /home/jay/DT/dl_lit_project/.venv/bin/python -m pytest tests -q`: all passed.
- `cd backend && npm test -- --maxWorkers=2`: all passed.
- `cd frontend && npx vite build`, then the CI Playwright list from `ci.yml:54` (now with `tests/dev-server-config.spec.ts`) with `E2E_FRONTEND_PORT=5190 … --workers=1`: all pass.
- `git diff c574aba --stat` touches no compose file, no Caddyfile, no `backend/src/index.js` and no schema DDL.
- The inventory's reconciliation section records FE-22 and BE-38 as closed (spec D5, D9), and the rows of this phase as done.
- After merge: `docker exec rag_feeder_frontend printenv CHOKIDAR_USEPOLLING` prints `1`, and `docker logs rag_feeder_frontend` shows Vite's restart after the config change with no error. Never the dev DB.
