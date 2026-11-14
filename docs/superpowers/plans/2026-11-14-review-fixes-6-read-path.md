# Review fixes, phase 6: read-path performance — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the Phase 2 rows of the review-fixes-5 inventory plus BE-51, the graph-node lookup. Polls and clicks stop doing work that grows with corpus size, and no request handler reads multi-MB files or gzips synchronously. The spec rules out nine rows, and BE-34 is fixed only for the per-candidate call.

**Architecture:** Five tasks by surface, in this order:
1. the seed read path (`backend/src/seed.js` and the seed routes);
2. the Kantropos browse and the 3D graph (`app.js`, `ThreeGraph.svelte`);
3. the logs tail and the child-output parser (`app.js`, `pythonProcess.js`, the App's log loaders);
4. the Corpus stage filter and the workspace loads (`corpusList.js`, `App.svelte`, `Corpus.svelte`);
5. the `pipeline_jobs` index and the dead ingest route.

No new module and no new dependency. Every performance claim is pinned by its mechanism: a query plan, a statement or call count, a request count, or an absent sync call. No test uses wall-clock thresholds.

**Tech Stack:** Node 20, Express 5, better-sqlite3, `node:stream/promises`, `node:zlib`, Jest + supertest; Svelte 5 + Vite, Playwright (mocked); Python 3.11, pytest, sqlite3.

**Spec:** `docs/superpowers/specs/2026-11-14-review-fixes-6-read-path-design.md`

## Global Constraints

- Work only in `/home/jay/DT/.worktrees/review-fixes-6`, on branch `review-fixes-6`. Do not push.
- Never open `dl_lit_project/data/literature.db`. Tests use `:memory:`, `fs.mkdtempSync` or `tmp_path` databases.
- Backend tests: `cd backend && npm test -- tests/<file>` while you iterate, then `npm test` once per task. Jest finds new files by itself, so no CI list changes for the backend.
- Python tests: `cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests -q`. This phase adds no new Python test file; the additions go into files already listed at `.github/workflows/ci.yml:68`.
- Frontend tests:
  - Run them from `frontend/` with `E2E_FRONTEND_PORT=5190 npx playwright test <specs> --workers=1`, and only the specs the task touches. Memory is low; CI runs the full list.
  - Run mocked specs only. Never use the dev stack on `:5175`. Never run `*.real*.spec.ts` or `scraper-lab.spec.ts`; the latter is a live spec that defaults to `literature.db`.
  - The touched specs are already in the list at `ci.yml:54`.
  - `npx vite build` must pass.
- Every mocked spec that fakes a token calls `mockLogSocket(page)`.
- These values are fixed by the spec:
  - D10: `LOG_READ_CHUNK_BYTES = 64 * 1024`.
  - D16: the index is `idx_pipeline_jobs_status_finished ON pipeline_jobs(status, finished_at)`.
  - D13: the stage values are `raw | metadata | downloaded | failed_enrichment | failed_download`. The `metadata` stage covers `matched`, `queued_download` and `enriching`.
  - D11: the scope value is `readable`.
- These strings are copied exactly from the spec:
  - D15: `Loading…`, with a single ellipsis character.
  - D12: the thrown error, `No valid JSON output from python. Output was: <output>`, is unchanged.
- Each commit message names the inventory rows it closes and ends with:
  ```
  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01U3c4gfg7jQvvmPdpLNBN5T
  ```
- Secrets stay in `.env`. Nothing from it goes into tests, docs or commits.

## Review Focus

1. **A candidate file request for another corpus's seed, or for a dismissed candidate.** D2 drops the resolver. The one-row read must keep today's corpus and dismissal scoping and answer 404, never another corpus's PDF. Test in Task 1.
2. **A UTF-8 character or a CRLF split across a 64 KiB read boundary, and a file without a trailing newline.** The backward reader must return byte-identical lines in the same order as today. Test in Task 3, the parity test.
3. **A log file rotated or deleted between two reads of one tail.** A missing rotated file counts as empty. The request must answer 200 and must not throw. Test in Task 3.
4. **A Kantropos work in several assigned corpora, where the page limit falls inside its rows.** Each page item's `source_corpora` must still list every corpus, in today's order. Test in Task 2, the fixture's shared work at the page boundary.
5. **A stage filter, a typed `q` and a background poll together.** The poll's prefix reload must carry the stage. A row that leaves the stage must disappear on the next poll, not leave a stale row. Test in Task 4.

---

### Task 1: Seed read path (BE-04, BE-05, BE-09, BE-48, BE-34 in part)

**Files:**
- Modify: `backend/src/seed.js`:
  - `:290-298` (comment);
  - `:330-430` (`createStateResolver`: hoist `tableExists`);
  - `:766-780` (`countSeedCandidates`);
  - `:889-1030` (`listSeedSources`);
  - new `countPdfCandidates` and `findSeedCandidateDownload`.
- Modify: `backend/src/app.js`:
  - `:1949-1988` (`downloadedFilePathCandidates`, `resolveDownloadedFilePath`, exported `findDownloadedFilePath`);
  - `:2606-2617` (delete `app.warmSeedState`);
  - `:4769-4832` (the candidates route stops passing `stateResolver` to `countSeedCandidates`);
  - `:4834-4878` (the candidate file route).
- Modify: `backend/src/index.js:102-112` (remove `seedWarmupTimer`).
- Test: new `backend/tests/seed-read-path.test.js`. Extend `backend/tests/seed-candidates-route.test.js` only if the route test needs its fixtures.

**Interfaces:**
- Produces (in `seed.js`):
  - `countPdfCandidates(db, corpusId, sourceKey, needle) -> number`;
  - `findSeedCandidateDownload(db, corpusId, sourceType, sourceKey, candidateKey) -> number | null`;
  - `countSeedCandidates(db, corpusId, sourceType, sourceKey, { q = '' } = {})`, which no longer takes `stateResolver`.
- Produces (in `app.js`): `export function findDownloadedFilePath(storedPath, context = {}) -> string | null`.
- `listSeedSources` keeps its signature and output.

- [ ] **Step 1: Write the failing tests** in `seed-read-path.test.js`. Reuse `createSeedDb()` from `seed-state.test.js`: copy it into the new file, since a test file exports nothing. The fixture holds the following:
  - corpus 1, with pdf seed `paperA` and 5 `ingest_entries`. One of them is dismissed, one matches only through its second author, and one has a downloaded work in corpus 2 (`works` and `corpus_works`);
  - pdf seed `paperB` in corpus 2;
  - search run 7, linked to corpus 1 with three `search_results`. One of them matches a downloaded work of corpus 1, and one is dismissed.

```js
function recordSql(db) { const seen = []; const orig = db.prepare.bind(db); db.prepare = (sql) => { seen.push(sql); return orig(sql) }; return seen }

test('the light seed list reads no works row and probes no file', () => {
  const probe = jest.fn(() => '/x.pdf')
  const seen = recordSql(db)
  const sources = listSeedSources(db, 1, { withStateCounts: false, resolveDownloadedFilePath: probe, q: '' })
  expect(probe).not.toHaveBeenCalled()
  expect(seen.some((sql) => /\bFROM works\b|JOIN works\b/.test(sql))).toBe(false)
  expect(sources.find((s) => s.source_key === 'paperA').candidate_count).toBe(4)
})
test('light counts equal the resolved candidate list for every q', () => {
  for (const q of ['', 'second author', 'nomatch', 'PUBLISHER']) {
    const light = listSeedSources(db, 1, { withStateCounts: false, q })
    const full = listSeedSources(db, 1, { withStateCounts: true, q })
    expect(light.map((s) => [s.id, s.candidate_count])).toEqual(full.map((s) => [s.id, s.candidate_count]))
    expect(light.every((s) => s.state_counts === null)).toBe(true)
  }
})
test('countSeedCandidates pdf equals listSeedCandidates length', () => { /* q '' and 'second author' */ })
test('findSeedCandidateDownload decides as the full resolver does', () => {
  // for every candidate of paperA and of run 7, plus 'ingest:<id of paperB entry>', 'search:999', 'bogus', 'ingest:0':
  // expected = full list .find(key): state in {downloaded, downloaded_elsewhere} && downloaded_work_id > 0 ? id : null
  // (paperB's key and the malformed keys expect null)
})
test('findSeedCandidateDownload never builds the corpus-local resolver', () => {
  // seen = recordSql(db); call it for the downloaded candidate
  expect(seen.some((sql) => sql.includes('JOIN corpus_works cw ON cw.work_id = t.id'))).toBe(false)
})
test('a dismissed candidate and another corpus\'s seed answer null', () => { /* Review Focus 1 */ })
test('findDownloadedFilePath probes an existing absolute path once', () => {
  // tmp file; spy = jest.spyOn(fs, 'existsSync'); findDownloadedFilePath(file) === file; spy calls with file === 1
})
test('index.js schedules no seed warm-up and createApp exposes none', () => {
  expect(fs.readFileSync(new URL('../src/index.js', import.meta.url), 'utf8')).not.toMatch(/warmSeedState/)
  // createApp({ broadcast: () => {} }).warmSeedState === undefined (stub mode, as logs.test.js boots it)
})
```

- [ ] **Step 2: Run them and verify they fail.** Run `cd backend && npm test -- tests/seed-read-path.test.js`. Expected: FAIL. The cases are:
  - `findSeedCandidateDownload` and `findDownloadedFilePath` are not exported;
  - the light list prepares the `FROM works` statements and calls the probe;
  - `existsSync` is called twice;
  - `index.js` contains `warmSeedState`.

  The parity cases (`light counts equal…`, `countSeedCandidates pdf…`) already pass on `0702e47`; they must keep passing.
- [ ] **Step 3: Implement D1 to D4.**
  - D1:
    - `listSeedSources` uses `let resolver = stateResolver; const getResolver = () => (resolver ??= createStateResolver(db, corpusId, { resolveDownloadedFilePath }))`.
    - The pdf branch calls `listSeedCandidates` with `getResolver()` only when `withStateCounts`. Otherwise it calls `countPdfCandidates(db, corpusId, sourceKey, needle)`, where `needle` is `String(q || '').trim().toLowerCase()`, as `listSeedCandidates` computes it.
    - The search listing calls `getResolver()` only when `withinLimit`.
    - `countSeedCandidates`'s pdf branch uses `countPdfCandidates`, and the candidates route's two calls drop `stateResolver`.
  - D2: the reduced bundle is `{ resolveState, resolveDownloadedWorkId, isInCorpus: () => false }`, with no `resolveDownloadedAvailability`, so the normalizers take their `downloaded_work_id`-only branch. The membership probe is one prepared statement, reused for every matched id. The route replaces its `listSeedCandidates(...).find(...)` and state check with `const workId = findSeedCandidateDownload(...)`. When it is `null`, the route answers `404 { error: 'Downloaded file not found for this seed candidate' }` (today's text).
  - D3: delete `app.warmSeedState` and the `index.js` timer and its `close` handler.
  - D4: do the following.
    - `downloadedFilePathCandidates` holds the current candidate-building code.
    - `resolveDownloadedFilePath` returns `list.find(fs.existsSync) || list[0] || null`.
    - `findDownloadedFilePath` returns `list.find(fs.existsSync) || null`.
    - The file route uses `findDownloadedFilePath(row.file_path, { originKey: row.origin_key })` and drops its own `existsSync`.
    - `createStateResolver` computes `const worksTable = tableExists(db, 'works')` once.
    - The comment at `seed.js:295-297` says that the corpus-local half loads every corpus row.
- [ ] **Step 4: Run the file, `seed-state.test.js`, `seed-candidates-route.test.js` and `seed-document.test.js`, then `npm test`.** Expected: all passed.
- [ ] **Step 5: Commit** — `seed: the seed list counts pdf candidates without the state resolver, the candidate file resolves one candidate against the cached downloaded index, no boot warm-up, one existence probe per path (BE-04, BE-05, BE-09, BE-48, BE-34 per-candidate)`

---

### Task 2: Kantropos browse and 3D graph (BE-08, BE-11, BE-12, BE-36, BE-37, BE-51)

**Files:**
- Modify: `backend/src/app.js`:
  - `:487-529` (`buildGraph3dSnapshot` awaits `gzipSnapshotFiles`);
  - `:1215-1223` (`countBibEntriesInFile(filePath, stats)` with its memo);
  - `:1356-1415` (the overview passes `stats`);
  - `:1520-1525` (edges);
  - `:1632-1703` (`listPendingKantroposWorkItems`, `upstreamFileIsPresent`);
  - `:1705-1777` (`buildKantroposBrowsePayload` consumes the new return shape);
  - `:5508-5572` (the node route uses `findGraphNodeWork`);
  - `:5574-5592` (delete the cluster route).
- Modify:
  - `frontend/src/components/ThreeGraph.svelte` (`:7` import, `:98` `clusterDetailCache`, `:1139-1160` `loadClusterDetail`);
  - `frontend/src/lib/api.js:818-823` (delete `fetchGraph3DClusterDetail`).
- Modify: `backend/tests/graph-access.test.js:189-212` (drop the cluster URL from the loop; the lonely-user check uses `/api/graph/3d/snapshot/${fx.k1}/clusters.json`).
- Test:
  - new `backend/tests/kantropos-browse.test.js`, with its setup copied from `corpus-sharing.test.js`: temp DB and Kantropos root, admin login, the `RAG_FEEDER_KANTROPOS_CORPORA` env;
  - new `backend/tests/graph-read-path.test.js`;
  - extended `frontend/tests/graph-3d.spec.ts`.

**Interfaces:**
- Produces (exported from `app.js`):
  - `gzipSnapshotFiles(dir: string, fileNames: string[]) -> Promise<void>`;
  - `findGraphNodeWork(db, nodeId: string, selectList: string, columns: Set<string>) -> object | undefined`.
- `listPendingKantroposWorkItems(db, targetId, importedWorkIdSet, pendingLimit) -> { items, totalCount, missingFileCount, pendingCountByCorpus: Map<number, number> }`. It is internal, and only `buildKantroposBrowsePayload` consumes it.
- The browse JSON and the node JSON are unchanged.

- [ ] **Step 1: Write the failing tests.**

```js
// kantropos-browse.test.js — target-a with metadata.bib (3 entries, 2 imported works); 3 assigned corpora;
// 7 downloaded pending works with years so that the page boundary (pending_limit=3) falls on
// work W5, a member of corpora 2 and 3 (Review Focus 4); 2 of the 7 have no file on disk
// (one real temp file per present work); citation_edges with the UNIQUE constraint
// (DDL as corpus-sharing.test.js:262) holding 3 edges among page nodes + 2 to off-page works.
test('browse payload is unchanged (golden)', async () => {
  // GET /api/kantropos/corpora/target-a/browse?pending_limit=3
  // expect summary.{pending_items_count: 5, pending_skipped_missing_file: 2, pending_items_loaded, pending_items_has_more: true,
  //   current_items_count: 2, metadata_entry_count: 3}, assigned_corpora[*].{pending_count, downloaded_count},
  //   pending_items ids in order, W5.source_corpora names in LOWER(name), id order, file_missing flags,
  //   graph.edges (only the 3 among nodes) — literal values written against 0702e47
})
test('browse reads full rows only for the page and edges through the UNIQUE index', async () => {
  const seen = []; const orig = app.authDb.prepare.bind(app.authDb)
  jest.spyOn(app.authDb, 'prepare').mockImplementation((sql) => { seen.push(sql); return orig(sql) })
  // GET browse
  const full = seen.filter((sql) => /SELECT w\.\*/.test(sql))
  expect(full.length).toBeGreaterThan(0)
  expect(full.every((sql) => sql.includes('json_each'))).toBe(true)
  const edgeSql = seen.find((sql) => sql.includes('FROM citation_edges'))
  expect(edgeSql).toContain('json_each')
  const plan = orig(`EXPLAIN QUERY PLAN ${edgeSql}`).all('[]', '[]').map((r) => r.detail).join(' | ')
  expect(plan).toContain('sqlite_autoindex_citation_edges_1')
  expect(plan).not.toMatch(/SCAN citation_edges/)
})
test('missing upstream files are probed without throwing', async () => {
  // spy = jest.spyOn(fs, 'statSync'); GET browse; every call for a pending file_path has args[1] { throwIfNoEntry: false }
})
test('metadata.bib is read once until it changes', async () => {
  // spy fs.readFileSync; GET /api/kantropos/corpora twice -> metadata.bib read once;
  // append an entry (size changes) -> third GET reads it again and metadata_entry_count is 4
})

// graph-read-path.test.js
test('findGraphNodeWork never scans works', () => {
  // :memory: db, works(id, title, openalex_id, doi, normalized_doi) + the two partial indexes with the exact
  // DDL from dl_lit_project/dl_lit/db_manager.py:24-25; record SQL via a prepare wrapper; call with '12', 'W77', '10.1/ABC'
  // every recorded statement: EXPLAIN QUERY PLAN has no /SCAN w\b/ and no /SCAN works/
})
test('findGraphNodeWork resolves id, openalex id and normalized DOI; raw-only doi no longer matches', () => {
  // '12' -> id 12; 'W77' -> row with openalex_id 'W77'; '10.1/ABC' -> row with normalized_doi '10.1/ABC' (doi '10.1/abc');
  // a row with doi 'RAW' and normalized_doi NULL is not found by 'RAW'; '' openalex rows never match ''
})
test('gzipSnapshotFiles streams: gunzip equals the source and no sync gzip runs', async () => {
  const gz = jest.spyOn(zlib, 'gzipSync')
  // dir with nodes.bin (2 MB random) and clusters.json; missing edges.bin is skipped
  let ticked = false; setImmediate(() => { ticked = true })
  await gzipSnapshotFiles(dir, ['nodes.bin', 'clusters.json', 'edges.bin'])
  expect(ticked).toBe(true)
  expect(gz).not.toHaveBeenCalled()
  // zlib.gunzipSync(read(nodes.bin.gz)) equals read(nodes.bin); no edges.bin.gz
})
```

```ts
// graph-3d.spec.ts
test('selecting a cluster shows its detail without a request', async ({ page }) => {
  let clusterRequests = 0
  await page.route('**/api/**', async (route) => {
    if (new URL(route.request().url()).pathname.startsWith('/api/graph/3d/cluster/')) clusterRequests += 1
    await mockApi(route)
  })
  const panel = await openLoadedGraphPanel(page)
  await panel.locator('.graph-3d-cluster-label').first().click()
  await expect(panel.getByText('Economic sociology / seed').first()).toBeVisible()
  await expect(panel.getByText('Countries: DE')).toBeVisible()
  expect(clusterRequests).toBe(0)
})
```

- [ ] **Step 2: Run them and verify they fail.** Run `cd backend && npm test -- tests/kantropos-browse.test.js tests/graph-read-path.test.js tests/graph-access.test.js`, then from `frontend/`: `E2E_FRONTEND_PORT=5190 npx playwright test tests/graph-3d.spec.ts --workers=1`. Expected: the golden test passes on `0702e47` and must keep passing. These cases FAIL:
  - `SELECT w.*` without `json_each`, and the edges without `WHERE`;
  - `statSync` without options;
  - `metadata.bib` read on each request;
  - the missing exports;
  - one cluster request.
- [ ] **Step 3: Implement D5 to D9.**
  - D5: `allowed = JSON.stringify([...allowedNodeIds])`, bound twice.
  - D6:
    - Pass 1 and pass 2 follow the spec. Pass 2's ids are the first `pendingLimit` distinct work ids of pass 1.
    - The summary uses these formulas:
      - `pending_items_count = totalCount - missingFileCount`;
      - `pending_items_has_more = items.length < totalCount`;
      - `bucket.pending_count = pendingCountByCorpus.get(id) || 0`.
    - `upstreamFileIsPresent` keeps its `try/catch` for other errors.
    - The memo `Map` is keyed by the path. Its entry is `{ mtimeMs, size, count }`, and an entry is reused when both `mtimeMs` and `size` match.
  - D7:
    - The route builds `selectList` and `columns` as today and calls `findGraphNodeWork`.
    - The numeric probe matches `/^\d+$/`.
    - The openalex probe runs only when `columns.has('openalex_id')`, and the DOI probe only when `columns.has('normalized_doi')`.
  - D8: `loadClusterDetail(cluster)` becomes `selectedClusterDetail = cluster || null`. Remove the cache, the import, the API export and the route.
  - D9: `import { pipeline } from 'node:stream/promises'`. Probe each file with `fs.existsSync` before streaming it.
- [ ] **Step 4: Run the Task 2 backend files plus `corpus-sharing.test.js`, `access-routes.test.js` and `graph.test.js`, then `npm test`; the Playwright spec; and `npx vite build`.** Expected: all pass.
- [ ] **Step 5: Commit** — `kantropos/graph: browse edges through the UNIQUE index, pending works in two passes with exception-free file probes, metadata.bib count kept until it changes, indexed graph-node lookup, cluster detail from the loaded snapshot, streamed snapshot gzip (BE-08, BE-11, BE-12, BE-36, BE-37, BE-51)`

---

### Task 3: Logs tail and child output (BE-10, FE-59, BE-42)

**Files:**
- Modify: `backend/src/app.js`:
  - `:2448-2516`: replace `readTailLinesFromFile`, `readAllLogLines` and the synchronous `readLogWindow` with the exported async `readLogWindow`;
  - `:5950-6030`: the async route, `scope=readable`, no `total_lines`.
- Modify: `backend/src/pythonProcess.js:35-46` (the fallback) and a new internal `matchingCloseIndex`.
- Modify: `frontend/src/lib/api.js:655-671` (`fetchLogsTail` gains `scope`) and `frontend/src/App.svelte:4268-4274`, `:4319-4325` (pass `scope`).
- Test:
  - new `backend/tests/logs-tail-reader.test.js`;
  - extended `backend/tests/access-routes.test.js`, `backend/tests/python-process.test.js` and `frontend/tests/logs-stream.spec.ts`.

**Interfaces:**
- Produces: `export async function readLogWindow(baseFilePath, { limit = 200, cursor = 0, includeRotated = true, maxFiles = 5, accept = () => true } = {}) -> Promise<{ entries: string[], hasMore: boolean, nextCursor: number, cursor: number }>`.
- `fetchLogsTail({ type, lines, includeRotated, cursor, corpusId, scope = 'corpus' })`. With `scope === 'readable'` and type `pipeline`, it sends `scope=readable` and no `corpus_id`.
- The response fields are unchanged except that `total_lines` is gone.

- [ ] **Step 1: Write the failing tests.**

```js
// logs-tail-reader.test.js
function reference(files, { limit, cursor, accept }) { // today's semantics, written out in the test
  const all = files.flatMap((text) => text.split(/\r?\n/).map((l) => l.trimEnd()).filter(Boolean)).filter(accept)
  const safe = Math.min(cursor, all.length); const end = all.length - safe; const start = Math.max(0, end - limit)
  return { entries: all.slice(start, end), hasMore: start > 0, nextCursor: safe + (end - start) }
}
test('matches the full-read reference across files, chunk boundaries, CRLF and multibyte text', async () => {
  // base + .1 .. .5 written oldest-first; .3 missing; lines include 'ü€😀' placed so that one straddles 65536 bytes,
  // CRLF endings, blank and whitespace-only lines, the newest file without a trailing newline, ~150 KiB per file
  for (const accept of [() => true, (l) => /\bcorpus=12\b/.test(l)])
    for (const cursor of [0, 1, 199, 1000, 2500]) for (const limit of [1, 200, 2000])
      expect(await readLogWindow(base, { limit, cursor, maxFiles: 5, accept })).toMatchObject(reference(files, { limit, cursor, accept }))
})
test('a default tail opens only the newest file when it holds enough lines', async () => {
  const open = jest.spyOn(fs.promises, 'open')
  await readLogWindow(base, { limit: 200, cursor: 0 })
  expect(open.mock.calls.map(([p]) => path.basename(p))).toEqual(['backend-pipeline.log'])
})
test('the tail route reads no file synchronously', async () => {
  const sync = jest.spyOn(fs, 'readFileSync')
  // stub-mode app as logs.test.js; GET /api/logs/tail?type=pipeline&lines=4
  expect(sync.mock.calls.filter(([p]) => String(p).includes('backend-pipeline'))).toEqual([])
})
test('a rotated file deleted mid-read is empty, not an error', async () => { /* Review Focus 3: delete .2 inside an open spy's first call; status 200 */ })

// access-routes.test.js — beforeAll appends `[p] untagged-line` and `[p] corpus=none line-n` to the pipeline log
test('logs: scope=readable shows what the live socket would', async () => {
  const viewer = await request(app).get('/api/logs/tail?type=pipeline&scope=readable').set(as('viewer3'))
  expect(viewer.body.entries).toEqual([`[p] corpus=${ids.A} line-a`])
  const admin = await request(app).get('/api/logs/tail?type=pipeline&scope=readable').set(as('admin'))
  expect(admin.body.entries).toEqual([`[p] corpus=${ids.A} line-a`, `[p] corpus=${ids.B} line-b`, '[p] untagged-line', '[p] corpus=none line-n'])
  expect(admin.body).not.toHaveProperty('total_lines')
})
// the two existing pipeline tests keep their expectations (no scope -> current corpus; corpus_id checked)

// python-process.test.js
test('fallback parsing is linear in the number of bracketed lines', () => {
  const parse = jest.spyOn(JSON, 'parse')
  const output = Array.from({ length: 300 }, (_, i) => `[DB Manager] step ${i} [${i}/300]`).join('\n')
  expect(() => parseJsonFromOutput(output)).toThrow('No valid JSON output from python.')
  expect(parse.mock.calls.length).toBeLessThanOrEqual(2 * 300 + 1)
})
test('a pretty-printed block before trailing log lines is found', () => {
  // '[log] a\n{\n  "ok": true,\n  "n": [1, 2]\n}\n[log] done' -> { ok: true, n: [1, 2] }
  // '{\n "s": "a } ] {"\n}' -> { s: 'a } ] {' }  (brackets inside strings)
  // '{\n "a": 1\n} done' -> { a: 1 }  (D12 controller call)
})
```

```ts
// logs-stream.spec.ts — record url.searchParams for every /api/logs/tail in boot()'s handler (tail.params)
test('turning "Current corpus only" off loads the readable scope', async ({ page }) => {
  const { tail } = await boot(page, '#/logs')
  await panel(page).getByLabel('Current corpus only').uncheck()
  await expect.poll(() => tail.params.at(-1)?.get('scope')).toBe('readable')
  expect(tail.params.at(-1)?.has('corpus_id')).toBe(false)
  expect(tail.params[0].get('corpus_id')).toBe('1')   // filter on at boot
})
```

- [ ] **Step 2: Run them and verify they fail.** Run `cd backend && npm test -- tests/logs-tail-reader.test.js tests/access-routes.test.js tests/python-process.test.js tests/logs.test.js`, then from `frontend/`: `E2E_FRONTEND_PORT=5190 npx playwright test tests/logs-stream.spec.ts --workers=1`. Expected: FAIL on these cases:
  - `readLogWindow` is not exported (it is synchronous and internal);
  - `readFileSync` is called;
  - the readable scope returns only corpus A for the admin;
  - more than 600 `JSON.parse` calls (about 45k);
  - no `scope` param.
- [ ] **Step 3: Implement D10 to D12.**
  - D10:
    - Each file is opened once, read backwards with `handle.read(buffer, 0, len, position)`, and closed in a `finally`.
    - `ENOENT` on open, and on any read, makes the file count as empty.
    - Collected lines are unshifted in the order they are found, so `entries` comes out oldest-first.
  - D11:
    - The route computes `accept`:
      - with `corpus_id` given, or no `scope`: today's `extractLogCorpusTag(line) === String(logCorpusId)`;
      - with `scope=readable`: admin or stub mode accepts every line; otherwise the line needs a numeric tag in the user's `user_corpora` set.
    - The `corpus_id`, `req.corpusId` and 403 logic is unchanged when `scope` is absent.
    - The frontend passes `scope: logsFilterCurrentCorpus ? 'corpus' : 'readable'` from both loaders.
  - D12: `matchingCloseIndex(text, start) -> number`, then one `tryParse` per bracketed line, from the last line to the first.
- [ ] **Step 4: Run the Task 3 backend files, then `npm test`; then `logs-stream.spec.ts` and `app.spec.ts` (logs type switch); then `npx vite build`.** Expected: all pass. Also check that `grep -n "readFileSync" backend/src/app.js` lists no log reader.
- [ ] **Step 5: Commit** — `logs/python: the log tail reads backwards in chunks and stops at its window, "Current corpus only" off loads every line the live view may show, the JSON fallback follows brackets instead of trying every line pair (BE-10, FE-59, BE-42)`

---

### Task 4: Corpus stage filter and workspace loads (FE-14, FE-18, FE-17)

**Files:**
- Modify: `backend/src/corpusList.js`:
  - `:327-345` (`readCorpus` takes `stage`; the `STAGE_STATUSES` map; the `where` term);
  - `:408` (`rest` joins every non-corpus term);
  - `:471` (the result gains `stage_total`).
- Modify: `backend/src/app.js:5180-5195` (`/api/corpus` passes `stage: String(req.query?.stage || '')`).
- Modify:
  - `frontend/src/lib/api.js:519-531` (`fetchCorpus({ …, stage = 'all' })` returns `stageTotal`);
  - `frontend/src/App.svelte`:
    - `:2099-2140`, `:2158-2205` (`corpusStageFilter: 'all'` in `initialWorkspaceState` and its restore);
    - `:2947` area (new `handleCorpusStageChange(value)`);
    - `:3784-3800` (signature and `applyCorpusCounts(total, stageTotals, stageTotal)`);
    - `:3807-3953` (`loadCorpus` sends `stage`, also in the prefix reload);
    - `:1058-1063` (D14);
    - `:58`, `:69`, `:5567`, `:5794` (D15);
    - `:5488-5515` (Corpus props);
  - `frontend/src/components/Corpus.svelte:113-132`, `:216-229`, `:277`, `:368` (props instead of local filtering).
- Test:
  - extended `backend/tests/corpus-list.test.js`;
  - extended `frontend/tests/corpus-load.spec.ts` (the `Params` type gains `stage`), `frontend/tests/dead-code.spec.ts` and `frontend/tests/boot-requests.spec.ts`.

**Interfaces:**
- Produces:
  - `listCorpus(db, { …, stage = '' })`, whose result gains `stage_total: number`;
  - `/api/corpus?stage=<value>`;
  - `fetchCorpus` returns `{ data, total, source, stageTotals, statusCounts, stageTotal }`, where `stageTotal = Number(payload.stage_total ?? total)`.
- Corpus props: `corpusStageFilter: string`, `onStageFilterChange: (value: string) => void`.

- [ ] **Step 1: Write the failing tests.**

```js
// corpus-list.test.js (fixture corpus 1, freshDb())
test('a stage returns exactly the unfiltered rows of that stage, paged, with its total', () => {
  const all = listCorpus(copy, { corpusId: 1, limit: 1000 }).items
  for (const [stage, statuses] of Object.entries({ raw: ['raw'], metadata: ['matched', 'queued_download', 'enriching'],
      downloaded: ['downloaded'], failed_enrichment: ['failed_enrichment'], failed_download: ['failed_download'] })) {
    const want = all.filter((i) => statuses.includes(i.status)).map((i) => i.id)
    const got = listCorpus(copy, { corpusId: 1, limit: 1000, stage })
    expect(got.items.map((i) => i.id)).toEqual(want)
    expect(got.stage_total).toBe(want.length)
    expect(got.total).toBe(all.length)                       // labels keep their totals
    expect(listCorpus(copy, { corpusId: 1, limit: 2, offset: 1, stage }).items.map((i) => i.id)).toEqual(want.slice(1, 3))
  }
})
test('stage combines with q and with every sort', () => { /* for sort in CORPUS_SORT_KEYS and q 'labour': ids equal the stage-filtered unstaged listing */ })
test('an unknown stage means all stages', () => { /* stage 'bogus' and 'all' -> same items as none, stage_total === total */ })
test('the large-corpus year path keeps both q and stage', () => { /* smallCorpusLimit: 1, q + stage: ids equal the filtered reference */ })
```

```ts
// corpus-load.spec.ts (rows(params) honours params.stage: 'downloaded' answers ids 501-560, total 300, stage_total 60)
test('the stage filter loads that stage from the server and pages within it', async ({ page }) => {
  // select 'downloaded' -> last /api/corpus request has stage=downloaded, offset 0; rows are 501..
  // "Show more" -> next request has stage=downloaded and offset 120 (CORPUS_PAGE_SIZE)
  // the option label still reads "All stages (300)"
})
test('a poll keeps the stage', async ({ page }) => { /* after the stage is chosen, the next quiet poll carries stage=downloaded (Review Focus 5) */ })
// the existing 'a corpus switch clears the Corpus selection and stage filter' stays green, and its
// first request after the switch has no stage param

// dead-code.spec.ts
test('extraction completion loads the seed, stats and runs in parallel', async ({ page }) => {
  // hook: hold /api/seed/sources/pdf/paper/candidates, /api/ingest/stats and /api/ingest/runs responses
  // issued after the done frame until all three have been requested (or 3 s passed); record max in flight
  expect(maxInFlight).toBe(3)
})

// boot-requests.spec.ts
test('the graph and scraper code loads on first open only', async ({ page }) => {
  // page.on('request'): urls; boot on #/workspace and wait for /api/corpus
  // no url path ends with '/src/components/ThreeGraph.svelte' or '/src/components/ScraperLab.svelte'
  // click tab-graph -> a ThreeGraph.svelte request appears
})
```

- [ ] **Step 2: Run them and verify they fail.** Run `cd backend && npm test -- tests/corpus-list.test.js`, then from `frontend/`: `E2E_FRONTEND_PORT=5190 npx playwright test tests/corpus-load.spec.ts tests/dead-code.spec.ts tests/boot-requests.spec.ts --workers=1`. Expected: FAIL on these cases:
  - `stage` is ignored;
  - there is no `stage_total`;
  - no `stage` param is sent;
  - at most 1 request in flight;
  - `ThreeGraph.svelte` is requested at boot.
- [ ] **Step 3: Implement D13 to D15.**
  - D13:
    - The `where` term is built from literals only, so no new parameter is bound.
    - `const rest = where.slice(scoped ? 1 : 0).map((t) => ` AND ${t}`).join('')` replaces the `needle`-only `rest`. The corpus term is always first when `scoped`.
    - `stage_total`: `stage ? stageTotals(statusCounts)[stage] : total`.
    - `handleCorpusStageChange(value)` sets `corpusStageFilter` and runs `loadCorpus({ quiet: true, supersede: true })`.
    - `corpusPageSignature` adds `corpusStageFilter`.
    - The "Showing" text and `corpusHasMore` use `corpusMatchTotal` (new state, set from `stageTotal`).
    - `Corpus.svelte`'s `{#each}`, `selectableWorkIds` and the empty state use `corpusItems`.
  - D14: `await Promise.all([...])` replaces the three awaits and the two middle `stale()` checks.
  - D15: `{#await import('./components/ThreeGraph.svelte')}<p class="muted">Loading…</p>{:then module}<svelte:component this={module.default} … />{/await}`, and the same for `ScraperLab.svelte`. Remove the two static imports.
- [ ] **Step 4: Run `corpus-list.test.js`, then `npm test`; then the three Task 4 specs plus `graph-3d.spec.ts`, `table-columns.spec.ts`, `corpus-reconcile.spec.ts`, `tab-resources.spec.ts` and `refresh-scheduler.spec.ts`, one spec file at a time with `--workers=1`; then `npx vite build`.** Expected: all pass, and the build output has separate chunks for `ThreeGraph` and `ScraperLab`.
- [ ] **Step 5: Commit** — `corpus/frontend: the stage filter runs on the server and pages within the stage, extraction completion loads in parallel, graph and scraper tabs load on first open (FE-14, FE-18, FE-17)`

---

### Task 5: `pipeline_jobs` index and the dead ingest route (BE-45, BE-06)

**Files:**
- Modify: `backend/src/app.js`:
  - `:910-912` (create the index after the `worker_id` ensure, inside the same `tableExists` guard);
  - `:164` and `:5640-5655` (delete).
- Modify: `dl_lit_project/dl_lit/db_manager.py:23-43` (`WORKS_INDEXES` gains the index DDL).
- Delete: `backend/scripts/ingest_latest.py`.
- Modify:
  - `frontend/src/lib/api.js:298-302` (delete `fetchBibliographyEntries`);
  - `frontend/README.md:23` (drop the line);
  - `frontend/tests/pipeline-perf.real-pdf.spec.ts:215-224`: read `/api/seed/sources/pdf/${encodeURIComponent(runName)}/candidates?limit=1000` and take the ids from `candidates`. Do not run it.
- Modify: `dl_lit_project/tests/test_identifiers.py:56`, `:199` (drop the `ingest_latest` entries).
- Test: extended `backend/tests/schema-indexes.test.js`, `dl_lit_project/tests/test_schema_indexes.py` and `dl_lit_project/tests/test_claim_index_plan.py`.

**Interfaces:**
- Produces the index `idx_pipeline_jobs_status_finished`. Phase 7's retention prune relies on it.

- [ ] **Step 1: Write the failing tests.**

```js
// schema-indexes.test.js
test('ensureAuthSchema indexes pipeline_jobs by status and finished_at, idempotently', () => {
  // withDb: ensureAuthSchema twice; index present; PRAGMA index_info -> ['status', 'finished_at']
  // EXPLAIN QUERY PLAN of the dedupe SQL from app.js:2962-2968 (job_type = ?, corpus_id IS ?, status IN (?, ?))
  //   and of `DELETE FROM pipeline_jobs WHERE status IN ('completed','failed','cancelled') AND finished_at < ?`
  //   both contain 'idx_pipeline_jobs_status_finished'
})
test('the ingest/latest route is gone', async () => { /* stub-mode app: GET /api/ingest/latest -> 404 */ })
```

```python
# test_schema_indexes.py
def test_pipeline_jobs_status_index_is_created(tmp_path):
    db = DatabaseManager(str(tmp_path / "lit.db"))
    names = {r[0] for r in db.conn.execute("SELECT name FROM sqlite_master WHERE type='index'")}
    assert "idx_pipeline_jobs_status_finished" in names
# test_claim_index_plan.py
def test_worker_job_probes_use_the_status_index(tmp_path):
    # EXPLAIN QUERY PLAN for: the fetch_next_job SELECT shape (status = 'pending' AND job_type = 'download'),
    # the _has_pending_interactive_jobs SELECT, and "UPDATE pipeline_jobs SET status='pending' WHERE status='running' AND worker_id=?"
    # each plan names idx_pipeline_jobs_status_finished
```

- [ ] **Step 2: Run them and verify they fail.** Run `cd backend && npm test -- tests/schema-indexes.test.js` and `cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests/test_schema_indexes.py tests/test_claim_index_plan.py -q`. Expected: FAIL. The index is missing, and the route answers through the stub fallback instead of 404. Check the stub behaviour first; if it already answers 404, assert that `INGEST_LATEST_SCRIPT` and the route string are absent from `app.js` instead.
- [ ] **Step 3: Implement D16 and D17.** Use the same DDL string in both places: `CREATE INDEX IF NOT EXISTS idx_pipeline_jobs_status_finished ON pipeline_jobs(status, finished_at)`. Then delete the route, the constant, the script, the export and the README line, and update `test_identifiers.py` and the real-pdf spec.
- [ ] **Step 4: Run the whole backend suite, the whole Python suite, then `dead-code.spec.ts` with `--workers=1` and `npx vite build`.** Expected: all pass. `grep -rn "ingest/latest\|ingest_latest" backend frontend/src dl_lit_project/tests` lists only `frontend/tests/dead-code.spec.ts`.
- [ ] **Step 5: Commit** — `schema/cleanup: pipeline_jobs indexed by status and finished_at for the daemon, the dedupe and the coming retention prune; the dead /api/ingest/latest route, script and export are deleted (BE-45, BE-06)`
