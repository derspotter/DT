# Review fixes, phase 6: read-path performance — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the Phase 2 rows of the review-fixes-5 inventory plus BE-51, the graph-node lookup. Polls and clicks stop doing work that grows with corpus size, and no request handler reads multi-MB files or gzips synchronously. The spec rules out nine rows; BE-34 is fixed only for the per-candidate call, and FE-17 only for the Graph and Scraper tabs.

**Architecture:** Five tasks by surface, in this order:
1. the seed read path (`backend/src/seed.js` and the seed routes);
2. the Kantropos browse and the 3D graph (`app.js`, `ThreeGraph.svelte`);
3. the logs tail and the child-output parser (`app.js`, `pythonProcess.js`, the App's log loaders);
4. the Corpus stage filter and the workspace loads (`corpusList.js`, `App.svelte`, `Corpus.svelte`);
5. the `pipeline_jobs` index and the dead ingest route.

The tasks run serially in this order. Every task edits `backend/src/app.js`, and Tasks 2–5 also
share `frontend/src/App.svelte` or `frontend/src/lib/api.js`, so no two tasks run in parallel.
Phase 6 merges before phase 7; the phase-7 branch rebases onto it.

No new module and no new dependency. Every performance claim is pinned by its mechanism: a query plan, a statement or call count, a request count, or an absent sync call. No test uses wall-clock thresholds.

**Tech Stack:** Node 20, Express 5, better-sqlite3, `node:stream/promises`, `node:zlib`, Jest + supertest; Svelte 5 + Vite, Playwright (mocked); Python 3.11, pytest, sqlite3.

**Spec:** `docs/superpowers/specs/2026-11-14-review-fixes-6-read-path-design.md`

## Global Constraints

- Work only in `/home/jay/DT/.worktrees/review-fixes-6`, on branch `review-fixes-6`. Do not push.
- Never open `dl_lit_project/data/literature.db`. Tests use `:memory:`, `fs.mkdtempSync` or `tmp_path` databases.
- Backend tests: `cd backend && npm test -- tests/<file>` while you iterate, then `npm test` once per task. Jest finds new files by itself, so no CI list changes for the backend.
- ESM test files import `jest` explicitly: `import { jest } from '@jest/globals'`. A test file reaches the exports this phase adds through a namespace import (`import * as seed from '../src/seed.js'`, `import * as appModule from '../src/app.js'`), so on `0702e47` a missing export fails only the tests that use it and the baseline parity tests still run.
- Python tests: `cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests -q`. This phase adds no new Python test file; the additions go into files already listed at `.github/workflows/ci.yml:68`.
- Frontend tests:
  - Run them from `frontend/` with `E2E_FRONTEND_PORT=5190 npx playwright test <specs> --workers=1`, and only the specs the task touches. Memory is low; CI runs the full list.
  - Run mocked specs only: every `/api` call is answered by `page.route`. Never use the dev stack on `:5175`. Never run `*.real*.spec.ts`, `scraper-lab.spec.ts` or `app.spec.ts`. The latter two talk to a real backend: `scraper-lab.spec.ts` defaults to `literature.db`, and `app.spec.ts` logs in and creates a corpus. `E2E_FRONTEND_PORT=5190` moves only the frontend; Vite still proxies `/api` to `:4000`.
  - Any post-merge or smoke check runs against a backend started with `RAG_FEEDER_DB_PATH` set to a temp database, never against the dev stack.
  - The touched specs are already in the list at `ci.yml:54`.
  - `npx vite build` must pass.
- Every mocked spec that fakes a token calls `mockLogSocket(page)`.
- These values are fixed by the spec:
  - D10: `LOG_READ_CHUNK_BYTES = 64 * 1024`.
  - D16: the index is `idx_pipeline_jobs_status_finished ON pipeline_jobs(status, finished_at)`.
  - D13: the stage values are `raw | metadata | downloaded | failed_enrichment | failed_download`. The `metadata` stage covers `matched`, `queued_download` and `enriching`.
  - D11: the scope value is `readable`.
- These strings are copied exactly from the spec:
  - D15: `Loading…`, with a single ellipsis character, and the import-error text `Could not load this tab. Reload the page to try again.`
  - D12: the thrown error, `No valid JSON output from python. Output was: <output>`, is unchanged.
  - D10: `503 { error: 'Log rotated during the read; retry' }`.
  - D11: `409 { error: 'Log access changed during the read' }`.
- Each commit message names the inventory rows it closes and ends with:
  ```
  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01U3c4gfg7jQvvmPdpLNBN5T
  ```
- Secrets stay in `.env`. Nothing from it goes into tests, docs or commits.

## Review Focus

1. **A candidate file request for another corpus's seed, for a dismissed candidate, or for a candidate whose only match is a work the caller cannot read.** D2 drops the resolver. The one-row read must keep today's corpus and dismissal scoping, the matched ids must pass `workReadableByUser`, and the answer is 404, never another corpus's PDF. Test in Task 1, through the HTTP route with two users and a real temp PDF.
2. **A UTF-8 character or a CRLF split across a 64 KiB read boundary, and a file without a trailing newline.** The backward reader must return byte-identical lines in the same order as today. Test in Task 3, the parity test.
3. **A log file rotated or deleted between two reads of one tail.** The window pins `dev`, `ino` and size per file; a change restarts the read once, a second change answers 503. No line may be returned twice and the cursor must not skip older lines. Test in Task 3 with a rename between two opens, asserting exact entries and cursor.
4. **A Kantropos work in several assigned corpora, where the page limit falls inside its rows.** Each page item's `source_corpora` must still list every corpus, in today's order. Test in Task 2, the fixture's shared work at the page boundary.
5. **A stage filter, a typed `q` and a background poll together.** The poll's prefix reload must carry the stage and check `stage_total`. A row that leaves the stage must disappear on the next poll, not leave a stale row. While the rows on screen belong to another stage than the selected one, no row or bulk action may run. Test in Task 4.

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
- Test: new `backend/tests/seed-read-path.test.js` (unit cases and the HTTP route case; the route case copies its temp-DB, two-user setup from `graph-access.test.js`). Extend `backend/tests/seed-candidates-route.test.js` only if the route test needs its fixtures.

**Interfaces:**
- Produces (in `seed.js`):
  - `countPdfCandidates(db, corpusId, sourceKey, needle) -> number`;
  - `findSeedCandidateDownload(db, corpusId, sourceType, sourceKey, candidateKey, canReadWork) -> number | null`, where `canReadWork(workId) -> boolean`;
  - `countSeedCandidates(db, corpusId, sourceType, sourceKey, { q = '' } = {})`, which no longer takes `stateResolver`.
- Produces (in `app.js`): `export function findDownloadedFilePath(storedPath, context = {}) -> string | null`.
- `listSeedSources` keeps its signature and output.

- [ ] **Step 1: Write the failing tests** in `seed-read-path.test.js`. Reuse `createSeedDb()` from `seed-state.test.js`: copy it into the new file, since a test file exports nothing. The fixture holds the following:
  - corpus 1, with pdf seed `paperA` and 5 `ingest_entries`. One of them is dismissed, one matches only through its second author, and one has a downloaded work in corpus 2 only (`works` and `corpus_works`); another matches a downloaded work that is in both corpus 1 and corpus 2;
  - pdf seed `paperB` in corpus 2;
  - search run 7, linked to corpus 1 with three `search_results`. One of them matches a downloaded work of corpus 1, and one is dismissed.

```js
import { jest } from '@jest/globals'
import * as seed from '../src/seed.js'
import * as appModule from '../src/app.js'
const all = () => true

function recordSql(db) { const seen = []; const orig = db.prepare.bind(db); db.prepare = (sql) => { seen.push(sql); return orig(sql) }; return seen }

test('the light seed list reads no works row and probes no file', () => {
  const probe = jest.fn(() => '/x.pdf')
  const seen = recordSql(db)
  const sources = seed.listSeedSources(db, 1, { withStateCounts: false, resolveDownloadedFilePath: probe, q: '' })
  expect(probe).not.toHaveBeenCalled()
  expect(seen.some((sql) => /\bFROM works\b|JOIN works\b/.test(sql))).toBe(false)
  expect(sources.find((s) => s.source_key === 'paperA').candidate_count).toBe(4)
})
test('light counts equal the resolved candidate list for every q', () => {
  for (const q of ['', 'second author', 'nomatch', 'PUBLISHER']) {
    const light = seed.listSeedSources(db, 1, { withStateCounts: false, q })
    const full = seed.listSeedSources(db, 1, { withStateCounts: true, q })
    expect(light.map((s) => [s.id, s.candidate_count])).toEqual(full.map((s) => [s.id, s.candidate_count]))
    expect(light.every((s) => s.state_counts === null)).toBe(true)
  }
})
test('countSeedCandidates pdf equals listSeedCandidates length', () => { /* q '' and 'second author' */ })
test('findSeedCandidateDownload decides as the full resolver does for a reader of every work', () => {
  // canReadWork = all (the admin case). For every candidate of paperA and of run 7, plus
  // 'ingest:<id of paperB entry>', 'search:999', 'bogus', 'ingest:0', 'ingest:07', 'search:<pdf id>' (wrong prefix):
  // expected = full list .find(key): state in {downloaded, downloaded_elsewhere} && downloaded_work_id > 0 ? id : null
  // (paperB's key, the malformed and non-canonical keys expect null)
})
test('findSeedCandidateDownload serves only readable works and prefers the current corpus', () => {
  // canReadWork = (id) => corpus 1 member ids only:
  //   the candidate whose only match is in corpus 2 -> null;
  //   the candidate matching the work in corpus 1 and 2 -> that work id;
  // canReadWork = () => false -> null for every candidate
})
test('findSeedCandidateDownload matches once, never builds the corpus-local resolver, probes the dismissal by its primary key', () => {
  const seen = recordSql(db)
  // call it for the downloaded pdf candidate and for the downloaded search candidate
  expect(seen.some((sql) => sql.includes('JOIN corpus_works cw ON cw.work_id = t.id'))).toBe(false)
  // the pdf dismissal statement names corpus_id, source_type, source_key and candidate_key;
  // the search statement joins search_run_corpora src ON src.search_run_id = sr.search_run_id
  // the stub bundle has no resolveDownloadedAvailability: no 'SELECT file_path, origin_key FROM works' statement
})
test('a dismissed candidate and another corpus\'s seed answer null', () => { /* Review Focus 1 */ })
test('createStateResolver probes sqlite_master for works once, whatever the candidate count', () => {
  // seen = recordSql(db); listSeedCandidates(db, 1, 'pdf', 'paperA', { resolveDownloadedFilePath: () => '/x.pdf' })
  // count statements matching /sqlite_master/ with 'works' bound: record via a wrapper on .get that logs (sql, args);
  // the count is the same with 1 and with 3 downloaded candidates (add two more downloaded matches and repeat)
})
test('findDownloadedFilePath probes an existing absolute path once', () => {
  // tmp file; spy = jest.spyOn(fs, 'existsSync'); appModule.findDownloadedFilePath(file) === file; spy calls with file === 1
})
test('index.js schedules no seed warm-up and createApp exposes none', () => {
  expect(fs.readFileSync(new URL('../src/index.js', import.meta.url), 'utf8')).not.toMatch(/warmSeedState/)
  // appModule.createApp({ broadcast: () => {} }).warmSeedState === undefined (stub mode, as logs.test.js boots it)
})

// HTTP route, non-stub temp DB as graph-access.test.js: users u1 (member of corpus 1 only) and u2 (member of 1 and 2);
// a real temp PDF on disk as the file_path of the corpus-2-only downloaded work
test('the candidate file route never serves a PDF from a corpus the caller cannot read', async () => {
  const url = `/api/seed/sources/pdf/paperA/candidates/ingest:${fx.entryInCorpus2Only}/file`
  const denied = await request(app).get(url).set(as('u1'))
  expect(denied.status).toBe(404)
  const allowed = await request(app).get(url).set(as('u2'))
  expect(allowed.status).toBe(200)
  expect(Buffer.compare(allowed.body, fs.readFileSync(fx.pdfPath))).toBe(0)
})
```

- [ ] **Step 2: Run them and verify they fail.** Run `cd backend && npm test -- tests/seed-read-path.test.js`. Expected: FAIL. The cases are:
  - `findSeedCandidateDownload` and `findDownloadedFilePath` are not exported (only the tests that call them fail);
  - the light list prepares the `FROM works` statements and calls the probe;
  - the `sqlite_master` probe count grows with the downloaded candidates;
  - `existsSync` is called twice;
  - `index.js` contains `warmSeedState`;
  - the route serves the corpus-2-only PDF to `u1`.

  The parity cases (`light counts equal…`, `countSeedCandidates pdf…`) already pass on `0702e47`; they must keep passing.
- [ ] **Step 3: Implement D1 to D4.**
  - D1:
    - `listSeedSources` uses `let resolver = stateResolver; const getResolver = () => (resolver ??= createStateResolver(db, corpusId, { resolveDownloadedFilePath }))`.
    - The pdf branch calls `listSeedCandidates` with `getResolver()` only when `withStateCounts`. Otherwise it calls `countPdfCandidates(db, corpusId, sourceKey, needle)`, where `needle` is `String(q || '').trim().toLowerCase()`, as `listSeedCandidates` computes it.
    - The search listing calls `getResolver()` only when `withinLimit`.
    - `countSeedCandidates`'s pdf branch uses `countPdfCandidates`, and the candidates route's two calls drop `stateResolver`.
  - D2:
    - The key regex is `/^(ingest|search):([1-9]\d*)$/`, and the prefix must belong to `sourceType`. The canonical key `buildCandidateKey(sourceType, id)` is the one bound in the dismissal probe.
    - The pdf row read selects the columns `listSeedCandidates` selects today. The dismissal probe binds all four primary-key columns. The search read uses `searchCandidateWhere(corpusId, runId, sourceRef, '')` over the existing `search_results sr JOIN search_run_corpora src` join, plus `AND sr.id = ?`.
    - The normalizers get the stub bundle `{ resolveState: () => 'pending', resolveDownloadedWorkId: () => null, isInCorpus: () => false }`, with no `resolveDownloadedAvailability`; they only build the match fields.
    - `findIdsWithAliases(candidate, global.lookup, global.aliasIndex)` runs once. The ids, in that order, are filtered by `canReadWork`; the first one with a row in `SELECT 1 FROM corpus_works WHERE corpus_id = ? AND work_id = ?` (one prepared statement, reused) wins, else the first readable id.
    - The route passes `(id) => workReadableByUser(authDb, req.user, id, authConfig)` and replaces its `listSeedCandidates(...).find(...)` and state check with `const workId = findSeedCandidateDownload(...)`. When it is `null`, the route answers `404 { error: 'Downloaded file not found for this seed candidate' }` (today's text).
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
  - `:752-760` area and `:933` area (`ensureAuthSchema` creates `idx_works_openalex_id` and `idx_works_normalized_doi`);
  - `:1215-1223` (async `countBibEntriesInFile(filePath, stats)` with its memo);
  - `:1356-1415` (async `listKantroposCorporaOverview(db, { targetId })`, which passes `stats`), and its callers: the browse and `/api/kantropos/corpora` (`:3494`);
  - `:1520-1525` (edges);
  - `:1632-1703` (`listPendingKantroposWorkItems`, `upstreamFileIsPresent`);
  - `:1705-1777` (`buildKantroposBrowsePayload` consumes the new return shape);
  - `:5508-5572` (the node route uses `findGraphNodeWork`);
  - `:5574-5592` (delete the cluster route).
- Modify:
  - `frontend/src/components/ThreeGraph.svelte` (`:7` import, `:85` `selectedClusterDetail`, `:98` `clusterDetailCache`, `:1040` and `:1139-1160` `loadClusterDetail`, `:1207`, `:1253`, `:1839` `selectedClusterPanel`; the panel renders `selectedClusterData`);
  - `frontend/src/lib/api.js:818-823` (delete `fetchGraph3DClusterDetail`).
- Modify: `backend/tests/graph-access.test.js:189-212` (drop the cluster URL from the loop; the lonely-user check uses `/api/graph/3d/snapshot/${fx.k1}/clusters.json`).
- Test:
  - new `backend/tests/kantropos-browse.test.js`, with its setup copied from `corpus-sharing.test.js`: temp DB and Kantropos root, admin login, the `RAG_FEEDER_KANTROPOS_CORPORA` env;
  - new `backend/tests/graph-read-path.test.js` (unit cases, the node route on a temp DB, and the snapshot build with a fake export);
  - extended `frontend/tests/graph-3d.spec.ts`.

**Interfaces:**
- Produces (exported from `app.js`):
  - `gzipSnapshotFiles(dir: string, fileNames: string[]) -> Promise<void>`;
  - `findGraphNodeWork(db, nodeId: string, selectList: string, columns: Set<string>) -> object | undefined`.
- `listPendingKantroposWorkItems(db, targetId, importedWorkIdSet, pendingLimit) -> { items, totalCount, missingFileCount, pendingCountByCorpus: Map<number, number> }`. It is internal, and only `buildKantroposBrowsePayload` consumes it. It runs both passes in one `db.transaction(...)()`.
- `countBibEntriesInFile(filePath, stats) -> Promise<number | null>`; `listKantroposCorporaOverview(db, { targetId } = {}) -> Promise<object[]>`; `buildKantroposBrowsePayload` becomes async.
- The browse JSON is unchanged up to the order of `graph.edges` (an unordered set). The node JSON is unchanged.

- [ ] **Step 1: Write the failing tests.**

```js
// kantropos-browse.test.js — target-a with metadata.bib (3 entries, 2 imported works); 3 assigned corpora;
// 7 downloaded pending works with years so that the page boundary (pending_limit=3) falls on
// work W5, a member of corpora 2 and 3 (Review Focus 4); 2 of the 7 have no file on disk, one on the page
// and one off the page (one real temp file per present work); citation_edges with the UNIQUE constraint
// (DDL as corpus-sharing.test.js:262) holding 3 edges among page nodes + 2 to off-page works
// + 1 edge whose stored source id is the page node id padded with spaces (D5: expected to drop out).
// target-b with its own metadata.bib, assigned to no corpus.
test('browse payload is unchanged (golden)', async () => {
  // GET /api/kantropos/corpora/target-a/browse?pending_limit=3
  // expect summary.{pending_items_count: 5, pending_skipped_missing_file: 2, pending_items_loaded, pending_items_has_more: true,
  //   current_items_count: 2, metadata_entry_count: 3}, assigned_corpora[*].{pending_count, downloaded_count},
  //   pending_items ids in order, W5.source_corpora names in LOWER(name), id order, file_missing flags — literal values written against 0702e47
  // graph.edges compared as a set of `${source}|${target}|${relationship_type}` strings: the 3 among nodes;
  //   on 0702e47 the padded edge is in the set, after D5 it is not (the only expected difference, asserted separately)
})
test('browse reads full rows only for the page, in one snapshot, and edges through the UNIQUE index', async () => {
  const seen = []; const orig = app.authDb.prepare.bind(app.authDb)
  jest.spyOn(app.authDb, 'prepare').mockImplementation((sql) => {
    const stmt = orig(sql); seen.push({ sql, stmt })
    const all = stmt.all.bind(stmt); stmt.all = (...args) => { seen.at(-1).args = args; return all(...args) }  // record per statement
    return stmt
  })
  const tx = jest.spyOn(app.authDb, 'transaction')
  // GET browse
  const full = seen.filter((s) => /SELECT w\.\*/.test(s.sql))
  expect(full.length).toBe(1)
  expect(full[0].sql).toContain('json_each')
  expect(JSON.parse(full[0].args.at(-1))).toEqual(fx.pageWorkIds)   // the first pending_limit work ids of pass 1, in order
  expect(tx).toHaveBeenCalled()                                      // both passes inside one read transaction
  const edgeSql = seen.find((s) => s.sql.includes('FROM citation_edges')).sql
  expect(edgeSql).toContain('json_each')
  const plan = orig(`EXPLAIN QUERY PLAN ${edgeSql}`).all('[]', '[]').map((r) => r.detail).join(' | ')
  expect(plan).toContain('sqlite_autoindex_citation_edges_1')
  expect(plan).not.toMatch(/SCAN citation_edges/)
})
test('missing upstream files are probed once per work without throwing', async () => {
  // spy = jest.spyOn(fs, 'statSync'); GET browse; every call for a pending file_path has args[1] { throwIfNoEntry: false };
  // W5's file_path is probed once although it has two rows
})
test('metadata.bib is read once until it changes, asynchronously, and only for the browsed target', async () => {
  // spy fs.readFileSync and fs.promises.readFile; GET /api/kantropos/corpora twice -> each metadata.bib read once via fs.promises.readFile,
  //   readFileSync never called with a metadata.bib path;
  // append an entry to target-a (size changes) -> third GET reads it again and metadata_entry_count is 4;
  // GET target-a browse after touching target-b's file -> target-b's metadata.bib is not read
  // a failed read (chmod 000 or a directory in its place) answers metadata_entry_count null and is retried on the next GET
})

// graph-read-path.test.js
test('findGraphNodeWork never scans works on the schema ensureAuthSchema builds', () => {
  // temp-file DB: appModule.ensureAuthSchema(db) creates works (Node DDL) and the two partial indexes;
  // the index DDL read from sqlite_master equals the strings in dl_lit_project/dl_lit/db_manager.py:24-25;
  // record SQL via a prepare wrapper; call with '12', 'work:12', 'W77', '10.1/ABC'
  // every recorded statement: EXPLAIN QUERY PLAN has no /SCAN w\b/ and no /SCAN works/
})
test('ensureAuthSchema is idempotent on a DatabaseManager-shaped schema with both indexes', () => { /* create the indexes first, run ensureAuthSchema twice, no error, one index of each name */ })
test('findGraphNodeWork resolves id, work:<id>, openalex id and normalized DOI; raw-only doi and other numeric spellings no longer match', () => {
  // '12' and 'work:12' -> id 12; 'W77' -> row with openalex_id 'W77'; '10.1/ABC' -> row with normalized_doi '10.1/ABC' (doi '10.1/abc');
  // a row with doi 'RAW' and normalized_doi NULL is not found by 'RAW'; '' openalex rows never match '';
  // '12.0', '1e1', '0x0c' -> undefined
})
test('the node route answers 404 for an unreadable work in every accepted form', async () => {
  // temp DB, viewer of corpus A; work 12 in corpus B only (openalex W77, normalized_doi 10.1/abc):
  // GET /api/graph/3d/node/{12, work:12, W77, 10.1%2Fabc} as the viewer -> 404 each; as admin -> 200 each
})
test('the snapshot build streams its gzip: no sync gzip and no sync payload read', async () => {
  // RAG_FEEDER_PYTHON points to a temp executable that writes nodes.bin (2 MB random), clusters.json,
  // nodes_meta.json and manifest.json into --snapshot-dir (no edges.bin) and prints '{}';
  // set before `await import('../src/app.js')`; temp DB and snapshot dir as graph-access.test.js
  const gz = jest.spyOn(zlib, 'gzipSync'); const rs = jest.spyOn(fs, 'createReadStream'); const rf = jest.spyOn(fs, 'readFileSync')
  // GET /api/graph/3d/snapshot as admin -> 200
  expect(gz).not.toHaveBeenCalled()
  // rs called once per existing payload file (3), never for edges.bin
  // rf never called with a path ending in nodes.bin, clusters.json or nodes_meta.json
  // zlib.gunzipSync(read(<snapshot>/nodes.bin.gz)) equals the written nodes.bin; no edges.bin.gz
})
test('the cluster detail route is gone', async () => { /* admin GET /api/graph/3d/cluster/<key>/0 -> 404 */ })
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

- [ ] **Step 2: Run them and verify they fail.** Run `cd backend && npm test -- tests/kantropos-browse.test.js tests/graph-read-path.test.js tests/graph-access.test.js`, then from `frontend/`: `E2E_FRONTEND_PORT=5190 npx playwright test tests/graph-3d.spec.ts --workers=1`. Expected: the golden test (edges compared as a set, without the padded edge) passes on `0702e47` and must keep passing. These cases FAIL:
  - `SELECT w.*` without `json_each`, no bound id list and no transaction, and the edges without `WHERE`;
  - the padded edge is still in the payload;
  - `statSync` without options, and W5 probed twice;
  - `metadata.bib` read synchronously on each request, and target-b's file read by target-a's browse;
  - the missing exports, and no partial identifier index after `ensureAuthSchema`;
  - `work:12` answers 404 for the admin;
  - `gzipSync` and `readFileSync` on the payload files;
  - the cluster route answers, and the spec sees one cluster request.
- [ ] **Step 3: Implement D5 to D9.**
  - D5: `allowed = JSON.stringify([...allowedNodeIds])`, bound twice.
  - D6:
    - Pass 1 and pass 2 follow the spec, inside one `db.transaction(() => …)()`; no `await` inside it. Pass 2's ids are the first `pendingLimit` distinct work ids of pass 1. Pass 1 calls `upstreamFileIsPresent` once per distinct work id.
    - The summary uses these formulas:
      - `pending_items_count = totalCount - missingFileCount`;
      - `pending_items_has_more = items.length < totalCount`;
      - `bucket.pending_count = pendingCountByCorpus.get(id) || 0`.
    - `upstreamFileIsPresent` keeps its `try/catch` for other errors.
    - The memo `Map` is keyed by the path. Its entry is `{ mtimeMs, size, count }`, and an entry is reused when both `mtimeMs` and `size` match. A miss uses `await fs.promises.readFile(filePath, 'utf8')`; only a successful read is stored.
    - `listKantroposCorporaOverview(db, { targetId })` maps only the matching target when `targetId` is given, and awaits the counts with `Promise.all`. The browse calls it with its target after the read transaction; the browse route and `/api/kantropos/corpora` await their payloads.
  - D7:
    - The route builds `selectList` and `columns` as today and calls `findGraphNodeWork`.
    - The id probe takes `/^\d+$/` or `/^work:(\d+)$/`; nothing else is read as an id.
    - The openalex probe runs only when `columns.has('openalex_id')`, and the DOI probe only when `columns.has('normalized_doi')`.
    - `ensureAuthSchema` runs the two `CREATE UNIQUE INDEX IF NOT EXISTS` strings copied from `WORKS_INDEXES` (`db_manager.py:24-25`), each when `works` has the column, each in its own `try`: a failure logs `console.warn('[schema] … index not created: …')` and continues.
  - D8: the panel markup reads `selectedClusterData`. Delete `selectedClusterDetail`, `selectedClusterPanel`, `loadClusterDetail` (and its call in `selectCluster`), the cache, the import, the API export and the route.
  - D9: `import { pipeline } from 'node:stream/promises'`. Probe each file with `fs.existsSync` before streaming it. The final `manifest.json` read stays; it is not a payload file.
- [ ] **Step 4: Run the Task 2 backend files plus `corpus-sharing.test.js`, `access-routes.test.js` and `graph.test.js`, then `npm test`; the Playwright spec; and `npx vite build`.** Expected: all pass.
- [ ] **Step 5: Commit** — `kantropos/graph: browse edges through the UNIQUE index, pending works in two passes with exception-free file probes, metadata.bib count kept until it changes, indexed graph-node lookup, cluster detail from the loaded snapshot, streamed snapshot gzip (BE-08, BE-11, BE-12, BE-36, BE-37, BE-51)`

---

### Task 3: Logs tail and child output (BE-10, FE-59, BE-42)

**Files:**
- Modify: `backend/src/app.js`:
  - `:2448-2516`: replace `readTailLinesFromFile`, `readAllLogLines` and the synchronous `readLogWindow` with the exported async `readLogWindow`;
  - `:2477-2480`: replace `extractLogCorpusTag` with the exported `logLineCorpusId`;
  - `:5950-6030`: the async route, `scope=readable`, the access recheck, no `total_lines`.
- Modify: `backend/src/logSocket.js:8-13` (`lineCorpusId` uses `logLineCorpusId` from `app.js`, which it already imports from).
- Modify: `backend/src/pythonProcess.js:35-46` (the fallback, the optional `stats` hook) and a new internal `bracketCloses`.
- Modify: `frontend/src/lib/api.js:655-671` (`fetchLogsTail` gains `scope`) and `frontend/src/App.svelte:4268-4274`, `:4319-4325` (pass `scope`).
- Test:
  - new `backend/tests/logs-tail-reader.test.js`;
  - extended `backend/tests/access-routes.test.js`, `backend/tests/python-process.test.js`, `backend/tests/log-socket.test.js` and `frontend/tests/logs-stream.spec.ts`;
  - `backend/tests/logs.test.js:19-30` and `backend/tests/access-routes.test.js:119`: the fixture lines move to the writer format, `[older][corpus=123] line-a` instead of `[older] corpus=123 line-a`, and the expectations follow. That is the format every backend writer uses (`app.js:4235-4576`, `log-socket.test.js:102`).

**Interfaces:**
- Produces:
  - `export async function readLogWindow(baseFilePath, { limit = 200, cursor = 0, includeRotated = true, maxFiles = 5, accept = () => true } = {}) -> Promise<{ entries: string[], hasMore: boolean, nextCursor: number, cursor: number }>`; it throws an error with `code === 'LOG_ROTATED'` after its one retry;
  - `export function logLineCorpusId(line: string) -> number | null`: the first `[corpus=<digits>]` group in the line's leading run of bracket groups, `null` when there is none, when the tag is not numeric, or when the leading run holds `[DAEMON]`;
  - `parseJsonFromOutput(output, { stats } = {})`.
- `fetchLogsTail({ type, lines, includeRotated, cursor, corpusId, scope = 'corpus' })`. With `scope === 'readable'` and type `pipeline`, it sends `scope=readable` and no `corpus_id`.
- The response fields are unchanged except that `total_lines` is gone.

- [ ] **Step 1: Write the failing tests.**

```js
// logs-tail-reader.test.js
import { jest } from '@jest/globals'
import * as appModule from '../src/app.js'
function reference(files, { limit, cursor, accept }) { // today's semantics, written out in the test
  const all = files.flatMap((text) => text.split(/\r?\n/).map((l) => l.trimEnd()).filter(Boolean)).filter(accept)
  const safe = Math.min(cursor, all.length); const end = all.length - safe; const start = Math.max(0, end - limit)
  return { entries: all.slice(start, end), hasMore: start > 0, nextCursor: safe + (end - start) }
}
// countReads(): wraps fs.promises.open so each returned handle's read() adds its bytesRead to a counter
test('matches the full-read reference across files, chunk boundaries, CRLF and multibyte text', async () => {
  // base + .1 .. .5 written oldest-first; .3 missing; lines include 'ü€😀' placed so that one straddles 65536 bytes,
  // CRLF endings, blank and whitespace-only lines, the newest file without a trailing newline, ~150 KiB per file
  for (const accept of [() => true, (l) => /\bcorpus=12\b/.test(l)])
    for (const cursor of [0, 1, 199, 1000]) for (const limit of [1, 200, 2000])   // cursors inside the data; past-end below
      expect(await appModule.readLogWindow(base, { limit, cursor, maxFiles: 5, accept })).toMatchObject(reference(files, { limit, cursor, accept }))
})
test('a cursor past the end answers nothing and echoes the cursor', async () => { /* cursor 1e6 -> entries [], hasMore false, cursor and nextCursor 1e6 */ })
test('a default tail reads one chunk of the newest file when it holds enough lines', async () => {
  // the newest file is 1 MiB, with more than 200 lines in its last 64 KiB
  const open = jest.spyOn(fs.promises, 'open'); const bytes = countReads()
  await appModule.readLogWindow(base, { limit: 200, cursor: 0 })
  expect(open.mock.calls.map(([p]) => path.basename(p))).toEqual(['backend-pipeline.log'])
  expect(bytes.total).toBeLessThanOrEqual(64 * 1024)
})
test('a rotation between two opens is read once, without duplicates or a skipped cursor', async () => {
  // limit larger than the newest file's lines, so the reader opens base then base.1;
  // inside the open spy, right after the first open resolves, rotate as index.js does
  // (rename .4 -> .5 … .1 -> .2, base -> .1, write a new base with two new lines)
  const got = await appModule.readLogWindow(base, { limit: 300, cursor: 0 })
  // expect exactly reference(<files after the rotation>, { limit: 300, cursor: 0, accept: all }): entries, hasMore, nextCursor
  // every entry is unique (the fixture lines are numbered); open was called again for base after the restart
})
test('a second rotation during the retry throws LOG_ROTATED, and the route answers 503', async () => {
  // rotate inside every open; readLogWindow rejects with code 'LOG_ROTATED';
  // stub-mode app: GET /api/logs/tail?type=pipeline -> 503 { error: 'Log rotated during the read; retry' }
})
test('a rotated file deleted mid-read restarts once and then counts as empty', async () => { /* Review Focus 3: delete .2 inside the first open; exact entries of the remaining files; 200 via the route */ })
test('the tail route reads no file synchronously', async () => {
  const sync = jest.spyOn(fs, 'readFileSync')
  // stub-mode app as logs.test.js; GET /api/logs/tail?type=pipeline&lines=4
  expect(sync.mock.calls.filter(([p]) => String(p).includes('backend-pipeline'))).toEqual([])
})
test('logLineCorpusId trusts only the writer prefix', () => {
  // '[ts] [x][corpus=12] a' -> 12; '[ts] [extract-status] [corpus=12] [base=b] done' -> 12;
  // '[ts] [x][corpus=12][file=[corpus=9].pdf] a' -> 12 (first group wins);
  // '[ts] [x] failed: corpus=12' -> null; '[ts] [DAEMON] [corpus=12] x' -> null; '[x][corpus=none] c' -> null
})

// access-routes.test.js — viewer3 is a member of corpora A and B; corpus C exists, foreign to viewer3.
// beforeAll appends `[p][corpus=${ids.A}] line-a`, `[p][corpus=${ids.B}] line-b`, `[p][corpus=${ids.C}] line-c`,
// `[p] untagged-line`, `[p][corpus=none] line-n` and `[ts] [DAEMON] Job 9 failed: corpus=${ids.A} boom` to the pipeline log
test('logs: scope=readable shows what the live socket would', async () => {
  const viewer = await request(app).get('/api/logs/tail?type=pipeline&scope=readable').set(as('viewer3'))
  expect(viewer.body.entries).toEqual([`[p][corpus=${ids.A}] line-a`, `[p][corpus=${ids.B}] line-b`])   // two readable corpora; C, untagged, none and the daemon text excluded
  const admin = await request(app).get('/api/logs/tail?type=pipeline&scope=readable').set(as('admin'))
  expect(admin.body.entries).toHaveLength(6)
  expect(admin.body).not.toHaveProperty('total_lines')
})
test('logs: an explicit foreign corpus_id with scope=readable is still 403', async () => { /* ?type=pipeline&scope=readable&corpus_id=${ids.C} as viewer3 -> 403 */ })
test('logs: a scope value other than readable keeps the current-corpus fallback', async () => { /* scope=READABLE, scope=all -> same entries as no scope */ })
test('logs: type=app stays admin-only with scope=readable', async () => { /* viewer3 ?type=app&scope=readable -> 403 */ })
test('logs: a membership revoked during the read discards the window', async () => {
  // inside a one-shot fs.promises.open spy, DELETE the user_corpora row for (viewer3, B);
  // GET ?type=pipeline&scope=readable as viewer3 -> 409 { error: 'Log access changed during the read' }, no entries in the body
})
// the two existing pipeline tests keep their expectations (no scope -> current corpus; corpus_id checked)

// log-socket.test.js
test('the live socket classifies by the writer prefix', () => { /* broadcast('[x] failed: corpus=1 …') reaches the admin only */ })

// python-process.test.js
test('fallback parsing is one scan plus one parse per bracketed line', () => {
  const stats = { visited: 0, parses: 0 }
  const output = Array.from({ length: 300 }, (_, i) => `[DB Manager] step ${i} [${i}/300]`).join('\n')
  expect(() => parseJsonFromOutput(output, { stats })).toThrow('No valid JSON output from python.')
  expect(stats.visited).toBeLessThanOrEqual(output.length)
  expect(stats.parses).toBeLessThanOrEqual(300)
})
test('repeated unclosed openers do not rescan the suffix', () => {
  const stats = { visited: 0, parses: 0 }
  const output = Array.from({ length: 2000 }, (_, i) => `[warn ${i} {unclosed`).join('\n')   // no line closes its openers
  expect(() => parseJsonFromOutput(output, { stats })).toThrow('No valid JSON output from python.')
  expect(stats.visited).toBeLessThanOrEqual(output.length)
  expect(stats.parses).toBe(0)
})
test('a pretty-printed block before trailing log lines is found', () => {
  // '[log] a\n{\n  "ok": true,\n  "n": [1, 2]\n}\n[log] done' -> { ok: true, n: [1, 2] }
  // '{\n "s": "a } ] {"\n}' -> { s: 'a } ] {' }  (brackets inside strings)
  // '{\n "q": "say \\"hi\\" \\\\"\n}' -> escaped quotes and backslashes inside a string
  // '[log] it said "unterminated\n{\n "a": 1\n}' -> { a: 1 }  (a stray quote ends at the newline)
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
test('the log type switch loads the app log without scope or corpus_id, then the pipeline log again', async ({ page }) => {
  // replaces the type-switch coverage of the live app.spec.ts, which is never run here
  const { tail } = await boot(page, '#/logs')
  await page.getByTestId('logs-type-select').selectOption('app')
  await expect.poll(() => tail.params.at(-1)?.get('type')).toBe('app')
  expect(tail.params.at(-1)?.has('scope')).toBe(false)
  expect(tail.params.at(-1)?.has('corpus_id')).toBe(false)
  await page.getByTestId('logs-type-select').selectOption('pipeline')
  await expect.poll(() => tail.params.at(-1)?.get('type')).toBe('pipeline')
  await expect(panel(page).getByRole('button', { name: 'Refresh from file' })).toBeVisible()
})
```

- [ ] **Step 2: Run them and verify they fail.** Run `cd backend && npm test -- tests/logs-tail-reader.test.js tests/access-routes.test.js tests/python-process.test.js tests/logs.test.js tests/log-socket.test.js`, then from `frontend/`: `E2E_FRONTEND_PORT=5190 npx playwright test tests/logs-stream.spec.ts --workers=1`. Expected: FAIL on these cases:
  - `readLogWindow` and `logLineCorpusId` are not exported (only the tests that call them fail);
  - `readFileSync` is called;
  - the readable scope returns only the current corpus, and the daemon line with `corpus=` in its text passes the corpus filter;
  - no 409 on a revoked membership, no 503 on repeated rotation;
  - the parser has no `stats` hook (`visited` stays 0 while `JSON.parse` runs about 45k times), and `} done` does not parse;
  - no `scope` param.

  The type-switch spec and the existing `logs.test.js` cases (with their fixtures moved to the writer format) pass on `0702e47`; they must keep passing.
- [ ] **Step 3: Implement D10 to D12.**
  - D10:
    - The pin is `await Promise.all(paths.map((p) => fs.promises.stat(p).then(({ dev, ino, size }) => ({ dev, ino, size }), () => null)))` over the window's paths, newest first.
    - Each file is opened once per attempt, checked with `handle.stat()` against its pin (`dev` and `ino`; an absent pin must still give `ENOENT`), read backwards from its pinned size with `handle.read(buffer, 0, len, position)`, and closed in a `finally`.
    - A mismatch restarts the attempt once with a fresh pin; a second one throws `Object.assign(new Error('Log rotated during the read'), { code: 'LOG_ROTATED' })`. The route maps it to 503.
    - `ENOENT` on open for a file pinned as absent makes it count as empty.
    - Collected lines are unshifted in the order they are found, so `entries` comes out oldest-first.
  - D11:
    - `logLineCorpusId` walks the leading `[...]` groups (spaces between them allowed) and stops at the first other character; it returns the first `corpus=<digits>` group, or `null` when the run holds `[DAEMON]` or no numeric tag.
    - The route decides in the spec's order: `type=app` admin-only; a given `corpus_id` validated and membership-checked (403), then `accept = (l) => logLineCorpusId(l) === corpusId`; exact `scope === 'readable'` without `corpus_id` → admin or stub accepts every line, otherwise `accept = (l) => readable.has(logLineCorpusId(l))`; anything else → today's current-corpus fallback.
    - After `await readLogWindow(...)`, the route re-runs the same access lookup once (the admin flag and `SELECT corpus_id FROM user_corpora WHERE user_id = ?` for the merged branch; the membership of the filtered corpus otherwise) and answers 409 when it differs. Stub mode skips it.
    - `logSocket.js`'s `lineCorpusId` becomes `logLineCorpusId`; its `CORPUS_TAG_RE` is deleted.
    - The frontend passes `scope: logsFilterCurrentCorpus ? 'corpus' : 'readable'` from both loaders.
  - D12: `bracketCloses(text) -> Map<number, number>` is one loop over `text` with a stack, an in-string flag reset at `\n`, and the escape handling; it adds its loop count to `stats.visited` when `stats` is given. Then one `tryParse` per bracketed line that has a recorded close, from the last line to the first, counted in `stats.parses`.
- [ ] **Step 4: Run the Task 3 backend files, then `npm test`; then `logs-stream.spec.ts` (it holds the type-switch case; `app.spec.ts` is never run); then `npx vite build`.** Expected: all pass. Also check that `grep -n "readFileSync" backend/src/app.js` lists no log reader.
- [ ] **Step 5: Commit** — `logs/python: the log tail reads backwards in chunks and stops at its window, "Current corpus only" off loads every line the live view may show, corpus tags trusted only in the writer prefix, the JSON fallback follows brackets in one scan instead of trying every line pair (BE-10, FE-59, BE-42)`

---

### Task 4: Corpus stage filter and workspace loads (FE-14, FE-18, FE-17)

**Files:**
- Modify: `backend/src/corpusList.js`:
  - `:327-345` (`readCorpus` takes `stage`; the `STAGE_STATUSES` map; `countFrom` for the counts and the stage term for the page queries; the empty-schema return gains `stage_total: 0`);
  - `:375-450` (the seed-sort light select and id fetch, the `added` path, the generic sort and the year-index path take the stage term);
  - `:408` (`rest` joins every non-corpus page term);
  - `:471` (the result gains `stage_total`).
- Modify: `backend/src/app.js:5180-5195` (`/api/corpus` passes `stage: String(req.query?.stage || '')`).
- Modify:
  - `frontend/src/lib/api.js:519-531` (`fetchCorpus({ …, stage = 'all' })` returns `stageTotal`);
  - `frontend/src/App.svelte`:
    - `:2099-2140`, `:2158-2205` (`corpusStageFilter: 'all'` in `initialWorkspaceState` and its restore);
    - `:2947` area (new `handleCorpusStageChange(value)`);
    - `:3784-3800` (signature and `applyCorpusCounts(total, stageTotals, stageTotal)`);
    - `:3807-3953` (`loadCorpus` sends `stage`, also in the prefix reload; `corpusRowsStage`; the append guard; the prefix check on `stageTotal`);
    - `:1058-1063` (D14);
    - `:58`, `:69`, `:5567`, `:5794` (D15);
    - `:5488-5515` (Corpus props);
  - `frontend/src/components/Corpus.svelte:101-112`, `:113-132`, `:216-229`, `:277`, `:368` (props instead of local filtering; row, selection, bulk and "Show more" actions disabled while `corpusRowsStale`).
- Test:
  - extended `backend/tests/corpus-list.test.js`;
  - extended `frontend/tests/corpus-load.spec.ts` (the `Params` type gains `stage`), `frontend/tests/dead-code.spec.ts` and `frontend/tests/boot-requests.spec.ts`.

**Interfaces:**
- Produces:
  - `listCorpus(db, { …, stage = '' })`, whose result gains `stage_total: number`;
  - `/api/corpus?stage=<value>`;
  - `fetchCorpus` returns `{ data, total, source, stageTotals, statusCounts, stageTotal }`, where `stageTotal = Number(payload.stage_total ?? total)`.
- Corpus props: `corpusStageFilter: string`, `onStageFilterChange: (value: string) => void`, `corpusRowsStale: boolean`.
- App state: `corpusStageFilter` (workspace state, like `corpusFilterQuery`), `corpusRowsStage` (the stage of the committed rows), `corpusMatchTotal` (from `stageTotal`).

- [ ] **Step 1: Write the failing tests.**

```js
// corpus-list.test.js (fixture corpus 1, freshDb())
const STAGES = { raw: ['raw'], metadata: ['matched', 'queued_download', 'enriching'],
  downloaded: ['downloaded'], failed_enrichment: ['failed_enrichment'], failed_download: ['failed_download'] }
test('a stage returns exactly the unfiltered rows of that stage, paged, with its total', () => {
  const base = listCorpus(copy, { corpusId: 1, limit: 1000 })
  for (const [stage, statuses] of Object.entries(STAGES)) {
    const want = base.items.filter((i) => statuses.includes(i.status)).map((i) => i.id)
    const got = listCorpus(copy, { corpusId: 1, limit: 1000, stage })
    expect(got.items.map((i) => i.id)).toEqual(want)
    expect(got.stage_total).toBe(want.length)
    expect(got.total).toBe(base.total)                        // labels keep their totals
    expect(got.stage_totals).toEqual(base.stage_totals)
    expect(got.status_counts).toEqual(base.status_counts)
    expect(listCorpus(copy, { corpusId: 1, limit: 2, offset: 1, stage }).items.map((i) => i.id)).toEqual(want.slice(1, 3))
  }
})
test('with q and a stage, the counts are the q counts and only the page is staged', () => {
  const qOnly = listCorpus(copy, { corpusId: 1, limit: 1000, q: 'labour' })
  for (const stage of Object.keys(STAGES)) {
    const got = listCorpus(copy, { corpusId: 1, limit: 1000, q: 'labour', stage })
    expect(got.total).toBe(qOnly.total)                        // "All stages" keeps the q total
    expect(got.stage_totals).toEqual(qOnly.stage_totals)       // every other stage label keeps its count
    expect(got.status_counts).toEqual(qOnly.status_counts)
    expect(got.stage_total).toBe(qOnly.stage_totals[stage])
  }
})
test('stage combines with q and with every sort', () => { /* for sort in CORPUS_SORT_KEYS (seed and added included) and q 'labour': ids equal the stage-filtered unstaged listing */ })
test('an unknown stage means all stages', () => { /* stage 'bogus' and 'all' -> same items as none, stage_total === total */ })
test('the empty schema answers stage_total 0', () => { /* a DB without works: listCorpus(db, { stage: 'raw' }) -> { items: [], total: 0, stage_total: 0, … } */ })
test('the large-corpus year-index path keeps both q and stage', () => {
  // intDb (integer/NULL years, the fixture of the existing idx_works_year_id golden test); smallCorpusLimit: 0;
  // page = capturePageQuery(intDb); for each stage, with and without q 'labour':
  //   ids equal the stage-filtered reference listing;
  //   page.sql contains 'WHERE EXISTS' and 'ORDER BY w.year DESC, w.id DESC', the stage term and (with q) the dt_corpus_search_text term;
  //   pagePlan(intDb, page) contains 'idx_works_year_id'
})
```

```ts
// corpus-load.spec.ts. rows(params) slices by params.offset and params.limit and honours params.stage:
// 'downloaded' covers ids 501-650 (150 rows, more than CORPUS_PAGE_SIZE = 120); total 300, stage_total 150.
test('the stage filter loads that stage from the server and pages within it', async ({ page }) => {
  // select 'downloaded' -> last /api/corpus request has stage=downloaded, offset 0; rows are 501..620
  // "Show more" -> next request has stage=downloaded and offset 120; rows 621..650 appended; 150 rows shown
  // the final page hides "Show more"; the text reads "Showing 150 entries."
  // the option label still reads "All stages (300)"
})
test('a poll keeps the stage and drops a row that left it', async ({ page }) => {
  // after the stage is chosen, the next quiet poll carries stage=downloaded (Review Focus 5);
  // the mock then moves id 505 out of 'downloaded' (stage_total 149): after the next poll 505 is gone
})
test('row actions wait for the rows of the selected stage', async ({ page }) => {
  // hold the stage=downloaded response behind a deferred gate; select 'downloaded':
  //   the per-row remove buttons, the selection checkboxes and the bulk remove are disabled; "Show more" is disabled
  // release the gate -> the downloaded rows render and the actions are enabled
})
test('a failed stage load keeps the old rows inert until the user reverts', async ({ page }) => {
  // the stage=failed_download response answers 500: the old rows stay, the refresh-failed notice shows, the actions stay disabled;
  // select 'all' again (the committed stage) -> the actions are enabled without a new row set
})
// the existing 'a corpus switch clears the Corpus selection and stage filter' stays green, and its
// first request after the switch has no stage param

// dead-code.spec.ts
test('extraction completion loads the seed, stats and runs in parallel', async ({ page }) => {
  // three deferred gates, one per response: /api/seed/sources/pdf/paper/candidates, /api/ingest/stats, /api/ingest/runs
  // (only those issued after the done frame); each route handler records its arrival and awaits its gate
  await expect.poll(() => arrived.size).toBe(3)       // all three requested while none has been answered
  // then release the gates; the card reaches its finished state
})

// boot-requests.spec.ts
test('the graph and scraper code loads on first open only', async ({ page }) => {
  // page.on('request'): urls; boot on #/workspace and wait for /api/corpus
  // no url path ends with '/src/components/ThreeGraph.svelte' or '/src/components/ScraperLab.svelte'
  // click tab-graph -> a ThreeGraph.svelte request appears and graph-3d-panel is visible
  // click tab-scraper -> a ScraperLab.svelte request appears and its panel is visible
})
test('a failed tab import shows the error text and leaves the app usable', async ({ page }) => {
  // page.route('**/src/components/ThreeGraph.svelte*', (r) => r.abort());
  // click tab-graph -> 'Could not load this tab. Reload the page to try again.' is visible;
  // click tab-workspace -> the Corpus panel is visible
})
```

- [ ] **Step 2: Run them and verify they fail.** Run `cd backend && npm test -- tests/corpus-list.test.js`, then from `frontend/`: `E2E_FRONTEND_PORT=5190 npx playwright test tests/corpus-load.spec.ts tests/dead-code.spec.ts tests/boot-requests.spec.ts --workers=1`. Expected: FAIL on these cases:
  - `stage` is ignored;
  - there is no `stage_total`;
  - no `stage` param is sent, and "Show more" pages the unfiltered corpus;
  - the row actions are live while the old rows show;
  - only one of the three completion requests arrives before the first is answered;
  - `ThreeGraph.svelte` is requested at boot, and no error text on a failed import.

  The q-only count expectations and the existing corpus-switch case pass on `0702e47`; they must keep passing.
- [ ] **Step 3: Implement D13 to D15.**
  - D13:
    - The stage term is built from literals only, so no new parameter is bound.
    - `where` (corpus and `q`) builds `countFrom`, which the `q` counts' `GROUP BY` uses. `pageWhere = stageTerm ? [...where, stageTerm] : where` builds the `from` that every page query uses: the seed-sort light select and id fetch, the `added` path and the generic sort.
    - `const rest = pageWhere.slice(scoped ? 1 : 0).map((t) => ` AND ${t}`).join('')` replaces the `needle`-only `rest` on the year-index path. The corpus term is always first when `scoped`.
    - `stage_total`: `stage ? stageTotals(statusCounts)[stage] : total`; the empty-schema return has `stage_total: 0`.
    - `handleCorpusStageChange(value)` sets `corpusStageFilter` and runs `loadCorpus({ quiet: true, supersede: true })`.
    - `corpusPageSignature` adds `corpusStageFilter`.
    - `loadCorpus` captures `const requestStage = corpusStageFilter` and sends it on every page of the request, including the prefix reload. When a first-page load commits its rows, it sets `corpusRowsStage = requestStage`. An append returns early while `corpusRowsStage !== corpusStageFilter`.
    - The "Showing" text, `corpusHasMore` and the prefix reload's `totals` set use `corpusMatchTotal` / `answer.stageTotal` (new state, set from `stageTotal`).
    - On a failed load the existing catch keeps the rows and sets `corpusRefreshFailed`; `corpusRowsStage` is unchanged, so the actions stay disabled until a load succeeds or the user selects `corpusRowsStage` again.
    - `corpusStageFilter` goes into `initialWorkspaceState()` and the restore beside `corpusFilterQuery`, so it survives a tab switch and resets on a corpus switch; `corpusRowsStage` resets with it. Nothing goes to `localStorage`.
    - `Corpus.svelte`'s `{#each}`, `selectableWorkIds` and the empty state use `corpusItems`. With `corpusRowsStale`, the row remove buttons, the checkboxes, the bulk remove and "Show more" get `disabled`, and `removeSelected` returns early.
  - D14: `await Promise.all([...])` replaces the three awaits and the two middle `stale()` checks.
  - D15: `{#await import('./components/ThreeGraph.svelte')}<p class="muted">Loading…</p>{:then module}<svelte:component this={module.default} … />{:catch}<p class="muted">Could not load this tab. Reload the page to try again.</p>{/await}`, and the same for `ScraperLab.svelte`. Remove the two static imports. `UpstreamBrowser` stays static (FE-17 in part).
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
- Modify: `dl_lit_project/tests/test_identifiers.py:56`, `:199` (drop the `ingest_latest` entries) and `:127` (`assert set(FIXTURE["doi"]) - set(DOI_MAP) == {"backend.scripts.ingest_latest.normalize_doi"}` and `set(DOI_MAP) <= set(FIXTURE["doi"])`). `tests/fixtures/identifier_cases.json` is not edited.
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
- [ ] **Step 4: Run the whole backend suite, the whole Python suite, then `dead-code.spec.ts` with `--workers=1` and `npx vite build`.** Expected: all pass. `grep -rn "ingest/latest\|ingest_latest" backend/src backend/scripts backend/tests frontend/src frontend/tests dl_lit_project/dl_lit dl_lit_project/tests` lists only `frontend/tests/dead-code.spec.ts`, the recorded `dl_lit_project/tests/fixtures/identifier_cases.json` key and the new `test_identifiers.py:127` assertion.
- [ ] **Step 5: Commit** — `schema/cleanup: pipeline_jobs indexed by status and finished_at for the daemon, the dedupe and the coming retention prune; the dead /api/ingest/latest route, script and export are deleted (BE-45, BE-06)`
