# Review fixes, phase 9: backend/frontend DRY cleanup plus the parked rows — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

Revised after the plan pre-flight (`.superpowers/sdd/2026-11-24-review-fixes-9-dry-cleanup/plan-preflight.md`;
binding rulings in `plan-preflight-rulings.md`). BE-56 and BE-57 are parked (spec Non-goals).

**Goal:** One copy of each duplicated helper (FE-11, FE-16, FE-23, BE-24, BE-27 to BE-31, BE-33,
BE-34), and fixes for the rows parked in phase 8 plus FE-68:
- import and extraction bound to the submitted corpus and to the uploader (FE-65b, FE-68);
- late extraction replies dropped; corpus button locks per session (FE-66); the graph picker
  rolled back (FE-67);
- 500 details, failed-run errors and promotion expansion errors for admins only (BE-29; BE-56
  and BE-57 parked);
- a membership-checked seed marker (PY-55), shared-token backfill rows (PY-57), no replay of a
  completed scrape (PY-58), and the CLI's citing fetch (PY-59).

PY-32 is closed without code (spec D13).

**Architecture:** Six tasks in the spec's order.
1. Frontend DRY (`App.svelte`, new `lib/download.js`).
2. Frontend fixes (`App.svelte`, `ThreeGraph.svelte`).
3. Backend helpers (new `dbHelpers.js`, `identifiers.js`).
4. Backend routes (`app.js`, `seed.js`, new `downloadTickets.js`).
5. Import/extraction binding (`app.js`, `staged_uploads` table, `api.js`, `App.svelte`).
6. Python (`seed_promote.py`, `backfill_openalex_topics.py`, `scraper_lab.py`, `OpenAlexScraper.py`).

One new table (`staged_uploads`, `CREATE TABLE IF NOT EXISTS` in `ensureAuthSchema`). Staged names
use `crypto.randomUUID()`. No new dependency. No new Playwright spec, so
`.github/workflows/ci.yml` is not edited.

**Tech Stack:** Node 20, Express 5, better-sqlite3, Jest + supertest; Svelte 5 (legacy syntax) +
Vite 7, Playwright (mocked); Python 3.11 (`/home/jay/DT/dl_lit_project/.venv/bin/python`), pytest,
sqlite3.

**Spec:** `docs/superpowers/specs/2026-11-24-review-fixes-9-dry-cleanup-design.md` (commit
`1fc9178`). Each task names its decisions (D1–D17); read them with the task.

## Setup (controller, once, before Task 1)

- `cd backend && npm ci`
- `cd frontend && npm ci && npx playwright install chromium`
- The worktree has no venv; use `/home/jay/DT/dl_lit_project/.venv/bin/python`.

## Parallelism and shared files

| Task | Surface | Runs |
|---|---|---|
| 1 Frontend DRY (FE-11, FE-16, FE-23) | frontend | first browser task |
| 2 Frontend fixes (FE-66, FE-67) | frontend | after 1 |
| 3 Backend helpers (BE-33, BE-34, BE-24) | backend | parallel to 1–2 |
| 4 Backend routes (BE-27–BE-31) | backend | after 3 |
| 5 Import/extraction binding (FE-65b, FE-68) | backend + frontend | after 2 and 4 |
| 6 Python (PY-55, PY-57, PY-58, PY-59) | Python | parallel to all |

- Tasks 1, 2 and 5 edit `App.svelte` and run Playwright on port 5190 in one worktree
  (`reuseExistingServer: true`), so they run one after another.
- Tasks 3, 4 and 5 edit `backend/src/app.js`, in that order. Task 4 adds `sendServerError` and
  converts the import route's 500; Task 5 then edits that handler.
- `backend/src/seed.js`: Task 3 (helpers, `isSeedSourceType`) and Task 4 (`runErrorDetail`).
- `frontend/tests/session.spec.ts`: Tasks 1 and 2. `frontend/tests/seed-upload.spec.ts`: Task 5.
- `frontend/tests/helpers/mockApi.ts` already has `trackFetches`, `fetchStats`, `afterBodyRead`,
  `settleReads`, `logIn`, `mockLogSocket`, `fulfil` and `mePayload`; no task changes their
  signatures.

## Global Constraints

- Work only in `/home/jay/DT/.worktrees/review-fixes-9`, on branch `review-fixes-9`. Do not push.
- Never open `dl_lit_project/data/literature.db` or any other dev or prod `*.db`; tests use
  `tmp_path` / `fs.mkdtempSync` DBs. Set `RAG_FEEDER_OPENALEX_QUOTA_PATH` into `tmp_path` in any
  test that reaches `openalex_request_json`.
- Never use bounded-repeat quantifiers (`{m,n}`) in `grep` patterns.
- Every grep check runs from the worktree root (`/home/jay/DT/.worktrees/review-fixes-9`) after
  `git add -A` of the task's files, so new files are searched; a negative grep from the wrong
  directory proves nothing.
- Every Playwright command carries `E2E_FRONTEND_PORT=5190`.
- No secret in a fixture: fake keys only (`sekret-…`, `fake-…`), never a value from `.env`.
- Commands:
  - Jest: `cd backend && npm test -- tests/<file> --runInBand` (the repo's ESM runner; never bare
    `npx jest`);
  - Python: `cd dl_lit_project && /home/jay/DT/dl_lit_project/.venv/bin/python -m pytest tests/<file> -q`;
  - Playwright: `cd frontend && E2E_FRONTEND_PORT=5190 npx playwright test tests/<file> --workers=1`
    (check `ss -ltn` that 5190 is free; never 5175, the dev container serving master; never a
    `*.real*` spec).
- Race tests never wait a fixed time (no `waitForTimeout`) and add no hook to the app. They call
  `trackFetches(page)` before `goto`, gate the late answer and its follow-ups, release it, then
  `afterBodyRead(page, <path>, <reads before>)` and assert right there. Follow-ups are asserted
  with `fetchStats(page, <path>).calls`.
- Every spec that fakes a token calls `mockLogSocket(page)`. `npx vite build` must pass after
  Tasks 1, 2 and 5.
- Exact strings from the spec:
  - D1: `Invite token is missing.`, `Reset token is missing.`, `Failed to accept invite`,
    `Failed to reset password` (and today's `Password must be at least 8 characters.`,
    `Passwords do not match.`);
  - D4: `Failed to start bibliography extraction` (extraction lookup error; the import keeps
    `Failed to import seed file`), `Uploaded file not found. Please re-upload.` (import),
    `Uploaded PDF not found (and no extracted pages exist). Please re-upload the PDF.` (extraction),
    `Invalid corpusId`;
  - D10: `Keyword search failed`, `search failed` (a failed run's `run.error` for non-admins),
    `Failed to build expansion seeds` (promotion expansion), and the 35 routes' own fallback texts,
    unchanged.
- Exact values: D11 TTL `60_000` ms, token `crypto.randomBytes(24).toString('base64url')`; D17 one
  page `per-page=100`, `max_citations` 100.
- No compose, Caddy or CI change. Schema: only `staged_uploads`.
- Don't commit `CODE_REVIEW.md`, `Korpus builder.txt`, `jochen_enriched.md` or `.superpowers/`; use
  `git add <paths>` only.
- Each commit message names the rows it closes and ends with:
  ```
  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01U3c4gfg7jQvvmPdpLNBN5T
  ```

## Review Focus

1. **The same uploader importing their own file into a second corpus they can write.** It must
   still work, as must the admin using anyone's file. A different user gets 404 even when the
   target corpus is writable. Test in Task 5.
2. **A staged name with surrounding spaces, `..`, or a separator, staged by nobody.** It must
   still answer today's 400, never a 404 that hints at ownership. Test in Task 5.
3. **A failed search run shown in the seed list to an editor.** They read `search failed`; the
   admin reads the stored text; a `done` run's quota note stays for everyone. Test in Task 4.
4. **A Crossref match in the CLI bibliography.** No HTTP request goes out, and its entry stays
   in the output. Test in Task 6.
5. **A graph render failure after the new data is published.** The picker must not roll back to
   the old corpus. Test in Task 2.

---

### Task 1: Frontend DRY (FE-11, FE-16, FE-23 remainder)

Spec D1, D2, D3.

**Files:**
- Create: `frontend/src/lib/download.js`.
- Modify: `frontend/src/App.svelte`:
  - `handleLogin` `:1729-1796`, `handleAcceptInvite` `:1818-1891`, `handleResetPassword`
    `:1893-1966`; the new helpers sit next to `enterSession` (`:2097`);
  - `workerErrorText`/`loadPipelineWorkerStatus` `:4202-4247`;
  - the four download handlers `:4318-4467`.
- Modify: `frontend/src/components/ScraperLab.svelte:183-205`,
  `frontend/src/components/UpstreamBrowser.svelte:119-136`.
- Test: `frontend/tests/session.spec.ts`, `frontend/tests/corpus-load.spec.ts`,
  `frontend/tests/tab-resources.spec.ts`.

**Interfaces:**
- Produces (`lib/download.js`):
  - `saveBlob(blob: Blob, filename: string): void`: link with `download = filename` and
    `rel = 'noopener'`, click, remove; `URL.revokeObjectURL` in a `finally`;
  - `openDownloadUrl(url: string): void`: link with `href = url` and `rel = 'noopener'`, click,
    remove.
- Produces (`App.svelte`, component-local):
  - `openSignedInSession(me, lostSession: () => boolean): Promise<void>`;
  - `submitNewPassword(event, { form: 'acceptInvite' | 'resetPassword', request: (body) => Promise<payload>, missingTokenText: string, failureText: string }): Promise<void>`;
  - `workerView(previous: object, entry: object): object` returning
    `{ ...previous, running, in_flight, started_at, last_tick_at, last_result, last_error }`.
- Consumes: nothing from other tasks.

- [ ] **Step 1: Write the tests.**

```ts
// session.spec.ts — parametrized over signInLinks (:572-575): { flow, endpoint, button }
for (const { flow, endpoint, button } of signInLinks) {
  test(`${flow} shows its own validation and failure texts`, async ({ page }) => {
    // goto `#/${flow}` without token: submit -> text `${flow === 'accept-invite' ? 'Invite' : 'Reset'} token is missing.`
    // with ?token=t: 'short' / 'short' -> 'Password must be at least 8 characters.' (native minlength would block the
    //   submit: set form.noValidate = true via page.locator('form.login-form').evaluate before clicking)
    // 'secret-password' / 'other-password' -> 'Passwords do not match.'
    // endpoint answers fulfil(route, { error: 'Link expired' }, 400): 'Link expired' visible; button still visible
  })
}
// corpus-load.spec.ts — preservation through the real ticket caller (passes on d4518a9)
test('a stored corpus file downloads through its ticket', async ({ page }) => {
  // corpus list with one downloaded item id 5; POST '/api/corpus/downloaded/5/download-ticket'
  //   -> { url: '/api/corpus/downloaded/5/file?ticket=t', expires_in_seconds: 60 }
  // GET that path -> body '%PDF-1.4', headers { 'Content-Disposition': 'attachment; filename="paper.pdf"' }
  // expand the row; const download = page.waitForEvent('download') BEFORE the click; click getByTitle('Download the stored file')
  // (await download).suggestedFilename() === 'paper.pdf'; 'Download started.' visible
})
// tab-resources.spec.ts — next to the FE-23 test (:855), admin boot, hash '#/downloads'
test('both worker views carry exactly their heartbeat fields', async ({ page }) => {
  // state.workers = { enrich: { online: true, status: 'running', last_seen_at: 'T1', details: { state: 'enriching' } },
  //                   download: { online: false, status: 'error', last_seen_at: 'T2', details: { error: 'boom' } } }
  // corpus-scoped backlog in the daemon-status answer: { queued_download: 4, downloading: 1 }
  // wait until the heartbeat is applied: expect.poll(() => page.evaluate(() => (window as any).__dtRefresh.workers()
  //   .downloadWorker.last_error)).toBe('boom'); then
  // const w = await page.evaluate(() => (window as any).__dtRefresh.workers())
  // expect(w.pipelineWorker).toMatchObject({ running: true, in_flight: true, started_at: 'T1', last_tick_at: 'T1',
  //   last_result: { state: 'enriching' }, last_error: null })
  // expect(w.downloadWorker).toMatchObject({ running: false, in_flight: false, started_at: 'T2', last_tick_at: 'T2',
  //   last_result: { error: 'boom' }, last_error: 'boom', queued_download_items: 4, running_jobs: 1 })
})
```

- [ ] **Step 2: Run them against the unchanged code.** Run
  `cd frontend && E2E_FRONTEND_PORT=5190 npx playwright test tests/session.spec.ts tests/corpus-load.spec.ts tests/tab-resources.spec.ts --workers=1`.
  Expected: all PASS (Task 1 changes no behaviour). These are the pins for the refactor. A failure
  here is a wrong fixture: fix the test, not the app.
- [ ] **Step 3: Implement D1, D2, D3.**
  - D1: login keeps `if (epoch !== sessionEpoch) return` after `fetchMe`; invite and reset keep
    `if (lostSession()) return`. All three then `await openSignedInSession(me, lostSession)`.
    `handleAcceptInvite`/`handleResetPassword` become one-line calls to `submitNewPassword` with
    the D1 values.
  - D2: the six sites call `saveBlob`/`openDownloadUrl` and keep their status texts and session
    guards; ScraperLab passes `filename || 'metadata.bib'`.
  - D3: `pipelineWorker = workerView(pipelineWorker, enrich)`;
    `downloadWorker = { ...workerView(downloadWorker, download), queued_download_items, running_jobs }`.
- [ ] **Step 4: Verify.** The Step 2 command passes; `cd frontend && npx vite build` succeeds;
  From the worktree root, after `git add -A frontend`, `git grep -n "createElement('a')" -- frontend/src` prints
  only `frontend/src/lib/download.js`.
- [ ] **Step 5: Commit**: `frontend: one sign-in tail, one set-password handler, one download helper and one worker-view builder (FE-11, FE-16, FE-23)`

---

### Task 2: Frontend fixes (FE-66, FE-67)

Spec D5, D6. Runs after Task 1.

**Files:**
- Modify: `frontend/src/App.svelte`: `handleCreateCorpus` `:2370-2402`, `handleDeleteCorpus`
  `:2404-2454`, the `signOut` branch of `resetFrontendState` `:2296-2299`.
- Modify: `frontend/src/components/ThreeGraph.svelte`: state `:77-84`, `onGroupModeChange`
  `:1232-1244`, `load3DGraph` `:1246-1320`.
- Test: `frontend/tests/session.spec.ts`, `frontend/tests/graph-3d.spec.ts`.

**Interfaces:**
- Produces (component-local in `ThreeGraph.svelte`): `let loadedCorpusId = null` (set with
  `loadedScope`) and `let keepLoadError = false`.
- Consumes: `trackFetches`, `fetchStats`, `afterBodyRead` (`helpers/mockApi.ts`).

- [ ] **Step 1: Write the failing tests.**

```ts
// session.spec.ts — the parametrized corpus-action test (:1934-1993), action in { creation, deletion }.
// The action route gains per-request gates keyed by bearer: alice's request (token-a) and bob's (token-b).
test(`a corpus ${action.name} pending at sign-out does not lock the next user's buttons`, async ({ page }) => {
  // as today up to bob's sign-in (commit: a seed request with 'Bearer token-b' answered)
  // expect(getByRole('button', { name: 'New corpus' })).toBeEnabled()               // fails on d4518a9
  // deletion: expect(getByRole('button', { name: 'Delete corpus' })).toBeEnabled()  // bob owns corpus 1 in his /me
  // bob clicks action.button (his request held at its gate)
  // reads = fetchStats(action.path).reads; release alice's request; afterBodyRead(action.path, reads)
  // both buttons still disabled                                                     // stale unlock
  // release bob's request; both buttons enabled
})
// graph-3d.spec.ts
// existing 'a recolour after a failed corpus load keeps the corpus error' (:794-811) gains:
//   after 'Corpus 2 layout failed' is visible: expect(corpus).toHaveValue('1')          // d4518a9: '2'
//   after the recolour: expect(corpus).toHaveValue('1'); the error still visible
test('a failed corpus manifest puts the picker back', async ({ page }) => {
  // graphMock versionsByCorpus { 1: 'A', 2: 'C' }; the manifest with corpus_id=2 answers 500 { error: 'Corpus 2 manifest failed' }
  // openGraph; loaded; select '2': 'Corpus 2 manifest failed' visible; picker '1'; recolour 'degree': error stays, picker '1'
})
test('a render failure after publishing keeps the new corpus in the picker', async ({ page }) => {
  // Review Focus 5. Corpus 2's manifest and binary files are valid, but its nodes_meta.json answers valid JSON that is
  // not an array ({}): the load publishes graphData (loadedScope and loadedCorpusId name corpus 2), then renderGraph
  // throws at nodes.map (ThreeGraph.svelte:274). (A short aligned nodes.bin would not throw: positionValue falls back.)
  // select '2': an error status shows; expect(corpus).toHaveValue('2')
})
```

- [ ] **Step 2: Run them and verify they fail.** Run
  `cd frontend && E2E_FRONTEND_PORT=5190 npx playwright test tests/session.spec.ts tests/graph-3d.spec.ts --workers=1`.
  Expected FAIL: `New corpus` disabled after bob's sign-in; the picker reads `2` after a failed
  switch. Expected PASS: the render-failure test (no rollback exists today).
- [ ] **Step 3: Implement D5 and D6.**
  - D5: each handler's `finally` runs `if (epoch === sessionEpoch) creatingCorpus = false` (and
    `deletingCorpus`); the `signOut` branch sets both to `false`.
  - D6: on success set `loadedCorpusId = options.corpusId` next to `loadedScope = scope`. In the
    `catch`, while `owns()`, when `loadedCorpusId !== null && options.corpusId !== loadedCorpusId`:
    `selectedGraphCorpusId = loadedCorpusId; keepLoadError = true`. `load3DGraph` sets
    `keepLoadError = false` first. `onGroupModeChange` restores `loadedStatus` only when
    `!keepLoadError`. Update the comment at `:77-81`.
- [ ] **Step 4: Verify.** The Step 2 command passes; `npx vite build` succeeds.
- [ ] **Step 5: Commit**: `frontend: corpus button locks belong to their session; a failed graph corpus switch puts the picker back (FE-66, FE-67)`

---

### Task 3: Backend helpers (BE-33, BE-34, BE-24)

Spec D7.

**Files:**
- Create: `backend/src/dbHelpers.js`, `backend/src/identifiers.js`.
- Modify: `backend/src/app.js` (drop `tableExists` `:339`, `listTableColumns` `:361`,
  `safeJsonParse` `:1327`, `normalizeGraphOpenAlexId` `:1528`, `normalizeGraphDoi` `:1535`; the
  five `['pdf', 'search']` sites).
- Modify: `backend/src/seed.js` (drop `tableExists` `:4`, `tableHasColumn` `:9`, `parseJson`
  `:14`, `normalizeDoi` `:46`, `normalizeOpenAlexId` `:53`; the redundant probe `:137`; three
  source-type sites).
- Modify: `backend/src/pipelineReads.js` (drop `tableExists` `:1`, `hasColumn` `:10`),
  `backend/src/corpusList.js` (import `tableExists` from `dbHelpers.js`).
- Test: new `backend/tests/db-helpers.test.js`, new `backend/tests/identifiers.test.js`,
  `backend/tests/seed-state.test.js`, `backend/tests/pipeline-reads.test.js` (import change).

**Interfaces:**
- Produces (`dbHelpers.js`):
  - `tableExists(db, name: string): boolean` (today's query; no cache, per the plan pre-flight ruling);
  - `tableHasColumn(db, table: string, column: string): boolean`;
  - `listTableColumns(db, table: string): string[]` (`[]` for a missing table);
  - `parseJson(value, fallback = null)` (`seed.js`'s rule: `''`/`null`/`undefined` → fallback;
    object passes through; parse failure → fallback).
- Produces (`identifiers.js`):
  - `normalizeOpenAlexId(value): string | null` (first `/W\d+/i`, uppercased, else `null`);
  - `normalizeDoi(value): string | null` (`seed.js`'s body).
- Produces (`seed.js`): `export const SEED_SOURCE_TYPES = new Set(['pdf', 'search'])`,
  `export function isSeedSourceType(value): boolean`.
- Consumes: nothing.

- [ ] **Step 1: Write the failing tests.**

```js
// db-helpers.test.js (in-memory better-sqlite3)
test('tableExists follows the schema', () => {
  // false; db.exec('CREATE TABLE t (a)') -> true; db.exec('DROP TABLE t') -> false
})
test('listTableColumns of a missing table is empty', () => { /* [] ; ['a'] after CREATE TABLE t (a) */ })
test('tableHasColumn sees a column added later', () => { /* false; ALTER TABLE t ADD COLUMN b; true */ })
test('parseJson keeps seed.js rule', () => {
  // null -> fb; '' -> fb; '{"a":1}' -> { a: 1 }; 'null' -> null; '{bad' -> fb; ({ x: 1 }) -> same object
})
// identifiers.test.js
test.each([['W12', 'W12'], ['w12', 'W12'], ['https://openalex.org/W12', 'W12'],
  ['https://api.openalex.org/works/W12', 'W12'], ['A123', null], ['', null], [null, null]])(
  'normalizeOpenAlexId(%p) is %p', (input, expected) => expect(normalizeOpenAlexId(input)).toBe(expected))
test.each([['https://doi.org/10.1000/abc', '10.1000/ABC'], ['  10.1000/x ', '10.1000/X'], ['abc', 'ABC'],
  ['   ', null], [0, null]])('normalizeDoi(%p) is %p', (input, expected) => expect(normalizeDoi(input)).toBe(expected))
// seed-state.test.js — the 'search' fixtures (:138-160, corpus 130, run '7')
test('a search candidate whose OpenAlex id is only its raw URL matches the corpus work', () => {
  // search_results row: openalex_id NULL, raw_json { id: 'https://openalex.org/W12', display_name: 'Other title' }
  // works row in corpus 130: openalex_id 'W12', title 'Different', download_status 'downloaded'
  // listSeedCandidates(db, 130, 'search', '7')[0].state === 'downloaded'                 // d4518a9: 'pending'
})
test('a URL-form match in another corpus follows the read rule', () => {
  // the W12 work is only in corpus 131
  // permit: stateResolver = createStateResolver(db, 130, { canReadWork: () => true }) ->
  //   state 'downloaded_elsewhere', downloaded_work_id === that id                         // d4518a9: 'pending'
  // refuse: canReadWork: () => false -> state 'pending', downloaded_work_id null           // both commits
})
```

- [ ] **Step 2: Run them and verify they fail.** Run
  `cd backend && npm test -- tests/db-helpers.test.js tests/identifiers.test.js tests/seed-state.test.js --runInBand`.
  Expected FAIL: the two new modules do not exist; the URL-form candidate is `pending`.
- [ ] **Step 3: Implement D7.** Create both modules; switch every importer; replace the eight
  `['pdf', 'search'].includes(x)` with `isSeedSourceType(x)`, keeping each site's trimming and
  lowercasing; in `pipeline-reads.test.js` import `tableExists` from `../src/dbHelpers.js`.
- [ ] **Step 4: Verify.** The Step 2 command plus
  `npm test -- tests/pipeline-reads.test.js tests/seed-read-path.test.js tests/seed-candidates-route.test.js tests/graph.test.js tests/graph-access.test.js tests/corpus-list.test.js tests/search-run-failure.test.js --runInBand`
  pass. From the worktree root after `git add -A backend`, `git grep -n "function tableExists" -- backend/src`
  prints only `backend/src/dbHelpers.js`;
  `git grep -n "\['pdf', 'search'\]" -- backend/src` prints only `SEED_SOURCE_TYPES`.
- [ ] **Step 5: Commit**: `backend: shared DB helpers and identifier rules; seed matching takes the works.openalex_id rule; redundant table probes go (BE-33, BE-34 in part, BE-24)`

---

### Task 4: Backend routes (BE-27, BE-28, BE-29, BE-30, BE-31)

Spec D8 to D12. Runs after Task 3.

**Files:**
- Create: `backend/src/downloadTickets.js`.
- Modify: `backend/src/app.js`:
  - D8: the four routes `:3855`, `:5157`, `:5631`, `:5843`;
  - D9: the seven stub checks, plus new `sendStubExport`, `stubCorpusPage`, `stubGraphNode` next to
    `STUB_RESULTS` (`:270-660`);
  - D10: `serverErrorText`/`sendServerError` in `createApp`, the 35 regular sites, the upload
    `details` (`:4455`), keyword preview (`:4915-4918`) and launch (`:4973-4979`), the two
    `listSeedSources` callers (`:5018`, `:5104`), and the promotion's `expansion_error` and
    `message` (`:5410-5446`);
  - D11: the ticket maps `:2824-2825`, prunes `:2864-2871`, `:2903-2910`, mints `:3828-3835`,
    `:5607-5614`;
  - D12: `:2219-2243`, `:2357-2366`, `:2388-2443`.
- Modify: `backend/src/seed.js`: `listSeedSources` option `runErrorDetail` (`:1004`, run object
  `:1115`).
- Test: new `backend/tests/download-tickets.test.js`, new `backend/tests/server-errors.test.js`,
  `backend/tests/recursion-args.test.js`, `backend/tests/corpus-sharing.test.js`,
  `backend/tests/search-run-failure.test.js`.

**Interfaces:**
- Consumes: `listTableColumns` (Task 3).
- Produces (`downloadTickets.js`):
  `createTicketStore({ ttlMs = 60_000, now = Date.now } = {}) -> { ttlSeconds: number, mint(fields: object): string, get(token: string): object | null, revoke(token: string): void }`;
  a stored ticket is `{ ...fields, expiresAt }`.
- Produces (in `createApp`, used by Task 5):
  - `serverErrorText(req, error, fallback: string): string` =
    `(isAdminUser(req.user, authConfig) && error?.message) || fallback`;
  - `sendServerError(req, res, route: string, error, fallback: string)`: logs
    `` `[${route}] Error:` `` with the error and answers `500 { error: serverErrorText(…) }`.
- Produces (`seed.js`): `listSeedSources(db, corpusId, { …, runErrorDetail = false })`. When it is
  `false` and `run.status === 'failed'`, `run.error` is `'search failed'`.
- Produces (`app.js`, module level): `buildExpansion(body, defaults) -> { relatedDepthDownstream, relatedDepthUpstream, maxRelated, relatedSort, promotionMode, includeDownstream, includeUpstream }`.

- [ ] **Step 1: Write the failing tests.**

```js
// download-tickets.test.js
test('a ticket is valid for its TTL and pruned after it', () => {
  // let t = 0; const s = createTicketStore({ now: () => t })
  // const tok = s.mint({ userId: 1, itemId: 5 }); expect(tok).toMatch(/^[A-Za-z0-9_-]+$/); expect(tok).toHaveLength(32)
  // s.get(tok) toMatchObject { userId: 1, itemId: 5 }; t = 60_000; s.get(tok) === null
  // revoke: a fresh token is null after s.revoke(tok); s.ttlSeconds === 60
})
// server-errors.test.js — fake Python (as keyword-search-corpus.test.js): writes process.env.FAKE_STDERR to stderr,
// prints process.env.FAKE_STDOUT if set, exits Number(process.env.FAKE_EXIT_CODE || 0); users admin, editor, member
// on one corpus; jest.spyOn(console, 'error')
test('a failed export hides the child diagnostic from a member and shows it to the admin', async () => {
  // FAKE_STDERR 'SECRET-DIAG /srv/private/path', FAKE_EXIT_CODE 3
  // member GET /api/exports/json -> 500 { error: 'Failed to export corpus data' }; JSON.stringify(body) has no 'SECRET-DIAG'
  // admin -> 500, body.error contains 'SECRET-DIAG'
  // console.error called with '[/api/exports] Error:' and an Error whose message contains 'SECRET-DIAG', twice
})
test('an unmapped keyword-search failure hides the diagnostic', async () => {
  // same fake, no run_created line: editor POST /api/keyword-search/preview -> 502 { error: 'Keyword search failed' };
  // POST /api/keyword-search -> 500 { error: 'Keyword search failed' }; admin gets the detail on both;
  // console.error got an Error containing 'SECRET-DIAG' for each route
})
test('mapped keyword-search failures keep their text', async () => {
  // FAKE_STDERR 'OpenAlex rate limit exceeded. Retry after 5s.' -> editor 429 { error: 'OpenAlex rate limit exceeded. Retry after 5s.' }
  // FAKE_STDERR 'ValueError: bad query' -> 400 { error: 'bad query' } (today's mapping, kept; BE-56 parked)
})
test('a failed promotion expansion hides the diagnostic from an editor', async () => {
  // fake Python branches on the script in argv: seed_promote.py answers a normal promotion summary, seed_expand.py
  //   writes 'SECRET-DIAG' to stderr and exits 3; POST .../promote { candidateKeys: [...], promotionMode: 'new_seed',
  //   includeDownstream: true, relatedDepthDownstream: 1 }
  // editor: 200; body.expansion_error === 'Failed to build expansion seeds';
  //   body.message contains 'Expansion seeds could not be built: Failed to build expansion seeds';
  //   JSON.stringify(body) has no 'SECRET-DIAG'                                       // d4518a9: has it
  // admin: expansion_error and message contain 'SECRET-DIAG'; body.promotion and body.jobs equal the editor's shape
  // console.error got the full error
})
// search-run-failure.test.js — the dead-child harness (:117); its temp DB gains the search_results and ingest_entries
// DDL of seed-candidates-route.test.js:55; every run is registered to the shared corpus; each read asserts 200 first
test('a failed run shows its error only to the admin', async () => {
  // fake prints run_created for a running row with one undismissed result, writes 'SECRET-DIAG' to stderr, exits 3
  // after the row settles: editor GET /api/seed/sources -> that source's run.error === 'search failed';
  //   editor GET candidates route -> body.source_summary.run.error === 'search failed'              // d4518a9: stored text
  // admin: both contain 'SECRET-DIAG'; console.error got the full error
  // a pre-existing 'failed' row with a diagnostic and one result: editor 'search failed'
  // a 'done' row with a quota note and one result: editor sees the note
})
// corpus-sharing.test.js — the trigger test (:400)
//   admin: also expect(res.body).toEqual({ error: 'marker delete failed' })
//   new: an editor of the corpus, same trigger -> 500 { error: 'Failed to remove work from corpus' }   // d4518a9: the SQLite text
// recursion-args.test.js
test('the upload expansion payload keeps its eight fields', async () => {
  // body with every field set -> readLatestJob('enrich').expansion toEqual the eight-field object
  //   { relatedDepth, relatedDepthDownstream, relatedDepthUpstream, maxRelated, relatedSort, promotionMode,
  //     includeDownstream, includeUpstream } with the coerced values
  // empty body -> the RECURSION_DEFAULTS.uploadedDocs values
})
```

- [ ] **Step 2: Run them and verify they fail.** Run
  `cd backend && npm test -- tests/download-tickets.test.js tests/server-errors.test.js tests/search-run-failure.test.js tests/corpus-sharing.test.js tests/recursion-args.test.js --runInBand`.
  Expected FAIL: `downloadTickets.js` missing; the member and editor bodies carry `SECRET-DIAG`,
  the SQLite text, or the stored run text. Expected PASS already: the payload test, the mapped
  cases, the admin cases.
- [ ] **Step 3: Implement D8–D12.**
  - D8: the four routes run their statements on `authDb`; the node route uses `listTableColumns(authDb, 'works')`.
  - D9: `if (isStubMode())` in the seven handlers, at the same place; move the bodies into
    `sendStubExport(res, format)`, `stubCorpusPage(stage)` and `stubGraphNode(nodeId)`.
  - D10: the 35 regular sites call `sendServerError(req, res, <tag>, error, <fallback>)` with their
    current tag and text. The upload sends `details` only when `isAdminUser(req.user, authConfig)`.
    The keyword preview answers `502 { error: serverErrorText(req, error, 'Keyword search failed') }`
    when the mapped status is 500, else the mapped answer. The launch does the same with 500 and
    logs `console.error('[/api/keyword-search] Search script failed:', error)` (the object) before
    the `responded` check. Both `listSeedSources` callers pass
    `runErrorDetail: isAdminUser(req.user, authConfig)`. The promotion sets
    `expansionError = serverErrorText(req, error, 'Failed to build expansion seeds')` after logging
    the full error, so `expansion_error` and `message` carry the same text; counts and jobs are
    unchanged. Nothing else (stopped-import `result.error`, scraper errors) changes (BE-57).
  - D11: both stores from `createTicketStore()`; the middlewares use `store.get`/`store.revoke`;
    the mints answer `expires_in_seconds: store.ttlSeconds`.
  - D12: `buildExpansion`; the keyword path calls `buildExpansion(req.body, RECURSION_DEFAULTS.keyword)`;
    `buildUploadedDocsExpansion` per D12; delete `applyUploadedDocsExpansionArgs`.
- [ ] **Step 4: Verify.** The Step 2 command plus
  `npm test -- tests/corpus.test.js tests/downloads.test.js tests/exports.test.js tests/graph.test.js tests/keyword-search.test.js tests/keyword-search-corpus.test.js tests/topic-search.test.js tests/scraper-draft.test.js tests/seed-read-path.test.js tests/access-revalidation.test.js tests/access-routes.test.js tests/auth.test.js tests/graph-access.test.js --runInBand`
  pass. Then `npm test -- --maxWorkers=2`. From the worktree root, these greps print nothing:
  - `git grep -n "new Database(DB_PATH, { readonly" -- backend/src`;
  - `git grep -n "applyUploadedDocsExpansionArgs\|normalizeGraphOpenAlexId\|safeJsonParse" -- backend/src`.

  And `git grep -n "RAG_FEEDER_STUB === '1'" -- backend/src/app.js` prints only `isStubMode`.
- [ ] **Step 5: Commit**: `backend: file routes read through authDb; one stub check per handler; 500 details, failed-run and expansion errors for admins only; one ticket store; one expansion builder (BE-27, BE-28, BE-29, BE-30, BE-31)`

---

### Task 5: Import and extraction binding (FE-65b, FE-68, late extraction replies)

Spec D4. Runs after Tasks 2 and 4.

**Files:**
- Modify: `backend/src/app.js`:
  - `ensureAuthSchema` (`:735-945`): `staged_uploads`;
  - `/api/process_pdf` (`:4426-4466`);
  - `/api/extract-bibliography/:filename` (`:4470-4502`);
  - `/api/ingest/import-seed` (`:5955-5967`);
  - multer's filename (`:4413-4419`): `crypto.randomUUID()` as the suffix;
  - the `bindSubmittedCorpus` comment (`:2961`).
- Modify: `frontend/src/lib/api.js` (`importSeed` `:282`, `extractBibliography` `:291`),
  `frontend/src/App.svelte` (`uploadStorageKey` `:758-762`, `persistStoredUploads` `:773-801`,
  `uploadItem` `:2721`, `extractForItem` `:2779-2800`).
- Test: new `backend/tests/staged-uploads.test.js`, `frontend/tests/seed-upload.spec.ts`.

**Interfaces:**
- Consumes: `sendServerError` (Task 4), `bindSubmittedCorpus`, `requireCorpusWriteAccess`
  (existing).
- Produces (in `createApp`): `ownsStagedUpload(req, filename: string): boolean`: `true` in stub
  mode and for the admin; else `true` only when
  `SELECT 1 FROM staged_uploads WHERE filename = ? AND user_id = ?` finds a row. It may throw; each
  caller runs it inside its `try`.
- Produces (`App.svelte`): `uploadStorageKey(): string` =
  `` `rag-feeder:uploads:${authUser.id}:${corpusId}` `` (`''` without a signed-in user or corpus).
- Produces (`api.js`): `importSeed(filename, kind, { corpusId } = {})`,
  `extractBibliography(filename, { corpusId } = {})`. Each sends `corpusId` in the JSON body only
  when `Number.isFinite(corpusId)`.
- Schema:
  `CREATE TABLE IF NOT EXISTS staged_uploads (filename TEXT PRIMARY KEY, user_id INTEGER NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP)`.

- [ ] **Step 1: Write the failing tests.**

```js
// staged-uploads.test.js — roles/corpora/fake Python of keyword-search-corpus.test.js; temp RAG_FEEDER_UPLOADS_DIR;
// createApp({ broadcast: (line) => lines.push(line) }). alice: editor of a and b (b granted BEFORE
// UPDATE users SET last_corpus_id = b, because ensureLastCorpus, app.js:1261-1271, replaces an unusable one);
// bob: editor of a, owner of b; plus viewed, foreign corpora and the admin.
test("another user's staged file is refused even into a writable corpus", async () => {
  // alice POST /api/process_pdf attach 'seed.json' -> filename F
  // bob POST /api/ingest/import-seed { filename: F, kind: 'json', corpusId: b } -> 404 { error: 'Uploaded file not found. Please re-upload.' }
  // bob POST /api/extract-bibliography/F { corpusId: b } ->
  //   404 { error: 'Uploaded PDF not found (and no extracted pages exist). Please re-upload the PDF.' }
  // corpusArgs() === []                                                    // d4518a9: 200 and 202
  // alice import { filename: F, kind: 'json' } -> 200; admin import of F -> 200
})
test('input errors stay 400 before any ownership check', async () => {
  // as bob, for a name nobody staged: import without kind -> 400; filename 'a/b.json' -> 400; 'a..b.json' -> 400
  // extraction of encodeURIComponent('a\\b.pdf') -> 400; corpusArgs() === []
  // alice import { filename: `  ${F}  `, kind: 'json' } -> 200 (checked as trimmed)
})
test('the import runs in the submitted corpus, with its write check', async () => {
  // alice (saved corpus b) import F with corpusId a -> argv --corpus-id a; without corpusId -> b
  // corpusId viewed -> 403; foreign -> 403; 'x' -> 400 { error: 'Invalid corpusId' }; no extra argv lines
})
test('the extraction starts under the submitted corpus, with its write check', async () => {
  // alice uploads P.pdf; POST /api/extract-bibliography/P.pdf { corpusId: a } -> 202
  // lines.some((l) => l.includes(`[extract-status][corpus=${ids.a}]`) && l.includes('started'))
  // await a terminal line for that file (`[extract-status]…done`) before the argv log is reset or tmpDir removed
  // corpusId viewed -> 403; foreign -> 403 (no extraction starts)
})
test('a failing ownership lookup answers the JSON fallback', async () => {
  // as an editor: db.exec('ALTER TABLE staged_uploads RENAME TO staged_uploads_x') on a second handle; import and
  // extraction -> 500 with content-type JSON and { error: 'Failed to import seed file' } /
  //   { error: 'Failed to start bibliography extraction' }; no HTML; rename back in finally
})
test('staged names carry a UUID', async () => {
  // alice uploads 'seed.json' -> filename matches /^[0-9a-f-]+-seed\.json$/ and its first 36 characters are a UUID
  //   (check with a JS regex in the test, not grep)
})
```

```ts
// seed-upload.spec.ts — corpora 1 and 2; '/api/corpora/2/select' answered; '/api/auth/me' gated after the select;
// trackFetches before goto
test('an import started before a saved switch keeps its corpus', async ({ page }) => {
  // hold '/api/process_pdf'; set the BibTeX file; select '2' in .header-corpus-picker (its /me held)
  // release the upload; poll the import request: its JSON body has corpusId === 1           // d4518a9: no corpusId
})
test('an extraction clicked before the commit keeps its corpus', async ({ page }) => {
  // PDF row uploaded; select '2' (/me held); click Extract on the old row: body corpusId === 1    // d4518a9: none
})
test('a late extraction reply is dropped after the commit', async ({ page }) => {
  // no body assertion here, so the late check runs on d4518a9 too
  // PDF row uploaded; '/api/extract-bibliography/…' held; select '2'; the switch's own /me is identified as in
  //   tab-resources.spec.ts:888-900: seq = __dtFetch.firstCallAfterRead('/api/corpora/2/select', '/api/auth/me');
  //   click Extract on the old row; release that /me; afterBodyRead('/api/auth/me', reads) (commit)
  // eReads = fetchStats(<extract path>).reads; release the extraction with 202 { state: 'started' }; afterBodyRead(…, eReads)
  // expect(page.locator('.extract-progress')).toHaveCount(0)                                 // d4518a9: 1
})
test('a late extraction failure does not clear the new corpus progress', async ({ page }) => {
  // as above up to the commit; corpus 2 uploads its own PDF and clicks Extract (answered 202 at once): .extract-progress visible
  // release the old extraction with 500; afterBodyRead: .extract-progress still visible      // d4518a9: cleared
})
test("saved upload rows belong to their user and lose transient messages", async ({ page }) => {
  // alice uploads a PDF in corpus 1 (row saved); log out; logIn bob (same corpus 1): alice's row is not shown  // d4518a9: shown
  // localStorage 'rag-feeder:uploads:1' (legacy key) holding a row: not shown after reload
  // alice: Extract held (row message 'Submitting extraction request...'); switch to 2 and back (commits confirmed):
  //   the restored row does not contain 'Submitting extraction request...'                                    // d4518a9: it does
})
```

- [ ] **Step 2: Run them and verify they fail.** Run
  `cd backend && npm test -- tests/staged-uploads.test.js --runInBand` and
  `cd frontend && E2E_FRONTEND_PORT=5190 npx playwright test tests/seed-upload.spec.ts --workers=1`.
  Expected FAIL: bob's 200/202; alice's corpus argument is `b`; the extraction under `b` and not
  refused for `viewed`/`foreign`; the lookup test (no table check exists); the non-UUID name; the
  request bodies carry no `corpusId`; the late progress shows, and the late failure clears it;
  bob sees alice's row and the transient message is restored. Expected PASS: the input-error test.
- [ ] **Step 3: Implement D4.**
  - Schema line in `ensureAuthSchema`.
  - Multer's `filename` callback uses `` `${crypto.randomUUID()}-${safeOriginalName}` ``.
  - `/api/process_pdf`: after multer, a plain `INSERT INTO staged_uploads (filename, user_id) VALUES (?, ?)`
    (never `OR REPLACE` or an upsert) with `req.file.filename` and `req.user.id`; on error, answer
    through `sendServerError` and leave the file in place.
  - Both routes: `requireAuthMiddleware, bindSubmittedCorpus, requireCorpusWriteAccess`. In each
    handler, after today's validation and before the existence checks:
    `if (!ownsStagedUpload(req, filename)) return res.status(404).json({ error: <route text> })`.
    The import uses the trimmed `filename`; the extraction uses `req.params.filename` after its
    separator and `ensureInsideDirectory` checks. Both calls sit inside a `try` whose `catch` is
    `sendServerError(req, res, <route tag>, error, <route fallback>)`; the extraction's is in the
    synchronous part, before `enqueueExtractionTask`.
  - `uploadStorageKey()` per Interfaces; the old corpus-only key is neither read nor deleted.
    `persistStoredUploads` saves `message: ''` when the row's status is `uploading`, `importing`,
    `extracting` or `queued`.
  - Frontend: `uploadItem` passes `{ corpusId: currentCorpusId }` captured next to `session`/`scope`.
    `extractForItem` captures `corpusId`, `session`, `scope` and `stale()` at the click, returns on
    `stale()` right after the reply and first thing in its `catch`.
- [ ] **Step 4: Verify.** The Step 2 commands pass. Also run
  `npm test -- tests/import-seed-route.test.js tests/keyword-search-corpus.test.js tests/extraction-models-route.test.js tests/logs-tail-reader.test.js --runInBand`
  and `cd frontend && E2E_FRONTEND_PORT=5190 npx playwright test tests/seed-upload.spec.ts tests/dead-code.spec.ts --workers=1`, then
  `npx vite build`.
- [ ] **Step 5: Commit**: `ingest: import and extraction take the submitted corpus and only the uploader's staged file; late extraction replies are dropped (FE-65b, FE-68)`

---

### Task 6: Python (PY-55, PY-57, PY-58, PY-59)

Spec D14 to D17. Parallel to all.

**Files:**
- Modify: `backend/scripts/seed_promote.py` (`_mark_candidate_in_corpus` `:118-146`).
- Modify: `backend/scripts/backfill_openalex_topics.py` (`run_pass` `:188-229`).
- Modify: `dl_lit_project/dl_lit/scraper_lab.py` (`execute_scraper_job` `:371-474`).
- Modify: `dl_lit_project/dl_lit/OpenAlexScraper.py` (`process_single_file` `:975-1004`).
- Test: new `dl_lit_project/tests/test_seed_promote_marker.py`,
  `dl_lit_project/tests/test_doi_rule_scripts.py`, `dl_lit_project/tests/test_scraper_lab.py`,
  `dl_lit_project/tests/test_hygiene.py`.

**Interfaces:**
- `_mark_candidate_in_corpus(db, corpus_id: int, candidate: dict, work_id: int) -> None` (the
  work id is required).
- `run_pass(...)` keeps its signature; internally
  `token_to_rows: dict[str, list[tuple[int, str]]]`.
- `execute_scraper_job(db_path: str, run_id: int, artifacts_root: str) -> dict`. A completed run
  returns `{"run_id", "source_type", "total", "downloaded", "failed", "replayed": True}`.
- `process_single_file(json_file, output_dir, searcher, fetch_citations=True)` keeps its signature.
- Consumes: nothing.

- [ ] **Step 1: Write the failing tests.**

```python
# test_seed_promote_marker.py — loads backend/scripts/seed_promote.py like _script() in test_doi_rule_scripts.py;
# a DatabaseManager on tmp_path; the marker DDL from _ensure_seed_membership_schema; corpus 1, work 10
def test_a_member_work_gets_its_marker(...):          # corpus_works (1, 10) -> one marker row with work_id 10
def test_a_removed_work_gets_no_marker(...):           # no corpus_works row -> no marker            (d4518a9: one)
def test_a_member_marker_is_refreshed(...):            # marked_at '2000-01-01' -> changed
def test_a_non_member_old_marker_is_not_refreshed(...):# marker exists, no membership -> marked_at unchanged
def test_the_membership_check_is_part_of_the_insert(...):
    # db.conn.set_trace_callback(sql.append); call once
    # inserts = [s for s in sql if s.lstrip().upper().startswith("INSERT")]; len(inserts) == 1
    # "EXISTS (SELECT 1 FROM corpus_works" in inserts[0]
    # no statement before it starts with SELECT and mentions corpus_works
# test_doi_rule_scripts.py (fixtures _backfill_db, _fake_openalex, _run, _topic_row)
def test_backfill_pass1_writes_every_row_of_a_shared_token(tmp_path, monkeypatch, capsys, backfill):
    # rows 1 openalex_id 'W1', 2 'https://openalex.org/W1' (matched); fake answers W1
    # both _topic_row -> topics set, topics_fetched 1; the recorded filter is 'openalex_id:W1' (W1 once)
    # the log has '2/2 processed, 2 resolved' (progress counts rows; the token is requested once per batch;
    # duplicate upstream results for one token keep last-wins)                         (d4518a9: one row unfetched)
def test_backfill_pass2_writes_every_row_of_a_shared_doi_key(tmp_path, monkeypatch, capsys, backfill):
    # rows 1 doi '10.1000/abc', 2 '10.1000/ABC'; both written; filter lists the DOI once
# test_scraper_lab.py
def test_a_completed_run_is_not_scraped_again(tmp_path, monkeypatch):
    # scraper_runs row status 'completed', total 3, downloaded 2, failed 1; one scraper_artifacts row
    # monkeypatch scrape_eurlex to raise AssertionError("scraped again")
    # execute_scraper_job(...) == {"run_id": id, "source_type": "eurlex", "total": 3, "downloaded": 2, "failed": 1, "replayed": True}
    # the run row is unchanged; artifact count stays 1                              (d4518a9: raises, run 'failed')
def test_a_pending_run_still_scrapes(tmp_path, monkeypatch):   # scrape_eurlex patched to [] -> status 'completed'
def test_every_connection_is_closed(tmp_path, monkeypatch):
    # wrap sqlite3.connect in scraper_lab to record connections; after the completed-run return and after an unknown run id
    # (ValueError) every recorded connection is closed (a closed connection raises ProgrammingError on execute)
# test_hygiene.py — a searcher whose rate_limiter is _FreeLimiter() (it has remaining_block_seconds, which the request
# wrapper calls; _NoRateLimit does not) and whose openalex_session is _RecordingSession([_citing_page("W99")]);
# RAG_FEEDER_OPENALEX_QUOTA_PATH into tmp_path; (tmp_path / "out").mkdir() before the call
def test_the_cli_fetches_citing_works_only_for_openalex_matches(tmp_path, monkeypatch):
    # input file: [ref_openalex, ref_crossref, ref_unmatched]; process_single_reference patched to return, in order:
    #   {"title": "A", "openalex_id": "https://openalex.org/W7", "cited_by_api_url": "https://api.openalex.org/works?filter=cites:W7"},
    #   {"title": "B", "openalex_id": None, "cited_by_api_url": "https://api.crossref.org/works/10.1/x"}, None
    # process_single_file(str(input), str(tmp_path / "out"), searcher)
    # out = json.loads(<written file>); len(out) == 3
    # out[0]["citing_works_ids"] == [one work object with openalex_id "https://openalex.org/W99"] (the fetcher keeps the
    #   full id); out[1]["citing_works_ids"] == []
    # out[2] == ref_unmatched; session.sent: exactly one URL, host api.openalex.org       (d4518a9: none sent)
def test_the_cli_fetches_nothing_without_fetch_citations(tmp_path, monkeypatch):  # fetch_citations=False -> no request
```

- [ ] **Step 2: Run them and verify they fail.** Run
  `cd dl_lit_project && /home/jay/DT/dl_lit_project/.venv/bin/python -m pytest tests/test_seed_promote_marker.py tests/test_doi_rule_scripts.py tests/test_scraper_lab.py tests/test_hygiene.py -q`.
  Expected FAIL: the removed work gets a marker; the non-member marker is refreshed; the
  membership check is missing; one row of each shared token stays unfetched; the completed run is
  scraped (it raises); the CLI makes no request. Expected PASS: the member-marker test, the
  pending run, no request with `fetch_citations=False`.
- [ ] **Step 3: Implement D14–D17.**
  - D14: the `INSERT … SELECT … WHERE EXISTS … ON CONFLICT … DO UPDATE SET work_id = excluded.work_id, marked_at = CURRENT_TIMESTAMP`
    of the spec; the `NULL` branch and `COALESCE` go.
  - D15: per batch, group by token; build the filter from `list(token_to_rows)` (once per batch);
    one update per row with its own raw value; last upstream result per token wins, as today.
  - D16: an outer `try`/`finally: conn.close()` from right after `connect`; the completed-run
    return after the source check; the inner `try`/`except` (mark failed, raise) around the scrape
    only.
  - D17: under `if enriched_ref and fetch_citations:` compute `cited_by_url` from the top-level
    `cited_by_api_url` and `openalex_id`; only `if cited_by_url:` print `enriched_ref.get('title')`
    and call `fetch_citing_work_ids` as today.
- [ ] **Step 4: Verify.** The Step 2 command passes; then
  `tests/test_schema_indexes.py tests/test_worker_watchdog.py tests/test_identifiers.py tests/test_openalex_partial.py`
  (same runner) pass. From the worktree root, `git grep -n "best_match_result" -- dl_lit_project/dl_lit`
  prints nothing.
- [ ] **Step 5: Commit**: `python: seed marker checks membership in its insert; shared backfill tokens write every row; a completed scrape is not replayed; the CLI fetches citing works again (PY-55, PY-57, PY-58, PY-59)`

---

## Final verification (controller, after all tasks)

- `cd dl_lit_project && /home/jay/DT/dl_lit_project/.venv/bin/python -m pytest tests -q`: all passed.
- `cd backend && npm test -- --maxWorkers=2`: all passed (compare with a `d4518a9` baseline before
  blaming a time-out).
- `cd frontend && npx vite build`, then once the whole CI list from `.github/workflows/ci.yml:54`:
  `E2E_FRONTEND_PORT=5190 npx playwright test <list> --workers=1`. All pass.
- The spec's Acceptance greps all hold.
- `git diff d4518a9 --stat` shows no compose, Caddy or CI file. The only DDL is `staged_uploads`.
- The inventory's reconciliation section records PY-32 closed (D13), FE-68 added and fixed, and
  this phase's rows done. Never the dev DB.
