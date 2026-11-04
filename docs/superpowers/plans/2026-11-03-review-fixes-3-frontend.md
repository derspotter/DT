# Review fixes, phase 3: frontend — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the frontend's dead code, make the seed table, corpus poll, refresh plan and log stream do work proportional to what changed, give `api.js` one request helper and one 401 path, consolidate work-state/author/cell logic into `lib/works.js`, and fix the four correctness bugs the review found.

**Architecture:** All work is in `frontend/`. `App.svelte` stays the owning component (Svelte 5 compat mode: `export let`, `$:`); logic moves into `src/lib/works.js` (new) and `src/lib/api.js`. Every behaviour change is pinned by a mocked Playwright spec (`page.route('**/api/**')`, `page.routeWebSocket('**/api/ws')`, fake token in `localStorage`) that runs in CI without a backend.

**Tech Stack:** Svelte 5 (legacy/compat syntax), Vite 7, Playwright (`npx playwright test tests/<spec>.spec.ts`, run from `frontend/`), Node 20/22.

**Spec:** `docs/superpowers/specs/2026-11-03-review-fixes-3-frontend-design.md`

## Global Constraints

- Svelte 5 compat/legacy syntax only: `export let` props, `$:` statements, `on:click` handlers, `{@const}` inside `{#each}`/`{#if}`. No runes (`$state`, `$derived`, `$props`).
- 2-space indentation; no reformatting of untouched lines; no broad rewrites of `App.svelte` beyond the task's lines.
- No user-visible behaviour change beyond the bugs the spec names (tab-switch refresh drop, author re-split, Dashboard global overview zeros, paged seed count, 401 partial sign-out, stale `/me` after re-login, sample-data rows).
- Every task runs `npx vite build` (must print `✓ built`) and its named specs; the always-on regression set is `tests/seed-upload.spec.ts tests/refresh-scheduler.spec.ts tests/boot-requests.spec.ts tests/corpus-reconcile.spec.ts tests/dead-code.spec.ts` (all mocked). Specs run against a fresh vite with no backend, never the dev stack's :5175 (whose backend closes unmocked sockets with 4401). Command, from `frontend/`: `E2E_FRONTEND_PORT=5190 npx playwright test <files> --workers=2`. `app.spec.ts` is a live spec and is not part of any task's run.
- A mocked spec that fakes a token must also mock the log socket: `page.routeWebSocket('**/api/ws', () => {})` (Task 7 centralises this; earlier tasks add the line to the specs they create).
- Backend code (`backend/`, `dl_lit_project/`) is not modified. `.github/workflows/ci.yml` and `README.md` only in Task 7.
- Secrets stay in `.env`; nothing from it goes into specs, docs or commits.
- Commit per task; commit messages end with the session's attribution trailers (the controller supplies them in the dispatch).

## Review Focus

1. A 401 that arrives mid-flow (e.g. during `promoteSeedCandidateKeys`'s `finally`) must leave no state of the signed-out user behind and must not throw past the handler — Task 2's spec asserts the seed candidates are gone after a 401 on a scoped request.
2. A corpus switch while the seed table is rendered must clear selections without errors from stale Set lookups — Task 4's spec switches corpora with a selection active.
3. A tab switch during an in-flight cycle must load the new tab's resources exactly once — Task 5's spec counts requests.
4. A worker burst while on a non-logs tab must not grow memory beyond the cap and must show the last `maxLogHistory` lines when the logs tab opens — Task 6's spec pushes 6,000 lines on the workspace tab, then opens logs and asserts `maxLogHistory` rows.
5. Expired-token boot: `bootstrapAuth`'s parallel workspace loads all 401 at once; the handler must run once and the login form must render — Task 2's spec boots with every route answering 401.

---

### Task 1: Dead code removal

**Files:**
- Modify: `frontend/src/App.svelte` (ingest/search tab markup ~5867–6262; script members listed in spec D2; `finalizeExtractionAfterBackendCompletion` ~1249; `loadCorpus` scroll branch ~4159)
- Modify: `frontend/src/app.css` (dead and duplicate selectors)
- Modify: `frontend/tests/pipeline-perf.real-pdf.spec.ts:179` (remove the `tab-ingest` click)
- Test: `frontend/tests/dead-code.spec.ts` (new, mocked)

**Interfaces:**
- Consumes: nothing new.
- Produces: `App.svelte` without `latestEntries*`, `searchResults*`, promotion-animation, scroll-preservation members; `finalizeExtractionAfterBackendCompletion(detail)` deriving "Extraction complete." from the pdf seed's `candidate_count`.

- [ ] **Step 1: Write the failing spec**

`tests/dead-code.spec.ts` (mocked like `seed-upload.spec.ts`, with `page.routeWebSocket('**/api/ws', () => {})`):

```ts
test('#/ingest and #/search land on the workspace', async ({ page }) => {
  await page.goto('/#/ingest')
  await expect(page).toHaveURL(/#\/workspace$/)
  await page.goto('/#/search')
  await expect(page).toHaveURL(/#\/workspace$/)
  await expect(page.getByTestId('tab-ingest')).toHaveCount(0)
})

test('extraction completion reports the seed count without fetching bibliography entries', async ({ page }) => {
  // mock: /api/seed/sources returns one pdf seed { source_type: 'pdf', source_key: 'paper', candidate_count: 3 }
  // mock: /api/extract-bibliography/* (POST) returns { state: 'started' }; count every request to /api/bibliography/*
  // push a JSON extraction frame { kind: 'extraction', event: 'done', status: 'success', corpus_id: 1, base_name: 'paper', filename: 'paper.pdf' } via routeWebSocket server-side send
  // assert the extraction card shows 'Extraction complete.' and bibliographyRequests === 0
})
```

(The implementer writes the upload step the way `seed-upload.spec.ts` does, and uses the `ws` handle from `page.routeWebSocket('**/api/ws', (ws) => { socket = ws })` to `socket.send(JSON.stringify(frame))`.)

- [ ] **Step 2: Run it to verify it fails**

Run: `cd frontend && npx playwright test tests/dead-code.spec.ts`
Expected: FAIL (the `tab-ingest` assertion may pass; the bibliography request count is 1).

- [ ] **Step 3: Delete the dead members**

In `App.svelte` delete, in this order and verifying with `npx vite build` after each group: (a) the `{#if activeTab === 'ingest'}` and `{#if activeTab === 'search'}` blocks; (b) the script members in spec D2 (`latest*`, `search*` machinery, `enqueueSelectedSearchResults`, `resetSearchForm` if unreferenced, the `$:` blocks at ~227–266 and ~348–363); (c) the promotion animation subsystem and `pendingPromotionEvents`/`promotionHighlights`/`corpusRowElements`; (d) scroll preservation and the `scrollSnapshot` branch of `loadCorpus`; (e) `formatPipelineStatus`, `pipelineStatusLabel/Tone/Title`, `buildDegreeMap`, `clamp`, the four `$:` filters (`rawItems`…`failedItems`) and `RAW_STATUSES`/`FAILED_STATUSES` if unreferenced; (f) `resetFrontendState` lines that assign deleted state. After each group: `grep -n '<name>' src/App.svelte` for every deleted name must return nothing.

- [ ] **Step 4: Rewrite the extraction finalizer**

`finalizeExtractionAfterBackendCompletion(detail)`: drop the `fetchBibliographyEntries` call and the `latestEntries*` writes; after `await loadSeedSources({ quiet: true })` find the seed with `source_type === 'pdf'` and `source_key === baseName` (same match `focusSeedSource('pdf', baseName)` uses); `updateExtractionProgress({ phase: 'completed', detail: found && Number(found.candidate_count) > 0 ? 'Extraction complete.' : detail, percent: 100, indeterminate: false })`; keep the `focusSeedSource` → `loadIngestStats` → `loadIngestRuns` sequence and the 1200 ms finish. Remove the `fetchBibliographyEntries` import if unused.

- [ ] **Step 5: CSS**

Write a throwaway Node script (`frontend/scripts/dead-css.mjs`, not committed, or inline in the report) that lists every `.class` selector in `src/app.css` whose class name appears in no `src/**/*.svelte` file, excluding names with a `--` suffix family whose prefix appears in a template literal, and every top-level selector defined more than once. Delete the dead ones; merge each duplicate into its later occurrence. Keep the list of removed selectors in the task report.

- [ ] **Step 6: Stale test**

Remove the `tab-ingest` click (and any assertions that depended on the ingest tab) from `tests/pipeline-perf.real-pdf.spec.ts`; the spec stays opt-in.

- [ ] **Step 7: Run the specs and the build**

Run: `cd frontend && npx vite build && npx playwright test tests/dead-code.spec.ts tests/seed-upload.spec.ts tests/refresh-scheduler.spec.ts tests/boot-requests.spec.ts tests/corpus-reconcile.spec.ts --workers=2`
Expected: `✓ built`, all passed.

- [ ] **Step 8: Commit**

```bash
git add frontend/src/App.svelte frontend/src/app.css frontend/tests/dead-code.spec.ts frontend/tests/pipeline-perf.real-pdf.spec.ts
git commit -m "App.svelte: delete the unreachable ingest/search tabs, promotion animation, scroll no-ops and dead CSS"
```

---

### Task 2: `api.js` request helper, one 401 path, session epoch

**Files:**
- Modify: `frontend/src/lib/api.js` (all exports)
- Delete: `frontend/src/lib/sample-data.js`
- Modify: `frontend/src/App.svelte` (36 catch blocks; `handleLogout` ~1995; `resetFrontendState` ~2068; `runCorpusReconcile` ~1662; `handleSelectCorpus`/`handleCreateCorpus`/`handleDeleteCorpus`/`handleShareCorpus` ~2124–2240)
- Test: `frontend/tests/session.spec.ts` (new, mocked)

**Interfaces:**
- Produces in `api.js`: `onUnauthorized(handler)`; private `requestJson(path, options)`; every export keeps its name and return shape (callers unchanged), except `fetchDownloadQueue()` now throws on failure and returns `{ data, source }` on success, and `runKeywordSearch` loses `fallbackToSample`.
- Produces in `App.svelte`: `expireSession()`, `initialWorkspaceState()`, `sessionEpoch`.

- [ ] **Step 1: Write the failing spec**

`tests/session.spec.ts` (mocked, socket mocked):

```ts
test('a 401 on a scoped request signs out completely', async ({ page }) => {
  // boot authenticated with one seed source and 2 candidates expanded (mock /api/seed/sources and /api/seed/sources/:type/:key/candidates)
  // flip a flag so the next /api/seed/sources answers 401 { error: 'Unauthorized' }
  // trigger a refresh (page.evaluate(() => window.dispatchEvent(new Event('focus'))))
  await expect(page.getByTestId('login-form')).toBeVisible()
  // the socket was closed: the routeWebSocket handler saw ws.onClose / closed === true
  // log back in (mock /api/auth/login → { token, user }) and assert the seed table is empty
  //   (no candidate rows from the previous session) and localStorage token is the new one
})

test('expired token at boot renders the login form after one unauthorized handling', async ({ page }) => {
  // every /api/** answers 401; count /api/auth/me requests
  await expect(page.getByTestId('login-form')).toBeVisible()
  // assert no uncaught page errors (page.on('pageerror'))
})

test('a /me answer from before a re-login is discarded', async ({ page }) => {
  // boot as user A (id 1); delay one /api/auth/me by 1500 ms; sign out via the logout button; log in as user B (id 2, same corpus)
  // after the delayed answer lands: the header shows user B's name, not A's
})

test('corpus action errors show in the corpus action status, not the auth error', async ({ page }) => {
  // /api/corpora POST answers 400 { error: 'Name taken' }; create a corpus; assert the status text contains 'Name taken' and the login form is not shown
})
```

The implementer adds `data-testid="login-form"` to the login `<form>` if absent and `data-testid="auth-user"` on the header's username element.

- [ ] **Step 2: Run it to verify it fails**

Run: `cd frontend && npx playwright test tests/session.spec.ts`
Expected: FAIL (test 1: candidate rows survive; test 3: header shows A).

- [ ] **Step 3: `requestJson` and `onUnauthorized` in `api.js`**

```js
let unauthorizedHandler = null
export function onUnauthorized(handler) { unauthorizedHandler = typeof handler === 'function' ? handler : null }

async function requestJson(path, { method = 'GET', body, headers = {}, timeout = DEFAULT_TIMEOUT, errorMessage = 'Request failed', raw = false } = {}) { ... }
```

Rules (spec D1): JSON-serialise a plain-object `body` with `Content-Type: application/json`; pass `FormData`/`Blob`/string bodies through untouched; on 401 call `unauthorizedHandler?.()` then throw `Object.assign(new Error(text || 'Unauthorized'), { status: 401 })`; on other non-OK read the text, try `JSON.parse`, use its `error` string when present, else the text, else `errorMessage`, throw with `status`; return `raw ? response : response.json()`. Rewrite every export to use it (keep `login`'s token handling; keep `UPLOAD_TIMEOUT`/`PIPELINE_TIMEOUT` where used). Delete `throwIfUnauthorized`, the sample fallbacks and `sample-data.js`. `fetchDownloadQueue` returns `{ data, source: payload.source || 'api' }`.

- [ ] **Step 4: One sign-out in `App.svelte`**

Add `let sessionEpoch = 0`; `function expireSession() { sessionEpoch += 1; handleLogout() }`; call `onUnauthorized(expireSession)` once in the script body (module init, before `bootstrapAuth`). `handleLogout` keeps its body (it also bumps `sessionEpoch`). Replace every `if (error?.status === 401) { authStatus = 'unauthenticated'; setAuthToken(''); return }` by `if (error?.status === 401) return` (or drop the catch when nothing else is in it; keep `finally` blocks). `bootstrapAuth`'s catch: on 401 the handler already ran; otherwise set `authStatus = 'unauthenticated'` as today. Successful `handleLogin`/`handleAcceptInvite`/`handleResetPassword` bump `sessionEpoch`. `runCorpusReconcile`: capture `const epoch = sessionEpoch` before `fetchMe()`; `if (epoch !== sessionEpoch) return false` after it.

- [ ] **Step 5: `initialWorkspaceState()` and corpus action errors**

`function initialWorkspaceState()` returns the object of every value `resetFrontendState` assigns (spec D1 list, with the five-key `corpusStageTotals`); `resetFrontendState()` becomes `invalidateCorpusScopedRequests(); Object.assign` is not possible on component lets, so it destructures: `({ ingestStats, ingestStatsStatus, ... } = initialWorkspaceState())` followed by the existing `resetKantroposAssignmentState()`, `resetUpstreamBrowseState()`, `finishExtractionProgress()`, `resetTopicSearch()`. The four corpus handlers write failures to `corpusActionStatus`/`corpusActionError`.

- [ ] **Step 6: Run the specs and the build**

Run: `cd frontend && npx vite build && npx playwright test tests/session.spec.ts tests/dead-code.spec.ts tests/seed-upload.spec.ts tests/refresh-scheduler.spec.ts tests/boot-requests.spec.ts tests/corpus-reconcile.spec.ts --workers=2`
Expected: `✓ built`, all passed. `grep -c "authStatus = 'unauthenticated'" src/App.svelte` ≤ 3 (bootstrap catch, handleLogout, and at most one auth-form path).

- [ ] **Step 7: Commit**

```bash
git add frontend/src/lib/api.js frontend/src/App.svelte frontend/tests/session.spec.ts
git rm frontend/src/lib/sample-data.js
git commit -m "api.js: one requestJson helper and one unauthorized handler; App signs out fully on 401 and ignores stale /me answers"
```

---

### Task 3: `lib/works.js`

**Files:**
- Create: `frontend/src/lib/works.js`
- Modify: `frontend/src/App.svelte` (author parsers ~779–856; `seedCandidateState` ~2851; `SEED_*_LABELS` ~3096; `seedMetadataLabel`/`seedDownloadLabel`/`seedCellText` ~3125–3166; `toggleSeedSort`/`seedSortIndicator` ~3167–3188; `toggleCorpusSort`/`corpusSortIndicator` ~3062–3074; `normalizeCorpusItem`/`normalizeUpstreamBrowseItem`)
- Modify: `frontend/src/components/Corpus.svelte` (status sets, labels, `cellText`, `corpusItemAuthors*`)
- Modify: `frontend/src/components/UpstreamBrowser.svelte` (`itemAuthors`)
- Test: `frontend/tests/works-lib.spec.ts` (new; imports the module directly in a Playwright test via `page.evaluate` is not needed — use `test()` with a plain `import { parseAuthors, cellText, ... } from '../src/lib/works.js'`, as `tests/graph-geometry.spec.ts` does for `lib/graphGeometry.js`)

**Interfaces:**
- Produces: the exports listed in spec D3 with these signatures: `workState(value: unknown): string`, `bucketForStatus(status: string|null): 'raw'|'metadata'|'failed'|'downloaded'`, `metadataLabel(item): string`, `downloadLabel(item): string`, `parseAuthors(value): string[]`, `formatAuthors(entry): string`, `formatAuthorsShort(entry, cap = 3): string`, `normalizeWorkItem(entry): entry & { authors_list, authors_display, authors_short }`, `cellText(item, key, { blank = '-' } = {}): string`, `nextSort(current: string, column: string): string`, `sortIndicator(current: string, column: string): string`.

- [ ] **Step 1: Write the failing unit spec**

```ts
import { parseAuthors, formatAuthorsShort, cellText, nextSort, sortIndicator, workState, bucketForStatus, metadataLabel } from '../src/lib/works.js'

test('parseAuthors keeps "Last, First" names intact when they arrive as an array', () => {
  expect(parseAuthors(['Smith, J.', 'Doe, A.'])).toEqual(['Smith, J.', 'Doe, A.'])
  expect(parseAuthors('["Smith, J.", "Doe, A."]')).toEqual(['Smith, J.', 'Doe, A.'])
  expect(parseAuthors('A. One; B. Two')).toEqual(['A. One', 'B. Two'])
  expect(parseAuthors('A. One, B. Two')).toEqual(['A. One', 'B. Two'])
  expect(parseAuthors('')).toEqual([])
})
test('formatAuthorsShort caps at three with et al.', () => {
  expect(formatAuthorsShort({ authors: ['A', 'B', 'C', 'D'] })).toBe('A, B, C et al.')
})
test('cellText blank default is per caller', () => {
  expect(cellText({}, 'year')).toBe('-')
  expect(cellText({}, 'year', { blank: '' })).toBe('')
})
test('sort toggles asc → desc → asc and the indicator follows', () => {
  expect(nextSort('', 'year')).toBe('year:asc')   // adjust to the existing encoding in App.svelte (read toggleSeedSort first and pin THAT encoding)
})
test('workState whitelists the ten states', () => { expect(workState('bogus')).toBe('pending'); expect(workState('DOWNLOADED')).toBe('downloaded') })
test('bucketForStatus and labels match the tables', () => { expect(bucketForStatus('matched')).toBe('metadata'); expect(metadataLabel({ status: 'failed_enrichment' })).toBe(/* the exact SEED_METADATA_LABELS.failed_enrichment string from App.svelte */) })
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd frontend && npx playwright test tests/works-lib.spec.ts`
Expected: FAIL with "Cannot find module '../src/lib/works.js'".

- [ ] **Step 3: Implement `lib/works.js`**

Move the existing tables and functions (exact strings) from `App.svelte`/`Corpus.svelte` into the module; `parseAuthors` per spec D3; `cellText` is the union switch of `seedCellText` and Corpus `cellText` (document the three keys that differed in a comment, keep both behaviours keyed on the column name). `nextSort`/`sortIndicator` encode exactly what `toggleSeedSort`/`seedSortIndicator` do today (read them first; the corpus toggle must behave identically after the switch).

- [ ] **Step 4: Switch the callers**

`App.svelte` imports from `./lib/works` and deletes its copies (`formatAuthors`, `formatAuthorsList`, `formatAuthorsShort`, `parseAuthorsValue` if its `;` rule is now inside `parseAuthors`, `seedCandidateState` → `workState`, the label tables and label functions, `seedCellText` → `cellText(candidate, key, { blank: '-' })`, the sort helpers). `Corpus.svelte` drops its status sets/labels/`cellText`/author helpers and uses `item.authors_short`/`item.authors_list`; `UpstreamBrowser.svelte` `itemAuthors` returns `item.authors_display`. Props `formatAuthors`/`formatAuthorsShort` passed into `Corpus` go away (the component imports the module). `normalizeCorpusItem`/`normalizeUpstreamBrowseItem` call `normalizeWorkItem`.

- [ ] **Step 5: Run the specs and the build**

Run: `cd frontend && npx vite build && npx playwright test tests/works-lib.spec.ts tests/seed-upload.spec.ts tests/refresh-scheduler.spec.ts tests/boot-requests.spec.ts tests/corpus-reconcile.spec.ts tests/upstream-metadata.spec.ts tests/table-columns.spec.ts --workers=2`
Expected: `✓ built`, all passed. `grep -n "split(','" src/components/Corpus.svelte` returns nothing.

- [ ] **Step 6: Commit**

```bash
git add frontend/src/lib/works.js frontend/src/App.svelte frontend/src/components/Corpus.svelte frontend/src/components/UpstreamBrowser.svelte frontend/tests/works-lib.spec.ts
git commit -m "lib/works.js: one module for work state, labels, authors, cell text and sort toggles"
```

---

### Task 4: Seed selection as Sets, no table remounts, paged seed counts

**Files:**
- Modify: `frontend/src/App.svelte` (selection functions ~3216–3342; `reconcileSeedSourceCandidates` ~2907; template ~5451–5710; `promoteSeedCandidateKeys`/`handleDismissSelectedSeed`/`handlePromoteWholeSeedSource` where they read selections)
- Test: `frontend/tests/seed-selection.spec.ts` (new, mocked)

**Interfaces:**
- Produces: `seedSelections: Record<string, Set<string>>`; `getSeedSelectionForSource(source): Set<string>`; `loadedSelectableKeys(source): string[]`; `sortedSeedCandidates(source, candidates, sorts)` memoised per source id.

- [ ] **Step 1: Write the failing spec**

`tests/seed-selection.spec.ts` (mocked; one search seed with `candidate_count: 70000`, candidates endpoint returns 3 rows with `total: 70000`, `state_counts: { in_corpus: 10, downloaded: 5 }` on the source):

```ts
test('toggling a row keeps the table mounted', async ({ page }) => {
  // expand the seed; const row = page.getByTestId('seed-candidate-row').first(); const handle = await row.elementHandle()
  // click its checkbox; expect(await handle.evaluate((el) => el.isConnected)).toBe(true); the checkbox is checked
  // select all; deselect one; the remaining two stay checked; the handle is still connected
})
test('expanding a paged seed keeps the server count', async ({ page }) => {
  // after expansion the seed pill still reads 70,000 (candidate_count) and "Promote all" is enabled with the estimate 69,985
})
test('a corpus switch clears selections', async ({ page }) => {
  // select a row; switch corpus via the picker (mock /api/corpora/:id/select + /api/auth/me with last_corpus_id 2); the selection count is 0 and no page error fired
})
```

Add `data-testid="seed-candidate-row"` on the candidate row element if absent.

- [ ] **Step 2: Run it to verify it fails**

Run: `cd frontend && npx playwright test tests/seed-selection.spec.ts`
Expected: FAIL (handle disconnected after the click; pill reads 3).

- [ ] **Step 3: Sets and no `{#key}`**

Replace array selections with Sets (immutably replaced), delete `seedSelectionVersions` and the four `{#key}` blocks, add the `{@const}` computations (spec D4), one `loadedSelectableKeys(source)`, memoised sorting (`const seedSortCache = new Map()` keyed by source id storing `{ candidates, sorts, sorted }`). `selectedSeedCount` uses `.size`. Promotion/dismiss paths use `[...selection]`.

- [ ] **Step 4: Paged seed counts**

`reconcileSeedSourceCandidates`: compute `const page = seedPages[sourceId]`; only when `!page || nextCandidates.length >= page.total` overwrite `candidate_count`/`state_counts`; otherwise leave the source as the server sent it.

- [ ] **Step 5: Run the specs and the build**

Run: `cd frontend && npx vite build && npx playwright test tests/seed-selection.spec.ts tests/seed-upload.spec.ts tests/search-scale.spec.ts tests/refresh-scheduler.spec.ts tests/boot-requests.spec.ts tests/corpus-reconcile.spec.ts --workers=2`
Expected: `✓ built`, all passed. `grep -c '{#key' src/App.svelte` = 0 (or only keys unrelated to seeds, listed in the report).

- [ ] **Step 6: Commit**

```bash
git add frontend/src/App.svelte frontend/tests/seed-selection.spec.ts
git commit -m "App.svelte: seed selections as Sets, no table remount per click, paged seeds keep the server count"
```

---

### Task 5: Refresh plan table, per-resource in-flight dedupe, request gating

**Files:**
- Modify: `frontend/src/App.svelte` (`runLiveRefreshCycleOnce` ~1702, `refreshForActiveTab` ~1772, `startWorkspaceLoads`/`refreshAll` ~2019–2056, `loadCorpus` ~4152, `loadIngestStats` ~2822, `loadKantroposTargets`/`loadUpstreamBrowse`/`loadKantroposAssignment` ~2240–2372, `onMount` focus/visibility listeners, `runSearch`/promotion/preview quota refresh)
- Test: `frontend/tests/tab-resources.spec.ts` (new, mocked)

**Interfaces:**
- Consumes: `awaitPendingCorpusReconcile()`, `reconcileCorpusWithBackend()` (phase 2).
- Produces: `TAB_RESOURCES`, `ADMIN_RESOURCES`, `resourcesFor(tab, { admin })`, `loaders`, `refreshResources(names: string[]): Promise<void>`, `refreshQuota()`.

- [ ] **Step 1: Write the failing spec**

`tests/tab-resources.spec.ts` (mocked; count requests per path; `corpusDelayMs` option as in `refresh-scheduler.spec.ts`):

```ts
test('switching to Korpus Management during a slow corpus poll still loads the browse', async ({ page }) => {
  // boot admin on #/workspace with /api/corpus delayed 1500 ms; immediately navigate to #/upstream
  // expect a request to /api/kantropos/corpora and the browse route within 2 s
})
test('an admin workspace cycle makes no downloads or download-worker-status requests', async ({ page }) => {
  // boot admin on #/workspace with a running search run (2 s cadence); wait 5 s
  // counts['/api/downloads'] === 0 && counts['/api/downloads/worker/status'] === 0; counts['/api/pipeline/daemon/status'] >= 2
})
test('the admin dashboard loads the global overview', async ({ page }) => {
  // boot admin on #/dashboard; /api/ingest/stats?global=1 answers { stats: { downloaded: 42 } }
  // the global overview shows 42; exactly one global stats and one global daemon request per refresh
})
test('a resource in flight is not requested twice', async ({ page }) => {
  // delay /api/seed/sources 1500 ms; trigger focus twice 100 ms apart; counts['/api/seed/sources'] === 1 after 2 s
})
test('the corpus poll asks for one page and skips unchanged pages', async ({ page }) => {
  // boot on #/workspace; wait two cycles; every /api/corpus request has limit=<CORPUS_PAGE_SIZE>;
  // instrument via page.evaluate a MutationObserver on the corpus table: zero mutations between two identical polls
})
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd frontend && npx playwright test tests/tab-resources.spec.ts`
Expected: FAIL (test 1 times out; test 2 counts downloads requests; test 3 shows 0).

- [ ] **Step 3: The resource table and `refreshResources`**

Add `TAB_RESOURCES`, `ADMIN_RESOURCES`, `resourcesFor`, `loaders`, `const inFlight = new Map()` and `async function refreshResources(names)` that, for each unique name, reuses `inFlight.get(name)` or starts `loaders[name]()` stored in the map and removed in `finally`; awaits all. Rewrite `refreshAll(targetTab, { inFlight })` to keep the boot pre-start (the pre-started promises are registered in the map under their names), `refreshForActiveTab(tabId)` to `await awaitPendingCorpusReconcile(); await refreshResources(resourcesFor(tabId, { admin: diagnosticsEnabled }))`, and `runLiveRefreshCycleOnce` to compute the names (tab resources + `runs`/`daemon` when dirty + `seeds` while a search runs; `quota` only when `refreshReason() === 'idle'` and the tab is `admin` or `workspace`). Delete `pipelineRefreshInFlight` and `lastTabRefreshAt`. Keep `refreshReason`/`scheduleRefresh`/`requestRefreshSoon` untouched.

- [ ] **Step 4: Gating**

Remove `loadDownloadWorkerStatus` from every refresh path (grep; keep the three worker-button handlers). Move `loadKantroposAssignment()` out of `refreshAll` into `loadUpstreamBrowse` (after targets), with `loadKantroposTargets` behind one shared promise (`kantroposTargetsPromise`). `refreshQuota()` = `loadOpenAlexQuota()` called after `runSearch`, in each promotion's `finally`, and after the expansion preview. One debounced `onFocusOrVisible` (250 ms). `loadCorpus` poll behaviour per spec D5 (one page, in-place first page, signature skip). `loadIngestStats` catch path: `if (epoch !== corpusScopeEpoch) return` before writing `ingestStatsStatus`. Dashboard: `globalStats`/`globalDaemon` resources (admin only) so `globalIngestStats`/`globalWorkerBacklog` fill.

- [ ] **Step 5: Run the specs and the build**

Run: `cd frontend && npx vite build && npx playwright test tests/tab-resources.spec.ts tests/refresh-scheduler.spec.ts tests/boot-requests.spec.ts tests/corpus-reconcile.spec.ts tests/seed-upload.spec.ts tests/navigation.spec.ts --workers=2`
Expected: `✓ built`, all passed. `grep -c pipelineRefreshInFlight src/App.svelte` = 0.

- [ ] **Step 6: Commit**

```bash
git add frontend/src/App.svelte frontend/tests/tab-resources.spec.ts
git commit -m "App.svelte: one tab→resources table with per-resource in-flight dedupe; drop the duplicate worker-status, downloads and quota requests"
```

---

### Task 6: Log stream batching, single JSON parse, keyed log rows

**Files:**
- Modify: `frontend/src/App.svelte` (`connectLogs` ~4664 `onmessage`; `hydrateLogsFromTail`/`loadOlderLogs` ~4603–4662; `disconnectLogs`; `resetLogState` ~610; `parseExtractionEventMessage` ~966; `setActiveTab` for the flush on opening logs)
- Modify: `frontend/src/components/Logs.svelte` (keyed each over `{ seq, text }`, `data-log-count`)
- Test: `frontend/tests/logs-stream.spec.ts` (new, mocked with `routeWebSocket` server-side sends)

**Interfaces:**
- Produces: `logs: Array<{ seq: number, text: string }>`; `pushLogLine(text)` (buffer), `flushLogs()`; `tryParseJson(data) → object|null`.

- [ ] **Step 1: Write the failing spec**

```ts
test('a burst of 200 lines is one flush', async ({ page }) => {
  // on #/logs, install a MutationObserver counting attribute changes of data-log-count on [data-testid=logs-panel]
  // send 200 text lines back-to-back from the mocked socket; wait 600 ms
  // expect the count attribute to read 200 and the observer to have fired ≤ 2 times
})
test('lines received on another tab appear when the logs tab opens, capped at the history', async ({ page }) => {
  // on #/workspace send 6000 lines (maxLogHistory is 5000: read the constant from App.svelte and assert that number); open #/logs; expect data-log-count = 5000 and the last line text visible
})
test('a JSON extraction frame drives the extraction card and a promotion frame is ignored', async ({ page }) => {
  // send { kind: 'promotion', ... } then { kind: 'extraction', event: 'started', corpus_id: 1, base_name: 'paper', filename: 'paper.pdf' }
  // expect no page error and the extraction card active for 'paper'; data-log-count unchanged by the JSON frames
})
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd frontend && npx playwright test tests/logs-stream.spec.ts`
Expected: FAIL (observer fires 200 times; `data-log-count` absent).

- [ ] **Step 3: Implement**

A non-reactive `let logBuffer = []`, `let logFlushTimer = null`, `let logSeq = 0`; `pushLogLine(text)` pushes `{ seq: ++logSeq, text }`, trims the buffer to `maxLogHistory`, and arms `logFlushTimer = setTimeout(flushLogs, 250)` when `activeTab === 'logs'`; `flushLogs()` assigns `logs = [...logs, ...logBuffer].slice(-maxLogHistory)`, clears the buffer, updates `logsCursor` as the old code did (per line count). `setActiveTab('logs')` calls `flushLogs()`. `disconnectLogs`/`resetLogState` clear the timer and buffer. `onmessage`: `const msg = line.startsWith('{') ? tryParseJson(line) : null`; `if (msg) { if (msg.kind === 'extraction') handleExtractionFrame(msg); return }` (move the existing extraction branch into `handleExtractionFrame(event)` taking the parsed object; `parseExtractionEventMessage` becomes `extractionEventFromObject(obj)`). Tail hydration maps strings to `{ seq, text }`. `Logs.svelte`: `{#each logs as entry (entry.seq)}<div class="log-line">{entry.text}</div>{/each}` and `data-log-count={logs.length}` on the panel.

- [ ] **Step 4: Run the specs and the build**

Run: `cd frontend && npx vite build && npx playwright test tests/logs-stream.spec.ts tests/refresh-scheduler.spec.ts tests/corpus-reconcile.spec.ts tests/seed-upload.spec.ts --workers=2`
Expected: `✓ built`, all passed.

- [ ] **Step 5: Commit**

```bash
git add frontend/src/App.svelte frontend/src/components/Logs.svelte frontend/tests/logs-stream.spec.ts
git commit -m "Log stream: buffer socket lines and flush once per 250 ms, parse JSON frames once, keyed log rows"
```

---

### Task 7: Shared test helpers, socket mocks everywhere, CI list, README

**Files:**
- Create: `frontend/tests/helpers/mockApi.ts`
- Modify: every mocked spec in `frontend/tests/*.spec.ts` that sets `rag_feeder_token` (add `mockLogSocket(page)`; optionally adopt `fulfil`/`mePayload` where the spec already defines identical helpers)
- Modify: `.github/workflows/ci.yml` (Playwright list)
- Modify: `README.md` (Current Runtime Architecture → "Frontend refresh model")

**Interfaces:**
- Produces: `fulfil(route, body, status = 200)`, `mePayload({ id = 1, username = 'admin', admin = true, corpusId = 1, corpora })`, `mockLogSocket(page): Promise<void>`.

- [ ] **Step 1: Helpers**

Create `tests/helpers/mockApi.ts` with the three exports. In each mocked spec that fakes a token (`grep -l rag_feeder_token tests/*.spec.ts`), add `await mockLogSocket(page)` before `page.goto` unless the spec already calls `page.routeWebSocket`. Do not change what the specs assert.

- [ ] **Step 2: CI**

Add `tests/dead-code.spec.ts tests/session.spec.ts tests/works-lib.spec.ts tests/seed-selection.spec.ts tests/tab-resources.spec.ts tests/logs-stream.spec.ts` to the explicit `npx playwright test` list in `.github/workflows/ci.yml` (keep `--workers=2`).

- [ ] **Step 3: README**

Under "Current Runtime Architecture" add "### Frontend refresh model": the `TAB_RESOURCES`/`ADMIN_RESOURCES` tables, per-resource in-flight dedupe, the scheduler cadences (2 s active, 20 s idle, hidden pauses), the single `onUnauthorized` → `expireSession` path, and the log buffer flush. Eight to twelve lines.

- [ ] **Step 4: Run the whole CI list and the build**

Run: `cd frontend && npx vite build && npx playwright test tests/graph-3d.spec.ts tests/seed-upload.spec.ts tests/boot-requests.spec.ts tests/search-scale.spec.ts tests/topic-search.spec.ts tests/table-columns.spec.ts tests/corpus-reconcile.spec.ts tests/dead-code.spec.ts tests/session.spec.ts tests/works-lib.spec.ts tests/seed-selection.spec.ts tests/tab-resources.spec.ts tests/logs-stream.spec.ts tests/refresh-scheduler.spec.ts tests/navigation.spec.ts tests/upstream-metadata.spec.ts --workers=2`
Expected: `✓ built`, all passed.

- [ ] **Step 5: Commit**

```bash
git add frontend/tests/helpers/mockApi.ts frontend/tests/*.spec.ts .github/workflows/ci.yml README.md
git commit -m "Frontend tests: shared mock helpers and log-socket mock in every mocked spec; CI runs the phase 3 specs; README refresh model"
```
