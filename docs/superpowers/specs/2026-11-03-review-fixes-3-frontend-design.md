# Review fixes, phase 3: frontend hot paths, dead code and request plumbing — design

Date: 2026-11-03. Source: `docs/reviews/2026-10-27-frontend-dry-perf.md` (findings 1–9, 11, 12;
minors 2, 3, 4, 6, 9, 16) plus three frontend follow-ups from the phase 2 review (PR #76).
Jay: "continue with the plan until all phases are done. use opus med subagents with gpt 6.1
xhigh gate as before. make no mistakes" — Opus-medium implementers under subagent-driven
development, Codex (`gpt-6.1-sol`, reasoning xhigh) as the per-task and final review gate.
Phases 1 (SQL hot paths, PR #74/#75) and 2 (backend access and child processes, PR #76) are
merged and deployed. Phase 4 (Python DRY) follows this one.

Jay opted out of design questions for this phase; the scope and the calls below are mine and
are marked as such where the review left a choice.

## Problems (all verified against master `33acdde`)

1. **The seed candidate table is rebuilt on every checkbox click.** Four
   `{#key …:${selectionVersion}}` blocks (`App.svelte` ~5472, 5514, 5556, 5588) wrap the
   select-all checkbox, the actions, the toolbar and the whole candidate table;
   `seedSelectionVersions` is bumped by every toggle. `isSeedCandidateSelected` runs
   `Array.includes` per row (2× per row), `seedCellText` runs twice per cell,
   `estimatedSelectableSeedCount` five times per seed summary, `sortSeedCandidates` for every
   seed on every seed-list poll.
2. **The live poll re-fetches and re-renders the whole loaded corpus.** `loadCorpus({quiet,
   preserveSelection})` requests `max(120, corpusItems.length)` rows every cycle; every row is
   rebuilt and reassigned even when nothing changed. Four `$:` filters over `corpusItems`
   (`rawItems`, `metaItems`, `downloadedItems`, `failedItems`) are computed and never read.
3. **Redundant requests per cycle.** `loadDownloadWorkerStatus` runs beside
   `loadPipelineWorkerStatus` in every refresh path although the daemon-status payload already
   fills `downloadWorker` (and the two responses race to write it). `loadDownloads` runs every
   admin cycle on tabs that do not render it. `loadOpenAlexQuota` runs every cycle.
   `refreshAll` always runs `loadKantroposAssignment`, which may trigger
   `loadKantroposTargets`, duplicated by `loadUpstreamBrowse`.
4. **The log stream copies the 5,000-line array per socket message on every tab**, parses each
   JSON frame twice (`parsePromotionEventMessage`, then `parseExtractionEventMessage`), and
   `Logs.svelte` renders an unkeyed `{#each}` over up to 5,000 lines.
5. **The refresh plan is written three times** (`refreshAll`/`startWorkspaceLoads`,
   `refreshForActiveTab`, both branches of `runLiveRefreshCycleOnce`) around one global
   `pipelineRefreshInFlight` flag. **Bug:** `refreshForActiveTab` returns early while a cycle is
   in flight or within 800 ms of the previous tab refresh; for `#/upstream` that call is the
   only automatic `loadUpstreamBrowse`, so the tab can open empty. `focus` and
   `visibilitychange` both trigger a full tab refresh.
6. **About 1,100 unreachable lines in `App.svelte`** — the `ingest` and `search` tabs (no tab id
   reaches them; `normalizeTab` maps only `seed`/`corpus`), their script support
   (`latestEntries`/`latestSelection` machinery, `loadLatestEntries`, `processMarkedBatch`,
   `searchResults`/`searchQueueConfigs`/`searchSelection`, `enqueueSelectedSearchResults`), the
   promotion animation subsystem (gated on `activeTab === 'corpus'`, never true), the no-op
   scroll preservation (`*CorpusTableEl` never bound), unused helpers
   (`formatPipelineStatus`, `pipelineStatus*`, `buildDegreeMap`, `clamp`), about 55 dead CSS
   classes and 24 duplicate selectors in `app.css`, and `tests/pipeline-perf.real-pdf.spec.ts`
   clicking a `tab-ingest` that no longer exists. `finalizeExtractionAfterBackendCompletion`
   fetches `fetchBibliographyEntries` into the dead `latestEntries`.
7. **The 401 block is copied 36 times and is incomplete**: `authStatus = 'unauthenticated';
   setAuthToken('')` neither disconnects the socket nor resets state, so a previous user's seed
   candidates can survive into the next login. `handleLogout` is the complete version (phase 2
   wired the socket's 4401 close to it).
8. **`api.js` repeats the request boilerplate 56 times.** Five functions (`fetchMe`,
   `fetchCorpora`, `selectCorpus`, `createCorpus`, `shareCorpus`) skip the 401 tag, so an
   expired token shows raw JSON instead of signing out. Error messages are raw response bodies.
   `fetchDownloadQueue` and `runKeywordSearch` fall back to sample data on any error, including
   a timeout, so the admin Downloads table can show fabricated rows under load.
9. **Status, label, author and cell logic is duplicated** across `App.svelte`, `Corpus.svelte`
   and `UpstreamBrowser.svelte`: `seedCandidateState` ≡ `latestIngestState` (dead);
   `SEED_METADATA_LABELS`/`SEED_DOWNLOAD_LABELS` ≡ Corpus's label tables; `RAW_STATUSES` and
   failed-status sets in both files; `seedCellText` ≡ Corpus `cellText`; the sort toggles and
   indicators; three author parsers with different split rules. **Bug:** Corpus re-splits the
   joined `authors_display` on `,`, so `["Smith, J.", "Doe, A."]` renders as four authors.
10. **Bug:** the admin Dashboard's "global overview" always shows zeros: nothing calls
    `loadIngestStats({ global: true })` or `loadPipelineWorkerStatus({ global: true })`.
11. **Bug:** expanding a paged seed overwrites its `candidate_count` and `state_counts` with the
    loaded page (`reconcileSeedSourceCandidates`), so a 70,000-item search seed shows
    "200 items" until the next poll.
12. Follow-ups from PR #76: a pending `/api/auth/me` poll answered after a sign-out and a new
    login can overwrite the new session's user and corpora when both share the corpus; the
    ingest-status catch path (`loadIngestStats`) lacks the corpus-epoch guard the success path
    has; mocked Playwright specs that fake a token open the real log socket, which a live dev
    stack closes with 4401 and signs the page out (CI has no backend, so CI never sees it).

## Goals

- The seed table, the corpus poll and the log stream do work proportional to what changed,
  not to what is loaded.
- One place says which tab needs which resource; a tab switch always loads its data.
- One request helper, one 401 path, one sign-out.
- One module for work state, labels, authors and cell text, used by every table.
- The four correctness bugs (5, 9, 10, 11) and the three PR #76 follow-ups fixed.
- No user-visible behaviour change beyond the bug fixes; every change covered by a mocked
  Playwright spec that runs in CI.

## Non-goals (my calls)

- No `WorkTable.svelte`/`WorkDetailCard.svelte` extraction (review finding 9's last step):
  the markup move is large and gains little once `lib/works.js` holds the logic.
- No consolidation of the four "enter session"/three "corpus change"/two "after promote" flows
  (finding 10) beyond what the 401 path needs: those flows interleave with the phase 2
  reconcile logic and are better handled with the backend-side scope answer the phase 2
  review asked for.
- No lazy tab components (minor 7), no server-side stage filter (minor 1), no vite polling
  flag (minor 15), no backend `limit` clamp on `/api/corpus` (the poll stops asking for more
  than a page; the clamp is a backend phase item).
- The Logs panel's history stays scoped to the current corpus while the live stream shows
  every readable corpus (PR #76 observation): aligning them needs `/api/logs/tail` to accept
  a merged scope, a backend item.
- Backend code is untouched except `.github/workflows/ci.yml` and `README.md`.

## Decisions

### D1. Session and request plumbing (`api.js`, 401)

- `api.js` gets one private `requestJson(path, { method = 'GET', body, headers, timeout =
  DEFAULT_TIMEOUT, errorMessage = 'Request failed', raw = false } = {})`: sets
  `Content-Type: application/json` when `body` is an object (serialises it), adds the bearer
  header, applies the timeout, calls the registered unauthorized handler and throws an error
  with `status = 401` on 401, on any other non-OK response throws `Error` whose `message` is
  the body's `error` field when the body is JSON with one, else the raw text, else
  `errorMessage`, with `error.status` set; returns `response.json()` (or the `Response` when
  `raw`). Every exported function that today inlines this sequence uses it. `login` keeps its
  own body handling for the token. Multipart uploads pass `body: FormData` and no JSON header.
- `export function onUnauthorized(handler)` registers one handler; `requestJson` calls it
  once per 401 before throwing. `App.svelte` registers `expireSession()` at module init:
  it runs the `handleLogout()` path (disconnect socket, clear token, reset state) and sets
  `authError = ''`. The 36 catch blocks become `if (error?.status === 401) return` where
  something follows, or disappear where the catch only handled 401.
- `fetchMe`, `fetchCorpora`, `selectCorpus`, `createCorpus`, `shareCorpus` go through
  `requestJson` and therefore tag 401 too.
- Sample fallbacks are removed: `fetchDownloadQueue` throws like every other call;
  `runKeywordSearch` loses `fallbackToSample`; `src/lib/sample-data.js` is deleted. Status
  text that said "Showing sample … data" goes with it.
- A session epoch: `let sessionEpoch = 0`, bumped in `expireSession`/`handleLogout` and on every
  successful login/accept-invite/reset. `runCorpusReconcile` captures it before `fetchMe` and
  discards the answer when it changed (same pattern as `corpusScopeEpoch`).
- `resetFrontendState()` assigns from one `initialWorkspaceState()` factory that also covers
  `seedSorts`, `selectedSeedCandidateKeys`, `startedSearchRunIds`, `corpusHasMore`,
  `corpusFilterQuery`, `seedFilterQuery`, and the full `corpusStageTotals` shape
  (`raw, metadata, downloaded, failed_enrichment, failed_download`).
- `handleSelectCorpus`/`handleCreateCorpus`/`handleDeleteCorpus`/`handleShareCorpus` report
  failures through `corpusActionStatus` (+ `corpusActionError = true`), not `authError`.

### D2. Dead code

Delete, in `App.svelte`: the `ingest` and `search` tab markup and every script member only they
reach (list in problem 6, plus `latestSelectionKey`, `hasLatestEntryId`, `latestIngestState`,
`isLatest*`, `latestIngestTag`, `clearLatestSelection`, `selectAllLatest`,
`toggleLatestSelection`, `enqueueSelectedLatest`, `handleToggleLatestRun`,
`clearLatestEntriesView`, `searchResultKey`, `defaultSearchQueueConfig`,
`initializeSearchQueueConfig`, `getSearchQueueConfig`, `updateSearchQueueConfig`,
`clearSearchSelection`, `selectAllSearchResults`, `toggleSearchSelection`,
`buildSeedItemFromSearchResult`, `enqueueSelectedSearchResults`, `resetSearchForm` if only the
dead tab calls it, the `$:` derivations at 227–266 and 348–363); the promotion animation
subsystem (`corpusMotionKey`, `isPromotionHighlighted`, `addPromotionHighlight`,
`stageFromTransitionSide`, `parsePromotionEventMessage`, `bindCorpusRow`,
`queuePromotionAnimation`, `waitForNextFrame`, `playPromotionAnimation`,
`flushPromotionAnimations`, `pendingPromotionEvents`, `promotionHighlights`,
`corpusRowElements`); scroll preservation (`snapshotCorpusScroll`, `restoreCorpusScroll`, the
three `*CorpusTableEl` lets, the `scrollSnapshot` branch in `loadCorpus`); the unused helpers
(`formatPipelineStatus`, `pipelineStatusLabel/Tone/Title`, `buildDegreeMap`, `clamp`); the four
dead `$:` filters and, if then unused, `RAW_STATUSES`/`FAILED_STATUSES` in App. The backend
keeps emitting promotion frames; the client ignores JSON frames it does not recognise.

`finalizeExtractionAfterBackendCompletion` stops calling `fetchBibliographyEntries`: after
`loadSeedSources` it looks the pdf seed up by `baseName` (the same lookup `focusSeedSource`
does) and reports "Extraction complete." when that seed has `candidate_count > 0`, else the
`detail` text. The `latestEntries*` state goes except any member a surviving template still
renders (the implementer checks `grep -n latest src/App.svelte` after the deletion; the
extraction progress card keeps its texts).

`app.css`: delete every class selector that matches nothing in `src/**/*.svelte` (excluding
dynamically built names: `openalex-quota-pill--*`, `seed-run-status--*`, and any prefix built
with a template literal), and merge the 24 duplicate top-level selectors, keeping the later
definition's declarations where they differ (that is what the cascade applied). A small Node
script in the task's commit message or report lists the removed selectors.

`tests/pipeline-perf.real-pdf.spec.ts`: the ingest-tab click goes; the spec stays opt-in.

### D3. `lib/works.js`

New `frontend/src/lib/works.js` exporting:
- `WORK_STATES` (the ten-state list) and `workState(value) → state|'pending'`;
- `RAW_STATUSES`, `METADATA_STATUSES`, `FAILED_ENRICHMENT_STATUSES`, `FAILED_DOWNLOAD_STATUSES`,
  `DOWNLOADED_STATUSES`, `bucketForStatus(status) → 'raw'|'metadata'|'failed'|'downloaded'`;
- `METADATA_LABELS`, `DOWNLOAD_LABELS`, `metadataLabel(item)`, `downloadLabel(item)` with the
  exact strings `App.svelte` and `Corpus.svelte` use today (they are identical tables);
- `parseAuthors(value) → string[]`: arrays as-is (each `String(v).trim()`, blanks dropped);
  strings that parse as a JSON array likewise; other strings split on `;` when one is present,
  else on `,`. This is the one rule. `formatAuthors(entry)` (the list joined with `, `) and
  `formatAuthorsShort(entry, cap = 3)` derive from it. The "Last, First" bug is fixed by never
  re-splitting a joined display string: tables read `authors_list`/`authors_short` from the
  normalised item;
- `normalizeWorkItem(entry)` adding `authors_list`, `authors_display`, `authors_short` once at
  load; `normalizeCorpusItem` and `normalizeUpstreamBrowseItem` call it;
- `cellText(item, key, { blank = '-' } = {})` with the union of today's `seedCellText` and
  Corpus `cellText` cases (blank default differs per table, passed by the caller);
- `nextSort(current, column)` and `sortIndicator(current, column)` for both tables' toggles.
`App.svelte`, `Corpus.svelte` and `UpstreamBrowser.svelte` import these and lose their copies.
Corpus's author cells read `authors_short`/`authors_list` and never re-split.

### D4. Seed selection and table

- `seedSelections: Record<sourceId, Set<string>>`, replaced immutably (`new Set(prev)`);
  `getSeedSelectionForSource` returns the Set; counts use `.size`; promotion/dismissal paths
  spread the Set where they need an array. `seedSelectionVersions` and the four `{#key}` blocks
  are deleted. The "selected keys of loaded candidates" pipeline exists once
  (`loadedSelectableKeys(source)`).
- In the candidate row: `{@const checked = selection.has(candidate.candidate_key)}` once per
  row, `{@const text = cellText(candidate, column.key)}` once per cell, and
  `{@const selectableCount = estimatedSelectableSeedCount(source)}` once per seed summary.
- `sortSeedCandidates` is memoised per `sourceId` on `(candidates array reference, sorts)`.
- `reconcileSeedSourceCandidates` only overwrites `candidate_count`/`state_counts` when
  `nextCandidates.length >= page.total` (seed fully loaded); otherwise it keeps the server's
  values (bug 11).
- Behaviour pinned before the change: `seed-upload.spec.ts` and `app.spec.ts` (mocked) and a
  new `seed-selection.spec.ts` that loads a 3-row seed, toggles rows, select-all, deselects one
  out of select-all, and asserts the row DOM nodes are not recreated (hold an element handle
  across a click and check `isConnected`).

### D5. Refresh plan and request gating

- One table: `const TAB_RESOURCES = { workspace: ['stats', 'corpus', 'seeds', 'runs'],
  dashboard: ['stats', 'corpus', 'runs'], downloads: [], upstream: ['upstream'], graph: [],
  scraper: [], logs: [], admin: [] }` and `const ADMIN_RESOURCES = { dashboard: ['downloads',
  'daemon', 'globalStats', 'globalDaemon'], downloads: ['downloads', 'daemon'], workspace:
  ['daemon'] }`; `resourcesFor(tab, { admin })` is their union. A `loaders` map from resource
  name to loader (`stats → loadIngestStats`, `corpus → loadCorpus({preserveSelection, quiet})`,
  `seeds → loadSeedSources({quiet})`, `runs → loadIngestRuns`, `upstream →
  loadUpstreamBrowse({quiet})`, `downloads → loadDownloads`, `daemon →
  loadPipelineWorkerStatus({quiet})`, `globalStats → loadIngestStats({global: true})`,
  `globalDaemon → loadPipelineWorkerStatus({global: true})`, `quota → loadOpenAlexQuota`).
- `refreshResources(names)` dedupes **per resource** through a `Map<name, Promise>`: a name
  already in flight joins that promise instead of starting another. Boot
  (`refreshAll`), tab switch (`refreshForActiveTab`) and the cycle (`runLiveRefreshCycleOnce`)
  call it with `resourcesFor(...)` plus the dirty extras (`runs`, `daemon` when dirty and admin;
  `seeds` while a search run is active). `pipelineRefreshInFlight` and the 800 ms tab guard
  go; the per-resource map is the guard. The scheduler (`refreshReason`/`scheduleRefresh`/
  `requestRefreshSoon`) is unchanged.
- The pending-reconcile rule from phase 2 stays: every entry point first awaits
  `awaitPendingCorpusReconcile()` (cycle: `reconcileCorpusWithBackend()`) and skips when it
  switched the corpus.
- `loadDownloadWorkerStatus` is removed from every refresh path and kept only for the explicit
  worker buttons (`handleStart/StopDownloadWorker`, `handleRunDownloadOnce`).
- `quota` loads once per authentication (as today), at the idle cadence while on `admin` or
  `workspace`, and after `runSearch`, each promotion and the expansion preview; not every
  active 2 s cycle.
- `loadKantroposAssignment` leaves `refreshAll`; `loadUpstreamBrowse` (the upstream tab loader)
  runs it, and `loadKantroposTargets` is wrapped in one shared in-flight promise so the
  browse and assignment paths never fetch targets twice. The Korpus Management tab's
  assignment card is the only consumer (verify with grep; if the workspace renders it too,
  add `'kantropos'` to the workspace resources instead).
- One `onFocusOrVisible` handler debounced so `focus` + `visibilitychange` within 250 ms run
  one refresh.
- `loadCorpus` for the poll (`quiet && preserveSelection`): request `CORPUS_PAGE_SIZE` rows.
  When more rows are loaded, replace the first page in place and keep the appended rows; skip
  the `corpusItems` assignment entirely when `total`, `stageTotals` and the page's
  `(id, status, updated_at)` triples are unchanged (compare a joined signature string). The
  four dead `$:` filters are gone (D2).
- The ingest-status catch path checks the corpus epoch before writing status (follow-up).
- Dashboard global overview (bug 10) is fed by the `globalStats`/`globalDaemon` resources.
- Pinned by `refresh-scheduler.spec.ts` (existing, must keep passing), `boot-requests.spec.ts`
  (one request per resource at boot), and a new `tab-resources.spec.ts`: switching to Korpus
  Management while a slow corpus poll is in flight still requests the Kantropos browse route
  (`/api/kantropos/corpora/:id/browse`)
  (bug 5); an admin workspace cycle makes no `/api/downloads` and no
  `/api/downloads/worker/status` request; the admin dashboard requests the global stats and
  daemon status once per refresh.

### D6. Log stream

- Incoming socket lines go into a plain (non-reactive) array; a flush runs at most every
  250 ms (`setTimeout`, cleared on disconnect) and assigns `logs` once, trimmed to
  `maxLogHistory`. While `activeTab !== 'logs'` the buffer is a ring (same cap) and `logs` is
  assigned when the tab opens. `logsCursor` bookkeeping moves into the flush.
- `onmessage` parses JSON once: `const msg = tryParseJson(event.data)`; extraction events
  dispatch on `msg.kind === 'extraction'`; other JSON frames (including promotion frames) are
  ignored; text lines take the existing path (corpus filter, extraction-progress regex, refresh
  trigger).
- `Logs.svelte` keys the list by a monotonically increasing sequence number carried on each
  entry (`{ seq, text }`); `logs` becomes an array of those objects; the tail hydration maps
  server lines to the same shape.
- Pinned by a new `logs-stream.spec.ts` (mocked `routeWebSocket`): 200 lines pushed in one
  burst produce one `logs` flush (assert via a `data-log-count` attribute on the panel updated
  from `logs.length`, observed once), a JSON extraction frame still drives the extraction
  card, and a promotion frame is ignored.

### D7. Test hygiene and CI

- `frontend/tests/helpers/mockApi.ts` gets (or starts as) the shared pieces specs already
  copy: `fulfil(route, body)`, the `/api/auth/me` payload builder, and
  `mockLogSocket(page)` = `page.routeWebSocket('**/api/ws', () => {})`. Every mocked spec that
  fakes a token calls `mockLogSocket` (the follow-up).
- `.github/workflows/ci.yml` lists the new specs (`seed-selection`, `tab-resources`,
  `logs-stream`, plus any other new mocked spec) in the explicit Playwright list.
- `README.md` gains a short "Frontend refresh model" paragraph under Current Runtime
  Architecture: the `TAB_RESOURCES` table, per-resource in-flight dedupe, the single 401 path.

## Order of work (one task each)

1. Dead code (D2) — shrinks the file for everything after.
2. `api.js` + session plumbing (D1).
3. `lib/works.js` (D3).
4. Seed selection and table (D4).
5. Refresh plan and request gating (D5).
6. Log stream (D6).
7. Test helpers, CI list, README (D7).

## Verification

Per task: the task's mocked spec(s) plus `seed-upload.spec.ts`, `app.spec.ts`,
`refresh-scheduler.spec.ts`, `boot-requests.spec.ts`, `corpus-reconcile.spec.ts` and
`npx vite build`. Final: the whole CI spec list, `vite build`, and a live check on the dev
stack after merge (login, seed table toggles, tab switches while a poll is in flight, logs
panel under a worker burst, admin dashboard global counts non-zero).
