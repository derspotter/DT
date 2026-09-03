# Korpus Builder — Jochen Round 2 — Design

Date: 2026-09-03
Source: Jochen's second feature list (11 items, relayed by Jay on 2026-09-03)
Branch: `jochen-todos` (on top of the 26-line round; master is 15 commits behind)
Scope: workspace tab (App.svelte, Corpus.svelte, tableColumns.js), keyword
search (backend/scripts/keyword_search.py, dl_lit/keyword_search.py,
dl_lit/utils.py), seed listing (backend/src/seed.js), promotion and expansion
(app.js promote route, seed_expand.py, enrich crawl in OpenAlexScraper.py),
extraction route (optional GROBID path).

## Decisions taken with Jay (2026-09-03)

| # | Jochen's line | Decision |
|---|---|---|
| 1 | Show OpenAlex rate limits | Persist last-seen headers from Python, expose via endpoint, show pill in search panel and admin settings |
| 2 | Stream big results | Keyword search becomes a background run that persists page by page; seed row shows progress; cancel keeps what was fetched |
| 3 | Warn at ~≥100,000 | Preflight count endpoint; always show the match count; warn with a fetch-anyway / cap choice at a configurable threshold |
| 4 | Page big results in the frontend | Seed candidates endpoint gets limit/offset/sort; seed table uses the corpus "show more" pattern; select-all means the whole seed |
| 5 | Surface promotion per seed / per item | **Already on this branch** (inline → promote per row, commit `b0dd52a`). Jochen reviewed master. Ships with the merge; no new work |
| 6 | Warn on promote-all + downstream | Inline confirmation with the item count, the expansion ceiling and a request estimate |
| 7 | Show number of refs | `Refs` column (OpenAlex `referenced_works_count`) in the seed table, sortable, toggleable; `Cited by` as a second hidden-by-default column |
| 8 | Downstream extraction with hard limit | (a) hard **total** cap per promotion for OpenAlex expansion, on top of the per-paper cap. (b) Recursive bibliography extraction of downloaded downstream PDFs is **deferred** to its own round: it needs a new worker job type after each download and was only cheap with GROBID |
| 9 | GROBID instead of LLM, LLM fallback for OCR | GROBID is **not** the default. Optional, env-gated path (`RAG_FEEDER_GROBID_URL`), compose profile off by default, never on the dev box. LLM remains the default and the fallback. Last phase, may be cut |
| 10 | Label snowball seeds | Expansion runs already carry `expansion_direction`; they get a distinct "Snowball items" tag |
| 11 | Sticky navigation bar | Sticky bar on the workspace tab with jump links, counts, "Collapse seed" and "Top" |

Why GROBID is not the default: on parsing quality frontier LLMs are at or above
GROBID, especially on the German social-science material this tool sees.
GROBID's advantages are cost, speed and determinism, and Jay judged LLM cost
low enough not to carry a 2–8 GB Java service on the dev server.

## Architecture overview

Nothing changes in the DB-first, queue-first shape. Two additions:

1. **Search runs become stateful.** `search_runs` gains `status`,
   `fetched_count`, `expected_count`, `error`, `finished_at`. The keyword
   search script creates the run first and writes each OpenAlex page into
   `search_results` as it arrives. The backend responds as soon as the run
   exists. The frontend's existing live-refresh poll reads the status through
   the seed list. No SSE, no new worker job type: the script is still spawned
   by the backend, but the HTTP call no longer waits for it.
2. **OpenAlex quota snapshot.** A small JSON file next to the SQLite DB,
   written by the Python request helper after every OpenAlex response and
   read by a new backend endpoint. This bridges the per-process Python rate
   limiter and the long-lived Node backend without touching the DB.

Everything else is additive: new columns, new params, new UI elements.

## Phase 1 — Small items (7, 10, 11; 5 ships as-is)

### 7. Refs column

- `dl_lit/keyword_search.py` `search_openalex`: add `referenced_works_count,cited_by_count`
  to the `select` list. `backend/scripts/keyword_search.py` `OPENALEX_SELECT`:
  add the same two fields (they also cover expansion results).
- `seed.js` `normalizeSearchCandidate`: read `referenced_works_count` and
  `cited_by_count` from `raw_json` into `refs_count` and `cited_by_count`
  (null when absent). `normalizePdfCandidate`: both null (document items are
  not on OpenAlex until enriched; showing the enriched work's count would mean
  a join on `works.openalex_json` — out of scope).
- `tableColumns.js`: `{ key: 'refs', label: 'Refs', width: '70px', sortable: true, defaultVisible: true, tables: ['seed'] }`
  and `{ key: 'cited_by', label: 'Cited by', width: '80px', sortable: true, defaultVisible: false, tables: ['seed'] }`.
  Header tooltip on Refs: "Works this item cites on OpenAlex — what a
  downstream promote pulls in".
- Cell renders the number or `–`. Sorting: nulls last in both directions.
  Client-side sorting stays until Phase 3 moves search seeds server-side; the
  server-side sort then accepts `refs` and `cited_by` (see Phase 3).
- Corpus table: not in this round.

### 10. Snowball label

- `seed.js` `listSeedSources` search branch: when
  `filters.expansion_direction` is set, add `seed_kind: 'snowball'` and
  `snowball: { direction, of_title: filters.expansion_of_title, of_openalex_id }`
  to the source. Otherwise `seed_kind: 'search'`; pdf sources `seed_kind: 'pdf'`.
- App.svelte seed row tag: `Document items` / `Search items` / **`Snowball items`**
  (new tag class, distinct colour, tooltip "Downstream of «title»" or
  "Upstream of «title»"). The label text already reads "Downstream of «…»";
  keep it.
- Stats bar and counters: unchanged.

### 11. Sticky bar

- New element at the top of the workspace tab content (inside
  `.seed-corpus-workspace`, before the section 1 card): `position: sticky;
  top: 0; z-index` above tables. Contents, left to right:
  `1 Find items` · `2 Seed (n)` · `3 Corpus (n)` as jump links to anchors on
  the three section cards (`id="section-find"`, `section-seed`,
  `section-corpus`), then on the right `Collapse seed` (visible only while
  `expandedSeedSourceId` is set; sets it to `''` and scrolls to the seed
  list top) and `Top`.
- Counts come from `seedSources.length` and the corpus total already in state.
- Smooth scroll via `scrollIntoView({ behavior: 'smooth', block: 'start' })`
  with `scroll-margin-top` on the anchors equal to the bar height.
- Responsive: at the 640 px breakpoint the links become a horizontally
  scrollable row; the bar never wraps to two lines.
- Only one seed can be expanded at a time (`expandedSeedSourceId` is a single
  id), so "collapse all" is "collapse the open one". Label it `Collapse seed`.

### 5. Per-item promote

No code. Note in the PR description that the inline promote is in `b0dd52a`
and that Jochen reviewed master.

## Phase 2 — OpenAlex quota (item 1)

### Python side

- `dl_lit/utils.py`: new `record_openalex_quota(response)` called from
  `_apply_openalex_response_limits` (or directly after it in
  `openalex_request_json`) on every response that carries
  `X-RateLimit-Limit`. Writes atomically (`tmp` + `os.replace`) to
  `RAG_FEEDER_OPENALEX_QUOTA_PATH`, default `<db dir>/openalex_quota.json`:

  ```json
  {"limit": 100000, "remaining": 87412, "credits_used": 1,
   "reset_seconds": 18720, "observed_at": "2026-09-03T16:30:12Z",
   "reset_at": "2026-09-03T21:42:12Z", "api_key_present": true}
  ```

  `reset_at` is computed at write time so the reader does not need the
  observation moment. Responses without the headers (no API key) write
  `{"api_key_present": false, "observed_at": ...}` so the UI can say why
  there is no budget.
- Write failures are logged at warning level and never raise.

### Backend

- `GET /api/openalex/quota` (auth required, any corpus): reads the file,
  returns it with `stale: true` when `observed_at` is older than 24 h, and
  `{}` with `available: false` when the file does not exist.
- The admin settings save path already restarts nothing; a changed key takes
  effect on the next script run, and the next response refreshes the file.

### Frontend

- `api.js`: `fetchOpenAlexQuota()`.
- Search panel, next to the Search button: pill
  `OpenAlex budget: 87,412 of 100,000 left · resets in 5 h 12 min`.
  States: no file → `OpenAlex budget: unknown until the first request`;
  `api_key_present: false` → `OpenAlex: no API key — daily budget not
  reported`; `remaining <= 0` → red pill with the reset time; `stale` →
  muted with "last seen <date>".
- Refresh after every search, promote and expansion result, and on the
  existing live-refresh interval while the workspace tab is visible.
- Admin panel: the same data in a read-only row under the OpenAlex key.

## Phase 3 — Search at scale (items 2, 3, 4)

### Schema

`db_manager.py` `_ensure_column` on `search_runs`:
`status TEXT` (`running | done | failed | cancelled`; NULL for legacy rows =
done), `fetched_count INTEGER`, `expected_count INTEGER`, `error TEXT`,
`finished_at TIMESTAMP`. New `DatabaseManager` helpers:
`update_search_run_progress(run_id, fetched, expected)`,
`finish_search_run(run_id, status, error=None)`.

### 3. Preflight count

- `POST /api/keyword-search/preview`: same body as the search; runs
  `backend/scripts/keyword_search.py --count-only`, which builds the exact
  same OpenAlex params and requests `per-page=1`, returning `meta.count`.
  Response `{ count, threshold }`. Threshold from
  `RAG_FEEDER_SEARCH_WARN_THRESHOLD` (default 100000), exposed through the
  admin settings as `search_warn_threshold`.
- Frontend: triggered on submit, before the real search. The panel shows
  `About 342,118 works match`. Below the threshold the search starts
  immediately. At or above it, an inline warning replaces the status line:

  > This search matches 342,118 works. Fetching all of them takes about
  > 1,711 OpenAlex requests and roughly 6 minutes. Narrow the query, or:
  > [Cap at 10,000] [Fetch all 342,118]

  Request estimate = `ceil(count / 200)`; time estimate uses the configured
  RPS. "Cap at N" sets `searchMaxResults` to N (N = threshold / 10) and
  starts. "Fetch all" starts uncapped.
- Preview failures (rate limit, network) do not block: the warning is skipped
  and the search proceeds as today, with the error in the status line.

### 2. Background fetch

- `backend/scripts/keyword_search.py`:
  - creates the run with `status='running'`, `expected_count` from the first
    page's `meta.count` (capped by `--max-results` when set), prints
    `{"event": "run_created", "runId": N}` as its **first stdout line**, then
    fetches.
  - `search_openalex` gets an `on_page(items, meta)` callback. Each page is
    converted with `openalex_result_to_record`, deduped against ids already
    in the run (in-memory set), inserted with `add_search_results`, and
    `update_search_run_progress` is called. Progress lines
    `{"event": "progress", "fetched": n, "expected": m}` go to stdout.
  - Expansion (when the search itself has depth ≥ 1) runs after the base
    fetch, still inside the run, and reports progress the same way.
  - On completion: `finish_search_run(run_id, 'done')` and the final JSON
    payload as today (results list omitted when `fetched_count` >
    `RAG_FEEDER_SEARCH_INLINE_RESULTS`, default 1000, to keep stdout small).
  - On `OpenAlexRateLimitExceeded` or any exception:
    `finish_search_run(run_id, 'failed', error=str(exc))`, rows already
    written stay.
  - SIGTERM handler: `finish_search_run(run_id, 'cancelled')`, exit 0.
- `app.js` `/api/keyword-search`: uses `spawnPythonJson` with
  `onStdoutLine`. On `run_created`, calls `upsertSearchRunCorpus` and
  responds `{ runId, status: 'running' }` immediately. The child keeps
  running; its handle is stored in `activeSearchRuns: Map<runId, child>`.
  Progress lines update an in-memory `{fetched, expected}` mirror (the DB is
  the source of truth; the mirror only avoids a query per poll). Exit
  removes the map entry. Backend restart: the DB row keeps `running`; on
  startup the backend marks every `running` run older than its own start
  time as `failed` with error `backend restarted`.
- `POST /api/keyword-search/:runId/cancel`: SIGTERM to the child if present
  (else 404 if the run is not running). Corpus write access required.
- `seed.js` `listSeedSources` search branch: select the new columns and add
  `run: { status, fetched_count, expected_count, error }` to the source.
  Running runs are listed even with zero results so far (relax the
  `candidates.length === 0` skip for `status='running'`).
- Frontend:
  - `runSearch` no longer awaits results; it focuses the new seed and sets
    the status line to `Fetching…`.
  - Seed row subtitle for running runs: `fetching 4,200 of 12,000 · cancel`
    (button calls the cancel endpoint). Failed: `stopped after 4,200 of
    12,000: <error>`. Cancelled: `cancelled at 4,200 of 12,000`.
  - The live-refresh interval already reloads seed sources; while any run is
    `running` the interval tightens to 2 s (same value as extraction
    polling) and the expanded seed's candidate page is reloaded too.
  - The stub path (`RAG_FEEDER_STUB=1`) returns a completed run as today so
    e2e tests keep working; one new e2e covers the running → done transition
    with a stubbed progress sequence.

### 4. Paging

- `GET /api/seed/sources/:type/:key/candidates` gets `limit` (default 200,
  max 2000), `offset`, `sort` (`title|authors|year|source|metadata|download|refs|cited_by`),
  `dir`. Response becomes `{ candidates, total, offset, limit }`.
  - Search seeds: paging and sorting in SQL over `search_results`. `title`,
    `year`, `doi`, `openalex_id` are columns; `authors`, `source`, `refs`,
    `cited_by` sort via `json_extract(raw_json, ...)` expressions
    (`$.referenced_works_count`, `$.cited_by_count`, first author
    `$.authorships[0].author.display_name`, `$.primary_location.source.display_name`).
    `metadata`/`download` sort by the resolved state and therefore fall back
    to loading all rows for that seed; the UI disables those two sorts when
    `total > 2000` (tooltip explains).
  - The `q` filter stays in JS for pdf seeds (small); for search seeds it
    becomes a SQL `LIKE` on title, the first author and source json paths,
    mirroring the corpus filter.
  - Dismissed keys are excluded in SQL via `NOT IN (SELECT candidate_key …)`.
  - Pdf seeds: keep the JS path, slice in JS. They are bounded by what one
    PDF cites.
- `listSeedSources`: `candidate_count` for search seeds comes from
  `COUNT(*)` minus dismissed, in SQL. `state_counts` (the pills) are computed
  only when `candidate_count <= RAG_FEEDER_SEED_STATE_COUNT_LIMIT` (default
  2000); otherwise `state_counts: null` and the UI hides the pills and shows
  `12,000 items` only. This removes the "load every candidate of every seed
  on every poll" cost.
- Frontend seed table:
  - State per seed: `{ items, total, offset, sort, dir }`. Expanding a seed
    loads page one. `Showing 200 of 12,000 · Show more` under the table, the
    same component pattern as Corpus.svelte.
  - Sorting a column resets to offset 0 and reloads (server-side) for search
    seeds; pdf seeds keep client-side sorting over the full list.
  - Select-all checkbox = `allSelected[sourceId] = true`, shown as
    `All 12,000 items selected`. Unchecking a single row switches to explicit
    selection of the loaded rows minus that one (explicit mode). Promote with
    `allSelected` sends no `candidateKeys` (existing "all promotable"
    semantics); explicit mode sends keys. `estimatedSelectableSeedCount`
    uses `total` in all-selected mode.
  - Dismiss selected in all-selected mode: the dismiss endpoint gets
    `all: true` and inserts dismissals for every current candidate key of the
    seed in SQL.
- `promote` route: unchanged semantics; `listSeedCandidates` there is called
  without paging (it needs every promotable row). It uses the SQL path
  without `limit`.

## Phase 4 — Expansion safety (items 6, 8a)

### 6. Confirmation

- Frontend only. Before `handlePromoteWholeSeedSource` and
  `handlePromoteSeedSource` (selection) when `expansionEnabled`:
  `ceiling = items × maxRelated × directions` (directions = 1 or 2), further
  multiplied by `maxRelated^(depth-1)` per direction for depth > 1. If
  `items > 25` or `ceiling > 500`, render an inline confirmation panel in
  the seed row (no `window.confirm`):

  > Promote 412 items with downstream expansion (depth 1, up to 30 related
  > per item)? This can add up to 12,360 works and takes about 660 OpenAlex
  > requests. The expansion stops at the total cap of 1,000.
  > [Promote anyway] [Cancel]

  Request estimate = `items × (1 + ceil(maxRelated / 50))` per direction.
- Without expansion nothing changes.

### 8a. Hard total cap

- New promotion setting `maxExpansionTotal` (UI: `Max expansion total`,
  number, default 1000, min 1) in the promotion settings row, persisted with
  the other settings. Absolute ceiling `RAG_FEEDER_EXPANSION_HARD_CAP`
  (default 5000, admin setting `expansion_hard_cap`); the backend clamps the
  request value to it.
- `seed_expand.py`: `--max-total`. Counts items across all seeds and
  directions in one invocation; stops collecting once reached and emits
  `truncated: true` plus `total_added` in its JSON. Ranking per paper stays
  as is; the cap only shortens the tail.
- Enrich crawl (`download_all` mode): the `expansion` payload passed to the
  enrich job gains `max_total`. `OpenAlexScraper.process_single_reference`
  and the recursive expansion take a shared `budget` object (`{remaining}`)
  threaded through `max_related_per_reference` calls; when it hits zero,
  further related ids are dropped. `worker.py` reports `expansion_truncated`
  in the job result.
- `keyword_search.py` expansion (search with depth ≥ 1): the same
  `--max-total`, same truncation flag in the payload.
- UI: the promote result message appends `Expansion stopped at the total cap
  of 1,000 works.` when truncated; the search seed subtitle shows the same
  for search-time expansion.

## Phase 5 — Optional GROBID (item 9)

Off by default. Cut if time is short; nothing else depends on it.

- Compose: `grobid` service under `profiles: ["grobid"]`, image
  `lfoppiano/grobid:0.8.1-crf` (CRF-only, ~2 GB), internal port 8070, no
  published port, `mem_limit: 3g`. Started only with
  `docker compose --profile grobid up -d`. Documented in README with the
  RAM note and the arm64 caveat (official images are amd64; on arm64 build
  locally or point at an external instance).
- Settings: `RAG_FEEDER_GROBID_URL` (admin key `grobid_url`, empty = off),
  `RAG_FEEDER_GROBID_MIN_REFS` (default 5), `RAG_FEEDER_GROBID_TIMEOUT_SEC`
  (default 120).
- New `dl_lit/grobid_extractor.py`: `extract_references(pdf_path) ->
  list[dict] | None`. POSTs the PDF to `/api/processReferences`
  (`consolidateCitations=0`, `includeRawCitations=1`), parses the TEI
  `biblStruct` elements into the same entry shape
  `validate_bibliography_entries` expects (title, authors, year, doi,
  source/container, volume, issue, pages, publisher, url, raw). Returns
  `None` on connection error or non-200 so the caller can fall back.
- Text-layer check: `pdfplumber`/`pypdf` page text on the first three pages;
  if empty, skip GROBID (it cannot OCR) and go straight to the LLM path.
- Extraction route: before the existing get-bib-pages + LLM chain, when the
  URL is set and the PDF has a text layer, call GROBID. If it returns at
  least `MIN_REFS` entries, insert them with `metadata_source_type:
  'grobid'`, emit `mode=grobid` in the extract-status signal, and skip the
  LLM. Otherwise continue with the LLM chain exactly as today and emit
  `reason=grobid_fallback` (`no_text_layer | too_few | unavailable`).
- Seed document metadata still comes from the LLM header pass (GROBID's
  header parser is weak on books); unchanged.
- UI: the Document items seed subtitle gains `via GROBID` / `via LLM`; the
  admin panel shows the GROBID URL field with a "Test connection" button
  hitting `/api/isalive` through a backend proxy endpoint.

## Error handling summary

- Quota file unreadable or missing → endpoint returns `available: false`,
  UI shows the unknown state; never a 500.
- Preview count fails → search proceeds, warning skipped, error in status.
- Background run fails or is cancelled → rows already stored remain, run
  status explains, seed remains promotable.
- Backend restart during a run → run marked failed on startup, subtitle says
  `backend restarted`.
- Expansion cap hit → not an error; flagged as truncated in result and UI.
- GROBID down → silent fallback to the LLM with a reason in the extract
  signal and log.

## Testing

- Backend Jest: quota endpoint (missing file, stale, no key); candidates
  paging/sorting/`q` in SQL incl. dismissed exclusion; `listSeedSources`
  snowball kind, running-run inclusion, state-count limit; cancel endpoint
  404 vs 200; startup marking of orphaned runs; promote confirmation is
  frontend-only (no backend test); `max_total` clamp.
- Python (pytest, `dl_lit_project/tests`): `record_openalex_quota` atomic
  write and no-key case; `search_openalex` `on_page` callback and
  `--count-only`; `seed_expand --max-total` truncation; TEI parsing fixture
  for GROBID (Phase 5).
- Playwright e2e (stub mode): sticky bar jump + collapse; Refs column
  visible and sortable; snowball tag rendered for a stubbed expansion run;
  search shows count then warning above threshold and "Cap at" starts a run;
  running → done subtitle transition; "Show more" on a 300-item stubbed seed;
  select-all label and promote payload without keys; confirmation panel on
  big promote with expansion.
- Manual against OpenAlex: one uncapped search on a query with >100k
  matches, cancelled after ~5k, to confirm persistence, progress and cancel.

## Delivery

Five PRs on top of `jochen-todos`, in this order, each independently
mergeable: Phase 1 small items, Phase 2 quota, Phase 3 search at scale,
Phase 4 expansion safety, Phase 5 GROBID (optional). Phase 3 is the only
one with a schema migration; it uses the existing `_ensure_column` path.

## Out of scope (deferred, recorded so it is not lost)

- 8b: automatic bibliography extraction from downloaded downstream PDFs.
  Needs a worker job type triggered after each download, a per-corpus cap on
  PDFs processed, and a way to show the resulting entries as seeds. Own
  round.
- Refs / Cited by columns in the corpus table.
- Grouping the corpus by seed.
