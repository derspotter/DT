# Purchaser Feedback Round 1 — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Address all 20 purchaser feedback items from `Korpus builder.txt` in five independent PRs (copy pass, stages/counters, search cap+sort, OpenAI-compatible LLM, visual restyle).

**Architecture:** The corpus builder UI lives in `frontend/src/App.svelte` (Workspace tab, lines ~3930–4422) and `frontend/src/components/Corpus.svelte`; all CSS is global in `frontend/src/app.css`. Keyword search flows frontend → `POST /api/keyword-search` (`backend/src/app.js:4476`) → `backend/scripts/keyword_search.py` → `dl_lit_project/dl_lit/keyword_search.py:search_openalex`. LLM extraction is Gemini-only in `dl_lit_project/dl_lit/get_bib_pages.py`, `APIscraper_v2.py`, `new_dl.py`, plus inline helpers in `backend/src/app.js`.

**Tech Stack:** Svelte 4 + Vite, Node/Express, Python 3 (`dl_lit` package), Jest (`backend/tests`), pytest (`dl_lit_project/tests`), Playwright MCP for UI verification.

## Global Constraints

- Each PR is a separate branch off `master`, merged via GitHub PR; run `git copilot-comments --wait` before squash-merging (user's standing workflow).
- Spec: `docs/superpowers/specs/2026-07-02-purchaser-feedback-design.md`. Commit the spec + this plan as part of PR A.
- Copy vocabulary (use verbatim everywhere): "Seed document", "Keyword search (OpenAlex)", "bibliographic metadata", "Seeds" (not "seed sources"), "items" (not "candidates"), "Document items" / "Search items", counters "items found / items promoted / items downloaded".
- Legacy hidden tabs (`ingest`, `search`, `downloads` blocks in App.svelte) are out of scope — do not rename strings inside them.
- Verification per UI PR: `cd frontend && npm run build` passes; dev stack via `docker compose up -d`; Playwright snapshot of Workspace tab shows the new strings.
- Python: `ruff check` on touched files; `pytest dl_lit_project/tests -q` green. Node: `cd backend && npm test` green.

---

## PR A — Copy & terminology pass + remove Refresh buttons

Branch: `feedback/copy-pass`. Files: `frontend/src/App.svelte` only (plus spec/plan docs).

### Task A1: Rename intake cards and section 1

**Files:**
- Modify: `frontend/src/App.svelte:3949-3958`, `:4048`, `:4091`

**Interfaces:** Produces final copy strings listed below; no code interfaces.

- [ ] **Step 1: Section 1 heading + description** (`App.svelte:3949-3950`)

```html
<h2 class="workspace-section-title">1. Find items</h2>
<p>Two ways in: extract the bibliography of an uploaded seed document (Document items), or run a keyword search on OpenAlex (Search items). Review everything in Seed below.</p>
```

- [ ] **Step 2: "Document Search" → "Seed document"** (`App.svelte:3958`)

```html
<h3>Seed document</h3>
```

- [ ] **Step 3: "Keyword Search" → "Keyword search (OpenAlex)"** (`App.svelte:4048`) and add a source line under the form's status paragraph (`:4091`)

```html
<h3>Keyword search (OpenAlex)</h3>
...
<p class="muted small">Queries run against the OpenAlex scholarly index.</p>
```

- [ ] **Step 4: Build check** — `cd frontend && npm run build` → exits 0.

- [ ] **Step 5: Commit** — `git commit -m "Rename intake cards: Seed document + Keyword search (OpenAlex)"`

### Task A2: Bibliographic-metadata wording + seeds wording

**Files:**
- Modify: `frontend/src/App.svelte:3934-3942`, `:4234`, `:2630` (status string), empty state `:4142`

- [ ] **Step 1: Hero copy** (`:3934-3937`) — working-on-a-corpus framing (feedback items 2, 3):

```html
<h2>You are building a corpus — not just browsing one.</h2>
<p>
  Feed it seed documents and OpenAlex searches, decide which found items belong, and the pipeline fetches bibliographic metadata and PDFs for everything you promote.
</p>
```

- [ ] **Step 2: Hero stats** (`:3940-3942`) — keep as-is for now (PR B replaces them with the three counters); only rename labels:

```html
<span><strong>{seedSources.length}</strong> seeds</span>
<span><strong>{pipelineMetadataCount}</strong> bibliographic metadata</span>
<span><strong>{pipelineDownloadedCount}</strong> downloaded</span>
```

- [ ] **Step 3: Seed pill** (`:4234`) with explanatory tooltip (feedback items 4, 7):

```html
<span class="pill" title="Items whose bibliographic metadata has been retrieved">Bibliographic metadata: {source.state_counts.enriched}</span>
```

- [ ] **Step 4: "seed sources" → "seeds"** in user-visible strings: empty state (`:4142`) → `No seeds yet. Upload a seed document or run a keyword search.`; any status strings containing "seed sources" (grep `seed sources` in App.svelte, rename user-facing ones only, not variables). Section 2 description (`:4103`):

```html
<p class="muted">Every seed document and search run lands here as a seed. Expand one to review its items and promote the keepers to the corpus.</p>
```

- [ ] **Step 5: Build + commit** — `npm run build`; `git commit -m "Bibliographic metadata + seeds wording, corpus-building framing"`

### Task A3: Items vocabulary (candidates → items, typed tags)

**Files:**
- Modify: `frontend/src/App.svelte:4187`, `:4220`, `:4255-4278` (toolbar/microcopy), select tooltips `:4172-4180`, `:4201`

- [ ] **Step 1: Source-type tag** (`:4187`):

```html
<span class={`tag ${source.source_type === 'pdf' ? 'pending' : 'queued'}`}>{source.source_type === 'pdf' ? 'Document items' : 'Search items'}</span>
```

- [ ] **Step 2: Candidate pill** (`:4220`): `<span class="pill">{source.candidate_count} items</span>`

- [ ] **Step 3: Rename remaining user-visible "candidate(s)"** inside the Workspace block only (tooltips `:4172-4180` → "Select all items in this source" etc., `:4201` promote tooltips, `:4255` "Selected: … selectable", `:4276-4278` loading/empty strings → "Loading items..." / "No active items remain in this seed."). Leave JS identifiers (`fetchSeedCandidates` etc.) unchanged.

- [ ] **Step 4: Build + commit** — `git commit -m "Items vocabulary: Document items / Search items"`

### Task A4: Upload chip, Extracted PDFs tooltip, remove Refresh buttons

**Files:**
- Modify: `frontend/src/App.svelte:3978`, `:3995-3997`, `:4003-4007`, `:4106`

- [ ] **Step 1: Hide the resting "uploaded" chip** (`:3978`) — render the status chip only when it says something (feedback 12):

```html
{#if item.status && item.status !== 'uploaded'}
  <span class={`status ${item.status}`}>{item.status}</span>
{/if}
```

- [ ] **Step 2: Extracted PDFs label + tooltip** (`:3996`, `:4003-4007`): label → `Extracted seed documents`; prefix the existing `title` (feedback 14):

```js
title={`Bibliographic metadata extracted from this PDF. ${formatSeedDocumentDetails({...}) || run.source_pdf || ''}`.trim()}
```

- [ ] **Step 3: Remove both Refresh buttons** — delete `:3997` (`loadIngestRuns` link) and `:4106` (seed panel Refresh). Then verify auto-refresh coverage: grep call sites of `loadIngestRuns()` and `loadSeedSources()`; both must be invoked after upload completion, extraction completion, promote, dismiss, and remove-source. If extraction completion doesn't already call `loadIngestRuns()`, add it in the extraction-completion handler (find via `extractionProgress.active = false` assignment). Add a `visibilitychange` listener in `onMount` that refetches both when the tab becomes visible.

- [ ] **Step 4: Manual verify** — dev stack up, upload a PDF, run extraction; the Extracted-seed-documents list and Seed panel update without any Refresh button.

- [ ] **Step 5: Build + commit** — `git commit -m "Remove Refresh buttons, hide uploaded chip, explain extracted PDFs"`

### Task A5: PR A ship

- [ ] Playwright snapshot of Workspace tab; confirm all new strings, no "candidates"/"Document Search"/"Refresh" remain visible.
- [ ] Commit spec + plan docs; push branch; `gh pr create`; `git copilot-comments --wait`; address comments; squash-merge.

---

## PR B — Stages & counters + collapsibility affordance

Branch: `feedback/stages-counters` (off master after A merges). Files: `frontend/src/App.svelte`, `frontend/src/components/Corpus.svelte`, `frontend/src/app.css`.

### Task B1: Pipeline counters in hero (items found / promoted / downloaded)

**Files:**
- Modify: `frontend/src/App.svelte:3939-3943` (hero stats), `:145-146` (derived counts)

**Interfaces:**
- Produces: derived values `itemsFoundCount` (sum of `source.candidate_count` over `seedSources`), existing `corpusTotal`-style promoted count, `pipelineDownloadedCount`.

- [ ] **Step 1:** Add derived store near `:145`:

```js
$: itemsFoundCount = seedSources.reduce((n, s) => n + (Number(s.candidate_count) || 0), 0)
$: itemsPromotedCount = corpusTotal || corpusItems.length
```

(Verify the actual promoted-count variable: `corpusTotal` is passed to `<Corpus>`; grep its definition and reuse it.)

- [ ] **Step 2:** Replace hero stats:

```html
<span><strong>{itemsFoundCount}</strong> items found</span>
<span><strong>{itemsPromotedCount}</strong> items promoted</span>
<span><strong>{pipelineDownloadedCount}</strong> items downloaded</span>
```

Move the seeds count into the Seed panel header: `<h3 class="workspace-section-title">2. Seed <span class="muted small">({seedSources.length})</span></h3>`

- [ ] **Step 3:** Build + commit — `git commit -m "Hero counters: items found / promoted / downloaded"`

### Task B2: Separate Seed / Promoted / Downloaded stages in Corpus panel

**Files:**
- Modify: `frontend/src/components/Corpus.svelte:144-151` (stage select), `:67-79` (labels)

- [ ] **Step 1:** Regroup the stage filter into optgroups (presentation-only; buckets unchanged):

```html
<select bind:value={stageFilter}>
  <option value="all">All stages ({corpusTotal || corpusItems.length})</option>
  <optgroup label="Seed stage">
    <option value="raw">Seed / raw ({rawTotal || 0})</option>
  </optgroup>
  <optgroup label="Promoted">
    <option value="metadata">Fetching bibliographic metadata ({metadataTotal || 0})</option>
    <option value="failed_enrichment">Failed enrichments ({failedEnrichmentTotal || 0})</option>
  </optgroup>
  <optgroup label="Downloaded">
    <option value="downloaded">Downloaded ({downloadedTotal || 0})</option>
    <option value="failed_download">Failed downloads ({failedDownloadTotal || 0})</option>
  </optgroup>
</select>
```

- [ ] **Step 2:** `itemStageLabel` raw case → `'Seed'` instead of `'Raw'`.

- [ ] **Step 3:** Build + commit — `git commit -m "Separate Seed / Promoted / Downloaded stage groups"`

### Task B3: Chevron affordance for collapsible rows

**Files:**
- Modify: `frontend/src/App.svelte:4246` (Show/Hide), `frontend/src/components/Corpus.svelte` corpus rows, `frontend/src/app.css`

- [ ] **Step 1:** Replace `Show`/`Hide` text (`App.svelte:4246`) with a chevron:

```html
<span class={`disclosure-chevron ${isExpanded ? 'open' : ''}`} aria-hidden="true">▸</span>
```

- [ ] **Step 2:** Add the same chevron as the first element of each corpus row title cell in `Corpus.svelte` (open when `selected`).

- [ ] **Step 3:** CSS in `app.css`:

```css
.disclosure-chevron { display: inline-block; transition: transform 0.15s ease; font-size: 0.8em; opacity: 0.6; }
.disclosure-chevron.open { transform: rotate(90deg); }
```

- [ ] **Step 4:** Build, Playwright snapshot, commit — `git commit -m "Chevron affordance for expandable seeds and corpus rows"`

### Task B4: PR B ship — push, PR, copilot-comments, merge.

---

## PR C — Search: no cap, user cap, sort

Branch: `feedback/search-cap-sort`. Files: `dl_lit_project/dl_lit/keyword_search.py`, `backend/scripts/keyword_search.py`, `backend/src/app.js`, `frontend/src/lib/api.js`, `frontend/src/App.svelte`, tests.

### Task C1: `search_openalex` supports `max_results=None` and `sort`

**Files:**
- Modify: `dl_lit_project/dl_lit/keyword_search.py:146-236`
- Test: `dl_lit_project/tests/test_keyword_search.py`

**Interfaces:**
- Produces: `search_openalex(query, max_results: int | None = 200, ..., sort: str | None = None)`. `sort` ∈ {None, 'relevance', 'cited_by_count', 'newest', 'oldest'} mapped to OpenAlex `sort=` values `relevance_score:desc`, `cited_by_count:desc`, `publication_date:desc`, `publication_date:asc`.

- [ ] **Step 1: Failing tests** (append to `test_keyword_search.py`, follow the file's existing mocking pattern for `_openalex_request`):

```python
def test_search_openalex_uncapped_pages_until_cursor_exhausted(monkeypatch):
    pages = [
        {"results": [{"id": f"W{i}"} for i in range(200)], "meta": {"next_cursor": "c2"}},
        {"results": [{"id": f"W{200+i}"} for i in range(50)], "meta": {"next_cursor": None}},
    ]
    calls = []
    def fake_request(endpoint, params, limiter):
        calls.append(dict(params))
        return pages[len(calls) - 1]
    monkeypatch.setattr(keyword_search, "_openalex_request", fake_request)
    results = keyword_search.search_openalex("x", max_results=None)
    assert len(results) == 250

def test_search_openalex_sort_param(monkeypatch):
    seen = {}
    def fake_request(endpoint, params, limiter):
        seen.update(params)
        return {"results": [], "meta": {"next_cursor": None}}
    monkeypatch.setattr(keyword_search, "_openalex_request", fake_request)
    keyword_search.search_openalex("x", sort="cited_by_count")
    assert seen["sort"] == "cited_by_count:desc"

def test_search_openalex_rejects_unknown_sort():
    with pytest.raises(ValueError):
        keyword_search.search_openalex("x", sort="banana")
```

- [ ] **Step 2:** Run: `pytest dl_lit_project/tests/test_keyword_search.py -q` → new tests FAIL (unexpected keyword `sort` / len mismatch).

- [ ] **Step 3: Implement** in `search_openalex`:

```python
SORT_OPTIONS = {
    "relevance": "relevance_score:desc",
    "cited_by_count": "cited_by_count:desc",
    "newest": "publication_date:desc",
    "oldest": "publication_date:asc",
}

def search_openalex(query, max_results: int | None = 200, ..., sort: str | None = None):
    ...
    if sort:
        sort_value = SORT_OPTIONS.get(str(sort).strip().lower())
        if sort_value is None:
            raise ValueError(f"Unknown sort option: {sort}")
        # relevance_score sort requires a search term
        if sort_value.startswith("relevance") and not openalex_query:
            sort_value = None
        if sort_value:
            params["sort"] = sort_value
    ...
    # in the pagination loop, guard the cap:
    if max_results is not None and len(results) >= max_results:
        return results
```

(`per-page` stays 200 — OpenAlex's max.)

- [ ] **Step 4:** `pytest dl_lit_project/tests/test_keyword_search.py -q` → PASS; `ruff check dl_lit_project/dl_lit/keyword_search.py`.

- [ ] **Step 5: Commit** — `git commit -m "search_openalex: optional cap + sort support"`

### Task C2: CLI bridge passes cap/sort through

**Files:**
- Modify: `backend/scripts/keyword_search.py:513` (arg), `:576`, `:593-596` (call site)
- Test: `dl_lit_project/tests/test_cli_keyword_search.py`

**Interfaces:**
- Produces: `--max-results 0` means uncapped; new `--sort {relevance,cited_by_count,newest,oldest}`.

- [ ] **Step 1: Failing test** (follow existing pattern in `test_cli_keyword_search.py` for arg parsing):

```python
def test_cli_max_results_zero_means_uncapped_and_sort_passthrough(...):
    # parse ['--query','x','--db-path',db,'--max-results','0','--sort','newest']
    # assert search_openalex called with max_results=None, sort='newest'
```

- [ ] **Step 2:** Run → FAIL. **Step 3: Implement:** `parser.add_argument('--max-results', type=int, default=200)`, `parser.add_argument('--sort', default=None, choices=['relevance','cited_by_count','newest','oldest'])`; at the call site `max_results=args.max_results if args.max_results and args.max_results > 0 else None, sort=args.sort`. **Step 4:** pytest PASS. **Step 5:** Commit.

### Task C3: Backend endpoint + frontend form

**Files:**
- Modify: `backend/src/app.js:4491-4496` , `frontend/src/lib/api.js:580+`, `frontend/src/App.svelte` (search form `:4049-4091`, `runSearch` at `:2891`)
- Test: `backend/tests/keyword-search.test.js`

**Interfaces:**
- Produces: `POST /api/keyword-search` body accepts `maxResults` (0/absent → uncapped) and `sort`.

- [ ] **Step 1: Failing Jest test** (follow existing spawn-mocking pattern in `keyword-search.test.js`): posting `{query:'x', maxResults: 0, sort:'newest'}` spawns the script with `--max-results 0 --sort newest`; posting without `maxResults` also yields `--max-results 0`.

- [ ] **Step 2:** Run `cd backend && npm test -- keyword-search` → FAIL.

- [ ] **Step 3: Implement** in `app.js`:

```js
const maxResults = coerceInt(req.body?.maxResults, 0); // 0 = uncapped (was default 200)
const sort = typeof req.body?.sort === 'string' && req.body.sort.trim() ? req.body.sort.trim() : '';
const args = ['--db-path', dbPath, '--max-results', String(maxResults), '--field', String(field)];
if (sort) args.push('--sort', sort);
```

- [ ] **Step 4:** Jest PASS. **Step 5:** Frontend: `runKeywordSearch` gains `maxResults`/`sort` params in the JSON body; App.svelte form adds after the Year fields:

```html
<label>
  <span>Max results</span>
  <input type="number" min="1" placeholder="No cap" bind:value={searchMaxResults} />
</label>
<label>
  <span>Sort</span>
  <select bind:value={searchSort}>
    <option value="relevance">Relevance (OpenAlex default)</option>
    <option value="cited_by_count">Most cited</option>
    <option value="newest">Newest</option>
    <option value="oldest">Oldest</option>
  </select>
</label>
```

with `let searchMaxResults = ''` / `let searchSort = 'relevance'` and `runSearch` passing `maxResults: Number(searchMaxResults) || 0, sort: searchSort`.

- [ ] **Step 6:** Build, live search against dev stack (small query with cap 5, then uncapped narrow query), commit, ship PR C (push, copilot-comments, merge).

---

## PR D — OpenAI-compatible LLM provider

Branch: `feedback/openai-compat`. Read `dl_lit_project/dl_lit/get_bib_pages.py`, `APIscraper_v2.py`, `new_dl.py` fully before starting; the exact Gemini call shapes must be catalogued first (this plan fixes the interface, the implementer maps call sites).

### Task D1: Provider module in `dl_lit`

**Files:**
- Create: `dl_lit_project/dl_lit/llm_provider.py`
- Test: `dl_lit_project/tests/test_llm_provider.py`

**Interfaces:**
- Produces:

```python
def get_provider() -> "LLMProvider":  # reads RAG_FEEDER_LLM_PROVIDER (default "gemini")
class LLMProvider(Protocol):
    def generate_text(self, prompt: str, model: str | None = None) -> str: ...
    def generate_from_pdf(self, pdf_path: str, prompt: str, model: str | None = None) -> str: ...
class GeminiProvider: ...   # wraps existing google-genai client + Files API
class OpenAICompatProvider: ...  # base_url=RAG_FEEDER_OPENAI_BASE_URL (default https://api.openai.com/v1), key=OPENAI_API_KEY, model=RAG_FEEDER_OPENAI_MODEL; PDF sent as base64 file content on chat.completions
```

- [ ] **Step 1: Failing tests:** `get_provider()` returns `GeminiProvider` by default; `RAG_FEEDER_LLM_PROVIDER=openai` returns `OpenAICompatProvider` with env-derived base_url/model; unknown value raises `ValueError`; missing `OPENAI_API_KEY` with provider=openai raises with a clear message; `OpenAICompatProvider.generate_text` posts to `{base_url}/chat/completions` (mock `requests`/httpx per repo convention — check `_openalex_request`'s HTTP lib and reuse).
- [ ] **Step 2:** pytest FAIL. **Step 3:** Implement (no new deps: use the HTTP lib already in the project; do NOT add the `openai` SDK). PDF path: read file, base64, content part `{"type": "file", "file": {"filename": ..., "file_data": "data:application/pdf;base64,..."}}`; on a 4xx complaining about file input, raise a clear error advising a vision-capable model (page-image fallback is out of scope — document it). **Step 4:** pytest PASS + ruff. **Step 5:** Commit.

### Task D2: Route Python extraction call sites through the provider

**Files:**
- Modify: `dl_lit_project/dl_lit/get_bib_pages.py:37-38,86-90`, `APIscraper_v2.py:27-28,240-257`, `new_dl.py:61,96-97`
- Test: existing `test_get_bib_pages.py`, `test_apiscraper_v2_args.py`, `test_gemini_timeout_retries.py` must stay green; add one test per file asserting the provider is consulted (monkeypatch `get_provider`).

- [ ] Replace direct `genai.Client(...)` construction with `llm_provider.get_provider()` calls at each catalogued call site, preserving retry/timeout behavior (`test_gemini_timeout_retries.py` guards this). Commit per file.

### Task D3: Node inline helpers + config surface

**Files:**
- Modify: `backend/src/app.js:2134,2183,4172-4429` (gemini helpers + model env plumbing)
- Modify: `.env.example` (or create), `README.md` env docs

- [ ] The inline-Python snippets in `app.js` switch on `RAG_FEEDER_LLM_PROVIDER` and import `dl_lit.llm_provider` instead of raw `google.genai`; document `RAG_FEEDER_LLM_PROVIDER`, `RAG_FEEDER_OPENAI_BASE_URL`, `OPENAI_API_KEY`, `RAG_FEEDER_OPENAI_MODEL` in README + `.env.example`; `docker-compose.yml` passes the new env vars through to backend + workers.
- [ ] Verify end-to-end: with provider=gemini (default) run a real PDF extraction on the dev stack (unchanged behavior); with provider=openai and a bogus key, confirm the extraction status surfaces the clear error (no silent Gemini fallback). Ship PR D.

---

## PR E — Visual restyle ("less AI-like")

Branch: `feedback/restyle`. Files: `frontend/src/app.css` (tokens + chrome only; no markup changes unless a class hook is missing).

### Task E1: Token pass

- [ ] Take "before" Playwright screenshots (desktop 1280, phone 390) of Workspace + Graph tabs.
- [ ] Rework `:root` tokens in `app.css:1-9+`: paper/ink palette (e.g. `--bg: #faf7f2; --ink: #1c1a17; --accent: #7a2e2e` oxblood or similar), radii 12px+ → 4px, replace any gradient/glow shadows with 1px rules (`border-bottom: 1px solid`), heading font stack → a serif display (`"Iowan Old Style", "Palatino Linotype", Georgia, serif`), body stays system sans; denser table row padding.
- [ ] Sweep component classes that hardcode the old look (`card`, `pill`, `tag`, `tab-hero`, buttons): flatten pills to bordered text chips, cards to ruled sections.

### Task E2: Review loop

- [ ] "After" screenshots at both breakpoints; ask Gemini CLI for a design critique (`gemini "critique @dashboard-after.png vs @dashboard-before.png — goal: editorial/academic, less generic-AI-tool"`); iterate once on its feedback.
- [ ] Check dark-on-light contrast (WCAG AA) for the new tokens; verify no layout breakage at 390/768/1024 (the responsive tiers from PR #53).
- [ ] Ship PR E. Present before/after screenshots to the user in the final report.

---

## Self-review notes

- Spec coverage: items 1(E), 2-5/7/10-12/14-16/20(A), 8-9/19(B), 17-18(C), 13(D), 6+18-answer(spec "Answers" section, delivered in final report) — all 20 covered.
- PR D Task D2/D3 are deliberately catalog-first (call shapes differ per file); the interfaces in D1 are the fixed contract.
- Type consistency: `max_results: int | None`, `--max-results 0 ⇒ None`, endpoint default 0 — consistent through C1→C3.
