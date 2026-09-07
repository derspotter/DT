# Jochen Round 2 — Phases 1 & 2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the small round-2 items (Refs column, snowball label, sticky bar) and the OpenAlex quota display as two independently mergeable PRs on top of `jochen-todos`.

**Architecture:** Phase 1 is additive UI plus two small backend field additions (OpenAlex select fields, a `seed_kind` on seed sources). Phase 2 bridges the per-process Python rate limiter and the long-lived Node backend with a JSON snapshot file written after every OpenAlex response and read by one new endpoint; the frontend polls it through the existing live-refresh cycle.

**Tech Stack:** Svelte 4 syntax in `frontend/src/App.svelte` (single large component), Node/Express in `backend/src/app.js` (`createApp`), better-sqlite3 in `backend/src/seed.js`, Python 3 package `dl_lit` under `dl_lit_project/`, Jest (`cd backend && npm test`), pytest (`cd dl_lit_project && python -m pytest tests`), Playwright (`cd frontend && npm run test:e2e`, needs the live stack and `E2E_USERNAME`/`E2E_PASSWORD`).

**Spec:** `docs/superpowers/specs/2026-09-03-jochen-round-2-design.md` — sections "Phase 1 — Small items" and "Phase 2 — OpenAlex quota".

## Global Constraints

- Branch: `jochen-todos` (PR #65 is open against master; these land as follow-up PRs on the same branch or on branches off it — ask Jay which before the first commit if unclear; default is commits on `jochen-todos`).
- Line anchors below were verified at commit `74afd4c` on 2026-09-03. If a line has moved, match on the quoted code, not the number.
- Item 5 (per-item promote) needs **no code**: it is commit `b0dd52a`. Mention it in the Phase 1 PR description.
- Refs / Cited by columns are **seed table only** this round (`tables: ['seed']`). Corpus table is out of scope.
- The snowball tag text is exactly `Snowball items`. The sticky-bar collapse action is labelled exactly `Collapse seed` (only one seed can be open at a time).
- Quota pill copy, verbatim from the spec: `OpenAlex budget: 87,412 of 100,000 left · resets in 5 h 12 min`; no file → `OpenAlex budget: unknown until the first request`; no key → `OpenAlex: no API key — daily budget not reported`.
- Snapshot file path: env `RAG_FEEDER_OPENALEX_QUOTA_PATH`, default `<directory of the SQLite DB>/openalex_quota.json`. Stale after 24 h.
- Never let the quota write raise: log a warning and continue.
- Commit messages end with the Co-Authored-By / Claude-Session trailer used on this branch (see `git log -1 --format=%B`).
- Backend tests: `cd backend && npm test`. Python tests: `cd dl_lit_project && python -m pytest tests -q`. E2e: `cd frontend && npm run test:e2e`.

---

## Phase 1 — Small items

### Task 1: Refs and Cited-by counts on search candidates (backend)

**Files:**
- Modify: `dl_lit_project/dl_lit/keyword_search.py:1002-1004` (the `select` string in `search_openalex`)
- Modify: `backend/scripts/keyword_search.py:130-134` (`OPENALEX_SELECT`)
- Modify: `backend/src/seed.js:489-533` (`normalizeSearchCandidate`) and `:421-451` (`normalizePdfCandidate`)
- Test: `backend/tests/seed-state.test.js`, `dl_lit_project/tests/test_keyword_search.py`

**Interfaces:**
- Produces: candidate objects gain `refs_count: number | null` and `cited_by_count: number | null`. Task 2 renders them; Phase 3 sorts on them in SQL via `json_extract(raw_json, '$.referenced_works_count')`.

- [ ] **Step 1: Write the failing backend test**

Append to `backend/tests/seed-state.test.js` inside the existing `describe('seed candidate venue fallback', …)` block, or as a new `describe` after it (reuse `createSearchSeedDb` and `insertSearchResult` defined at lines 131-158):

```js
describe('seed candidate reference counts', () => {
  let db

  afterEach(() => {
    db?.close()
    db = null
  })

  test('exposes referenced_works_count and cited_by_count from raw_json', () => {
    db = createSearchSeedDb()
    insertSearchResult(db, { referenced_works_count: 42, cited_by_count: 1234 })
    const [candidate] = listSeedCandidates(db, 130, 'search', '7')
    expect(candidate.refs_count).toBe(42)
    expect(candidate.cited_by_count).toBe(1234)
  })

  test('is null when the run predates the select change', () => {
    db = createSearchSeedDb()
    insertSearchResult(db, { display_name: 'Old run' })
    const [candidate] = listSeedCandidates(db, 130, 'search', '7')
    expect(candidate.refs_count).toBeNull()
    expect(candidate.cited_by_count).toBeNull()
  })
})
```

- [ ] **Step 2: Run it and confirm it fails**

Run: `cd backend && npm test -- seed-state`
Expected: the two new tests FAIL with `expected 42, received undefined`.

- [ ] **Step 3: Implement the normalizer fields**

In `backend/src/seed.js`, add a helper next to `normalizeYear` (line 53):

```js
function normalizeCount(value) {
  if (value === null || value === undefined || value === '') return null
  const parsed = Number(value)
  return Number.isFinite(parsed) ? Math.max(0, Math.trunc(parsed)) : null
}
```

In `normalizeSearchCandidate`, inside the `candidate = { … }` literal after `type: raw?.type || null,`:

```js
    refs_count: normalizeCount(raw?.referenced_works_count),
    cited_by_count: normalizeCount(raw?.cited_by_count),
```

In `normalizePdfCandidate`, add to its candidate literal (document items are not on OpenAlex until enriched):

```js
    refs_count: null,
    cited_by_count: null,
```

- [ ] **Step 4: Run the backend tests**

Run: `cd backend && npm test -- seed-state`
Expected: PASS.

- [ ] **Step 5: Write the failing Python test**

Append to `dl_lit_project/tests/test_keyword_search.py`:

```python
def test_search_select_includes_reference_counts(monkeypatch):
    captured = {}

    def fake_request(endpoint, params, rate_limiter, retries=3):
        captured.update(params)
        return {"results": [], "meta": {"next_cursor": None}}

    monkeypatch.setattr(keyword_search, "_openalex_request", fake_request)
    keyword_search.search_openalex(query="labour", max_results=5)
    select_fields = captured["select"].split(",")
    assert "referenced_works_count" in select_fields
    assert "cited_by_count" in select_fields
```

- [ ] **Step 6: Run it and confirm it fails**

Run: `cd dl_lit_project && python -m pytest tests/test_keyword_search.py -q -k reference_counts`
Expected: FAIL on the first assert.

- [ ] **Step 7: Add the fields to both select lists**

`dl_lit_project/dl_lit/keyword_search.py` line 1003, the `"select"` value becomes:

```python
        "select": "id,doi,display_name,authorships,publication_year,type,abstract_inverted_index,keywords,primary_location,open_access,biblio,referenced_works_count,cited_by_count",
```

`backend/scripts/keyword_search.py` lines 130-134:

```python
OPENALEX_SELECT = (
    'id,doi,display_name,authorships,publication_year,type,'
    'abstract_inverted_index,keywords,primary_location,open_access,biblio,'
    'referenced_works,cited_by_api_url,referenced_works_count,cited_by_count'
)
```

Also in `backend/scripts/keyword_search.py` `_to_openalex_like` (line 218-232), carry the two fields through so expansion results keep them:

```python
        'referenced_works_count': work.get('referenced_works_count'),
        'cited_by_count': work.get('cited_by_count'),
```

- [ ] **Step 8: Run the Python tests**

Run: `cd dl_lit_project && python -m pytest tests -q`
Expected: all PASS.

- [ ] **Step 9: Commit**

```bash
git add backend/src/seed.js backend/tests/seed-state.test.js dl_lit_project/dl_lit/keyword_search.py backend/scripts/keyword_search.py dl_lit_project/tests/test_keyword_search.py
git commit -m "Carry OpenAlex reference and citation counts onto seed candidates"
```

---

### Task 2: Refs and Cited-by columns in the seed table (frontend)

**Files:**
- Modify: `frontend/src/lib/tableColumns.js:11-27` (`COLUMN_DEFS`)
- Modify: `frontend/src/App.svelte:2700-2708` (`SEED_SORT_ACCESSORS`), `:2743-2759` (`seedCellText`), `:2778-2797` (`sortSeedCandidates`)
- Test: `frontend/tests/app.spec.ts`

**Interfaces:**
- Consumes: `candidate.refs_count`, `candidate.cited_by_count` from Task 1.
- Produces: column keys `refs` and `cited_by` (Phase 3 uses the same keys as server-side sort names).

- [ ] **Step 1: Add the column definitions**

In `frontend/src/lib/tableColumns.js`, after the `year` entry:

```js
  // Round 2 item 7: how many works the item cites on OpenAlex — what a
  // downstream promote pulls in. Seed table only this round.
  { key: 'refs', label: 'Refs', width: '70px', sortable: true, defaultVisible: true, tables: ['seed'],
    hint: 'Works this item cites on OpenAlex — what a downstream promote pulls in' },
  { key: 'cited_by', label: 'Cited by', width: '80px', sortable: true, defaultVisible: false, tables: ['seed'],
    hint: 'How often OpenAlex has seen this item cited' },
```

- [ ] **Step 2: Render and sort the cells**

`App.svelte` `seedCellText` (line 2743): add before `default:`

```js
      case 'refs': return candidate?.refs_count ?? ''
      case 'cited_by': return candidate?.cited_by_count ?? ''
```

`SEED_SORT_ACCESSORS` (line 2700): add

```js
    refs: (candidate) => candidate?.refs_count,
    cited_by: (candidate) => candidate?.cited_by_count,
```

`sortSeedCandidates` (line 2794): make the numeric branch cover the new keys:

```js
      if (current.column === 'year' || current.column === 'refs' || current.column === 'cited_by') {
        return (Number(a) - Number(b)) * factor
      }
```

Blanks already sort last in both directions (lines 2788-2792), which is what the spec asks for.

- [ ] **Step 3: Show the hint on the header**

Find the seed header loop at `App.svelte:4698` (`{#each seedActiveColumns as column (column.key)}`). The header cell renders `column.label`; add `title={column.hint || ''}` to the header element inside that loop (the `<button>` or `<span>` that shows the label — match on `seedSortIndicator(source, column.key`). Verify with `grep -n "seedSortIndicator(source, column.key" frontend/src/App.svelte`.

- [ ] **Step 4: Add the e2e assertion**

In `frontend/tests/app.spec.ts`, add a test next to the existing seed-table tests (search the file for `Corpus` column header expectations to find the right describe; the helper `ensureSignedIn(page, request)` is at the top of the file):

```ts
test('seed table exposes a Refs column', async ({ page, request }) => {
  await ensureSignedIn(page, request)
  const firstSeed = page.locator('.seed-source__summary').first()
  if (!(await firstSeed.isVisible().catch(() => false))) {
    test.skip(true, 'No seeds in this corpus')
  }
  await firstSeed.click()
  await expect(page.getByText('Refs', { exact: true }).first()).toBeVisible()
})
```

- [ ] **Step 5: Run the e2e suite**

Run: `cd frontend && npm run test:e2e -- app.spec.ts`
Expected: PASS (or the new test skipped when the corpus has no seeds).

- [ ] **Step 6: Commit**

```bash
git add frontend/src/lib/tableColumns.js frontend/src/App.svelte frontend/tests/app.spec.ts
git commit -m "Seed table: Refs and Cited by columns"
```

---

### Task 3: Snowball seed kind and tag

**Files:**
- Modify: `backend/src/seed.js:700-798` (`listSeedSources`), `:391-401` (`formatSearchSubtitle` untouched, referenced for context)
- Modify: `frontend/src/App.svelte:4622` (the `<span class="tag …">` in the seed row heading)
- Modify: `frontend/src/app.css:2130-2148` (tag colours)
- Test: `backend/tests/seed-state.test.js`

**Interfaces:**
- Produces: `source.seed_kind: 'pdf' | 'search' | 'snowball'` and, for snowball, `source.snowball: { direction, of_title, of_openalex_id }`. Phase 3 keeps `source_type` unchanged (`'search'`), so promote/dismiss routes need no change.

- [ ] **Step 1: Write the failing backend test**

In `backend/tests/seed-state.test.js`, the `describe('seed candidate text filter', …)` block (line 197) has a local fixture `seedTwoEntries()` (line 205) that assigns `db` and creates `search_runs (id, query, filters_json, created_at)` plus `search_results`. Add this test inside that describe, after the existing filter tests:

```js
  test('marks expansion runs as snowball seeds', () => {
    seedTwoEntries() // assigns `db` with two pdf entries and empty search tables
    db.prepare(
      `INSERT INTO search_runs (id, query, filters_json) VALUES (9, 'Downstream of «Bazaar Economies»', ?)`
    ).run(JSON.stringify({
      expansion_direction: 'downstream',
      expansion_of_openalex_id: 'https://openalex.org/W1',
      expansion_of_title: 'Bazaar Economies',
    }))
    db.prepare(`INSERT INTO search_run_corpora (search_run_id, corpus_id) VALUES (9, 130)`).run()
    db.prepare(
      `INSERT INTO search_results (id, search_run_id, title, year, raw_json) VALUES (50, 9, 'Cited Work', '2010', '{}')`
    ).run()
    db.prepare(
      `INSERT INTO search_runs (id, query, filters_json) VALUES (10, 'plain query', ?)`
    ).run(JSON.stringify({ mode: 'query' }))
    db.prepare(`INSERT INTO search_run_corpora (search_run_id, corpus_id) VALUES (10, 130)`).run()
    db.prepare(
      `INSERT INTO search_results (id, search_run_id, title, year, raw_json) VALUES (51, 10, 'Plain Work', '2011', '{}')`
    ).run()

    const sources = listSeedSources(db, 130)
    const snowball = sources.find((s) => s.source_key === '9')
    const plain = sources.find((s) => s.source_key === '10')
    const pdf = sources.find((s) => s.source_type === 'pdf')
    expect(snowball.seed_kind).toBe('snowball')
    expect(snowball.snowball).toEqual({
      direction: 'downstream',
      of_title: 'Bazaar Economies',
      of_openalex_id: 'https://openalex.org/W1',
    })
    expect(plain.seed_kind).toBe('search')
    expect(plain.snowball).toBeUndefined()
    expect(pdf.seed_kind).toBe('pdf')
  })
```

- [ ] **Step 2: Run it and confirm it fails**

Run: `cd backend && npm test -- seed-state`
Expected: FAIL with `expected 'snowball', received undefined`.

- [ ] **Step 3: Implement in `listSeedSources`**

In the `pdfSources.forEach` push (line 758-770), add `seed_kind: 'pdf',` after `source_type: 'pdf',`.

In the `searchSources.forEach` push (line 777-796), compute the kind first:

```js
    const filters = parseJson(row.filters_json, {}) || {}
    const direction = String(filters.expansion_direction || '').trim().toLowerCase()
    const isSnowball = direction === 'downstream' || direction === 'upstream'
    sources.push({
      id: buildSourceId('search', sourceKey),
      source_type: 'search',
      seed_kind: isSnowball ? 'snowball' : 'search',
      ...(isSnowball
        ? {
          snowball: {
            direction,
            of_title: filters.expansion_of_title || null,
            of_openalex_id: filters.expansion_of_openalex_id || null,
          },
        }
        : {}),
      source_key: sourceKey,
      label: row.query || `Search #${sourceKey}`,
      subtitle: formatSearchSubtitle(row),
      created_at: row.created_at,
      candidate_count: candidates.length,
      state_counts: summarizeStates(candidates),
      removable: true,
      meta: { query: row.query, filters },
    })
```

- [ ] **Step 4: Run the backend tests**

Run: `cd backend && npm test`
Expected: all PASS.

- [ ] **Step 5: Render the tag**

`App.svelte:4622` currently:

```svelte
<span class={`tag ${source.source_type === 'pdf' ? 'pending' : 'queued'}`}>{source.source_type === 'pdf' ? 'Document items' : 'Search items'}</span>
```

Replace with:

```svelte
{#if source.seed_kind === 'snowball'}
  <span
    class="tag snowball"
    title={`${source.snowball?.direction === 'upstream' ? 'Upstream' : 'Downstream'} of «${source.snowball?.of_title || 'untitled work'}»`}
  >Snowball items</span>
{:else if source.source_type === 'pdf'}
  <span class="tag pending">Document items</span>
{:else}
  <span class="tag queued">Search items</span>
{/if}
```

- [ ] **Step 6: Add the tag colour**

`frontend/src/app.css` after `.tag.queued` (line 2133):

```css
.tag.snowball {
  background: #ecfdf5;
  color: #047857;
}
```

- [ ] **Step 7: Verify visually**

Run the dev stack, promote one item with "Make new seed" and downstream on, and confirm the resulting seed shows the green `Snowball items` tag with the parent title on hover. Take a screenshot into the scratchpad if working with a subagent.

- [ ] **Step 8: Commit**

```bash
git add backend/src/seed.js backend/tests/seed-state.test.js frontend/src/App.svelte frontend/src/app.css
git commit -m "Label expansion runs as Snowball items in the seed list"
```

---

### Task 4: Sticky workspace bar

**Files:**
- Modify: `frontend/src/App.svelte:4320-4326` (workspace wrapper start), `:4500-4502` (section 2 header), `:4822-4828` (section 3 column), `:2979-3000` (`toggleSeedSource` / expanded id handling for the collapse action)
- Modify: `frontend/src/app.css` (new `.workspace-sticky-bar` block; `scroll-margin-top` on the three anchors)
- Test: `frontend/tests/navigation.spec.ts`

**Interfaces:**
- Consumes: `seedSources.length`, `corpusTotal` (already in App state; `corpusTotal` is passed to `<Corpus>` at line 4830), `expandedSeedSourceId`.

- [ ] **Step 1: Add anchors to the three sections**

- Section 1 card at line 4321: `<div class="card seed-corpus-toolbar" id="section-find">`.
- Section 2 panel: find the card wrapper that contains the `2. Seed` header (search upward from line 4500 for the nearest `<div class="card`) and give it `id="section-seed"`.
- Section 3: the `<div class="seed-corpus-column seed-corpus-column--corpus">` at line 4826 gets `id="section-corpus"`.

- [ ] **Step 2: Add the bar markup**

Immediately after `<div class="seed-corpus-workspace">` (line 4320):

```svelte
<nav class="workspace-sticky-bar" aria-label="Workspace sections" data-testid="workspace-sticky-bar">
  <div class="workspace-sticky-bar__links">
    <button type="button" class="workspace-sticky-bar__link" on:click={() => jumpToSection('section-find')}>1 Find items</button>
    <button type="button" class="workspace-sticky-bar__link" on:click={() => jumpToSection('section-seed')}>2 Seed <span class="muted small">({seedSources.length})</span></button>
    <button type="button" class="workspace-sticky-bar__link" on:click={() => jumpToSection('section-corpus')}>3 Corpus <span class="muted small">({corpusTotal})</span></button>
  </div>
  <div class="workspace-sticky-bar__actions">
    {#if expandedSeedSourceId}
      <button type="button" class="secondary" on:click={collapseExpandedSeed}>Collapse seed</button>
    {/if}
    <button type="button" class="secondary" on:click={() => jumpToSection('section-find')}>Top</button>
  </div>
</nav>
```

- [ ] **Step 3: Add the two handlers**

In the `<script>` block near `toggleSeedSource` (line 2979):

```js
  function jumpToSection(id) {
    const el = document.getElementById(id)
    if (!el) return
    const behavior = prefersReducedMotion ? 'auto' : 'smooth'
    el.scrollIntoView({ behavior, block: 'start' })
  }

  function collapseExpandedSeed() {
    expandedSeedSourceId = ''
    jumpToSection('section-seed')
  }
```

`prefersReducedMotion` already exists in the component (the media query listener at line ~4150 sets it; verify the variable name with `grep -n "reducedMotion" frontend/src/App.svelte` and use the boolean that is actually maintained).

- [ ] **Step 4: Style it**

Append to `frontend/src/app.css`:

```css
.workspace-sticky-bar {
  position: sticky;
  top: 0;
  z-index: 20;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 8px 14px;
  margin-bottom: 12px;
  border-radius: var(--radius-lg);
  background: var(--surface);
  box-shadow: var(--shadow);
}

.workspace-sticky-bar__links {
  display: flex;
  gap: 4px;
  overflow-x: auto;
  white-space: nowrap;
}

.workspace-sticky-bar__link {
  background: none;
  border: 0;
  padding: 6px 10px;
  border-radius: 6px;
  font: inherit;
  color: inherit;
  cursor: pointer;
}

.workspace-sticky-bar__link:hover {
  background: rgba(0, 0, 0, 0.05);
}

.workspace-sticky-bar__actions {
  display: flex;
  gap: 8px;
  flex-shrink: 0;
}

#section-find,
#section-seed,
#section-corpus {
  scroll-margin-top: 64px;
}

@media (max-width: 640px) {
  .workspace-sticky-bar {
    padding: 6px 10px;
  }
  .workspace-sticky-bar__actions button {
    padding: 4px 8px;
  }
}
```

Check how the existing `.secondary` button class looks in the header so the two action buttons match; if `.secondary` is table-scoped, use the button class used by "Show more" in Corpus.svelte line 442.

- [ ] **Step 5: E2e**

In `frontend/tests/navigation.spec.ts` add:

```ts
test('sticky bar jumps to the corpus section', async ({ page, request }) => {
  await ensureSignedIn(page, request)
  const bar = page.getByTestId('workspace-sticky-bar')
  await expect(bar).toBeVisible()
  await bar.getByRole('button', { name: /3 Corpus/ }).click()
  const corpus = page.locator('#section-corpus')
  await expect(corpus).toBeInViewport()
  await expect(bar).toBeInViewport()
})
```

If `navigation.spec.ts` does not have `ensureSignedIn`, copy the helper from `app.spec.ts` lines 5-29 into it (the two files do not share a module today).

- [ ] **Step 6: Run the e2e suite**

Run: `cd frontend && npm run test:e2e -- navigation.spec.ts`
Expected: PASS. Also open the app at 390 px width and confirm the bar stays one line with the links scrolling horizontally.

- [ ] **Step 7: Commit**

```bash
git add frontend/src/App.svelte frontend/src/app.css frontend/tests/navigation.spec.ts
git commit -m "Sticky workspace bar with section jumps and Collapse seed"
```

---

### Task 5: Phase 1 PR

- [ ] **Step 1: Full test run**

Run: `cd backend && npm test` then `cd dl_lit_project && python -m pytest tests -q` then `cd frontend && npm run test:e2e`.
Expected: all green; record the counts in the PR body.

- [ ] **Step 2: Open the PR**

Base: whatever Jay chose in Global Constraints (default `master` after #65 merges, else `jochen-todos`). Title: `Round 2 small items: Refs column, Snowball label, sticky bar`. Body lists items 7, 10, 11 and states that item 5 is already in `b0dd52a`. End with the standard footer used on PR #65.

---

## Phase 2 — OpenAlex quota

### Task 6: Quota snapshot writer (Python)

**Files:**
- Modify: `dl_lit_project/dl_lit/utils.py` (new functions near `get_openalex_api_key` at line 197; call site in `openalex_request_json` after line 377 `response = http.get(…)`)
- Test: `dl_lit_project/tests/test_openalex_quota.py` (new)

**Interfaces:**
- Produces: `openalex_quota_path() -> Path`, `record_openalex_quota(response) -> dict | None`. File schema (read by Task 7):

  ```json
  {"limit": 100000, "remaining": 87412, "credits_used": 1, "reset_seconds": 18720,
   "observed_at": "2026-09-03T16:30:12Z", "reset_at": "2026-09-03T21:42:12Z",
   "api_key_present": true}
  ```

  Without the headers: `{"api_key_present": false, "observed_at": "…"}` (other keys absent).

- [ ] **Step 1: Write the failing tests**

Create `dl_lit_project/tests/test_openalex_quota.py`:

```python
import json
from datetime import datetime, timezone

from dl_lit import utils


class _Resp:
    def __init__(self, headers):
        self.headers = headers


def test_quota_path_defaults_next_to_db(monkeypatch, tmp_path):
    monkeypatch.delenv("RAG_FEEDER_OPENALEX_QUOTA_PATH", raising=False)
    monkeypatch.setenv("RAG_FEEDER_DB_PATH", str(tmp_path / "data" / "literature.db"))
    assert utils.openalex_quota_path() == tmp_path / "data" / "openalex_quota.json"


def test_quota_path_env_override(monkeypatch, tmp_path):
    monkeypatch.setenv("RAG_FEEDER_OPENALEX_QUOTA_PATH", str(tmp_path / "q.json"))
    assert utils.openalex_quota_path() == tmp_path / "q.json"


def test_record_writes_snapshot(monkeypatch, tmp_path):
    target = tmp_path / "q.json"
    monkeypatch.setenv("RAG_FEEDER_OPENALEX_QUOTA_PATH", str(target))
    snapshot = utils.record_openalex_quota(_Resp({
        "X-RateLimit-Limit": "100000",
        "X-RateLimit-Remaining": "87412",
        "X-RateLimit-Credits-Used": "1",
        "X-RateLimit-Reset": "18720",
    }))
    assert snapshot["limit"] == 100000
    assert snapshot["remaining"] == 87412
    assert snapshot["credits_used"] == 1
    assert snapshot["reset_seconds"] == 18720
    assert snapshot["api_key_present"] is True
    on_disk = json.loads(target.read_text())
    assert on_disk == snapshot
    observed = datetime.fromisoformat(on_disk["observed_at"].replace("Z", "+00:00"))
    reset_at = datetime.fromisoformat(on_disk["reset_at"].replace("Z", "+00:00"))
    assert (reset_at - observed).total_seconds() == 18720
    assert observed.tzinfo == timezone.utc


def test_record_without_headers_marks_no_key(monkeypatch, tmp_path):
    target = tmp_path / "q.json"
    monkeypatch.setenv("RAG_FEEDER_OPENALEX_QUOTA_PATH", str(target))
    snapshot = utils.record_openalex_quota(_Resp({}))
    assert snapshot == {"api_key_present": False, "observed_at": snapshot["observed_at"]}
    assert json.loads(target.read_text())["api_key_present"] is False


def test_record_never_raises(monkeypatch, tmp_path):
    # Directory that cannot be created: a file where the parent should be.
    blocker = tmp_path / "blocker"
    blocker.write_text("x")
    monkeypatch.setenv("RAG_FEEDER_OPENALEX_QUOTA_PATH", str(blocker / "q.json"))
    assert utils.record_openalex_quota(_Resp({"X-RateLimit-Limit": "5"})) is None
```

- [ ] **Step 2: Run them and confirm they fail**

Run: `cd dl_lit_project && python -m pytest tests/test_openalex_quota.py -q`
Expected: FAIL with `AttributeError: module 'dl_lit.utils' has no attribute 'openalex_quota_path'`.

- [ ] **Step 3: Implement**

In `dl_lit_project/dl_lit/utils.py`, after `get_openalex_mailto` (line 205). `json`, `os`, `time` may already be imported at the top; add `from datetime import datetime, timedelta, timezone` and `from pathlib import Path` and `import tempfile` if missing.

```python
OPENALEX_QUOTA_FILENAME = 'openalex_quota.json'


def openalex_quota_path() -> Path:
    override = _env_str('RAG_FEEDER_OPENALEX_QUOTA_PATH')
    if override:
        return Path(override)
    db_path = _env_str('RAG_FEEDER_DB_PATH')
    if db_path:
        return Path(db_path).resolve().parent / OPENALEX_QUOTA_FILENAME
    return Path(__file__).resolve().parents[1] / 'data' / OPENALEX_QUOTA_FILENAME


def _header_int(headers, name: str) -> int | None:
    try:
        raw = headers.get(name)
    except Exception:
        return None
    if raw is None:
        return None
    try:
        return int(str(raw).strip())
    except (TypeError, ValueError):
        return None


def record_openalex_quota(response) -> dict | None:
    """Persist the daily-budget headers OpenAlex sends with keyed requests.

    The rate limiter lives per Python process, so this file is how the Node
    backend learns where the budget stands. Never raises: a failed write is
    logged and ignored because it must not sink the request that produced it.
    """
    now = datetime.now(timezone.utc).replace(microsecond=0)
    observed_at = now.isoformat().replace('+00:00', 'Z')
    headers = getattr(response, 'headers', None) or {}
    limit = _header_int(headers, 'X-RateLimit-Limit')
    if limit is None:
        snapshot = {'api_key_present': False, 'observed_at': observed_at}
    else:
        reset_seconds = _header_int(headers, 'X-RateLimit-Reset')
        snapshot = {
            'limit': limit,
            'remaining': _header_int(headers, 'X-RateLimit-Remaining'),
            'credits_used': _header_int(headers, 'X-RateLimit-Credits-Used'),
            'reset_seconds': reset_seconds,
            'observed_at': observed_at,
            'reset_at': (
                (now + timedelta(seconds=reset_seconds)).isoformat().replace('+00:00', 'Z')
                if reset_seconds is not None else None
            ),
            'api_key_present': True,
        }

    target = openalex_quota_path()
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(prefix='.openalex_quota-', dir=str(target.parent))
        with os.fdopen(fd, 'w', encoding='utf-8') as handle:
            json.dump(snapshot, handle)
        os.replace(tmp_name, target)
    except Exception as exc:  # noqa: BLE001 - deliberately broad, see docstring
        print(f'[OpenAlex WARN] Could not write quota snapshot to {target}: {exc}', file=sys.stderr)
        return None
    return snapshot
```

Add `import sys` if the module does not import it. Then wire the call site in `openalex_request_json`, directly after the `response = http.get(…)` call (line 372-377) and before `_apply_openalex_response_limits`:

```python
            record_openalex_quota(response)
```

- [ ] **Step 4: Run the Python tests**

Run: `cd dl_lit_project && python -m pytest tests -q`
Expected: all PASS.

- [ ] **Step 5: Smoke against OpenAlex**

Run from the repo root with the real env loaded (`set -a; . ./.env; set +a`):

```bash
cd dl_lit_project && python -c "from dl_lit.utils import openalex_request_json, openalex_quota_path; openalex_request_json(endpoint='works', params={'per-page': 1}); print(openalex_quota_path().read_text())"
```

Expected: a JSON line with `api_key_present: true` and a `limit` when `OPENALEX_API_KEY` is set, or `api_key_present: false` without it.

- [ ] **Step 6: Commit**

```bash
git add dl_lit_project/dl_lit/utils.py dl_lit_project/tests/test_openalex_quota.py
git commit -m "Persist OpenAlex daily-budget headers to a quota snapshot file"
```

---

### Task 7: Quota endpoint (backend) and API client

**Files:**
- Modify: `backend/src/app.js` (new constant near `DB_PATH`; new route next to `app.get('/api/recursion-config'` at line 5288)
- Modify: `frontend/src/lib/api.js` (new `fetchOpenAlexQuota` after `fetchRecursionConfig`, line 654-662)
- Test: `backend/tests/openalex-quota.test.js` (new)

**Interfaces:**
- Consumes: the file schema from Task 6.
- Produces: `GET /api/openalex/quota` →
  `{ available: false }` when no file;
  otherwise the snapshot plus `available: true`, `stale: boolean` (observed_at older than 24 h), `reset_in_seconds: number | null` (computed from `reset_at` at read time so the UI never does date math).
  Frontend: `fetchOpenAlexQuota(): Promise<typeof body>`.

- [ ] **Step 1: Write the failing test**

Create `backend/tests/openalex-quota.test.js`:

```js
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import request from 'supertest'
import { createApp } from '../src/app.js'

function tmpQuotaPath() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'quota-'))
  return path.join(dir, 'openalex_quota.json')
}

describe('GET /api/openalex/quota', () => {
  let app
  let quotaPath

  beforeEach(() => {
    quotaPath = tmpQuotaPath()
    process.env.RAG_FEEDER_STUB = '1'
    process.env.RAG_FEEDER_OPENALEX_QUOTA_PATH = quotaPath
    app = createApp({ broadcast: () => {} })
  })

  afterEach(() => {
    delete process.env.RAG_FEEDER_STUB
    delete process.env.RAG_FEEDER_OPENALEX_QUOTA_PATH
  })

  test('reports unavailable when no snapshot exists', async () => {
    const res = await request(app).get('/api/openalex/quota')
    expect(res.status).toBe(200)
    expect(res.body).toEqual({ available: false })
  })

  test('returns the snapshot with a live reset countdown', async () => {
    const resetAt = new Date(Date.now() + 3600 * 1000)
    fs.writeFileSync(quotaPath, JSON.stringify({
      limit: 100000, remaining: 87412, credits_used: 1, reset_seconds: 3600,
      observed_at: new Date().toISOString(), reset_at: resetAt.toISOString(),
      api_key_present: true,
    }))
    const res = await request(app).get('/api/openalex/quota')
    expect(res.status).toBe(200)
    expect(res.body.available).toBe(true)
    expect(res.body.stale).toBe(false)
    expect(res.body.remaining).toBe(87412)
    expect(res.body.reset_in_seconds).toBeGreaterThan(3500)
    expect(res.body.reset_in_seconds).toBeLessThanOrEqual(3600)
  })

  test('flags a snapshot older than 24 hours as stale', async () => {
    fs.writeFileSync(quotaPath, JSON.stringify({
      limit: 100000, remaining: 5, observed_at: new Date(Date.now() - 25 * 3600 * 1000).toISOString(),
      reset_at: new Date(Date.now() - 20 * 3600 * 1000).toISOString(), api_key_present: true,
    }))
    const res = await request(app).get('/api/openalex/quota')
    expect(res.body.stale).toBe(true)
    expect(res.body.reset_in_seconds).toBe(0)
  })

  test('passes through the no-key marker', async () => {
    fs.writeFileSync(quotaPath, JSON.stringify({ api_key_present: false, observed_at: new Date().toISOString() }))
    const res = await request(app).get('/api/openalex/quota')
    expect(res.body).toMatchObject({ available: true, api_key_present: false, stale: false })
  })

  test('treats an unreadable file as unavailable', async () => {
    fs.writeFileSync(quotaPath, '{not json')
    const res = await request(app).get('/api/openalex/quota')
    expect(res.status).toBe(200)
    expect(res.body).toEqual({ available: false })
  })
})
```

Check how the other route tests authenticate: `keyword-search.test.js` calls the route without a token under `RAG_FEEDER_STUB=1`, so `requireAuthMiddleware` must be permissive in stub mode; if the test gets a 401, look at `auth.test.js` for the token helper and add the header.

- [ ] **Step 2: Run it and confirm it fails**

Run: `cd backend && npm test -- openalex-quota`
Expected: FAIL with 404s.

- [ ] **Step 3: Implement the route**

Near the `DB_PATH` constant in `app.js` (grep `const DB_PATH`):

```js
const OPENALEX_QUOTA_STALE_MS = 24 * 60 * 60 * 1000;

function openalexQuotaPath() {
  // Resolved per request so tests (and admins) can point it elsewhere via env.
  const override = String(process.env.RAG_FEEDER_OPENALEX_QUOTA_PATH || '').trim();
  return override || path.join(path.dirname(DB_PATH), 'openalex_quota.json');
}

function readOpenAlexQuota() {
  let parsed;
  try {
    parsed = JSON.parse(fs.readFileSync(openalexQuotaPath(), 'utf8'));
  } catch {
    return { available: false };
  }
  if (!parsed || typeof parsed !== 'object') return { available: false };
  const now = Date.now();
  const observedMs = Date.parse(parsed.observed_at || '');
  const resetMs = Date.parse(parsed.reset_at || '');
  return {
    ...parsed,
    available: true,
    stale: !Number.isFinite(observedMs) || now - observedMs > OPENALEX_QUOTA_STALE_MS,
    reset_in_seconds: Number.isFinite(resetMs) ? Math.max(0, Math.round((resetMs - now) / 1000)) : null,
  };
}
```

Route, placed right before `app.get('/api/recursion-config'` (line 5288):

```js
  app.get('/api/openalex/quota', requireAuthMiddleware, (req, res) => {
    return res.json(readOpenAlexQuota());
  });
```

- [ ] **Step 4: Run the backend tests**

Run: `cd backend && npm test`
Expected: all PASS.

- [ ] **Step 5: Add the API client**

`frontend/src/lib/api.js`, after `fetchRecursionConfig`:

```js
export async function fetchOpenAlexQuota() {
  const response = await fetchWithTimeout(`${API_BASE}/api/openalex/quota`)
  await throwIfUnauthorized(response)
  if (!response.ok) {
    const payload = await response.text()
    throw new Error(payload || 'Failed to load OpenAlex quota')
  }
  return response.json()
}
```

- [ ] **Step 6: Commit**

```bash
git add backend/src/app.js backend/tests/openalex-quota.test.js frontend/src/lib/api.js
git commit -m "Serve the OpenAlex quota snapshot from the backend"
```

---

### Task 8: Quota pill in the search panel and admin row

**Files:**
- Modify: `frontend/src/App.svelte` — state near line 259 (`let searchResults = []`), refresh in `runLiveRefreshCycle` (line 1428-1448), after `runSearch` (line 3189-3222) and after the promote handlers (`handlePromoteSeedSource` line 3062, `handlePromoteWholeSeedSource`, and the single-row promote), markup next to the Search button (line 5273-5280), Admin panel props where `<AdminPanel` is rendered (grep `<AdminPanel`)
- Modify: `frontend/src/components/AdminPanel.svelte:65-85` (OpenAlex group)
- Modify: `frontend/src/app.css` (pill styles)
- Test: `frontend/tests/app.spec.ts`

**Interfaces:**
- Consumes: `fetchOpenAlexQuota()` from Task 7.
- Produces: `openalexQuota` state object, `formatOpenAlexQuota(quota) -> { text, tone }` used by both the pill and the admin row.

- [ ] **Step 1: State and formatter**

Near line 259 in `App.svelte`:

```js
  let openalexQuota = null
  let openalexQuotaLoading = false

  async function loadOpenAlexQuota() {
    if (openalexQuotaLoading || authStatus !== 'authenticated') return
    openalexQuotaLoading = true
    try {
      openalexQuota = await fetchOpenAlexQuota()
    } catch {
      // Leave the last known value; the pill is informational only.
    } finally {
      openalexQuotaLoading = false
    }
  }

  function formatResetIn(seconds) {
    if (!Number.isFinite(seconds) || seconds <= 0) return 'now'
    const h = Math.floor(seconds / 3600)
    const m = Math.round((seconds % 3600) / 60)
    if (h === 0) return `${m} min`
    return m === 0 ? `${h} h` : `${h} h ${m} min`
  }

  // Copy is fixed by the spec; tone drives the pill colour.
  function formatOpenAlexQuota(quota) {
    if (!quota || !quota.available) {
      return { text: 'OpenAlex budget: unknown until the first request', tone: 'muted' }
    }
    if (!quota.api_key_present) {
      return { text: 'OpenAlex: no API key — daily budget not reported', tone: 'muted' }
    }
    const remaining = Number(quota.remaining ?? 0)
    const limit = Number(quota.limit ?? 0)
    const base = `OpenAlex budget: ${remaining.toLocaleString('en-US')} of ${limit.toLocaleString('en-US')} left · resets in ${formatResetIn(quota.reset_in_seconds)}`
    if (quota.stale) {
      const seen = quota.observed_at ? new Date(quota.observed_at).toLocaleString() : 'unknown'
      return { text: `${base} · last seen ${seen}`, tone: 'muted' }
    }
    if (remaining <= 0) return { text: base, tone: 'danger' }
    if (limit > 0 && remaining / limit < 0.1) return { text: base, tone: 'warn' }
    return { text: base, tone: 'ok' }
  }

  $: openalexQuotaView = formatOpenAlexQuota(openalexQuota)
```

Import `fetchOpenAlexQuota` in the existing `import { … } from './lib/api.js'` list.

- [ ] **Step 2: Refresh hooks**

- In `runLiveRefreshCycle` (line 1433) add `loadOpenAlexQuota()` to the `tasks` array.
- In `runSearch` after `searchResults = data` add `loadOpenAlexQuota()` (no await).
- In each promote handler's `finally` (or after the refresh sequence that calls `loadSeedSources`) add `loadOpenAlexQuota()`. There are three call sites: whole-seed, selection, single-row. Find them with `grep -n "promoteSeedCandidates(" frontend/src/App.svelte`.
- Call `loadOpenAlexQuota()` once after authentication succeeds (where `loadSeedSources()` is first called after login, around line 1692).

- [ ] **Step 3: Pill markup**

Right after the Search `<button …>Search</button>` (line 5273-5280), inside the same row container:

```svelte
<span
  class={`openalex-quota-pill openalex-quota-pill--${openalexQuotaView.tone}`}
  data-testid="openalex-quota"
  title="Daily OpenAlex API budget as reported by the last request"
>{openalexQuotaView.text}</span>
```

If the row is `display: flex` with `flex-wrap: nowrap`, put the pill in a new line below the row instead (a `<div class="search-quota-row">`) so it does not squeeze the inputs at 1024 px.

- [ ] **Step 4: Admin row**

`AdminPanel.svelte`: add `export let openalexQuotaText = ''` to the props. After the "Requests per second" label (line 87) add:

```svelte
        <div class="admin-settings-form__readonly">
          <span class="muted small">Daily budget</span>
          <span data-testid="admin-openalex-quota">{openalexQuotaText}</span>
        </div>
```

Where `<AdminPanel` is rendered in `App.svelte`, pass `openalexQuotaText={openalexQuotaView.text}`.

- [ ] **Step 5: Styles**

Append to `frontend/src/app.css`:

```css
.openalex-quota-pill {
  display: inline-block;
  padding: 4px 10px;
  border-radius: 999px;
  font-size: 12px;
  white-space: nowrap;
  background: #f4f4f5;
  color: #3f3f46;
}
.openalex-quota-pill--ok { background: #ecfdf5; color: #047857; }
.openalex-quota-pill--warn { background: #fffbeb; color: #b45309; }
.openalex-quota-pill--danger { background: #fef2f2; color: #b91c1c; }
.openalex-quota-pill--muted { background: #f4f4f5; color: #52525b; }

.admin-settings-form__readonly {
  display: flex;
  flex-direction: column;
  gap: 4px;
}
```

- [ ] **Step 6: E2e**

In `frontend/tests/app.spec.ts`:

```ts
test('search panel shows the OpenAlex budget pill', async ({ page, request }) => {
  await ensureSignedIn(page, request)
  const pill = page.getByTestId('openalex-quota')
  await expect(pill).toBeVisible()
  await expect(pill).toContainText(/OpenAlex/)
})
```

- [ ] **Step 7: Run e2e and check by eye**

Run: `cd frontend && npm run test:e2e -- app.spec.ts`
Expected: PASS. Then run one keyword search in the browser and confirm the pill updates within a few seconds (the live refresh runs every 3 s).

- [ ] **Step 8: Commit**

```bash
git add frontend/src/App.svelte frontend/src/components/AdminPanel.svelte frontend/src/app.css frontend/tests/app.spec.ts
git commit -m "Show the OpenAlex daily budget in the search panel and admin settings"
```

---

### Task 9: Docs and Phase 2 PR

**Files:**
- Modify: `.env.example` (after line 30 `RAG_FEEDER_OPENALEX_RPS=30`), `README.md` (Quick Start or the LLM provider section; add a short "OpenAlex budget" paragraph), `docker-compose.yml:38` and `:91` (pass the env through to backend and enrich worker)

- [ ] **Step 1: Env documentation**

`.env.example`:

```
# Where the OpenAlex daily-budget snapshot is written (default: next to the SQLite DB)
# RAG_FEEDER_OPENALEX_QUOTA_PATH=
```

`docker-compose.yml`: add `- RAG_FEEDER_OPENALEX_QUOTA_PATH=${RAG_FEEDER_OPENALEX_QUOTA_PATH:-}` under the same `environment:` blocks that carry `RAG_FEEDER_OPENALEX_RPS` (lines 38 and 91), so backend and workers agree on the path. Confirm both containers mount the same data directory (they must, since they share the SQLite DB).

`README.md`, after the LLM provider section:

```markdown
### OpenAlex daily budget

With an `OPENALEX_API_KEY` set, every OpenAlex response carries the daily
budget headers. The Python request helper writes them to
`openalex_quota.json` next to the SQLite database (override with
`RAG_FEEDER_OPENALEX_QUOTA_PATH`), the backend serves them at
`GET /api/openalex/quota`, and the workspace shows them as a pill next to the
Search button. Without a key OpenAlex does not report a budget and the pill
says so.
```

- [ ] **Step 2: Full test run and PR**

Run all three suites (see Global Constraints). Open the PR titled `Show the OpenAlex daily budget (round 2 item 1)` with the test counts, one screenshot of the pill, and the standard footer.

- [ ] **Step 3: Commit**

```bash
git add .env.example README.md docker-compose.yml
git commit -m "Document the OpenAlex quota snapshot"
```

---

## Self-review notes (done while writing)

- Spec coverage, Phase 1: item 7 → Tasks 1-2; item 10 → Task 3; item 11 → Task 4; item 5 → Task 5 note. Phase 2: Python writer → Task 6; endpoint → Task 7; pill, refresh hooks, admin row → Task 8; docs → Task 9. The spec's "stale after 24 h" and the three copy states are all in Tasks 7-8.
- Type consistency: `refs_count`/`cited_by_count` (Task 1) are what `seedCellText` and `SEED_SORT_ACCESSORS` read (Task 2). `seed_kind`/`snowball` (Task 3 backend) are what the tag reads (Task 3 frontend). The snapshot keys written in Task 6 are the ones Task 7 spreads into the response and Task 8 formats (`remaining`, `limit`, `reset_in_seconds`, `api_key_present`, `stale`, `observed_at`).
- Not in this plan by design: Phases 3-5 (own plan files once Phases 1-2 land).
