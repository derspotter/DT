# Korpus Builder — Jochen To-dos, Phase 1: Labels, Copy & Small Fixes

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the eight spec lines that are pure label/copy/redundancy fixes plus the author cap, with no dependency on the table refactor.

**Architecture:** Frontend-only except one backend one-liner (`seed.js` venue fallback). No new state, no new endpoints, no schema change. Every change is a rename, a deletion, or a display-only helper. This is deliberately the lowest-risk slice of the 26 lines so it can ship on its own.

**Tech Stack:** Svelte 5 (`frontend/src/App.svelte`, `frontend/src/components/Corpus.svelte`), Node/Express (`backend/src/seed.js`), Jest (backend), Playwright (frontend e2e).

## Global Constraints

- Source spec: `jochen_enriched.md` at repo root. All decisions there are RESOLVED — do not relitigate them.
- **Spec path correction:** the spec says `dl_lit/corpus_list.py`, `dl_lit/keyword_search.py`, `dl_lit/upstream_update.py`, `dl_lit/graph_3d_export.py`. The real paths are `backend/scripts/`. Library code (`llm_provider.py`, `get_bib_pages.py`, `APIscraper_v2.py`, `utils.py`) is at `dl_lit_project/dl_lit/`. `backend/scripts/keyword_search.py` imports from the `dl_lit` package.
- **Label + tooltip pattern** (used by lines 2, 15, 21): the visible badge gets the short general label; the specific nuance moves to a `hint`/`title` tooltip. Never drop the nuance.
- All spec line anchors were verified exact against `master@b86135e` on 2026-07-30. If a line has moved, match on the quoted string, not the number.
- Branch: `jochen-todos`.
- Backend tests: `cd backend && npm test`. Frontend e2e: `cd frontend && npm run test:e2e`.

---

## Phase Roadmap — ALL 26 LINES SHIPPED (2026-07-31)

| Phase | Spec lines | Scope | Commit |
|---|---|---|---|
| 1 | 1, 2, 3, 7, 8, 15&26, 21, 25 | Labels, copy, redundancy, author cap | `113d96f` `cd026ae` `c3400ee` |
| 2 | 4, 5, 9 | Seed table: inline promote, client-side sort, downstream selectability | `b0dd52a` |
| 3 | 6, 17 | `q` text filter across three endpoints + corpus server-side sort | `a1fa07c` |
| 5 | 20, 22, 23, 24 | Seed-PDF endpoint, graph corpus scoping, corpus removal, upstream count | `5890803` |
| 6 | 13&14 | `app_settings` store + admin Settings UI | `0491685` |
| 7 | 10&11, 12 | Two promotion modes + related-paper ranking | `93cbb89` |
| 4 | C1, C2, 16, 18, 19 | Unified sortable/toggleable column system, metadata/download split | `d3d8085` |

Line 17 moved from phase 4 into phase 3: the spec notes it shares the
`/api/corpus` round-trip with the line-6 filter, and both land in the same
`corpus_list.py` region.

### Known gap

Line 23 asked for a per-row **✕** *and* a bulk "Remove selected". Only the
per-row removal shipped: the corpus table has single-row expansion, not
multi-select, so a bulk action needs selection infrastructure the spec did not
scope. `DELETE /api/corpus/works/:workId` is per-work, so a bulk UI can be
layered on top without further backend work.

---

## File Structure

| File | Responsibility in this phase |
|---|---|
| `frontend/src/App.svelte` | `seedCandidateTag` labels (2, 15, 21), status/tone map (21), seed pill (2), table header (3), action bar (1), `formatAuthorsShort` (8), corpus status line (25) |
| `frontend/src/components/Corpus.svelte` | Corpus badge labels (15, 21), stage filter option (21), "Show more" (25), author cell (8) |
| `backend/src/seed.js` | Venue fallback — never emit a URL as `source` (7) |
| `backend/tests/seed-state.test.js` | Regression test for the venue fallback (7) |

---

### Task 1: Backend — never show a DOI URL as the venue (spec line 7)

**Files:**
- Modify: `backend/src/seed.js:508`
- Test: `backend/tests/seed-state.test.js`

**Interfaces:**
- Consumes: nothing.
- Produces: OpenAlex-search seed candidates whose `source` is either a venue display name or `null` — never a `doi.org/...` URL. Phase 4 (line 19, Source = venue) relies on this.

Root cause per spec: when the OpenAlex venue has no `display_name`, the code falls back to `landing_page_url`, which is frequently a DOI URL. Decision was option 2 — blank.

Current line 508:

```js
      source: primarySource.display_name || raw?.primary_location?.landing_page_url || null,
```

- [ ] **Step 1: Read the surrounding mapper**

Run: `sed -n '495,520p' backend/src/seed.js` — confirm `primarySource` is in scope and that line 508 is inside the OpenAlex-search mapping path (NOT the seed-doc-refs path at line 434, which the spec says to leave alone).

- [ ] **Step 2: Write the failing test**

Append to `backend/tests/seed-state.test.js` — first open the file and match the existing import/describe style; the assertion is what matters:

```js
describe('seed candidate venue fallback', () => {
  it('returns null instead of a landing page URL when the venue has no display_name', () => {
    const mapped = mapOpenAlexCandidate({
      id: 'https://openalex.org/W123',
      primary_location: {
        source: { display_name: null },
        landing_page_url: 'https://doi.org/10.1234/abcd',
      },
    });
    expect(mapped.source).toBeNull();
  });

  it('still returns the venue display_name when present', () => {
    const mapped = mapOpenAlexCandidate({
      id: 'https://openalex.org/W124',
      primary_location: {
        source: { display_name: 'Journal of Labour Studies' },
        landing_page_url: 'https://doi.org/10.1234/abcd',
      },
    });
    expect(mapped.source).toBe('Journal of Labour Studies');
  });
});
```

If the mapping function is not exported, export it — do not restructure it. If it is not a standalone function, drive the assertion through whichever exported seam `seed-state.test.js` already uses and keep the two cases identical in intent.

- [ ] **Step 3: Run the test and confirm it fails**

Run: `cd backend && npm test -- seed-state`
Expected: FAIL — first case returns `'https://doi.org/10.1234/abcd'`, not `null`.

- [ ] **Step 4: Apply the one-line fix**

```js
      source: primarySource.display_name || null,
```

- [ ] **Step 5: Run the test and confirm it passes**

Run: `cd backend && npm test -- seed-state`
Expected: PASS, both cases.

- [ ] **Step 6: Run the whole backend suite for regressions**

Run: `cd backend && npm test`
Expected: all pass. If a fixture asserted the URL fallback, update the fixture — the spec makes blank the intended behavior.

- [ ] **Step 7: Commit**

```bash
git add backend/src/seed.js backend/tests/seed-state.test.js
git commit -m "Show venue name or nothing as seed source, never a DOI URL"
```

---

### Task 2: Delete the redundant "Remove source" button (spec line 1)

**Files:**
- Modify: `frontend/src/App.svelte:4267-4269`

**Interfaces:**
- Consumes: nothing.
- Produces: expanded seed action bar containing exactly two selection-scoped buttons. Phase 2 (line 4, inline promote) adds to the row, not this bar.

Spec: this button calls the same `handleRemoveSeedSource(source)` as the header ✕ at 4229, which is already labeled "Remove seed". Pure redundancy, and it is the only non-selection-scoped control in a selection-scoped bar.

- [ ] **Step 1: Delete the button**

Remove exactly this block (App.svelte ~4267):

```svelte
                                <button class="danger" type="button" on:click={() => handleRemoveSeedSource(source)} disabled={seedActionBusy}>
                                  Remove source
                                </button>
```

- [ ] **Step 2: Confirm the handler is still used**

Run: `grep -n "handleRemoveSeedSource" frontend/src/App.svelte`
Expected: still referenced at ~4229 (the header ✕). Do NOT delete the handler — if the grep shows it orphaned, you deleted the wrong block; revert and retry.

- [ ] **Step 3: Commit**

```bash
git add frontend/src/App.svelte
git commit -m "Drop the redundant Remove source button from the seed action bar"
```

---

### Task 3: "PDF reusable" → "PDF downloaded" (spec line 2)

**Files:**
- Modify: `frontend/src/App.svelte:2496` (item badge), `frontend/src/App.svelte:4244` (seed pill)

**Interfaces:**
- Consumes: nothing.
- Produces: `downloaded_elsewhere_available` displaying the same visible label as plain `downloaded`. This is intended per spec — the corpus-of-origin distinction survives only in the tooltip.

- [ ] **Step 1: Rename the badge label, keep the hint**

App.svelte:2496 — change `label` only, leave `className` and `hint` untouched:

```js
          ? { label: 'PDF downloaded', className: 'downloaded', hint: 'Downloaded by another corpus and the file is present — promoting reuses it' }
```

- [ ] **Step 2: Rename the seed pill, keep the title**

App.svelte:4244:

```svelte
                          <span class="pill" title="Same work downloaded by another corpus and the file is present — promoting reuses it">PDF downloaded: {source.state_counts.downloaded_elsewhere_available}</span>
```

- [ ] **Step 3: Verify no stray occurrences**

Run: `grep -rn "PDF reusable" frontend/src/`
Expected: no output.

- [ ] **Step 4: Commit**

```bash
git add frontend/src/App.svelte
git commit -m "Rename PDF reusable to PDF downloaded, keep the reuse hint on hover"
```

---

### Task 4: Unify `failed_enrichment` to "Metadata not confirmed" (spec lines 15 & 26)

**Files:**
- Modify: `frontend/src/App.svelte:2509`, `frontend/src/App.svelte:657`, `frontend/src/components/Corpus.svelte:69`

**Interfaces:**
- Consumes: nothing.
- Produces: one visible label for `failed_enrichment` across both tables. Phase 4 (line 16) reuses this exact string as a Metadata-axis label.

Spec: one state, two labels today ("Metadata failed" in §2, "Enrich failed" in §3). Unify to **"Metadata not confirmed"** — accurate for both origins (seed-doc refs and search results) and both causes (no Crossref match, transient error). Keep the Crossref reason as a hover hint.

- [ ] **Step 1: Update the seed badge and add the reason hint**

App.svelte:2509:

```js
        return { label: 'Metadata not confirmed', className: 'failed', hint: 'No Crossref match found' }
```

- [ ] **Step 2: Update the status/tone map**

App.svelte:657 currently reads `failed_enrichment: { label: 'Enrich failed', tone: 'in_progress' },`. Change to:

```js
      failed_enrichment: { label: 'Metadata not confirmed', tone: 'in_progress' },
```

- [ ] **Step 3: Update the corpus badge**

Corpus.svelte:69:

```js
    if (FAILED_ENRICHMENT_STATUSES.has(item.status)) return 'Metadata not confirmed';
```

- [ ] **Step 4: Verify**

Run: `grep -rn "Enrich failed\|Metadata failed" frontend/src/`
Expected: no output.

- [ ] **Step 5: Commit**

```bash
git add frontend/src/App.svelte frontend/src/components/Corpus.svelte
git commit -m "Unify failed_enrichment label to Metadata not confirmed"
```

---

### Task 5: `failed_download` → "Not retrievable" (spec line 21)

**Files:**
- Modify: `frontend/src/App.svelte:2511`, `frontend/src/App.svelte:658`, `frontend/src/components/Corpus.svelte:70`, `frontend/src/components/Corpus.svelte:155`

**Interfaces:**
- Consumes: nothing.
- Produces: one visible label for `failed_download`. Phase 4 (line 16) reuses it as a Download-axis label.

- [ ] **Step 1: Seed badge + tooltip**

App.svelte:2511:

```js
        return { label: 'Not retrievable', className: 'failed', hint: 'Document not retrievable' }
```

- [ ] **Step 2: Status/tone map**

App.svelte:658:

```js
      failed_download: { label: 'Not retrievable', tone: 'in_progress' },
```

- [ ] **Step 3: Corpus badge**

Corpus.svelte:70:

```js
    if (FAILED_DOWNLOAD_STATUSES.has(item.status)) return 'Not retrievable';
```

- [ ] **Step 4: Corpus stage-filter option**

Corpus.svelte:155 — relabel only; the `value="failed_download"` must not change or the filter breaks:

```svelte
              <option value="failed_download">Not retrievable ({failedDownloadTotal || 0})</option>
```

- [ ] **Step 5: Verify**

Run: `grep -rn "Download failed\|Failed downloads" frontend/src/`
Expected: no output. Then `grep -n 'value="failed_download"' frontend/src/components/Corpus.svelte` — expected: still present.

- [ ] **Step 6: Commit**

```bash
git add frontend/src/App.svelte frontend/src/components/Corpus.svelte
git commit -m "Rename failed_download label to Not retrievable"
```

---

### Task 6: "Load more" → "Show more", "Showing X of Y" (spec line 25)

**Files:**
- Modify: `frontend/src/components/Corpus.svelte:249`, `frontend/src/App.svelte:3159`

- [ ] **Step 1: Button label**

Corpus.svelte:249 — keep the loading branch:

```svelte
          {corpusLoadingMore ? 'Loading…' : 'Show more'}
```

- [ ] **Step 2: Status line**

App.svelte:3159 — drop the scroll tail:

```js
              ? `Showing ${corpusItems.length} of ${corpusTotal} entries.`
```

Read the full ternary at 3155-3162 first and preserve the other branch verbatim.

- [ ] **Step 3: Verify**

Run: `grep -rn "Load more\|Scroll to load" frontend/src/`
Expected: no output.

- [ ] **Step 4: Commit**

```bash
git add frontend/src/App.svelte frontend/src/components/Corpus.svelte
git commit -m "Rename Load more to Show more and Loaded X of Y to Showing X of Y"
```

---

### Task 7: Give the seed table's 7th column a "Corpus" heading (spec line 3)

**Files:**
- Modify: `frontend/src/App.svelte:4290`

**Interfaces:**
- Produces: a labeled 7th column. Phase 2 (line 4) puts the inline **→ Promote** button in this column — which is why the spec revised the heading from "In corpus" to "Corpus".

- [ ] **Step 1: Replace the empty header span**

App.svelte:4290 — currently `<span aria-hidden="true"></span>`:

```svelte
                                <span>Corpus</span>
```

- [ ] **Step 2: Confirm the grid still has 7 columns**

Run: `grep -n "cols-7" frontend/src/App.svelte`
Expected: the header row and body rows both still use `cols-7`. No CSS change — only the text content changed.

- [ ] **Step 3: Commit**

```bash
git add frontend/src/App.svelte
git commit -m "Label the seed candidate table's corpus column"
```

---

### Task 8: Cap displayed authors at 3 + "et al." (spec line 8)

**Files:**
- Modify: `frontend/src/App.svelte` (add helper near `formatAuthors` at 575; use at 4347; pass as prop at 4407 and 4462)
- Modify: `frontend/src/components/Corpus.svelte` (accept prop at ~12; use in `corpusItemAuthors` at ~96)

**Interfaces:**
- Consumes: existing `formatAuthors(entry)` at App.svelte:575, which joins all authors and is already passed into `Corpus.svelte` as a prop (Corpus.svelte:12).
- Produces: `formatAuthorsShort(entry, cap = 3) -> string`. Passed into `Corpus.svelte` as a prop named `formatAuthorsShort`. Phase 4 (C1 unified columns) uses it for the shared Authors cell in both tables.

Spec: table cells show first 3 + "et al."; the full list stays in the expanded inline-detail card (App.svelte:4366) and as a `title` tooltip. `formatAuthors` itself must NOT change — line 4366 and the search filter at 2087 depend on the full string.

- [ ] **Step 1: Add the helper directly below `formatAuthors`**

Insert after App.svelte:588 (the closing brace of `formatAuthors`):

```js
  function formatAuthorsList(entry) {
    if (!entry) return []
    if (Array.isArray(entry.authors)) return entry.authors
    if (typeof entry.authors === 'string') {
      try {
        const parsed = JSON.parse(entry.authors)
        if (Array.isArray(parsed)) return parsed
      } catch (error) {
        // fall through
      }
    }
    if (Array.isArray(entry.author)) return entry.author
    const raw = entry.authors || entry.author || ''
    return String(raw).trim() ? String(raw).split(',').map((name) => name.trim()).filter(Boolean) : []
  }

  function formatAuthorsShort(entry, cap = 3) {
    const list = formatAuthorsList(entry)
    if (list.length === 0) return formatAuthors(entry)
    if (list.length <= cap) return list.join(', ')
    return `${list.slice(0, cap).join(', ')} et al.`
  }
```

- [ ] **Step 2: Use it in the seed candidate row, with the full list on hover**

App.svelte:4347 — currently `<span>{formatAuthors(candidate)}</span>`:

```svelte
                                  <span title={formatAuthors(candidate)}>{formatAuthorsShort(candidate)}</span>
```

Leave App.svelte:4366 (the inline-detail chip) on `formatAuthors` — the expanded card shows the full list by design.

- [ ] **Step 3: Pass the helper into Corpus.svelte**

At App.svelte:4407 and 4462 the component already receives `{formatAuthors}`. Add alongside each:

```svelte
                {formatAuthorsShort}
```

- [ ] **Step 4: Accept and use the prop in Corpus.svelte**

Add next to the existing prop at Corpus.svelte:12:

```js
  export let formatAuthorsShort;
```

Then change `corpusItemAuthors` (~96) to return the capped form, and add a full-list companion for the tooltip:

```js
  function corpusItemAuthors(item) {
    if (!item) return ''
    if (typeof item.authors_display === 'string' && item.authors_display.trim()) {
      return formatAuthorsShort({ authors: item.authors_display.split(',').map((n) => n.trim()) })
    }
    return formatAuthorsShort(item)
  }

  function corpusItemAuthorsFull(item) {
    if (!item) return ''
    if (typeof item.authors_display === 'string' && item.authors_display.trim()) return item.authors_display
    return formatAuthors(item)
  }
```

Find the markup that renders `corpusItemAuthors(item)` and add `title={corpusItemAuthorsFull(item)}` to its element. If the corpus table has no Authors cell yet (spec C1 notes Corpus is missing Authors), leave the helpers defined and unused — Phase 4 wires the column. Note that in the plan file's own words: do not add the column here, that is C1's job.

- [ ] **Step 5: Verify the app builds**

Run: `cd frontend && npm run build`
Expected: build succeeds. A Svelte "unused export property" warning for `formatAuthorsShort` is acceptable if the corpus Authors cell does not exist yet.

- [ ] **Step 6: Commit**

```bash
git add frontend/src/App.svelte frontend/src/components/Corpus.svelte
git commit -m "Cap table author lists at three names with the full list on hover"
```

---

### Task 9: Phase verification

- [ ] **Step 1: Backend suite**

Run: `cd backend && npm test`
Expected: all pass.

- [ ] **Step 2: Frontend build**

Run: `cd frontend && npm run build`
Expected: succeeds.

- [ ] **Step 3: E2E suite**

Run: `cd frontend && npm run test:e2e`
Expected: pass. Several specs assert on visible copy — if `app.spec.ts` or `seed-upload.spec.ts` asserts "Load more", "Enrich failed", "Download failed", or "PDF reusable", update those assertions to the new strings. That is an expected consequence of this phase, not a regression.

- [ ] **Step 4: Confirm every spec line in this phase is covered**

| Spec line | Task |
|---|---|
| 1 remove source → remove seed | 2 |
| 2 pdf reusable → pdf downloaded | 3 |
| 3 green checkmark → corpus heading | 7 |
| 7 source is sometimes a DOI | 1 |
| 8 cap too many authors | 8 |
| 15 & 26 metadata failed / enrich failed | 4 |
| 21 download failed → not retrievable | 5 |
| 25 load more → show more | 6 |
