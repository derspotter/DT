# Purchaser Feedback Round 1 — Design

Date: 2026-07-02
Source: `Korpus builder.txt` (purchaser notes, mixed DE/EN)
Scope: Workspace tab ("Korpus Builder"), keyword search backend, LLM extraction backend.

## Goal

Address all 20 feedback items in five independent PRs. The feedback splits into:
wording/clarity (most items), stage/stats presentation, search behavior, LLM
provider flexibility, and a visual restyle. Two items are questions from the
purchaser that get written answers (see "Answers for the purchaser").

## Feedback item → chunk mapping

| # | Feedback (paraphrased) | Chunk |
|---|---|---|
| 1 | Look a little different, less AI-like, different CSS | E |
| 2 | Make descriptions more telling | A |
| 3 | UI should say "you are *building* a corpus", not just viewing one | A |
| 4 | "metadata" → "bibliographic metadata / information" | A |
| 5 | "seeded sources" → "seeds" | A |
| 6 | Is there something better than "seeds"? | Answer |
| 7 | What does "metadata" on the right mean? | A (rename + tooltip) |
| 8 | Seed stage separate from promoted and downloaded | B |
| 9 | Counters: items found / items promoted / items downloaded | B |
| 10 | "Document Search" → "Seed document" | A |
| 11 | "Search" → item identification/extraction; "Search items" and "Document items" | A |
| 12 | Remove "uploaded" chip on seed doc | A |
| 13 | Make compatible with OpenAI API | D |
| 14 | "Extracted PDF"? → tooltip "bibliographic metadata extracted from PDF" | A |
| 15 | Throw out "Refresh" | A |
| 16 | Make explicit that keyword search uses OpenAlex | A |
| 17 | Why cap at 200? No cap; let user cap | C |
| 18 | What is OpenAlex default sort? Can we sort? | C + Answer |
| 19 | Show items are collapsible/expandable | B |
| 20 | Delete "Seed Refresh" | A |

## PR A — Copy & terminology pass (App.svelte, Corpus.svelte)

All wording changes in one pass so the vocabulary stays consistent:

- **"Document Search" → "Seed document"** (`App.svelte:3958`). Remove the
  now-wrong "Search" framing from the upload card.
- **Section 1 rename**: "1. Search" → "1. Find items" with description
  explaining the two intake paths produce **Document items** (extracted from an
  uploaded seed document) and **Search items** (from an OpenAlex keyword
  search). The source-type tag per seed (`PDF` / `Search`) becomes
  `Document items` / `Search items`; the "candidates" pill becomes "items".
- **"Keyword Search" → "Keyword search (OpenAlex)"** plus one description line
  naming OpenAlex as the data source (item 16).
- **"metadata" → "bibliographic metadata"** everywhere it labels enrichment
  state: hero stat (`:3941`), seed pill `Metadata: n` (`:4234`), Corpus stage
  labels. Pills get `title=` tooltips spelling out the meaning ("n items with
  bibliographic metadata retrieved").
- **"seed sources" → "seeds"** (hero stat `:3940`, empty states, status
  strings). Section "2. Seed" keeps its name; description rewritten.
- **Remove the `uploaded` status chip** on upload rows (`:3978`) — show the
  chip only for active states (queued/extracting/failed), not the resting
  "uploaded" state (item 12).
- **"Extracted PDFs"**: label becomes "Extracted seed documents" and each row's
  existing `title` tooltip is prefixed with "Bibliographic metadata extracted
  from this PDF" (item 14; tooltip plumbing already exists at `:4003`).
- **Remove both Refresh buttons**: Extracted-PDFs `Refresh` (`:3997`) and Seed
  panel `Refresh` (`:4106`). Both lists must self-update: they already reload
  after every mutating action; add reload-on-extraction-completion and a
  lightweight visibility-change refetch so manual refresh is never needed.
- **Working-on-a-corpus framing** (items 2, 3): rewrite the hero h2/paragraph
  and section descriptions in active, task-oriented language ("Build your
  corpus in three steps: find items, review them in Seed, promote them into
  the corpus and download PDFs."). Each of the three sections' description
  says what the user *does* there, not what the panel *shows*.

## PR B — Stages & counters, collapsibility affordance

- **Hero stats become the pipeline counters** (item 9):
  `items found` (total items across seeds) / `items promoted` (works in
  corpus) / `items downloaded`. "Seeds: n" moves to the Seed section header.
- **Seed stage shown separately from promoted/downloaded** (item 8): in the
  Corpus panel's stage filter and per-item stage pills, group into
  *Seed* (raw/pending) vs *Promoted* (enriching/enriched) vs *Downloaded*
  rather than the current mixed list; failed states stay as sub-filters.
- **Collapsibility affordance** (item 19): seeds already expand/collapse but
  the only cue is a small "Show/Hide" text. Replace with a chevron that
  rotates on expand, on both seed sources and corpus item detail rows, so it
  is visible at a glance that lists are collapsible.

## PR C — Search: no cap, user cap, sort

- **Remove the 200 cap** (item 17): UI gets a "Max results" field, empty by
  default = no cap. Backend treats absent/0 `maxResults` as unlimited;
  `dl_lit/keyword_search.py` `search_openalex` accepts `max_results=None` and
  pages with cursor until exhausted (per-page stays 200, the API maximum).
  The search status line shows live progress (OpenAlex `meta.count` first, then
  fetched count) so an accidental 500k-result query is visible and the user
  can cap next time.
- **Sort control** (item 18): dropdown `Relevance (default) / Most cited /
  Newest / Oldest`, mapped to OpenAlex `sort=relevance_score:desc |
  cited_by_count:desc | publication_date:desc | publication_date:asc`, passed
  through `/api/keyword-search` → `keyword_search.py --sort`.

## PR D — OpenAI-compatible LLM provider

Decision (user AFK, recommended option): **configurable OpenAI-compatible
endpoint**, Gemini remains the default. This covers OpenAI itself, Azure,
and local vLLM/Ollama-style servers.

- New env config: `RAG_FEEDER_LLM_PROVIDER=gemini|openai` (default `gemini`),
  `RAG_FEEDER_OPENAI_BASE_URL` (default `https://api.openai.com/v1`),
  `OPENAI_API_KEY`, `RAG_FEEDER_OPENAI_MODEL`.
- Python: a small provider module in `dl_lit` wrapping the two call shapes
  used today (PDF+prompt → text, text prompt → JSON). Gemini path keeps the
  Files API; OpenAI path sends the PDF as base64 `input_file` content on
  chat/completions, falling back to per-page images if the endpoint rejects
  file inputs. Callers (`get_bib_pages.py`, `APIscraper_v2.py`, `new_dl.py`)
  switch to the provider module.
- Node: the inline Gemini helpers in `backend/src/app.js` (upload/delete,
  inline extraction) route through the same provider selection.
- Docs: `.env.example`/README gain the new variables.

## PR E — Visual restyle ("less AI-like")

Constraint: "look **a little** different" — restyle, not redesign. Layout and
behavior unchanged; only `app.css` tokens and chrome:

- Move away from the generic AI-tool look (soft cards, teal accent, pill
  clouds): editorial/academic direction — a serif display face for headings,
  tighter radii, ruled section dividers instead of floating cards, a
  paper/ink palette with one restrained accent, denser tables.
- Validate with Gemini CLI design feedback and before/after screenshots at
  desktop + phone breakpoints.

## Answers for the purchaser (deliver with the PRs)

- **OpenAlex default sort**: with a search term, OpenAlex sorts by
  `relevance_score` descending (text-match relevance); without one, by
  publication date. Sorting by citations or date is supported and PR C adds a
  sort selector.
- **Better word than "seeds"?** "Seeds" is the established term in systematic
  review / citation-mining tooling (seed works, seed set) and is short;
  recommendation: keep **"Seeds"** but let every description explain it
  ("items you start from"). German-facing alternatives, if preferred:
  "Ausgangsquellen" or "Startmenge". Renaming later is a copy-only change.
- **What "metadata" meant**: the count of items whose bibliographic metadata
  has been retrieved — now labeled "bibliographic metadata" with a tooltip.

## Error handling & testing

- Unlimited search uses cursor pagination with the existing retry/backoff in
  `search_openalex`; a failed page surfaces the partial count and an error
  status instead of silently truncating.
- Provider abstraction: unknown provider value → hard startup error; missing
  key → clear API error message in the extraction status, not a silent fall
  back to Gemini.
- Each PR: run the frontend build, exercise the workspace flow against the
  dev stack (upload → extract → promote → search), Playwright screenshots for
  UI PRs, `ruff` on touched Python.

## Out of scope

- No changes to the graph tab, Scraper Lab, or legacy hidden tabs (`ingest`,
  `search`, `downloads`) beyond what breaks otherwise.
- No data-model/schema changes; stage regrouping is presentation-only.
