# Phase 1 Plan — Refactor Core + Keyword Search

## Goals (Milestone 1)
- Stabilize the current extraction → enrichment → download pipeline so it is repeatable and testable.
- Deliver a working keyword search flow that persists results to SQLite and can be queued for download.

## Scope (Code Areas)
- CLI: `dl_lit_project/dl_lit/cli.py`
- DB + schema: `dl_lit_project/dl_lit/db_manager.py`
- Extraction: `dl_lit_project/dl_lit/get_bib_pages.py`, `dl_lit_project/dl_lit/APIscraper_v2.py`
- Enrichment: `dl_lit_project/dl_lit/OpenAlexScraper.py`, `dl_lit_project/dl_lit/metadata_fetcher.py`
- Downloads: `dl_lit_project/dl_lit/new_dl.py`, `dl_lit_project/dl_lit/pdf_downloader.py`
- Orchestration: `dl_lit_project/dl_lit/pipeline.py`
- Tests: `dl_lit_project/tests/*`

## Workstreams and Tasks

### 1) Canonicalize the runtime entrypoints
- Decide and document `dl_lit_project/dl_lit` as the canonical package (avoid root `dl_lit/`). (done)
- Update CLI/docs to reference only the canonical module path. (done)
- Add a short note in README (or a new dev doc) stating which package is authoritative. (done)

### 2) Pipeline stabilization (end-to-end consistency)
- Fix `pipeline.py` flow so each stage receives the correct type (e.g., `process_single_pdf` should get a PDF path, not a directory). (done)
- Ensure `extract_reference_sections` → API extraction (`process_single_pdf`) → DB insertion works consistently. (done)
- Add a simple orchestration helper (function or CLI command) that runs: (done)
  1) extract bibliography pages
  2) extract bibliography entries to JSON
  3) insert to `no_metadata`
  4) enrich to `with_metadata`
  5) enqueue for download

### 3) DB schema + normalization pass
- Verify normalized columns are set for all ingestion paths (`no_metadata`, `with_metadata`, `to_download_references`). (done)
- Add/extend indices as needed for DOI/title/author matching. (done)
- Normalize title/author fields consistently across staging tables. (done)
- Preserve source metadata across stages (carry source/volume/issue/pages/etc.). (done)
- Add provenance/run tracking (`pipeline_runs`, `ingest_source`, `run_id`). (done)
- Add `work_aliases` table for translations/alternate titles and update dedupe logic to consult aliases. (done)
- If keyword search adds new tables (see Workstream 5), add idempotent creation in `DatabaseManager._create_schema()`.

### 4) OpenAlex/Crossref enrichment hardening
- Normalize how rate limiting is passed into `OpenAlexScraper` (avoid implicit globals). (done)
- Decide whether `metadata_fetcher.py` is still needed; otherwise fold into OpenAlex path to reduce duplication. (done; deprecated file)
- Standardize the metadata shape returned by enrichment so the downloader always gets consistent fields. (done)
- Normalize DOI inputs and resolve Crossref DOIs back into OpenAlex; apply soft tie-breakers (title/year) only when author scores are tied. (done)

### 5) Keyword search implementation (Phase 1 deliverable)
- Add `keyword_search.py` to support boolean parsing (AND/OR) and OpenAlex query generation. (done)
- OpenAlex API notes to encode in the implementation (aligns with current `OpenAlexScraper` usage):
  - Default to the `search` parameter for Works (searches title, abstract, and fulltext when available). Support boolean operators `AND`/`OR`/`NOT` (must be uppercase); boolean mode is enabled by including these operators, and supports parentheses and quoted phrases. Wildcards/fuzzy operators (`*`, `?`, `~`) are not allowed. Stemming and stop‑word removal are applied; use `.no_stem` variants for title/abstract searches when exact forms are required.
  - Support scoped search filters: `title.search`, `abstract.search`, `title_and_abstract.search`, `fulltext.search`, and `default.search` (equivalent to `search`).
  - Note: `fulltext.search` is backed by n‑grams, so exact phrase matches with quotes can be unreliable there.
  - Combine filters using OpenAlex rules: comma for AND across filters, `|` for OR within a single filter (up to 50 values), `!` for negation.
  - Use `per-page` between 1 and 200 (default 25). Basic paging only returns the first 10,000 results; use `cursor=*` for larger result sets.
  - Use `select` to limit payload to fields already used by `OpenAlexScraper` (e.g., `id`, `doi`, `display_name`, `authorships`, `publication_year`, `type`, `abstract_inverted_index`, `keywords`, `cited_by_api_url`, `open_access`, `primary_location`).
  - For author/institution/source name constraints, do a 2‑step lookup: search the entity endpoint to resolve the OpenAlex ID, then filter works by that ID.
  - LLM guide addenda (official best practices):
    - Do not use random page numbers for sampling. Use `sample` with optional `seed`, and deduplicate across multiple samples for large datasets.
    - Don’t filter by entity names directly; resolve the entity ID first, then filter works by ID.
    - Only one `group_by` per request; for multi‑dimensional analysis, do multiple queries and combine client‑side.
    - Implement exponential backoff on 429/5xx/timeouts, with a max retry cap and logging; set timeouts (30s recommended).
    - Use `per-page=200` for bulk list retrieval.
    - Batch ID lookups with the pipe `|` operator to reduce calls.
    - Include `mailto=` to access the higher rate pool (10 req/s vs 1 req/s without email); daily limit is 100k requests.
- Persist search runs and results in new tables: (done)
  - `search_runs(id, query, filters_json, created_at)`
  - `search_results(id, search_run_id, openalex_id, doi, title, year, raw_json)`
- Add CLI command: (done)
  - `keyword-search --query "A AND B" --max-results 200 --year-from 2000 --year-to 2024`
  - Optional `--enqueue` to push results into `to_download_references`.

### 6) Download pipeline hygiene
- Fix fragile logic in `new_dl.py` (e.g., use of undefined variables, HTML parsing branches). (done)
- Consolidate core download logic in `pdf_downloader.py` and keep `BibliographyEnhancer` thin.

### 7) Tests and regression coverage
- Add tests for keyword parsing and OpenAlex query shaping. (done)
- Add CLI tests for the new `keyword-search` command. (done)
- Ensure existing tests for enrichment and downloads still pass. (done)

## Acceptance Criteria (Phase 1)
- A single command can execute extract → enrich → queue without manual patching.
- `keyword-search` supports AND/OR and persists results to SQLite.
- No runtime errors in the OpenAlex enrichment path or download worker.
- Tests pass for existing CLI workflows plus new keyword-search coverage.

## Status / Validation
- Partial pipeline smoke test run on `dl_lit_project/completed/2004_kapitel-02.pdf` (Jan 26, 2026): extraction succeeded (5 ref pages, 93 entries inserted); enrichment began, then run was stopped early to avoid a long OpenAlex batch.
- Enrichment smoke test (Jan 26, 2026): `enrich-openalex-db --batch-size 10 --no-fetch-references` → 9 promoted, 1 failed.
- Failed enrichment retry (Jan 26, 2026): `retry-failed-enrichments` then `enrich-openalex-db --batch-size 5 --no-fetch-references` → 5 promoted, 0 failed.
- Targeted retry of a previously failed entry (Jan 26, 2026): ID 4159 (`Shareholder vs. Stakeholder: ökonomische Aspekte`) still failed; DOI field contained a non-DOI string (likely extraction issue).
- Additional enrichment batch (Jan 26, 2026): `enrich-openalex-db --batch-size 10 --no-fetch-references` → 9 promoted, 1 failed (ID 304 `Abwanderung und Widerspruch`).
- Translation-aware extraction test (Jan 26, 2026): re-extracted `Stakeholderorientierung..._refs_physical_p26.pdf` and inserted 23/24 entries; Hirschman entry extracted as English title with `translated_*` fields. Alias moved to existing download-queue record (OpenAlex duplicate) after enrichment attempt.
- Targeted enrichment batch from `..._refs_physical_p26.pdf` (Jan 26, 2026): 5 entries attempted; all skipped due to duplicates in existing queue/downloaded records (0 promoted, 0 failed). One item hit `with_metadata.openalex_id` unique constraint (duplicate in with_metadata).
- Targeted enrichment batch from `..._refs_physical_p26.pdf` (Jan 26, 2026): IDs 4164–4168 attempted; 3 promoted (4164, 4165, 4167) and 2 moved to failed_enrichments (4166 `Varieties of Capitalism`, 4168 `Firms, Contracts and Financial Structure`).
- Follow-up on 4166/4168 (Jan 26, 2026): both lacked year/DOI in failed_enrichments and already existed in `downloaded_references` (4166 DOI `10.1093/0199247757.001.0001`, 4168 DOI `10.1093/0198288816.001.0001`); removed from failed_enrichments as duplicates.
- Duplicate handling hardening (Jan 26, 2026): `check_if_exists` now supports normalized title+authors when year is missing and title+year when authors missing; `retry_failed_enrichments` now runs duplicate checks and preserves normalized fields/source_pdf/year on reinsert.
- Pipeline helper update (Jan 26, 2026): `run-pipeline` now routes through `pipeline.run_pipeline`, which writes extracted entries to JSON, scopes enrichment/queue to newly inserted IDs, and logs runs in `pipeline_runs`.
- Pipeline helper update (Jan 26, 2026): `run-pipeline` now supports `--max-ref-pages` to limit processed reference pages per PDF for quick smoke tests.
- Pipeline smoke attempt (Jan 26, 2026): run on `2008_Corporate Moral Legitimacy and the Legitimacy of M.pdf` with `--max-ref-pages 1` hung in Gemini extraction; run marked aborted in `pipeline_runs`.
- Gemini hang fix (Jan 26, 2026): added a timeout guard (default 120s) around `generate_content` in `upload_pdf_and_extract_bibliography` to avoid indefinite stalls.
- Gemini SDK update (Jan 26, 2026): switched to the new Google GenAI Python SDK + Files API (`google-genai`), including `client.files.upload/delete` and `client.models.generate_content` with JSON output config.
- Download pipeline hygiene (Jan 26, 2026): `BibliographyEnhancer` now uses `PDFDownloader` for OpenAlex/OA downloads before falling back to direct URL/Sci‑Hub logic.
- Download pipeline hygiene (Jan 26, 2026): centralized PDF validation in `pdf_downloader.py` and reuse it for direct URL downloads; queue worker now drops entries already present in `downloaded_references`.
- Duplicate handling (Jan 26, 2026): `promote_to_with_metadata` now drops duplicates found in `with_metadata` too; JSON ingestion now checks title/authors/year in `check_if_exists`.
- Duplicate handling tests (Jan 27, 2026): added db-level tests covering `promote_to_with_metadata` dedupe and JSON ingestion title/author/year skips.
- Duplicate handling upgrades (Jan 27, 2026): alias matching now allows ±1 year; new `merge_log` table records dedupe decisions; duplicates are merged into the highest-priority survivor (downloaded > queue > with_metadata > no_metadata) with field backfill; pipeline ingestion can resolve identifiers for potential duplicates before insertion.
- Duplicate handling tests (Jan 28, 2026): added tests for alias-year tolerance and merge_log entries.
- CLI inspection update (Jan 28, 2026): `inspect-tables` now includes `merge_log` for dedupe auditing.
- Pipeline smoke test (Jan 28, 2026): `run-pipeline completed/2004_kapitel-02.pdf --max-ref-pages 1 --no-fetch-references` extracted refs and started enrichment; run manually aborted to avoid long OpenAlex batch, status recorded as `aborted` in `pipeline_runs`.
- Pipeline helper update (Jan 28, 2026): `run-pipeline` supports `--max-entries` to cap processed references for faster smoke tests.
- Pipeline helper update (Jan 28, 2026): `run-pipeline` supports `--no-enrich` to skip OpenAlex enrichment and queueing for fast extraction-only smoke tests.
- Pipeline smoke test (Jan 28, 2026): `run-pipeline completed/2004_kapitel-02.pdf --max-ref-pages 1 --max-entries 5 --no-fetch-references --no-fetch-citations` completed on `literature_smoke.db` (5 inserted, 1 enriched failed, 0 queued).
- Pipeline smoke test (Jan 28, 2026): `run-pipeline completed/2004_kapitel-02.pdf --max-ref-pages 1 --max-entries 5 --no-enrich` completed on `literature_smoke.db` (5 inserted, 0 enriched, 0 queued).
- Test sweep (Jan 28, 2026): `pytest dl_lit_project/tests` → 20 passed after updating CLI/test mocks for new GenAI Files API and extract‑pages behavior.

## Proposed PR/Commit Sequence
1) OpenAlex enrichment fixes + rate limiter normalization.
2) Pipeline stabilization + orchestration helper.
3) Keyword search module + DB tables + CLI wiring.
4) Download cleanup + tests.

## Notes / Risks
- Gemini API dependency is required for extraction paths; mock or guard in tests.
- OpenAlex rate limits should be enforced centrally via `get_global_rate_limiter()`.
- Root-level `dl_lit/` appears legacy; avoid mixing imports.
