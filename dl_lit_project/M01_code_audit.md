# M-01 Code Audit & Refactoring Closure

Date: 2026-02-16

Scope audited (Pflichtenheft M-01):
- `dl_lit/get_bib_pages.py`
- `dl_lit/APIscraper_v2.py`
- `dl_lit/OpenAlexScraper.py`
- `dl_lit/new_dl.py`
- `dl_lit/cli.py`

## Completed refactoring actions

1. Runtime extraction flow was consolidated for DB-first ingestion:
   - fallback from bibliography-page extraction to inline citation extraction is now orchestrated in backend.
   - for PDFs with <= 50 pages, Gemini file upload is reused between `get_bib_pages.py` and `APIscraper_v2.py` fallback (no redundant second upload).
2. `get_bib_pages.py` and `APIscraper_v2.py` now support `--uploaded-file-name` for explicit file reuse via Gemini Files API.
3. `APIscraper_v2.py` supports explicit extraction mode controls (`page`/`inline`) and chunking controls.
4. Code hygiene cleanup across audited modules:
   - removed unused imports and dead locals in `OpenAlexScraper.py`, `new_dl.py`, `cli.py`, `APIscraper_v2.py`.
   - verified with `ruff` (`F401`, `F841`) on all audited modules.

## Consolidation status

- Database pipeline is canonicalized around staged tables (`no_metadata` -> `with_metadata` -> `downloaded_references`) with corpus scoping via `corpus_items`.
- CLI surface is stable and DB-first commands remain canonical (`extract-bib-pages`, `extract-bib-api`, `run-pipeline`, `keyword-search`, `enrich-openalex-db`, download commands).
- Legacy compatibility is preserved; behavior is aligned to DB-first runtime.

## Verification

- `ruff` audit checks passed for audited modules.
- CLI/API scraper regression subset passed:
  - `test_apiscraper_v2_args.py`
  - `test_get_bib_pages.py`
  - `test_cli_keyword_search.py`
  - `test_cli_enricher.py`
  - `test_cli_downloader.py`
