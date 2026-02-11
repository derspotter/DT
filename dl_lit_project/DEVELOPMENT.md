# Development Notes

## Canonical package and runtime

Use `dl_lit_project/dl_lit` as the authoritative Python package for CLI and pipeline work.

Do not import from the root-level legacy folder `dl_lit/`.

Backend runtime now resolves project paths defensively and only accepts a valid `dl_lit_project/dl_lit` tree.
Python worker scripts use shared bootstrap logic from `backend/scripts/_bootstrap.py`.

## Canonical entrypoints

- CLI module: `python -m dl_lit.cli`
- Backend API service: `backend/src/app.js`
- Worker scripts (backend): `backend/scripts/*.py`
- Tests: `dl_lit_project/tests/`, `backend/tests/`, `frontend/tests/`

## M-01 module audit status

- `get_bib_pages.py`: modernized to `google-genai` Files API, supports both positional input and `--input-pdf`/`--input-dir`.
- `APIscraper_v2.py`: normalized argument handling and DB-first insertion path; cleaned startup noise.
- `OpenAlexScraper.py`: remains canonical enrichment/search helper for DB pipeline.
- `new_dl.py`: canonical location of `BibliographyEnhancer`; legacy standalone script mode is retained for compatibility but DB-first CLI is preferred.
- `cli.py`: central orchestration surface (`run-pipeline`, `keyword-search`, `enrich-openalex-db`, queue/download commands).

## DB-first processing guidance

Prefer this stage flow:
`no_metadata -> with_metadata -> to_download_references -> downloaded_references`

Ingestion and queue transitions should always pass through `DatabaseManager` helpers to preserve dedupe behavior, merge-log recording, and corpus mapping.

## Library notes

- `fitz` is the import name for **PyMuPDF**.
- Gemini extraction/enrichment uses `google-genai`; API key is read from `GOOGLE_API_KEY` or `GEMINI_API_KEY`.
