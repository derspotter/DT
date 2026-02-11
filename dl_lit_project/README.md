# dl_lit_project

Database-first literature corpus builder for seed-document ingestion, keyword search, enrichment, and resilient PDF download queuing.

## What this project is now

Canonical Python package: `dl_lit_project/dl_lit`  
Canonical CLI: `python -m dl_lit.cli`  
Canonical database: `dl_lit_project/data/literature.db`

The root-level `dl_lit/` folder is legacy and not used by the backend runtime.

## Core workflow (DB-first)

1. Extract bibliography pages from a seed PDF:
   - `python -m dl_lit.cli extract-bib-pages <pdf-or-dir> --output-dir artifacts/bibliographies`
2. Parse extracted bibliography pages into DB (`no_metadata`):
   - `python -m dl_lit.cli extract-bib-api artifacts/bibliographies/<run_dir> --db-path data/literature.db`
3. Enrich selected rows via OpenAlex/Crossref:
   - `python -m dl_lit.cli enrich-openalex-db --db-path data/literature.db --batch-size 50`
4. Queue and process downloads:
   - `python -m dl_lit.cli process-downloads --db-path data/literature.db --batch-size 20`
   - `python -m dl_lit.cli download-pdfs --db-path data/literature.db --limit 20`

Single-command orchestration:
- `python -m dl_lit.cli run-pipeline <pdf-or-dir> --db-path data/literature.db`

## Keyword search (Milestone 1 scope)

Query mode:
- `python -m dl_lit.cli keyword-search --query "institution AND governance" --max-results 200 --db-path data/literature.db`

Seed-JSON mode (web backend uses the same script path):
- `backend/scripts/keyword_search.py --seed-json '[{"openalex_id":"W2015930340"}]' --db-path dl_lit_project/data/literature.db`

Supports recursive expansion:
- `--related-depth <n>` and `--max-related <n>`

## Web app

Use root `docker-compose.yml` services:
- backend: `rag_backend` on `:4000`
- frontend: `rag_feeder` on `:5175`

Frontend talks to backend via Vite API base during development.

## Development and quality checks

- Python tests: `venv/bin/pytest -q dl_lit_project/tests`
- Backend tests: `cd backend && npm test -- --runInBand`
- Lint (Python): `venv/bin/ruff check --config dl_lit_project/pyproject.toml dl_lit_project/dl_lit backend/scripts`

## Environment

Required for extraction/enrichment:
- `GOOGLE_API_KEY` (or `GEMINI_API_KEY`)

Optional but recommended:
- `RAG_FEEDER_JWT_SECRET`
- `RAG_FEEDER_GEMINI_MODEL` (default: `gemini-3-flash-preview`)
