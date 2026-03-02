# dl_lit_project

Canonical Python pipeline package used by the app (`dl_lit`).

## Canonical Paths

- Package: `dl_lit_project/dl_lit`
- DB: `dl_lit_project/data/literature.db`
- CLI module: `python -m dl_lit.cli`

Root-level `dl_lit/` is legacy.

## DB-First Model

Core staged tables:

- `no_metadata`
- `with_metadata`
- `downloaded_references`

Queueing for downloads is currently state-based in `with_metadata.download_state`.

Related tables still in use:

- `failed_enrichments`
- `failed_downloads`
- `ingest_entries`
- `corpus_items`
- `pipeline_jobs`
- `pipeline_runs`

## Worker Architecture (Current)

Backend inserts rows into `pipeline_jobs`.
Python daemon (`backend/scripts/daemon/worker.py`) polls pending jobs and executes:

- `enrich`
- `download`
- `pipeline_tick` (mark -> enrich -> download)

Results are written to `pipeline_jobs.result_json`.

## CLI: Direct Operations

Run from `dl_lit_project/`.

Extraction:

- `python -m dl_lit.cli extract-bib-pages <pdf-or-dir> --output-dir artifacts/bibliographies`
- `python -m dl_lit.cli extract-bib-api artifacts/bibliographies/<run_dir> --db-path data/literature.db`

Enrichment:

- `python -m dl_lit.cli enrich-openalex-db --db-path data/literature.db --batch-size 50`
- `python -m dl_lit.cli retry-failed-enrichments --db-path data/literature.db`

Downloads:

- `python -m dl_lit.cli process-downloads --db-path data/literature.db --batch-size 20`
- `python -m dl_lit.cli download-pdfs --db-path data/literature.db --limit 20`

End-to-end helper:

- `python -m dl_lit.cli run-pipeline <pdf-or-dir> --db-path data/literature.db`

Keyword search:

- `python -m dl_lit.cli keyword-search --query "institution AND governance" --db-path data/literature.db`
- `python -m dl_lit.cli keyword-search --seed-json '[{\"openalex_id\":\"W2015930340\"}]' --db-path data/literature.db`

## Environment

Required:

- `GOOGLE_API_KEY` (or `GEMINI_API_KEY`)

Recommended:

- `OPENALEX_API_KEY`
- `RAG_FEEDER_MAILTO`
- `RAG_FEEDER_OPENALEX_RPS` (default `30`)
- `RAG_FEEDER_CROSSREF_RPS` (default `20`)
- `RAG_FEEDER_ENRICH_WORKERS` (default `6`)

## Docs

- `docs/USER_GUIDE.md`
- `docs/OPS_RUNBOOK.md`
- `DEVELOPMENT.md`
