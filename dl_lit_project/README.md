# dl_lit_project

Canonical Python package and data model for the Korpus Builder application.

This directory contains the authoritative Python package `dl_lit` and the canonical SQLite database used by the web app and worker services.

## What This Project Does

The Korpus Builder supports two main literature-building workflows:

1. start from a keyword search
2. start from a seed document such as a PDF

The application then:

- stores candidate references
- promotes selected references into a corpus
- enriches metadata
- downloads PDFs where possible
- exposes graph, logs, and export views

## Canonical Paths

- Package: `dl_lit_project/dl_lit`
- Database: `dl_lit_project/data/literature.db`
- CLI module: `python -m dl_lit.cli`

Root-level `dl_lit/` is legacy and should not be used for new runtime work.

## Canonical Data Model

The app now uses a single canonical work model.

Primary runtime tables:

- `works`
- `corpus_works`
- `work_aliases`
- `ingest_entries`
- `ingest_source_metadata`
- `pipeline_jobs`
- `pipeline_runs`

Work lifecycle is represented directly on `works`:

- `metadata_status`: `pending | in_progress | matched | failed`
- `download_status`: `not_requested | queued | in_progress | downloaded | failed`

This replaces the earlier multi-table stage model.

## Worker Architecture

The backend enqueues work into `pipeline_jobs`.

Python workers consume those jobs and operate against the canonical `works` table:

- enrich worker
- download worker
- pipeline tick orchestration

Results are written back into the database and surfaced through the web frontend.

## Quick Start

Run from the repository root.

1. Set required environment in `.env`
   - at minimum: `GOOGLE_API_KEY` or `GEMINI_API_KEY`
2. Start the stack:

```bash
docker compose up -d
```

3. Open the frontend:
   - `http://localhost:5175`
   - or the configured deployed URL

## Useful CLI Commands

Run from `dl_lit_project/`.

Keyword search:

```bash
python -m dl_lit.cli keyword-search --query "institution AND governance" --db-path data/literature.db
python -m dl_lit.cli keyword-search --seed-json '[{"openalex_id":"W2015930340"}]' --db-path data/literature.db
```

Bibliography extraction:

```bash
python -m dl_lit.cli extract-bib-pages <pdf-or-dir> --output-dir artifacts/bibliographies
python -m dl_lit.cli extract-bib-api artifacts/bibliographies/<run_dir> --db-path data/literature.db
```

Download processing:

```bash
python -m dl_lit.cli process-downloads --db-path data/literature.db --batch-size 20
python -m dl_lit.cli download-pdfs --db-path data/literature.db --limit 20
```

## Environment

Required:

- `GOOGLE_API_KEY` or `GEMINI_API_KEY`

Recommended:

- `OPENALEX_API_KEY`
- `SEMANTIC_SCHOLAR_API_KEY`
- `RAG_FEEDER_MAILTO`

Useful throughput settings:

- `RAG_FEEDER_OPENALEX_RPS`
- `RAG_FEEDER_CROSSREF_RPS`
- `RAG_FEEDER_SEMANTIC_SCHOLAR_RPS`
- `RAG_FEEDER_SEMANTIC_SCHOLAR_MIN_INTERVAL_SEC`
- `RAG_FEEDER_ENRICH_WORKERS`
- `RAG_FEEDER_DOWNLOAD_WORKERS`

Mail settings for invites and password resets are configured at the repository root `.env`, not inside this directory alone.

## Documentation

Presentable documentation for the current system lives here:

- user guide: `docs/USER_GUIDE.md`
- admin guide: `docs/ADMIN_GUIDE.md`
- ops runbook: `docs/OPS_RUNBOOK.md`
- development notes: `DEVELOPMENT.md`
 
## Safety

Before schema changes or repair scripts, back up:

- `dl_lit_project/data/literature.db`

The application keeps works globally and maps them into corpora separately. Do not assume that deleting a corpus should delete the underlying canonical work records.
