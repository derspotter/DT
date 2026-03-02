# RAG Feeder

Monorepo for PDF ingestion, bibliography extraction, metadata enrichment, and download queueing.

## Repository Layout

- `frontend/`: Svelte + Vite app
- `backend/`: Node.js API and orchestration routes
- `backend/scripts/daemon/worker.py`: queue consumer daemon
- `dl_lit_project/`: canonical Python pipeline package (`dl_lit`)
- `dl_lit/`: legacy scripts (not canonical runtime)

## Current Runtime Architecture

The app is DB-first and queue-first.

1. Backend writes jobs to `pipeline_jobs` in `dl_lit_project/data/literature.db`.
2. `rag_feeder_worker` polls `pipeline_jobs` and executes jobs.
3. Worker writes completion/failure payloads back to `pipeline_jobs.result_json`.

Supported daemon job types in current code:

- `enrich`
- `download`
- `pipeline_tick` (mark -> enrich -> download)

## Stage Flow

Primary data flow:

`no_metadata -> with_metadata -> downloaded_references`

Download queue state is tracked in `with_metadata.download_state` (`queued` / `in_progress` / etc.).
`to_download_references` remains in schema for compatibility but the current queue path is state-based.

## Services (docker compose)

- `rag_feeder_frontend` on `http://localhost:5175`
- `rag_feeder_backend` on `http://localhost:4000`
- `rag_feeder_worker` (no HTTP port)

## Quick Start

1. Set `.env` values (at least `GOOGLE_API_KEY`, optional `OPENALEX_API_KEY`).
2. Start stack:
   - `docker compose up -d`
3. Open:
   - `http://localhost:5175`

## Paths You Actually Use

- SQLite DB: `dl_lit_project/data/literature.db`
- Uploaded PDFs inside container: `/usr/src/app/uploads`
- Upload volume: `rag_feeder_uploads`
- Logs volume: `rag_feeder_logs`
- Pipeline log file: `/usr/src/app/logs/backend-pipeline.log`

## Important Implementation Note

`/api/ingest/process-marked`, `/api/downloads/worker/start`, and `/api/downloads/worker/run-once` queue real jobs immediately.

`/api/pipeline/worker/start` and `/api/pipeline/worker/pause` currently update in-memory API state; continuous interval scheduling is transitional in current implementation.

## Read More

- Backend details: `backend/README.md`
- Frontend details: `frontend/README.md`
- Python pipeline details: `dl_lit_project/README.md`
