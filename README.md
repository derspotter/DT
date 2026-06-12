# RAG Feeder

Monorepo for PDF ingestion, bibliography extraction, metadata enrichment, and download queueing.

## Repository Layout

- `frontend/`: Svelte + Vite app (incl. the 3D graph explorer, `src/components/ThreeGraph.svelte`)
- `backend/`: Node.js API and orchestration routes; `backend/scripts/` holds the Python entrypoints (graph export, OpenAlex/citation backfills, seed import) that import the `dl_lit` package
- `backend/scripts/daemon/worker.py`: queue consumer daemon
- `dl_lit_project/`: canonical Python pipeline package (`dl_lit`) + docs (`README.md`, `docs/`)
- `experiments/`: scratch / experimental scripts (not part of the runtime)
- `docs/archive/`: historical planning notes

## Current Runtime Architecture

The app is DB-first and queue-first.

1. Backend writes jobs to `pipeline_jobs` in `dl_lit_project/data/literature.db`.
2. `rag_feeder_worker` polls `pipeline_jobs` and executes jobs.
3. Worker writes completion/failure payloads back to `pipeline_jobs.result_json`.

Supported daemon job types in current code:

- `enrich`
- `download`
- `pipeline_tick` (mark -> enrich -> download)

## Data Model

Primary runtime tables:

- `works`: canonical work records, including metadata/download status and file info
- `corpus_works`: corpus membership join table

Status lives directly on `works`:

- `metadata_status`: `pending | in_progress | matched | failed`
- `download_status`: `not_requested | queued | in_progress | downloaded | failed`

## Services (docker compose)

- `rag_feeder_frontend` on `http://localhost:5175`
- `rag_feeder_backend` on `http://localhost:4000`
- `rag_feeder_enrich_worker` and `rag_feeder_download_worker` (no HTTP port)

## Quick Start

1. Set `.env` values (at least `GOOGLE_API_KEY`, optional `OPENALEX_API_KEY`).
2. Start stack:
   - `docker compose up -d`
3. Open:
   - `http://localhost:5175`

The production site does not hot reload. Rebuild the frontend when you want changes on the live stack.

## Live Deploys Behind Caddy

Do not patch tracked compose files on the server. Keep the live host and Caddy labels in an untracked `docker-compose.override.yml` instead so a `git pull` cannot wipe them.

1. Add the live host to `.env`:
   - `RAG_FEEDER_PUBLIC_HOST=corpus4uol.university-of-labour.de`
   - `RAG_FEEDER_PROXY_NETWORK=reverse_proxy`
2. Create a local override once:
   - `cp docker-compose.override.example.yml docker-compose.override.yml`
3. Deploy normally after pulls:
   - `docker compose up -d`

`docker compose` loads `docker-compose.override.yml` automatically, so the frontend keeps its `caddy` labels and host overrides without requiring a special command.

## Paths You Actually Use

- SQLite DB: `dl_lit_project/data/literature.db`
- Uploaded PDFs inside container: `/usr/src/app/uploads`
- Upload volume: `rag_feeder_uploads`
- Logs volume: `rag_feeder_logs`
- Pipeline log file: `/usr/src/app/logs/backend-pipeline.log`

## Important Implementation Note

`/api/ingest/process-marked`, `/api/downloads/worker/start`, and `/api/downloads/worker/run-once` queue real jobs immediately.

`/api/pipeline/worker/start` and `/api/pipeline/worker/pause` currently update in-memory API state; continuous interval scheduling is transitional in current implementation.

## Development & Testing

Local setup:

- `cp .env.example .env` and fill in keys (`GOOGLE_API_KEY` required; `OPENALEX_API_KEY`, `RAG_ADMIN_USER`/`RAG_ADMIN_PASSWORD`, `RAG_FEEDER_JWT_SECRET` as needed). The bootstrap admin password is re-synced from `RAG_ADMIN_PASSWORD` on every backend start.
- `docker compose up -d` runs the full stack; backend (`backend/src`) and Python scripts (`backend/scripts`) are bind-mounted and hot-reload via nodemon.

Run the tests:

- Backend (Jest, stub mode — no DB needed): `cd backend && npm ci && npm test`
- Frontend build + mocked e2e: `cd frontend && npm ci && npx vite build && npx playwright install chromium && npx playwright test tests/graph-3d.spec.ts tests/seed-upload.spec.ts`
- Python lint: `python -m ruff check backend/scripts`
- Live/authenticated e2e specs are opt-in via env flags (`RUN_SEED_UPLOAD_REAL=1`, `RUN_REAL_PDF_PERF=1`) and need a running backend + `RAG_ADMIN_*`.

CI (`.github/workflows/ci.yml`) runs the backend suite and the frontend build + mocked e2e on every PR.

One-off maintenance scripts (`backend/scripts/`, write to the live DB, re-runnable):

- `backfill_openalex_topics.py` — OpenAlex topic/field/domain columns (powers the 3D graph's field clustering)
- `backfill_citation_edges.py` — in-corpus citation edges from `referenced_works`
- `ingest_import_seed.py` — used by the `.bib`/`.json` web upload

## Read More

- Backend details: `backend/README.md`
- Frontend details: `frontend/README.md`
- Python pipeline details: `dl_lit_project/README.md`
- Architecture & web-app notes: `dl_lit_project/CLAUDE.md`
