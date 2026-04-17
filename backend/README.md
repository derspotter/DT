# Backend API

Node.js/Express service for auth, corpus management, ingestion orchestration, and job queueing.

## Run

- Install deps: `npm install`
- Dev: `npm run dev`
- Prod-like: `npm start`
- Tests: `npm test`

Default port: `4000`

## What This Service Does

- Accepts uploads (`/api/process_pdf`)
- Starts bibliography extraction (`/api/extract-bibliography/:filename`)
- Exposes ingest/corpus/download/graph/log APIs
- Writes background jobs to `pipeline_jobs`
- Streams logs/events through WebSocket (`/api/ws`)

Heavy enrichment/download work is executed by the Python daemon in `backend/scripts/daemon/worker.py`.

## Queue-First Endpoints (Current Behavior)

- `POST /api/ingest/process-marked`
  - Inserts `job_type='enrich'` into `pipeline_jobs`
- `POST /api/downloads/worker/start`
  - Inserts `job_type='download'` into `pipeline_jobs`
- `POST /api/downloads/worker/run-once`
  - Inserts `job_type='download'` into `pipeline_jobs`

## Pipeline Worker Control Endpoints

- `POST /api/pipeline/worker/start`
- `POST /api/pipeline/worker/pause`
- `POST /api/pipeline/worker/pause-all`

These currently mutate in-memory API state for UI controls.
Continuous interval scheduling logic exists in code but is in a transitional wiring state.

## Extraction Path (Upload -> Ingest)

`/api/extract-bibliography/:filename` runs background Python scripts:

1. `get_bib_pages.py` for reference-page extraction
2. `APIscraper_v2.py` to parse refs and insert into DB
3. Inline fallback (`extract-mode inline`) when page extraction finds no bibliography pages

## Key Files

- `src/index.js`: HTTP server + WebSocket + log fanout
- `src/app.js`: API routes + queue insertion + Python spawning
- `scripts/daemon/worker.py`: `pipeline_jobs` consumer
- `scripts/ingest_enqueue.py`: marks selected ingest rows for enrichment
- `scripts/migrate_to_works.py`: one-time canonical schema migration

## Environment

- `RAG_FEEDER_DB_PATH`
- `RAG_FEEDER_PYTHON`
- `RAG_FEEDER_JWT_SECRET`
- `GOOGLE_API_KEY` or `GEMINI_API_KEY`
- `OPENALEX_API_KEY` (recommended)
- `RAG_FEEDER_MAILTO`
- `RAG_FEEDER_OPENALEX_RPS` (default `30`)
- `RAG_FEEDER_CROSSREF_RPS` (default `20`)
- `RAG_FEEDER_ENRICH_WORKERS` (default `6`)
- `RAG_FEEDER_LOG_DIR`
- `RAG_FEEDER_VPN_PROXY_URL` (optional SOCKS/HTTP proxy for download fallback)
- `RAG_FEEDER_VPN_MODE` (`fallback` | `prefer` | `force`, default `fallback`)
- `RAG_FEEDER_VPN_ENFORCE_EDUVPN` (default `1`; blocks proxy route unless eduVPN is connected)
- `RAG_FEEDER_VPN_STATUS_CMD` (required when enforcement is on; e.g. `ssh jayjag@desktop 'eduvpn-cli status'`)
- `RAG_FEEDER_VPN_CONNECT_CMD` (optional auto-connect command, run when status is disconnected)
