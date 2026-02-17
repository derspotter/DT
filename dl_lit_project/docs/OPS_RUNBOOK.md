# Ops Runbook

## Services

- Backend API: `rag_backend` (`:4000`)
- Frontend: `rag_feeder` (`:5175`)
- DB file: `dl_lit_project/data/literature.db`

## Start, stop, restart

- Start:
  - `docker compose up -d`
- Restart backend only:
  - `docker compose restart rag_backend`
- Stop all:
  - `docker compose down`

## Health checks

- Backend:
  - `curl -s http://localhost:4000/api/health`
- Frontend:
  - `curl -I http://localhost:5175/`
- Logs:
  - `docker logs -f rag_feeder_backend`
  - `docker logs -f rag_feeder_frontend`
  - persistent files:
    - `tail -f logs/backend-app.log`
    - `tail -f logs/backend-pipeline.log`

## Required environment

Set in `.env` (or compose env):

- `GOOGLE_API_KEY` (or `GEMINI_API_KEY`) for extraction
- `OPENALEX_API_KEY` for higher OpenAlex quota
- `RAG_FEEDER_JWT_SECRET` for stable auth tokens

Optional throughput controls:

- `RAG_FEEDER_OPENALEX_RPS`
- `RAG_FEEDER_CROSSREF_RPS`
- `RAG_FEEDER_ENRICH_WORKERS`

Optional logging controls:

- `RAG_FEEDER_LOG_DIR` (default: `<repo>/logs`)
- `RAG_FEEDER_LOG_MAX_BYTES` (default: `10485760`)
- `RAG_FEEDER_LOG_MAX_FILES` (default: `5`)

## Log API

- Tail latest lines:
  - `curl -H "Authorization: Bearer <token>" "http://localhost:4000/api/logs/tail?type=pipeline&lines=200"`
- Fetch older page using cursor:
  - `curl -H "Authorization: Bearer <token>" "http://localhost:4000/api/logs/tail?type=pipeline&lines=200&cursor=200"`
- App logs instead of pipeline logs:
  - `curl -H "Authorization: Bearer <token>" "http://localhost:4000/api/logs/tail?type=app&lines=100"`

Response fields:
- `entries`: log lines in chronological order
- `has_more`: whether older lines are available
- `next_cursor`: cursor for the next "older" page
- `total_lines`: total merged lines (including rotated files if enabled)

## Data safety

- Keep `dl_lit_project/data/literature.db` on persistent storage.
- Backup DB before schema or pipeline changes:
  - `cp dl_lit_project/data/literature.db dl_lit_project/data/literature.db.bak.$(date +%F-%H%M%S)`

## Common failures

### Invalid JSON from backend

- Symptom: frontend error like `Unexpected token ... is not valid JSON`.
- Cause: mixed Python logs/JSON output.
- Action: ensure backend contains hardened parser in `backend/src/app.js`, then restart backend.

### Empty/stale stage counts

- Symptom: UI counts disagree with visible rows.
- Cause: stale `corpus_items` mappings.
- Action: backend startup now prunes stale mappings; restart backend once.

### OpenAlex throughput too low

- Verify `OPENALEX_API_KEY` is loaded in container env.
- Raise `RAG_FEEDER_OPENALEX_RPS` gradually.
- Watch logs for HTTP 429/5xx retries.

## Verification commands

- Python tests:
  - `venv/bin/pytest -q dl_lit_project/tests`
- Backend tests:
  - `cd backend && npm test -- --runInBand`
- Frontend build:
  - `cd frontend && npm run build`
