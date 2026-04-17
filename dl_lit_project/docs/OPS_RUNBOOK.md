# Ops Runbook

## Scope

This runbook is for operating the current Docker-based stack locally or on the target host.

It covers:

- services
- environment
- startup and restart
- health checks
- logs
- backups
- common failure modes

## Services

The current compose setup defines these main services:

- `rag_backend`
  - container: `rag_feeder_backend`
  - API / orchestration
- `rag_enrich_worker`
  - container: `rag_feeder_enrich_worker`
  - metadata enrichment worker
- `rag_download_worker`
  - container: `rag_feeder_download_worker`
  - download worker
- `rag_feeder`
  - container: `rag_feeder_frontend`
  - frontend on port `5175`

Primary data path:

- `dl_lit_project/data/literature.db`

Primary canonical runtime tables:

- `works`
- `corpus_works`

## Runtime Model

The application now uses a canonical work model.

State is stored directly on `works`:

- `metadata_status`: `pending | in_progress | matched | failed`
- `download_status`: `not_requested | queued | in_progress | downloaded | failed`

Corpus membership is stored in:

- `corpus_works`

This means:

- works are global
- corpora are views/memberships over those works
- deleting a corpus does not delete the canonical work record

## Environment

Important variables live in the repository root `.env`.

Minimum useful configuration:

- `GOOGLE_API_KEY` or `GEMINI_API_KEY`
- `RAG_FEEDER_JWT_SECRET`

Recommended API configuration:

- `OPENALEX_API_KEY`
- `SEMANTIC_SCHOLAR_API_KEY`
- `RAG_FEEDER_MAILTO`

SMTP for invite/reset mail:

- `RAG_FEEDER_FRONTEND_URL`
- `RAG_FEEDER_SMTP_HOST`
- `RAG_FEEDER_SMTP_PORT`
- `RAG_FEEDER_SMTP_SECURE`
- `RAG_FEEDER_SMTP_USER`
- `RAG_FEEDER_SMTP_PASS`
- `RAG_FEEDER_SMTP_FROM`
- `RAG_FEEDER_SMTP_TLS_REJECT_UNAUTHORIZED`
- `RAG_FEEDER_SMTP_IGNORE_TLS`

Useful throughput controls:

- `RAG_FEEDER_OPENALEX_RPS`
- `RAG_FEEDER_CROSSREF_RPS`
- `RAG_FEEDER_SEMANTIC_SCHOLAR_RPS`
- `RAG_FEEDER_ENRICH_WORKERS`
- `RAG_FEEDER_DOWNLOAD_WORKERS`
- `RAG_FEEDER_DOWNLOAD_BATCH_SIZE`

## Start and Stop

Start the full stack:

```bash
docker compose up -d
```

Restart backend only:

```bash
docker compose restart rag_backend
```

Restart frontend only:

```bash
docker compose restart rag_feeder
```

Restart workers:

```bash
docker compose restart rag_enrich_worker rag_download_worker
```

Stop all:

```bash
docker compose down
```

## Development vs Live Behavior

The current compose stack runs the frontend through the Vite dev server and the backend through nodemon-based dev mode.

That means:

- frontend edits should hot reload in the compose stack
- backend source edits should restart automatically

Still rebuild images when:

- dependencies changed
- Dockerfiles changed
- native modules were rebuilt

## Health Checks

Backend:

```bash
curl -s http://localhost:4000/api/health
```

Frontend:

```bash
curl -I http://localhost:5175/
```

Container state:

```bash
docker compose ps
```

## Logs

Container logs:

```bash
docker logs -f rag_feeder_backend
docker logs -f rag_feeder_enrich_worker
docker logs -f rag_feeder_download_worker
docker logs -f rag_feeder_frontend
```

Persistent log files:

- `logs/backend-app.log`
- `logs/backend-pipeline.log`

Tail them directly:

```bash
tail -f logs/backend-app.log
tail -f logs/backend-pipeline.log
```

Log API examples:

```bash
curl -H "Authorization: Bearer <token>" "http://localhost:4000/api/logs/tail?type=pipeline&lines=200"
curl -H "Authorization: Bearer <token>" "http://localhost:4000/api/logs/tail?type=app&lines=100"
```

## Backups

Back up the database before risky operations:

- schema changes
- bulk repair scripts
- destructive corpus cleanup
- import/export experiments that may mutate state

Quick backup:

```bash
cp dl_lit_project/data/literature.db dl_lit_project/data/literature.db.bak.$(date +%F-%H%M%S)
```

For major changes, also copy the WAL and SHM files if they exist and the DB is live:

```bash
cp dl_lit_project/data/literature.db-wal dl_lit_project/data/literature.db-wal.bak.$(date +%F-%H%M%S) 2>/dev/null || true
cp dl_lit_project/data/literature.db-shm dl_lit_project/data/literature.db-shm.bak.$(date +%F-%H%M%S) 2>/dev/null || true
```

## Verification Commands

Python tests:

```bash
venv/bin/pytest -q dl_lit_project/tests
```

Backend tests:

```bash
cd backend && npm test -- --runInBand
```

Frontend build:

```bash
cd frontend && npm run build
```

## Common Failure Modes

### Frontend shows stale or inconsistent corpus data

Typical causes:

- race between refresh requests
- stale browser state
- a previous frontend bundle still cached

Actions:

1. hard refresh the browser
2. restart `rag_feeder` if needed
3. confirm backend API output directly if the UI still looks wrong

### Extraction appears stuck

Typical causes:

- bibliography section detection is still running
- inline fallback is active
- frontend status text is behind the backend completion signal

Actions:

1. check `logs/backend-pipeline.log`
2. check backend container logs
3. confirm whether extraction actually finished and the issue is only UI status

### No recent downloads visible

Interpretation:

- `Queued DL: 0` and `Downloading: 0` only mean no active backlog
- they do not mean no downloaded works exist

Check:

- `Downloads` tab for recent downloaded items
- `Pipeline Status` for current backlog

### Invite or reset emails are not delivered

Check:

1. SMTP variables in `.env`
2. backend logs for SMTP auth errors
3. whether the mail provider requires an app password

Current known-good pattern:

- Gmail SMTP with an app password

### Worker appears idle but corpus is unfinished

Interpretation:

- the selected corpus may still have failed or pending works even if no downloads are queued right now

Check:

- `Pipeline Status`
- `Downloads`
- worker logs
- corpus row statuses in the UI

## Safe Restart Strategy

When behavior is unclear, restart in this order:

1. `rag_backend`
2. `rag_feeder`
3. `rag_enrich_worker`
4. `rag_download_worker`

This usually resolves stale API/frontend state without touching the database.
