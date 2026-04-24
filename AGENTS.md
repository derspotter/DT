# Repository Guidelines

## What This Repo Is
- Monorepo for PDF ingestion, bibliography extraction, metadata enrichment, corpus management, and download processing.
- The runtime is split across a Svelte frontend, a Node/Express backend, and Python pipeline code under `dl_lit_project/`.

## Project Structure & Entry Points
- `frontend/`: Svelte 5 + Vite app. Main app shell is `frontend/src/App.svelte`; global styles are in `frontend/src/app.css`; API client is `frontend/src/lib/api.js`.
- `backend/`: Node.js API and orchestration layer. HTTP/WebSocket entry is `backend/src/index.js`; most route and pipeline logic lives in `backend/src/app.js`; seed-source logic lives in `backend/src/seed.js`.
- `backend/scripts/daemon/worker.py`: background worker for enrich/download jobs.
- `dl_lit_project/`: canonical Python package and tests for extraction/enrichment. Prefer this over the legacy top-level `dl_lit/` folder when tracing current pipeline behavior.
- `docker-compose.yml`: primary local/dev stack definition and the first place to check for ports, volume mounts, env wiring, and container names.
- `README.md`: best high-level architecture overview. Use this before broader code exploration.

## Files That Give The Fastest App Overview
When orienting on a feature, prefer these files in roughly this order:
- `README.md`
- `docker-compose.yml`
- `frontend/src/App.svelte`
- `frontend/src/lib/api.js`
- `backend/src/app.js`
- `backend/src/seed.js`

`AGENTS.md` is a working guide, not the source of truth for behavior. Treat the code and compose files as authoritative.

## Runtime & Access Notes
- Backend container name: `rag_feeder_backend`
- Frontend container name: `rag_feeder_frontend`
- Public app URL is configured through `RAG_FEEDER_FRONTEND_URL` in `.env`.
- In Docker compose, the frontend dev server is published as `127.0.0.1:5175:5175`. Shell access to `localhost:5175` may work even when browser-tool access to `127.0.0.1:5175` does not, due to loopback/network-context differences.
- For browser-driven validation, prefer the configured public URL when available instead of assuming `genkia` or local loopback.

## Build, Test, and Development Commands
Run commands from the relevant directory.
- Backend dev server: `cd backend && npm run dev`
- Backend production-style start: `cd backend && npm start`
- Backend tests: `cd backend && npm test`
- Frontend dev server: `cd frontend && npm run dev`
- Frontend build: `cd frontend && npm run build`
- Frontend preview: `cd frontend && npm run preview`
- Frontend e2e tests: `cd frontend && npm run test:e2e`
- Python tests: `cd dl_lit_project && pytest tests`
- Docker stack: `docker compose up -d`

## Coding Style & Conventions
- Backend JS uses ES modules and 2-space indentation.
- Frontend Svelte/CSS generally uses 2-space indentation in source files; preserve surrounding style instead of normalizing globally.
- Python code in `dl_lit_project/` follows the existing module style; prefer descriptive names and small targeted changes.
- Avoid broad rewrites in `App.svelte` or `backend/src/app.js`; both are large owning surfaces where local edits are safer.

## Testing Guidance
- For backend/API changes, prefer a narrow Jest file in `backend/tests/` before broader test runs.
- For Python pipeline changes, prefer a narrow pytest target in `dl_lit_project/tests/`.
- For frontend behavior/layout changes, validate in the browser and run the narrowest available automated check if one exists.
- If testing inside containers, remember some dev images may not include every convenience package by default.

## Persistence & UI State Notes
- The Document Search upload queue in `frontend/src/App.svelte` is frontend session state only.
- Persisted extracted PDFs reappear through the seed-source APIs, not by repopulating the upload list after reload.
- If a UI row disappears on hard reload, check whether it is backed by frontend-local state or by `fetchSeedSources` / related backend data.

## Configuration & Security Notes
- Keep secrets in `.env` files and never copy them into docs, commits, or test fixtures.
- Avoid committing generated PDFs, uploaded artifacts, logs, or model outputs unless explicitly required.
- Be careful when quoting runtime config from `.env`; summarize keys and behavior without reproducing secrets.

## Agent-Specific Notes
- Start from the concrete owning file for the feature you are changing rather than from a broad repo crawl.
- For runtime issues, verify behavior in both code and `docker-compose.yml`; many apparent app bugs are actually port, mount, or container-environment mismatches.
- Claude Code CLI note: if an invalid/stale `ANTHROPIC_API_KEY` is set in the shell, `claude` may fail with “Invalid API key”. Prefer unsetting it for Claude runs:
  - `env -u ANTHROPIC_API_KEY claude "<prompt>"`
  - `env -u ANTHROPIC_API_KEY claude --dangerously-skip-permissions "<prompt>"`
