# Frontend App

Svelte + Vite UI for corpus management, ingest, enrichment queueing, download monitoring, logs, and graph views.

## Run

- Install deps: `npm install`
- Dev: `npm run dev`
- Build: `npm run build`
- Preview: `npm run preview`
- E2E: `npm run test:e2e`

## Backend Connection

Set `VITE_API_BASE` (example: `http://localhost:4000`).

In Docker compose this is already configured.

## Current Interaction Model

- Upload PDFs via `/api/process_pdf`
- Trigger extraction via `/api/extract-bibliography/:filename`
- Load staged ingest rows via `/api/ingest/latest`
- Mark entries via `/api/ingest/enqueue`
- Queue enrichment via `/api/ingest/process-marked`
- Queue downloads via `/api/downloads/worker/start` or `/api/downloads/worker/run-once`

The UI is mostly queue-oriented: actions enqueue jobs, daemon executes them asynchronously, UI polls status/log endpoints.

## Important Note About "Global Pipeline" Buttons

The dashboard "Start / Continue" and pause controls call `/api/pipeline/worker/*`.
These endpoints currently reflect in-memory control state; immediate queueing actions are the ingest/download enqueue endpoints above.

## Key Files

- `src/App.svelte`: main orchestration, polling, and action handlers
- `src/components/`: dashboard/corpus/graph/logs views
- `src/lib/api.js`: API client wrappers
