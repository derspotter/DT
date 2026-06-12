# Architecture Refactor Plan: Node.js API + Python Daemon

## Current State & Issues
The current pipeline architecture relies on a "Script Orchestration" pattern where the Node.js server repeatedly spawns Python scripts (via CLI) using `setInterval` loops. 

**Problems:**
1. **High Overhead (Cold Starts):** Spinning up the Python interpreter, loading libraries, and establishing SQLite connections repeatedly is resource-intensive.
2. **Brittle Communication:** Node.js parses `stdout` to determine script success/failure. Any unexpected print statements can break the parser.
3. **Database Concurrency Risks:** Multiple concurrent DB connections from short-lived scripts can cause SQLite `database is locked` errors.
4. **State Management:** Tracking long-running job progress is difficult across transient processes.

## The Goal
Replace the Node.js `setInterval` orchestration with a single, persistent Python Daemon. Node.js will strictly act as an API/Frontend server, while the Python Daemon handles all heavy lifting (scraping, downloading, enriching) in the background.

---

## Phase 1: Database Job Queue Integration (COMPLETED)
Communicate between Node.js and the Python Daemon using a "Jobs" table in SQLite.
1. **Schema Update:** Create a `pipeline_jobs` table to track job types (`enrich`, `download`, `keyword_search`), statuses (`pending`, `running`, `completed`, `failed`), progress, and `corpus_id`.
2. **Node.js Update:** Refactor API endpoints (`/api/ingest/process-marked`, `/api/downloads/worker/start`, etc.) to insert rows into `pipeline_jobs` instead of spawning Python scripts.
3. **WebSocket Update:** Node.js reads the `pipeline_jobs` table to push real-time status updates to the frontend via WebSockets.

## Phase 2: Building the Python Daemon (COMPLETED)
1. **The Daemon Script:** Create a long-running Python process (`backend/scripts/daemon/worker.py`).
2. **The Loop:** Run a continuous loop polling the `pipeline_jobs` table for `status = 'pending'`.
3. **Task Routing:** Route fetched jobs to existing core logic (e.g., `DatabaseManager`, `OpenAlexScraper`).
4. **Persistent Connections:** Maintain a single, open SQLite connection and a persistent HTTP connection pool for APIs to drastically speed up processing.
5. **Logging:** Log directly to files or a database table, bypassing Node.js entirely.

## Phase 3: Docker Refactoring & Cleanup (COMPLETED)
1. **Docker Compose:** Add a new `rag_worker` service in `docker-compose.yml` to run `python backend/scripts/daemon/worker.py`.
2. **Clean Up:** Remove the orchestration functions (`startPipelineWorker`, `tickPipelineWorker`, `spawnDownloadOnce`) from `app.js`.

---

## Branching Strategy
To ensure a safe transition without breaking the currently deployed application:
* We will create a new git branch: `refactor/python-daemon`.
* All Phase 1-3 changes will be committed to this branch.
* If anything goes wrong, or if we need to quickly deploy a hotfix to the live app, we can simply switch back to `master`.
* Once the daemon is fully tested and stable, we will merge the branch back into `master`.