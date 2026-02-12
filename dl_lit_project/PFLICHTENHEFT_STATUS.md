# Pflichtenheft Status (Working)

This file tracks implementation status against `Pflichtenheft_Korpus_Builder.md`.

## M-01 Code-Audit & Refactoring

Status: `completed` for Phase-1 scope.

- Audited and stabilized targeted modules: `get_bib_pages.py`, `APIscraper_v2.py`, `OpenAlexScraper.py`, `new_dl.py`, `cli.py`.
- Backend runtime now enforces canonical `dl_lit_project/dl_lit` resolution.
- Worker script import bootstrapping consolidated via `backend/scripts/_bootstrap.py`.
- CLI and DB-first stage flow are documented and tested.

## M-02 Keyword-Suche

Status: `completed`.

- Boolean operator support (`AND`/`OR`/`NOT`) with query normalization.
- Input modes: free-text query and JSON seed.
- Recursive source expansion implemented (`--related-depth`, `--max-related`).
- Search run/result persistence in SQLite.

## M-03 Visualisierung

Status: `deferred (nice-to-have for now)`.

- Graph API and UI exist in basic form.
- Further visualization work intentionally deferred while ingestion/search/download are prioritized.

## M-04 Frontend

Status: `completed` for current scope.

- Svelte/Vite frontend in production use for ingest/search/corpus/download/logs/graph.
- DB-backed ingest selection/queueing and download controls implemented.
- Final export interface implemented: JSON, BibTeX, PDFs ZIP, and Bundle ZIP downloads from current corpus, with status/year/source filters.
- URL-based tab routing + auth/corpus selection are integrated.

## M-05 Dockerisierung

Status: `completed`.

- `docker-compose.yml` includes frontend + backend services.
- Persistent DB and artifact paths are wired.
- Hot-reload workflow is available for local development.

## M-06 Dokumentation

Status: `completed` baseline, expandable.

- `dl_lit_project/README.md` added with setup and DB-first workflow.
- `dl_lit_project/DEVELOPMENT.md` expanded with canonical architecture and module guidance.
- `dl_lit_project/pyproject.toml` + `requirements-dev.txt` include a lint baseline (`ruff`).
