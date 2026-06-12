# Pflichtenheft Status (Working)

This file tracks implementation status against `Pflichtenheft_Korpus_Builder.md`.
Last reviewed: 2026-06-12.

## M-01 Code-Audit & Refactoring

Status: `completed`.

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

Status: `completed`.

- A single Three.js/WebGL **3D Graph Explorer** is the graph UI (the legacy 2D SVG sampler was retired 2026-06-12). Nodes = works, edges = citation relationships.
- Layout is computed server-side at snapshot time with a real force-directed
  layout (igraph), grouped into territories. A **"Group by"** control
  re-arranges the graph by dimension — academic field, search path, work type,
  region, year decade, or connected component — satisfying the spec's
  "Farbliche Clustering-Optionen (Themen / Suchpfade)". Each territory gets a
  distinct colour; "Degree" is a colour-only overlay.
- Citation edges densified from OpenAlex `referenced_works`
  (`backend/scripts/backfill_citation_edges.py`): in-graph edges
  35,838 → 655,201, isolated works ~80% → 18%.
- Academic-field metadata backfilled from OpenAlex
  (`backend/scripts/backfill_openalex_topics.py`).
- Performance: stable snapshot cache + stale-while-revalidate + startup
  pre-warm; gzip delivery; whole-corpus (~73k) view supported. Default 10k view
  loads from cache instantly; full-corpus build ~136s (cached).
- Korpus Management also includes an upstream corpus map (imported
  `metadata.bib` baseline, pending local additions, citation edges,
  source/status/year clustering, node metrics, reference details).

## M-04 Frontend

Status: `completed`.

- Svelte/Vite frontend in production use for ingest/search/corpus/download/logs/graph.
- DB-backed ingest selection/queueing and download controls implemented.
- Corpus graph display and pipeline/progress indicators present.
- **Seed upload accepts PDF, BibTeX, and JSON.** PDFs go through bibliography
  extraction; `.bib`/`.json` seed files are imported straight into the current
  corpus via `POST /api/ingest/import-seed`
  (`backend/scripts/ingest_import_seed.py`). The keyword search also accepts a
  JSON seed.
- Final export interface implemented: JSON, BibTeX, PDFs ZIP, and Bundle ZIP downloads from current corpus, with status/year/source filters.
- URL-based tab routing + auth/corpus selection are integrated.

## M-05 Dockerisierung

Status: `completed`.

- `docker-compose.yml` runs separate frontend, backend, enrich-worker, and download-worker services.
- Persistent SQLite DB and artifact paths are wired; `.env.example` documents environment setup.
- Hot-reload workflow is available for local development.

## M-06 Dokumentation

Status: `completed`.

- `dl_lit_project/README.md` documents the canonical work model, quick start, CLI entrypoints, and doc map.
- `dl_lit_project/docs/USER_GUIDE.md` provides end-user workflow documentation for search, seed review, corpus work, downloads, graph, logs, export, and password reset.
- `dl_lit_project/docs/ADMIN_GUIDE.md` documents admin responsibilities including invitations, corpus sharing, Kantropos target assignment, SMTP, and operational responsibilities.
- `dl_lit_project/docs/OPS_RUNBOOK.md` documents the live Docker stack, environment, restart procedures, health checks, logs, backups, and common failure modes.
- `dl_lit_project/DEVELOPMENT.md` remains the developer-oriented architecture note.
- `dl_lit_project/pyproject.toml` + `requirements-dev.txt` include a lint baseline (`ruff`).
