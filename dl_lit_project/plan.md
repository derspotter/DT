# Project Plan — Korpus‑Builder 2.0 (Contract‑Aligned)

## 1) Scope & Contract Alignment
This plan implements the signed Werkvertrag (WISY‑2025‑00021) and the Pflichtenheft. The product modernizes and extends `dl_lit_project` into a web‑based Korpus‑Builder that supports:
- Seed‑document ingestion (references + citations) with automated retrieval
- Keyword search with result persistence and download queueing
- Visualization of citation networks (D3.js)
- React/Vite frontend
- Docker deployment with persistence and resumable workflows
- Code audit/refactor + documentation

**End‑to‑end objective:** build a system that can assemble a scientific corpus for the AG’s RAG pipeline (Kantropos), with both seed‑based and keyword‑based acquisition.

## 2) Milestones (per Vertrag)
**MS1 — Refactored Core + Keyword Search (Feb 2026)**
- Code audit/refactor of core pipeline modules
- Stable extract → enrich → queue pipeline
- Keyword search (OpenAlex) with boolean parsing and persistence
- Tests for CLI + keyword search + pipeline

**MS2 — Visualization (Apr 2026)**
- D3‑based citation/cluster visualization
- Data pipeline to compute graph nodes/edges
- Export graph JSON for frontend use

**MS3 — React/Vite Frontend (May 2026)**
- Upload flows (PDF/BibTeX/JSON)
- Keyword search UI
- Progress + run history
- Graph visualization UI

**MS4 — Docker + Persistence (Jun 2026)**
- Docker compose for frontend, backend, workers
- Stateful SQLite volume
- Resume interrupted workflows

**MS5 — Documentation (Jul 2026)**
- README (install/usage)
- Developer notes (architecture, workflows)
- Ops notes (deployment, API keys)

**MS6 — Rework + Tests + Final Acceptance (Jul 2026)**
- Fixes from AG testing
- Full regression test sweep
- End‑to‑end validation + handover

## 3) Current Status (Jan 2026)
- Phase 1 (Refactor + Keyword Search) completed and tested.
- Pipeline helper supports `--max-ref-pages`, `--max-entries`, and `--no-enrich` for smoke tests.
- Dedupe hardening, merge logging, and alias handling implemented.
- Full test suite passes (`pytest dl_lit_project/tests`).

## 4) Workstreams & Deliverables

### A) Pipeline & Data Layer
- Harden extraction, enrichment, queueing, and download stages
- Maintain provenance (`pipeline_runs`, `ingest_source`, `run_id`)
- Support dedupe + alias merge logging (`merge_log`)
- Ensure all ingestion paths set normalized fields

### B) Keyword Search (Seed‑Independent Acquisition)
- Boolean query parsing
- OpenAlex filters + cursor pagination
- Persist `search_runs` and `search_results`
- Optional enqueue to download queue

### C) Seed‑Document Workflow
- Extract bibliography pages
- Parse references via Gemini
- Enrich via OpenAlex/Crossref
- Queue & download referenced papers
- Optional “cited‑by” recursion

### D) Visualization (MS2)
- Build graph datasets from DB
- D3 citation graph + clustering
- API endpoint to feed graph JSON

### E) Web Frontend (MS3)
- React/Vite UI
- Upload + search + progress views
- Visualization page
- Export/download workflows

### F) Deployment & Persistence (MS4)
- Docker compose
- SQLite volume persistence
- Restartable pipeline runs

### G) Documentation & Acceptance (MS5‑MS6)
- README + Dev docs
- Ops runbook + API key setup
- Final test suite and AG acceptance

## 5) Dependencies & Assumptions
- AG provides Gemini API key + budget (≤ €250/month without approval).
- AG provides server environment (Linux, 4 vCPU / 8 GB RAM, SSH) before deployment.
- OpenAlex/Crossref rate limits respected via global limiter.

## 6) Risks & Mitigations
- **LLM extraction variability** → stronger schemas + retry + validation
- **API rate limits** → backoff + batching + `mailto` use
- **Download coverage gaps** → multiple sources + PDF validation
- **Performance at scale** → batch tooling + optional concurrency plan

## 7) Acceptance Criteria (per contract)
- Seed‑document and keyword search workflows operational end‑to‑end
- Visualization available in UI
- React/Vite frontend supports upload/search/download
- Docker deployment with persistent state
- Documentation complete, tests pass, final acceptance by AG

