# Seed -> Corpus Refactor Plan

## Goal
Simplify the application to two user-facing surfaces:

1. `Seed`
2. `Corpus`

The primary user action becomes:

- `Promote to Corpus`

Everything else becomes either:

- a secondary action inside `Seed`
- a live status inside `Corpus`
- or an admin-only diagnostics/control surface

This is not a greenfield rewrite. The existing extraction, enrichment, dedupe, download, auth, corpus, and daemon infrastructure should be reused where possible.

---

## Why This Refactor Is Needed

The current application leaks too much pipeline implementation detail into the user workflow:

- ingest runs
- search results
- raw / with metadata / queued / downloaded as separate user workflows
- manual worker start/stop
- global queue behavior
- daemon quirks

This creates the wrong mental model. Users should think in terms of:

- seed sources
- candidate works
- promotion into a corpus

They should not have to understand:

- `no_metadata`
- `with_metadata`
- `pipeline_jobs`
- `download_state`
- cross-corpus worker starvation

---

## Product Model

### 1. Seed
The Seed area is a staging workspace for potential corpus items.

Top-level seed sources:

- uploaded PDF
- search run
- later: manual entry batch

Each seed source is expandable.

When collapsed, the row shows:

- source type
- source label
- candidate count
- added time
- per-source state summary

When expanded, the source shows candidate items.

### 2. Corpus
The Corpus area is the accepted working set.

It shows live state for items already promoted or staged into the corpus pipeline.

User-facing corpus states should describe outcomes, not storage tables:

- raw
- queued
- metadata
- downloading
- downloaded
- failed

---

## Core User Flow

### Seed flow
1. User uploads a PDF or runs a search.
2. The new source appears as an expandable row in `Seed`.
3. User expands the row and reviews candidate items.
4. User selects candidates.
5. User sets recursion:
   - downstream on/off
   - downstream depth
   - upstream on/off
   - upstream depth
   - max related
6. User clicks `Promote to Corpus`.
7. The system stages, enriches, expands, queues, and downloads automatically.

### Corpus flow
1. User opens `Corpus`.
2. User sees accepted items and their live state.
3. User filters or searches corpus content.
4. User exports or continues working with the corpus.

No normal user should need to:

- start a download worker
- pause a daemon
- inspect pipeline jobs
- reconcile multiple staging pages

---

## Unified Domain Model

### Seed source
A container for candidates.

Proposed normalized fields:

- `seed_source_id`
- `source_type` (`pdf`, `search`)
- `source_ref_id` (original PDF/search-run identifier)
- `label`
- `subtitle`
- `created_at`
- `removed_at`
- `meta_json`

### Seed candidate
A normalized candidate work inside a seed source.

Proposed normalized fields:

- `seed_candidate_id`
- `seed_source_id`
- `origin_type` (`pdf_reference`, `search_result`)
- `origin_ref_id`
- `title`
- `authors`
- `year`
- `doi`
- `source`
- `publisher`
- `url`
- `openalex_id`
- `state`
- `state_detail_json`
- `removed_at`
- `promoted_to_table`
- `promoted_to_id`
- `created_at`
- `updated_at`

### Corpus item
The accepted durable working-set entry.

This remains backed by existing corpus-linked records:

- `no_metadata`
- `with_metadata`
- `downloaded_references`
- failed tables

The refactor should avoid inventing a parallel corpus model unless necessary.

---

## State Model

### Seed candidate states
These should be explicit and live-derived, not inferred from `processed`.

- `pending`
  - candidate exists only in seed
  - not staged anywhere else yet

- `staged_raw`
  - candidate matches a corpus `no_metadata` row
  - not currently queued for enrichment

- `queued_enrichment`
  - candidate matches a `no_metadata` row with `selected_for_enrichment = 1`

- `enriched`
  - candidate matches `with_metadata`
  - not yet queued for download

- `queued_download`
  - candidate matches `with_metadata`
  - `download_state IN ('queued', 'in_progress')`

- `downloaded`
  - candidate matches `downloaded_references`

- `failed_enrichment`
  - candidate matches `failed_enrichments`

- `failed_download`
  - candidate matches `failed_downloads`
  - or `with_metadata.download_last_error`

- `dismissed`
  - removed from seed intentionally

- `promoted_duplicate`
  - candidate resolved to an existing canonical corpus item

### Important rule
The `processed` column in `ingest_entries` must not drive UI semantics.

The current refactor has already removed it from the ingest badge path. That direction should continue.

---

## UI Plan

## Seed page

### Top-level layout
- source list/table
- each row expandable
- unified regardless of origin

### Source row fields
- source label
- source type badge (`PDF`, `Search`)
- added time
- candidate count
- summary counts by state
- actions:
  - expand/collapse
  - remove source

### Expanded source contents
- candidate table
- selection checkboxes
- state badges
- source-specific metadata header
- promotion controls

### Source actions
- `Select all`
- `Clear selection`
- `Dismiss selected`
- `Promote selected`
- `Remove source`

### Promotion controls
Per-source by default:

- downstream toggle
- downstream depth
- upstream toggle
- upstream depth
- max related

Optional later:
- allow per-row override

### Filtering
At minimum:

- all
- PDF only
- Search only
- pending/selectable only
- active failures only

## Corpus page

### Top-level layout
- summary tiles
- corpus item list
- filters

### Summary tiles
- raw
- metadata
- queued
- downloaded
- failed

### Corpus item list
- title
- authors
- year
- source
- current status
- optional actions (export/open/remove later)

### Hidden from normal users
- worker start/stop controls
- daemon details
- queue internals
- pipeline job rows

Keep those only in an admin/debug page.

---

## API Refactor

## New read endpoints

### `GET /api/seed/sources`
Returns normalized seed sources.

Response shape:

- `sources[]`
  - `id`
  - `sourceType`
  - `label`
  - `subtitle`
  - `candidateCount`
  - `stateCounts`
  - `createdAt`
  - `removable`
  - `meta`

### `GET /api/seed/sources/:id/candidates`
Returns normalized candidates for one source.

Response shape:

- `source`
- `items[]`
  - `seedCandidateId`
  - `originType`
  - `title`
  - `authors`
  - `year`
  - `doi`
  - `source`
  - `publisher`
  - `state`
  - `selectable`
  - `meta`

## New write endpoints

### `POST /api/seed/sources/:id/promote`
Promotes selected candidates from a source.

Request:

- `candidateIds`
- `includeDownstream`
- `relatedDepthDownstream`
- `includeUpstream`
- `relatedDepthUpstream`
- `maxRelated`
- `autoDownload`

Response:

- `requestId`
- `jobs`
- `queuedCounts`

### `POST /api/seed/candidates/dismiss`
Dismisses selected seed candidates.

### `DELETE /api/seed/sources/:id`
Removes or archives a seed source from the Seed workspace.

Important:

- does not delete already promoted corpus items

## Existing endpoints to deprecate

- `/api/ingest/latest`
- `/api/ingest/enqueue`
- `/api/ingest/process-marked`
- search-page-specific enqueue flows
- user-facing download worker endpoints

These can remain temporarily behind compatibility wrappers.

---

## Data Strategy

## Phase 1: Build a unified read model on top of existing tables

Do not migrate the entire DB immediately.

Use existing data sources:

- PDF-side:
  - `ingest_entries`
  - `ingest_source_metadata`

- Search-side:
  - existing search run/results tables or current search persistence layer

Normalize them into a single `Seed` API response.

This is the fastest route to the new UI.

## Phase 2: Add explicit seed tables if needed

If the read-model layer becomes too fragile, add:

- `seed_sources`
- `seed_candidates`

This should happen only after the new Seed UI is proven.

---

## Scheduler and Worker Policy

The current single global daemon model is serviceable, but the scheduling policy is not.

Observed failure mode:

- one corpus can monopolize the daemon
- another user’s direct download request can sit pending behind unrelated enrich backlog

### Required scheduling rules

1. Direct user-triggered jobs outrank background jobs
2. User-triggered `download` outranks stale `enrich`
3. Cross-corpus fairness must be enforced
4. Background maintenance only runs when no interactive work is waiting

### Proposed priority order

1. corpus `download`
2. corpus `enrich`
3. global `download`
4. global `enrich`
5. corpus `pipeline_tick`
6. global `pipeline_tick`

### Additional requirement

Jobs older than a threshold and still `pending` or long-`running` should be visible in admin diagnostics and cancellable.

---

## What Should Be Removed from the User Workflow

### Remove as primary pages
- Ingest
- Keyword Search
- Downloads

### Remove as normal-user actions
- Start worker
- Stop worker
- Run once
- Pause all
- direct queue tuning

### Keep only for admin/debug
- pipeline job table
- daemon heartbeat
- worker knobs
- raw logs

---

## Migration Path

## Phase A: Stabilize semantics
1. finish removing `processed` from seed UI logic
2. standardize live state resolution
3. ensure seed badges are DB-backed only

## Phase B: New Seed APIs
1. build unified source listing
2. build unified candidate listing
3. build unified promote endpoint
4. build dismiss/remove-source actions

## Phase C: New Seed UI
1. create expandable unified source table
2. embed candidate tables
3. embed promotion controls
4. wire request-scoped progress

## Phase D: Simplified Corpus UI
1. remove worker controls from normal user view
2. present only live corpus states
3. keep diagnostics behind admin/debug flag

## Phase E: Navigation switch
1. make `Seed` and `Corpus` the default navigation
2. move old pages behind hidden/debug routes
3. remove legacy navigation entry points

## Phase F: Data cleanup
1. optionally introduce `seed_sources` / `seed_candidates`
2. retire dead compatibility code
3. stop writing obsolete fields

---

## Concrete Code Workstreams

## 1. State cleanup
Files likely involved:

- `backend/scripts/ingest_latest.py`
- `backend/scripts/ingest_enqueue.py`
- `frontend/src/App.svelte`

Tasks:

- remove UI dependency on `processed`
- expose explicit live states
- align selection rules with states

## 2. Unified seed backend
Files likely involved:

- `backend/src/app.js`
- new backend seed scripts or route handlers
- search persistence layer

Tasks:

- build source normalization
- build candidate normalization
- add promote/dismiss/remove-source endpoints

## 3. New Seed UI
Files likely involved:

- `frontend/src/App.svelte`
- new extracted components if split out
- `frontend/src/lib/api.js`

Tasks:

- build source table
- build expandable candidate panels
- reuse live progress components where possible

## 4. Corpus simplification
Files likely involved:

- `frontend/src/App.svelte`
- corpus data API layer

Tasks:

- simplify status surfaces
- remove user-facing worker controls
- keep only meaningful corpus state counts

## 5. Scheduler policy fix
Files likely involved:

- `backend/src/app.js`
- `backend/scripts/daemon/worker.py`

Tasks:

- user job priority
- cross-corpus fairness
- stalled-job visibility/cancellation

---

## Acceptance Criteria

The refactor is successful when:

1. users see only `Seed` and `Corpus` as primary surfaces
2. search runs and uploaded PDFs appear as the same kind of expandable seed source
3. all seed badges reflect live DB state only
4. promotion is the single primary user action
5. removing a seed source does not delete already promoted corpus items
6. direct user download/promote actions are not blocked by unrelated stale corpus jobs
7. the app no longer requires normal users to reason about workers or queue internals

---

## Testing Plan

### Backend tests
- unified source listing returns mixed PDF/search sources
- candidate normalization is consistent across source types
- promotion endpoint respects recursion settings
- dismiss/remove-source is seed-only and non-destructive to corpus
- scheduler prioritizes interactive user download over stale enrich backlog

### Frontend tests
- source expand/collapse
- source removal
- candidate selection
- promotion controls persistence
- live seed state badges
- corpus status rendering

### Manual acceptance tests
1. Upload PDF -> source appears in Seed
2. Run search -> source appears in Seed
3. Expand either source -> candidate rows look identical in structure
4. Promote selected -> corpus states update
5. Remove search source -> corpus remains intact
6. Trigger competing corpus jobs -> direct user download still starts promptly

---

## Risks

### Risk 1: Hidden coupling in current tables
Mitigation:

- start with read-model normalization instead of immediate schema replacement

### Risk 2: Scheduler changes break existing flows
Mitigation:

- keep admin diagnostics visible during rollout
- add targeted queue tests

### Risk 3: Seed removal semantics become destructive
Mitigation:

- soft-delete sources first
- never delete promoted corpus items via seed actions

### Risk 4: UI refactor stalls behind backend perfectionism
Mitigation:

- build the new Seed page on top of compatibility endpoints first
- improve data model in parallel, not serially

---

## Recommended Execution Order

1. finish live-state cleanup and remove obsolete seed semantics
2. implement unified `Seed` read endpoints
3. implement new `Seed` UI with expandable sources
4. implement `Promote to Corpus` source/candidate actions
5. simplify `Corpus`
6. fix scheduler priority and fairness
7. hide legacy pages
8. optionally normalize seed tables

---

## Bottom Line

This should be approached as a vertical refactor, not a full rewrite.

Reuse:

- extraction
- enrichment
- dedupe
- download engine
- auth
- corpus linkage
- daemon

Replace or heavily rework:

- user-facing workflow
- ingest/search/download page structure
- state semantics
- scheduling policy

The winning abstraction is:

- `Seed source` -> expandable
- `Seed candidate` -> selectable
- `Promote to Corpus` -> primary action
- `Corpus` -> durable accepted set
