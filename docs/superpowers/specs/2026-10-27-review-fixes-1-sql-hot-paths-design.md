# Review fixes, phase 1: SQL hot paths and merge correctness — design

Date: 2026-10-27. Source: the DRY/performance review in `docs/reviews/2026-10-27-summary.md`
(backend I1, I2, I10; Python I1, I2, I3, I4). Jay: "address the findings from the subagents".
Later phases: backend security/boilerplate (I5, I7, I8, I9, I11), frontend, Python DRY.

## Problems (measured on the live 103k-work database)

1. **Dedupe lookups cannot use their indexes.** `_find_existing_work` runs
   `WHERE normalized_doi = ?` / `WHERE openalex_id = ?`, but the only indexes on those
   columns are partial (`WHERE col IS NOT NULL AND col != ''`); a bound parameter does
   not imply the index predicate, so each lookup scans `works`: 252 ms, versus 0 ms with
   `AND col != ''`. `create_pending_work` runs the lookup twice (~1 s per imported
   reference); the download worker runs it under `db_lock`, serialising 16 threads.
2. **`COALESCE` on NOT NULL status columns defeats the status indexes.** The enrich claim
   query (under `BEGIN IMMEDIATE`, every 5 s idle poll) costs 261 ms vs 3 ms without it;
   `do_mark` adds a 280 ms `COUNT(*)` nobody reads on every `pipeline_tick`.
3. **Duplicate merges drop corpus membership.** `apply_enriched_metadata`,
   `mark_raw_work_downloaded` and `merge_duplicate_queued_work` delete the duplicate row
   without moving its `corpus_works` rows; `pruneStaleCorpusItems` then removes the
   orphans. The one correct implementation, `_merge_records`, has no callers.
4. **Expansion fetches related works that nothing stores** (since `0b639ec`): OpenAlex
   credits and seconds per enriched item for no result; `apply_enriched_metadata` ignores
   its `expand_related`/`max_related_per_source` parameters.
5. **Status counts are computed four ways per poll, each a full fat-row scan** (0.25–0.7 s):
   `corpusList` (`GROUP BY STATUS_SQL`), `readIngestStats`, `readWorkerBacklogSummary`
   (twice per admin cycle), `seed.worksFingerprint`. `raw_pending` means
   `pending + in_progress` in one and `pending` in another.
6. **The corpus page sort uses a CASE no index can serve**, so all 72k member rows are
   read and sorted per poll (190–720 ms). Live years are only integer or NULL.
7. **Duplicate indexes** on `corpus_works`, `work_aliases`, `search_results` and the seed
   tables are maintained on every insert for nothing; new indexes get added in three
   places (`db_manager.py`, `app.js ensureAuthSchema`, `migrate_to_works.py`).

## Decisions

- **Queries match the partial-index predicates literally**: `AND normalized_doi != ''`,
  `AND openalex_id != ''`. `create_pending_work` resolves the existing id once and hands
  it to `_resolve_or_create_work`. A pytest asserts the `EXPLAIN QUERY PLAN` uses the index.
- **Drop `COALESCE` around `metadata_status` / `download_status`** everywhere the column is
  NOT NULL (claim select and update, `fetch_matched_work_batch`, `claim_download_batch`'s
  equivalent, `worker.do_mark`, `readWorkerBacklogSummary`). `do_mark` returns its constant
  dict without the count. `COALESCE(attempt_count, 0)` stays (nullable column).
- **One `_absorb_duplicate(canonical_id, duplicate_id, *, match_field, notes)`** in
  `db_manager`, used by the three merge paths and by `_merge_records`: moves
  `corpus_works` (`INSERT OR IGNORE ... SELECT corpus_id, canonical, added_at`), moves
  `work_aliases`, deletes the duplicate, writes `merge_log`. It runs inside the caller's
  transaction. The worker's second `delete_work` after `mark_raw_work_downloaded` is removed.
- **Stop fetching related works in the enrich worker** (`fetch_references=False`,
  `fetch_citations=False`) and delete the dead `expand_related`/`max_related_per_source`
  parameters. Persisting expansion is a feature for the phase-4 expansion plan, not a
  silent side effect of enrichment. The promotion UI is untouched (the `new_seed` mode
  still expands through the search path).
- **One grouped state read**: `readWorkStateCounts(db, corpusId)` in `pipelineReads.js`
  returns rows `{metadata_status, download_status, n, live_meta, live_dl}` grouped by the
  raw status pair (a few dozen rows), scoped with an `EXISTS` probe on `corpus_works`, over
  a new covering index `idx_works_state(download_status, metadata_status,
  metadata_lease_expires_at, download_lease_expires_at)`. One exported
  `workStatus(meta, dl)` (ported from `STATUS_SQL`, with the same trim/lower) buckets those
  rows in JS. `readIngestStats`, `readWorkerBacklogSummary` and the corpus-list
  `status_counts`/`stage_totals` derive from it. Results are memoised per
  `(corpusId, PRAGMA data_version)` with a 1 s TTL (own-connection writes do not bump
  `data_version`, hence the TTL). `raw_pending` keeps both meanings, derived from the
  same rows: ingest stats = `pending + in_progress`, backlog = `pending` (documented).
  `worksFingerprint` uses the global counts from the same helper.
- **Index-served default sort**: when no `works` row has a text year (probed with
  `SELECT 1 FROM works WHERE typeof(year) = 'text' LIMIT 1`, which the new covering
  index `idx_works_year_id(year, id)` answers in milliseconds), the default order is
  `ORDER BY w.year DESC, w.id DESC` with an `EXISTS` corpus probe, which is identical to
  the CASE order for integer/NULL years (NULL sorts last in DESC, like 0). Otherwise the
  CASE expression stays, so the golden test against `corpus_list.py` holds on the fixture
  (which has text years). The `added` sort gets `idx_corpus_works_added(corpus_id,
  added_at, work_id)` and is driven from `corpus_works`.
- **Indexes live in one list** `WORKS_INDEXES` in `db_manager.py` (Python owns the schema)
  and the same DDL in `app.js ensureAuthSchema` for Node-booted databases, both
  idempotent; duplicate indexes are dropped in both places
  (`idx_corpus_works_lookup`, `idx_corpus_works_corpus`, `idx_work_aliases_norm_title`,
  `idx_work_aliases_owner`, `idx_search_results_run`, the seed `*_lookup` trio).
  Full schema unification (single `schema.sql`, `user_version`) is deferred.

## Non-goals (later phases)

`q` filter / title / authors sorts via stored sort keys or FTS5; `seed.js` state-resolver
caching (backend I3); `ingest_latest` port (I4); child-process env and async spawn (I7);
WebSocket auth (I8); dead scheduler code (I9); frontend findings.

## Verification

pytest: index-plan assertions for the dedupe and claim queries; `_absorb_duplicate`
moves membership and aliases in all three paths; worker enrichment passes no expansion
flags. Jest: `readWorkStateCounts` buckets equal the old per-route numbers on the
pipeline-reads fixture; the corpus-list golden test still passes (fixture with text
years) and a second fixture without text years takes the index path with identical
output; the duplicate-index drop is idempotent. Timing on the live database, read-only,
before/after for: dedupe lookup, claim query, status counts (corpus 99), default page.
