# Review fixes phase 1: SQL hot paths and merge correctness — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the per-poll and per-import database work index-served, stop the silent loss of corpus membership on duplicate merges, and stop fetching expansion data nothing stores.

**Architecture:** Python owns the `works` schema (`dl_lit/db_manager.py`); Node reads it through better-sqlite3 (`backend/src/pipelineReads.js`, `corpusList.js`, `app.js`). Fixes are query-shape changes plus three indexes, one grouped state read shared by every count consumer, and one duplicate-absorbing helper shared by every merge path. No schema columns change.

**Tech Stack:** Python 3.11 / sqlite3 / pytest (`cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests -q`); Node 20 / better-sqlite3 / Jest ESM (`cd backend && npm test`).

**Spec:** `docs/superpowers/specs/2026-10-27-review-fixes-1-sql-hot-paths-design.md`. Findings with measurements: `docs/reviews/2026-10-27-backend-dry-perf.md` (I1, I2, I10) and `docs/reviews/2026-10-27-python-dry-perf.md` (I1–I4). Line numbers in those reports predate PR #73; locate code by name.

## Global Constraints

- Never run anything against `dl_lit_project/data/literature.db` except read-only timing probes (`sqlite3.connect('file:...?mode=ro', uri=True)`); tests use `:memory:` or `tmp_path`.
- `COALESCE(<attempt_count>, 0)` stays (nullable column); only `metadata_status`/`download_status` lose their `COALESCE` (both `NOT NULL DEFAULT`).
- The corpus-list golden test (`backend/tests/corpus-list.test.js`, fixture `backend/tests/fixtures/corpus-list.sql`) must keep passing unchanged: it has text years, so it must still take the CASE sort path.
- Every `CREATE INDEX` / `DROP INDEX` is `IF NOT EXISTS` / `IF EXISTS` and appears in exactly two places: `db_manager.py` (`WORKS_INDEXES` / `DROPPED_INDEXES`) and `app.js ensureAuthSchema`.
- Commit messages end with the session trailers (see repository convention in recent `git log`).

## Review Focus

1. A work promoted into corpus A whose enrichment resolves to a canonical row that is only in corpus B: after the merge the canonical must be in A and B (Task 3 test).
2. A fixture whose years are all integer/NULL must produce the same page order through the index path as through the CASE path (Task 7 test).
3. `readWorkStateCounts` memo must not serve corpus 1's counts for corpus 2, and must refresh within 1 s of a write by the same connection (Task 5 tests).
4. Dropping an index that another connection (the running worker) is using must not error: `DROP INDEX IF EXISTS` inside the schema-ensure runs on boot only (Task 6 idempotence test runs it twice).
5. `do_mark` callers (`pipeline_tick`) must still get a dict with `requested`, `marked`, `available`, `ids` (Task 2 test asserts the keys).

---

### Task 1: Dedupe lookups use the partial indexes, once per import

**Files:**
- Modify: `dl_lit_project/dl_lit/db_manager.py` (`_find_existing_work`, `create_pending_work`, `_resolve_or_create_work`)
- Test: `dl_lit_project/tests/test_dedupe_index_plan.py` (new)

**Interfaces:**
- Produces: `_resolve_or_create_work(data, *, allow_merge=True, commit=True, existing=None)` where `existing` is an optional `(id, field)` already resolved by the caller (skips the lookup when given).

- [ ] **Step 1: Write the failing tests**

```python
def test_doi_and_openalex_lookups_use_their_partial_indexes(tmp_path):
    db = DatabaseManager(str(tmp_path / "lit.db"))
    cur = db.conn.cursor()
    plans = {}
    for col in ("normalized_doi", "openalex_id"):
        cur.execute(f"EXPLAIN QUERY PLAN SELECT id FROM works WHERE {col} = ? AND {col} != ''", ("x",))
        plans[col] = " ".join(str(r[-1]) for r in cur.fetchall())
    assert "idx_works_normalized_doi" in plans["normalized_doi"]
    assert "idx_works_openalex_id" in plans["openalex_id"]

def test_find_existing_work_query_text_matches_index_predicate(tmp_path, monkeypatch):
    db = DatabaseManager(str(tmp_path / "lit.db"))
    seen = []
    real = db.conn.cursor
    class Spy:
        def __init__(self, cur): self.cur = cur
        def execute(self, q, p=()): seen.append(q); return self.cur.execute(q, p)
        def __getattr__(self, n): return getattr(self.cur, n)
    monkeypatch.setattr(db.conn, "cursor", lambda: Spy(real()))
    db._find_existing_work(doi="10.1/x", openalex_id="W1", title="t", authors=None, editors=None, year=2020)
    assert any("normalized_doi = ? AND normalized_doi != ''" in q for q in seen)
    assert any("openalex_id = ? AND openalex_id != ''" in q for q in seen)

def test_create_pending_work_resolves_the_existing_work_once(tmp_path, monkeypatch):
    db = DatabaseManager(str(tmp_path / "lit.db"))
    calls = []
    orig = db._find_existing_work
    monkeypatch.setattr(db, "_find_existing_work", lambda *a, **k: calls.append(1) or orig(*a, **k))
    db.create_pending_work({"title": "A", "doi": "10.1/a", "year": 2020})   # use the real signature
    assert len(calls) == 1
```
(Adapt the `create_pending_work` call to its real signature; read it first.)

- [ ] **Step 2: Run them, expect failure** (`pytest tests/test_dedupe_index_plan.py -v`): the plan test fails on `SCAN works`, the query-text test on the missing predicate, the count test on 2 calls.

- [ ] **Step 3: Implement.** In `_find_existing_work` append ` AND normalized_doi != ''` / ` AND openalex_id != ''` to the two queries. Add the `existing` keyword to `_resolve_or_create_work`; in `create_pending_work` pass the `(id, field)` it already got from `check_if_exists` so the lookup runs once. Keep every other caller unchanged.

- [ ] **Step 4: Run** `pytest tests -q`: all pass.

- [ ] **Step 5: Commit** `dedupe lookups match the partial-index predicate; create_pending_work resolves once`.

---

### Task 2: Status predicates without COALESCE; `do_mark` stops counting

**Files:**
- Modify: `dl_lit_project/dl_lit/db_manager.py` (`claim_enrich_batch` select + update, `fetch_matched_work_batch`), `backend/scripts/daemon/worker.py` (`do_mark`), `backend/src/app.js` (`readWorkerBacklogSummary` — only the `COALESCE` wrappers; Task 5 replaces the function)
- Test: `dl_lit_project/tests/test_claim_index_plan.py` (new), `dl_lit_project/tests/test_pipeline_daemon.py` (extend)

- [ ] **Step 1: Write the failing tests**

```python
def test_enrich_claim_select_uses_the_download_status_index(tmp_path):
    db = DatabaseManager(str(tmp_path / "lit.db"))
    cur = db.conn.cursor()
    cur.execute("""EXPLAIN QUERY PLAN SELECT id FROM works nm
        WHERE (metadata_status = 'pending' OR (metadata_status = 'in_progress' AND metadata_lease_expires_at IS NOT NULL AND metadata_lease_expires_at <= ?))
          AND nm.download_status = 'not_requested' ORDER BY id LIMIT 10""", (0,))
    plan = " ".join(str(r[-1]) for r in cur.fetchall())
    assert "idx_works_download_status" in plan or "idx_works_metadata_status" in plan

def test_claim_sql_has_no_coalesce_on_status_columns(tmp_path):
    src = inspect.getsource(DatabaseManager.claim_enrich_batch) + inspect.getsource(DatabaseManager.fetch_matched_work_batch)
    assert "COALESCE(metadata_status" not in src and "COALESCE(nm.download_status" not in src and "COALESCE(download_status" not in src

# test_pipeline_daemon.py
def test_do_mark_returns_the_constant_shape_without_querying(monkeypatch):
    conn = make_conn()
    daemon = make_daemon(conn)
    executed = []
    monkeypatch.setattr(daemon.db.conn, "cursor", lambda: (_ for _ in ()).throw(AssertionError("do_mark must not query")))
    assert daemon.do_mark(None, 10) == {"requested": 10, "marked": 0, "available": 0, "ids": []}
```

- [ ] **Step 2: Run, expect failure.**

- [ ] **Step 3: Implement.** Replace `COALESCE(metadata_status, 'pending') = 'pending'` with `metadata_status = 'pending'` (select and update in `claim_enrich_batch`), `COALESCE(nm.download_status, 'not_requested')` / `COALESCE(download_status, 'not_requested')` with the bare column (`claim_enrich_batch`, `fetch_matched_work_batch`). `do_mark` returns `{"requested": limit, "marked": 0, "available": 0, "ids": []}` with no query (keep the comment that marking is obsolete). In `app.js readWorkerBacklogSummary` drop the `COALESCE(t.metadata_status, 'pending')` / `COALESCE(t.download_status, 'not_requested')` wrappers.

- [ ] **Step 4: Run** `pytest tests -q` and `cd backend && npm test`: all pass.

- [ ] **Step 5: Commit** `status predicates without COALESCE (NOT NULL columns); do_mark no longer counts`.

---

### Task 3: One `_absorb_duplicate` for every merge path

**Files:**
- Modify: `dl_lit_project/dl_lit/db_manager.py` (new `_absorb_duplicate`; `apply_enriched_metadata`, `mark_raw_work_downloaded`, `merge_duplicate_queued_work`, `_merge_records`), `backend/scripts/daemon/worker.py` (remove the `delete_work` after `mark_raw_work_downloaded` if the row is already gone; verify by reading the code)
- Test: `dl_lit_project/tests/test_absorb_duplicate.py` (new)

**Interfaces:**
- Produces: `_absorb_duplicate(self, canonical_id: int, duplicate_id: int, *, match_field: str | None, notes: str | None = None, updates: dict | None = None) -> None`. Runs inside the caller's open transaction (no commit). Steps, in order: `INSERT OR IGNORE INTO corpus_works (corpus_id, work_id, added_at) SELECT corpus_id, ?, added_at FROM corpus_works WHERE work_id = ?`; `UPDATE OR IGNORE work_aliases SET work_id = ? WHERE work_table = 'works' AND work_id = ?` then `DELETE FROM work_aliases WHERE work_table = 'works' AND work_id = ?` (leftovers after a unique conflict); `DELETE FROM corpus_works WHERE work_id = ?`; `DELETE FROM works WHERE id = ?`; `INSERT INTO merge_log (canonical_table, canonical_id, duplicate_table, duplicate_id, match_field, action, notes, updates_json) VALUES ('works', ?, 'works', ?, ?, 'absorbed', ?, ?)`.

- [ ] **Step 1: Write the failing tests**

```python
def _work(db, title, corpus_ids, **cols): ...  # insert a works row (+ corpus_works rows), return id

def test_enrichment_merge_keeps_the_duplicates_corpus_membership(tmp_path):
    db = DatabaseManager(str(tmp_path / "lit.db"))
    canonical = _work(db, "Canon", [2], doi="10.1/c")
    dup = _work(db, "Canon draft", [1], metadata_status="in_progress")
    db.apply_enriched_metadata(dup, {"doi": "10.1/c", "title": "Canon", "year": 2020})  # real signature
    assert set(db.conn.execute("SELECT corpus_id FROM corpus_works WHERE work_id = ?", (canonical,)).fetchall()) == {(1,), (2,)}
    assert db.conn.execute("SELECT COUNT(*) FROM works WHERE id = ?", (dup,)).fetchone()[0] == 0
    assert db.conn.execute("SELECT action FROM merge_log WHERE duplicate_id = ?", (dup,)).fetchone() == ("absorbed",)

def test_download_merge_and_queue_merge_keep_membership(tmp_path): ...  # same shape for mark_raw_work_downloaded and merge_duplicate_queued_work

def test_absorb_moves_aliases_and_tolerates_duplicates(tmp_path): ...  # alias present on both sides -> one row, pointing at canonical

def test_merge_records_uses_absorb(tmp_path): ...  # _merge_records on two works rows -> membership moved, merge_log row written once
```

- [ ] **Step 2: Run, expect failure** (membership lost today).

- [ ] **Step 3: Implement** `_absorb_duplicate` and call it from the three paths and `_merge_records` in place of their inline `DELETE FROM works` (+ alias moves). Remove the worker's redundant `delete_work` if the implementer confirms the row is already deleted on that path (read `mark_raw_work_downloaded`'s merge branch); otherwise leave it and note why in the report.

- [ ] **Step 4: Run** `pytest tests -q`.

- [ ] **Step 5: Commit** `_absorb_duplicate: merges keep corpus membership and aliases, logged once`.

---

### Task 4: Enrichment stops fetching expansion data nothing stores

**Files:**
- Modify: `backend/scripts/daemon/worker.py` (`do_enrich` / `_enrich_single`: call `process_single_reference(..., fetch_references=False, fetch_citations=False)`; stop threading `expand_related`/`max_related_per_source` into `apply_enriched_metadata`), `dl_lit_project/dl_lit/db_manager.py` (`apply_enriched_metadata` drops the two unused parameters)
- Test: `dl_lit_project/tests/test_worker_enrich_no_expansion.py` (new)

- [ ] **Step 1: Write the failing test**

```python
def test_enrich_single_never_asks_for_references_or_citations(monkeypatch, tmp_path):
    mod, daemon = ...  # same loader as test_worker_settings._daemon
    seen = {}
    def fake(entry, searcher, limiter, **kw): seen.update(kw); return ({"title": "x"}, {})
    monkeypatch.setattr(mod, "process_single_reference", fake)
    daemon._enrich_single({"id": 1, "title": "x"}, True, True, 100, 2, 1, 40)   # the old flags are ignored
    assert seen["fetch_references"] is False and seen["fetch_citations"] is False

def test_apply_enriched_metadata_has_no_expansion_parameters():
    params = inspect.signature(DatabaseManager.apply_enriched_metadata).parameters
    assert "expand_related" not in params and "max_related_per_source" not in params
```

- [ ] **Step 2: Run, expect failure.**

- [ ] **Step 3: Implement.** Keep `do_enrich`'s signature (callers pass expansion config) but ignore the flags for fetching; log once per process (`log("Expansion during enrichment is disabled: related works are not persisted; see the expansion plan")`) when a caller asks for it. Delete the two parameters from `apply_enriched_metadata` and every call site.

- [ ] **Step 4: Run** `pytest tests -q`.

- [ ] **Step 5: Commit** `enrichment no longer fetches related works it cannot store`.

---

### Task 5: One grouped work-state read for every count consumer

**Files:**
- Modify: `backend/src/pipelineReads.js` (new `readWorkStateCounts`, `workStatus`, `stateCountsMemo`; `readIngestStats` derives), `backend/src/app.js` (`readWorkerBacklogSummary` derives; `statusFromWorkRow` replaced by `workStatus`), `backend/src/corpusList.js` (`status_counts`/`stage_totals` derive; `STATUS_SQL` kept only for the row projection), `backend/src/seed.js` (`worksFingerprint` uses the global counts)
- Test: `backend/tests/pipeline-reads.test.js` (extend), `backend/tests/corpus-list.test.js` (golden must still pass)

**Interfaces:**
- Produces: `readWorkStateCounts(db, corpusId, { nowSec = Math.floor(Date.now()/1000) } = {}) -> Array<{ metadata_status, download_status, n, live_meta, live_dl }>`; `workStatus(meta, dl) -> 'downloaded'|'queued_download'|'failed_download'|'enriching'|'failed_enrichment'|'matched'|'raw'` (trim with the same whitespace set as `STATUS_SQL`, lowercase); `bucketCounts(rows) -> { status_counts, raw_pending_total, pending_only, in_progress, matched_not_requested, queued, downloading, downloaded, failed_meta, failed_dl, enriching_live, downloading_live }`. Memo: `Map` keyed `${corpusId}` holding `{ dataVersion, at, rows }`, hit when `PRAGMA data_version` equals and `Date.now() - at < 1000`.
- SQL: `SELECT w.metadata_status, w.download_status, COUNT(*) AS n, SUM(CASE WHEN w.metadata_status = 'in_progress' AND w.metadata_lease_expires_at > ? THEN 1 ELSE 0 END) AS live_meta, SUM(CASE WHEN w.download_status = 'in_progress' AND w.download_lease_expires_at > ? THEN 1 ELSE 0 END) AS live_dl FROM works w WHERE EXISTS (SELECT 1 FROM corpus_works cw WHERE cw.corpus_id = ? AND cw.work_id = w.id) GROUP BY 1, 2` (no `WHERE` for global scope). When the lease columns are missing (old DB), `live_*` are 0 (keep the existing `hasColumn` guard).

- [ ] **Step 1: Write the failing tests** (extend the `seedStats` fixture; it already has one row per bucket)

```js
test('readWorkStateCounts groups by raw status pair with live-lease sums', () => { seedStats(); ... expect rows to contain {metadata_status:'in_progress', download_status:null→'not_requested'?...} })  // match the fixture's actual values
test('readIngestStats equals the old numbers, derived from the grouped read', () => { seedStats(); expect(readIngestStats(db, 1)).toEqual({ raw_pending: 2, matched: 1, queued_download: 2, enriching: 1, downloading: 1, downloaded: 1, enriching_live: 0, downloading_live: 0 }) })
test('workStatus buckets like STATUS_SQL, including padded/odd-case values', () => { expect(workStatus(' Matched ', 'not_requested')).toBe('matched'); expect(workStatus('x', ' downloaded ')).toBe('downloaded'); ... })
test('the memo is per corpus and expires after a same-connection write', async () => { seedStats(); readIngestStats(db,1); addWork(8, 2, 'pending', null); expect(readIngestStats(db,2).raw_pending).toBe(1); addWork(9, 1, 'pending', null); await sleep(1100); expect(readIngestStats(db,1).raw_pending).toBe(3) })
```
Plus in `corpus-list.test.js`: the existing golden assertions on `status_counts`/`stage_totals` cover the derivation; add one test that `listCorpus` issues no `GROUP BY` query containing `CASE` (wrap `db.prepare` to record SQL).

- [ ] **Step 2: Run, expect failure.**

- [ ] **Step 3: Implement** as specified. `readWorkerBacklogSummary(corpusId)` in `app.js` becomes a thin wrapper: `raw_pending` = pending only (document next to ingest stats' `pending + in_progress`). `corpusList` computes `status_counts` from `bucketCounts(readWorkStateCounts(...))` when there is no `q` filter; with a filter it keeps the filtered `GROUP BY` (the filter needs the UDF) — note this in a comment. `seed.worksFingerprint` uses the global rows (`n` total, `downloaded`) plus `MAX(id)` (an index-only read on the PK).

- [ ] **Step 4: Run** `cd backend && npm test`: all pass, golden included.

- [ ] **Step 5: Commit** `one grouped work-state read; ingest stats, backlog, corpus counts and the seed fingerprint derive from it`.

---

### Task 6: Indexes: three new, duplicates dropped, declared in two places

**Files:**
- Modify: `dl_lit_project/dl_lit/db_manager.py` (`WORKS_INDEXES` list + `DROPPED_INDEXES` list, applied in the schema ensure), `backend/src/app.js` (`ensureAuthSchema`: same DDL; stop creating `idx_corpus_works_lookup`), `backend/src/seed.js` (stop creating the three `*_lookup` indexes if they duplicate the tables' primary keys — verify the PKs first; if a table has no PK covering those columns, keep its index and say so in the report)
- Test: `dl_lit_project/tests/test_schema_indexes.py` (new), `backend/tests/schema-indexes.test.js` (new)

- [ ] **Step 1: Write the failing tests**

```python
NEW = {"idx_works_state", "idx_works_year_id", "idx_corpus_works_added"}
DROPPED = {"idx_corpus_works_lookup", "idx_corpus_works_corpus", "idx_work_aliases_norm_title", "idx_work_aliases_owner", "idx_search_results_run"}
def test_schema_ensure_creates_new_and_drops_duplicate_indexes_idempotently(tmp_path):
    path = str(tmp_path / "lit.db")
    c = sqlite3.connect(path); c.execute("CREATE TABLE corpus_works (corpus_id INTEGER, work_id INTEGER, added_at TIMESTAMP, PRIMARY KEY (corpus_id, work_id))")
    c.execute("CREATE INDEX idx_corpus_works_lookup ON corpus_works(corpus_id, work_id)"); c.commit(); c.close()
    for _ in range(2):
        DatabaseManager(path).close()
    names = {r[0] for r in sqlite3.connect(path).execute("SELECT name FROM sqlite_master WHERE type='index'")}
    assert NEW <= names and not (DROPPED & names)
```
```js
test('ensureAuthSchema creates the state/year/added indexes and drops the duplicates, twice', () => { ... same shape against a temp DB_PATH via createApp or the exported ensureAuthSchema ... })
```

- [ ] **Step 2: Run, expect failure.**

- [ ] **Step 3: Implement.** DDL:
```sql
CREATE INDEX IF NOT EXISTS idx_works_state ON works(download_status, metadata_status, metadata_lease_expires_at, download_lease_expires_at);
CREATE INDEX IF NOT EXISTS idx_works_year_id ON works(year, id);
CREATE INDEX IF NOT EXISTS idx_corpus_works_added ON corpus_works(corpus_id, added_at, work_id);
DROP INDEX IF EXISTS idx_corpus_works_lookup; DROP INDEX IF EXISTS idx_corpus_works_corpus;
DROP INDEX IF EXISTS idx_work_aliases_norm_title; DROP INDEX IF EXISTS idx_work_aliases_owner;
DROP INDEX IF EXISTS idx_search_results_run;
```
`idx_corpus_works_corpus` is a prefix of the PK `(corpus_id, work_id)`; `idx_search_results_run` a prefix of `idx_search_results_run_openalex`. For `work_aliases`, keep `idx_work_aliases_title` and `idx_work_aliases_work` (drop only the duplicates). Seed `*_lookup`: drop only those that duplicate a PK.

- [ ] **Step 4: Run** both suites.

- [ ] **Step 5: Commit** `indexes: works(state), works(year,id), corpus_works(added); duplicate indexes dropped on both schema paths`.

---

### Task 7: Index-served default and `added` sorts in `listCorpus`

**Files:**
- Modify: `backend/src/corpusList.js`
- Test: `backend/tests/corpus-list.test.js` (extend), new fixture `backend/tests/fixtures/corpus-list-int-years.sql` (copy of the fixture with every text year replaced by an integer or NULL)

**Interfaces:**
- Consumes: `idx_works_year_id`, `idx_corpus_works_added` (Task 6).
- Produces: in `readCorpus`, `const hasTextYears = Boolean(db.prepare("SELECT 1 FROM works WHERE typeof(year) = 'text' LIMIT 1").get())`; default order is `w.year DESC, w.id DESC` when `!hasTextYears`, else the existing `YEAR_SQL` CASE; scope via `EXISTS (SELECT 1 FROM corpus_works cw WHERE cw.corpus_id = ? AND cw.work_id = w.id)` instead of the JOIN for the default sort; the `added` sort reads `FROM corpus_works cw JOIN works w ON w.id = cw.work_id WHERE cw.corpus_id = ? ORDER BY cw.added_at <dir>, w.id DESC` (NULL `added_at` first in ASC, last in DESC, matching the current `CASE ... THEN ''` expression — assert this in the test).

- [ ] **Step 1: Write the failing tests**

```js
test('with integer/NULL years the default sort is served by idx_works_year_id and matches the Python script', () => { load int-years fixture; run corpus_list.py vs listCorpus for corpus 1 default sort, both directions of page/offset; assert equal; assert EXPLAIN QUERY PLAN of the page query mentions idx_works_year_id and not 'TEMP B-TREE' })
test('with text years the CASE path is used and the golden output is unchanged', () => { existing fixture; same comparison; plan may use TEMP B-TREE })
test('added sort uses idx_corpus_works_added and keeps NULL ordering', () => { ... })
```

- [ ] **Step 2: Run, expect failure** (plan assertion).

- [ ] **Step 3: Implement** as in Interfaces. Expose the chosen path for the test via a `listCorpus(..., { explain: true })` option that returns `{ ..., sql }` or by wrapping `db.prepare` in the test.

- [ ] **Step 4: Run** `cd backend && npm test`.

- [ ] **Step 5: Commit** `listCorpus: index-served default and added sorts when years are integer/NULL`.

---

### Task 8: Timing report on the live database (read-only)

**Files:**
- Create: `backend/scripts/timing_probe.py` (read-only, `mode=ro`), results pasted into the PR description, not committed as data.

- [ ] **Step 1:** Measure, with the live DB opened read-only and the indexes from Task 6 present (run the worker schema ensure once so they exist; it is idempotent): dedupe lookup by `openalex_id` (old query vs new), enrich claim select (old vs new), grouped state read for corpus 99 vs the old `GROUP BY STATUS_SQL`, default page for the largest corpus (CASE vs index path). Print a table.
- [ ] **Step 2:** Paste the table into the plan's completion note / PR body.
- [ ] **Step 3: Commit** the probe script.
