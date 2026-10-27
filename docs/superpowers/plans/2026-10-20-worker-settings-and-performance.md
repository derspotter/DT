# Worker Settings and Performance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Background workers honour the Admin settings; the four Python-backed read routes become bounded SQL in Node; the live refresh polls only when it has a reason to; OpenAlex paging reuses one HTTP session; a production frontend build is available behind one Caddy upstream.

**Architecture:** A small Python module resolves `app_settings` rows into environment overrides and the daemon applies it before each job, rebuilding its cached LLM client only on change. Node gains one module per listing (`corpusList.js`, `pipelineReads.js`) with projection, filter, order and paging in SQL; a Jest golden test runs the old Python script against the same fixture DB and asserts equality. The frontend gets a single refresh scheduler keyed on visibility and activity. The frontend Dockerfile's `runner` stage serves `dist/` with Caddy and proxies `/api/*`; a `docker-compose.prod.yml` overlay selects it.

**Tech Stack:** Node 22 / Express / better-sqlite3 / Jest 30 / supertest; Python 3 / sqlite3 / requests / pytest; Svelte 5 (compat) / Vite / Playwright; Docker Compose v5; Caddy 2.

**Spec:** `docs/superpowers/specs/2026-10-20-worker-settings-and-performance-design.md`

## Global Constraints

- Precedence for every setting: stored `app_settings` value wins over the process environment; a blank or missing stored value leaves the environment untouched (mirrors `appSettingsEnv()` in `backend/src/app.js`).
- Settings keys and env names are exactly the ten in `APP_SETTING_DEFS` (`backend/src/app.js` ~line 2151); do not invent new keys.
- The Python scripts `corpus_list.py`, `downloads_list.py`, `ingest_runs.py`, `ingest_stats.py` stay on disk unchanged; only the routes stop spawning them.
- Response shapes of `/api/corpus`, `/api/downloads`, `/api/ingest/runs`, `/api/ingest/stats` are unchanged: same keys, same item fields, same ordering semantics. `source` stays `'db'` where the script set it.
- `RAG_FEEDER_STUB=1` behaviour of every route is unchanged.
- The default `docker-compose.yml` keeps `target: dev` for the frontend; production serving is opt-in through `docker-compose.prod.yml`. No change to production hosts is part of this plan.
- Commit after every task; run the narrow test first, then the suite the task names.
- Test commands: backend `cd backend && npm test`; Python `cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests -q`; e2e `cd frontend && set -a && . ../.env && set +a && npx playwright test` (the `.env` sourcing prints a harmless `-o: command not found`).

## Review Focus

1. A stored setting whose value is blank must not export an empty env var (which would make `get_client()` think a provider is configured with no key). Pinned in Task 1 (`test_blank_stored_value_does_not_override_env`).
2. A corpus filter containing `%` or `_` must match literally, not as a wildcard. Pinned in Task 4 (`escapes LIKE metacharacters in q`).
3. `/api/ingest/stats?global=1` (admin, no corpus) and a corpus with zero works must both answer with all six counters as `0`, never missing keys. Pinned in Task 3.
4. A tab that was hidden and becomes visible must refresh once immediately, then resume its idle cadence; it must not fire a burst of queued refreshes. Pinned in Task 5.
5. The production image must serve `index.html` for a hash deep link (`/#/workspace` is just `/`) and must answer 404 for `/src/App.svelte`, `/Dockerfile` and `/.env` (no source leak). Pinned in Task 6.

---

### Task 1: Workers resolve Admin settings per job

**Files:**
- Create: `dl_lit_project/dl_lit/app_settings.py`
- Modify: `backend/scripts/daemon/worker.py` (constructor ~line 123–150; `_process_job` ~line 861; the direct-cycle calls in `run()`)
- Test: `dl_lit_project/tests/test_app_settings.py`, `dl_lit_project/tests/test_worker_settings.py`

**Interfaces:**
- Produces: `APP_SETTING_ENV: dict[str, str]` mapping the ten settings keys to env names, copied verbatim from `APP_SETTING_DEFS`; `load_app_settings_env(db_path: str | Path) -> dict[str, str]` (env name → stored non-blank value; `{}` when the table is missing); `apply_app_settings_env(db_path) -> dict[str, str]` which sets `os.environ[name] = value` for each returned pair and returns the dict; `settings_fingerprint(env: dict[str, str]) -> str` (stable hash of sorted items, `''` for empty).
- Produces on `PipelineDaemon`: `refresh_app_settings(self) -> bool` returning True when the fingerprint changed and the enhancer was rebuilt; `_build_enhancer(self) -> BibliographyEnhancer` factored out of `__init__`.

- [ ] **Step 1: Write the failing module tests**

```python
# dl_lit_project/tests/test_app_settings.py
import os, sqlite3
from dl_lit import app_settings

def _db(tmp_path, rows):
    p = tmp_path / "auth.db"
    c = sqlite3.connect(p)
    c.execute("CREATE TABLE app_settings (key TEXT PRIMARY KEY, value TEXT, updated_by_user_id INTEGER, updated_at TIMESTAMP)")
    c.executemany("INSERT INTO app_settings (key, value) VALUES (?, ?)", rows)
    c.commit(); c.close()
    return p

def test_env_map_matches_backend_defs():
    assert app_settings.APP_SETTING_ENV["llm_provider"] == "RAG_FEEDER_LLM_PROVIDER"
    assert app_settings.APP_SETTING_ENV["openalex_api_key"] == "OPENALEX_API_KEY"
    assert len(app_settings.APP_SETTING_ENV) == 10

def test_stored_value_wins_over_env(tmp_path, monkeypatch):
    monkeypatch.setenv("RAG_FEEDER_LLM_PROVIDER", "gemini")
    db = _db(tmp_path, [("llm_provider", "openai")])
    assert app_settings.apply_app_settings_env(db) == {"RAG_FEEDER_LLM_PROVIDER": "openai"}
    assert os.environ["RAG_FEEDER_LLM_PROVIDER"] == "openai"

def test_blank_stored_value_does_not_override_env(tmp_path, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "from-env")
    db = _db(tmp_path, [("openai_api_key", "   "), ("unknown_key", "x")])
    assert app_settings.load_app_settings_env(db) == {}
    app_settings.apply_app_settings_env(db)
    assert os.environ["OPENAI_API_KEY"] == "from-env"

def test_missing_table_is_empty(tmp_path):
    p = tmp_path / "empty.db"; sqlite3.connect(p).close()
    assert app_settings.load_app_settings_env(p) == {}

def test_fingerprint_is_order_independent_and_empty_for_nothing():
    a = app_settings.settings_fingerprint({"A": "1", "B": "2"})
    assert a == app_settings.settings_fingerprint({"B": "2", "A": "1"})
    assert a != app_settings.settings_fingerprint({"A": "1"})
    assert app_settings.settings_fingerprint({}) == ""
```

- [ ] **Step 2: Run them, expect ImportError**

Run: `cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests/test_app_settings.py -q`

- [ ] **Step 3: Implement `dl_lit/app_settings.py`**

Read the table with `sqlite3` in read-only mode (`file:...?mode=ro` URI), swallow `sqlite3.OperationalError` for a missing table, strip values, skip blanks and unknown keys. The ten keys, verbatim from `APP_SETTING_DEFS`: `openalex_api_key→OPENALEX_API_KEY`, `openalex_rps→RAG_FEEDER_OPENALEX_RPS`, `search_warn_threshold→RAG_FEEDER_SEARCH_WARN_THRESHOLD`, `llm_provider→RAG_FEEDER_LLM_PROVIDER`, `openai_base_url→RAG_FEEDER_OPENAI_BASE_URL`, `openai_api_key→OPENAI_API_KEY`, `gemini_api_key→GEMINI_API_KEY`, `extract_model→RAG_FEEDER_API_EXTRACT_MODEL`, `openai_model→RAG_FEEDER_OPENAI_MODEL`, `gemini_model→RAG_FEEDER_GEMINI_MODEL`.

- [ ] **Step 4: Run the module tests, expect PASS**

- [ ] **Step 5: Write the failing worker test**

```python
# dl_lit_project/tests/test_worker_settings.py
import importlib.util, sqlite3, sys
from pathlib import Path
WORKER = Path(__file__).resolve().parents[2] / "backend" / "scripts" / "daemon" / "worker.py"

def _load(monkeypatch):
    monkeypatch.syspath_prepend(str(WORKER.parent))
    spec = importlib.util.spec_from_file_location("dt_worker", WORKER)
    mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
    return mod

def test_refresh_rebuilds_enhancer_only_when_settings_change(monkeypatch, tmp_path):
    mod = _load(monkeypatch)
    monkeypatch.chdir(tmp_path)
    builds = []
    monkeypatch.setattr(mod.PipelineDaemon, "_build_enhancer", lambda self: builds.append(1) or object())
    daemon = mod.PipelineDaemon(str(tmp_path / "lit.db"), role="enrich")
    assert len(builds) == 1                      # built once at construction
    assert daemon.refresh_app_settings() is False
    c = sqlite3.connect(tmp_path / "lit.db")
    c.execute("CREATE TABLE IF NOT EXISTS app_settings (key TEXT PRIMARY KEY, value TEXT, updated_by_user_id INTEGER, updated_at TIMESTAMP)")
    c.execute("INSERT INTO app_settings (key, value) VALUES ('llm_provider', 'openai')"); c.commit(); c.close()
    assert daemon.refresh_app_settings() is True
    assert len(builds) == 2
    assert daemon.refresh_app_settings() is False   # unchanged → no rebuild
```

The constructor opens `DatabaseManager(db_path)`, which creates the schema in the temp file; `_build_enhancer` is patched so no LLM client or network is touched.

- [ ] **Step 6: Run it, expect AttributeError on `_build_enhancer`**

- [ ] **Step 7: Modify `worker.py`**

Factor the `BibliographyEnhancer(...)` construction (constructor lines ~140–145) into `_build_enhancer(self)`. In `__init__`, before building, call `apply_app_settings_env(db_path)` and keep `self._settings_fingerprint = settings_fingerprint(...)`. Add `refresh_app_settings(self)`: apply, compute fingerprint, if different store it, rebuild `self.enhancer`, `log("Admin settings changed; rebuilt enhancer")`, return True. Call `self.refresh_app_settings()` at the top of `_process_job` and at the top of `_run_direct_enrich_cycle` / `_run_direct_download_cycle`. The settings live in the same SQLite file as the works (the backend's `authDb` is `DB_PATH`), so pass `self.db_path`.

- [ ] **Step 8: Run both test files, then the Python suite**

Run: `cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests/test_app_settings.py tests/test_worker_settings.py -q` then the full suite. Expected: all pass.

- [ ] **Step 9: Live check on the dev stack**

Run: `docker compose restart rag_feeder_enrich_worker && sleep 8 && docker logs --since 20s rag_feeder_enrich_worker 2>&1 | tail -5`. Expected: the worker polls without tracebacks.

- [ ] **Step 10: Commit**

```bash
git add dl_lit_project/dl_lit/app_settings.py backend/scripts/daemon/worker.py dl_lit_project/tests/test_app_settings.py dl_lit_project/tests/test_worker_settings.py
git commit -m "Workers resolve Admin settings per job and rebuild the LLM client on change"
```

---

### Task 2: One HTTP session per thread for OpenAlex paging

**Files:**
- Modify: `dl_lit_project/dl_lit/utils.py` (near `get_global_rate_limiter`, ~line 506), `dl_lit_project/dl_lit/keyword_search.py:113-120`
- Test: `dl_lit_project/tests/test_keyword_search.py`

**Interfaces:**
- Produces: `get_openalex_session() -> requests.Session` in `dl_lit.utils`, one `Session` per thread (`threading.local`), created on first use.

- [ ] **Step 1: Write the failing tests**

```python
def test_openalex_session_is_shared_per_thread():
    from dl_lit import utils
    import threading, requests
    a = utils.get_openalex_session(); b = utils.get_openalex_session()
    assert a is b and isinstance(a, requests.Session)
    seen = []
    t = threading.Thread(target=lambda: seen.append(utils.get_openalex_session())); t.start(); t.join()
    assert seen[0] is not a

def test_keyword_requests_reuse_the_session(monkeypatch):
    captured = []
    monkeypatch.setattr(keyword_search, "openalex_request_json", lambda **kw: captured.append(kw) or {"results": [], "meta": {"count": 0}})
    keyword_search.count_openalex(query="a"); keyword_search.count_openalex(query="b")
    assert captured[0]["session"] is captured[1]["session"] is not None
```

- [ ] **Step 2: Run, expect AttributeError / KeyError**

- [ ] **Step 3: Implement** `get_openalex_session()` in `utils.py` and pass `session=get_openalex_session()` from `_openalex_request` in `keyword_search.py`. Import the helper into `keyword_search` next to `get_global_rate_limiter`.

- [ ] **Step 4: Run the file, then the Python suite; expect PASS**

- [ ] **Step 5: Commit**

```bash
git add dl_lit_project/dl_lit/utils.py dl_lit_project/dl_lit/keyword_search.py dl_lit_project/tests/test_keyword_search.py
git commit -m "OpenAlex keyword paging reuses one requests.Session per thread"
```

---

### Task 3: Ingest stats, downloads and ingest runs in Node SQL

**Files:**
- Create: `backend/src/pipelineReads.js`
- Modify: `backend/src/app.js` routes `/api/ingest/stats` (~6085), `/api/downloads` (~5671), `/api/ingest/runs` (~6072)
- Test: `backend/tests/pipeline-reads.test.js`

**Interfaces:**
- Produces: `readIngestStats(db, corpusId: number|null) -> { raw_pending, matched, queued_download, enriching, downloading, downloaded }` (all integers); `listDownloads(db, { corpusId, limit, offset }) -> { items: [{ id, title, status, attempts, downloaded_at }], total }`; `listIngestRuns(db, { corpusId, limit }) -> { runs: [...], total }` with `seed_authors` parsed to a list when it is a JSON list. Each takes an open `better-sqlite3` Database.
- Consumes: `tableExists(db, name)` — copy the three-line helper from `seed.js` or import it if exported there.

- [ ] **Step 1: Write the failing tests**

Build an in-memory DB per test with `CREATE TABLE works (id INTEGER PRIMARY KEY, title TEXT, metadata_status TEXT, download_status TEXT, download_attempt_count INTEGER, downloaded_at TEXT)`, `corpus_works (corpus_id, work_id)`, and for runs `ingest_entries (ingest_source TEXT, corpus_id INTEGER, created_at TEXT, source_pdf TEXT)` plus `ingest_source_metadata (ingest_source, corpus_id, title, authors, year, doi, source, publisher)`.

```js
test('ingest stats count each bucket with the script\'s predicates', () => {
  // rows: pending/-, in_progress/-, matched/not_requested, matched/queued, matched/in_progress, matched/downloaded, failed/-
  expect(readIngestStats(db, 1)).toEqual({ raw_pending: 2, matched: 1, queued_download: 2, enriching: 1, downloading: 1, downloaded: 1 })
})
test('ingest stats for an empty corpus and for global scope return zeros / all works', () => {
  expect(readIngestStats(db, 999)).toEqual({ raw_pending: 0, matched: 0, queued_download: 0, enriching: 0, downloading: 0, downloaded: 0 })
  expect(readIngestStats(db, null).downloaded).toBe(/* count across all corpora */)
})
test('downloads: in_progress first, then queued, then downloaded newest first; id desc ties; paged with total', () => {
  const page = listDownloads(db, { corpusId: 1, limit: 2, offset: 1 })
  expect(page.total).toBe(4)
  expect(page.items.map((i) => i.id)).toEqual([/* second and third of the expected order */])
  expect(page.items[0]).toEqual({ id: expect.any(Number), title: expect.any(String), status: 'queued', attempts: 0, downloaded_at: null })
})
test('downloads: a blank download_status reports as queued and limit 0 returns no items but the total', () => { /* ... */ })
test('ingest runs group by source, newest first, limited, with seed_authors parsed', () => {
  const out = listIngestRuns(db, { corpusId: 1, limit: 5 })
  expect(out.runs[0].seed_authors).toEqual(['A. Author', 'B. Author'])
  expect(out.total).toBe(out.runs.length)
})
test('ingest runs without the ingest tables returns an empty list', () => { /* db with no tables */ })
```

- [ ] **Step 2: Run, expect module-not-found**

- [ ] **Step 3: Implement `pipelineReads.js`**

`readIngestStats`: one query, `SUM(CASE WHEN ... THEN 1 ELSE 0 END)` per bucket, predicates copied from `ingest_stats.py`, joined to `corpus_works` when `corpusId` is not null. `listDownloads`: the script's `SELECT`/`ORDER BY` plus `LIMIT ? OFFSET ?`, and a separate `COUNT(*)` with the same `WHERE` for `total`; `limit` clamped to ≥ 0, `offset` to ≥ 0. `listIngestRuns`: the script's grouped query verbatim (the `ism` join only when that table exists), then parse `seed_authors`.

- [ ] **Step 4: Run the test file, expect PASS**

- [ ] **Step 5: Switch the three routes**

Each route keeps its stub branch and its admin check, then calls the Node function on `authDb` and answers `{ stats, source: 'db' }`, `{ items, total }`, `{ runs, total, source: 'db' }` respectively. Remove the now-unused `DOWNLOADS_LIST_SCRIPT`, `INGEST_RUNS_SCRIPT`, `INGEST_STATS_SCRIPT` constants only if nothing else references them (grep first).

- [ ] **Step 6: Run `cd backend && npm test`; expect all pass**

- [ ] **Step 7: Live timing on the dev stack**

Run the three endpoints with a token five times each via curl `-w '%{time_total}\n'`. Expected: each under 40 ms (was 100–210 ms). Record the numbers in the commit message.

- [ ] **Step 8: Commit**

```bash
git add backend/src/pipelineReads.js backend/src/app.js backend/tests/pipeline-reads.test.js
git commit -m "Ingest stats, downloads and ingest runs read from SQLite in Node (no Python child)"
```

---

### Task 4: Corpus listing in Node SQL, golden against the Python script

**Files:**
- Create: `backend/src/corpusList.js`
- Modify: `backend/src/app.js` route `/api/corpus` (~5499)
- Test: `backend/tests/corpus-list.test.js`

**Interfaces:**
- Produces: `listCorpus(db, { corpusId: number|null, limit, offset, q, sort }) -> { items, total, stage_totals, status_counts }` with item fields exactly as `corpus_list.py` emits (`id, work_id, title, authors, year, added_at, source, origin, publisher, type, source_label, source_type, source_key, status, metadata_status, download_status, doi, openalex_id, file_path`). `CORPUS_SORT_KEYS = ['title','authors','year','added','source','seed','metadata','download']`.
- Consumes: `tableExists`.

- [ ] **Step 1: Write the golden test fixture and tests**

The fixture DB (`backend/tests/fixtures/corpus-list.sql` or built in a `beforeAll`) covers: 14 works in corpus 1 and 3 outside it; every `status_from_row` branch; `authors` as JSON list, as plain string, as `'[]'`, and NULL; years including NULL and a non-numeric string; a NULL `corpus_works.added_at`; titles containing `%` and `_`; provenance of each kind: `seed_candidates_in_corpus` pdf and search rows, `origin_key` starting with `search:`, `run_id` set, `origin_type='bibtex_import'`, `origin_key` matching `ingest_source_metadata` (with authors list of 1 and of 3), `source_pdf` only, and none. Create `search_runs (id, query)`, `ingest_source_metadata`, `seed_candidates_in_corpus (corpus_id, work_id, source_type, source_key, marked_at)`.

```js
import { execFileSync } from 'node:child_process'
const PY = process.env.RAG_FEEDER_PYTHON || '/home/jay/DT/venv/bin/python'
function viaScript(args) { return JSON.parse(execFileSync(PY, [SCRIPT, '--db-path', dbPath, ...args], { encoding: 'utf8' })) }

const CASES = [
  [], ['--sort', 'title:asc'], ['--sort', 'authors:asc'], ['--sort', 'authors:desc'], ['--sort', 'year:desc'],
  ['--sort', 'added:desc'], ['--sort', 'source:asc'], ['--sort', 'seed:asc'], ['--sort', 'metadata:asc'],
  ['--sort', 'download:desc'], ['--q', 'labour'], ['--q', '100%'], ['--q', 'a_b'], ['--limit', '5', '--offset', '5'],
  ['--limit', '0'], ['--offset', '999'], ['--q', 'LABOUR', '--sort', 'year:asc', '--limit', '3'],
]
test.each(CASES)('matches corpus_list.py for %j', (...args) => {
  const expected = viaScript(['--corpus-id', '1', ...args])
  const actual = listCorpus(db, parseArgs(args, { corpusId: 1 }))
  expect(actual).toEqual(expected)
})
test('no corpus selected lists every work and uses created_at for added_at', () => {
  expect(listCorpus(db, { corpusId: null, limit: 200, offset: 0, q: '', sort: '' })).toEqual(viaScript([]))
})
test('escapes LIKE metacharacters in q', () => {
  expect(listCorpus(db, { corpusId: 1, limit: 200, offset: 0, q: '100%', sort: '' }).items.map((i) => i.id)).toEqual([/* only the title containing "100%" */])
})
test('unknown sort falls back to the default order', () => { /* equals viaScript([]) */ })
```

Skip the golden cases with `test.skip` when the Python binary is absent (`fs.existsSync(PY)`), so CI without the venv still runs the pure-Node tests.

- [ ] **Step 2: Run, expect module-not-found**

- [ ] **Step 3: Implement `corpusList.js`**

Read `corpus_list.py` lines 30–62 (status, year, id normalization), 72–198 (labels), 200–270 (filter, author key, sort keys), 300–395 (main) before writing. Decisions already made:
- Status is a SQL `CASE` mirroring `status_from_row` (download precedence first, then metadata), used for `status`, for `status_counts` (`GROUP BY`), and for `stage_totals` (`SUM(CASE ...)` over the filtered set, buckets as in the script).
- Filter: `q` lowercased, applied with `LIKE ? ESCAPE '\'` on `lower(title)`, `lower(authors)`, `lower(source)`; escape `\`, `%`, `_` in the needle.
- Sort keys in SQL: `title` → `lower(COALESCE(title,''))`; `year` → `CAST(year AS INTEGER)` with non-numeric → 0 (use `CASE WHEN CAST(year AS INTEGER) || '' = year THEN CAST(year AS INTEGER) ELSE 0 END`, matching `normalize_year`); `added` → `COALESCE(cw.added_at,'')`; `source` → `lower(COALESCE(source,''))`; `metadata`/`download` → `lower(COALESCE(metadata_status,''))` / `lower(COALESCE(download_status,''))`; `authors` → first non-blank element of the JSON list when `json_valid(authors)` and it is an array, else the string, lowercased (a small SQL `CASE` over `json_extract(authors,'$[0]')`, `'$[1]'`, `'$[2]'` is acceptable; the fixture has no list whose first three names are all blank). Default: `year DESC, id DESC`. Every explicit sort adds `id DESC` as the tiebreak; `asc`/`desc` follow the script (`reverse = direction != 'asc'`).
- `seed` sort cannot be expressed in SQL (labels come from four tables with precedence). For `sort=seed` only: select the light columns (`id, origin_type, origin_key, run_id, source_pdf, metadata_source, source`) for the filtered set, compute labels in JS, sort, slice ids, then fetch the page rows by id. Memory stays bounded to those seven columns.
- Labels: port `load_pdf_source_labels`, `_first_author`, `load_search_source_labels`, `load_seed_provenance`, `source_label_for_work` one-to-one into JS; load the three lookup maps once per call (they are small), compute labels only for the returned page except in the `seed` sort path.
- Select for the page: the item fields only, not `w.*`.

- [ ] **Step 4: Run the test file, expect PASS (golden equality on every case)**

- [ ] **Step 5: Switch the route** to `listCorpus(authDb, { corpusId: req.corpusId ?? null, limit, offset, q: query, sort })`, keeping the stub branch and the 500 handler.

- [ ] **Step 6: Run `cd backend && npm test` and the e2e suite; expect all pass**

- [ ] **Step 7: Live timing**: `/api/corpus?limit=120` five times via curl. Expected: under 40 ms (was 150–270 ms). Record in the commit message.

- [ ] **Step 8: Commit**

```bash
git add backend/src/corpusList.js backend/src/app.js backend/tests/corpus-list.test.js backend/tests/fixtures
git commit -m "Corpus listing in Node SQL with paging, filter and sort; golden-tested against corpus_list.py"
```

---

### Task 5: One adaptive refresh scheduler

**Files:**
- Modify: `frontend/src/App.svelte` — the `$:` block at ~208–216 (`searchRefreshIntervalId`), `schedulePipelineRefresh` (~1526), `shouldRunLiveRefresh`/`runLiveRefreshCycle` (~1574–1610), the `liveRefreshIntervalId` setup and teardown in `onMount` (~4710–4730), the log handler call at ~4556
- Test: `frontend/tests/refresh-scheduler.spec.ts`

**Interfaces:**
- Produces (module-level in `App.svelte`): `REFRESH_ACTIVE_MS = 2000`, `REFRESH_IDLE_MS = 20000`, `REFRESH_TRAILING_MS = 1500`, `REFRESH_WORKER_TICK_MS = 300`; `refreshReason(): 'hidden' | 'active' | 'idle' | 'none'`; `scheduleRefresh({ delayMs = null } = {})` which arms exactly one timer; `requestRefreshSoon(delayMs)` used by the log handler (marks dirty, coalesces to one trailing run). `shouldRunLiveRefresh` and `runLiveRefreshCycle` keep their names.

- [ ] **Step 1: Write the failing spec** (mocked API as in `boot-requests.spec.ts`; count `/api/corpus` requests after boot settles)

```ts
test('idle visible workspace polls at the idle cadence, not every 3 s', async ({ page }) => {
  /* boot, wait 1.5 s, reset count, wait 7 s */
  expect(corpusRequests).toBeLessThanOrEqual(1)
})
test('a running search run polls every 2 s', async ({ page }) => {
  /* seed sources mock returns run.status === 'running'; wait 7 s after boot */
  expect(corpusRequests).toBeGreaterThanOrEqual(3)
})
test('a hidden tab does not poll, and becoming visible refreshes once', async ({ page }) => {
  await page.evaluate(() => { Object.defineProperty(document, 'visibilityState', { value: 'hidden', configurable: true }); document.dispatchEvent(new Event('visibilitychange')) })
  /* reset, wait 5 s */ expect(corpusRequests).toBe(0)
  await page.evaluate(() => { Object.defineProperty(document, 'visibilityState', { value: 'visible', configurable: true }); document.dispatchEvent(new Event('visibilitychange')) })
  await page.waitForTimeout(1500); expect(corpusRequests).toBe(1)
})
test('a worker log line triggers one trailing refresh, not one per line', async ({ page }) => {
  /* call window.__dtRequestRefreshSoon?.(300) five times within 100 ms if exposed; otherwise drive via the mocked WebSocket if the harness allows; else assert via requestRefreshSoon exported for tests */
})
```

If the fourth test cannot drive the log path from Playwright, expose `window.__dtRefresh = { requestRefreshSoon }` only when `import.meta.env.DEV` and assert through it.

- [ ] **Step 2: Run, expect the idle test to fail (≥2 requests in 7 s)**

- [ ] **Step 3: Implement** the scheduler: a single `refreshTimer`; `refreshReason()` returns `'hidden'` when `document.visibilityState === 'hidden'`, `'active'` when `anySearchRunning` or (diagnostics and a worker is running/in flight), `'idle'` when `shouldRunLiveRefresh()`, else `'none'`; `scheduleRefresh` clears and re-arms with 2000 / 20000 / nothing. After each `runLiveRefreshCycle` completes, call `scheduleRefresh()`. React to `anySearchRunning` changes and to `visibilitychange` by calling `scheduleRefresh()` (visible after hidden → `delayMs: 0`). Replace `schedulePipelineRefresh` with `requestRefreshSoon(delayMs)`: if a timer is armed sooner, keep it; else re-arm at `delayMs`. Remove both `setInterval`s and clear the timer in the `onMount` cleanup. The Admin-tab quota refresh keeps running at the idle cadence.

- [ ] **Step 4: Run the new spec, `search-scale.spec.ts`, `boot-requests.spec.ts`; expect PASS** (the "running seed settles to done" case still relies on the 2 s active cadence).

- [ ] **Step 5: Run the full e2e suite; expect all pass**

- [ ] **Step 6: Commit**

```bash
git add frontend/src/App.svelte frontend/tests/refresh-scheduler.spec.ts
git commit -m "One refresh scheduler: 2 s while active, 20 s idle, none when hidden, one trailing run per log burst"
```

---

### Task 6: Production frontend image behind one Caddy upstream

**Files:**
- Modify: `frontend/Dockerfile` (the `runner` stage)
- Create: `frontend/Caddyfile`, `docker-compose.prod.yml`
- Modify: `README.md` ("Live Deploys Behind Caddy")
- Test: shell verification in this task (no unit test); `frontend/tests/prod-image.sh` holds the checks so they can be re-run

**Interfaces:**
- Produces: image stage `runner` based on `caddy:2-alpine` serving `/srv` on `:5175` with `handle /api/* { reverse_proxy {$RAG_FEEDER_BACKEND_UPSTREAM:rag_backend:4000} }` and `handle { root * /srv; try_files {path} /index.html; file_server }`; `docker-compose.prod.yml` sets `rag_feeder.build.target: runner` and `volumes: !reset []`.

- [ ] **Step 1: Write `frontend/tests/prod-image.sh`** (fails until the stage exists)

```bash
#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
docker compose -f docker-compose.yml -f docker-compose.prod.yml build rag_feeder
NET=$(docker network ls --format '{{.Name}}' | grep -m1 'rag-network')
CID=$(docker run -d --rm --network "$NET" -p 127.0.0.1:5176:5175 "$(docker compose -f docker-compose.yml -f docker-compose.prod.yml images -q rag_feeder | head -1)")
trap 'docker stop "$CID" >/dev/null' EXIT
sleep 2
code() { curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:5176$1"; }
[ "$(code /)" = 200 ]
curl -s http://127.0.0.1:5176/ | grep -q '<div id="app"'
[ "$(code /api/openalex/quota)" = 401 ]          # proxied to the backend, which wants auth
[ "$(code /src/App.svelte)" = 200 ] && exit 1     # must not serve source
[ "$(code /Dockerfile)" = 404 ]; [ "$(code /.env)" = 404 ]
curl -s -D - -o /dev/null http://127.0.0.1:5176/assets/ | head -1
echo "prod image OK"
```

Adjust the `/src/App.svelte` line so a 404 passes and anything else fails.

- [ ] **Step 2: Run it, expect failure at build (no `Caddyfile`)**

- [ ] **Step 3: Implement**: `runner` stage `FROM caddy:2-alpine`, `COPY --from=build /app/dist /srv`, `COPY Caddyfile /etc/caddy/Caddyfile`, `EXPOSE 5175`. `Caddyfile` as in Interfaces; Caddy proxies WebSocket upgrades on `/api/ws` without extra config. `docker-compose.prod.yml` overrides only `rag_feeder`: `build.target: runner`, `volumes: !reset []`, `environment` without the `VITE_*` dev entries. The `fs.deny` stopgap in `vite.config.js` stays for the dev stack.

- [ ] **Step 4: Run the script; expect `prod image OK`**

- [ ] **Step 5: README**: in "Live Deploys Behind Caddy", add the production-serving command `docker compose -f docker-compose.yml -f docker-compose.prod.yml -f docker-compose.override.yml up -d --build rag_feeder`, state that the public Caddy still targets port 5175 of the frontend container, that `/api/*` is proxied inside, and that `genkia.de` keeps the dev server on purpose.

- [ ] **Step 6: Commit**

```bash
git add frontend/Dockerfile frontend/Caddyfile docker-compose.prod.yml frontend/tests/prod-image.sh README.md
git commit -m "Production frontend image: Caddy serves dist and proxies /api; compose prod overlay"
```

---

## Self-review notes

- Spec coverage: problems 1–5 map to Tasks 1, 2, 3+4, 5, 6. Non-goals untouched.
- Types: `listCorpus`/`listDownloads`/`listIngestRuns`/`readIngestStats` take an open Database; routes pass `authDb`. `corpusId` is `number|null` everywhere.
- Review Focus 1→T1, 2→T4, 3→T3, 4→T5, 5→T6.
