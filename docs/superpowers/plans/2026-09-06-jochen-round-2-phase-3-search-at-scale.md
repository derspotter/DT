# Jochen Round 2 — Phase 3: Search at Scale — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A keyword search that may match 100,000+ works no longer blocks the request, no longer holds everything in memory, and no longer dumps every item into the browser: the user sees a match count up front, gets a warning above a threshold, watches the run fill in page by page with a cancel button, and pages through the seed's items server-side.

**Architecture:** Search runs become stateful rows (`status`, `fetched_count`, `expected_count`, `error`, `finished_at` on `search_runs`). The Python script creates the run first, writes each OpenAlex page straight into `search_results`, and prints progress lines; the backend answers as soon as the run id exists and keeps the child running. The existing 3-second live-refresh poll carries progress to the UI through the seed list. The candidates endpoint gains limit/offset/sort and does its paging and sorting in SQL for search seeds. No new worker job type, no server-sent events.

**Tech Stack:** Node/Express + better-sqlite3 (`backend/src/app.js`, `backend/src/seed.js`), Python 3 (`backend/scripts/keyword_search.py`, `dl_lit_project/dl_lit/keyword_search.py`, `dl_lit_project/dl_lit/db_manager.py`), Svelte (`frontend/src/App.svelte`, `frontend/src/lib/api.js`), Jest, pytest, Playwright.

**Spec:** `docs/superpowers/specs/2026-09-03-jochen-round-2-design.md`, section "Phase 3 — Search at scale (items 2, 3, 4)". The spec is the binding authority where this plan and the code disagree.

## Global Constraints

- Branch: `jochen-todos` or a branch off it after PR #65 merges; line anchors verified at `b0c7b43` on 2026-09-06. Match on quoted code if a line has moved.
- Threshold env `RAG_FEEDER_SEARCH_WARN_THRESHOLD`, default `100000`, admin setting key `search_warn_threshold`. Inline results limit env `RAG_FEEDER_SEARCH_INLINE_RESULTS`, default `1000`. State-count limit env `RAG_FEEDER_SEED_STATE_COUNT_LIMIT`, default `2000`.
- `search_runs.status` values: `running | done | failed | cancelled`; NULL on legacy rows means done.
- Progress lines from the Python script are single-line JSON on **stdout**: `{"event":"run_created","runId":N}` first, then `{"event":"progress","fetched":n,"expected":m}`, then the final payload as today. The script's DB work runs inside `contextlib.redirect_stdout`, so progress must be written to the real stdout (`sys.__stdout__`) and flushed.
- Candidates endpoint: `limit` default 200, max 2000; `offset`; `sort` in `title|authors|year|source|metadata|download|refs|cited_by`; `dir` in `asc|desc`. Response `{ candidates, total, offset, limit, source_summary }`. `metadata` and `download` sorts require resolving every row and are disabled in the UI when `total > 2000`.
- Copy, verbatim: match count `About {n} works match`; warning `This search matches {n} works. Fetching all of them takes about {requests} OpenAlex requests and roughly {minutes} minutes. Narrow the query, or:` with buttons `Cap at {threshold/10}` and `Fetch all {n}`; running subtitle `fetching {fetched} of {expected} · cancel`; failed `stopped after {fetched} of {expected}: {error}`; cancelled `cancelled at {fetched} of {expected}`; paging `Showing {loaded} of {total}` and `Show more`; all-selected label `All {total} items selected`.
- Request estimate = `ceil(count / 200)`; minutes estimate = `requests / (RAG_FEEDER_OPENALEX_RPS or 30) / 60`, shown with one decimal.
- Never let a progress or preview failure sink the search: preview errors skip the warning; a progress-line parse error is ignored.
- Commit trailer on every commit: `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>` and `Claude-Session: https://claude.ai/code/session_01U3c4gfg7jQvvmPdpLNBN5T`.
- Tests: `cd backend && npm test`; `cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests -q`; `cd frontend && set -a && . ../.env && set +a && npx playwright test`. The live e2e suite isolates itself in a scratch corpus; keep it that way.

---

## File structure

| File | Responsibility in this phase |
|---|---|
| `dl_lit_project/dl_lit/db_manager.py` | `search_runs` columns migration; `update_search_run_progress`, `finish_search_run`, `count_search_results` |
| `dl_lit_project/dl_lit/keyword_search.py` | `search_openalex(..., on_page=None)` callback; `count_openalex(...)` preflight |
| `backend/scripts/keyword_search.py` | `--count-only`; create run first; page-by-page persistence; progress lines; SIGTERM → cancelled; inline-results cap |
| `backend/src/app.js` | preview route; background spawn + `activeSearchRuns`; cancel route; startup orphan marking; candidates paging/sort params; dismiss `all: true`; `search_warn_threshold` setting |
| `backend/src/seed.js` | run status on seed sources; SQL paging/sorting/filter for search seeds; `state_counts` size limit; `dismissAllSeedCandidates` |
| `frontend/src/lib/api.js` | `previewKeywordSearch`, `cancelKeywordSearch`, `fetchSeedCandidates` with paging, `dismissSeedCandidates({ all })` |
| `frontend/src/App.svelte` | preflight + warning, non-blocking search, running/failed/cancelled subtitles with cancel, per-seed paging state, server-side sort, all-selected mode |
| `frontend/src/components/AdminPanel.svelte` | `search_warn_threshold` field |

---

## Task 1: Search-run status columns and DB helpers (Python)

**Files:**
- Modify: `dl_lit_project/dl_lit/db_manager.py` (add `_ensure_search_runs_columns` next to `_ensure_works_schema_columns` at ~line 69; call it where `_ensure_works_schema_columns()` is called at ~line 158; new methods after `add_search_results` ~line 2517)
- Test: `dl_lit_project/tests/test_search_run_status.py` (new)

**Interfaces:**
- Produces: columns `search_runs.status TEXT`, `fetched_count INTEGER`, `expected_count INTEGER`, `error TEXT`, `finished_at TIMESTAMP`. Methods:
  - `create_search_run(query, filters=None, status=None) -> int` (existing signature plus optional `status`)
  - `update_search_run_progress(run_id: int, fetched: int, expected: int | None) -> None`
  - `finish_search_run(run_id: int, status: str, error: str | None = None) -> None` (sets `finished_at = CURRENT_TIMESTAMP`)
  - `count_search_results(run_id: int) -> int`

- [ ] **Step 1: Write the failing tests**

Create `dl_lit_project/tests/test_search_run_status.py`:

```python
from dl_lit.db_manager import DatabaseManager


def _db(tmp_path):
    return DatabaseManager(db_path=tmp_path / "t.db")


def test_search_runs_has_status_columns(tmp_path):
    db = _db(tmp_path)
    cols = db._table_columns("search_runs")
    assert {"status", "fetched_count", "expected_count", "error", "finished_at"} <= cols
    db.close_connection()


def test_progress_and_finish_round_trip(tmp_path):
    db = _db(tmp_path)
    run_id = db.create_search_run(query="q", filters={"mode": "query"}, status="running")
    row = db.conn.execute("SELECT status, fetched_count, expected_count FROM search_runs WHERE id = ?", (run_id,)).fetchone()
    assert tuple(row) == ("running", 0, None)

    db.update_search_run_progress(run_id, fetched=400, expected=12000)
    row = db.conn.execute("SELECT fetched_count, expected_count FROM search_runs WHERE id = ?", (run_id,)).fetchone()
    assert tuple(row) == (400, 12000)

    db.finish_search_run(run_id, "failed", error="boom")
    row = db.conn.execute("SELECT status, error, finished_at IS NOT NULL FROM search_runs WHERE id = ?", (run_id,)).fetchone()
    assert tuple(row) == ("failed", "boom", 1)
    db.close_connection()


def test_legacy_run_has_null_status(tmp_path):
    db = _db(tmp_path)
    run_id = db.create_search_run(query="legacy")
    assert db.conn.execute("SELECT status FROM search_runs WHERE id = ?", (run_id,)).fetchone()[0] is None
    db.close_connection()


def test_count_search_results(tmp_path):
    db = _db(tmp_path)
    run_id = db.create_search_run(query="q")
    db.add_search_results(run_id, [{"openalex_id": "W1", "title": "a"}, {"openalex_id": "W2", "title": "b"}])
    assert db.count_search_results(run_id) == 2
    db.close_connection()
```

- [ ] **Step 2: Run them and confirm they fail**

Run: `cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests/test_search_run_status.py -q`
Expected: FAIL (`status` not in columns; `create_search_run() got an unexpected keyword argument 'status'`).

- [ ] **Step 3: Implement**

In `db_manager.py`, after `_ensure_works_schema_columns`:

```python
    def _ensure_search_runs_columns(self) -> None:
        """Run lifecycle for background keyword searches (round 2, phase 3)."""
        if "search_runs" not in self._existing_tables():
            return
        self._ensure_column("search_runs", "status", "TEXT")
        self._ensure_column("search_runs", "fetched_count", "INTEGER")
        self._ensure_column("search_runs", "expected_count", "INTEGER")
        self._ensure_column("search_runs", "error", "TEXT")
        self._ensure_column("search_runs", "finished_at", "TIMESTAMP")
```

If there is no `_existing_tables` helper, use `self._table_columns("search_runs")` being non-empty as the guard (it returns an empty set for a missing table — verify with `PRAGMA table_info`). Call `self._ensure_search_runs_columns()` right after the existing `self._ensure_works_schema_columns()` call (~line 158).

Replace `create_search_run`:

```python
    def create_search_run(self, query: str, filters: dict | None = None, status: str | None = None) -> int:
        """Create a search run. `status` is 'running' for background fetches; None for legacy one-shot runs."""
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT INTO search_runs (query, filters_json, status, fetched_count) VALUES (?, ?, ?, ?)",
            (query, json.dumps(filters) if filters else None, status, 0 if status else None),
        )
        self.conn.commit()
        return cursor.lastrowid

    def update_search_run_progress(self, run_id: int, fetched: int, expected: int | None) -> None:
        self.conn.execute(
            "UPDATE search_runs SET fetched_count = ?, expected_count = ? WHERE id = ?",
            (int(fetched), int(expected) if expected is not None else None, int(run_id)),
        )
        self.conn.commit()

    def finish_search_run(self, run_id: int, status: str, error: str | None = None) -> None:
        if status not in ("done", "failed", "cancelled"):
            raise ValueError(f"Unknown search run status: {status}")
        self.conn.execute(
            "UPDATE search_runs SET status = ?, error = ?, finished_at = CURRENT_TIMESTAMP WHERE id = ?",
            (status, error, int(run_id)),
        )
        self.conn.commit()

    def count_search_results(self, run_id: int) -> int:
        row = self.conn.execute("SELECT COUNT(*) FROM search_results WHERE search_run_id = ?", (int(run_id),)).fetchone()
        return int(row[0] if row else 0)
```

- [ ] **Step 4: Run the Python suite**

Run: `cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests -q`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add dl_lit_project/dl_lit/db_manager.py dl_lit_project/tests/test_search_run_status.py
git commit -m "Search runs carry status, progress counts and an error"
```

---

## Task 2: Page callback and preflight count in `search_openalex`

**Files:**
- Modify: `dl_lit_project/dl_lit/keyword_search.py:163-257` (`search_openalex`), new `count_openalex` after it
- Test: `dl_lit_project/tests/test_keyword_search.py`

**Interfaces:**
- Produces: `search_openalex(..., on_page=None)` where `on_page(items: list[dict], meta: dict) -> None` is called once per fetched page with the deduplicated new items of that page and OpenAlex's `meta` (which carries `count`). Return value unchanged (the full list) so existing callers keep working. `count_openalex(query, year_from, year_to, mailto, field, author) -> int` builds the identical params with `per-page=1` and returns `meta.count` (0 when the author filter resolves nobody).

- [ ] **Step 1: Write the failing tests**

Append to `dl_lit_project/tests/test_keyword_search.py`:

```python
def test_search_openalex_reports_each_page(monkeypatch):
    pages = [
        {"results": [{"id": "W1"}, {"id": "W2"}], "meta": {"count": 3, "next_cursor": "c2"}},
        {"results": [{"id": "W2"}, {"id": "W3"}], "meta": {"count": 3, "next_cursor": None}},
    ]
    calls = iter(pages)
    monkeypatch.setattr(keyword_search, "_openalex_request", lambda endpoint, params, rl, retries=3: next(calls))
    seen = []
    out = keyword_search.search_openalex(query="x", max_results=None, on_page=lambda items, meta: seen.append(([i["id"] for i in items], meta["count"])))
    assert [i["id"] for i in out] == ["W1", "W2", "W3"]
    # The duplicate W2 on page two is not reported twice.
    assert seen == [(["W1", "W2"], 3), (["W3"], 3)]


def test_count_openalex_uses_per_page_one(monkeypatch):
    captured = {}

    def fake(endpoint, params, rl, retries=3):
        captured.update(params)
        return {"results": [], "meta": {"count": 342118}}

    monkeypatch.setattr(keyword_search, "_openalex_request", fake)
    assert keyword_search.count_openalex(query="labour") == 342118
    assert captured["per-page"] == 1
    assert "cursor" not in captured
```

- [ ] **Step 2: Run and confirm failure**

Run: `cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests/test_keyword_search.py -q -k "each_page or count_openalex"`
Expected: FAIL (`unexpected keyword argument 'on_page'`; no attribute `count_openalex`).

- [ ] **Step 3: Implement**

Refactor the parameter building in `search_openalex` into a helper so the count query is guaranteed identical:

```python
def _build_search_params(query, year_from, year_to, mailto, field, author, sort):
    """Everything search_openalex sends except pagination. Returns (params, openalex_query)."""
    raw_query = (query or '').strip()
    openalex_query = build_openalex_query_text(raw_query) if raw_query else ''
    sort_value = None
    if sort:
        sort_value = SORT_OPTIONS.get(str(sort).strip().lower())
        if sort_value is None:
            raise ValueError(f"Unknown sort option: {sort}")
    params = {
        "select": "id,doi,display_name,authorships,publication_year,type,abstract_inverted_index,keywords,primary_location,open_access,biblio,referenced_works_count,cited_by_count",
    }
    if mailto:
        params["mailto"] = mailto
    # ... move the existing field_key / filters / year / author logic here unchanged,
    # ending with the relevance guard:
    if sort_value and not (sort_value.startswith("relevance") and not openalex_query):
        params["sort"] = sort_value
    return params, openalex_query
```

(Move the body verbatim from the current function; the only new lines are the signature, the `return`, and dropping `per-page`/`cursor` which the callers add.) Keep the `author and not author_ids` early return by raising a private `_NoAuthorMatch` exception inside the helper and catching it in both callers (return `[]` / `0`).

Then:

```python
def search_openalex(query, max_results=200, year_from=None, year_to=None, mailto=None,
                    field="default", author=None, sort=None, on_page=None):
    try:
        params, _ = _build_search_params(query, year_from, year_to, mailto, field, author, sort)
    except _NoAuthorMatch:
        return []
    params["per-page"] = 200
    params["cursor"] = "*"
    rate_limiter = get_global_rate_limiter()
    results, seen_ids = [], set()
    while True:
        data = _openalex_request('works', params, rate_limiter)
        page_items = []
        for item in data.get("results", []):
            item_id = item.get("id")
            if not item_id or item_id in seen_ids:
                continue
            seen_ids.add(item_id)
            results.append(item)
            page_items.append(item)
            if max_results is not None and len(results) >= max_results:
                break
        if on_page is not None and page_items:
            on_page(page_items, data.get("meta") or {})
        if max_results is not None and len(results) >= max_results:
            return results
        next_cursor = (data.get("meta") or {}).get("next_cursor")
        if not next_cursor:
            break
        params["cursor"] = next_cursor
    return results


def count_openalex(query, year_from=None, year_to=None, mailto=None, field="default", author=None) -> int:
    """How many works the same search would return. One request, per-page=1."""
    try:
        params, _ = _build_search_params(query, year_from, year_to, mailto, field, author, sort=None)
    except _NoAuthorMatch:
        return 0
    params["per-page"] = 1
    data = _openalex_request('works', params, get_global_rate_limiter())
    return int((data.get("meta") or {}).get("count") or 0)
```

- [ ] **Step 4: Run the Python suite**

Run: `cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests -q`
Expected: all PASS (the existing search tests still pass because the return value is unchanged).

- [ ] **Step 5: Commit**

```bash
git add dl_lit_project/dl_lit/keyword_search.py dl_lit_project/tests/test_keyword_search.py
git commit -m "search_openalex: per-page callback and a preflight count"
```

---

## Task 3: Background-capable search script

**Files:**
- Modify: `backend/scripts/keyword_search.py:600-750` (`main`), imports at top
- Test: `dl_lit_project/tests/test_keyword_search_script.py` (new; imports the script via `importlib` the way `tests/test_corpus_list_sort.py` does)

**Interfaces:**
- Produces, on stdout, in order: `{"event":"run_created","runId":N}`; zero or more `{"event":"progress","fetched":n,"expected":m}`; the final payload `{"runId", "results", "source", "mode", "expansion", "fetched_count", "truncated_results": bool}`. `results` is empty and `truncated_results` true when `fetched_count > RAG_FEEDER_SEARCH_INLINE_RESULTS`.
- New flags: `--count-only` (prints `{"count": N}` and exits 0), `--inline-results-limit N` (default from env, 1000).
- SIGTERM → `finish_search_run(run_id, 'cancelled')`, exit 0. Any exception after run creation → `finish_search_run(run_id, 'failed', error=...)`, then re-raise (non-zero exit).

- [ ] **Step 1: Write the failing test**

Create `dl_lit_project/tests/test_keyword_search_script.py`:

```python
import importlib.util
import json
import sys
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / 'backend' / 'scripts' / 'keyword_search.py'
SCRIPTS_DIR = SCRIPT.parent


def _load(monkeypatch):
    monkeypatch.syspath_prepend(str(SCRIPTS_DIR))
    spec = importlib.util.spec_from_file_location('keyword_search_script', SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _events(capsys):
    out = capsys.readouterr().out.strip().splitlines()
    return [json.loads(line) for line in out if line.startswith('{')]


def test_persists_each_page_and_emits_progress(monkeypatch, tmp_path, capsys):
    mod = _load(monkeypatch)
    pages = [
        [{"id": "https://openalex.org/W1", "display_name": "one"}, {"id": "https://openalex.org/W2", "display_name": "two"}],
        [{"id": "https://openalex.org/W3", "display_name": "three"}],
    ]

    def fake_search(**kwargs):
        on_page = kwargs["on_page"]
        for page in pages:
            on_page(page, {"count": 3})
        return [i for p in pages for i in p]

    monkeypatch.setattr(mod, "search_openalex", fake_search)
    monkeypatch.setattr(sys, "argv", ["x", "--db-path", str(tmp_path / "t.db"), "--query", "q", "--corpus-id", "1"])
    mod.main()
    events = _events(capsys)
    assert events[0]["event"] == "run_created"
    run_id = events[0]["runId"]
    progress = [e for e in events if e.get("event") == "progress"]
    assert [(p["fetched"], p["expected"]) for p in progress] == [(2, 3), (3, 3)]
    final = events[-1]
    assert final["runId"] == run_id and final["fetched_count"] == 3 and final["truncated_results"] is False

    from dl_lit.db_manager import DatabaseManager
    db = DatabaseManager(db_path=tmp_path / "t.db")
    row = db.conn.execute("SELECT status, fetched_count, expected_count FROM search_runs WHERE id = ?", (run_id,)).fetchone()
    assert tuple(row) == ("done", 3, 3)
    assert db.count_search_results(run_id) == 3
    db.close_connection()


def test_failure_after_run_creation_marks_failed(monkeypatch, tmp_path, capsys):
    mod = _load(monkeypatch)

    def boom(**kwargs):
        kwargs["on_page"]([{"id": "https://openalex.org/W1", "display_name": "one"}], {"count": 9})
        raise RuntimeError("openalex down")

    monkeypatch.setattr(mod, "search_openalex", boom)
    monkeypatch.setattr(sys, "argv", ["x", "--db-path", str(tmp_path / "t.db"), "--query", "q", "--corpus-id", "1"])
    try:
        mod.main()
    except RuntimeError:
        pass
    events = _events(capsys)
    run_id = events[0]["runId"]
    from dl_lit.db_manager import DatabaseManager
    db = DatabaseManager(db_path=tmp_path / "t.db")
    row = db.conn.execute("SELECT status, error, fetched_count FROM search_runs WHERE id = ?", (run_id,)).fetchone()
    assert row[0] == "failed" and "openalex down" in row[1] and row[2] == 1
    db.close_connection()


def test_count_only(monkeypatch, tmp_path, capsys):
    mod = _load(monkeypatch)
    monkeypatch.setattr(mod, "count_openalex", lambda **kwargs: 342118)
    monkeypatch.setattr(sys, "argv", ["x", "--db-path", str(tmp_path / "t.db"), "--query", "q", "--count-only"])
    mod.main()
    assert _events(capsys)[-1] == {"count": 342118}
```

- [ ] **Step 2: Run and confirm failure**

Run: `cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests/test_keyword_search_script.py -q`
Expected: FAIL (no `run_created` event; `--count-only` unrecognised).

- [ ] **Step 3: Implement**

In `backend/scripts/keyword_search.py`:

Imports: add `import signal` and `from dl_lit.keyword_search import count_openalex` to the existing import from `dl_lit.keyword_search`.

Add helpers above `main`:

```python
def _emit(event: dict) -> None:
    """Progress lines go to the real stdout; main() redirects sys.stdout while the DB works."""
    sys.__stdout__.write(json.dumps(event) + "\n")
    sys.__stdout__.flush()


def _inline_results_limit(cli_value):
    if cli_value is not None:
        return max(0, int(cli_value))
    try:
        return max(0, int(os.environ.get('RAG_FEEDER_SEARCH_INLINE_RESULTS', '1000')))
    except ValueError:
        return 1000
```

Argparse: add `parser.add_argument('--count-only', action='store_true')` and `parser.add_argument('--inline-results-limit', type=int, default=None)`.

Right after `args = parser.parse_args()` and the stub check, handle count-only:

```python
    if args.count_only:
        if args.query is None:
            print(json.dumps({'count': 0}))
            return
        count = count_openalex(query=args.query or '', year_from=args.year_from, year_to=args.year_to,
                               author=args.author, field=args.field, mailto=args.mailto)
        print(json.dumps({'count': int(count)}))
        return
```

Rewrite the `with contextlib.redirect_stdout(io.StringIO()):` block body. Keep `filters` and `run_query_label` as they are, then:

```python
        run_id = db.create_search_run(query=run_query_label, filters=filters, status='running')
        _emit({'event': 'run_created', 'runId': run_id})

        state = {'fetched': 0, 'expected': None, 'seen': set()}

        def persist(items, meta):
            records = [openalex_result_to_record(item, run_id=run_id) for item in _dedupe_openalex_items(items)]
            fresh = []
            for r in records:
                key = r.get('openalex_id') or r.get('doi') or r.get('title')
                if not key or key in state['seen']:
                    continue
                state['seen'].add(key)
                fresh.append(r)
            if fresh:
                db.add_search_results(run_id, [
                    {'openalex_id': r.get('openalex_id'), 'doi': r.get('doi'), 'title': r.get('title'),
                     'year': r.get('year'), 'raw_json': r.get('openalex_json')}
                    for r in fresh
                ])
            state['fetched'] += len(fresh)
            if state['expected'] is None and meta and meta.get('count') is not None:
                cap = effective_max_results(args.max_results)
                state['expected'] = min(int(meta['count']), cap) if cap else int(meta['count'])
            db.update_search_run_progress(run_id, state['fetched'], state['expected'])
            _emit({'event': 'progress', 'fetched': state['fetched'], 'expected': state['expected']})

        def on_sigterm(signum, frame):
            db.finish_search_run(run_id, 'cancelled')
            db.close_connection()
            sys.exit(0)

        signal.signal(signal.SIGTERM, on_sigterm)

        try:
            if is_query_mode:
                base_items = search_openalex(
                    query=args.query or '', max_results=effective_max_results(args.max_results),
                    year_from=args.year_from, year_to=args.year_to, author=args.author,
                    field=args.field, mailto=args.mailto, sort=args.sort, on_page=persist,
                )
            else:
                seeds = _parse_seed_json(args.seed_json)
                base_items = [w for w in (_resolve_seed_item(s, mailto=args.mailto) for s in seeds) if w]
                persist(base_items, {'count': len(base_items)})

            all_items, expansion_stats = expand_references_recursive(
                base_items, related_depth_downstream=related_depth_downstream,
                related_depth_upstream=related_depth_upstream, max_related=max_related,
                mailto=args.mailto, include_downstream=args.include_downstream,
                include_upstream=args.include_upstream, related_sort=args.related_sort,
            )
            # Expansion results are new items beyond the base set: persist the delta.
            base_ids = {_normalize_candidate_id(i.get('id')) for i in base_items}
            extra = [i for i in all_items if _normalize_candidate_id(i.get('id')) not in base_ids]
            if extra:
                state['expected'] = (state['expected'] or 0) + len(extra)
                persist(extra, None)

            records = [openalex_result_to_record(item, run_id=run_id) for item in _dedupe_openalex_items(all_items)]
            records = dedupe_results(records)
            if args.enqueue:
                for record in records:
                    db.add_entry_to_download_queue(record, corpus_id=args.corpus_id)
            db.finish_search_run(run_id, 'done')
        except SystemExit:
            raise
        except Exception as exc:
            db.finish_search_run(run_id, 'failed', error=str(exc)[:500])
            db.close_connection()
            raise
        db.close_connection()

    limit = _inline_results_limit(args.inline_results_limit)
    truncated = state['fetched'] > limit
    payload = {
        'runId': run_id,
        'results': [] if truncated else _render_results(records),
        'source': 'openalex',
        'mode': mode_label,
        'expansion': expansion_stats,
        'fetched_count': state['fetched'],
        'truncated_results': truncated,
    }
    print(json.dumps(payload))
```

Remove the old single `db.add_search_results(run_id, [...])` call: persistence now happens in `persist`. `count_openalex` must be imported from `dl_lit.keyword_search`.

- [ ] **Step 4: Run the Python suite**

Run: `cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests -q`
Expected: all PASS.

- [ ] **Step 5: Smoke against OpenAlex (one small real run)**

```bash
cd /home/jay/DT && set -a && . ./.env && set +a && /home/jay/DT/venv/bin/python backend/scripts/keyword_search.py --db-path /tmp/claude-1000/-home-jay-DT/554d271b-8238-40a5-a3ec-58c8c717a15c/scratchpad/smoke.db --query "institutional economics" --max-results 3 --corpus-id 1
```

Expected: a `run_created` line, one `progress` line with `fetched: 3`, then the payload with `fetched_count: 3`. Then `--count-only --query "institutional economics"` prints a count in the tens of thousands.

- [ ] **Step 6: Commit**

```bash
git add backend/scripts/keyword_search.py dl_lit_project/tests/test_keyword_search_script.py
git commit -m "keyword_search.py: create the run first, persist per page, emit progress, --count-only"
```

---

## Task 4: Preview route, background search route, cancel route, startup orphan marking

**Files:**
- Modify: `backend/src/app.js` — `/api/keyword-search` route (~line 4811); new routes beside it; startup block after `pruneOrphanedCorpusRows` call; `APP_SETTING_DEFS` (~line 1996) gains `search_warn_threshold`
- Test: `backend/tests/keyword-search.test.js`, `backend/tests/search-run-lifecycle.test.js` (new)

**Interfaces:**
- Produces:
  - `POST /api/keyword-search/preview` → `{ count: number, threshold: number }` (same body as search; stub mode returns `{ count: 2, threshold }`).
  - `POST /api/keyword-search` → `202 { runId, status: 'running' }` as soon as the run exists; stub mode unchanged (`200` with results, `runId: 0`).
  - `POST /api/keyword-search/:runId/cancel` → `200 { cancelled: true }` if a child is running for that id, else `404`.
  - `markOrphanedSearchRuns(db, startedAtIso)` exported: sets `status='failed', error='backend restarted'` on rows with `status='running'` and `created_at < startedAtIso`.
  - `activeSearchRuns: Map<number, ChildProcess>` module-level.
  - Admin setting `search_warn_threshold` (env `RAG_FEEDER_SEARCH_WARN_THRESHOLD`), validated as a positive integer like `openalex_rps`.

- [ ] **Step 1: Write the failing tests**

Append to `backend/tests/keyword-search.test.js` (inside the existing describe, stub mode):

```js
  test('preview returns a count and the threshold in stub mode', async () => {
    const res = await request(app).post('/api/keyword-search/preview').send({ query: 'x' })
    expect(res.status).toBe(200)
    expect(res.body).toMatchObject({ count: 2, threshold: 100000 })
  })

  test('cancel of an unknown run is 404', async () => {
    const res = await request(app).post('/api/keyword-search/999999/cancel')
    expect(res.status).toBe(404)
  })
```

Create `backend/tests/search-run-lifecycle.test.js`:

```js
import Database from 'better-sqlite3'
import { markOrphanedSearchRuns } from '../src/app.js'

test('runs still "running" from before this backend started are marked failed', () => {
  const db = new Database(':memory:')
  db.exec(`CREATE TABLE search_runs (id INTEGER PRIMARY KEY, query TEXT, status TEXT, error TEXT, finished_at TIMESTAMP, created_at TIMESTAMP)`)
  db.prepare(`INSERT INTO search_runs (id, query, status, created_at) VALUES (1, 'old', 'running', '2026-01-01T00:00:00Z')`).run()
  db.prepare(`INSERT INTO search_runs (id, query, status, created_at) VALUES (2, 'new', 'running', '2026-12-31T00:00:00Z')`).run()
  db.prepare(`INSERT INTO search_runs (id, query, status, created_at) VALUES (3, 'done', 'done', '2026-01-01T00:00:00Z')`).run()
  const n = markOrphanedSearchRuns(db, '2026-06-01T00:00:00Z')
  expect(n).toBe(1)
  expect(db.prepare('SELECT status, error FROM search_runs WHERE id = 1').get()).toEqual({ status: 'failed', error: 'backend restarted' })
  expect(db.prepare('SELECT status FROM search_runs WHERE id = 2').get().status).toBe('running')
  expect(db.prepare('SELECT status FROM search_runs WHERE id = 3').get().status).toBe('done')
  db.close()
})
```

- [ ] **Step 2: Run and confirm failure**

Run: `cd backend && npm test -- keyword-search search-run-lifecycle`
Expected: FAIL (404 on preview; `markOrphanedSearchRuns` not exported).

- [ ] **Step 3: Implement**

Module level, near `pruneOrphanedCorpusRows`:

```js
const activeSearchRuns = new Map();

function searchWarnThreshold() {
  const raw = appSettingsEnv().RAG_FEEDER_SEARCH_WARN_THRESHOLD || process.env.RAG_FEEDER_SEARCH_WARN_THRESHOLD || '';
  const parsed = Number(raw);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : 100000;
}

export function markOrphanedSearchRuns(db, startedAtIso) {
  if (!tableExists(db, 'search_runs')) return 0;
  const cols = db.prepare('PRAGMA table_info(search_runs)').all().map((c) => c.name);
  if (!cols.includes('status')) return 0;
  const result = db
    .prepare(`UPDATE search_runs SET status = 'failed', error = 'backend restarted', finished_at = CURRENT_TIMESTAMP
              WHERE status = 'running' AND created_at < ?`)
    .run(startedAtIso);
  return result.changes;
}
```

`APP_SETTING_DEFS`: add `{ key: 'search_warn_threshold', env: 'RAG_FEEDER_SEARCH_WARN_THRESHOLD', secret: false }`. In the PUT validation, after the `openalex_rps` block, add the same positive-integer check for `search_warn_threshold` (`Number.isInteger(parsed) && parsed > 0`, error text `search_warn_threshold must be a positive integer`).

Startup, right after the `pruneOrphanedCorpusRows` call in `createApp`:

```js
  const orphanedRuns = markOrphanedSearchRuns(authDb, new Date().toISOString());
  if (orphanedRuns > 0) console.log(`[startup] Marked ${orphanedRuns} interrupted search run(s) as failed`);
```

Note: the Python migration adds the columns; the backend only reads them. If the backend starts against a DB the Python side has never opened, `markOrphanedSearchRuns` returns 0 (column guard).

Extract the argument building in `/api/keyword-search` into `function buildKeywordSearchArgs(req)` returning `{ args, error }` (move the existing validation and `args.push(...)` lines verbatim; keep the 400 for a missing query). Then:

```js
  app.post('/api/keyword-search/preview', requireAuthMiddleware, requireCorpusWriteAccess, async (req, res) => {
    const built = buildKeywordSearchArgs(req);
    if (built.error) return res.status(400).json({ error: built.error });
    const threshold = searchWarnThreshold();
    if (process.env.RAG_FEEDER_STUB === '1') {
      return res.json({ count: STUB_RESULTS.keywordResults.length, threshold });
    }
    try {
      const payload = await runPythonJson(KEYWORD_SEARCH_SCRIPT, [...built.args, '--count-only'], { dbPath: DB_PATH, corpusId: req.corpusId });
      return res.json({ count: Number(payload?.count || 0), threshold });
    } catch (error) {
      console.error('[/api/keyword-search/preview] Error:', error);
      return res.status(502).json({ error: error.message || 'Preview failed' });
    }
  });

  app.post('/api/keyword-search', requireAuthMiddleware, requireCorpusWriteAccess, (req, res) => {
    const built = buildKeywordSearchArgs(req);
    if (built.error) return res.status(400).json({ error: built.error });
    if (process.env.RAG_FEEDER_STUB === '1') {
      return res.json({ runId: 0, results: STUB_RESULTS.keywordResults, source: 'stub' });
    }
    let responded = false;
    const { child, done } = spawnPythonJson(KEYWORD_SEARCH_SCRIPT, built.args, {
      dbPath: DB_PATH,
      corpusId: req.corpusId,
      onStdoutLine: (line) => {
        if (!line.startsWith('{')) return;
        let event;
        try { event = JSON.parse(line); } catch { return; }
        if (event?.event === 'run_created' && !responded) {
          const runId = Number(event.runId);
          if (Number.isFinite(runId) && runId > 0) {
            upsertSearchRunCorpus(authDb, { searchRunId: runId, corpusId: Number(req.corpusId) });
            activeSearchRuns.set(runId, child);
            responded = true;
            res.status(202).json({ runId, status: 'running' });
          }
        }
      },
    });
    done
      .catch((error) => {
        console.error('[/api/keyword-search] Search script failed:', error?.message || error);
        if (!responded) { responded = true; res.status(500).json({ error: error?.message || 'Keyword search failed' }); }
      })
      .finally(() => {
        for (const [runId, proc] of activeSearchRuns) if (proc === child) activeSearchRuns.delete(runId);
        if (!responded) { responded = true; res.status(500).json({ error: 'Search ended before creating a run' }); }
      });
  });

  app.post('/api/keyword-search/:runId/cancel', requireAuthMiddleware, requireCorpusWriteAccess, (req, res) => {
    const runId = Number(req.params.runId);
    const child = activeSearchRuns.get(runId);
    if (!child) return res.status(404).json({ error: 'No running search with that id' });
    child.kill('SIGTERM');
    return res.json({ cancelled: true });
  });
```

The `runId` ownership check: `upsertSearchRunCorpus` ties the run to `req.corpusId`; the cancel route additionally verifies `search_run_corpora.corpus_id === req.corpusId` before killing (query `SELECT corpus_id FROM search_run_corpora WHERE search_run_id = ?`; 404 on mismatch).

- [ ] **Step 4: Run the backend suite**

Run: `cd backend && npm test`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add backend/src/app.js backend/tests/keyword-search.test.js backend/tests/search-run-lifecycle.test.js
git commit -m "Keyword search runs in the background: preview count, 202 with runId, cancel, orphan marking"
```

---

## Task 5: Run status on seed sources; SQL paging, sorting and filtering for search seeds

**Files:**
- Modify: `backend/src/seed.js` — `listSeedSources` search query (~line 780-800) and push (~line 810-830); `listSeedCandidates` search branch (~line 655-680); new `countSeedCandidates`, `dismissAllSeedCandidates`
- Modify: `backend/src/app.js` — candidates route (~line 4934); dismiss route (~line 5240)
- Test: `backend/tests/seed-state.test.js`

**Interfaces:**
- Produces:
  - `listSeedCandidates(db, corpusId, type, key, { stateResolver, resolveDownloadedFilePath, q, limit = null, offset = 0, sort = '', dir = 'asc' })` → array (page when `limit` given). Sorting for search seeds happens in SQL for `title|year|authors|source|refs|cited_by`; `metadata|download` sort loads all rows and sorts in JS. Pdf seeds keep the JS path and slice.
  - `countSeedCandidates(db, corpusId, type, key, { q })` → total after dismissals and filter.
  - `listSeedSources(...)` sources gain `run: { status, fetched_count, expected_count, error } | null` (search seeds only) and `state_counts: null` when `candidate_count > RAG_FEEDER_SEED_STATE_COUNT_LIMIT`; running runs with zero results are listed.
  - `dismissAllSeedCandidates(db, corpusId, type, key, { q })` → number dismissed.
  - Route: `GET .../candidates?limit&offset&sort&dir&q` → `{ candidates, total, offset, limit, source_summary }`. `POST /api/seed/candidates/dismiss` accepts `{ sourceType, sourceKey, all: true, q }` as an alternative to `candidateKeys`.

- [ ] **Step 1: Write the failing tests**

Append to `backend/tests/seed-state.test.js` (reuse `createSearchSeedDb`, which has `search_runs (id, created_at)` — extend the fixture inside the new describe with the status columns via `ALTER TABLE`):

```js
describe('search seed paging, sorting and run status', () => {
  let db

  function seedRun(n) {
    db = createSearchSeedDb()
    db.exec(`ALTER TABLE search_runs ADD COLUMN query TEXT; ALTER TABLE search_runs ADD COLUMN filters_json TEXT;
             ALTER TABLE search_runs ADD COLUMN status TEXT; ALTER TABLE search_runs ADD COLUMN fetched_count INTEGER;
             ALTER TABLE search_runs ADD COLUMN expected_count INTEGER; ALTER TABLE search_runs ADD COLUMN error TEXT;`)
    db.prepare(`UPDATE search_runs SET query = 'big', status = 'running', fetched_count = ?, expected_count = 500 WHERE id = 7`).run(n)
    const ins = db.prepare(`INSERT INTO search_results (id, search_run_id, title, year, raw_json) VALUES (?, 7, ?, ?, ?)`)
    for (let i = 1; i <= n; i += 1) {
      ins.run(i, `Title ${String(i).padStart(3, '0')}`, String(1900 + i), JSON.stringify({
        referenced_works_count: n - i, cited_by_count: i * 10,
        authorships: [{ author: { display_name: i % 2 ? 'Zed Author' : 'Anna Author' } }],
        primary_location: { source: { display_name: i % 3 ? 'Journal A' : 'Journal B' } },
      }))
    }
  }

  afterEach(() => { db?.close(); db = null })

  test('pages in SQL and reports the total', () => {
    seedRun(25)
    const page = listSeedCandidates(db, 130, 'search', '7', { limit: 10, offset: 10, sort: 'title', dir: 'asc' })
    expect(page).toHaveLength(10)
    expect(page[0].title).toBe('Title 011')
    expect(countSeedCandidates(db, 130, 'search', '7', {})).toBe(25)
  })

  test('sorts by refs descending in SQL with blanks last', () => {
    seedRun(5)
    db.prepare(`UPDATE search_results SET raw_json = '{}' WHERE id = 3`).run()
    const rows = listSeedCandidates(db, 130, 'search', '7', { limit: 5, offset: 0, sort: 'refs', dir: 'desc' })
    expect(rows.map((r) => r.refs_count)).toEqual([4, 3, 1, 0, null])
  })

  test('filter and dismissals apply before paging', () => {
    seedRun(6)
    db.prepare(`INSERT INTO seed_candidates_dismissed (corpus_id, source_type, source_key, candidate_key) VALUES (130, 'search', '7', 'search:1')`).run()
    expect(countSeedCandidates(db, 130, 'search', '7', { q: 'title 00' })).toBe(5)
    const rows = listSeedCandidates(db, 130, 'search', '7', { q: 'title 00', limit: 2, offset: 0, sort: 'title', dir: 'asc' })
    expect(rows.map((r) => r.title)).toEqual(['Title 002', 'Title 003'])
  })

  test('seed sources carry the run status and skip state counts above the limit', () => {
    process.env.RAG_FEEDER_SEED_STATE_COUNT_LIMIT = '10'
    try {
      seedRun(12)
      const [source] = listSeedSources(db, 130)
      expect(source.run).toEqual({ status: 'running', fetched_count: 12, expected_count: 500, error: null })
      expect(source.candidate_count).toBe(12)
      expect(source.state_counts).toBeNull()
    } finally {
      delete process.env.RAG_FEEDER_SEED_STATE_COUNT_LIMIT
    }
  })

  test('a running run with no results yet is still listed', () => {
    seedRun(0)
    expect(listSeedSources(db, 130).map((s) => s.source_key)).toEqual(['7'])
  })

  test('dismissAllSeedCandidates dismisses the filtered set', () => {
    seedRun(4)
    expect(dismissAllSeedCandidates(db, 130, 'search', '7', { q: 'title 00' })).toBe(4)
    expect(countSeedCandidates(db, 130, 'search', '7', {})).toBe(0)
  })
})
```

Add `countSeedCandidates, dismissAllSeedCandidates` to the import at the top of the test file.

- [ ] **Step 2: Run and confirm failure**

Run: `cd backend && npm test -- seed-state`
Expected: FAIL (`countSeedCandidates` is not a function; `listSeedCandidates` ignores `limit`).

- [ ] **Step 3: Implement in `seed.js`**

Shared SQL fragments for the search branch:

```js
const SEARCH_SORT_SQL = {
  title: 'LOWER(COALESCE(sr.title, ""))',
  year: 'CAST(sr.year AS INTEGER)',
  authors: 'LOWER(COALESCE(json_extract(sr.raw_json, "$.authorships[0].author.display_name"), ""))',
  source: 'LOWER(COALESCE(json_extract(sr.raw_json, "$.primary_location.source.display_name"), ""))',
  refs: 'json_extract(sr.raw_json, "$.referenced_works_count")',
  cited_by: 'json_extract(sr.raw_json, "$.cited_by_count")',
}
const SEARCH_JS_SORTS = new Set(['metadata', 'download'])

function seedStateCountLimit() {
  const parsed = Number(process.env.RAG_FEEDER_SEED_STATE_COUNT_LIMIT || 2000)
  return Number.isInteger(parsed) && parsed > 0 ? parsed : 2000
}

function searchCandidateWhere(corpusId, runId, sourceRef, needle) {
  const where = ['src.corpus_id = ?', 'sr.search_run_id = ?',
    `NOT EXISTS (SELECT 1 FROM seed_candidates_dismissed d
                 WHERE d.corpus_id = ? AND d.source_type = 'search' AND d.source_key = ? AND d.candidate_key = 'search:' || sr.id)`]
  const params = [corpusId, runId, corpusId, sourceRef]
  if (needle) {
    where.push(`(LOWER(COALESCE(sr.title, '')) LIKE ? OR LOWER(COALESCE(json_extract(sr.raw_json, '$.authorships[0].author.display_name'), '')) LIKE ?
                 OR LOWER(COALESCE(json_extract(sr.raw_json, '$.primary_location.source.display_name'), '')) LIKE ?)`)
    const like = `%${needle}%`
    params.push(like, like, like)
  }
  return { where: where.join(' AND '), params }
}
```

`countSeedCandidates`:

```js
export function countSeedCandidates(db, corpusId, sourceType, sourceKey, { q = '' } = {}) {
  const needle = String(q || '').trim().toLowerCase()
  const sourceKind = String(sourceType || '').trim().toLowerCase()
  const sourceRef = String(sourceKey || '').trim()
  if (sourceKind === 'pdf') {
    return listSeedCandidates(db, corpusId, 'pdf', sourceRef, { q }).length
  }
  const runId = Number(sourceRef)
  if (!Number.isFinite(runId) || runId <= 0) return 0
  const { where, params } = searchCandidateWhere(corpusId, runId, sourceRef, needle)
  return db.prepare(`SELECT COUNT(*) AS n FROM search_results sr JOIN search_run_corpora src ON src.search_run_id = sr.search_run_id WHERE ${where}`).get(...params).n
}
```

In `listSeedCandidates`, extend the signature with `limit = null, offset = 0, sort = '', dir = 'asc'`. Pdf branch: after the existing filter chain, if `limit` is set, `.slice(offset, offset + limit)` (the pdf branch keeps its existing state-first ordering). Search branch:

```js
  const { where, params } = searchCandidateWhere(corpusId, runId, sourceRef, needle)
  const sortKey = String(sort || '').trim().toLowerCase()
  const direction = String(dir || 'asc').toLowerCase() === 'desc' ? 'DESC' : 'ASC'
  const sqlSort = SEARCH_SORT_SQL[sortKey]
  // Blanks last in both directions: NULL/'' sort after real values.
  const orderBy = sqlSort
    ? `(${sqlSort} IS NULL OR ${sqlSort} = '') ASC, ${sqlSort} ${direction}, sr.id DESC`
    : 'sr.id DESC'
  const useSqlPaging = limit !== null && !SEARCH_JS_SORTS.has(sortKey)
  const pageSql = useSqlPaging ? ' LIMIT ? OFFSET ?' : ''
  const rows = db.prepare(
    `SELECT sr.id, sr.search_run_id, sr.title, sr.doi, sr.openalex_id, sr.year, sr.raw_json, s.created_at
     FROM search_results sr
     JOIN search_run_corpora src ON src.search_run_id = sr.search_run_id
     JOIN search_runs s ON s.id = sr.search_run_id
     WHERE ${where}
     ORDER BY ${orderBy}${pageSql}`
  ).all(...params, ...(useSqlPaging ? [Number(limit), Number(offset) || 0] : []))
  let candidates = rows.map((row) => applyExplicitCorpusMembership(normalizeSearchCandidate(row, resolverBundle), inCorpusMarked))
  if (SEARCH_JS_SORTS.has(sortKey)) {
    const label = sortKey === 'metadata' ? (c) => String(c.metadata_status || c.state || '') : (c) => String(c.download_status || c.state || '')
    candidates.sort((a, b) => label(a).localeCompare(label(b)) * (direction === 'DESC' ? -1 : 1))
    if (limit !== null) candidates = candidates.slice(Number(offset) || 0, (Number(offset) || 0) + Number(limit))
  } else if (!sqlSort) {
    candidates = sortSeedCandidates(candidates)
  }
  return candidates
```

(The dismissed-key `Set` and the JS `matchesSeedQuery` filter are no longer applied on the search branch: the SQL does both. Keep them for the pdf branch.)

`listSeedSources` search query: add `sr.status, sr.fetched_count, sr.expected_count, sr.error` to the SELECT (guard with `tableHasColumn(db, 'search_runs', 'status')`, a small helper using `PRAGMA table_info`, so the seed-state fixtures without the columns keep working), change the `JOIN search_results` to `LEFT JOIN` so a running run with no rows is listed, and in the push:

```js
    const total = countSeedCandidates(db, corpusId, 'search', sourceKey, { q })
    const run = row.status !== undefined
      ? { status: row.status || null, fetched_count: row.fetched_count ?? null, expected_count: row.expected_count ?? null, error: row.error || null }
      : null
    if (total === 0 && run?.status !== 'running') return
    const withinLimit = total <= seedStateCountLimit()
    const candidates = withinLimit ? listSeedCandidates(db, corpusId, 'search', sourceKey, { stateResolver: resolver, q }) : []
    sources.push({
      // ...existing fields...
      candidate_count: total,
      state_counts: withinLimit ? summarizeStates(candidates) : null,
      run,
    })
```

`dismissAllSeedCandidates`:

```js
export function dismissAllSeedCandidates(db, corpusId, sourceType, sourceKey, { q = '' } = {}) {
  const keys = listSeedCandidates(db, corpusId, sourceType, sourceKey, { q }).map((c) => c.candidate_key)
  return dismissSeedCandidates(db, corpusId, sourceType, sourceKey, keys)
}
```

(For search seeds this loads the filtered rows once; dismissal is a one-off action, so that is acceptable.)

- [ ] **Step 4: Wire the routes in `app.js`**

Candidates route: parse `limit` (`coerceInt`, default 200, clamp 1..2000), `offset` (default 0, min 0), `sort` (must be in the eight allowed names else `''`), `dir`. Pass them to `listSeedCandidates`; compute `total` via `countSeedCandidates(...)`; respond `{ source: 'db', source_summary, candidates, total, offset, limit }`.

Dismiss route: if `req.body?.all === true`, call `dismissAllSeedCandidates(authDb, req.corpusId, sourceType, sourceKey, { q: String(req.body?.q || '') })` instead of requiring `candidateKeys`.

- [ ] **Step 5: Run the backend suite**

Run: `cd backend && npm test`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add backend/src/seed.js backend/src/app.js backend/tests/seed-state.test.js
git commit -m "Search seeds: SQL paging, sorting and filter; run status on seed sources; dismiss all"
```

---

## Task 6: API client additions

**Files:**
- Modify: `frontend/src/lib/api.js` — `fetchSeedCandidates` (~line 417), `runKeywordSearch` (~line 588), `dismissSeedCandidates` (~line 500); new `previewKeywordSearch`, `cancelKeywordSearch`

**Interfaces:**
- Produces:
  - `fetchSeedCandidates(type, key, { q, limit, offset, sort, dir })` → `{ candidates, total, offset, limit, source_summary }`
  - `previewKeywordSearch(body)` → `{ count, threshold }`
  - `runKeywordSearch(body)` → `{ data, source, runId, expansion, running: boolean }` (`running` true on a 202)
  - `cancelKeywordSearch(runId)` → `{ cancelled }`
  - `dismissSeedCandidates(type, key, candidateKeys, { all = false, q = '' } = {})`

- [ ] **Step 1: Implement**

```js
export async function fetchSeedCandidates(sourceType, sourceKey, { q = '', limit = 200, offset = 0, sort = '', dir = 'asc' } = {}) {
  const params = new URLSearchParams()
  if (q) params.set('q', q)
  params.set('limit', String(limit))
  params.set('offset', String(offset))
  if (sort) { params.set('sort', sort); params.set('dir', dir) }
  const response = await fetchWithTimeout(
    `${API_BASE}/api/seed/sources/${encodeURIComponent(String(sourceType || ''))}/${encodeURIComponent(String(sourceKey || ''))}/candidates?${params}`
  )
  await throwIfUnauthorized(response)
  if (!response.ok) throw new Error((await response.text()) || 'Failed to load seed candidates')
  return response.json()
}

export async function previewKeywordSearch(body) {
  const response = await fetchWithTimeout(`${API_BASE}/api/keyword-search/preview`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body), timeout: 60_000,
  })
  await throwIfUnauthorized(response)
  if (!response.ok) throw new Error((await response.text()) || 'Preview failed')
  return response.json()
}

export async function cancelKeywordSearch(runId) {
  const response = await fetchWithTimeout(`${API_BASE}/api/keyword-search/${encodeURIComponent(String(runId))}/cancel`, { method: 'POST' })
  await throwIfUnauthorized(response)
  if (!response.ok) throw new Error((await response.text()) || 'Cancel failed')
  return response.json()
}
```

In `runKeywordSearch`, after the response is read: `const running = response.status === 202`; include `running` in the returned object (`data` is `[]` when running). Check how the existing function builds `fetchWithTimeout` options and whether a `timeout` option exists; if not, leave the default. In `dismissSeedCandidates`, accept the fourth argument and send `{ sourceType, sourceKey, ...(all ? { all: true, q } : { candidateKeys }) }`.

- [ ] **Step 2: Commit**

```bash
git add frontend/src/lib/api.js
git commit -m "API client: search preview, cancel, paged candidates, dismiss all"
```

---

## Task 7: Preflight count and warning in the search card

**Files:**
- Modify: `frontend/src/App.svelte` — state near `let searchResults = []` (~line 259); `runSearch` (~line 3301); search card markup after `<p class="muted">{searchStatus}</p>` (~line 4540); `frontend/src/app.css`
- Test: `frontend/tests/app.spec.ts`

**Interfaces:**
- Consumes: `previewKeywordSearch`, `runKeywordSearch` from Task 6.
- Produces: `searchPreview = { count, threshold } | null`, `searchWarning: boolean`, `startSearch({ maxResultsOverride })`, and the body builder `buildSearchBody(maxResults)` used by both preview and search.

- [ ] **Step 1: Implement**

State and helpers:

```js
  let searchPreview = null
  let searchWarning = false
  let searchPreviewBusy = false

  function buildSearchBody(maxResults) {
    return {
      query: searchQuery, seedJson: '', field: searchField, author: searchAuthor, yearFrom, yearTo,
      maxResults: Math.max(0, Math.trunc(Number(maxResults) || 0)), sort: searchSort,
      includeDownstream: false, includeUpstream: false, relatedDepthDownstream: 0, relatedDepthUpstream: 0,
      maxRelated: 30, fallbackToSample: false,
    }
  }

  function searchEstimate(count) {
    const requests = Math.ceil(count / 200)
    const rps = Number(appSettings?.openalex_rps?.value || appSettings?.openalex_rps?.env_fallback || 30) || 30
    const minutes = requests / rps / 60
    return { requests, minutes: minutes.toFixed(1) }
  }

  // Submit: ask for the count first; warn above the threshold, else start.
  async function runSearch() {
    searchWarning = false
    searchStatus = 'Checking how many works match...'
    searchPreviewBusy = true
    try {
      searchPreview = await previewKeywordSearch(buildSearchBody(searchMaxResults))
    } catch (error) {
      // A failed preview must not block the search.
      searchPreview = null
    } finally {
      searchPreviewBusy = false
    }
    const cap = Math.max(0, Math.trunc(Number(searchMaxResults) || 0))
    if (searchPreview && searchPreview.count >= searchPreview.threshold && (cap === 0 || cap >= searchPreview.threshold)) {
      searchWarning = true
      searchStatus = ''
      return
    }
    await startSearch()
  }

  async function startSearch({ maxResultsOverride = null } = {}) {
    searchWarning = false
    if (maxResultsOverride !== null) searchMaxResults = maxResultsOverride
    searchStatus = 'Starting search...'
    try {
      const { data, source, expansion, runId, running } = await runKeywordSearch(buildSearchBody(searchMaxResults))
      searchSource = source
      loadOpenAlexQuota()
      if (runId) {
        await loadSeedSources({ quiet: true })
        await focusSeedSource('search', runId)
      }
      if (running) {
        searchStatus = 'Fetching in the background. The seed below fills in as pages arrive.'
        return
      }
      searchResults = data
      initializeSearchQueueConfig(data)
      searchQueueStatus = ''
      const suffix = expansion?.added ? ` (+${expansion.added} related works)` : ''
      searchStatus = `Search complete. Added ${data.length} item(s) to Seed.${suffix}`
    } catch (error) {
      if (error?.status === 401) { authStatus = 'unauthenticated'; setAuthToken(''); return }
      searchStatus = error?.message || 'Search failed.'
    }
  }
```

Markup, directly after `<p class="muted">{searchStatus}</p>` in `.seed-intake-card--search`:

```svelte
              {#if searchPreview && !searchWarning}
                <p class="muted small search-preview" data-testid="search-preview">About {searchPreview.count.toLocaleString('en-US')} works match</p>
              {/if}
              {#if searchWarning && searchPreview}
                {@const est = searchEstimate(searchPreview.count)}
                <div class="search-warning" role="alert" data-testid="search-warning">
                  <p>This search matches {searchPreview.count.toLocaleString('en-US')} works. Fetching all of them takes about {est.requests.toLocaleString('en-US')} OpenAlex requests and roughly {est.minutes} minutes. Narrow the query, or:</p>
                  <div class="search-warning__actions">
                    <button class="secondary" type="button" on:click={() => startSearch({ maxResultsOverride: Math.floor(searchPreview.threshold / 10) })}>Cap at {Math.floor(searchPreview.threshold / 10).toLocaleString('en-US')}</button>
                    <button class="primary" type="button" on:click={() => startSearch()}>Fetch all {searchPreview.count.toLocaleString('en-US')}</button>
                  </div>
                </div>
              {/if}
```

CSS (append to `app.css`):

```css
.search-preview { margin: 6px 0 0; }
.search-warning { margin-top: 8px; padding: 10px 12px; border: 1px solid #f59e0b; border-radius: var(--radius-md); background: #fffbeb; }
.search-warning p { margin: 0 0 8px; }
.search-warning__actions { display: flex; gap: 8px; justify-content: flex-end; }
```

`appSettings` is loaded only on the admin tab (`$: if (activeTab === 'admin' ...)`); for the RPS estimate it may be null, so the fallback of 30 applies. That is acceptable.

- [ ] **Step 2: E2e, mocked**

Add `frontend/tests/search-scale.spec.ts` using the route-mocking pattern from `seed-upload.spec.ts` (copy its `mockApi` skeleton for `/api/auth/me`, `/api/corpora`, `/api/ingest/stats`, `/api/corpus`, `/api/seed/sources`, `/api/openalex/quota`, `/api/pipeline/*`, `/api/downloads*` returning empty shapes as that file does). Add:

```ts
test('a search above the threshold shows the warning and "Cap at" starts a capped run', async ({ page }) => {
  let searchBody: any = null
  await page.route('**/api/**', async (route) => {
    const url = new URL(route.request().url())
    if (url.pathname === '/api/keyword-search/preview') {
      return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ count: 342118, threshold: 100000 }) })
    }
    if (url.pathname === '/api/keyword-search' && route.request().method() === 'POST') {
      searchBody = route.request().postDataJSON()
      return route.fulfill({ status: 202, contentType: 'application/json', body: JSON.stringify({ runId: 55, status: 'running' }) })
    }
    return mockApi(route)
  })
  await page.goto('/')
  const card = page.locator('.seed-intake-card--search')
  await card.getByRole('textbox', { name: 'Query' }).fill('economics')
  await card.getByRole('button', { name: 'Search', exact: true }).click()
  const warning = page.getByTestId('search-warning')
  await expect(warning).toContainText('This search matches 342,118 works')
  await warning.getByRole('button', { name: 'Cap at 10,000' }).click()
  await expect.poll(() => searchBody?.maxResults).toBe(10000)
  await expect(card.locator('p.muted').first()).toContainText('Fetching in the background')
})
```

Run: `cd frontend && npx playwright test search-scale.spec.ts`
Expected: PASS (mocked; no backend needed beyond the dev server).

- [ ] **Step 3: Commit**

```bash
git add frontend/src/App.svelte frontend/src/app.css frontend/tests/search-scale.spec.ts
git commit -m "Search card: preflight match count and the 100k warning with cap / fetch-all"
```

---

## Task 8: Running, failed and cancelled seeds in the seed list

**Files:**
- Modify: `frontend/src/App.svelte` — seed row subtitle (~line 4760); `runLiveRefreshCycle` (~line 1489); `loadSeedSources`
- Test: `frontend/tests/search-scale.spec.ts`

**Interfaces:**
- Consumes: `source.run` from Task 5; `cancelKeywordSearch` from Task 6.
- Produces: `anySearchRunning` reactive boolean; the live-refresh interval tightens to 2 s while true and reloads seed sources (and the expanded seed's page) each cycle.

- [ ] **Step 1: Implement**

Reactive state and helpers:

```js
  $: anySearchRunning = seedSources.some((s) => s?.run?.status === 'running')

  function runSubtitle(source) {
    const run = source?.run
    if (!run || !run.status || run.status === 'done') return ''
    const fetched = Number(run.fetched_count || 0).toLocaleString('en-US')
    const expected = run.expected_count === null || run.expected_count === undefined ? '…' : Number(run.expected_count).toLocaleString('en-US')
    if (run.status === 'running') return `fetching ${fetched} of ${expected}`
    if (run.status === 'failed') return `stopped after ${fetched} of ${expected}: ${run.error || 'unknown error'}`
    if (run.status === 'cancelled') return `cancelled at ${fetched} of ${expected}`
    return ''
  }

  async function handleCancelSearch(source) {
    try {
      await cancelKeywordSearch(source.source_key)
      seedSourcesStatus = 'Cancelling search…'
      await loadSeedSources({ quiet: true })
    } catch (error) {
      seedSourcesStatus = error?.message || 'Could not cancel the search.'
    }
  }
```

Markup: replace the `{#if source.subtitle}` block with:

```svelte
                      {#if runSubtitle(source)}
                        <span class={`muted small seed-run-status seed-run-status--${source.run.status}`} data-testid="seed-run-status">
                          {runSubtitle(source)}
                          {#if source.run.status === 'running'}
                            · <button type="button" class="link" on:click|stopPropagation={() => handleCancelSearch(source)}>cancel</button>
                          {/if}
                        </span>
                      {:else if source.subtitle}
                        <span class="muted small">{source.subtitle}</span>
                      {/if}
```

Live refresh: `runLiveRefreshCycle` currently reloads stats, corpus and quota. Add `loadSeedSources({ quiet: true })` to its `tasks` when `anySearchRunning` (loadSeedSources already reloads the expanded seed's candidates in background mode). Change the interval: where `setInterval(() => runLiveRefreshCycle(), 3000)` is created, keep 3000, but inside `runLiveRefreshCycle` when `anySearchRunning` schedule an extra `setTimeout(runLiveRefreshCycle, 1000)` guarded by `pipelineRefreshInFlight` so the effective cadence is ~2 s. Simpler alternative that satisfies the spec: replace the fixed interval with `setInterval(..., 3000)` plus a `$:` block that, when `anySearchRunning` flips true, starts a 2 s interval and clears it when it flips false.

CSS:

```css
.seed-run-status--running { color: #0369a1; }
.seed-run-status--failed { color: #b91c1c; }
.seed-run-status--cancelled { color: #6b7280; }
```

- [ ] **Step 2: E2e, mocked (running → done)**

In `search-scale.spec.ts`:

```ts
test('a running seed shows progress and settles to done', async ({ page }) => {
  let polls = 0
  await page.route('**/api/seed/sources**', async (route) => {
    polls += 1
    const run = polls < 3
      ? { status: 'running', fetched_count: 400 * polls, expected_count: 1200, error: null }
      : { status: 'done', fetched_count: 1200, expected_count: 1200, error: null }
    return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ sources: [{
      id: 'search:55', source_type: 'search', seed_kind: 'search', source_key: '55', label: 'economics', subtitle: '',
      created_at: '2026-09-06T10:00:00Z', candidate_count: run.fetched_count, state_counts: null, removable: true, meta: {}, run,
    }] }) })
  })
  await page.route('**/api/**', mockApi)
  await page.goto('/')
  const status = page.getByTestId('seed-run-status')
  await expect(status).toContainText('fetching 400 of 1,200')
  await expect(status).toContainText('fetching 800 of 1,200', { timeout: 10_000 })
  await expect(status).toHaveCount(0, { timeout: 10_000 })
})
```

(Order matters: register the specific `seed/sources` route before the catch-all.)

- [ ] **Step 3: Commit**

```bash
git add frontend/src/App.svelte frontend/src/app.css frontend/tests/search-scale.spec.ts
git commit -m "Seed list shows fetching / stopped / cancelled runs with a cancel action"
```

---

## Task 9: Server-side paging, sorting and all-selected mode in the seed table

**Files:**
- Modify: `frontend/src/App.svelte` — `loadSeedCandidatesForSource` (~line 3026), `toggleSeedSort` (~2857), `sortSeedCandidates` (~2873), selection helpers (~2895-2990), `handleDismissSelectedSeed` (~3242), `promoteSeedCandidateKeys`/`handlePromoteWholeSeedSource` (~3119-3242), the seed table footer markup (after the `{#each sourceCandidates}` loop), header sort buttons (~4800)
- Test: `frontend/tests/search-scale.spec.ts`, live `frontend/tests/app.spec.ts`

**Interfaces:**
- Consumes: paged `fetchSeedCandidates`, `dismissSeedCandidates(..., { all, q })`.
- Produces: `seedPages[sourceId] = { total, offset, limit }`, `seedAllSelected[sourceId]: boolean`; `loadMoreSeedCandidates(source)`.

- [ ] **Step 1: Implement paging state**

```js
  let seedPages = {}          // sourceId -> { total, offset, limit }
  let seedAllSelected = {}    // sourceId -> true when "All N items selected"
  const SEED_PAGE_SIZE = 200

  function seedPage(source) { return seedPages[seedSourceId(source)] || { total: 0, offset: 0, limit: SEED_PAGE_SIZE } }
  function seedSortParams(source) {
    const current = seedSorts[seedSourceId(source)]
    return current ? { sort: current.column, dir: current.direction } : { sort: '', dir: 'asc' }
  }
```

Rewrite `loadSeedCandidatesForSource(source, { quiet, background, append = false })`:

```js
    const page = seedPage(source)
    const offset = append ? (seedCandidatesBySource[sourceId] || []).length : 0
    const payload = await fetchSeedCandidates(source.source_type, source.source_key, {
      q: seedFilterQuery, limit: SEED_PAGE_SIZE, offset, ...seedSortParams(source),
    })
    const incoming = payload.candidates || []
    const nextCandidates = append ? [...(seedCandidatesBySource[sourceId] || []), ...incoming] : incoming
    seedCandidatesBySource = { ...seedCandidatesBySource, [sourceId]: nextCandidates }
    seedPages = { ...seedPages, [sourceId]: { total: Number(payload.total || nextCandidates.length), offset, limit: SEED_PAGE_SIZE } }
```

then the existing reconciliation of `seedSelections` (unchanged). `loadMoreSeedCandidates(source)` = `loadSeedCandidatesForSource(source, { quiet: true, background: true, append: true })`.

Sorting: `toggleSeedSort` keeps updating `seedSorts`, then for search seeds calls `loadSeedCandidatesForSource(source, { quiet: true })` (offset 0). `sortSeedCandidates` returns the array untouched for search seeds (`source.source_type === 'search'`) and keeps the client-side sort for pdf seeds. In the header, disable the `metadata`/`download` sort buttons when `source.source_type === 'search' && seedPage(source).total > 2000` with `title="Sorting by state needs every item resolved; not available above 2,000 items"`.

- [ ] **Step 2: Implement all-selected mode**

- `setAllSeedCandidatesSelected(source, checked)`: when checked and `source.source_type === 'search'`, set `seedAllSelected[sourceId] = true` and also select all loaded rows; when unchecked, clear both.
- `toggleSeedCandidateSelection`: if the row is being deselected while `seedAllSelected[sourceId]`, switch to explicit mode: `seedAllSelected[sourceId] = false` and selection = all loaded selectable keys minus this one.
- `selectedSeedCount(source)`: `seedAllSelected[sourceId] ? seedPage(source).total : getSeedSelectionForSource(source).length`.
- `estimatedSelectableSeedCount(source)`: when the page total is known, use `seedPage(source).total - Number(source?.state_counts?.in_corpus || 0)` (state counts may be null above the limit: then just `total`).
- Toolbar label: `{#if seedAllSelected[sourceId]}All {seedPage(source).total.toLocaleString('en-US')} items selected{:else}Selected: {selectedSeedCount(source)} / {selectableSeedCount(source)} selectable{/if}`.
- `handlePromoteSeedSource(source)` (promote selected): if `seedAllSelected[sourceId]`, call `handlePromoteWholeSeedSource(source)` (which sends no `candidateKeys`, the existing "all promotable" semantics) instead of `promoteSeedCandidateKeys`.
- `handleDismissSelectedSeed(source)`: if `seedAllSelected[sourceId]`, call `dismissSeedCandidatesApi(source.source_type, source.source_key, [], { all: true, q: seedFilterQuery })`.

- [ ] **Step 3: Footer markup**

After the candidate rows loop inside the expanded seed table:

```svelte
                              {#if seedPage(source).total > sourceCandidates.length}
                                <div class="seed-table-footer">
                                  <span class="muted small">Showing {sourceCandidates.length.toLocaleString('en-US')} of {seedPage(source).total.toLocaleString('en-US')}</span>
                                  <button class="secondary" type="button" disabled={seedCandidatesLoading[sourceId]} on:click|stopPropagation={() => loadMoreSeedCandidates(source)}>Show more</button>
                                </div>
                              {/if}
```

CSS: `.seed-table-footer { display: flex; justify-content: space-between; align-items: center; padding: 8px 12px; }`.

- [ ] **Step 4: Tests**

Mocked, in `search-scale.spec.ts`: a seed with `candidate_count: 300`; the candidates route returns 200 rows for offset 0 and 100 for offset 200 with `total: 300`; expand, assert `Showing 200 of 300`, click `Show more`, assert the second request had `offset=200` and the footer disappears. Then click the seed's select-all checkbox and assert `All 300 items selected`; click the seed's "Dismiss selected" and assert the dismiss request body is `{ all: true }`.

Live, in `app.spec.ts` "seed table exposes a Refs column": no change needed; run the full live suite to confirm nothing regressed.

Run: `cd frontend && npx playwright test search-scale.spec.ts` then the full suite with `.env` loaded.
Expected: PASS; live suite 25 passed (plus the new spec's tests).

- [ ] **Step 5: Commit**

```bash
git add frontend/src/App.svelte frontend/src/app.css frontend/tests/search-scale.spec.ts
git commit -m "Seed table: server-side paging and sorting, Show more, all-items selection"
```

---

## Task 10: Admin threshold field, docs, and the live smoke

**Files:**
- Modify: `frontend/src/components/AdminPanel.svelte` (OpenAlex group, after "Requests per second"), `frontend/src/App.svelte` (`appSettingsDraft` initialisation ~line 3844 includes the new key), `.env.example`, `README.md`, `docker-compose.yml` (env passthrough for `RAG_FEEDER_SEARCH_WARN_THRESHOLD`, `RAG_FEEDER_SEARCH_INLINE_RESULTS`, `RAG_FEEDER_SEED_STATE_COUNT_LIMIT` on `rag_backend`)

- [ ] **Step 1: Admin field**

```svelte
        <label>
          <span class="muted small">Warn when a search matches at least</span>
          <input type="number" min="1000" step="1000" placeholder="100000" bind:value={appSettingsDraft.search_warn_threshold} />
          <span class="muted small">{fallbackHint(appSettings.search_warn_threshold)}</span>
        </label>
```

- [ ] **Step 2: Docs**

`.env.example`:

```
# Keyword search: warn before fetching at least this many works (default 100000)
# RAG_FEEDER_SEARCH_WARN_THRESHOLD=100000
# Results above this count are not echoed inline in the search response (default 1000)
# RAG_FEEDER_SEARCH_INLINE_RESULTS=1000
# Seeds with more items than this skip the per-state pills in the seed list (default 2000)
# RAG_FEEDER_SEED_STATE_COUNT_LIMIT=2000
```

README, after the "OpenAlex daily budget" section:

```markdown
### Large keyword searches

Submitting a search first asks OpenAlex how many works match. At or above
`RAG_FEEDER_SEARCH_WARN_THRESHOLD` (default 100,000) the search card warns
with a request estimate and offers a capped run. The search itself runs in
the background: the seed appears at once and fills in page by page, with a
cancel action; a cancelled or failed run keeps what it already stored. Seeds
page their items 200 at a time and sort on the server.
```

- [ ] **Step 3: Live smoke (one uncapped search, cancelled)**

On the dev stack: search a broad term (e.g. `economics`), accept "Fetch all", watch the seed subtitle count up for ~10 seconds, click cancel, and confirm: subtitle reads `cancelled at N of M`, the seed still lists N items, and the OpenAlex budget pill dropped by roughly `N/200` requests. Note the numbers in the PR.

- [ ] **Step 4: Full verification and commit**

Run all three suites (see Global Constraints).

```bash
git add frontend/src/components/AdminPanel.svelte frontend/src/App.svelte .env.example README.md docker-compose.yml
git commit -m "Admin threshold for large searches; document the background search"
```

Open the PR titled `Round 2 phase 3: search at scale` against the base Jay names, with the smoke numbers and the three suite counts.

---

## Self-review notes

- Spec coverage: preflight count + warning (Task 3 `--count-only`, Task 4 preview, Task 7); background fetch with per-page persistence, progress, cancel, restart handling (Tasks 1, 3, 4, 8); `results` omitted above the inline limit (Task 3); paging/sorting/filter in SQL, `metadata`/`download` fallback and the 2,000 UI guard, state-count limit, running-with-zero-results listing (Tasks 5, 9); select-all semantics and dismiss-all (Tasks 5, 6, 9); admin threshold (Tasks 4, 10); e2e stub path unchanged (Task 4 keeps the stub branch returning a completed run).
- Type consistency: `run` object keys `{ status, fetched_count, expected_count, error }` are produced in Task 5 and read in Task 8. Candidates response `{ candidates, total, offset, limit, source_summary }` is produced in Task 5, wrapped in Task 6, consumed in Task 9. Sort names in the route allow-list match `SEARCH_SORT_SQL` plus the two JS sorts and the frontend column keys from `tableColumns.js`.
- Deliberate deviation from the spec: progress is not mirrored in an in-memory map on the backend; the DB row is read on each poll (one indexed row per seed), which is simpler and survives restarts identically.
