# Review fixes, phase 5: correctness, silent failures and hardening — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the Phase 1 rows of the review-fixes-5 inventory (all except PY-02; BE-40 and FE-60 in part, as the spec's non-goals record): no silent failures, no state leaking across sessions, no dropped user requests, hashing off the event loop, one-time tokens used once, atomic multi-statement writes and truthful exit codes.

**Architecture:** Six tasks by surface, in this order:
1. three Python tasks (`dl_lit_project/dl_lit/`, `backend/scripts/`);
2. one backend task (`backend/src/`, plus a small Admin panel addition);
3. two frontend tasks (`frontend/src/App.svelte` and its components).

The task adds three backend modules:
- `backend/src/passwordHasher.js`, a worker pool, with its worker `backend/src/passwordWorker.js`;
- `backend/src/authMail.js`, the auth mail template.

Every fix is pinned by a test that fails on `2c8b8b7`. This revision applies Codex's pre-flight findings 1-15 and the controller's rulings on them.

**Tech Stack:** Python 3.11 (`/home/jay/DT/venv/bin/python`), pytest, sqlite3, click 8.2; Node 20, Express 5, better-sqlite3, bcryptjs, `node:worker_threads`, Jest + supertest; Svelte 5 + Vite, Playwright (mocked).

**Spec:** `docs/superpowers/specs/2026-11-13-review-fixes-5-correctness-design.md`

## Global Constraints

- Work only in `/home/jay/DT/.worktrees/review-fixes-5`, on branch `review-fixes-5`. Do not push.
- Never open `dl_lit_project/data/literature.db`; tests use `tmp_path` / `fs.mkdtempSync` DBs.
- Python tests: `cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests -q` (whole suite) plus the task's file. Every new test file goes into the pytest list at `.github/workflows/ci.yml:68`.
- Backend tests: `cd backend && npm test` (CI runs all); prefer `npm test -- tests/<file>` while iterating. CI's Python for backend tests has only `requests`, so a backend test that spawns `ingest_import_seed.py` must use `--kind json`, which needs no bibtexparser.
- Frontend tests: from `frontend/`, `E2E_FRONTEND_PORT=5190 npx playwright test <specs> --workers=2`, mocked specs only. Never use the dev stack on `:5175` and never run `*.real*.spec.ts` or `pipeline-perf.real-pdf.spec.ts`. Every new spec goes into the Playwright list at `.github/workflows/ci.yml:54`. `npx vite build` must pass.
- Every mocked spec that fakes a token calls `mockLogSocket(page)` from `tests/helpers/mockApi.ts`.
- These strings are copied exactly from the spec:
  - D6:
    - `The log view holds 5,000 lines. Older lines stay in the log file.`
    - `Loaded <n> older log lines.`
    - `Loaded <k> of <n> older log lines; the view holds 5,000 lines.`
  - D7: `Connection lost <from>–<to>. Lines from that period may be missing.` (en dash, local `HH:MM:SS`) and the button label `Reload logs`.
  - D8: `Could not load the Kantropos corpora: <message>` and `No upstream corpora are configured.`
  - D9: `Could not load the upstream corpus: <message>.` and ` Showing the last loaded data.`
  - D10: `<transport message> Your session is kept; press Retry when the server is back.`
  - D11: `Your session has expired. Please sign in again.`
  - D12: `Your password was saved, but signing in failed (<message>). Sign in with your new password.`
  - D14: `Global totals could not be loaded.`
  - D15:
    - `Import failed: <error>`
    - `Import stopped after <processed> of <total> entries: <error> (<added> added, <corpus_linked> linked before it).`
  - D17: `The server is busy; try again in a moment.`
- These values are fixed by the spec:
  - D17: hasher `size = 2`, `maxQueue = 64`, `HASHER_BUSY` → `503` with `Retry-After: 5`, bcrypt rounds 10.
  - D23: `CORPUS_MAX_LIMIT = 1000`.
  - D29: exit code `75`, watchdog statements at `busy_timeout` 1000 ms, recovery interval 60 s.
- There is no login rate limit and no `trust proxy` change in this phase: that is an owner/ops follow-up.
- Each commit message names the inventory rows it closes and ends with:
  ```
  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01U3c4gfg7jQvvmPdpLNBN5T
  ```
- Secrets stay in `.env`; nothing from it goes into tests, docs or commits.

## Review Focus

1. **A late deadline callback from finished work.** The heartbeat thread of a job that just completed can still fire after the 1-second teardown join. It must not fail or kill the next job. Test in Task 3 (generation token).
2. **A login burst.** With hashes queued past `maxQueue`, the server must answer `503` at once and keep serving other routes. It must not queue without limit or stall. Test in Task 4.
3. **A filter typed while "Show more" is in flight.** The append's answer must not land on the new filter's rows, and the queued append must not run against the new query. Test in Task 5.
4. **A late result of an abandoned attempt after a retry succeeded.** It must not raise in any thread and must not print a traceback. It logs one line and charges its tokens once. Test in Task 2.
5. **"Load older" racing live lines.** The view fills while the older page is in flight. The answer must admit only what fits, and the cursor must advance only by what was admitted, so the next request neither skips nor repeats lines. Test in Task 5.

---

### Task 1: Python transactions and exit codes (PY-36, PY-39, PY-52, PY-53)

**Files:**
- Modify: `dl_lit_project/dl_lit/db_manager.py`, in `create_pending_work` (`:1244-1421`), `apply_enriched_metadata` (`:1524-1596`), `enqueue_for_download` (`:1598-1622`) and `queue_for_enrichment` (`:1624-1648`)
- Modify: `backend/scripts/ingest_import_seed.py:85-148` (`main`)
- Modify: `dl_lit_project/dl_lit/cli.py:461-477` (`add_pending_works_command`)
- Modify: `dl_lit_project/tests/test_ingest_batching.py:41`, `:45` (the exact summary dictionaries gain `"processed": 3, "stopped": False, "error": None`)
- Test: `dl_lit_project/tests/test_transaction_failures.py` (new)
- Modify: `.github/workflows/ci.yml:68`

**Interfaces:**
- Produces: the import summary JSON from `ingest_import_seed.py` (spec D25), printed for every outcome with exit 0:

  ```
  added: int
  skipped: int
  corpus_linked: int
  total: int
  processed: int
  stopped: bool
  error: str | None
  errors: list[str]
  ```

  Task 4's route test and Task 6's UI consume it.
- `create_pending_work` keeps its signature and return values.

- [ ] **Step 1: Write the failing tests** in `test_transaction_failures.py`. Use a temp DB from `DatabaseManager(str(tmp_path / "t.db"))`. Run the import script as `test_ingest_batching.py` does (`_load_import_script`). The IntegrityError must reach `create_pending_work`'s own handler while a transaction is open. A trigger on `work_aliases` does not do that: `add_work_alias` swallows its error, and the work is already committed. So patch `db.add_corpus_item` (it runs after the works insert when `ref["corpus_id"]` is set) with:

```python
def _failing_link(db):
    def link(corpus_id, table, work_id, commit=True):
        db.conn.execute("UPDATE works SET title = title WHERE id = ?", (work_id,))  # opens / extends the transaction
        raise sqlite3.IntegrityError("boom")
    return link

def test_integrity_error_commit_true_leaves_no_open_transaction(tmp_path, monkeypatch):
    # monkeypatch.setattr(db, "add_corpus_item", _failing_link(db))
    row_id, err = db.create_pending_work({"title": "T", "corpus_id": 1})
    assert row_id is None and err.startswith("IntegrityError inserting into works: boom")
    assert db.conn.in_transaction is False
    # monkeypatch.undo(); a following create_pending_work({"title": "Other"}) is visible from a second sqlite3 connection
def test_integrity_error_commit_false_undoes_the_entry(tmp_path, monkeypatch):
    # same patch; with db.transaction(): create_pending_work({"title": "T2", "corpus_id": 1}, commit=False) -> (None, "IntegrityError…")
    # after the block: SELECT COUNT(*) FROM works WHERE title='T2' == 0
def test_import_seed_stopped_chunk_prints_summary(tmp_path, monkeypatch, capsys):
    # 3-entry .json file, INGEST_COMMIT_CHUNK monkeypatched to 1 in the script module;
    # DatabaseManager.transaction patched so its 2nd entry raises sqlite3.OperationalError("database is locked")
    out = _last_json(capsys.readouterr().out)
    assert out["stopped"] is True and out["processed"] == 1 and out["added"] == 1 and out["total"] == 3
    assert out["error"] == "OperationalError: database is locked"
def test_import_seed_parse_failure_uses_the_summary_shape(tmp_path, capsys):
    # --kind json with file content "{not json"
    # out["stopped"] is True, out["processed"] == 0, out["total"] == 0, out["error"].startswith("JSONDecodeError: Failed to parse json:")
def test_import_seed_db_setup_failure_uses_the_summary_shape(tmp_path, capsys):
    # --db-path pointing at an existing directory -> out["stopped"] is True, out["processed"] == 0, out["total"] == 2,
    # out["error"].startswith("OperationalError")
def test_import_seed_complete_run_reports_not_stopped(tmp_path, capsys):
    # out["stopped"] is False, out["processed"] == out["total"] == 3, out["error"] is None
def test_add_pending_works_exits_1_on_exception(tmp_path, monkeypatch):
    # CliRunner().invoke(cli, ["add-pending-works", "--json-file", f, "--db-path", db]) with
    # DatabaseManager.add_pending_works_from_json patched to raise RuntimeError("x")
    assert result.exit_code == 1 and "x" in result.stderr   # click 8.2 keeps stderr separate
class _ConnProxy:      # delegates to the real connection; its cursor raises ValueError("bad") on the first UPDATE works
    ...
def test_apply_enriched_metadata_rolls_back_non_sqlite_error(tmp_path, monkeypatch):
    # monkeypatch db._update_work_record to raise ValueError("bad")
    with pytest.raises(ValueError): db.apply_enriched_metadata(work_id, {"title": "New"})
    assert db.conn.in_transaction is False
    assert db.get_entry_by_id("works", work_id)["title"] == "Old"
    assert db.queue_for_enrichment(work_id) == (work_id, None)
@pytest.mark.parametrize("method", ["enqueue_for_download", "queue_for_enrichment"])
def test_queue_writers_roll_back_non_sqlite_error(tmp_path, method):
    # real = db.conn; db.conn = _ConnProxy(real); with pytest.raises(ValueError): getattr(db, method)(work_id)
    # db.conn = real; assert real.in_transaction is False; the row's status column is unchanged;
    # getattr(db, method)(work_id) == (work_id, None)
```

- [ ] **Step 2: Run them and verify they fail.** Run `cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests/test_transaction_failures.py -q`. Expected: FAIL. The cases are:
  - `in_transaction` is True;
  - the T2 row is left behind;
  - there is no `stopped` key, and the parse and DB-setup outputs have the old shape or raise;
  - exit code 0;
  - the ValueError leaves a transaction open, so the next `BEGIN` fails.
- [ ] **Step 3: Implement D24, D25, D26 (PY-52 half) and D32.**
  - `create_pending_work`: run the body inside `with (nullcontext() if commit else self.savepoint()):`, inside the existing `try`. With `commit=True`, the IntegrityError handler calls `self.conn.rollback()` when `in_transaction`.
  - `ingest_import_seed.py`: route parse failures, `DatabaseManager(...)` failures and chunk failures through one summary printer that always prints the eight keys.
  - `cli.py`: `click.echo(..., err=True)`, then `raise SystemExit(1)`.
  - The three writers: `try: with self.transaction(): … except sqlite3.Error as e: return None, <today's text>`.
- [ ] **Step 4: Run the file and `test_ingest_batching.py` (with its updated dictionaries), then the whole suite.** Expected: all passed.
- [ ] **Step 5: Add `tests/test_transaction_failures.py` to the `ci.yml:68` list and commit** — `db_manager/import-seed/cli: IntegrityError and non-SQLite errors roll back, every import outcome prints one summary shape, add-pending-works exits 1 (PY-36, PY-39, PY-52, PY-53)`

---

### Task 2: LLM attempts and get_bib_pages exit code (PY-44, PY-45, PY-46)

**Files:**
- Modify: `dl_lit_project/dl_lit/llm_common.py` (`_run_attempt`, `run_with_timeout`)
- Modify: `dl_lit_project/dl_lit/get_bib_pages.py:198-211` (waits move into `before_attempt`), `:551-571` (`main`)
- Modify: `dl_lit_project/dl_lit/APIscraper_v2.py:255-304` (`_run_gemini_request`, `_run_generate_content`), `:430-431`, `:671-672` (pre-call waits removed)
- Modify: `dl_lit_project/tests/test_gemini_timeout_retries.py:11` (the fake `_run_attempt` accepts `**_kw`)
- Test: `dl_lit_project/tests/test_llm_attempts.py` (new)
- Modify: `.github/workflows/ci.yml:68`

**Interfaces:**
- Produces:
  - `run_with_timeout(call, timeout_seconds, *, retries=0, backoff_seconds=0.0, label="LLM request", sleep=time.sleep, before_attempt=None, token_service=None)`
  - `_run_attempt(call, timeout_seconds, *, on_late=None)`, where `on_late(ok: bool, value, late_seconds: float)` runs on the attempt's thread
  - `_run_gemini_request(..., before_attempt=None, token_service=None)`, which forwards both to `run_with_timeout`
- Late-line format (stderr, flushed): `[WARNING] {label} attempt {n} finished {late:.1f}s after its timeout and was discarded (tokens={t})`, or `(error={Type}: {msg})` in place of the tokens part.

- [ ] **Step 1: Write the failing tests** in `test_llm_attempts.py`.

```python
def test_before_attempt_runs_for_every_attempt():
    calls = []
    with pytest.raises(TimeoutError):
        run_with_timeout(lambda: time.sleep(1), 0.05, retries=2, before_attempt=lambda: calls.append(1))
    assert len(calls) == 3
def test_late_result_is_logged_and_its_tokens_charged(monkeypatch, capsys):
    charged = []
    monkeypatch.setattr(llm_common, "wait_rate_limit", lambda service, units=1: charged.append((service, units)))
    resp = SimpleNamespace(usage_metadata=SimpleNamespace(total_token_count=42))
    seq = iter([0.3, 0.0])                    # first attempt sleeps past the timeout, second answers at once
    def call():
        time.sleep(next(seq)); return resp
    assert run_with_timeout(call, 0.1, retries=1, token_service="gemini_tokens") is resp
    time.sleep(0.4)                           # let the abandoned attempt finish
    err = capsys.readouterr().err
    assert "attempt 1 finished" in err and "tokens=42" in err
    assert charged == [("gemini_tokens", 42)]     # the late call only; the caller charges the returned one
def test_late_error_is_logged_not_raised(capsys, monkeypatch):
    # threading.excepthook patched to record; call sleeps 0.3 then raises ValueError("late"); timeout 0.1, retries=0 -> TimeoutError;
    # after 0.4 s stderr contains "error=ValueError: late"; the excepthook recorded nothing
def test_result_on_time_is_not_late(capsys):
    # call returns at once; no "[WARNING]" in stderr
def test_get_bib_pages_returns_1_when_a_pdf_failed(tmp_path, monkeypatch):
    # get_bib_pages.api_client = object(); _process_pdf patched to raise RuntimeError("timeout")
    # one dummy .pdf in tmp_path; main(["--input-pdf", str(pdf), "--output-dir", str(tmp_path / "o")]) == 1
    # stdout contains "1 of 1 PDF(s) failed"
def test_get_bib_pages_returns_0_on_success(tmp_path, monkeypatch):
    # _process_pdf returns ["p1.pdf"] -> main(...) == 0
def test_section_detection_waits_per_attempt(monkeypatch):
    # get_bib_pages: patch wait_rate_limit to record service names and llm_common._run_attempt to time out twice then succeed;
    # the recorded list contains "gemini" 3 times and "gemini_daily" 3 times
```

- [ ] **Step 2: Run them and verify they fail.** Run `cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests/test_llm_attempts.py -q`. Expected: FAIL with `unexpected keyword 'before_attempt'`, no late line, and return code 0.
- [ ] **Step 3: Implement D27 and the PY-46 half of D26.**
  - `_run_attempt` uses one `threading.Lock`. The target appends the outcome under the lock and, if `abandoned` is already set, calls `on_late` outside it. The caller, after `join`, takes the lock, sets `abandoned = True` when there is no outcome, and raises `_AttemptTimedOut`.
  - `run_with_timeout` builds `on_late` per attempt (it captures `n`) from `label` and `token_service`.
  - Remove the five pre-call `wait_rate_limit("gemini"/"gemini_daily")` calls and pass `before_attempt` and `token_service` instead.
  - `main()` counts failures and returns 1 when there is any.
- [ ] **Step 4: Run the file, `test_llm_common.py`, `test_gemini_timeout_retries.py` and `test_get_bib_pages.py`, then the whole suite.** Expected: all passed.
- [ ] **Step 5: Add `tests/test_llm_attempts.py` to `ci.yml:68` and commit** — `llm_common: rate-limit waits per attempt, late results logged and their tokens charged; get_bib_pages exits 1 when a PDF failed (PY-44, PY-45, PY-46)`

---

### Task 3: Worker liveness and partial OpenAlex fetches (PY-47, PY-48, PY-49, PY-50, PY-51)

**Files:**
- Modify: `backend/scripts/daemon/worker.py`:
  - `:279-326` (`_heartbeat` catches `sqlite3.Error`)
  - `:328-361` (one `_start_heartbeat_thread`, used by `_start_job_heartbeat` and `_start_direct_batch_heartbeat`)
  - `__init__` (`self._exit = os._exit`, `self._work_lock`, `self._work_generation`, `self._work_finished`, `self._last_recovery_at`)
  - `:1069-1188` (`_process_job`: generation start/finish)
  - `:1190-1220` (`_run_direct_cycles`: generation per cycle)
  - `:1222-1258` (`run`: recovery every 60 s)
- Modify: `dl_lit_project/dl_lit/utils.py` (new `FetchResult` next to `OpenAlexRateLimitExceeded`, `:21`)
- Modify: `dl_lit_project/dl_lit/OpenAlexScraper.py:85-256` (`fetch_citing_work_ids`, `fetch_referenced_work_details`: `FetchResult`, `raise_on_quota`)
- Modify: `dl_lit_project/dl_lit/new_dl.py`:
  - `:966`, `:976` (LibGen)
  - `:2186-2235` (`fetch_openalex_batch`: `FetchResult`, `wait_if_needed("openalex")`)
  - `:2334` (caller)
- Modify: `backend/scripts/keyword_search.py:297`, `:384` and `backend/scripts/seed_expand.py:135`, `:145` (pass `raise_on_quota=True`; keep the `except` branches)
- Test: `dl_lit_project/tests/test_worker_watchdog.py` (new), `dl_lit_project/tests/test_openalex_partial.py` (new)
- Modify: `.github/workflows/ci.yml:68`

**Interfaces:**
- Produces:
  - `class FetchResult(list)` in `dl_lit.utils`, with these attributes:
    - `complete: bool = True`
    - `quota_exhausted: bool = False`
    - `errors: list[str]` (default empty)
    - `failed_ids: list[str]` (default empty)

    It compares equal to a plain list.
  - The keyword argument `raise_on_quota: bool = False` on `fetch_citing_work_ids` and `fetch_referenced_work_details`.
  - `PipelineDaemon._start_heartbeat_thread(payload: dict, *, name: str, generation: int, deadline_seconds: float | None = None, on_deadline: Callable[[int], None] | None = None) -> tuple[threading.Event, threading.Thread]`. `on_deadline` receives the generation.
  - `PipelineDaemon._exit`, an injectable `os._exit`.
  - The constant `JOB_TIMEOUT_EXIT_CODE = 75`.
  - `PipelineDaemon._begin_work() -> int` and `PipelineDaemon._end_work(generation: int) -> None`:
    - `_begin_work` takes `_work_lock`, increments the generation, sets `_work_finished = False` and returns the generation.
    - `_end_work` sets `_work_finished = True` when the generation is current.

    `_process_job` and each direct cycle call them around the work. `_start_job_heartbeat(job, *, generation)` and `_start_direct_batch_heartbeat(details, *, generation)` gain the keyword.
  - `PipelineDaemon._direct_batch_timeout_seconds() -> float`, which reads `RAG_FEEDER_DIRECT_BATCH_TIMEOUT`.
- The env var `RAG_FEEDER_DIRECT_BATCH_TIMEOUT` defaults to `job_timeout_seconds`, with a minimum of 60.

- [ ] **Step 1: Write the failing tests.** Build the daemon the way `tests/test_pipeline_daemon.py` does (temp DB, `role="enrich"`). Set `daemon.heartbeat_interval_seconds = 0.05` and `daemon.job_timeout_seconds = 0.3` directly on the instance (this bypasses the env minimum). Set `daemon._exit = exits.append`.

```python
# test_worker_watchdog.py
def test_beat_thread_survives_database_error(tmp_path, monkeypatch):
    # heartbeat_conn.cursor raises sqlite3.DatabaseError("database disk image is malformed") on the first call only
    # start _start_job_heartbeat(job); sleep 0.3; thread.is_alive() is True and the daemon_heartbeat row has status "running"
def test_job_past_deadline_is_failed_and_worker_exits(tmp_path):
    # pipeline_jobs row status='running', worker_id=daemon.worker_id; gen = daemon._begin_work(); _start_job_heartbeat(job, generation=gen)
    # poll up to 3 s: exits == [75]; row status 'failed', json.loads(result_json)["error"].startswith("timed out after")
    # daemon_heartbeat details_json has state "job_timeout"
def test_completed_job_wins_and_no_exit(tmp_path):
    # mark_job_completed before the deadline (row 'completed'), keep the heartbeat thread alive past the deadline
    # after 1 s: exits == [], row still 'completed'
def test_stale_callback_does_not_touch_the_next_job(tmp_path):
    # job A: gen_a = _begin_work(), _end_work(gen_a), its thread left running; job B: gen_b = _begin_work(), below its deadline
    # call the captured on_deadline(1) directly -> exits == [], job B row still 'running'
def test_watchdog_waits_for_the_write_lock_and_exits_only_after_winning(tmp_path):
    # a second sqlite3 connection holds BEGIN IMMEDIATE; job past deadline -> after 2.5 s: exits == [], row 'running'
    # release the lock (rollback) -> poll up to 3 s: exits == [75], row 'failed'
def test_terminal_reporting_is_bounded(tmp_path):
    # wrap heartbeat_conn so that right after the ownership UPDATE commits, a second connection takes BEGIN IMMEDIATE
    # (the error heartbeat then waits on its 1000 ms busy timeout and fails); record the time of the win
    # exits == [75] and exit time - win time < 2.5 s (poll timeout 4 s)
def test_direct_batch_deadline_exits(tmp_path):
    # monkeypatch daemon._direct_batch_timeout_seconds = lambda: 0.3; gen = _begin_work(); _start_direct_batch_heartbeat({...}, generation=gen)
    # -> exits == [75], heartbeat state "direct_batch_timeout"
def test_restart_recovery_releases_every_claim_of_the_dead_worker(tmp_path):
    # 3 works rows metadata_status 'in_progress', metadata_claimed_by = old worker_id; old daemon_heartbeat row names that worker
    # one 'running' job claimed by it; construct a new daemon (same daemon_name)
    # -> all 3 rows back to 'pending' with claims NULL; the job back to 'pending'
def test_run_recovers_stale_jobs_every_60s(tmp_path, monkeypatch):
    # patch time.time to advance 61 s between loop iterations and fetch_next_job to stop the loop after 2 iterations;
    # the _recover_stale_running_jobs spy is called twice (once from __init__, once from run)
# test_openalex_partial.py
def test_citing_fetch_error_returns_incomplete(monkeypatch, capsys):
    # OpenAlexScraper.openalex_request_json: first page returns 2 results with meta.next_page, the second raises requests.ConnectionError
    res = fetch_citing_work_ids("https://api.openalex.org/works?filter=cites:W1", None)
    assert isinstance(res, FetchResult) and len(res) == 2 and res.complete is False and res.errors
    assert "[OpenAlex WARN]" in capsys.readouterr().out
def test_quota_is_flagged_by_default_and_raised_on_request():
    # openalex_request_json raises OpenAlexRateLimitExceeded -> default: res.quota_exhausted is True, res == []
    # raise_on_quota=True -> pytest.raises(OpenAlexRateLimitExceeded)
def test_referenced_batch_failure_lists_failed_ids(monkeypatch):
    # 60 ids -> 2 batches; the batch holding W51..W60 raises -> res.failed_ids == ["W51", …, "W60"], complete False
def test_fetch_openalex_batch_with_the_real_limiter(monkeypatch):
    # limiter = ServiceRateLimiter(...) as utils builds it (not a fake); get_openalex_session().get raises requests.Timeout for batch 2
    # -> no TypeError from wait_if_needed; FetchResult, failed_ids == the batch-2 ids
def test_libgen_429_saves_debug_capture(monkeypatch):
    # BibliographyEnhancer built as test_thread_sessions.py builds it; _request_get_direct raises
    # requests.HTTPError(response=resp with status_code 429); the _save_libgen_debug_response spy is called with context="http_429"
def test_libgen_503_saves_debug_capture(monkeypatch):
    # same with 503 -> context="http_503"
def test_keyword_expansion_stops_on_quota(monkeypatch, capsys):
    # backend/scripts/keyword_search: fetch_citing_work_ids patched to raise OpenAlexRateLimitExceeded when called with raise_on_quota=True
    # (assert it was called with raise_on_quota=True); _crawl_direction prints "Expansion stopped early" and returns
def test_seed_expand_stops_on_quota(monkeypatch, capsys):
    # same for seed_expand's fetch_referenced_work_details -> stderr has "detail fetch stopped early"
```

- [ ] **Step 2: Run them and verify they fail.** Run `cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests/test_worker_watchdog.py tests/test_openalex_partial.py -q`. Expected: FAIL. The cases are:
  - the thread is dead after the DatabaseError;
  - there is no watchdog and no generation;
  - the restart test stays green: it pins existing behaviour that the watchdog relies on;
  - the results are plain `list`s;
  - `wait_if_needed()` raises `TypeError` on the real limiter;
  - there is no debug capture, and no `raise_on_quota`.
- [ ] **Step 3: Implement D28, D29, D30 and D31.**
  - The beat loop checks `time.monotonic() - started >= deadline_seconds` and calls `on_deadline(generation)` on that beat and on every later beat until it returns `True`. A return of `True` means it won, lost the race, or found the generation stale; any of those ends the watchdog role.
  - The job callback does the ownership `UPDATE` from the spec under `_work_lock`, with `PRAGMA busy_timeout = 1000` on `heartbeat_conn` around it.
  - After winning, it makes one `error`-heartbeat attempt (busy 1000 ms, errors swallowed), then `log(...)`, then `self._exit(JOB_TIMEOUT_EXIT_CODE)`.
  - `run()` calls `_recover_stale_running_jobs()` when `time.time() - self._last_recovery_at >= 60`. `__init__` sets `_last_recovery_at` after its own call.
  - `FetchResult` is a plain `list` subclass whose `__init__` takes the keyword attributes.
- [ ] **Step 4: Run both files, then the whole suite.** Expected: all passed. These must stay green: `test_pipeline_daemon.py`, `test_worker_recovery.py`, `test_keyword_search_script.py` and `test_openalex_ref_counts.py`.
- [ ] **Step 5: Add both files to `ci.yml:68` and commit** — `worker: heartbeat threads survive any DB error, watchdog fails timed-out work only after winning ownership and restarts the worker, periodic stale-job recovery; OpenAlex fetches report partial results and raise quota exhaustion where handled; LibGen error branches run (PY-47, PY-48, PY-49, PY-50, PY-51)`

---

### Task 4: Backend auth and route hardening (BE-32, BE-40 hashing half, BE-41, BE-43, BE-44, BE-49, BE-50; token race)

**Files:**
- Create: `backend/src/authMail.js`, `backend/src/passwordHasher.js`, `backend/src/passwordWorker.js`
- Modify: `backend/src/app.js`:
  - `:963-986` (`bootstrapDefaultCorpus`)
  - `:1869-1880` (`sendTransactionalMail` uses `mailTransport` when given)
  - `:2264-2275` (`APP_SETTING_DEFS`)
  - `:2509` (`createApp({ broadcast, broadcastEvent, mailTransport } = {})`)
  - `:2997-3083` (`createUserWithDefaultCorpus`, the mail senders)
  - `:3086-3104` (login)
  - `:3195-3365` (register, invitations, accept-invite, reset-password: hasher, atomic token consumption)
  - `:3773-3786` (`POST /api/corpora`)
  - `:4234-4253` and `:4430-4455` (inline models)
  - `:5065` (limit clamp)
  - `:5553` (drop the `--corpus-id` push)
- Modify: `backend/src/pythonProcess.js:101-103`
- Modify: `frontend/src/components/AdminPanel.svelte:148-158` and `frontend/src/App.svelte:3868-3879` (two draft keys)
- Test:
  - new: `backend/tests/auth-mail.test.js`, `backend/tests/password-hasher.test.js`, `backend/tests/import-seed-route.test.js`
  - extended: `backend/tests/auth.test.js`, `backend/tests/python-process.test.js`, `backend/tests/corpus.test.js`, `backend/tests/corpus-sharing.test.js`

**Interfaces:**
- Consumes: Task 1's import summary.
- Produces:
  - `escapeHtml(value: unknown) -> string`
  - `tokenMailContent({ kind: 'invite' | 'reset', username: string, url: string }) -> { subject, html, text }`
  - `createPasswordHasher({ size = 2, maxQueue = 64 } = {}) -> { hash(password, rounds = 10) -> Promise<string>, compare(password, hash) -> Promise<boolean>, close() -> Promise<void> }`, plus `getPasswordHasher()` (a lazy singleton with `unref()`'d workers). A rejection beyond the queue carries `code: 'HASHER_BUSY'`.
  - `export function extractionInlineModels(settingsEnv = appSettingsEnv(), env = process.env) -> { cheap: string, pro: string }` from `app.js`
  - the exported constant `CORPUS_MAX_LIMIT = 1000`
  - the `createApp` option `mailTransport: { sendMail(options) -> Promise<{ messageId }> }`
- Admin settings keys `inline_cheap_model` and `inline_model`.

- [ ] **Step 1: Write the failing tests.**

```js
// auth-mail.test.js
test('invite and reset mails escape the username and the url', () => {
  for (const kind of ['invite', 'reset']) {
    const { html, text } = tokenMailContent({ kind, username: '<b>x</b>&"\'', url: 'https://h/#/a?token=a"b' })
    expect(html).toContain('&lt;b&gt;x&lt;/b&gt;&amp;&quot;&#39;')
    expect(html).not.toContain('<b>x</b>')
    expect(html).toContain('href="https://h/#/a?token=a&quot;b"')
    expect(text).toContain('a"b')
  }
})
test('subjects and expiry lines are unchanged', () => {
  // invite: subject 'Set your Korpus Builder password', html contains 'This link expires in 72 hours.'
  // reset: subject 'Reset your Korpus Builder password', 'This link expires in 2 hours.'
})
// password-hasher.test.js
test('hashes are bcryptjs-compatible both ways', async () => {
  // h = await hasher.hash('pw'); bcrypt.compareSync('pw', h) === true; await hasher.compare('pw', bcrypt.hashSync('pw', 10)) === true
})
test('the main thread keeps running while hashes run', async () => {
  // setInterval 10 ms records Date.now() gaps; await Promise.all(4 × hasher.hash('pw', 12)); clearInterval
  // max gap < 60 ms (a sync rounds-12 hash blocks ~250 ms); at least 10 ticks happened
})
test('at most size hashes run at once and the queue bound holds', async () => {
  // createPasswordHasher({ size: 1, maxQueue: 2 }); start 3 hashes at rounds 12, then a 4th
  // -> the 4th rejects with code 'HASHER_BUSY' at once; the first 3 resolve
})
test('a dead worker is replaced and its call rejects', async () => {})
// import-seed-route.test.js (RAG_FEEDER_UPLOADS_DIR set to a temp dir before importing app.js)
test('a malformed seed file answers the stopped summary, not a 500', async () => {
  // write bad.json = "{not json" into the uploads dir; POST /api/ingest/import-seed {filename:'bad.json', kind:'json'}
  // -> 200; body.stopped === true, body.processed === 0, body.error starts with 'JSONDecodeError: Failed to parse json:'
})
test('a good json seed answers stopped false with processed === total', async () => {})
// auth.test.js additions
test('login and invite answer 503 when the hasher queue is full', async () => {
  // jest.unstable_mockModule('../src/passwordHasher.js') so getPasswordHasher().compare rejects with code 'HASHER_BUSY'
  // POST /api/auth/login -> 503, header retry-after '5', body.error 'The server is busy; try again in a moment.'
})
test('two concurrent accept-invite submissions: exactly one succeeds', async () => {
  // invite via the route with a mailTransport capturing the token from sent[0].html;
  // Promise.all two POST /api/auth/accept-invite with passwords 'first-pass' and 'second-pass'
  // -> statuses sorted [200, 400]; the 400 body.error 'Invite link is invalid or expired';
  // login with the 200's password succeeds, with the other 401
})
test('two concurrent reset-password submissions: exactly one succeeds', async () => {
  // same via POST /api/auth/forgot-password with mailTransport; error text 'Reset link is invalid or expired'
})
test('the invitation and forgot-password routes send escaped HTML', async () => {
  // createApp({ broadcast() {}, mailTransport }); POST /api/auth/invitations username '<b>x</b>' -> sent[0].html contains '&lt;b&gt;x&lt;/b&gt;'
  // and not '<b>x</b>'; forgot-password for an active user named '<i>y</i>' -> escaped in the reset mail
})
test('boot does not rewrite a matching admin hash', () => {
  // createApp twice on the same DB; users.password_hash for the admin is byte-identical; login still works
  // change RAG_ADMIN_PASSWORD, createApp -> hash differs and the new password logs in
})
test('a failing membership insert leaves no corpus and no user behind', async () => {
  // CREATE TRIGGER boom BEFORE INSERT ON user_corpora BEGIN SELECT RAISE(ABORT,'boom'); END
  // POST /api/corpora {name:'X'} -> 500 with body.error; SELECT COUNT(*) FROM corpora WHERE name='X' === 0
  // POST /api/auth/invitations {username:'u-x', …} -> 500; no users row 'u-x' and no corpora row owned by it
})
// python-process.test.js addition
test('spawnPython does not add a second --corpus-id', async () => {
  // the script prints json.dumps(sys.argv[1:]); spawnPython(script, ['--corpus-id','5'], {corpusId: 5})
  // -> argv.filter(a => a === '--corpus-id').length === 1; without it in args -> appended once
})
// corpus.test.js addition
test('GET /api/corpus clamps limit to 1000', async () => {
  // seed 1005 works into the test corpus; ?limit=5000 -> items.length === 1000, total === 1005
})
// corpus-sharing.test.js additions (admin settings)
test('inline models come from Admin settings, then env, then defaults', () => {
  expect(extractionInlineModels({}, {})).toEqual({ cheap: 'gemini-3-flash-preview', pro: 'gemini-3.1-pro-preview' })
  expect(extractionInlineModels({}, { RAG_FEEDER_API_INLINE_MODEL: 'env-pro' }).pro).toBe('env-pro')
  expect(extractionInlineModels({ RAG_FEEDER_API_INLINE_MODEL: 'adm' }, { RAG_FEEDER_API_INLINE_MODEL: 'env-pro' }).pro).toBe('adm')
  // PUT /api/admin/settings {inline_model:'m', inline_cheap_model:'c'} -> 200; GET shows both values
})
test('the extraction route reads its inline models only through the helper', () => {
  // src = fs.readFileSync('src/app.js', 'utf8'); each of 'gemini-3.1-pro-preview' and 'gemini-3-flash-preview' occurs exactly once,
  // and src has no RAG_FEEDER_API_INLINE_MODEL / _CHEAP_MODEL reads outside extractionInlineModels and APP_SETTING_DEFS
})
```

- [ ] **Step 2: Run them and verify they fail.** Run `cd backend && npm test -- tests/auth-mail.test.js tests/password-hasher.test.js tests/import-seed-route.test.js tests/auth.test.js tests/python-process.test.js tests/corpus.test.js tests/corpus-sharing.test.js`. Expected: FAIL. The cases are:
  - the modules are missing;
  - the route answers a 500 (or the old shape) for the malformed seed;
  - both concurrent submissions succeed (once hashing is async) or the test cannot capture the token;
  - unescaped HTML;
  - a rewritten admin hash;
  - an orphan corpus;
  - two `--corpus-id`;
  - 1005 items;
  - an unknown settings key;
  - literals in the route.
- [ ] **Step 3: Implement D16 to D23.**
  - `passwordWorker.js` answers `{ id, op: 'hash' | 'compare', ... }` messages with bcryptjs `hashSync`/`compareSync`.
  - The pool dispatches FIFO to idle workers.
  - The route order in accept-invite and reset is: early token read (fast 400), `await getPasswordHasher().hash(...)`, then one `authDb.transaction` that runs the conditional token `UPDATE` (exactly one row, or it throws a marked error → 400) before the user update.
  - The route builds its inline-model values from `extractionInlineModels()` once per request.
  - In the Admin panel's "Advanced" block, add the inputs `Inline model (cheap)` and `Inline model (fallback)`, both with placeholder `model name` and with `fallbackHint(appSettings.<key>)`. `App.svelte`'s draft gains `inline_cheap_model` and `inline_model`.
- [ ] **Step 4: Run `cd backend && npm test`.** Expected: all passed. Then check these:
  - `grep -n "compareSync\|hashSync" backend/src/app.js` lists only lines inside `bootstrapDefaultCorpus`.
  - `cd frontend && npx vite build` passes.
- [ ] **Step 5: Commit** — `backend: escaped auth mails, password hashing in a bounded worker pool, one-time tokens consumed atomically, no admin rehash at boot, atomic user/corpus creation, one --corpus-id, inline models as Admin settings, /api/corpus limit clamp (BE-32, BE-40 hashing, BE-41, BE-43, BE-44, BE-49, BE-50)`

---

### Task 5: Frontend workspace tables and logs (FE-39, FE-40, FE-41, FE-44, FE-45, FE-56, FE-60)

**Files:**
- Modify: `frontend/src/App.svelte`:
  - `:603-620` (`flushLogs` counts non-marker entries)
  - `:916-924` (scroll handler)
  - `:2636-2642` and `:2688-2691` (`supersede: true`)
  - `:2783-2791`, `:4818-4821` (seed select-all)
  - `:3547-3669` (`loadCorpus`)
  - `:3995-4031` (`loadOlderLogs`)
  - `:4050-4140` (`connectLogs`: disconnect time, marker on reconnect)
  - `:5122` (`{#key currentCorpusId}`)
- Modify: `frontend/src/components/Logs.svelte:55-70` (marker row with `Reload logs`), `frontend/src/components/Corpus.svelte:379` (button disabled only while `corpusLoadingMore`)
- Modify: `frontend/src/app.css:2920`, `:3335` (`--ink`), plus a `.log-gap` rule
- Test: `frontend/tests/corpus-load.spec.ts` (new), `frontend/tests/css-tokens.spec.ts` (new); extend `frontend/tests/seed-selection.spec.ts` and `frontend/tests/logs-stream.spec.ts`
- Modify: `.github/workflows/ci.yml:54`

**Interfaces:**
- Produces:
  - log entries `{ seq, text, marker?: true }`
  - the `loadCorpus` option `supersede`
  - `connectLogs({ reconnect = false } = {})`
  - the test id `logs-reload`

- [ ] **Step 1: Write the failing tests.**

```ts
// css-tokens.spec.ts (node-side, like works-lib.spec.ts)
test('every var(--name) without a fallback is defined somewhere', () => {
  // read src/app.css and all src/**/*.svelte; used = /var\(--([a-z0-9-]+)\)/g (no comma); defined = /--([a-z0-9-]+)\s*:/g over the same text
  // expect(used.filter((n) => !defined.has(n))).toEqual([])   // on 2c8b8b7 this lists exactly ['text']
})
// corpus-load.spec.ts (mocked; /api/corpus answers delayed by a per-request promise the test resolves)
test('a filter typed during a slow poll reloads with the filter', async ({ page }) => {
  // hold the poll's /api/corpus; type 'labour' in the corpus filter; expect a request with q=labour while the poll is pending;
  // release the poll last -> the rows shown are the q=labour rows
})
test('a sort click during a poll is applied', async ({ page }) => {})
test('Show more clicked during a poll runs after it', async ({ page }) => {
  // hold the poll; click Show more (enabled); release -> one request with offset=120 follows
})
test('a filter typed while Show more is in flight discards the append and its queue', async ({ page }) => {
  // hold the append (offset=120); type 'x'; release the append -> rows are only the q=x rows; no request with q=x&offset=120
})
test('Show more drops rows already loaded', async ({ page }) => {
  // the page-2 answer repeats the last id of page 1 -> each data-work-id appears once; no console "each_key_duplicate" error
})
test('a corpus switch clears the Corpus selection and stage filter', async ({ page }) => {
  // select 2 rows and stage "Downloaded" in corpus 1; switch to corpus 2 (same work ids) -> no row checked, stage "All"
})
// seed-selection.spec.ts addition
test('select-all on a partly promoted paged seed reads checked and clears on the next click', async ({ page }) => {
  // seed total 10, state_counts {in_corpus: 3}, page of 5 loaded; click select-all -> checkbox checked, text "Selected: 7"
  // click again -> unchecked, "Selected: 0"
})
// logs-stream.spec.ts additions
test('Load older sends nothing when the view is full and says so', async ({ page }) => {
  // tail 200 lines with has_more; push 4,800 live lines (5,000 shown); click Load older -> no tail request with cursor > 0;
  // status 'The log view holds 5,000 lines. Older lines stay in the log file.'
})
test('Load older admits only what still fits when the answer arrives', async ({ page }) => {
  // 4,950 shown; click Load older -> request lines=50 (held); push 30 live lines and let them flush; answer 50 older lines
  // -> 5,000 shown; the 20 newest of the 50 were prepended; status 'Loaded 20 of 50 older log lines; the view holds 5,000 lines.'
  // the next Load older request carries cursor = (cursor before the click) + 30 (flushed live lines) + 20 (admitted), not + 50
})
test('a reconnect keeps the view and adds one gap marker with Reload logs', async ({ page }) => {
  // the first socket sends a,b,c then closes with 1006; the reconnected socket opens and sends d
  // -> rendered texts: a, b, c, 'Connection lost HH:MM:SS–HH:MM:SS. Lines from that period may be missing.', d
  // the tail was requested exactly once (first connect)
  // click getByTestId('logs-reload') -> the view equals the mocked tail; the marker is gone
})
test('a marker does not move the Load older cursor', async ({ page }) => {
  // after a reconnect with a marker, Load older requests cursor = the number of file lines shown (marker excluded)
})
// The existing reconnect tests ('lines buffered before a socket reconnect survive it and are not re-hydrated',
// '500 buffered lines survive a reconnect whole') keep their assertions: one tail request, every line kept. Only the
// rendered count grows by the one marker line, and they assert the marker is present.
```

- [ ] **Step 2: Run them and verify they fail.** Run `cd frontend && E2E_FRONTEND_PORT=5190 npx playwright test tests/css-tokens.spec.ts tests/corpus-load.spec.ts tests/seed-selection.spec.ts tests/logs-stream.spec.ts --workers=2`. Expected: FAIL. The cases are:
  - `--text` is undefined;
  - the filter request is dropped;
  - "Show more" is disabled;
  - a row is duplicated;
  - the selection survives the corpus switch;
  - the select-all checkbox stays unchecked;
  - "Load older" trims what it fetched or overfills the view;
  - there is no marker.
- [ ] **Step 3: Implement D1 to D7.**
  - `logsDisconnectedAt` is set in `onclose` only when it is null, and cleared in `resetLogState` and after the marker is pushed.
  - Times use `toLocaleTimeString('en-GB', { hour12: false })`.
  - Nothing changes for the first connect, and a 4401 close adds no marker.
- [ ] **Step 4: Run the Task 5 specs plus the regression set, and build.** Run `E2E_FRONTEND_PORT=5190 npx playwright test tests/tab-resources.spec.ts tests/refresh-scheduler.spec.ts tests/corpus-reconcile.spec.ts tests/dead-code.spec.ts tests/table-columns.spec.ts tests/session.spec.ts --workers=2`, then `npx vite build`. Expected: all pass.
- [ ] **Step 5: Add `tests/corpus-load.spec.ts tests/css-tokens.spec.ts` to `ci.yml:54` and commit** — `frontend: user corpus reloads supersede polls, Show more queues, append dedupes, Corpus keyed by corpus, select-all counts selectable rows, --text fixed, Load older admits what fits, reconnect gap marker (FE-39, FE-40, FE-41, FE-44, FE-45, FE-56, FE-60)`

---

### Task 6: Frontend session and failure states (FE-42, FE-43, FE-46, FE-48, FE-50, FE-57, FE-58; PY-39 UI)

**Files:**
- Modify: `frontend/src/App.svelte`:
  - `:341-353` (quota epoch)
  - `:168-172` and `:2020-2025` (`null` global defaults and their reset)
  - `:1599-1610` (boot catch)
  - `:1612-1767` (submit handlers count `authSubmitsInFlight`; invite/reset catches)
  - `:1796-1823` (`expireSession` message rule)
  - `:2192-2275` (targets error, browse error)
  - `:2399-2431` (stopped import)
  - `:2474-2494` and `:3697-3738` (global error flags with session epoch)
  - `:4128-4135` (4401 → `expireSession`)
  - the sign-in card `:4377-4400` (Retry button)
  - `<UpstreamBrowser>` and `<Dashboard>` props
- Modify: `frontend/src/components/UpstreamBrowser.svelte:35`, `:304-306`, `:383-490` (`statusError`, the Retry button, sections gated on `browse.target`)
- Modify: `frontend/src/components/Dashboard.svelte:10-12`, `:106-148` (`globalUnavailable`, `—` for `null`)
- Test: `frontend/tests/upstream-status.spec.ts` (new); extend `frontend/tests/session.spec.ts`, `frontend/tests/tab-resources.spec.ts` and `frontend/tests/seed-upload.spec.ts`
- Modify: `.github/workflows/ci.yml:54`

**Interfaces:**
- Consumes: Task 1's import summary (`stopped`, `processed`, `error`, `added`, `corpus_linked`, `total`).
- Produces:
  - `UpstreamBrowser` props `statusError: boolean`, and `refresh` (existing), which the Retry button uses
  - the `Dashboard` prop `globalUnavailable: boolean`
  - test ids `auth-retry` and `upstream-retry`

- [ ] **Step 1: Write the failing tests.**

```ts
// session.spec.ts additions
test('a 502 at boot keeps the token and offers Retry', async ({ page }) => {
  // stored token; /api/auth/me -> 502 once, then the normal payload
  // the login form shows '<transport or server text> Your session is kept; press Retry when the server is back.'
  // the localStorage token is unchanged; click getByTestId('auth-retry') -> the workspace is visible without signing in
})
test('a 401 mid-session says the session expired', async ({ page }) => {
  // signed in; the next request answers 401 -> login form .error 'Your session has expired. Please sign in again.'
})
test('a 4401 socket close says the session expired', async ({ page }) => {
  // mockLogSocket(page, (ws) => setTimeout(() => ws.close({ code: 4401 }), 200)) -> same message
})
test('a stale token at boot says the session expired; an explicit sign-out says nothing', async ({ page }) => {})
test('a stale boot 401 during a sign-in submit leaves the submit outcome on the form', async ({ page }) => {
  // stored stale token, the boot /me held; submit a sign-in whose /api/auth/login is held; release the boot 401, then answer login 401 {error:'Invalid credentials'}
  // -> .error 'Invalid credentials' (no expiry text at any point after the release: poll 300 ms)
})
// The existing 'a stale-token 401 at boot leaves the failure of ${flow} on the form' (session.spec.ts:706) keeps passing unchanged.
test('invite accepted but /me fails: sign-in form with the username and the saved-password text', async ({ page }) => {
  // #/accept-invite?token=t; /api/auth/accept-invite -> {token, user:{username:'neu'}}; /api/auth/me -> 500 {error:'boom'}
  // the sign-in form is visible, the username input value is 'neu', .error starts 'Your password was saved, but signing in failed (boom).'
  // the localStorage token is null
})
test('reset accepted but /me fails behaves the same', async ({ page }) => {})
test('a quota answer from before a sign-out does not reach the next session', async ({ page }) => {
  // hold /api/openalex/quota; sign out; sign in as bob; release the old answer -> the pill shows bob's quota (or none), not the old value;
  // bob's own quota request was sent (the old flag did not block it)
})
// upstream-status.spec.ts (new)
test('no Kantropos targets shows "No upstream corpora are configured."', async ({ page }) => {})
test('a failed target list shows the error, not "none configured"', async ({ page }) => {
  // /api/kantropos/corpora -> 500 {error:'down'} -> 'Could not load the Kantropos corpora: down'
})
test('a failed first browse shows the error and Retry, and no empty-state texts', async ({ page }) => {
  // browse -> 502 -> status .error 'Could not load the upstream corpus: …'; 'No pending additions match' and 'has not been imported' are absent;
  // Retry -> the browse succeeds -> the sections render and the error is gone
})
test('a failed quiet refresh keeps the data and says it is stale', async ({ page }) => {
  // the first browse is ok; the next quiet browse answers 500 -> the status ends ' Showing the last loaded data.'; the pending rows stay visible
})
// tab-resources.spec.ts additions
test('a failed global stats load shows dashes and the unavailable note', async ({ page }) => {
  // admin dashboard; /api/ingest/stats?global=1 -> 500 -> 'Global totals could not be loaded.'; Raw shows '—', not '0'
})
test('global stats and daemon answers from before a sign-out do not reach the next session', async ({ page }) => {
  // hold /api/ingest/stats?global=1 and /api/pipeline/daemon/status?global=1 (one answering 200 with numbers, one 500);
  // sign out, sign in as another admin whose global loads answer normally; release the held answers
  // -> the dashboard shows the new admin's numbers and no 'Global totals could not be loaded.'
})
// seed-upload.spec.ts additions
test('a stopped import shows the counts it saved', async ({ page }) => {
  // import-seed -> {added: 380, skipped: 0, corpus_linked: 20, total: 1000, processed: 400, stopped: true, error: 'OperationalError: database is locked', errors: []}
  // the item message is 'Import stopped after 400 of 1000 entries: OperationalError: database is locked (380 added, 20 linked before it).'; status failed
})
test('an import that saved nothing shows Import failed', async ({ page }) => {
  // {added: 0, skipped: 0, corpus_linked: 0, total: 0, processed: 0, stopped: true, error: 'JSONDecodeError: Failed to parse json: …', errors: []}
  // -> 'Import failed: JSONDecodeError: Failed to parse json: …'; status failed
})
```

- [ ] **Step 2: Run them and verify they fail.** Run `cd frontend && E2E_FRONTEND_PORT=5190 npx playwright test tests/session.spec.ts tests/upstream-status.spec.ts tests/tab-resources.spec.ts tests/seed-upload.spec.ts --workers=2`. Expected: FAIL. The cases are:
  - the token is cleared at boot;
  - there is no expiry text;
  - the raw invite error is shown;
  - the stale quota or global answers reach the next session;
  - Korpus Management is blank;
  - zeros are shown as real numbers;
  - the upload says "imported".
- [ ] **Step 3: Implement D8 to D15.**
  - `authBootRetry` is cleared by `bootstrapAuth`'s start, `enterSession` and `endSession`.
  - `authSubmitsInFlight` is incremented and decremented in `try`/`finally` of the four submit handlers.
  - `upstreamBrowseError` and `kantroposTargetsError` are reset by `resetFrontendState`. `globalStatsError` and `globalDaemonError` are reset on sign-out.
  - The 4401 branch becomes `expireSession(); logsStatus = 'unauthorized'`.
- [ ] **Step 4: Run the Task 6 specs plus the whole CI Playwright list, and build.** Run `E2E_FRONTEND_PORT=5190 npx playwright test <ci.yml:54 list> --workers=2`, then `npx vite build`. Expected: all pass.
- [ ] **Step 5: Add `tests/upstream-status.spec.ts` to `ci.yml:54` and commit** — `frontend: boot keeps the token on server errors, session-expired message that never overrides a current outcome, invite/reset recovery text, session-scoped quota and global loads, Korpus Management errors with Retry, global overview unavailable state, stopped imports shown (FE-42, FE-43, FE-46, FE-48, FE-50, FE-57, FE-58, PY-39)`
