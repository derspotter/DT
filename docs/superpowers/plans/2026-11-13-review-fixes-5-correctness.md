# Review fixes, phase 5: correctness, silent failures and hardening — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the 33 Phase 1 rows of the review-fixes-5 inventory (all except PY-02): no silent failures, no state leaking across sessions, no dropped user requests, hardened auth endpoints, atomic multi-statement writes and truthful exit codes.

**Architecture:** Six tasks by surface, in this order: three Python tasks (`dl_lit_project/dl_lit/`, `backend/scripts/`), one backend task (`backend/src/`, with a small Admin panel addition), and two frontend tasks (`frontend/src/App.svelte` and its components). Two new pure modules are testable on their own: `backend/src/loginLimiter.js` and `frontend/src/lib/logMerge.js`. One new helper module, `backend/src/authMail.js`, holds the auth mail template. Every fix is pinned by a test that fails on `2c8b8b7`.

**Tech Stack:** Python 3.11 (`/home/jay/DT/venv/bin/python`), pytest, sqlite3; Node 20, Express 5, better-sqlite3, bcryptjs, Jest + supertest; Svelte 5 + Vite, Playwright (mocked).

**Spec:** `docs/superpowers/specs/2026-11-13-review-fixes-5-correctness-design.md`

## Global Constraints

- Work only in `/home/jay/DT/.worktrees/review-fixes-5`, on branch `review-fixes-5`. Do not push.
- Never open `dl_lit_project/data/literature.db`; tests use `tmp_path` / `fs.mkdtempSync` DBs.
- Python tests: `cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests -q` (whole suite) plus the task's file. Every new test file goes into the pytest list at `.github/workflows/ci.yml:68`.
- Backend tests: `cd backend && npm test` (CI runs all); prefer `npm test -- tests/<file>` while iterating.
- Frontend tests: from `frontend/`, `E2E_FRONTEND_PORT=5190 npx playwright test <specs> --workers=2`, mocked specs only. Never use the dev stack on `:5175` and never run `*.real*.spec.ts` or `pipeline-perf.real-pdf.spec.ts`. Every new spec goes into the Playwright list at `.github/workflows/ci.yml:54`. `npx vite build` must pass.
- Every mocked spec that fakes a token calls `mockLogSocket(page)` from `tests/helpers/mockApi.ts`.
- These strings are copied exactly from the spec:
  - D6: `The log view holds 5,000 lines. Older lines stay in the log file.`
  - D7: `Reconnected; reloaded the latest log lines.`
  - D8: `Could not load the Kantropos corpora: <message>` and `No upstream corpora are configured.`
  - D9: `Could not load the upstream corpus: <message>.` and ` Showing the last loaded data.`
  - D10: `<transport message> Your session is kept; press Retry when the server is back.`
  - D11: `Your session has expired. Please sign in again.`
  - D12: `Your password was saved, but signing in failed (<message>). Sign in with your new password.`
  - D14: `Global totals could not be loaded.`
  - D15: `Import stopped after <processed> of <total> entries: <error> (<added> added, <corpus_linked> linked before it).`
  - D18: `Too many failed sign-in attempts. Try again in <n> minute(s).`
- These values are fixed by the spec:
  - D18 limits: 10 failures per (IP, username), 100 per IP, 15-minute sliding window, `maxKeys` 10,000, trust proxy `'loopback, linklocal, uniquelocal'`.
  - D23: `CORPUS_MAX_LIMIT = 1000`.
  - D29: watchdog exit code `75`, recovery interval 60 s.
  - D7: anchor of 3 lines, tail of 200 lines.
- Each commit message names the inventory rows it closes and ends with:
  ```
  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01U3c4gfg7jQvvmPdpLNBN5T
  ```
- Secrets stay in `.env`; nothing from it goes into tests, docs or commits.

## Review Focus

1. **Repeated identical log lines around a reconnect gap.** Idle workers print the same line many times, so a 3-line anchor can occur several times in the tail. The backfill must anchor on the last occurrence and must neither duplicate nor drop lines. Test in Task 5 (`gapLines` with a repeated anchor).
2. **A spoofed `X-Forwarded-For` entry.** A client sending `X-Forwarded-For: 1.2.3.4` must not get a fresh limiter bucket. The bucket is keyed on the right-most untrusted address. Test in Task 4.
3. **A filter typed while "Show more" is in flight.** The append's answer must not land on the new filter's rows, and the queued append must not run against the new query. Test in Task 5.
4. **A late result of an abandoned attempt after a retry succeeded.** It must not raise in any thread and must not print a traceback. It logs one line and charges its tokens once. Test in Task 2.
5. **The watchdog firing while the main connection holds the write lock.** The failed-mark times out (`busy_timeout` 3 s), and the worker must still exit with 75. Test in Task 3, with a second connection holding `BEGIN IMMEDIATE`.

---

### Task 1: Python transactions and exit codes (PY-36, PY-39, PY-52, PY-53)

**Files:**
- Modify: `dl_lit_project/dl_lit/db_manager.py`, in `create_pending_work` (`:1244-1421`), `apply_enriched_metadata` (`:1524-1596`), `enqueue_for_download` (`:1598-1622`) and `queue_for_enrichment` (`:1624-1648`)
- Modify: `backend/scripts/ingest_import_seed.py:104-148`
- Modify: `dl_lit_project/dl_lit/cli.py:461-477` (`add_pending_works_command`)
- Test: `dl_lit_project/tests/test_transaction_failures.py` (new)
- Modify: `.github/workflows/ci.yml:68`

**Interfaces:**
- Produces: the import summary JSON from `ingest_import_seed.py`, which always has these keys:

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

  Task 6 renders it.
- `create_pending_work` keeps its signature and return values.

- [ ] **Step 1: Write the failing tests** in `test_transaction_failures.py`. Use a temp DB from `DatabaseManager(str(tmp_path / "t.db"))`. Force an IntegrityError with `db.conn.execute("CREATE TRIGGER boom BEFORE INSERT ON work_aliases BEGIN SELECT RAISE(ABORT, 'boom'); END")`.

```python
def test_integrity_error_commit_true_leaves_no_open_transaction(tmp_path):
    # create_pending_work({"title": "T", "translated_title": "Ü"}) -> (None, msg) with "IntegrityError" in msg
    assert db.conn.in_transaction is False
    # a following create_pending_work({"title": "Other"}) is visible from a second sqlite3 connection
def test_integrity_error_commit_false_undoes_the_entry(tmp_path):
    # inside db.transaction(): create_pending_work({"title": "T2", "translated_title": "Ü"}, commit=False)
    # -> (None, "IntegrityError…"); after the block: SELECT COUNT(*) FROM works WHERE title='T2' == 0
def test_import_seed_stopped_chunk_prints_summary(tmp_path, monkeypatch, capsys):
    # 3-entry .bib, INGEST_COMMIT_CHUNK monkeypatched to 1 in ingest_import_seed;
    # DatabaseManager.transaction patched to raise sqlite3.OperationalError("database is locked") on its 2nd entry
    out = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert out["stopped"] is True and out["processed"] == 1 and out["added"] == 1 and out["total"] == 3
    assert out["error"].startswith("OperationalError: database is locked")
def test_import_seed_complete_run_reports_not_stopped(tmp_path, capsys):
    # out["stopped"] is False, out["processed"] == out["total"] == 3, out["error"] is None
def test_add_pending_works_exits_1_on_exception(tmp_path, monkeypatch):
    # click.testing.CliRunner(mix_stderr=False) invoking cli add-pending-works with
    # DatabaseManager.add_pending_works_from_json patched to raise RuntimeError("x")
    assert result.exit_code == 1 and "x" in result.stderr
def test_apply_enriched_metadata_rolls_back_non_sqlite_error(tmp_path, monkeypatch):
    # monkeypatch db._update_work_record to raise ValueError("bad")
    with pytest.raises(ValueError): db.apply_enriched_metadata(work_id, {"title": "New"})
    assert db.conn.in_transaction is False
    assert db.queue_for_enrichment(work_id) == (work_id, None)
def test_queue_writers_use_transaction(tmp_path, monkeypatch):
    # wrap db.transaction with a spy contextmanager; enqueue_for_download and queue_for_enrichment each enter it once
```

- [ ] **Step 2: Run them and verify they fail.** Run `cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests/test_transaction_failures.py -q`. Expected: FAIL. The cases are: `in_transaction` True, the entry row left behind, no JSON line, exit code 0, the ValueError leaving a transaction open (the next `BEGIN` fails), and the spy never entered.
- [ ] **Step 3: Implement D24, D25, D26 (PY-52 half) and D32.**
  - `create_pending_work`: run the body inside `with (nullcontext() if commit else self.savepoint()):`, inside the existing `try`. With `commit=True`, the IntegrityError handler calls `self.conn.rollback()` when `in_transaction`.
  - `ingest_import_seed.py`: give each chunk its own counters and add them to the totals after the `with` exits. Wrap the chunk loop in `except Exception as exc`, which sets `stopped=True` and `error=f"{type(exc).__name__}: {exc}"`. Always print the summary.
  - `cli.py`: `click.echo(..., err=True)`, then `raise SystemExit(1)`.
  - The three writers: `try: with self.transaction(): … except sqlite3.Error as e: return None, <today's text>`.
- [ ] **Step 4: Run the file, then the whole suite.** Run `/home/jay/DT/venv/bin/python -m pytest tests -q`. Expected: all passed. `test_ingest_batching.py`, which runs `ingest_import_seed.main`, must still pass with the new keys.
- [ ] **Step 5: Add `tests/test_transaction_failures.py` to the `ci.yml:68` list and commit** — `db_manager/import-seed/cli: IntegrityError and non-SQLite errors roll back, a stopped import prints its summary, add-pending-works exits 1 (PY-36, PY-39, PY-52, PY-53)`

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
def test_late_error_is_logged_not_raised(capsys):
    # call sleeps 0.3 then raises ValueError("late"); timeout 0.1, retries=0 -> TimeoutError;
    # after 0.4 s stderr contains "error=ValueError: late"; threading.excepthook was not called (patch it to record)
def test_result_on_time_is_not_late(capsys):
    # call returns at once; no "[WARNING]" in stderr
def test_get_bib_pages_returns_1_when_a_pdf_failed(tmp_path, monkeypatch):
    # get_bib_pages.api_client = object(); _process_pdf patched to raise RuntimeError("timeout")
    # one dummy .pdf in tmp_path; main(["--input-pdf", str(pdf), "--output-dir", str(tmp_path / "o")]) == 1
    # stdout contains "1 of 1 PDF(s) failed"
def test_get_bib_pages_returns_0_on_success(tmp_path, monkeypatch):
    # _process_pdf returns ["p1.pdf"] -> main(...) == 0
def test_section_detection_waits_per_attempt(monkeypatch):
    # get_bib_pages: patch wait_rate_limit to record service names and _run_attempt to time out twice then succeed;
    # the recorded list contains "gemini" 3 times and "gemini_daily" 3 times
```

- [ ] **Step 2: Run them and verify they fail.** Run `cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests/test_llm_attempts.py -q`. Expected: FAIL with `unexpected keyword 'before_attempt'`, no late line, and return code 0.
- [ ] **Step 3: Implement D27 and the PY-46 half of D26.**
  - `_run_attempt` uses one `threading.Lock`. The target appends the outcome under the lock and, if `abandoned` is already set, calls `on_late` outside it. The caller, after `join`, takes the lock, sets `abandoned = True` when there is no outcome, and raises `_AttemptTimedOut`.
  - `run_with_timeout` builds `on_late` per attempt (it captures `n`) from `label` and `token_service` as specified.
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
  - `__init__` (`self._exit = os._exit`, `self._last_recovery_at`)
  - `:1222-1258` (`run`: recovery every 60 s)
- Modify: `dl_lit_project/dl_lit/utils.py` (new `FetchResult` next to `OpenAlexRateLimitExceeded`, `:21`)
- Modify: `dl_lit_project/dl_lit/OpenAlexScraper.py:85-256` (`fetch_citing_work_ids`, `fetch_referenced_work_details`)
- Modify: `dl_lit_project/dl_lit/new_dl.py:966`, `:976` (LibGen), `:2186-2235` (`fetch_openalex_batch`), `:2334` (caller)
- Modify: `backend/scripts/keyword_search.py:297`, `:384-391`, and `backend/scripts/seed_expand.py:133-153`
- Test: `dl_lit_project/tests/test_worker_watchdog.py` (new), `dl_lit_project/tests/test_openalex_partial.py` (new)
- Modify: `.github/workflows/ci.yml:68`

**Interfaces:**
- Produces:
  - `class FetchResult(list)` in `dl_lit.utils`, with these attributes:
    - `complete: bool = True`
    - `quota_exhausted: bool = False`
    - `errors: list[str]` (default empty)
    - `failed_ids: list[str]` (default empty)
  - `PipelineDaemon._start_heartbeat_thread(payload: dict, *, name: str, deadline_seconds: float | None = None, on_deadline: Callable[[], None] | None = None) -> tuple[threading.Event, threading.Thread]`
  - `PipelineDaemon._exit`, an injectable `os._exit`
  - the constant `JOB_TIMEOUT_EXIT_CODE = 75`
- The env var `RAG_FEEDER_DIRECT_BATCH_TIMEOUT` defaults to `job_timeout_seconds`, with a minimum of 60.

- [ ] **Step 1: Write the failing tests.** Build the daemon the way `tests/test_pipeline_daemon.py` does (temp DB, `role="enrich"`). Set `daemon.heartbeat_interval_seconds = 0.05` and `daemon.job_timeout_seconds = 0.3` directly on the instance (this bypasses the env minimum). Set `daemon._exit = exits.append`.

```python
# test_worker_watchdog.py
def test_beat_thread_survives_database_error(tmp_path, monkeypatch):
    # make heartbeat_conn.cursor raise sqlite3.DatabaseError("database disk image is malformed") on the first call only
    # start _start_job_heartbeat(job); sleep 0.3; thread.is_alive() is True and the daemon_heartbeat row has status "running"
def test_job_past_deadline_is_failed_and_worker_exits(tmp_path):
    # insert a pipeline_jobs row status='running', worker_id=daemon.worker_id; _start_job_heartbeat(job); sleep 0.6
    assert exits == [75]
    # row status == 'failed' and json.loads(result_json)["error"].startswith("timed out after")
    # daemon_heartbeat details_json has state "job_timeout"
def test_watchdog_exits_even_when_the_db_is_locked(tmp_path):
    # a second sqlite3 connection holds BEGIN IMMEDIATE; job past deadline -> exits == [75] within 5 s; row still 'running'
def test_direct_batch_deadline_exits(tmp_path, monkeypatch):
    # monkeypatch.setenv("RAG_FEEDER_DIRECT_BATCH_TIMEOUT", "60"); daemon.job_timeout_seconds irrelevant;
    # call _start_direct_batch_heartbeat with deadline patched to 0.3 via _start_heartbeat_thread kwargs spy -> exits == [75], state "direct_batch_timeout"
def test_run_recovers_stale_jobs_every_60s(tmp_path, monkeypatch):
    # patch time.time to advance 61 s between loop iterations, fetch_next_job to stop the loop after 2 iterations;
    # _recover_stale_running_jobs spy called twice (once from __init__, once from run)
# test_openalex_partial.py
def test_citing_fetch_error_returns_incomplete(monkeypatch, capsys):
    # OpenAlexScraper.openalex_request_json: first page returns 2 results with meta.next_page, second raises requests.ConnectionError
    res = fetch_citing_work_ids("https://api.openalex.org/works?filter=cites:W1", None)
    assert isinstance(res, FetchResult) and len(res) == 2 and res.complete is False and res.errors
    assert "[OpenAlex WARN]" in capsys.readouterr().out
def test_quota_exhausted_is_flagged():
    # openalex_request_json raises OpenAlexRateLimitExceeded -> res.quota_exhausted is True, res == []
def test_referenced_batch_failure_lists_failed_ids(monkeypatch):
    # 60 ids -> 2 batches; the batch holding W51..W60 raises -> res.failed_ids == ["W51", …, "W60"], complete False
def test_fetch_openalex_batch_reports_failed_batches(monkeypatch):
    # get_openalex_session().get raises requests.Timeout for batch 2 -> FetchResult, failed_ids == batch-2 ids
def test_libgen_429_saves_debug_capture(monkeypatch):
    # BibliographyEnhancer instance (as test_thread_sessions.py builds it); _request_get_direct raises
    # requests.HTTPError(response=resp with status_code 429); _save_libgen_debug_response spy called with context="http_429"
def test_libgen_503_saves_debug_capture(monkeypatch):
    # same with 503 -> context="http_503"
def test_seed_expand_stops_on_quota(monkeypatch, capsys):
    # seed_expand: fetch_referenced_work_details returns FetchResult(quota_exhausted=True) -> stderr has "detail fetch stopped early"
```

- [ ] **Step 2: Run them and verify they fail.** Run `cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests/test_worker_watchdog.py tests/test_openalex_partial.py -q`. Expected: FAIL. The cases are: the thread dead after the DatabaseError, no exit, plain `list` results, and no debug capture.
- [ ] **Step 3: Implement D28, D29, D30 and D31.**
  - The deadline check sits in the beat loop: `time.monotonic() - started >= deadline_seconds`. It calls `on_deadline()` once, then stops beating.
  - The job's `on_deadline` runs the `UPDATE` from the spec on `heartbeat_conn` under `heartbeat_lock` and catches `sqlite3.Error` (logged). It then calls `_heartbeat("error", {...,"state": "job_timeout"}, force=True)`, `log(...)` and `self._exit(JOB_TIMEOUT_EXIT_CODE)`.
  - `run()` calls `_recover_stale_running_jobs()` when `time.time() - self._last_recovery_at >= 60`. `__init__` sets `_last_recovery_at` after its own call.
  - `FetchResult` is a plain `list` subclass whose `__init__` takes the keyword attributes. In the scripts, the existing `except OpenAlexRateLimitExceeded` branches are replaced by `if result.quota_exhausted:` with the same message.
- [ ] **Step 4: Run both files, then the whole suite.** Expected: all passed. `test_pipeline_daemon.py`, `test_worker_recovery.py`, `test_keyword_search_script.py` and `test_openalex_ref_counts.py` must stay green: `FetchResult` compares equal to a list.
- [ ] **Step 5: Add both files to `ci.yml:68` and commit** — `worker: heartbeat threads survive any DB error, job/batch watchdog fails and restarts, periodic stale-job recovery; OpenAlex fetches report partial results; LibGen error branches run (PY-47, PY-48, PY-49, PY-50, PY-51)`

---

### Task 4: Backend auth and route hardening (BE-32, BE-40, BE-41, BE-43, BE-44, BE-49, BE-50)

**Files:**
- Create: `backend/src/authMail.js`, `backend/src/loginLimiter.js`
- Modify: `backend/src/app.js`:
  - `:963-986` (`bootstrapDefaultCorpus`)
  - `:2264-2275` (`APP_SETTING_DEFS`)
  - `:2997-3083` (trust proxy, `createUserWithDefaultCorpus`, the mail senders)
  - `:3086-3104` (login)
  - `:3195-3365` (register, invitations, accept-invite, reset-password: async bcrypt)
  - `:3773-3786` (`POST /api/corpora`)
  - `:4234-4253` and `:4430-4455` (inline models)
  - `:5065` (limit clamp)
  - `:5553` (drop the `--corpus-id` push)
- Modify: `backend/src/pythonProcess.js:101-103`
- Modify: `frontend/src/components/AdminPanel.svelte:148-158` and `frontend/src/App.svelte:3868-3879` (two draft keys)
- Test: `backend/tests/auth-mail.test.js` (new), `backend/tests/login-limiter.test.js` (new); extend `backend/tests/auth.test.js`, `backend/tests/python-process.test.js`, `backend/tests/corpus.test.js`, `backend/tests/corpus-sharing.test.js`

**Interfaces:**
- Produces:
  - `escapeHtml(value: unknown) -> string`
  - `tokenMailContent({ kind: 'invite' | 'reset', username: string, url: string }) -> { subject, html, text }`
  - `createLoginLimiter({ maxPerUser = 10, maxPerIp = 100, windowMs = 900_000, maxKeys = 10_000, now = Date.now } = {}) -> { check(ip, username) -> { allowed, retryAfterSeconds }, recordFailure(ip, username), recordSuccess(ip, username) }`
  - `export function extractionInlineModels(settingsEnv = appSettingsEnv(), env = process.env) -> { cheap: string, pro: string }` from `app.js`
  - the exported constant `CORPUS_MAX_LIMIT = 1000`
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
// login-limiter.test.js
test('the 11th failure for one ip+username is refused until the oldest ages out', () => {
  // now = () => t; ten recordFailure('1.1.1.1','Alice'); check('1.1.1.1','alice').allowed === false;
  // retryAfterSeconds === 900; t += 900_001 -> allowed
})
test('another ip or another username is not affected; success clears the pair', () => {})
test('100 failures from one ip across usernames lock that ip', () => {})
test('keys beyond maxKeys evict the oldest', () => {})
// auth.test.js additions
test('login answers 429 with Retry-After after 10 failures', async () => {
  // 10 × POST /api/auth/login wrong password with X-Forwarded-For: 203.0.113.9 -> 401;
  // 11th -> 429, header retry-after '900', body.error 'Too many failed sign-in attempts. Try again in 15 minute(s).'
  // the right password from that ip+username is also 429; from X-Forwarded-For 203.0.113.10 -> 200
})
test('a spoofed leftmost X-Forwarded-For does not reset the bucket', async () => {
  // after the lockout, 'X-Forwarded-For: 1.2.3.4, 203.0.113.9' -> still 429
})
test('boot does not rewrite a matching admin hash', () => {
  // createApp twice on the same DB; users.password_hash for the admin is byte-identical; login still works
  // change RAG_ADMIN_PASSWORD, createApp -> hash differs and the new password logs in
})
test('a failing membership insert leaves no corpus behind', async () => {
  // CREATE TRIGGER boom BEFORE INSERT ON user_corpora BEGIN SELECT RAISE(ABORT,'boom'); END
  // POST /api/corpora {name:'X'} -> 500 with body.error; SELECT COUNT(*) FROM corpora WHERE name='X' === 0
  // POST /api/auth/invitations -> 500; no users row for the invited name
})
// python-process.test.js addition
test('spawnPython does not add a second --corpus-id', async () => {
  // script prints json.dumps(sys.argv[1:]); spawnPython(script, ['--corpus-id','5'], {corpusId: 5})
  // -> argv.filter(a => a === '--corpus-id').length === 1; without it in args -> appended once
})
// corpus.test.js addition
test('GET /api/corpus clamps limit to 1000', async () => {
  // seed 1005 works into the test corpus; ?limit=5000 -> items.length === 1000, total === 1005
})
// corpus-sharing.test.js addition (admin settings)
test('inline models come from Admin settings, then env, then defaults', () => {
  expect(extractionInlineModels({}, {})).toEqual({ cheap: 'gemini-3-flash-preview', pro: 'gemini-3.1-pro-preview' })
  expect(extractionInlineModels({}, { RAG_FEEDER_API_INLINE_MODEL: 'env-pro' }).pro).toBe('env-pro')
  expect(extractionInlineModels({ RAG_FEEDER_API_INLINE_MODEL: 'adm' }, { RAG_FEEDER_API_INLINE_MODEL: 'env-pro' }).pro).toBe('adm')
  // PUT /api/admin/settings {inline_model:'m', inline_cheap_model:'c'} -> 200; GET shows both values
})
```

- [ ] **Step 2: Run them and verify they fail.** Run `cd backend && npm test -- tests/auth-mail.test.js tests/login-limiter.test.js tests/auth.test.js tests/python-process.test.js tests/corpus.test.js tests/corpus-sharing.test.js`. Expected: FAIL. The cases are: missing modules, no 429, a rewritten hash, an orphan corpus, two `--corpus-id`, 1005 items, and an unknown settings key.
- [ ] **Step 3: Implement D16 to D23.**
  - One limiter instance per `createApp`.
  - `check` runs before the user `SELECT`. `recordFailure` runs on the 401 branch only, and `recordSuccess` before the 200 answer.
  - Use `app.set('trust proxy', 'loopback, linklocal, uniquelocal')` right after `const app = express()`.
  - The route builds its inline-model values from `extractionInlineModels()` once per request.
  - In the Admin panel's "Advanced" block, add the inputs `Inline model (cheap)` and `Inline model (fallback)`, both with placeholder `model name` and with `fallbackHint(appSettings.<key>)`. `App.svelte`'s draft gains `inline_cheap_model` and `inline_model`.
- [ ] **Step 4: Run `cd backend && npm test`.** Expected: all passed. Then check these:
  - `grep -n "compareSync\|hashSync" backend/src/app.js` lists only lines inside `bootstrapDefaultCorpus`.
  - `cd frontend && npx vite build` passes.
- [ ] **Step 5: Commit** — `backend: escaped auth mails, async bcrypt with a login rate limit, no admin rehash at boot, atomic user/corpus creation, one --corpus-id, inline models as Admin settings, /api/corpus limit clamp (BE-32, BE-40, BE-41, BE-43, BE-44, BE-49, BE-50)`

---

### Task 5: Frontend workspace tables and logs (FE-39, FE-40, FE-41, FE-44, FE-45, FE-56, FE-60)

**Files:**
- Create: `frontend/src/lib/logMerge.js`
- Modify: `frontend/src/App.svelte`:
  - `:916-924` (scroll handler)
  - `:2636-2642` and `:2688-2691` (`supersede: true`)
  - `:2783-2791`, `:4818-4821` (seed select-all)
  - `:3547-3669` (`loadCorpus`)
  - `:3995-4031` (`loadOlderLogs`)
  - `:4050-4140` (`connectLogs`, reconnect backfill)
  - `:5122` (`{#key currentCorpusId}`)
- Modify: `frontend/src/components/Corpus.svelte:379` (button disabled only while `corpusLoadingMore`)
- Modify: `frontend/src/app.css:2920`, `:3335`
- Test: `frontend/tests/corpus-load.spec.ts` (new), `frontend/tests/css-tokens.spec.ts` (new), `frontend/tests/log-merge.spec.ts` (new); extend `frontend/tests/seed-selection.spec.ts` and `frontend/tests/logs-stream.spec.ts`
- Modify: `.github/workflows/ci.yml:54`

**Interfaces:**
- Produces:
  - `gapLines(anchor: string[], tail: string[], receivedSince: string[]) -> string[] | null` in `lib/logMerge.js`
  - the new `loadCorpus` option `supersede`
  - `connectLogs({ reconnect = false } = {})`

- [ ] **Step 1: Write the failing tests.**

```ts
// log-merge.spec.ts (node-side, like works-lib.spec.ts)
test('gapLines returns the lines after the anchor', () => {
  expect(gapLines(['a', 'b', 'c'], ['x', 'a', 'b', 'c', 'd', 'e'], [])).toEqual(['d', 'e'])
})
test('gapLines anchors on the last occurrence of a repeated anchor', () => {
  expect(gapLines(['idle', 'idle', 'idle'], ['idle', 'idle', 'idle', 'idle', 'n1'], [])).toEqual(['n1'])
})
test('gapLines cuts the lines already received on the new socket', () => {
  expect(gapLines(['a'], ['a', 'm1', 'm2', 'r1', 'r2'], ['r1', 'r2'])).toEqual(['m1', 'm2'])
})
test('gapLines returns null without an anchor match or an anchor', () => {
  expect(gapLines(['zz'], ['a', 'b'], [])).toBeNull()
  expect(gapLines([], ['a'], [])).toBeNull()
})
// css-tokens.spec.ts (node-side)
test('every var(--name) used in app.css and component styles is defined', () => {
  // read src/app.css and the <style> blocks of src/**/*.svelte; collect /var\(--([a-z0-9-]+)/g and /--([a-z0-9-]+)\s*:/g; expect(undefined).toEqual([])
})
// corpus-load.spec.ts (mocked; /api/corpus answers delayed by a per-request promise the test resolves)
test('a filter typed during a slow poll reloads with the filter', async ({ page }) => {
  // hold the poll's /api/corpus; type 'labour' in the corpus filter; expect a request with q=labour while the poll is pending;
  // release the poll last -> rows shown are the q=labour rows
})
test('a sort click during a poll is applied', async ({ page }) => {})
test('Show more clicked during a poll runs after it', async ({ page }) => {
  // hold the poll; click Show more (enabled); release -> one request with offset=120 follows
})
test('a filter typed while Show more is in flight discards the append and its queue', async ({ page }) => {
  // hold the append (offset=120); type 'x'; release the append -> rows are only the q=x rows; no request with q=x&offset=120
})
test('Show more drops rows already loaded', async ({ page }) => {
  // page 2 answer repeats the last id of page 1 -> each data-work-id appears once; no console "each_key_duplicate" error
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
test('Load older stops at 5,000 lines and says so', async ({ page }) => {
  // tail returns 200 lines with has_more; live socket pushes 4,900 lines; click Load older -> a request with lines=0 is NOT sent,
  // status reads 'The log view holds 5,000 lines. Older lines stay in the log file.'; with 4,950 shown -> request lines=50 and all 50 prepended
})
test('a reconnect backfills the lines missed while the socket was down', async ({ page }) => {
  // first socket sends a,b,c then closes (code 1006); tail on reconnect answers [a,b,c,m1,m2,r1]; new socket sends r1 before the tail answers
  // -> rendered texts end with a,b,c,m1,m2,r1 exactly once each
})
test('a reconnect after a gap beyond the tail reloads from the file', async ({ page }) => {
  // tail has no anchor -> view equals the tail; status 'Reconnected; reloaded the latest log lines.'
})
```

- [ ] **Step 2: Run them and verify they fail.** Run `cd frontend && E2E_FRONTEND_PORT=5190 npx playwright test tests/log-merge.spec.ts tests/css-tokens.spec.ts tests/corpus-load.spec.ts tests/seed-selection.spec.ts tests/logs-stream.spec.ts --workers=2`. Expected: FAIL. The cases are: the missing module, `--text` undefined, the dropped filter request, the disabled Show more, a duplicate row, the selection surviving, the checkbox unchecked, the trimmed Load older, and no backfill.
- [ ] **Step 3: Implement D1 to D7.** `receivedSince` is the texts pushed by the new socket since its `onopen`, counted in `logsSinceReconnect`, which `onopen` resets for a reconnect. `logsGapAnchor` is cleared after a backfill and by `resetLogState`.
- [ ] **Step 4: Run the Task 5 specs plus the regression set, and build.** Run `E2E_FRONTEND_PORT=5190 npx playwright test tests/tab-resources.spec.ts tests/refresh-scheduler.spec.ts tests/corpus-reconcile.spec.ts tests/dead-code.spec.ts tests/table-columns.spec.ts tests/session.spec.ts --workers=2`, then `npx vite build`. Expected: all pass. The existing `logs-stream.spec.ts` reconnect tests ("lines buffered before a socket reconnect survive it", "500 buffered lines survive a reconnect whole") must still pass. In those, the tail mock answers with the anchor present.
- [ ] **Step 5: Add `tests/corpus-load.spec.ts tests/css-tokens.spec.ts tests/log-merge.spec.ts` to `ci.yml:54` and commit** — `frontend: user corpus reloads supersede polls, Show more queues, append dedupes, Corpus keyed by corpus, select-all counts selectable rows, --text fixed, Load older honest at the cap, reconnect backfill (FE-39, FE-40, FE-41, FE-44, FE-45, FE-56, FE-60)`

---

### Task 6: Frontend session and failure states (FE-42, FE-43, FE-46, FE-48, FE-50, FE-57, FE-58; PY-39 UI)

**Files:**
- Modify: `frontend/src/App.svelte`:
  - `:341-353` (quota epoch)
  - `:168-172` and `:2020-2025` (`null` global defaults and their reset)
  - `:1599-1610` (boot catch)
  - `:1700-1716` and `:1750-1766` (invite/reset catches)
  - `:1796-1823` (`expireSession` message)
  - `:2192-2275` (targets error, browse error)
  - `:2399-2431` (stopped import)
  - `:2474-2494` and `:3697-3738` (global error flags)
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
  // login form shows '<transport or server text> Your session is kept; press Retry when the server is back.'
  // localStorage token unchanged; click getByTestId('auth-retry') -> workspace visible without signing in
})
test('a 401 mid-session says the session expired', async ({ page }) => {
  // signed in; next request 401 -> login form .error 'Your session has expired. Please sign in again.'
})
test('a 4401 socket close says the session expired', async ({ page }) => {
  // mockLogSocket(page, (ws) => setTimeout(() => ws.close({ code: 4401 }), 200)) -> same message
})
test('a stale token at boot says the session expired; an explicit sign-out says nothing', async ({ page }) => {})
test('invite accepted but /me fails: sign-in form with the username and the saved-password text', async ({ page }) => {
  // #/accept-invite?token=t; /api/auth/accept-invite -> {token, user:{username:'neu'}}; /api/auth/me -> 500 {error:'boom'}
  // sign-in form visible, username input value 'neu', .error starts 'Your password was saved, but signing in failed (boom).'
  // localStorage token null
})
test('reset accepted but /me fails behaves the same', async ({ page }) => {})
test('a quota answer from before a sign-out does not reach the next session', async ({ page }) => {
  // hold /api/openalex/quota; sign out; sign in as bob; release the old answer -> pill shows bob's quota (or none), not the old value;
  // bob's own quota request was sent (the old flag did not block it)
})
// upstream-status.spec.ts (new)
test('no Kantropos targets shows "No upstream corpora are configured."', async ({ page }) => {})
test('a failed target list shows the error, not "none configured"', async ({ page }) => {
  // /api/kantropos/corpora -> 500 {error:'down'} -> 'Could not load the Kantropos corpora: down'
})
test('a failed first browse shows the error and Retry, and no empty-state texts', async ({ page }) => {
  // browse -> 502 -> status .error 'Could not load the upstream corpus: …'; 'No pending additions match' and 'has not been imported' absent;
  // Retry -> browse succeeds -> sections render, error gone
})
test('a failed quiet refresh keeps the data and says it is stale', async ({ page }) => {
  // first browse ok; next quiet browse 500 -> status ends ' Showing the last loaded data.'; pending rows still visible
})
// tab-resources.spec.ts addition
test('a failed global stats load shows dashes and the unavailable note', async ({ page }) => {
  // admin dashboard; /api/ingest/stats?global=1 -> 500 -> 'Global totals could not be loaded.'; Raw shows '—' not '0'
})
// seed-upload.spec.ts addition
test('a stopped import shows the counts it saved', async ({ page }) => {
  // import-seed -> {added: 380, skipped: 0, corpus_linked: 20, total: 1000, processed: 400, stopped: true, error: 'OperationalError: database is locked', errors: []}
  // item message 'Import stopped after 400 of 1000 entries: OperationalError: database is locked (380 added, 20 linked before it).'; status failed
})
```

- [ ] **Step 2: Run them and verify they fail.** Run `cd frontend && E2E_FRONTEND_PORT=5190 npx playwright test tests/session.spec.ts tests/upstream-status.spec.ts tests/tab-resources.spec.ts tests/seed-upload.spec.ts --workers=2`. Expected: FAIL. The cases are: the token cleared at boot, no expiry text, the raw invite error, the stale quota, a blank Korpus Management, zeros, and an "imported" message.
- [ ] **Step 3: Implement D8 to D15.**
  - `authBootRetry` is cleared by `bootstrapAuth`'s start, `enterSession` and `endSession`.
  - `upstreamBrowseError` and `kantroposTargetsError` are reset by `resetFrontendState`. `globalStatsError` and `globalDaemonError` are reset on sign-out.
  - The 4401 branch becomes `expireSession(); logsStatus = 'unauthorized'`.
- [ ] **Step 4: Run the Task 6 specs plus the whole CI Playwright list, and build.** Run `E2E_FRONTEND_PORT=5190 npx playwright test <ci.yml:54 list> --workers=2`, then `npx vite build`. Expected: all pass. The existing session tests that assert an empty `authError` after an explicit sign-out keep passing, because only `expireSession` sets the message.
- [ ] **Step 5: Add `tests/upstream-status.spec.ts` to `ci.yml:54` and commit** — `frontend: boot keeps the token on server errors, session-expired message, invite/reset recovery text, session-scoped quota, Korpus Management errors with Retry, global overview unavailable state, stopped imports shown (FE-42, FE-43, FE-46, FE-48, FE-50, FE-57, FE-58, PY-39)`
