# Review fixes, phase 7: owner decisions, login limit and pipeline robustness — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Carry out the owner decisions of 2026-11-14 and the slimmed robustness phase:
- BE-19, BE-46, FE-55 and PY-14;
- a per-account login limit;
- CLI download leases (PY-16) and a hang limit for hung items (K-M4);
- Jest temp-dir cleanup and the whole Python suite in CI;
- worker errors shown as text (FE-23, error-text half).

**Architecture:** Seven tasks grouped by surface. Tasks on different surfaces touch disjoint files.
1. Python: Tasks 1, 2 and 3. Tasks 2 and 3 share `backend/scripts/daemon/worker.py`, so Task 3 runs after Task 2.
2. Backend: Tasks 4 and 5.
3. Frontend: Task 6.
4. CI: Task 7.

Two new backend modules: `loginLimiter.js` and `pipelineJobRetention.js`. One new frontend module: `lib/workerStatus.js`. One new Jest helper: `tests/jest-tmp.js`. No schema change.

**Tech Stack:** Python 3.11 (`/home/jay/DT/venv/bin/python`), pytest, sqlite3, click 8.2; Node 20, Express 5, better-sqlite3, Jest + supertest; Svelte 5 + Vite, Playwright (mocked).

**Spec:** `docs/superpowers/specs/2026-11-14-review-fixes-7-decisions-and-robustness-design.md`

## Parallelism and shared files

| Task | Surface | Runs |
|---|---|---|
| 1 DOI rule (PY-14) | Python | parallel |
| 2 CLI leases (PY-16) | Python | parallel |
| 3 Hung items (K-M4) | Python | **after Task 2** |
| 4 Assignment + login limit (BE-19, BE-40) | backend | parallel |
| 5 Job retention + Jest temp dirs (BE-46, BE-51) | backend | parallel |
| 6 Frontend (FE-55, BE-19 UI, FE-23, BE-40 UI) | frontend | parallel |
| 7 Whole Python suite (D12) | CI | parallel |

Shared files:
- `backend/scripts/daemon/worker.py` is shared by Task 2 and Task 3.
  - Task 2 edits `_download_single` and `_periodic_owner_is_dead`.
  - Task 3 edits `_init_work_state`, `_finish_after_win`, `_job_deadline_reached`, `_direct_batch_deadline_reached`, `do_enrich` and `do_download`.
  - The tasks run in sequence, never in parallel.

No other file is touched by two tasks. One behavioural coupling crosses tasks: Task 5's `TMPDIR` change moves every backend suite's temp dirs into the run dir, including the files Task 4 adds. Task 4's tests must therefore not depend on `/tmp` paths.

## Global Constraints

- Work only in `/home/jay/DT/.worktrees/review-fixes-7`, on branch `review-fixes-7`. Do not push.
- Never open `dl_lit_project/data/literature.db`; tests use `tmp_path` / `fs.mkdtempSync` DBs.
- Python tests: `cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests -q` (whole suite) plus the task's files. New test files need no CI edit after Task 7 (CI runs the whole suite).
- Backend tests: `cd backend && npm test`; prefer `npm test -- tests/<file>` while iterating. On this machine, full parallel runs can time out suites that pass alone. Use `npm test -- --maxWorkers=2` for the full run, and compare with a `0702e47` baseline before blaming your change. CI's Python for backend tests has only `requests`.
- Frontend tests: from `frontend/`, `E2E_FRONTEND_PORT=5190 npx playwright test <specs> --workers=1`, mocked specs only. Never use the dev stack on `:5175`, and never run `*.real*.spec.ts` or `pipeline-perf.real-pdf.spec.ts`. `npx vite build` must pass. Every mocked spec that fakes a token calls `mockLogSocket(page)`.
- These strings are copied exactly from the spec:
  - D3:
    - `hung past the <deadline:g>s timeout; attempt limit <limit> reached`
    - `Watchdog: <n> hung item(s) failed at the attempt limit, <m> left for a retry after the restart.`
    - `Watchdog: hung items not recorded (<reason>).`
  - D4: `Only the global admin can assign a Kantropos corpus`
  - D5: `Too many failed sign-ins for this account. Try again in <n> minute(s).`. The word is "minute" when `n === 1`, otherwise "minutes".
  - D6:
    - `[pipeline-jobs] pruned <n> finished job(s) older than 30 days`
    - `[pipeline-jobs] retention failed: <message>`
  - D7: `DT_JEST_TMP_ROOT is unset: run the backend tests through jest's globalSetup (npm test).`
  - D8:
    - `Could not check how many works match (<message>).`
    - `This search has no cap, so it may fetch a very large number of works.`
    - `This search stops at <cap> works.`
    - `Narrow the query, or:`
    - the button label `Search anyway`
    - the fallback message `the server did not answer`
  - D9:
    - the select title `Only admins can change the Kantropos assignment`
    - the save guard `Only admins can change the Kantropos assignment.`
- These values are fixed by the spec:
  - D3: limit `RAG_FEEDER_HUNG_ITEM_MAX_ATTEMPTS`, default 3, minimum 1.
  - D5: `maxFailures = 10`, `windowMs = 900000`.
  - D6: `days = 30`, `intervalMs = 86400000`.
  - D7: sweep `maxAgeMs = 86400000`, prefix `dt-jest-`.
  - D2: claim lease 900 s, claimant `cli:<host>:<pid>:<6 hex>`.
- No proxy, `trust proxy` or Caddy change. No new DB column or index.
- Each commit message names the rows it closes and ends with:
  ```
  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01U3c4gfg7jQvvmPdpLNBN5T
  ```
- Secrets stay in `.env`; nothing from it goes into tests, docs or commits.

## Review Focus

1. **A burst of concurrent wrong passwords for one account.** At most 10 password checks may run in a window; the rest answer 429 at once. Test in Task 4 (`concurrent wrong passwords get at most 10 checks`).
2. **A claimed row that had not started when the watchdog won.** A row waiting in the pool queue or a later enrich wave must never be failed, whatever its attempt count. Test in Task 3 (row C).
3. **The CLI dies mid-run.** An exception or Ctrl-C must leave no `in_progress` row claimed by the CLI. Test in Task 2.
4. **Old pending or running jobs.** Retention must never delete them, however old. Test in Task 5.
5. **Enter pressed again while the unknown-count warning shows.** The form resubmits, the preview runs again, and the run still waits for "Search anyway". Test in Task 6.

---

### Task 1: The three scripts use the DB's DOI rule (PY-14)

**Files:**
- Modify: `backend/scripts/export_bundle.py`:
  - `:1-9` (bootstrap and import of `normalize_doi`);
  - `:22-32` (delete `_normalize_doi`);
  - `:128` (`normalized_doi`).
- Modify: `backend/scripts/upstream_update.py`:
  - imports (bootstrap and `normalize_doi`);
  - `:234-245` (replace `_normalize_doi` with `display_doi`);
  - `:307`, `:379`, `:831`.
- Modify: `backend/scripts/backfill_openalex_topics.py`:
  - `:32` (import);
  - `:55-64` (delete `_normalize_doi`);
  - `:234-258` (pass 2 uses `doi_rows`, `doi_filter` and `normalize_doi`).
- Modify: `dl_lit_project/tests/test_identifiers.py:61-66` (three `DOI_MAP` entries out) and `:213-226` (the private-copies test out).
- Modify: `dl_lit_project/tests/fixtures/identifier_cases.json` (remove the `doi` entries `backend.scripts.backfill_openalex_topics.normalize_doi`, `backend.scripts.export_bundle.normalize_doi`, `backend.scripts.upstream_update.normalize_doi`).
- Test: `dl_lit_project/tests/test_doi_rule_scripts.py` (new)

**Interfaces:**
- Produces:
  - `upstream_update.display_doi(value) -> str`: `normalize_doi(value, case="lower") or ""`.
  - `backfill_openalex_topics.doi_rows(conn) -> tuple[list[tuple[int, str]], int]`: the `(work_id, DB key)` pairs for pass 2, and the count skipped because the key does not start with `10.`.
  - `backfill_openalex_topics.doi_filter(tokens: list[str]) -> str`: `"doi:" + "|".join(t.lower() for t in tokens)`.
  - Each of the three modules exposes `normalize_doi`, which is `dl_lit.identifiers.normalize_doi`.

- [ ] **Step 1: Write the failing tests** in `test_doi_rule_scripts.py`. Load the scripts as `test_identifiers._script` does.

```python
def test_scripts_use_the_db_doi_rule():
    for name in ("export_bundle", "upstream_update", "backfill_openalex_topics"):
        mod = _script(name)
        assert mod.normalize_doi is normalize_doi
        assert not hasattr(mod, "_normalize_doi")
def test_export_uses_the_stored_db_key(tmp_path):
    # DatabaseManager(tmp db); _insert_work-style INSERT with doi "https://doi.org/10.1000/Ab.C", normalized_doi = normalize_doi(doi)
    # item = export_bundle.load_items(conn (row_factory=sqlite3.Row), None)[0]
    assert item["normalized_doi"] == "10.1000/AB.C" and item["doi"] == "https://doi.org/10.1000/Ab.C"
def test_export_falls_back_to_the_rule_when_the_key_is_empty(tmp_path):
    # normalized_doi NULL, doi "doi:10.5/xy" -> item["normalized_doi"] == "10.5/XY"
def test_export_dedupes_like_the_db(tmp_path):
    # two rows, both normalized_doi NULL and no openalex_id: doi "10.1/ABC" and "https://doi.org/10.1/abc" -> len(load_items(...)) == 1
def test_upstream_bibtex_doi_is_the_lowercase_rule():
    # sqlite3.Row from an in-memory table with the columns render_bibtex_entry reads;
    # doi "DOI: 10.1000/ABC" -> key, entry = render_bibtex_entry(row, "x.pdf", set())
    assert "doi = {10.1000/abc}" in entry and key == "doi_10_1000_abc"
    assert upstream_update.display_doi("https://doi.org/10.1000/ABC") == "10.1000/abc"
    assert upstream_update.display_doi(None) == ""
def test_backfill_keys_on_the_db_rule_and_skips_non_dois(tmp_path):
    # works rows (topics_fetched_at NULL or whatever pass 2 selects): doi "10.1/ABC", doi "not a doi"
    rows, skipped = backfill.doi_rows(conn)
    assert rows == [(1, "10.1/ABC")] and skipped == 1
    assert backfill.doi_filter(["10.1/ABC"]) == "doi:10.1/abc"
def test_backfill_matches_a_returned_work_on_the_db_key(tmp_path, monkeypatch):
    # monkeypatch backfill.openalex_request_json -> {"results": [{"id": "https://openalex.org/W1", "doi": "https://doi.org/10.1/abc", "primary_topic": {...}, "topics": []}]}
    # backfill.run_pass(conn, rows, key=lambda w: normalize_doi(w.get("doi")), build_filter=backfill.doi_filter, ...) -> matched == 1
```

In `test_identifiers.py`, remove the three entries from `DOI_MAP` and from the fixture, and delete `test_private_doi_copies_differ_from_every_shared_rule`. `test_fixture_covers_every_former_copy` must still pass, with its count of 25 cases per DOI copy.

- [ ] **Step 2: Run the tests and verify they fail.** Run `cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests/test_doi_rule_scripts.py -q`. Expected: FAIL. The scripts have no `normalize_doi`, the export key is lowercase, and `display_doi`, `doi_rows` and `doi_filter` are undefined.
- [ ] **Step 3: Implement D1.**
  - `export_bundle.py` and `upstream_update.py` gain the `_bootstrap.ensure_import_paths(__file__)` preamble that `backfill_openalex_topics.py:24-30` uses, then `from dl_lit.identifiers import normalize_doi`.
  - `export_bundle`: `"normalized_doi": data.get("normalized_doi") or normalize_doi(data.get("doi"))`.
  - `upstream_update`:
    - `:307` uses `normalize_doi(...)` for the key candidate;
    - `:379` and `:831` use `display_doi(...)`.
  - `backfill`: pass 2 builds its rows with `doi_rows`, matches with `key=lambda work: normalize_doi(work.get("doi"))` and filters with `doi_filter`, and adds `<skipped> skipped (not a 10. DOI)` to the `Pass 2 (by doi)` log line.
- [ ] **Step 4: Run `test_doi_rule_scripts.py`, `test_identifiers.py`, `test_backend_helper_scripts.py`, `test_upstream_recovery.py` and `test_upstream_text_scan.py`, then the whole suite.** Expected: all passed.
- [ ] **Step 5: Commit** — `scripts: export_bundle, upstream_update and backfill_openalex_topics use the DB's DOI rule (PY-14)`

---

### Task 2: The CLI claims rows like the daemon (PY-16)

**Files:**
- Modify: `dl_lit_project/dl_lit/db_manager.py`:
  - add `find_downloaded_duplicate` and `release_download_claims` next to `claim_download_batch` (`:2638`);
  - delete `fetch_download_queue` (`:1855`).
- Modify: `dl_lit_project/dl_lit/cli.py:649-736` (`download_pdfs_command`).
- Modify: `backend/scripts/daemon/worker.py`:
  - `:1199-1211` (`_download_single` calls `find_downloaded_duplicate`);
  - `:704` (`_periodic_owner_is_dead` accepts the prefixes `daemon` and `cli`; update its docstring).

  **Shared with Task 3.**
- Modify: `dl_lit_project/tests/test_cli_downloader.py` (the mocks move from `fetch_download_queue`/`check_if_exists` to `claim_download_batch`/`find_downloaded_duplicate`/`release_download_claims`).
- Modify: `dl_lit_project/tests/test_download_path.py:198-221` (the same switch in `test_cli_downloaded_count_ignores_the_duplicate_merge_result`).
- Test: `dl_lit_project/tests/test_cli_download_leases.py` (new)

**Interfaces:**
- Produces:
  - `DatabaseManager.find_downloaded_duplicate(row: dict, authors) -> tuple[str | None, int | None, str | None]`. It calls `check_if_exists` with the arguments listed in spec D2 and returns the hit only for a `works` row whose `download_status` is `downloaded`.
  - `DatabaseManager.release_download_claims(claimed_by: str) -> int`. It sets every `in_progress` row with that `download_claimed_by` back to `queued`, clears `download_claimed_by`, `download_claimed_at` and `download_lease_expires_at`, commits, and returns the count.
- Consumed by Task 3: none. Task 3 edits other functions of `worker.py`.

- [ ] **Step 1: Write the failing tests** in `test_cli_download_leases.py`. Use real temp DBs with `_insert_work` from `tests.test_download_path`, patch `dl_lit.cli.download_ref` and `dl_lit.cli.BibliographyEnhancer`, and run through `CliRunner().invoke(cli, ["download-pdfs", "--db-path", db, "--download-dir", d])`.

```python
def test_cli_claims_a_row_before_downloading_it(tmp_path, mocker):
    # queued row; download_ref side effect reads the row from a second sqlite3 connection and records it, then returns (None, None, [])
    assert seen == [("in_progress", True)]   # (download_status, download_claimed_by.startswith("cli:"))
    # afterwards: download_status 'failed', download_claimed_by NULL
def test_cli_skips_a_row_a_live_daemon_holds(tmp_path, mocker):
    # row in_progress, download_claimed_by 'daemon:h:1:abc', download_lease_expires_at = now + 900
    assert "Queue is empty." in result.output and download_ref_mock.call_count == 0
@pytest.mark.parametrize("other_status,merged", [("downloaded", True), ("not_requested", False)])
def test_cli_merges_only_into_a_downloaded_duplicate(tmp_path, mocker, other_status, merged):
    # same title/authors/year; the other work has download_status=other_status
    # merged -> "duplicate detected" in output and download_ref not called; else download_ref called once
def test_cli_releases_its_claims_when_it_fails(tmp_path, mocker):
    # two queued rows; download_ref raises RuntimeError("boom") on the first call
    assert result.exit_code != 0
    # both rows: download_status 'queued', download_claimed_by NULL, download_lease_expires_at NULL
def test_daemon_download_uses_the_shared_duplicate_rule(tmp_path, monkeypatch):
    # load_worker; daemon.db.find_downloaded_duplicate patched to return ("works", 9, "normalized_doi");
    # merge_duplicate_queued_work patched -> daemon._download_single(row)[0] == "skipped" and find_downloaded_duplicate was called once
@pytest.mark.parametrize("owner,released", [
    ("cli:{host}:{live_pid}:abc", False),   # this host, live pid (os.getpid())
    ("cli:{host}:{dead_pid}:abc", True),    # this host, pid of a reaped child process
    ("cli:otherhost:1:abc", False),
])
def test_periodic_recovery_treats_a_cli_owner_like_a_daemon(tmp_path, monkeypatch, owner, released):
    # works row in_progress claimed by owner, no daemon_heartbeat row for it; download-role daemon; _recover_stale_running_jobs(periodic=True)
```

- [ ] **Step 2: Run the tests and verify they fail.** Run `cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests/test_cli_download_leases.py -q`. Expected: FAIL. The row is not claimed, the CLI downloads a row the daemon holds, it merges into an undownloaded work, rows stay in a claimed state, `find_downloaded_duplicate` is missing, and the live CLI owner is released.
- [ ] **Step 3: Implement D2.**
  - The CLI builds `cli_id` (spec D2), claims with `lease_seconds=900`, keeps `'Queue is empty.'` for an empty claim, dedupes with `find_downloaded_duplicate`, and wraps the loop in `try/finally` that calls `release_download_claims(cli_id)` and then `close_connection()`.
  - `_download_single` replaces its inline check with `self.db.find_downloaded_duplicate(row, authors)` under `db_lock`.
- [ ] **Step 4: Run the new file, `test_cli_downloader.py`, `test_download_path.py`, `test_download_claims.py`, `test_worker_watchdog.py` and `test_worker_recovery.py`, then the whole suite.** Expected: all passed.
- [ ] **Step 5: Commit** — `cli: download-pdfs claims and releases rows like the daemon and merges only into downloaded works (PY-16)`

---

### Task 3: A hung item fails after 3 hangs (K-M4) — after Task 2

**Files:**
- Modify: `backend/scripts/daemon/worker.py`. **Shared with Task 2**; start from Task 2's commit.
  - `_init_work_state` (`:277-285`): add `self._running_items = {}` and `self._running_items_lock = threading.Lock()`.
  - New `_item_running`, `_hung_item_limit` and `_fail_hung_items`, placed next to `_finish_after_win`.
  - `_job_deadline_reached` (`:503-525`) and `_direct_batch_deadline_reached` (`:527-537`): pass `deadline` on.
  - `_finish_after_win` (`:539-563`): gains `deadline`, and runs `_fail_hung_items` inside its `heartbeat_lock` section after the heartbeat attempt.
  - `do_enrich` (`:1086-1110`): track the submitted `_enrich_single` and the `_attempt_raw_download(entry)` call.
  - `do_download` (`:1304-1305`): track each submitted `_download_single`.
- Test: `dl_lit_project/tests/test_hung_items.py` (new)

**Interfaces:**
- Produces (all on `PipelineDaemon`):
  - `_item_running(phase: str, work_id: int)`, a context manager. It adds `(phase, work_id) -> time.monotonic()` to `_running_items` under `_running_items_lock` and removes the entry in its `finally`.
  - `_hung_item_limit() -> int`: `env_int("RAG_FEEDER_HUNG_ITEM_MAX_ATTEMPTS", 3, minimum=1)`.
  - `_fail_hung_items(conn, deadline: float) -> tuple[int, int]`, which returns `(failed, left)`. It runs on the caller's connection with the lock already held, and raises `sqlite3.Error`. The columns per phase are those of spec D3. `HUNG_ITEM_COLUMNS` maps `"enrich"` and `"download"` to them.
  - `_finish_after_win(details: dict, message: str, deadline: float) -> None`.
- Consumes: `claim_download_batch` / `claim_enrich_batch`, unchanged, and start-up recovery, unchanged.

- [ ] **Step 1: Write the failing tests** in `test_hung_items.py`. Use a local `_daemon(monkeypatch, tmp_path, role)` built like `test_worker_watchdog._daemon`, with `daemon._exit = exits.append`.

```python
def test_a_download_is_tracked_only_while_it_runs(tmp_path, monkeypatch):
    # role download; two queued rows; _download_single patched to record set(daemon._running_items) and return ("failed", {...})
    # each recorded set contains ("download", its id); after do_download, daemon._running_items == {}
def test_enrichment_and_its_raw_download_are_tracked(tmp_path, monkeypatch):
    # role enrich; one pending row; _enrich_single returns (None, None, {"status": "api_error"});
    # _attempt_raw_download patched to record set(daemon._running_items) -> contains ("enrich", id); after: empty
def test_win_fails_only_running_items_at_the_limit(tmp_path, monkeypatch):
    # role download; rows (download_status in_progress, claimed by daemon.worker_id unless noted):
    #   A attempt 3 running; B attempt 2 running; C attempt 3 claimed, not running; D attempt 3 running, claimed by 'daemon:x:1:zzz'
    gen = daemon._begin_work()
    with daemon._item_running("download", A), daemon._item_running("download", B), daemon._item_running("download", D):
        daemon._direct_batch_deadline_reached(gen, {"phase": "download"}, 0.3)
    assert exits == [75]
    # A: ('failed', 'hung past the 0.3s timeout; attempt limit 3 reached', claimed_by NULL)
    # B and C: ('in_progress', claimed by daemon.worker_id); D unchanged
    # log contains 'Watchdog: 1 hung item(s) failed at the attempt limit, 1 left for a retry after the restart.'
def test_an_item_that_hangs_every_time_fails_on_its_third_claim(tmp_path, monkeypatch):
    # one queued row; for round in 1..3: d = _daemon(...) (start-up recovery releases the previous worker's claim),
    # rows = d.db.claim_download_batch(limit=5, corpus_id=None, claimed_by=d.worker_id, lease_seconds=900);
    # with d._item_running("download", row): d._direct_batch_deadline_reached(d._begin_work(), {"phase": "download"}, 0.3)
    # after rounds 1 and 2 the next daemon's recovery puts it back to 'queued'; after round 3 it is 'failed'
    # a fourth daemon's claim_download_batch(...) == []
def test_a_job_win_blames_running_items_too(tmp_path, monkeypatch):
    # role enrich; running job row of this worker; pending->in_progress metadata row attempt 3 claimed by this worker, running;
    # _job_deadline_reached(gen, {"job_id": job, ...}, 0.3) -> metadata_status 'failed', metadata_error starts 'hung past the 0.3s timeout'
def test_a_skipped_hung_write_is_logged_and_the_worker_still_exits(tmp_path, monkeypatch):
    # another thread holds daemon.heartbeat_lock; win -> log contains 'Watchdog: hung items not recorded (' ; exits == [75];
    # the running row stays in_progress
def test_hung_item_limit_reads_env_with_minimum(tmp_path, monkeypatch):
    # unset -> 3; "0" -> 1; "5" -> 5
```

- [ ] **Step 2: Run the tests and verify they fail.** Run `cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests/test_hung_items.py -q`. Expected: FAIL. `_item_running` is undefined, and nothing is failed at the win.
- [ ] **Step 3: Implement D3.**
  - `_finish_after_win` keeps its order: test seam, then the heartbeat attempt, then the log, then the exit. Inside the acquired `heartbeat_lock` with the 1000 ms busy timeout, it calls `_fail_hung_items(conn, deadline)` in its own `try` after the heartbeat. The lock-timeout and exception paths log the D3 skip line.
  - `_fail_hung_items` reads a snapshot of `_running_items` under its lock and runs one `UPDATE` per phase present (spec D3 SQL), with `<claimed_by> = self.worker_id` and the limit as parameters, then commits. It returns the failed count, and the running ids not failed as `left`.
- [ ] **Step 4: Run the new file, `test_worker_watchdog.py`, `test_pipeline_daemon.py` and `test_worker_recovery.py`, then the whole suite.** Expected: all passed. The watchdog tests' bounds still hold.
- [ ] **Step 5: Commit** — `worker: a watchdog win fails the items that were running once they reach the attempt limit, ending the restart loop of an item that always hangs (K-M4)`

---

### Task 4: Admin-only Kantropos assignment and the per-account login limit (BE-19, BE-40)

**Files:**
- Create: `backend/src/loginLimiter.js`
- Modify: `backend/src/app.js`:
  - `:2573` (`createApp({ broadcast, broadcastEvent, mailTransport, loginLimiter } = {})`);
  - `:3123-3153` (login handler);
  - `:3935-3949` (GET `can_edit`; `requireKantroposAssigner` replaces `requireCorpusConfigurer`).
- Modify tests: `backend/tests/access-routes.test.js`:
  - `:129-133` (`beforeAll` assigns as admin);
  - `:228-257` (renamed `kantropos-assignment: GET member, PUT admin only`).
- Modify tests: `backend/tests/corpus-sharing.test.js:218-223` (the editor's PUT now answers 403).
- Modify tests: `backend/tests/auth.test.js`, which gains one test in the hasher-mock block.
- Test: `backend/tests/login-limit.test.js` (new)

**Interfaces:**
- Produces:
  - `createLoginLimiter({ maxFailures = 10, windowMs = 900000, now = () => Date.now() } = {}) -> { take(key) -> { allowed: true } | { allowed: false, retryAfterSeconds: number }, refund(key) -> void, clear(key) -> void }`
  - the `createApp` option `loginLimiter`
- The 429 body and header (spec D5) are consumed by Task 6's mocked spec, which copies the text.

- [ ] **Step 1: Write the failing tests.**

```js
// login-limit.test.js — unit
test('the 11th take in a window is refused with the time left', () => {
  // clock t=0; 10 × take('u') -> allowed; take('u') -> { allowed: false, retryAfterSeconds: 900 }
  // t=899_500 -> retryAfterSeconds 1; t=900_000 -> allowed (a new window, count 1)
})
test('refund undoes one count and clear resets', () => {})
// login-limit.test.js — routes. Set RAG_FEEDER_JWT_SECRET, RAG_ADMIN_USER, RAG_ADMIN_PASSWORD and a temp
// RAG_FEEDER_DB_PATH, then dynamic-import app.js; createApp({ broadcast() {}, loginLimiter: createLoginLimiter({ now: () => clock }) });
// user 'b' inserted with bcryptjs.hashSync('pw-b', 4), account_status 'active'.
test('10 wrong passwords lock the account; the 11th attempt answers 429 with Retry-After', async () => {
  // 10 × wrong -> 401; 11th -> 429, headers['retry-after'] === '900',
  // body.error === 'Too many failed sign-ins for this account. Try again in 15 minutes.'
})
test('a locked account refuses the right password until the window passes', async () => {
  // right -> 429; clock += 899_000 -> 429, retry-after '1', 'Try again in 1 minute.'; clock += 1_000 -> right -> 200
})
test('a successful sign-in clears the count', async () => {
  // 9 wrong, right (200), 9 wrong -> no 429
})
test('concurrent wrong passwords get at most 10 checks', async () => {
  // Promise.all(30 × wrong) -> exactly 10 × 401 and 20 × 429
})
test('other accounts and unknown usernames are not counted', async () => {
  // lock 'b'; admin right -> 200; 15 × unknown 'nobody' -> all 401
})
// auth.test.js addition (hasher mock block)
test('a busy hasher does not count against the account', async () => {
  // hasherState.busy = true; 11 × login -> all 503; busy = false; wrong -> 401 (not 429); right -> 200
})
// access-routes.test.js
test('kantropos-assignment: GET member, PUT admin only', async () => {
  // viewer3, editor4, owner1 PUT -> 403, body.error 'Only the global admin can assign a Kantropos corpus'
  // admin PUT -> 200; GET can_edit: owner1 false, editor4 false, admin true
})
// corpus-sharing.test.js: the editor's PUT expects 403 with the same text
```

- [ ] **Step 2: Run them and verify they fail.** Run `cd backend && npm test -- tests/login-limit.test.js tests/access-routes.test.js tests/corpus-sharing.test.js tests/auth.test.js`. Expected: FAIL. There is no `loginLimiter.js`, no 429 is answered, and the editor and owner PUTs answer 200.
- [ ] **Step 3: Implement D4 and D5.**
  - Login: after the invited check and `if (!user)`, call `const gate = loginLimiter.take(user.id)`. When it is not allowed, set `Retry-After` and answer 429.
  - The compare's `catch` calls `loginLimiter.refund(user.id)` before it answers 503 or 500.
  - `!valid` keeps the count; success calls `clear`.
- [ ] **Step 4: Run the four files, then `npm test -- --maxWorkers=2`.** Expected: all passed. `grep -n "requireCorpusConfigurer" src/app.js` prints nothing.
- [ ] **Step 5: Commit** — `backend: only the admin assigns a Kantropos corpus; 10 failed sign-ins per account in 15 minutes answer 429 until the window passes (BE-19, BE-40)`

---

### Task 5: Pipeline job retention and Jest temp dirs (BE-46, BE-51)

**Files:**
- Create: `backend/src/pipelineJobRetention.js`
- Modify: `backend/src/index.js:101` (call `startPipelineJobRetention(app.authDb)` right after `createApp`)
- Create: `backend/tests/jest-tmp.js`
- Modify: `backend/tests/global-setup.js` (sweep first) and `backend/tests/setup-env.js:12-14` (throw instead of the fallback; set `TMPDIR`)
- Test: `backend/tests/pipeline-job-retention.test.js` (new), `backend/tests/jest-tmp.test.js` (new)

**Interfaces:**
- Produces:
  - `pruneFinishedPipelineJobs(db, { days = 30 } = {}) -> number`
  - `startPipelineJobRetention(db, { intervalMs = 86400000, prune = pruneFinishedPipelineJobs, setIntervalFn = setInterval, clearIntervalFn = clearInterval, log = console } = {}) -> () => void`
  - `sweepStaleJestDirs(dir, { now = Date.now(), maxAgeMs = 86400000 } = {}) -> string[]` (removed names, sorted)

- [ ] **Step 1: Write the failing tests.**

```js
// pipeline-job-retention.test.js (better-sqlite3 DB in fs.mkdtempSync(path.join(os.tmpdir(), 'dt-retention-')); pipeline_jobs DDL as app.js:847-858)
test('prunes finished jobs older than 30 days and nothing else', () => {
  // rows (status, finished_at, created_at):
  //   completed  -31 d / -40 d   -> deleted
  //   failed     -31 d / -40 d   -> deleted
  //   cancelled  NULL  / -31 d   -> deleted
  //   completed  -29 d / -40 d   -> kept
  //   pending    NULL  / -400 d  -> kept
  //   running    NULL  / -400 d  -> kept (started_at -400 d)
  // datetime('now', '-31 days') etc. in the INSERTs; prune(db) === 3; the remaining statuses are completed, pending, running
})
test('prune without the table returns 0', () => {})
test('retention prunes at once, then daily on an unref()d timer, and never throws', () => {
  // fake setIntervalFn records (fn, ms) and returns { unref() { unrefd = true } }; prune stub returns 2 then throws Error('boom')
  // after start: prune called once, ms === 86400000, unrefd === true, log.log called with '[pipeline-jobs] pruned 2 finished job(s) older than 30 days'
  // fn() -> log.error called with '[pipeline-jobs] retention failed: boom', no throw; stop() -> clearIntervalFn called with the timer
})
// jest-tmp.test.js
test('the sweep removes only dt-jest- entries older than a day', () => {
  // in a mkdtemp dir: dt-jest-run-old, dt-jest-abc (mtime now-2 d via fs.utimesSync), dt-jest-new (fresh), other-old (now-2 d)
  // sweepStaleJestDirs(dir, { now }) deep-equals ['dt-jest-abc', 'dt-jest-run-old']; dt-jest-new and other-old remain
})
test("every suite's os.tmpdir() is inside the run dir", () => {
  expect(os.tmpdir().startsWith(process.env.DT_JEST_TMP_ROOT)).toBe(true)
})
test('setup-env refuses to run without the run dir', () => {
  // spawnSync(process.execPath, ['tests/setup-env.js'], { cwd: backendDir, env: without DT_JEST_TMP_ROOT })
  // status !== 0 and stderr contains 'DT_JEST_TMP_ROOT is unset'
})
```

- [ ] **Step 2: Run them and verify they fail.** Run `cd backend && npm test -- tests/pipeline-job-retention.test.js tests/jest-tmp.test.js`. Expected: FAIL. The modules are missing, `os.tmpdir()` is `/tmp`, and setup-env falls back silently.
- [ ] **Step 3: Implement D6 and D7.** `pruneFinishedPipelineJobs` checks `sqlite_master` for the table first. `startPipelineJobRetention` wraps each run in `try/catch`. `setup-env.js` sets `process.env.TMPDIR = root` before its `mkdtempSync`. `global-setup.js` calls `sweepStaleJestDirs(os.tmpdir())` before creating the run dir.
- [ ] **Step 4: Run the two files and `setup-env-isolation.test.js`, then `npm test -- --maxWorkers=2`.** Expected: all passed. Then `ls -d /tmp/dt-jest-*` prints nothing older than a day. The 29 leftovers from 2026-11-03 are gone after this run.
- [ ] **Step 5: Commit** — `backend: prune finished pipeline jobs after 30 days at start-up and daily; Jest keeps every temp dir under its run dir and sweeps stale ones (BE-46, BE-51)`

---

### Task 6: Frontend: unknown-count confirmation, admin-only assignment control, worker error text, 429 text (FE-55, BE-19 UI, FE-23, BE-40 UI)

**Files:**
- Create: `frontend/src/lib/workerStatus.js`
- Modify: `frontend/src/App.svelte`:
  - `:544` (`canConfigureCurrentCorpusKantropos`);
  - `:2099-2216` (`searchPreviewError` in `initialWorkspaceState` and the `resetFrontendState` destructuring);
  - `:2580` (save guard text);
  - `:3655-3690` (`addTopic`, `removeTopic`, `resetTopicSearch` clear it);
  - `:3712-3741` (`runSearch`);
  - `:3770-3783` (`resetSearchForm`);
  - `:4015`, `:4024` (`workerErrorText`);
  - `:5062-5075` (the unknown-count variant).
- Modify: `frontend/src/components/UpstreamBrowser.svelte:254` (title)
- Test (additions to CI-listed specs, so `ci.yml` is not touched): `frontend/tests/search-scale.spec.ts`, `frontend/tests/upstream-status.spec.ts`, `frontend/tests/session.spec.ts`, `frontend/tests/works-lib.spec.ts`

**Interfaces:**
- Produces: `workerErrorText(details: object | null) -> string` (spec D10).
- Consumes: the 429 text of Task 4. The spec copies it here; there is no runtime dependency.

- [ ] **Step 1: Write the failing tests.**

```ts
// search-scale.spec.ts
test('a failed preview asks before starting, and "Search anyway" starts the run', async ({ page }) => {
  // preview -> 502 {error: 'OpenAlex timed out'}; Max results '0'; Search
  // warning contains 'Could not check how many works match (OpenAlex timed out).' and
  //   'This search has no cap, so it may fetch a very large number of works.'; no POST /api/keyword-search yet
  // click 'Search anyway' -> exactly one POST, body.maxResults === 0
})
test('the unknown-count warning names the cap', async ({ page }) => {
  // default cap 50 -> 'This search stops at 50 works.'
})
test('Enter while the unknown-count warning shows runs the preview again, not the search', async ({ page }) => {
  // preview fails twice; press Enter in the Query field -> preview requests === 2, keyword-search requests === 0, warning still shown
})
test('Reset clears the unknown-count warning', async ({ page }) => {})
// upstream-status.spec.ts
test('a non-admin owner cannot change the Kantropos assignment', async ({ page }) => {
  // me: admin false, corpus role owner -> the 'Upstream target' select is disabled, title 'Only admins can change the Kantropos assignment'
})
test('the admin can change the Kantropos assignment', async ({ page }) => {
  // me: admin true -> the select is enabled
})
// session.spec.ts
test('a locked account shows the 429 text on the sign-in form', async ({ page }) => {
  // /api/auth/login -> 429 {error: 'Too many failed sign-ins for this account. Try again in 15 minutes.'}, header Retry-After 900
  // the sign-in form's .error has exactly that text; the localStorage token is null
})
// works-lib.spec.ts
test('workerErrorText shows the error text, not JSON', () => {
  expect(workerErrorText({ phase: 'download', state: 'direct_batch_timeout', error: 'timed out after 1800s; worker restarted' }))
    .toBe('timed out after 1800s; worker restarted')
  expect(workerErrorText({ message: 'm' })).toBe('m')
  expect(workerErrorText({ state: 'job_timeout' })).toBe('job_timeout')
  expect(workerErrorText({ a: 1 })).toBe('{"a":1}')
  expect(workerErrorText(null)).toBe('{}')
})
```

- [ ] **Step 2: Run them and verify they fail.** Run `cd frontend && E2E_FRONTEND_PORT=5190 npx playwright test tests/search-scale.spec.ts tests/upstream-status.spec.ts tests/session.spec.ts tests/works-lib.spec.ts --workers=1`. Expected: FAIL. The search starts at once, the owner's select is enabled, and `workerErrorText` is missing. The 429 test may already pass; it pins D11.
- [ ] **Step 3: Implement D8 to D10** as the spec states. The unknown-count block reuses `.search-warning` and `.search-warning__actions`. No CSS change.
- [ ] **Step 4: Run the four specs plus the whole CI Playwright list (`ci.yml:54`) with `--workers=1`, then `npx vite build`.** Expected: all pass.
- [ ] **Step 5: Commit** — `frontend: a failed search preview asks before starting; only the admin can change the Kantropos assignment; worker errors show their text (FE-55, BE-19, FE-23, BE-40)`

---

### Task 7: CI runs the whole Python suite (D12)

**Files:**
- Modify: `.github/workflows/ci.yml:56-70` (the `python-search` job)

**Interfaces:** none.

- [ ] **Step 1: Edit the job.**
  - Job id `python-tests`, `name: Python tests`.
  - Keep the `pip install` line (`:67`) unchanged.
  - Replace `:68-70` with a single `- run: python -m pytest tests -q`.
- [ ] **Step 2: Verify the workflow still parses.** Run `/home/jay/DT/venv/bin/python -c "import yaml; d = yaml.safe_load(open('.github/workflows/ci.yml')); print(d['jobs']['python-tests']['steps'][-1])"` (install nothing if `yaml` is missing; fall back to a careful read). Expected: `{'run': 'python -m pytest tests -q'}`.
- [ ] **Step 3: Verify the command locally.** Run `cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests -q`. Expected: all passed. The spec's clean-venv, `env -i`, no-network run on `0702e47` passed 872 tests with CI's pip list. Repeat it only if a task added an import beyond that list.
- [ ] **Step 4: Commit** — `ci: run the whole Python test suite instead of a file list (D12)`

---

## Final verification (controller, after all tasks)

- `cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests -q`: all passed.
- `cd backend && npm test -- --maxWorkers=2`: all passed; `ls -d /tmp/dt-jest-*` lists nothing older than a day.
- `cd frontend && npx vite build`, then the CI Playwright list with `E2E_FRONTEND_PORT=5190 … --workers=1`: all pass.
- `git diff 0702e47 --stat` touches no Caddyfile, no compose file and no schema DDL.
