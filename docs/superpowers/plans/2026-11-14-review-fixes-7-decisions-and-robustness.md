# Review fixes, phase 7: owner decisions, login limit and pipeline robustness — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

Revised 2026-11-22 after the Codex pre-flight (`.superpowers/sdd/2026-11-14-review-fixes-7-decisions-and-robustness/preflight.md`, rulings in `preflight-rulings.md`).

**Goal:** Carry out the owner decisions of 2026-11-14 and the slimmed robustness phase:
- BE-19, BE-46, FE-55 and PY-14;
- a per-username login limit;
- CLI download leases with ownership-checked writes (PY-16) and a hang limit for hung items (K-M4);
- Jest temp dirs kept under the run dir, and the whole Python suite in CI;
- worker errors shown as text (FE-23, error-text half).

**Architecture:** Seven tasks grouped by surface. Tasks on different surfaces touch disjoint files.
1. Python: Tasks 1, 2 and 3. Tasks 2 and 3 share `backend/scripts/daemon/worker.py`, and Task 3 uses Task 2's `owned_claim`, so Task 3 runs after Task 2.
2. Backend: Tasks 4 and 5.
3. Frontend: Task 6.
4. CI: Task 7.

Two new backend modules: `loginLimiter.js` and `pipelineJobRetention.js`. No new frontend module and no new Jest helper. No schema change.

**Tech Stack:** Python 3.11 (`/home/jay/DT/venv/bin/python`), pytest, sqlite3, click 8.2; Node 20, Express 5, better-sqlite3, Jest + supertest; Svelte 5 (legacy syntax in `App.svelte`) + Vite, Playwright (mocked).

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
  - Task 2 edits `_download_single`, `_periodic_owner_is_dead`, `_release_work_claims` and its start-up caller, and the claim calls in `do_enrich` and `do_download` (new `_claim_lease_seconds`).
  - Task 3 edits `_init_work_state`, `_finish_after_win`, `_job_deadline_reached`, `_direct_batch_deadline_reached`, `_attempt_raw_download`, and the item bodies and terminal writes in `do_enrich` and `do_download`.
  - The tasks run in sequence, never in parallel.
- `dl_lit_project/dl_lit/db_manager.py`: Task 2 only. Task 3 consumes `owned_claim`.

No other file is touched by two tasks. One behavioural coupling crosses tasks: Task 5's `TMPDIR` change moves every backend suite's temp dirs into the run dir, including the files Task 4 adds. Task 4's tests must therefore not depend on `/tmp` paths.

**Integration with phase 6:** phase 6 merges into master first; this branch then rebases onto it. Conflicts are expected in `backend/src/index.js` (phase 6 removes the `seedWarmupTimer` block next to Task 5's call: keep the removal, add Task 5's line after `createApp`) and in `dl_lit_project/tests/test_identifiers.py` plus its fixture (both phases remove entries: keep both removals). After the rebase, the controller runs phase 6's verification and this plan's Final verification.

## Global Constraints

- Work only in `/home/jay/DT/.worktrees/review-fixes-7`, on branch `review-fixes-7`. Do not push.
- Never open `dl_lit_project/data/literature.db`; tests use `tmp_path` / `fs.mkdtempSync` DBs.
- Python tests: `cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests -q` (whole suite) plus the task's files. New test files need no CI edit after Task 7 (CI runs the whole suite).
- Backend tests: `cd backend && npm test`; prefer `npm test -- tests/<file>` while iterating. On this machine, full parallel runs can time out suites that pass alone. Use `npm test -- --maxWorkers=2` for the full run, and compare with a `0702e47` baseline before blaming your change. CI's Python for backend tests has only `requests`.
- Frontend tests: from `frontend/`, `E2E_FRONTEND_PORT=5190 npx playwright test <specs> --workers=1`, mocked specs only. Never use the dev stack on `:5175`, and never run `*.real*.spec.ts`, `pipeline-perf.real-pdf.spec.ts` or the opt-in `upstream-metadata.spec.ts`. `npx vite build` must pass. Every mocked spec that fakes a token calls `mockLogSocket(page)`.
- These strings are copied exactly from the spec:
  - D1: the pass-2 log suffix `<skipped> skipped (not a whole DOI)`
  - D2: `Work <id>: <phase> claim lost before its result was written; result discarded.`
  - D3:
    - `hung past the <deadline:g>s timeout; attempt limit <limit> reached`
    - `Watchdog: <n> hung item(s) failed at the attempt limit, <m> left for a retry after the restart.`
    - `Watchdog: hung items not recorded (<reason>).`
    - the skip reason `reporting budget spent`
  - D4: `Only the global admin can assign a Kantropos corpus`; README: `Kantropos assignment needs the admin`
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
  - D2: CLI claim `limit=1`, lease 900 s, claimant `cli:<host>:<pid>:<6 hex>`; daemon lease `max(900, int(max(job_timeout, direct_batch_timeout)) + 60)` (1860 s by default).
  - D3: limit `RAG_FEEDER_HUNG_ITEM_MAX_ATTEMPTS`, default 3, minimum 1; `HUNG_ITEM_MIN_RUN_FRACTION = 0.5`; one shared busy budget of `WATCHDOG_BUSY_TIMEOUT_MS` (1000 ms) for both reporting writes.
  - D5: `maxFailures = 10`, `windowMs = 900000`, `maxKeys = 10000`.
  - D6: `days = 30`, `intervalMs = 86400000`, `PRUNE_BUSY_TIMEOUT_MS = 250`.
  - D7: no sweep; `DT_JEST_PRIOR_TMPDIR` holds the prior `TMPDIR` (`''` for unset).
- No proxy, `trust proxy` or Caddy change. No new DB column or index (D6's test creates phase 6's index DDL in its own temp DB only).
- Each commit message names the rows it closes and ends with:
  ```
  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01U3c4gfg7jQvvmPdpLNBN5T
  ```
- Secrets stay in `.env`; nothing from it goes into tests, docs or commits.

## Review Focus

1. **A burst of concurrent wrong passwords for one username.** At most 10 password checks may run in a window; the rest answer 429 at once. Test in Task 4 (`concurrent wrong passwords get at most 10 checks`).
2. **Late settlement.** A compare that settles after a rollover, or a success that settles while other attempts are in flight, must touch only its own window and keep the other reservations. Tests in Task 4 (limiter unit tests and the deferred-hasher test).
3. **No enumeration through the 429.** Active, invited and unknown names reach the same 429 after the same failures. Test in Task 4.
4. **A result written after its claim was lost.** CLI or daemon, the late writer must write nothing. Tests in Tasks 2 and 3.
5. **A claimed row that had not started, or a sibling that started late.** Neither may be failed at a win, whatever its attempt count. Test in Task 3 (rows C and E).
6. **Phase 5's exit bound.** From the win to the exit, at most 1 s for `heartbeat_lock` plus 1 s of SQLite busy wait in total. Tests in Task 3.
7. **The CLI dies mid-run.** An exception or Ctrl-C must leave no `in_progress` row claimed by the CLI. Test in Task 2.
8. **Old pending or running jobs.** Retention must never delete them, however old. Test in Task 5.
9. **A confirmation that outlives its search.** Reset, an edit or a late preview answer must never show a warning for a request that is no longer the form's, and a button must start the submitted body. Tests in Task 6.

---

### Task 1: The three scripts use the DB's DOI rule (PY-14)

**Files:**
- Modify: `backend/scripts/export_bundle.py`:
  - `:1-9` (bootstrap and import of `normalize_doi`);
  - `:22-32` (delete `_normalize_doi`);
  - `:128` (`normalized_doi`).
- Modify: `backend/scripts/upstream_update.py`:
  - imports (bootstrap and `normalize_doi`);
  - `:232-243` (replace `_normalize_doi` with `display_doi`);
  - `:307`, `:379`, `:831`.
- Modify: `backend/scripts/backfill_openalex_topics.py`:
  - `:32` (import);
  - `:55-64` (delete `_normalize_doi`);
  - `:234-258` (pass 2 uses `doi_rows`, `is_doi_key`, `doi_filter` and `normalize_doi`).
- Modify: `dl_lit_project/tests/test_identifiers.py:61-66` (three `DOI_MAP` entries out) and `:213-226` (the private-copies test out).
- Modify: `dl_lit_project/tests/fixtures/identifier_cases.json` (remove the `doi` entries `backend.scripts.backfill_openalex_topics.normalize_doi`, `backend.scripts.export_bundle.normalize_doi`, `backend.scripts.upstream_update.normalize_doi`).
- Modify: `dl_lit_project/tests/test_upstream_recovery.py:14-17` and `dl_lit_project/tests/test_upstream_text_scan.py:17-21`: insert `backend/scripts` into `sys.path` (if absent) before `spec_from_file_location`, as `test_identifiers._script` does.
- Test: `dl_lit_project/tests/test_doi_rule_scripts.py` (new)

**Interfaces:**
- Produces:
  - `upstream_update.display_doi(value) -> str`: `normalize_doi(value, case="lower") or ""`.
  - `backfill_openalex_topics.is_doi_key(key: str | None) -> bool`: `bool(key) and identifiers._DOI_IN_TEXT_RE.fullmatch(key) is not None`.
  - `backfill_openalex_topics.doi_rows(conn, *, scope_clause: str, limit_clause: str) -> tuple[list[tuple[int, str]], int]`: pass 2's query unchanged (scope clause, `openalex_id` empty, `doi` non-empty, `topics_fetched IS NULL OR topics_fetched = 0`, `ORDER BY id` plus the limit clause), keyed on `normalize_doi(doi)`; returns the `(work_id, key)` pairs whose key passes `is_doi_key`, and the count skipped.
  - `backfill_openalex_topics.doi_filter(tokens: list[str]) -> str`: `"doi:" + "|".join(t.lower() for t in tokens)`.
  - Each of the three modules exposes `normalize_doi`, which is `dl_lit.identifiers.normalize_doi`.

- [ ] **Step 1: Write the failing tests** in `test_doi_rule_scripts.py`. Load the scripts as `test_identifiers._script` does. All DOIs use registrants of 4 or more digits (`10.1000/…`, `10.5555/…`); the shared rule leaves `10.1/…` whole.

```python
def test_scripts_use_the_db_doi_rule():
    for name in ("export_bundle", "upstream_update", "backfill_openalex_topics"):
        mod = _script(name)
        assert mod.normalize_doi is normalize_doi
        assert not hasattr(mod, "_normalize_doi")
def test_export_uses_the_stored_db_key(tmp_path):
    # DatabaseManager(tmp db); INSERT with doi "https://doi.org/10.1000/Ab.C", normalized_doi = normalize_doi(doi)
    # item = export_bundle.load_items(conn (row_factory=sqlite3.Row), None)[0]
    assert item["normalized_doi"] == "10.1000/AB.C" and item["doi"] == "https://doi.org/10.1000/Ab.C"
def test_export_falls_back_to_the_rule_when_the_key_is_empty(tmp_path):
    # normalized_doi NULL, doi "doi:10.5555/xy" -> item["normalized_doi"] == "10.5555/XY"
def test_export_dedupes_on_the_db_key(tmp_path):
    # two rows, both normalized_doi NULL and no openalex_id:
    #   doi "10.1000/ABC" and doi "Smith 2020, https://doi.org/10.1000/abc" -> len(load_items(...)) == 1
def test_export_still_prefers_the_openalex_key(tmp_path):
    # two rows, same doi "10.1000/ABC", openalex_id W1 and W2 -> len(load_items(...)) == 2
def test_export_bibtex_keeps_the_doi_of_bibtex_entry_json():
    # item with bibtex_entry_json '{"doi": "10.1000/FROM-BIB"}' and doi "10.1000/other"
    entry = export_bundle.to_bibtex_entry(item)
    assert "10.1000/FROM-BIB" in entry and "10.1000/other" not in entry   # precedence guard; passes on 0702e47 too
def test_upstream_bibtex_doi_is_the_lowercase_rule():
    # sqlite3.Row from an in-memory table with the columns render_bibtex_entry reads;
    # doi "Smith 2020, doi 10.1000/ABC, p. 3" -> key, entry = render_bibtex_entry(row, "x.pdf", set())
    assert "doi = {10.1000/abc}" in entry and key == "doi_10_1000_abc"   # 0702e47 writes the whole lowercased text
    assert upstream_update.display_doi("https://doi.org/10.1000/ABC") == "10.1000/abc"
    assert upstream_update.display_doi(None) == ""
def test_backfill_doi_keys_must_be_whole_dois():
    assert backfill.is_doi_key("10.1000/ABC")
    assert not backfill.is_doi_key("10.BOGUS|XYZ") and not backfill.is_doi_key("HTTPS://DOI.ORG/10.1/ABC") and not backfill.is_doi_key(None)
    assert backfill.doi_filter(["10.1000/ABC"]) == "doi:10.1000/abc"
def test_backfill_pass2_sends_only_whole_dois_and_updates_the_row(tmp_path, monkeypatch, capsys):
    # DatabaseManager(tmp db) creates works; rows (all openalex_id NULL, metadata_status 'matched'):
    #   1 doi "https://doi.org/10.1000/ABC"; 2 doi "10.bogus|xyz"; 3 doi "10.1/abc"; 4 doi "10.1000/DEF" with topics_fetched 1
    # monkeypatch backfill.get_openalex_mailto -> ""; backfill.openalex_request_json records params["filter"] and returns
    #   {"results": [{"id": "https://openalex.org/W1", "doi": "https://doi.org/10.1000/abc", "primary_topic": {...}, "topics": []}]}
    # sys.argv = ["backfill", "--db-path", db]; backfill.main()
    assert filters == ["doi:10.1000/abc"]
    # row 1: primary_topic set, topics_fetched == 1; rows 2-4 unchanged
    assert "2 skipped (not a whole DOI)" in capsys.readouterr().err
def test_backfill_pass2_keeps_scope_and_limit(tmp_path, monkeypatch):
    # rows 1 and 2 valid DOIs, matched; row 3 valid DOI, metadata_status 'pending'
    # argv --limit 1 -> filters == ["doi:<row 1's doi>"]; argv --scope all -> row 3's DOI is in the filter
```

In `test_identifiers.py`, remove the three entries from `DOI_MAP` and from the fixture, and delete `test_private_doi_copies_differ_from_every_shared_rule`. `test_fixture_covers_every_former_copy` must still pass, with its count of 25 cases per DOI copy.

- [ ] **Step 2: Run the tests and verify they fail.** Run `cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests/test_doi_rule_scripts.py -q`. Expected: FAIL. The scripts have no `normalize_doi`, the export key is lowercase and comes from the raw `doi`, the embedded-citation DOI is not extracted, `display_doi`, `is_doi_key`, `doi_rows` and `doi_filter` are undefined, and the backfill filter carries the malformed tokens. The two precedence guards (`openalex` key, BibTeX DOI) pass already.
- [ ] **Step 3: Implement D1.**
  - `export_bundle.py` and `upstream_update.py` gain the `_bootstrap.ensure_import_paths(__file__)` preamble that `backfill_openalex_topics.py:24-30` uses, then `from dl_lit.identifiers import normalize_doi`.
  - Fix the two upstream suites' loaders (Files) before running them.
  - `export_bundle`: `"normalized_doi": data.get("normalized_doi") or normalize_doi(data.get("doi"))`. `pick_key` and `to_bibtex_entry` stay as they are.
  - `upstream_update`:
    - `:307` uses `normalize_doi(...)` for the key candidate;
    - `:379` and `:831` use `display_doi(...)`.
  - `backfill`: pass 2 builds its rows with `doi_rows(conn, scope_clause=…, limit_clause=…)`, matches with `key=lambda work: normalize_doi(work.get("doi"))` and filters with `doi_filter`, and adds `<skipped> skipped (not a whole DOI)` to the `Pass 2 (by doi)` log line.
- [ ] **Step 4: Run the suites.** Run `test_doi_rule_scripts.py`, `test_identifiers.py` and `test_backend_helper_scripts.py` together; then `test_upstream_recovery.py` alone and `test_upstream_text_scan.py` alone, each in its own `python -m pytest` process (a fresh interpreter, so a loader that only works after another suite fixed `sys.path` is caught); then the whole suite. Expected: all passed.
- [ ] **Step 5: Commit** — `scripts: export_bundle, upstream_update and backfill_openalex_topics use the DB's DOI rule (PY-14)`

---

### Task 2: The CLI claims rows like the daemon (PY-16)

**Files:**
- Modify: `dl_lit_project/dl_lit/db_manager.py`:
  - add `owned_claim`, `find_downloaded_duplicate` and `release_download_claims` next to `claim_download_batch` (`:2638`);
  - delete `fetch_download_queue` (`:1855`).
- Modify: `dl_lit_project/dl_lit/cli.py:649-736` (`download_pdfs_command`).
- Modify: `backend/scripts/daemon/worker.py`:
  - `:1050` and `:1290` (claims use `self._claim_lease_seconds()`); new `_claim_lease_seconds` next to `_direct_batch_timeout_seconds` (`:301`);
  - `:1199-1278` (`_download_single`: `find_downloaded_duplicate`, and the merge and the three terminal writes inside `owned_claim`);
  - `:634-640` (start-up recovery passes `keep_live_cli=True`) and `:753-783` (`_release_work_claims` gains `keep_live_cli`);
  - `:704` (`_periodic_owner_is_dead` accepts the prefixes `daemon` and `cli`; update its docstring).

  **Shared with Task 3.**
- Modify: `dl_lit_project/tests/test_cli_downloader.py` (the mocks move from `fetch_download_queue`/`check_if_exists` to `claim_download_batch`/`find_downloaded_duplicate`/`release_download_claims`/`owned_claim`).
- Modify: `dl_lit_project/tests/test_download_path.py:198-221` (the same switch in `test_cli_downloaded_count_ignores_the_duplicate_merge_result`).
- Test: `dl_lit_project/tests/test_cli_download_leases.py` (new)

**Interfaces:**
- Produces:
  - `DatabaseManager.owned_claim(phase: str, work_id: int, claimed_by: str)`, a context manager yielding `bool` (spec D2): `BEGIN IMMEDIATE` unless a transaction is open; checks `<status> = 'in_progress' AND <claimed_by> = ?` with the `download_*` columns for `"download"` and the `metadata_*` columns for `"enrich"`; on a failed check rolls back and yields `False`; else yields `True`, commits what is still open on exit, rolls back and re-raises on an exception. Writers inside may commit themselves. Unlike `transaction()`, it raises nothing when the writer has already committed.
  - `DatabaseManager.find_downloaded_duplicate(row: dict, authors) -> tuple[str | None, int | None, str | None]`. It calls `check_if_exists` with the arguments listed in spec D2 and returns the hit only for a `works` row whose `download_status` is `downloaded`.
  - `DatabaseManager.release_download_claims(claimed_by: str) -> int`. It sets every `in_progress` row with that `download_claimed_by` back to `queued`, clears `download_claimed_by`, `download_claimed_at` and `download_lease_expires_at`, commits, and returns the count.
  - `PipelineDaemon._claim_lease_seconds() -> int` (spec D2 formula).
  - `PipelineDaemon._release_work_claims(where_claimed_by: str, params: list, *, keep_live_cli: bool = False) -> int`.
- Consumed by Task 3: `owned_claim`.

- [ ] **Step 1: Write the failing tests** in `test_cli_download_leases.py`. Use real temp DBs with `_insert_work` from `tests.test_download_path`, patch `dl_lit.cli.download_ref` and `dl_lit.cli.BibliographyEnhancer`, and run through `CliRunner().invoke(cli, ["download-pdfs", "--db-path", db, "--download-dir", d])`. "Second connection" means a second `DatabaseManager` (or `sqlite3.connect`) on the same temp file. The fake clock replaces `time.time` with `lambda: real() + offset[0]` via `monkeypatch`.

```python
def test_cli_claims_a_row_before_downloading_it(tmp_path, mocker):
    # queued row; download_ref side effect reads the row from a second connection and records it, then returns (None, None, [])
    assert seen == [("in_progress", True)]   # (download_status, download_claimed_by.startswith("cli:"))
    # afterwards: download_status 'failed', download_claimed_by NULL
def test_the_daemon_cannot_claim_a_row_the_cli_is_downloading(tmp_path, mocker):
    # download_ref side effect: second connection's claim_download_batch(limit=5, corpus_id=None, claimed_by="daemon:h:1:abc", lease_seconds=900) -> recorded
    assert recorded == [[]]            # 0702e47: the row is still 'queued' and the daemon takes it
def test_the_cli_claims_one_row_at_a_time(tmp_path, mocker):
    # three queued rows; download_ref side effect counts rows in_progress with a cli: owner (second connection)
    assert counts == [1, 1, 1]
def test_cli_takes_a_daemon_row_only_after_its_lease_expires(tmp_path, mocker, monkeypatch):
    # row in_progress, download_claimed_by 'daemon:h:1:abc', lease = now + 10
    # first run: "Queue is empty." and download_ref not called; fake clock +11 s: second run claims and downloads it
def test_a_stale_cli_completion_writes_nothing(tmp_path, mocker, monkeypatch):
    # download_ref side effect: fake clock past the CLI's lease; a second connection claims the expired row as 'daemon:h:1:abc';
    # then returns a successful download result
    # row stays in_progress, claimed by 'daemon:h:1:abc'; output contains 'claim lost before its result was written; result discarded.'
def test_a_stale_daemon_completion_writes_nothing(tmp_path, monkeypatch):
    # load_worker; row claimed by daemon.worker_id; download_ref side effect re-points download_claimed_by to 'cli:h:1:abc' (second connection)
    status, data = daemon._download_single(row)
    assert status == "skipped" and data["action"] == "claim_lost"
    # row still in_progress, claimed by 'cli:h:1:abc'
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
@pytest.mark.parametrize("lease_offset,released", [(900, False), (-1, True)])
def test_startup_recovery_keeps_a_live_cli_lease(tmp_path, monkeypatch, lease_offset, released):
    # works row in_progress claimed by 'cli:otherhost:1:abc', download_lease_expires_at = now + lease_offset; new download-role daemon (start-up recovery)
    # released -> 'queued' and claim columns NULL; else unchanged
def test_daemon_leases_outlast_its_watchdog(tmp_path, monkeypatch):
    # defaults: daemon._claim_lease_seconds() == 1860; RAG_FEEDER_PIPELINE_JOB_TIMEOUT=60 and RAG_FEEDER_DIRECT_BATCH_TIMEOUT=60 -> 900
    # spy on claim_download_batch / claim_enrich_batch: do_download and do_enrich pass lease_seconds == daemon._claim_lease_seconds()
@pytest.mark.parametrize("owner,released", [
    ("cli:{host}:{live_pid}:abc", False),   # this host, live pid (os.getpid())
    ("cli:{host}:{dead_pid}:abc", True),    # this host, pid of a reaped child process
    ("cli:otherhost:1:abc", False),
])
def test_periodic_recovery_treats_a_cli_owner_like_a_daemon(tmp_path, monkeypatch, owner, released):
    # works row in_progress claimed by owner, no daemon_heartbeat row for it; download-role daemon; _recover_stale_running_jobs(periodic=True)
```

- [ ] **Step 2: Run the tests and verify they fail.** Run `cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests/test_cli_download_leases.py -q`. Expected: FAIL. The row is not claimed, the daemon can claim the CLI's row, stale completions overwrite the new owner, the CLI merges into an undownloaded work, `find_downloaded_duplicate` is missing, start-up recovery releases a live CLI lease, the daemon's lease is 900 s, and the live CLI owner is released periodically. `test_cli_releases_its_claims_when_it_fails` may pass already (nothing is claimed on `0702e47`); it guards the `finally`.
- [ ] **Step 3: Implement D2.**
  - The CLI builds `cli_id` (spec D2) and loops up to `limit` times over `claim_download_batch(limit=1, …, lease_seconds=900)`, stopping at the first empty claim (keep `'Queue is empty.'` when the first one is empty). It dedupes with `find_downloaded_duplicate`, runs the merge and its downloaded/failed write inside `owned_claim("download", id, cli_id)`, prints the D2 claim-lost line when it yields `False`, and wraps the loop in `try/finally` that calls `release_download_claims(cli_id)` and then `close_connection()`.
  - `_download_single` replaces its inline check with `self.db.find_downloaded_duplicate(row, authors)`, and runs the merge, `move_entry_to_downloaded` and both `mark_download_failed` calls inside `owned_claim("download", qid, self.worker_id)`, all under `db_lock` as today. A `False` logs the D2 line and returns `("skipped", {"queue_id": qid, "action": "claim_lost"})`.
  - `_release_work_claims(..., keep_live_cli=True)` appends `AND NOT ({by_col} LIKE 'cli:%' AND COALESCE({lease_col}, 0) > ?)` with `int(time.time())`; only the start-up call passes it.
- [ ] **Step 4: Run the new file, `test_cli_downloader.py`, `test_download_path.py`, `test_download_claims.py`, `test_worker_watchdog.py` and `test_worker_recovery.py`, then the whole suite.** Expected: all passed.
- [ ] **Step 5: Commit** — `cli: download-pdfs claims one row at a time, writes only while it owns the claim, and merges only into downloaded works; daemon leases outlast its watchdog (PY-16)`

---

### Task 3: A hung item fails after 3 hangs (K-M4) — after Task 2

**Files:**
- Modify: `backend/scripts/daemon/worker.py`. **Shared with Task 2**; start from Task 2's commit.
  - `_init_work_state` (`:277-285`): add `self._running_items = {}`, `self._running_items_lock = threading.Lock()`, `self._items_latched = False` and `self._running_clock = time.monotonic`.
  - New module-level `ItemsLatched(Exception)` and `HUNG_ITEM_MIN_RUN_FRACTION = 0.5`; new methods `_item_running`, `_latch_running_items`, `_hung_item_limit` and `_fail_hung_items`, placed next to `_finish_after_win`.
  - `_job_deadline_reached` (`:503-525`) and `_direct_batch_deadline_reached` (`:527-537`): take the snapshot in the latch section and pass `deadline` and `running` on.
  - `_finish_after_win` (`:539-563`): new signature; hung write first, then the heartbeat, under one busy budget.
  - `do_enrich` (`:1086-1150`): track each submitted `_enrich_single`; run `apply_enriched_metadata`, `mark_metadata_failed` and `reset_enrich_claim` inside `owned_claim("enrich", id, self.worker_id)`.
  - `_attempt_raw_download` (`:988-1040`): track the call with phase `"enrich"` and run `mark_raw_work_downloaded` inside `owned_claim("enrich", …)`.
  - `do_download` (`:1304-1305`): track each submitted `_download_single`.
- Modify: `dl_lit_project/tests/test_worker_watchdog.py:701`, `:720` (the two `_finish_after_win` callers pass `deadline=0.3, running={}, now=0.0`).
- Test: `dl_lit_project/tests/test_hung_items.py` (new)

**Interfaces:**
- Produces (all on `PipelineDaemon` unless noted):
  - `ItemsLatched(Exception)` (module level).
  - `_item_running(phase: str, work_id: int)`, a context manager. Under `_running_items_lock` it raises `ItemsLatched` when `_items_latched` is set, else adds `(phase, work_id) -> self._running_clock()`; it removes the entry in its `finally`.
  - `_latch_running_items() -> dict[tuple[str, int], float]`: under `_running_items_lock`, sets `_items_latched = True` and returns a copy. Called only inside the winner's `_work_lock` section.
  - `_hung_item_limit() -> int`: `env_int("RAG_FEEDER_HUNG_ITEM_MAX_ATTEMPTS", 3, minimum=1)`.
  - `_fail_hung_items(conn, running: dict, deadline: float, now: float) -> tuple[int, int]`, which returns `(failed, left)`. It selects the overdue entries (`now - started >= deadline * HUNG_ITEM_MIN_RUN_FRACTION`), runs one `UPDATE` per phase present (spec D3 SQL) on the caller's connection with the lock already held and the busy timeout already set, commits, and raises `sqlite3.Error`. `left` is `len(running) - failed`. `HUNG_ITEM_COLUMNS` maps `"enrich"` and `"download"` to the column sets.
  - `_finish_after_win(details: dict, message: str, deadline: float, running: dict, now: float) -> None`.
- Consumes: Task 2's `owned_claim`; `claim_download_batch` / `claim_enrich_batch` and start-up recovery as Task 2 left them.

- [ ] **Step 1: Write the failing tests** in `test_hung_items.py`. Use a local `_daemon(monkeypatch, tmp_path, role)` built like `test_worker_watchdog._daemon`, with `daemon._exit = exits.append`, and a fake clock `clock = [0.0]; daemon._running_clock = lambda: clock[0]`.

```python
def test_a_download_is_tracked_only_while_it_runs(tmp_path, monkeypatch):
    # role download; two queued rows; _download_single patched to record set(daemon._running_items) and return ("failed", {...})
    # each recorded set contains ("download", its id); after do_download, daemon._running_items == {}
def test_enrichment_and_its_raw_download_are_tracked(tmp_path, monkeypatch):
    # role enrich; one pending row; _enrich_single returns (None, None, {"status": "api_error"});
    # _attempt_raw_download's inner download patched to record set(daemon._running_items) -> contains ("enrich", id); after: empty
def test_no_item_starts_after_the_latch(tmp_path, monkeypatch):
    daemon._latch_running_items()
    with pytest.raises(ItemsLatched):
        with daemon._item_running("download", 1): pass
    assert daemon._running_items == {}
def test_win_fails_only_overdue_running_items_at_the_limit(tmp_path, monkeypatch):
    # role download; rows (download_status in_progress, claimed by daemon.worker_id unless noted):
    #   A attempt 3, started at clock 0; B attempt 2, started at 0; C attempt 3, claimed, not running;
    #   D attempt 3, started at 0, claimed by 'daemon:x:1:zzz'; E attempt 3, started at clock 0.29 (fresh sibling)
    gen = daemon._begin_work()
    # enter _item_running for A, B, D at clock 0 and E at clock 0.29; clock[0] = 0.3
    daemon._direct_batch_deadline_reached(gen, {"phase": "download"}, 0.3)
    assert exits == [75]
    # A: ('failed', 'hung past the 0.3s timeout; attempt limit 3 reached', claimed_by NULL)
    # B, C and E: ('in_progress', claimed by daemon.worker_id); D unchanged
    # log contains 'Watchdog: 1 hung item(s) failed at the attempt limit, 3 left for a retry after the restart.'
def test_a_late_result_after_the_win_writes_nothing(tmp_path, monkeypatch):
    # role download; row A attempt 3 claimed by daemon.worker_id; download_ref patched: clock[0] = 0.3, then
    # daemon._direct_batch_deadline_reached(gen, {"phase": "download"}, 0.3), then return a successful result
    # with daemon._item_running("download", A) entered at clock 0: status, data = daemon._download_single(row)
    assert data["action"] == "claim_lost"
    # A stays 'failed' with the hung error; download_status is not 'downloaded'
def test_an_item_that_hangs_every_time_fails_on_its_third_claim(tmp_path, monkeypatch):
    # one queued row; for round in 1..3: d = _daemon(...) (start-up recovery releases the previous worker's claim),
    # rows = d.db.claim_download_batch(limit=5, corpus_id=None, claimed_by=d.worker_id, lease_seconds=900);
    # with d._item_running("download", row) entered at fake clock 0, clock = 0.3:
    #     d._direct_batch_deadline_reached(d._begin_work(), {"phase": "download"}, 0.3)
    # after rounds 1 and 2 the next daemon's recovery puts it back to 'queued'; after round 3 it is 'failed'
    # a fourth daemon's claim_download_batch(...) == []
def test_a_job_win_blames_running_items_too(tmp_path, monkeypatch):
    # role enrich; running job row of this worker; pending->in_progress metadata row attempt 3 claimed by this worker,
    # running since clock 0; clock 0.3: _job_deadline_reached(gen, {"job_id": job, ...}, 0.3)
    # -> metadata_status 'failed', metadata_error starts 'hung past the 0.3s timeout'
def test_reporting_shares_one_busy_budget(tmp_path, monkeypatch):
    # heartbeat_conn wrapped in a _ConnProxy-style fake: records every "PRAGMA busy_timeout = N" value; the hung UPDATE
    # sleeps 0.8 s, then raises sqlite3.OperationalError("database is locked")
    # one overdue running row at the limit; win
    # the busy values set for the two writes sum to <= 1000; the connection is not in_transaction afterwards (rolled back);
    # log contains 'Watchdog: hung items not recorded (database is locked).'; exits == [75]; elapsed < 1.5 s;
    # the prior busy_timeout is restored
def test_a_locked_database_keeps_the_phase5_exit_bound(tmp_path, monkeypatch):
    # a second sqlite3 connection holds BEGIN IMMEDIATE on the DB; one overdue running row at the limit; win
    # exits == [75]; elapsed from the call to the exit < 1.6 s (no lock wait, 1 s busy budget in total);
    # the row stays in_progress; the log has the hung-items skip line and the terminal-heartbeat skip line
def test_a_skipped_hung_write_on_lock_timeout_still_exits(tmp_path, monkeypatch):
    # WATCHDOG_LOCK_TIMEOUT_SECONDS patched to 0.05; another thread holds daemon.heartbeat_lock;
    # win -> log contains 'Watchdog: hung items not recorded (' ; exits == [75]; the running row stays in_progress
def test_hung_item_limit_reads_env_with_minimum(tmp_path, monkeypatch):
    # unset -> 3; "0" -> 1; "5" -> 5
```

- [ ] **Step 2: Run the tests and verify they fail.** Run `cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests/test_hung_items.py -q`. Expected: FAIL. `_item_running`, `_latch_running_items` and `ItemsLatched` are undefined, nothing is failed at the win, and a late result overwrites.
- [ ] **Step 3: Implement D3.**
  - The winners call `running = self._latch_running_items()` in the `_work_lock` section that sets `_terminal` (`_job_deadline_reached` only on `outcome is True`), read `now = self._running_clock()` there, and pass `deadline`, `running` and `now` to `_finish_after_win`.
  - `_finish_after_win` keeps the test seam first and the exit last. Inside the acquired `heartbeat_lock`: read the prior `busy_timeout`, set `budget_end = time.monotonic() + WATCHDOG_BUSY_TIMEOUT_MS / 1000`; for each of the two writes (hung items first, then the heartbeat), set `busy_timeout` to the milliseconds left, or skip with `reporting budget spent` when less than 1 ms is left; restore the prior value in a `finally`. The hung write runs in its own `try`: on `sqlite3.Error` it rolls back (`if conn.in_transaction`) and logs the D3 skip line. The lock-timeout path logs the D3 skip line too, then the existing heartbeat skip line.
  - In `do_download`, the submitted callable is a small wrapper that enters `_item_running("download", id)` around `_download_single(row)`. In `do_enrich`, the same for `_enrich_single`; `_attempt_raw_download` enters it itself.
  - The enrich terminal writes go inside `owned_claim("enrich", id, self.worker_id)`; a `False` logs the D2 claim-lost line and skips the result.
- [ ] **Step 4: Run the new file, `test_worker_watchdog.py`, `test_pipeline_daemon.py`, `test_worker_recovery.py` and `test_cli_download_leases.py`, then the whole suite.** Expected: all passed. The watchdog tests' bounds still hold.
- [ ] **Step 5: Commit** — `worker: a watchdog win fails the items that ran past half the deadline once they reach the attempt limit, within phase 5's exit bound; late results write nothing (K-M4)`

---

### Task 4: Admin-only Kantropos assignment and the per-username login limit (BE-19, BE-40)

**Files:**
- Create: `backend/src/loginLimiter.js`
- Modify: `backend/src/app.js`:
  - `:2573` (`createApp({ broadcast, broadcastEvent, mailTransport, loginLimiter } = {})`);
  - `:3123-3153` (login handler);
  - `:3935-3949` (GET `can_edit`; `requireKantroposAssigner` replaces `requireCorpusConfigurer`).
- Modify: `README.md:30` (role text, spec D4).
- Modify tests: `backend/tests/access-routes.test.js`:
  - `:129-133` (`beforeAll` assigns as admin);
  - `:228-257` (renamed `kantropos-assignment: GET member, PUT admin only`).
- Modify tests: `backend/tests/corpus-sharing.test.js:218-223` (the editor's PUT now answers 403).
- Modify tests: `backend/tests/auth.test.js`: the hasher mock gains `hasherState.compareGate` (a promise `compare` awaits when set); two tests in the hasher-mock block.
- Test: `backend/tests/login-limit.test.js` (new)

**Interfaces:**
- Produces:
  - `createLoginLimiter({ maxFailures = 10, windowMs = 900000, maxKeys = 10000, now = () => Date.now() } = {}) -> { reserve(key) -> { allowed: true, ticket: { key, windowId } | null } | { allowed: false, retryAfterSeconds: number }, fail(ticket) -> void, refund(ticket) -> void, succeed(ticket) -> void }` (semantics in spec D5).
  - the `createApp` option `loginLimiter`
- The 429 body and header (spec D5) are consumed by Task 6's mocked spec, which copies the text.

- [ ] **Step 1: Write the failing tests.**

```js
// login-limit.test.js — unit, clock t
test('the 11th reservation in a window is refused with the time left', () => {
  // t=0; 10 × (reserve('u') -> allowed, fail(ticket)); reserve('u') -> { allowed: false, retryAfterSeconds: 900 }
  // t=899_500 -> retryAfterSeconds 1; t=900_000 -> allowed (a new window)
})
test('reservations in flight count against the limit', () => {
  // 10 × reserve('u') without settling -> all allowed; the 11th -> refused
})
test('a settlement after a rollover leaves the new window alone', () => {
  // t=0 reserve A; t=900_000 reserve B (new window); fail(A)
  // with B still in flight, exactly 9 further reserves are allowed (8 if A had leaked into the new window)
})
test('success clears settled failures but keeps reservations in flight', () => {
  // 5 × (reserve, fail); reserve A, reserve B (in flight); reserve C; succeed(C); fail(A); fail(B)
  // -> exactly 8 further reserves allowed, then refused
})
test('refund releases the slot without counting a failure', () => {})
test('a full map never evicts a locked entry and fails open when all are locked', () => {
  // maxKeys 2: lock 'a' (10 fails); 'b' 1 fail; reserve('c') evicts 'b' (oldest unlocked) -> 'a' still refused
  // lock 'c' too; reserve('d') -> { allowed: true, ticket: null }; fail(null) is a no-op; 'a' and 'c' still refused
  // an expired entry is deleted before any unlocked one
})
// login-limit.test.js — routes. Set RAG_FEEDER_JWT_SECRET, RAG_ADMIN_USER, RAG_ADMIN_PASSWORD and a temp
// RAG_FEEDER_DB_PATH, then dynamic-import app.js; createApp({ broadcast() {}, loginLimiter: createLoginLimiter({ now: () => clock }) });
// user 'b' active and user 'c' invited, inserted with bcryptjs.hashSync('pw-b', 4).
test('10 failed sign-ins lock the username; the 11th attempt answers 429 with Retry-After', async () => {
  // 10 × wrong -> 401; 11th -> 429, headers['retry-after'] === '900',
  // body.error === 'Too many failed sign-ins for this account. Try again in 15 minutes.'
})
test('a locked username refuses the right password until the window passes', async () => {
  // right -> 429; clock += 899_000 -> 429, retry-after '1', 'Try again in 1 minute.'; clock += 1_000 -> right -> 200
})
test('a successful sign-in clears the count', async () => {
  // 9 wrong, right (200), 9 wrong -> no 429
})
test('concurrent wrong passwords get at most 10 checks', async () => {
  // Promise.all(30 × wrong) -> exactly 10 × 401 and 20 × 429
})
test('active, invited and unknown names get the same 429 after 10 failures', async () => {
  // for 'b' (wrong password, 401), 'c' (invited, 403) and 'nobody' (401): 10 attempts, then the 11th
  // -> three 429s with equal body.error and equal retry-after; ' b ' counts against 'b' (trimmed key)
})
test('other usernames are not affected', async () => {
  // lock 'b'; admin right -> 200
})
// auth.test.js additions (hasher mock block)
test('a busy hasher does not count against the username', async () => {
  // hasherState.busy = true; 11 × login -> all 503; busy = false; wrong -> 401 (not 429); right -> 200
})
test('a late success clears settled failures but keeps in-flight reservations', async () => {
  // 3 wrong (401); set compareGate; start 2 wrong and 1 right (held at the gate); release the right one first -> 200;
  // release the gate -> the 2 wrong answer 401; then 8 wrong -> 401; the next -> 429
})
// access-routes.test.js
test('kantropos-assignment: GET member, PUT admin only', async () => {
  // viewer3, editor4, owner1 PUT {targetId} -> 403, body.error 'Only the global admin can assign a Kantropos corpus'
  // owner1 PUT {targetId: null} -> 403 with the same text, and the assignment row is still there
  // outsider5 PUT -> 403 'No access to this corpus'
  // admin PUT -> 200; GET can_edit: owner1 false, editor4 false, admin true
})
// corpus-sharing.test.js: the editor's PUT expects 403 with the admin-only text
```

- [ ] **Step 2: Run them and verify they fail.** Run `cd backend && npm test -- tests/login-limit.test.js tests/access-routes.test.js tests/corpus-sharing.test.js tests/auth.test.js`. Expected: FAIL. There is no `loginLimiter.js`, no 429 is answered, and the editor and owner PUTs answer 200.
- [ ] **Step 3: Implement D4 and D5.**
  - Login: after the 400 check, `const key = String(username).trim()` and `const gate = loginLimiter.reserve(key)`, **before** the user lookup. When it is not allowed, set `Retry-After` and answer 429.
  - Invited (403) and unknown (401) call `fail(gate.ticket)` before answering.
  - The compare's `catch` calls `refund(gate.ticket)` before it answers 503 or 500; `!valid` calls `fail`; success calls `succeed`.
  - D4: `requireKantroposAssigner` (spec D4), GET `can_edit: isAdminUser(req.user, authConfig)`, README line.
- [ ] **Step 4: Run the four files, then `npm test -- --maxWorkers=2`.** Expected: all passed. From `backend/`, `grep -n "requireCorpusConfigurer" src/app.js` prints nothing.
- [ ] **Step 5: Commit** — `backend: only the admin assigns a Kantropos corpus; 10 failed sign-ins per username in 15 minutes answer 429 until the window passes (BE-19, BE-40)`

---

### Task 5: Pipeline job retention and Jest temp dirs (BE-46, BE-51)

**Files:**
- Create: `backend/src/pipelineJobRetention.js`
- Modify: `backend/src/index.js:101` (call `startPipelineJobRetention(app.authDb)` right after `createApp`)
- Modify: `backend/tests/global-setup.js` (record the prior `TMPDIR`, point `TMPDIR` at the run dir), `backend/tests/global-teardown.js` (restore `TMPDIR`, then remove the run dir) and `backend/tests/setup-env.js:12-14` (throw instead of the fallback)
- Test: `backend/tests/pipeline-job-retention.test.js` (new), `backend/tests/jest-tmp-dir.test.js` (new); extend `backend/tests/setup-env-isolation.test.js`

**Interfaces:**
- Produces:
  - `pruneFinishedPipelineJobs(db, { days = 30 } = {}) -> number`
  - `PRUNE_STATEMENTS: readonly string[]` (the two `DELETE`s of spec D6, exported so the plan test checks the production SQL)
  - `PRUNE_BUSY_TIMEOUT_MS = 250`
  - `startPipelineJobRetention(db, { intervalMs = 86400000 } = {}) -> () => void`

- [ ] **Step 1: Write the failing tests.**

```js
// pipeline-job-retention.test.js (better-sqlite3 DB in fs.mkdtempSync(path.join(os.tmpdir(), 'dt-retention-'));
// pipeline_jobs DDL as app.js:847-858 plus phase 6's
// 'CREATE INDEX IF NOT EXISTS idx_pipeline_jobs_status_finished ON pipeline_jobs(status, finished_at)')
test('prunes finished jobs older than 30 days and nothing else', () => {
  // rows (status, finished_at, created_at):
  //   completed  -31 d / -40 d   -> deleted
  //   failed     -31 d / -40 d   -> deleted
  //   cancelled  NULL  / -31 d   -> deleted (legacy fallback)
  //   cancelled  NULL  / -29 d   -> kept
  //   completed  -29 d / -40 d   -> kept
  //   pending    NULL  / -400 d  -> kept
  //   running    NULL  / -400 d  -> kept (started_at -400 d)
  // datetime('now', '-31 days') etc. in the INSERTs; prune(db) === 3
})
test('both prune statements search the (status, finished_at) index', () => {
  // for each sql of PRUNE_STATEMENTS: db.prepare('EXPLAIN QUERY PLAN ' + sql).all('-30 days'[, '-30 days'])
  // detail matches /USING INDEX idx_pipeline_jobs_status_finished \(status=\? AND finished_at[<=]\?\)/
})
test('prune without the table returns 0', () => {})
test('a locked database fails the prune within its busy budget and restores busy_timeout', () => {
  // db.pragma('busy_timeout = 15000'); a second connection runs BEGIN IMMEDIATE
  // expect(() => pruneFinishedPipelineJobs(db)).toThrow(/locked|busy/i); elapsed < 2000 ms;
  // db.pragma('busy_timeout', { simple: true }) === 15000; all rows still there
})
test('retention prunes at once, then daily, logs, never throws, and stops', () => {
  // jest.useFakeTimers(); spy console.log / console.error; one old completed job
  // stop = startPipelineJobRetention(db) -> console.log called with '[pipeline-jobs] pruned 1 finished job(s) older than 30 days'
  // insert another old job; jest.advanceTimersByTime(86_400_000) -> pruned 1 again
  // db.close(); advance a day -> console.error called with a string starting '[pipeline-jobs] retention failed: '; no throw
  // stop(); advance a day -> no further calls
})
// jest-tmp-dir.test.js
test("every suite's os.tmpdir() is exactly the run dir", () => {
  expect(os.tmpdir()).toBe(process.env.DT_JEST_TMP_ROOT)
})
test('global setup points TMPDIR at its run dir and teardown restores it', () => {
  // spawnSync(process.execPath, ['--input-type=module', '-e', <imports tests/global-setup.js and tests/global-teardown.js,
  //   runs both, prints JSON { root, during, rootExisted, after, rootExistsAfter }>], { cwd: backendDir, env: { ...env, TMPDIR: base } })
  // root is inside base; during === root; rootExisted; after === base; !rootExistsAfter
  // with TMPDIR deleted from env: after is undefined
})
test('setup-env refuses to run without the run dir', () => {
  // spawnSync(process.execPath, ['tests/setup-env.js'], { cwd: backendDir, env: without DT_JEST_TMP_ROOT })
  // status !== 0 and stderr contains 'DT_JEST_TMP_ROOT is unset'
})
// setup-env-isolation.test.js, existing test extended (nested Jest):
//   before the spawn, list the dt-jest-run-* entries of os.tmpdir() (the parent run dir); after it, the list is the same
//   (the child made its run dir there and removed it)
```

- [ ] **Step 2: Run them and verify they fail.** Run `cd backend && npm test -- tests/pipeline-job-retention.test.js tests/jest-tmp-dir.test.js tests/setup-env-isolation.test.js`. Expected: FAIL. The module is missing, `os.tmpdir()` is `/tmp`, and setup-env falls back silently. The nested-Jest assertion may pass already; it guards the inheritance.
- [ ] **Step 3: Implement D6 and D7.**
  - `pruneFinishedPipelineJobs` checks `sqlite_master` for the table, reads the prior `busy_timeout`, sets `PRUNE_BUSY_TIMEOUT_MS`, runs `PRUNE_STATEMENTS` in one `db.transaction`, and restores the prior value in a `finally`. `startPipelineJobRetention` runs at once, then on `setInterval(…).unref()`, and wraps each run in `try/catch` (spec D6 log lines).
  - `global-setup.js`: after `mkdtempSync`, `process.env.DT_JEST_PRIOR_TMPDIR = process.env.TMPDIR ?? ''` and `process.env.TMPDIR = root`. `global-teardown.js`: restore (delete `TMPDIR` when the prior value is `''`), then remove the run dir. `setup-env.js` throws the D7 text when `DT_JEST_TMP_ROOT` is unset and keeps the DB-path override.
- [ ] **Step 4: Run the three files, then `npm test -- --maxWorkers=2`.** Expected: all passed. List `/tmp` before and after the full run: no new entry.
- [ ] **Step 5: Commit** — `backend: prune finished pipeline jobs after 30 days at start-up and daily with a short lock wait; Jest keeps every temp dir under its run dir (BE-46, BE-51)`

---

### Task 6: Frontend: unknown-count confirmation, admin-only assignment control, worker error text, 429 text (FE-55, BE-19 UI, FE-23, BE-40 UI)

**Files:**
- Modify: `frontend/src/App.svelte`:
  - `:544` (`canConfigureCurrentCorpusKantropos`);
  - `:2099-2216` (`searchPreviewError`, `searchRequestSeq` and `submittedSearch` in `initialWorkspaceState` and the `resetFrontendState` destructuring);
  - `:2580` (save guard text);
  - `:3655-3690` (`addTopic`, `removeTopic`, `resetTopicSearch` call `invalidateSearchConfirmation`);
  - `:3712-3741` (`runSearch`), `:3742-3768` (`startSearch` takes `body`), new `invalidateSearchConfirmation`;
  - `:3770-3783` (`resetSearchForm`);
  - local `workerErrorText` next to `loadPipelineWorkerStatus`; `:4015`, `:4024` use it;
  - `:4971` (`on:input={invalidateSearchConfirmation}` on the search form);
  - `:5062-5075` (both warning variants start `submittedSearch.body`; the unknown-count variant).
- Modify: `frontend/src/components/UpstreamBrowser.svelte:254` (title)
- Modify: `frontend/tests/upstream-metadata.spec.ts:41-44` (opt-in, not run): the login assertion messages and a header comment say the credentials must be the global admin's, because the assignment PUT is admin-only.
- Test (additions to CI-listed specs, so `ci.yml` is not touched): `frontend/tests/search-scale.spec.ts`, `frontend/tests/upstream-status.spec.ts`, `frontend/tests/session.spec.ts`, `frontend/tests/tab-resources.spec.ts`

**Interfaces:**
- Produces: nothing exported. `workerErrorText(details)` is local to `App.svelte` (spec D10).
- Consumes: the 429 text of Task 4. The spec copies it here; there is no runtime dependency.

- [ ] **Step 1: Write the failing tests.**

```ts
// search-scale.spec.ts
test('a failed preview asks before starting, and "Search anyway" starts the submitted search', async ({ page }) => {
  // preview -> 502 {error: 'OpenAlex timed out'}; Query 'climate'; Max results '0'; Search
  // warning contains 'Could not check how many works match (OpenAlex timed out).' and
  //   'This search has no cap, so it may fetch a very large number of works.'; no POST /api/keyword-search yet
  // click 'Search anyway' -> exactly one POST, body.maxResults === 0 and body.query === 'climate'
})
test('the unknown-count warning names the cap', async ({ page }) => {
  // default cap 50 -> 'This search stops at 50 works.'
})
test('Enter while the unknown-count warning shows runs the preview again, not the search', async ({ page }) => {
  // preview fails twice; press Enter in the Query field -> preview requests === 2, keyword-search requests === 0, warning still shown
})
test('Reset clears the unknown-count warning', async ({ page }) => {})
test('a preview answering after Reset shows nothing and starts nothing', async ({ page }) => {
  // the preview route waits on a promise; Search; click Reset; release it with 502 (and, in a second case, with a count above the threshold)
  // -> no search-warning, no POST /api/keyword-search
})
test('an edit withdraws the warning, and a late preview for the edited form is dropped', async ({ page }) => {
  // (a) failed preview shows the warning; type into Query -> warning gone; no POST
  // (b) held preview; Search; type into Query; release with 502 -> no warning; no POST
})
for (const status of [400, 403]) {
  test(`a ${status} preview shows its message and starts nothing`, async ({ page }) => {
    // preview -> status {error: 'denied or invalid'} -> the search status shows 'denied or invalid'; no search-warning; no POST
  })
}
// upstream-status.spec.ts
test('a non-admin owner cannot change the Kantropos assignment', async ({ page }) => {
  // me: admin false, corpus role owner -> the 'Upstream target' select is disabled, title 'Only admins can change the Kantropos assignment'
})
test('the admin can change the Kantropos assignment', async ({ page }) => {
  // me: admin true -> the select is enabled
})
// session.spec.ts — compatibility test, expected to pass on 0702e47 (D11)
test('a locked account shows the 429 text on the sign-in form', async ({ page }) => {
  // /api/auth/login -> 429 {error: 'Too many failed sign-ins for this account. Try again in 15 minutes.'}, header Retry-After 900
  // the sign-in form's .error has exactly that text; the localStorage token is null
})
// tab-resources.spec.ts
test('an error heartbeat shows its error text on the download worker card', async ({ page }) => {
  // admin; state.workers.download = { online: false, status: 'error',
  //   details: { phase: 'download', state: 'direct_batch_timeout', error: 'timed out after 1800s; worker restarted' } }
  // open the Downloads tab -> the download worker card's .error has text 'timed out after 1800s; worker restarted'
  //   and the card contains no '{"phase"'
})
```

- [ ] **Step 2: Run them and verify they fail.** Run `cd frontend && E2E_FRONTEND_PORT=5190 npx playwright test tests/search-scale.spec.ts tests/upstream-status.spec.ts tests/session.spec.ts tests/tab-resources.spec.ts --workers=1`. Expected: FAIL. The search starts at once, a late preview still acts, the owner's select is enabled, and the card shows JSON. The 429 test passes already; it pins D11.
- [ ] **Step 3: Implement D8 to D10** as the spec states. The unknown-count block reuses `.search-warning` and `.search-warning__actions`. No CSS change. Edit the opt-in metadata spec's messages (Files).
- [ ] **Step 4: Run the four specs plus the whole CI Playwright list (`ci.yml:54`) with `--workers=1`, then `npx vite build`.** Expected: all pass.
- [ ] **Step 5: Commit** — `frontend: a failed search preview asks before starting and every confirmation starts the submitted search; only the admin can change the Kantropos assignment; worker errors show their text (FE-55, BE-19, FE-23, BE-40)`

---

### Task 7: CI runs the whole Python suite (D12)

**Files:**
- Modify: `.github/workflows/ci.yml:68-70` (the `python-search` job's test steps)

**Interfaces:** none.

- [ ] **Step 1: Edit the job.**
  - Keep the job id `python-search` and `name: Python search and schema tests` (the check name stays; `master` has no branch protection, and nothing else refers to it).
  - Keep the `pip install` line (`:67`) unchanged.
  - Replace `:68-70` with a single `- run: python -m pytest tests -q`.
- [ ] **Step 2: Verify the workflow still parses.** Run `/home/jay/DT/venv/bin/python -c "import yaml; d = yaml.safe_load(open('.github/workflows/ci.yml')); print(d['jobs']['python-search']['steps'][-1])"` (install nothing if `yaml` is missing; fall back to a careful read). Expected: `{'run': 'python -m pytest tests -q'}`.
- [ ] **Step 3: Verify the command locally.** Run `cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests -q`. Expected: all passed. The spec's clean-venv, `env -i`, no-network run on `0702e47` passed 872 tests with CI's pip list. Repeat it only if a task added an import beyond that list.
- [ ] **Step 4: Commit** — `ci: run the whole Python test suite instead of a file list (D12)`

---

## Final verification (controller, after all tasks and after the rebase onto phase 6)

- `cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests -q`: all passed; `test_upstream_recovery.py` and `test_upstream_text_scan.py` also pass each in its own process.
- `cd backend && npm test -- --maxWorkers=2`: all passed; `/tmp` has no new entry afterwards (list before and after).
- From the repo root: `git grep -n requireCorpusConfigurer -- backend/src/app.js` prints nothing.
- `cd frontend && npx vite build`, then the CI Playwright list with `E2E_FRONTEND_PORT=5190 … --workers=1`: all pass.
- Phase 6's plan's verification, rerun on the rebased branch.
- `git diff 0702e47 --stat` touches no Caddyfile, no compose file and no schema DDL beyond phase 6's.
- After merge: the one-time manual cleanup and the disposable-DB smoke checks of the spec's Acceptance. Never the dev stack, which uses `literature.db`.
