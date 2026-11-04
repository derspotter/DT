# Review fixes, phase 4: Python DRY and pipeline correctness — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** One env parser, one identifier module, one LLM timeout helper, one download-and-mark path, batched ingest with correct corpus linking, keep-alive sessions, and the `db_manager`/worker hygiene the review listed — with the three correctness bugs fixed (import links duplicates, Gemini timeouts retry, downloads keep the abstract).

**Architecture:** Shared helpers live in `dl_lit_project/dl_lit/` (`utils.py`, new `identifiers.py`, new `llm_common.py`); `backend/scripts/*.py` and `backend/scripts/daemon/worker.py` import them (they already import `dl_lit`). Every change is pinned by a pytest in `dl_lit_project/tests/` using a temp SQLite file or fakes; nothing touches the live DB.

**Tech Stack:** Python 3.11 (`/home/jay/DT/venv/bin/python`), pytest, sqlite3, requests.

**Spec:** `docs/superpowers/specs/2026-11-03-review-fixes-4-python-dry-design.md`

## Global Constraints

- The DB dedupe keys do not change: `normalized_doi`, `openalex_id`, `normalized_title`, `normalized_authors` are produced byte-for-byte as before at every call site (pinned by Task 2's fixture test).
- No behaviour change for users beyond the spec's bugs (import links duplicates to the corpus; Gemini timeouts actually retry; downloads keep the abstract; CLI counter; env typos no longer crash).
- Tests: `cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests -q` must stay green (259 passed at the start); each task adds its own test file or extends the named one. Never open `dl_lit_project/data/literature.db`; tests use `tmp_path`.
- Run every task's tests from the worktree root `/home/jay/DT/.worktrees/review-fixes-4` (the venv is `/home/jay/DT/venv`).
- Backend JS is not modified; `.github/workflows/ci.yml` only in Task 7.
- Secrets stay in `.env`; nothing from it goes into tests, docs or commits.
- Commit per task with the session's attribution trailers.

## Review Focus

1. A non-numeric env value at daemon startup (`RAG_FEEDER_ENRICH_WAVE_SIZE=two`) must warn and use the default, not crash — Task 1 test.
2. A DOI in `https://doi.org/10.1000/ABC` form must produce the same `normalized_doi` as before in `db_manager` and the same lowercase form in the scripts that lowercased — Task 2 fixture test.
3. A Gemini call that hangs past the timeout must not delay the retry by the hang's duration — Task 3 elapsed-time test.
4. Importing a .bib whose entries already exist in another corpus must link every one to the target corpus and report `corpus_linked` — Task 4 test.
5. A download whose result carries `checksum` must not re-read the file; a result without one must hash it chunked — Task 5 test.

---

### Task 1: `utils.env_int` / `env_float` / `env_bool`

**Files:**
- Modify: `dl_lit_project/dl_lit/utils.py:173-197` (`_env_int`, `_env_float`, `_env_str`)
- Modify: `dl_lit_project/dl_lit/APIscraper_v2.py:57-75`, `dl_lit_project/dl_lit/get_bib_pages.py:60-78` (delete local copies, import), `backend/scripts/reference_expansion.py:6-21`
- Modify: `backend/scripts/daemon/worker.py` (lines 206, 212, 711, 919, 937, 1005, 1006, 1012: `int(os.getenv(...))`), `dl_lit_project/dl_lit/new_dl.py:117-125` (five `int(os.getenv(...))`)
- Test: `dl_lit_project/tests/test_env_parsing.py` (new)

**Interfaces:**
- Produces: `env_int(name: str, default: int, *, minimum: int | None = None, maximum: int | None = None) -> int`, `env_float(name, default, *, minimum=None, maximum=None) -> float`, `env_bool(name: str, default: bool = False) -> bool` (truthy set `{"1","true","yes","on"}`, case-insensitive, stripped). Invalid values: one `warnings.warn`/stderr line `"[env] <NAME>=<value> is not a valid <int|float>; using <default>"` and the default. The old private names stay as aliases for one release (`_env_int = env_int`).

- [ ] **Step 1: Write the failing test**

```python
def test_env_int_invalid_uses_default_and_warns(monkeypatch, capsys):
    monkeypatch.setenv("X_INT", "two")
    assert env_int("X_INT", 2, minimum=1) == 2
    assert "X_INT=two" in capsys.readouterr().err
def test_env_int_clamps(monkeypatch):
    monkeypatch.setenv("X_INT", "0"); assert env_int("X_INT", 2, minimum=1) == 1
    monkeypatch.setenv("X_INT", "99"); assert env_int("X_INT", 2, maximum=10) == 10
def test_env_bool(monkeypatch):
    for v, want in [("1", True), ("YES", True), (" on ", True), ("0", False), ("", False), ("maybe", False)]:
        monkeypatch.setenv("X_B", v); assert env_bool("X_B") is want
    monkeypatch.delenv("X_B"); assert env_bool("X_B", True) is True
def test_daemon_init_survives_env_typo(monkeypatch, tmp_path):
    monkeypatch.setenv("RAG_FEEDER_DAEMON_HEARTBEAT_SECONDS", "fast")
    # construct PipelineDaemon(str(tmp_path / "t.db"), role="enrich") as tests/test_pipeline_daemon.py does; assert heartbeat_interval_seconds == 3
```

- [ ] **Step 2: Run it to verify it fails** — `cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests/test_env_parsing.py -q` → ImportError / ValueError.
- [ ] **Step 3: Implement** the three functions in `utils.py`; replace every copy and every inline `int(os.getenv(...))` (grep `os.getenv` in the listed files; keep string envs as they are).
- [ ] **Step 4: Run the whole suite** — `/home/jay/DT/venv/bin/python -m pytest tests -q` → all passed. `grep -rn "int(os.getenv" backend/scripts dl_lit_project/dl_lit` returns nothing.
- [ ] **Step 5: Commit** — `utils: public env_int/env_float/env_bool; invalid values warn and fall back instead of crashing the daemon`

---

### Task 2: `dl_lit/identifiers.py`

**Files:**
- Create: `dl_lit_project/dl_lit/identifiers.py`
- Modify: `dl_lit_project/dl_lit/db_manager.py:676-708` (`_normalize_doi`, `_normalize_openalex_id` delegate), `:2041` and `:2966` (`_convert_inverted_index_to_text` — one delegating method, delete the shadowing second), `dl_lit_project/dl_lit/OpenAlexScraper.py:54` (`convert_inverted_index_to_text`), `:837` (`normalize_doi`), `:924` (`get_abstract`), `dl_lit_project/dl_lit/new_dl.py:2240`, `dl_lit_project/dl_lit/keyword_search.py:364` (`openalex_result_to_record`), `backend/scripts/keyword_search.py:79-99` and `:178` (`normalize_openalex_id`, `normalize_doi`, `openalex_result_to_record`), `backend/scripts/{backfill_openalex_topics.py:61, ingest_latest.py:43, migrate_to_works.py:19, export_bundle.py:22, upstream_update.py:234, graph_3d_export.py:144}` (`normalize_doi` copies)
- Test: `dl_lit_project/tests/test_identifiers.py` (new)

**Interfaces:**
- Produces: `normalize_doi(value, *, case: str = "upper") -> str | None`, `normalize_openalex_id(value) -> str | None`, `inverted_index_to_text(idx) -> str | None`, `openalex_work_to_record(item: dict, *, run_id=None) -> dict`.
- Rule: before deleting a copy, the implementer records its behaviour on the fixture list (`tests/fixtures/identifier_cases.json`: 25 DOI inputs incl. `https://doi.org/`, `doi:`, mixed case, whitespace, URL-encoded, `None`, `""`; 12 OpenAlex ids incl. `https://openalex.org/W123`, `w123`, `W123`, `None`) by running the OLD function and saving the outputs; the test asserts the new function with the right `case` reproduces every saved output per former call site. Where two former copies disagree on the same input, the call site keeps its own former output via the `case` argument; if a difference is not expressible by `case`, STOP and report (do not silently pick one).

- [ ] **Step 1: Generate the fixture from the old copies** (a throwaway script in the report; the JSON is committed).
- [ ] **Step 2: Write the failing test** (`test_identifiers.py` loads the fixture and asserts per call site; plus `inverted_index_to_text({"a":[0],"b":[1]}) == "a b"` and the record mapping equality against `dl_lit/keyword_search.openalex_result_to_record` on a sample item).
- [ ] **Step 3: Run it** → ImportError.
- [ ] **Step 4: Implement the module and switch every call site** (thin aliases allowed where a module re-exports the name for its own tests).
- [ ] **Step 5: Whole suite green; `grep -rn "def normalize_doi\|def _normalize_doi\|def convert_inverted_index_to_text\|def openalex_result_to_record" dl_lit_project/dl_lit backend/scripts`** lists only `identifiers.py` (and delegating aliases).
- [ ] **Step 6: Commit** — `identifiers.py: one DOI/OpenAlex-id normaliser, one inverted-index converter, one OpenAlex→record mapping`

---

### Task 3: `dl_lit/llm_common.py`

**Files:**
- Create: `dl_lit_project/dl_lit/llm_common.py`
- Modify: `dl_lit_project/dl_lit/APIscraper_v2.py:249-260, 293-324, 374, 513` (`_wait_rate_limit`, `_extract_total_tokens`, `_run_gemini_request`, `_looks_like_location_label`, `_normalize_source_metadata`), `dl_lit_project/dl_lit/get_bib_pages.py:113-142, 214-230, 295-330` (same helpers and the `with ThreadPoolExecutor` retry loop)
- Test: `dl_lit_project/tests/test_llm_common.py` (new); keep `tests/test_gemini_timeout_retries.py` passing (its monkeypatches move to `llm_common`)

**Interfaces:**
- Produces: `run_with_timeout(call, timeout_seconds, *, retries=0, backoff_seconds=0.0, label="LLM request", sleep=time.sleep)`, `extract_total_tokens(response)`, `looks_like_location_label(value)`, `normalize_source_metadata(value)`, `wait_rate_limit(service, units=1)`.

- [ ] **Step 1: Write the failing test** — `run_with_timeout(lambda: time.sleep(2), 0.2, retries=1, backoff_seconds=0)` raises `TimeoutError` and `elapsed < 1.0` (proves no `shutdown(wait=True)`); a call that succeeds on the second attempt returns its value; `looks_like_location_label`/`normalize_source_metadata` cases copied from both modules' current behaviour (pin them first).
- [ ] **Step 2: Run it** → ImportError.
- [ ] **Step 3: Implement** (body of `APIscraper_v2._run_gemini_request` with `shutdown(wait=False, cancel_futures=True)`), switch both modules, delete the copies.
- [ ] **Step 4: Whole suite green.**
- [ ] **Step 5: Commit** — `llm_common.py: one Gemini timeout/retry helper (get_bib_pages no longer waits for the hung call) and the shared LLM helpers`

---

### Task 4: Ingest batching, confidence fields, import links duplicates

**Files:**
- Modify: `dl_lit_project/dl_lit/db_manager.py:1036` (`add_corpus_item`), `:924` (`add_work_alias`), `:993` (`_log_merge`), `:1471-1606` (`create_pending_work`), `:800` (`add_pending_works_from_json`), new `transaction()` context manager near `close_connection` (`:122`)
- Modify: `backend/scripts/ingest_import_seed.py:100-125`
- Test: `dl_lit_project/tests/test_ingest_batching.py` (new); extend `tests/test_backend_helper_scripts.py` or `tests/test_db_duplicates.py` as fits

**Interfaces:**
- Produces: `DatabaseManager.transaction()` (contextmanager: `BEGIN` unless `self.conn.in_transaction`, `COMMIT` on exit, `ROLLBACK` on exception; nested use is a no-op passthrough), `commit: bool = True` keyword on `add_corpus_item`, `add_work_alias`, `_log_merge`.

- [ ] **Step 1: Write the failing tests** — (a) import the same 3-entry .bib into corpus A then corpus B via `ingest_import_seed.main([...])` on a temp DB: both corpora hold all 3 works, the second run's JSON says `added 0, corpus_linked 3, skipped 0`; (b) `add_pending_works_from_json` with 50 entries performs ≤ 2 commits (count via a `conn.commit` spy); (c) `create_pending_work` on an `alias_title` match merges and logs `low_confidence_match_merged`; a `normalized_doi` match is high confidence.
- [ ] **Step 2: Run them** → fail (second import skips 3; many commits; wrong notes).
- [ ] **Step 3: Implement** per spec D5/D6 (the `high_confidence` set `{"normalized_doi","openalex_id","title_authors_year","alias_title"}`, resolver guard `{"normalized_doi","openalex_id"}`, delete the unreachable `return`).
- [ ] **Step 4: Whole suite green.**
- [ ] **Step 5: Commit** — `db_manager: transaction() and commit=False for batch ingest; import-seed links duplicates to the target corpus; confidence fields match the lookup`

---

### Task 5: One download-and-mark path

**Files:**
- Modify: `backend/scripts/daemon/worker.py:609-672` (`_attempt_raw_download`), `:815-915` (`_download_single`), `dl_lit_project/dl_lit/cli.py:653-770` (`download_pdfs_command`), `dl_lit_project/dl_lit/db_manager.py:2121-2247` (`mark_raw_work_downloaded` payload duplication)
- Test: `dl_lit_project/tests/test_download_path.py` (new); keep `tests/test_cli_downloader.py`, `tests/test_worker_recovery.py`, `tests/test_download_claims.py` green

**Interfaces:**
- Produces: `backend/scripts/daemon/worker.py::download_ref(enhancer, ref: dict, download_dir) -> tuple[dict | None, str, list[dict]]` = `(download_result_or_None, checksum, trace)`; `checksum = result.get("checksum") or sha256 via hashlib.file_digest(fh, "sha256")` (chunked); `DatabaseManager._raw_download_payload(original, download_result, file_path, checksum, source) -> dict`.
- `cli.download_pdfs_command` uses `merged, msg = db.merge_duplicate_queued_work(...)` (counter bug) and `download_ref`.

- [ ] **Step 1: Write the failing tests** — (a) `download_ref` with a fake enhancer returning `{"downloaded_file": p, "checksum": "abc"}` returns `"abc"` without opening the file (monkeypatch `open`/`hashlib.file_digest` to fail); without `checksum` it returns the real sha256 of the temp file; (b) `mark_raw_work_downloaded` duplicate and non-duplicate branches produce identical payloads except `type`-related keys (snapshot both on a temp DB before refactor, assert equality after); (c) the CLI command's `ok` count equals the number of downloaded rows when one duplicate is merged.
- [ ] **Step 2: Run them** → fail.
- [ ] **Step 3: Implement**; move `get_entry_by_id` under `db_lock` in `_download_single`; replace the bare `except:` at worker 821/877.
- [ ] **Step 4: Whole suite green; `grep -n "except:" backend/scripts/daemon/worker.py` returns nothing.**
- [ ] **Step 5: Commit** — `worker/cli: one download_ref path (checksum from the downloader, chunked fallback); one raw-download payload; CLI counter fixed`

---

### Task 6: Keep-alive sessions

**Files:**
- Modify: `dl_lit_project/dl_lit/utils.py:509` (`get_openalex_session` → `get_thread_session`), `dl_lit_project/dl_lit/new_dl.py:376-391` (`_request_get_direct`, `_request_get_vpn`), `:1118-1130` (`download_paper` HTML branch → `_extract_pdf_link_from_response(response)`), `:2186-2216` (`fetch_openalex_batch`), `dl_lit_project/dl_lit/pdf_downloader.py:107`, `backend/scripts/keyword_search.py:105` (`_openalex_get`), `dl_lit_project/dl_lit/OpenAlexScraper.py:523` (`openalex_session = _build_retry_session(pool_maxsize=64)`)
- Test: `dl_lit_project/tests/test_thread_sessions.py` (new)

**Interfaces:**
- Produces: `get_thread_session(kind: str, *, proxies: dict | None = None, trust_env: bool = False, pool_maxsize: int = 16) -> requests.Session` (thread-local, cached per `(kind, tuple(sorted(proxies.items())) if proxies else None)`); `get_openalex_session()` stays as `get_thread_session("openalex", pool_maxsize=64)`.

- [ ] **Step 1: Write the failing tests** — same session object on repeated calls in one thread, different across threads (`threading.Thread`), different per `kind`/proxies; `trust_env` False; `download_paper` on a fake HTML response calls `_extract_pdf_link_from_response` once and performs no second GET of the same URL (fake session recording calls).
- [ ] **Step 2: Run them** → fail.
- [ ] **Step 3: Implement and switch the call sites** (`_request_get_vpn` keeps its eduVPN checks).
- [ ] **Step 4: Whole suite green; `grep -rn "with requests.Session()" dl_lit_project/dl_lit backend/scripts` returns nothing.**
- [ ] **Step 5: Commit** — `utils.get_thread_session: keep-alive sessions for downloads and OpenAlex side paths; landing pages parsed from the response already fetched`

---

### Task 7: Hygiene and CI

**Files:**
- Modify: `backend/scripts/daemon/worker.py:96-108` (`log()` single handle), `:1130-1210` (one `run()` loop)
- Modify: `dl_lit_project/dl_lit/db_manager.py:96-100` (`_table_columns` cache + invalidation in `_ensure_column`), delete `_merge_records` (1204), `_merge_incoming_into_existing` (1349), `_insert_citation_edge` (720), `_make_node_id` (709), `update_work_alias_owner` (969), `reassign_corpus_membership` (1075), `_get_corpus_ids_for_item` (1062), `fetch_pending_metadata_batch` (1655), `get_all_from_queue` (2828), `mark_downloaded_work` (2977) — each only after `grep -rn <name> dl_lit_project backend` shows the definition alone; `move_entry_to_downloaded` (2062) None-guard
- Modify: `dl_lit_project/dl_lit/OpenAlexScraper.py:821` (redact `api_key` in the printed params), `:94-95`/error prints (strip `api_key` from printed URLs), `:1047-1052` (per-candidate debug behind `env_bool("RAG_FEEDER_MATCH_DEBUG")`)
- Modify: `.github/workflows/ci.yml:68` (add the new test files)
- Test: `dl_lit_project/tests/test_hygiene.py` (new); extend `tests/test_pipeline_daemon.py` for the unified loop

**Interfaces:** none new.

- [ ] **Step 1: Write the failing tests** — `move_entry_to_downloaded` with a ref lacking `abstract_inverted_index` keeps the row's existing abstract; `_table_columns` runs `PRAGMA table_info` once per table across 10 inserts (spy on `cursor.execute`); a captured stdout of the fallback search path contains no `api_key=`; `log()` writes 3 lines with one `open` (spy on `builtins.open`); the unified `run()` processes one job and one direct cycle for `role="all"` and for `role="enrich"` (extend the existing daemon test harness).
- [ ] **Step 2: Run them** → fail.
- [ ] **Step 3: Implement**, delete the dead members, add the new test files to `ci.yml`.
- [ ] **Step 4: Whole suite green; CI list runs locally: `python -m pytest <ci list> -q`.**
- [ ] **Step 5: Commit** — `db_manager/worker/OpenAlexScraper hygiene: column cache, dead members removed, abstract kept on download, API key never printed, one daemon loop; CI runs the phase 4 tests`
