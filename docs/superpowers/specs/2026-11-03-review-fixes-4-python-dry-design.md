# Review fixes, phase 4: Python DRY and pipeline correctness — design

Date: 2026-11-03. Branch `review-fixes-4` (worktree from master 33acdde; phase 3 is in PR #77). Source:
`docs/reviews/2026-10-27-python-dry-perf.md` (I5, I8, I9 remainder, I10, I11, I12, I13; minors
2, 4, 5, 6, 8, 9, 11, 16). Phase 1 (PR #74) already did Python I1–I4 (index-served dedupe, the
`COALESCE` removal, `_absorb_duplicate`, expansion persistence), phase 2 (PR #76) deleted the
unreferenced helper scripts. Same method as phases 2 and 3: Opus-medium implementers under
subagent-driven development, Codex `gpt-6.1-sol` xhigh as the review gate. Jay opted out of
design questions; scope calls are the controller's and marked.

## Problems (verified against master after PR #76)

1. **Environment parsing is duplicated and crashes on typos.** `_env_int`/`_env_float` are
   defined in `utils.py`, `APIscraper_v2.py`, `get_bib_pages.py` and
   `backend/scripts/reference_expansion.py`; 13 inline `int(os.getenv("X","N") or "N")` calls
   (8 in `worker.py`, 5 in `new_dl.py`) raise `ValueError` on a non-numeric value, in
   `PipelineDaemon.__init__` and `BibliographyEnhancer.__init__` at startup.
2. **Identifier normalisation is copied 9+ times with two case conventions**; the inverted
   index converter exists five times (twice in `db_manager`, the second shadowing the first);
   `openalex_result_to_record` is line-for-line duplicated between `dl_lit/keyword_search.py`
   and `backend/scripts/keyword_search.py`.
3. **`get_bib_pages` cannot time out**: its `with ThreadPoolExecutor(...)` block waits for the
   hung Gemini call before retrying; `APIscraper_v2._run_gemini_request` does it right
   (`shutdown(wait=False, cancel_futures=True)`). The two modules also duplicate
   `_extract_total_tokens`, `_looks_like_location_label`, `_normalize_source_metadata` and the
   rate-limit wait wrapper.
4. **"Download, hash, mark" is copied three times** (`worker._download_single`,
   `worker._attempt_raw_download`, `cli.download_pdfs_command`); each re-reads the whole PDF to
   hash it although `PDFDownloader` already computed the checksum; the CLI copy overwrites its
   `ok` counter with the bool from `merge_duplicate_queued_work`; `mark_raw_work_downloaded`
   builds the same 35-line payload twice; `_download_single` reads `get_entry_by_id` outside
   `db_lock` and has two bare `except:`.
5. **Bulk ingest commits per row** (`add_corpus_item`, `add_work_alias`, `_log_merge` each
   commit) and `create_pending_work`'s confidence check names fields `_find_existing_work`
   never returns (`alias_title_year`; `"doi"` instead of `normalized_doi`), so its
   "low-confidence insert" comment is false: every match merges through
   `_resolve_or_create_work(allow_merge=True)`.
6. **`ingest_import_seed.py` drops entries that already exist**: it never passes `corpus_id`
   into `create_pending_work`, so a duplicate is counted as skipped and not linked to the
   target corpus.
7. **No HTTP keep-alive on the download path**: `_request_get_direct`/`_request_get_vpn` and
   `pdf_downloader._download_and_save` open a `requests.Session()` per GET;
   `download_paper` refetches an HTML landing page through `try_extract_pdf_link` although
   `_extract_pdf_link_from_response` can parse the response it already has; the OpenAlex side
   paths (`backend/scripts/keyword_search._openalex_get`, `new_dl.fetch_openalex_batch`) use
   bare `requests.get`; the searcher's `openalex_session` is a plain `Session()` with the
   default pool of 10 shared by up to 32 threads.
8. **Hygiene**: `worker.log()` opens the log file per line; `_table_columns` runs
   `PRAGMA table_info` on every insert/update; about 500 dead lines in `db_manager`
   (`_merge_records`, `_merge_incoming_into_existing`, `_insert_citation_edge`, `_make_node_id`,
   `update_work_alias_owner`, `reassign_corpus_membership`, `_get_corpus_ids_for_item`,
   `fetch_pending_metadata_batch`, `get_all_from_queue`, `mark_downloaded_work`, the shadowed
   `_convert_inverted_index_to_text`, an unreachable `return`); `move_entry_to_downloaded`
   overwrites `abstract` with `None` because the worker's ref never carries
   `abstract_inverted_index`; the OpenAlex API key is printed with the params in the fallback
   search path and embedded in the URL that `fetch_citing_work_ids` prints on error;
   `_select_best_result` prints three debug lines per candidate on every enrichment; the
   daemon has two near-identical `run()` loops.

## Goals

- One env parser, one identifier module, one LLM timeout/retry helper, one download-and-mark
  path, one daemon loop.
- The three correctness bugs fixed: import links duplicates to the corpus, Gemini timeouts
  actually retry, downloads keep the abstract.
- No change to what the DB stores for the dedupe keys (`normalized_doi`, `openalex_id`,
  `normalized_title`, `normalized_authors`): every call site keeps the case it used.
- pytest suite green; every change covered by a pytest that uses a temp DB or a fake session.

## Non-goals (my calls)

- I6 (parallel keyword expansion) and I7 (enrich loop sliding window): throughput changes that
  need measurement on the live stack; separate phase.
- Minor 1 (`pipeline_jobs` index + retention), minor 10 (Semantic Scholar ordering), minor 12
  (shared job schema), minor 13 (`graph_3d_export` SQL), minor 14 (backfill skeleton), minor 15
  (monotonic limiter windows): untouched.
- `bibtex_entry_json` keeps storing the whole download ref (minor 5's second half): eight
  readers depend on its shape.
- Backend JS untouched except `.github/workflows/ci.yml`.

## Decisions

### D1. `utils.env_int / env_float / env_bool`
Public `env_int(name, default, *, minimum=None, maximum=None)`, `env_float(...)`,
`env_bool(name, default=False)` (truthy set `{"1","true","yes","on"}`); invalid values log one
warning to stderr and return the default. Every `_env_int`/`_env_float` copy and every inline
`int(os.getenv(...))` call uses them; `reference_expansion.py` imports from `dl_lit.utils`.

### D2. `dl_lit/identifiers.py`
`normalize_doi(value, *, case="upper") -> str | None` (the regex extract + prefix strip the
`db_manager` version does; `case="lower"` for the modules that lowercased),
`normalize_openalex_id(value) -> str | None` (the `db_manager` rule: bare `W…` id, uppercase),
`inverted_index_to_text(idx) -> str | None`, `openalex_work_to_record(item, *, run_id=None) ->
dict` (the duplicated `openalex_result_to_record`), `crossref_item_to_openalex_like(item) ->
dict` (the superset: with the fallback date fields). Each former copy becomes a thin alias or
an import; `db_manager._normalize_doi`/`_normalize_openalex_id` delegate. A pytest pins the
outputs of every former copy against the shared function on a fixture list of DOIs/ids so the
DB keys cannot drift.

### D3. `dl_lit/llm_common.py`
`run_with_timeout(call, timeout_seconds, *, retries, backoff_seconds, label) -> result` (the
`APIscraper_v2._run_gemini_request` body), `extract_total_tokens(response)`,
`looks_like_location_label(value)`, `normalize_source_metadata(value)`,
`wait_rate_limit(service, units=1)`. Both modules import them. A pytest with a call that sleeps
past the timeout asserts the function raises/retries without waiting for the sleeping thread
(elapsed < timeout × attempts + 1 s).

### D4. One download path
`worker.download_ref(enhancer, ref, download_dir) -> tuple[dict | None, str, dict]` returning
`(result_or_None, checksum, trace_info)` where `checksum = result.get("checksum") or
hashlib.file_digest(open(fp,"rb"), "sha256").hexdigest()` (chunked, never `fh.read()` whole);
`_download_single`, `_attempt_raw_download` and `cli.download_pdfs_command` call it. `cli`
uses a separate name for the merge result (`merged, msg = …`). `db_manager._raw_download_payload
(original, download_result, file_path, checksum, source)` builds the payload once for both
branches of `mark_raw_work_downloaded`. `get_entry_by_id` in `_download_single` moves under
`db_lock`; bare `except:` become `except (json.JSONDecodeError, TypeError)` / `except OSError`.

### D5. Ingest batching and confidence fields
`add_corpus_item`, `add_work_alias`, `_log_merge` gain `commit: bool = True`;
`DatabaseManager.transaction()` context manager (`BEGIN` / `COMMIT` / `ROLLBACK` on exception,
no-op when already in one); `add_pending_works_from_json` and `ingest_import_seed.py` wrap
their loops in it and pass `commit=False`. `create_pending_work`: `high_confidence` set becomes
`{"normalized_doi", "openalex_id", "title_authors_year", "alias_title"}`, the resolver guard
checks `field not in {"normalized_doi", "openalex_id"}`, and the low-confidence branch keeps
merging (current behaviour) but logs `action="possible_duplicate", notes=
"low_confidence_match_merged"` and the comment says so. The unreachable `return` goes.

### D6. Import links duplicates
`ingest_import_seed.py` sets `entry["corpus_id"] = args.corpus_id` before
`create_pending_work` and drops the manual `add_corpus_item`; a duplicate counts as
`corpus_linked` (the DB call links it), `skipped` only for parse errors and missing titles. A
pytest imports the same .bib into corpus A then corpus B on a temp DB and asserts both
memberships exist and the JSON counts.

### D7. Sessions
`utils.get_thread_session(kind, *, proxies=None, trust_env=False, pool_maxsize=16) ->
requests.Session` (thread-local, cached per `(kind, proxies)`); `get_openalex_session()` becomes
`get_thread_session("openalex", pool_maxsize=64)`. `_request_get_direct` uses
`get_thread_session("download")`, `_request_get_vpn` `get_thread_session("download-vpn",
proxies=self.proxies)`, `pdf_downloader._download_and_save` the download session,
`backend/scripts/keyword_search._openalex_get` and `new_dl.fetch_openalex_batch` the OpenAlex
session. `download_paper` passes its HTML response to `_extract_pdf_link_from_response`
instead of refetching. `OpenAlexCrossrefSearcher.openalex_session = _build_retry_session(
pool_maxsize=64)`.

### D8. Hygiene
`worker.log()` keeps one append handle per process (reopened on `OSError`); `_table_columns`
caches per table on the instance, invalidated by `_ensure_column`; the dead `db_manager`
members in problem 8 are deleted (each verified by `grep -rn` across `dl_lit_project`,
`backend/scripts`, `backend/src` returning only the definition); `move_entry_to_downloaded`
only overwrites a column when the new value is not `None`; the fallback search path logs
params with `api_key` redacted and `fetch_citing_work_ids` strips it from the printed URL;
`_select_best_result`'s per-candidate prints go behind `env_bool("RAG_FEEDER_MATCH_DEBUG")`;
the two daemon loops become one (the role-specific body with the direct-batch heartbeat thread,
`all` taking both direct cycles in turn), pinned by `test_pipeline_daemon.py`.

## Order of work
1. D1 env parsing (small, unblocks the rest). 2. D2 identifiers. 3. D3 llm_common.
4. D5 + D6 ingest. 5. D4 download path. 6. D7 sessions. 7. D8 hygiene + CI pytest list.

## Verification
Per task: the task's pytest files plus `cd dl_lit_project && /home/jay/DT/venv/bin/python -m
pytest tests -q` (whole suite, ~15 s). Never against `dl_lit_project/data/literature.db`.
Final: suite green, CI pytest list updated, a dev-stack run after merge (enrich and download
daemons start, one extraction, one seed import of a duplicate into a second corpus).
