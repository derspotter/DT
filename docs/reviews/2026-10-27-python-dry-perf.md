# Python pipeline review: DRY and performance

Scope: `dl_lit_project/dl_lit/*`, `backend/scripts/*.py`, `backend/scripts/daemon/worker.py`, all at worktree HEAD `eb15bc5`.
Method: I read the code, grepped for repeated patterns, and ran `EXPLAIN QUERY PLAN` and timings. Timings come from a synthetic DB and from a **read-only** (`mode=ro`) connection to the live `dl_lit_project/data/literature.db`: 103,449 works, 147,738 corpus_works rows, 689,339 citation_edges, 913 MB, no `sqlite_stat1`.
There is no `dl_lit/pipeline.py` in the package.

**Totals: 13 Important, 16 Minor.**

---

## Important

### I1. The DOI and OpenAlex-ID dedupe lookups never use their indexes. Each one is a 250 ms full scan.
`db_manager.py:459-478` (`_find_existing_work`) runs `SELECT id FROM works WHERE normalized_doi = ?` and `... WHERE openalex_id = ?`.
The only indexes on those columns are **partial**: `idx_works_normalized_doi` and `idx_works_openalex_id` (`db_manager.py:360-361`), both `WHERE x IS NOT NULL AND x != ''`.
SQLite can only use a partial index when the query's WHERE clause implies the index's WHERE clause. `col = ?` with a bound parameter does not imply `col != ''`, so every lookup does `SCAN works`.

Measured on the live DB, warm cache:
- `openalex_id = ?`: **251.9 ms**
- `openalex_id = ? AND openalex_id != ''`: **0.0 ms** (`SEARCH ... USING COVERING INDEX`)

How this multiplies:
- `check_if_exists` does both lookups, so about 0.5 s per call.
- `create_pending_work` calls `_find_existing_work` twice: through `check_if_exists` at 1388, then again through `_resolve_or_create_work` at 1483. That is about **1 s per imported reference**, so a 1,000-entry .bib import spends about 17 min scanning.
- `worker._download_single` (`worker.py:715-720`) runs `check_if_exists` **under `self.db_lock`**. All 16 download threads therefore serialize on about 0.5 s of scanning each, which caps dedupe at about 2 rows/s.
- `apply_enriched_metadata` (1669) runs the lookup inside an open transaction. `mark_raw_work_downloaded` (2051) and `add_entry_to_download_queue` (2322, called once per record by `keyword_search.py --enqueue` and seed promotion) do the same lookup.

**Fix (one line per query):** add `AND normalized_doi != ''` and `AND openalex_id != ''` to the two queries in `_find_existing_work`. Alternatively, add non-partial indexes, though the unique partial ones should stay. Add a regression test that asserts `EXPLAIN QUERY PLAN` contains `USING ... INDEX idx_works_openalex_id`.

### I2. The enrich claim query and `do_mark` use `COALESCE`, which defeats the status index. That costs 260 ms under a write lock on every idle poll.
- `db_manager.py:1763-1771` and `1784` (`claim_enrich_batch`): `COALESCE(metadata_status,'pending') = 'pending' ... AND COALESCE(nm.download_status,'not_requested') = 'not_requested'`.
- Both columns are `NOT NULL DEFAULT` (`db_manager.py:132,136`), so the `COALESCE` is a no-op that only prevents index use.
- The query runs after `BEGIN IMMEDIATE`, so it holds the **write lock** for the whole scan. The enrich daemon runs it every 5 s when idle (`_run_direct_enrich_cycle`, then `time.sleep(5)` at `worker.py:1063`).
- Live: current form **261 ms**. Rewritten as `(metadata_status='pending' OR (metadata_status='in_progress' AND metadata_lease_expires_at <= ?)) AND download_status='not_requested'`: **3.0 ms** (`SEARCH USING INDEX idx_works_download_status`).
- The same `COALESCE` also appears in:
  - the claim `UPDATE` at 1815-1826;
  - `do_mark` (`worker.py:439-466`): a 280 ms `COUNT(*)` whose result nothing uses. The comment says marking is obsolete, and it runs on every `pipeline_tick`;
  - `fetch_matched_work_batch` (1543);
  - `backend/scripts/ingest_stats.py`.

**Fix:** drop the `COALESCE` calls and delete the body of `do_mark`, returning the constant dict. For a better ORDER BY, add a partial index `CREATE INDEX idx_works_enrich_claimable ON works(id) WHERE metadata_status IN ('pending','in_progress') AND download_status='not_requested'` and write the predicate so it matches literally.

### I3. Three duplicate-merge paths drop corpus membership. This is a correctness bug.
These paths delete the duplicate row without moving its `corpus_works` rows to the canonical work:
- `apply_enriched_metadata` (`db_manager.py:1678-1685`)
- `mark_raw_work_downloaded` (2096)
- `merge_duplicate_queued_work` (2899-2902)

`pruneStaleCorpusItems` (`backend/src/app.js:1047`) later deletes the orphaned rows. That is why the live DB shows 0 orphans: the work silently leaves the corpus it was promoted into unless the canonical work happened to be there already.

The one correct implementation is `_merge_records` (`db_manager.py:1105-1247`). It moves `corpus_works` and aliases and writes `merge_log`, but **nothing calls it**.

**Fix (DRY and correctness):** add one helper, `_absorb_duplicate(canonical_id: int, duplicate_id: int, *, match_field: str | None, merged_payload: dict | None) -> None`, used by all three paths and by `_merge_records`. It should:
- `INSERT OR IGNORE INTO corpus_works SELECT corpus_id, ?, added_at FROM corpus_works WHERE work_id = ?`
- move the aliases
- delete the duplicate
- write `merge_log`

Also remove the second `self.db.delete_work(pending_work_id)` in `worker.py:663-664`, because the row is already gone.

### I4. Enrichment expansion fetches related works and then throws them away. This is a regression from `0b639ec`.
- `worker.do_enrich` passes `fetch_refs` and `fetch_cites` (`worker.py:589-594`) whenever the promotion is not in `new_seed` mode (`app.js:5306-5317`).
- `process_single_reference` then calls `fetch_referenced_work_details`, plus a second depth level when `related_depth>1`, and `fetch_citing_work_ids` (`OpenAlexScraper.py:1266-1376`). That costs `ceil(n/50)` OpenAlex calls per level per item, plus paged cited-by calls.
- `apply_enriched_metadata` ignores its `expand_related` and `max_related_per_source` parameters (1623-1624). `_update_work_record` filters `referenced_works` and `citing_works` out by column, and `_insert_citation_edge` has no callers.
- `git log -S expand_related` shows that the refactor `0b639ec` ("simplify dl_lit cli and db tooling", 2026-04-17) removed the persistence (`add_citing_works_to_with_metadata`, the edge inserts) but left the fetch.

Result: OpenAlex credits and seconds of latency per item are spent for nothing, and "expand on promote" in direct mode does nothing without telling anyone.

**Fix:** either restore persistence (edges plus pending works) or call `process_single_reference(..., fetch_references=False, fetch_citations=False)` in the worker until that is done. In both cases, delete the dead parameters.

### I5. No HTTP keep-alive on the download path and most OpenAlex side paths.
- `new_dl._request_get_direct` and `_request_get_vpn` (`new_dl.py:376-391`) wrap every GET in `with requests.Session()`. This affects every Unpaywall, DOI, Sci-Hub, LibGen and landing-page request. `pdf_downloader._download_and_save` (`pdf_downloader.py:~95`) does the same.
- So every request pays a fresh TCP and TLS setup, roughly 100-300 ms, with 16 download threads running.
- `download_paper` (1070-1140): when the response is HTML, it calls `try_extract_pdf_link(url)` (1042), which **fetches the same URL a second time** instead of parsing the `response` it already has.
- On the OpenAlex side, only keyword paging uses `utils.get_openalex_session()`. These calls pass no session and fall back to bare `requests.get`:
  - `backend/scripts/keyword_search._openalex_get` (100-111), used for seed resolution and the ranking batches
  - the expansion calls to `fetch_referenced_work_details` and `fetch_citing_work_ids` (`keyword_search.py:376-381, 435-440`)
  - `backfill_*.py`
  - `new_dl.fetch_openalex_batch` (2216)

**Fix:** generalize `utils.get_openalex_session()` into `get_thread_session(kind: str, *, proxies: dict | None = None, trust_env: bool = False) -> requests.Session`, a thread-local session cached per `(kind, proxies)`. Use it in `_request_get_direct`, `_request_get_vpn`, `PDFDownloader` and `_openalex_get`, and make `session=get_openalex_session()` the default in `fetch_*` when `None`. Pass the HTML response into `_extract_pdf_link_from_response` directly.

### I6. Keyword-search expansion is serial and fetches each work twice.
In `backend/scripts/keyword_search._crawl_direction` (365-440), the BFS handles one item at a time. For each downstream item:
1. `_rank_candidate_ids` makes `ceil(ids/50)` **sequential** calls with `select=id,cited_by_count,publication_date`.
2. `fetch_referenced_work_details` then fetches the kept top N again with the full select.
3. Sometimes there is an extra `fetch_work_by_openalex_id` refresh (338-342).

With 200 base items at depth 1, that is at least 400 sequential round trips, each on a new connection (see I5).

**Fix:**
- Request the detail `select` in the ranking batches, sort locally, and keep the top N from the same responses. This drops one round trip per batch.
- Process each BFS level with a `ThreadPoolExecutor(max_workers=8)`. The global `ServiceRateLimiter` already bounds the rate.

### I7. The enrich loop runs raw-download fallbacks serially and puts a barrier after every two items.
`worker.do_enrich` (`worker.py:596-670`):
- The wave size defaults to **2** (`RAG_FEEDER_ENRICH_WAVE_SIZE`), and a **new `ThreadPoolExecutor` is created per wave**. Each wave waits for its slowest item.
- When enrichment fails, `_attempt_raw_download(entry)` (622) runs **on the main thread** inside the result loop. A full download chain (OA, DOI, Unpaywall, Sci-Hub, LibGen; each step has a 12 s timeout plus VPN retry) blocks the whole batch.
- `_has_pending_corpus_download_jobs()` and `_has_pending_interactive_jobs()` scan `pipeline_jobs` after every wave.

**Fix:**
- Do the fallback inside the worker thread: have `_enrich_single` return the download result too.
- Use one executor per `do_enrich` with a sliding window: keep `wave_size` futures in flight and submit the next as each completes.
- Check the yield conditions every k completions instead of every wave.

### I8. `get_bib_pages` cannot actually time out, and its retry wrapper is duplicated.
- `get_bib_pages.py:309-330` uses `with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:`. Leaving the `with` block calls `shutdown(wait=True)`, so after `future.result(timeout=...)` raises, the process **still waits for the hung Gemini call to finish** before it retries.
- `APIscraper_v2._run_gemini_request` (293-323) does this correctly with `shutdown(wait=False, cancel_futures=True)`.
- The existing test (`tests/test_gemini_timeout_retries.py:121`) mocks the executor away, so it cannot catch this.

**Fix:** move the APIscraper version to `llm_provider.run_with_timeout(call, timeout_s, *, retries, backoff_s, label)` and call it from both modules.
The two modules also duplicate `_extract_total_tokens`, `_looks_like_location_label`, `_normalize_source_metadata` and the rate-limit wrappers (`_wait_rate_limit` and `_rate_limiter_wait`). Those belong in a shared `dl_lit/llm_common.py`.

### I9. Bulk ingest commits per row and runs dedupe twice. The "low-confidence insert" branch is dead.
- `create_pending_work` (1371-1501) commits 2-4 times per entry: in `add_corpus_item` (1022), `_resolve_or_create_work` (551/555), `add_work_alias` (928) and `_log_merge` (987). It runs the dedupe lookup twice (see I1).
- After logging `possible_duplicate` / `low_confidence_match_inserted` (1468-1478), it calls `_resolve_or_create_work`. That method finds the same row again and **merges** (`allow_merge=True`) instead of inserting, so the comment "Low-confidence duplicates are logged but allowed to insert" is false.
- Callers that loop over this per row:
  - `add_pending_works_from_json` (803-826)
  - `backend/scripts/ingest_import_seed.py` (100-124), whose final `commit()` is a no-op
  - `keyword_search.py --enqueue` (848-850), through `add_entry_to_download_queue`
- `high_confidence` checks for `"alias_title_year"`, but `_find_existing_work` only returns `"alias_title"`. The check `field not in {"doi","openalex_id"}` should be `"normalized_doi"`.

**Fix:**
- Thread the already-found `(existing_id, field)` into a `_create_or_merge(payload, existing_id)` call, so there is no second lookup.
- Add `commit: bool = True` to `add_corpus_item`, `add_work_alias` and `_log_merge`.
- Wrap batch loops in a `DatabaseManager.transaction()` context manager (`BEGIN` / `COMMIT` / `ROLLBACK`).
- Decide whether low-confidence matches should insert, then make the code do it.

### I10. `ingest_import_seed.py` does not link duplicate works to the target corpus.
`ingest_import_seed.py:100-120` calls `db.create_pending_work(entry)` without `entry["corpus_id"]`. `create_pending_work` links a matched duplicate to a corpus only through `ref.get("corpus_id")` (1449, 1462), and the script links only when `row_id` is truthy.

So importing a .bib into corpus B **silently drops every entry that already exists** (for example, in corpus A). It counts them as "skipped" instead of linking them. No test covers this script.

**Fix:** set `entry["corpus_id"] = args.corpus_id` before the call and drop the manual `add_corpus_item`, or link on duplicate as well.

### I11. Identifier and record normalization is copy-pasted 10+ times, with opposite case conventions.
- **DOI normalizers (10):**
  - uppercase, with a regex extract: `db_manager._normalize_doi` (650), `graph_3d_export.normalize_doi` (144), `migrate_to_works` (19), `ingest_latest` (43)
  - lowercase, with prefix stripping: `OpenAlexScraper.normalize_doi` (837), `backend/scripts/keyword_search.normalize_doi` (90), `backfill_openalex_topics` (61), `export_bundle` (22), `upstream_update` (234)
  - inline: `new_dl.check_unpaywall` (626-640) and `clean_doi_url`
- **OpenAlex-ID normalizers (6).**
- **Inverted index to text (5):**
  - `db_manager` 1938 **and** 2862. The second definition silently shadows the first.
  - `OpenAlexScraper.convert_inverted_index_to_text` (54) and `get_abstract` (924)
  - `new_dl.convert_inverted_index_to_text` (2240)
- **OpenAlex work to record mapping (4):**
  - `dl_lit/keyword_search.openalex_result_to_record` (364) and `backend/scripts/keyword_search.openalex_result_to_record` (178) are line-for-line identical.
  - `OpenAlexScraper._build_enrichment_payload` (938)
  - `db_manager.move_entry_to_downloaded` (1968-1995), inline
- **Crossref item to OpenAlex-like mapping (2):** `search_crossref` (600-647) and step 8 of `search()` (731-787). They differ: one has no fallback date fields, the other does.

**Fix:** create `dl_lit/identifiers.py` with:
- `normalize_doi(value, *, case: Literal["upper","lower"]="upper") -> str | None`
- `normalize_openalex_id(value) -> str | None`
- `inverted_index_to_text(idx) -> str | None`
- `openalex_work_to_record(item, *, run_id=None, ingest_source=None) -> dict`
- `crossref_item_to_openalex_like(item) -> dict`

Make every module above import these. One normalizer also means one hash per string: today `_find_existing_work` and `create_pending_work` re-normalize the same title and authors several times per call.

### I12. The "download, hash, mark" sequence is copied three times. The file is hashed twice, and the CLI copy has a counter bug.
- The copies:
  - `worker._download_single` (`worker.py:704-803`)
  - `worker._attempt_raw_download` (495-557)
  - `cli.download_pdfs_command` (`cli.py:649-762`)
- Each copy builds the same `ref` dict, calls `enhance_bibliography`, then rereads the whole PDF into memory to compute SHA-256 (`worker.py:530,765`, `cli.py:749`). `PDFDownloader` already computed that hash and put it in `ref['checksum']` (`new_dl.py:1246`).
- `cli.py:717` does `ok, msg = db.merge_duplicate_queued_work(...)`, which **overwrites the `ok` counter** with a bool.
- The CLI path never claims or leases the rows, so it races the daemon. Its dedupe also merges into non-downloaded works, which the worker deliberately does not do (`worker.py:721-725`).
- Inside `db_manager.mark_raw_work_downloaded`, the duplicate branch (2062-2095) and the normal branch (2101-2135) build the same 35-line payload; only `type` differs.

**Fix:** add `PipelineDaemon._download_ref(ref, *, mark: Callable) -> tuple[status, data]` (or a module-level function shared with the CLI) that returns `checksum` from `result.get("checksum")` and falls back to a chunked `hashlib.file_digest`. Build the raw payload once in `_raw_download_payload(original, download_result, ...)`.

### I13. Environment parsing is duplicated and fragile.
- `_env_int` / `_env_float` are defined 4 times: `utils.py:173-197`, `APIscraper_v2.py:57-75`, `get_bib_pages.py:60-78`, `reference_expansion.py:6-21`.
- There are 13 inline `int(os.getenv("X","N") or "N")` calls: 8 in `worker.py` (for example 184, 188, 596, 811, 826, 894, 895, 900) and 5 in `new_dl.py` (116-125). Each **raises `ValueError` on a typo**. In `__init__` that crashes the daemon at startup. In `do_enrich` and `do_download` it fails every job.
- 4 separate truthy-set parsers (`in {"1","true","yes","on"}`).

**Fix:** make `utils.env_int(name, default, *, minimum=None, maximum=None)`, `env_float` and `env_bool` public and use them everywhere.

---

## Minor

1. **`pipeline_jobs` has no index and no retention.** `fetch_next_job` (`worker.py:304-365`) and both `_has_pending_*` probes run `SCAN pipeline_jobs`. The table has 7k rows today (1.5 ms per probe). Nothing prunes completed or cancelled jobs (6,827 cancelled), and `result_json` stores per-item results.
   Add `CREATE INDEX idx_pipeline_jobs_pending ON pipeline_jobs(job_type, corpus_id, created_at, id) WHERE status = 'pending'` (the literal `status = 'pending'` matches), plus a retention sweep (for example, delete finished jobs older than 30 days).
2. **`_table_columns()` runs `PRAGMA table_info` on every insert and update** (`_insert_work_record` 521, `_update_work_record` 530) and in the dead merge helpers. Cache it per table on the instance and invalidate the cache in `_ensure_column`.
3. **Redundant `corpus_works` indexes.** The PK `(corpus_id, work_id)` already covers `idx_corpus_works_corpus(corpus_id)` (`db_manager.py:367`) and `idx_corpus_works_lookup(corpus_id, work_id)` (`app.js:880`). They only add write amplification on 148k rows. Keep the PK and `idx_corpus_works_work`.
4. **About 500 lines of dead code in `db_manager`:**
   - `_merge_records` and `_merge_incoming_into_existing`, which also duplicate a 25-field `merge_fields` list and the normalization block
   - `_insert_citation_edge`, `_make_node_id`, `update_work_alias_owner`, `reassign_corpus_membership`, `_get_corpus_ids_for_item`, `fetch_pending_metadata_batch`, `get_all_from_queue`, `mark_downloaded_work`
   - the shadowed `_convert_inverted_index_to_text` at 1938
   - an unreachable `return int(new_id), None` (~1497)

   Separately, `backend/scripts/downloads_list.py` and `ingest_runs.py` are no longer spawned, since the reads were ported to JS. `downloads_list.py` also fetched every row and sliced in Python.
5. **`move_entry_to_downloaded` blanks abstracts and bloats `bibtex_entry_json`.** It computes `abstract` from `enriched_metadata['abstract_inverted_index']` (1973), but the worker's `ref` never has that key, so every download writes `abstract = NULL`. Five live rows show this: an abstract is present in `openalex_json` but `abstract` is empty. The same method stores the whole ref, including `download_trace` and the embedded `openalex_json` string, in `bibtex_entry_json`: 113 MB across 48k rows.
   **Fix:** only overwrite a field when the new value is not `None`; store just `download_trace`.
6. **`get_entry_by_id` runs outside `db_lock`** in `worker._download_single` (722), on the connection the other download threads share.
7. **`record_openalex_quota` writes a file per response** (`utils.py:236-295`): `mkstemp`, `json.dump`, `chmod` and `replace` on every OpenAlex response, from every thread (up to 30 rps). Throttle it to once per second per process, or write only when `remaining` changes bucket.
8. **The shared searcher session's pool is too small.** `OpenAlexCrossrefSearcher.openalex_session` is a plain `requests.Session()` (`OpenAlexScraper.py:264`) with the default `pool_maxsize=10`. Up to 32 enrich threads use it, plus 8 inner batch threads in `fetch_referenced_work_details`, which leads to "connection pool is full" discards. Use `_build_retry_session(pool_maxsize=64)` like the Crossref and S2 sessions, or the thread-local session from I5.
9. **The OpenAlex API key can leak into logs.** `OpenAlexCrossrefSearcher.search` prints `Params: {params}` after `_with_openalex_auth` has added `api_key` (`OpenAlexScraper.py:821`; only when `RAG_FEEDER_ENABLE_OPENALEX_FALLBACK=1`). `fetch_citing_work_ids` embeds `api_key` in the URL (93-95) and prints that URL on error.
   The key is also read in 3 places, each with its own "missing key warned" flag: `utils.get_openalex_api_key`, `OpenAlexCrossrefSearcher.__init__` and the `_with_openalex_auth` path. That is redundant because `openalex_request_json` already adds it.
10. **Semantic Scholar sets the real throughput ceiling.** Every reference without a DOI hits `search_semantic_scholar_match` first, under a global limit of 1 rps with a 1.1 s minimum interval (`utils.py:550-551`). Enrichment is therefore capped at about 0.9 refs/s no matter what `RAG_FEEDER_ENRICH_WORKERS` says. Either document this next to the workers setting, or try Crossref first when S2 runs without a key.
11. **Daemon loop duplication.** The two `run()` loops (`worker.py:1019-1100`, role-specific vs `all`) and the two heartbeat-thread starters (248-280) are near-identical. Fold them into one loop and one `@contextmanager _heartbeat_while(payload)`. The idle poll is a fixed `time.sleep(5)`; backing off to about 30 s after N empty polls would lower the idle scan cost, though I2 makes this mostly moot.
12. **The job parameter schema lives only in code, in two languages.**
    - Job types and their priority are hard-coded in the worker's role filter and `ORDER BY CASE` (`worker.py:307-333`).
    - The keys (`limit`, `workers`, `expansion.{relatedDepthDownstream,...,maxRelated,maxCitations}`, `pending_work_ids`, `batchSize`, `config.promoteBatchSize`/`promoteWorkers`, `run_id`, `artifacts_root`) are written at 4+ `enqueuePipelineJob` call sites in `app.js` and read with separate defaults in `worker._process_job` (`limit` 10 vs `batchSize` 3, ...).

    Move this to a `backend/shared/pipeline_jobs.json` that lists the types, their role, priority and parameter defaults, and load it from both sides.
13. **`graph_3d_export` loads everything.** It reads every works row (all columns) and **all** 689k `citation_edges` (856 ms), then filters by status and corpus in Python (`graph_3d_export.py:892-936`). Push the status predicate into SQL and join the edges against the kept ids, for example through a temp table. The results are cached, so this is low priority.
14. **The backfill scripts share a skeleton.** `backfill_citation_edges.py` and `backfill_openalex_topics.py` both have `log`, `normalize_openalex_id`, an "ensure column" step and the same batch-of-50 / per-batch-commit loop. Extract a `run_openalex_batches(rows, build_params, handle_results)` helper.
15. **`ServiceRateLimiter` measures windows with `datetime.now()`** (`utils.py:72-122`), which is wall-clock time. An NTP or DST jump can stall or burst the limiter. Use `time.monotonic()` for the window log; it is already used for spacing.
16. **The worker's `log()` opens the log file once per line** (`worker.py:94-107`). On top of that, `_select_best_result` prints 3 debug lines per candidate (`OpenAlexScraper.py:1047-1052`) into the daemon's stdout, which is both noisy and slow under the stream lock. Hold one append handle, and put the per-candidate debug output behind `RAG_FEEDER_DOWNLOAD_DEBUG` or a similar flag.

---

## What is already good
- **The download claim** (`claim_download_batch`) uses `BEGIN IMMEDIATE`, a guarded `UPDATE ... WHERE <eligible>`, a lease, and re-reads by `claimed_by`/`claimed_at`. Its predicate has no `COALESCE`, so it uses `idx_works_download_status` (0.3 ms live). Stale leases are reclaimed through the same predicate, and stale `pipeline_jobs` are requeued at startup.
- **Heartbeats** use a dedicated connection with `busy_timeout`. A single-row upsert every 3 s is negligible, and the main-loop heartbeat is throttled.
- **The settings refresh** (`refresh_app_settings`) is one cheap read-only query per poll. It is fingerprint-guarded, and on failure it rolls back the global limiter, so an Admin change does not churn the clients.
- **Keyword search paging** streams pages with `accumulate=False`. Dedupe happens in the store (`executemany ... WHERE NOT EXISTS`, backed by `idx_search_results_run_openalex`), it reuses a thread-local session, and memory stays flat.
- **`fetch_referenced_work_details`** batches 50 ids per request and runs the batches 8-way in parallel. The backfills batch 50 ids per request with one `executemany` and one commit per batch.
- **Concurrency setup:** WAL, `synchronous=NORMAL` and `busy_timeout` are set on the main connection. The stdout/stderr line-locking wrapper in the daemon prevents interleaved log lines.
- **The OpenAlex quota snapshot** is written atomically (temp file plus `os.replace`), and its no-key / no-headers semantics are handled with care.
