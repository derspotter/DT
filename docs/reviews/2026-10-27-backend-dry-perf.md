# Node backend review: DRY and performance (Korpus Builder)

Scope: `backend/src/{app.js,index.js,seed.js,corpusList.js,pipelineReads.js}`, the Python children they spawn, and the frontend polling that drives them (`frontend/src/App.svelte` around lines 551-1739). This was a read-only review. Timings come from the `sqlite3` CLI against the live `dl_lit_project/data/literature.db`, opened read-only. Index experiments ran on a faithful scratch copy of `works` and `corpus_works`, so the live DB was never written.

**Data shape that matters:** `works` has 103,449 rows. Corpora 99 and 1 hold 74k and 72k works. `citation_edges` has 689,339 rows. The DB is 871 MB. `works` rows are fat: the average abstract is 1.0 KB, `bibtex_entry_json` 1.6 KB, `openalex_json` 0.5 KB and `crossref_json` 0.5 KB. Columns such as `metadata_status` and `download_status` come after `abstract` in the record, so reading them for most rows means following overflow pages. That is why every query that looks only at status per work over a whole corpus costs 200-700 ms.

**Polling load:** On the workspace or dashboard tab, the frontend runs one refresh cycle every 2 s while anything is active. Worker log lines arriving over the WebSocket can trigger extra cycles 300 ms apart. Each cycle calls `/api/corpus`, `/api/ingest/stats` and `/api/seed/sources`. Admins also get `/api/downloads` and two worker-status routes, and each worker-status route runs `readWorkerBacklogSummary`.

Totals: **11 Important, 16 Minor.**

---

## Important

### I1. Status counts are computed four different ways, each a full fat-row scan per poll

The same `(metadata_status, download_status)` aggregation over the corpus runs in four places on every refresh cycle:

| Where | What | Measured |
|---|---|---|
| `corpusList.js:334-339` (`readCorpus`) | `GROUP BY STATUS_SQL`, where `STATUS_SQL` is `lower(trim(x, char(29 code points)))` evaluated twice per row | 0.74 s cold, 0.24 s warm |
| `pipelineReads.js:156-186` (`readIngestStats`) | `SUM(CASE ...)` ×8, plus two `PRAGMA table_info` calls per request | ~0.25 s |
| `app.js:2871-2911` (`readWorkerBacklogSummary`) | `SUM(CASE ...)` ×8. Called twice per admin cycle, and again in `/api/pipeline/daemon/status`, promote and process-marked | ~0.25 s each |
| `seed.js:300-311` (`worksFingerprint`) | `COUNT`, `MAX(id)` and `SUM(CASE download_status)` over all of `works`, on every `createStateResolver` | 0.13 s |

These also disagree with each other:
- `raw_pending` means `pending + in_progress` in `readIngestStats` but only `pending` in `readWorkerBacklogSummary`.
- The status bucketing exists a fourth time in JS at `app.js:1296` (`statusFromWorkRow`), and again in Python in `corpus_list.py` and `ingest_stats.py`.
- The trim/lower defence in `STATUS_SQL` guards against nothing today: a live check found 0 rows whose status differs from `lower(trim(status))`.

**Fix:**
1. Add a covering index so these aggregates never touch the table b-tree:
   ```sql
   CREATE INDEX IF NOT EXISTS idx_works_state
     ON works(download_status, metadata_status, metadata_lease_expires_at, download_lease_expires_at);
   ```
   On the scratch copy, the corpus-99 count dropped from 240 ms to 67 ms. The plan is `SCAN w USING COVERING INDEX idx_works_state` plus a PK probe on `corpus_works`. Write the scope as `WHERE EXISTS (SELECT 1 FROM corpus_works cw WHERE cw.corpus_id=? AND cw.work_id=w.id)` rather than a JOIN driven from `corpus_works`, which looks up each fat row by rowid.
2. Add one helper, for example `readWorkStateCounts(db, corpusId, nowSec) -> [{download_status, metadata_status, live_meta, live_dl, n}]`, that runs `GROUP BY download_status, metadata_status`. Derive `status_counts`/`stage_totals`, ingest stats and the backlog summary from those few dozen rows in JS. Put the bucket mapping in one exported `workStatus(meta, dl)` and delete `statusFromWorkRow` and `STATUS_SQL`.
3. Memoise the result per `(corpusId, PRAGMA data_version)` for about 1 s. `data_version` is cheap and changes whenever another connection commits, so the cache stays correct. Polls from several tabs, and the two worker-status calls in the same cycle, then share one scan.

### I2. `listCorpus` page query sorts all 72k fat rows on every poll (the 480 ms)

`corpusList.js:160` and `:385` sort by `CASE WHEN typeof(w.year)='integer' THEN w.year WHEN w.year IS NULL THEN 0 ELSE dt_corpus_int(w.year) END DESC, w.id DESC`. No index can serve that expression, so the plan is `SEARCH cw ... / SEARCH w USING INTEGER PRIMARY KEY / USE TEMP B-TREE FOR ORDER BY`: every member row is read, including overflow pages, and then sorted. That took 0.19-0.72 s for the default page.

Live data has only `integer` (102,532) and `null` (917) years. For those types, `ORDER BY w.year DESC, w.id DESC` gives the same order, because SQLite sorts NULL lowest, so it comes last in DESC, which matches treating NULL as 0.

**Fix:**
```sql
CREATE INDEX IF NOT EXISTS idx_works_year_id ON works(year, id);
-- page query
SELECT ... FROM works w
WHERE EXISTS (SELECT 1 FROM corpus_works cw WHERE cw.corpus_id=? AND cw.work_id=w.id)
ORDER BY w.year DESC, w.id DESC LIMIT ? OFFSET ?
```
On the scratch copy this dropped from 190 ms to 2 ms. The plan becomes `SCAN w USING INDEX idx_works_year_id` plus a PK probe, and it stops after `LIMIT` hits. For `added_at`, the corpus-scoped date-added sort, add `CREATE INDEX idx_corpus_works_added ON corpus_works(corpus_id, added_at, work_id)` and drive the query from `cw`.

To keep the golden-test parity:
- Coerce text years to integers at write time, in `db_manager` and `seed_promote`, and keep `dt_corpus_int` only for a one-off backfill.
- Alternatively, keep the current expression as the fallback only when `SELECT 1 FROM works WHERE typeof(year) NOT IN ('integer','null') LIMIT 1` finds a row.

Other parts of `readCorpus`:
- The title, authors, source and status sorts, and the `q` filter, call JS UDFs (`dt_corpus_lower`, `dt_corpus_authors_key` with a `JSON.parse` per row, `dt_corpus_search_text`) over all 72k rows on each request, about 100-300 ms per keystroke.
- **Fix for the `q` filter and those sorts:** Store a lowercased `sort_title`, `sort_authors` and `search_text` column, written by the Python writers or by a trigger plus a one-off JS backfill, and index the sort keys. Or use FTS5 for `q`.
- The status counts should come from I1.

### I3. `/api/seed/sources` (polled every 2 s on the workspace) rebuilds the whole state resolver per request

- `listSeedSources` always calls `createStateResolver` (`seed.js:900`), even with `withStateCounts:false`. `createStateResolver` (`seed.js:331-433`):
  - runs `worksFingerprint`, a full scan (I1);
  - loads every work in the corpus: `loadMatchRows(... {corpusId})`, 74k rows, 0.39 s for the SQL alone. The code comment at `seed.js:295` says the "corpus-local half (a few hundred rows)", which is wrong for the two real corpora;
  - filters those rows into seven subsets with `String().trim().toLowerCase()` per row per subset;
  - builds seven indexes, calling `normalizeText`, `normalizeContributors` (a `JSON.parse` of authors) and a regex `normalizeDoi` per row;
  - reloads all `work_aliases` for the failed-download alias index.
- For every pdf seed, `listSeedSources` then resolves every candidate just to get `candidate_count` (`seed.js:970`). Each resolution calls `resolveDownloadedAvailability` (`seed.js:400-426`). For a downloaded candidate, that does a fresh `db.prepare(...).get()` plus `tableExists`, which is an N+1 with a statement compile each time, and then `findDownloadedFilePath`, which makes up to four `fs.existsSync` calls (`app.js:1877-1916`), synchronously, per candidate, per poll.
- `/api/seed/sources/:type/:key/candidates/:candidateKey/file` (`app.js:5239`) resolves the whole seed to `.find()` one candidate.
- **Fix:**
  - Build the resolver lazily, only when state is needed.
  - Cache the corpus-local index per `(corpusId, data_version)`, as the global half already is.
  - Count pdf candidates in SQL: `COUNT(*) FROM ingest_entries ... AND NOT EXISTS (dismissed)` when `q` is empty. That is the same shape as the search branch.
  - Hoist the `file_path` statement and resolve availability only for the returned page.
  - Give the file route a `getSeedCandidate(db, corpusId, type, key, candidateKey)` that selects one row by id. The `candidate_key` is `ingest:<id>` or `search:<id>`.

### I4. `/api/ingest/latest` spawns Python on every 2 s poll and duplicates the resolver a third time

`App.svelte:2330-2345` polls `fetchBibliographyEntries` (`limit=5000`) every 2 s for up to 180 s during each extraction, plus once on completion. Each call (`app.js:6028`) starts a Python interpreter on `ingest_latest.py`. That script:
- loads all 72k corpus works (`build_work_indexes`);
- normalises them in Python;
- runs one alias SQL query per entry (`is_downloaded_by_alias`, an N+1).

This is the same doi / title+authors+year / alias matching as `seed.js` `createStateResolver`, with different rules: no openalex key, no title+year key, and an alias year window that differs from the JS ±1 lookup. So the two can disagree. It is roughly a CPU core busy for the whole extraction.

**Fix:** Port it to Node, as was done for `corpus_list.py`: `listIngestEntries(db, {corpusId, baseName, limit, stateResolver})` in `pipelineReads.js`, reusing a cached resolver from I3. Delete `ingest_latest.py`.

### I5. Synchronous work that blocks the event loop (and the WebSocket) for seconds

- **PDF extraction runs synchronous subprocesses on the main thread** (`app.js:2494-2596`, called from `app.js:4657-4672`):
  - `getPdfPageCountSync`, a `spawnSync` of Python plus pikepdf, about 0.3-1 s;
  - `llmUploadPdfSync`, a `spawnSync` of Python that uploads the PDF to Gemini over the network, seconds;
  - `llmDeleteUploadedSync`, another `spawnSync` network call.

  While they run, every HTTP request and WebSocket frame for every user stalls. **Fix:** run all three through the async spawn helper (`runPythonJson`, or the one from I7) and `await` them inside the extraction task.
- **Graph snapshot build** (`app.js:455-460`): `zlib.gzipSync(fs.readFileSync(...))` on `nodes_meta.json`, `clusters.json`, `nodes.bin` and `edges.bin`, which are multi-MB, on the main thread after every hourly background rebuild and at warm-up. **Fix:** `await pipeline(fs.createReadStream(f), zlib.createGzip(), fs.createWriteStream(f + '.gz'))`, or let `graph_3d_export.py` write the `.gz` files itself.
- **Startup warm-up:** `app.warmSeedState` (`app.js:2634`, run 250 ms after listen) builds the global downloaded index synchronously. That is 73k rows at 0.4 s of SQL plus about 1.5 s of normalisation, so the first requests after boot hang. **Fix:** run it in a `worker_threads` worker, or chunk it with `setImmediate`.
- **`/api/logs/tail`** (`app.js:2424-2492`, `6331`): `readFileSync` and `split` on every rotated file, up to 6 × 10 MB, per request and per "load older" page, then filter. **Fix:** read backwards from the end with `fs.promises.open` and positional reads until `limit` matching lines are found. The cursor becomes a byte offset.

### I6. Kantropos browse: loads every downloaded work, stats every file, and loads all 689k citation edges

`buildKantroposBrowsePayload` (`app.js:1594-1667`) does all of the following per request:
- **Overview:** `listKantroposCorporaOverview(db)` for every target (`app.js:1601`), which `readFileSync`s each `metadata.bib` and regex-counts entries (`app.js:1092-1100`).
- **Three queries over the same works set:** count, id set and `SELECT *` items (`app.js:1470-1519`).
- **Pending items:** `listPendingKantroposWorkItems` (`app.js:1521-1582`) runs `SELECT w.*`, every fat column, over all downloaded works of all assigned corpora (up to about 72k rows × about 4.5 KB). It also calls `fs.statSync` per row (`upstreamFileIsPresent`), then slices to 120 in JS.
- **Graph edges:** `buildKantroposGraphPayload` (`app.js:1409-1414`) runs `SELECT source_id, target_id, relationship_type FROM citation_edges` with no WHERE clause. That is 689,339 rows into JS (3.5 s in the CLI) to keep the edges among at most 240 nodes.

**Fix:**
- Select explicit columns.
- Replace `importedWorkIdSet` with an SQL anti-join.
- `COUNT` and `LIMIT` in SQL.
- `statSync` only the returned page. For the missing-file count, cache per `(target, data_version)`, or count asynchronously with `fs.promises.stat` and a concurrency cap.
- Query edges only for the page's node ids, using the existing `idx_citation_edges_source`/`_target` indexes:
  ```sql
  SELECT source_id, target_id, relationship_type FROM citation_edges
  WHERE source_id IN (SELECT value FROM json_each(?)) AND target_id IN (SELECT value FROM json_each(?))
  ```
- Cache the bib entry count by `(path, mtimeMs, size)`.

### I7. Child-process boilerplate is copied 6 times, and the copies ignore Admin settings (bug)

- `spawnPythonJson` (`app.js:2255-2331`) builds the env with `...appSettingsEnv()`, the Admin-tab overrides.
- Five other places rebuild the env by hand: `app.js:2505-2508`, `2559-2562`, `2585-2588`, `4705-4713`, `4823-4828`. Six `PYTHONPATH:` literals in total.
- **None of the hand-built copies merges `appSettingsEnv()`.** The Python side does not compensate: only `daemon/worker.py` calls `apply_app_settings_env`.
- So `get_bib_pages.py`, `APIscraper_v2.py`, the Gemini upload/delete and the pikepdf probe run with the container env. An LLM provider, API key, `extract_model` or Gemini model set in the Admin tab is silently ignored for PDF extraction, which contradicts the comment at `app.js:2258-2259`.
- The stdout/stderr → `console` + `send()` forwarding is also duplicated, at `4716-4726` and `4831-4841`.
- **Fix:**
  - Add `pythonEnv(extra = {})`, which returns `{...process.env, ...appSettingsEnv(), PYTHONPATH, RAG_FEEDER_DL_LIT_PROJECT_DIR, RAG_FEEDER_DB_PATH, ...extra}`.
  - Add `spawnPython(script, args, {env, onStdoutLine, onStderrLine, lowPriority, parseJson})`, which returns `{child, done}`. Both `runPythonJson` and the extraction runners should use it.
  - Turn the inline `-c` Python snippets into script files under `backend/scripts/`.

### I8. Security: unauthenticated log stream and missing corpus/role checks

1. **Unauthenticated WebSocket** (`index.js:118-138`): `/api/ws` accepts any connection without a token and receives `broadcast()` output for all corpora. That includes filenames, corpus ids, extraction progress and raw script stdout/stderr. The frontend filters by corpus client-side only. **Fix:** check a JWT from `Sec-WebSocket-Protocol` or `?token=` in `server.on('upgrade')`, and send each line only to clients whose user can read the line's `corpus=` tag.
2. **`/api/logs/tail`** (`app.js:6331`): any logged-in user, including viewers, can read the whole app log with `type=app`, and any corpus's pipeline log via `corpus_id=<any>`, with no membership check.
3. **Graph** (`app.js:359-389`, `5865`, `5897`): `corpus_id` is taken from the query string with no `user_corpora` check, the `all` view merges every corpus, and `/api/graph/3d/node/:id` returns any work.
4. **No role checks on scraper and Kantropos routes:** `/api/scraper/*` (`3984-4248`) and `/api/kantropos/corpora/:id/browse|download-ticket` (`3883-3946`) only need authentication. Any viewer can enqueue scrape jobs, stage `metadata.bib` drafts, and list or download works of every corpus assigned to a target, owners' names included.
5. **Stale access:** `req.corpusId = user.last_corpus_id` (`app.js:1827`) is never checked against `user_corpora`. After an owner un-shares a corpus, the former member keeps read access through every read route until they select another corpus. Writes are safe because `requireCorpusWriteAccess` looks the role up.

**Fix:** add `requireCorpusMember` (role lookup sets `req.corpusRole`, 403 if none) to `requireAuth` for corpus-scoped routes. Add `requireAdminMiddleware` (it already exists) to scraper apply and logs.

### I9. About 500 lines of dead in-memory scheduler code, plus dead Python scripts

The routes at `app.js:6396-6524` were reduced to stubs ("DB-driven workers run continuously"), but their machinery is still there:
- `downloadWorkers` and `pipelineWorkers` (`2602-2603`);
- `getOrInit*` (`2751-2792`);
- `tickDownloadWorker` (`2913`);
- everything from `startDownloadWorker` to `stopAllDownloadWorkers` (`3017-3445`).

These are defined once and never called: `startDownloadWorker`, `startPipelineWorker`, `cancelDownloadJobs`, `cancelAllDownloadJobs`, `countDownloadPipelineJobs`, `resolveAutoDownloadBatchSize`, `resolveDownloadWorkerCount`, `aggregateDownloadPayloads`, `emitPromotionEvent`, `pauseAllPipelineWorkers`, `stopAllDownloadWorkers`. `tick*`, `countPipelineTickJobs` and `countQueuedDownloads` are reachable only from them. `pausePipelineWorker` and `stopDownloadWorker` are still called from corpus delete (`4386-4389`), where they are no-ops.

Inside the dead block, `cancelDownloadJobs`/`cancelAllDownloadJobs` and `countDownloadPipelineJobs`/`countPipelineTickJobs` are four copies of the same two-statement pattern, and `pauseAllPipelineWorkers`/`stopAllDownloadWorkers` are copies of each other.

`backend/scripts/downloads_list.py` and `ingest_runs.py` are referenced nowhere. `ingest_stats.py` is referenced only in a comment, since the reads were ported to `pipelineReads.js` and the tests do not use them as oracles.

**Fix:** delete all of it. Also simplify the stub routes to one `dbDrivenWorkerStatus(kind)` helper; today the status JSON is duplicated across six handlers.

### I10. Redundant indexes add write cost for the workers, and the schema is defined in three places

Live index set:
- `corpus_works`: the PK autoindex `(corpus_id, work_id)`, **plus** `idx_corpus_works_lookup(corpus_id, work_id)`, identical and created by `app.js:880`, **plus** `idx_corpus_works_corpus(corpus_id)`, a prefix.
- `work_aliases`: `idx_work_aliases_norm_title` ≡ `idx_work_aliases_title`, and `idx_work_aliases_owner` ≡ `idx_work_aliases_work`.
- `search_results`: `idx_search_results_run(search_run_id)` is a prefix of `idx_search_results_run_openalex`.
- `seed.js:624-626`: all three `*_lookup` indexes are the PK or a prefix of it.

Every promotion or bulk insert into `corpus_works` and `search_results` maintains 2-3 copies. **Fix:** drop them (`DROP INDEX IF EXISTS idx_corpus_works_lookup, idx_corpus_works_corpus, idx_work_aliases_norm_title, idx_work_aliases_owner, idx_search_results_run, idx_seed_*_lookup`) and stop creating them.

Schema ownership is split three ways:
- `ensureAuthSchema` (`app.js:647-888`) creates `works` but none of its indexes;
- `db_manager.py:360-376` and `migrate_to_works.py:165-172` each create overlapping, slightly different sets.

A fresh DB booted by Node has no `idx_works_origin` or status indexes until Python runs. **Fix:** one `schema.sql` (or a Python `ensure_schema`) read by both sides, with a `PRAGMA user_version` gate so boot skips the 30 or so `CREATE ... IF NOT EXISTS`/`PRAGMA table_info` statements, and new indexes such as I1 and I2 are added in exactly one place.

### I11. Route-guard and response boilerplate not factored (more than 3× each)

Counts are for `app.js`:

| Pattern | Count | Proposed helper |
|---|---|---|
| `['pdf','search'].includes(sourceType)` validation | 5 routes, plus 3 in `seed.js` | `parseSeedSource(req)` or `app.param` middleware that sets `req.seedSource` |
| Inline `SELECT role FROM user_corpora` (`4268`, `4288`, `4371`), even though `getCorpusRoleForUser` exists, and `canConfigureCorpus` checks repeated | 4 | `requireCorpusRole(...roles)` middleware |
| "last corpus fallback" `SELECT corpus_id ... ORDER BY created_at DESC LIMIT 1` plus `UPDATE users`; the accept-invite and reset-password response tails (`3778-3792` vs `3844-3858`) are identical | 3 | `ensureLastCorpus(user)` / `authResponse(user)` |
| `new Database(DB_PATH,{readonly:true})` per request, which opens the file and parses the schema each time, instead of `authDb` | 4 (`3957`, `5250`, `5699`, `5908`) | use `authDb` |
| `process.env.RAG_FEEDER_STUB === '1'` branches inside handlers | 10 | stub router mounted first |
| `catch (error) { console.error(...); return res.status(500).json({error: error.message \|\| ...}) }` | 38 | `route(handler)` wrapper with one error mapper |
| Download-ticket mint/verify pairs (`2653-2724`, `3903-3946`, `5657-5691`) | 2 near-identical | `createTicketStore(ttl)` |
| `buildKeywordSearchExpansion`/`buildUploadedDocsExpansion` (`1988-2012` vs `2146-2178`) and their `apply*Args` (`2014-2031` vs `2180-2200`); `RECURSION_DEFAULTS.keyword` and `.uploadedDocs` are identical apart from one key (`2391-2411`) | 2 + 2 | `buildExpansion(body, defaults)` + `expansionArgs(exp, {explicitUpstreamFlag})` |
| `sendInviteEmail`/`sendPasswordResetEmail` (`3488-3551`) | 2 | `sendTokenMail({table, hours, path, subject, intro})` |

---

## Minor

1. **Helpers defined more than once:**
   - `tableExists` ×3 (`app.js:332`, `seed.js:3`, `pipelineReads.js:139`);
   - `nonNegInt` ×2 (`pipelineReads.js:143`, `corpusList.js:279`);
   - column probes ×4 (`ensureColumn`, `listTableColumns`, `hasColumn`, `tableHasColumn`, plus inline `PRAGMA table_info` at `app.js:1014,1033` and `seed.js:137`);
   - JSON-parse wrappers ×3 (`safeJsonParse`, `seed.parseJson`, `corpusList.parseJson`);
   - `normalizeGraphDoi`/`normalizeGraphOpenAlexId` (`app.js:1308-1320`) vs `seed.js:45-58`. The OpenAlex variants differ: `seed.js` keeps the `https://openalex.org/` prefix and uppercases it, so the same work gets different keys.

   **Fix:** one `backend/src/dbUtil.js` and one `normalize.js`.
2. **`tableExists` on request paths:** 39 calls (one `sqlite_master` query each) on request paths for tables that `ensureAuthSchema` or `ensureSeedSchema` just created. Cache them in a `Set` at startup.
3. **Statements not hoisted:** 143 `authDb.prepare(...)` calls are inline in handlers, including `requireAuth` → `getUserById` on every request. Each one recompiles. Hoist hot ones into a `stmts` object built in `createApp`.
4. **Graph node lookup scans all of `works`** (`app.js:5936-5942`): `WHERE w.id = ? OR w.openalex_id = ? OR w.doi = ?`. `doi` is unindexed, so the plan is `SCAN w` (0.26 s). Dispatch on id shape: integer → PK, `W\d+` → `openalex_id`, otherwise `normalized_doi`, which has a unique index.
5. **Cluster detail** (`app.js:5973`) parses the whole multi-MB `clusters.json` per click. Cache the parsed array by `(key, mtimeMs)`.
6. **Synchronous file logging** (`index.js:26-57`): every `console.*` call and every `broadcast()` does `existsSync`, `statSync` and `appendFileSync`, three synchronous syscalls per line. Script stdout is broadcast per chunk. Use one `fs.createWriteStream` per file with a byte counter for rotation.
7. **WebSocket broadcast fan-out:** `broadcast()` sends every line to every client, and each client's `shouldTriggerPipelineRefresh` can schedule a refresh 300 ms later (`App.svelte:4641-4645`). That couples log volume to DB load. Send structured `{kind:'refresh', corpus_id}` events at most once per second per corpus instead of having clients parse log text.
8. **bcrypt blocks the event loop:**
   - `bcrypt.compareSync` and `hashSync` (cost 10, about 70 ms each) block it in login, register, invite, accept and reset (`3565`, `3684`, `3711`, `3766`, `3837`). Combined with no rate limiting on `/api/auth/login`, that is a cheap event-loop DoS. Use `await bcrypt.compare/hash` and a small per-IP limiter.
   - Boot rehashes and rewrites the admin password on every start (`app.js:898-910`). Compare first, write only on mismatch.
9. **Fallback JSON scan in `parseJsonFromOutput` is cubic** (`app.js:1861-1872`): O(L³) in output lines, a `join` inside nested loops. Have scripts print a sentinel line (`__RESULT__{json}`) or emit the result last, and parse only that.
10. **Multi-statement writes without a transaction:** `createUserWithDefaultCorpus` (`3469-3486`), `POST /api/corpora` (`4250-4264`) and `scraper/apply`'s per-artifact `UPDATE`s (`4176`) are not wrapped in `db.transaction`. A crash can leave a user without a corpus, and the separate fsyncs cost time.
11. **Duplicate `--corpus-id`:** `/api/ingest/import-seed` (`6060`) pushes `--corpus-id` and `runPythonJson` appends it again. Harmless with argparse, but a sign of the duplication.
12. **Unindexed `pipeline_jobs`:** `findExistingPipelineJob` and `/api/pipeline/jobs/summary` filter on `corpus_id`, `job_type` and `status` and order by `created_at` with no index. It is 7k rows today and grows forever. Add `CREATE INDEX idx_pipeline_jobs_corpus ON pipeline_jobs(corpus_id, job_type, status, created_at)` and prune finished jobs.
13. **Per-request provenance labels:** `loadPdfSourceLabels`/`loadSearchSourceLabels`/`loadSeedProvenance` (`corpusList.js:192-245`) rebuild their maps per request. The tables are small today (12 / 102 / 8 rows), so this is fine, but they belong in the same `data_version` memo as I1.
14. **Unescaped username in email HTML:** invite and reset emails interpolate `username` into HTML unescaped (`3503`, `3535`). The input is admin-supplied, but escape it anyway.
15. **Extra `existsSync` calls:** `findDownloadedFilePath` (`1913-1916`) calls `existsSync` again on a path `resolveDownloadedFilePath` just probed. Return the hit from the resolver.
16. **Hard-coded model names** in route code (`app.js:4759`, `4955`, `4964`) bypass `APP_SETTING_DEFS`. Move them into settings defaults.

---

## Suggested order of work

1. I1 + I2: add two indexes (`idx_works_state`, `idx_works_year_id`), write the page query and the status counts as `EXISTS` probes, and add a `data_version` memo. This is small and covers most of the per-poll cost.
2. I7: add `pythonEnv`/`spawnPython`, which fixes the Admin-settings bug, then I5 (no `spawnSync` or `gzipSync` on the main thread).
3. I8: WebSocket auth and the corpus-member middleware.
4. I3/I4: cache the state resolver and port `ingest_latest` to Node.
5. I9/I10: delete dead code and indexes, and unify the schema.

## What is already good

- `listCorpus` and `listDownloads` read inside one transaction, so counts and pages come from one WAL snapshot. Id lists go through `json_each(?)` instead of thousands of bound parameters. The Python→SQL ports keep golden-test parity with the script oracles.
- The global "downloaded anywhere" index in `seed.js` is cached per DB handle with a fingerprint plus max age. The same idea just needs extending to the corpus-local half.
- `dismissAllSeedCandidates` uses a single `INSERT ... SELECT` for search seeds. Search-seed candidates are paged and sorted in SQL. The bulk unlink is transactional.
- 3D graph:
  - snapshots are content-keyed;
  - builds run at low priority and are shared between concurrent requests;
  - rebuilds are stale-while-revalidate with an atomic directory swap;
  - pre-gzipped files are streamed with abort handling and cache headers.
- Big promotion and expansion payloads go through temp files rather than argv (ARG_MAX-safe). Search runs are registered, cancellable, and their rows are settled when the child dies.
- Path-traversal guards are in place on upload, extraction, seed-document and export paths. Download tickets are short-lived and single-item. Admin secrets are never echoed back.
- Several earlier hot spots have comments explaining previous fixes: the 8-COUNT backlog collapse and the `entry_count` join removal from the seed list. The remaining cost is mostly the fat-row layout plus missing covering indexes, not the query style.
