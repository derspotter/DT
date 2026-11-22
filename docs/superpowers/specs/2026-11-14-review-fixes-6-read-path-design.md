# Review fixes, phase 6: read-path performance — design

Date: 2026-11-14. Branch `review-fixes-6` (worktree from master `0702e47`). Source: Phase 2 of
`/home/jay/DT/.superpowers/sdd/2026-11-13-review-fixes-5-remaining/inventory.md` ("Proposed
fix phases", 27 rows) plus BE-51, the row appended at the end of that file:
- FE-14, FE-17, FE-18, FE-20, FE-21, FE-59;
- BE-03 to BE-06, BE-08 to BE-12, BE-34 to BE-39, BE-42, BE-45, BE-47, BE-48;
- PY-25, PY-31;
- BE-51 (graph-node lookup full scan).

Same method as phase 5: Opus-medium implementers under subagent-driven development, Codex
`gpt-6.1-sol` xhigh as the review gate.

The owner's rule for this phase is "secure and fast, not over-engineered". A row is fixed with
the smallest change that removes a measured or obvious cost. A cache, an index or an
abstraction whose benefit is speculative is rejected, and the non-goals say why. A
user-visible change keeps today's behaviour unless the row is a bug. The owner's
2026-11-14 decisions (`owner-decisions-2026-11-14.md`) belong to phase 7. The one exception
is BE-46: the 30-day `pipeline_jobs` retention prune shapes the BE-45 index (D16).

Every row was re-read in the code on `0702e47`. Phase 5 (PR #80) moved most line numbers. The
references below are from `0702e47`.

**Rows already fixed:** none. Every row is still reproducible on `0702e47`.

**Duplicate:** BE-51 is BE-36: the same query at `app.js:5548`. Both are closed by D7. The
inventory also uses the id BE-51 for an unrelated Phase 3 row (stale `/tmp/dt-jest-*`
directories). This spec uses BE-51 only for the graph-node lookup.

**Ruled out (non-goals, with reasons):** BE-03, BE-35, BE-38, BE-39, BE-47, FE-20, FE-21,
PY-25, PY-31. BE-34 is fixed only in part, where the call runs once per candidate. FE-17 is
fixed only in part: `UpstreamBrowser` stays in the main chunk (D15). Where a
decision is a product choice, it says **controller call**. Those calls go to the owner as a
list at the end.

## Measurements

These numbers come from a synthetic database built with `DatabaseManager`'s schema and the
master code from `/home/jay/DT/backend/src`. It holds 72,000 works, all in corpus 1, a third of
them downloaded, and 689,000 `citation_edges`. The work rows are short, so the live database
is slower. Treat the numbers as ratios; the absolute values are only indicative.
`literature.db` was not opened.

| Path | Today | Measured alternative |
|---|---|---|
| `createStateResolver(db, 72k-corpus)`, global half cached (BE-04/05) | 845 ms | not built: 0 ms |
| Kantropos edges: all rows (BE-12) | 2,925 ms | `IN (json_each)` on the UNIQUE index: 23 ms |
| Kantropos pending: `SELECT w.*` over the downloaded works (BE-11) | 685 ms | `id, file_path, corpus_id` only: 63 ms |
| `statSync` on 24k missing files, throwing (BE-11) | 322 ms | `throwIfNoEntry: false`: 39 ms (async `stat`: 1,369 ms wall) |
| Graph node lookup `id = ? OR openalex_id = ? OR doi = ?` (BE-36) | `SCAN w`, 21 ms (0.26 s live) | PK / partial-index probe |
| `listCorpus` with `q` (BE-03) | 309–451 ms | native ASCII prefilter: ×1.4 at best, slower on common needles |
| `listCorpus` title sort (BE-03) | 129 ms | native `lower()` for ASCII rows: 58 ms |
| One log line: `existsSync`+`statSync`+`appendFileSync` (BE-38) | 40 µs | `appendFileSync` only: 24 µs |

## Problems (verified against master `0702e47`)

### Backend: seed sources

1. **The seed list builds the full state resolver on every poll, only to count (BE-04).**
   `/api/seed/sources` (`app.js:4722-4741`) calls `listSeedSources(..., { withStateCounts:
   false })`. That call still builds `createStateResolver` (`seed.js:902`). The resolver loads
   every corpus row (`seed.js:332`, 845 ms at 72k) and builds 7 indexes. It then lists and
   resolves every candidate of every pdf seed (`:972`), with a `prepare` and a file probe per
   candidate (`:399-418`). All of that produces `candidate_count`, which depends only on the
   row text, the dismissals and `q`. The comment at `:295-297` still says the corpus-local half
   holds "a few hundred rows".
2. **The seed candidate file route builds the whole resolver to find one candidate (BE-05).**
   `app.js:4843-4847` calls `listSeedCandidates(..., { stateResolver: null }).find(...)`: a full
   resolver build plus a resolved list of the whole seed, per click.
3. **Boot blocks on the global index build (BE-09).** `index.js:105` calls
   `app.warmSeedState` 250 ms after `listen`. It builds the cross-corpus downloaded index
   synchronously (`app.js:2609-2617`, about 1.5 s live), while every client reconnects after a
   deploy. Once problem 1 is fixed, no poll needs that index.
4. **Repeated probes on the candidate path (BE-48, BE-34 in part).** `findDownloadedFilePath`
   (`app.js:1985-1988`) calls `existsSync` again on the path that `resolveDownloadedFilePath`
   (`:1949-1983`) has just found to exist. The candidate file route probes it a third time
   (`:4864`). `resolveDownloadedAvailability` runs `tableExists(db, 'works')` once per
   candidate (`seed.js:406`).

### Backend: Kantropos browse and 3D graph

5. **The browse graph reads every citation edge (BE-12).** `app.js:1520-1525` runs `SELECT ...
   FROM citation_edges` with no `WHERE` and keeps only the edges among at most 1,000 nodes (two
   lists of up to 500). That costs 2.9 s on the synthetic DB, and about 3.5 s live.
6. **The browse pending list reads and stats every downloaded work (BE-11).**
   `listPendingKantroposWorkItems` (`:1632-1693`) selects `w.*` for every downloaded work of
   the assigned corpora and maps each one to a browser item. It runs `statSync` per row, which
   throws on every missing file (`upstreamFileIsPresent`, `:1695-1703`). The payload then
   keeps at most 500 rows (`:1722`). `listKantroposCorporaOverview` (`:1356-1415`), called by the
   browse and by `/api/kantropos/corpora` (`:3494`), reads and regex-counts each target's whole
   `metadata.bib` per request (`countBibEntriesInFile`, `:1215-1223`).
7. **The graph node lookup scans works (BE-36 = BE-51).** `app.js:5548`: `w.id = ? OR
   w.openalex_id = ? OR w.doi = ?`. Raw `doi` has no index, so the OR cannot use one, and the
   plan is `SCAN w`. The frontend sends the numeric `work_id` (`ThreeGraph.svelte:1127`), which
   the primary key alone would answer.
8. **The cluster detail parses the whole `clusters.json` per click (BE-37).** `app.js:5585`. The
   frontend already holds that same file: `fetchGraph3DSnapshotResources` loads it, and
   `ThreeGraph` keeps it in `graphData.clusters`. The detail request therefore returns an
   object the client already has (`ThreeGraph.svelte:1139-1160`).
9. **The snapshot build gzips on the main thread (BE-08).** `app.js:504-508` runs
   `gzipSync(readFileSync(...))` on multi-MB files after every hourly rebuild. All requests and
   the log socket stall meanwhile.

### Backend: logs and child output

10. **The log tail reads every rotated file per request (BE-10).** `readLogWindow`
    (`app.js:2482-2516`) reads and splits all of up to 6 × 10 MB through `readFileSync`
    (`:2448-2475`), for each tail and each "Load older".
11. **History and live lines cover different corpora when the filter is off (FE-59).**
    `getLogsCorpusId` returns `null` when "current corpus only" is off (`App.svelte:1270-1273`).
    The tail then falls back to the current corpus (`app.js:5985`). The live socket meanwhile
    delivers every line the user may read: admins get every line, members get the lines tagged
    with one of their corpora (`logSocket.js:62-67`).
12. **The JSON fallback parser is O(L³) (BE-42).** In `parseJsonFromOutput`
    (`pythonProcess.js:36-46`), every line containing `[` or `{` is a start, and every later
    line is tried as an end, with a fresh `join` per pair. Log lines such as `[DB Manager] …`
    are starts. A child that prints a few thousand tagged lines and no JSON pins a CPU core
    before the error is thrown.

### Backend and frontend: Corpus

13. **The stage filter only filters the loaded page (FE-14).** `filteredItems`
    (`Corpus.svelte:116-132`) filters `corpusItems`, while the labels show server totals
    (`:216-229`). "Downloaded (5,000)" can list a handful of rows. "Show more" pages through
    the unfiltered corpus, and the scroll trigger may never fire, because the short list does
    not scroll. This is a bug.

### Frontend

14. **Extraction completion awaits three independent loads in a row (FE-18).**
    `App.svelte:1058-1063`: `focusSeedSource`, then `loadIngestStats`, then `loadIngestRuns`. The
    card reaches its 1.2 s finish timer two round trips late.
15. **The Graph and Scraper tabs are in the main chunk (FE-17).** `App.svelte:58`, `:69` import
    `ThreeGraph` (90 KB of source) and `ScraperLab` (23 KB) statically. Three.js itself is
    already a dynamic chunk (`ThreeGraph.svelte:1647-1650`). `UpstreamBrowser` (`:67`) is the
    Korpus Management tab, which most users open. It takes two `bind:` props
    (`App.svelte:5548-5549`).

### Schema and dead code

16. **`pipeline_jobs` has no index (BE-45).** The daemon's claim and probes (`fetch_next_job`, `worker.py:821`;
    `:884-908`), its recovery (`:577`, `:719`, `:785`), and the backend dedupe
    (`app.js:2958-2971`) all filter on `status`, and each one scans the table. 6.8k cancelled
    jobs are never pruned today. Phase 7 adds a 30-day prune
    (`status IN ('completed','failed','cancelled') AND finished_at < …`).
17. **`/api/ingest/latest` is dead (BE-06).** It has no caller since #77. These pieces remain:
    - the route `app.js:5640-5655` and `INGEST_LATEST_SCRIPT` (`:164`);
    - `backend/scripts/ingest_latest.py`;
    - the export `fetchBibliographyEntries` (`frontend/src/lib/api.js:298-302`);
    - a README line (`frontend/README.md:23`);
    - two identity assertions in `dl_lit_project/tests/test_identifiers.py:56`, `:199`;
    - a call in the opt-in `frontend/tests/pipeline-perf.real-pdf.spec.ts:218`.

## Goals

- No poll and no click does work that grows with corpus size when the answer does not need it,
  on these paths: the seed list endpoint (`/api/seed/sources`), the Kantropos browse, the graph
  node and cluster clicks. The seed-poll guarantee covers `/api/seed/sources` only: the
  candidates route, which the expanded seed polls while a search runs, keeps its cost (D3). The
  seed candidate file drops the corpus-local resolver and the full-seed listing; it still uses
  the cached global index and its fingerprint (D2, D3).
- The named read paths read no multi-MB file and gzip nothing synchronously: the log tail
  (which reads only what the window needs), the Kantropos browse and overview's `metadata.bib`
  count, and the snapshot build's gzip. Other handlers are out of scope; Scraper Apply, for
  one, still reads the whole `metadata.bib` synchronously (`app.js:3711`).
- Every claim is pinned by a test of the mechanism: a query plan, a statement or call count, a
  request count, or an absent sync call. No test uses wall-clock thresholds.
- Each behaviour change is the minimum the row needs. Each has a parity test where the output
  must stay identical.

## Non-goals

- **BE-03 (`q` filter and sort UDFs) is ruled out.** The `q` filter must match Python's
  `str.lower()` (non-ASCII folding, the Kelvin sign) and the joined title/author/venue text
  (matches across fields). An exact native prefilter therefore still sends every non-ASCII row
  and every multi-word needle to the UDF. Measured, it gains at most ×1.4, and it is slower for
  common needles. Only a maintained lowercase search column would remove the cost, and both
  the Python and the Node writers would have to maintain it: schema work out of proportion to
  this phase. The title sort could gain ×2 (129 → 58 ms) with a native `lower()` for ASCII rows.
  That saves 70 ms on a sort click and is not worth a second code path. The filter input is
  already debounced (phase 5 D1).
- **BE-34 beyond the per-candidate call, BE-35 and BE-47 are ruled out.** A `sqlite_master`
  lookup or a `prepare` costs microseconds per request. Hoisting the 65 `prepare` sites in
  `app.js` is a broad rewrite that AGENTS.md advises against. The provenance tables of BE-47
  are small, and the inventory itself lists no current cost.
- **BE-38 is ruled out.** A log line costs 40 µs, and dropping the size probe saves 16 µs
  (1.6 ms per 100 lines). In exchange, the rotation would change, and the worker's `samestat`
  handle check (`worker.py:114-120`) depends on that rotation.
- **BE-39 is ruled out.** Log-driven refreshes already coalesce: `scheduleRefresh` keeps the
  earliest pending timer (`App.svelte:1343-1346`). A log line asks for a refresh in 1.5 s, a
  worker tick line in 300 ms (`REFRESH_WORKER_TICK_MS`, `App.svelte:4474`). Log volume
  therefore triggers at most one refresh per pending timer per tab: at most one per 300 ms
  while worker ticks arrive, else one per 1.5 s. That rate does not grow with log volume.
  Structured refresh events would be a refactor with no measured gain.
- **FE-20 and FE-21 are ruled out.** Uploads persist a few hundred bytes a handful of times per
  file. The handlers are event-driven, not per log line. The Korpus Management filter runs over
  at most 1,000 loaded rows; the server clamps each list at 500.
- **PY-25 is ruled out.** The quota snapshot costs about five metadata syscalls, with no fsync,
  against an OpenAlex round trip of 100 ms or more.
- **PY-31 is ruled out.** The export runs out of process, at low priority and hourly, and the
  previous snapshot is served while it runs. It streams the SQLite rows through a cursor, but it
  materialises the accepted edges (those among the allowed nodes) before the node cap
  (`graph_3d_export.py:923-925`). The cap selects nodes by degree, which needs every accepted
  edge, so trimming that list would change the node selection. The out-of-process, hourly
  cadence keeps the cost off the request path; the work stays deferred.
- **The candidates route still builds the corpus-local resolver.** After D1 it runs only on a
  user's expand, page, sort or filter, and on the expanded seed's background reload while a
  search runs (`App.svelte:3194-3198`). It also calls `loadGlobalDownloaded`, which computes
  the works fingerprint (a grouped count over `works`, `MAX(id)` and an alias count,
  `seed.js:301-310`) before it consults its cache, and rebuilds the index when the fingerprint
  changes or the 5-minute max age expires. The fingerprint design is unchanged in this phase.
  Two fixes were rejected:
  - A per-corpus cache would need a fingerprint, which misses same-count state swaps (the global
    cache accepts 5 minutes of staleness for that).
  - Per-candidate indexed matching would need the JS and Python title normalisations to agree.

  Neither is justified without a live measurement.
- **The `/api/pipeline/jobs/summary` order scan gets no index.** After the phase-7 retention
  the table holds 30 days of jobs, so the scan is bounded.
- **A tail with "current corpus only" on still excludes untagged lines, which the admin's live
  view shows.** This admin-only difference predates FE-59 and stays.
- No README change beyond the dead `/api/ingest/latest` line.

## Decisions

### Backend: seed sources (Task 1)

**D1. The seed list counts without resolving (BE-04).**
- `listSeedSources` builds the resolver lazily. A `getResolver()` closure creates it on first
  use; a passed `stateResolver` is used as is. It is used only for `withStateCounts`.
- With `withStateCounts: false`, a pdf seed's `candidate_count` comes from the new
  `countPdfCandidates(db, corpusId, sourceKey, needle) -> number`, which does the following:
  1. It reads `SELECT id, title, authors, source, publisher FROM ingest_entries WHERE corpus_id
     = ? AND ingest_source = ?`.
  2. It drops dismissed keys: the same set as `loadDismissedCandidateKeySet`, `ingest:<id>`.
  3. It counts the rows for which `matchesSeedQuery({ title: row.title || '', authors:
     parseNameList(row.authors), source: row.source || null, publisher: row.publisher || null },
     needle)` holds. Those are the fields `normalizePdfCandidate` builds.
- A seed with count 0 is hidden, as today.
- `countSeedCandidates` uses the same helper for its pdf branch. It loses the
  `stateResolver` option, which no branch needs any more, and the candidates route stops
  passing it.
- The search branch is unchanged (SQL count). The comment at `seed.js:295-297` is corrected:
  the corpus-local half holds every corpus row.

**D2. The candidate file route resolves one candidate and serves only a readable work
(BE-05).** New
`findSeedCandidateDownload(db, corpusId, sourceType, sourceKey, candidateKey, canReadWork) -> number | null`
in `seed.js` returns the work id to serve, or `null` for 404. `canReadWork(workId) -> boolean`
is the existing work-read rule, passed in by the route as `(id) => workReadableByUser(authDb,
req.user, id, authConfig)` (`app.js:468`): an admin reads every work, anyone else only works in a
corpus they belong to. There is no cross-corpus PDF sharing. It works in five steps:

1. The `candidateKey` must match `/^(ingest|search):([1-9]\d*)$/`, with the prefix that belongs
   to `sourceType` (`ingest` for `pdf`, `search` for `search`). Anything else gives `null`. The
   key bound below is the canonical `buildCandidateKey(sourceType, id)`.
2. The one row is read with today's scoping:
   - pdf: `ingest_entries WHERE id = ? AND corpus_id = ? AND ingest_source = ?`. The dismissal
     is one probe by the table's full primary key: `SELECT 1 FROM seed_candidates_dismissed
     WHERE corpus_id = ? AND source_type = 'pdf' AND source_key = ? AND candidate_key = ?`, bound
     with the canonical key. A hit gives `null`.
   - search: the source key must be a canonical run id (`/^[1-9]\d*$/`), else `null`, and
     `String(runId)` is passed as `sourceRef`, so an alternate spelling such as `07` cannot
     miss the dismissal rows stored under `7`. The read uses the candidate list's join, `FROM
     search_results sr JOIN search_run_corpora src ON src.search_run_id = sr.search_run_id JOIN
     search_runs s ON s.id = sr.search_run_id WHERE <searchCandidateWhere(corpusId, runId,
     String(runId), '')> AND sr.id = ?` (`seed.js:817-820`). That clause already excludes
     dismissed rows and other corpora's runs.
3. The row goes through `normalizePdfCandidate` or `normalizeSearchCandidate` only to build the
   match fields, with a stub bundle (`resolveState: () => 'pending'`, `resolveDownloadedWorkId:
   () => null`, `isInCorpus: () => false`).
4. The matched ids are computed once: `findIdsWithAliases(candidate, global.lookup,
   global.aliasIndex)` with `global = loadGlobalDownloaded(db)` (the cached half). The global
   index holds downloaded works only, so every matched id is a downloaded work.
5. The matched ids, in match order, are filtered through `canReadWork`. Among the readable ids,
   the first one in the current corpus wins (one prepared probe, `SELECT 1 FROM corpus_works
   WHERE corpus_id = ? AND work_id = ?`, reused per id). Otherwise the first readable id wins.
   No readable id gives `null`.

For every match the caller may read, the decision is the one the full resolver makes today; a
parity test pins it for an admin. A match that lies only in corpora the caller cannot read now
gives 404, where today the route served that file. The route keeps its read-only row fetch, path
resolution and `res.download`.

**D3. No boot warm-up (BE-09), controller call.** `app.warmSeedState` and the `index.js` timer
are removed. After D1, the seed-list poll builds no index. The candidates route and the
candidate file still call `loadGlobalDownloaded`: they pay the fingerprint on every call and the
build when the fingerprint changes or the max age expires, which the existing cache already
decides. The candidates route also runs from the expanded seed's background reload while a
search runs (`App.svelte:3194-3198`). The first such call after a deploy pays for the build,
about 1.5 s live.

**D4. One probe per path (BE-48, BE-34 in part).**
- `app.js` gains `downloadedFilePathCandidates(storedPath, context) -> string[]`: today's
  candidate list, built without any probe.
- `resolveDownloadedFilePath` returns the first existing candidate, else the first candidate,
  else `null`.
- `findDownloadedFilePath` returns the first existing candidate or `null`, so each path is
  probed once. It is exported for the test.
- The candidate file route uses `findDownloadedFilePath` instead of `resolveDownloadedFilePath`
  plus `existsSync`.
- `createStateResolver` reads `tableExists(db, 'works')` once, outside the per-candidate
  closure.

### Backend: Kantropos browse and 3D graph (Task 2)

**D5. Edges by index (BE-12).** `buildKantroposGraphPayload` reads only the edges among its
nodes:

```sql
SELECT source_id, target_id, relationship_type FROM citation_edges
WHERE source_id IN (SELECT value FROM json_each(?)) AND target_id IN (SELECT value FROM json_each(?))
```

Both parameters are `JSON.stringify([...allowedNodeIds])`. The plan uses the table's UNIQUE
autoindex (`sqlite_autoindex_citation_edges_1 (source_id=? AND target_id=?)`, 23 ms). No new
index is needed. `addEdge` keeps its checks.

The contract changes in two ways, and both are documented as the contract:
- Endpoint ids are canonical (trimmed) node ids. The index compares stored values exactly, so a
  stored endpoint with surrounding whitespace, which `addEdge` trimmed before, no longer
  matches.
- `graph.edges` is an unordered set. Its order follows the index instead of the table's rowid
  order.

The golden test compares the edges as sets and includes a padded endpoint that is expected to
drop out.

**D6. Pending works in two passes in one snapshot; cheap stats; bib count keyed by stat
(BE-11), controller call (the memo).**
1. Pass 1 and pass 2 run inside one short read transaction (`db.transaction(fn)()`, as
   `listCorpus` does): in WAL mode both SELECTs see one snapshot, so a worker commit between
   them cannot make the summary and the page disagree. Nothing asynchronous and no file probe
   runs inside it.
2. **Pass 1** reads `w.id, w.file_path, cw.corpus_id`, with the same joins and `WHERE` as today,
   ordered `COALESCE(w.year, 0) DESC, w.id DESC`. From those rows it computes, in that order:
   - each assigned corpus's `downloaded_count` (distinct works per corpus), before the
     imported-set exclusion. This replaces the separate grouped query (`app.js:1732-1745`),
     which read the same rows again;
   - with the imported-set exclusion: the distinct work ids with their `file_path`, and each
     work's corpus ids, for the per-corpus `pending_count`.
3. **Pass 2** reads today's full `SELECT w.*, c.id AS corpus_id, …` with the extra term `AND
   w.id IN (SELECT value FROM json_each(?))`, bound with the first `pendingLimit` work ids of
   pass 1, in today's order (`…, LOWER(c.name), c.id`). It maps those rows exactly as today.
4. After the transaction closes, the file-missing set is computed with one probe per distinct
   work (a work in several corpora has several rows and is probed once), and the page items get
   their `file_missing` flags.

The function returns `{ items, totalCount, missingFileCount, pendingCountByCorpus,
downloadedCountByCorpus }`. `buildKantroposBrowsePayload` derives every summary field from these
values with today's formulas. The JSON payload is unchanged; a fixed-fixture test pins it,
including a file-missing work outside the page.

`upstreamFileIsPresent` uses `fs.statSync(raw, { throwIfNoEntry: false })?.isFile() === true`
inside its `try`, which drops the exception cost of a missing file (322 → 39 ms per 24k). It
stays synchronous: async `stat` takes 1.4 s of wall time for the same set.

`countBibEntriesInFile(filePath, stats) -> Promise<number | null>` keeps a module-level `Map` of
`path → { mtimeMs, size, count }`. A hit (same `mtimeMs` and `size`) returns the count without a
read. A miss reads the file with `fs.promises.readFile`, counts, and stores the entry only when
the read succeeded; a failed read returns `null` and stores nothing. The map is bounded by the
configured Kantropos targets.

`listKantroposCorporaOverview(db, { targetId } = {})` becomes async. With `targetId` it stats
and counts only that target; the browse passes its own target, so it no longer counts the other
targets' files. `/api/kantropos/corpora` passes none and counts every target, each through the
memo. The bib count runs after the read transaction.

**D7. Graph node lookup by index (BE-36 = BE-51), controller call.** New exported
`findGraphNodeWork(db, nodeId, selectList, columns) -> row | undefined` in `app.js` makes at
most one probe per form. The node id is trimmed by the route, as today. The accepted forms are:
- plain decimal digits (`/^\d+$/`, for example `12`; leading zeros are read as `Number` does)
  give `WHERE w.id = ?`;
- the exporter's fallback id `work:<digits>` (`derive_node_id`, `graph_3d_export.py:148`) gives
  the same `WHERE w.id = ?` probe; today it always answers 404;
- any other id first tries `WHERE w.openalex_id = ? AND w.openalex_id != ''` (the partial
  unique index `idx_works_openalex_id`);
- after that it tries `WHERE w.normalized_doi = ? AND w.normalized_doi != ''`
  (`idx_works_normalized_doi`) when the column exists.

Other numeric spellings that `Number()` accepted today (`12.0`, `1e1`, `0x0c`) no longer resolve
as ids; they fall through to the openalex and DOI probes and answer 404.

The explicit `!= ''` term is what lets SQLite use the partial indexes (the PY-02 lesson).
Raw `w.doi = ?` is dropped. A DOI node id is `normalize_doi(doi)` (`graph_3d_export.py:148-149`),
which is the `normalized_doi` key. Today's raw comparison misses most stored DOIs, which are
lowercase.

The lookup is scan-free on both schemas. `ensureAuthSchema` creates the two partial indexes
with the same DDL as `db_manager.py`'s `WORKS_INDEXES` (`CREATE UNIQUE INDEX IF NOT EXISTS
idx_works_openalex_id …` and `… idx_works_normalized_doi …`), each when `works` has the column.
A database the Python side initialised already has them, so the statements are no-ops there;
production databases are Python-managed.

**Accepted exception to "scan-free".** On a Node-only database with duplicate `openalex_id` or
`normalized_doi` values, `CREATE UNIQUE INDEX` fails. `ensureAuthSchema` catches only that
failure (`error.code === 'SQLITE_CONSTRAINT_UNIQUE'`) and logs it with `console.error`, naming
the index and the duplicated column. Any other DDL error is rethrown. Boot continues, and on
that schema the symbolic probes run as scans, with the same results: the right work, or 404.
This is a degraded development case. There is no "unavailable" mode.

The route keeps its 404 for unreadable works, for every accepted form.

**D8. Cluster detail from the loaded snapshot (BE-37), controller call.**
- The cluster panel renders `selectedClusterData`, which `ThreeGraph` already derives from the
  selected cluster and `graphData.clusters` (`ThreeGraph.svelte:1838`), the same
  `clusters.json` content. It sends no request.
- `selectedClusterDetail`, `selectedClusterPanel`, `loadClusterDetail`, `clusterDetailCache`,
  `fetchGraph3DClusterDetail` and the route `/api/graph/3d/cluster/:snapshotKey/:clusterId`
  (`app.js:5574-5592`) are removed. A request to the old route answers 404.
- `graph-access.test.js` keeps the scope checks on the snapshot file route
  (`/api/graph/3d/snapshot/:key/clusters.json`).

A background rebuild between load and click no longer pairs the rendered graph with a newer
cluster.

**D9. Streamed gzip (BE-08).** The existing loop in `buildGraph3dSnapshot` over
`['nodes_meta.json', 'clusters.json', 'nodes.bin', 'edges.bin']` keeps its `existsSync` check
and replaces `writeFileSync(gzipSync(readFileSync(…)))` with `await pipeline(fs.createReadStream(p),
zlib.createGzip(), fs.createWriteStream(`${p}.gz`))` (`node:stream/promises`), one file after
another. No new function or export. The output is
byte-compatible: `gunzipSync(gz)` equals the source. The test drives the real
`buildGraph3dSnapshot` through the snapshot route, with a fake export script, and checks the
stream calls and the absence of synchronous payload reads.

### Backend and frontend: logs and child output (Task 3)

**D10. The tail reads backwards and stops early (BE-10), controller call.** `readLogWindow`
becomes:

```
async readLogWindow(baseFilePath, { limit, cursor, includeRotated, maxFiles, accept = () => true })
  -> { entries, hasMore, nextCursor, cursor }
```

It is exported, and it reads as follows:
- It walks the files newest first: `base`, `base.1`, …, `base.<maxFiles>` (the rotated ones
  only with `includeRotated`).
- It reads each file backwards in `LOG_READ_CHUNK_BYTES = 64 * 1024` chunks through
  `fs.promises.open` and `handle.read`. It splits on the byte `0x0A`, carries a partial line's
  bytes across chunks, and decodes each complete line as UTF-8. `trimEnd` removes `\r`, as
  today. A blank line is skipped, and so is a line `accept` rejects.
- The accepted lines are counted from the newest. Lines `cursor .. cursor+limit-1` form the
  window. Reading stops once line `cursor + limit` has been seen (that is `hasMore`) or the
  files are exhausted.
- A missing file counts as empty, as `readTailLinesFromFile` does today.
- The window is pinned against rotation (`index.js:25-41` renames `base` to `base.1` and shifts
  the backups). Before the first read, the reader stats every file of the window and keeps
  `{ dev, ino, size }` per path (a missing file pins as absent). Each file is read only up to its
  pinned size, so a line appended meanwhile is not counted. After each `open`, `handle.stat()`
  must show the pinned `dev` and `ino`; an absent pin must still be absent. A mismatch means a
  rotation ran during the read: the reader drops what it collected and starts over once with a
  fresh pin. A second mismatch throws, and the route answers `503 { error: 'Log rotated during
  the read; retry' }`. A file is therefore never read twice under two names, and the cursor
  never skips older lines.
- Lines are pushed in the order they are found (newest first), and the window is reversed once
  at the end, so `entries` is in file order, oldest first, as today. `nextCursor = cursor +
  entries.length`.
- The response drops `total_lines`, which nobody reads: neither the frontend nor any test, and
  computing it needs the full read this removes.
- A cursor past the end answers `entries: []`, `has_more: false`, and echoes `cursor` and
  `next_cursor`. Today it clamps both to the line count.
- The route becomes `async`. The `corpus_id` filter becomes an `accept` predicate. The old
  synchronous readers are deleted.

**D11. Filter off means everything the live view may show (FE-59), controller call.**
- `fetchLogsTail` gains `scope`. When `type === 'pipeline'` and `scope === 'readable'`, it
  sends `scope=readable` and no `corpus_id`.
- The App sends `scope: logsFilterCurrentCorpus ? 'corpus' : 'readable'` from
  `hydrateLogsFromTail` and `loadOlderLogs` (`App.svelte:4268-4274`, `:4319-4325`).
- The route decides in this order:
  1. `type=app` stays admin-only, as today; `scope` is ignored for it.
  2. A given `corpus_id` always goes through today's validation and membership check (403 for a
     foreign corpus), also when `scope=readable` is sent with it. It then filters on that
     corpus's tag.
  3. Only the exact value `scope=readable` without a `corpus_id` reaches the merged branch. Any
     other `scope` value, or none, keeps today's fallback to the current corpus.
  4. In the merged branch, an admin or stub mode accepts every line. Any other user accepts a
     line whose trusted tag names a corpus in `SELECT corpus_id FROM user_corpora WHERE user_id
     = ?`.
- A corpus tag is trusted only in a fixed position written by a recognized scoped writer. The
  new exported `logLineCorpusId(line) -> number | null` in `app.js` accepts exactly
  `^\[<ISO timestamp>\] \[<label>\] ?\[corpus=(<digits>)\]`, where `<label>` is one of the
  labels the extraction route writes with a corpus tag (`app.js:4235-4576`):
  `extract-status`, `/api/extract-bibliography`, `get_bib_pages`, `get_bib_pages ERROR`, and
  `APIscraper`, `APIscraper inline fallback`, `APIscraper (reused pages)`, each also with the
  ` ERROR` suffix. The labels live in one exported constant, `SCOPED_LOG_LABELS`, next to the
  writers. Everything else is unclassified and admin-only: a `corpus=` in message text, a
  `[DAEMON]` line, a line without the timestamp (such as a continuation of an old multiline
  record), and a tag after another group.
- Both pipeline writers escape embedded line breaks, so one record is one line and a
  continuation can never be classified apart from its header:
  - Node: `logSocket.broadcast` (and the `index.js` fallback before the socket exists) writes
    `escapeLogLineBreaks(message)`, which turns `\r\n`, `\r` and `\n` into the two
    characters `\n`. The live frame carries the same escaped text.
  - The worker: `log()` (`worker.py:143-152`) escapes `\r` and `\n` in `msg` the same way
    before it formats the line, which covers the traceback text it writes
    (`worker.py:1482`).
- `logLineCorpusId` replaces `extractLogCorpusTag` in the tail route (both the `corpus_id`
  filter and the merged branch). `logSocket.js` classifies the formatted line (timestamp
  included) with the same function instead of its own regex, so the history and the live view
  classify a line the same way.
- The read is asynchronous (D10), so the route rechecks access before it answers. It reloads
  the user (`getUserById(authDb, req.user.id)`; `req.user` was loaded before the await, and the
  admin flag derives from the username) and re-evaluates the branch the window was built with:
  - `type=app`: the reloaded user is still the admin;
  - the merged branch: the same admin flag and the same `user_corpora` set;
  - an explicit or current corpus: the reloaded user may still read that corpus.

  A missing user or any difference discards the window, and the route answers `409 { error:
  'Log access changed during the read' }`. Stub mode skips the recheck.

**D12. The fallback parser follows brackets in one pass, within a parse budget (BE-42),
controller call.**
- The fast path is unchanged.
- The fallback joins the trimmed lines with `\n` once into `text`, then makes one structural
  scan over it: `bracketCloses(text) -> Map<number, number>`, opener index → matching close
  index. The scan keeps a stack of open `[`/`{` positions and skips JSON strings, honouring
  `\"` and `\\`. A string ends at the next newline at the latest (a JSON string cannot hold a
  raw newline), so an unbalanced quote in a log line cannot swallow the rest of the output. A
  close that does not match the top of the stack clears the stack. Each character is visited
  once; an opener that never closes has no entry.
- Only multiline blocks are candidates: a block whose open and close are on the same line is
  skipped, since the fast path already tried that line. A later single-line log label such as
  `[3] done` therefore cannot displace an earlier pretty-printed result.
- From the last line to the first, at each line's first `[` or `{`, a recorded multiline close
  gives one `JSON.parse(text.slice(start, close + 1))`.
- Nested malformed blocks overlap, so the sum of their lengths can still be quadratic. The
  fallback therefore has a budget: the sum of the candidate lengths it slices and parses is at
  most `FALLBACK_PARSE_BUDGET_FACTOR * text.length`, with the factor 4. A candidate that would
  exceed the budget ends the fallback, which then throws the unchanged error. Total work is one
  scan plus at most four times the text in parse input.
- The first block that parses wins, as today.
- A multiline block followed by text on its last line (`} done`) now parses; today it is
  skipped.
- The thrown error text is unchanged.
- `parseJsonFromOutput(output, { stats } = {})` takes an optional test hook: when `stats` is
  given, the fallback adds the characters its scan visited to `stats.visited`, its parse
  attempts to `stats.parses` and the candidate lengths it parsed to `stats.parsedChars`.

### Backend and frontend: Corpus and workspace loads (Task 4)

**D13. Server-side stage filter (FE-14), controller call.**
- `listCorpus` accepts `stage`, one of `raw | metadata | downloaded | failed_enrichment |
  failed_download`. Any other value means all stages.
- `STAGE_STATUSES` maps each stage to the `STATUS_SQL` values it covers:
  - `raw` → `raw`;
  - `metadata` → `matched`, `queued_download`, `enriching`;
  - each of the other stages → its own name.
- The counts keep their scope: corpus plus `q`, never the stage. `readCorpus` builds two
  fragments:
  - `countFrom` holds the corpus and `q` terms. The `q` counts' `GROUP BY` uses it, so every
    stage label keeps its count and "All stages" keeps the `q` total.
  - The page queries add the stage term `${STATUS_SQL} IN (<literals>)`. Those are the seed-sort
    light select and its id fetch, the `added` path, the generic sort and the fast year-index
    path. The year-index path appends every non-corpus term, not just the last one (`rest`,
    `corpusList.js:408`).
- The response adds `stage_total`: `stageTotals(statusCounts)[stage]` when a stage is set,
  else `total`. The empty-schema response returns `stage_total: 0`. `total`, `stage_totals`
  and `status_counts` keep their meaning, so the labels and the "3 Corpus (N)" count do not
  change.
- `/api/corpus` passes `req.query.stage`.
- The frontend moves the stage filter into App state, beside `corpusFilterQuery`.
  `corpusStageFilter` lives in `initialWorkspaceState()` and in `resetFrontendState()`'s
  restore. It therefore behaves as the `q` filter does today: it survives leaving and reopening
  the Workspace tab within the session, a corpus switch resets it to "All" (phase 5 D3), and a
  reload starts at "All". It is not written to `localStorage`.
- `Corpus.svelte` receives `corpusStageFilter`, `onStageFilterChange` and `corpusRowsStale`. It
  drops `filteredItems` and renders `corpusItems`.
- A stage change calls `loadCorpus({ quiet: true, supersede: true })`, as the sort does.
- Stale rows stay inert. `corpusRowsStage` is the stage the committed `corpusItems` were loaded
  for; it is set when a first-page load replaces the rows. `corpusRowsStale = corpusRowsStage
  !== corpusStageFilter`. While it is true:
  - the per-row remove, the selection checkboxes, select-all, the bulk remove, the row click
    (selection and expansion), the expanded row's PDF button and "Show more" (and the scroll
    trigger) are disabled or ignored;
  - an append request returns early, because its offset belongs to the old stage.
- The counts shown are the committed ones: the header's `Loaded N/M` uses the committed
  `stage_total` (`corpusMatchTotal`) instead of `corpusTotal`, and when a quiet stage load
  commits, it sets the "Showing N of M" status as a non-quiet load does.
- When the stage load fails, the old rows stay, the existing `corpusRefreshFailed` notice
  shows, and the actions stay disabled until a load for the selected stage succeeds or the user
  selects the committed stage again.
- `fetchCorpus` sends `stage` unless it is `'all'`. The poll's prefix reload sends it too.
  `corpusPageSignature` includes it.
- `applyCorpusCounts` takes `stageTotal`. `corpusHasMore`, the "Showing N of M" text, the
  header's `Loaded N/M` and the prefix reload's consistency check (`totals.size === 1`) use it.
- The Python oracle (`corpus_list.py`) has no `stage_total`. The parity tests compare
  `listCorpus` with `{ ...expected, stage_total: expected.total }` and keep strict equality on
  every field.

**D14. Completion loads in parallel (FE-18).** In `finalizeExtractionAfterBackendCompletion`,
the three awaits become `await Promise.all([focusSeedSource('pdf', baseName),
loadIngestStats(), loadIngestRuns()])`. The single `stale()` check after them stays. The catch
is unchanged; a 401 from any of them still finishes the card.

**D15. Graph and Scraper load on first open (FE-17), controller call.**
- `ThreeGraph` and `ScraperLab` are no longer imported statically. Each tab renders `{#await
  import('./components/ThreeGraph.svelte') then module}<svelte:component this={module.default}
  … />{/await}`, with the same props. While the import loads, the tab shows a muted `Loading…`.
  The same pattern is used for ScraperLab.
- `UpstreamBrowser` stays static. It is the Korpus Management tab, used by most users, and it
  takes `bind:` props. FE-17 is therefore fixed in part.
- A failed import shows `{:catch}` text, `Could not load this tab. Reload the page to try
  again.`, in the tab's place. The navigation and the other tabs keep working.

### Schema and dead code (Task 5)

**D16. One `pipeline_jobs` index, shaped for the prune (BE-45), controller call.**
`CREATE INDEX IF NOT EXISTS idx_pipeline_jobs_status_finished ON pipeline_jobs(status,
finished_at)`. It is created in `ensureAuthSchema`, right after the `pipeline_jobs` column
ensure, and in `db_manager.py`'s `WORKS_INDEXES`, the list that runs after table creation.

Checked plans:
- the claim, the probes and the recovery give `SEARCH … (status=?)`;
- the backend dedupe gives `SEARCH … (status=?)`;
- phase 7's `DELETE … WHERE status IN (…terminal) AND finished_at < ?` gives `SEARCH …
  (status=? AND finished_at<?)`.

The summary route's `corpus_id IS ? ORDER BY created_at DESC` still scans; see the non-goals.

**D17. Delete `/api/ingest/latest` (BE-06), controller call.** Remove:
- the route and `INGEST_LATEST_SCRIPT`;
- `backend/scripts/ingest_latest.py`;
- `fetchBibliographyEntries`;
- the README line;
- the two `test_identifiers.py` entries, the `DOI_MAP` key and the `_script("ingest_latest")`
  assertion. The recorded fixture `tests/fixtures/identifier_cases.json` keeps its
  `ingest_latest` key (it records the old functions); `test_fixture_covers_every_former_copy`
  (`test_identifiers.py:126-128`) then asserts that `set(FIXTURE["doi"]) - set(DOI_MAP)` is
  exactly that key, as it already does for the deleted inverted-index copy.

`pipeline-perf.real-pdf.spec.ts` reads the entry ids from
`/api/seed/sources/pdf/<runName>/candidates?limit=1000`: the candidate `id`, which is the
`ingest_entries` id. It is opt-in, and it is updated without being run. `dead-code.spec.ts`
keeps asserting that the frontend never calls the route.

## Order of work

1. Seed read path (D1–D4).
2. Kantropos browse and 3D graph (D5–D9).
3. Logs tail and child output (D10–D12).
4. Corpus stage filter and workspace loads (D13–D15).
5. `pipeline_jobs` index and the dead ingest route (D16, D17).

The tasks run serially in this order: every task edits `app.js`, and Tasks 2–5 share
`App.svelte` or `api.js`. Phase 6 merges before phase 7, and phase 7 rebases onto it.

## Acceptance

- Each fixed row has a test that fails on `0702e47` and passes after its task. Each task's
  commit names its rows. The parity tests come in two kinds:
  - D1 and D6 compare with today's code path (the full resolver, the golden browse payload) and
    must pass both before and after;
  - D2 and D10 compare the new export with a reference computed in the test (the full resolver,
    a full-read reference); they fail on `0702e47` only because the export is missing.

  Test files reach new exports through a namespace import (`import * as seed from …`), so a
  missing export fails only the tests that use it, and ESM test files import `jest` from
  `@jest/globals`.
- Backend: `cd backend && npm test` is green. Jest picks up new test files without a CI list
  change.
- Python: `cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests -q` is green.
  No new Python test file is created: the additions go into `test_schema_indexes.py` and
  `test_claim_index_plan.py`, which are already in the `ci.yml:68` list.
- Frontend: `npx vite build` succeeds. The touched specs pass from `frontend/` with
  `E2E_FRONTEND_PORT=5190 … --workers=1`: `graph-3d`, `logs-stream`, `corpus-load`, `dead-code`
  and `boot-requests`. All of them are mocked and already in the `ci.yml:54` list, and CI runs
  the whole list. No spec that talks to a backend runs: `app.spec.ts` logs in and creates a
  corpus, and port 5190 moves only the frontend (Vite still proxies to `:4000`).
- After the change, `grep -n "gzipSync\|readFileSync(filePath, 'utf8')" backend/src/app.js`
  shows no snapshot gzip and no log reader.
- `literature.db` is never opened.
- After merge, smoke-check against a backend started with `RAG_FEEDER_DB_PATH` set to a temp
  synthetic database (built with `DatabaseManager`'s schema, as for the measurements), never
  against the dev stack or `literature.db`:
  - `/api/seed/sources` on the synthetic 72k corpus;
  - a Korpus Management browse;
  - the Logs tab, with and without "current corpus only";
  - a 3D graph node click and a cluster click;
  - the Downloaded stage filter with "Show more".

## Controller calls (for the owner)

- **D2 (BE-05):** a seed candidate's PDF is served only when the matched work is readable by
  the caller; the current corpus's copy is preferred. A match that lies only in another user's
  corpus answers 404, where today it was served.
- **D3 (BE-09):** the boot warm-up is removed. The first seed expand, promote or candidate file
  after a deploy waits about 1.5 s for the downloaded index. The seed-list poll
  (`/api/seed/sources`) never does; the expanded seed's candidates reload while a search runs
  can, like any candidates request after a fingerprint change.
- **D6 (BE-11):** each Kantropos target's `metadata.bib` entry count is remembered until the
  file's size or mtime changes. The cache is bounded by the configured targets.
- **D7 (BE-36/BE-51):** a DOI node id resolves through `normalized_doi`, the key the graph
  export generates, instead of the raw `doi` text. Plain digits and the exporter's `work:<id>`
  resolve as work ids; `12.0`, `1e1` and `0x0c` no longer do. The Node schema bootstrap creates
  the two partial identifier indexes. On a Node-only database with duplicate identifiers it
  logs an error and boots; the lookup scans there (the one accepted exception).
- **D8 (BE-37):** the cluster panel uses the cluster loaded with the graph, and the
  cluster-detail route is removed.
- **D10 (BE-10):** `/api/logs/tail` no longer returns `total_lines`. A cursor past the end
  returns nothing and echoes the cursor. A rotation during the read is retried once; a second
  one answers 503.
- **D11 (FE-59):** with "current corpus only" off, the history shows what the live view may
  show: everything for admins, and the user's own corpora for members. A corpus tag counts only
  right after the timestamp and a recognized extraction label, in the tail and in the live
  socket; any other line is admin-only. Both pipeline writers escape line breaks inside a
  record. Access that changes during the read answers 409.
- **D12 (BE-42):** a multiline JSON block followed by text on its closing line now parses. The
  fallback gives up, with the unchanged error, after parse input of four times the output.
- **D13 (FE-14):** the stage filter is applied by the server. "Show more" and the scroll
  trigger page within the stage, and the stage labels keep their totals. Row actions wait until
  the rows of the selected stage have loaded. The stage persists like the `q` filter: across
  tab switches, not across a corpus switch or a reload.
- **D15 (FE-17, in part):** the Graph and Scraper tabs load their code on first open, with a
  short "Loading…" and an error text if the load fails. `UpstreamBrowser` stays in the main
  chunk.
- **D16 (BE-45):** a single index on `pipeline_jobs(status, finished_at)` serves the daemon, the
  dedupe and phase 7's 30-day prune. The summary route gets no index.
- **D17 (BE-06):** `/api/ingest/latest`, its script and its frontend export are deleted.
- **Ruled out:** BE-03, BE-35, BE-38, BE-39, BE-47, FE-20, FE-21, PY-25 and PY-31, and BE-34
  except the per-candidate call. See the non-goals for the measured reasons.
