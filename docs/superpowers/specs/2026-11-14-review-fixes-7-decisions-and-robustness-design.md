# Review fixes, phase 7: owner decisions, login limit and pipeline robustness — design

Date: 2026-11-14, revised 2026-11-22 after the Codex pre-flight
(`.superpowers/sdd/2026-11-14-review-fixes-7-decisions-and-robustness/preflight.md`, rulings in
`preflight-rulings.md` next to it). Branch `review-fixes-7` (worktree from master `0702e47`).
Source: the owner decisions of 2026-11-14
(`/home/jay/DT/.superpowers/sdd/2026-11-13-review-fixes-5-remaining/owner-decisions-2026-11-14.md`)
on the DECISION rows of
`/home/jay/DT/.superpowers/sdd/2026-11-13-review-fixes-5-remaining/inventory.md`, plus the slimmed
Phase 3. Rows: BE-19, BE-46, FE-55, PY-14 (decisions); the login rate limit of BE-40 (phase 5
closed only the hashing half); PY-16; BE-51 (the `/tmp/dt-jest-*` row); the parked phase 5 item
K-M4 (`phase5-archive/second-pass-code.md` M4); the whole Python suite in CI. FE-23 joins in part
(see non-goals). Same method as phases 2 to 5: Opus-medium implementers under subagent-driven
development, Codex `gpt-6.1-sol` xhigh as the review gate, plus the Claude review pass while
Copilot is out of quota.

The owner's principle: secure and fast, not over-engineered; the smallest change that removes the
cost or the risk.

Phase 6 (read-path performance, inventory "Phase 2" plus the BE-51 graph-node scan) is specified
in parallel in another worktree. No row of it is touched here. **Phase 6 merges first; phase 7
rebases onto it** (see "Integration with phase 6").

Every row was re-read in the code on `0702e47`; the line numbers below are from that commit.
Where a fix is a product choice the decision leaves open, the decision says **controller call**.
Those calls go to the owner as a list at the end.

## Problems (verified against master `0702e47`)

### Python

1. **Three scripts keep their own DOI rule (PY-14).** `export_bundle._normalize_doi`
   (`export_bundle.py:22-32`), `upstream_update._normalize_doi` (`upstream_update.py:232-243`) and
   `backfill_openalex_topics._normalize_doi` (`backfill_openalex_topics.py:55-64`) lowercase and
   strip prefixes without extracting the `10.NNNN/…` run. `test_identifiers.py:213-226` pins that
   they match none of the shared rules. Uses:
   - `export_bundle` writes the private form as the bundle field `normalized_doi`
     (`export_bundle.py:128`, from `doi` first, then the stored key) and dedupes the export on it
     (`pick_key`, `:91-96`) for rows without an OpenAlex id; `pick_key` prefers
     `openalex:<id>` when there is one. The DB key `works.normalized_doi` is the shared
     `identifiers.normalize_doi` (uppercase, first `10.` run), so the two disagree for every DOI
     with a letter in it. The BibTeX `doi` field prefers a `doi` already in `bibtex_entry_json`
     over the row's `doi` (`:190`).
   - `upstream_update` builds the BibTeX key candidate (`:307`), the BibTeX `doi` field
     (`:379-381`) and the draft manifest's `doi` (`:831`) from it.
   - `backfill_openalex_topics` keys the OpenAlex DOI pass on it, on both sides: rows (`:246`) and
     returned works (`:253`).

   The shared rule extracts a run only when the registrant has 4 to 9 digits
   (`identifiers.py:18`, `10\.\d{4,9}/…`). Otherwise it returns the whole value, unchanged apart
   from case: `normalize_doi("https://doi.org/10.1/ABC")` is `HTTPS://DOI.ORG/10.1/ABC`.

   **Consumers of the bundle format in this repo:** none. `metadata.json`/`references.bib` are
   written by `export_bundle.py` for download through `/api/exports/:format`
   (`app.js:5440-5470`). No script, route or frontend code reads a bundle back. The draft manifest
   of `upstream_update` is read by its own `validate`/`apply`/`markdown` steps
   (`upstream_update.py:718`, `:920`, `:1024`, `:1128-1153`, `:1264`) and by
   `backend/kantropos/embed_draft.py`. None of them reads an item's `doi`. Old bundles and old
   drafts therefore need no handling.

   Two suites load `upstream_update.py` by file name without putting `backend/scripts` on
   `sys.path` (`test_upstream_recovery.py:14-17`, `test_upstream_text_scan.py:17-21`). A bare
   `from _bootstrap import …` in the script would fail their collection.
2. **`dl_lit download-pdfs` races the daemon (PY-16).** The CLI reads rows with
   `fetch_download_queue` (`cli.py:664`, `db_manager.py:1855`) and claims nothing, so a running
   download daemon can claim and download the same row. Its dedupe (`cli.py:712-726`) merges a
   queued row into any matching work, including one that is not downloaded. The daemon merges
   only into a downloaded work (`worker.py:1199-1211`). `fetch_download_queue` has no other caller.
   Three further facts shape the fix:
   - an ordinary claim also takes `in_progress` rows whose lease has expired
     (`db_manager.py:2654-2663`), so a lease that ends while its owner still works lets a second
     owner in;
   - the daemon's download lease is 900 s (`worker.py:1290`; enrich `15 * 60`, `:1050`), but its
     job and direct-batch watchdogs default to 1800 s (`:269`, `:301-302`), so its own rows can
     expire while a batch legitimately runs;
   - start-up recovery releases every claim whose owner has no fresh heartbeat row
     (`worker.py:634-640`), which a CLI never writes.
3. **A direct batch whose item hangs every time restarts the worker every 30 minutes forever
   (K-M4).** When the direct-batch watchdog wins (`worker.py:527-537`), nothing is marked failed.
   The restarted worker's start-up recovery returns every claim to `pending`/`queued`
   (`_release_work_claims`, `:753-783`), and the next cycle claims the same oldest rows again
   (`db_manager.py:2687`, `:2697`: `ORDER BY created_at`; enrich `ORDER BY id`). Neither
   `claim_enrich_batch` (`worker.py:1046`) nor `claim_download_batch` (`:1290`) passes
   `max_attempts`, so `*_attempt_count` grows without a cap. A job's watchdog win
   (`_job_deadline_reached`, `:503-525`) fails the job row but returns the hung item to the queue
   the same way. After the latch, the executor threads keep running and may start further rows,
   and an item that finishes late writes its result over whatever the watchdog recorded
   (`worker.py:1212`, `:1246`, `:1249`, `:1271`, unconditional on the claim).

### Backend

4. **Any corpus owner or editor can assign their corpus to a Kantropos target (BE-19).** The PUT
   uses `requireCorpusConfigurer` (owner/editor, `app.js:3946-3949`). An assignment grants the
   owners and editors of that corpus scraper-apply and metadata-bib rights on the target
   (`canUseKantroposTarget`, `:2765-2780`). The GET answers `can_edit` for owner/editor
   (`:3935-3944`), and the frontend enables the control for them (`App.svelte:544`,
   `UpstreamBrowser.svelte:253-254`). `README.md:30` documents "Kantropos assignment needs
   owner/editor or admin".
5. **Sign-in has no attempt limit (BE-40 remainder).** `/api/auth/login` (`app.js:3123-3153`)
   answers every wrong password with `401` and nothing counts them. The phase 5 hasher pool bounds
   the CPU (2 running, 64 queued), but not the number of guesses. The user lookup is
   `WHERE username = ?` on the untrimmed body value (`:3128`, BINARY collation, so
   case-sensitive); usernames are stored trimmed (`:3280`).
6. **Finished `pipeline_jobs` are never pruned (BE-46).** The only delete is per corpus on corpus
   delete (`app.js:4013-4014`). Jobs end as `completed` (`worker.py:917`), `failed` (`:943`,
   `:477`) or `cancelled` (older code; 6.8k rows per the inventory), each with its `result_json`.
   `finished_at` is written as `CURRENT_TIMESTAMP` (`YYYY-MM-DD HH:MM:SS`); old cancelled rows
   have none. `authDb` runs with `busy_timeout = 15000` (`app.js:2600`) on the synchronous
   better-sqlite3 handle, so a statement that waits on a worker's write lock blocks the event
   loop for up to 15 s.
7. **Stale `/tmp/dt-jest-*` dirs (BE-51, the Jest row).** The 29 dirs on this machine all date from
   2026-11-03 11:17, the day #76 (`33acdde`) put each run's per-file DBs under one run dir
   (`tests/global-setup.js`, removed by `tests/global-teardown.js`). A full `npm test` on `0702e47`
   leaves no new entry in `/tmp` (checked by listing `/tmp` before and after). Two paths still leak:
   - `setup-env.js:12` falls back to `os.tmpdir()` when `DT_JEST_TMP_ROOT` is unset, and nothing
     removes those dirs.
   - About twenty suites create their own dirs directly under `os.tmpdir()` (for example
     `auth.test.js:49`, `access-routes.test.js:78`). A killed run (timeout, OOM, Ctrl-C) reaches
     neither their `afterAll` nor `globalTeardown`, so these dirs stay beside its run dir;
     `/tmp/dt-graph-access-*` from 2026-11-03 is one.

   Setting `process.env.TMPDIR` inside a suite does not move `os.tmpdir()`: the pre-flight probed
   this with the installed Jest environment. Only the env a worker process starts with counts.

### Frontend

8. **A failed search preview starts the search unconfirmed (FE-55).** `runSearch`
   (`App.svelte:3712-3741`) swallows every non-401 preview error ("Any other failed preview must not
   block the search", `:3725`) and calls `startSearch()`. An OpenAlex timeout on the count can
   therefore start a 70k-work run without the large-search warning (`:5062-5075`). Two related
   gaps: the epoch checks cover a sign-out and a corpus switch, but not Reset or a form edit
   during the preview, and every warning button builds a fresh body from the current fields
   (`startSearch`, `:3742-3744`). The preview and the launch run the same validation (`400`) and
   permission check (`403`) (`app.js:4612-4614`, `:4634-4636`).
9. **Worker errors show as raw JSON (FE-23, error-text half).** `loadPipelineWorkerStatus` sets
   `last_error` to `JSON.stringify(details)` (`App.svelte:4015`, `:4024`), and the download worker
   card renders it (`:5661-5662`). Phase 5's watchdog writes exactly such an error heartbeat
   (`state: "direct_batch_timeout"`, `error: "timed out after …"`), so after K-M4's restarts the
   admin reads `{"phase":"download","state":"direct_batch_timeout","error":"timed out after
   1800s; worker restarted"}`.

### CI

10. **CI runs 23 named Python test files of 47 (`ci.yml:68`), plus two `unittest discover` lines
    (`:69-70`).** A new test file is not run until someone adds it to the list. There is no reason
    left for the list: on `0702e47`, `python -m pytest tests -q` passes (872 passed, 7 subtests)
    in a fresh Python 3.11.2 venv that holds only the `ci.yml:67` pip list, with an empty
    environment (`env -i`) and no network (`unshare -rn`). No test file needs another pip package,
    and none is a live or network test. pytest also collects the `unittest.TestCase` files
    (`test_upstream_*`, `test_ocr_*`, `test_get_bib_pages`, `test_kantropos_*`). `master` has no
    branch protection (`gh api …/branches/master/protection` answers 404), so no required check
    names the job.

## Goals

- Only an admin creates or clears the link that grants Kantropos apply rights.
- Password guessing against one username stops after 10 failed sign-ins per 15 minutes, with no
  proxy configuration involved, and the limit answers alike for active, invited and unknown
  names.
- Job history no longer grows without bound, and pruning it never stalls the API for long.
- A count the user never saw cannot start a large search unconfirmed, and a confirmation always
  starts the request it describes.
- Every DOI key in the three scripts is the DB key.
- The CLI and the daemon never download the same row at once; a result written after its claim
  was lost is discarded, and a late attempt never overwrites the published PDF. A hung item ends as a visible failure after a bounded number of hangs
  instead of a restart loop.
- A Jest run keeps every temp dir under its run dir and removes it at the end; CI runs every
  Python test.
- Each change has a test that fails before it, except D11, a compatibility test.

## Non-goals

Owner: not over-engineered. Each item below was re-read; none fixes a real bug cheaply except the
error-text half of FE-23, which is in scope (Problem 9, D10). The deferred items are tradeoffs,
not non-issues.

- **PY-07** (serial keyword-search expansion; each work fetched twice): deferred. Besides wall
  time, the repeat fetches spend OpenAlex credits and bring a search closer to a quota stop
  (`keyword_search.py:408`). The fix is a restructure of `_crawl_direction` (M), not a cheap
  correctness change.
- **PY-08** (enrich waves of 2, raw download on the main thread): throughput. A raw download that
  hangs is covered by D3, because it runs inside its item's tracking.
- **BE-23** (schema in three places, no `user_version`): structural. No drift bug was found, and
  this phase adds no column or index.
- **BE-24** (`['pdf','search'].includes` copies): boilerplate only.
- **BE-27** (`new Database` per request): a file open per request on a few routes. Read-path cost
  is phase 6's surface.
- **BE-28, BE-29, BE-30, BE-31** (stub branches, 500 boilerplate, ticket stores, expansion
  builders): maintainability only.
- **BE-33** (helper copies, two OpenAlex-id normalizers): re-read. `seed.js` compares ids through
  its own normalizer on both sides (`seed.js:166`, `:201`), and the graph through its own
  (`app.js:1419`). No comparison mixes the two rules, so there is no mismatch to fix.
- **BE-52** (two test PDFs in dev corpus 182): parked, **unverified**. Checking it means opening
  `dl_lit_project/data/literature.db`, which this phase never does.
- **PY-23** (`bibtex_entry_json` size): storage only.
- **PY-27** (API key read in three places): a duplicate warning only.
- **PY-28** (Semantic Scholar at 1 rps): deferred. It is user-visible: raising the worker count in
  Admin does not speed up enrichment of DOI-less references, and nothing says so. The reorder is
  UV and M; the docs half waits for it.
- **PY-30, PY-32** (job parameter schema in two languages, backfill skeletons): maintenance.
- **PY-33** (wall-clock rate windows): deferred. The containers run in UTC, which removes the DST
  half only; an NTP step can still stall or burst calls for a few seconds.
- **FE-10, FE-11, FE-16, FE-22** (component extraction, auth flow copies, download snippet,
  `rearmRefreshOnChange` deps): structure, no user-visible defect.
- **FE-23, the "built twice" half:** structure. The raw-JSON error text is fixed (D10).
- **FE-24** (vite `usePolling`): dev container CPU only.
- **The login limit is per username, in memory.** It is not per IP: the per-IP problem of the
  phase 5 non-goal (proxy chain, `trust proxy`) stays out of scope. D5 adds no enumeration
  signal (the 429 answers alike for every name). Two existing signals are pre-existing and stay
  out of scope: the `403` "Invitation pending" answer, and the timing difference between an
  unknown name (no bcrypt) and a known one.
- **The CLI gets no heartbeat (D2).** Its claims are protected by their lease instead: start-up
  recovery keeps a live CLI lease, and the CLI claims one row at a time so one lease covers one
  download.
- **No age-based sweep of old Jest dirs (D7).** Age cannot tell a dead run from a live one, and
  the code removes nothing it did not create. The 29 leftovers from 2026-11-03 are removed once by
  hand (see Acceptance). A killed run leaves exactly one run dir behind, with every suite's dirs
  inside it.
- **No VACUUM after pruning (D6):** SQLite reuses the freed pages.

## Decisions

### Python

**D1. The three scripts use the DB's DOI rule (PY-14), controller call.** Each imports
`normalize_doi` from `dl_lit.identifiers` through `_bootstrap.ensure_import_paths(__file__)`, as
`backfill_openalex_topics.py` already does for `normalize_openalex_id`. The private
`_normalize_doi` copies are deleted.
- **`export_bundle.py`:**
  - `normalized_doi = data.get("normalized_doi") or normalize_doi(data.get("doi"))`: the stored
    DB key, with the shared rule as fallback for a row whose key column is empty. Today the
    raw `doi` wins over the stored key.
  - `pick_key` is unchanged: it still prefers `openalex:<id>`, and only rows without an OpenAlex
    id dedupe on `doi:<normalized_doi>`. The export therefore does not dedupe exactly as the DB
    does; it now uses the DB's DOI key where it uses a DOI at all.
  - **Effect on bundle counts:** two rows without an OpenAlex id whose DOIs differ only in their
    wrapping (a URL prefix, a `doi:` prefix, surrounding citation text) now collapse into one
    item; a DOI whose stored key differs from its lowercased raw value can now split from a row
    it used to match. Item counts of an export can change in either direction. No reader of old
    bundles exists.
  - The bundle's `doi` field stays the raw `works.doi`. The BibTeX `doi` keeps its precedence: a
    `doi` already in `bibtex_entry_json` wins, then the raw `works.doi` (`:190`).
- **`upstream_update.py`:**
  - The BibTeX key candidate uses `normalize_doi(...)`. `simple_key_part` lowercases it, so keys
    of ordinary DOIs do not change.
  - The BibTeX `doi` field and the manifest item's `doi` use the same rule in its lower form,
    `normalize_doi(value, case="lower")`. These are values people and the Kantropos RAG read,
    not keys. DOIs are case-insensitive, and the entries already in the upstream `metadata.bib`
    are lowercase. A DOI embedded in citation text (`Smith 2020, doi 10.1000/ABC, p. 3`) now
    yields `10.1000/abc`; today it yields the whole lowercased text.
- **`backfill_openalex_topics.py`:** rows and returned works are matched on the DB key
  (`normalize_doi`) on both sides. Pass 2 keeps its query: the `--scope` clause, the `--limit`
  clause and `topics_fetched IS NULL OR topics_fetched = 0`.
  - **The whole DOI is validated before the key is taken.** In the existing pass-2 loop, each
    row's `doi` is first stripped of its recognised wrappers with the shared
    `normalize_doi_lower` (a `doi.org` URL or `doi:` prefix, surrounding ` .;,`). The row is
    sent only when that **bare value as a whole** has the DOI shape
    (`_DOI_IN_TEXT_RE.fullmatch`): `10.`, 4 to 9 digits, `/`, then the pattern's character
    class, which has no `|`, `,` or `+`. Its key is then `normalize_doi(bare)`.
  - Validating after `normalize_doi` would not do: the rule extracts the first DOI-shaped run, so
    `10.1000/ABC+123` and `10.1000/ABC|xyz` would both become the valid-looking `10.1000/ABC` and
    fetch (and write) another work's topics. Embedded citation text is skipped too.
  - A skipped row is counted in the pass-2 log line, because one malformed token in the
    `doi:a|b|…` filter makes OpenAlex reject the whole batch of 50. The filter sends each token
    lowercased.
- **`test_identifiers.py`:** the three names leave `DOI_MAP` and their entries leave
  `tests/fixtures/identifier_cases.json`; `test_private_doi_copies_differ_from_every_shared_rule`
  is replaced by a test that each script's `normalize_doi` is `dl_lit.identifiers.normalize_doi`.
- **Loaders:** `test_upstream_recovery.py` and `test_upstream_text_scan.py` put
  `backend/scripts` on `sys.path` before they load `upstream_update.py`, as
  `test_identifiers._script` does. Each of the two is verified in a fresh process.
- **Fixtures** use DOIs the shared rule extracts (`10.1000/…`, 4 or more registrant digits).

**D2. The CLI claims rows like the daemon, one at a time (PY-16), controller call.**
- **Claim.** `download-pdfs` loops up to `--limit` times, each time claiming one row with
  `db.claim_download_batch(limit=1, corpus_id=None, claimed_by=cli_id, lease_seconds=900)`, and
  stops at the first empty claim (`Queue is empty.` when the first claim is empty).
  `cli_id = f"cli:{socket.gethostname()}:{os.getpid()}:{secrets.token_hex(3)}"`. One lease covers
  one download, so a long run never holds leases on rows it has not reached yet. A single download
  that outlasts its lease loses the row to the next claimant, and its result is discarded (below).
- **Daemon lease matches its watchdog.** New `PipelineDaemon._claim_lease_seconds() -> int`:
  `max(900, int(max(self.job_timeout_seconds, self._direct_batch_timeout_seconds())) + 60)`.
  Both `do_enrich` (`:1050`) and `do_download` (`:1290`) claim with it. Claims happen after the
  heartbeat thread (and so the deadline clock) starts (`:1394`, `:1525`), so a daemon lease
  outlasts its watchdog's deadline; past the deadline, the watchdog exits the worker and recovery
  takes over. This is the smaller change than renewing leases from the heartbeat.
- **A deliberate enrich deferral releases its unstarted rows.** When `do_enrich` yields
  (`pending_download` or `interactive_backlog`, `worker.py:1157-1172`), the rows of the later
  waves are still claimed by this worker, and the targeted continuation job (`:1409-1415`) may
  run at once and find none of them eligible until their lease ends (31 min with the longer
  lease). New `DatabaseManager.release_enrich_claims(work_ids: list[int], claimed_by: str) -> int`
  returns those rows that are still `in_progress` with that `metadata_claimed_by` to `pending`,
  clears the claim columns and takes back the attempt they never used
  (`metadata_attempt_count = MAX(COALESCE(metadata_attempt_count, 0) - 1, 0)`). `do_enrich` calls
  it for `remaining_ids` before it returns them.
- **Ownership on every terminal write, inside the writer's own transaction.** Each writer that
  ends a claim gains a keyword-only `claimed_by: str | None = None`. When it is given, the
  ownership check is the first statement of the writer's **own** transaction:
  - writers that already use `self.transaction()` (`apply_enriched_metadata`) run
    `_claim_held(phase, work_id, claimed_by)` as the first statement inside that block;
  - writers that commit themselves (`move_entry_to_downloaded`, `move_entry_to_failed` and so
    `mark_download_failed`, `merge_duplicate_queued_work`, `mark_metadata_failed`,
    `reset_enrich_claim`, `mark_raw_work_downloaded`) start with
    `_begin_owned(phase, work_id, claimed_by)`: `BEGIN IMMEDIATE` (unless a transaction is open),
    then the check, and a rollback when it fails. Their existing commit and rollback then end
    that one transaction.

  `_claim_held` checks `<status> = 'in_progress' AND <claimed_by> = ?` for the phase's columns
  (`download_*` or `metadata_*`). The write lock is held from the check through the write, so no
  other process can take the row in between, and a writer that fails part-way rolls back all of
  its writes: there is no outer wrapper whose commit could keep a partial merge (the first
  revision's `owned_claim` context manager could, because `transaction()` nested inside it skips
  its rollback while `apply_enriched_metadata` swallows the error). A lost claim returns
  `(None, CLAIM_LOST)` (`(False, CLAIM_LOST)` for the merge), `CLAIM_LOST = "claim_lost"`. Callers:
  - daemon `_download_single`: the duplicate merge, `move_entry_to_downloaded`, both
    `mark_download_failed` calls, with `claimed_by=self.worker_id`;
  - the CLI: the duplicate merge, the downloaded and the failed write, with `claimed_by=cli_id`;
  - the enrich writes of D3.

  A write whose claim was lost is skipped and logged once:
  `Work <id>: <phase> claim lost before its result was written; result discarded.` The daemon
  counts it as `skipped` (`action: "claim_lost"`).
- **The guarded merge rechecks its target.** Inside `merge_duplicate_queued_work`'s owned
  transaction, the duplicate target is re-read: when it no longer exists or is no longer
  `downloaded`, nothing is merged, it returns `(False, "duplicate_target_gone")`, and the caller
  downloads the row as usual.
- **A stale downloader never overwrites the published PDF.** Today all three file writers
  (`new_dl.py:1120`, `:1468`, `pdf_downloader.py:139-140`) write a deterministic final name, so
  a late attempt for the same work can overwrite the bytes behind the winner's `file_checksum`.
  The smallest fix, no per-attempt directories:
  - `download_ref(enhancer, ref, download_dir, *, part_suffix: str | None = None)` puts the
    suffix into `ref["part_suffix"]`; the three writers write `<final><part_suffix>` in the same
    directory and report that path as `downloaded_file`. CLI and daemon pass
    `part_suffix=f".part-{secrets.token_hex(4)}"` per attempt. Other callers pass nothing and keep
    today's behaviour.
  - `move_entry_to_downloaded` and `mark_raw_work_downloaded` gain `part_path: str | None = None`.
    After the ownership check passes, inside the same transaction, they publish it with
    `download_path.publish_download(part_path) -> str` (`os.replace` to the name without the
    `.part-…` suffix, atomic in one directory) and store the final path.
  - On a lost claim or any other failed write, the caller removes its part file with
    `download_path.discard_download(part_path)` (`missing_ok`). A stale attempt therefore leaves
    the winner's bytes and checksum untouched; at worst a crash leaves one `*.part-*` file.
- The CLI's dedupe uses the daemon's rule through a new shared method,
  `DatabaseManager.find_downloaded_duplicate(row: dict, authors) -> tuple[str | None, int | None, str | None]`.
  It runs `check_if_exists(doi, openalex_id, title, authors, editors=row.get("editors"), year,
  exclude_id=row["id"], exclude_table="works")` and returns the hit only when it is a `works` row
  whose `download_status` is `downloaded`; otherwise `(None, None, None)`. `worker._download_single`
  (`worker.py:1199-1211`) calls it instead of its inline copy.
- The CLI's loop runs in `try/finally`. The `finally` calls the new
  `DatabaseManager.release_download_claims(claimed_by: str) -> int`, which returns every
  `in_progress` row with that `download_claimed_by` to `queued` and clears the claim columns. It
  then closes the connection. An exception or Ctrl-C therefore leaves no claim behind; the
  exception still propagates (non-zero exit).
- **Start-up recovery keeps a live CLI lease.** `_release_work_claims` gains
  `keep_live_cli: bool = False`. With it, the release condition adds
  `AND NOT (<claimed_by> LIKE 'cli:%' AND COALESCE(<lease>, 0) > ?)` with `int(time.time())`.
  Start-up recovery (`:634-640`) passes `True`. A CLI claim is released at start-up only once its
  lease has expired; an expired one is also claimable by any worker as today.
- **Periodic recovery treats a `cli:` owner like a daemon worker.** `_periodic_owner_is_dead`
  (`worker.py:681-716`) accepts the prefixes `daemon` and `cli` for the
  `<prefix>:<host>:<pid>:<nonce>` form. Without this, the "non-daemon owner without a heartbeat
  row" clause would release a running CLI's claims within 60 s. A `cli:` owner is then released
  periodically only when it is on this host and its pid is gone.
- `fetch_download_queue` has no caller left and is deleted.

**D3. A hung item fails after 3 hangs (K-M4), controller call.**
- **Tracking.** The worker records which items are *running*, not merely claimed:
  `self._running_items: dict[tuple[str, int], float]` (phase, work id → start time from
  `self._running_clock`, `time.monotonic` by default, a test seam that the reporting budget below
  reads too), guarded by
  `self._running_items_lock`. That lock is only ever held for a dict operation or a copy of at
  most a few dozen entries, never across I/O. The context manager
  `_item_running(phase: str, work_id: int)` adds the entry and removes it in its `finally`. It
  wraps:
  - the body of each submitted download (`do_download`, `worker.py:1304-1305`) with phase
    `"download"`;
  - each submitted `_enrich_single` and the main-thread `_attempt_raw_download(entry)` call
    (`:1108`) with phase `"enrich"`, because the metadata claim is what that item holds.

  A row that waits in the pool's queue or in a later enrich wave is claimed but not running, so it
  is never blamed.
- **The winner latches and snapshots atomically.** New `_latch_running_items() -> dict`: under
  `_running_items_lock`, it sets `self._items_latched = True` and returns a copy of
  `_running_items`. The winner calls it inside the same `_work_lock` section that sets
  `_terminal` (`_job_deadline_reached` only when the job claim was won;
  `_direct_batch_deadline_reached` always). `_item_running` checks `_items_latched` under the
  same lock and, once it is set, raises `ItemsLatched` instead of starting the item: no item
  starts after the latch. The item's body does not run and writes nothing; its row keeps its
  claim for start-up recovery, and the exit follows. (`do_download` counts the exception as a
  worker exception; in `do_enrich` it leaves the batch.) Lock order is `_work_lock` → `_running_items_lock`;
  `_item_running` never takes `_work_lock`.
- **Item-level criterion.** An item in the snapshot counts a hung attempt only when it has been
  running for at least half the deadline: `now - started >= deadline * HUNG_ITEM_MIN_RUN_FRACTION`
  (`0.5`), with `now` read from `_running_clock` at the latch. A sibling that started recently is
  not counted, whatever its attempt count. Half rather than the whole deadline, because every item
  starts after its batch's deadline clock: at the win, even an item that hung from the first
  second has run slightly less than the deadline.
- **Terminal writes are guarded by ownership.** The enrich writes that end a metadata claim pass
  `claimed_by=self.worker_id` (D2): `apply_enriched_metadata`,
  `mark_metadata_failed`, `reset_enrich_claim` in `do_enrich`, and `mark_raw_work_downloaded` in
  `_attempt_raw_download`. The download writes already do (D2). An item that finishes after the
  watchdog failed it therefore writes nothing.
- **At a watchdog win.** This covers both a job win and a direct-batch win, because both end in
  `_finish_after_win`, which becomes
  `_finish_after_win(details: dict, message: str, deadline: float, running: dict, now: float) -> None`,
  with `running` and `now` from the latch. The
  hung-item write runs first, inside the `heartbeat_lock` acquisition, one `UPDATE` per phase for
  the overdue items of the snapshot:

  ```sql
  UPDATE works
     SET <status> = 'failed', <error> = ?, <claimed_by> = NULL, <claimed_at> = NULL, <lease> = NULL
         [, status_notes = ?]                      -- download only, as move_entry_to_failed does
   WHERE id IN (<overdue ids>) AND <status> = 'in_progress' AND <claimed_by> = <this worker_id>
     AND COALESCE(<attempt_count>, 0) >= <limit>
  ```

  The columns are the `metadata_*` set for `"enrich"` and the `download_*` set for `"download"`.
  The error text is `hung past the <deadline>s timeout; attempt limit <limit> reached` (deadline
  formatted with `:g`). Then the terminal heartbeat as in phase 5, then the log, then the exit.
  The hung write goes first because skipping it costs a restart cycle, while the heartbeat is
  display only.
- **One shared reporting budget keeps phase 5's exit bound.** Phase 5 bounds the time from the win
  to the exit by 1 s for `heartbeat_lock` and 1 s of SQLite busy wait, about 2 s. That bound is
  kept, not amended. After the lock is taken, `budget_end = self._running_clock() +
  WATCHDOG_BUSY_TIMEOUT_MS / 1000`. Each of the two writes sets `PRAGMA busy_timeout` to the milliseconds left; a write
  with less than 1 ms left is skipped with the reason `reporting budget spent`. The prior
  `busy_timeout` is restored afterwards, as today. A failed hung write rolls back
  (`conn.rollback()` when `conn.in_transaction`) before the heartbeat runs. The snapshot is taken
  at the latch, so `_finish_after_win` takes no other lock.
- **Below the limit, the item is left to recovery.** Restart recovery returns it to the queue
  as today. Each claim adds 1 to `*_attempt_count` (`db_manager.py:1769`, `:2713`), so an item
  that hangs on every attempt is failed at the win of its third claim, **provided that win's
  write is persisted**. A skipped write (lock timeout, budget spent, SQLite error) costs one more
  restart cycle; if every win's write were skipped, the loop would continue as today. Since the
  attempt count keeps growing, the first persisted write fails the item. The bound also assumes
  the item is overdue at the win: an item that hangs but always starts in the second half of its
  batch (for example behind a full pool, or in a late enrich wave) misses the half-deadline
  cutoff each time and keeps being retried. That case is accepted; it needs a batch that is
  nearly full of slow items every time.
- **Limit:** `env_int("RAG_FEEDER_HUNG_ITEM_MAX_ATTEMPTS", 3, minimum=1)`. The attempt count
  counts claims. A row the user re-queued earlier therefore reaches the limit sooner. That is
  acceptable: the row is blamed only when it has been running past half the deadline at a
  timeout.
- **Log lines, one per win:**
  - when it wrote: `Watchdog: <n> hung item(s) failed at the attempt limit, <m> left for a retry after the restart.`
    (`<m>` = items in the snapshot not failed, overdue or not);
  - when the write was skipped: `Watchdog: hung items not recorded (<reason>).`.
- No new column. No change to claim ordering. Start-up recovery changes only by D2's
  `keep_live_cli`.

### Backend

**D4. Kantropos assignment is admin-only (BE-19), controller call.**
- `requireCorpusConfigurer` (`app.js:3946-3948`) is replaced by
  `requireKantroposAssigner = requireCorpusRole([], corpusIdFromParams, { forbiddenError: 'Only the global admin can assign a Kantropos corpus' })`.
  With no roles listed, only the corpus role `'admin'` passes. `resolveRequestCorpusRole` still
  answers first, so:
  - a non-member (outsider) keeps getting `403 { error: 'No access to this corpus' }`;
  - a missing or invalid corpus id answers that `403` even for the admin
    (`access-routes.test.js:392`), so no assignment row for a missing corpus can be written;
  - a member who is not the admin (viewer, editor, owner) gets
    `403 { error: 'Only the global admin can assign a Kantropos corpus' }`, for an assignment and
    for clearing one (`targetId: null`) alike.
- The GET keeps phase 2's member check and answers `can_edit: isAdminUser(req.user, authConfig)`.
- `canUseKantroposTarget` (apply and metadata-bib rights) is unchanged: owners and editors of an
  assigned corpus may still apply, but only an admin creates the assignment.
- **Existing grants are accepted as they are.** Assignments that an owner or editor made before
  this change keep their rights until an admin changes or clears them. No migration reviews them.
- `README.md:30` changes to "Kantropos assignment needs the admin".
- `frontend/tests/upstream-metadata.spec.ts` (opt-in, `RUN_UPSTREAM_METADATA_E2E=1`) PUTs an
  assignment with the `E2E_USERNAME` credentials. Its login assertion message and header comment
  state that those credentials must be the global admin's.

**D5. Per-username login limit (BE-40 remainder), controller call.**
- New module `backend/src/loginLimiter.js`:
  `createLoginLimiter({ maxFailures = 10, windowMs = 15 * 60 * 1000, maxKeys = 10_000, now = () => Date.now() } = {}) -> { reserve(key), fail(ticket), refund(ticket), succeed(ticket) }`.
  - **State:** a `Map` from key to `{ windowId, windowStart, failures, inFlight }`, in insertion
    order. `windowId` comes from a counter in the limiter that grows by 1 for every new window,
    so two windows never share an id, even within one millisecond.
  - **`reserve(key)`:**
    - an entry whose window has passed (`now() - windowStart >= windowMs`) starts a new window:
      a new `windowId`, `windowStart = now()`, `failures = 0`, `inFlight = 0`;
    - when `failures + inFlight >= maxFailures` (the entry is **locked**), it returns
      `{ allowed: false, retryAfterSeconds: Math.max(1, Math.ceil((windowStart + windowMs - now()) / 1000)) }`;
    - otherwise it adds 1 to `inFlight` and returns `{ allowed: true, ticket: { key, windowId } }`.
  - **Settlement is bound to the reservation's window.** `fail`, `refund` and `succeed` act only
    when the key's entry still has `windowId === ticket.windowId`; otherwise (rollover, or the
    entry was evicted) they do nothing. Within the window:
    - `fail(ticket)`: `inFlight -= 1`, `failures += 1`;
    - `refund(ticket)`: `inFlight -= 1` (a hasher failure was not a guess);
    - `succeed(ticket)`: `inFlight -= 1`, `failures = 0`. Other reservations still in flight stay
      counted and settle on their own.

    An entry with `failures + inFlight === 0` is deleted; no ticket refers to it then.
  - **Bounded map.** When a new key arrives and the map holds `maxKeys` entries: first delete the
    entries whose window has passed; if none was deleted, delete the oldest entry that is not
    locked. A locked entry is never evicted. If every entry is locked, the new key is not
    tracked: `reserve` returns `{ allowed: true, ticket: null }`, and settling a `null` ticket
    does nothing (fail open). The scan runs only at capacity.
- **The key is the submitted username.** The handler first answers `400 Missing username or
  password` unless `username` and `password` are both strings and `username.trim()` is not
  empty. It then uses `name = username.trim()` for **both** the user lookup and the limiter (so
  ` b ` now signs in as `b`; usernames are stored trimmed). The limiter stores
  `sha256(name)` as hex (64 characters) as its map key, so a request-sized username costs no
  more memory than a short one; equality stays exact (BINARY). The limiter runs **before** the
  user lookup and treats every name alike:
  - `reserve` first; when it is not allowed, the answer is `429` with header
    `Retry-After: <retryAfterSeconds>` and
    `{ error: 'Too many failed sign-ins for this account. Try again in <n> minute(s).' }`. Here
    `<n> = Math.ceil(retryAfterSeconds / 60)`, and the text is "minute" when `n === 1`, else
    "minutes". A correct password is refused too until the window passes.
  - an unknown username (`401`) and an invited account (`403`) call `fail`;
  - a hasher failure (`HASHER_BUSY` 503 or the 500) calls `refund`;
  - a wrong password calls `fail` (`401` as today);
  - a correct password calls `succeed`.

  Active, invited and unknown names therefore reach the same 429, with the same body and header,
  after the same 10 failures.
- **The slot is reserved before the password check.** Concurrent wrong passwords therefore
  cannot get more than 10 checks into one window.
- `createApp({ broadcast, broadcastEvent, mailTransport, loginLimiter } = {})` uses the given
  limiter or `createLoginLimiter()`. One limiter lives per app.
- **In memory, by design.** The backend is one Node process (`index.js`, no cluster), so one map
  sees every sign-in. A restart resets the counts. That costs at most one extra window of 10
  guesses per restart. The map holds at most `maxKeys` entries of fixed size.
- **The limit is per username, not per IP.** It needs no client address, so there is no proxy or
  `trust proxy` change. As with any per-account lock, anyone who knows a username can lock that
  account for 15 minutes, the admin included. The owner chose this rule.

**D6. Pipeline jobs older than 30 days are pruned (BE-46), controller call.**
- New module `backend/src/pipelineJobRetention.js`:
  - `pruneFinishedPipelineJobs(db, { days = 30 } = {}) -> number` returns 0 when the table does
    not exist. Otherwise it runs two `DELETE`s in one `db.transaction`, both shaped for phase 6's
    index `idx_pipeline_jobs_status_finished (status, finished_at)`:

    ```sql
    DELETE FROM pipeline_jobs
     WHERE status IN ('completed', 'failed', 'cancelled') AND finished_at < datetime('now', ?);
    DELETE FROM pipeline_jobs
     WHERE status IN ('completed', 'failed', 'cancelled') AND finished_at IS NULL
       AND created_at < datetime('now', ?);
    ```

    with `'-30 days'`, and returns the sum of the deleted counts. `finished_at` and `created_at`
    are `CURRENT_TIMESTAMP` text, so the plain comparison is correct. The second statement is the
    legacy fallback for jobs that never got a `finished_at`. An in-memory `EXPLAIN QUERY PLAN`
    with that index gives `SEARCH … (status=? AND finished_at<?)` and
    `SEARCH … (status=? AND finished_at=?)`. The `datetime(COALESCE(finished_at, created_at))`
    form of the first draft uses only the `status` prefix and is not used. `pending` and
    `running` jobs are never deleted, whatever their age.
  - **Short busy budget.** The prune reads the connection's `busy_timeout`, sets it to
    `PRUNE_BUSY_TIMEOUT_MS = 250` for the transaction and restores the prior value in a
    `finally`. A worker holding the write lock therefore blocks the event loop for at most about
    250 ms. On `SQLITE_BUSY` the prune throws; nothing is deleted, and the next run retries.
  - `startPipelineJobRetention(db, { intervalMs = 24 * 60 * 60 * 1000 } = {}) -> () => void`.
    It prunes once at once, then every `intervalMs` on an `unref()`'d `setInterval`. Each run is
    wrapped: it logs `[pipeline-jobs] pruned <n> finished job(s) older than 30 days` when `n > 0`
    (`console.log`), and `[pipeline-jobs] retention failed: <message>` on an error
    (`console.error`, contention included). It never throws. The returned function clears the
    timer. Tests use Jest fake timers and a temp DB, so the module takes no timer, prune or
    logger parameters.
- **Where it runs:** the backend, which owns `authDb` and is a single process. `index.js` calls
  `startPipelineJobRetention(app.authDb)` right after `createApp`. It does not run in the Python
  workers: three roles would each prune. It does not run inside `createApp` either, so no test
  app starts a timer.
- Before phase 6 merges, the table has no such index and the prune scans; phase 6 merges first
  (see "Integration with phase 6").

**D7. Jest keeps its temp dirs under its run dir (BE-51).**
- **`tests/global-setup.js`** creates the run dir as today, then points `TMPDIR` at it in the
  parent process, before Jest starts any worker: `process.env.DT_JEST_PRIOR_TMPDIR` records the
  prior value (the empty string for unset), then `process.env.TMPDIR = root`. Workers inherit it,
  so `os.tmpdir()` in every suite is the run dir. Every suite's own
  `mkdtempSync(path.join(os.tmpdir(), …))` lands inside it without editing the twenty suites, and
  Python children inherit it too.
- **`tests/global-teardown.js`** restores `TMPDIR` (deletes it when the prior value was unset) and
  removes the run dir, as today. It removes nothing else.
- **`tests/setup-env.js`** keeps the per-suite DB path override (`RAG_FEEDER_DB_PATH` under a
  `dt-jest-` dir in the run dir). It throws
  `DT_JEST_TMP_ROOT is unset: run the backend tests through jest's globalSetup (npm test).`
  instead of falling back to `os.tmpdir()`. It does not touch `TMPDIR`.
- **Nested Jest.** `setup-env-isolation.test.js` runs a child Jest with the worker's env. Its
  `globalSetup` makes its run dir under the inherited `TMPDIR`, inside the parent's run dir, and
  its teardown restores the parent's value, so it is removed with the parent's run dir.
- **No sweep.** A killed run leaves its run dir (one entry in `/tmp`); a completed run leaves
  nothing.

### Frontend

**D8. A failed preview asks before starting, and a confirmation starts what it describes (FE-55),
controller call.**
- **One pending request object.** `runSearch` creates
  `const request = { body: buildSearchBody(searchMaxResults), previewing: true }` and sets
  `searchRequest = request`; the object's identity is the request's identity. The preview is sent
  with `request.body`. The Search button is disabled while `searchRequest?.previewing` (instead
  of the unconditional `searchPreviewBusy`). When the preview settles, only the current request
  is touched: `if (request === searchRequest) { request.previewing = false; searchRequest = searchRequest }`
  (the reassignment notifies Svelte). Its result is used only while `request === searchRequest`
  and the session and corpus epochs still match; otherwise it is dropped.
- **Invalidation.** `invalidateSearchConfirmation()` sets `searchRequest = null` and clears
  `searchPreview`, `searchPreviewError` and `searchWarning`, and `searchStatus` when it still
  says `Checking how many works match...`. A discarded preview therefore no longer keeps the
  button disabled or the status on an edited form. It runs:
  - from `resetSearchForm`;
  - on any `input` event inside the search form (one `on:input` on the form element covers the
    text fields, the selects and the max-results field);
  - from `addTopic`, `removeTopic` and `resetTopicSearch`, which already clear the preview.

  Enter and the Search button fire no `input` event, so resubmitting keeps working.
- **Error handling.** `runSearch`'s catch keeps the 401 return.
  - A `400` or `403` preview error is shown directly: `searchStatus = error.message`, no warning,
    nothing starts. The launch runs the same validation and permission check, so it would fail the
    same way.
  - Any other error stores `previewError = error?.message || 'the server did not answer'`. After
    the session, corpus and sequence checks it sets `searchPreview = null`,
    `searchPreviewError = previewError`, `searchWarning = true` and `searchStatus = ''`, still
    refreshes the quota, and returns without starting.
  - A successful preview clears `searchPreviewError`.
- **Every warning button starts the submitted body.** `startSearch({ maxResultsOverride = null, body = null } = {})`
  uses `body` when given (with `maxResults` replaced by the override, if any), else builds one as
  today. Both warning variants pass `searchRequest.body`.
- `searchPreviewError` (`''` by default) and `searchRequest` (`null`) are part of
  `initialWorkspaceState()` and of the `resetFrontendState` destructuring.
- The warning box (`data-testid="search-warning"`, same classes and `role="alert"`) gets a second
  variant, rendered when `searchWarning && !searchPreview && searchPreviewError`:
  - text: `Could not check how many works match (<message>). <cap sentence> Narrow the query, or:`;
  - cap sentence, with `cap = searchResultCap(searchMaxResults)`: when `cap === 0`,
    `This search has no cap, so it may fetch a very large number of works.`; otherwise
    `This search stops at <cap, en-US grouping> works.`;
  - one primary button, `Search anyway`, which calls `startSearch({ body: searchRequest.body })`.
- The rule holds whatever the cap: the count is unknown, and so is the server's threshold, which
  only the preview reports. Submitting the form again runs the preview again.

**D9. The assignment control is admin-only (BE-19 UI).**
- `canConfigureCurrentCorpusKantropos = Boolean(currentCorpusId && isAdmin)` (`App.svelte:544`).
- The disabled `<select>`'s title becomes `Only admins can change the Kantropos assignment`
  (`UpstreamBrowser.svelte:254`).
- The save guard's message (`App.svelte:2580`) becomes
  `Only admins can change the Kantropos assignment.`.

**D10. Worker errors are text (FE-23, error-text half).**
- A local function in `App.svelte`, `workerErrorText(details) -> string`, returns the first
  non-empty string of `details.error` and `details.message`, then `details.state`. With none of
  them, it returns `JSON.stringify(details || {})`. It has one caller, so it stays in
  `App.svelte`; no new module.
- `loadPipelineWorkerStatus` uses it for both `last_error` values.
- The test renders the download worker card from a mocked error heartbeat and reads the text the
  admin sees.

**D11. The 429 sign-in text reaches the form (D5 UI).** `handleLogin` already shows
`error.message` (`App.svelte:1725`), and `requestJson` carries the body's `error`. No code
changes. A mocked spec pins it as a compatibility test; it is expected to pass on `0702e47`.

### CI

**D12. CI runs the whole Python suite (Problem 10).** The `python-search` job keeps its id and its
display name `Python search and schema tests`, so nothing that refers to the check changes. It
keeps its pip list and runs `python -m pytest tests -q`. The two `unittest discover` lines are
removed: pytest collects those files. No file is excluded; the spec records the evidence
(Problem 10). A future test file that needs a new package then fails CI until `ci.yml:67` lists
the package. The four frontend specs this phase touches are already in the CI Playwright list
(`ci.yml:54`), and Jest discovers the new backend test files itself.

## Order of work

Tasks are grouped by surface; tasks on different surfaces touch disjoint files.
1. Python:
   - D1;
   - D2;
   - D3, after D2, because both edit `backend/scripts/daemon/worker.py`, and D3 uses D2's
     `claimed_by` writers.
2. Backend:
   - D4 and D5 (`app.js`, `loginLimiter.js`, `README.md`);
   - D6 and D7 (new module, `index.js`, Jest setup).
3. Frontend: D8 to D11, plus the opt-in metadata spec's credential note (D4).
4. CI: D12.

Every task except D3 can run in parallel with the others.

## Integration with phase 6

Phase 6 merges into master first; `review-fixes-7` then rebases onto it. Overlaps:
- `backend/src/index.js`: phase 6 removes the `seedWarmupTimer` start-up block (`:102-112`) that
  D6's call sits next to. Resolve by keeping phase 6's removal and adding D6's line after
  `createApp`.
- `dl_lit_project/tests/test_identifiers.py` and its fixture: phase 6 drops the `ingest_latest`
  entries from the maps (`:56`, `:199`) but keeps its key in `identifier_cases.json` as a
  historical record, and rewrites `test_fixture_covers_every_former_copy` to
  `set(FIXTURE["doi"]) - set(DOI_MAP) == {"backend.scripts.ingest_latest.normalize_doi"}` plus
  `set(DOI_MAP) <= set(FIXTURE["doi"])`. Resolve by keeping phase 6's historical key and both
  assertions as they are, and removing D1's three entries from `DOI_MAP` **and** from the
  fixture, so the set difference stays exactly the `ingest_latest` key.
- `frontend/src/App.svelte`: phase 6 adds `corpusStageFilter` (and `corpusRowsStage`) to
  `initialWorkspaceState()` and the restore beside `corpusFilterQuery`; D8 adds
  `searchPreviewError` and `searchRequest` to the same block and the `resetFrontendState`
  destructuring, and edits the search reset. Resolve by keeping both sets of fields and both
  reset changes.
- `pipeline_jobs`: D6's prune relies on phase 6's index. D6's query-plan test creates the same
  index DDL in its temp DB, so it passes on either base.

After the rebase, the controller runs both phases' checks: phase 6's plan's verification and this
phase's Acceptance.

## Acceptance

- Every change has a test that fails on `0702e47` and passes after its task; D11's spec is a
  compatibility test and passes on both. Each commit names the rows it closes.
- Python: `cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests -q` is green, and
  `test_upstream_recovery.py` and `test_upstream_text_scan.py` each pass alone in a fresh process.
- Backend: `cd backend && npm test` is green. On this machine, suites can time out under the
  full parallel load and pass alone; compare with a `0702e47` baseline, or use
  `npm test -- --maxWorkers=2`, before blaming a task. Also, from the repo root:
  - `git grep -n requireCorpusConfigurer -- backend/src/app.js` finds nothing;
  - a completed `npm test` adds no entry to `/tmp` (list `/tmp` before and after).
- Frontend: `npx vite build` succeeds. The specs this phase touches pass with
  `E2E_FRONTEND_PORT=5190 npx playwright test <specs> --workers=1`, and so does the CI Playwright
  list (`ci.yml:54`). No live spec is run, and the dev stack on `:5175` is not used.
- CI: the workflow's Python job runs `python -m pytest tests -q` and nothing else.
- Never against `dl_lit_project/data/literature.db`.
- One-time manual step (controller, after merge): with no Jest run active, remove the 29
  `/tmp/dt-jest-*` and the `/tmp/dt-graph-access-*` dirs from 2026-11-03.
- After merge, smoke checks on a **disposable DB**, never the dev stack (which uses
  `literature.db`): start the backend from `backend/` with `RAG_FEEDER_DB_PATH` under a new
  `mktemp -d` dir, a spare `PORT`, and test `RAG_ADMIN_*` values. Before the start, create the
  schema by one start/stop and insert one `cancelled` job with `created_at` 40 days back. Then:
  - the backend log shows `[pipeline-jobs] pruned 1 finished job(s) older than 30 days`;
  - 11 wrong passwords for one username answer `429` with `Retry-After`, for an existing and an
    unknown name alike;
  - remove the temp dir afterwards.

  The UI checks (disabled select, unknown-count warning) are covered by the mocked specs.

## Controller calls (for the owner)

- **D1 (PY-14):** exports and the backfill use the DB key (`normalize_doi`, uppercase).
  `upstream_update` writes the same rule's lowercase form into the BibTeX `doi` field and the
  manifest, because people and the RAG read those values and the existing upstream entries are
  lowercase. Export item counts can change where rows without an OpenAlex id carry the same DOI
  in different wrappings. The BibTeX DOI precedence is unchanged. The backfill skips values that
  are not whole DOIs. No reader of old bundles exists in this repo.
- **D2 (PY-16):** the CLI claims one row at a time as `cli:<host>:<pid>:<nonce>`, releases it on
  exit, dedupes only into downloaded works; start-up recovery keeps its live lease, and periodic
  recovery treats it like a daemon worker. The daemon's lease grows to its watchdog timeout plus
  60 s (1860 s by default). A result written after its claim was lost is discarded, with a log
  line; a download is written to `<final>.part-<token>` and moved into place only while the claim
  is held, so a late attempt never replaces the winner's PDF. A deliberate enrich deferral hands
  its unstarted rows straight back to the queue.
- **D3 (K-M4):** an item running for at least half the timeout when a watchdog wins is failed
  once its attempt count reaches 3 (`RAG_FEEDER_HUNG_ITEM_MAX_ATTEMPTS`); an item that always
  starts in the second half of its batch keeps missing that cutoff; below the limit it is retried
  after the restart. Claimed rows that had not started, and siblings that started recently, are
  never blamed. The three-attempt bound holds when the win's write is persisted; a skipped write
  costs one more cycle. Phase 5's 2 s exit bound is kept. The error is
  `hung past the <N>s timeout; attempt limit 3 reached`. The "half the timeout" criterion
  replaces the "older than the timeout" wording of the rulings, which no item can meet at the
  win (see D3).
- **D4 (BE-19):** only the admin may assign or clear; existing assignments made by owners or
  editors stay until an admin changes them.
- **D5 (BE-40):** 10 failed sign-ins for one username within 15 minutes lock that username until
  the window passes, even for the right password; a successful sign-in clears the settled
  failures; unknown and invited names are counted the same way, so the 429 reveals nothing; in
  memory, at most 10,000 names stored as SHA-256 digests, reset by a backend restart. The
  username is trimmed for the lookup too, and a non-string username or password answers 400. Anyone can lock a known account for
  15 minutes. The invitation 403 and the timing difference stay (pre-existing).
- **D6 (BE-46):** the backend deletes `completed`, `failed` and `cancelled` jobs 30 days after they
  finished (or were created, when they never finished), at start-up and then daily, waiting at
  most 250 ms for a lock and retrying on the next run.
- **D7 (BE-51):** every Jest temp dir lives under one run dir, removed at the end of the run. A
  killed run leaves that one dir; nothing sweeps old ones.
- **D8 (FE-55):** a failed preview always asks, whatever the cap, with a "Search anyway" button; a
  400 or 403 is shown as is; any edit or Reset withdraws the question, and a confirmation starts
  exactly the submitted search.
- **D10 (FE-23):** worker errors show their `error` text instead of JSON.
- **D12:** CI runs every Python test file under the unchanged check name; nothing needed an
  exclusion.
