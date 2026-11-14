# Review fixes, phase 7: owner decisions, login limit and pipeline robustness — design

Date: 2026-11-14. Branch `review-fixes-7` (worktree from master `0702e47`). Source: the owner
decisions of 2026-11-14
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
in parallel in another worktree. No row of it is touched here.

Every row was re-read in the code on `0702e47`; the line numbers below are from that commit.
Where a fix is a product choice the decision leaves open, the decision says **controller call**.
Those calls go to the owner as a list at the end.

## Problems (verified against master `0702e47`)

### Python

1. **Three scripts keep their own DOI rule (PY-14).** `export_bundle._normalize_doi`
   (`export_bundle.py:22-32`), `upstream_update._normalize_doi` (`upstream_update.py:234-245`) and
   `backfill_openalex_topics._normalize_doi` (`backfill_openalex_topics.py:55-64`) lowercase and
   strip prefixes without extracting the `10.NNNN/…` run. `test_identifiers.py:213-226` pins that
   they match none of the shared rules. Uses:
   - `export_bundle` writes the private form as the bundle field `normalized_doi`
     (`export_bundle.py:128`) and dedupes the export on it (`pick_key`, `:91-96`). The DB key
     `works.normalized_doi` is the shared `identifiers.normalize_doi` (uppercase, first `10.`
     run), so the two disagree for every DOI with a letter in it.
   - `upstream_update` builds the BibTeX key candidate (`:307-309`), the BibTeX `doi` field
     (`:379-381`) and the draft manifest's `doi` (`:831`) from it.
   - `backfill_openalex_topics` keys the OpenAlex DOI pass on it, on both sides: rows (`:246`) and
     returned works (`:253`).

   **Consumers of the bundle format in this repo:** none. `metadata.json`/`references.bib` are
   written by `export_bundle.py` for download through `/api/exports/:format`
   (`app.js:5440-5470`). No script, route or frontend code reads a bundle back. The draft manifest
   of `upstream_update` is read by its own `validate`/`apply`/`markdown` steps
   (`upstream_update.py:718`, `:920`, `:1024`, `:1128-1153`, `:1264`) and by
   `backend/kantropos/embed_draft.py`. None of them reads an item's `doi`. Old bundles and old
   drafts therefore need no handling.
2. **`dl_lit download-pdfs` races the daemon (PY-16).** The CLI reads rows with
   `fetch_download_queue` (`cli.py:664`, `db_manager.py:1855`) and claims nothing, so a running
   download daemon can claim and download the same row. Its dedupe (`cli.py:712-726`) merges a
   queued row into any matching work, including one that is not downloaded. The daemon merges
   only into a downloaded work (`worker.py:1199-1211`). `fetch_download_queue` has no other caller.
3. **A direct batch whose item hangs every time restarts the worker every 30 minutes forever
   (K-M4).** When the direct-batch watchdog wins (`worker.py:527-537`), nothing is marked failed.
   The restarted worker's start-up recovery returns every claim to `pending`/`queued`
   (`_release_work_claims`, `:753-783`), and the next cycle claims the same oldest rows again
   (`db_manager.py:2687`, `:2697`: `ORDER BY created_at`; enrich `ORDER BY id`). Neither
   `claim_enrich_batch` (`worker.py:1046`) nor `claim_download_batch` (`:1290`) passes
   `max_attempts`, so `*_attempt_count` grows without a cap. A job's watchdog win
   (`_job_deadline_reached`, `:503-525`) fails the job row but returns the hung item to the queue
   the same way.

### Backend

4. **Any corpus owner or editor can assign their corpus to a Kantropos target (BE-19).** The PUT
   uses `requireCorpusConfigurer` (owner/editor, `app.js:3946-3949`). An assignment grants the
   owners and editors of that corpus scraper-apply and metadata-bib rights on the target
   (`canUseKantroposTarget`, `:2765-2780`). The GET answers `can_edit` for owner/editor
   (`:3935-3944`), and the frontend enables the control for them (`App.svelte:544`,
   `UpstreamBrowser.svelte:253-254`).
5. **Sign-in has no attempt limit (BE-40 remainder).** `/api/auth/login` (`app.js:3123-3153`)
   answers every wrong password with `401` and nothing counts them. The phase 5 hasher pool bounds
   the CPU (2 running, 64 queued), but not the number of guesses.
6. **Finished `pipeline_jobs` are never pruned (BE-46).** The only delete is per corpus on corpus
   delete (`app.js:4013-4014`). Jobs end as `completed` (`worker.py:917`), `failed` (`:943`,
   `:477`) or `cancelled` (older code; 6.8k rows per the inventory), each with its `result_json`.
7. **Stale `/tmp/dt-jest-*` dirs (BE-51, the Jest row).** The 29 dirs on this machine all date from
   2026-11-03 11:17, the day #76 (`33acdde`) put each run's per-file DBs under one run dir
   (`tests/global-setup.js`, removed by `tests/global-teardown.js`). A full `npm test` on `0702e47`
   leaves no new entry in `/tmp` (checked by listing `/tmp` before and after). Two paths still leak:
   - `setup-env.js:12` falls back to `os.tmpdir()` when `DT_JEST_TMP_ROOT` is unset, and nothing
     removes those dirs.
   - A run that is killed (timeout, OOM, Ctrl-C) never reaches `globalTeardown`, so its run dir
     stays. So do the per-suite dirs that about twenty suites create directly under
     `os.tmpdir()` (for example `auth.test.js:49`, `access-routes.test.js:78`), whose `afterAll`
     never runs either; `/tmp/dt-graph-access-*` from 2026-11-03 is one.

   Nothing removes the old leftovers.

### Frontend

8. **A failed search preview starts the search unconfirmed (FE-55).** `runSearch`
   (`App.svelte:3712-3741`) swallows every non-401 preview error ("Any other failed preview must not
   block the search", `:3725`) and calls `startSearch()`. An OpenAlex timeout on the count can
   therefore start a 70k-work run without the large-search warning (`:5062-5075`).
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
    (`test_upstream_*`, `test_ocr_*`, `test_get_bib_pages`, `test_kantropos_*`).

## Goals

- Only an admin creates the link that grants Kantropos apply rights.
- Password guessing against one account stops after 10 wrong passwords per 15 minutes, with no
  proxy configuration involved.
- Job history no longer grows without bound.
- A count the user never saw cannot start a large search unconfirmed.
- Every DOI key in the three scripts is the DB key.
- The CLI and the daemon never download the same row at once, and a hung item ends as a visible
  failure after a bounded number of hangs instead of a restart loop.
- A Jest run cleans up after itself, including after a killed run; CI runs every Python test.
- Each change has a test that fails before it.

## Non-goals

Owner: not over-engineered. Each item below was re-read; none fixes a real bug cheaply except the
error-text half of FE-23, which is in scope (Problem 9, D10).

- **PY-07** (serial keyword-search expansion): throughput, not correctness.
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
- **PY-23** (`bibtex_entry_json` size): storage only.
- **PY-27** (API key read in three places): a duplicate warning only.
- **PY-28** (Semantic Scholar at 1 rps): throughput and docs.
- **PY-30, PY-32** (job parameter schema in two languages, backfill skeletons): maintenance.
- **PY-33** (wall-clock rate windows): the containers run in UTC, so there is no DST jump, and an
  NTP step is seconds.
- **FE-10, FE-11, FE-16, FE-22** (component extraction, auth flow copies, download snippet,
  `rearmRefreshOnChange` deps): structure, no user-visible defect.
- **FE-23, the "built twice" half:** structure. The raw-JSON error text is fixed (D10).
- **FE-24** (vite `usePolling`): dev container CPU only.
- **The login limit is per account, in memory.** It is not per IP: the per-IP problem of the phase 5
  non-goal (proxy chain, `trust proxy`) stays out of scope. Phase 5 noted username enumeration
  through timing, and the `403` "Invitation pending" answer also reveals an account. Neither
  changes; the 429 adds one more such signal, after 10 failures.
- **D2 does not give the CLI a heartbeat.** A daemon that starts while the CLI runs releases the
  CLI's claims in its start-up recovery (no fresh heartbeat row). The two can then download one row
  twice, as today. The CLI is an operator tool, and this needs a daemon restart during a manual run.
- **No VACUUM after pruning (D6):** SQLite reuses the freed pages.
- `README.md` is not changed.

## Decisions

### Python

**D1. The three scripts use the DB's DOI rule (PY-14), controller call.** Each imports
`normalize_doi` from `dl_lit.identifiers` through `_bootstrap.ensure_import_paths(__file__)`, as
`backfill_openalex_topics.py` already does for `normalize_openalex_id`. The private
`_normalize_doi` copies are deleted.
- **`export_bundle.py`:** `normalized_doi = data.get("normalized_doi") or normalize_doi(data.get("doi"))`,
  the stored DB key with the shared rule as fallback for a row whose key column is empty.
  `pick_key` keeps using it, so the export dedupes exactly as the DB does. The bundle's `doi`
  field and the BibTeX `doi` stay the raw `works.doi`.
- **`upstream_update.py`:**
  - The BibTeX key candidate uses `normalize_doi(...)`. `simple_key_part` lowercases it, so keys
    of ordinary DOIs do not change.
  - The BibTeX `doi` field and the manifest item's `doi` use the same rule in its lower form,
    `normalize_doi(value, case="lower")`. These are values people and the Kantropos RAG read,
    not keys. DOIs are case-insensitive, and the entries already in the upstream `metadata.bib`
    are lowercase.
- **`backfill_openalex_topics.py`:** rows and returned works are matched on `normalize_doi(...)`,
  the DB key, on both sides. The OpenAlex filter sends each token lowercased. A row whose key
  does not start with `10.` (the rule keeps a non-DOI string whole) is skipped and counted in
  the pass-2 log line, because one malformed token in the `doi:a|b|…` filter makes OpenAlex reject
  the whole batch of 50.
- **`test_identifiers.py`:** the three names leave `DOI_MAP` and their entries leave
  `tests/fixtures/identifier_cases.json`; `test_private_doi_copies_differ_from_every_shared_rule`
  is replaced by a test that each script's `normalize_doi` is `dl_lit.identifiers.normalize_doi`.

**D2. The CLI claims rows like the daemon (PY-16), controller call.**
- `download-pdfs` claims with
  `db.claim_download_batch(limit=limit, corpus_id=None, claimed_by=cli_id, lease_seconds=900)`.
  `cli_id = f"cli:{socket.gethostname()}:{os.getpid()}:{secrets.token_hex(3)}"`. The claim skips
  rows a live daemon holds (unexpired lease), and the daemon skips the CLI's rows the same way.
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
- **Periodic recovery treats a `cli:` owner like a daemon worker.** `_periodic_owner_is_dead`
  (`worker.py:681-716`) accepts the prefixes `daemon` and `cli` for the
  `<prefix>:<host>:<pid>:<nonce>` form. Without this, the "non-daemon owner without a heartbeat
  row" clause would release a running CLI's claims within 60 s. A `cli:` owner is then released
  periodically only when it is on this host and its pid is gone.
- `fetch_download_queue` has no caller left and is deleted.

**D3. A hung item fails after 3 hangs (K-M4), controller call.**
- **Tracking.** The worker records which items are *running*, not merely claimed:
  `self._running_items: dict[tuple[str, int], float]` (phase, work id → start time), guarded by
  `self._running_items_lock`. The context manager `_item_running(phase: str, work_id: int)` adds
  the entry and removes it in its `finally`. It wraps:
  - the body of each submitted download (`do_download`, `worker.py:1304-1305`) with phase
    `"download"`;
  - each submitted `_enrich_single` and the main-thread `_attempt_raw_download(entry)` call
    (`:1108`) with phase `"enrich"`, because the metadata claim is what that item holds.

  A row that waits in the pool's queue or in a later enrich wave is claimed but not running, so it
  is never blamed.
- **At a watchdog win.** This covers both a job win and a direct-batch win, because both end in
  `_finish_after_win`, which gains the parameter `deadline: float`. After the terminal heartbeat
  attempt, inside the same `heartbeat_lock` acquisition and 1000 ms busy timeout, the worker runs
  one `UPDATE` per phase for the running items:

  ```sql
  UPDATE works
     SET <status> = 'failed', <error> = ?, <claimed_by> = NULL, <claimed_at> = NULL, <lease> = NULL
         [, status_notes = ?]                      -- download only, as move_entry_to_failed does
   WHERE id IN (<running ids>) AND <status> = 'in_progress' AND <claimed_by> = <this worker_id>
     AND COALESCE(<attempt_count>, 0) >= <limit>
  ```

  The columns are the `metadata_*` set for `"enrich"` and the `download_*` set for `"download"`.
  The error text is `hung past the <deadline>s timeout; attempt limit <limit> reached` (deadline
  formatted with `:g`).
- **Below the limit, the item is left to recovery.** Restart recovery returns it to the queue
  as today. Each claim adds 1 to `*_attempt_count` (`db_manager.py:1769`, `:2713`), so an item
  that hangs on every attempt is failed at the win of its third claim. The restart loop ends after
  three timeouts.
- **Limit:** `env_int("RAG_FEEDER_HUNG_ITEM_MAX_ATTEMPTS", 3, minimum=1)`. The attempt count
  counts claims. A row the user re-queued earlier therefore reaches the limit sooner. That is
  acceptable: the row is blamed only when it is running at a timeout.
- **Log lines, one per win:**
  - when it wrote: `Watchdog: <n> hung item(s) failed at the attempt limit, <m> left for a retry after the restart.`;
  - when the write was skipped (lock timeout or exception): `Watchdog: hung items not recorded (<reason>).`.

  A skipped write costs one more restart cycle, since recovery requeues the item. The exit still
  follows. From the win to the exit, the bound becomes about 3 s (1 s lock, 2 × 1 s busy).
- No new column. No change to claim ordering or to start-up recovery.

### Backend

**D4. Kantropos assignment is admin-only (BE-19), controller call.**
- `requireCorpusConfigurer` (`app.js:3946-3948`) is replaced by
  `requireKantroposAssigner = requireCorpusRole([], corpusIdFromParams, { forbiddenError: 'Only the global admin can assign a Kantropos corpus' })`.
  With no roles listed, only the corpus role `'admin'` passes. `resolveRequestCorpusRole` still
  answers a missing or invalid corpus id first, so the admin keeps getting `403` there
  (`access-routes.test.js:392`) and no assignment row for a missing corpus can be written. Every
  non-admin, owner and editor included, gets
  `403 { error: 'Only the global admin can assign a Kantropos corpus' }`.
- The GET answers `can_edit: isAdminUser(req.user, authConfig)`.
- Existing assignments are kept. Assignments an owner made before this change keep their
  rights until an admin changes or clears them. `canUseKantroposTarget` is unchanged: owners and
  editors of an assigned corpus may still apply, but only an admin creates the assignment.

**D5. Per-account login limit (BE-40 remainder), controller call.**
- New module `backend/src/loginLimiter.js`:
  `createLoginLimiter({ maxFailures = 10, windowMs = 15 * 60 * 1000, now = () => Date.now() } = {}) -> { take(key), refund(key), clear(key) }`.
  - **State:** a `Map` from key to `{ count, windowStart }`.
  - **`take(key)`:**
    - an entry whose window has passed (`now() - windowStart >= windowMs`) is dropped first;
    - when `count >= maxFailures`, it returns
      `{ allowed: false, retryAfterSeconds: Math.max(1, Math.ceil((windowStart + windowMs - now()) / 1000)) }`;
    - otherwise it counts the attempt (a new entry starts its window at `now()`) and returns
      `{ allowed: true }`.
  - **`refund(key)`** undoes one count, and deletes the entry at 0. **`clear(key)`** deletes it.
- **The attempt is counted before the password check.** Concurrent wrong passwords therefore
  cannot get more than 10 checks into one window.
- `createApp({ broadcast, broadcastEvent, mailTransport, loginLimiter } = {})` uses the given
  limiter or `createLoginLimiter()`. One limiter lives per app.
- **Login handler.** The key is the account's `users.id`. An unknown username (`401`) and an
  invited account (`403`) are not counted: they cost no bcrypt, and the map stays bounded by the
  number of accounts. For an existing active account:
  - `take` before the compare; when it is blocked, the answer is `429` with header
    `Retry-After: <retryAfterSeconds>` and
    `{ error: 'Too many failed sign-ins for this account. Try again in <n> minute(s).' }`. Here
    `<n> = Math.ceil(retryAfterSeconds / 60)`, and the text is "minute" when `n === 1`, else
    "minutes". A correct password is refused too until the window passes.
  - a hasher failure (`HASHER_BUSY` 503 or the 500) calls `refund`: it was not a guess;
  - a wrong password keeps the count (`401` as today);
  - a correct password calls `clear`.
- **In memory, by design.** The backend is one Node process (`index.js`, no cluster), so one map
  sees every sign-in. A restart resets the counts. That costs at most one extra window of 10
  guesses per restart.
- **The limit is per account, not per IP.** It needs no client address, so there is no proxy or
  `trust proxy` change. As with any per-account lock, anyone who knows a username can lock that
  account for 15 minutes, the admin included. The owner chose this rule.

**D6. Pipeline jobs older than 30 days are pruned (BE-46), controller call.**
- New module `backend/src/pipelineJobRetention.js`:
  - `pruneFinishedPipelineJobs(db, { days = 30 } = {}) -> number` runs one
    `DELETE FROM pipeline_jobs WHERE status IN ('completed', 'failed', 'cancelled') AND
    datetime(COALESCE(finished_at, created_at)) < datetime('now', ?)` with `'-30 days'` and
    returns the deleted count. It returns 0 when the table does not exist. `pending` and
    `running` jobs are never deleted, whatever their age.
  - `startPipelineJobRetention(db, { intervalMs = 24 * 60 * 60 * 1000, prune = pruneFinishedPipelineJobs, setIntervalFn = setInterval, clearIntervalFn = clearInterval, log = console } = {}) -> () => void`.
    It prunes once at once, then every `intervalMs`. The timer is `unref()`'d. Each run is wrapped:
    it logs `[pipeline-jobs] pruned <n> finished job(s) older than 30 days` when `n > 0`, and
    `[pipeline-jobs] retention failed: <message>` on an error (`log.log` / `log.error`). It never
    throws. The returned function clears the timer.
- **Where it runs:** the backend, which owns `authDb` and is a single process. `index.js` calls
  `startPipelineJobRetention(app.authDb)` right after `createApp`. It does not run in the Python
  workers: three roles would each prune. It does not run inside `createApp` either, so no test
  app starts a timer.

**D7. Jest cleans up its temp dirs, including after a killed run (BE-51).**
- **`tests/setup-env.js`:**
  - it throws `DT_JEST_TMP_ROOT is unset: run the backend tests through jest's globalSetup (npm test).`
    instead of falling back to `os.tmpdir()`;
  - it sets `process.env.TMPDIR` to the run dir. Node's `os.tmpdir()` reads `TMPDIR` on every
    call, so every suite's own `mkdtempSync(path.join(os.tmpdir(), …))` lands inside the run dir
    without editing the twenty suites, and Python children inherit it.
- **New `tests/jest-tmp.js`** exports
  `sweepStaleJestDirs(dir, { now = Date.now(), maxAgeMs = 24 * 60 * 60 * 1000 } = {}) -> string[]`.
  It removes every entry of `dir` whose name starts with `dt-jest-` and whose mtime is older than
  `maxAgeMs`, and returns the removed names. That covers the run dirs `dt-jest-run-*` and the
  legacy per-file `dt-jest-*` dirs. A run younger than a day, for example a concurrent run in
  another worktree, is never touched.
- `tests/global-setup.js` calls `sweepStaleJestDirs(os.tmpdir())` before it creates the run dir.
  The 29 leftovers go away on the next `npm test`; a killed run's dir goes a day later.
- `setup-env-isolation.test.js` keeps working. Its child Jest inherits `TMPDIR` (the parent's run
  dir), so its own run dir lands inside the parent's and is removed with it.

### Frontend

**D8. A failed preview asks before starting (FE-55), controller call.**
- `runSearch`'s catch keeps the 401 return. For any other error it stores
  `previewError = error?.message || 'the server did not answer'`.
- After the session and corpus check:
  - when the preview failed, it sets `searchPreview = null`,
    `searchPreviewError = previewError`, `searchWarning = true` and `searchStatus = ''`, still
    refreshes the quota, and returns without starting;
  - a successful preview clears `searchPreviewError`.
- `searchPreviewError` (`''` by default) is part of `initialWorkspaceState()` and of the
  `resetFrontendState` destructuring. Every place that sets `searchPreview = null` clears it too.
- The warning box (`data-testid="search-warning"`, same classes and `role="alert"`) gets a second
  variant, rendered when `searchWarning && !searchPreview && searchPreviewError`:
  - text: `Could not check how many works match (<message>). <cap sentence> Narrow the query, or:`;
  - cap sentence, with `cap = searchResultCap(searchMaxResults)`: when `cap === 0`,
    `This search has no cap, so it may fetch a very large number of works.`; otherwise
    `This search stops at <cap, en-US grouping> works.`;
  - one primary button, `Search anyway`, which calls `startSearch()`.
- The rule holds whatever the cap: the count is unknown, and so is the server's threshold, which
  only the preview reports. Submitting the form again runs the preview again.

**D9. The assignment control is admin-only (BE-19 UI).**
- `canConfigureCurrentCorpusKantropos = Boolean(currentCorpusId && isAdmin)` (`App.svelte:544`).
- The disabled `<select>`'s title becomes `Only admins can change the Kantropos assignment`
  (`UpstreamBrowser.svelte:254`).
- The save guard's message (`App.svelte:2580`) becomes
  `Only admins can change the Kantropos assignment.`.

**D10. Worker errors are text (FE-23, error-text half).**
- New `frontend/src/lib/workerStatus.js` exports `workerErrorText(details) -> string`. It returns
  the first non-empty string of `details.error` and `details.message`, then
  `details.state`. With none of them, it returns `JSON.stringify(details || {})`.
- `loadPipelineWorkerStatus` uses it for both `last_error` values.

**D11. The 429 sign-in text reaches the form (D5 UI).** `handleLogin` already shows
`error.message` (`App.svelte:1725`), and `requestJson` carries the body's `error`. No code
changes; a mocked spec pins it.

### CI

**D12. CI runs the whole Python suite (Problem 10).** The `python-search` job is renamed
`python-tests` (display name `Python tests`). It keeps its pip list and runs
`python -m pytest tests -q`. The two `unittest discover` lines are removed: pytest collects those
files. No file is excluded; the spec records the evidence (Problem 10). A future test file that
needs a new package then fails CI until `ci.yml:67` lists the package.

## Order of work

Tasks are grouped by surface; tasks on different surfaces touch disjoint files.
1. Python:
   - D1;
   - D2;
   - D3, after D2, because both edit `backend/scripts/daemon/worker.py` (different functions).
2. Backend:
   - D4 and D5 (`app.js`, `loginLimiter.js`);
   - D6 and D7 (new modules, `index.js`, Jest setup).
3. Frontend: D8 to D11.
4. CI: D12.

Every task except D3 can run in parallel with the others.

## Acceptance

- Every change has a test that fails on `0702e47` and passes after its task. Each commit names the
  rows it closes.
- Python: `cd dl_lit_project && /home/jay/DT/venv/bin/python -m pytest tests -q` is green.
- Backend: `cd backend && npm test` is green. On this machine, suites can time out under the
  full parallel load and pass alone; compare with a `0702e47` baseline, or use
  `npm test -- --maxWorkers=2`, before blaming a task. Also:
  - `grep -n "requireCorpusConfigurer" backend/src/app.js` finds nothing;
  - after `npm test`, `ls -d /tmp/dt-jest-*` lists nothing older than a day.
- Frontend: `npx vite build` succeeds. The specs this phase touches pass with
  `E2E_FRONTEND_PORT=5190 npx playwright test <specs> --workers=1`, and so does the CI Playwright
  list (`ci.yml:54`). No live spec is run, and the dev stack on `:5175` is not used.
- CI: the workflow's Python job runs `python -m pytest tests -q` and nothing else.
- Never against `dl_lit_project/data/literature.db`.
- After merge, check on the dev stack:
  - 11 wrong passwords for a test account answer `429` with `Retry-After`;
  - a non-admin owner sees the assignment select disabled;
  - with OpenAlex blocked, a search shows the unknown-count warning;
  - the backend log shows the first prune.

## Controller calls (for the owner)

- **D1 (PY-14):** exports and the backfill use the DB key (`normalize_doi`, uppercase).
  `upstream_update` writes the same rule's lowercase form into the BibTeX `doi` field and the
  manifest, because people and the RAG read those values and the existing upstream entries are
  lowercase. The backfill skips values that are not `10.` DOIs. No reader of old bundles exists in
  this repo.
- **D2 (PY-16):** the CLI claims rows as `cli:<host>:<pid>:<nonce>`, releases them on exit,
  dedupes only into downloaded works, and periodic recovery treats it like a daemon worker. The CLI
  gets no heartbeat: a daemon restart during a CLI run can still release the CLI's claims.
- **D3 (K-M4):** an item running when a watchdog wins is failed once its attempt count reaches 3
  (`RAG_FEEDER_HUNG_ITEM_MAX_ATTEMPTS`); below that it is retried after the restart. Claimed rows
  that had not started are never blamed. The error is
  `hung past the <N>s timeout; attempt limit 3 reached`.
- **D4 (BE-19):** only the admin may assign; existing assignments made by owners stay until an
  admin changes them.
- **D5 (BE-40):** 10 wrong passwords for one account within 15 minutes lock that account until
  the window passes, even for the right password; a successful sign-in clears the count; unknown
  usernames are not counted; in memory, reset by a backend restart. Anyone can lock a known
  account for 15 minutes.
- **D6 (BE-46):** the backend deletes `completed`, `failed` and `cancelled` jobs 30 days after they
  finished (or were created, when they never finished), at start-up and then daily.
- **D7 (BE-51):** Jest's temp dirs live under one run dir, and each `npm test` removes `dt-jest-*`
  leftovers older than a day.
- **D8 (FE-55):** a failed preview always asks, whatever the cap, with a "Search anyway" button.
- **D10 (FE-23):** worker errors show their `error` text instead of JSON.
- **D12:** CI runs every Python test file; nothing needed an exclusion.
