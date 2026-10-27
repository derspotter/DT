# Worker settings and page/OpenAlex performance — design

Date: 2026-10-20. Agreed in chat with Jay; argues from the Codex (gpt-6-astra)
performance consult of 2026-09-07 and the admin-settings gap found 2026-09-08.

## Problems

1. **Admin settings never reach the background workers.** The Admin tab stores
   the OpenAlex key, LLM provider, provider keys and models in `app_settings`.
   The backend overlays them on the environment of every Python child it
   spawns (`appSettingsEnv()`), but the enrich and download daemons read only
   the compose environment at start-up. A provider or key changed in Admin
   applies to interactive runs and silently not to queue jobs.
2. **OpenAlex paging opens a new connection per page.** The keyword-search
   request wrapper never passes a `requests.Session`, so each of the ~1.1 s
   pages pays a TLS handshake.
3. **Four read routes spawn a Python child per request** and materialize the
   whole corpus before paging: `/api/corpus`, `/api/downloads`,
   `/api/ingest/runs`, `/api/ingest/stats`. 100–270 ms each, on every 3 s poll.
4. **The live refresh polls every 3 s regardless of activity or visibility.**
   Hidden tabs and idle workspaces re-fetch corpus, seeds and counts forever.
5. **Both stacks serve the Vite dev server in production.** Source modules are
   served on demand, nothing is minified or cached, and a stopgap `fs.deny`
   guards config files.

## Decisions

- **Workers resolve settings per job from the database**, same precedence as
  the backend: stored value wins over environment; blank stored values do not
  override. The worker rebuilds its `BibliographyEnhancer` (which caches the
  LLM client) only when the resolved settings change. Admin-only settings
  remain the model; per-user keys are out of scope (Jay, 2026-09-23).
- **One shared `requests.Session` per thread** for OpenAlex calls from
  `dl_lit.keyword_search`, via a helper in `dl_lit.utils`.
- **The four read routes move to `better-sqlite3` with projection, filter,
  order and paging in SQL.** The Python scripts stay on disk (other callers,
  rollback) but the routes no longer spawn them. A golden test pins the Node
  result against the Python script's output on a fixture database for the
  corpus listing, which carries the most logic (status precedence, provenance
  labels, author sort, deterministic ties).
- **One refresh scheduler** replaces the two intervals and the log-triggered
  timer: hidden tab → no polling; visible and active (search running, worker
  running) → 2 s; visible and idle on a workspace tab → 20 s; local mutation
  or worker log line → one trailing refresh. Becoming visible triggers one
  immediate refresh.
- **Production frontend build served by Caddy inside the image**: the
  Dockerfile's `runner` stage serves `dist/` and reverse-proxies `/api/*`
  (including the WebSocket) to the backend, so the public Caddy keeps one
  upstream. Selected by a `docker-compose.prod.yml` overlay; the default
  compose file keeps the dev server for genkia.de hot reload. Switching
  production is a manual deploy step documented in the README, not part of
  the branch.

## Non-goals

Per-user API keys; parallel OpenAlex cursors and the quota governor (needs
checkpoints first); WebSocket change notifications; GROBID; phase 4 hard caps.

## Verification

Jest for the Node listings (golden against Python), pytest for settings
resolution and session reuse, a mocked Playwright spec counting requests per
refresh mode, and a built production image curl-checked for `/`, a deep link,
`/api/*` proxying, and 404 on source paths.
